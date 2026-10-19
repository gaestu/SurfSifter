"""Opt-in URL query-plan and performance benchmark (issues #61, #67).

Data layer only (no Qt). Builds a deterministic synthetic case on disk with
``tests/fixtures/url_fixtures.py`` (90% NULL ``first_seen_utc`` as in real data,
see #47; 1000 domains; 4 sources; sparse tags: every 100th URL; matches: every
20th URL over 3 lists) and measures, for each URL filter:

- EXPLAIN QUERY PLAN of the keyset phase-1 / phase-2, OFFSET and COUNT queries,
- the first page (500 rows + 1 look-ahead row) via keyset,
- a deep page at 5% and 90% depth via OFFSET (``iter_urls``) vs. via cursor
  (``fetch_url_rows_after``; the cursor comes from an untimed OFFSET query),
- the exact COUNT,

on several variants of the evidence database (copies of the base case):

- ``stats``          as built by the fixture (all migrations, ANALYZE run)
- ``nostats``        sqlite_stat1 emptied: what the app sees, it never runs ANALYZE
- ``approx_stats``   nostats + ``PRAGMA analysis_limit=1000; ANALYZE``
- ``no0004*``        without idx_urls_evidence_domain_nocase_first_seen
- ``source_pre0005_nostats``  idx_urls_evidence_source as before migration 0005
  (evidence_id, discovered_by)
- ``source_0005_nostats``     the same copy after running the real migration 0005

plus index sizes (dbstat), upgrade cost at first open (CREATE INDEX of
migration 0004, migration 0005 script) on a populated table, ANALYZE /
PRAGMA optimize cost, and URL ingest time with all indexes (``current``),
without the 0004 index, and with the pre-0005 source index. Timings are medians in ms. Only
correctness invariants are asserted (cursor page == OFFSET page, count ==
keyset walk, identical results across variants), never timing thresholds.

Opt-in: skipped unless URL_QUERY_BENCH_ROWS is set. Run explicitly:

    URL_QUERY_BENCH_ROWS=1000000 poetry run pytest tests/app/test_url_query_benchmark.py -m slow -s

On hybrid CPUs, pin to performance cores (e.g. ``taskset -c 0-5``) for stable numbers.

Environment:
    URL_QUERY_BENCH_ROWS         URL rows (required; 1_000_000 standard, 10_000_000 opt-in)
    URL_QUERY_BENCH_DIR          work folder; the base case in it is reused across runs
    URL_QUERY_BENCH_OUTPUT       write JSON results to this path
    URL_QUERY_BENCH_REPEAT       timed runs per measurement (default 5)
    URL_QUERY_BENCH_INGEST_ROWS  rows for the ingest comparison (default: ROWS)
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import sqlite3
import statistics
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import pytest

from app.data import CaseDataAccess, UrlCursor, UrlFilterSpec, UrlPageQuery
from core.database import EVIDENCE_MIGRATIONS_DIR, DatabaseManager
from tests.fixtures.url_fixtures import UrlFixtureCase, _populate_evidence, build_url_fixture_case

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        not os.environ.get("URL_QUERY_BENCH_ROWS"), reason="set URL_QUERY_BENCH_ROWS to run the benchmark"
    ),
]

ROWS = int(os.environ.get("URL_QUERY_BENCH_ROWS") or 0)
REPEAT = int(os.environ.get("URL_QUERY_BENCH_REPEAT") or 5)
PAGE_SIZE = 500
DEPTHS = (0.05, 0.9)
WALK_LIMIT = 50_000  # keyset-walk / full-order checks only for filters up to this size
FIXTURE = dict(timestamp_every=10, tag_every=100, match_every=20, domain_count=1000)
NEW_INDEX = "idx_urls_evidence_domain_nocase_first_seen"
SOURCE_INDEX = "idx_urls_evidence_source"
# idx_urls_evidence_source before evidence migration 0005 (issue #67).
OLD_SOURCE_INDEX_SQL = f"CREATE INDEX {SOURCE_INDEX} ON urls(evidence_id, discovered_by)"
NEW_INDEX_SQL = f"CREATE INDEX {NEW_INDEX} ON urls(evidence_id, domain COLLATE NOCASE, first_seen_utc DESC)"

# Fixture row i has a timestamp iff i % 10 == 0, domain d{i % 1000}, source
# i % 4. Hence a domain is either fully timestamped (d120) or never (d123), and
# odd sources (firefox_history, bulk_extractor) never are; tagged (every 100th)
# and matched (every 20th) URLs are all timestamped. Comments: share of rows.
FILTERS: Dict[str, UrlFilterSpec] = {
    "default": UrlFilterSpec(),
    "domain_exact": UrlFilterSpec(domain_exact="D123.Example.COM"),  # 0.1%, no timestamps
    "domain_exact_ts": UrlFilterSpec(domain_exact="d120.example.com"),  # 0.1%, all timestamped
    "domain_contains_rare": UrlFilterSpec(domain_contains="d999."),  # 0.1%, no timestamps
    "domain_contains_broad": UrlFilterSpec(domain_contains="d1"),  # 11.1%
    "source_one": UrlFilterSpec(discovered_by=("chromium_history",)),  # 25%, 20% timestamped
    "source_no_ts": UrlFilterSpec(discovered_by=("bulk_extractor",)),  # 25%, no timestamps
    "source_two": UrlFilterSpec(discovered_by=("chromium_history", "bulk_extractor")),  # 50%
    "tag": UrlFilterSpec(tag_like="Relevant"),  # 0.5%
    "matched": UrlFilterSpec(match_filter="matched"),  # 5%
    "unmatched": UrlFilterSpec(match_filter="unmatched"),  # 95%
    "match_list": UrlFilterSpec(match_filter="list1"),  # ~1.7%
}
DOMAIN_FILTERS = ("domain_exact", "domain_exact_ts", "domain_contains_rare")
SOURCE_FILTERS = ("default", "source_one", "source_no_ts", "source_two")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _timed(fn: Callable[[], Any]) -> Tuple[float, Any]:
    start = time.perf_counter()
    result = fn()
    return round((time.perf_counter() - start) * 1000, 1), result


def _median_ms(fn: Callable[[], Any]) -> Tuple[float, Any]:
    runs = [_timed(fn) for _ in range(REPEAT)]
    return round(statistics.median(ms for ms, _ in runs), 2), runs[-1][1]


def _ids(rows: Iterable[Dict[str, Any]]) -> List[int]:
    return [row["id"] for row in rows]


def _digest(ids: List[int]) -> str:
    return hashlib.sha1(",".join(map(str, ids)).encode()).hexdigest()[:12]


def _assert_url_order(rows: List[Dict[str, Any]]) -> None:
    """first_seen_utc DESC with NULLs last, ties (and NULLs) by ascending id."""
    for a, b in zip(rows, rows[1:]):
        ta, tb = a["first_seen_utc"], b["first_seen_utc"]
        if ta is None:
            assert tb is None and b["id"] > a["id"]
        elif tb is not None:
            assert ta > tb or (ta == tb and b["id"] > a["id"])


class _SqlTrace:
    """Records the expanded SQL of URL row/count statements, keyed by step."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.step = "setup"
        self.statements: Dict[str, str] = {}

    def __enter__(self) -> "_SqlTrace":
        self.conn.set_trace_callback(self._on_sql)
        return self

    def __exit__(self, *exc) -> None:
        self.conn.set_trace_callback(None)

    def _on_sql(self, sql: str) -> None:
        if self.step == "setup" or "FROM urls u" not in sql:
            return
        if "COUNT(*)" in sql:
            kind = "count"
        elif "OFFSET" in sql:
            kind = "offset"
        elif "first_seen_utc IS NOT NULL" in sql:
            kind = "phase1"
        else:
            kind = "phase2"
        self.statements.setdefault(f"{self.step}:{kind}", sql)

    def plans(self) -> Dict[str, str]:
        return {
            key: " | ".join(row[3] for row in self.conn.execute("EXPLAIN QUERY PLAN " + sql))
            for key, sql in self.statements.items()
        }


def _measure_filter(data: CaseDataAccess, conn: sqlite3.Connection, evidence_id: int,
                    spec: UrlFilterSpec) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    with _SqlTrace(conn) as trace:
        trace.step = "count"
        out["count_ms"], total = _median_ms(lambda: data.count_urls(evidence_id, filters=spec))
        out["count"] = total

        trace.step = "first"
        out["first_page_ms"], first = _median_ms(
            lambda: data.fetch_url_rows_after(evidence_id, spec, None, PAGE_SIZE + 1))
        trace.step = "setup"
        out["first_page_full_ms"], page = _median_ms(
            lambda: data.load_url_page(UrlPageQuery(evidence_id, spec, page_size=PAGE_SIZE)))
        assert _ids(page.rows) == _ids(first[:PAGE_SIZE])
        assert page.has_more == (len(first) > PAGE_SIZE)
        _assert_url_order(first)
        out["first_ids"] = _digest(_ids(first))

        for depth in DEPTHS:
            offset = int(total * depth)
            if offset < 1:
                continue
            tag = f"p{int(depth * 100)}"
            trace.step = "setup"
            before = data.iter_urls(evidence_id, filters=spec, limit=1, offset=offset - 1)
            cursor = UrlCursor.from_row(before[0])
            trace.step = f"{tag}_offset"
            out[f"{tag}_offset_ms"], by_offset = _median_ms(
                lambda: data.iter_urls(evidence_id, filters=spec, limit=PAGE_SIZE + 1, offset=offset))
            trace.step = f"{tag}_cursor"
            out[f"{tag}_cursor_ms"], by_cursor = _median_ms(
                lambda: data.fetch_url_rows_after(evidence_id, spec, cursor, PAGE_SIZE + 1))
            assert _ids(by_cursor) == _ids(by_offset), f"cursor page != OFFSET page at {tag}"
            out[f"{tag}_ids"] = _digest(_ids(by_cursor))
        trace.step = "setup"

        if total <= WALK_LIMIT:
            walk = list(data.iter_url_rows(evidence_id, spec, batch_size=PAGE_SIZE))
            assert len(walk) == total, "count disagrees with keyset walk"
            assert _ids(walk) == _ids(data.iter_urls(evidence_id, filters=spec, limit=total + 1))
            assert len(set(_ids(walk))) == total
            _assert_url_order(walk)
            out["walk_checked"] = True
    out["plans"] = trace.plans()
    return out


def _evidence_db_path(case: UrlFixtureCase, folder: Path) -> Path:
    manager = DatabaseManager(folder, case_db_path=folder / case.case_db_path.name)
    try:
        eid = case.evidence_ids[0]
        return manager.evidence_db_path(eid, case.labels[eid], create_dirs=False)
    finally:
        manager.close_all()


def _raw(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA cache_size = -64000")  # same as DatabaseManager
    conn.execute("PRAGMA temp_store = MEMORY")
    return conn


def _drop_stats(db_path: Path) -> None:
    conn = _raw(db_path)
    try:
        for table in ("sqlite_stat1", "sqlite_stat4"):
            if conn.execute("SELECT 1 FROM sqlite_master WHERE name = ?", (table,)).fetchone():
                conn.execute(f"DELETE FROM {table}")
    finally:
        conn.close()


def _exec_ms(db_path: Path, *sql: str) -> float:
    conn = _raw(db_path)
    try:
        start = time.perf_counter()
        for statement in sql:
            conn.execute(statement)
        return round((time.perf_counter() - start) * 1000, 1)
    finally:
        conn.close()


def _script_ms(db_path: Path, script: str) -> float:
    conn = _raw(db_path)
    try:
        start = time.perf_counter()
        conn.executescript(script)
        return round((time.perf_counter() - start) * 1000, 1)
    finally:
        conn.close()


def _migration_sql(number: int) -> str:
    (path,) = EVIDENCE_MIGRATIONS_DIR.glob(f"{number:04d}_*.sql")
    return path.read_text()


def _index_sizes(db_path: Path) -> Dict[str, int]:
    conn = _raw(db_path)
    try:
        rows = conn.execute(
            "SELECT s.name, s.pgsize FROM dbstat AS s JOIN sqlite_master AS m ON m.name = s.name "
            "WHERE s.aggregate = 1 AND m.tbl_name = 'urls' ORDER BY s.name"
        ).fetchall()
        return {name: size for name, size in rows}
    finally:
        conn.close()


class _AnalyzeTimer:
    """Connection proxy that times ANALYZE separately from the inserts."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self.analyze_ms = 0.0

    def execute(self, sql: str, *args):
        start = time.perf_counter()
        try:
            return self._conn.execute(sql, *args)
        finally:
            if sql.strip().upper() == "ANALYZE":
                self.analyze_ms += (time.perf_counter() - start) * 1000

    def __getattr__(self, name: str):
        return getattr(self._conn, name)

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, *exc):
        return self._conn.__exit__(*exc)


# ---------------------------------------------------------------------------
# Fixture and runners
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def work_dir(tmp_path_factory) -> Path:
    folder_env = os.environ.get("URL_QUERY_BENCH_DIR")
    return Path(folder_env) if folder_env else tmp_path_factory.mktemp("url_query_bench")


@pytest.fixture(scope="module")
def bench_case(work_dir: Path) -> Tuple[UrlFixtureCase, Optional[float]]:
    folder = work_dir / f"base_{ROWS}"
    marker = folder / ".url_query_bench_ok"
    build_s: Optional[float] = None
    if not marker.exists():
        if folder.exists():
            shutil.rmtree(folder)
        start = time.perf_counter()
        build_url_fixture_case(folder, rows_per_evidence=ROWS, **FIXTURE)
        build_s = round(time.perf_counter() - start, 1)
        print(f"\n[bench] built {ROWS:,} URL rows in {build_s}s at {folder}")
        marker.write_text("ok")
    manager = DatabaseManager(folder, case_db_path=folder / "bench_surfsifter.sqlite")
    try:
        rows = manager.get_case_conn().execute("SELECT id, label FROM evidences ORDER BY id").fetchall()
    finally:
        manager.close_all()
    case = UrlFixtureCase(
        case_folder=folder,
        case_db_path=folder / "bench_surfsifter.sqlite",
        evidence_ids=[r[0] for r in rows],
        labels={r[0]: r[1] for r in rows},
    )
    return case, build_s


def _run_filters(case: UrlFixtureCase, folder: Path, names: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    data = CaseDataAccess(folder, folder / case.case_db_path.name)
    try:
        eid = case.evidence_ids[0]
        conn = data.db_manager.get_evidence_conn(eid, case.labels[eid])
        return {name: _measure_filter(data, conn, eid, FILTERS[name]) for name in names}
    finally:
        data.close()


def _copy_case(case: UrlFixtureCase, work_dir: Path, name: str) -> Tuple[Path, Path]:
    folder = work_dir / "variants" / name
    if folder.exists():
        shutil.rmtree(folder)
    shutil.copytree(case.case_folder, folder)
    return folder, _evidence_db_path(case, folder)


def _ingest_ms(work_dir: Path, rows: int, name: str, *setup_sql: str) -> Dict[str, float]:
    """Time URL ingest into a migrated, empty case after running ``setup_sql``."""
    folder = work_dir / "ingest" / name
    if folder.exists():
        shutil.rmtree(folder)
    case = build_url_fixture_case(folder, rows_per_evidence=0)  # migrated, empty
    manager = DatabaseManager(folder, case_db_path=case.case_db_path)
    try:
        eid = case.evidence_ids[0]
        conn = manager.get_evidence_conn(eid, case.labels[eid])
        for statement in setup_sql:
            conn.execute(statement)
        proxy = _AnalyzeTimer(conn)
        total_ms, _ = _timed(lambda: _populate_evidence(
            proxy, eid, rows=rows, timestamp_every=FIXTURE["timestamp_every"], tag_every=0,
            match_every=0, domain_count=FIXTURE["domain_count"], batch_size=50_000))
    finally:
        manager.close_all()
    shutil.rmtree(folder)
    return {"insert_ms": round(total_ms - proxy.analyze_ms, 1), "analyze_ms": round(proxy.analyze_ms, 1)}


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------


def test_url_query_benchmark(bench_case, work_dir: Path):
    case, build_s = bench_case
    base_db = _evidence_db_path(case, case.case_folder)
    results: Dict[str, Any] = {"variants": {}, "costs": {}}
    variants = results["variants"]
    costs = results["costs"]

    variants["stats"] = _run_filters(case, case.case_folder, FILTERS)
    results["sizes"] = _index_sizes(base_db)
    results["evidence_db_bytes"] = base_db.stat().st_size

    # No planner statistics (the app never runs ANALYZE), then approximate / full stats.
    folder, db = _copy_case(case, work_dir, "nostats")
    _drop_stats(db)
    variants["nostats"] = _run_filters(case, folder, FILTERS)
    conn = _raw(db)
    try:  # Does PRAGMA optimize (3.45 semantics) analyze urls after it was queried?
        conn.execute("SELECT id FROM urls WHERE evidence_id = 1 AND domain = 'x' COLLATE NOCASE").fetchall()
        costs["pragma_optimize_ms"], _ = _timed(lambda: conn.execute("PRAGMA optimize"))
        costs["pragma_optimize_stat1_rows_for_urls"] = conn.execute(
            "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl = 'urls'").fetchone()[0]
    finally:
        conn.close()
    _drop_stats(db)
    costs["analyze_limit_1000_ms"] = _exec_ms(db, "PRAGMA analysis_limit = 1000", "ANALYZE")
    variants["approx_stats"] = _run_filters(case, folder, FILTERS)
    costs["analyze_full_ms"] = _exec_ms(db, "ANALYZE")
    shutil.rmtree(folder)

    # Without the migration-0004 index (with and without stats), then its build cost.
    folder, db = _copy_case(case, work_dir, "no0004")
    _exec_ms(db, f"DROP INDEX {NEW_INDEX}")
    variants["no0004"] = _run_filters(case, folder, DOMAIN_FILTERS)
    _drop_stats(db)
    variants["no0004_nostats"] = _run_filters(case, folder, DOMAIN_FILTERS)
    costs["create_index_0004_ms"] = _exec_ms(db, NEW_INDEX_SQL)
    shutil.rmtree(folder)

    # Source index as before migration 0005, then the real 0005 upgrade (issue #67).
    folder, db = _copy_case(case, work_dir, "source_pre0005")
    _drop_stats(db)
    _exec_ms(db, f"DROP INDEX {SOURCE_INDEX}", OLD_SOURCE_INDEX_SQL)
    results["sizes"][f"{SOURCE_INDEX} (pre-0005)"] = _index_sizes(db)[SOURCE_INDEX]
    variants["source_pre0005_nostats"] = _run_filters(case, folder, SOURCE_FILTERS)
    costs["migration_0005_ms"] = _script_ms(db, _migration_sql(5))
    variants["source_0005_nostats"] = _run_filters(case, folder, SOURCE_FILTERS)
    shutil.rmtree(folder)

    # Results must not depend on plans/indexes.
    for name, measured in variants.items():
        for filter_name, m in measured.items():
            ref = variants["stats"][filter_name]
            for key in ("count", "first_ids", "p5_ids", "p90_ids"):
                assert m.get(key) == ref.get(key), f"{name}/{filter_name}: {key} differs"
    assert variants["stats"]["default"]["count"] == ROWS
    assert variants["stats"]["domain_exact"]["count"] == len(range(123, ROWS, FIXTURE["domain_count"]))
    assert variants["stats"]["source_no_ts"]["count"] == len(range(3, ROWS, 4))

    ingest_rows = int(os.environ.get("URL_QUERY_BENCH_INGEST_ROWS") or ROWS)
    results["ingest"] = {
        "rows": ingest_rows,
        "current": _ingest_ms(work_dir, ingest_rows, "current"),
        "without_0004": _ingest_ms(work_dir, ingest_rows, "without_0004", f"DROP INDEX {NEW_INDEX}"),
        "source_pre0005": _ingest_ms(
            work_dir, ingest_rows, "source_pre0005", f"DROP INDEX {SOURCE_INDEX}", OLD_SOURCE_INDEX_SQL
        ),
    }

    results["environment"] = {
        "rows": ROWS,
        "fixture": FIXTURE,
        "repeat": REPEAT,
        "page_size": PAGE_SIZE,
        "fixture_build_s": build_s,
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "platform": platform.platform(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
    }
    _print_report(results)
    output = os.environ.get("URL_QUERY_BENCH_OUTPUT")
    if output:
        Path(output).write_text(json.dumps(results, indent=2))


def _print_report(results: Dict[str, Any]) -> None:
    print("\n[bench] environment:", json.dumps(results["environment"]))
    columns = ("count", "count_ms", "first_page_ms", "first_page_full_ms",
               "p5_offset_ms", "p5_cursor_ms", "p90_offset_ms", "p90_cursor_ms")
    for variant, measured in results["variants"].items():
        print(f"[bench] --- {variant}: " + " | ".join(columns))
        for name, m in measured.items():
            print(f"[bench] {name:22} " + " | ".join(str(m.get(c, "-")) for c in columns))
        for name, m in measured.items():
            for step, plan in m["plans"].items():
                print(f"[bench]   plan {variant}/{name}/{step}: {plan}")
    print("[bench] sizes:", json.dumps(results["sizes"]))
    print("[bench] evidence_db_bytes:", results["evidence_db_bytes"])
    print("[bench] costs:", json.dumps(results["costs"]))
    print("[bench] ingest:", json.dumps(results["ingest"]))
