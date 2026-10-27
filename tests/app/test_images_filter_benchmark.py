"""Opt-in Images tab filter benchmark (issue #74).

Data layer only (no Qt). Builds deterministic synthetic cases on disk and
measures, per Images filter, the first page of ``iter_images`` (page size 200,
offset 0: what the Grid and Table models load synchronously on the GUI thread
when a filter changes) and its EXPLAIN QUERY PLAN.

Fixture: image ``i`` has ``DISCOVERIES`` discoveries ``k``. Discovery URLs are
``https://cdn{i % 50}.example.com/img/{i:07d}_{k}.jpg`` and cache keys
``key/{i:07d}/{k}``, except that:

- every 10th image has one discovery with ``favicon`` in its URL,
- every 20th image has one discovery on ``lh3.googleusercontent.com``,
- every 7th image has only discoveries without URL and cache key (carved).

Cases (each measured as built, without planner statistics, and on a copy
after ANALYZE, ``*_stats``):

- ``sparse``         every 100th image tagged; all filters
- ``mostly_tagged``  every image but each 100th tagged; tag filters only

Timings are medians in ms of ``REPEAT`` warm runs. Only correctness invariants
are asserted (include X + exclude X == all images, untagged counts), never
timings.

Opt-in: skipped unless IMAGES_QUERY_BENCH_ROWS is set. Run explicitly:

    IMAGES_QUERY_BENCH_ROWS=100000 poetry run pytest \\
        tests/app/test_images_filter_benchmark.py -m slow -s

Environment:
    IMAGES_QUERY_BENCH_ROWS    images (required; 100_000 standard)
    IMAGES_QUERY_BENCH_DIR     work folder; base cases in it are reused across runs
    IMAGES_QUERY_BENCH_OUTPUT  write JSON results to this path
    IMAGES_QUERY_BENCH_REPEAT  timed runs per measurement (default 5)
"""
from __future__ import annotations

import json
import os
import platform
import shutil
import sqlite3
import statistics
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Tuple

import pytest

from app.data import CaseDataAccess, discard_case_stats_cache
from core.database import DatabaseManager

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        not os.environ.get("IMAGES_QUERY_BENCH_ROWS"),
        reason="set IMAGES_QUERY_BENCH_ROWS to run the benchmark",
    ),
]

ROWS = int(os.environ.get("IMAGES_QUERY_BENCH_ROWS") or 0)
REPEAT = int(os.environ.get("IMAGES_QUERY_BENCH_REPEAT") or 5)
PAGE_SIZE = 200  # ImagesListModel / ImagesTableModel default page_size
DISCOVERIES = 3
EVIDENCE_LABEL = "Images Evidence"
FIXED_TS = "2025-01-01T00:00:00Z"
CASES: Dict[str, Dict[str, int]] = {
    "sparse": dict(tag_every=100),
    "mostly_tagged": dict(untag_every=100),
}

URL_TERMS = {
    "url_exclude_rare": "0000042_",  # one image; "_" must be literal
    "url_exclude_favicon": "favicon",  # 10% of images
    "url_exclude_google": "googleusercontent",  # 5%
    "url_exclude_everywhere": "example.com",  # every image with a URL
}
FILTERS: Dict[str, Dict[str, Any]] = {
    "baseline": {},
    **{name: dict(url_excludes=term) for name, term in URL_TERMS.items()},
    "cache_key_exclude_rare": dict(cache_key_excludes="key/0000042/"),
    "url_cdn1_exclude_favicon": dict(url_text="cdn1.", url_excludes="favicon"),
    "url_include_rare": dict(url_text="0000042_"),
    "untagged": dict(untagged=True),
    "untagged_exclude_favicon": dict(untagged=True, url_excludes="favicon"),
    "untagged_browser_sources": dict(untagged=True, discovered_by=("cache_simple", "cache_blockfile")),
}
TAG_FILTERS = ("untagged", "untagged_exclude_favicon", "untagged_browser_sources")
CASE_FILTERS = {"sparse": tuple(FILTERS), "mostly_tagged": TAG_FILTERS}


def _carved(i: int) -> bool:
    return i % 7 == 0


def _discovery_rows(evidence_id: int, i: int, image_id: int) -> Iterator[Tuple[Any, ...]]:
    for k in range(DISCOVERIES):
        if _carved(i):
            yield (evidence_id, image_id, "foremost_carver", "run-1", None, None, i * 10_000 + k)
            continue
        url = f"https://cdn{i % 50}.example.com/img/{i:07d}_{k}.jpg"
        if k == 1 and i % 10 == 0:
            url = f"https://cdn{i % 50}.example.com/favicon_{i:07d}.ico"
        elif k == 2 and i % 20 == 0:
            url = f"https://lh3.googleusercontent.com/p/{i:07d}.jpg"
        source = "cache_simple" if k % 2 == 0 else "cache_blockfile"
        yield (evidence_id, image_id, source, "run-1", url, f"key/{i:07d}/{k}", None)


def _build_case(folder: Path, *, tag_every: int = 0, untag_every: int = 0) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    case_db_path = folder / "images_surfsifter.sqlite"
    manager = DatabaseManager(folder, case_db_path=case_db_path)
    try:
        case_conn = manager.get_case_conn()
        with case_conn:
            case_id = case_conn.execute(
                "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                ("IMG001", "Images Fixture", FIXED_TS),
            ).lastrowid
            evidence_id = case_conn.execute(
                "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                (case_id, EVIDENCE_LABEL, "/fixture/evidence1.E01", FIXED_TS),
            ).lastrowid
        assert evidence_id == 1
        conn = manager.get_evidence_conn(evidence_id, EVIDENCE_LABEL)
        with conn:
            tag_id = conn.execute(
                "INSERT INTO tags(evidence_id, name, name_normalized) VALUES (?, 'Relevant', 'relevant')",
                (evidence_id,),
            ).lastrowid
            images = []
            discoveries = []
            associations = []
            for i in range(ROWS):
                image_id = i + 1
                source = "foremost_carver" if _carved(i) else "cache_simple"
                ts = f"2025-{1 + i % 12:02d}-{1 + i % 28:02d}T{i % 24:02d}:{i % 60:02d}:00Z"
                images.append((
                    image_id, evidence_id, f"images/{i:07d}.jpg", f"{i:07d}.jpg",
                    f"{i:032x}", f"{i:064x}", source, ts, 1000 + (i % 5000) * 100,
                ))
                discoveries.extend(_discovery_rows(evidence_id, i, image_id))
                tagged = (i % untag_every != 0) if untag_every else (tag_every and i % tag_every == 0)
                if tagged:
                    associations.append((tag_id, evidence_id, image_id))
            conn.executemany(
                "INSERT INTO images(id, evidence_id, rel_path, filename, md5, sha256, "
                "first_discovered_by, ts_utc, size_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                images,
            )
            conn.executemany(
                "INSERT INTO image_discoveries(evidence_id, image_id, discovered_by, run_id, "
                "cache_url, cache_key, carved_offset_bytes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                discoveries,
            )
            conn.executemany(
                "INSERT INTO tag_associations(tag_id, evidence_id, artifact_type, artifact_id) "
                "VALUES (?, ?, 'image', ?)",
                associations,
            )
    finally:
        manager.close_all()
    return case_db_path


def _median_ms(fn: Callable[[], Any]) -> Tuple[float, Any]:
    timings: List[float] = []
    result = None
    for _ in range(REPEAT):
        start = time.perf_counter()
        result = fn()
        timings.append((time.perf_counter() - start) * 1000)
    return round(statistics.median(timings), 1), result


def _capture_sql(conn: sqlite3.Connection, fn: Callable[[], Any]) -> str:
    """Run ``fn`` once (untimed, warms the cache) and return its images SQL."""
    statements: List[str] = []

    def on_sql(sql: str) -> None:
        if "FROM images i" in sql:
            statements.append(sql)

    conn.set_trace_callback(on_sql)
    try:
        fn()
    finally:
        conn.set_trace_callback(None)
    assert len(statements) == 1, statements
    return statements[0]


def _count(data: CaseDataAccess, **filters: Any) -> int:
    return len(data.iter_images(1, limit=ROWS + 1, **filters))


def _measure(data: CaseDataAccess, conn: sqlite3.Connection, filters: Dict[str, Any]) -> Dict[str, Any]:
    def page() -> List[Dict[str, Any]]:
        return data.iter_images(1, limit=PAGE_SIZE, offset=0, **filters)

    sql = _capture_sql(conn, page)  # expanded SQL, literal parameters
    page_ms, rows = _median_ms(page)
    count = _count(data, **filters)  # untimed
    assert len(rows) == min(count, PAGE_SIZE)
    plan = " | ".join(row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql))
    return {"count": count, "first_page_ms": page_ms, "plan": plan}


@pytest.fixture(scope="module")
def work_dir(tmp_path_factory) -> Path:
    folder_env = os.environ.get("IMAGES_QUERY_BENCH_DIR")
    return Path(folder_env) if folder_env else tmp_path_factory.mktemp("images_query_bench")


def _base_case(work_dir: Path, name: str, build_s: Dict[str, float]) -> Path:
    folder = work_dir / f"{name}_{ROWS}"
    marker = folder / ".images_query_bench_ok"
    if not marker.exists():
        if folder.exists():
            shutil.rmtree(folder)
        start = time.perf_counter()
        _build_case(folder, **CASES[name])
        build_s[name] = round(time.perf_counter() - start, 1)
        print(f"\n[bench] built {name} ({ROWS:,} images) in {build_s[name]}s at {folder}")
        marker.write_text("ok")
    return folder


def _run_case(folder: Path, names: Tuple[str, ...], *, analyze: bool) -> Dict[str, Dict[str, Any]]:
    case_db_path = folder / "images_surfsifter.sqlite"
    discard_case_stats_cache(case_db_path)
    data = CaseDataAccess(folder, case_db_path)
    try:
        conn = data.db_manager.get_evidence_conn(1, EVIDENCE_LABEL)
        if analyze:
            conn.execute("ANALYZE")
        has_stats = conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'sqlite_stat1'").fetchone()
        assert bool(has_stats) == analyze
        results = {name: _measure(data, conn, FILTERS[name]) for name in names}
        # URL X and URL exclude X are exact complements (untimed counts).
        for name, term in URL_TERMS.items():
            if name in results:
                assert _count(data, url_text=term) + results[name]["count"] == ROWS, name
    finally:
        data.close()
    return results


def test_images_filter_benchmark(work_dir: Path):
    build_s: Dict[str, float] = {}
    cases = {name: _base_case(work_dir, name, build_s) for name in CASES}
    results: Dict[str, Any] = {"cases": {}}
    for name, folder in cases.items():
        results["cases"][name] = _run_case(folder, CASE_FILTERS[name], analyze=False)
        copy = work_dir / "variants" / f"{name}_stats"
        if copy.exists():
            shutil.rmtree(copy)
        shutil.copytree(folder, copy)
        results["cases"][f"{name}_stats"] = _run_case(copy, CASE_FILTERS[name], analyze=True)
        shutil.rmtree(copy)
        for filter_name, m in results["cases"][f"{name}_stats"].items():
            assert m["count"] == results["cases"][name][filter_name]["count"], (name, filter_name)

    carved = len(range(0, ROWS, 7))
    sparse = results["cases"]["sparse"]
    assert sparse["baseline"]["count"] == ROWS
    assert sparse["untagged"]["count"] == ROWS - len(range(0, ROWS, 100))
    assert results["cases"]["mostly_tagged"]["untagged"]["count"] == len(range(0, ROWS, 100))
    assert sparse["url_exclude_everywhere"]["count"] == carved
    assert sparse["url_exclude_favicon"]["count"] == ROWS - len(
        [i for i in range(0, ROWS, 10) if not i % 7 == 0]
    )

    results["environment"] = {
        "images": ROWS,
        "discoveries_per_image": DISCOVERIES,
        "cases": CASES,
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
    output = os.environ.get("IMAGES_QUERY_BENCH_OUTPUT")
    if output:
        Path(output).write_text(json.dumps(results, indent=2))


def _print_report(results: Dict[str, Any]) -> None:
    print("\n[bench] environment:", json.dumps(results["environment"]))
    for case, measured in results["cases"].items():
        print(f"[bench] --- {case}")
        print(f"[bench] {'filter':28} {'rows':>9} {'page ms':>9}")
        for name, m in measured.items():
            print(f"[bench] {name:28} {m['count']:>9} {m['first_page_ms']:>9}")
        for name, m in measured.items():
            print(f"[bench]   plan {case}/{name}: {m['plan']}")
