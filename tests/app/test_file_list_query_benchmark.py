"""Opt-in File List query benchmark (issue #73).

Data layer only (no Qt). Builds deterministic synthetic cases on disk with
``tests/fixtures/file_list_fixtures.py`` and measures, per File List filter:

- the first page (``FileListModel.PAGE_SIZE`` = 10000 rows, offset 0),
- the exact COUNT,
- EXPLAIN QUERY PLAN of the page and count queries.

The tab loads the first page and the count synchronously on the GUI thread
when a filter changes, so first page + count is the user-visible blocking
time. Cases:

- ``sparse``         every 100th file tagged, every 20th matched; all filters
- ``mostly_tagged``  every file but each 100th tagged; tag filters only

Each case is measured as built, without planner statistics (what the app
sees: it never runs ANALYZE), and on a copy after ANALYZE (``*_stats``).

Timings are medians in ms of ``REPEAT`` warm runs. Only correctness invariants
are asserted (Search X + Exclude X == all files, Untagged + Any Tag == all
files, page length == min(count, page size), page order), never timings.

Opt-in: skipped unless FILE_LIST_QUERY_BENCH_ROWS is set. Run explicitly:

    FILE_LIST_QUERY_BENCH_ROWS=1000000 poetry run pytest \\
        tests/app/test_file_list_query_benchmark.py -m slow -s

On hybrid CPUs, pin to performance cores (e.g. ``taskset -c 0-5``) for stable numbers.

Environment:
    FILE_LIST_QUERY_BENCH_ROWS    file list rows (required; 1_000_000 standard)
    FILE_LIST_QUERY_BENCH_DIR     work folder; base cases in it are reused across runs
    FILE_LIST_QUERY_BENCH_OUTPUT  write JSON results to this path
    FILE_LIST_QUERY_BENCH_REPEAT  timed runs per measurement (default 5)
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
from typing import Any, Callable, Dict, List, Tuple

import pytest

from app.data import CaseDataAccess, FileListFilterSpec, discard_case_stats_cache
from tests.fixtures.file_list_fixtures import FileListFixtureCase, build_file_list_fixture_case

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        not os.environ.get("FILE_LIST_QUERY_BENCH_ROWS"),
        reason="set FILE_LIST_QUERY_BENCH_ROWS to run the benchmark",
    ),
]

ROWS = int(os.environ.get("FILE_LIST_QUERY_BENCH_ROWS") or 0)
REPEAT = int(os.environ.get("FILE_LIST_QUERY_BENCH_REPEAT") or 5)
PAGE_SIZE = 10_000  # FileListModel.PAGE_SIZE
CASES: Dict[str, Dict[str, int]] = {
    "sparse": dict(tag_every=100, match_every=20),
    "mostly_tagged": dict(untag_every=100),
}

# Fixture: 20% of paths contain WinSxS, 40% Windows, all ":\"; names are
# f{i:07d}.{ext} with ext cycling dll/exe/txt/jpg/docx/none (1/6 dll).
EXCLUDE_TERMS = {
    "exclude_rare": "f0000042",  # one file
    "exclude_winsxs": "WinSxS",
    "exclude_windows": "Windows",
    "exclude_everywhere": ":\\",  # every path: no rows, full scan
}
FILTERS: Dict[str, FileListFilterSpec] = {
    "baseline": FileListFilterSpec(),
    **{name: FileListFilterSpec(exclude=term) for name, term in EXCLUDE_TERMS.items()},
    "search_dll": FileListFilterSpec(search=".dll"),
    "search_dll_exclude_winsxs": FileListFilterSpec(search=".dll", exclude="WinSxS"),
    "search_rare": FileListFilterSpec(search="f0000042"),
    "untagged": FileListFilterSpec(untagged=True),
    "any_tag": FileListFilterSpec(any_tag=True),
    "untagged_exclude_winsxs": FileListFilterSpec(untagged=True, exclude="WinSxS"),
    "untagged_ext_dll": FileListFilterSpec(untagged=True, extension="dll"),
}
TAG_FILTERS = ("untagged", "any_tag", "untagged_exclude_winsxs", "untagged_ext_dll")
CASE_FILTERS = {"sparse": tuple(FILTERS), "mostly_tagged": TAG_FILTERS}


def _median_ms(fn: Callable[[], Any]) -> Tuple[float, Any]:
    timings: List[float] = []
    result = None
    for _ in range(REPEAT):
        start = time.perf_counter()
        result = fn()
        timings.append((time.perf_counter() - start) * 1000)
    return round(statistics.median(timings), 1), result


def _plan(conn: sqlite3.Connection, sql: str) -> str:
    return " | ".join(row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql))


def _capture_sql(conn: sqlite3.Connection, fn: Callable[[], Any]) -> str:
    """Run ``fn`` once (untimed, warms the cache) and return its File List SQL."""
    statements: List[str] = []

    def on_sql(sql: str) -> None:
        if "FROM file_list fl" in sql:
            statements.append(sql)

    conn.set_trace_callback(on_sql)
    try:
        fn()
    finally:
        conn.set_trace_callback(None)
    assert len(statements) == 1, statements
    return statements[0]


def _measure(data: CaseDataAccess, conn: sqlite3.Connection, eid: int, spec: FileListFilterSpec) -> Dict[str, Any]:
    def page() -> List[Dict[str, Any]]:
        return data.list_file_list_rows(eid, spec, offset=0, limit=PAGE_SIZE)

    def count() -> int:
        return data.count_file_list(eid, spec)

    page_sql = _capture_sql(conn, page)  # expanded SQL, literal parameters
    count_sql = _capture_sql(conn, count)
    page_ms, rows = _median_ms(page)
    count_ms, total = _median_ms(count)
    assert len(rows) == min(total, PAGE_SIZE)
    keys = [(row["file_path"], row["id"]) for row in rows]
    assert keys == sorted(keys), "first page not ordered by file_path, id"
    return {
        "count": total,
        "first_page_ms": page_ms,
        "count_ms": count_ms,
        "total_ms": round(page_ms + count_ms, 1),
        "page_plan": _plan(conn, page_sql),
        "count_plan": _plan(conn, count_sql),
    }


@pytest.fixture(scope="module")
def work_dir(tmp_path_factory) -> Path:
    folder_env = os.environ.get("FILE_LIST_QUERY_BENCH_DIR")
    return Path(folder_env) if folder_env else tmp_path_factory.mktemp("file_list_query_bench")


def _base_case(work_dir: Path, name: str, build_s: Dict[str, float]) -> FileListFixtureCase:
    folder = work_dir / f"{name}_{ROWS}"
    marker = folder / ".file_list_query_bench_ok"
    if not marker.exists():
        if folder.exists():
            shutil.rmtree(folder)
        start = time.perf_counter()
        case = build_file_list_fixture_case(folder, rows=ROWS, analyze=False, **CASES[name])
        build_s[name] = round(time.perf_counter() - start, 1)
        print(f"\n[bench] built {name} ({ROWS:,} rows) in {build_s[name]}s at {folder}")
        marker.write_text("ok")
        return case
    # The fixture always creates evidence 1 with this label in a fresh folder.
    return FileListFixtureCase(folder, folder / "filelist_surfsifter.sqlite", 1, "File List Evidence")


def _run_case(case: FileListFixtureCase, names: Tuple[str, ...], *, analyze: bool) -> Dict[str, Dict[str, Any]]:
    discard_case_stats_cache(case.case_db_path)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    try:
        conn = data.db_manager.get_evidence_conn(case.evidence_id, case.label)
        if analyze:
            conn.execute("ANALYZE")
        has_stats = conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'sqlite_stat1'").fetchone()
        assert bool(has_stats) == analyze
        results = {name: _measure(data, conn, case.evidence_id, FILTERS[name]) for name in names}
        # Exclude X is the exact complement of Search X (untimed counts).
        for name, term in EXCLUDE_TERMS.items():
            if name in results:
                searched = data.count_file_list(case.evidence_id, FileListFilterSpec(search=term))
                assert searched + results[name]["count"] == ROWS, name
    finally:
        data.close()
    return results


def test_file_list_query_benchmark(work_dir: Path):
    build_s: Dict[str, float] = {}
    cases = {name: _base_case(work_dir, name, build_s) for name in CASES}
    results: Dict[str, Any] = {"cases": {}}
    for name, case in cases.items():
        results["cases"][name] = _run_case(case, CASE_FILTERS[name], analyze=False)
        copy = work_dir / "variants" / f"{name}_stats"
        if copy.exists():
            shutil.rmtree(copy)
        shutil.copytree(case.case_folder, copy)
        stats_case = FileListFixtureCase(copy, copy / case.case_db_path.name, case.evidence_id, case.label)
        results["cases"][f"{name}_stats"] = _run_case(stats_case, CASE_FILTERS[name], analyze=True)
        shutil.rmtree(copy)
        for filter_name, m in results["cases"][f"{name}_stats"].items():
            assert m["count"] == results["cases"][name][filter_name]["count"], (name, filter_name)

    for name, measured in results["cases"].items():
        assert measured["untagged"]["count"] + measured["any_tag"]["count"] == ROWS, name
    sparse = results["cases"]["sparse"]
    assert sparse["baseline"]["count"] == ROWS
    assert sparse["any_tag"]["count"] == len(range(0, ROWS, 100))
    assert results["cases"]["mostly_tagged"]["untagged"]["count"] == len(range(0, ROWS, 100))
    assert sparse["exclude_winsxs"]["count"] == ROWS - len(range(0, ROWS, 5))
    assert sparse["exclude_everywhere"]["count"] == 0

    results["environment"] = {
        "rows": ROWS,
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
    output = os.environ.get("FILE_LIST_QUERY_BENCH_OUTPUT")
    if output:
        Path(output).write_text(json.dumps(results, indent=2))


def _print_report(results: Dict[str, Any]) -> None:
    print("\n[bench] environment:", json.dumps(results["environment"]))
    for case, measured in results["cases"].items():
        print(f"[bench] --- {case}")
        print(f"[bench] {'filter':26} {'rows':>9} {'page ms':>9} {'count ms':>9} {'total ms':>9}")
        for name, m in measured.items():
            print(f"[bench] {name:26} {m['count']:>9} {m['first_page_ms']:>9} {m['count_ms']:>9} {m['total_ms']:>9}")
        for name, m in measured.items():
            print(f"[bench]   plan {case}/{name}/page: {m['page_plan']}")
            print(f"[bench]   plan {case}/{name}/count: {m['count_plan']}")
