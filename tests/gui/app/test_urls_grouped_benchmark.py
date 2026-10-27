"""Opt-in grouped URL view benchmark (issue #64).

Builds one deterministic synthetic case per domain distribution with
``tests/fixtures/url_fixtures.py`` (90% NULL ``first_seen_utc``, sparse tags
and matches, 2% of URLs without a domain) and measures:

- data layer (median of ``REPEAT`` runs): first and deep (90%) page of domain
  groups, walking all group pages, first and deep (90%) child page of the
  largest group and of the no-domain group, and first group pages under
  selective filters;
- UI (``UrlsTab`` with grouping on, one run): time until the first group page
  is shown, a group's first child page after expanding it, the next child
  page, the next group page, each with the longest event-loop stall;
- memory (tracemalloc) of the tree model after the first group page and after
  loading 10 child pages;
- the pre-#64 eager build (first 10,000 URLs grouped into ``QStandardItem``s on
  the GUI thread) as a baseline.

Only correctness invariants are asserted, never timing thresholds.

Opt-in: skipped unless URL_GROUP_BENCH_ROWS is set. Run explicitly:

    URL_GROUP_BENCH_ROWS=1000000 poetry run pytest tests/gui/app/test_urls_grouped_benchmark.py -m slow -s

Environment:
    URL_GROUP_BENCH_ROWS    URL rows per case (required; 1_000_000 standard)
    URL_GROUP_BENCH_DIR     reuse/create the synthetic cases in this folder
    URL_GROUP_BENCH_OUTPUT  write JSON results to this path
    URL_GROUP_BENCH_REPEAT  timed runs per data-layer measurement (default 5)
"""
from __future__ import annotations

import gc
import json
import os
import platform
import sqlite3
import statistics
import time
import tracemalloc
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest
from PySide6.QtCore import QElapsedTimer, QTimer
from PySide6.QtWidgets import QApplication

from app.data import (
    CaseDataAccess,
    UrlCursor,
    UrlDomainChildQuery,
    UrlDomainGroupQuery,
    UrlFilterSpec,
)
from tests.fixtures.url_fixtures import UrlFixtureCase, build_url_fixture_case

pytestmark = [
    pytest.mark.slow,
    pytest.mark.gui_offscreen,
    pytest.mark.skipif(
        not os.environ.get("URL_GROUP_BENCH_ROWS"), reason="set URL_GROUP_BENCH_ROWS to run the benchmark"
    ),
]

ROWS = int(os.environ.get("URL_GROUP_BENCH_ROWS") or 0)
REPEAT = int(os.environ.get("URL_GROUP_BENCH_REPEAT") or 5)
DISTRIBUTIONS = {"few_domains": 5, "many_domains": 100_000}
NO_DOMAIN_EVERY = 50  # 2% of URLs without a domain (half NULL, half '')
FILTERS = {
    "matched": UrlFilterSpec(match_filter="matched"),  # 5%
    "tag": UrlFilterSpec(tag_name="relevant"),  # 1%
    "source": UrlFilterSpec(discovered_by=("bulk_extractor",)),  # 25%
    "text": UrlFilterSpec(url_contains="?q=96"),  # ~1%, scan (index not built)
}
GROUP_PAGE = 500
CHILD_PAGE = 500


def _median_ms(fn: Callable[[], Any]) -> Tuple[float, Any]:
    runs = []
    result = None
    for _ in range(REPEAT):
        start = time.perf_counter()
        result = fn()
        runs.append((time.perf_counter() - start) * 1000)
    return round(statistics.median(runs), 2), result


class _StallMonitor:
    """Tracks the longest gap between 1 ms timer ticks on the GUI thread."""

    def __init__(self) -> None:
        self._clock = QElapsedTimer()
        self._timer = QTimer()
        self._timer.setInterval(1)
        self._timer.timeout.connect(self._tick)
        self._last = 0
        self.max_gap_ms = 0

    def start(self) -> None:
        self._clock.start()
        self._last = 0
        self.max_gap_ms = 0
        self._timer.start()

    def stop(self) -> None:
        self._tick()
        self._timer.stop()

    def _tick(self) -> None:
        now = self._clock.elapsed()
        self.max_gap_ms = max(self.max_gap_ms, now - self._last)
        self._last = now


def _wait_until(predicate: Callable[[], bool], timeout_s: float) -> bool:
    app = QApplication.instance()
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        app.processEvents()
        if predicate():
            return True
        time.sleep(0.001)
    return False


def _measure_ui(action: Callable[[], None], done: Callable[[], bool], monitor: _StallMonitor) -> Dict[str, Any]:
    monitor.start()
    start = time.perf_counter()
    action()
    ok = _wait_until(done, 300)
    elapsed = (time.perf_counter() - start) * 1000
    monitor.stop()
    return {"completed": ok, "time_to_result_ms": round(elapsed, 1), "max_event_loop_stall_ms": monitor.max_gap_ms}


def _open_case(folder: Path) -> UrlFixtureCase:
    from core.database import DatabaseManager, find_case_database

    case_db = find_case_database(folder)
    manager = DatabaseManager(folder, case_db_path=case_db)
    rows = manager.get_case_conn().execute("SELECT id, label FROM evidences ORDER BY id").fetchall()
    manager.close_all()
    return UrlFixtureCase(folder, case_db, [r[0] for r in rows], {r[0]: r[1] for r in rows})


@pytest.fixture(scope="module")
def bench_cases(tmp_path_factory) -> Dict[str, UrlFixtureCase]:
    base_env = os.environ.get("URL_GROUP_BENCH_DIR")
    base = Path(base_env) if base_env else tmp_path_factory.mktemp("url_group_bench")
    cases = {}
    for name, domains in DISTRIBUTIONS.items():
        folder = base / f"{name}_{ROWS}"
        marker = folder / ".url_group_bench"
        if marker.exists():
            cases[name] = _open_case(folder)
            continue
        start = time.perf_counter()
        case = build_url_fixture_case(
            folder, rows_per_evidence=ROWS, timestamp_every=10, tag_every=100, match_every=20, domain_count=domains,
        )
        data = CaseDataAccess(case.case_folder, case.case_db_path)
        with data._use_evidence_conn(case.evidence_ids[0]):
            with data._connect() as conn:
                conn.execute(
                    f"UPDATE urls SET domain = CASE WHEN id % {2 * NO_DOMAIN_EVERY} = 0 THEN NULL ELSE '' END "
                    f"WHERE id % {NO_DOMAIN_EVERY} = 0"
                )
        data.close()
        marker.write_text("ok")
        print(f"\n[bench] generated {ROWS:,} rows over {domains:,} domains in {time.perf_counter() - start:.1f}s")
        cases[name] = case
    return cases


def _legacy_build(data: CaseDataAccess, evidence_id: int):
    """The pre-#64 grouped reload: first 10,000 URLs as eager QStandardItems."""
    from PySide6.QtCore import Qt
    from PySide6.QtGui import QStandardItem, QStandardItemModel

    model = QStandardItemModel()
    rows = data.iter_urls(evidence_id, filters=UrlFilterSpec(), limit=10_000, offset=0)
    url_ids = [row["id"] for row in rows if row.get("id")]
    tags = data.get_tag_strings_for_artifacts(evidence_id, "url", url_ids) if url_ids else {}
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row.get("domain") or "<none>", []).append(row)
    for domain, children in grouped.items():
        parent_item = QStandardItem(domain)
        parent_row = [parent_item] + [QStandardItem("") for _ in range(7)]
        model.appendRow(parent_row)
        for child in children:
            items = [QStandardItem(str(child.get(k, ""))) for k in (
                "url", "domain", "scheme", "discovered_by", "first_seen_utc", "last_seen_utc", "source_path",
            )] + [QStandardItem(tags.get(child.get("id"), ""))]
            for idx, item in enumerate(items):
                item.setEditable(idx == 7)
                item.setData(child.get("id"), Qt.UserRole)
                if idx == 0:
                    item.setData(child, Qt.UserRole + 1)
            parent_item.appendRow(items)
    return model, len(grouped)


def _traced_kib(fn: Callable[[], Any]) -> Tuple[float, Any]:
    gc.collect()
    tracemalloc.start()
    before = tracemalloc.take_snapshot()
    result = fn()
    gc.collect()
    after = tracemalloc.take_snapshot()
    tracemalloc.stop()
    size = sum(stat.size_diff for stat in after.compare_to(before, "filename"))
    return round(size / 1024, 1), result


def _deep_cursor(data: CaseDataAccess, evidence_id: int, spec: UrlFilterSpec, count: int) -> Optional[UrlCursor]:
    rows = data.iter_urls(evidence_id, filters=spec, limit=1, offset=int(count * 0.9))
    return UrlCursor.from_row(rows[0]) if rows else None


def _data_layer(data: CaseDataAccess, evidence_id: int) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    ms, first = _median_ms(lambda: data.load_url_domain_groups(UrlDomainGroupQuery(evidence_id, page_size=GROUP_PAGE)))
    results["groups_first_page_ms"] = ms
    results["groups_first_page_size"] = len(first.groups)

    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            group_total = conn.execute(
                "SELECT COUNT(*) FROM (SELECT 1 FROM urls WHERE evidence_id = ? AND domain > '' COLLATE NOCASE "
                "GROUP BY domain COLLATE NOCASE)", (evidence_id,),
            ).fetchone()[0]
            deep_after = conn.execute(
                "SELECT MIN(domain) FROM urls WHERE evidence_id = ? AND domain > '' COLLATE NOCASE "
                "GROUP BY domain COLLATE NOCASE ORDER BY domain COLLATE NOCASE LIMIT 1 OFFSET ?",
                (evidence_id, max(0, int(group_total * 0.9) - 1)),
            ).fetchone()[0]
    results["named_groups"] = group_total
    ms, _ = _median_ms(
        lambda: data.load_url_domain_groups(UrlDomainGroupQuery(evidence_id, after=deep_after, page_size=GROUP_PAGE))
    )
    results["groups_deep_page_ms"] = ms

    def walk() -> int:
        after, pages = None, 0
        while True:
            page = data.load_url_domain_groups(UrlDomainGroupQuery(evidence_id, after=after, page_size=GROUP_PAGE))
            pages += 1
            if not page.has_more:
                return pages
            after = page.next_after

    start = time.perf_counter()
    results["groups_walk_pages"] = walk()
    results["groups_walk_all_ms"] = round((time.perf_counter() - start) * 1000, 1)

    largest = max((g for g in first.groups if g.domain is not None), key=lambda g: g.count)
    no_domain = data.load_url_domain_groups(
        UrlDomainGroupQuery(evidence_id, after="\U0010ffff", page_size=GROUP_PAGE)
    ).groups
    for label, group_domain, count in (
        ("largest_group", largest.domain, largest.count),
        ("no_domain_group", None, no_domain[0].count if no_domain else 0),
    ):
        results[f"{label}_urls"] = count
        ms, page = _median_ms(
            lambda d=group_domain: data.load_url_domain_children(UrlDomainChildQuery(evidence_id, d, page_size=CHILD_PAGE))
        )
        assert len(page.rows) == min(CHILD_PAGE, count)
        results[f"{label}_first_child_page_ms"] = ms
        if group_domain is not None:
            cursor = _deep_cursor(data, evidence_id, UrlFilterSpec(domain_exact=group_domain), count)
        else:
            cursor = None
            # No flat filter selects the no-domain group: walk to 90% via pages (untimed).
            for _ in range(int(count * 0.9) // CHILD_PAGE):
                page = data.load_url_domain_children(UrlDomainChildQuery(evidence_id, None, after=cursor, page_size=CHILD_PAGE))
                cursor = page.next_cursor
        ms, _ = _median_ms(
            lambda d=group_domain, c=cursor: data.load_url_domain_children(
                UrlDomainChildQuery(evidence_id, d, after=c, page_size=CHILD_PAGE)
            )
        )
        results[f"{label}_deep_child_page_ms"] = ms

    for name, spec in FILTERS.items():
        ms, page = _median_ms(
            lambda s=spec: data.load_url_domain_groups(UrlDomainGroupQuery(evidence_id, s, page_size=GROUP_PAGE))
        )
        results[f"filter_{name}_groups_first_page_ms"] = ms
    return results


def _ui(case: UrlFixtureCase, data: CaseDataAccess) -> Dict[str, Any]:
    from app.features.urls import UrlsTab
    from app.features.urls.models.urls_grouped import LOADED

    evidence_id = case.evidence_ids[0]
    monitor = _StallMonitor()
    tab = UrlsTab()
    results: Dict[str, Any] = {}
    try:
        tab.set_case_data(data, defer_load=True)
        tab.set_evidence(evidence_id, defer_load=True, evidence_label=case.labels[evidence_id])
        tab.group_checkbox.setChecked(True)
        model = tab.group_model
        results["show_first_groups"] = _measure_ui(
            tab.show, lambda: model.state() == LOADED and model.group_count() > 0, monitor
        )
        root_status = model.index(model.rowCount() - 1, 0)
        largest = max(range(model.group_count()), key=model.group_url_count)
        results["expand_largest_group"] = _measure_ui(
            lambda: tab.tree_view.expand(model.index(largest, 0)),
            lambda: model.group_state(largest) == LOADED,
            monitor,
        )
        parent = model.index(largest, 0)
        if model.group_has_more(largest):
            status = model.index(model.rowCount(parent) - 1, 0, parent)
            results["load_more_urls"] = _measure_ui(
                lambda: tab.tree_view.activated.emit(status),
                lambda: model.group_state(largest) == LOADED and model.loaded_url_count(largest) > CHILD_PAGE,
                monitor,
            )
        if model.has_more_groups() and model.is_status_index(root_status):
            groups = model.group_count()
            root_status = model.index(model.rowCount() - 1, 0)
            results["load_more_domains"] = _measure_ui(
                lambda: tab.tree_view.activated.emit(root_status),
                lambda: model.state() == LOADED and model.group_count() > groups,
                monitor,
            )
    finally:
        tab.shutdown()
        tab.deleteLater()
    return results


def _memory(data: CaseDataAccess, evidence_id: int) -> Dict[str, Any]:
    from app.features.urls.models import UrlsGroupedModel

    model = UrlsGroupedModel(data)
    model.set_evidence(evidence_id, reload=False)
    query = model.begin_group_load()
    groups_page = data.load_url_domain_groups(query)
    groups_kib, _ = _traced_kib(lambda: model.apply_group_page(groups_page))
    row = max(range(model.group_count()), key=model.group_url_count)
    pages = 0
    child_kib = 0.0
    while pages < 10 and model.can_load_children(row):
        model.begin_child_load(row)
        page = data.load_url_domain_children(model.build_child_query(row))
        kib, _ = _traced_kib(lambda p=page: model.apply_child_page(p))
        child_kib += kib
        pages += 1
    rows = model.loaded_url_count(row)
    legacy_ms_start = time.perf_counter()
    legacy_kib, (legacy_model, legacy_groups) = _traced_kib(lambda: _legacy_build(data, evidence_id))
    legacy_ms = round((time.perf_counter() - legacy_ms_start) * 1000, 1)
    start = time.perf_counter()
    _legacy_build(data, evidence_id)
    legacy_untraced_ms = round((time.perf_counter() - start) * 1000, 1)
    return {
        "model_groups": model.group_count(),
        "model_groups_kib": groups_kib,
        "model_child_rows": rows,
        "model_child_rows_kib": round(child_kib, 1),
        "model_bytes_per_child_row": round(child_kib * 1024 / rows) if rows else None,
        "legacy_gui_thread_build_ms": legacy_untraced_ms,
        "legacy_gui_thread_build_traced_ms": legacy_ms,
        "legacy_python_kib": legacy_kib,
        "legacy_groups": legacy_groups,
        "legacy_reachable_urls": legacy_model.rowCount() and sum(
            legacy_model.item(r).rowCount() for r in range(legacy_model.rowCount())
        ),
    }


def test_grouped_view_benchmark(qapp, bench_cases):
    results: Dict[str, Any] = {
        "rows": ROWS,
        "repeat": REPEAT,
        "platform": platform.platform(),
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "distributions": {},
    }
    for name, case in bench_cases.items():
        data = CaseDataAccess(case.case_folder, case.case_db_path)
        try:
            evidence_id = case.evidence_ids[0]
            entry = {"domains": DISTRIBUTIONS[name]}
            entry["data_layer"] = _data_layer(data, evidence_id)
            entry["memory"] = _memory(data, evidence_id)
            entry["ui"] = _ui(case, data)
            results["distributions"][name] = entry
            assert all(m["completed"] for m in entry["ui"].values())
            expected_first = min(GROUP_PAGE, entry["data_layer"]["named_groups"])
            assert entry["data_layer"]["groups_first_page_size"] >= expected_first
        finally:
            data.close()

    text = json.dumps(results, indent=2, sort_keys=True)
    print("\n" + text)
    output = os.environ.get("URL_GROUP_BENCH_OUTPUT")
    if output:
        Path(output).write_text(text)
