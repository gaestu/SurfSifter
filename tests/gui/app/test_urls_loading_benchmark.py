"""Opt-in URL tab loading benchmark (issue #60).

Measures, per phase (initial load, Next, Previous, refresh, filter change):
- wall time until the page is shown (time to first result),
- the longest event-loop stall observed while waiting (UI responsiveness),
- database connections opened on the GUI thread,
- evidence connection setups (schema checks / migrations),
plus individual tag/match lookups triggered by displaying rows.

Opt-in: skipped unless URL_BENCH_ROWS is set. Run explicitly:

    URL_BENCH_ROWS=1000000 poetry run pytest tests/gui/app/test_urls_loading_benchmark.py -m "slow" -s

Environment:
    URL_BENCH_ROWS    rows per evidence (required; 1_000_000 standard, 10_000_000 opt-in)
    URL_BENCH_DIR     reuse/create the synthetic case in this folder
    URL_BENCH_OUTPUT  write JSON results to this path
"""
from __future__ import annotations

import json
import os
import platform
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, Dict

import pytest
from PySide6.QtCore import QElapsedTimer, QTimer
from PySide6.QtWidgets import QApplication

from tests.fixtures.url_fixtures import UrlFixtureCase, build_url_fixture_case

pytestmark = [
    pytest.mark.slow,
    pytest.mark.gui_offscreen,
    pytest.mark.skipif(not os.environ.get("URL_BENCH_ROWS"), reason="set URL_BENCH_ROWS to run the benchmark"),
]


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


class _Counters:
    def __init__(self) -> None:
        self.gui_connects = 0
        self.connection_setups = 0
        self.single_tag_lookups = 0
        self.single_match_lookups = 0
        self.query_ms: list = []


def _install_counters(monkeypatch) -> _Counters:
    from app.data._base import BaseDataAccess
    from app.data.case_data import CaseDataAccess
    import core.database.manager as manager_mod

    counters = _Counters()
    main_thread = threading.main_thread()

    original_connect = BaseDataAccess._connect

    def counting_connect(self):
        if threading.current_thread() is main_thread:
            counters.gui_connects += 1
        return original_connect(self)

    monkeypatch.setattr(BaseDataAccess, "_connect", counting_connect)

    original_migrate = manager_mod.migrate

    def counting_migrate(*args, **kwargs):
        counters.connection_setups += 1
        return original_migrate(*args, **kwargs)

    monkeypatch.setattr(manager_mod, "migrate", counting_migrate)

    original_tags = CaseDataAccess.get_artifact_tags_str
    original_matches = CaseDataAccess.get_url_matches

    def counting_tags(self, *args, **kwargs):
        counters.single_tag_lookups += 1
        return original_tags(self, *args, **kwargs)

    def counting_matches(self, *args, **kwargs):
        counters.single_match_lookups += 1
        return original_matches(self, *args, **kwargs)

    monkeypatch.setattr(CaseDataAccess, "get_artifact_tags_str", counting_tags)
    monkeypatch.setattr(CaseDataAccess, "get_url_matches", counting_matches)

    # Worker-side total query duration (only exists after issue #60).
    if hasattr(CaseDataAccess, "load_url_page"):
        original_load = CaseDataAccess.load_url_page

        def timed_load(self, *args, **kwargs):
            start = time.perf_counter()
            try:
                return original_load(self, *args, **kwargs)
            finally:
                counters.query_ms.append(round((time.perf_counter() - start) * 1000, 1))

        monkeypatch.setattr(CaseDataAccess, "load_url_page", timed_load)
    return counters


def _wait_until(predicate: Callable[[], bool], timeout_s: float) -> bool:
    app = QApplication.instance()
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        app.processEvents()
        if predicate():
            return True
        time.sleep(0.001)
    return False


def _measure(name: str, action: Callable[[], None], done: Callable[[], bool],
             counters: _Counters, monitor: _StallMonitor, timeout_s: float) -> Dict[str, object]:
    gui_before = counters.gui_connects
    setups_before = counters.connection_setups
    queries_before = len(counters.query_ms)
    monitor.start()
    start = time.perf_counter()
    action()
    ok = _wait_until(done, timeout_s)
    elapsed_ms = (time.perf_counter() - start) * 1000
    monitor.stop()
    return {
        "phase": name,
        "completed": ok,
        "time_to_result_ms": round(elapsed_ms, 1),
        "max_event_loop_stall_ms": monitor.max_gap_ms,
        "gui_thread_connections": counters.gui_connects - gui_before,
        "connection_setups": counters.connection_setups - setups_before,
        "worker_query_ms": counters.query_ms[queries_before:],
    }


@pytest.fixture(scope="module")
def bench_case(tmp_path_factory) -> UrlFixtureCase:
    rows = int(os.environ["URL_BENCH_ROWS"])
    folder_env = os.environ.get("URL_BENCH_DIR")
    folder = Path(folder_env) if folder_env else tmp_path_factory.mktemp("url_bench") / "case"
    marker = folder / f".url_bench_{rows}"
    if marker.exists():
        from core.database import find_case_database
        from core.database import DatabaseManager

        case_db = find_case_database(folder)
        manager = DatabaseManager(folder, case_db_path=case_db)
        conn = manager.get_case_conn()
        rows_db = conn.execute("SELECT id, label FROM evidences ORDER BY id").fetchall()
        manager.close_all()
        return UrlFixtureCase(
            case_folder=folder,
            case_db_path=case_db,
            evidence_ids=[r[0] for r in rows_db],
            labels={r[0]: r[1] for r in rows_db},
        )
    # Evidence 1: dense tags/matches; evidence 2: sparse tags/matches.
    start = time.perf_counter()
    case = build_url_fixture_case(folder, rows_per_evidence=rows, evidence_count=1,
                                  timestamp_every=10, tag_every=2, match_every=3)
    sparse = build_url_fixture_case_extra(case, rows)
    print(f"\n[bench] generated {rows:,} rows x {len(sparse.evidence_ids)} evidences "
          f"in {time.perf_counter() - start:.1f}s at {folder}")
    marker.write_text("ok")
    return sparse


def build_url_fixture_case_extra(case: UrlFixtureCase, rows: int) -> UrlFixtureCase:
    """Add a second, sparsely tagged/matched evidence to an existing case."""
    from core.database import DatabaseManager
    from tests.fixtures.url_fixtures import _populate_evidence

    manager = DatabaseManager(case.case_folder, case_db_path=case.case_db_path)
    try:
        conn = manager.get_case_conn()
        with conn:
            case_id = conn.execute("SELECT id FROM cases LIMIT 1").fetchone()[0]
            label = "Bench Evidence 2"
            evidence_id = conn.execute(
                "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                (case_id, label, "/bench/evidence2.E01", "2025-01-01T00:00:00Z"),
            ).lastrowid
        _populate_evidence(
            manager.get_evidence_conn(evidence_id, label), evidence_id, rows=rows,
            timestamp_every=10, tag_every=100, match_every=100, domain_count=1000, batch_size=50_000,
        )
    finally:
        manager.close_all()
    case.evidence_ids.append(evidence_id)
    case.labels[evidence_id] = label
    return case


def _loaded(tab) -> bool:
    text = tab.page_label.text()
    return text.startswith("Showing") or text == "No URLs"


def test_urls_loading_benchmark(qapp, bench_case: UrlFixtureCase, monkeypatch):
    from app.data.case_data import CaseDataAccess
    from app.features.urls import UrlsTab

    counters = _install_counters(monkeypatch)
    monitor = _StallMonitor()
    timeout_s = float(os.environ.get("URL_BENCH_TIMEOUT", "600"))
    case_data = CaseDataAccess(bench_case.case_folder, bench_case.case_db_path)
    results = []

    tabs = []
    for evidence_id in bench_case.evidence_ids:
        tab = UrlsTab()
        tab.set_case_data(case_data, defer_load=True)
        tab.set_evidence(evidence_id, defer_load=True, evidence_label=bench_case.labels[evidence_id])
        tab.resize(1200, 800)
        tabs.append((evidence_id, tab))

    for evidence_id, tab in tabs:
        label = f"ev{evidence_id}"
        results.append(_measure(f"{label}:initial", tab.show,
                                lambda: _loaded(tab) and tab.model.rowCount() > 0,
                                counters, monitor, timeout_s))
        results.append(_measure(f"{label}:next", tab.next_button.click,
                                lambda: tab.page_label.text().startswith("Showing 10001-"),
                                counters, monitor, timeout_s))
        results.append(_measure(f"{label}:previous", tab.prev_button.click,
                                lambda: tab.page_label.text().startswith("Showing 1-"),
                                counters, monitor, timeout_s))

        def do_refresh(t=tab):
            t.page_label.setText("__refreshing__")
            t.refresh()

        results.append(_measure(f"{label}:refresh", do_refresh,
                                lambda: _loaded(tab), counters, monitor, timeout_s))

        def do_filter(t=tab):
            t.page_label.setText("__filtering__")
            t.match_combo.setCurrentIndex(t.match_combo.findData("matched"))

        results.append(_measure(f"{label}:filter_matched", do_filter,
                                lambda: _loaded(tab), counters, monitor, timeout_s))

        # Display probe: render Tags/Match cells for the first 100 rows.
        tags_before = counters.single_tag_lookups
        matches_before = counters.single_match_lookups
        tab.match_combo.setCurrentIndex(tab.match_combo.findData("unmatched"))
        _wait_until(lambda: _loaded(tab), timeout_s)
        model = tab.model
        for row in range(min(100, model.rowCount())):
            model.data(model.index(row, 8))
            model.data(model.index(row, 9))
        results.append({
            "phase": f"{label}:display_100_rows",
            "single_tag_lookups": counters.single_tag_lookups - tags_before,
            "single_match_lookups": counters.single_match_lookups - matches_before,
        })

    # Multi-evidence: refresh both tabs at once.
    def refresh_all():
        for _, t in tabs:
            t.page_label.setText("__refreshing__")
            t.refresh()

    results.append(_measure("all:concurrent_refresh", refresh_all,
                            lambda: all(_loaded(t) for _, t in tabs), counters, monitor, timeout_s))

    for _, tab in tabs:
        tab.shutdown()
        tab.close()
        tab.deleteLater()
    case_data.close()
    qapp.processEvents()

    env = {
        "rows_per_evidence": int(os.environ.get("URL_BENCH_ROWS", "1000000")),
        "evidences": len(bench_case.evidence_ids),
        "python": platform.python_version(),
        "sqlite": sqlite3.sqlite_version,
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
    }
    print("\n[bench] environment:", json.dumps(env))
    for row in results:
        print("[bench]", json.dumps(row))
    output = os.environ.get("URL_BENCH_OUTPUT")
    if output:
        Path(output).write_text(json.dumps({"environment": env, "results": results}, indent=2))

    assert all(r.get("completed", True) for r in results)
