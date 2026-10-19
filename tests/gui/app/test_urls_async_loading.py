"""GUI tests for asynchronous, cancellable URL loading (issue #60)."""
from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest
from PySide6.QtCore import QTimer

from app.data import CaseDataAccess, QueryCancelledError
from app.services.query_loader import CancellableQueryWorker, QueryRequest, ReadLimiter
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = pytest.mark.gui_offscreen


# ---------------------------------------------------------------------------
# CancellableQueryWorker with a controlled fake data access
# ---------------------------------------------------------------------------


class _FakeDataAccess:
    """Stand-in for CaseDataAccess that records its lifecycle threads."""

    def __init__(self, log: dict) -> None:
        self._log = log
        self._check = None

    @contextmanager
    def cancellable_reads(self, is_cancelled):
        self._check = is_cancelled
        try:
            yield
        finally:
            self._check = None

    def is_cancelled(self) -> bool:
        return bool(self._check and self._check())

    def close(self) -> None:
        self._log["closed_on"] = threading.get_ident()


def _slow_handler(log: dict, duration_s: float = 0.4):
    lock = threading.Lock()

    def handler(data: _FakeDataAccess, request: QueryRequest):
        with lock:
            log.setdefault("started", []).append(request.generation)
            log["running"] = log.get("running", 0) + 1
            log["max_running"] = max(log.get("max_running", 0), log["running"])
        try:
            deadline = time.perf_counter() + duration_s
            while time.perf_counter() < deadline:
                if data.is_cancelled():
                    log.setdefault("cancelled", []).append(request.generation)
                    raise QueryCancelledError("interrupted")
                time.sleep(0.005)
            return f"payload-{request.payload}"
        finally:
            with lock:
                log["running"] -= 1

    return handler


def _make_worker(log: dict, limiter=None, duration_s: float = 0.4) -> CancellableQueryWorker:
    return CancellableQueryWorker(
        Path("."), Path("."), _slow_handler(log, duration_s),
        limiter=limiter or ReadLimiter(4),
        data_access_factory=lambda: _FakeDataAccess(log),
    )


def test_latest_request_wins_and_obsolete_work_is_cancelled(qtbot):
    log: dict = {}
    worker = _make_worker(log)
    results = []
    worker.result_ready.connect(results.append)

    worker.submit(QueryRequest(1, 7, "a"))
    qtbot.waitUntil(lambda: log.get("started") == [1], timeout=2000)
    for generation in range(2, 6):
        worker.submit(QueryRequest(generation, 7, str(generation)))

    qtbot.waitUntil(lambda: len(results) == 1, timeout=5000)
    qtbot.wait(100)
    assert [r.generation for r in results] == [5]
    assert results[0].payload == "payload-5"
    # Running request was interrupted; queued intermediates never ran.
    assert log["cancelled"] == [1]
    assert log["started"] == [1, 5]
    assert worker.stop()
    assert log["closed_on"] != threading.get_ident()


def test_read_limiter_bounds_concurrent_reads_across_workers(qtbot):
    log: dict = {}
    limiter = ReadLimiter(1)
    workers = [_make_worker(log, limiter=limiter, duration_s=0.2) for _ in range(3)]
    results = []
    for index, worker in enumerate(workers):
        worker.result_ready.connect(results.append)
        worker.submit(QueryRequest(1, index, index))

    qtbot.waitUntil(lambda: len(results) == 3, timeout=5000)
    assert log["max_running"] == 1
    for worker in workers:
        assert worker.stop()


def test_stop_interrupts_running_request_without_late_results(qtbot):
    log: dict = {}
    worker = _make_worker(log, duration_s=30)
    results, errors = [], []
    worker.result_ready.connect(results.append)
    worker.failed.connect(lambda *args: errors.append(args))
    worker.submit(QueryRequest(1, 1, "x"))
    qtbot.waitUntil(lambda: log.get("started") == [1], timeout=2000)

    start = time.perf_counter()
    assert worker.stop(timeout_ms=2000)
    assert time.perf_counter() - start < 1.0
    qtbot.wait(100)
    assert results == [] and errors == []
    assert worker.isFinished()
    assert "closed_on" in log


def test_cancellation_interrupts_long_running_sqlite_query(qtbot, tmp_path):
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=10)
    evidence_id = case.evidence_ids[0]
    slow_sql = (
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 2000000000) "
        "SELECT COUNT(*) FROM c"
    )
    started = threading.Event()

    def handler(data: CaseDataAccess, request: QueryRequest):
        with data._use_evidence_conn(request.evidence_id):
            with data._connect() as conn:
                if request.payload == "slow":
                    started.set()
                    return conn.execute(slow_sql).fetchone()[0]
                return conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0]

    worker = CancellableQueryWorker(case.case_folder, case.case_db_path, handler, limiter=ReadLimiter(1))
    results = []
    worker.result_ready.connect(results.append)
    worker.submit(QueryRequest(1, evidence_id, "slow"))
    qtbot.waitUntil(started.is_set, timeout=5000)
    time.sleep(0.1)

    start = time.perf_counter()
    worker.submit(QueryRequest(2, evidence_id, "fast"))
    qtbot.waitUntil(lambda: len(results) == 1, timeout=5000)
    assert time.perf_counter() - start < 3.0
    assert results[0].generation == 2
    assert results[0].payload == 10
    assert worker.stop()


# ---------------------------------------------------------------------------
# UrlsTab end-to-end with a real synthetic case
# ---------------------------------------------------------------------------


@pytest.fixture
def url_case(tmp_path):
    case = build_url_fixture_case(
        tmp_path / "case", rows_per_evidence=250, evidence_count=2,
        tag_every=5, match_every=4, domain_count=5,
    )
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    yield case, data
    data.close()


@pytest.fixture
def gui_thread_connects(monkeypatch):
    """Counts database connections requested on the GUI thread."""
    from app.data._base import BaseDataAccess

    counter = {"count": 0}
    main = threading.main_thread()
    original = BaseDataAccess._connect

    def counting(self):
        if threading.current_thread() is main:
            counter["count"] += 1
        return original(self)

    monkeypatch.setattr(BaseDataAccess, "_connect", counting)
    return counter


@pytest.fixture
def make_tab(qtbot):
    """Create UrlsTabs that are always shut down (stops loader threads)."""
    from app.features.urls import UrlsTab

    created = []

    def _make(case, data, evidence_id, page_size=100):
        tab = UrlsTab()
        qtbot.addWidget(tab)
        created.append(tab)
        tab.model.page_size = page_size
        tab.set_case_data(data, defer_load=True)
        tab.set_evidence(evidence_id, defer_load=True, evidence_label=case.labels[evidence_id])
        return tab

    yield _make
    for tab in created:
        tab.shutdown()


def _loaded(tab) -> bool:
    return (
        not tab._load_pending
        and not tab._filters_loading
        and not tab._data_loading
        and tab.page_label.text().startswith(("Showing", "No URLs"))
    )


def _slow_page_loader(monkeypatch, delay_s: float):
    import app.features.urls.tab as tab_module

    original = tab_module._load_url_page

    def slow(case_data, request):
        deadline = time.perf_counter() + delay_s
        while time.perf_counter() < deadline:
            time.sleep(0.01)
        return original(case_data, request)

    monkeypatch.setattr(tab_module, "_load_url_page", slow)


def test_initial_filter_navigation_refresh_run_no_gui_thread_queries(qtbot, make_tab, url_case, gui_thread_connects):
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)
    assert tab.model.rowCount() == 100
    assert tab.page_label.text() == "Showing 1-100 of 250 URLs"

    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)
    assert tab.page_label.text() == "Showing 101-200 of 250 URLs"

    tab.prev_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 0, timeout=10000)

    tab.refresh()
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)

    tab.match_combo.setCurrentIndex(tab.match_combo.findData("matched"))
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.total_count() == 63, timeout=10000)

    # Render every Tags/Match cell: served from batch caches.
    for row in range(tab.model.rowCount()):
        tab.model.data(tab.model.index(row, 8))
        tab.model.data(tab.model.index(row, 9))

    assert gui_thread_connects["count"] == 0


def test_event_loop_keeps_running_and_stale_results_are_rejected(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    _slow_page_loader(monkeypatch, 0.4)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)

    applied = []
    original_apply = tab.model.set_loaded_page

    def spy(page):
        applied.append(page.query)
        original_apply(page)

    monkeypatch.setattr(tab.model, "set_loaded_page", spy)

    ticks = []
    timer = QTimer()
    timer.setInterval(10)
    timer.timeout.connect(lambda: ticks.append(time.perf_counter()))
    timer.start()

    # Next page, then immediately a filter change: only the filter result lands.
    tab.next_button.click()
    tab.match_combo.setCurrentIndex(tab.match_combo.findData("unmatched"))
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)
    qtbot.wait(600)
    timer.stop()

    assert [(q.page, q.match_filter) for q in applied] == [(0, "unmatched")]
    assert tab.model.total_count() == 250 - 63
    # GUI thread kept processing events while the worker was busy.
    assert len(ticks) >= 20
    gaps = [b - a for a, b in zip(ticks, ticks[1:])]
    assert max(gaps) < 0.3


def test_evidence_change_rejects_previous_evidence_results(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    ev1, ev2 = case.evidence_ids
    _slow_page_loader(monkeypatch, 0.3)
    tab = make_tab(case, data, ev1)
    applied = []
    original_apply = tab.model.set_loaded_page

    def spy(page):
        applied.append(page.query.evidence_id)
        original_apply(page)

    monkeypatch.setattr(tab.model, "set_loaded_page", spy)
    tab.show()
    qtbot.waitUntil(lambda: tab._data_loading, timeout=10000)

    tab.set_evidence(ev2, evidence_label=case.labels[ev2])
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)
    qtbot.wait(500)
    assert applied == [ev2]
    assert tab.model.evidence_id == ev2


def test_tagging_refresh_invalidates_cached_tags(qtbot, make_tab, url_case):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)

    row = next(r for r in range(tab.model.rowCount()) if tab.model.data(tab.model.index(r, 8)) == "")
    url_id = tab.model.get_row(row)["id"]
    data.tag_artifact(evidence_id, "Fresh", "url", url_id)

    tab.refresh()
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)
    row = next(r for r in range(tab.model.rowCount()) if tab.model.get_row(r)["id"] == url_id)
    assert "Fresh" in tab.model.data(tab.model.index(row, 8))


def test_refresh_marks_hidden_grouped_view_stale(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)

    grouped_reloads = []
    monkeypatch.setattr(tab.group_model, "reload", lambda: grouped_reloads.append(1))
    tab.refresh()
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)
    assert grouped_reloads == []
    assert tab._grouped_stale

    tab.group_checkbox.setChecked(True)
    assert grouped_reloads == [1]
    tab.refresh()
    assert grouped_reloads == [1, 1]
    assert tab._flat_stale


def test_shutdown_during_loading_has_no_late_updates(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    _slow_page_loader(monkeypatch, 0.5)
    closed_threads = []
    original_close = CaseDataAccess.close

    def recording_close(self):
        closed_threads.append(threading.get_ident())
        original_close(self)

    monkeypatch.setattr(CaseDataAccess, "close", recording_close)
    tab = make_tab(case, data, case.evidence_ids[0])
    applied = []
    monkeypatch.setattr(tab.model, "set_loaded_page", lambda page: applied.append(page))
    tab.show()
    qtbot.waitUntil(lambda: tab._url_loader is not None and tab._data_loading, timeout=10000)
    loader = tab._url_loader

    tab.shutdown()
    assert tab._url_loader is None
    assert loader.isFinished()
    qtbot.wait(800)
    assert applied == []
    # Worker-owned connections were closed on the worker thread.
    assert any(ident != threading.get_ident() for ident in closed_threads)


def test_set_case_data_after_evidence_still_loads(qtbot, make_tab, url_case):
    """set_case_data() resets the model's evidence; loading must still work."""
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)

    other = CaseDataAccess(case.case_folder, case.case_db_path)
    try:
        tab.set_case_data(other)
        assert tab.model.evidence_id is None
        qtbot.waitUntil(lambda: _loaded(tab) and tab.model.rowCount() == 100, timeout=10000)
        assert tab.model.evidence_id == case.evidence_ids[0]
    finally:
        tab.shutdown()
        other.close()
