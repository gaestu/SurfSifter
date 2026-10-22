"""GUI tests for asynchronous, cancellable URL loading (issues #60, #61)."""
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
    # Build the URL search index up front: these tests are about loading and
    # counting, and a first-show build would commit while they run (its
    # interplay with paging is tested in test_urls_search_index_build.py).
    for evidence_id in case.evidence_ids:
        assert data.build_url_search_index(evidence_id).is_ready
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
        tab.set_page_size(page_size)
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
        and tab.page_label.text().startswith(("Showing", "No URLs", "Page "))
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
    # The exact total arrives separately (issue #61).
    qtbot.waitUntil(lambda: tab.page_label.text() == "Showing 1-100 of 250 URLs", timeout=10000)

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

    assert [(q.page, q.filters.match_filter) for q in applied] == [(0, "unmatched")]
    qtbot.waitUntil(lambda: tab.model.total_count() == 250 - 63, timeout=10000)
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
        # The model's evidence was reset, but the first page is requested
        # right away (it no longer waits for the dropdown lists, issue #63),
        # which restores it.
        assert tab._data_loading
        qtbot.waitUntil(lambda: _loaded(tab) and tab.model.rowCount() == 100, timeout=10000)
        assert tab.model.evidence_id == case.evidence_ids[0]
    finally:
        tab.shutdown()
        other.close()


# ---------------------------------------------------------------------------
# Issue #61: separate exact totals, cursor navigation, page size, domain modes
# ---------------------------------------------------------------------------


def _counting_count_handler(monkeypatch, delay_s: float = 0.0, fail: bool = False):
    """Wrap the tab's count handler; returns the list of counted filter specs."""
    import app.features.urls.tab as tab_module

    original = tab_module._count_urls
    calls = []

    def counting(case_data, request):
        calls.append(request.payload.filters)
        # Honour the worker's cancellation like an interrupted SQLite read,
        # so shutdown never waits for the artificial delay.
        is_cancelled = getattr(case_data._thread_local, "cancel_check", None) or (lambda: False)
        deadline = time.perf_counter() + delay_s
        while time.perf_counter() < deadline:
            if is_cancelled():
                raise QueryCancelledError("interrupted")
            time.sleep(0.01)
        if fail:
            raise RuntimeError("count failed")
        return original(case_data, request)

    monkeypatch.setattr(tab_module, "_count_urls", counting)
    return calls


def _total_shown(tab, total: int) -> bool:
    return _loaded(tab) and tab.page_label.text().endswith(f"of {total:,} URLs")


def test_first_rows_appear_while_total_is_pending(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    calls = _counting_count_handler(monkeypatch, delay_s=0.8)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()

    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)
    assert tab.model.rowCount() == 100
    assert tab.model.total_count() is None
    assert tab.page_label.text() == "Showing 1-100 of … URLs (counting)"
    assert tab.next_button.isEnabled()

    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    assert len(calls) == 1


def test_page_navigation_does_not_repeat_unchanged_count(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    calls = _counting_count_handler(monkeypatch)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)

    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)
    assert tab.page_label.text() == "Showing 101-200 of 250 URLs"
    tab.prev_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 0, timeout=10000)

    # Filter change counts again; returning to cached filters does not.
    tab.match_combo.setCurrentIndex(tab.match_combo.findData("matched"))
    qtbot.waitUntil(lambda: _total_shown(tab, 63), timeout=10000)
    tab.match_combo.setCurrentIndex(tab.match_combo.findData("all"))
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    assert [spec.match_filter for spec in calls] == [None, "matched"]


def test_walk_all_pages_forward_and_backward(qtbot, make_tab, url_case):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id, page_size=100)
    tab.show()
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)

    def page_ids():
        return [tab.model.get_row(r)["id"] for r in range(tab.model.rowCount())]

    forward = [page_ids()]
    while tab.next_button.isEnabled():
        page = tab.model.current_page()
        tab.next_button.click()
        qtbot.waitUntil(lambda p=page: _loaded(tab) and tab.model.current_page() == p + 1, timeout=10000)
        forward.append(page_ids())
    assert [len(p) for p in forward] == [100, 100, 50]
    assert tab.page_label.text() == "Showing 201-250 of 250 URLs"

    backward = [page_ids()]
    while tab.prev_button.isEnabled():
        page = tab.model.current_page()
        tab.prev_button.click()
        qtbot.waitUntil(lambda p=page: _loaded(tab) and tab.model.current_page() == p - 1, timeout=10000)
        backward.append(page_ids())
    assert backward == list(reversed(forward))

    walked = [url_id for page in forward for url_id in page]
    expected = [r["id"] for r in data.iter_urls(evidence_id, limit=1000)]
    assert walked == expected and len(set(walked)) == 250


def test_page_size_change_resets_to_first_page_and_is_reported(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    calls = _counting_count_handler(monkeypatch)
    tab = make_tab(case, data, case.evidence_ids[0])
    emitted = []
    tab.page_size_changed.connect(emitted.append)
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)

    tab.page_size_combo.setCurrentIndex(tab.page_size_combo.findData(500))
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.rowCount() == 250, timeout=10000)
    assert tab.model.current_page() == 0
    assert tab.page_label.text() == "Showing 1-250 of 250 URLs"
    assert not tab.next_button.isEnabled() and not tab.prev_button.isEnabled()
    assert emitted == [500]
    assert len(calls) == 1  # the total does not depend on the page size

    # Applying a stored setting does not re-emit; unknown values fall back.
    tab.set_page_size(12345)
    assert tab.model.page_size == tab.model.PAGE_SIZE and emitted == [500]


def test_tagging_keeps_page_but_data_refresh_resets(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    calls = _counting_count_handler(monkeypatch)
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)
    first_id = tab.model.get_row(0)["id"]

    data.tag_artifact(evidence_id, "Fresh", "url", first_id)
    tab._on_tags_changed()  # TagArtifactsDialog.tags_changed
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    assert tab.model.current_page() == 1
    assert tab.model.get_row(0)["id"] == first_id
    assert "Fresh" in tab.model.data(tab.model.index(0, 8))
    assert tab.page_label.text() == "Showing 101-200 of 250 URLs"
    assert len(calls) == 2  # tag totals may have changed

    tab.refresh()  # ingestion / deduplication / matching
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    assert tab.model.current_page() == 0
    assert len(calls) == 3


def test_count_failure_is_shown_without_hiding_rows(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    _counting_count_handler(monkeypatch, fail=True)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(
        lambda: _loaded(tab) and tab.page_label.text() == "Showing 1-100 URLs (total unavailable)",
        timeout=10000,
    )
    assert tab.model.rowCount() == 100 and tab.next_button.isEnabled()


def test_single_page_result_needs_no_count(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    calls = _counting_count_handler(monkeypatch, delay_s=5.0)
    tab = make_tab(case, data, case.evidence_ids[0], page_size=500)
    tab.show()
    # The complete result fits on page 1: exact total without waiting for COUNT.
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=3000)
    assert tab._count_in_flight_key is None
    assert len(calls) <= 1


def test_domain_dropdown_is_exact_and_contains_field_is_substring(qtbot, make_tab, url_case):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _loaded(tab), timeout=10000)

    index = tab.domain_combo.findData("d1.example.com")
    assert index > 0
    tab.domain_combo.setCurrentIndex(index)
    qtbot.waitUntil(lambda: _total_shown(tab, 50), timeout=10000)
    assert {tab.model.get_row(r)["domain"] for r in range(tab.model.rowCount())} == {"d1.example.com"}
    assert tab.group_model._spec.domain_exact == "d1.example.com"

    tab.domain_combo.setCurrentIndex(0)  # All domains
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.domain_contains_edit.setText("D2.EXAMPLE")
    tab.search_button.click()
    qtbot.waitUntil(lambda: _total_shown(tab, 50), timeout=10000)
    assert tab.model.filter_spec().domain_contains == "D2.EXAMPLE"
    assert tab.model.filter_spec().domain_exact is None


def test_unnotified_tag_change_invalidates_cached_total(qtbot, make_tab, url_case, monkeypatch):
    """E.g. a tag merged in the Tags tab: no refresh(), but totals must not go stale."""
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    calls = _counting_count_handler(monkeypatch)
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)

    tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("Relevant"))
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.total_count() is not None, timeout=10000)
    before = tab.model.total_count()
    untagged = [r["id"] for r in data.iter_urls(evidence_id, limit=1000) if r["id"] % 5 == 2][:10]
    for url_id in untagged:  # written through another connection, no notification
        data.tag_artifact(evidence_id, "Relevant", "url", url_id)

    tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("*"))
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("Relevant"))
    qtbot.waitUntil(lambda: _total_shown(tab, before + 10), timeout=10000)
    assert sum(1 for spec in calls if spec.tag_name == "Relevant") == 2


def _url_tags(data, evidence_id, url_id):
    return sorted(t["name"] for t in data.get_artifact_tags(evidence_id, "url", url_id))


def test_occurrences_column_is_read_only_and_tags_column_is_editable(url_case):
    """Issue #66: editing Occurrences used to overwrite the URL's tags."""
    from PySide6.QtCore import Qt

    from app.features.urls.models import UrlsTableModel

    case, data = url_case
    evidence_id = case.evidence_ids[0]
    model = UrlsTableModel(data, page_size=100)
    model.set_evidence(evidence_id)
    edited = []
    model.tags_edited.connect(lambda: edited.append(True))
    row = next(r for r in range(model.rowCount()) if model.data(model.index(r, 8)))
    url_id = model.get_row(row)["id"]
    before = _url_tags(data, evidence_id, url_id)
    assert before

    occurrences = model.index(row, 7)
    assert model.headers[7] == "Occurrences"
    assert not model.flags(occurrences) & Qt.ItemIsEditable
    assert not model.setData(occurrences, "3")
    assert _url_tags(data, evidence_id, url_id) == before
    assert not edited

    tags = model.index(row, 8)
    assert model.headers[8] == "Tags"
    assert model.flags(tags) & Qt.ItemIsEditable
    assert not model.setData(tags, model.data(tags, Qt.EditRole))  # unchanged
    assert not edited
    assert model.setData(tags, f"{before[0]}, Inline")
    assert _url_tags(data, evidence_id, url_id) == sorted([before[0], "Inline"])
    assert model.data(tags) == f"{before[0]}, Inline"
    assert edited == [True]


def test_inline_tag_edit_recounts_tag_filtered_total(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    calls = _counting_count_handler(monkeypatch)
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)

    # Cache the tag-filtered total, then return to the unfiltered list.
    tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("Relevant"))
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.total_count() is not None, timeout=10000)
    before = tab.model.total_count()
    tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("*"))
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)
    counted = len(calls)

    row = next(r for r in range(tab.model.rowCount()) if "Relevant" not in tab.model.data(tab.model.index(r, 8)))
    url_id = tab.model.get_row(row)["id"]
    current = tab.model.data(tab.model.index(row, 8))
    assert tab.model.setData(tab.model.index(row, 8), f"{current}, Relevant" if current else "Relevant")
    assert "Relevant" in _url_tags(data, evidence_id, url_id)
    qtbot.waitUntil(lambda: _total_shown(tab, 250) and len(calls) == counted + 1, timeout=10000)
    assert tab.model.current_page() == 1  # edited on the visible page: position kept
    assert tab.model.get_row(row)["id"] == url_id

    # The cached tag-filtered total was invalidated by the inline edit.
    tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("Relevant"))
    qtbot.waitUntil(lambda: _total_shown(tab, before + 1), timeout=10000)


def test_page_arriving_during_inline_edit_waits_for_editor(qtbot, make_tab, url_case):
    """A reload (e.g. after a previous inline edit) must not discard an open editor."""
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QAbstractItemView, QLineEdit

    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)

    row = next(r for r in range(tab.model.rowCount()) if not tab.model.data(tab.model.index(r, 8)))
    url_id = tab.model.get_row(row)["id"]
    index = tab.model.index(row, 8)
    tab.table_view.edit(index)
    assert tab.table_view.state() == QAbstractItemView.EditingState
    editor = tab.table_view.findChild(QLineEdit)

    tab._on_tags_changed()  # reload triggered while the editor is open
    qtbot.waitUntil(lambda: tab._pending_page_result is not None, timeout=10000)
    assert tab.table_view.state() == QAbstractItemView.EditingState

    editor.setText("Typed")
    qtbot.keyClick(editor, Qt.Key_Return)  # the delegate commits via a queued call
    qtbot.waitUntil(lambda: _url_tags(data, evidence_id, url_id) == ["Typed"], timeout=5000)
    qtbot.waitUntil(lambda: _total_shown(tab, 250) and tab._pending_page_result is None, timeout=10000)
    assert tab.model.get_row(row)["id"] == url_id
    assert tab.model.data(tab.model.index(row, 8)) == "Typed"


def test_grouped_view_shows_and_edits_existing_tags(qtbot, make_tab, url_case):
    from PySide6.QtCore import Qt

    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.group_checkbox.setChecked(True)

    model = tab.group_model
    parents = [model.item(parent, column) for parent in range(model.rowCount()) for column in range(8)]
    assert parents and not any(item.isEditable() for item in parents)
    children = [
        model.item(parent).child(row, 7)
        for parent in range(model.rowCount())
        for row in range(model.item(parent).rowCount())
    ]
    assert len(children) == 250
    tagged = [item for item in children if item.text()]
    assert tagged and all(item.isEditable() for item in tagged)
    item = tagged[0]
    url_id = item.data(Qt.UserRole)
    before = _url_tags(data, evidence_id, url_id)
    assert item.text() == ", ".join(before)

    revision = tab.model._data_revision
    item.setText(f"{item.text()}, Grouped")
    assert _url_tags(data, evidence_id, url_id) == sorted(before + ["Grouped"])
    assert tab.model._data_revision == revision + 1  # flat totals invalidated


def test_grouped_view_tagging_returns_flat_view_to_first_page(qtbot, make_tab, url_case):
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)

    tab.group_checkbox.setChecked(True)
    tab._on_tags_changed()  # TagArtifactsDialog.tags_changed from the grouped view
    tab.group_checkbox.setChecked(False)
    qtbot.waitUntil(lambda: _loaded(tab) and tab._flat_stale is False, timeout=10000)
    assert tab.model.current_page() == 0


@pytest.mark.parametrize("change", ["page_size", "filter", "refresh"])
def test_flat_load_in_flight_while_grouped_is_not_applied_after_changes(qtbot, make_tab, url_case, monkeypatch, change):
    """A hidden flat load must not corrupt the cursor stack after filter/size/data changes."""
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)

    _slow_page_loader(monkeypatch, 0.5)
    tab.next_button.click()  # page 3 in flight
    tab.group_checkbox.setChecked(True)
    if change == "page_size":
        tab.page_size_combo.setCurrentIndex(tab.page_size_combo.findData(500))
    elif change == "filter":
        tab.match_combo.setCurrentIndex(tab.match_combo.findData("matched"))
    else:
        tab.refresh()
    qtbot.wait(900)  # the obsolete load would have finished by now
    assert tab.model.current_page() == 0

    tab.group_checkbox.setChecked(False)
    expected = {"page_size": 250, "filter": 63, "refresh": 250}[change]
    qtbot.waitUntil(lambda: _total_shown(tab, expected), timeout=10000)
    assert tab.model.current_page() == 0
    assert tab.page_label.text().startswith("Showing 1-")


def test_model_rejects_page_built_for_other_state(url_case):
    from app.features.urls.models import UrlsTableModel

    case, data = url_case
    model = UrlsTableModel(data, page_size=100)
    model.set_evidence(case.evidence_ids[0], reload=False)
    query = model.build_query(0)
    assert model.is_current_query(query)
    model.set_filters(match_filter="matched", reload=False)
    assert not model.is_current_query(query)
    query = model.build_query(0)
    model.mark_data_changed(keep_position=True)
    assert not model.is_current_query(query)
    assert model.is_current_query(model.build_query(0))


def test_first_count_waits_for_data_version_baseline(qtbot, make_tab, url_case, monkeypatch):
    """Without a baseline, a count racing an unnotified write could stay stale for good."""
    case, data = url_case
    _slow_page_loader(monkeypatch, 0.4)
    calls = _counting_count_handler(monkeypatch)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: tab._data_loading, timeout=10000)
    assert not tab.model.has_data_baseline()
    assert tab._count_in_flight_key is None and calls == []

    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    assert len(calls) == 1 and tab.model.has_data_baseline()

    # With a baseline, the count runs alongside the page again.
    tab.match_combo.setCurrentIndex(tab.match_combo.findData("matched"))
    qtbot.waitUntil(lambda: tab._data_loading, timeout=10000)
    assert tab._count_in_flight_key is not None or len(calls) == 2


def test_unnotified_change_hides_row_numbers_until_first_page(qtbot, make_tab, url_case):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)

    data.tag_artifact(evidence_id, "Elsewhere", "url", tab.model.get_row(0)["id"])  # other connection
    tab.next_button.click()
    qtbot.waitUntil(
        lambda: _loaded(tab) and tab.page_label.text() == "Page 3: 50 of 250 URLs; data changed, row numbers hidden",
        timeout=10000,
    )
    assert not tab.model.row_numbers_exact()

    tab.prev_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)
    assert tab.page_label.text().startswith("Page 2: 100 of ")
    tab.prev_button.click()
    qtbot.waitUntil(lambda: _total_shown(tab, 250) and tab.model.current_page() == 0, timeout=10000)
    assert tab.page_label.text() == "Showing 1-100 of 250 URLs"


def _counting_page_loader(monkeypatch):
    """Wrap the tab's page handler; returns the list of requested page indexes."""
    import app.features.urls.tab as tab_module

    original = tab_module._load_url_page
    calls = []

    def counting(case_data, request):
        calls.append(request.payload.page)
        return original(case_data, request)

    monkeypatch.setattr(tab_module, "_load_url_page", counting)
    return calls


def test_returning_to_tab_after_tag_merge_elsewhere_shows_recounted_total(qtbot, make_tab, url_case):
    """Issue #67: a merge in the Tags tab (other connection, no notification)
    is picked up when the URLs tab is shown again, without navigating."""
    from app.data import UrlFilterSpec

    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("Relevant"))
    relevant = data.count_urls(evidence_id, filters=UrlFilterSpec(tag_name="Relevant"))
    qtbot.waitUntil(lambda: _total_shown(tab, relevant), timeout=10000)

    tab.hide()
    tags = {t["name"]: t["id"] for t in data.list_tags(evidence_id, use_cache=False)}
    data.merge_tags(evidence_id, [tags["Review"]], tags["Relevant"])
    merged = data.count_urls(evidence_id, filters=UrlFilterSpec(tag_name="Relevant"))
    assert merged > relevant

    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, merged), timeout=10000)
    assert tab.page_label.text() == f"Showing 1-{merged} of {merged:,} URLs"
    assert tab.model.current_page() == 0
    assert tab.model.rowCount() == merged


def test_returning_to_tab_on_later_page_recounts_total(qtbot, make_tab, url_case, monkeypatch):
    """Issue #67: the re-check on show also recounts a multi-page total."""
    from app.data import UrlFilterSpec

    case, data = url_case
    evidence_id = case.evidence_ids[0]
    counts = _counting_count_handler(monkeypatch)
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.match_combo.setCurrentIndex(tab.match_combo.findData("unmatched"))
    unmatched = data.count_urls(evidence_id, filters=UrlFilterSpec(match_filter="unmatched"))
    assert unmatched > 100
    qtbot.waitUntil(lambda: _total_shown(tab, unmatched), timeout=10000)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)
    counted = len(counts)

    tab.hide()
    # Written through another connection without notifying the tab.
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            conn.execute("DELETE FROM url_matches WHERE evidence_id = ? AND list_name = 'list0'", (evidence_id,))
            conn.commit()
    recounted = data.count_urls(evidence_id, filters=UrlFilterSpec(match_filter="unmatched"))
    assert recounted > unmatched

    tab.show()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.page_label.text().endswith(f"of {recounted:,} URLs; data changed, row numbers hidden"), timeout=10000)
    assert tab.model.current_page() == 1
    assert len(counts) == counted + 1


def test_unchanged_recheck_on_show_keeps_selection_without_loading_state(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    counts = _counting_count_handler(monkeypatch)
    pages = _counting_page_loader(monkeypatch)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)
    tab.table_view.selectRow(3)
    label = tab.page_label.text()
    loads, counted = len(pages), len(counts)
    loading_states = []
    monkeypatch.setattr(tab, "_set_table_loading", lambda: loading_states.append(tab.page_label.text()))
    resets = []
    tab.model.modelReset.connect(lambda: resets.append(True))

    tab.hide()
    tab.show()
    qtbot.waitUntil(lambda: len(pages) == loads + 1 and not tab._data_loading, timeout=10000)

    assert pages[-1] == 1
    assert loading_states == [] and resets == []
    assert tab.page_label.text() == label
    assert [i.row() for i in tab.table_view.selectionModel().selectedRows()] == [3]
    assert len(counts) == counted


def test_no_recheck_on_show_in_grouped_view(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    pages = _counting_page_loader(monkeypatch)
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.group_checkbox.setChecked(True)
    loads = len(pages)

    tab.hide()
    tab.show()
    qtbot.wait(100)
    assert len(pages) == loads


def test_recheck_on_show_for_tab_loaded_without_deferral(qtbot, url_case):
    """Tabs for evidence added during the session load immediately (defer_load=False)."""
    from app.data import UrlFilterSpec
    from app.features.urls import UrlsTab

    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = UrlsTab()
    qtbot.addWidget(tab)
    try:
        tab.set_page_size(100)
        tab.set_case_data(data)
        tab.set_evidence(evidence_id, evidence_label=case.labels[evidence_id])
        tab.show()
        qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
        tab.tag_combo.setCurrentIndex(tab.tag_combo.findData("Relevant"))
        relevant = data.count_urls(evidence_id, filters=UrlFilterSpec(tag_name="Relevant"))
        qtbot.waitUntil(lambda: _total_shown(tab, relevant), timeout=10000)

        tab.hide()
        tags = {t["name"]: t["id"] for t in data.list_tags(evidence_id, use_cache=False)}
        data.merge_tags(evidence_id, [tags["Review"]], tags["Relevant"])
        merged = data.count_urls(evidence_id, filters=UrlFilterSpec(tag_name="Relevant"))

        tab.show()
        qtbot.waitUntil(lambda: _total_shown(tab, merged), timeout=10000)
    finally:
        tab.shutdown()


def test_failed_recheck_on_show_keeps_shown_page(qtbot, make_tab, url_case, monkeypatch):
    import app.features.urls.tab as tab_module

    original = tab_module._load_url_page
    attempts = []
    fail = threading.Event()

    def failing(case_data, request):
        if not fail.is_set():
            return original(case_data, request)
        attempts.append(request.payload.page)
        raise RuntimeError("database is locked")

    monkeypatch.setattr(tab_module, "_load_url_page", failing)
    case, data = url_case
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()
    qtbot.waitUntil(lambda: _total_shown(tab, 250), timeout=10000)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)
    label = tab.page_label.text()
    first_id = tab.model.get_row(0)["id"]

    fail.set()
    tab.hide()
    tab.show()
    qtbot.waitUntil(lambda: attempts == [1] and not tab._data_loading, timeout=10000)

    assert tab.model.current_page() == 1
    assert tab.model.rowCount() == 100 and tab.model.get_row(0)["id"] == first_id
    assert tab.page_label.text() == label
