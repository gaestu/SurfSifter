"""GUI tests for the background URL search index build (issue #62)."""
from __future__ import annotations

import json
import threading

import pytest

import app.data._urls as urls_module
from app.data import CaseDataAccess, UrlSearchIndexStatus
from app.services.url_search_index_worker import UrlSearchIndexWorker
from core.database.helpers.url_search_index import url_search_index_supported
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = [
    pytest.mark.gui_offscreen,
    pytest.mark.skipif(not url_search_index_supported(), reason="SQLite lacks FTS5 with the trigram tokenizer"),
]


@pytest.fixture
def url_case(tmp_path):
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=120, evidence_count=2, domain_count=5)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    yield case, data
    data.close()


@pytest.fixture
def make_tab(qtbot):
    from app.features.urls import UrlsTab

    created = []

    def _make(case, data, evidence_id):
        tab = UrlsTab()
        qtbot.addWidget(tab)
        created.append(tab)
        tab.set_case_data(data, defer_load=True)
        tab.set_evidence(evidence_id, defer_load=True, evidence_label=case.labels[evidence_id])
        return tab

    yield _make
    for tab in created:
        tab.shutdown()
        # A build started late (see test_no_index_build_starts_after_shutdown)
        # must end before the widget (which owns the worker) is destroyed.
        qtbot.waitUntil(lambda: tab._index_worker is None or tab._index_worker.isFinished(), timeout=10000)


@pytest.fixture
def gated_build(monkeypatch):
    """Build steps of 20 rows that wait for ``gate`` (set = run freely)."""
    gate = threading.Event()
    entered = threading.Event()
    original = urls_module.build_url_search_index_step

    def step(conn, batch_size):
        entered.set()
        assert gate.wait(10), "test gate never opened"
        return original(conn, 20)

    monkeypatch.setattr(urls_module, "build_url_search_index_step", step)
    return gate, entered


@pytest.fixture
def ended_runs(monkeypatch):
    """Evidence ids of worker runs that returned (recorded on the worker thread)."""
    ended = []
    original = UrlSearchIndexWorker.run

    def run(self):
        try:
            original(self)
        finally:
            ended.append(self.evidence_id)

    monkeypatch.setattr(UrlSearchIndexWorker, "run", run)
    return ended


def _build_logs(data, evidence_id):
    with data._use_evidence_conn(evidence_id):
        with data._connect() as conn:
            return [
                json.loads(row[0]) if row[0] else None
                for row in conn.execute(
                    "SELECT stdout FROM process_log WHERE task = 'url_search_index_build' ORDER BY id"
                )
            ]


# ---------------------------------------------------------------------------
# UrlSearchIndexWorker
# ---------------------------------------------------------------------------


def test_worker_builds_index_and_reports_ready(qtbot, url_case):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    assert data.get_url_search_index_status(evidence_id).state == UrlSearchIndexStatus.BUILDING
    worker = UrlSearchIndexWorker(case.case_folder, case.case_db_path, evidence_id)
    failures = []
    worker.failed.connect(lambda *args: failures.append(args))
    with qtbot.waitSignal(worker.completed, timeout=10000) as blocker:
        worker.start()
    assert blocker.args[0] == evidence_id
    assert blocker.args[1].is_ready
    assert worker.wait(5000)
    assert failures == []
    assert data.get_url_search_index_status(evidence_id).is_ready
    assert data.get_url_search_index_status(case.evidence_ids[1]).state == UrlSearchIndexStatus.BUILDING
    assert [log["result"] for log in _build_logs(data, evidence_id)] == ["ready"]


def test_worker_cancelled_before_start_does_nothing(qtbot, url_case):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    worker = UrlSearchIndexWorker(case.case_folder, case.case_db_path, evidence_id)
    signals = []
    worker.completed.connect(lambda *args: signals.append(("completed", args)))
    worker.failed.connect(lambda *args: signals.append(("failed", args)))
    worker.cancel()
    assert worker.is_cancelled()
    with qtbot.waitSignal(worker.finished, timeout=10000):
        worker.start()
    qtbot.wait(50)
    assert signals == []
    status = data.get_url_search_index_status(evidence_id)
    assert status.state == UrlSearchIndexStatus.BUILDING and status.indexed_through_id == 0
    assert _build_logs(data, evidence_id) == []
    assert worker.stop()


def test_worker_reports_failure(qtbot, url_case, monkeypatch):
    case, data = url_case

    def boom(conn, batch_size):
        raise RuntimeError("simulated build failure")

    monkeypatch.setattr(urls_module, "build_url_search_index_step", boom)
    worker = UrlSearchIndexWorker(case.case_folder, case.case_db_path, case.evidence_ids[0])
    with qtbot.waitSignal(worker.failed, timeout=10000) as blocker:
        worker.start()
    assert blocker.args == [case.evidence_ids[0], "simulated build failure"]
    assert worker.wait(5000)


def test_worker_stop_mid_build_keeps_progress_and_resumes(qtbot, url_case, gated_build):
    case, data = url_case
    gate, entered = gated_build
    evidence_id = case.evidence_ids[0]
    worker = UrlSearchIndexWorker(case.case_folder, case.case_db_path, evidence_id)
    completed = []
    worker.completed.connect(lambda *args: completed.append(args))
    worker.start()
    assert entered.wait(10)
    worker.cancel()
    gate.set()
    assert worker.stop(5000)
    qtbot.wait(50)
    assert completed == []
    status = data.get_url_search_index_status(evidence_id)
    assert status.state == UrlSearchIndexStatus.BUILDING and status.indexed_through_id > 0
    assert [log["result"] for log in _build_logs(data, evidence_id)] == ["cancelled"]

    resumed = UrlSearchIndexWorker(case.case_folder, case.case_db_path, evidence_id)
    with qtbot.waitSignal(resumed.completed, timeout=10000) as blocker:
        resumed.start()
    assert blocker.args[1].is_ready
    assert resumed.wait(5000)


# ---------------------------------------------------------------------------
# UrlsTab
# ---------------------------------------------------------------------------


def test_showing_tab_builds_index_and_hides_label(qtbot, make_tab, url_case, ended_runs):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    assert tab._index_worker is None
    tab.show()
    qtbot.waitUntil(lambda: evidence_id in ended_runs, timeout=10000)
    qtbot.waitUntil(lambda: tab._index_worker is None, timeout=10000)
    assert data.get_url_search_index_status(evidence_id).is_ready
    assert tab.search_index_label.isHidden()

    # Showing again with a ready index: the worker ends after one status check.
    runs = len(ended_runs)
    tab.hide()
    tab.show()
    qtbot.waitUntil(lambda: len(ended_runs) > runs, timeout=10000)
    qtbot.waitUntil(lambda: tab._index_worker is None, timeout=10000)
    assert len(_build_logs(data, evidence_id)) == 1
    assert tab.search_index_label.isHidden()


def test_label_reflects_index_state(qtbot, make_tab, url_case, gated_build):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: tab._index_worker is not None, timeout=10000)
    S = UrlSearchIndexStatus
    label = tab.search_index_label

    tab._on_search_index_progress(evidence_id, S(S.BUILDING, 50, 1, 100))
    assert label.isVisible() and label.text() == "Search index: building 50%"
    tab._on_search_index_progress(evidence_id, S(S.UNSUPPORTED))
    assert label.isVisible() and label.text() == "Search index: unavailable"
    tab._on_search_index_progress(evidence_id, S(S.UNAVAILABLE))
    assert label.isVisible() and label.text() == "Search index: unavailable"
    tab._on_search_index_failed(evidence_id, "disk full")
    assert label.isVisible() and label.text() == "Search index: build failed"
    assert "disk full" in label.toolTip()
    # Other evidences' signals are ignored.
    tab._on_search_index_progress(evidence_id + 1, S(S.READY))
    assert label.isVisible()
    tab._on_search_index_progress(evidence_id, S(S.READY))
    assert label.isHidden()

    _gate, _entered = gated_build
    _gate.set()
    qtbot.waitUntil(lambda: data.get_url_search_index_status(evidence_id).is_ready, timeout=10000)
    qtbot.waitUntil(lambda: tab._index_worker is None, timeout=10000)
    assert label.isHidden()


def test_evidence_switch_stops_build_without_late_updates(qtbot, make_tab, url_case, gated_build, ended_runs):
    case, data = url_case
    ev1, ev2 = case.evidence_ids
    gate, entered = gated_build
    tab = make_tab(case, data, ev1)
    tab.show()
    qtbot.waitUntil(lambda: tab._index_worker is not None, timeout=10000)
    assert entered.wait(10)
    first = tab._index_worker
    tab.search_index_label.setText("Search index: building 0%")
    tab.search_index_label.show()

    tab.set_evidence(ev2, defer_load=True, evidence_label=case.labels[ev2])
    assert tab._index_worker is None
    assert first.is_cancelled()
    assert tab.search_index_label.isHidden()

    gate.set()  # let the running batch finish: the worker then stops
    qtbot.waitUntil(lambda: ev1 in ended_runs, timeout=10000)
    status = data.get_url_search_index_status(ev1)
    assert status.state == UrlSearchIndexStatus.BUILDING and status.indexed_through_id > 0
    assert [log["result"] for log in _build_logs(data, ev1)] == ["cancelled"]

    # The new evidence builds when the tab is shown again; the old one's
    # signals never reach the tab.
    tab.hide()
    tab.show()
    qtbot.waitUntil(lambda: ev2 in ended_runs, timeout=10000)
    qtbot.waitUntil(lambda: tab._index_worker is None, timeout=10000)
    assert data.get_url_search_index_status(ev2).is_ready
    assert tab.search_index_label.isHidden()


def test_shutdown_stops_build(qtbot, make_tab, url_case, gated_build, ended_runs):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    gate, entered = gated_build
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: tab._index_worker is not None, timeout=10000)
    assert entered.wait(10)
    worker = tab._index_worker

    gate.set()
    tab.shutdown()
    assert tab._index_worker is None
    assert worker.is_cancelled()
    assert tab.search_index_label.isHidden()
    qtbot.waitUntil(lambda: evidence_id in ended_runs, timeout=10000)
    # Cancelled after the running batch; the next build resumes.
    assert data.get_url_search_index_status(evidence_id).indexed_through_id > 0
    assert data.build_url_search_index(evidence_id).is_ready


def test_hiding_tab_pauses_build_and_showing_resumes(qtbot, make_tab, url_case, gated_build, ended_runs):
    """Background writes stop when the tab is hidden (after the running batch)."""
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    gate, entered = gated_build
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: tab._index_worker is not None, timeout=10000)
    assert entered.wait(10)
    worker = tab._index_worker

    tab.hide()  # must not block on the gated batch
    assert tab._index_worker is None and worker.is_cancelled()
    gate.set()
    qtbot.waitUntil(lambda: evidence_id in ended_runs, timeout=10000)
    assert not data.get_url_search_index_status(evidence_id).is_ready

    tab.show()
    qtbot.waitUntil(lambda: data.get_url_search_index_status(evidence_id).is_ready, timeout=10000)


def test_pause_blocks_build_until_resumed(qtbot, make_tab, url_case, gated_build, ended_runs):
    """Case export pauses the build: it waits for the running batch, nothing restarts."""
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    gate, entered = gated_build
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: tab._index_worker is not None, timeout=10000)
    assert entered.wait(10)

    gate.set()
    assert tab.pause_search_index_build()
    assert tab._index_worker is None
    tab._start_search_index_build()  # e.g. a refresh while paused
    assert tab._index_worker is None
    assert not data.get_url_search_index_status(evidence_id).is_ready

    tab.resume_search_index_build()
    qtbot.waitUntil(lambda: data.get_url_search_index_status(evidence_id).is_ready, timeout=10000)


def test_no_index_build_starts_after_shutdown(qtbot, make_tab, url_case, monkeypatch):
    case, data = url_case
    starts = []
    original_start = UrlSearchIndexWorker.start
    monkeypatch.setattr(UrlSearchIndexWorker, "start", lambda self, *a: (starts.append(self.evidence_id), original_start(self, *a))[1])
    tab = make_tab(case, data, case.evidence_ids[0])
    tab.show()  # schedules the build start
    tab.shutdown()  # e.g. the evidence tab is closed right away
    qtbot.wait(200)
    assert starts == []
    assert tab._index_worker is None


def _loaded(tab) -> bool:
    return (
        not tab._load_pending
        and not tab._filters_loading
        and not tab._data_loading
        and tab.page_label.text().startswith(("Showing", "No URLs", "Page "))
    )


def test_own_index_build_is_not_reported_as_url_data_change(qtbot, make_tab, url_case, gated_build):
    """The tab's own build commits change data_version but not URL data (#62).

    While the build runs, paging keeps exact row numbers and the cached
    total. A change set aside meanwhile is applied once after the build: a
    real unnotified write (here a URL inserted on another connection) may
    have happened during it, so the total is recounted.
    """
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    gate, entered = gated_build
    tab = make_tab(case, data, evidence_id)
    tab.set_page_size(100)
    tab.show()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.page_label.text() == "Showing 1-100 of 120 URLs", timeout=10000)
    assert entered.wait(10)  # build started (its process_log row is committed)

    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)
    assert tab.page_label.text() == "Showing 101-120 of 120 URLs"  # set aside during the build

    # Unnotified write during the build, on another connection.
    writer = CaseDataAccess(case.case_folder, case.case_db_path)
    try:
        with writer._use_evidence_conn(evidence_id):
            with writer._connect() as conn:
                conn.execute(
                    "INSERT INTO urls(evidence_id, url, domain, discovered_by) "
                    "VALUES (?, 'https://late.example/x', 'late.example', 'late')",
                    (evidence_id,),
                )
    finally:
        writer.close()

    gate.set()
    qtbot.waitUntil(lambda: tab._index_worker is None, timeout=10000)
    # Applied once after the build: recounted; row numbers hidden beyond page 1.
    qtbot.waitUntil(lambda: _loaded(tab) and "of 121 URLs" in tab.page_label.text(), timeout=10000)
    assert "row numbers hidden" in tab.page_label.text()

    tab.prev_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 0, timeout=10000)
    assert tab.page_label.text() == "Showing 1-100 of 121 URLs"
    assert data.get_url_search_index_status(evidence_id).is_ready


def test_build_never_starts_while_a_page_load_is_in_flight(qtbot, make_tab, url_case, monkeypatch, ended_runs):
    """A page in flight may hold an external change; it must be applied before own writes begin."""
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    loading_at_start = []
    original_start = UrlSearchIndexWorker.start

    def start(self, *args):
        loading_at_start.append(tab._data_loading)
        return original_start(self, *args)

    monkeypatch.setattr(UrlSearchIndexWorker, "start", start)
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: data.get_url_search_index_status(evidence_id).is_ready, timeout=10000)
    tab.hide()
    tab.show()  # re-check page and build start are both scheduled
    qtbot.waitUntil(lambda: len(loading_at_start) >= 2 and tab._index_worker is None, timeout=10000)
    assert loading_at_start and not any(loading_at_start)


def test_external_change_while_hidden_is_applied_on_show_during_build(qtbot, make_tab, url_case, gated_build):
    """The #67 re-check on show still detects writes made while hidden, though a build resumes."""
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    gate, entered = gated_build
    tab = make_tab(case, data, evidence_id)
    tab.set_page_size(100)
    tab.show()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.page_label.text() == "Showing 1-100 of 120 URLs", timeout=10000)
    assert entered.wait(10)
    tab.next_button.click()
    qtbot.waitUntil(lambda: _loaded(tab) and tab.model.current_page() == 1, timeout=10000)
    tab.hide()

    writer = CaseDataAccess(case.case_folder, case.case_db_path)
    try:
        with writer._use_evidence_conn(evidence_id):
            with writer._connect() as conn:
                conn.execute(
                    "INSERT INTO urls(evidence_id, url, domain, discovered_by) "
                    "VALUES (?, 'https://late.example/x', 'late.example', 'late')",
                    (evidence_id,),
                )
    finally:
        writer.close()

    try:
        tab.show()  # the index is still not built: the build resumes after the re-check
        qtbot.waitUntil(
            lambda: _loaded(tab) and tab.page_label.text().endswith("of 121 URLs; data changed, row numbers hidden"),
            timeout=10000,
        )
    finally:
        gate.set()


def test_pause_waits_for_builds_stopped_without_waiting(qtbot, make_tab, url_case, gated_build, ended_runs):
    """Export pauses: a build detached on hide may still be committing; pause waits for it."""
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    gate, entered = gated_build
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: tab._index_worker is not None, timeout=10000)
    assert entered.wait(10)
    worker = tab._index_worker
    tab.hide()  # detached without waiting; its batch is still running
    assert tab._index_worker is None and worker.isRunning()

    threading.Timer(0.3, gate.set).start()
    assert tab.pause_search_index_build(timeout_ms=10000)
    assert not worker.isRunning()
    assert tab._stopping_index_workers == []
    tab.resume_search_index_build()


def test_pause_reports_a_build_that_does_not_stop_in_time(qtbot, make_tab, url_case, gated_build, ended_runs):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    gate, entered = gated_build
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: tab._index_worker is not None, timeout=10000)
    assert entered.wait(10)
    try:
        assert not tab.pause_search_index_build(timeout_ms=100)
    finally:
        gate.set()
    qtbot.waitUntil(lambda: evidence_id in ended_runs, timeout=10000)


def test_minimizing_does_not_stop_the_build(qtbot, make_tab, url_case, gated_build):
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    gate, entered = gated_build
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: tab._index_worker is not None, timeout=10000)
    assert entered.wait(10)
    worker = tab._index_worker
    try:
        tab.showMinimized()
        qtbot.wait(50)
        assert tab._index_worker is worker and not worker.is_cancelled()
    finally:
        gate.set()
    qtbot.waitUntil(lambda: data.get_url_search_index_status(evidence_id).is_ready, timeout=10000)


def test_deferred_build_start_survives_switching_to_grouped_view(qtbot, make_tab, url_case, monkeypatch):
    """Grouping while the deferring page load is in flight cancels it; the build still starts."""
    case, data = url_case
    evidence_id = case.evidence_ids[0]
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(
        lambda: _loaded(tab) and data.get_url_search_index_status(evidence_id).is_ready
        and tab._index_worker is None,
        timeout=10000,
    )
    tab.hide()
    writer = CaseDataAccess(case.case_folder, case.case_db_path)
    try:
        with writer._use_evidence_conn(evidence_id):
            with writer._connect() as conn:
                conn.execute(
                    "INSERT INTO urls(evidence_id, url, domain, discovered_by) "
                    "VALUES (?, 'https://late.example/x', 'late.example', 'late')",
                    (evidence_id,),
                )
    finally:
        writer.close()
    assert not data.get_url_search_index_status(evidence_id).is_ready

    release = threading.Event()
    original_load = CaseDataAccess.load_url_page

    def slow_load(self, query):
        assert release.wait(10)
        return original_load(self, query)

    monkeypatch.setattr(CaseDataAccess, "load_url_page", slow_load)
    try:
        tab.show()  # re-check in flight: the build start is deferred
        qtbot.waitUntil(lambda: tab._start_index_after_load, timeout=10000)
        tab.group_checkbox.setChecked(True)  # cancels the flat load
    finally:
        release.set()
    qtbot.waitUntil(lambda: data.get_url_search_index_status(evidence_id).is_ready, timeout=10000)
    assert not tab._start_index_after_load


def test_wait_for_builds_covers_builds_of_closed_tabs(qtbot, make_tab, url_case, gated_build, ended_runs):
    from app.services.url_search_index_worker import wait_for_url_search_index_builds

    case, data = url_case
    evidence_id = case.evidence_ids[0]
    gate, entered = gated_build
    tab = make_tab(case, data, evidence_id)
    tab.show()
    qtbot.waitUntil(lambda: tab._index_worker is not None, timeout=10000)
    assert entered.wait(10)
    tab.shutdown()  # closed evidence tab: its build is cancelled but still in its batch
    assert not wait_for_url_search_index_builds(100)
    threading.Timer(0.3, gate.set).start()
    assert wait_for_url_search_index_builds(10000)
    assert evidence_id in ended_runs
