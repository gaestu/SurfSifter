"""MainWindow Overview statistics and hidden-evidence deferral (issue #63).

Opens a small synthetic case with three evidences in a real ``MainWindow``
(offscreen) and spies on the data reads of the Overview and of the sub-tabs
that used to read the database for every evidence at case open.
"""
from __future__ import annotations

import shutil
import threading
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pytest
from PySide6.QtCore import QCoreApplication, QEvent, QThread, QThreadPool, Signal
from PySide6.QtWidgets import QDialog, QMessageBox

from app.data import STATS_URLS, CaseDataAccess, CaseStatsCache
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = pytest.mark.gui_offscreen

TIMEOUT_MS = 15000
ROWS = 30
CODE_ROOT = Path(__file__).resolve().parents[3]


def _label_int(label: Any) -> Optional[int]:
    text = label.text().replace(",", "").replace("'", "").replace("’", "").strip()
    return int(text) if text.isdigit() else None


def _overview_label(evidence_tabs: Any, name: str = "urls_count_label") -> Any:
    return evidence_tabs.property("overview_widget").property(name)


def _overview_urls(evidence_tabs: Any) -> Optional[int]:
    return _label_int(_overview_label(evidence_tabs))


def _make_base_dir(tmp_path: Path) -> Path:
    """MainWindow base_dir with the shipped config (not the user's settings.json)."""
    base_dir = tmp_path / "base_dir"
    config_dst = base_dir / "config"
    config_dst.mkdir(parents=True)
    for name in ("config.yml", "settings.defaults.json"):
        source = CODE_ROOT / "config" / name
        if source.exists():
            shutil.copy2(source, config_dst / name)
    return base_dir


class Spies:
    """Records calls of selected methods: name -> [(key, on_gui_thread)]."""

    def __init__(self, monkeypatch) -> None:
        self._monkeypatch = monkeypatch
        self.calls: Dict[str, List[tuple]] = defaultdict(list)

    def watch(self, cls: type, name: str, key: Callable[..., Any], label: Optional[str] = None) -> None:
        original = getattr(cls, name)
        calls = self.calls[label or f"{cls.__name__}.{name}"]

        def wrapper(obj, *args, **kwargs):
            calls.append((key(obj, *args, **kwargs), threading.current_thread() is threading.main_thread()))
            return original(obj, *args, **kwargs)

        self._monkeypatch.setattr(cls, name, wrapper)

    def keys(self, label: str) -> List[Any]:
        return [key for key, _gui in self.calls[label]]

    def clear(self) -> None:
        for calls in self.calls.values():
            calls.clear()


@pytest.fixture
def spies(monkeypatch):
    from app.features.audit.tab import ExtractionSubtab
    from app.features.browser_inventory.inventory.model import BrowserInventoryModel
    from app.features.browser_inventory.inventory.widget import InventorySubtab
    from app.features.downloads import DownloadTab
    from app.features.extraction import ExtractionTab
    from app.features.file_list.models import FileListModel
    from app.features.reports import ReportsTab
    from app.features.screenshots import ScreenshotsTab
    from app.features.tags.models.tags_table import TagsTableModel
    from app.features.urls.tab import UrlsTab
    from app.services.overview_stats import OverviewStatsLoader

    # No URL search index build: the fixture case stays unchanged.
    monkeypatch.setattr(UrlsTab, "_start_search_index_build", lambda self: None)
    # External tool version probes (one subprocess per tool, per extractor
    # card, per evidence) are irrelevant here and take seconds.
    import core.tool_discovery as tool_discovery

    monkeypatch.setattr(tool_discovery, "get_tool_version", lambda cmd: None)

    def unexpected_box(*args, **kwargs):
        raise AssertionError(f"unexpected QMessageBox: {args[1:3]}")

    for name in ("critical", "warning", "information", "question"):
        monkeypatch.setattr(QMessageBox, name, staticmethod(unexpected_box))

    s = Spies(monkeypatch)
    first_arg = lambda obj, evidence_id, *a, **k: int(evidence_id)  # noqa: E731
    s.watch(CaseDataAccess, "load_overview_stats", first_arg, "load_overview_stats")
    s.watch(CaseDataAccess, "get_evidence_counts", first_arg, "get_evidence_counts")
    s.watch(CaseDataAccess, "get_tag_statistics", first_arg, "get_tag_statistics")
    s.watch(CaseDataAccess, "drop_evidence_stats", first_arg, "drop_evidence_stats")
    s.watch(CaseStatsCache, "begin_writes", first_arg, "begin_stats_writes")
    s.watch(CaseStatsCache, "end_writes", first_arg, "end_stats_writes")
    s.watch(OverviewStatsLoader, "request", first_arg, "overview_request")
    s.watch(DownloadTab, "_update_tab_counts", lambda obj: obj.evidence_id, "download_counts")
    s.watch(BrowserInventoryModel, "_load_data", lambda obj: obj.evidence_id, "inventory_load")
    s.watch(InventorySubtab, "_populate_filters", lambda obj: obj.ctx.evidence_id, "inventory_filters")
    s.watch(FileListModel, "_load_data", lambda obj: obj.evidence_id, "file_list_load")
    s.watch(ExtractionTab, "_refresh_ui", lambda obj: obj.evidence_id, "extraction_status")
    s.watch(TagsTableModel, "reload", lambda obj: obj.evidence_id, "tags_reload")
    s.watch(ScreenshotsTab, "_refresh", lambda obj: obj.evidence_id, "screenshots_refresh")
    s.watch(ReportsTab, "_apply_evidence", lambda obj: obj._evidence_id, "reports_evidence")
    s.watch(ReportsTab, "_apply_case_data", lambda obj: obj._evidence_id, "reports_case")
    s.watch(ExtractionSubtab, "load", lambda obj: obj.evidence_id, "audit_extraction_load")
    return s


def _close_window(window: Any) -> None:
    window.close()
    QThreadPool.globalInstance().waitForDone(10000)
    QCoreApplication.processEvents()
    window.deleteLater()
    QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)


@pytest.fixture
def open_window(qtbot, tmp_path, spies):
    """Factory: open a fixture case in a fresh, shown MainWindow."""
    from app.main import MainWindow

    windows = []
    base_dir = _make_base_dir(tmp_path)

    def _open(case, window=None):
        if window is None:
            window = MainWindow(base_dir)
            windows.append(window)
            window.show()
        evidence_count = len(case.evidence_ids)
        before = window.case_data
        window._open_case(case.case_folder, case.case_db_path)
        qtbot.waitUntil(
            lambda: window.case_data is not before
            and window.case_data is not None
            and window.main_tabs.count() == 1 + evidence_count,
            timeout=TIMEOUT_MS,
        )
        return window

    yield _open
    for window in windows:
        _close_window(window)


@pytest.fixture
def three_evidence_case(tmp_path):
    return build_url_fixture_case(
        tmp_path / "case", rows_per_evidence=ROWS, evidence_count=3, tag_every=3, domain_count=4
    )


# ---------------------------------------------------------------------------
# Case open: only the visible evidence's Overview loads, off the GUI thread
# ---------------------------------------------------------------------------


def test_case_open_loads_only_the_visible_overview_off_the_gui_thread(qtbot, open_window, spies, three_evidence_case):
    case = three_evidence_case
    ev1, ev2, ev3 = case.evidence_ids
    window = open_window(case)
    first = window.main_tabs.widget(1)
    assert window.main_tabs.currentWidget() is first

    qtbot.waitUntil(lambda: _overview_urls(first) == ROWS, timeout=TIMEOUT_MS)
    assert _label_int(_overview_label(first, "tags_count_label")) == 2
    qtbot.wait(200)  # anything else queued would run now

    assert spies.keys("load_overview_stats") == [ev1]
    assert all(not gui for _eid, gui in spies.calls["load_overview_stats"])
    assert all(not gui for _eid, gui in spies.calls["get_evidence_counts"])
    assert all(not gui for _eid, gui in spies.calls["get_tag_statistics"])
    assert set(spies.keys("overview_request")) == {ev1}

    # Hidden evidences' Overviews were never loaded.
    for index in (2, 3):
        assert _overview_urls(window.main_tabs.widget(index)) is None
    # No sub-tab read data for any evidence (only Overviews are shown).
    for name in (
        "download_counts",
        "inventory_load",
        "inventory_filters",
        "file_list_load",
        "extraction_status",
        "tags_reload",
        "screenshots_refresh",
        "reports_evidence",
        "reports_case",
        "audit_extraction_load",
    ):
        assert spies.calls[name] == [], f"{name} ran at case open: {spies.calls[name]}"


def test_switching_evidence_loads_once_and_switching_back_is_cached(qtbot, open_window, spies, three_evidence_case):
    case = three_evidence_case
    ev1, ev2, ev3 = case.evidence_ids
    window = open_window(case)
    tabs = window.main_tabs
    first, second = tabs.widget(1), tabs.widget(2)
    qtbot.waitUntil(lambda: _overview_urls(first) == ROWS, timeout=TIMEOUT_MS)

    tabs.setCurrentIndex(2)
    qtbot.waitUntil(lambda: _overview_urls(second) == ROWS, timeout=TIMEOUT_MS)
    assert spies.keys("load_overview_stats") == [ev1, ev2]

    # Back to the first evidence: served from the cache, synchronously.
    tabs.setCurrentIndex(1)
    assert _overview_urls(first) == ROWS
    tabs.setCurrentIndex(2)
    assert _overview_urls(second) == ROWS
    qtbot.wait(200)
    assert spies.keys("load_overview_stats") == [ev1, ev2]
    assert ev3 not in spies.keys("overview_request")


def test_overview_loads_only_when_overview_subtab_is_shown(qtbot, open_window, spies, three_evidence_case):
    case = three_evidence_case
    ev1, ev2, _ev3 = case.evidence_ids
    window = open_window(case)
    tabs = window.main_tabs
    first, second = tabs.widget(1), tabs.widget(2)
    qtbot.waitUntil(lambda: _overview_urls(first) == ROWS, timeout=TIMEOUT_MS)

    # Second evidence opened on a non-Overview sub-tab: no Overview load.
    second.blockSignals(True)
    second.setCurrentIndex(second.indexOf(second.property("tags_tab")))
    second.blockSignals(False)
    tabs.setCurrentIndex(2)
    qtbot.wait(200)
    assert ev2 not in spies.keys("load_overview_stats")

    second.setCurrentIndex(0)
    qtbot.waitUntil(lambda: _overview_urls(second) == ROWS, timeout=TIMEOUT_MS)
    assert spies.keys("load_overview_stats") == [ev1, ev2]


# ---------------------------------------------------------------------------
# Data changes
# ---------------------------------------------------------------------------


def test_data_change_of_visible_evidence_reloads_its_overview(qtbot, open_window, spies, three_evidence_case):
    case = three_evidence_case
    ev1 = case.evidence_ids[0]
    window = open_window(case)
    first = window.main_tabs.widget(1)
    qtbot.waitUntil(lambda: _overview_urls(first) == ROWS, timeout=TIMEOUT_MS)

    window.case_data.tag_artifact(ev1, "Fresh", "url", 1)
    window._on_data_changed(first)
    tags_label = _overview_label(first, "tags_count_label")
    qtbot.waitUntil(lambda: _label_int(tags_label) == 3, timeout=TIMEOUT_MS)
    qtbot.wait(100)
    assert spies.keys("load_overview_stats") == [ev1, ev1]


def test_data_change_of_hidden_evidence_only_invalidates_and_marks_stale(qtbot, open_window, spies, three_evidence_case):
    case = three_evidence_case
    ev1, ev2, _ev3 = case.evidence_ids
    window = open_window(case)
    tabs = window.main_tabs
    first, second = tabs.widget(1), tabs.widget(2)
    qtbot.waitUntil(lambda: _overview_urls(first) == ROWS, timeout=TIMEOUT_MS)
    # Cache the second evidence's stats, then go back to the first one.
    tabs.setCurrentIndex(2)
    qtbot.waitUntil(lambda: _overview_urls(second) == ROWS, timeout=TIMEOUT_MS)
    tabs.setCurrentIndex(1)
    assert window.case_data.peek_overview_stats(ev2) is not None
    spies.clear()

    refreshed: List[str] = []
    for index in range(second.count()):
        sub = second.widget(index)
        if hasattr(sub, "refresh") and hasattr(sub, "mark_stale"):
            sub.refresh = lambda name=type(sub).__name__: refreshed.append(name)

    window._on_data_changed(second)
    qtbot.wait(200)

    assert window.case_data.peek_overview_stats(ev2) is None  # invalidated
    assert spies.calls["load_overview_stats"] == []  # but not reloaded while hidden
    assert spies.calls["overview_request"] == []
    assert refreshed == []
    assert second.property("download_tab")._data_stale
    assert second.property("file_list_tab")._data_stale
    assert second.property("browser_inventory_tab")._data_stale
    # The visible evidence keeps its cached stats.
    assert window.case_data.peek_overview_stats(ev1) is not None

    tabs.setCurrentIndex(2)
    qtbot.waitUntil(lambda: spies.keys("load_overview_stats") == [ev2], timeout=TIMEOUT_MS)


def test_extraction_finished_for_hidden_evidence_does_not_load(qtbot, open_window, spies, three_evidence_case):
    case = three_evidence_case
    ev1, ev2, _ev3 = case.evidence_ids
    window = open_window(case)
    first, second = window.main_tabs.widget(1), window.main_tabs.widget(2)
    qtbot.waitUntil(lambda: _overview_urls(first) == ROWS, timeout=TIMEOUT_MS)
    spies.clear()

    window._on_extraction_finished(second, "Some Extractor", True, "")
    qtbot.wait(200)
    assert spies.calls["load_overview_stats"] == []
    assert second.property("download_tab")._data_stale

    window._on_extraction_finished(first, "Some Extractor", True, "")
    qtbot.waitUntil(lambda: spies.keys("load_overview_stats") == [ev1], timeout=TIMEOUT_MS)


def test_failed_overview_load_shows_dash(qtbot, open_window, spies, three_evidence_case, monkeypatch):
    case = three_evidence_case
    ev1 = case.evidence_ids[0]
    window = open_window(case)
    first = window.main_tabs.widget(1)
    qtbot.waitUntil(lambda: _overview_urls(first) == ROWS, timeout=TIMEOUT_MS)

    def boom(self, evidence_id, **kwargs):
        raise RuntimeError("overview read failed")

    monkeypatch.setattr(CaseDataAccess, "load_overview_stats", boom)
    window.case_data.invalidate_stats(ev1, STATS_URLS)
    window._refresh_counts()
    assert _overview_label(first).text() == "Loading…"
    qtbot.waitUntil(lambda: _overview_label(first).text() == "—", timeout=TIMEOUT_MS)
    for name in window._OVERVIEW_STAT_LABELS:
        assert _overview_label(first, name).text() == "—"


# ---------------------------------------------------------------------------
# Case-wide extract & ingest: statistics write scopes always end
# ---------------------------------------------------------------------------


class _FakeCaseWideDialog:
    def __init__(self, evidences, parent=None) -> None:
        self.evidences = evidences

    def exec(self):
        return QDialog.Accepted

    def get_selected_extractors(self):
        return ["fake_extractor"]

    def get_overwrite_mode(self):
        return "append"

    def get_extractor_configs(self):
        return {}


class _FakeCaseWideWorker(QThread):
    progress = Signal(int, int, str)
    log_message = Signal(int, str)
    batch_finished = Signal(dict)
    evidence_started = Signal(int, str)
    evidence_finished = Signal(int, str, bool, str)

    mode = "finish"

    def __init__(self, evidence_ids, **kwargs) -> None:
        super().__init__()
        self.evidence_ids = evidence_ids

    def start(self, *args):
        if self.mode == "raise":
            raise RuntimeError("cannot start thread")
        super().start(*args)

    def run(self):
        if self.mode == "finish":
            self.batch_finished.emit({"succeeded": list(self.evidence_ids), "failed": [], "skipped": []})
        elif self.mode == "crash":
            raise RuntimeError("worker crashed without reporting")

    def cancel(self):
        pass


@pytest.mark.parametrize("mode", ["finish", "crash", "raise"])
def test_case_wide_run_ends_write_scopes_in_every_outcome(qtbot, open_window, spies, three_evidence_case, monkeypatch, mode):
    import app.common.dialogs as dialogs_module
    import app.services.workers as workers_module

    case = three_evidence_case
    ev1, ev2, ev3 = case.evidence_ids
    window = open_window(case)
    first = window.main_tabs.widget(1)
    qtbot.waitUntil(lambda: _overview_urls(first) == ROWS, timeout=TIMEOUT_MS)

    monkeypatch.setattr(dialogs_module, "CaseWideExtractIngestDialog", _FakeCaseWideDialog)
    monkeypatch.setattr(_FakeCaseWideWorker, "mode", mode)
    monkeypatch.setattr(workers_module, "CaseWideExtractAndIngestWorker", _FakeCaseWideWorker)
    monkeypatch.setattr(QMessageBox, "information", staticmethod(lambda *a, **k: None))
    spies.clear()

    if mode == "raise":
        with pytest.raises(RuntimeError, match="cannot start thread"):
            window._on_case_wide_extract_requested([ev1, ev2])
    else:
        # "crash": run() raises without reporting (pytest-qt would fail the
        # test on the uncaught exception; it is expected here).
        with qtbot.capture_exceptions() as exceptions:
            window._on_case_wide_extract_requested([ev1, ev2])
            worker = window._case_wide_worker
            qtbot.waitUntil(lambda: sorted(spies.keys("end_stats_writes")) == [ev1, ev2], timeout=TIMEOUT_MS)
            if worker is not None:
                qtbot.waitUntil(worker.isFinished, timeout=TIMEOUT_MS)
            qtbot.wait(100)
        assert len(exceptions) == (1 if mode == "crash" else 0)

    assert sorted(spies.keys("begin_stats_writes")) == [ev1, ev2]
    assert sorted(spies.keys("end_stats_writes")) == [ev1, ev2]  # exactly once each
    for evidence_id in (ev1, ev2, ev3):
        assert not window.case_data.stats_cache.writes_open(evidence_id)
    if mode == "finish":
        # The visible evidence's Overview was reloaded after the run.
        qtbot.waitUntil(lambda: ev1 in spies.keys("load_overview_stats"), timeout=TIMEOUT_MS)
        # The hidden processed evidence was not loaded, only marked stale.
        assert ev2 not in spies.keys("load_overview_stats")
        assert window.main_tabs.widget(2).property("download_tab")._data_stale
    progress = getattr(window, "_case_wide_progress", None)
    if progress is not None:
        progress.close()


def test_case_wide_scopes_close_once_when_thread_end_and_report_both_happen(
    qtbot, open_window, spies, three_evidence_case
):
    case = three_evidence_case
    ev1 = case.evidence_ids[0]
    window = open_window(case)
    scope = window.case_data.stats_write_scope(ev1)
    window._case_wide_stats_scopes = [scope]
    assert window.case_data.stats_cache.writes_open(ev1)
    scope.close("thread", "finished")  # e.g. the worker thread ended first
    window._close_case_wide_stats_scopes()  # then batch_finished arrived
    window._close_case_wide_stats_scopes()
    assert spies.keys("end_stats_writes") == [ev1]
    assert not window.case_data.stats_cache.writes_open(ev1)
    assert window._case_wide_stats_scopes == []


# ---------------------------------------------------------------------------
# Evidence removal, case switch and close with never-opened tabs
# ---------------------------------------------------------------------------


def test_evidence_removal_drops_its_stats_and_pending_load(qtbot, open_window, spies, three_evidence_case):
    case = three_evidence_case
    ev1, ev2, ev3 = case.evidence_ids
    window = open_window(case)
    tabs = window.main_tabs
    first, second = tabs.widget(1), tabs.widget(2)
    qtbot.waitUntil(lambda: _overview_urls(first) == ROWS, timeout=TIMEOUT_MS)
    tabs.setCurrentIndex(2)
    qtbot.waitUntil(lambda: _overview_urls(second) == ROWS, timeout=TIMEOUT_MS)

    window._remove_evidence(ev2, case.labels[ev2])
    assert spies.keys("drop_evidence_stats") == [ev2]
    assert window.case_data.peek_overview_stats(ev2) is None
    assert window.case_data.peek_overview_stats(ev1) is not None
    assert tabs.count() == 3
    assert window._overview_loader.pending_evidence_id != ev2


def test_case_switch_and_close_shut_down_never_opened_tabs(qtbot, open_window, spies, tmp_path, caplog):
    first_case = build_url_fixture_case(tmp_path / "first", rows_per_evidence=10, evidence_count=2, domain_count=2)
    second_case = build_url_fixture_case(tmp_path / "second", rows_per_evidence=20, evidence_count=1, domain_count=2)

    shutdowns: List[str] = []
    window = open_window(first_case)
    qtbot.waitUntil(lambda: _overview_urls(window.main_tabs.widget(1)) == 10, timeout=TIMEOUT_MS)
    old_tabs = [window.main_tabs.widget(i) for i in range(1, window.main_tabs.count())]
    for evidence_tabs in old_tabs:
        for index in range(evidence_tabs.count()):
            sub = evidence_tabs.widget(index)
            original = getattr(sub, "shutdown", None)
            if original is None:
                continue

            def recording(original=original, name=f"{evidence_tabs.property('evidence_id')}:{type(sub).__name__}"):
                shutdowns.append(name)
                return original()

            sub.shutdown = recording
    expected = {
        f"{t.property('evidence_id')}:{type(t.widget(i)).__name__}"
        for t in old_tabs
        for i in range(t.count())
        if hasattr(t.widget(i), "shutdown")
    }
    old_loader = window._overview_loader
    old_cache = window.case_data.stats_cache

    caplog.clear()
    open_window(second_case, window)
    assert set(shutdowns) == expected
    assert len(shutdowns) == len(expected)  # each exactly once
    # Beyond URLs: downloads, file list, extraction ... are shut down too.
    assert any(name.endswith(":DownloadTab") for name in shutdowns)
    assert any(name.endswith(":UrlsTab") for name in shutdowns)
    assert window._overview_loader is not old_loader
    assert old_cache._retired
    qtbot.waitUntil(lambda: _overview_urls(window.main_tabs.widget(1)) == 20, timeout=TIMEOUT_MS)

    window.close()
    QThreadPool.globalInstance().waitForDone(10000)
    assert window._overview_loader is None
    errors = [r for r in caplog.records if "Error during" in r.getMessage()]
    assert errors == []


def test_closing_never_shown_evidence_tab_shuts_down_each_subtab_once(
    qtbot, open_window, spies, three_evidence_case, caplog
):
    case = three_evidence_case
    ev1, ev2, ev3 = case.evidence_ids
    window = open_window(case)
    tabs = window.main_tabs
    qtbot.waitUntil(lambda: _overview_urls(tabs.widget(1)) == ROWS, timeout=TIMEOUT_MS)
    hidden = tabs.widget(3)
    assert hidden.property("evidence_id") == ev3 and _overview_urls(hidden) is None  # never shown

    shutdowns: List[str] = []
    expected: List[str] = []
    for index in range(hidden.count()):
        sub = hidden.widget(index)
        original = getattr(sub, "shutdown", None)
        if original is None:
            continue
        name = type(sub).__name__
        expected.append(name)

        def recording(original=original, name=name):
            shutdowns.append(name)
            return original()

        sub.shutdown = recording
    assert "UrlsTab" in expected and "ExtractionTab" in expected and "DownloadTab" in expected

    caplog.clear()
    window._on_close_evidence_tab(3)
    assert sorted(shutdowns) == sorted(expected)  # each exactly once
    assert tabs.count() == 3
    assert ev3 not in window._evidence_tabs
    errors = [r for r in caplog.records if "Error during" in r.getMessage()]
    assert errors == []


def test_extraction_log_of_hidden_evidence_reaches_its_audit_log(qtbot, open_window, spies, three_evidence_case):
    case = three_evidence_case
    ev1, ev2, ev3 = case.evidence_ids
    window = open_window(case)
    tabs = window.main_tabs
    qtbot.waitUntil(lambda: _overview_urls(tabs.widget(1)) == ROWS, timeout=TIMEOUT_MS)
    hidden = tabs.widget(2)
    assert hidden.property("evidence_id") == ev2
    assert tabs.currentWidget() is not hidden  # never opened

    extraction_tab = hidden.property("extraction_tab")
    hidden_log = hidden.property("audit_tab").log_widget
    visible_log = tabs.widget(1).property("audit_tab").log_widget
    visible_before = visible_log.toPlainText()

    extraction_tab.log_message.emit(ev2, "hidden evidence log line")
    qtbot.waitUntil(lambda: "hidden evidence log line" in hidden_log.toPlainText(), timeout=TIMEOUT_MS)
    assert hidden_log.toPlainText().splitlines()[-1] == "hidden evidence log line"
    assert visible_log.toPlainText() == visible_before  # routed to its own evidence only
