"""Evidence sub-tabs read nothing until they are shown (issue #63).

Each tab is built the way ``MainWindow._build_evidence_tab`` builds it for a
hidden evidence, on a small real case, while its data reads are spied on.
"""
from __future__ import annotations

import threading
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, List, Optional

import pytest
from PySide6.QtCore import QObject, QThread, Signal
from PySide6.QtWidgets import QMessageBox

from app.data import STATS_PROCESS_LOG, CaseDataAccess, CaseStatsCache, discard_case_stats_cache
from core.database import DatabaseManager
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = pytest.mark.gui_offscreen

TIMEOUT_MS = 10000


class Calls:
    """Method spies: label -> list of keys (in call order, across labels in ``order``)."""

    def __init__(self, monkeypatch) -> None:
        self._monkeypatch = monkeypatch
        self.by_label: Dict[str, List[Any]] = defaultdict(list)
        self.order: List[str] = []

    def watch(self, cls: type, name: str, label: str, key: Callable[..., Any] = lambda obj, *a, **k: None) -> None:
        original = getattr(cls, name)

        def wrapper(obj, *args, **kwargs):
            self.by_label[label].append(key(obj, *args, **kwargs))
            self.order.append(label)
            return original(obj, *args, **kwargs)

        self._monkeypatch.setattr(cls, name, wrapper)

    def __getitem__(self, label: str) -> List[Any]:
        return self.by_label[label]


@pytest.fixture
def calls(monkeypatch):
    return Calls(monkeypatch)


@pytest.fixture
def env(tmp_path: Path):
    case = build_url_fixture_case(
        tmp_path / "case", rows_per_evidence=20, evidence_count=1, tag_every=3, domain_count=3
    )
    discard_case_stats_cache(case.case_db_path)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    manager = DatabaseManager(case.case_folder, case_db_path=case.case_db_path)
    evidence_id = case.evidence_ids[0]
    yield SimpleNamespace(
        case=case,
        data=data,
        manager=manager,
        evidence_id=evidence_id,
        label=case.labels[evidence_id],
    )
    data.close()
    manager.close_all()
    discard_case_stats_cache(case.case_db_path)


@pytest.fixture
def no_message_boxes(monkeypatch):
    shown: List[tuple] = []
    for name in ("critical", "warning", "information"):
        monkeypatch.setattr(QMessageBox, name, staticmethod(lambda *a, _n=name, **k: shown.append((_n, a[1:3]))))
    return shown


# ---------------------------------------------------------------------------
# Download tab
# ---------------------------------------------------------------------------


def test_download_tab_counts_wait_for_first_show(qtbot, env, calls, monkeypatch):
    from app.features.downloads import DownloadTab

    calls.watch(DownloadTab, "_update_tab_counts", "counts")
    tab = DownloadTab(env.evidence_id, env.data, env.case.case_folder)
    qtbot.addWidget(tab)
    tab.set_case_data(env.data, env.case.case_folder)
    assert calls["counts"] == []
    assert tab._counts_worker is None

    panel_refreshes: List[str] = []
    for name in ("available_panel", "images_panel", "files_panel"):
        monkeypatch.setattr(getattr(tab, name), "refresh", lambda n=name: panel_refreshes.append(n))
    tab.refresh()  # hidden: only remembered
    assert calls["counts"] == [] and panel_refreshes == []
    assert tab._counts_pending and tab._data_stale

    tab.show()
    qtbot.waitUntil(lambda: len(calls["counts"]) == 1, timeout=TIMEOUT_MS)
    qtbot.waitUntil(lambda: sorted(panel_refreshes) == ["available_panel", "files_panel", "images_panel"],
                    timeout=TIMEOUT_MS)
    assert not tab._counts_pending
    qtbot.wait(50)
    assert len(calls["counts"]) == 1

    # Visible: refresh and set_case_data count right away.
    tab.refresh()
    assert len(calls["counts"]) == 2
    tab.set_case_data(env.data, env.case.case_folder)
    assert len(calls["counts"]) == 3
    tab.shutdown()


def test_download_tab_first_show_without_pending_refresh_counts_once(qtbot, env, calls):
    from app.features.downloads import DownloadTab

    calls.watch(DownloadTab, "_update_tab_counts", "counts")
    tab = DownloadTab(env.evidence_id, env.data, env.case.case_folder)
    qtbot.addWidget(tab)
    tab.show()
    qtbot.waitUntil(lambda: len(calls["counts"]) == 1, timeout=TIMEOUT_MS)
    tab.hide()
    tab.show()
    qtbot.wait(50)
    assert len(calls["counts"]) == 1
    tab.shutdown()


# ---------------------------------------------------------------------------
# Browser Inventory tab
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("defer_load", [True, False])
def test_browser_inventory_loads_once_on_first_show(qtbot, env, calls, defer_load):
    from app.features.browser_inventory import BrowserInventoryTab
    from app.features.browser_inventory.inventory.model import BrowserInventoryModel
    from app.features.browser_inventory.inventory.widget import InventorySubtab

    calls.watch(BrowserInventoryModel, "_load_data", "model_load")
    calls.watch(InventorySubtab, "_populate_filters", "filters")
    tab = BrowserInventoryTab(
        env.case.case_folder, env.evidence_id, env.case.case_db_path, case_data=env.data, defer_load=defer_load
    )
    qtbot.addWidget(tab)
    assert calls["model_load"] == [] and calls["filters"] == []

    tab.show()
    qtbot.waitUntil(lambda: len(calls["model_load"]) == 1, timeout=TIMEOUT_MS)
    qtbot.wait(100)
    assert len(calls["model_load"]) == 1  # the double load is gone
    assert len(calls["filters"]) == 1
    assert tab._all_subtabs[0].is_loaded
    assert not any(sub.is_loaded for sub in tab._all_subtabs[1:])


def test_browser_inventory_stale_before_first_show_loads_once(qtbot, env, calls):
    from app.features.browser_inventory import BrowserInventoryTab
    from app.features.browser_inventory.inventory.model import BrowserInventoryModel

    calls.watch(BrowserInventoryModel, "_load_data", "model_load")
    tab = BrowserInventoryTab(
        env.case.case_folder, env.evidence_id, env.case.case_db_path, case_data=env.data, defer_load=True
    )
    qtbot.addWidget(tab)
    tab.mark_stale()
    tab.show()
    qtbot.waitUntil(lambda: len(calls["model_load"]) == 1, timeout=TIMEOUT_MS)
    qtbot.wait(100)
    assert len(calls["model_load"]) == 1


def test_browser_inventory_subtab_switch_before_show_loads_nothing(qtbot, env, calls):
    from app.features.browser_inventory import BrowserInventoryTab
    from app.features.browser_inventory.inventory.model import BrowserInventoryModel

    calls.watch(BrowserInventoryModel, "_load_data", "model_load")
    tab = BrowserInventoryTab(
        env.case.case_folder, env.evidence_id, env.case.case_db_path, case_data=env.data, defer_load=True
    )
    qtbot.addWidget(tab)
    loads: List[int] = []
    for index, sub in enumerate(tab._all_subtabs):
        sub.load = lambda i=index: loads.append(i)
    tab.subtabs.setCurrentIndex(2)
    assert loads == []
    tab.show()
    qtbot.waitUntil(lambda: loads == [2], timeout=TIMEOUT_MS)  # the current sub-tab, once
    qtbot.wait(50)
    assert loads == [2]
    assert calls["model_load"] == []


# ---------------------------------------------------------------------------
# File List tab
# ---------------------------------------------------------------------------


def test_file_list_with_defer_load_reads_no_rows_until_shown(qtbot, env, calls):
    from app.features.file_list import FileListTab
    from app.features.file_list.models import FileListModel

    calls.watch(FileListModel, "_load_data", "rows")
    calls.watch(FileListTab, "_load_data", "filters")
    tab = FileListTab(env.case.case_folder, env.evidence_id, env.case.case_db_path, defer_load=True)
    qtbot.addWidget(tab)
    tab.set_case_data(env.data)
    tab.mark_stale()
    qtbot.wait(50)
    assert calls["rows"] == [] and calls["filters"] == []
    assert tab._filter_worker is None

    tab.show()
    qtbot.waitUntil(lambda: len(calls["filters"]) == 1, timeout=TIMEOUT_MS)
    qtbot.waitUntil(lambda: len(calls["rows"]) >= 1 and tab._filter_worker is None, timeout=TIMEOUT_MS)


def test_file_list_filter_error_still_loads_rows(qtbot, env, calls):
    from app.features.file_list import FileListTab
    from app.features.file_list.models import FileListModel

    calls.watch(FileListModel, "_load_data", "rows")
    tab = FileListTab(env.case.case_folder, env.evidence_id, env.case.case_db_path, defer_load=True)
    qtbot.addWidget(tab)
    tab._on_filters_load_error("boom")
    assert len(calls["rows"]) == 1
    assert all(combo.isEnabled() for combo in (tab.extension_combo, tab.matches_combo, tab.tags_combo))


# ---------------------------------------------------------------------------
# Tags and Screenshots tabs
# ---------------------------------------------------------------------------


def test_tags_tab_reads_tags_only_when_shown(qtbot, env, calls):
    from app.features.tags import TagsTab
    from app.features.tags.models.tags_table import TagsTableModel

    calls.watch(TagsTableModel, "reload", "reload", key=lambda obj: obj.evidence_id)
    calls.watch(CaseDataAccess, "list_tags", "list_tags", key=lambda obj, eid, *a, **k: eid)
    tab = TagsTab()
    qtbot.addWidget(tab)
    tab.set_case_data(env.data)
    tab.set_evidence(env.evidence_id, env.label)
    assert calls["reload"] == [] and calls["list_tags"] == []

    tab.show()
    qtbot.waitUntil(lambda: env.evidence_id in calls["reload"], timeout=TIMEOUT_MS)
    qtbot.waitUntil(lambda: tab.model.rowCount() == 2, timeout=TIMEOUT_MS)

    # Visible: a new evidence/case data reloads right away.
    before = len(calls["reload"])
    tab.set_evidence(env.evidence_id, env.label)
    assert len(calls["reload"]) == before + 1


def test_screenshots_tab_reads_only_when_shown(qtbot, env, calls):
    from app.features.screenshots import ScreenshotsTab

    calls.watch(ScreenshotsTab, "_refresh", "refresh", key=lambda obj: obj.evidence_id)
    tab = ScreenshotsTab(env.data, env.case.case_folder, env.manager)
    qtbot.addWidget(tab)
    tab.set_evidence(env.evidence_id, env.label)
    assert calls["refresh"] == []

    tab.show()
    qtbot.waitUntil(lambda: env.evidence_id in calls["refresh"], timeout=TIMEOUT_MS)
    before = len(calls["refresh"])
    tab.set_evidence(env.evidence_id, env.label)  # visible: immediate
    assert len(calls["refresh"]) == before + 1


# ---------------------------------------------------------------------------
# Reports tab
# ---------------------------------------------------------------------------


def _watch_report_widget(calls: Calls) -> None:
    from reports.ui.report_tab_widget import ReportTabWidget

    calls.watch(ReportTabWidget, "set_case_data", "widget.set_case_data")
    calls.watch(ReportTabWidget, "set_evidence", "widget.set_evidence", key=lambda obj, eid, *a, **k: eid)
    calls.watch(ReportTabWidget, "set_db_connection", "widget.set_db_connection")


def _make_reports_tab(qtbot, env):
    from app.features.reports import ReportsTab

    tab = ReportsTab()
    qtbot.addWidget(tab)
    tab.set_database_manager(env.manager)
    tab.set_case_data(env.data)
    tab.set_evidence(env.evidence_id, env.label)
    return tab


def test_reports_tab_defers_db_work_until_shown_case_first(qtbot, env, calls):
    _watch_report_widget(calls)
    tab = _make_reports_tab(qtbot, env)
    assert calls.order == []
    assert tab._export_btn.isEnabled()  # the button state needs no DB work

    tab.show()
    assert calls.order[:1] == ["widget.set_case_data"]
    assert calls["widget.set_evidence"] == [env.evidence_id]
    assert calls.order.index("widget.set_case_data") < calls.order.index("widget.set_evidence")

    # Loaded once; later changes apply right away.
    tab.hide()
    tab.show()
    assert calls["widget.set_evidence"] == [env.evidence_id]
    tab.set_evidence(env.evidence_id, env.label)
    assert calls["widget.set_evidence"] == [env.evidence_id, env.evidence_id]


def test_reports_tab_summary_entry_point_loads_while_hidden(qtbot, env, calls):
    _watch_report_widget(calls)
    tab = _make_reports_tab(qtbot, env)
    assert calls.order == []
    tab._open_summary()
    try:
        assert calls.order.index("widget.set_case_data") < calls.order.index("widget.set_evidence")
        assert tab._summary_dialog is not None
    finally:
        if tab._summary_dialog is not None:
            tab._summary_dialog.close()


def test_reports_tab_export_entry_point_loads_while_hidden(qtbot, env, calls, monkeypatch, no_message_boxes):
    from PySide6.QtWidgets import QFileDialog

    _watch_report_widget(calls)
    tab = _make_reports_tab(qtbot, env)
    monkeypatch.setattr(QFileDialog, "getSaveFileName", staticmethod(lambda *a, **k: ("", "")))
    tab._on_export_tag_summary()  # cancelled in the file dialog
    assert calls.order.index("widget.set_case_data") < calls.order.index("widget.set_evidence")


def test_reports_tab_process_log_write_invalidates_overview_last_run(qtbot, env):
    from app.features.reports import ReportsTab

    invalidations: List[tuple] = []
    case_data = SimpleNamespace(invalidate_stats=lambda *args: invalidations.append(args))
    tab = ReportsTab()
    qtbot.addWidget(tab)
    tab.set_case_data(case_data)

    tab._report_widget.process_log_written.emit()  # no evidence yet: no-op
    assert invalidations == []

    tab.set_evidence(env.evidence_id, env.label)
    tab._report_widget.process_log_written.emit()
    assert invalidations == [(env.evidence_id, STATS_PROCESS_LOG)]

    # Case data without a statistics cache (e.g. a metadata dict): no-op.
    tab.set_case_data({"case_number": "X"})
    tab._report_widget.process_log_written.emit()
    assert invalidations == [(env.evidence_id, STATS_PROCESS_LOG)]


@pytest.mark.parametrize("outcome", ["success", "error"])
def test_reports_tab_tag_summary_export_invalidates_last_run(
    qtbot, env, monkeypatch, no_message_boxes, outcome
):
    """The export writes process_log in both outcomes (Overview "last run")."""
    from PySide6.QtWidgets import QFileDialog

    import app.features.reports.tab as reports_module
    from reports.tag_summary_service import TagSummaryExportError

    invalidations: List[tuple] = []
    original = CaseDataAccess.invalidate_stats
    monkeypatch.setattr(
        CaseDataAccess,
        "invalidate_stats",
        lambda self, eid=None, cats=None: (invalidations.append((eid, cats)), original(self, eid, cats))[1],
    )
    target = Path(env.case.case_folder) / "exports" / "summary.xlsx"
    monkeypatch.setattr(QFileDialog, "getSaveFileName", staticmethod(lambda *a, **k: (str(target), "")))

    def fake_export(**kwargs):
        if outcome == "error":
            raise TagSummaryExportError("workbook write failed", stage="write")
        return SimpleNamespace(output_path=kwargs["output_path"])

    monkeypatch.setattr(reports_module, "export_tag_summary", fake_export)
    tab = _make_reports_tab(qtbot, env)
    tab._on_export_tag_summary()
    assert invalidations == [(env.evidence_id, STATS_PROCESS_LOG)]
    expected_box = "information" if outcome == "success" else "critical"
    assert [kind for kind, _args in no_message_boxes] == [expected_box]


# ---------------------------------------------------------------------------
# Audit tab
# ---------------------------------------------------------------------------


def test_audit_tab_extraction_subtab_loads_on_first_show(qtbot, env, calls):
    from app.features.audit import AuditTab
    from app.features.audit.tab import ExtractionSubtab

    calls.watch(ExtractionSubtab, "load", "extraction_load")
    tab = AuditTab(env.manager, env.evidence_id, env.label, case_path=env.case.case_folder)
    qtbot.addWidget(tab)
    assert calls["extraction_load"] == []
    assert not tab._extraction_loaded

    tab.show()
    qtbot.waitUntil(lambda: len(calls["extraction_load"]) == 1, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    assert len(calls["extraction_load"]) == 1


# ---------------------------------------------------------------------------
# Extractors tab
# ---------------------------------------------------------------------------


@pytest.fixture
def fast_tool_probes(monkeypatch):
    """External tool version probes are irrelevant here and take seconds."""
    import core.tool_discovery as tool_discovery

    monkeypatch.setattr(tool_discovery, "get_tool_version", lambda cmd: None)


def _make_extraction_tab(qtbot, env):
    from app.features.extraction import ExtractionTab

    tab = ExtractionTab(env.evidence_id, env.data)
    qtbot.addWidget(tab)
    case = SimpleNamespace(workspace_dir=str(env.case.case_folder), database_path=str(env.case.case_db_path))
    evidence = {"id": env.evidence_id, "label": env.label, "source_path": "/bench/evidence1.E01"}
    tab.set_current_case(case=case, evidence=evidence, evidence_fs=None, db_manager=env.manager, audit_logger=None)
    return tab


def test_extraction_tab_status_queries_wait_for_show(qtbot, env, calls, fast_tool_probes, monkeypatch):
    import app.features.extraction.tab as extraction_module
    from app.features.extraction import ExtractionTab

    calls.watch(ExtractionTab, "_refresh_ui", "refresh_ui")
    queries: List[str] = []
    original_query = extraction_module.get_extractor_run_status

    def counting_query(db_path, extractor_name, evidence_id):
        queries.append(extractor_name)
        return original_query(db_path, extractor_name, evidence_id)

    # The per-extractor process_log query
    monkeypatch.setattr(extraction_module, "get_extractor_run_status", counting_query)
    calls.by_label["status_query"] = queries
    tab = _make_extraction_tab(qtbot, env)
    assert calls["refresh_ui"] == [] and calls["status_query"] == []
    assert tab._status_stale

    tab.refresh_status()  # hidden: still only marked stale
    assert calls["refresh_ui"] == []

    tab.show()
    assert len(calls["refresh_ui"]) == 1
    assert len(calls["status_query"]) == len(tab._run_status_widgets) > 0
    assert not tab._status_stale
    tab.hide()
    tab.show()
    assert len(calls["refresh_ui"]) == 1

    tab.refresh_status()  # visible: immediate
    assert len(calls["refresh_ui"]) == 2
    tab.shutdown()


def test_extraction_tabs_spawn_no_tool_version_probes(qtbot, env, monkeypatch):
    """Building extractor cards for several evidences probes no tool (issue #70)."""
    import core.tool_discovery as tool_discovery

    probes: List[List[str]] = []

    def counting_probe(cmd):
        probes.append(cmd)
        return None

    monkeypatch.setattr(tool_discovery, "get_tool_version", counting_probe)
    tabs = [_make_extraction_tab(qtbot, env) for _ in range(3)]
    for tab in tabs:
        tab.show()  # run-status refresh reads metadata again
        assert tab._extractors
    assert probes == []
    for tab in tabs:
        tab.shutdown()


class _FakeSignals(QObject):
    progress = Signal(int, int, str)
    log_message = Signal(str, str)
    step = Signal(str)


class _FakeIngestionWorker(QThread):
    """Stands in for IngestionWorker; ``mode`` picks the outcome."""

    finished = Signal(bool, dict)  # shadows QThread.finished like the real worker
    error = Signal(str)
    mode = "success"
    instances: List["_FakeIngestionWorker"] = []

    def __init__(self, *args, parent=None, **kwargs) -> None:
        super().__init__()
        self.callbacks = _FakeSignals()
        self.cancelled = False
        _FakeIngestionWorker.instances.append(self)

    def start(self, *args):
        if self.mode == "start_raises":
            raise RuntimeError("cannot start worker")
        super().start(*args)

    def run(self):
        if self.mode == "success":
            self.finished.emit(True, {"urls": 1})
        elif self.mode == "failure":
            self.finished.emit(False, {})
        elif self.mode == "error":
            self.error.emit("ingestion exploded")
        elif self.mode == "crash":
            raise RuntimeError("worker died without reporting")

    def cancel(self):
        self.cancelled = True


class _FakeExtractor:
    def __init__(self, tmp_path: Path) -> None:
        self.metadata = SimpleNamespace(name="fake_extractor", display_name="Fake Extractor", can_ingest=True)
        self._tmp_path = tmp_path
        self._config = {}

    def get_output_dir(self, workspace: Path, slug: str) -> Path:
        out = self._tmp_path / "out" / slug
        out.mkdir(parents=True, exist_ok=True)
        return out

    def can_run_ingestion(self, output_dir: Path):
        return True, ""


@pytest.mark.parametrize("mode", ["success", "failure", "error", "crash", "start_raises"])
def test_extraction_tab_ingestion_ends_stats_writes_exactly_once(
    qtbot, env, calls, fast_tool_probes, monkeypatch, no_message_boxes, tmp_path, mode
):
    import app.features.extraction.tab as extraction_module

    calls.watch(CaseStatsCache, "begin_writes", "begin", key=lambda obj, eid: eid)
    calls.watch(CaseStatsCache, "end_writes", "end", key=lambda obj, eid: eid)
    monkeypatch.setattr(_FakeIngestionWorker, "mode", mode)
    monkeypatch.setattr(_FakeIngestionWorker, "instances", [])
    monkeypatch.setattr(extraction_module, "IngestionWorker", _FakeIngestionWorker)

    tab = _make_extraction_tab(qtbot, env)
    data_changed: List[bool] = []
    tab.data_changed.connect(lambda: data_changed.append(True))
    cache = env.data.stats_cache

    if mode == "start_raises":
        with pytest.raises(RuntimeError, match="cannot start worker"):
            tab._run_ingestion(_FakeExtractor(tmp_path))
    else:
        with qtbot.capture_exceptions() as exceptions:
            tab._run_ingestion(_FakeExtractor(tmp_path))
            assert calls["begin"] == [env.evidence_id]
            worker = _FakeIngestionWorker.instances[0]
            qtbot.waitUntil(lambda: calls["end"] == [env.evidence_id], timeout=TIMEOUT_MS)
            qtbot.waitUntil(worker.isFinished, timeout=TIMEOUT_MS)
            qtbot.wait(100)
        assert len(exceptions) == (1 if mode == "crash" else 0)

    assert calls["begin"] == [env.evidence_id]
    assert calls["end"] == [env.evidence_id]
    assert not cache.writes_open(env.evidence_id)
    # Closed scopes are pruned on the GUI thread (a crashed worker's scope,
    # closed from its thread, stays listed until the next prune).
    assert all(scope.closed for _worker, scope in tab._stats_write_scopes)
    if mode != "crash":
        assert tab._stats_write_scopes == []
    assert data_changed == ([True] if mode == "success" else [])
    tab.shutdown()


def test_extraction_tab_shutdown_ends_scopes_of_stopped_writers(qtbot, env, calls, fast_tool_probes):
    calls.watch(CaseStatsCache, "end_writes", "end", key=lambda obj, eid: eid)
    tab = _make_extraction_tab(qtbot, env)
    scope = tab._open_stats_write_scope()  # e.g. a purge dialog that never returned
    assert env.data.stats_cache.writes_open(env.evidence_id)
    tab.shutdown()
    assert calls["end"] == [env.evidence_id]
    assert tab._stats_write_scopes == []
    scope.close()
    assert calls["end"] == [env.evidence_id]
    assert not env.data.stats_cache.writes_open(env.evidence_id)


def test_extraction_tab_scope_ends_when_worker_thread_finishes(qtbot, env, calls, fast_tool_probes):
    calls.watch(CaseStatsCache, "end_writes", "end", key=lambda obj, eid: eid)
    tab = _make_extraction_tab(qtbot, env)
    gate = threading.Event()

    class Writer(QThread):
        finished = Signal(bool)  # custom signal shadowing QThread.finished

        def run(self):
            gate.wait(5)

    worker = Writer()
    scope = tab._open_stats_write_scope(worker, close_on_thread_end=True)
    worker.start()
    qtbot.wait(50)
    assert calls["end"] == []
    assert env.data.stats_cache.writes_open(env.evidence_id)
    gate.set()
    qtbot.waitUntil(lambda: calls["end"] == [env.evidence_id], timeout=TIMEOUT_MS)
    worker.wait()
    assert scope.closed
    # Closed from the worker thread: the GUI-thread list still holds it
    assert tab._stats_write_scopes == [(worker, scope)]
    tab._close_stats_write_scopes(worker)  # the GUI-side terminal path
    qtbot.wait(50)
    assert calls["end"] == [env.evidence_id]
    assert tab._stats_write_scopes == []
    tab.shutdown()
