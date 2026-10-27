"""OS Artifacts shows rows committed after it was first loaded (issue #68).

The tab is built the way ``MainWindow._build_evidence_tab`` builds it (with the
embedded DPAPI subtab) on a small real case. Rows are committed through
``core.database.helpers`` on a separate connection, as ingestion does, and the
refresh is routed through ``MainWindow``'s data-change and sub-tab handlers.
"""
from __future__ import annotations

import logging
import shutil
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List

import pytest
from PySide6.QtCore import QCoreApplication, QEvent, QThreadPool
from PySide6.QtWidgets import QMessageBox, QTabWidget, QWidget

from app.data import CaseDataAccess, discard_case_stats_cache
from app.features.dpapi import DPAPITab
from app.features.os_artifacts import OSArtifactsTab
from app.main import MainWindow
from core.database import (
    DatabaseManager,
    insert_jump_list_entries,
    insert_os_indicators,
    insert_windows_users,
)
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = pytest.mark.gui_offscreen

TIMEOUT_MS = 10000
CODE_ROOT = Path(__file__).resolve().parents[3]


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def env(tmp_path: Path):
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=3, evidence_count=2, domain_count=2)
    discard_case_stats_cache(case.case_db_path)
    data = CaseDataAccess(case.case_folder, case.case_db_path)
    # Separate manager: rows are committed like an ingestion worker does
    writer = DatabaseManager(case.case_folder, case_db_path=case.case_db_path)
    yield SimpleNamespace(case=case, data=data, writer=writer, labels=case.labels, ids=case.evidence_ids)
    data.close()
    writer.close_all()
    discard_case_stats_cache(case.case_db_path)


@pytest.fixture
def loads(monkeypatch):
    """Counts full loads / reloads per tab class and evidence."""
    counts: Dict[str, List[int]] = defaultdict(list)

    def watch(cls, name, label):
        original = getattr(cls, name)

        def wrapper(obj, *args, **kwargs):
            counts[label].append(obj.evidence_id)
            return original(obj, *args, **kwargs)

        monkeypatch.setattr(cls, name, wrapper)

    watch(OSArtifactsTab, "_load_data", "os_load")
    watch(OSArtifactsTab, "_reload_data", "os_reload")
    watch(DPAPITab, "_load_data", "dpapi_load")
    return counts


def _commit(env, evidence_id, insert, rows) -> None:
    conn = env.writer.get_evidence_conn(evidence_id, env.labels[evidence_id])
    with conn:
        insert(conn, evidence_id, rows)


def add_indicators(env, evidence_id, indicator_type, *names) -> None:
    _commit(env, evidence_id, insert_os_indicators, [
        {
            "type": indicator_type,
            "name": name,
            "value": f"{name}-value",
            "path": f"HKLM\\Test\\{name}",
            "hive": "SOFTWARE",
            "confidence": "high",
            "detected_at_utc": "2024-01-01T00:00:00+00:00",
            "provenance": "test",
            "run_id": "run-68",
        }
        for name in names
    ])


def add_jump_list_entries(env, evidence_id, browser, count=1) -> None:
    _commit(env, evidence_id, insert_jump_list_entries, [
        {
            "appid": f"{browser}-appid",
            "browser": browser,
            "jumplist_path": f"C:/Users/u/{browser}-{i}.automaticDestinations-ms",
            "url": f"https://{browser.lower()}.example/{i}",
            "pin_status": "recent",
            "lnk_access_time": f"2024-01-0{i + 1}T00:00:00+00:00",
            "run_id": "run-68",
            "source_path": f"C:/Users/u/{browser}-{i}",
        }
        for i in range(count)
    ])


def add_windows_users(env, evidence_id, *usernames) -> None:
    _commit(env, evidence_id, insert_windows_users, [
        {"sid": f"S-1-5-21-1-{1000 + i}", "rid": 1000 + i, "username": name, "run_id": "run-68"}
        for i, name in enumerate(usernames)
    ])


def delete_rows(env, evidence_id, table, where, params) -> None:
    conn = env.writer.get_evidence_conn(evidence_id, env.labels[evidence_id])
    with conn:
        conn.execute(f"DELETE FROM {table} WHERE {where}", params)


def build_os_tab(env, evidence_id, defer_load) -> OSArtifactsTab:
    """Build OS Artifacts + embedded DPAPI like ``MainWindow._build_evidence_tab``."""
    label = env.labels[evidence_id]
    tab = OSArtifactsTab()
    tab.set_case_data(env.data, defer_load=defer_load)
    tab.set_evidence(evidence_id, defer_load=defer_load)
    tab.set_db_manager(env.data._db_manager, label)
    dpapi = DPAPITab(parent=tab)
    dpapi.set_db_manager(env.data._db_manager)
    dpapi.set_case_data(env.data, defer_load=True)
    dpapi.set_evidence(evidence_id, label, defer_load=True)
    tab.add_subtab(dpapi, "DPAPI")
    return tab


def combo_values(combo) -> List[str]:
    return [combo.itemData(i) for i in range(combo.count())]


class Window:
    """Main tabs with one evidence QTabWidget each, driven by MainWindow's handlers."""

    def __init__(self, qtbot, env, defer_load) -> None:
        self.main_tabs = QTabWidget()
        qtbot.addWidget(self.main_tabs)
        self.fake = SimpleNamespace(
            main_tabs=self.main_tabs,
            logger=logging.getLogger("test_os_artifacts_refresh"),
            _refresh_overview_for_evidence=lambda tabs: None,
        )
        self.evidence_tabs: Dict[int, QTabWidget] = {}
        self.os_tabs: Dict[int, OSArtifactsTab] = {}
        for evidence_id in env.ids:
            tabs = QTabWidget()
            tabs.addTab(QWidget(), "Overview")
            os_tab = build_os_tab(env, evidence_id, defer_load)
            tabs.addTab(os_tab, "OS Artifacts")
            tabs.currentChanged.connect(
                lambda idx, t=tabs: MainWindow._on_evidence_subtab_changed(self.fake, t, idx)
            )
            self.main_tabs.addTab(tabs, f"Evidence {evidence_id}")
            self.evidence_tabs[evidence_id] = tabs
            self.os_tabs[evidence_id] = os_tab
        self.main_tabs.show()
        qtbot.waitExposed(self.main_tabs)

    def show_os_artifacts(self, evidence_id) -> OSArtifactsTab:
        tabs = self.evidence_tabs[evidence_id]
        self.main_tabs.setCurrentWidget(tabs)
        tabs.setCurrentWidget(self.os_tabs[evidence_id])
        return self.os_tabs[evidence_id]

    def show_evidence(self, evidence_id) -> None:
        self.main_tabs.setCurrentWidget(self.evidence_tabs[evidence_id])

    def data_changed(self, evidence_id) -> None:
        """What ``MainWindow._on_data_changed`` does for the sub-tabs."""
        MainWindow._refresh_evidence_subtabs(self.fake, self.evidence_tabs[evidence_id])


def wait_loaded(qtbot, tab) -> None:
    qtbot.waitUntil(lambda: tab._data_loaded, timeout=TIMEOUT_MS)


# ---------------------------------------------------------------------------
# Confirmed reproduction: returning to an evidence with OS Artifacts selected
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("defer_load", [True, False])
def test_hidden_evidence_shows_new_rows_and_filters_on_return(qtbot, env, loads, defer_load):
    ev1, ev2 = env.ids
    window = Window(qtbot, env, defer_load)
    tab = window.show_os_artifacts(ev1)
    wait_loaded(qtbot, tab)
    assert tab.model.rowCount() == 0
    assert combo_values(tab.type_combo) == [""]
    # Immediate loading reads every evidence at build time, deferred only ev1
    assert loads["os_load"] == ([ev1] if defer_load else [ev1, ev2])
    loads.clear()

    window.show_evidence(ev2)
    add_indicators(env, ev1, "deep_freeze", "Frozen")
    add_jump_list_entries(env, ev1, "Chrome")
    window.data_changed(ev1)
    qtbot.wait(50)

    # Hidden: nothing is read until the evidence is shown again
    assert tab._data_stale
    assert tab.model.rowCount() == 0
    assert loads["os_reload"] == []

    window.show_evidence(ev1)
    qtbot.waitUntil(lambda: tab.model.rowCount() == 1, timeout=TIMEOUT_MS)
    assert combo_values(tab.type_combo) == ["", "deep_freeze"]
    assert combo_values(tab.jl_browser_combo) == ["", "Chrome"]
    assert tab.jl_model.rowCount() == 1
    assert tab.summary_label.text() == "Indicators: 1"
    assert tab.jl_summary_label.text().startswith("Jump List Entries: 1")
    assert not tab._data_stale
    qtbot.wait(50)
    assert loads["os_reload"] == [ev1]
    # The other evidence's OS Artifacts were not touched by the update
    assert ev2 not in loads["os_load"] + loads["os_reload"]


def test_subtab_switch_back_refreshes_once(qtbot, env, loads):
    ev1, _ev2 = env.ids
    window = Window(qtbot, env, defer_load=True)
    tab = window.show_os_artifacts(ev1)
    wait_loaded(qtbot, tab)
    tabs = window.evidence_tabs[ev1]

    tabs.setCurrentIndex(0)
    add_indicators(env, ev1, "kiosk", "Kiosk")
    window.data_changed(ev1)
    assert tab._data_stale

    # showEvent and MainWindow's sub-tab fallback both see the stale tab
    tabs.setCurrentWidget(tab)
    qtbot.waitUntil(lambda: tab.model.rowCount() == 1, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    assert loads["os_reload"] == [ev1]
    assert combo_values(tab.type_combo) == ["", "kiosk"]


# ---------------------------------------------------------------------------
# Visible, first visit, repeated ingestion
# ---------------------------------------------------------------------------


def test_visible_tab_refreshes_every_subtab_on_repeated_ingestion(qtbot, env, loads):
    ev1, _ev2 = env.ids
    window = Window(qtbot, env, defer_load=True)
    tab = window.show_os_artifacts(ev1)
    wait_loaded(qtbot, tab)

    add_indicators(env, ev1, "system:installed_software", "App A")
    add_indicators(env, ev1, "execution:user_assist", "C:/a.exe")
    add_indicators(env, ev1, "user_activity:recent_docs", "doc1")
    window.data_changed(ev1)  # visible: refreshed now
    assert tab.sw_model.rowCount() == 1
    assert tab.ae_model.rowCount() == 1
    assert tab.ua_model.rowCount() == 1
    assert tab.model.rowCount() == 3
    assert tab.sw_summary_label.text().startswith("Installed Software: 1")
    assert tab.ae_summary_label.text().startswith("Application Execution Entries: 1")
    assert tab.ua_summary_label.text().startswith("User Activity Entries: 1")

    add_indicators(env, ev1, "system:installed_software", "App B")
    add_jump_list_entries(env, ev1, "Edge", count=2)
    window.data_changed(ev1)
    assert tab.sw_model.rowCount() == 2
    assert tab.jl_model.rowCount() == 2
    assert tab.model.rowCount() == 4
    assert combo_values(tab.jl_browser_combo) == ["", "Edge"]
    assert loads["os_reload"] == [ev1, ev1]
    assert loads["os_load"] == [ev1]


def test_visible_tab_refresh_before_scheduled_first_load_loads_once(qtbot, env, loads):
    ev1, _ev2 = env.ids
    window = Window(qtbot, env, defer_load=True)
    tab = window.show_os_artifacts(ev1)
    add_indicators(env, ev1, "deep_freeze", "Frozen")
    window.data_changed(ev1)  # before the 10 ms deferred load ran
    assert tab._data_loaded
    assert tab.model.rowCount() == 1
    qtbot.wait(50)
    assert loads["os_load"] == [ev1]
    assert loads["os_reload"] == []


@pytest.mark.parametrize("defer_load", [True, False])
def test_first_visit_after_ingestion_loads_once(qtbot, env, loads, defer_load):
    ev1, ev2 = env.ids
    window = Window(qtbot, env, defer_load)
    window.show_evidence(ev2)
    loads.clear()

    add_indicators(env, ev1, "deep_freeze", "Frozen")
    add_windows_users(env, ev1, "alice")
    window.data_changed(ev1)
    tab = window.os_tabs[ev1]
    assert tab._data_stale

    tab = window.show_os_artifacts(ev1)
    qtbot.waitUntil(lambda: tab.model.rowCount() == 1, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    if defer_load:
        # Deferred: the first load reads fresh rows, the stale mark is consumed
        assert loads["os_load"] == [ev1]
        assert loads["os_reload"] == []
    else:
        # Loaded at build time (before the rows): one refresh on show
        assert loads["os_load"] == []
        assert loads["os_reload"] == [ev1]
    assert not tab._data_stale
    assert loads["dpapi_load"] == []  # DPAPI subtab not shown yet


def test_refresh_of_hidden_never_loaded_tab_stays_lazy(qtbot, env, loads):
    ev1, _ev2 = env.ids
    tab = build_os_tab(env, ev1, defer_load=True)
    qtbot.addWidget(tab)
    tab.refresh()
    tab.mark_stale()
    assert loads["os_load"] == []
    assert tab._load_pending and not tab._data_loaded


def test_immediate_load_marks_tab_loaded(qtbot, env, loads):
    ev1, _ev2 = env.ids
    tab = build_os_tab(env, ev1, defer_load=False)
    qtbot.addWidget(tab)
    assert tab._data_loaded
    assert not tab._load_pending
    assert loads["os_load"] == [ev1]


# ---------------------------------------------------------------------------
# Nested subtabs and embedded DPAPI
# ---------------------------------------------------------------------------


def test_nested_subtabs_and_dpapi_follow_updates(qtbot, env, loads):
    ev1, _ev2 = env.ids
    window = Window(qtbot, env, defer_load=True)
    tab = window.show_os_artifacts(ev1)
    wait_loaded(qtbot, tab)
    dpapi = tab._dpapi_tab

    add_jump_list_entries(env, ev1, "Chrome")
    add_windows_users(env, ev1, "alice")
    window.data_changed(ev1)  # Registry nested subtab shown
    assert loads["dpapi_load"] == []
    assert dpapi._data_stale

    # Nested navigation shows the refreshed rows without loading again
    tab.tabs.setCurrentWidget(tab.jump_lists_widget)
    assert tab.jl_model.rowCount() == 1

    # DPAPI loads on first visit with the committed users
    tab.tabs.setCurrentWidget(dpapi)
    qtbot.waitUntil(lambda: dpapi._users_model.rowCount() == 1, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    assert loads["dpapi_load"] == [ev1]
    assert dpapi._data_loaded and not dpapi._data_stale

    # Visible DPAPI refreshes immediately
    add_windows_users(env, ev1, "bob")
    window.data_changed(ev1)
    assert dpapi._users_model.rowCount() == 2

    # Hidden DPAPI is only marked stale, then refreshes on its next show
    tab.tabs.setCurrentIndex(0)
    add_windows_users(env, ev1, "carol")
    window.data_changed(ev1)
    assert dpapi._users_model.rowCount() == 2
    assert dpapi._data_stale
    tab.tabs.setCurrentWidget(dpapi)
    qtbot.waitUntil(lambda: dpapi._users_model.rowCount() == 3, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    assert loads["dpapi_load"] == [ev1, ev1, ev1]
    assert not dpapi._data_stale


@pytest.mark.parametrize("away", ["evidence", "subtab"])
def test_dpapi_shown_on_return_loads_once(qtbot, env, loads, away):
    ev1, ev2 = env.ids
    window = Window(qtbot, env, defer_load=True)
    tab = window.show_os_artifacts(ev1)
    wait_loaded(qtbot, tab)
    dpapi = tab._dpapi_tab
    tab.tabs.setCurrentWidget(dpapi)
    qtbot.waitUntil(lambda: dpapi._data_loaded, timeout=TIMEOUT_MS)
    loads.clear()

    if away == "evidence":
        window.show_evidence(ev2)
    else:
        window.evidence_tabs[ev1].setCurrentIndex(0)
    add_windows_users(env, ev1, "alice")
    window.data_changed(ev1)
    window.show_os_artifacts(ev1)
    qtbot.waitUntil(lambda: dpapi._users_model.rowCount() == 1, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    assert loads["dpapi_load"] == [ev1]
    assert loads["os_reload"] == [ev1]


def test_failed_model_load_inside_subtab_is_retried(qtbot, env, monkeypatch):
    from app.features.os_artifacts.models import JumpListsTableModel

    ev1, _ev2 = env.ids
    window = Window(qtbot, env, defer_load=True)
    tabs = window.evidence_tabs[ev1]
    add_jump_list_entries(env, ev1, "Chrome")

    def boom(self):
        raise RuntimeError("db locked")

    original = JumpListsTableModel._refresh_tags
    monkeypatch.setattr(JumpListsTableModel, "_refresh_tags", boom)
    tab = window.show_os_artifacts(ev1)
    qtbot.wait(50)
    assert tab.jl_model.rowCount() == 0  # the model swallowed the error
    assert not tab._data_loaded and tab._load_pending

    monkeypatch.setattr(JumpListsTableModel, "_refresh_tags", original)
    tabs.setCurrentIndex(0)
    tabs.setCurrentWidget(tab)
    qtbot.waitUntil(lambda: tab._data_loaded, timeout=TIMEOUT_MS)
    assert tab.jl_model.rowCount() == 1

    monkeypatch.setattr(JumpListsTableModel, "_refresh_tags", boom)
    add_jump_list_entries(env, ev1, "Edge")
    window.data_changed(ev1)
    assert tab._data_stale

    monkeypatch.setattr(JumpListsTableModel, "_refresh_tags", original)
    tabs.setCurrentIndex(0)
    tabs.setCurrentWidget(tab)
    qtbot.waitUntil(lambda: tab.jl_model.rowCount() == 2, timeout=TIMEOUT_MS)
    assert not tab._data_stale


def test_dpapi_load_failure_stays_pending_and_stale(qtbot, env, monkeypatch):
    ev1, _ev2 = env.ids
    window = Window(qtbot, env, defer_load=True)
    tab = window.show_os_artifacts(ev1)
    wait_loaded(qtbot, tab)
    dpapi = tab._dpapi_tab
    add_windows_users(env, ev1, "alice")

    import core.database.helpers.dpapi as dpapi_helpers

    original = dpapi_helpers.get_windows_users
    monkeypatch.setattr(dpapi_helpers, "get_windows_users", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db")))
    tab.tabs.setCurrentWidget(dpapi)
    qtbot.wait(50)
    assert not dpapi._data_loaded and dpapi._load_pending

    monkeypatch.setattr(dpapi_helpers, "get_windows_users", original)
    tab.tabs.setCurrentIndex(0)
    tab.tabs.setCurrentWidget(dpapi)
    qtbot.waitUntil(lambda: dpapi._data_loaded, timeout=TIMEOUT_MS)
    assert dpapi._users_model.rowCount() == 1

    monkeypatch.setattr(dpapi_helpers, "get_windows_users", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db")))
    dpapi.refresh()
    assert dpapi._data_stale


# ---------------------------------------------------------------------------
# Filter choices
# ---------------------------------------------------------------------------


def test_active_filters_stay_applied_after_refresh(qtbot, env):
    ev1, _ev2 = env.ids
    add_indicators(env, ev1, "deep_freeze", "Frozen")
    add_jump_list_entries(env, ev1, "Chrome")
    window = Window(qtbot, env, defer_load=True)
    tab = window.show_os_artifacts(ev1)
    wait_loaded(qtbot, tab)

    tab.type_combo.setCurrentIndex(tab.type_combo.findData("deep_freeze"))
    tab.jl_browser_combo.setCurrentIndex(tab.jl_browser_combo.findData("Chrome"))

    add_indicators(env, ev1, "kiosk", "Kiosk")
    add_indicators(env, ev1, "deep_freeze", "Frozen 2")
    add_jump_list_entries(env, ev1, "Edge")
    window.data_changed(ev1)

    assert combo_values(tab.type_combo) == ["", "deep_freeze", "kiosk"]
    assert tab.type_combo.currentData() == "deep_freeze"
    assert tab.model.rowCount() == 2
    assert {row["type"] for row in tab.model._rows} == {"deep_freeze"}
    assert combo_values(tab.jl_browser_combo) == ["", "Chrome", "Edge"]
    assert tab.jl_browser_combo.currentData() == "Chrome"
    assert tab.jl_model.rowCount() == 1


def test_vanished_filter_resets_widget_and_model(qtbot, env):
    ev1, _ev2 = env.ids
    add_indicators(env, ev1, "deep_freeze", "Frozen")
    add_jump_list_entries(env, ev1, "Chrome")
    window = Window(qtbot, env, defer_load=True)
    tab = window.show_os_artifacts(ev1)
    wait_loaded(qtbot, tab)
    tab.type_combo.setCurrentIndex(tab.type_combo.findData("deep_freeze"))
    tab.jl_browser_combo.setCurrentIndex(tab.jl_browser_combo.findData("Chrome"))

    # Re-ingestion replaced the rows
    delete_rows(env, ev1, "os_indicators", "type = ?", ("deep_freeze",))
    delete_rows(env, ev1, "jump_list_entries", "browser = ?", ("Chrome",))
    add_indicators(env, ev1, "kiosk", "Kiosk")
    add_jump_list_entries(env, ev1, "Edge", count=2)
    window.data_changed(ev1)

    assert tab.type_combo.currentData() == ""
    assert tab.filters.indicator_type == ""
    assert tab.model._filter_type is None
    assert tab.model.rowCount() == 1
    assert tab.jl_browser_combo.currentData() == ""
    assert tab.jl_model._browser_filter == ""
    assert tab.jl_model.rowCount() == 2
    assert tab.jl_summary_label.text().startswith("Jump List Entries: 2")


# ---------------------------------------------------------------------------
# Failures keep the invalidation
# ---------------------------------------------------------------------------


def test_failed_load_and_refresh_keep_state_for_retry(qtbot, env, monkeypatch):
    ev1, _ev2 = env.ids
    window = Window(qtbot, env, defer_load=True)
    tabs = window.evidence_tabs[ev1]

    def boom(*args, **kwargs):
        raise RuntimeError("db unavailable")

    original = CaseDataAccess.list_indicator_types
    monkeypatch.setattr(CaseDataAccess, "list_indicator_types", boom)
    tab = window.show_os_artifacts(ev1)
    qtbot.wait(50)
    assert not tab._data_loaded and tab._load_pending

    monkeypatch.setattr(CaseDataAccess, "list_indicator_types", original)
    add_indicators(env, ev1, "deep_freeze", "Frozen")
    tabs.setCurrentIndex(0)
    tabs.setCurrentWidget(tab)
    qtbot.waitUntil(lambda: tab._data_loaded, timeout=TIMEOUT_MS)
    assert tab.model.rowCount() == 1

    monkeypatch.setattr(CaseDataAccess, "list_indicator_types", boom)
    add_indicators(env, ev1, "kiosk", "Kiosk")
    window.data_changed(ev1)
    assert tab._data_stale

    monkeypatch.setattr(CaseDataAccess, "list_indicator_types", original)
    tabs.setCurrentIndex(0)
    tabs.setCurrentWidget(tab)
    qtbot.waitUntil(lambda: tab.model.rowCount() == 2, timeout=TIMEOUT_MS)
    assert not tab._data_stale


# ---------------------------------------------------------------------------
# Integration: real MainWindow, committed rows in a temporary case workspace
# ---------------------------------------------------------------------------


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


@pytest.fixture
def main_window(qtbot, tmp_path, monkeypatch):
    from app.features.urls.tab import UrlsTab
    import core.tool_discovery as tool_discovery

    monkeypatch.setattr(UrlsTab, "_start_search_index_build", lambda self: None)
    monkeypatch.setattr(tool_discovery, "get_tool_version", lambda cmd: None)

    def unexpected_box(*args, **kwargs):
        raise AssertionError(f"unexpected QMessageBox: {args[1:3]}")

    for name in ("critical", "warning", "information", "question"):
        monkeypatch.setattr(QMessageBox, name, staticmethod(unexpected_box))

    window = MainWindow(_make_base_dir(tmp_path))
    window.show()
    yield window
    window.close()
    QThreadPool.globalInstance().waitForDone(10000)
    QCoreApplication.processEvents()
    window.deleteLater()
    QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)


def test_main_window_shows_ingested_rows_without_restart(qtbot, env, loads, main_window):
    ev1, ev2 = env.ids
    window = main_window
    window._open_case(env.case.case_folder, env.case.case_db_path)
    qtbot.waitUntil(
        lambda: window.case_data is not None and window.main_tabs.count() == 3, timeout=TIMEOUT_MS
    )
    first, second = window.main_tabs.widget(1), window.main_tabs.widget(2)
    os_first = first.property("os_tab")

    # Visit OS Artifacts before importing new results
    window.main_tabs.setCurrentWidget(first)
    first.setCurrentWidget(os_first)
    wait_loaded(qtbot, os_first)
    assert os_first.model.rowCount() == 0

    # Ingestion for the first evidence finishes while the second is shown
    window.main_tabs.setCurrentWidget(second)
    add_indicators(env, ev1, "deep_freeze", "Frozen")
    add_jump_list_entries(env, ev1, "Chrome")
    window._on_data_changed(first)
    qtbot.wait(50)
    assert os_first.model.rowCount() == 0  # hidden: still lazy
    assert ev2 not in loads["os_load"] + loads["os_reload"]

    window.main_tabs.setCurrentWidget(first)
    qtbot.waitUntil(lambda: os_first.model.rowCount() == 1, timeout=TIMEOUT_MS)
    assert combo_values(os_first.type_combo) == ["", "deep_freeze"]
    assert combo_values(os_first.jl_browser_combo) == ["", "Chrome"]

    # Repeated ingestion while OS Artifacts is visible
    add_indicators(env, ev1, "kiosk", "Kiosk")
    window._on_data_changed(first)
    assert os_first.model.rowCount() == 2
    assert combo_values(os_first.type_combo) == ["", "deep_freeze", "kiosk"]
    # Updates stayed scoped to the originating evidence
    assert ev2 not in loads["os_load"] + loads["os_reload"]
