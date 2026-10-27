"""Browser → History stays consistent with committed history (issue #69).

The Browser Inventory tab is built the way ``MainWindow._build_evidence_tab``
builds it, on a small real case. History rows are committed through
``core.database.helpers`` on a separate connection, as ingestion does, and the
refresh is routed through ``MainWindow``'s data-change and sub-tab handlers.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List

import pytest
from PySide6.QtWidgets import QComboBox, QTabWidget, QWidget

from app.data import CaseDataAccess, discard_case_stats_cache
from app.features.browser_inventory import BrowserInventoryTab
from app.features.browser_inventory._base import BaseArtifactSubtab
from app.features.browser_inventory.cookies.model import CookiesTableModel
from app.features.browser_inventory.history.model import BrowserHistoryTableModel
from app.main import MainWindow
from core.database import DatabaseManager, insert_browser_history_rows, insert_cookies
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = pytest.mark.gui_offscreen

TIMEOUT_MS = 10000

# Chromium core transition types used below
LINK = 0
TYPED = 1


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
    """Records the evidence of every model load, per model class."""
    counts: Dict[str, List[int]] = defaultdict(list)

    def watch(cls, label):
        original = cls.load

        def wrapper(obj, *args, **kwargs):
            counts[label].append(obj.evidence_id)
            return original(obj, *args, **kwargs)

        monkeypatch.setattr(cls, "load", wrapper)

    watch(BrowserHistoryTableModel, "history")
    watch(CookiesTableModel, "cookies")
    return counts


def add_history(env, evidence_id, browser, profile, *urls, transition=LINK) -> None:
    conn = env.writer.get_evidence_conn(evidence_id, env.labels[evidence_id])
    with conn:
        insert_browser_history_rows(conn, evidence_id, [
            {
                "browser": browser,
                "profile": profile,
                "url": url,
                "title": url,
                "ts_utc": f"2024-01-01T00:00:{i:02d}+00:00",
                "transition_type": transition,
                "source_path": f"C:/Users/u/{browser}/{profile}/History",
                "discovered_by": "test",
                "run_id": "run-69",
            }
            for i, url in enumerate(urls)
        ])


def delete_history(env, evidence_id, browser) -> None:
    conn = env.writer.get_evidence_conn(evidence_id, env.labels[evidence_id])
    with conn:
        conn.execute("DELETE FROM browser_history WHERE browser = ?", (browser,))


def combo_values(combo) -> List[str]:
    return [combo.itemData(i) for i in range(combo.count())]


def urls(history) -> List[str]:
    model = history._model
    return sorted(model._rows[i]["url"] for i in range(model.rowCount()))


class Window:
    """Main tabs with one evidence QTabWidget each, driven by MainWindow's handlers."""

    def __init__(self, qtbot, env, defer_load=True) -> None:
        self.main_tabs = QTabWidget()
        qtbot.addWidget(self.main_tabs)
        self.fake = SimpleNamespace(
            main_tabs=self.main_tabs,
            logger=logging.getLogger("test_browser_history_refresh"),
            _refresh_overview_for_evidence=lambda tabs: None,
        )
        self.evidence_tabs: Dict[int, QTabWidget] = {}
        self.browser_tabs: Dict[int, BrowserInventoryTab] = {}
        for evidence_id in env.ids:
            tabs = QTabWidget()
            tabs.addTab(QWidget(), "Overview")
            tabs.addTab(QWidget(), "Extractors")
            browser_tab = BrowserInventoryTab(
                str(env.case.case_folder),
                evidence_id,
                env.case.case_db_path,
                case_data=env.data,
                defer_load=defer_load,
            )
            # History is the remembered subtab: it loads on the first show
            browser_tab.subtabs.setCurrentWidget(browser_tab._history)
            tabs.addTab(browser_tab, "Browser Inventory")
            tabs.currentChanged.connect(
                lambda idx, t=tabs: MainWindow._on_evidence_subtab_changed(self.fake, t, idx)
            )
            self.main_tabs.addTab(tabs, f"Evidence {evidence_id}")
            self.evidence_tabs[evidence_id] = tabs
            self.browser_tabs[evidence_id] = browser_tab
        self.main_tabs.show()
        qtbot.waitExposed(self.main_tabs)

    def show_browser(self, evidence_id) -> BrowserInventoryTab:
        tabs = self.evidence_tabs[evidence_id]
        self.main_tabs.setCurrentWidget(tabs)
        tabs.setCurrentWidget(self.browser_tabs[evidence_id])
        return self.browser_tabs[evidence_id]

    def show_extractors(self, evidence_id) -> None:
        """Where the user is while ingesting."""
        tabs = self.evidence_tabs[evidence_id]
        self.main_tabs.setCurrentWidget(tabs)
        tabs.setCurrentIndex(1)

    def show_evidence(self, evidence_id) -> None:
        self.main_tabs.setCurrentWidget(self.evidence_tabs[evidence_id])

    def data_changed(self, evidence_id) -> None:
        """What ``MainWindow._on_data_changed`` does for the sub-tabs."""
        MainWindow._refresh_evidence_subtabs(self.fake, self.evidence_tabs[evidence_id])


def open_history(qtbot, window, evidence_id):
    history = window.show_browser(evidence_id)._history
    qtbot.waitUntil(lambda: history.is_loaded, timeout=TIMEOUT_MS)
    return history


def select(combo: QComboBox, value: str) -> None:
    index = combo.findData(value)
    assert index >= 0, value
    combo.setCurrentIndex(index)


# ---------------------------------------------------------------------------
# First and repeated ingestion
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("defer_load", [True, False])
def test_first_ingestion_after_empty_history_shows_rows_and_options(qtbot, env, loads, defer_load):
    ev1, _ev2 = env.ids
    window = Window(qtbot, env, defer_load)
    history = open_history(qtbot, window, ev1)
    assert history._model.rowCount() == 0
    assert combo_values(history.browser_filter) == [""]
    assert combo_values(history.profile_filter) == [""]
    loads.clear()

    window.show_extractors(ev1)
    add_history(env, ev1, "chrome", "Default", "https://a.example/")
    window.data_changed(ev1)
    qtbot.wait(50)
    # Hidden: nothing is read until the Browser tab is shown again
    assert history._model.rowCount() == 0
    assert loads["history"] == []

    window.show_browser(ev1)
    qtbot.waitUntil(lambda: history._model.rowCount() == 1, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    assert combo_values(history.browser_filter) == ["", "chrome"]
    assert combo_values(history.profile_filter) == ["", "Default"]
    assert history.status_label.text() == "1 history records"
    assert loads["history"] == [ev1]


def test_visible_history_refreshes_without_duplicates(qtbot, env, loads):
    ev1, _ev2 = env.ids
    window = Window(qtbot, env)
    history = open_history(qtbot, window, ev1)
    loads.clear()

    add_history(env, ev1, "chrome", "Default", "https://a.example/")
    window.data_changed(ev1)  # visible: refreshed now
    assert history._model.rowCount() == 1

    add_history(env, ev1, "firefox", "abc.default", "https://b.example/")
    window.data_changed(ev1)
    window.data_changed(ev1)  # an unchanged refresh adds nothing
    assert urls(history) == ["https://a.example/", "https://b.example/"]
    assert combo_values(history.browser_filter) == ["", "chrome", "firefox"]
    assert combo_values(history.profile_filter) == ["", "Default", "abc.default"]
    assert history.status_label.text() == "2 history records"
    assert loads["history"] == [ev1, ev1, ev1]


# ---------------------------------------------------------------------------
# Filter state across refresh
# ---------------------------------------------------------------------------


def test_refresh_keeps_and_applies_displayed_filters(qtbot, env):
    ev1, _ev2 = env.ids
    add_history(env, ev1, "chrome", "Default", "https://first.example/typed", transition=TYPED)
    add_history(env, ev1, "chrome", "Default", "https://first.example/link")
    add_history(env, ev1, "firefox", "abc.default", "https://first.example/fx", transition=TYPED)
    window = Window(qtbot, env)
    history = open_history(qtbot, window, ev1)

    select(history.browser_filter, "chrome")
    select(history.profile_filter, "Default")
    select(history.type_filter, "typed")
    history.url_filter.setText("first")
    history._on_apply_clicked()
    assert urls(history) == ["https://first.example/typed"]

    add_history(env, ev1, "chrome", "Default", "https://first.example/typed2", transition=TYPED)
    add_history(env, ev1, "chrome", "Profile 1", "https://first.example/p1", transition=TYPED)
    add_history(env, ev1, "chrome", "Default", "https://other.example/", transition=TYPED)
    window.data_changed(ev1)

    assert history.browser_filter.currentData() == "chrome"
    assert history.profile_filter.currentData() == "Default"
    assert history.type_filter.currentData() == "typed"
    assert history.url_filter.text() == "first"
    assert combo_values(history.profile_filter) == ["", "Default", "Profile 1", "abc.default"]
    assert urls(history) == ["https://first.example/typed", "https://first.example/typed2"]
    assert history.status_label.text() == "2 history records"


def test_refresh_applies_pending_url_edit(qtbot, env):
    ev1, _ev2 = env.ids
    add_history(env, ev1, "chrome", "Default", "https://first.example/", "https://second.example/")
    window = Window(qtbot, env)
    history = open_history(qtbot, window, ev1)

    history.url_filter.setText("second")  # typed, Apply not clicked
    add_history(env, ev1, "chrome", "Default", "https://second.example/more")
    window.data_changed(ev1)
    assert urls(history) == ["https://second.example/", "https://second.example/more"]
    assert history.status_label.text() == "2 history records"


@pytest.mark.parametrize("vanished", ["browser", "profile"])
def test_vanished_selection_falls_back_to_all(qtbot, env, vanished):
    ev1, _ev2 = env.ids
    add_history(env, ev1, "chrome", "Default", "https://a.example/")
    add_history(env, ev1, "firefox", "abc.default", "https://b.example/")
    window = Window(qtbot, env)
    history = open_history(qtbot, window, ev1)
    combo = history.browser_filter if vanished == "browser" else history.profile_filter
    select(combo, "firefox" if vanished == "browser" else "abc.default")
    history._on_apply_clicked()
    assert urls(history) == ["https://b.example/"]

    # Re-ingestion replaced Firefox's rows with Chrome's
    delete_history(env, ev1, "firefox")
    add_history(env, ev1, "chrome", "Default", "https://c.example/")
    window.data_changed(ev1)

    assert combo.currentIndex() == 0
    assert combo.currentText() == "All"
    assert combo_values(history.browser_filter) == ["", "chrome"]
    assert combo_values(history.profile_filter) == ["", "Default"]
    assert urls(history) == ["https://a.example/", "https://c.example/"]
    assert history.status_label.text() == "2 history records"


def test_option_rebuild_emits_no_change_signals(qtbot):
    combo = QComboBox()
    qtbot.addWidget(combo)
    combo.addItem("All", "")
    combo.addItem("Chrome", "chrome")
    combo.setCurrentIndex(1)
    changes: List[int] = []
    combo.currentIndexChanged.connect(changes.append)

    BaseArtifactSubtab._set_combo_options(combo, [("Edge", "edge"), ("Chrome", "chrome")])
    assert combo_values(combo) == ["", "edge", "chrome"]
    assert combo.currentData() == "chrome"
    BaseArtifactSubtab._set_combo_options(combo, [("Edge", "edge")])
    assert combo.currentIndex() == 0
    assert changes == []


# ---------------------------------------------------------------------------
# Lazy loading: other Browser subtabs and other evidences
# ---------------------------------------------------------------------------


def test_return_through_other_browser_subtab_reloads_history_once(qtbot, env, loads):
    ev1, _ev2 = env.ids
    window = Window(qtbot, env)
    history = open_history(qtbot, window, ev1)
    browser_tab = window.browser_tabs[ev1]
    browser_tab.subtabs.setCurrentWidget(browser_tab._inventory)
    loads.clear()

    add_history(env, ev1, "edge", "Default", "https://a.example/")
    window.data_changed(ev1)  # Browser tab visible, Inventory selected
    assert not history.is_loaded
    assert loads["history"] == []

    browser_tab.subtabs.setCurrentWidget(history)
    assert history._model.rowCount() == 1
    assert combo_values(history.browser_filter) == ["", "edge"]
    qtbot.wait(50)
    assert loads["history"] == [ev1]


def test_ingestion_into_hidden_evidence_stays_separate(qtbot, env, loads):
    ev1, ev2 = env.ids
    add_history(env, ev2, "firefox", "abc.default", "https://ev2.example/")
    window = Window(qtbot, env)
    history1 = open_history(qtbot, window, ev1)
    history2 = open_history(qtbot, window, ev2)
    loads.clear()

    add_history(env, ev1, "chrome", "Default", "https://ev1.example/")
    window.data_changed(ev1)  # ev2 is shown
    qtbot.wait(50)
    assert loads["history"] == []
    assert history1._model.rowCount() == 0

    window.show_evidence(ev1)
    qtbot.waitUntil(lambda: history1._model.rowCount() == 1, timeout=TIMEOUT_MS)
    qtbot.wait(50)
    assert urls(history1) == ["https://ev1.example/"]
    assert combo_values(history1.browser_filter) == ["", "chrome"]
    assert urls(history2) == ["https://ev2.example/"]
    assert combo_values(history2.browser_filter) == ["", "firefox"]
    assert loads["history"] == [ev1]


# ---------------------------------------------------------------------------
# Sibling subtabs keep the base reload contract
# ---------------------------------------------------------------------------


def test_sibling_subtab_reload_keeps_base_contract(qtbot, env, loads):
    ev1, _ev2 = env.ids
    conn = env.writer.get_evidence_conn(ev1, env.labels[ev1])

    def add_cookie(name):
        with conn:
            insert_cookies(conn, ev1, [
                {"browser": "chrome", "profile": "Default", "name": name, "domain": "a.example", "run_id": "run-69"}
            ])

    add_cookie("first")
    window = Window(qtbot, env)
    open_history(qtbot, window, ev1)
    browser_tab = window.browser_tabs[ev1]
    cookies = browser_tab._cookies
    browser_tab.subtabs.setCurrentWidget(cookies)
    assert cookies._model.rowCount() == 1
    assert combo_values(cookies.browser_filter) == ["", "chrome"]

    add_cookie("second")
    window.data_changed(ev1)
    window.data_changed(ev1)
    assert cookies._model.rowCount() == 2
    assert cookies.status_label.text() == "2 cookies"
    # Options are populated on the first load only: no duplicates
    assert combo_values(cookies.browser_filter) == ["", "chrome"]
    assert loads["cookies"] == [ev1, ev1, ev1]
