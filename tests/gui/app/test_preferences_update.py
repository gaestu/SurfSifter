"""Saving Preferences while evidences are open (issue #75).

Opens a small synthetic case in a real ``MainWindow`` (offscreen) with a stubbed
``PreferencesDialog`` and checks that the new thumbnail size reaches the Images
tab of every open evidence, wherever that tab is in the sub-tab order.
"""
from __future__ import annotations

import copy
import logging
import shutil
from pathlib import Path
from typing import Any, List

import pytest
from PySide6.QtCore import QCoreApplication, QEvent, QSize, QThreadPool
from PySide6.QtWidgets import QDialog, QMessageBox

import app.main as main_module
from app.config.settings import AppSettings
from app.features.images import ImagesTab
from app.features.urls.tab import UrlsTab
from tests.fixtures.url_fixtures import build_url_fixture_case

pytestmark = pytest.mark.gui_offscreen

TIMEOUT_MS = 15000
CODE_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def window(qtbot, tmp_path, monkeypatch):
    """A shown MainWindow without a case, using the shipped config."""
    import core.tool_discovery as tool_discovery

    # No search index build and no per-extractor-card tool version probes.
    monkeypatch.setattr(UrlsTab, "_start_search_index_build", lambda self: None)
    monkeypatch.setattr(tool_discovery, "get_tool_version", lambda cmd: None)

    def unexpected_box(*args, **kwargs):
        raise AssertionError(f"unexpected QMessageBox: {args[1:3]}")

    for name in ("critical", "warning", "information", "question"):
        monkeypatch.setattr(QMessageBox, name, staticmethod(unexpected_box))

    base_dir = tmp_path / "base_dir"
    (base_dir / "config").mkdir(parents=True)
    for name in ("config.yml", "settings.defaults.json"):
        source = CODE_ROOT / "config" / name
        if source.exists():
            shutil.copy2(source, base_dir / "config" / name)

    win = main_module.MainWindow(base_dir)
    win.show()
    yield win
    win.close()
    QThreadPool.globalInstance().waitForDone(10000)
    QCoreApplication.processEvents()
    win.deleteLater()
    QCoreApplication.sendPostedEvents(None, QEvent.DeferredDelete)


@pytest.fixture
def images_tabs(qtbot, window, tmp_path) -> List[ImagesTab]:
    """Open a case with two evidences; their Images tabs."""
    case = build_url_fixture_case(tmp_path / "case", rows_per_evidence=10, evidence_count=2)
    window._open_case(case.case_folder, case.case_db_path)
    qtbot.waitUntil(
        lambda: window.case_data is not None and window.main_tabs.count() == 3,
        timeout=TIMEOUT_MS,
    )
    tabs = [window.main_tabs.widget(i).findChild(ImagesTab) for i in (1, 2)]
    assert all(tabs) and tabs[0] is not tabs[1]
    return tabs


def _stub_dialog(monkeypatch, settings: AppSettings, result: int = QDialog.Accepted) -> None:
    class Dialog:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.result_settings = settings

        def exec(self) -> int:
            return result

    monkeypatch.setattr(main_module, "PreferencesDialog", Dialog)


def _changed_settings(window: Any) -> AppSettings:
    settings = copy.deepcopy(window.settings)
    settings.general.thumbnail_size = window.settings.general.thumbnail_size + 60
    return settings


def _assert_saved(window: Any, size: int, caplog: Any) -> None:
    assert window.settings.general.thumbnail_size == size
    assert AppSettings.load(window.settings_file).general.thumbnail_size == size
    assert caplog.messages.count("Preferences updated") == 1


def _assert_size(tabs: List[ImagesTab], size: int) -> None:
    for tab in tabs:
        assert tab.list_view.iconSize() == QSize(size, size)
        assert tab.model.thumb_size == size


def test_save_updates_images_tab_of_every_evidence(monkeypatch, window, images_tabs, caplog):
    caplog.set_level(logging.INFO, logger="surfsifter.app.gui")
    settings = _changed_settings(window)
    size = settings.general.thumbnail_size
    _assert_size(images_tabs, window.settings.general.thumbnail_size)
    _stub_dialog(monkeypatch, settings)

    window._open_preferences_dialog()

    _assert_size(images_tabs, size)
    _assert_saved(window, size, caplog)


def test_save_does_not_depend_on_the_sub_tab_order(monkeypatch, window, images_tabs, caplog):
    caplog.set_level(logging.INFO, logger="surfsifter.app.gui")
    # A different position per evidence: last and first.
    for index, position in ((1, window.main_tabs.widget(1).count() - 1), (2, 0)):
        evidence_tabs = window.main_tabs.widget(index)
        images_tab = images_tabs[index - 1]
        evidence_tabs.tabBar().moveTab(evidence_tabs.indexOf(images_tab), position)
        assert evidence_tabs.widget(position) is images_tab
    settings = _changed_settings(window)
    size = settings.general.thumbnail_size
    _stub_dialog(monkeypatch, settings)

    window._open_preferences_dialog()

    _assert_size(images_tabs, size)
    _assert_saved(window, size, caplog)


def test_cancel_changes_nothing(monkeypatch, window, images_tabs, caplog):
    caplog.set_level(logging.INFO, logger="surfsifter.app.gui")
    before = window.settings.general.thumbnail_size
    _stub_dialog(monkeypatch, _changed_settings(window), QDialog.Rejected)

    window._open_preferences_dialog()

    _assert_size(images_tabs, before)
    assert window.settings.general.thumbnail_size == before
    assert "Preferences updated" not in caplog.messages


def test_save_without_a_case(monkeypatch, window, caplog):
    caplog.set_level(logging.INFO, logger="surfsifter.app.gui")
    settings = _changed_settings(window)
    _stub_dialog(monkeypatch, settings)

    window._open_preferences_dialog()

    _assert_saved(window, settings.general.thumbnail_size, caplog)
