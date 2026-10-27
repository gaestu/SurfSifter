"""MainWindow hooks for the URL search index build (issue #62).

Unbound MainWindow methods run against a minimal stand-in, so no full window
(case, evidences, menus) is needed.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, List

import pytest
from PySide6.QtWidgets import QDialog

import app.main as main_module
from app.main import MainWindow

pytestmark = pytest.mark.gui_offscreen


class FakeUrlsTab:
    def __init__(self, events: List[str], name: str) -> None:
        self.events = events
        self.name = name
        self.paused = False

    def pause_search_index_build(self, timeout_ms: int = 15000) -> bool:
        self.paused = True
        self.events.append(f"pause {self.name}")
        return True

    def resume_search_index_build(self) -> None:
        self.paused = False
        self.events.append(f"resume {self.name}")

    def shutdown(self) -> None:
        self.events.append(f"shutdown {self.name}")
        if self.name == "broken":
            raise RuntimeError("boom")


class FakeEvidenceTab:
    """Evidence QTabWidget stand-in: one sub-tab (the URLs tab), exposed as ``urls_tab`` property."""

    def __init__(self, urls_tab: Any) -> None:
        self.urls_tab = urls_tab

    def property(self, name: str) -> Any:
        return self.urls_tab if name == "urls_tab" else None

    def count(self) -> int:
        return 1

    def widget(self, index: int) -> Any:
        return self.urls_tab


class FakeTabs:
    """QTabWidget stand-in: index 0 is the Case tab, then evidence tabs with a ``urls_tab`` property."""

    def __init__(self, urls_tabs: List[Any]) -> None:
        self.pages = [SimpleNamespace(property=lambda name: None)] + [FakeEvidenceTab(tab) for tab in urls_tabs]

    def count(self) -> int:
        return len(self.pages)

    def widget(self, index: int):
        return self.pages[index]

    def removeTab(self, index: int) -> None:
        del self.pages[index]


def _window(urls_tabs: List[Any]) -> SimpleNamespace:
    window = SimpleNamespace(
        main_tabs=FakeTabs(urls_tabs),
        logger=SimpleNamespace(warning=lambda *a, **k: None),
        _evidence_tabs={},
        case_data=None,
        case_path="/case",
        statusBar=lambda: SimpleNamespace(showMessage=lambda *a, **k: None),
    )
    window._urls_tabs = lambda: MainWindow._urls_tabs(window)
    window._shutdown_evidence_tab = lambda tab: MainWindow._shutdown_evidence_tab(window, tab)
    window._shutdown_all_evidence_tabs = lambda: MainWindow._shutdown_all_evidence_tabs(window)
    return window


@pytest.mark.parametrize("dialog_raises", [False, True])
def test_export_pauses_url_index_builds_while_the_dialog_runs(monkeypatch, dialog_raises):
    events: List[str] = []
    tabs = [FakeUrlsTab(events, "a"), FakeUrlsTab(events, "b")]
    window = _window(tabs)
    monkeypatch.setattr(
        main_module, "wait_for_url_search_index_builds", lambda timeout_ms: events.append("wait") or True
    )
    window.case_data = SimpleNamespace(get_case_metadata=lambda: {"case_id": "C1"})

    class Dialog:
        def __init__(self, *args) -> None:
            pass

        def exec(self) -> int:
            events.append("export " + ",".join(str(tab.paused) for tab in tabs))
            if dialog_raises:
                raise RuntimeError("export failed")
            return QDialog.Accepted

    monkeypatch.setattr(main_module, "ExportDialog", Dialog)
    if dialog_raises:
        with pytest.raises(RuntimeError):
            MainWindow._export_case(window)
    else:
        MainWindow._export_case(window)
    assert events == ["pause a", "pause b", "wait", "export True,True", "resume a", "resume b"]


def test_export_is_not_started_while_a_build_is_still_finishing(monkeypatch):
    """E.g. a build of an already closed evidence tab that has not stopped yet."""
    events: List[str] = []
    window = _window([FakeUrlsTab(events, "a")])
    window.case_data = SimpleNamespace(get_case_metadata=lambda: {"case_id": "C1"})
    monkeypatch.setattr(main_module, "wait_for_url_search_index_builds", lambda timeout_ms: False)
    monkeypatch.setattr(main_module.QMessageBox, "warning", lambda *a, **k: events.append("warning"))

    class Dialog:
        def __init__(self, *args) -> None:
            events.append("dialog")

    monkeypatch.setattr(main_module, "ExportDialog", Dialog)
    MainWindow._export_case(window)
    assert events == ["pause a", "warning", "resume a"]


@pytest.mark.parametrize("loader", ["_load_existing_evidences", "_load_existing_evidences_lazy"])
def test_opening_another_case_shuts_down_url_tabs(loader):
    # Every sub-tab of every evidence tab is shut down (issue #63); here
    # each evidence tab holds only its URLs tab.
    events: List[str] = []
    window = _window([FakeUrlsTab(events, "broken"), FakeUrlsTab(events, "b")])
    if loader == "_load_existing_evidences":
        MainWindow._load_existing_evidences(window)
    else:
        MainWindow._load_existing_evidences_lazy(window, [])
    assert events == ["shutdown broken", "shutdown b"]  # one failing tab does not stop the others
    assert window.main_tabs.count() == 1
