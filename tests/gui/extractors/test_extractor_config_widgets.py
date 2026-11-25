"""Configure must open for every registered extractor (issue #76)."""

from __future__ import annotations

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QCheckBox, QDialog, QMessageBox, QWidget

from app.common.dialogs.extraction import CaseWideExtractIngestDialog
from app.features.extraction import ExtractionTab
from extractors import ExtractorRegistry

pytestmark = pytest.mark.gui_offscreen

# Extractors whose Configure dialog failed with a TypeError before issue #76.
IE_LEGACY_PARTITION_EXTRACTORS = [
    "edge_legacy_container",
    "edge_reading_list",
    "ie_classic_history",
    "ie_dom_storage",
    "ie_favorites",
    "ie_inetcookies",
    "ie_tab_recovery",
    "ie_typed_urls",
    "ie_webcache",
]


@pytest.fixture
def opened_dialogs(monkeypatch):
    """Replace modal dialogs; record title and partition checkbox state."""
    opened = []

    def fake_exec(dialog):
        checkbox = dialog.findChild(QCheckBox, "scan_all_partitions")
        opened.append((dialog.windowTitle(), checkbox.isChecked() if checkbox else None))
        return QDialog.Rejected

    def fail_information(*args, **kwargs):
        pytest.fail(f"Unexpected message box: {args[1:]}")

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    monkeypatch.setattr(QMessageBox, "information", fail_information)
    return opened


def test_every_registered_extractor_builds_its_config_widget(qtbot):
    # Own registry: some extractors keep their widgets, which die with parent.
    extractor_registry = ExtractorRegistry()
    parent = QWidget()
    qtbot.addWidget(parent)
    names = extractor_registry.list_names()
    assert set(IE_LEGACY_PARTITION_EXTRACTORS) <= set(names)

    failures = []
    for name in names:
        try:
            widget = extractor_registry.get(name).get_config_widget(parent)
        except Exception as exc:
            failures.append(f"{name}: {type(exc).__name__}: {exc}")
            continue
        if widget is not None and not isinstance(widget, QWidget):
            failures.append(f"{name}: returned {type(widget).__name__}")

    assert not failures, "\n".join(failures)


@pytest.mark.parametrize("name", IE_LEGACY_PARTITION_EXTRACTORS)
def test_configure_opens_from_extractors_tab(qtbot, extractor_registry, opened_dialogs, name):
    tab = ExtractionTab(1)
    qtbot.addWidget(tab)
    extractor = extractor_registry.get(name)

    tab._show_config_dialog(extractor)

    assert opened_dialogs == [(f"{extractor.metadata.display_name} - Configuration", True)]


@pytest.mark.parametrize("name", IE_LEGACY_PARTITION_EXTRACTORS)
def test_configure_opens_from_case_wide_dialog(qtbot, extractor_registry, opened_dialogs, name):
    dialog = CaseWideExtractIngestDialog([{"id": 1, "label": "ev"}])
    qtbot.addWidget(dialog)
    items = [dialog.extractor_list.item(i) for i in range(dialog.extractor_list.count())]
    item = next(i for i in items if i.data(Qt.UserRole) == name)
    dialog.extractor_list.setCurrentItem(item)

    dialog._configure_selected_extractor()

    display_name = extractor_registry.get(name).metadata.display_name
    assert opened_dialogs == [(f"{display_name} - Configuration", True)]
