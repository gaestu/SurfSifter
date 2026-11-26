"""Configure works for every registered extractor (issues #76, #78)."""

from __future__ import annotations

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QCheckBox, QDialog, QMessageBox, QWidget

from app.common.dialogs.extraction import CaseWideExtractIngestDialog
from app.features.extraction import ExtractionTab
from extractors import ExtractorRegistry

pytestmark = pytest.mark.gui_offscreen

# Extractors whose Configure dialog failed with a TypeError before issue #76.
# Their only option was "Scan all partitions", removed with issue #78.
IE_LEGACY_NO_OPTION_EXTRACTORS = [
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

# All extractors whose only option was "Scan all partitions".
NO_OPTION_EXTRACTORS = IE_LEGACY_NO_OPTION_EXTRACTORS + [
    "safari_bookmarks",
    "safari_cache",
    "safari_cookies",
    "safari_downloads",
    "safari_extensions",
    "safari_favicons",
    "safari_history",
    "safari_reading_list",
    "safari_sessions",
    "safari_storage",
    "safari_top_sites",
]


@pytest.fixture
def configure_outcomes(monkeypatch):
    """Replace modal dialogs and message boxes; record what Configure shows."""
    outcomes = []

    def fake_exec(dialog):
        outcomes.append(("dialog", dialog.windowTitle()))
        return QDialog.Rejected

    def fake_information(parent, title, text, *args, **kwargs):  # noqa: ARG001
        outcomes.append(("information", text))
        return QMessageBox.Ok

    monkeypatch.setattr(QDialog, "exec", fake_exec)
    monkeypatch.setattr(QMessageBox, "information", fake_information)
    return outcomes


def test_every_registered_extractor_builds_its_config_widget(qtbot):
    # Own registry: some extractors keep their widgets, which die with parent.
    extractor_registry = ExtractorRegistry()
    parent = QWidget()
    qtbot.addWidget(parent)
    names = extractor_registry.list_names()
    assert set(IE_LEGACY_NO_OPTION_EXTRACTORS) <= set(names)

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


def test_no_extractor_offers_a_partition_option(qtbot):
    """No option limits extraction to one partition (issue #78)."""
    extractor_registry = ExtractorRegistry()
    parent = QWidget()
    qtbot.addWidget(parent)

    offenders = []
    for name in extractor_registry.list_names():
        extractor = extractor_registry.get(name)
        widget = extractor.get_config_widget(parent)
        if widget is None:
            continue
        if widget.findChild(QCheckBox, "scan_all_partitions") is not None:
            offenders.append(f"{name}: shows the checkbox")
        partition_options = [
            box.text() for box in widget.findChildren(QCheckBox) if "partition" in box.text().lower()
        ]
        if partition_options:
            offenders.append(f"{name}: {partition_options}")
        # The key would be written to the evidence log with the run's config.
        config = widget.get_config() if hasattr(widget, "get_config") else {}
        if "scan_all_partitions" in config:
            offenders.append(f"{name}: get_config() returns scan_all_partitions")
        from_extractor = getattr(extractor, "_get_config_from_widget", None)
        if from_extractor is not None and "scan_all_partitions" in from_extractor():
            offenders.append(f"{name}: _get_config_from_widget() returns scan_all_partitions")

    assert not offenders, "\n".join(offenders)


@pytest.mark.parametrize("name", NO_OPTION_EXTRACTORS)
def test_configure_from_extractors_tab_reports_no_options(
    qtbot, extractor_registry, configure_outcomes, name
):
    tab = ExtractionTab(1)
    qtbot.addWidget(tab)
    extractor = extractor_registry.get(name)

    tab._show_config_dialog(extractor)

    assert configure_outcomes == [
        ("information", f"{extractor.metadata.display_name} has no configuration options.")
    ]


@pytest.mark.parametrize("name", NO_OPTION_EXTRACTORS)
def test_configure_from_case_wide_dialog_reports_no_options(
    qtbot, extractor_registry, configure_outcomes, name
):
    dialog = CaseWideExtractIngestDialog([{"id": 1, "label": "ev"}])
    qtbot.addWidget(dialog)
    items = [dialog.extractor_list.item(i) for i in range(dialog.extractor_list.count())]
    item = next(i for i in items if i.data(Qt.UserRole) == name)
    dialog.extractor_list.setCurrentItem(item)

    dialog._configure_selected_extractor()

    display_name = extractor_registry.get(name).metadata.display_name
    assert configure_outcomes == [
        ("information", f"{display_name} has no configuration options.")
    ]


def test_configure_still_opens_a_dialog_for_browser_selection(
    qtbot, extractor_registry, configure_outcomes
):
    tab = ExtractionTab(1)
    qtbot.addWidget(tab)
    extractor = extractor_registry.get("chromium_history")

    tab._show_config_dialog(extractor)

    assert configure_outcomes == [
        ("dialog", f"{extractor.metadata.display_name} - Configuration")
    ]
