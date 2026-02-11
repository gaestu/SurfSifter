"""GUI tests for text blocks feature integration."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QMessageBox

from app.config.settings import AppSettings
from app.features.settings.preferences import PreferencesDialog
from reports.ui.report_tab_widget import ReportTabWidget
from reports.ui.section_editor import SectionEditorDialog


@pytest.mark.gui_offscreen
def test_preferences_has_text_blocks_tab(qtbot, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.config.text_blocks.default_text_blocks_dir", lambda: tmp_path)

    dialog = PreferencesDialog(
        AppSettings(),
        config_dir=tmp_path,
        rules_dir=tmp_path,
        tool_registry=None,
    )
    qtbot.addWidget(dialog)

    tab_names = [dialog.tabs.tabText(i) for i in range(dialog.tabs.count())]
    assert "Text Blocks" in tab_names


@pytest.mark.gui_offscreen
def test_section_editor_applies_selected_text_block(qtbot) -> None:
    dialog = SectionEditorDialog(
        text_blocks=[{"title": "Methodology", "content": "Standard process text."}],
        edit_mode=False,
    )
    qtbot.addWidget(dialog)

    dialog._text_block_combo.setCurrentIndex(1)

    assert dialog.get_title() == "Methodology"
    assert dialog.get_plain_content() == "Standard process text."


@pytest.mark.gui_offscreen
def test_section_editor_does_not_overwrite_when_user_declines(qtbot) -> None:
    dialog = SectionEditorDialog(
        text_blocks=[{"title": "Methodology", "content": "Template content"}],
        edit_mode=False,
    )
    qtbot.addWidget(dialog)

    dialog._title_input.setText("Existing Title")
    dialog._content_edit.setPlainText("Existing Content")

    with patch.object(QMessageBox, "question", return_value=QMessageBox.No):
        dialog._text_block_combo.setCurrentIndex(1)

    assert dialog.get_title() == "Existing Title"
    assert dialog.get_plain_content() == "Existing Content"


@pytest.mark.gui_offscreen
def test_reports_manage_text_blocks_button_emits_signal(qtbot) -> None:
    widget = ReportTabWidget()
    qtbot.addWidget(widget)

    emitted = []
    widget.manage_text_blocks_requested.connect(lambda: emitted.append(True))

    qtbot.mouseClick(widget._manage_text_blocks_btn, Qt.LeftButton)

    assert emitted == [True]
