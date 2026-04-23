"""GUI tests for text blocks feature integration."""

from __future__ import annotations

import sqlite3
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


@pytest.mark.gui_offscreen
def test_reports_preview_buttons_exist_in_expected_order(qtbot) -> None:
    widget = ReportTabWidget()
    qtbot.addWidget(widget)

    assert widget._preview_btn.text() == "👁️ Report Preview"
    assert widget._preview_appendix_btn.text() == "👁️📎 Appendix Preview"
    assert widget._preview_complete_btn.text() == "👁️📄📎 Complete Preview"


@pytest.mark.gui_offscreen
def test_reports_appendix_preview_uses_appendix_html(qtbot) -> None:
    widget = ReportTabWidget()
    qtbot.addWidget(widget)

    with (
        patch.object(widget, "_build_report_html", return_value="<html>appendix</html>") as build_html,
        patch.object(widget._generator, "preview_in_browser") as preview_in_browser,
    ):
        widget._on_preview_appendix()

    build_html.assert_called_once()
    preview_in_browser.assert_called_once_with("<html>appendix</html>")


@pytest.mark.gui_offscreen
def test_reports_complete_preview_opens_report_and_appendix(qtbot) -> None:
    widget = ReportTabWidget()
    qtbot.addWidget(widget)

    with (
        patch.object(
            widget,
            "_build_report_html",
            return_value=("<html>report</html>", "<html>appendix</html>"),
        ) as build_html,
        patch.object(widget._generator, "preview_in_browser") as preview_in_browser,
    ):
        widget._on_preview_complete()

    build_html.assert_called_once()
    assert preview_in_browser.call_args_list == [
        (("<html>report</html>",), {}),
        (("<html>appendix</html>",), {}),
    ]


@pytest.mark.gui_offscreen
def test_reports_preview_logs_browser_invocation(qtbot) -> None:
    widget = ReportTabWidget()
    qtbot.addWidget(widget)

    widget._db_conn = sqlite3.connect(":memory:")
    widget._evidence_id = 7

    with (
        patch("reports.ui.report_tab_widget.create_process_log", return_value=11) as create_log,
        patch("reports.ui.report_tab_widget.finalize_process_log") as finalize_log,
        patch.object(widget._generator, "preview_in_browser", return_value="preview.html") as preview_in_browser,
    ):
        widget._open_preview_with_audit("<html>report</html>", "report")

    create_log.assert_called_once_with(
        widget._db_conn,
        7,
        "report_preview",
        "Open report preview in default browser",
    )
    preview_in_browser.assert_called_once_with("<html>report</html>")
    finalize_log.assert_called_once_with(
        widget._db_conn,
        11,
        exit_code=0,
        stdout="preview.html",
        stderr=None,
    )
