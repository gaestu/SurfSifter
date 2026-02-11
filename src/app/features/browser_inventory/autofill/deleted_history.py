"""Deleted Form History subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QMenu,
    QTextEdit,
    QVBoxLayout,
)

from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .deleted_form_history_model import DeletedFormHistoryTableModel

logger = logging.getLogger(__name__)


class DeletedHistorySubtab(BaseArtifactSubtab):
    """Firefox deleted form history entries with browser filtering."""

    def _default_status_text(self):
        return "0 deleted entries"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

    def _create_model(self):
        return DeletedFormHistoryTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 200)   # GUID
        t.setColumnWidth(1, 160)   # Time Deleted
        t.setColumnWidth(2, 80)    # Browser
        t.setColumnWidth(3, 80)    # Profile
        t.setColumnWidth(4, 200)   # Source Path
        t.setColumnWidth(5, 140)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        self._model.load(browser_filter=browser)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        self.status_label.setText(f"{count} deleted entries")

    def _artifact_type_for_tagging(self):
        return "deleted_form_history"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("Deleted Form History Entry")
        dialog.setMinimumSize(500, 300)
        layout = QVBoxLayout(dialog)
        text = QTextEdit()
        text.setReadOnly(True)
        details = []
        for key, value in row_data.items():
            if value is not None:
                details.append(f"<b>{key}:</b> {value}")
        text.setHtml("<br>".join(details))
        layout.addWidget(text)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok)
        buttons.accepted.connect(dialog.accept)
        layout.addWidget(buttons)
        dialog.exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
