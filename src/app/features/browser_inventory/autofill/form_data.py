"""Form Data subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import QHBoxLayout, QLabel, QLineEdit, QMenu

from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .model import AutofillTableModel
from .dialog import AutofillDetailsDialog

logger = logging.getLogger(__name__)


class FormDataSubtab(BaseArtifactSubtab):
    """Autofill form data entries with browser/field filtering."""

    def _default_status_text(self):
        return "0 autofill entries"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Field:"))
        self.field_filter = QLineEdit()
        self.field_filter.setPlaceholderText("Filter by field name...")
        self.field_filter.setMaximumWidth(200)
        fl.addWidget(self.field_filter)

    def _create_model(self):
        return AutofillTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 150)   # Field Name
        t.setColumnWidth(1, 200)   # Value
        t.setColumnWidth(2, 80)    # Browser
        t.setColumnWidth(3, 80)    # Profile
        t.setColumnWidth(4, 70)    # Use Count
        t.setColumnWidth(5, 100)   # First Used
        t.setColumnWidth(6, 100)   # Last Used
        t.setColumnWidth(7, 140)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        field = self.field_filter.text().strip()
        self._model.load(browser_filter=browser, field_filter=field)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        self.status_label.setText(f"{count} autofill entries")

    def _artifact_type_for_tagging(self):
        return "autofill"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return
        AutofillDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
