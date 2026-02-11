"""Session Form Data subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
)

from app.common import add_sandbox_url_actions
from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .form_data_dialog import SessionFormDataDetailsDialog
from .form_data_model import SessionFormDataTableModel

logger = logging.getLogger(__name__)


class SessionFormDataSubtab(BaseArtifactSubtab):
    """Session form data entries with browser/field/URL filtering."""

    def _description_text(self):
        return (
            "Form field values captured from Firefox session restore files. "
            "Shows text the user entered in form fields (search boxes, login forms, "
            "text areas) that was preserved in the session snapshot."
        )

    def _default_status_text(self):
        return "0 form data entries"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Field:"))
        self.field_filter = QLineEdit()
        self.field_filter.setPlaceholderText("Filter by field name...")
        self.field_filter.setMaximumWidth(150)
        fl.addWidget(self.field_filter)

        fl.addWidget(QLabel("URL:"))
        self.url_filter = QLineEdit()
        self.url_filter.setPlaceholderText("Filter by URL...")
        self.url_filter.setMaximumWidth(200)
        fl.addWidget(self.url_filter)

    def _create_model(self):
        return SessionFormDataTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 200)   # Page URL
        t.setColumnWidth(1, 120)   # Field Name
        t.setColumnWidth(2, 200)   # Field Value
        t.setColumnWidth(3, 80)    # Type
        t.setColumnWidth(4, 80)    # Browser
        t.setColumnWidth(5, 80)    # Profile
        t.setColumnWidth(6, 140)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        field_name = self.field_filter.text().strip()
        url = self.url_filter.text().strip()
        self._model.load(
            browser_filter=browser,
            field_name_filter=field_name,
            url_filter=url,
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        stats = self._model.get_stats()
        unique_urls = stats.get("unique_urls", 0)
        unique_fields = stats.get("unique_fields", 0)
        self.status_label.setText(
            f"{count} form fields ({unique_fields} unique names, {unique_urls} URLs)"
        )

    def _artifact_type_for_tagging(self):
        return "session_form_data"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return
        SessionFormDataDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        field_name = row_data.get("field_name", "")
        field_value = row_data.get("field_value", "")

        if field_name:
            copy_name_action = menu.addAction("📋 Copy Field Name")
            copy_name_action.triggered.connect(
                lambda: QApplication.clipboard().setText(field_name)
            )

        if field_value:
            copy_value_action = menu.addAction("📋 Copy Field Value")
            copy_value_action.triggered.connect(
                lambda: QApplication.clipboard().setText(field_value)
            )

        url = row_data.get("url", "")
        if url:
            menu.addSeparator()
            add_sandbox_url_actions(
                menu,
                url,
                self,
                self.ctx.evidence_id,
                evidence_label=self.ctx.get_evidence_label(),
                workspace_path=self.ctx.case_folder,
                case_data=self.ctx.case_data,
            )
            menu.addSeparator()

            copy_url_action = menu.addAction("📋 Copy URL")
            copy_url_action.triggered.connect(
                lambda: QApplication.clipboard().setText(url)
            )

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
