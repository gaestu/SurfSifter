"""Downloads subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import QComboBox, QHBoxLayout, QLabel, QLineEdit, QMenu

from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .dialog import BrowserDownloadDetailsDialog
from .model import BrowserDownloadsTableModel

logger = logging.getLogger(__name__)


class DownloadsSubtab(BaseArtifactSubtab):
    """Browser downloads with state and filename filtering."""

    def _default_status_text(self):
        return "0 downloads"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("State:"))
        self.state_filter = QComboBox()
        self.state_filter.addItem("All", "")
        self.state_filter.addItem("Complete", "complete")
        self.state_filter.addItem("Cancelled", "cancelled")
        self.state_filter.addItem("Interrupted", "interrupted")
        self.state_filter.addItem("In Progress", "in_progress")
        fl.addWidget(self.state_filter)

        self.filename_filter = QLineEdit()
        self.filename_filter.setPlaceholderText("Filter by filename...")
        self.filename_filter.setMaximumWidth(200)
        fl.addWidget(self.filename_filter)

    def _create_model(self):
        return BrowserDownloadsTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 200)   # Filename
        t.setColumnWidth(1, 250)   # URL
        t.setColumnWidth(2, 80)    # Browser
        t.setColumnWidth(3, 80)    # State
        t.setColumnWidth(4, 100)   # Danger
        t.setColumnWidth(5, 80)    # Size
        t.setColumnWidth(6, 120)   # Start Time
        t.setColumnWidth(7, 120)   # End Time
        t.setColumnWidth(8, 140)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        state = self.state_filter.currentData() or ""
        filename = self.filename_filter.text().strip()
        self._model.load(
            browser_filter=browser,
            state_filter=state,
            filename_filter=filename,
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        total_bytes = self._model.get_total_bytes()
        total_mb = total_bytes / (1024 * 1024) if total_bytes else 0
        self.status_label.setText(f"{count} downloads ({total_mb:.1f} MB total)")

    def _artifact_type_for_tagging(self):
        return "browser_download"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._model.get_row_data(index)
        if not row_data:
            return
        BrowserDownloadDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
