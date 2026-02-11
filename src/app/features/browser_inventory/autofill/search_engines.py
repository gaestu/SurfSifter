"""Search Engines subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QTextEdit,
    QVBoxLayout,
)

from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .search_engines_model import SearchEnginesTableModel

logger = logging.getLogger(__name__)


class SearchEnginesSubtab(BaseArtifactSubtab):
    """Configured search providers with browser/keyword filtering."""

    def _default_status_text(self):
        return "0 search engines"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Keyword:"))
        self.keyword_filter = QLineEdit()
        self.keyword_filter.setPlaceholderText("Filter by keyword...")
        self.keyword_filter.setMaximumWidth(200)
        fl.addWidget(self.keyword_filter)

    def _create_model(self):
        return SearchEnginesTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 120)    # Name
        t.setColumnWidth(1, 80)     # Keyword
        t.setColumnWidth(2, 250)    # URL Template
        t.setColumnWidth(3, 120)    # Favicon URL
        t.setColumnWidth(4, 80)     # Browser
        t.setColumnWidth(5, 80)     # Profile
        t.setColumnWidth(6, 60)     # Default
        t.setColumnWidth(7, 80)     # Auto-Replace
        t.setColumnWidth(8, 100)    # Created
        t.setColumnWidth(9, 100)    # Modified
        t.setColumnWidth(10, 140)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        keyword = self.keyword_filter.text().strip()
        self._model.load(browser_filter=browser, keyword_filter=keyword)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        self.status_label.setText(f"{count} search engines")

    def _artifact_type_for_tagging(self):
        return "search_engine"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return

        dialog = QDialog(self)
        dialog.setWindowTitle("Search Engine Details")
        dialog.setMinimumSize(500, 400)
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
