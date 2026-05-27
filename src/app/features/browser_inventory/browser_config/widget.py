"""Browser Config subtab widget."""
from __future__ import annotations

import logging
from typing import cast

from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
)

from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .dialog import BrowserConfigDetailsDialog
from .model import BrowserConfigTableModel

logger = logging.getLogger(__name__)


class BrowserConfigSubtab(BaseArtifactSubtab):
    """Read-only browser configuration records with filters and search."""

    def _default_status_text(self):
        return "0 config records"

    def _setup_filters(self, filter_layout: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(filter_layout)

        filter_layout.addWidget(QLabel("Type:"))
        self.config_type_filter = QComboBox()
        self.config_type_filter.addItem("All", "")
        self.config_type_filter.setMinimumWidth(150)
        filter_layout.addWidget(self.config_type_filter)

        filter_layout.addWidget(QLabel("Key:"))
        self.key_search = QLineEdit()
        self.key_search.setPlaceholderText("Search key")
        self.key_search.setClearButtonEnabled(True)
        self.key_search.returnPressed.connect(self._apply_filters)
        filter_layout.addWidget(self.key_search)

        filter_layout.addWidget(QLabel("Profile:"))
        self.profile_search = QLineEdit()
        self.profile_search.setPlaceholderText("Search profile")
        self.profile_search.setClearButtonEnabled(True)
        self.profile_search.returnPressed.connect(self._apply_filters)
        filter_layout.addWidget(self.profile_search)

    def _create_model(self):
        return BrowserConfigTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            parent=self,
        )

    def _configure_table(self):
        self.table.setColumnWidth(0, 90)    # Browser
        self.table.setColumnWidth(1, 100)   # Profile
        self.table.setColumnWidth(2, 120)   # Config Type
        self.table.setColumnWidth(3, 220)   # Config Key
        self.table.setColumnWidth(4, 260)   # Config Value
        self.table.setColumnWidth(5, 280)   # Source Path
        self.table.setColumnWidth(6, 80)    # Partition
        self.table.setColumnWidth(7, 180)   # Notes

    def _populate_filter_options(self):
        if self._model is None:
            return
        model = cast(BrowserConfigTableModel, self._model)
        for browser in model.get_available_browsers():
            self.browser_filter.addItem(browser.capitalize(), browser)
        for config_type in model.get_available_config_types():
            display = config_type.replace("_", " ").title()
            self.config_type_filter.addItem(display, config_type)

    def _apply_filters(self):
        if self._model is None:
            return
        self._model.load(
            browser_filter=self.browser_filter.currentData() or "",
            config_type_filter=self.config_type_filter.currentData() or "",
            key_search=self.key_search.text(),
            profile_search=self.profile_search.text(),
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        type_counts = self._model.get_type_counts()
        parts = [f"{count} config records"]
        if type_counts:
            summary = ", ".join(
                f"{config_type.replace('_', ' ')}: {value}"
                for config_type, value in sorted(type_counts.items())[:3]
            )
            parts.append(summary)
        self.status_label.setText(" | ".join(parts))

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._model.get_row_data(index)
        if not row_data:
            return
        BrowserConfigDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        config_key = row_data.get("config_key") or ""
        if config_key:
            copy_key = menu.addAction("Copy Key")
            copy_key.triggered.connect(lambda: QApplication.clipboard().setText(config_key))

        config_value = row_data.get("config_value") or ""
        if config_value:
            copy_value = menu.addAction("Copy Value")
            copy_value.triggered.connect(lambda: QApplication.clipboard().setText(config_value))

        source_path = row_data.get("source_path") or ""
        if source_path:
            copy_source = menu.addAction("Copy Source Path")
            copy_source.triggered.connect(lambda: QApplication.clipboard().setText(source_path))