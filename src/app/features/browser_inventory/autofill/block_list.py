"""Autofill Block List subtab widget."""
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
from .block_list_model import AutofillBlockListModel

logger = logging.getLogger(__name__)


class BlockListSubtab(BaseArtifactSubtab):
    """Edge autofill block list entries with browser/site filtering."""

    def _description_text(self):
        return (
            "Sites where autofill is disabled. Edge-specific feature. "
            "Shows financial sites, brokerage accounts, and other sensitive "
            "services the user accessed."
        )

    def _default_status_text(self):
        return "0 blocked sites"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Site:"))
        self.site_filter = QLineEdit()
        self.site_filter.setPlaceholderText("Filter by site/domain...")
        self.site_filter.setMaximumWidth(200)
        fl.addWidget(self.site_filter)

    def _create_model(self):
        return AutofillBlockListModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 200)   # Block Value
        t.setColumnWidth(1, 100)   # Block Type
        t.setColumnWidth(2, 200)   # Meta Data
        t.setColumnWidth(3, 100)   # Device
        t.setColumnWidth(4, 100)   # Date Created
        t.setColumnWidth(5, 80)    # Browser
        t.setColumnWidth(6, 80)    # Profile
        t.setColumnWidth(7, 140)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        site = self.site_filter.text().strip()
        self._model.load(browser_filter=browser, block_value_filter=site)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        self.status_label.setText(f"{count} blocked sites")

    def _artifact_type_for_tagging(self):
        return "autofill_block_list"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return

        display_names = {
            "block_value": "Blocked Site/Domain",
            "block_value_type": "Block Type",
            "meta_data": "Metadata",
            "device_model": "Device Model",
            "date_created_utc": "Date Created (UTC)",
            "browser": "Browser",
            "profile": "Profile",
            "tags": "Tags",
        }

        dialog = QDialog(self)
        dialog.setWindowTitle("Autofill Block List Entry")
        dialog.setMinimumSize(500, 300)
        layout = QVBoxLayout(dialog)
        text = QTextEdit()
        text.setReadOnly(True)
        details = []
        for key, value in row_data.items():
            if value is not None:
                label = display_names.get(key, key)
                details.append(f"<b>{label}:</b> {value}")
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
