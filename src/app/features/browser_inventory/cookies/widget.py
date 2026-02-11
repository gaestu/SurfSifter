"""Cookies subtab widget."""
from __future__ import annotations

import logging
import sqlite3

from PySide6.QtWidgets import QComboBox, QHBoxLayout, QLabel, QLineEdit, QMenu

from app.common import add_sandbox_url_actions
from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .model import CookiesTableModel
from .dialog import CookieDetailsDialog
from core.database import get_cookie_by_id

logger = logging.getLogger(__name__)


class CookiesSubtab(BaseArtifactSubtab):
    """Parsed cookies with domain/browser filtering."""

    def _default_status_text(self):
        return "0 cookies"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Domain:"))
        self.domain_filter = QLineEdit()
        self.domain_filter.setPlaceholderText("Filter by domain...")
        self.domain_filter.setMaximumWidth(200)
        fl.addWidget(self.domain_filter)

    def _create_model(self):
        return CookiesTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 150)   # Domain
        t.setColumnWidth(1, 120)   # Name
        t.setColumnWidth(2, 150)   # Value
        t.setColumnWidth(3, 80)    # Browser
        t.setColumnWidth(4, 80)    # Profile
        t.setColumnWidth(5, 50)    # Secure
        t.setColumnWidth(6, 60)    # HttpOnly
        t.setColumnWidth(7, 70)    # SameSite
        t.setColumnWidth(8, 80)    # Expires
        t.setColumnWidth(9, 60)    # Encrypted
        t.setColumnWidth(10, 140)  # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        domain = self.domain_filter.text().strip()
        self._model.load(browser_filter=browser, domain_filter=domain)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        encrypted = self._model.get_encrypted_count()
        if encrypted > 0:
            self.status_label.setText(f"{count} cookies ({encrypted} encrypted)")
        else:
            self.status_label.setText(f"{count} cookies")

    def _artifact_type_for_tagging(self):
        return "cookie"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._model.get_row_data(index)
        if not row_data:
            return

        cookie_id = row_data.get("id")
        if cookie_id:
            try:
                with sqlite3.connect(self.ctx.evidence_db_path()) as conn:
                    conn.row_factory = sqlite3.Row
                    full_data = get_cookie_by_id(conn, cookie_id)
                    if full_data:
                        row_data = full_data
            except Exception as e:
                logger.error(f"Failed to get full cookie data: {e}", exc_info=True)

        dialog = CookieDetailsDialog(row_data, parent=self)
        dialog.exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        tag_action = menu.addAction("Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
