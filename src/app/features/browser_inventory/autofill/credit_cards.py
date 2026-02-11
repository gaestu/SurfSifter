"""Credit Cards subtab widget."""
from __future__ import annotations

import logging
import sqlite3

from PySide6.QtWidgets import QHBoxLayout, QLabel, QLineEdit, QMenu

from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .credit_cards_model import CreditCardsTableModel
from .credit_card_dialog import CreditCardDetailsDialog

logger = logging.getLogger(__name__)


class CreditCardsSubtab(BaseArtifactSubtab):
    """Saved payment cards with browser/name filtering."""

    def _default_status_text(self):
        return "0 saved cards"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Name:"))
        self.name_filter = QLineEdit()
        self.name_filter.setPlaceholderText("Filter by name/nickname/last4...")
        self.name_filter.setMaximumWidth(260)
        fl.addWidget(self.name_filter)

    def _create_model(self):
        return CreditCardsTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 170)   # Name On Card
        t.setColumnWidth(1, 120)   # Nickname
        t.setColumnWidth(2, 70)    # Last 4
        t.setColumnWidth(3, 70)    # Exp Month
        t.setColumnWidth(4, 70)    # Exp Year
        t.setColumnWidth(5, 80)    # Use Count
        t.setColumnWidth(6, 140)   # Last Used
        t.setColumnWidth(7, 80)    # Browser
        t.setColumnWidth(8, 80)    # Profile
        t.setColumnWidth(9, 140)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        name_term = self.name_filter.text().strip()
        self._model.load(browser_filter=browser, name_filter=name_term)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        self.status_label.setText(f"{count} saved cards")

    def _artifact_type_for_tagging(self):
        return "credit_card"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return

        card_id = row_data.get("id")
        if card_id:
            try:
                with sqlite3.connect(self.ctx.evidence_db_path()) as conn:
                    conn.row_factory = sqlite3.Row
                    full_row = conn.execute(
                        "SELECT * FROM credit_cards WHERE evidence_id = ? AND id = ?",
                        (self.ctx.evidence_id, card_id),
                    ).fetchone()
                    if full_row:
                        row_data = dict(full_row)
            except Exception as e:
                logger.error(
                    f"Failed to fetch full credit card details: {e}", exc_info=True
                )

        CreditCardDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
