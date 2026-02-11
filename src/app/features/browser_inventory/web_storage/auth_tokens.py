"""Auth tokens subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
)

from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .auth_tokens_dialog import StorageTokenDetailsDialog
from .auth_tokens_model import AuthTokensTableModel

logger = logging.getLogger(__name__)


class AuthTokensSubtab(BaseArtifactSubtab):
    """Authentication tokens extracted from web storage."""

    def _default_status_text(self):
        return "0 tokens"

    def _setup_filters(self, fl: QHBoxLayout):
        fl.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All", "")
        self.type_filter.addItem("JWT", "jwt")
        self.type_filter.addItem("OAuth", "oauth")
        self.type_filter.addItem("MS OAuth", "ms_oauth")
        self.type_filter.addItem("Session", "session")
        self.type_filter.addItem("API Key", "api_key")
        fl.addWidget(self.type_filter)

        fl.addWidget(QLabel("Risk:"))
        self.risk_filter = QComboBox()
        self.risk_filter.addItem("All", "")
        self.risk_filter.addItem("High", "high")
        self.risk_filter.addItem("Medium", "medium")
        self.risk_filter.addItem("Low", "low")
        fl.addWidget(self.risk_filter)

        fl.addWidget(QLabel("Origin:"))
        self.origin_filter = QLineEdit()
        self.origin_filter.setPlaceholderText("Filter by origin...")
        self.origin_filter.setMaximumWidth(200)
        fl.addWidget(self.origin_filter)

        self.include_expired = QCheckBox("Include Expired")
        self.include_expired.setChecked(True)
        fl.addWidget(self.include_expired)

    def _create_model(self):
        return AuthTokensTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 80)    # Type
        t.setColumnWidth(1, 200)   # Origin
        t.setColumnWidth(2, 150)   # Key
        t.setColumnWidth(3, 150)   # Email
        t.setColumnWidth(4, 90)    # Expires
        t.setColumnWidth(5, 60)    # Expired
        t.setColumnWidth(6, 60)    # Risk
        t.setColumnWidth(7, 70)    # Browser
        t.setColumnWidth(8, 70)    # Profile
        t.setColumnWidth(9, 80)    # Storage

    def _apply_filters(self):
        if self._model is None:
            return
        token_type = self.type_filter.currentData() or ""
        risk = self.risk_filter.currentData() or ""
        origin = self.origin_filter.text().strip()
        include_expired = self.include_expired.isChecked()
        self._model.load(
            token_type_filter=token_type,
            origin_filter=origin,
            risk_filter=risk,
            include_expired=include_expired,
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        stats = self._model.get_stats()
        high_risk = stats.get("by_risk", {}).get("high", 0)
        expired = stats.get("expired", 0)
        parts = [f"{count} tokens"]
        if high_risk > 0:
            parts.append(f"{high_risk} high risk")
        if expired > 0:
            parts.append(f"{expired} expired")
        self.status_label.setText(" | ".join(parts))

    def _artifact_type_for_tagging(self):
        return "auth_token"

    def _get_row_data(self, index):
        """Use get_record_by_row for AuthTokensTableModel."""
        if self._model is None:
            return None
        return self._model.get_record_by_row(index.row())

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._model.get_record_by_row(index.row())
        if not row_data:
            return
        StorageTokenDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        token_value = row_data.get("token_value", "") or ""
        if token_value:
            copy_token = menu.addAction("📋 Copy Token Value")
            copy_token.triggered.connect(
                lambda: QApplication.clipboard().setText(token_value)
            )

        origin = row_data.get("origin", "") or ""
        if origin:
            copy_origin = menu.addAction("📋 Copy Origin")
            copy_origin.triggered.connect(
                lambda: QApplication.clipboard().setText(origin)
            )

        menu.addSeparator()
        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
