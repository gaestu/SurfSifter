"""Saved Logins subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import QHBoxLayout, QLabel, QLineEdit, QMenu, QPushButton

from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .credentials_model import CredentialsTableModel
from .credentials_dialog import CredentialsDetailsDialog

logger = logging.getLogger(__name__)


class SavedLoginsSubtab(BaseArtifactSubtab):
    """Saved website credentials with browser/origin filtering."""

    def _default_status_text(self):
        return "0 saved logins"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Origin:"))
        self.origin_filter = QLineEdit()
        self.origin_filter.setPlaceholderText("Filter by origin URL...")
        self.origin_filter.setMaximumWidth(200)
        fl.addWidget(self.origin_filter)

        # Reveal/hide decrypted passwords toggle
        self._reveal_btn = QPushButton("👁 Reveal Passwords")
        self._reveal_btn.setCheckable(True)
        self._reveal_btn.setToolTip("Show or hide decrypted password values")
        self._reveal_btn.toggled.connect(self._on_reveal_toggled)
        fl.addWidget(self._reveal_btn)

    def _create_model(self):
        return CredentialsTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 200)   # Origin URL
        t.setColumnWidth(1, 100)   # Username Element
        t.setColumnWidth(2, 120)   # Username
        t.setColumnWidth(3, 100)   # Password
        t.setColumnWidth(4, 80)    # Browser
        t.setColumnWidth(5, 80)    # Profile
        t.setColumnWidth(6, 70)    # Encrypted
        t.setColumnWidth(7, 100)   # Decrypt
        t.setColumnWidth(8, 100)   # Created
        t.setColumnWidth(9, 100)   # Last Used
        t.setColumnWidth(10, 140)  # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        origin = self.origin_filter.text().strip()
        self._model.load(browser_filter=browser, origin_filter=origin)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        encrypted = self._model.get_encrypted_count()
        if encrypted > 0:
            self.status_label.setText(f"{count} saved logins ({encrypted} encrypted)")
        else:
            self.status_label.setText(f"{count} saved logins")

    def _on_reveal_toggled(self, checked: bool) -> None:
        """Toggle password visibility in the model."""
        if self._model is None:
            return
        self._model.toggle_secrets()
        self._reveal_btn.setText(
            "👁 Hide Passwords" if checked else "👁 Reveal Passwords"
        )

    def _artifact_type_for_tagging(self):
        return "credential"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return
        CredentialsDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
