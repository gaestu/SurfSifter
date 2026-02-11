"""Permissions subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import QComboBox, QHBoxLayout, QLabel, QMenu

from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .dialog import PermissionDetailsDialog
from .model import PermissionsTableModel

logger = logging.getLogger(__name__)


class PermissionsSubtab(BaseArtifactSubtab):
    """Site permissions with permission type filtering."""

    def _default_status_text(self):
        return "0 permissions"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All", "")
        self.type_filter.setMinimumWidth(150)
        fl.addWidget(self.type_filter)

    def _create_model(self):
        return PermissionsTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 200)   # Origin
        t.setColumnWidth(1, 120)   # Permission
        t.setColumnWidth(2, 70)    # Decision
        t.setColumnWidth(3, 80)    # Browser
        t.setColumnWidth(4, 80)    # Profile
        t.setColumnWidth(5, 100)   # Granted
        t.setColumnWidth(6, 100)   # Expires
        t.setColumnWidth(7, 140)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)
        for perm_type in self._model.get_available_permission_types():
            display = perm_type.replace("_", " ").title()
            self.type_filter.addItem(display, perm_type)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        perm_type = self.type_filter.currentData() or ""
        self._model.load(
            browser_filter=browser,
            permission_filter=perm_type,
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        decisions = self._model.get_decision_counts()
        allow = decisions.get("allow", 0)
        deny = decisions.get("deny", 0)
        self.status_label.setText(
            f"{count} permissions ({allow} allow, {deny} deny)"
        )

    def _artifact_type_for_tagging(self):
        return "site_permission"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._model.get_row_data(index)
        if not row_data:
            return
        PermissionDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
