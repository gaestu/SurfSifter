"""Closed Tabs subtab widget for recently closed browser tabs."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QMenu,
)

from app.common import add_sandbox_url_actions
from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .closed_tabs_model import ClosedTabsTableModel
from .closed_tabs_dialog import ClosedTabDetailsDialog

logger = logging.getLogger(__name__)


class ClosedTabsSubtab(BaseArtifactSubtab):
    """Recently closed browser tabs with browser filtering."""

    def _default_status_text(self):
        return "0 closed tabs"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

    def _create_model(self):
        return ClosedTabsTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 250)   # URL
        t.setColumnWidth(1, 200)   # Title
        t.setColumnWidth(2, 80)    # Browser
        t.setColumnWidth(3, 80)    # Profile
        t.setColumnWidth(4, 140)   # Closed At
        t.setColumnWidth(5, 60)    # Window
        t.setColumnWidth(6, 50)    # Tab
        t.setColumnWidth(7, 140)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        self._model.load(browser_filter=browser)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        self.status_label.setText(f"{count} closed tabs")

    def _artifact_type_for_tagging(self):
        return "closed_tab"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return

        dialog = ClosedTabDetailsDialog(row_data, parent=self)
        dialog.exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        url = row_data.get("url", "")
        if url:
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
