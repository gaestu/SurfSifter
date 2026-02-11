"""History subtab widget."""
from __future__ import annotations

import logging
import sqlite3

from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
)

from app.common import add_sandbox_url_actions
from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .dialog import HistoryDetailsDialog
from .model import BrowserHistoryTableModel
from core.database import get_browser_history_by_id

logger = logging.getLogger(__name__)


class HistorySubtab(BaseArtifactSubtab):
    """Browser history records with browser/profile/type/URL filtering."""

    def _default_status_text(self):
        return "0 history records"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Profile:"))
        self.profile_filter = QComboBox()
        self.profile_filter.addItem("All", "")
        fl.addWidget(self.profile_filter)

        fl.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All", "")
        self.type_filter.addItem("Link", "link")
        self.type_filter.addItem("Typed", "typed")
        self.type_filter.addItem("Bookmark", "bookmark")
        self.type_filter.addItem("Redirect", "redirect_permanent")
        self.type_filter.addItem("Download", "download")
        self.type_filter.addItem("Reload", "reload")
        fl.addWidget(self.type_filter)

        self.url_filter = QLineEdit()
        self.url_filter.setPlaceholderText("Filter by URL...")
        self.url_filter.setMaximumWidth(200)
        fl.addWidget(self.url_filter)

    def _create_model(self):
        return BrowserHistoryTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 300)  # URL
        t.setColumnWidth(1, 200)  # Title
        t.setColumnWidth(2, 140)  # Visit Time
        t.setColumnWidth(3, 80)   # Browser
        t.setColumnWidth(4, 80)   # Profile
        t.setColumnWidth(5, 60)   # Visits
        t.setColumnWidth(6, 50)   # Typed
        t.setColumnWidth(7, 80)   # Type
        t.setColumnWidth(8, 140)  # Tags

    def _populate_filter_options(self):
        for b in self._model.get_browsers():
            self.browser_filter.addItem(b.capitalize(), b)
        for profile in self._model.get_profiles():
            self.profile_filter.addItem(profile, profile)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        profile = self.profile_filter.currentData() or ""
        visit_type = self.type_filter.currentData() or ""
        url_filter = self.url_filter.text().strip()
        self._model.load(
            browser_filter=browser,
            profile_filter=profile,
            visit_type_filter=visit_type,
            url_filter=url_filter,
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        stats = self._model.get_stats()
        typed_count = stats.get("typed_count", 0)
        if typed_count > 0:
            self.status_label.setText(f"{count} history records ({typed_count} typed)")
        else:
            self.status_label.setText(f"{count} history records")

    def _artifact_type_for_tagging(self):
        return "browser_history"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._model.get_row_data(index)
        if not row_data:
            return

        record_id = row_data.get("id")
        if record_id:
            try:
                with sqlite3.connect(self.ctx.evidence_db_path()) as conn:
                    conn.row_factory = sqlite3.Row
                    full_record = get_browser_history_by_id(conn, record_id)
                    if full_record:
                        row_data = full_record
            except Exception as e:
                logger.error(
                    f"Failed to get full history data: {e}", exc_info=True
                )

        dialog = HistoryDetailsDialog(row_data, parent=self)
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

            copy_action = menu.addAction("📋 Copy URL")
            copy_action.triggered.connect(
                lambda: QApplication.clipboard().setText(url)
            )
            menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
