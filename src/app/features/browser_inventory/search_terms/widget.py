"""Search terms subtab widget."""
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
from .dialog import SearchTermsDetailsDialog
from .model import SearchTermsTableModel
from core.database.helpers.browser_search_terms import get_search_term_by_id

logger = logging.getLogger(__name__)


class SearchTermsSubtab(BaseArtifactSubtab):
    """Browser search terms with engine and term filtering."""

    def _default_status_text(self):
        return "0 search terms"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Engine:"))
        self.engine_filter = QComboBox()
        self.engine_filter.addItem("All", "")
        fl.addWidget(self.engine_filter)

        self.term_filter = QLineEdit()
        self.term_filter.setPlaceholderText("Filter by search term...")
        self.term_filter.setMaximumWidth(200)
        fl.addWidget(self.term_filter)

    def _create_model(self):
        return SearchTermsTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 250)   # Term
        t.setColumnWidth(1, 200)   # URL
        t.setColumnWidth(2, 140)   # Search Time
        t.setColumnWidth(3, 80)    # Browser
        t.setColumnWidth(4, 80)    # Profile
        t.setColumnWidth(5, 100)   # Search Engine
        t.setColumnWidth(6, 100)   # Tags

    def _populate_filter_options(self):
        self._model.load()
        for b in self._model.get_browsers():
            self.browser_filter.addItem(b.capitalize(), b)
        for engine in self._model.get_search_engines():
            self.engine_filter.addItem(engine, engine)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        engine = self.engine_filter.currentData() or ""
        term = self.term_filter.text().strip()
        self._model.load(
            browser_filter=browser,
            term_filter=term,
            search_engine_filter=engine,
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        stats = self._model.get_stats()
        unique_terms = stats.get("unique_terms", 0)
        if unique_terms > 0 and unique_terms != count:
            self.status_label.setText(
                f"{count} search terms ({unique_terms} unique)"
            )
        else:
            self.status_label.setText(f"{count} search terms")

    def _artifact_type_for_tagging(self):
        return "browser_search_term"

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
                    full_record = get_search_term_by_id(conn, record_id)
                    if full_record:
                        row_data = full_record
            except Exception as e:
                logger.error(
                    f"Failed to get full search term data: {e}", exc_info=True
                )

        SearchTermsDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        term = row_data.get("term", "") or row_data.get("search_term", "")
        if term:
            copy_term = menu.addAction("📋 Copy Search Term")
            copy_term.triggered.connect(
                lambda: QApplication.clipboard().setText(term)
            )
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

            copy_url = menu.addAction("📋 Copy URL")
            copy_url.triggered.connect(
                lambda: QApplication.clipboard().setText(url)
            )
            menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
