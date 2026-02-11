"""Bookmarks subtab widget."""
from __future__ import annotations

from PySide6.QtWidgets import QApplication, QComboBox, QHBoxLayout, QLabel, QMenu

from app.common import add_sandbox_url_actions
from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .model import BookmarksTableModel
from .dialog import BookmarkDetailsDialog


class BookmarksSubtab(BaseArtifactSubtab):
    """Parsed bookmarks with folder column."""

    def _default_status_text(self):
        return "0 bookmarks"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Folder:"))
        self.folder_filter = QComboBox()
        self.folder_filter.addItem("All", "")
        self.folder_filter.setMinimumWidth(200)
        fl.addWidget(self.folder_filter)

    def _create_model(self):
        return BookmarksTableModel(
            self.ctx.db_manager, self.ctx.evidence_id,
            self.ctx.get_evidence_label(), case_data=self.ctx.case_data, parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 200)  # Title
        t.setColumnWidth(1, 300)  # URL
        t.setColumnWidth(2, 150)  # Folder
        t.setColumnWidth(3, 80)   # Browser
        t.setColumnWidth(4, 80)   # Profile
        t.setColumnWidth(5, 90)   # Date Added
        t.setColumnWidth(6, 140)  # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)
        for folder in self._model.get_available_folders():
            display = folder if len(folder) <= 40 else "..." + folder[-37:]
            self.folder_filter.addItem(display, folder)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        folder = self.folder_filter.currentData() or ""
        self._model.load(browser_filter=browser, folder_filter=folder)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        folders = self._model.get_folder_count()
        self.status_label.setText(f"{count} bookmarks in {folders} folders")

    def _artifact_type_for_tagging(self):
        return "bookmark"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._model.get_row_data(index)
        if not row_data:
            return
        BookmarkDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))
        menu.addSeparator()

        url = row_data.get("url", "")
        if url:
            add_sandbox_url_actions(menu, url, self, self.ctx.evidence_id, evidence_label=self.ctx.get_evidence_label(), workspace_path=self.ctx.case_folder, case_data=self.ctx.case_data)
            menu.addSeparator()
            copy_url = menu.addAction("📋 Copy URL")
            copy_url.triggered.connect(lambda: QApplication.clipboard().setText(url))

        menu.addSeparator()
        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
