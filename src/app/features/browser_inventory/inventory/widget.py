"""Inventory subtab widget — browser artifact files with extraction status."""
from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMenu,
    QMessageBox,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from app.features.browser_inventory._base import SubtabContext
from .model import BrowserInventoryModel
from .dialog import BrowserInventoryDetailsDialog

logger = logging.getLogger(__name__)


class InventorySubtab(QWidget):
    """Raw browser artifact files with extraction/ingestion status."""

    def __init__(self, ctx: SubtabContext, parent=None):
        super().__init__(parent)
        self.ctx = ctx
        self._loaded = False

        self.model = BrowserInventoryModel(
            str(ctx.case_folder),
            ctx.evidence_id,
            ctx.case_db_path,
            parent=self,
        )

        self._setup_ui()
        self._populate_filters()

    @property
    def db_manager(self):
        """Expose db_manager for legacy compatibility."""
        return self.model.db_manager

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Filter controls
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel("Browser:"))
        self.browser_filter = QComboBox()
        self.browser_filter.addItem("All", "")
        self.browser_filter.currentIndexChanged.connect(self._apply_filters)
        filter_layout.addWidget(self.browser_filter)

        filter_layout.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All", "")
        self.type_filter.currentIndexChanged.connect(self._apply_filters)
        filter_layout.addWidget(self.type_filter)

        filter_layout.addWidget(QLabel("Status:"))
        self.status_filter = QComboBox()
        self.status_filter.addItem("All", "")
        self.status_filter.addItem("OK", "ok")
        self.status_filter.addItem("Pending", "pending")
        self.status_filter.addItem("Partial", "partial")
        self.status_filter.addItem("Failed", "failed")
        self.status_filter.addItem("Error", "error")
        self.status_filter.addItem("Skipped", "skipped")
        self.status_filter.currentIndexChanged.connect(self._apply_filters)
        filter_layout.addWidget(self.status_filter)

        filter_layout.addStretch()

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh)
        filter_layout.addWidget(self.refresh_btn)

        layout.addLayout(filter_layout)

        # Table view
        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setSelectionMode(QTableView.SingleSelection)
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._show_context_menu)
        self.table_view.doubleClicked.connect(self._view_details)

        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(False)

        self.table_view.setColumnWidth(0, 80)   # Browser
        self.table_view.setColumnWidth(1, 120)  # Type
        self.table_view.setColumnWidth(2, 100)  # Profile
        self.table_view.setColumnWidth(3, 300)  # Path
        self.table_view.setColumnWidth(4, 100)  # Extraction
        self.table_view.setColumnWidth(5, 100)  # Ingestion
        self.table_view.setColumnWidth(6, 60)   # URLs
        self.table_view.setColumnWidth(7, 70)   # Records

        layout.addWidget(self.table_view)

        self.status_label = QLabel()
        self._update_status()
        layout.addWidget(self.status_label)

    # ─── Public interface ─────────────────────────────────────────────

    @property
    def is_loaded(self):
        return self._loaded

    def load(self):
        self.refresh()
        self._loaded = True

    def mark_needs_reload(self):
        self._loaded = False

    def refresh(self):
        self.model.refresh()
        self._update_status()

    # ─── Filters ──────────────────────────────────────────────────────

    def _populate_filters(self):
        browsers = self.model.get_available_browsers()
        for browser in browsers:
            self.browser_filter.addItem(browser.capitalize(), browser)

        types = self.model.get_available_types()
        for artifact_type in types:
            display_name = artifact_type.replace("_", " ").title()
            self.type_filter.addItem(display_name, artifact_type)

    def _apply_filters(self):
        browser = self.browser_filter.currentData() or ""
        artifact_type = self.type_filter.currentData() or ""
        status = self.status_filter.currentData() or ""

        self.model.set_filters(
            browser=browser,
            artifact_type=artifact_type,
            status=status,
        )
        self._update_status()

    def _update_status(self):
        count = self.model.rowCount()
        self.status_label.setText(f"{count} artifact(s)")

    # ─── Context Menu & Details ──────────────────────────────────────

    def _show_context_menu(self, position):
        index = self.table_view.indexAt(position)
        if not index.isValid():
            return

        row_data = self.model.get_row_data(index)
        if not row_data:
            return

        menu = QMenu(self)

        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))
        menu.addSeparator()

        extracted_path = row_data.get("extracted_path")
        if extracted_path:
            open_action = menu.addAction("Open Extracted File")
            open_action.triggered.connect(lambda: self._open_file(row_data))

        menu.addSeparator()

        copy_action = menu.addAction("Copy Logical Path")
        copy_action.triggered.connect(lambda: self._copy_path(row_data))

        menu.exec(self.table_view.viewport().mapToGlobal(position))

    def _view_details(self, index):
        row_data = self.model.get_row_data(index)
        if not row_data:
            return
        BrowserInventoryDetailsDialog(row_data, parent=self).exec()

    def _open_file(self, row_data: dict):
        extracted_path = row_data.get("extracted_path")
        if not extracted_path:
            return

        evidence_label = self.model._get_evidence_label()
        run_id = row_data.get("run_id", "")
        artifact_type = row_data.get("artifact_type", "")

        extractor_name_map = {
            "history": "browser_history",
            "cache_simple": "cache_simple",
            "cache_firefox": "cache_firefox",
        }
        extractor_name = extractor_name_map.get(artifact_type, artifact_type)

        full_path = (
            self.ctx.case_folder / "evidences" / evidence_label
            / extractor_name / run_id / extracted_path
        )

        if not full_path.exists():
            QMessageBox.warning(self, "File Not Found", f"Extracted file not found:\n\n{full_path}")
            return

        try:
            if sys.platform == "win32":
                subprocess.run(["explorer", "/select,", str(full_path)])
            elif sys.platform == "darwin":
                subprocess.run(["open", "-R", str(full_path)])
            else:
                subprocess.run(["xdg-open", str(full_path.parent)])
        except Exception as e:
            logger.error(f"Failed to open file browser: {e}", exc_info=True)
            QMessageBox.warning(self, "Error", f"Failed to open file browser:\n\n{e}")

    def _copy_path(self, row_data: dict):
        logical_path = row_data.get("logical_path", "")
        if logical_path:
            QApplication.clipboard().setText(logical_path)
            self.status_label.setText(f"Copied: {logical_path}")
