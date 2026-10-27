"""
File List Tab - UI for viewing and managing file list entries.
"""
from __future__ import annotations

import csv
import logging
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QThread, Signal, Qt, QTimer, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QProgressDialog,
    QTableView,
    QVBoxLayout,
    QWidget,
    QHeaderView,
    QMenu,
    QApplication
)

from PySide6.QtCore import QThreadPool

from core.database import DatabaseManager
from core.matching import ReferenceListManager, ReferenceListMatcher
from app.features.file_list.models import FileListModel
from app.common.dialogs import ReferenceListSelectorDialog, TagArtifactsDialog
from app.data import FILE_LIST_ANY_MATCH, STATS_PROCESS_LOG, FileListFilterSpec
from app.data.case_data import CaseDataAccess
from app.services.matching_workers import FileListMatchWorker as MatchWorker
from app.services.evidence_access import evidence_has_source
from app.services.file_export_worker import FileExportRequest, FileExportTask

logger = logging.getLogger(__name__)


# MatchWorker imported from app.services.matching_workers

# Item data of the Tags dropdown's fixed "Any Tag" and "Untagged" entries
# (issue #73). Tag entries hold the tag name (a str), so no tag name can
# equal these objects.
_ANY_TAG = object()
_UNTAGGED_TAG = object()

# Item data of the dropdowns' "Loading..." / "Error loading" placeholders;
# while shown, the applied filter value is kept (issue #73). Objects, so no
# real extension, list or tag name can equal them.
_LOADING_DATA = object()
_ERROR_DATA = object()
# Item data of the Extension dropdown's "... (N more)" entry; not a filter.
_TRUNCATED_DATA = "__truncated__"


def _is_placeholder(data: Any) -> bool:
    """True for item data that keeps the applied filter value."""
    return data is _LOADING_DATA or data is _ERROR_DATA or data == _TRUNCATED_DATA


class ModelRefreshWorker(QThread):
    """
    Background worker for refreshing FileListModel data after matching.

    This prevents UI freeze when reloading large datasets (100k+ files)
    after reference list matching completes.
    """

    finished = Signal(list, int)  # (rows, total_count)
    error = Signal(str)

    def __init__(
        self,
        data_access: CaseDataAccess,
        evidence_id: int,
        filters: FileListFilterSpec,
        page_size: int = FileListModel.PAGE_SIZE,
    ):
        """
        Initialize refresh worker.

        Args:
            data_access: Data access (queries run on this worker's thread)
            evidence_id: Evidence ID
            filters: Current filter settings
            page_size: Rows of the first page (the model's page size)
        """
        super().__init__()
        self.data_access = data_access
        self.evidence_id = evidence_id
        self.filters = filters
        self.page_size = page_size

    def run(self):
        """Load model data in background thread."""
        try:
            import time
            start_time = time.time()

            # Same queries as the model (issue #73). This thread's connection
            # is closed before the signals are emitted.
            try:
                total_rows = self.data_access.count_file_list(self.evidence_id, self.filters)
                rows = self.data_access.list_file_list_rows(
                    self.evidence_id, self.filters, offset=0, limit=self.page_size
                )
            finally:
                if self.data_access.db_manager is not None:
                    self.data_access.db_manager.close_thread_connections()

            elapsed = time.time() - start_time
            logger.info(
                "ModelRefreshWorker: loaded %d/%d rows in %.3fs",
                len(rows), total_rows, elapsed
            )

            self.finished.emit(rows, total_rows)

        except Exception as e:
            logger.exception("ModelRefreshWorker error")
            self.error.emit(str(e))


class FileListFilterLoadWorker(QThread):
    """
    Background worker for loading file list filter dropdown data.

    Phase 3 performance optimization - prevents UI freeze when
    loading filter values from databases with 1M+ file entries.
    """

    finished = Signal(dict)  # {extensions: [...], matches: [...], tags: [...]}
    error = Signal(str)

    # Truncation limits for very large datasets
    MAX_EXTENSIONS = 200

    def __init__(self, model, case_data, evidence_id: int):
        """
        Initialize filter load worker.

        Args:
            model: FileListModel instance for get_filter_values()
            case_data: CaseDataAccess instance for tag loading
            evidence_id: Evidence ID
        """
        super().__init__()
        self.model = model
        self.case_data = case_data
        self.evidence_id = evidence_id

    def run(self):
        """Load filter values in background thread."""
        try:
            import time
            start_time = time.time()

            # Get filter values from model (uses cache if available). Its
            # connection on this thread is closed here, before the signals:
            # the tab drops this worker once ``finished`` is handled.
            try:
                filter_values = self.model.get_filter_values()
            finally:
                self.model.db_manager.close_thread_connections()

            # Truncate extensions if too many
            extensions = filter_values.get("extensions", [])
            extension_count = len(extensions)
            if extension_count > self.MAX_EXTENSIONS:
                extensions = extensions[:self.MAX_EXTENSIONS]

            # Load tags using CaseDataAccess (uses cache)
            tags = []
            if self.case_data:
                tags = self.case_data.list_tags(self.evidence_id)

            elapsed = time.time() - start_time
            logger.info(
                "FileListFilterLoadWorker: evidence_id=%s, %d extensions, %d matches, %d tags, elapsed=%.3fs",
                self.evidence_id,
                len(extensions),
                len(filter_values.get("matches", [])),
                len(tags),
                elapsed,
            )

            self.finished.emit({
                "extensions": extensions,
                "extension_count": extension_count,
                "truncated": extension_count > self.MAX_EXTENSIONS,
                "matches": filter_values.get("matches", []),
                "tags": tags,
            })

        except Exception as e:
            logger.exception("FileListFilterLoadWorker error")
            self.error.emit(str(e))


class FileListTab(QWidget):
    """File List tab with table, filters, and actions."""

    def __init__(self, case_folder: str, evidence_id: int, case_db_path: Path, parent=None, *, defer_load: bool = False):
        """
        Initialize file list tab.

        Args:
            case_folder: Path to case folder
            evidence_id: Evidence ID
            case_db_path: Path to case database file
            parent: Parent widget
            defer_load: If True, defer data loading until tab is visible (Phase 3)
        """
        super().__init__(parent)
        self.case_folder = Path(case_folder)
        self.evidence_id = evidence_id
        self.case_db_path = case_db_path
        self.db_manager = DatabaseManager(self.case_folder, case_db_path=case_db_path)

        # Initialize reference list manager
        self.ref_manager = ReferenceListManager(create_dirs=False)

        # Cache evidence label to avoid repeated queries
        self._evidence_label_cache: Optional[str] = None

        # Deferred loading support (Phase 3)
        self._data_loaded = False
        self._load_pending = not defer_load  # If not deferred, mark as pending immediate load
        self._filter_worker: Optional[FileListFilterLoadWorker] = None
        self._case_data: Optional[CaseDataAccess] = None

        # Stale data flag for lazy refresh after ingestion
        self._data_stale = False

        # Evidence source availability (set on first context-menu open)
        self._evidence_has_source: Optional[bool] = None
        self._export_task: Optional[FileExportTask] = None

        self._init_ui()

        # Only load immediately if not deferred
        if not defer_load:
            self._load_data()
        else:
            self._load_pending = True

    def _get_evidence_label(self) -> str:
        """
        Retrieve the evidence label from the database.

        Returns:
            Evidence label from database, or fallback format if not found.

        Note:
            Uses actual label from evidences table (typically E01 base filename).
            Falls back to EV-XXX format only if label is not in database.
        """
        if self._evidence_label_cache is not None:
            return self._evidence_label_cache

        import sqlite3
        with sqlite3.connect(self.case_db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT label FROM evidences WHERE id = ?",
                (self.evidence_id,),
            ).fetchone()
            if row and row["label"]:
                self._evidence_label_cache = row["label"]
            else:
                # Fallback (should not happen with  auto-derived labels)
                self._evidence_label_cache = f"EV-{self.evidence_id:03d}"
                logger.warning(
                    "Evidence %d has no label in database, using fallback: %s",
                    self.evidence_id,
                    self._evidence_label_cache
                )

        return self._evidence_label_cache

    def _init_ui(self):
        """Initialize user interface."""
        layout = QVBoxLayout()

        # Top toolbar
        toolbar_layout = QHBoxLayout()

        self.match_btn = QPushButton("Match Against Reference Lists")
        self.match_btn.setToolTip("Match files against reference lists")
        self.match_btn.clicked.connect(self.match_reference_lists)
        toolbar_layout.addWidget(self.match_btn)

        self.export_btn = QPushButton("Export CSV")
        self.export_btn.setToolTip("Export filtered file list to CSV")
        self.export_btn.clicked.connect(self.export_csv)
        toolbar_layout.addWidget(self.export_btn)

        self.tag_btn = QPushButton("Tag Selected")
        self.tag_btn.setToolTip("Tag selected files")
        self.tag_btn.clicked.connect(self._tag_selected)
        self.tag_btn.setEnabled(False)
        toolbar_layout.addWidget(self.tag_btn)

        toolbar_layout.addStretch()
        layout.addLayout(toolbar_layout)

        # Filter bar
        filter_layout = QGridLayout()

        # Extension filter
        filter_layout.addWidget(QLabel("Extension:"), 0, 0)
        self.extension_combo = QComboBox()
        self.extension_combo.addItem("All", "")
        self.extension_combo.currentTextChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.extension_combo, 0, 1)

        # Size filter
        filter_layout.addWidget(QLabel("Size:"), 0, 2)
        self.size_combo = QComboBox()
        self.size_combo.addItem("Any", "")
        self.size_combo.addItem("> 1 MB", "1048576")
        self.size_combo.addItem("> 10 MB", "10485760")
        self.size_combo.addItem("> 100 MB", "104857600")
        self.size_combo.currentTextChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.size_combo, 0, 3)

        # Deleted filter
        filter_layout.addWidget(QLabel("Deleted:"), 0, 4)
        self.deleted_combo = QComboBox()
        self.deleted_combo.addItem("All", "all")
        self.deleted_combo.addItem("Hide Deleted", "hide")
        self.deleted_combo.addItem("Show Only Deleted", "show_only")
        self.deleted_combo.currentTextChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.deleted_combo, 0, 5)

        # Matches filter
        filter_layout.addWidget(QLabel("Matches:"), 1, 0)
        self.matches_combo = QComboBox()
        self.matches_combo.addItem("All", "")
        self.matches_combo.addItem("Any Match", "any")
        self.matches_combo.currentTextChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.matches_combo, 1, 1)

        # Tags filter
        filter_layout.addWidget(QLabel("Tags:"), 1, 2)
        self.tags_combo = QComboBox()
        self._add_fixed_tag_items(self.tags_combo)
        # By index: a tag named "Untagged" has the fixed entry's text (issue #73)
        self.tags_combo.currentIndexChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.tags_combo, 1, 3)

        # Search filter with button
        filter_layout.addWidget(QLabel("Search:"), 1, 4)
        search_layout = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search filename or path...")
        self.search_edit.setToolTip(
            "Show files whose name or path contains this text (ASCII case-insensitive; "
            "%, _ and \\ match literally). Press Enter or Search to apply."
        )
        self.search_edit.returnPressed.connect(self._on_search_clicked)
        search_layout.addWidget(self.search_edit)

        # Issue #73: hides files whose name or path contains a literal
        # substring; applied with Search, like the search text.
        self.exclude_label = QLabel("Exclude:")
        search_layout.addWidget(self.exclude_label)
        self.exclude_edit = QLineEdit()
        self.exclude_edit.setPlaceholderText("Exclude filename or path...")
        self.exclude_edit.setToolTip(
            "Hide files whose name or path contains this text (ASCII case-insensitive; "
            "%, _ and \\ match literally). Press Enter or Search to apply."
        )
        self.exclude_edit.returnPressed.connect(self._on_search_clicked)
        search_layout.addWidget(self.exclude_edit)

        self.search_btn = QPushButton("Search")
        self.search_btn.setToolTip("Apply Search and Exclude")
        self.search_btn.clicked.connect(self._on_search_clicked)
        search_layout.addWidget(self.search_btn)

        filter_layout.addLayout(search_layout, 1, 5)

        layout.addLayout(filter_layout)

        # Table view
        # Rows load once the filter lists are loaded (_on_filters_loaded), i.e.
        # when the tab is first shown with defer_load (issue #63)
        self.model = FileListModel(self.case_folder, self.evidence_id, self.case_db_path, self, load=False)
        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSortingEnabled(True)

        # Connect model signals to update pagination and toolbar buttons
        self.model.layoutChanged.connect(self._update_pagination_controls)
        self.model.dataChanged.connect(self._update_toolbar_buttons)
        self.model.modelReset.connect(self._update_toolbar_buttons)

        # Configure column widths
        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.Fixed)  # Checkbox
        header.resizeSection(0, 50)
        header.setSectionResizeMode(1, QHeaderView.Stretch)  # File Path
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)  # File Name
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)  # Extension
        header.setSectionResizeMode(4, QHeaderView.ResizeToContents)  # Size
        header.setSectionResizeMode(5, QHeaderView.ResizeToContents)  # Modified
        header.setSectionResizeMode(6, QHeaderView.ResizeToContents)  # Matches
        header.setSectionResizeMode(7, QHeaderView.ResizeToContents)  # Tags

        # Context menu
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._show_context_menu)

        layout.addWidget(self.table_view)

        # Pagination controls
        pagination_layout = QHBoxLayout()
        pagination_layout.addStretch()

        self.prev_page_btn = QPushButton("◄ Previous Page")
        self.prev_page_btn.setEnabled(False)
        self.prev_page_btn.clicked.connect(self._load_previous_page)
        pagination_layout.addWidget(self.prev_page_btn)

        self.page_label = QLabel("Page 1")
        self.page_label.setStyleSheet("font-weight: bold; padding: 0 10px;")
        pagination_layout.addWidget(self.page_label)

        self.next_page_btn = QPushButton("Next Page ►")
        self.next_page_btn.setEnabled(False)
        self.next_page_btn.clicked.connect(self._load_next_page)
        pagination_layout.addWidget(self.next_page_btn)

        pagination_layout.addStretch()
        layout.addLayout(pagination_layout)

        # Summary bar
        summary_layout = QHBoxLayout()
        self.summary_label = QLabel()
        summary_layout.addWidget(self.summary_label)

        # Selection actions
        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.clicked.connect(self.model.select_all)
        summary_layout.addWidget(self.select_all_btn)

        self.clear_selection_btn = QPushButton("Clear Selection")
        self.clear_selection_btn.clicked.connect(self.model.clear_selection)
        summary_layout.addWidget(self.clear_selection_btn)

        summary_layout.addStretch()
        layout.addLayout(summary_layout)

        self.setLayout(layout)

    def set_case_data(self, case_data: Optional[CaseDataAccess]) -> None:
        """
        Set the case data access object for tag operations.

        Added for background filter loading.
        """
        self._case_data = case_data

    def refresh(self) -> None:
        """Public refresh method for external calls (matches other tabs pattern).

        Added to support lazy refresh pattern.
        """
        self._load_data()

    def mark_stale(self) -> None:
        """Mark data as stale - will refresh on next showEvent.

        Part of lazy refresh pattern to prevent UI freezes.
        Called by main.py when data changes but tab is not visible.
        """
        self._data_stale = True

    def showEvent(self, event):
        """
        Override showEvent to perform lazy loading when tab becomes visible.

        Changed from immediate reload to deferred loading (Phase 3).
        """
        super().showEvent(event)

        # Phase 3: Lazy loading - load data on first visibility
        if self._load_pending and not self._data_loaded:
            # Use a short timer to let the UI paint first
            QTimer.singleShot(10, self._perform_deferred_load)
        # Refresh if data was marked stale while tab was hidden
        elif self._data_stale and self._data_loaded:
            self._data_stale = False
            QTimer.singleShot(10, self.refresh)

    def _perform_deferred_load(self) -> None:
        """Perform the deferred data loading."""
        if self._data_loaded:
            return

        self._data_loaded = True
        self._load_pending = False

        # Start background filter loading
        # Note: _on_filters_changed() will be called by _on_filters_loaded() after worker completes
        self._load_data()

    def _load_data(self):
        """
        Load initial data and populate filter dropdowns.

        Now uses background loading to prevent UI freeze with large databases.
        """
        # Cancel any existing worker
        if self._filter_worker is not None:
            try:
                self._filter_worker.finished.disconnect()
                self._filter_worker.error.disconnect()
            except RuntimeError:
                pass  # Already disconnected
            self._filter_worker = None

        # Show loading state in dropdowns
        self._set_filters_loading()

        # Start background loading
        self._filter_worker = FileListFilterLoadWorker(
            self.model,
            self._case_data,
            self.evidence_id
        )
        self._filter_worker.finished.connect(self._on_filters_loaded)
        self._filter_worker.error.connect(self._on_filters_load_error)
        self._filter_worker.start()

    def _set_filters_loading(self) -> None:
        """Show loading state in filter dropdowns."""
        for combo in [self.extension_combo, self.matches_combo, self.tags_combo]:
            combo.blockSignals(True)
            combo.clear()
            combo.addItem("Loading...", _LOADING_DATA)
            combo.setEnabled(False)
            combo.blockSignals(False)

    def _on_filters_loaded(self, data: dict) -> None:
        """
        Handle filter loading completion.

        Args:
            data: Dict with extensions, matches, tags lists
        """
        self._filter_worker = None

        # Block signals while repopulating to prevent premature filtering
        self.extension_combo.blockSignals(True)
        self.matches_combo.blockSignals(True)
        self.tags_combo.blockSignals(True)

        filters = self.model.filters
        try:
            # Populate extension combo
            self.extension_combo.clear()
            self.extension_combo.setEnabled(True)
            self.extension_combo.addItem("All", "")

            extensions = data.get("extensions", [])
            for ext in extensions:
                self.extension_combo.addItem(ext, ext)

            # Show truncation indicator if needed
            if data.get("truncated", False):
                total = data.get("extension_count", len(extensions))
                self.extension_combo.addItem(
                    f"... ({total - len(extensions)} more)",
                    _TRUNCATED_DATA
                )
            # Keep the applied selections (issue #73)
            self._select_combo_data(self.extension_combo, filters.extension or "", filters.extension, insert_at=1)

            # Populate matches combo
            self.matches_combo.clear()
            self.matches_combo.setEnabled(True)
            self.matches_combo.addItem("All", "")
            self.matches_combo.addItem("Any Match", FILE_LIST_ANY_MATCH)
            for match in data.get("matches", []):
                self.matches_combo.addItem(match, match)
            self._select_combo_data(self.matches_combo, filters.match_list or "", filters.match_list, insert_at=2)

            # Populate tags combo
            self.tags_combo.clear()
            self.tags_combo.setEnabled(True)
            self._add_fixed_tag_items(self.tags_combo)
            for tag in data.get("tags", []):
                if isinstance(tag, dict):
                    self.tags_combo.addItem(tag.get("name", ""), tag.get("name", ""))
                else:
                    self.tags_combo.addItem(str(tag), str(tag))
            self._select_applied_tag()

            logger.debug(
                "FileListTab filters loaded: %d extensions, %d matches, %d tags",
                len(extensions),
                len(data.get("matches", [])),
                len(data.get("tags", []))
            )
        finally:
            # Restore signals
            self.extension_combo.blockSignals(False)
            self.matches_combo.blockSignals(False)
            self.tags_combo.blockSignals(False)

        self._update_summary()
        self._update_pagination_controls()
        self._update_toolbar_buttons()

        # Now trigger the initial data load (moved from _perform_deferred_load)
        self._on_filters_changed()

    def _on_filters_load_error(self, error_msg: str) -> None:
        """Handle filter loading error."""
        self._filter_worker = None
        logger.error("FileListTab filter load error: %s", error_msg)

        # Show the rows anyway (they are no longer loaded at construction;
        # issue #63), with the applied filters (issue #73)
        self.model.apply_filters()
        self._update_summary()

        # Re-enable dropdowns with error state
        for combo in [self.extension_combo, self.matches_combo, self.tags_combo]:
            combo.blockSignals(True)
            combo.clear()
            combo.addItem("Error loading", _ERROR_DATA)
            combo.setEnabled(True)
            combo.blockSignals(False)

    def _update_toolbar_buttons(self):
        """Update toolbar button enabled states based on selection."""
        has_selection = len(self.model.selected_rows) > 0
        self.tag_btn.setEnabled(has_selection)

    @staticmethod
    def _add_fixed_tag_items(combo: QComboBox) -> None:
        """Add the Tags dropdown's fixed entries; tag names follow them (issue #73)."""
        combo.addItem("All", "")
        combo.addItem("Any Tag", _ANY_TAG)
        combo.addItem("Untagged", _UNTAGGED_TAG)
        # Tell them apart from tags that are themselves named so.
        combo.setItemData(1, "Files with at least one tag", Qt.ToolTipRole)
        combo.setItemData(2, "Files without any tag", Qt.ToolTipRole)

    @staticmethod
    def _select_combo_data(combo: QComboBox, value: Any, label: Optional[str], insert_at: int) -> None:
        """Select the item holding ``value``, inserting it (as ``label``) if missing."""
        index = combo.findData(value)
        if index < 0:
            index = min(insert_at, combo.count())
            combo.insertItem(index, label or str(value), value)
        combo.setCurrentIndex(index)

    def _select_applied_tag(self) -> None:
        """Select the applied tag filter in the (rebuilt) Tags dropdown."""
        filters = self.model.filters
        if filters.untagged:
            value: Any = _UNTAGGED_TAG
        elif filters.any_tag:
            value = _ANY_TAG
        else:
            value = filters.tag_name or ""
        self._select_combo_data(self.tags_combo, value, filters.tag_name, insert_at=3)

    def _filters_from_controls(self) -> FileListFilterSpec:
        """Build the filter spec from the controls.

        Dropdowns showing a loading/error placeholder keep the applied value
        (issue #73).
        """
        changes: Dict[str, Any] = {
            "size_min": int(self.size_combo.currentData()) if self.size_combo.currentData() else None,
            "deleted": self.deleted_combo.currentData(),
            "search": self.search_edit.text().strip(),
            "exclude": self.exclude_edit.text().strip(),
        }
        extension = self.extension_combo.currentData()
        if not _is_placeholder(extension):
            changes["extension"] = extension or ""
        match_list = self.matches_combo.currentData()
        if not _is_placeholder(match_list):
            changes["match_list"] = match_list or ""
        tag = self.tags_combo.currentData()
        if tag is _UNTAGGED_TAG:
            changes.update(tag_name="", any_tag=False, untagged=True)
        elif tag is _ANY_TAG:
            changes.update(tag_name="", any_tag=True, untagged=False)
        elif not _is_placeholder(tag):
            changes.update(tag_name=tag or "", any_tag=False, untagged=False)
        return replace(self.model.filters, **changes)

    def _on_search_clicked(self):
        """Handle search button click - applies all filters including Search and Exclude."""
        self._on_filters_changed()

    def _on_filters_changed(self):
        """Handle filter changes (Phase 1: with progress feedback)."""
        # Phase 1: Show cursor feedback for long operations
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            self.model.apply_filters(self._filters_from_controls())
            self._update_summary()
            self._update_pagination_controls()
        finally:
            QApplication.restoreOverrideCursor()

    def _update_summary(self):
        """Update summary statistics (Phase 1: optimized for pagination)."""
        loaded_files = len(self.model._rows)  # Currently loaded
        total_files = self.model._total_rows  # Total in database
        selected_files = len(self.model.selected_rows)

        # Phase 1: Show loaded vs total for pagination awareness
        if total_files > loaded_files:
            summary = f"{total_files:,} files (showing {loaded_files:,})"
        else:
            summary = f"{total_files:,} files"

        if selected_files > 0:
            summary += f" | {selected_files:,} selected"

        # Show filter status instead of counts (Phase 1 optimization)
        if self.model.filters.tag_filtered:
            summary += " | 🏷️ filtered"
        if self.model.filters.match_list:
            summary += " | 🎯 filtered"

        self.summary_label.setText(summary)

        # Update pagination controls
        self._update_pagination_controls()

    def _update_pagination_controls(self):
        """Update pagination button states and label."""
        # Calculate current page (1-indexed)
        if self.model._loaded_rows == 0:
            current_page = 0
        else:
            current_page = ((self.model._loaded_rows - 1) // self.model.PAGE_SIZE) + 1

        # Calculate total pages
        if self.model._total_rows == 0:
            total_pages = 0
        else:
            total_pages = ((self.model._total_rows - 1) // self.model.PAGE_SIZE) + 1

        # Update page label
        if self.model._total_rows > 0:
            self.page_label.setText(f"Page {current_page} of {total_pages}")
        else:
            self.page_label.setText("No data")

        # Enable/disable buttons
        self.prev_page_btn.setEnabled(current_page > 1)
        self.next_page_btn.setEnabled(self.model.canFetchMore())

    def _load_next_page(self):
        """Load the next page of data."""
        if not self.model.canFetchMore():
            return

        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            # Use Qt's fetchMore mechanism
            self.model.fetchMore()
            self._update_summary()

            # Scroll to the newly loaded data
            if self.model.rowCount() > 0:
                # Scroll to first row of new page
                new_page_start = self.model._loaded_rows - self.model.PAGE_SIZE
                index = self.model.index(new_page_start, 0)
                self.table_view.scrollTo(index)
        finally:
            QApplication.restoreOverrideCursor()

    def _load_previous_page(self):
        """Load the previous page of data (scroll back)."""
        if self.model._loaded_rows <= self.model.PAGE_SIZE:
            return

        # Scroll to the previous page
        prev_page_start = max(0, self.model._loaded_rows - (2 * self.model.PAGE_SIZE))
        index = self.model.index(prev_page_start, 0)
        self.table_view.scrollTo(index)

        # Update controls
        self._update_summary()

    def match_reference_lists(self):
        """Run reference list matching - ONLY against file lists."""
        # Check if file_list table has any entries
        if self.model.rowCount() == 0:
            QMessageBox.information(
                self,
                "No File List Data",
                "No file list entries found. Import a file list CSV first before matching."
            )
            return

        # Get available file lists (exclude hash lists)
        available = self.ref_manager.list_available()
        available_filelists = available.get("filelists", [])

        if not available_filelists:
            QMessageBox.information(
                self, "No Reference Lists",
                "No file lists found. Create some file lists in Settings first."
            )
            return

        # Show selection dialog
        dialog = ReferenceListSelectorDialog(available_filelists, self)
        if dialog.exec() != QDialog.Accepted:
            return

        selected_names = dialog.get_selected_lists()
        if not selected_names:
            QMessageBox.information(
                self, "No Selection",
                "Please select at least one file list to match against."
            )
            return

        # Convert to (list_type, name) tuples - only filelists
        selected_lists = [("filelist", name) for name in selected_names]

        try:
            # Create and start worker (db_manager is thread-safe, worker creates connection)
            self.match_worker = MatchWorker(
                self.db_manager, self.evidence_id, selected_lists
            )

            # Progress dialog
            self.progress_dialog = QProgressDialog(
                "Matching against reference lists...", "Cancel", 0, 100, self
            )
            self.progress_dialog.setWindowModality(Qt.WindowModal)
            self.progress_dialog.setAutoClose(True)

            # Connect signals
            self.match_worker.progress.connect(self._update_match_progress)
            self.match_worker.finished.connect(self._match_finished)
            self.match_worker.error.connect(self._match_error)
            self.progress_dialog.canceled.connect(self._cancel_match_worker)

            self.match_worker.start()
            self.progress_dialog.show()

        except Exception as e:
            QMessageBox.critical(self, "Match Error", f"Failed to start matching: {e}")

    def _cancel_match_worker(self) -> None:
        """Gracefully cancel match worker when user clicks Cancel."""
        if hasattr(self, 'match_worker') and self.match_worker is not None:
            if self.match_worker.isRunning():
                self.match_worker.requestInterruption()
                self.match_worker.quit()
                if not self.match_worker.wait(1000):
                    logger.warning("MatchWorker did not stop on cancel, terminating")
                    self.match_worker.terminate()
                    self.match_worker.wait(500)
            self.match_worker = None

    def _update_match_progress(self, current, total):
        """Update match progress dialog."""
        if total > 0:
            progress = int((current / total) * 100)
            self.progress_dialog.setValue(progress)
            self.progress_dialog.setLabelText(f"Matching... {progress}%")

    def _match_finished(self, results):
        """Handle match completion."""
        self.progress_dialog.close()

        # Clean up worker thread
        if hasattr(self, 'match_worker') and self.match_worker is not None:
            self.match_worker.wait(1000)  # Wait up to 1 second for thread to finish
            self.match_worker.deleteLater()
            self.match_worker = None

        total_matches = sum(results.values())
        message = f"Matching completed!\n\nTotal matches found: {total_matches:,}\n\n"

        # Show results for each list (sorted by name)
        for list_name in sorted(results.keys()):
            count = results[list_name]
            message += f"• {list_name}: {count:,} matches\n"

        QMessageBox.information(self, "Matching Complete", message)

        # Defer refresh to allow UI to settle after message box closes.
        # This prevents UI freeze by allowing the event loop to process
        # pending events before starting the synchronous refresh.
        QTimer.singleShot(0, self._refresh_after_match)

    def _refresh_after_match(self):
        """Refresh data and filter dropdowns after matching (deferred, async)."""
        # Start background worker for model data refresh
        self._refresh_worker = ModelRefreshWorker(
            self.model.data_access,
            self.evidence_id,
            self.model.filters,
        )

        # Show progress dialog
        self._refresh_progress = QProgressDialog(
            "Refreshing file list...", None, 0, 0, self
        )
        self._refresh_progress.setWindowModality(Qt.WindowModal)
        self._refresh_progress.setCancelButton(None)  # Not cancellable
        self._refresh_progress.setMinimumDuration(0)  # Show immediately

        # Connect signals
        self._refresh_worker.finished.connect(self._on_refresh_finished)
        self._refresh_worker.error.connect(self._on_refresh_error)

        self._refresh_worker.start()
        self._refresh_progress.show()

    def _on_refresh_finished(self, rows: list, total_rows: int):
        """Handle model refresh completion."""
        self._refresh_progress.close()

        # Clean up worker
        if hasattr(self, '_refresh_worker') and self._refresh_worker is not None:
            self._refresh_worker.wait(1000)
            self._refresh_worker.deleteLater()
            self._refresh_worker = None

        # Update model with loaded data
        self.model.set_data(rows, total_rows)

        # Update UI
        self._update_summary()
        self._update_pagination_controls()

        # Refresh filter dropdowns (background worker)
        self._load_data()

    def _on_refresh_error(self, error_msg: str):
        """Handle model refresh error."""
        self._refresh_progress.close()

        # Clean up worker
        if hasattr(self, '_refresh_worker') and self._refresh_worker is not None:
            self._refresh_worker.wait(1000)
            self._refresh_worker.deleteLater()
            self._refresh_worker = None

        QMessageBox.warning(self, "Refresh Error", f"Failed to refresh file list: {error_msg}")

    def _match_error(self, error_msg):
        """Handle match error."""
        self.progress_dialog.close()

        # Clean up worker thread
        if hasattr(self, 'match_worker') and self.match_worker is not None:
            self.match_worker.wait(1000)  # Wait up to 1 second for thread to finish
            self.match_worker.deleteLater()
            self.match_worker = None

        QMessageBox.critical(self, "Match Error", f"Matching failed: {error_msg}")

    def export_csv(self):
        """Export filtered file list to CSV."""
        if self.model.rowCount() == 0:
            QMessageBox.information(self, "No Data", "No files to export.")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export File List",
            f"file_list_EV{self.evidence_id:03d}.csv",
            "CSV Files (*.csv)"
        )

        if not file_path:
            return

        try:
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                # Write headers
                headers = ["File Path", "File Name", "Extension", "Size (bytes)",
                          "Modified", "Deleted", "Matches", "Tags"]
                writer.writerow(headers)

                # Phase 1: Export only loaded rows (pagination-aware)
                # Note: For full export, would need to load all pages
                for i in range(len(self.model._rows)):
                    row_data = self.model._rows[i]
                    file_id = row_data.get("id")

                    # Load matches and tags on-demand
                    matches = self.model._get_matches(file_id) if file_id else ""
                    tags = self.model._get_tags(file_id) if file_id else ""

                    writer.writerow([
                        row_data.get("file_path", ""),
                        row_data.get("file_name", ""),
                        row_data.get("extension", ""),
                        row_data.get("size_bytes", ""),
                        row_data.get("modified_ts", ""),
                        "Yes" if row_data.get("deleted") else "No",
                        matches,
                        tags,
                    ])

            exported_count = len(self.model._rows)
            total_count = self.model.rowCount()

            if exported_count < total_count:
                msg = (
                    f"Exported {exported_count:,} loaded files to {file_path}\n\n"
                    f"Note: {total_count - exported_count:,} files not yet loaded were skipped.\n"
                    f"Scroll to load more data before exporting for complete results."
                )
            else:
                msg = f"Exported {exported_count:,} files to {file_path}"

            QMessageBox.information(self, "Export Complete", msg)

        except Exception as e:
            QMessageBox.critical(self, "Export Error", f"Export failed: {e}")

    def _show_context_menu(self, position):
        """Show context menu for table."""
        if self.model.rowCount() == 0:
            return

        # Lazy check: resolve evidence source availability once
        if self._evidence_has_source is None:
            self._check_evidence_source()

        menu = QMenu(self)

        selected_count = len(self.model.selected_rows)
        if selected_count > 0:
            menu.addAction(f"{selected_count:,} file(s) selected").setEnabled(False)
            menu.addSeparator()
            tag_action = menu.addAction("Tag selected...")

            # Export action — only when evidence image is accessible
            export_action = None
            if self._evidence_has_source:
                export_action = menu.addAction(
                    f"Export {selected_count:,} file(s) from evidence…"
                )

            action = menu.exec(self.table_view.mapToGlobal(position))
            if action == tag_action:
                self._tag_selected()
            elif export_action is not None and action == export_action:
                self._export_selected_files()
        else:
            menu.addAction("No files selected").setEnabled(False)
            menu.exec(self.table_view.mapToGlobal(position))

    def _tag_selected(self):
        """Open dialog to tag selected files."""
        selected_ids = self.model.get_selected_ids()
        if not selected_ids:
            return

        # Create a temporary CaseDataAccess since FileListTab manages its own DB connection
        # Use context manager to ensure connections are closed
        with CaseDataAccess(self.case_folder, db_path=self.case_db_path) as case_data:
            dialog = TagArtifactsDialog(case_data, self.evidence_id, "file_list", selected_ids, self)
            dialog.tags_changed.connect(self._load_tags)
            if dialog.exec():
                # Refresh data
                self.model.apply_filters()
                self._update_summary()
                self._load_data()

    # ------------------------------------------------------------------
    # Evidence file export
    # ------------------------------------------------------------------

    def _check_evidence_source(self) -> None:
        """Resolve whether the evidence has an accessible source on disk.

        Sets ``self._evidence_has_source`` so the context-menu can decide
        whether to show the export action.
        """
        try:
            with CaseDataAccess(self.case_folder, db_path=self.case_db_path) as cd:
                ev = cd.get_evidence(self.evidence_id)
            self._evidence_has_source = evidence_has_source(ev)
        except Exception:
            logger.debug("Could not check evidence source", exc_info=True)
            self._evidence_has_source = False

    def _export_selected_files(self) -> None:
        """Export selected file-list entries from the evidence image."""
        file_info = self.model.get_selected_file_info()
        if not file_info:
            return

        # Prompt for destination
        dest_dir = QFileDialog.getExistingDirectory(
            self,
            "Export files to…",
            str(Path.home()),
        )
        if not dest_dir:
            return
        dest_path = Path(dest_dir)

        # Build request list
        requests = [
            FileExportRequest(
                file_path=fi["file_path"],
                partition_index=fi.get("partition_index"),
            )
            for fi in file_info
        ]

        # Get evidence metadata
        with CaseDataAccess(self.case_folder, db_path=self.case_db_path) as cd:
            evidence = cd.get_evidence(self.evidence_id)
        if evidence is None:
            QMessageBox.critical(self, "Export Error", "Evidence not found.")
            return

        # Create and run the background task
        task = FileExportTask(evidence, requests, dest_path)
        self._export_task = task  # prevent GC

        # Wire up progress dialog
        total = len(requests)
        progress = QProgressDialog(
            f"Exporting 0 / {total} files…",
            "Cancel",
            0,
            100,
            self,
        )
        progress.setWindowTitle("Exporting files from evidence")
        progress.setMinimumDuration(0)
        progress.setValue(0)

        def _on_progress(percent: int, message: str) -> None:
            if progress.wasCanceled():
                task.cancel()
                return
            progress.setValue(percent)
            progress.setLabelText(message)

        def _on_result(result: object) -> None:
            progress.close()
            self._log_export(result, dest_path)
            # Ask whether to open the destination folder
            buttons = QMessageBox.Open | QMessageBox.Close
            answer = QMessageBox.information(
                self,
                "Export Complete",
                result.summary_message(),
                buttons,
                QMessageBox.Open,
            )
            if answer == QMessageBox.Open:
                QDesktopServices.openUrl(QUrl.fromLocalFile(str(dest_path)))
            self._export_task = None

        def _on_error(error: str, _tb: str) -> None:
            progress.close()
            QMessageBox.critical(self, "Export Error", f"Export failed:\n{error}")
            self._export_task = None

        task.signals.progress.connect(_on_progress)
        task.signals.result.connect(_on_result)
        task.signals.error.connect(_on_error)
        task.signals.finished.connect(lambda: progress.close())

        progress.canceled.connect(task.cancel)
        QThreadPool.globalInstance().start(task)

    def _log_export(self, result: object, dest_path: Path) -> None:
        """Write a process_log entry for the export action."""
        try:
            from core.database import create_process_log, finalize_process_log

            evidence_conn = self.db_manager.get_evidence_conn(
                self.evidence_id, label=self._get_evidence_label()
            )
            log_id = create_process_log(
                evidence_conn,
                self.evidence_id,
                "file_export",
                f"Exported {result.total} file(s) to {dest_path}",
            )
            finalize_process_log(
                evidence_conn,
                log_id,
                exit_code=0 if result.failed == 0 else 1,
                stdout=f"exported={result.exported}, failed={result.failed}",
                stderr=("\n".join(result.errors) if result.errors else None),
            )
        except Exception:
            logger.warning("Failed to log export action", exc_info=True)
        # The Overview's last run reads process_log (issue #63)
        if self._case_data is not None:
            self._case_data.invalidate_stats(int(self.evidence_id), STATS_PROCESS_LOG)

    def _load_tags(self):
        """Load tags into the filter combo box."""
        self.tags_combo.blockSignals(True)
        self.tags_combo.clear()
        self._add_fixed_tag_items(self.tags_combo)

        # Use CaseDataAccess to list tags
        # Use context manager to ensure connections are closed
        with CaseDataAccess(self.case_folder, db_path=self.case_db_path) as case_data:
            tags = case_data.list_tags(self.evidence_id)

        for tag in tags:
            name = tag["name"]
            count = tag["usage_count"]
            self.tags_combo.addItem(f"{name} ({count})", userData=name)

        # Keep the applied selection (issue #73)
        self._select_applied_tag()

        self.tags_combo.blockSignals(False)

    def shutdown(self) -> None:
        """
        Gracefully stop all background workers before widget destruction.

        Called by MainWindow.closeEvent() and _on_close_evidence_tab() to prevent
        Qt abort from destroying QThread while still running.
        """
        # Stop filter worker
        if self._filter_worker is not None:
            try:
                self._filter_worker.finished.disconnect()
                self._filter_worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            if self._filter_worker.isRunning():
                self._filter_worker.requestInterruption()
                self._filter_worker.quit()
                if not self._filter_worker.wait(2000):
                    logger.warning("FileListFilterLoadWorker did not stop in 2s, terminating")
                    self._filter_worker.terminate()
                    self._filter_worker.wait(500)
            self._filter_worker = None

        # Stop import worker
        if hasattr(self, 'import_worker') and self.import_worker is not None:
            try:
                self.import_worker.progress.disconnect()
                self.import_worker.finished.disconnect()
                self.import_worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            if self.import_worker.isRunning():
                self.import_worker.requestInterruption()
                self.import_worker.quit()
                if not self.import_worker.wait(2000):
                    logger.warning("ImportWorker did not stop in 2s, terminating")
                    self.import_worker.terminate()
                    self.import_worker.wait(500)
            self.import_worker = None

        # Stop match worker
        if hasattr(self, 'match_worker') and self.match_worker is not None:
            try:
                self.match_worker.progress.disconnect()
                self.match_worker.finished.disconnect()
                self.match_worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            if self.match_worker.isRunning():
                self.match_worker.requestInterruption()
                self.match_worker.quit()
                if not self.match_worker.wait(2000):
                    logger.warning("MatchWorker did not stop in 2s, terminating")
                    self.match_worker.terminate()
                    self.match_worker.wait(500)
            self.match_worker = None

        # Close progress dialog if open
        if hasattr(self, 'progress_dialog') and self.progress_dialog is not None:
            try:
                self.progress_dialog.close()
            except RuntimeError:
                pass
            self.progress_dialog = None

        logger.debug("FileListTab shutdown complete")