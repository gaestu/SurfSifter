"""
Qt models for file list data.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QIcon

from app.data import FileListFilterSpec
from app.data.case_data import CaseDataAccess
from core.database import DatabaseManager

logger = logging.getLogger(__name__)


class FileListModel(QAbstractTableModel):
    """Qt model for file_list table with filters and bulk selection."""

    # Pagination settings (Phase 1 performance optimization)
    PAGE_SIZE = 10000  # Load 10k rows at a time to prevent memory exhaustion

    # Column definitions
    COLUMNS = [
        "checkbox",
        "file_path",
        "file_name",
        "extension",
        "size_bytes",
        "modified_ts",
        "matches",
        "tags"
    ]

    HEADERS = [
        "Select",
        "File Path",
        "File Name",
        "Extension",
        "Size",
        "Modified",
        "Matches",
        "Tags"
    ]

    def __init__(
        self,
        case_folder: str,
        evidence_id: int,
        case_db_path: Path,
        parent=None,
        *,
        load: bool = True,
    ):
        """
        Initialize file list model.

        Args:
            case_folder: Path to case folder
            evidence_id: Evidence ID
            case_db_path: Path to case database file
            parent: Parent widget
            load: Load the first page now (False: on the first ``apply_filters``)
        """
        super().__init__(parent)
        self.case_folder = Path(case_folder)
        self.evidence_id = evidence_id
        self.case_db_path = case_db_path
        self.db_manager = DatabaseManager(self.case_folder, case_db_path=case_db_path)
        # All queries go through the data layer (issue #73)
        self.data_access = CaseDataAccess(self.case_folder, db_manager=self.db_manager)

        # Data storage (paginated)
        self._rows: List[Dict[str, Any]] = []
        self._total_rows: int = 0  # Total count across all pages
        self._loaded_rows: int = 0  # Number of rows currently loaded
        self.selected_rows: Set[int] = set()  # Set of row indices

        # Caches for on-demand loading (Phase 1 optimization)
        self._matches_cache: Dict[int, str] = {}  # file_id -> matches string
        self._tags_cache: Dict[int, str] = {}  # file_id -> tags string

        # Applied filters
        self._filters = FileListFilterSpec()

        if load:
            self._load_data()

    def _load_data(self, append: bool = False) -> None:
        """
        Load file_list data with pagination (Phase 1 optimization).

        Args:
            append: If True, append to existing rows (for fetchMore).
                   If False, reset and load from start.
        """
        import time  # Phase 2: Add performance logging
        start_time = time.time()

        try:
            if not append:
                logger.debug(f"Loading file list data for evidence_id={self.evidence_id}")
                self._rows = []
                self._loaded_rows = 0
                self._matches_cache.clear()
                self._tags_cache.clear()

            # Phase 1: Count total rows first (fast with indexes)
            if not append:
                count_start = time.time()
                self._total_rows = self.data_access.count_file_list(self.evidence_id, self._filters)
                count_time = time.time() - count_start
                logger.info(f"Total rows matching filters: {self._total_rows} (count took {count_time:.3f}s)")

            # Phase 1: Load only one page at a time (no GROUP_CONCAT!)
            offset = self._loaded_rows
            page_start = time.time()
            logger.debug(f"Loading page at offset {offset}, limit {self.PAGE_SIZE}")
            # matches and tags loaded on-demand via _get_matches()/_get_tags()
            rows = self.data_access.list_file_list_rows(
                self.evidence_id, self._filters, offset=offset, limit=self.PAGE_SIZE
            )
            self._rows.extend(rows)
            rows_loaded = len(rows)
            self._loaded_rows += rows_loaded

            page_time = time.time() - page_start
            total_time = time.time() - start_time
            logger.info(
                f"Loaded {rows_loaded} rows (total loaded: {self._loaded_rows}/{self._total_rows}), "
                f"page query: {page_time:.3f}s, total: {total_time:.3f}s"
            )

            if self._total_rows == 0:
                # Debug: Check if table has any data at all
                logger.warning(
                    f"Total rows in file_list table for evidence_id={self.evidence_id}: "
                    f"{self.data_access.count_file_list(self.evidence_id)}"
                )

        except Exception as e:
            logger.error(f"Failed to load file list data: {e}", exc_info=True)
            self._rows = []
            self._total_rows = 0
            self._loaded_rows = 0

    def _get_matches(self, file_id: int) -> str:
        """
        Load matches for a file on-demand (Phase 1 optimization).

        Args:
            file_id: File list ID

        Returns:
            Comma-separated list of matched reference lists
        """
        if file_id not in self._matches_cache:
            try:
                self._matches_cache[file_id] = self.data_access.get_file_list_matches_str(
                    self.evidence_id, file_id
                )
            except Exception as e:
                logger.error(f"Failed to load matches for file {file_id}: {e}")
                self._matches_cache[file_id] = ""

        return self._matches_cache[file_id]

    def _get_tags(self, file_id: int) -> str:
        """
        Load tags for a file on-demand (Phase 1 optimization).

        Args:
            file_id: File list ID

        Returns:
            Comma-separated list of tags
        """
        if file_id not in self._tags_cache:
            try:
                # Same definition as the Tags filters (issue #73)
                self._tags_cache[file_id] = self.data_access.get_artifact_tags_str(
                    self.evidence_id, "file_list", file_id
                )
            except Exception as e:
                logger.error(f"Failed to load tags for file {file_id}: {e}")
                self._tags_cache[file_id] = ""

        return self._tags_cache[file_id]

    @property
    def filters(self) -> FileListFilterSpec:
        """The applied filters."""
        return self._filters

    def apply_filters(self, filters: Optional[FileListFilterSpec] = None) -> None:
        """
        Apply filters and reload data.

        Args:
            filters: New filters (``None`` keeps the applied ones)
        """
        self.beginResetModel()
        if filters is not None:
            self._filters = filters
        self.selected_rows.clear()  # Clear selection when filtering
        self._load_data()
        self.endResetModel()

    def apply_filters_async(self, filters: FileListFilterSpec) -> None:
        """
        Apply filters without loading data (for async refresh).

        Call set_data() afterwards when data is available from background worker.

        Args:
            filters: New filters
        """
        self._filters = filters
        self.selected_rows.clear()

    def set_data(self, rows: List[Dict[str, Any]], total_rows: int) -> None:
        """
        Set model data from external source (e.g., background worker).

        Args:
            rows: List of row dictionaries
            total_rows: Total row count for pagination
        """
        self.beginResetModel()
        self._rows = rows
        self._loaded_rows = len(rows)
        self._total_rows = total_rows
        self._matches_cache.clear()
        self._tags_cache.clear()
        self.endResetModel()

    def get_selected_ids(self) -> List[int]:
        """
        Get file_list IDs of selected rows.

        Returns:
            List of file_list IDs
        """
        return [self._rows[row_idx]["id"] for row_idx in self.selected_rows]

    def get_selected_file_info(self) -> List[Dict[str, Any]]:
        """Return ``file_path`` and ``partition_index`` for each selected row.

        Used by the file-export handler to know *which* files to read from
        the evidence image and on *which* partition they reside.

        Returns:
            List of dicts, each with keys ``file_path`` and ``partition_index``.
        """
        return [
            {
                "file_path": self._rows[idx]["file_path"],
                "partition_index": self._rows[idx].get("partition_index"),
            }
            for idx in sorted(self.selected_rows)
            if 0 <= idx < len(self._rows)
        ]

    def toggle_selection(self, row_idx: int) -> None:
        """
        Toggle selection state of a row.

        Args:
            row_idx: Row index
        """
        if 0 <= row_idx < len(self._rows):
            if row_idx in self.selected_rows:
                self.selected_rows.remove(row_idx)
            else:
                self.selected_rows.add(row_idx)

            # Emit data changed for checkbox column
            index = self.createIndex(row_idx, 0)  # Checkbox column
            self.dataChanged.emit(index, index, [Qt.CheckStateRole])

    def select_all(self) -> None:
        """Select all visible rows (Phase 1: only loaded rows)."""
        self.beginResetModel()
        self.selected_rows = set(range(len(self._rows)))
        self.endResetModel()

    def clear_selection(self) -> None:
        """Clear all selections."""
        self.beginResetModel()
        self.selected_rows.clear()
        self.endResetModel()

    # Qt Model Interface (with Phase 1 pagination support)

    def rowCount(self, parent=QModelIndex()) -> int:
        """
        Return number of currently loaded rows (Phase 1: proper lazy loading).

        Qt will call canFetchMore() when user scrolls near the end.
        """
        if parent.isValid():
            return 0
        return len(self._rows)  # Return loaded count, not total

    def canFetchMore(self, parent=QModelIndex()) -> bool:
        """
        Check if more data is available to load (Phase 1 pagination).

        Returns:
            True if there are more rows to load beyond what's currently loaded
        """
        if parent.isValid():
            return False
        return self._loaded_rows < self._total_rows

    def fetchMore(self, parent=QModelIndex()) -> None:
        """
        Load next page of data (Phase 1 pagination).

        Called automatically by QTableView when user scrolls near the bottom.
        """
        if parent.isValid():
            return

        if self._loaded_rows >= self._total_rows:
            return

        # Calculate how many rows to fetch
        remainder = self._total_rows - self._loaded_rows
        rows_to_fetch = min(remainder, self.PAGE_SIZE)

        # Store current row count
        current_count = len(self._rows)

        logger.debug(f"fetchMore: loading up to {rows_to_fetch} more rows (current: {current_count})")

        # Notify Qt we're about to insert rows (optimistic - we'll adjust if needed)
        self.beginInsertRows(QModelIndex(), current_count, current_count + rows_to_fetch - 1)

        # Load next page (append=True)
        self._load_data(append=True)

        # Complete the insertion
        self.endInsertRows()

        # Log actual result
        rows_added = len(self._rows) - current_count
        logger.debug(f"Added {rows_added} rows (total loaded: {len(self._rows)}/{self._total_rows})")

    def columnCount(self, parent=QModelIndex()) -> int:
        """Return number of columns."""
        return len(self.COLUMNS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        """Return data for given index and role (Phase 1: lazy-load matches/tags)."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None

        row_data = self._rows[index.row()]
        column = self.COLUMNS[index.column()]

        if role == Qt.DisplayRole:
            if column == "checkbox":
                return None  # Checkbox handled by CheckStateRole
            elif column == "size_bytes":
                size = row_data.get("size_bytes", 0)
                if size == 0:
                    return ""
                # Format size in human readable format
                return self._format_size(size)
            elif column == "modified_ts":
                timestamp = row_data.get("modified_ts", "")
                if timestamp:
                    try:
                        # Parse ISO timestamp
                        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        return dt.strftime("%Y-%m-%d %H:%M")
                    except (ValueError, AttributeError):
                        return timestamp
                return ""
            elif column == "matches":
                # Phase 1: Load matches on-demand
                file_id = row_data.get("id")
                matches = self._get_matches(file_id)
                if matches:
                    # Limit display to first 3 matches
                    match_list = matches.split(",")
                    if len(match_list) > 3:
                        return f"{', '.join(match_list[:3])}... (+{len(match_list)-3})"
                    return matches
                return ""
            elif column == "tags":
                # Phase 1: Load tags on-demand
                file_id = row_data.get("id")
                tags = self._get_tags(file_id)
                if tags:
                    return f"🏷️ {tags}"
                return ""
            else:
                return row_data.get(column, "")

        elif role == Qt.CheckStateRole and column == "checkbox":
            return Qt.Checked if index.row() in self.selected_rows else Qt.Unchecked

        elif role == Qt.ToolTipRole:
            if column == "file_path":
                return row_data.get("file_path", "")
            elif column == "matches":
                # Phase 1: Load matches on-demand for tooltip
                file_id = row_data.get("id")
                matches = self._get_matches(file_id)
                if matches:
                    return f"Matched lists: {matches}"
            elif column == "tags":
                # Phase 1: Load tags on-demand for tooltip
                file_id = row_data.get("id")
                tags = self._get_tags(file_id)
                if tags:
                    return f"Tags: {tags}"

        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlags:
        """Return item flags."""
        if not index.isValid():
            return Qt.NoItemFlags

        flags = Qt.ItemIsEnabled | Qt.ItemIsSelectable

        # Make checkbox column checkable
        if self.COLUMNS[index.column()] == "checkbox":
            flags |= Qt.ItemIsUserCheckable

        return flags

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.EditRole) -> bool:
        """Set data for given index."""
        if not index.isValid() or index.row() >= len(self._rows):
            return False

        column = self.COLUMNS[index.column()]

        if role == Qt.CheckStateRole and column == "checkbox":
            self.toggle_selection(index.row())
            return True

        return False

    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format."""
        if size_bytes == 0:
            return "0 B"

        units = ["B", "KB", "MB", "GB", "TB"]
        unit_index = 0
        size = float(size_bytes)

        while size >= 1024 and unit_index < len(units) - 1:
            size /= 1024
            unit_index += 1

        if unit_index == 0:
            return f"{int(size)} {units[unit_index]}"
        else:
            return f"{size:.1f} {units[unit_index]}"

    def get_filter_values(self) -> Dict[str, List[str]]:
        """
        Get available values for filter dropdowns (uses the filter cache if present).

        Returns:
            Dictionary with lists of available extensions, matches, tags
        """
        try:
            return self.data_access.list_file_list_filter_values(self.evidence_id)
        except Exception as e:
            logger.error(f"Failed to get filter values: {e}")
            return {"extensions": [], "matches": [], "tags": []}
