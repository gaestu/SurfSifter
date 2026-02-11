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

    def __init__(self, case_folder: str, evidence_id: int, case_db_path: Path, parent=None):
        """
        Initialize file list model.

        Args:
            case_folder: Path to case folder
            evidence_id: Evidence ID
            case_db_path: Path to case database file
            parent: Parent widget
        """
        super().__init__(parent)
        self.case_folder = Path(case_folder)
        self.evidence_id = evidence_id
        self.case_db_path = case_db_path
        self.db_manager = DatabaseManager(self.case_folder, case_db_path=case_db_path)

        # Cache evidence label to avoid repeated queries
        self._evidence_label_cache: Optional[str] = None

        # Data storage (paginated)
        self._rows: List[Dict[str, Any]] = []
        self._total_rows: int = 0  # Total count across all pages
        self._loaded_rows: int = 0  # Number of rows currently loaded
        self.selected_rows: Set[int] = set()  # Set of row indices

        # Caches for on-demand loading (Phase 1 optimization)
        self._matches_cache: Dict[int, str] = {}  # file_id -> matches string
        self._tags_cache: Dict[int, str] = {}  # file_id -> tags string

        # Filters
        self._filters = {
            "extension": "",  # All extensions
            "size_min": None,
            "size_max": None,
            "deleted": "all",  # "all", "show_only", "hide"
            "matches": "",  # All lists, or specific list name
            "tags": "",  # All tags, or specific tag name
            "search": "",  # Search in filename/path
        }

        self._load_data()

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

            evidence_conn = self.db_manager.get_evidence_conn(
                self.evidence_id, label=self._get_evidence_label()
            )

            # Build filter clauses
            filter_clauses = []
            params = [self.evidence_id]

            if self._filters["extension"]:
                filter_clauses.append("fl.extension = ?")
                params.append(self._filters["extension"])

            if self._filters["size_min"] is not None:
                filter_clauses.append("fl.size_bytes >= ?")
                params.append(self._filters["size_min"])

            if self._filters["size_max"] is not None:
                filter_clauses.append("fl.size_bytes <= ?")
                params.append(self._filters["size_max"])

            if self._filters["deleted"] == "show_only":
                filter_clauses.append("fl.deleted = 1")
            elif self._filters["deleted"] == "hide":
                filter_clauses.append("fl.deleted = 0")

            # For matches/tags filters, we need subqueries since we removed JOINs
            if self._filters["matches"]:
                if self._filters["matches"] == "any":
                    filter_clauses.append(
                        "EXISTS (SELECT 1 FROM file_list_matches WHERE file_list_id = fl.id)"
                    )
                else:
                    filter_clauses.append(
                        "EXISTS (SELECT 1 FROM file_list_matches WHERE file_list_id = fl.id AND reference_list_name = ?)"
                    )
                    params.append(self._filters["matches"])

            if self._filters["tags"]:
                if self._filters["tags"] == "any":
                    filter_clauses.append(
                        """EXISTS (
                            SELECT 1 FROM tag_associations ta
                            WHERE ta.artifact_type = 'file_list'
                            AND ta.artifact_id = fl.id
                        )"""
                    )
                else:
                    filter_clauses.append(
                        """EXISTS (
                            SELECT 1 FROM tag_associations ta
                            JOIN tags t ON ta.tag_id = t.id
                            WHERE ta.artifact_type = 'file_list'
                            AND ta.artifact_id = fl.id
                            AND t.name = ?
                        )"""
                    )
                    params.append(self._filters["tags"])

            if self._filters["search"]:
                filter_clauses.append("(fl.file_name LIKE ? OR fl.file_path LIKE ?)")
                search_term = f"%{self._filters['search']}%"
                params.extend([search_term, search_term])

            # Build filter clause
            filter_clause = ""
            if filter_clauses:
                filter_clause = "AND " + " AND ".join(filter_clauses)

            # Phase 1: Count total rows first (fast with indexes)
            if not append:
                count_start = time.time()
                count_query = f"""
                    SELECT COUNT(*)
                    FROM file_list fl
                    WHERE fl.evidence_id = ?
                    {filter_clause}
                """
                self._total_rows = evidence_conn.execute(count_query, params).fetchone()[0]
                count_time = time.time() - count_start
                logger.info(f"Total rows matching filters: {self._total_rows} (count took {count_time:.3f}s)")

            # Phase 1: Load only one page at a time (no GROUP_CONCAT!)
            offset = self._loaded_rows
            page_start = time.time()
            data_query = f"""
                SELECT
                    fl.id,
                    fl.file_path,
                    fl.file_name,
                    fl.extension,
                    fl.size_bytes,
                    fl.modified_ts,
                    fl.deleted
                FROM file_list fl
                WHERE fl.evidence_id = ?
                {filter_clause}
                ORDER BY fl.file_path
                LIMIT {self.PAGE_SIZE} OFFSET {offset}
            """

            logger.debug(f"Loading page at offset {offset}, limit {self.PAGE_SIZE}")
            cursor = evidence_conn.execute(data_query, params)

            rows_loaded = 0
            for row in cursor.fetchall():
                self._rows.append({
                    "id": row[0],
                    "file_path": row[1] or "",
                    "file_name": row[2] or "",
                    "extension": row[3] or "",
                    "size_bytes": row[4] or 0,
                    "modified_ts": row[5] or "",
                    "deleted": bool(row[6]),
                    # matches and tags loaded on-demand via _get_matches()/_get_tags()
                })
                rows_loaded += 1

            self._loaded_rows += rows_loaded

            page_time = time.time() - page_start
            total_time = time.time() - start_time
            logger.info(
                f"Loaded {rows_loaded} rows (total loaded: {self._loaded_rows}/{self._total_rows}), "
                f"page query: {page_time:.3f}s, total: {total_time:.3f}s"
            )

            if self._total_rows == 0:
                # Debug: Check if table has any data at all
                count_result = evidence_conn.execute(
                    "SELECT COUNT(*) FROM file_list WHERE evidence_id = ?",
                    [self.evidence_id]
                ).fetchone()
                logger.warning(
                    f"Total rows in file_list table for evidence_id={self.evidence_id}: "
                    f"{count_result[0] if count_result else 0}"
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
                evidence_conn = self.db_manager.get_evidence_conn(
                    self.evidence_id, label=self._get_evidence_label()
                )
                result = evidence_conn.execute(
                    "SELECT GROUP_CONCAT(reference_list_name) FROM file_list_matches WHERE file_list_id = ?",
                    (file_id,)
                ).fetchone()
                self._matches_cache[file_id] = result[0] if result and result[0] else ""
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
                evidence_conn = self.db_manager.get_evidence_conn(
                    self.evidence_id, label=self._get_evidence_label()
                )
                result = evidence_conn.execute(
                    """
                    SELECT GROUP_CONCAT(t.name, ', ')
                    FROM tag_associations ta
                    JOIN tags t ON ta.tag_id = t.id
                    WHERE ta.artifact_type = 'file_list' AND ta.artifact_id = ?
                    """,
                    (file_id,)
                ).fetchone()
                self._tags_cache[file_id] = result[0] if result and result[0] else ""
            except Exception as e:
                logger.error(f"Failed to load tags for file {file_id}: {e}")
                self._tags_cache[file_id] = ""

        return self._tags_cache[file_id]

    def apply_filters(self, filters: Dict[str, Any]) -> None:
        """
        Apply filters and reload data.

        Args:
            filters: Dictionary of filter values
        """
        self.beginResetModel()
        self._filters.update(filters)
        self.selected_rows.clear()  # Clear selection when filtering
        self._load_data()
        self.endResetModel()

    def apply_filters_async(self, filters: Dict[str, Any]) -> None:
        """
        Apply filters without loading data (for async refresh).

        Call set_data() afterwards when data is available from background worker.

        Args:
            filters: Dictionary of filter values
        """
        self._filters.update(filters)
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
        Get available values for filter dropdowns (Phase 2: uses cache for 100x speedup).

        Returns:
            Dictionary with lists of available extensions, matches, tags
        """
        try:
            evidence_conn = self.db_manager.get_evidence_conn(
                self.evidence_id, label=self._get_evidence_label()
            )

            # Phase 2: Check if filter cache exists (from migration 0004)
            cache_exists = evidence_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='file_list_filter_cache'"
            ).fetchone()

            if cache_exists:
                # Fast path: Use pre-computed cache (100x faster)
                # But fall back to DISTINCT queries if cache is empty for a specific type
                cursor = evidence_conn.execute(
                    """
                    SELECT filter_value
                    FROM file_list_filter_cache
                    WHERE evidence_id = ? AND filter_type = 'extension'
                    ORDER BY filter_value
                    """,
                    (self.evidence_id,),
                )
                extensions = [row[0] for row in cursor.fetchall()]

                # If cache is empty for extensions, use fallback query
                if not extensions:
                    logger.debug("Extension cache empty, using DISTINCT query")
                    cursor = evidence_conn.execute(
                        """
                        SELECT DISTINCT extension
                        FROM file_list
                        WHERE evidence_id = ? AND extension IS NOT NULL AND extension != ''
                        ORDER BY extension
                        """,
                        (self.evidence_id,),
                    )
                    extensions = [row[0] for row in cursor.fetchall()]

                cursor = evidence_conn.execute(
                    """
                    SELECT filter_value
                    FROM file_list_filter_cache
                    WHERE evidence_id = ? AND filter_type = 'match'
                    ORDER BY filter_value
                    """,
                    (self.evidence_id,),
                )
                matches = [row[0] for row in cursor.fetchall()]

                cursor = evidence_conn.execute(
                    """
                    SELECT filter_value
                    FROM file_list_filter_cache
                    WHERE evidence_id = ? AND filter_type = 'tag'
                    ORDER BY filter_value
                    """,
                    (self.evidence_id,),
                )
                tags = [row[0] for row in cursor.fetchall()]

                # Debug: If tags cache is empty, use fallback
                if not tags:
                    logger.debug("Tag cache empty, using DISTINCT query for tags")
                    cursor = evidence_conn.execute(
                        """
                        SELECT DISTINCT t.name
                        FROM tags t
                        JOIN tag_associations ta ON t.id = ta.tag_id
                        WHERE ta.evidence_id = ? AND ta.artifact_type = 'file_list'
                        ORDER BY t.name
                        """,
                        (self.evidence_id,),
                    )
                    tags = [row[0] for row in cursor.fetchall()]
                    logger.debug(f"Found {len(tags)} tags via DISTINCT query: {tags}")

            else:
                # Fallback: Use original DISTINCT queries (slower)
                logger.debug("Filter cache not available, using DISTINCT queries")

                cursor = evidence_conn.execute(
                    """
                    SELECT DISTINCT extension
                    FROM file_list
                    WHERE evidence_id = ? AND extension IS NOT NULL AND extension != ''
                    ORDER BY extension
                """,
                    (self.evidence_id,),
                )
                extensions = [row[0] for row in cursor.fetchall()]

                cursor = evidence_conn.execute(
                    """
                    SELECT DISTINCT reference_list_name
                    FROM file_list_matches flm
                    JOIN file_list fl ON flm.file_list_id = fl.id
                    WHERE fl.evidence_id = ?
                    ORDER BY reference_list_name
                """,
                    (self.evidence_id,),
                )
                matches = [row[0] for row in cursor.fetchall()]

                # Query tags directly from tags table
                cursor = evidence_conn.execute(
                    """
                    SELECT DISTINCT t.name
                    FROM tags t
                    JOIN tag_associations ta ON t.id = ta.tag_id
                    WHERE ta.evidence_id = ? AND ta.artifact_type = 'file_list'
                    ORDER BY t.name
                """,
                    (self.evidence_id,),
                )
                tags = [row[0] for row in cursor.fetchall()]
                logger.debug(f"Loaded filter values via DISTINCT: {len(extensions)} extensions, {len(matches)} matches, {len(tags)} tags")

            return {
                "extensions": extensions,
                "matches": matches,
                "tags": tags,
            }

        except Exception as e:
            logger.error(f"Failed to get filter values: {e}")
            return {"extensions": [], "matches": [], "tags": []}