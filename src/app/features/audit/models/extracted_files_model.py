"""
Qt model for extracted_files table.

Displays all files extracted by any extractor with forensic provenance.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import DatabaseManager
from core.database import (
    get_extracted_files,
    get_distinct_extractors,
    get_extraction_stats,
)

logger = logging.getLogger(__name__)


class ExtractedFilesTableModel(QAbstractTableModel):
    """
    Qt model for extracted_files table.

    Displays files extracted by all extractors with forensic provenance.
    """

    # Column definitions
    COLUMNS = [
        "extractor_name",
        "dest_filename",
        "source_path",
        "size_bytes",
        "file_type",
        "sha256",
        "status",
        "extracted_at_utc",
        "run_id",
    ]

    HEADERS = [
        "Extractor",
        "Filename",
        "Source Path",
        "Size",
        "Type",
        "SHA256",
        "Status",
        "Extracted At",
        "Run ID",
    ]

    # Column indexes
    COL_EXTRACTOR = 0
    COL_FILENAME = 1
    COL_SOURCE_PATH = 2
    COL_SIZE = 3
    COL_TYPE = 4
    COL_SHA256 = 5
    COL_STATUS = 6
    COL_EXTRACTED_AT = 7
    COL_RUN_ID = 8

    # Status colors for display
    STATUS_COLORS = {
        "ok": "#2e7d32",       # Green
        "partial": "#f57f17", # Yellow
        "error": "#c62828",   # Red
        "skipped": "#546e7a", # Gray
    }

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        parent=None
    ):
        """
        Initialize extracted files model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            parent: Parent widget
        """
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label

        # Data storage
        self._rows: List[Dict[str, Any]] = []

        # Filters
        self._extractor_filter: str = ""
        self._status_filter: str = ""

        # Pagination
        self._limit: int = 1000
        self._offset: int = 0
        self._total_count: int = 0

    def load(
        self,
        extractor_filter: str = "",
        status_filter: str = "",
        limit: int = 1000,
        offset: int = 0,
    ) -> None:
        """
        Load extracted files from database with optional filters.

        Args:
            extractor_filter: Extractor name filter (empty = all)
            status_filter: Status filter (empty = all)
            limit: Maximum rows to return
            offset: Offset for pagination
        """
        self._extractor_filter = extractor_filter
        self._status_filter = status_filter
        self._limit = limit
        self._offset = offset

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row

                # Check if table exists
                tables = {row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )}
                if "extracted_files" not in tables:
                    logger.debug("extracted_files table does not exist yet")
                    self._rows = []
                    self._total_count = 0
                    self.endResetModel()
                    return

                self._rows = get_extracted_files(
                    conn,
                    self.evidence_id,
                    extractor_name=extractor_filter or None,
                    status=status_filter or None,
                    limit=limit,
                    offset=offset,
                )

                # Get total count for pagination
                stats = get_extraction_stats(conn, self.evidence_id)
                self._total_count = stats.get("total_count", len(self._rows))

            logger.debug(f"Loaded {len(self._rows)} extracted files")

        except Exception as e:
            logger.error(f"Failed to load extracted files: {e}", exc_info=True)
            self._rows = []
            self._total_count = 0

        self.endResetModel()

    def get_distinct_extractors(self) -> List[str]:
        """Get list of distinct extractor names for filter dropdown."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                # Check if table exists
                tables = {row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )}
                if "extracted_files" not in tables:
                    return []

                return get_distinct_extractors(conn, self.evidence_id)

        except Exception as e:
            logger.error(f"Failed to get extractors: {e}", exc_info=True)
            return []

    def get_stats(self) -> Dict[str, Any]:
        """Get extraction statistics for summary display."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                # Check if table exists
                tables = {row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )}
                if "extracted_files" not in tables:
                    return {
                        "total_count": 0,
                        "total_size_bytes": 0,
                        "by_extractor": {},
                        "by_status": {},
                        "by_file_type": {},
                        "error_count": 0,
                    }

                return get_extraction_stats(conn, self.evidence_id)

        except Exception as e:
            logger.error(f"Failed to get stats: {e}", exc_info=True)
            return {
                "total_count": 0,
                "total_size_bytes": 0,
                "by_extractor": {},
                "by_status": {},
                "by_file_type": {},
                "error_count": 0,
            }

    @property
    def total_count(self) -> int:
        """Total number of rows (for pagination)."""
        return self._total_count

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Return number of rows."""
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Return number of columns."""
        if parent.isValid():
            return 0
        return len(self.COLUMNS)

    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.DisplayRole
    ) -> Any:
        """Return header data."""
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        """Return data for the given index and role."""
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if row < 0 or row >= len(self._rows):
            return None

        record = self._rows[row]
        column_name = self.COLUMNS[col]

        if role == Qt.DisplayRole:
            value = record.get(column_name)

            # Format size_bytes
            if col == self.COL_SIZE and value is not None:
                return self._format_size(value)

            # Truncate SHA256 for display
            if col == self.COL_SHA256 and value:
                return value[:16] + "..." if len(value) > 16 else value

            # Truncate source path for display
            if col == self.COL_SOURCE_PATH and value:
                return value if len(value) <= 60 else "..." + value[-57:]

            # Format extracted_at timestamp
            if col == self.COL_EXTRACTED_AT and value:
                # ISO format: show date and time
                return value[:19].replace("T", " ") if len(value) >= 19 else value

            return value if value is not None else ""

        elif role == Qt.ForegroundRole:
            # Color status column
            if col == self.COL_STATUS:
                status = record.get("status", "")
                color = self.STATUS_COLORS.get(status)
                if color:
                    from PySide6.QtGui import QColor
                    return QColor(color)

        elif role == Qt.ToolTipRole:
            # Full SHA256 on hover
            if col == self.COL_SHA256:
                return record.get("sha256", "")

            # Full source path on hover
            if col == self.COL_SOURCE_PATH:
                return record.get("source_path", "")

            # Error message on hover for status
            if col == self.COL_STATUS:
                error = record.get("error_message")
                if error:
                    return f"Error: {error}"

        elif role == Qt.UserRole:
            # Return the row ID for context menu actions
            return record.get("id")

        return None

    def get_row_data(self, row: int) -> Optional[Dict[str, Any]]:
        """Get full row data for detail dialog."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format file size in human-readable form."""
        if size_bytes is None:
            return ""
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
