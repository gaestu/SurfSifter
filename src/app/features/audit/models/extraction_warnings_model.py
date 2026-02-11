"""
Qt model for extraction_warnings table.

Displays extraction warnings (unknown schemas, parse errors, etc.) with
filtering and summary statistics for the Audit tab.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor

from core.database import DatabaseManager
from core.database.helpers.extraction_warnings import (
    get_extraction_warnings,
    get_extraction_warnings_count,
    get_extraction_warnings_summary,
    get_distinct_warning_extractors,
)

logger = logging.getLogger(__name__)


class ExtractionWarningsTableModel(QAbstractTableModel):
    """
    Qt model for extraction_warnings table.

    Displays warnings collected during extraction with severity coloring
    and filtering support.
    """

    # Column definitions
    COLUMNS = [
        "extractor_name",
        "category",
        "warning_type",
        "severity",
        "item_name",
        "item_value",
        "source_file",
        "artifact_type",
        "created_at_utc",
    ]

    HEADERS = [
        "Extractor",
        "Category",
        "Type",
        "Severity",
        "Item",
        "Value",
        "Source File",
        "Artifact",
        "Created",
    ]

    # Column indexes
    COL_EXTRACTOR = 0
    COL_CATEGORY = 1
    COL_TYPE = 2
    COL_SEVERITY = 3
    COL_ITEM = 4
    COL_VALUE = 5
    COL_SOURCE = 6
    COL_ARTIFACT = 7
    COL_CREATED = 8

    # Severity colors for display
    SEVERITY_COLORS = {
        "info": "#1976d2",     # Blue
        "warning": "#f57f17",  # Yellow/Orange
        "error": "#c62828",    # Red
    }

    # Severity icons
    SEVERITY_ICONS = {
        "info": "ℹ️",
        "warning": "⚠️",
        "error": "❌",
    }

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        parent=None
    ):
        """
        Initialize extraction warnings model.

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
        self._category_filter: str = ""
        self._severity_filter: str = ""

        # Pagination
        self._limit: int = 1000
        self._offset: int = 0
        self._total_count: int = 0

    def load(
        self,
        extractor_filter: str = "",
        category_filter: str = "",
        severity_filter: str = "",
        limit: int = 1000,
        offset: int = 0,
    ) -> None:
        """
        Load extraction warnings from database with optional filters.

        Args:
            extractor_filter: Extractor name filter (empty = all)
            category_filter: Category filter (empty = all)
            severity_filter: Severity filter (empty = all)
            limit: Maximum rows to return
            offset: Offset for pagination
        """
        self._extractor_filter = extractor_filter
        self._category_filter = category_filter
        self._severity_filter = severity_filter
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
                if "extraction_warnings" not in tables:
                    logger.debug("extraction_warnings table does not exist yet")
                    self._rows = []
                    self._total_count = 0
                    self.endResetModel()
                    return

                self._rows = get_extraction_warnings(
                    conn,
                    self.evidence_id,
                    extractor_name=extractor_filter or None,
                    category=category_filter or None,
                    severity=severity_filter or None,
                    limit=limit,
                    offset=offset,
                )

                # Get total count for pagination
                self._total_count = get_extraction_warnings_count(
                    conn,
                    self.evidence_id,
                    extractor_name=extractor_filter or None,
                    category=category_filter or None,
                    severity=severity_filter or None,
                )

            logger.debug(f"Loaded {len(self._rows)} extraction warnings")

        except Exception as e:
            logger.error(f"Failed to load extraction warnings: {e}", exc_info=True)
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
                if "extraction_warnings" not in tables:
                    return []

                return get_distinct_warning_extractors(conn, self.evidence_id)

        except Exception as e:
            logger.error(f"Failed to get extractors: {e}", exc_info=True)
            return []

    def get_summary(self) -> Dict[str, Any]:
        """Get warning summary statistics for display."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                # Check if table exists
                tables = {row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )}
                if "extraction_warnings" not in tables:
                    return {
                        "total": 0,
                        "by_severity": {"info": 0, "warning": 0, "error": 0},
                        "by_category": {},
                        "by_extractor": {},
                    }

                return get_extraction_warnings_summary(conn, self.evidence_id)

        except Exception as e:
            logger.error(f"Failed to get summary: {e}", exc_info=True)
            return {
                "total": 0,
                "by_severity": {"info": 0, "warning": 0, "error": 0},
                "by_category": {},
                "by_extractor": {},
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

            # Add icon to severity
            if col == self.COL_SEVERITY and value:
                icon = self.SEVERITY_ICONS.get(value, "")
                return f"{icon} {value}" if icon else value

            # Format warning_type for readability
            if col == self.COL_TYPE and value:
                return value.replace("_", " ").title()

            # Format extractor_name for readability
            if col == self.COL_EXTRACTOR and value:
                return value.replace("_", " ").title()

            # Truncate item_value for display
            if col == self.COL_VALUE and value:
                return value[:50] + "..." if len(str(value)) > 50 else value

            # Truncate source_file for display
            if col == self.COL_SOURCE and value:
                return value if len(value) <= 40 else "..." + value[-37:]

            # Format created_at timestamp
            if col == self.COL_CREATED and value:
                return value[:19].replace("T", " ") if len(value) >= 19 else value

            return value if value is not None else ""

        elif role == Qt.ForegroundRole:
            # Color severity column
            if col == self.COL_SEVERITY:
                severity = record.get("severity", "")
                color = self.SEVERITY_COLORS.get(severity)
                if color:
                    return QColor(color)

        elif role == Qt.ToolTipRole:
            # Full value on hover
            if col == self.COL_VALUE:
                value = record.get("item_value", "")
                context = record.get("context_json")
                if context:
                    import json
                    try:
                        if isinstance(context, str):
                            context = json.loads(context)
                        return f"Value: {value}\n\nContext:\n{json.dumps(context, indent=2)}"
                    except Exception:
                        pass
                return value

            # Full source file on hover
            if col == self.COL_SOURCE:
                return record.get("source_file", "")

            # Run ID and ID on hover for any column
            return f"ID: {record.get('id')}\nRun ID: {record.get('run_id', 'N/A')}"

        elif role == Qt.UserRole:
            # Return the row ID for context menu actions
            return record.get("id")

        return None

    def get_row_data(self, row: int) -> Optional[Dict[str, Any]]:
        """Get full row data for detail dialog."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_distinct_categories(self) -> List[str]:
        """Get list of distinct categories for filter dropdown."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                # Check if table exists
                tables = {row[0] for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )}
                if "extraction_warnings" not in tables:
                    return []

                cursor = conn.execute("""
                    SELECT DISTINCT category FROM extraction_warnings
                    WHERE evidence_id = ? AND category IS NOT NULL
                    ORDER BY category
                """, (self.evidence_id,))
                return [row[0] for row in cursor.fetchall()]

        except Exception as e:
            logger.error(f"Failed to get categories: {e}", exc_info=True)
            return []
