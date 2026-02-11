"""
Qt model for autofill table.

Displays parsed autofill data with filtering by browser and field type.

Added Domain column to show related URLs from Edge autofill data.
"""
from __future__ import annotations

import logging
import re
import sqlite3
from typing import Any, Dict, List

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_autofill_entries

logger = logging.getLogger(__name__)


class AutofillTableModel(QAbstractTableModel):
    """
    Qt model for autofill table.

    Displays parsed autofill entries extracted from browser databases.
    """

    # Column definitions - DB column names
    COLUMNS = [
        "name",  # DB uses 'name', not 'field_name'
        "value",
        "browser",
        "profile",
        "count",  # DB uses 'count', not 'use_count'
        "date_created_utc",  # DB uses date_created_utc, not first_used_utc
        "date_last_used_utc",  # DB column name
        "domain",  # Extracted from notes field for Edge autofill
        "tags",
    ]

    HEADERS = [
        "Field Name",
        "Value",
        "Browser",
        "Profile",
        "Use Count",
        "First Used",
        "Last Used",
        "Domain",  # Related URL domain
        "Tags",
    ]

    # Column indexes
    COL_FIELD_NAME = 0
    COL_VALUE = 1
    COL_BROWSER = 2
    COL_PROFILE = 3
    COL_USE_COUNT = 4
    COL_FIRST_USED = 5
    COL_LAST_USED = 6
    COL_DOMAIN = 7  #
    COL_TAGS = 8

    ARTIFACT_TYPE = "autofill"

    # Regex to extract domain from notes field (e.g., "domain:secure.startups.ch")
    _DOMAIN_PATTERN = re.compile(r'domain:([^;]+)')

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize autofill model.

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
        self.case_data = case_data

        # Data storage
        self._rows: List[Dict[str, Any]] = []
        self._tag_map: Dict[int, str] = {}

        # Filters
        self._browser_filter: str = ""
        self._field_filter: str = ""

    def load(self, browser_filter: str = "", field_filter: str = "") -> None:
        """
        Load autofill data from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            field_filter: Field name substring filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._field_filter = field_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_autofill_entries(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    limit=5000,
                )

                # Apply field filter in-memory - DB column is 'name', not 'field_name'
                if field_filter:
                    field_lower = field_filter.lower()
                    self._rows = [
                        r for r in self._rows
                        if field_lower in (r.get("name") or "").lower()
                    ]
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} autofill entries")

        except Exception as e:
            logger.error(f"Failed to load autofill: {e}", exc_info=True)
            self._rows = []
            self._tag_map = {}

        self.endResetModel()

    def _refresh_tags(self) -> None:
        """Refresh tag strings for current rows."""
        if not self.case_data:
            self._tag_map = {}
            return
        ids = [row.get("id") for row in self._rows if row.get("id") is not None]
        self._tag_map = self.case_data.get_tag_strings_for_artifacts(
            self.evidence_id,
            self.ARTIFACT_TYPE,
            ids,
        )

    def get_available_browsers(self) -> List[str]:
        """Get list of browsers that have autofill data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM autofill WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get autofill browsers: {e}", exc_info=True)
            return []

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """Get full row data for given index."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return self._rows[index.row()]

    # Qt interface methods

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Return number of rows."""
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        """Return number of columns."""
        return len(self.HEADERS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        """Return data for given index and role."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None

        row_data = self._rows[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            if col == self.COL_FIELD_NAME:
                return row_data.get("name", "")  # DB uses 'name'
            elif col == self.COL_VALUE:
                value = row_data.get("value") or ""
                # Truncate long values
                if len(value) > 50:
                    return value[:47] + "..."
                return value
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_USE_COUNT:
                return str(row_data.get("count") or 0)  # DB uses 'count'
            elif col == self.COL_FIRST_USED:
                first_used = row_data.get("date_created_utc")  # DB uses date_created_utc
                if first_used:
                    return first_used[:10] if len(first_used) > 10 else first_used
                return ""
            elif col == self.COL_LAST_USED:
                last_used = row_data.get("date_last_used_utc")  # DB column name
                if last_used:
                    return last_used[:10] if len(last_used) > 10 else last_used
                return ""
            elif col == self.COL_DOMAIN:
                # Extract domain from notes field (Edge autofill stores domain there)
                return self._extract_domain(row_data.get("notes"))
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_VALUE:
                return row_data.get("value", "")
            elif col in (self.COL_FIRST_USED, self.COL_LAST_USED):
                return row_data.get(self.COLUMNS[col], "")
            elif col == self.COL_DOMAIN:
                # Show full notes as tooltip
                return row_data.get("notes") or ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.TextAlignmentRole:
            if col == self.COL_USE_COUNT:
                return Qt.AlignCenter

        return None

    def _extract_domain(self, notes: str | None) -> str:
        """Extract domain from notes field.

        Edge autofill data stores domain as 'domain:example.com' in notes.
        """
        if not notes:
            return ""
        match = self._DOMAIN_PATTERN.search(notes)
        if match:
            return match.group(1).strip()
        return ""

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
