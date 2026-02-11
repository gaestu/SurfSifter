"""
Qt model for session form data table.

Displays form field data extracted from Firefox session restore files.
Form data includes user-entered text in form fields captured at session save time.

Initial implementation for Firefox session form data.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_session_form_data

logger = logging.getLogger(__name__)


class SessionFormDataTableModel(QAbstractTableModel):
    """
    Qt model for session form data table.

    Displays form field data extracted from browser session restore files.
    Currently Firefox-only (Chromium doesn't store form data in sessions).
    """

    # Column definitions - DB column names
    COLUMNS = [
        "url",
        "field_name",
        "field_value",
        "field_type",
        "browser",
        "profile",
        "tags",
    ]

    HEADERS = [
        "Page URL",
        "Field Name",
        "Field Value",
        "Type",
        "Browser",
        "Profile",
        "Tags",
    ]

    # Column indexes
    COL_URL = 0
    COL_FIELD_NAME = 1
    COL_FIELD_VALUE = 2
    COL_FIELD_TYPE = 3
    COL_BROWSER = 4
    COL_PROFILE = 5
    COL_TAGS = 6

    ARTIFACT_TYPE = "session_form_data"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize session form data model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            case_data: CaseDataAccess for tag operations
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
        self._field_name_filter: str = ""
        self._url_filter: str = ""

    def load(
        self,
        browser_filter: str = "",
        field_name_filter: str = "",
        url_filter: str = "",
    ) -> None:
        """
        Load session form data from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            field_name_filter: Field name filter (empty = all)
            url_filter: URL filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._field_name_filter = field_name_filter
        self._url_filter = url_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row

                # Check if table exists (migration may not have run yet)
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='session_form_data'"
                )
                if not cursor.fetchone():
                    logger.debug("session_form_data table does not exist yet")
                    self._rows = []
                    self._tag_map = {}
                    self.endResetModel()
                    return

                # Use helper with available filters
                self._rows = get_session_form_data(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    field_name=field_name_filter or None,
                    url=url_filter or None,
                    limit=5000,
                )

                # Apply additional text filters that helper doesn't support
                if url_filter and self._rows:
                    # Helper uses exact match, we want contains
                    self._rows = [
                        r for r in self._rows
                        if url_filter.lower() in (r.get("url") or "").lower()
                    ]

                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} session form data entries")

        except Exception as e:
            logger.error(f"Failed to load session form data: {e}", exc_info=True)
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
        """Get list of browsers that have session form data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                # Check if table exists
                cursor = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='session_form_data'"
                )
                if not cursor.fetchone():
                    return []
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM session_form_data WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get session form data browsers: {e}", exc_info=True)
            return []

    def get_stats(self) -> Dict[str, int]:
        """Get statistics about currently loaded form data."""
        if not self._rows:
            return {"unique_urls": 0, "unique_fields": 0}

        unique_urls = len(set(
            r.get("url") for r in self._rows if r.get("url")
        ))
        unique_fields = len(set(
            r.get("field_name") for r in self._rows if r.get("field_name")
        ))
        return {
            "unique_urls": unique_urls,
            "unique_fields": unique_fields,
        }

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
            if col == self.COL_URL:
                url = row_data.get("url", "") or ""
                if len(url) > 50:
                    return url[:47] + "..."
                return url
            elif col == self.COL_FIELD_NAME:
                name = row_data.get("field_name", "") or ""
                if len(name) > 30:
                    return name[:27] + "..."
                return name
            elif col == self.COL_FIELD_VALUE:
                value = row_data.get("field_value", "") or ""
                # Truncate long values for display
                if len(value) > 50:
                    return value[:47] + "..."
                return value
            elif col == self.COL_FIELD_TYPE:
                return row_data.get("field_type") or ""
            elif col == self.COL_BROWSER:
                return (row_data.get("browser") or "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_URL:
                return row_data.get("url", "")
            elif col == self.COL_FIELD_NAME:
                return row_data.get("field_name", "")
            elif col == self.COL_FIELD_VALUE:
                # Show full value in tooltip
                return row_data.get("field_value", "")
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.TextAlignmentRole:
            if col == self.COL_FIELD_TYPE:
                return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
