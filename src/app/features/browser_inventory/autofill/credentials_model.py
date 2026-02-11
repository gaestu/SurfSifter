"""
Qt model for credentials table.

Displays parsed credentials with filtering by browser and origin.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_credentials

logger = logging.getLogger(__name__)


class CredentialsTableModel(QAbstractTableModel):
    """
    Qt model for credentials table.

    Displays saved login credentials extracted from browser databases.
    """

    # Column definitions - DB column names
    COLUMNS = [
        "origin_url",
        "username_element",
        "username_value",
        "browser",
        "profile",
        "password_value_encrypted",  # DB stores encrypted as BLOB, not boolean 'encrypted'
        "date_created_utc",
        "date_last_used_utc",
        "tags",
    ]

    HEADERS = [
        "Origin URL",
        "Username Field",
        "Username",
        "Browser",
        "Profile",
        "Encrypted",
        "Created",
        "Last Used",
        "Tags",
    ]

    # Column indexes
    COL_ORIGIN = 0
    COL_USERNAME_ELEMENT = 1
    COL_USERNAME = 2
    COL_BROWSER = 3
    COL_PROFILE = 4
    COL_ENCRYPTED = 5
    COL_CREATED = 6
    COL_LAST_USED = 7
    COL_TAGS = 8

    ARTIFACT_TYPE = "credential"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize credentials model.

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
        self._origin_filter: str = ""

    def load(self, browser_filter: str = "", origin_filter: str = "") -> None:
        """
        Load credentials data from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            origin_filter: Origin URL substring filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._origin_filter = origin_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_credentials(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    limit=5000,
                )

                # Apply origin filter in-memory
                if origin_filter:
                    origin_lower = origin_filter.lower()
                    self._rows = [
                        r for r in self._rows
                        if origin_lower in (r.get("origin_url") or "").lower()
                    ]
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} credentials")

        except Exception as e:
            logger.error(f"Failed to load credentials: {e}", exc_info=True)
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
        """Get list of browsers that have credentials."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM credentials WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get credentials browsers: {e}", exc_info=True)
            return []

    def get_encrypted_count(self) -> int:
        """Return count of encrypted credentials in current dataset."""
        # DB stores password_value_encrypted as BLOB - check if non-empty
        return sum(1 for row in self._rows if row.get("password_value_encrypted"))

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
            if col == self.COL_ORIGIN:
                origin = row_data.get("origin_url", "")
                if len(origin) > 60:
                    return origin[:57] + "..."
                return origin
            elif col == self.COL_USERNAME_ELEMENT:
                return row_data.get("username_element") or ""
            elif col == self.COL_USERNAME:
                return row_data.get("username_value") or ""
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_ENCRYPTED:
                # DB stores password_value_encrypted as BLOB - check if non-empty
                return "Yes" if row_data.get("password_value_encrypted") else "No"
            elif col == self.COL_CREATED:
                created = row_data.get("date_created_utc")
                if created:
                    return created[:10] if len(created) > 10 else created
                return ""
            elif col == self.COL_LAST_USED:
                last_used = row_data.get("date_last_used_utc")
                if last_used:
                    return last_used[:10] if len(last_used) > 10 else last_used
                return ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_ORIGIN:
                return row_data.get("origin_url", "")
            elif col in (self.COL_CREATED, self.COL_LAST_USED):
                return row_data.get(self.COLUMNS[col], "")
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.TextAlignmentRole:
            if col == self.COL_ENCRYPTED:
                return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
