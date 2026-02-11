"""
Qt model for cookies table.

Displays parsed cookies with filtering by browser and domain.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_cookies, get_distinct_cookie_browsers, get_cookie_domains
from core.database import DatabaseManager

logger = logging.getLogger(__name__)


class CookiesTableModel(QAbstractTableModel):
    """
    Qt model for cookies table.

    Displays parsed cookies extracted from browser databases.
    """

    # Column definitions
    COLUMNS = [
        "domain",
        "name",
        "value",
        "browser",
        "profile",
        "is_secure",
        "is_httponly",
        "samesite",
        "expires_utc",
        "encrypted",
        "tags",
    ]

    HEADERS = [
        "Domain",
        "Name",
        "Value",
        "Browser",
        "Profile",
        "Secure",
        "HttpOnly",
        "SameSite",
        "Expires",
        "Encrypted",
        "Tags",
    ]

    # Column indexes
    COL_DOMAIN = 0
    COL_NAME = 1
    COL_VALUE = 2
    COL_BROWSER = 3
    COL_PROFILE = 4
    COL_SECURE = 5
    COL_HTTPONLY = 6
    COL_SAMESITE = 7
    COL_EXPIRES = 8
    COL_ENCRYPTED = 9
    COL_TAGS = 10

    ARTIFACT_TYPE = "cookie"

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize cookies model.

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
        self._domain_filter: str = ""

    def load(self, browser_filter: str = "", domain_filter: str = "") -> None:
        """
        Load cookies from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            domain_filter: Domain substring filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._domain_filter = domain_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_cookies(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    domain=domain_filter or None,
                    limit=5000,
                )
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} cookies")

        except Exception as e:
            logger.error(f"Failed to load cookies: {e}", exc_info=True)
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
        """Get list of browsers that have cookies."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                return get_distinct_cookie_browsers(conn, self.evidence_id)
        except Exception as e:
            logger.error(f"Failed to get cookie browsers: {e}", exc_info=True)
            return []

    def get_encrypted_count(self) -> int:
        """Return count of encrypted cookies in current dataset."""
        return sum(1 for row in self._rows if row.get("encrypted"))

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
            if col == self.COL_DOMAIN:
                return row_data.get("domain", "")
            elif col == self.COL_NAME:
                return row_data.get("name") or ""
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
            elif col == self.COL_SECURE:
                return "Yes" if row_data.get("is_secure") else "No"
            elif col == self.COL_HTTPONLY:
                return "Yes" if row_data.get("is_httponly") else "No"
            elif col == self.COL_SAMESITE:
                return row_data.get("samesite") or "unset"
            elif col == self.COL_EXPIRES:
                expires = row_data.get("expires_utc")
                if expires:
                    # Truncate to date only if long
                    return expires[:10] if len(expires) > 10 else expires
                return "Session"
            elif col == self.COL_ENCRYPTED:
                return "Yes" if row_data.get("encrypted") else "No"
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_VALUE:
                # Full value in tooltip
                return row_data.get("value", "")
            elif col == self.COL_EXPIRES:
                return row_data.get("expires_utc") or "Session cookie"
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.TextAlignmentRole:
            if col in (self.COL_SECURE, self.COL_HTTPONLY, self.COL_ENCRYPTED):
                return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
