"""
Qt model for search_engines table.

Displays Chromium browser search engine configurations.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_search_engines

logger = logging.getLogger(__name__)


class SearchEnginesTableModel(QAbstractTableModel):
    """
    Qt model for search_engines table.

    Displays search engine entries extracted from Chromium browsers' keywords table.
    Useful for forensic analysis of:
    - Default search engine preference
    - Custom search engines added by user
    - Site-specific search shortcuts (e.g., "yt" for YouTube)
    """

    # Column definitions - DB column names
    COLUMNS = [
        "short_name",
        "keyword",
        "url",
        "favicon_url",
        "browser",
        "profile",
        "is_default",
        "safe_for_autoreplace",
        "date_created_utc",
        "last_modified_utc",
        "tags",
    ]

    HEADERS = [
        "Name",
        "Keyword",
        "URL Template",
        "Favicon URL",
        "Browser",
        "Profile",
        "Likely Default",
        "Auto-Replace",
        "Created",
        "Modified",
        "Tags",
    ]

    # Column indexes
    COL_SHORT_NAME = 0
    COL_KEYWORD = 1
    COL_URL = 2
    COL_FAVICON_URL = 3
    COL_BROWSER = 4
    COL_PROFILE = 5
    COL_IS_DEFAULT = 6
    COL_SAFE_FOR_AUTOREPLACE = 7
    COL_DATE_CREATED = 8
    COL_LAST_MODIFIED = 9
    COL_TAGS = 10

    ARTIFACT_TYPE = "search_engine"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize search engines model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            case_data: CaseDataAccess for tag resolution
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
        self._keyword_filter: str = ""

    def load(self, browser_filter: str = "", keyword_filter: str = "") -> None:
        """
        Load search engines data from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            keyword_filter: Keyword substring filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._keyword_filter = keyword_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_search_engines(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    limit=5000,
                )

                # Apply keyword filter in-memory
                if keyword_filter:
                    keyword_lower = keyword_filter.lower()
                    self._rows = [
                        r for r in self._rows
                        if keyword_lower in (r.get("keyword") or "").lower()
                        or keyword_lower in (r.get("short_name") or "").lower()
                    ]
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} search engine entries")

        except Exception as e:
            logger.error(f"Failed to load search engines: {e}", exc_info=True)
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
        """Get list of browsers that have search engine data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM search_engines WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get search engine browsers: {e}", exc_info=True)
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
            if col == self.COL_SHORT_NAME:
                return row_data.get("short_name", "")
            elif col == self.COL_KEYWORD:
                return row_data.get("keyword", "")
            elif col == self.COL_URL:
                url = row_data.get("url") or ""
                # Truncate long URLs
                if len(url) > 60:
                    return url[:57] + "..."
                return url
            elif col == self.COL_FAVICON_URL:
                fav = row_data.get("favicon_url") or ""
                if len(fav) > 40:
                    return fav[:37] + "..."
                return fav
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_IS_DEFAULT:
                return "✓" if row_data.get("is_default") else ""
            elif col == self.COL_SAFE_FOR_AUTOREPLACE:
                return "✓" if row_data.get("safe_for_autoreplace") else ""
            elif col == self.COL_DATE_CREATED:
                created = row_data.get("date_created_utc")
                if created:
                    return created[:10] if len(created) > 10 else created
                return ""
            elif col == self.COL_LAST_MODIFIED:
                modified = row_data.get("last_modified_utc")
                if modified:
                    return modified[:10] if len(modified) > 10 else modified
                return ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_URL:
                return row_data.get("url", "")
            elif col == self.COL_FAVICON_URL:
                return row_data.get("favicon_url", "")
            elif col == self.COL_IS_DEFAULT:
                return "Inferred from usage count heuristic, not an explicit browser default flag."
            elif col in (self.COL_DATE_CREATED, self.COL_LAST_MODIFIED):
                return row_data.get(self.COLUMNS[col], "")
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.TextAlignmentRole:
            if col in (self.COL_IS_DEFAULT, self.COL_SAFE_FOR_AUTOREPLACE):
                return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
