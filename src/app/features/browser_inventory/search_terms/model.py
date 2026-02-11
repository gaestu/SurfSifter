"""
Qt model for browser_search_terms table.

Displays extracted keyword search terms from browser history databases.
Shows search queries typed into browser omniboxes with forensic metadata.

Initial implementation for Chromium keyword_search_terms.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import DatabaseManager
from core.database.helpers.browser_search_terms import (
    get_search_terms,
    get_search_terms_stats,
)

logger = logging.getLogger(__name__)


class SearchTermsTableModel(QAbstractTableModel):
    """
    Qt model for browser_search_terms table.

    Displays search terms extracted from browser history databases.
    Shows per-search records with forensic metadata.
    """

    # Column definitions
    COLUMNS = [
        "term",
        "url",
        "search_time_utc",
        "browser",
        "profile",
        "search_engine",
        "tags",
    ]

    HEADERS = [
        "Search Term",
        "URL",
        "Search Time",
        "Browser",
        "Profile",
        "Search Engine",
        "Tags",
    ]

    # Column indexes
    COL_TERM = 0
    COL_URL = 1
    COL_SEARCH_TIME = 2
    COL_BROWSER = 3
    COL_PROFILE = 4
    COL_SEARCH_ENGINE = 5
    COL_TAGS = 6

    ARTIFACT_TYPE = "browser_search_term"

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize search terms model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            case_data: CaseDataAccess instance for tagging
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
        self._term_filter: str = ""
        self._search_engine_filter: str = ""

    def load(
        self,
        browser_filter: str = "",
        term_filter: str = "",
        search_engine_filter: str = "",
    ) -> None:
        """
        Load search terms from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            term_filter: Term substring filter (empty = all)
            search_engine_filter: Search engine filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._term_filter = term_filter
        self._search_engine_filter = search_engine_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = get_search_terms(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    term_filter=term_filter or None,
                    search_engine=search_engine_filter or None,
                    limit=10000,
                )
                self._rows = rows
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} search term records")

        except Exception as e:
            logger.error(f"Failed to load search terms: {e}", exc_info=True)
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

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        """Return data for cell."""
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if row < 0 or row >= len(self._rows):
            return None

        record = self._rows[row]

        if role == Qt.DisplayRole:
            if col == self.COL_TERM:
                term = record.get("term", "")
                # Truncate long terms
                return term[:100] + "..." if len(term) > 100 else term
            elif col == self.COL_URL:
                url = record.get("url", "")
                # Truncate long URLs
                return url[:80] + "..." if url and len(url) > 80 else url
            elif col == self.COL_SEARCH_TIME:
                ts = record.get("search_time_utc", "")
                # Format timestamp for display (show date and time)
                if ts:
                    return ts.replace("T", " ")[:19]
                return ""
            elif col == self.COL_BROWSER:
                return record.get("browser", "")
            elif col == self.COL_PROFILE:
                return record.get("profile", "")
            elif col == self.COL_SEARCH_ENGINE:
                return record.get("search_engine", "")
            elif col == self.COL_TAGS:
                record_id = record.get("id")
                return self._tag_map.get(record_id, "")

        elif role == Qt.ToolTipRole:
            if col == self.COL_TERM:
                return record.get("term", "")
            elif col == self.COL_URL:
                return record.get("url", "")
            elif col == self.COL_SEARCH_TIME:
                return record.get("search_time_utc", "")

        elif role == Qt.UserRole:
            # Return full record for details dialog
            return record

        return None

    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.DisplayRole
    ) -> Any:
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def get_row_data(self, index: QModelIndex) -> Optional[Dict[str, Any]]:
        """Get full row data for given index."""
        if not index.isValid():
            return None
        row = index.row()
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_browsers(self) -> List[str]:
        """Get distinct browsers from loaded data."""
        browsers = set()
        for row in self._rows:
            browser = row.get("browser")
            if browser:
                browsers.add(browser)
        return sorted(browsers)

    def get_search_engines(self) -> List[str]:
        """Get distinct search engines from loaded data."""
        engines = set()
        for row in self._rows:
            engine = row.get("search_engine")
            if engine:
                engines.add(engine)
        return sorted(engines)

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for current data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                return get_search_terms_stats(conn, self.evidence_id)
        except Exception as e:
            logger.error(f"Failed to get search terms stats: {e}", exc_info=True)
            return {"total_count": 0, "unique_terms": 0}
