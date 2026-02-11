"""
Qt model for session tabs table.

Displays parsed browser session tabs with filtering by browser and window.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_session_tabs

logger = logging.getLogger(__name__)


class SessionTabsTableModel(QAbstractTableModel):
    """
    Qt model for session tabs table.

    Displays browser session tab data extracted from session restore files.
    """

    # Column definitions - DB column names
    COLUMNS = [
        "url",
        "title",
        "browser",
        "profile",
        "window_id",
        "tab_index",  # DB uses tab_index, not tab_id
        "pinned",
        "history_count",  # Computed from session_tab_history
        "last_accessed_utc",  # DB uses last_accessed_utc
        "tags",
    ]

    HEADERS = [
        "URL",
        "Title",
        "Browser",
        "Profile",
        "Window",
        "Tab",
        "Pinned",
        "History",
        "Last Accessed",
        "Tags",
    ]

    # Column indexes
    COL_URL = 0
    COL_TITLE = 1
    COL_BROWSER = 2
    COL_PROFILE = 3
    COL_WINDOW = 4
    COL_TAB = 5
    COL_PINNED = 6
    COL_HISTORY = 7
    COL_LAST_ACCESSED = 8
    COL_TAGS = 9

    ARTIFACT_TYPE = "session_tab"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize sessions model.

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
        self._history_counts: Dict[int, int] = {}  # tab_id -> history entry count

        # Filters
        self._browser_filter: str = ""

    def load(self, browser_filter: str = "") -> None:
        """
        Load session tabs from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
        """
        self._browser_filter = browser_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_session_tabs(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    limit=5000,
                )
                self._load_history_counts(conn)
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} session tabs")

        except Exception as e:
            logger.error(f"Failed to load session tabs: {e}", exc_info=True)
            self._rows = []
            self._tag_map = {}
            self._history_counts = {}

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

    def _load_history_counts(self, conn: sqlite3.Connection) -> None:
        """Load navigation history counts for all loaded tabs."""
        self._history_counts = {}
        if not self._rows:
            return
        try:
            cursor = conn.execute(
                """
                SELECT tab_id, COUNT(*) as cnt
                FROM session_tab_history
                WHERE evidence_id = ?
                GROUP BY tab_id
                """,
                (self.evidence_id,)
            )
            for row in cursor.fetchall():
                self._history_counts[row["tab_id"]] = row["cnt"]
        except Exception as e:
            logger.warning(f"Failed to load history counts: {e}")

    def get_available_browsers(self) -> List[str]:
        """Get list of browsers that have session data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM session_tabs WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get session browsers: {e}", exc_info=True)
            return []

    def get_pinned_count(self) -> int:
        """Return count of pinned tabs in current dataset."""
        return sum(1 for row in self._rows if row.get("pinned"))

    def get_window_count(self) -> int:
        """Return count of unique windows in current dataset."""
        windows = set(r.get("window_id") for r in self._rows if r.get("window_id"))
        return len(windows)

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
                url = row_data.get("url", "")
                if len(url) > 60:
                    return url[:57] + "..."
                return url
            elif col == self.COL_TITLE:
                title = row_data.get("title") or ""
                if len(title) > 50:
                    return title[:47] + "..."
                return title
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_WINDOW:
                return row_data.get("window_id") or ""
            elif col == self.COL_TAB:
                return row_data.get("tab_index") or ""  # Use tab_index from DB
            elif col == self.COL_PINNED:
                return "Yes" if row_data.get("pinned") else "No"
            elif col == self.COL_HISTORY:
                count = self._history_counts.get(row_data.get("id"), 0)
                return str(count) if count else ""
            elif col == self.COL_LAST_ACCESSED:
                last_accessed = row_data.get("last_accessed_utc")  # Use last_accessed_utc from DB
                if last_accessed:
                    return last_accessed[:16] if len(last_accessed) > 16 else last_accessed
                return ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_URL:
                return row_data.get("url", "")
            elif col == self.COL_TITLE:
                return row_data.get("title", "")
            elif col == self.COL_HISTORY:
                count = self._history_counts.get(row_data.get("id"), 0)
                return f"{count} navigation history entries" if count else "No navigation history"
            elif col == self.COL_LAST_ACCESSED:
                return row_data.get("last_accessed_utc", "")  # Use last_accessed_utc from DB
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.TextAlignmentRole:
            if col in (self.COL_PINNED, self.COL_HISTORY):
                return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
