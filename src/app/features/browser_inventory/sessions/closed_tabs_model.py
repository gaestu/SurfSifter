"""
Qt model for closed_tabs table.

Displays recently closed browser tabs with filtering by browser.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_closed_tabs

logger = logging.getLogger(__name__)


class ClosedTabsTableModel(QAbstractTableModel):
    """
    Qt model for closed_tabs table.

    Displays recently closed tab data from session restore files.
    """

    COLUMNS = [
        "url",
        "title",
        "browser",
        "profile",
        "closed_at_utc",
        "original_window_id",
        "original_tab_index",
        "tags",
    ]

    HEADERS = [
        "URL",
        "Title",
        "Browser",
        "Profile",
        "Closed At",
        "Window",
        "Tab",
        "Tags",
    ]

    COL_URL = 0
    COL_TITLE = 1
    COL_BROWSER = 2
    COL_PROFILE = 3
    COL_CLOSED_AT = 4
    COL_WINDOW = 5
    COL_TAB = 6
    COL_TAGS = 7

    ARTIFACT_TYPE = "closed_tab"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None,
    ):
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.case_data = case_data

        self._rows: List[Dict[str, Any]] = []
        self._tag_map: Dict[int, str] = {}
        self._browser_filter: str = ""

    def load(self, browser_filter: str = "") -> None:
        """Load closed tabs from database with optional browser filter."""
        self._browser_filter = browser_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_closed_tabs(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    limit=5000,
                )
                self._refresh_tags()

            logger.debug("Loaded %d closed tabs", len(self._rows))

        except Exception as e:
            logger.error("Failed to load closed tabs: %s", e, exc_info=True)
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
        """Get list of browsers that have closed tab data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM closed_tabs WHERE evidence_id = ?",
                    (self.evidence_id,),
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error("Failed to get closed tab browsers: %s", e, exc_info=True)
            return []

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """Get full row data for given index."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return self._rows[index.row()]

    # ─── Qt interface ────────────────────────────────────────────────

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self.HEADERS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None

        row_data = self._rows[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            if col == self.COL_URL:
                url = row_data.get("url", "")
                return url[:57] + "..." if len(url) > 60 else url
            elif col == self.COL_TITLE:
                title = row_data.get("title") or ""
                return title[:47] + "..." if len(title) > 50 else title
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_CLOSED_AT:
                ts = row_data.get("closed_at_utc")
                if ts:
                    return ts[:16] if len(ts) > 16 else ts
                return ""
            elif col == self.COL_WINDOW:
                return row_data.get("original_window_id") or ""
            elif col == self.COL_TAB:
                return row_data.get("original_tab_index") or ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_URL:
                return row_data.get("url", "")
            elif col == self.COL_TITLE:
                return row_data.get("title", "")
            elif col == self.COL_CLOSED_AT:
                return row_data.get("closed_at_utc", "")
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.TextAlignmentRole:
            if col in (self.COL_WINDOW, self.COL_TAB):
                return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
