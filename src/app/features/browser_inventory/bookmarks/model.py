"""
Qt model for bookmarks table.

Displays parsed bookmarks with filtering by browser and folder.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_bookmarks, get_distinct_bookmark_browsers, get_bookmark_folders
from core.database import DatabaseManager

logger = logging.getLogger(__name__)


class BookmarksTableModel(QAbstractTableModel):
    """
    Qt model for bookmarks table.

    Displays parsed bookmarks extracted from browser databases.
    Uses flat table with folder_path column (not tree view).
    """

    # Column definitions
    COLUMNS = [
        "title",
        "url",
        "folder_path",
        "browser",
        "profile",
        "date_added_utc",
        "tags",
    ]

    HEADERS = [
        "Title",
        "URL",
        "Folder",
        "Browser",
        "Profile",
        "Date Added",
        "Tags",
    ]

    # Column indexes
    COL_TITLE = 0
    COL_URL = 1
    COL_FOLDER = 2
    COL_BROWSER = 3
    COL_PROFILE = 4
    COL_DATE_ADDED = 5
    COL_TAGS = 6

    ARTIFACT_TYPE = "bookmark"

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize bookmarks model.

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
        self._folder_filter: str = ""

    def load(self, browser_filter: str = "", folder_filter: str = "") -> None:
        """
        Load bookmarks from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            folder_filter: Folder path prefix filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._folder_filter = folder_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_bookmarks(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    folder_path=folder_filter or None,
                    limit=5000,
                )
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} bookmarks")

        except Exception as e:
            logger.error(f"Failed to load bookmarks: {e}", exc_info=True)
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
        """Get list of browsers that have bookmarks."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                return get_distinct_bookmark_browsers(conn, self.evidence_id)
        except Exception as e:
            logger.error(f"Failed to get bookmark browsers: {e}", exc_info=True)
            return []

    def get_available_folders(self) -> List[str]:
        """Get list of distinct folder paths."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                folders = get_bookmark_folders(conn, self.evidence_id)
                return [f["folder_path"] for f in folders if f["folder_path"]]
        except Exception as e:
            logger.error(f"Failed to get bookmark folders: {e}", exc_info=True)
            return []

    def get_folder_count(self) -> int:
        """Return count of unique folders in current dataset."""
        folders = set()
        for row in self._rows:
            fp = row.get("folder_path")
            if fp:
                folders.add(fp)
        return len(folders)

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
            if col == self.COL_TITLE:
                title = row_data.get("title", "")
                # Truncate long titles
                if len(title) > 60:
                    return title[:57] + "..."
                return title
            elif col == self.COL_URL:
                url = row_data.get("url", "")
                # Truncate long URLs
                if len(url) > 80:
                    return url[:77] + "..."
                return url
            elif col == self.COL_FOLDER:
                folder = row_data.get("folder_path") or ""
                # Truncate long folder paths
                if len(folder) > 40:
                    return "..." + folder[-37:]
                return folder
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_DATE_ADDED:
                date = row_data.get("date_added_utc")
                if date:
                    # Truncate to date only
                    return date[:10] if len(date) > 10 else date
                return ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_TITLE:
                return row_data.get("title", "")
            elif col == self.COL_URL:
                return row_data.get("url", "")
            elif col == self.COL_FOLDER:
                return row_data.get("folder_path") or ""
            elif col == self.COL_DATE_ADDED:
                return row_data.get("date_added_utc") or ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
