"""
Qt model for browser_downloads table.

Displays browser download history with state/danger info and filtering.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor

from core.database import get_browser_downloads, get_distinct_download_browsers, get_browser_download_stats
from core.database import DatabaseManager

logger = logging.getLogger(__name__)


class BrowserDownloadsTableModel(QAbstractTableModel):
    """
    Qt model for browser_downloads table.

    Displays browser download history with color coding for state.
    """

    # Column definitions
    COLUMNS = [
        "filename",
        "url",
        "browser",
        "state",
        "danger_type",
        "total_bytes",
        "start_time_utc",
        "end_time_utc",
        "tags",
    ]

    HEADERS = [
        "Filename",
        "URL",
        "Browser",
        "State",
        "Danger",
        "Size",
        "Start Time",
        "End Time",
        "Tags",
    ]

    # Column indexes
    COL_FILENAME = 0
    COL_URL = 1
    COL_BROWSER = 2
    COL_STATE = 3
    COL_DANGER = 4
    COL_SIZE = 5
    COL_START_TIME = 6
    COL_END_TIME = 7
    COL_TAGS = 8

    ARTIFACT_TYPE = "browser_download"

    # State colors
    STATE_COLORS = {
        "complete": QColor(144, 238, 144),      # Light green
        "in_progress": QColor(255, 255, 150),   # Light yellow
        "cancelled": QColor(255, 182, 182),     # Light red
        "interrupted": QColor(255, 182, 182),   # Light red
        "interrupted_network": QColor(255, 200, 200),  # Lighter red
    }

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize browser downloads model.

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
        self._state_filter: str = ""
        self._filename_filter: str = ""

        # Stats cache
        self._stats: Dict[str, Any] = {}

    def load(
        self,
        browser_filter: str = "",
        state_filter: str = "",
        filename_filter: str = ""
    ) -> None:
        """
        Load browser downloads from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            state_filter: State filter (empty = all)
            filename_filter: Filename substring filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._state_filter = state_filter
        self._filename_filter = filename_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_browser_downloads(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    state=state_filter or None,
                    filename=filename_filter or None,
                    limit=5000,
                )
                # Also get stats
                self._stats = get_browser_download_stats(conn, self.evidence_id)
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} browser downloads")

        except Exception as e:
            logger.error(f"Failed to load browser downloads: {e}", exc_info=True)
            self._rows = []
            self._stats = {}
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
        """Get list of browsers that have downloads."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                return get_distinct_download_browsers(conn, self.evidence_id)
        except Exception as e:
            logger.error(f"Failed to get download browsers: {e}", exc_info=True)
            return []

    def get_stats(self) -> Dict[str, Any]:
        """Get download statistics."""
        return self._stats

    def get_total_bytes(self) -> int:
        """Get total bytes from all downloads."""
        return self._stats.get("total_bytes", 0)

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """Get full row data for given index."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return self._rows[index.row()]

    @staticmethod
    def format_bytes(size_bytes: Optional[int]) -> str:
        """Format bytes as human-readable size."""
        if size_bytes is None or size_bytes == 0:
            return ""

        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

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
            if col == self.COL_FILENAME:
                filename = row_data.get("filename", "")
                # Truncate long filenames
                if len(filename) > 50:
                    return "..." + filename[-47:]
                return filename
            elif col == self.COL_URL:
                url = row_data.get("url", "")
                # Truncate long URLs
                if len(url) > 60:
                    return url[:57] + "..."
                return url
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_STATE:
                state = row_data.get("state") or ""
                # Make state readable
                return state.replace("_", " ").title()
            elif col == self.COL_DANGER:
                danger = row_data.get("danger_type") or "not_dangerous"
                if danger == "not_dangerous":
                    return ""
                return danger.replace("_", " ").title()
            elif col == self.COL_SIZE:
                return self.format_bytes(row_data.get("total_bytes"))
            elif col == self.COL_START_TIME:
                start = row_data.get("start_time_utc")
                if start:
                    # Show date and time
                    return start[:16] if len(start) > 16 else start
                return ""
            elif col == self.COL_END_TIME:
                end = row_data.get("end_time_utc")
                if end:
                    return end[:16] if len(end) > 16 else end
                return ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_FILENAME:
                return row_data.get("filename", "")
            elif col == self.COL_URL:
                return row_data.get("url", "")
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""
            elif col == self.COL_DANGER:
                danger = row_data.get("danger_type") or "not_dangerous"
                # Provide explanation
                explanations = {
                    "not_dangerous": "No known risk",
                    "dangerous_file": "File type is potentially dangerous",
                    "dangerous_url": "URL is known to be dangerous",
                    "dangerous_content": "Content flagged as dangerous",
                    "uncommon_content": "Content is uncommon/suspicious",
                    "user_validated": "User validated as safe",
                }
                return explanations.get(danger, danger)

        elif role == Qt.BackgroundRole:
            # Color code by state
            state = row_data.get("state") or ""
            if state in self.STATE_COLORS:
                return self.STATE_COLORS[state]

        elif role == Qt.TextAlignmentRole:
            if col == self.COL_SIZE:
                return Qt.AlignRight | Qt.AlignVCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
