"""
Qt model for media history table.

Displays parsed media playback history with filtering by browser and origin.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_media_playback

logger = logging.getLogger(__name__)


class MediaHistoryTableModel(QAbstractTableModel):
    """
    Qt model for media playback history table.

    Displays media playback records extracted from Chromium Media History database.
    """

    # Column definitions - DB column names
    COLUMNS = [
        "url",
        "browser",
        "profile",
        "watch_time_seconds",
        "has_video",
        "has_audio",
        "last_played_utc",  # DB uses last_played_utc, not last_updated
        "tags",
    ]

    HEADERS = [
        "URL",
        "Browser",
        "Profile",
        "Watch Time",
        "Video",
        "Audio",
        "Last Played",  # Renamed to match DB column semantics
        "Tags",
    ]

    # Column indexes
    COL_URL = 0
    COL_BROWSER = 1
    COL_PROFILE = 2
    COL_WATCH_TIME = 3
    COL_VIDEO = 4
    COL_AUDIO = 5
    COL_LAST_PLAYED = 6  # Renamed to match DB column
    COL_TAGS = 7

    ARTIFACT_TYPE = "media_playback"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize media history model.

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

    def load(self, browser_filter: str = "") -> None:
        """
        Load media history from database with optional filters.

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
                self._rows = get_media_playback(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    limit=5000,
                )
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} media playback records")

        except Exception as e:
            logger.error(f"Failed to load media history: {e}", exc_info=True)
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
        """Get list of browsers that have media history data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM media_playback WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get media browsers: {e}", exc_info=True)
            return []

    def get_total_watch_time(self) -> int:
        """Return total watch time in seconds for current dataset."""
        return sum(r.get("watch_time_seconds") or 0 for r in self._rows)

    def get_video_count(self) -> int:
        """Return count of records with video."""
        return sum(1 for r in self._rows if r.get("has_video"))

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """Get full row data for given index."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return self._rows[index.row()]

    @staticmethod
    def format_watch_time(seconds: int) -> str:
        """Format seconds into human-readable time."""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            minutes = seconds // 60
            secs = seconds % 60
            return f"{minutes}m {secs}s"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            return f"{hours}h {minutes}m"

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
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_WATCH_TIME:
                seconds = row_data.get("watch_time_seconds") or 0
                return self.format_watch_time(seconds)
            elif col == self.COL_VIDEO:
                return "Yes" if row_data.get("has_video") else "No"
            elif col == self.COL_AUDIO:
                return "Yes" if row_data.get("has_audio") else "No"
            elif col == self.COL_LAST_PLAYED:
                updated = row_data.get("last_played_utc")  # Use last_played_utc from DB
                if updated:
                    return updated[:16] if len(updated) > 16 else updated
                return ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_URL:
                return row_data.get("url", "")
            elif col == self.COL_WATCH_TIME:
                seconds = row_data.get("watch_time_seconds") or 0
                return f"{seconds:,} seconds"
            elif col == self.COL_LAST_PLAYED:
                return row_data.get("last_played_utc", "")  # Use last_played_utc from DB
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.TextAlignmentRole:
            if col in (self.COL_VIDEO, self.COL_AUDIO):
                return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
