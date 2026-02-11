"""
Qt model for site engagement table.

Displays site and media engagement data from Chromium browsers.
Engagement scores indicate user interaction levels with sites.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_site_engagements

logger = logging.getLogger(__name__)


class SiteEngagementTableModel(QAbstractTableModel):
    """
    Qt model for site engagement table.

    Displays site and media engagement records extracted from Chromium Preferences.
    Site engagement tracks user interactions (browsing, typing, clicking).
    Media engagement tracks media playback activity.
    """

    # Column definitions - DB column names
    COLUMNS = [
        "origin",
        "engagement_type",
        "browser",
        "profile",
        "raw_score",
        "visits",
        "media_playbacks",
        "last_engagement_time_utc",
        "tags",
    ]

    HEADERS = [
        "Origin",
        "Type",
        "Browser",
        "Profile",
        "Score",
        "Visits",
        "Playbacks",
        "Last Engagement",
        "Tags",
    ]

    # Column indexes
    COL_ORIGIN = 0
    COL_TYPE = 1
    COL_BROWSER = 2
    COL_PROFILE = 3
    COL_SCORE = 4
    COL_VISITS = 5
    COL_PLAYBACKS = 6
    COL_LAST_ENGAGEMENT = 7
    COL_TAGS = 8

    ARTIFACT_TYPE = "site_engagement"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize site engagement model.

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
        self._type_filter: str = ""
        self._min_score: Optional[float] = None

    def load(
        self,
        browser_filter: str = "",
        type_filter: str = "",
        min_score: Optional[float] = None
    ) -> None:
        """
        Load site engagement from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            type_filter: Engagement type filter ("site_engagement", "media_engagement", or empty)
            min_score: Minimum score filter (only for site_engagement type)
        """
        self._browser_filter = browser_filter
        self._type_filter = type_filter
        self._min_score = min_score

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_site_engagements(
                    conn,
                    self.evidence_id,
                    engagement_type=type_filter or None,
                    browser=browser_filter or None,
                    min_score=min_score,
                    limit=5000,
                )
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} site engagement records")

        except Exception as e:
            logger.error(f"Failed to load site engagement: {e}", exc_info=True)
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
        """Get list of browsers that have site engagement data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM site_engagement WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get engagement browsers: {e}", exc_info=True)
            return []

    def get_available_types(self) -> List[str]:
        """Get list of engagement types in the data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT engagement_type FROM site_engagement WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["engagement_type"] for row in cursor.fetchall() if row["engagement_type"]]
        except Exception as e:
            logger.error(f"Failed to get engagement types: {e}", exc_info=True)
            return []

    def get_site_engagement_count(self) -> int:
        """Return count of site_engagement records."""
        return sum(1 for r in self._rows if r.get("engagement_type") == "site_engagement")

    def get_media_engagement_count(self) -> int:
        """Return count of media_engagement records."""
        return sum(1 for r in self._rows if r.get("engagement_type") == "media_engagement")

    def get_max_score(self) -> float:
        """Return maximum engagement score."""
        scores = [r.get("raw_score") or 0 for r in self._rows if r.get("raw_score") is not None]
        return max(scores) if scores else 0

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """Get full row data for given index."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return self._rows[index.row()]

    @staticmethod
    def format_score(score: Optional[float]) -> str:
        """Format engagement score for display."""
        if score is None:
            return "-"
        return f"{score:.2f}"

    @staticmethod
    def format_type(engagement_type: str) -> str:
        """Format engagement type for display."""
        if engagement_type == "site_engagement":
            return "Site"
        elif engagement_type == "media_engagement":
            return "Media"
        return engagement_type

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
                origin = row_data.get("origin", "")
                if len(origin) > 50:
                    return origin[:47] + "..."
                return origin
            elif col == self.COL_TYPE:
                return self.format_type(row_data.get("engagement_type", ""))
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_SCORE:
                return self.format_score(row_data.get("raw_score"))
            elif col == self.COL_VISITS:
                visits = row_data.get("visits")
                return str(visits) if visits is not None else "-"
            elif col == self.COL_PLAYBACKS:
                playbacks = row_data.get("media_playbacks")
                return str(playbacks) if playbacks is not None else "-"
            elif col == self.COL_LAST_ENGAGEMENT:
                # Use last_engagement_time_utc for site, last_media_playback_time_utc for media
                timestamp = row_data.get("last_engagement_time_utc") or row_data.get("last_media_playback_time_utc")
                if timestamp:
                    return timestamp[:16] if len(timestamp) > 16 else timestamp
                return ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_ORIGIN:
                return row_data.get("origin", "")
            elif col == self.COL_SCORE:
                score = row_data.get("raw_score")
                if score is not None:
                    return f"Engagement score: {score:.4f}"
                return "No score (media engagement)"
            elif col == self.COL_LAST_ENGAGEMENT:
                return row_data.get("last_engagement_time_utc") or row_data.get("last_media_playback_time_utc") or ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.TextAlignmentRole:
            if col in (self.COL_SCORE, self.COL_VISITS, self.COL_PLAYBACKS):
                return Qt.AlignRight | Qt.AlignVCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
