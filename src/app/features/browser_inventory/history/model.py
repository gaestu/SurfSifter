"""
Qt model for browser_history table.

Displays parsed browser history with filtering by browser, profile, and URL.
Updated Uses transition_type column directly instead of notes JSON.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import (
    get_browser_history,
    get_distinct_history_browsers,
    get_distinct_history_profiles,
    get_browser_history_stats,
)
from core.database import DatabaseManager

logger = logging.getLogger(__name__)


# Chromium page transition types (core types, bits 0-7)
# See: https://source.chromium.org/chromium/chromium/src/+/main:ui/base/page_transition_types.h
TRANSITION_TYPES = {
    0: "link",           # User clicked a link
    1: "typed",          # User typed URL in omnibox
    2: "auto_bookmark",  # User selected from bookmarks/history
    3: "auto_subframe",  # Automatic subframe navigation
    4: "manual_subframe",  # User-initiated subframe navigation
    5: "generated",      # Keyword search or omnibox suggestion
    6: "auto_toplevel",  # Automatic top-level navigation (e.g., meta refresh)
    7: "form_submit",    # User submitted a form
    8: "reload",         # Page reload
    9: "keyword",        # Keyword search (e.g., search engine shortcut)
    10: "keyword_generated",  # Generated from keyword search
}


def get_transition_label(transition) -> str:
    """
    Convert a transition code to a human-readable label.

    Chromium stores transitions as integer bitmasks (core type in bits 0-7).
    Safari and Firefox may store them as plain strings (e.g., "link", "typed").

    Args:
        transition: Chromium page transition code (int) or label string

    Returns:
        Human-readable label (e.g., "link", "typed", "reload")
    """
    if transition is None:
        return ""
    # Safari/Firefox may store transition_type as a human-readable string.
    if isinstance(transition, str):
        # Try converting numeric strings to int for Chromium-style data.
        try:
            transition = int(transition)
        except (ValueError, TypeError):
            return transition
    # Extract core type (bits 0-7)
    core_type = transition & 0xFF
    return TRANSITION_TYPES.get(core_type, f"unknown_{core_type}")


class BrowserHistoryTableModel(QAbstractTableModel):
    """
    Qt model for browser_history table.

    Displays parsed browser history extracted from browser databases.
    Shows per-visit records with forensic metadata.
    """

    # Column definitions (added duration, hidden)
    COLUMNS = [
        "url",
        "title",
        "ts_utc",
        "browser",
        "profile",
        "visit_count",
        "typed_count",
        "visit_type_label",
        "duration",
        "hidden",
        "tags",
    ]

    HEADERS = [
        "URL",
        "Title",
        "Visit Time",
        "Browser",
        "Profile",
        "Visits",
        "Typed",
        "Type",
        "Duration",
        "Hidden",
        "Tags",
    ]

    # Column indexes
    COL_URL = 0
    COL_TITLE = 1
    COL_VISIT_TIME = 2
    COL_BROWSER = 3
    COL_PROFILE = 4
    COL_VISIT_COUNT = 5
    COL_TYPED_COUNT = 6
    COL_VISIT_TYPE = 7
    COL_DURATION = 8
    COL_HIDDEN = 9
    COL_TAGS = 10

    ARTIFACT_TYPE = "browser_history"

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize browser history model.

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
        self._profile_filter: str = ""
        self._visit_type_filter: str = ""
        self._url_filter: str = ""

    def load(
        self,
        browser_filter: str = "",
        profile_filter: str = "",
        visit_type_filter: str = "",
        url_filter: str = "",
    ) -> None:
        """
        Load browser history from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            profile_filter: Profile name filter (empty = all)
            visit_type_filter: Visit type label filter (empty = all)
            url_filter: URL substring filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._profile_filter = profile_filter
        self._visit_type_filter = visit_type_filter
        self._url_filter = url_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = get_browser_history(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    profile=profile_filter or None,
                    url_filter=url_filter or None,
                    limit=10000,
                )
                # Enrich with visit_type_label from transition_type column
                for row in rows:
                    row["visit_type_label"] = get_transition_label(row.get("transition_type"))

                # Apply visit_type filter after enrichment
                if visit_type_filter:
                    rows = [
                        r for r in rows
                        if r.get("visit_type_label", "").lower() == visit_type_filter.lower()
                    ]

                self._rows = rows
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} browser history records")

        except Exception as e:
            logger.error(f"Failed to load browser history: {e}", exc_info=True)
            self._rows = []
            self._tag_map = {}

        self.endResetModel()

    def _format_duration(self, duration_ms: Optional[int]) -> str:
        """Format duration in milliseconds to human-readable format."""
        if duration_ms is None or duration_ms <= 0:
            return ""
        seconds = duration_ms // 1000
        if seconds < 60:
            return f"{seconds}s"
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        if minutes < 60:
            return f"{minutes}m {remaining_seconds}s"
        hours = minutes // 60
        remaining_minutes = minutes % 60
        return f"{hours}h {remaining_minutes}m"

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
            if col == self.COL_URL:
                url = record.get("url", "")
                # Truncate long URLs
                return url[:100] + "..." if len(url) > 100 else url
            elif col == self.COL_TITLE:
                title = record.get("title", "")
                return title[:80] + "..." if title and len(title) > 80 else title
            elif col == self.COL_VISIT_TIME:
                ts = record.get("ts_utc", "")
                # Format timestamp for display (show date and time)
                if ts:
                    return ts.replace("T", " ")[:19]
                return ""
            elif col == self.COL_BROWSER:
                return record.get("browser", "")
            elif col == self.COL_PROFILE:
                return record.get("profile", "")
            elif col == self.COL_VISIT_COUNT:
                return record.get("visit_count", 0)
            elif col == self.COL_TYPED_COUNT:
                return record.get("typed_count", 0)
            elif col == self.COL_VISIT_TYPE:
                return record.get("visit_type_label", "")
            elif col == self.COL_DURATION:
                return self._format_duration(record.get("visit_duration_ms"))
            elif col == self.COL_HIDDEN:
                hidden = record.get("hidden", 0)
                return "Yes" if hidden else ""
            elif col == self.COL_TAGS:
                record_id = record.get("id")
                return self._tag_map.get(record_id, "")

        elif role == Qt.ToolTipRole:
            if col == self.COL_URL:
                return record.get("url", "")
            elif col == self.COL_TITLE:
                return record.get("title", "")
            elif col == self.COL_VISIT_TYPE:
                # Show detailed transition info
                transition = record.get("transition_type")
                if transition is not None:
                    parts = [f"Transition code: {transition}"]
                    parts.append(f"Type: {get_transition_label(transition)}")
                    # Decode qualifiers (bits 8+)
                    if transition & 0x01000000:
                        parts.append("Qualifier: Forward/Back navigation")
                    if transition & 0x02000000:
                        parts.append("Qualifier: From address bar")
                    if transition & 0x04000000:
                        parts.append("Qualifier: Home page")
                    if transition & 0x08000000:
                        parts.append("Qualifier: From API (e.g., extension)")
                    if transition & 0x10000000:
                        parts.append("Qualifier: User started navigation in URL bar")
                    if transition & 0x20000000:
                        parts.append("Qualifier: User started navigation without URL bar")
                    if transition & 0x40000000:
                        parts.append("Qualifier: Server-side redirect")
                    if transition & 0x80000000:
                        parts.append("Qualifier: Client-side redirect")
                    return "\n".join(parts)
                return None
            elif col == self.COL_DURATION:
                duration_ms = record.get("visit_duration_ms")
                if duration_ms is not None and duration_ms > 0:
                    return f"Duration: {duration_ms:,} ms ({self._format_duration(duration_ms)})"
                return None
            elif col == self.COL_HIDDEN:
                if record.get("hidden"):
                    return "Hidden visit (subframe navigation, error page, or ad frame)"
                return None

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
        """Get full row data for details dialog."""
        if not index.isValid():
            return None
        row = index.row()
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_row_ids(self, rows: List[int]) -> List[int]:
        """Get IDs for specified row indices."""
        return [
            self._rows[row].get("id")
            for row in rows
            if 0 <= row < len(self._rows) and self._rows[row].get("id")
        ]

    def get_browsers(self) -> List[str]:
        """Get distinct browsers for filter dropdown."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                return get_distinct_history_browsers(conn, self.evidence_id)
        except Exception as e:
            logger.error(f"Failed to get browsers: {e}")
            return []

    def get_profiles(self, browser: Optional[str] = None) -> List[str]:
        """Get distinct profiles for filter dropdown."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                return get_distinct_history_profiles(conn, self.evidence_id, browser)
        except Exception as e:
            logger.error(f"Failed to get profiles: {e}")
            return []

    def get_stats(self) -> Dict[str, Any]:
        """Get browser history statistics."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                stats = get_browser_history_stats(conn, self.evidence_id)
                # Add typed count from current rows
                # Handle None values explicitly (row may have typed_count=None)
                typed_count = sum(
                    1 for row in self._rows
                    if (row.get("typed_count") or 0) > 0
                )
                stats["typed_count"] = typed_count
                return stats
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return {
                "total_visits": 0,
                "unique_urls": 0,
                "browser_count": 0,
                "earliest_visit": None,
                "latest_visit": None,
                "typed_count": 0,
            }
