"""Qt model for Jump Lists entries table.

Displays Windows Jump List entries with filtering by browser and pin status.
Uses centralized AppID registry for application name resolution.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import DatabaseManager
from extractors._shared.appid_loader import get_app_name

logger = logging.getLogger(__name__)


class JumpListsTableModel(QAbstractTableModel):
    """
    Qt model for jump_list_entries table.

    Displays Windows Jump List entries extracted from AutomaticDestinations-ms
    and CustomDestinations-ms files.
    """

    # Column definitions - includes both URL (for browsers) and Target Path (for file apps)
    COLUMNS = [
        "browser",
        "url",
        "target_path",
        "title",
        "lnk_access_time",
        "lnk_creation_time",
        "access_count",
        "pin_status",
        "appid",
        "jumplist_path",
        "tags",
    ]

    HEADERS = [
        "Application",
        "URL",
        "Target Path",
        "Title",
        "Access Time",
        "Creation Time",
        "Access Count",
        "Pin Status",
        "App ID",
        "Jump List Path",
        "Tags",
    ]

    # Column indexes
    COL_APPLICATION = 0
    COL_URL = 1
    COL_TARGET_PATH = 2
    COL_TITLE = 3
    COL_ACCESS_TIME = 4
    COL_CREATION_TIME = 5
    COL_ACCESS_COUNT = 6
    COL_PIN_STATUS = 7
    COL_APPID = 8
    COL_PATH = 9
    COL_TAGS = 10

    ARTIFACT_TYPE = "jump_list"

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize Jump Lists model.

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
        self._pin_status_filter: str = ""
        self._urls_only: bool = False

        # Initial load
        self._load_data()

    def _load_data(self) -> None:
        """Load data from database with current filters."""
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )
            if not conn:
                logger.warning("No evidence connection for Jump Lists")
                self._rows = []
                return

            # Build query with filters
            # Note: In evidence-local database architecture, evidence_id filtering
            # is not needed since each evidence has its own database file.
            # The evidence_id column may have different values but all rows
            # belong to this evidence.
            where_clauses = []
            params: List[Any] = []

            if self._browser_filter:
                where_clauses.append("browser = ?")
                params.append(self._browser_filter)

            if self._pin_status_filter:
                where_clauses.append("pin_status = ?")
                params.append(self._pin_status_filter)

            if self._urls_only:
                where_clauses.append("url IS NOT NULL AND url != ''")

            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            sql = f"""
                SELECT id, evidence_id, appid, browser, jumplist_path, entry_id,
                       target_path, arguments, url, title, lnk_creation_time,
                       lnk_modification_time, lnk_access_time, access_count,
                       pin_status, source_path, run_id, discovered_by,
                       partition_index, fs_type, logical_path, forensic_path,
                       tags, notes, created_at_utc
                FROM jump_list_entries
                WHERE {where_sql}
                ORDER BY lnk_access_time DESC
                LIMIT 5000
            """

            cursor = conn.execute(sql, params)
            self._rows = [dict(row) for row in cursor.fetchall()]
            self._refresh_tags()

        except Exception as e:
            logger.error("Failed to load Jump Lists: %s", e)
            self._rows = []
            self._tag_map = {}

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

    def reload(self) -> None:
        """Reload data from database."""
        self.beginResetModel()
        self._load_data()
        self.endResetModel()

    def set_browser_filter(self, browser: str) -> None:
        """Set browser filter."""
        if browser != self._browser_filter:
            self._browser_filter = browser
            self.reload()

    def set_pin_status_filter(self, pin_status: str) -> None:
        """Set pin status filter."""
        if pin_status != self._pin_status_filter:
            self._pin_status_filter = pin_status
            self.reload()

    def set_urls_only(self, urls_only: bool) -> None:
        """Toggle showing only entries with URLs."""
        if urls_only != self._urls_only:
            self._urls_only = urls_only
            self.reload()

    def set_filters(
        self,
        browser: str = "",
        pin_status: str = "",
        urls_only: bool = False
    ) -> None:
        """Set all filters at once."""
        changed = (
            browser != self._browser_filter or
            pin_status != self._pin_status_filter or
            urls_only != self._urls_only
        )
        if changed:
            self._browser_filter = browser
            self._pin_status_filter = pin_status
            self._urls_only = urls_only
            self.reload()

    # ----- QAbstractTableModel Interface -----

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self.COLUMNS)

    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.DisplayRole
    ):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if row < 0 or row >= len(self._rows):
            return None

        record = self._rows[row]

        if role == Qt.DisplayRole:
            column_name = self.COLUMNS[col]
            value = record.get(column_name)

            # Format specific columns
            if col == self.COL_APPLICATION:
                # Show browser name if set, otherwise resolve AppID to app name
                if value:
                    return value
                appid = record.get("appid", "")
                if appid:
                    return get_app_name(appid)
                return "Unknown"
            if col == self.COL_ACCESS_COUNT:
                return str(value) if value is not None else ""
            if col == self.COL_PIN_STATUS:
                return (value or "recent").title()
            if col == self.COL_URL:
                return value or ""
            if col == self.COL_TARGET_PATH:
                return value or ""
            if col == self.COL_TAGS:
                return self._tag_map.get(record.get("id"), "") or ""

            return str(value) if value is not None else ""

        if role == Qt.UserRole:
            return record

        if role == Qt.ToolTipRole:
            if col == self.COL_URL:
                url = record.get("url", "")
                if url:
                    return url
            if col == self.COL_TARGET_PATH:
                # Show full path in tooltip
                target_path = record.get("target_path", "")
                if target_path:
                    return target_path
            if col == self.COL_PATH:
                return record.get("jumplist_path", "")
            if col == self.COL_TAGS:
                return self._tag_map.get(record.get("id"), "") or ""

        return None

    def get_row_data(self, row: int) -> Optional[Dict[str, Any]]:
        """Get full row data for details dialog."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_browsers(self) -> List[str]:
        """Get list of distinct browsers in current data."""
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )
            if not conn:
                return []

            # No evidence_id filter needed - evidence-local database
            cursor = conn.execute(
                """
                SELECT DISTINCT browser FROM jump_list_entries
                WHERE browser IS NOT NULL
                ORDER BY browser
                """
            )
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error("Failed to get browsers: %s", e)
            return []

    def get_stats(self) -> Dict[str, Any]:
        """Get Jump List statistics."""
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )
            if not conn:
                return {"total": 0, "url_count": 0, "pinned_count": 0, "by_browser": {}}

            # Custom stats query without evidence_id filter (evidence-local DB)
            total = conn.execute("SELECT COUNT(*) FROM jump_list_entries").fetchone()[0]

            url_count = conn.execute(
                "SELECT COUNT(*) FROM jump_list_entries WHERE url IS NOT NULL AND url != ''"
            ).fetchone()[0]

            pinned_count = conn.execute(
                "SELECT COUNT(*) FROM jump_list_entries WHERE pin_status = 'pinned'"
            ).fetchone()[0]

            by_browser = {}
            for row in conn.execute(
                "SELECT browser, COUNT(*) as count FROM jump_list_entries GROUP BY browser"
            ):
                by_browser[row[0] or "Unknown"] = row[1]

            return {
                "total": total,
                "url_count": url_count,
                "pinned_count": pinned_count,
                "by_browser": by_browser,
            }
        except Exception as e:
            logger.error("Failed to get Jump List stats: %s", e)
            return {"total": 0, "url_count": 0, "pinned_count": 0, "by_browser": {}}
