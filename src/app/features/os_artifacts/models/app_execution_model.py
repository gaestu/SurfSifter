"""Qt model for Application Execution (UserAssist) table.

Displays application execution history extracted from Windows registry
UserAssist keys with decoded paths, run counts, and timestamps.

Features:
- ROT13-decoded application paths
- Run count, focus count, focus time display
- Forensic interest highlighting (browsers, wiping tools, Tor, etc.)
- Filtering by path and forensic-only
- Sorting by any column
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QBrush, QColor

from core.database import DatabaseManager

logger = logging.getLogger(__name__)


def _format_focus_time(ms: Optional[int]) -> str:
    """Format focus time from milliseconds to human-readable string."""
    if ms is None or ms == 0:
        return ""
    seconds = ms // 1000
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    remaining_seconds = seconds % 60
    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"
    hours = minutes // 60
    remaining_minutes = minutes % 60
    return f"{hours}h {remaining_minutes}m"


class AppExecutionModel(QAbstractTableModel):
    """
    Qt model for application execution from os_indicators table.

    Queries indicators with type='execution:user_assist' and parses
    extra_json for decoded_path, run_count, focus_count, focus_time, last_run.
    """

    COLUMNS = [
        "decoded_path",
        "run_count",
        "last_run",
        "focus_time",
        "focus_count",
        "source",
    ]

    HEADERS = [
        "Application Path",
        "Run Count",
        "Last Run",
        "Focus Time",
        "Focus Count",
        "Source",
    ]

    COL_PATH = 0
    COL_RUN_COUNT = 1
    COL_LAST_RUN = 2
    COL_FOCUS_TIME = 3
    COL_FOCUS_COUNT = 4
    COL_SOURCE = 5

    # Forensic interest colors (same palette as InstalledSoftwareModel)
    COLOR_BROWSER = QColor(200, 220, 255)        # Light blue for browsers
    COLOR_WIPING = QColor(255, 200, 200)          # Light red for wiping tools
    COLOR_TOR = QColor(255, 220, 180)             # Light orange for Tor
    COLOR_ENCRYPTION = QColor(255, 255, 180)      # Light yellow for encryption
    COLOR_PRIVACY = QColor(220, 255, 220)         # Light green for privacy tools
    COLOR_FILE_SHARING = QColor(230, 210, 255)    # Light purple for file sharing

    FORENSIC_COLORS = {
        "browser": COLOR_BROWSER,
        "wiping_tool": COLOR_WIPING,
        "tor": COLOR_TOR,
        "encryption": COLOR_ENCRYPTION,
        "privacy": COLOR_PRIVACY,
        "file_sharing": COLOR_FILE_SHARING,
    }

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        parent=None,
    ):
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label

        self._rows: List[Dict[str, Any]] = []
        self._search_text: str = ""
        self._forensic_only: bool = False

        self._load_data()

    def _load_data(self) -> None:
        """Load data from database with current filters."""
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )
            if not conn:
                logger.warning("No evidence connection for App Execution")
                self._rows = []
                return

            sql = """
                SELECT id, value, path, hive, extra_json
                FROM os_indicators
                WHERE type = 'execution:user_assist'
                ORDER BY value COLLATE NOCASE
            """

            cursor = conn.execute(sql)
            raw_rows = cursor.fetchall()

            parsed_rows = []
            for row in raw_rows:
                row_id, value, path, hive, extra_json_str = row

                extra = {}
                if extra_json_str:
                    try:
                        extra = json.loads(extra_json_str)
                    except json.JSONDecodeError:
                        pass

                row_data = {
                    "id": row_id,
                    "decoded_path": extra.get("decoded_path", value or ""),
                    "run_count": extra.get("run_count"),
                    "focus_count": extra.get("focus_count"),
                    "focus_time_ms": extra.get("focus_time_ms"),
                    "last_run_utc": extra.get("last_run_utc", ""),
                    "rot13_name": extra.get("rot13_name", ""),
                    "forensic_interest": extra.get("forensic_interest", False),
                    "forensic_category": extra.get("forensic_category", ""),
                    "path": path,
                    "hive": hive,
                    "extra": extra,
                }
                parsed_rows.append(row_data)

            self._rows = self._apply_filters(parsed_rows)

        except Exception as e:
            logger.exception("Failed to load app execution data: %s", e)
            self._rows = []

    def _apply_filters(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply search and forensic filters."""
        result = rows

        if self._search_text:
            search_lower = self._search_text.lower()
            result = [
                r for r in result
                if search_lower in r.get("decoded_path", "").lower()
            ]

        if self._forensic_only:
            result = [r for r in result if r.get("forensic_interest")]

        return result

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self.HEADERS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None

        row = self._rows[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            if col == self.COL_PATH:
                return row.get("decoded_path", "")
            elif col == self.COL_RUN_COUNT:
                count = row.get("run_count")
                return str(count) if count is not None else ""
            elif col == self.COL_LAST_RUN:
                return row.get("last_run_utc", "").replace("T", " ").split("+")[0] if row.get("last_run_utc") else ""
            elif col == self.COL_FOCUS_TIME:
                return _format_focus_time(row.get("focus_time_ms"))
            elif col == self.COL_FOCUS_COUNT:
                count = row.get("focus_count")
                return str(count) if count is not None else ""
            elif col == self.COL_SOURCE:
                hive = row.get("hive", "")
                if hive:
                    # Extract just the filename from the hive path
                    parts = hive.replace("\\", "/").split("/")
                    return parts[-1] if parts else hive
                return ""

        elif role == Qt.BackgroundRole:
            if row.get("forensic_interest"):
                category = row.get("forensic_category", "")
                color = self.FORENSIC_COLORS.get(category)
                if color:
                    return QBrush(color)

        elif role == Qt.ToolTipRole:
            tips = [f"Path: {row.get('decoded_path', '')}"]
            if row.get("run_count") is not None:
                tips.append(f"Run Count: {row['run_count']}")
            if row.get("focus_count") is not None:
                tips.append(f"Focus Count: {row['focus_count']}")
            if row.get("focus_time_ms") is not None:
                tips.append(f"Focus Time: {_format_focus_time(row['focus_time_ms'])}")
            if row.get("last_run_utc"):
                tips.append(f"Last Run: {row['last_run_utc']}")
            if row.get("forensic_category"):
                tips.append(f"Forensic Category: {row['forensic_category']}")
            return "\n".join(tips)

        elif role == Qt.UserRole:
            return row

        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self.HEADERS):
            return self.HEADERS[section]
        return None

    def set_filters(self, search_text: str = "", forensic_only: bool = False) -> None:
        """Apply filters and reload."""
        self._search_text = search_text
        self._forensic_only = forensic_only
        self.reload()

    def reload(self) -> None:
        """Reload data from database."""
        self.beginResetModel()
        self._load_data()
        self.endResetModel()

    def get_row_data(self, row: int) -> Optional[Dict[str, Any]]:
        """Get full row data for a given row index."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the loaded data."""
        total = len(self._rows)
        forensic_count = sum(1 for r in self._rows if r.get("forensic_interest"))
        with_run_count = sum(1 for r in self._rows if r.get("run_count"))
        with_last_run = sum(1 for r in self._rows if r.get("last_run_utc"))

        return {
            "total": total,
            "forensic_count": forensic_count,
            "with_run_count": with_run_count,
            "with_last_run": with_last_run,
        }
