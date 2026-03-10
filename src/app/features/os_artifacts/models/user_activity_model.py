"""Qt model for User Activity table.

Displays user activity indicators from the os_indicators table including
recent items, Finder preferences, Spotlight searches, and quarantine events.

Cross-platform: Shows activity from both macOS (macos_plist provenance)
and future Windows user activity sources.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QBrush, QColor

from core.database import DatabaseManager

logger = logging.getLogger(__name__)


class UserActivityModel(QAbstractTableModel):
    """
    Qt model for user activity from os_indicators table.

    Queries indicators with type LIKE 'user_activity:%'.
    """

    COLUMNS = [
        "activity_type",
        "name",
        "value",
        "timestamp",
        "source_path",
        "os_source",
        "tags",
    ]

    HEADERS = [
        "Activity Type",
        "Name",
        "Value",
        "Timestamp",
        "Source Path",
        "OS",
        "Tags",
    ]

    COL_ACTIVITY_TYPE = 0
    COL_NAME = 1
    COL_VALUE = 2
    COL_TIMESTAMP = 3
    COL_SOURCE_PATH = 4
    COL_OS = 5
    COL_TAGS = 6

    ARTIFACT_TYPE = "user_activity"

    # Highlight colours per activity sub-type
    COLOR_QUARANTINE = QColor(255, 200, 200)       # Light red — forensically important
    COLOR_RECENT = QColor(200, 220, 255)            # Light blue
    COLOR_SPOTLIGHT = QColor(255, 255, 180)         # Light yellow
    COLOR_FINDER = QColor(220, 255, 220)            # Light green

    SUBTYPE_COLORS = {
        "quarantine_events": COLOR_QUARANTINE,
        "recent_items": COLOR_RECENT,
        "spotlight_searches": COLOR_SPOTLIGHT,
        "finder_prefs": COLOR_FINDER,
    }

    def __init__(
        self,
        db_manager: DatabaseManager,
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
        self._search_text: str = ""

        self._load_data()

    def _load_data(self) -> None:
        """Load data from database with current filters."""
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )
            if not conn:
                logger.warning("No evidence connection for User Activity")
                self._rows = []
                return

            sql = """
                SELECT id, type, name, value, path, hive, confidence,
                       detected_at_utc, provenance, extra_json
                FROM os_indicators
                WHERE type LIKE 'user_activity:%'
                ORDER BY type, name COLLATE NOCASE
            """

            cursor = conn.execute(sql)
            raw_rows = cursor.fetchall()

            parsed_rows = []
            for row in raw_rows:
                (row_id, indicator_type, name, value, path,
                 hive, confidence, detected_at_utc, provenance, extra_json_str) = row

                extra: Dict[str, Any] = {}
                if extra_json_str:
                    try:
                        extra = json.loads(extra_json_str)
                    except json.JSONDecodeError:
                        pass

                # Derive readable sub-type from the type field
                subtype = indicator_type.split(":", 1)[1] if ":" in indicator_type else indicator_type

                # Determine OS from provenance
                _OS_MAP = {"macos_plist": "macOS", "registry": "Windows"}
                os_source = _OS_MAP.get(provenance, provenance or "Unknown")

                row_data = {
                    "id": row_id,
                    "type": indicator_type,
                    "subtype": subtype,
                    "activity_type": subtype.replace("_", " ").title(),
                    "name": name or "",
                    "value": value or "",
                    "detected_at_utc": detected_at_utc or "",
                    "path": path or "",
                    "hive": hive or "",
                    "confidence": confidence or "",
                    "provenance": provenance or "",
                    "os_source": os_source,
                    "extra": extra,
                }
                parsed_rows.append(row_data)

            self._rows = self._apply_filters(parsed_rows)
            self._refresh_tags()

        except Exception as e:
            logger.exception("Failed to load user activity data: %s", e)
            self._rows = []
            self._tag_map = {}

    def _apply_filters(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply search filter."""
        if not self._search_text:
            return rows

        search_lower = self._search_text.lower()
        return [
            r for r in rows
            if search_lower in r.get("name", "").lower()
            or search_lower in r.get("value", "").lower()
            or search_lower in r.get("activity_type", "").lower()
        ]

    def _refresh_tags(self) -> None:
        if not self.case_data:
            self._tag_map = {}
            return
        ids = [row.get("id") for row in self._rows if row.get("id") is not None]
        self._tag_map = self.case_data.get_tag_strings_for_artifacts(
            self.evidence_id,
            self.ARTIFACT_TYPE,
            ids,
        )

    # ------------------------------------------------------------------
    # Qt model interface
    # ------------------------------------------------------------------

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
            if col == self.COL_ACTIVITY_TYPE:
                return row.get("activity_type", "")
            elif col == self.COL_NAME:
                return row.get("name", "")
            elif col == self.COL_VALUE:
                return row.get("value", "")
            elif col == self.COL_TIMESTAMP:
                ts = row.get("detected_at_utc", "")
                if ts:
                    return ts.replace("T", " ").split("+")[0]
                return ""
            elif col == self.COL_SOURCE_PATH:
                return row.get("path", "")
            elif col == self.COL_OS:
                return row.get("os_source", "")
            elif col == self.COL_TAGS:
                return self._tag_map.get(row.get("id"), "") or ""

        elif role == Qt.BackgroundRole:
            subtype = row.get("subtype", "")
            color = self.SUBTYPE_COLORS.get(subtype)
            if color:
                return QBrush(color)

        elif role == Qt.ToolTipRole:
            tips = [
                f"Type: {row.get('type', '')}",
                f"Name: {row.get('name', '')}",
                f"Value: {row.get('value', '')}",
            ]
            if row.get("detected_at_utc"):
                tips.append(f"Timestamp: {row['detected_at_utc']}")
            if row.get("path"):
                tips.append(f"Source: {row['path']}")
            if row.get("confidence"):
                tips.append(f"Confidence: {row['confidence']}")
            tag_str = self._tag_map.get(row.get("id"), "")
            if tag_str:
                tips.append(f"Tags: {tag_str}")
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_filters(self, search_text: str = "") -> None:
        """Apply filters and reload."""
        self._search_text = search_text
        self.reload()

    def reload(self) -> None:
        self.beginResetModel()
        self._load_data()
        self.endResetModel()

    def get_row_data(self, row: int) -> Optional[Dict[str, Any]]:
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_stats(self) -> Dict[str, Any]:
        total = len(self._rows)
        quarantine = sum(1 for r in self._rows if r.get("subtype") == "quarantine_events")
        recent = sum(1 for r in self._rows if r.get("subtype") == "recent_items")
        with_ts = sum(1 for r in self._rows if r.get("detected_at_utc"))
        return {
            "total": total,
            "quarantine_count": quarantine,
            "recent_count": recent,
            "with_timestamp": with_ts,
        }
