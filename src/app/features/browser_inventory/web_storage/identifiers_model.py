"""
Qt model for storage identifiers table.

Displays user IDs, device IDs, tracking IDs, and other identifiers
extracted from browser web storage by the Firefox Storage Extractor.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor, QFont

from core.database import get_storage_identifiers, get_storage_identifier_stats

logger = logging.getLogger(__name__)


class StorageIdentifiersTableModel(QAbstractTableModel):
    """
    Qt model for storage_identifiers table.

    Displays tracking IDs, user IDs, device IDs, and other identifiers
    extracted from browser LocalStorage, SessionStorage, and IndexedDB.
    """

    # Column definitions - DB column names
    COLUMNS = [
        "identifier_type",
        "identifier_name",
        "identifier_value",
        "origin",
        "storage_key",
        "browser",
        "profile",
        "storage_type",
        "first_seen_utc",
        "last_seen_utc",
        "partition_index",
    ]

    HEADERS = [
        "Type",
        "Name",
        "Value",
        "Origin",
        "Storage Key",
        "Browser",
        "Profile",
        "Storage",
        "First Seen",
        "Last Seen",
        "Partition",
    ]

    # Column indexes
    COL_TYPE = 0
    COL_NAME = 1
    COL_VALUE = 2
    COL_ORIGIN = 3
    COL_STORAGE_KEY = 4
    COL_BROWSER = 5
    COL_PROFILE = 6
    COL_STORAGE_TYPE = 7
    COL_FIRST_SEEN = 8
    COL_LAST_SEEN = 9
    COL_PARTITION = 10

    ARTIFACT_TYPE = "storage_identifier"

    # Identifier type colors for visual distinction
    TYPE_COLORS = {
        "user_id": QColor(200, 220, 255),      # Light blue
        "device_id": QColor(255, 220, 200),    # Light orange
        "tracking_id": QColor(255, 200, 200),  # Light red (high interest)
        "visitor_id": QColor(255, 230, 200),   # Light peach
        "session_id": QColor(220, 255, 220),   # Light green
        "email": QColor(230, 200, 255),        # Light purple
    }

    # Max value length for display
    MAX_VALUE_DISPLAY = 60

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize storage identifiers model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            case_data: CaseDataAccess for tags
            parent: Parent widget
        """
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.case_data = case_data

        # Data storage
        self._rows: List[Dict[str, Any]] = []
        self._stats: Dict[str, Any] = {}

        # Filters
        self._identifier_type_filter: str = ""
        self._origin_filter: str = ""
        self._value_filter: str = ""

    def load(
        self,
        identifier_type_filter: str = "",
        origin_filter: str = "",
        value_filter: str = "",
    ) -> None:
        """
        Load storage identifiers from database with optional filters.

        Args:
            identifier_type_filter: Identifier type filter (empty = all)
            origin_filter: Origin substring filter
            value_filter: Value substring filter
        """
        self._identifier_type_filter = identifier_type_filter
        self._origin_filter = origin_filter
        self._value_filter = value_filter

        self.beginResetModel()
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )

            self._rows = get_storage_identifiers(
                conn,
                self.evidence_id,
                identifier_type=identifier_type_filter or None,
                origin=origin_filter or None,
                limit=10000,
            )

            # Apply value filter (not supported by get_storage_identifiers)
            if value_filter:
                value_lower = value_filter.lower()
                self._rows = [
                    r for r in self._rows
                    if value_lower in (r.get("identifier_value") or "").lower()
                ]

            self._stats = get_storage_identifier_stats(conn, self.evidence_id)

            logger.debug(
                "Loaded %d storage identifiers for evidence %d",
                len(self._rows),
                self.evidence_id,
            )
        except Exception as e:
            logger.error("Failed to load storage identifiers: %s", e)
            self._rows = []
            self._stats = {}
        finally:
            self.endResetModel()

    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        return self._stats

    def get_row_data(self, row: int) -> Optional[Dict[str, Any]]:
        """Get full row data for a given row index."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

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

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        """Return cell data."""
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if row < 0 or row >= len(self._rows):
            return None

        row_data = self._rows[row]

        if role == Qt.DisplayRole:
            return self._get_display_value(col, row_data)
        elif role == Qt.BackgroundRole:
            return self._get_background_color(col, row_data)
        elif role == Qt.FontRole:
            return self._get_font(col, row_data)
        elif role == Qt.ToolTipRole:
            return self._get_tooltip(col, row_data)
        elif role == Qt.UserRole:
            # Return raw value for sorting
            return self._get_sort_value(col, row_data)

        return None

    def _get_display_value(self, col: int, row_data: Dict[str, Any]) -> str:
        """Get display value for a cell."""
        if col == self.COL_TYPE:
            id_type = row_data.get("identifier_type", "")
            return id_type.replace("_", " ").title()

        elif col == self.COL_NAME:
            return row_data.get("identifier_name") or ""

        elif col == self.COL_VALUE:
            value = row_data.get("identifier_value") or ""
            if len(value) > self.MAX_VALUE_DISPLAY:
                return value[:self.MAX_VALUE_DISPLAY] + "..."
            return value

        elif col == self.COL_ORIGIN:
            origin = row_data.get("origin") or ""
            # Strip protocol for display
            if origin.startswith("https://"):
                origin = origin[8:]
            elif origin.startswith("http://"):
                origin = origin[7:]
            return origin

        elif col == self.COL_STORAGE_KEY:
            key = row_data.get("storage_key") or ""
            if len(key) > 40:
                return key[:40] + "..."
            return key

        elif col == self.COL_BROWSER:
            browser = row_data.get("browser") or ""
            return browser.capitalize()

        elif col == self.COL_PROFILE:
            return row_data.get("profile") or ""

        elif col == self.COL_STORAGE_TYPE:
            storage_type = row_data.get("storage_type") or ""
            return storage_type.replace("_", " ").title()

        elif col == self.COL_FIRST_SEEN:
            ts = row_data.get("first_seen_utc")
            if ts:
                return ts[:16] if len(ts) > 16 else ts
            return ""

        elif col == self.COL_LAST_SEEN:
            ts = row_data.get("last_seen_utc")
            if ts:
                return ts[:16] if len(ts) > 16 else ts
            return ""

        elif col == self.COL_PARTITION:
            partition = row_data.get("partition_index")
            return str(partition) if partition is not None else ""

        return ""

    def _get_background_color(self, col: int, row_data: Dict[str, Any]) -> Optional[QColor]:
        """Get background color for identifier type highlighting."""
        if col == self.COL_TYPE:
            id_type = row_data.get("identifier_type", "")
            return self.TYPE_COLORS.get(id_type)
        return None

    def _get_font(self, col: int, row_data: Dict[str, Any]) -> Optional[QFont]:
        """Get font for highlighting tracking IDs."""
        if col == self.COL_TYPE:
            id_type = row_data.get("identifier_type", "")
            if id_type == "tracking_id":
                font = QFont()
                font.setBold(True)
                return font
        return None

    def _get_tooltip(self, col: int, row_data: Dict[str, Any]) -> Optional[str]:
        """Get tooltip for a cell."""
        if col == self.COL_VALUE:
            # Show full value in tooltip
            return row_data.get("identifier_value") or ""
        elif col == self.COL_STORAGE_KEY:
            return row_data.get("storage_key") or ""
        elif col == self.COL_ORIGIN:
            return row_data.get("origin") or ""
        return None

    def _get_sort_value(self, col: int, row_data: Dict[str, Any]) -> Any:
        """Get sort value for a cell."""
        col_name = self.COLUMNS[col] if col < len(self.COLUMNS) else None
        if col_name:
            return row_data.get(col_name, "")
        return ""

    def get_distinct_types(self) -> List[str]:
        """Get distinct identifier types from current data."""
        types = set()
        for row in self._rows:
            id_type = row.get("identifier_type")
            if id_type:
                types.add(id_type)
        return sorted(types)

    def get_distinct_browsers(self) -> List[str]:
        """Get distinct browsers from current data."""
        browsers = set()
        for row in self._rows:
            browser = row.get("browser")
            if browser:
                browsers.add(browser)
        return sorted(browsers)
