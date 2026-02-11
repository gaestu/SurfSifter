"""
Qt model for autofill block list table.

Displays Edge-specific autofill block list data showing sites where
autofill is disabled. This has forensic value as it reveals:
- Financial/sensitive sites user accessed
- User security awareness
- Device correlation via device_model
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database.helpers.autofill_block_list import (
    get_autofill_block_list,
    get_block_type_name,
)

logger = logging.getLogger(__name__)


class AutofillBlockListModel(QAbstractTableModel):
    """
    Qt model for autofill block list table.

    Displays sites/domains where the user has disabled autofill.
    Edge-specific feature with significant forensic value.
    """

    # Column definitions - DB column names
    COLUMNS = [
        "block_value",
        "block_value_type",
        "meta_data",
        "device_model",
        "date_created_utc",
        "browser",
        "profile",
        "tags",
    ]

    HEADERS = [
        "Blocked Site/Domain",
        "Block Type",
        "Description",
        "Device",
        "Date Created",
        "Browser",
        "Profile",
        "Tags",
    ]

    # Column indexes
    COL_BLOCK_VALUE = 0
    COL_BLOCK_TYPE = 1
    COL_META_DATA = 2
    COL_DEVICE = 3
    COL_DATE_CREATED = 4
    COL_BROWSER = 5
    COL_PROFILE = 6
    COL_TAGS = 7

    ARTIFACT_TYPE = "autofill_block_list"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize autofill block list model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            case_data: Optional case data access for tagging
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
        self._block_value_filter: str = ""

    def load(self, browser_filter: str = "", block_value_filter: str = "") -> None:
        """
        Load autofill block list data from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            block_value_filter: Block value substring filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._block_value_filter = block_value_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_autofill_block_list(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    block_value=block_value_filter or None,
                    limit=5000,
                )
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} autofill block list entries")

        except Exception as e:
            logger.error(f"Failed to load autofill block list: {e}", exc_info=True)
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
        """Get list of browsers that have block list data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM autofill_block_list WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get block list browsers: {e}", exc_info=True)
            return []

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
            if col == self.COL_BLOCK_VALUE:
                return row_data.get("block_value", "")
            elif col == self.COL_BLOCK_TYPE:
                block_type = row_data.get("block_value_type")
                return get_block_type_name(block_type)
            elif col == self.COL_META_DATA:
                meta = row_data.get("meta_data") or ""
                # Truncate long descriptions
                if len(meta) > 50:
                    return meta[:47] + "..."
                return meta
            elif col == self.COL_DEVICE:
                return row_data.get("device_model") or ""
            elif col == self.COL_DATE_CREATED:
                date = row_data.get("date_created_utc")
                if date:
                    return date[:10] if len(date) > 10 else date
                return ""
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_BLOCK_VALUE:
                # Show full block value and type info
                block_type = row_data.get("block_value_type")
                return f"{row_data.get('block_value', '')}\nType: {get_block_type_name(block_type)}"
            elif col == self.COL_META_DATA:
                return row_data.get("meta_data") or ""
            elif col == self.COL_DATE_CREATED:
                return row_data.get("date_created_utc") or ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.TextAlignmentRole:
            if col == self.COL_BLOCK_TYPE:
                return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
