"""Qt model for browser configuration records."""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_browser_config_filter_values, get_browser_configs

logger = logging.getLogger(__name__)


class BrowserConfigTableModel(QAbstractTableModel):
    """Read-only table model for browser_config evidence rows."""

    COLUMNS = [
        "browser",
        "profile",
        "config_type",
        "config_key",
        "config_value",
        "source_path",
        "partition_index",
        "notes",
    ]

    HEADERS = [
        "Browser",
        "Profile",
        "Config Type",
        "Config Key",
        "Config Value",
        "Source Path",
        "Partition",
        "Notes",
    ]

    COL_BROWSER = 0
    COL_PROFILE = 1
    COL_CONFIG_TYPE = 2
    COL_CONFIG_KEY = 3
    COL_CONFIG_VALUE = 4
    COL_SOURCE_PATH = 5
    COL_PARTITION = 6
    COL_NOTES = 7

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        parent=None,
    ):
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self._rows: List[Dict[str, Any]] = []

    def load(
        self,
        browser_filter: str = "",
        config_type_filter: str = "",
        key_search: str = "",
        profile_search: str = "",
    ) -> None:
        """Load browser_config rows with optional UI filters."""
        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_browser_configs(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    config_type=config_type_filter or None,
                    config_key_search=key_search.strip() or None,
                    profile_search=profile_search.strip() or None,
                    limit=5000,
                )

            logger.debug("Loaded %d browser config records", len(self._rows))

        except Exception as exc:
            logger.error("Failed to load browser config records: %s", exc, exc_info=True)
            self._rows = []

        self.endResetModel()

    def get_available_browsers(self) -> List[str]:
        """Return browsers with browser_config rows."""
        return self._get_filter_values("browser")

    def get_available_config_types(self) -> List[str]:
        """Return config types with browser_config rows."""
        return self._get_filter_values("config_type")

    def _get_filter_values(self, column: str) -> List[str]:
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                return get_browser_config_filter_values(conn, self.evidence_id, column)
        except Exception as exc:
            logger.error("Failed to get browser config %s values: %s", column, exc, exc_info=True)
            return []

    def get_type_counts(self) -> Dict[str, int]:
        """Return counts by config type for loaded rows."""
        counts: Dict[str, int] = {}
        for row in self._rows:
            config_type = row.get("config_type") or "unknown"
            counts[config_type] = counts.get(config_type, 0) + 1
        return counts

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """Return complete row data for detail dialogs."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return self._rows[index.row()]

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
        role: int = Qt.DisplayRole,
    ) -> Any:
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None

        row = self._rows[index.row()]
        column = index.column()
        column_name = self.COLUMNS[column]
        value = row.get(column_name)

        if role == Qt.DisplayRole:
            if column == self.COL_BROWSER:
                return str(value).capitalize() if value else ""
            if column == self.COL_CONFIG_TYPE:
                return str(value).replace("_", " ").title() if value else ""
            if column == self.COL_PARTITION:
                return "" if value is None else str(value)
            if column in {self.COL_CONFIG_KEY, self.COL_CONFIG_VALUE, self.COL_SOURCE_PATH, self.COL_NOTES}:
                return self._truncate(value)
            return value if value is not None else ""

        if role == Qt.ToolTipRole:
            if column in {self.COL_CONFIG_KEY, self.COL_CONFIG_VALUE, self.COL_SOURCE_PATH, self.COL_NOTES}:
                return value or ""

        if role == Qt.UserRole:
            return row

        return None

    @staticmethod
    def _truncate(value: Any, max_length: int = 120) -> str:
        if value is None:
            return ""
        text = str(value)
        if len(text) > max_length:
            return text[: max_length - 3] + "..."
        return text