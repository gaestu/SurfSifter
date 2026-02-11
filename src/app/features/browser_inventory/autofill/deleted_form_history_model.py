"""
Qt model for deleted_form_history table.

Displays Firefox deleted form history entries for forensic analysis.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_deleted_form_history

logger = logging.getLogger(__name__)


class DeletedFormHistoryTableModel(QAbstractTableModel):
    """
    Qt model for deleted_form_history table.

    Displays entries from Firefox's moz_deleted_formhistory table.
    Forensically valuable for:
    - Understanding what data the user chose to delete
    - Correlating deletion times with other activities
    - Recovering GUIDs for potential data recovery
    """

    # Column definitions - DB column names
    COLUMNS = [
        "guid",
        "time_deleted_utc",
        "browser",
        "profile",
        "source_path",
        "tags",
    ]

    HEADERS = [
        "GUID",
        "Time Deleted",
        "Browser",
        "Profile",
        "Source Path",
        "Tags",
    ]

    # Column indexes
    COL_GUID = 0
    COL_TIME_DELETED = 1
    COL_BROWSER = 2
    COL_PROFILE = 3
    COL_SOURCE_PATH = 4
    COL_TAGS = 5

    ARTIFACT_TYPE = "deleted_form_history"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize deleted form history model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            case_data: CaseDataAccess for tag resolution
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
        Load deleted form history data from database with optional filters.

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
                self._rows = get_deleted_form_history(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    limit=5000,
                )
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} deleted form history entries")

        except Exception as e:
            logger.error(f"Failed to load deleted form history: {e}", exc_info=True)
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
        """Get list of browsers that have deleted form history data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM deleted_form_history WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get deleted form history browsers: {e}", exc_info=True)
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
            if col == self.COL_GUID:
                return row_data.get("guid", "")
            elif col == self.COL_TIME_DELETED:
                deleted = row_data.get("time_deleted_utc")
                if deleted:
                    # Show date and time for deletion events
                    return deleted[:19] if len(deleted) > 19 else deleted
                return ""
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_SOURCE_PATH:
                path = row_data.get("source_path") or ""
                # Truncate long paths
                if len(path) > 50:
                    return "..." + path[-47:]
                return path
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_TIME_DELETED:
                return row_data.get("time_deleted_utc", "")
            elif col == self.COL_SOURCE_PATH:
                return row_data.get("source_path", "")
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ForegroundRole:
            # Use a distinct color to highlight deleted entries
            from PySide6.QtGui import QColor
            return QColor(255, 100, 100)  # Reddish to indicate deletion

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
