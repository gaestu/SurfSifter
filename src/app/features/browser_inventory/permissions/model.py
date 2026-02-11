"""
Qt model for site permissions table.

Displays parsed site permissions with filtering by browser and permission type.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_permissions

logger = logging.getLogger(__name__)


class PermissionsTableModel(QAbstractTableModel):
    """
    Qt model for site permissions table.

    Displays site permission settings extracted from browser preferences.
    """

    # Column definitions - DB column names
    COLUMNS = [
        "origin",
        "permission_type",
        "permission_value",  # DB uses permission_value, not decision
        "browser",
        "profile",
        "granted_at_utc",  # DB uses granted_at_utc
        "expires_at_utc",  # DB uses expires_at_utc
        "tags",
    ]

    HEADERS = [
        "Origin",
        "Permission",
        "Decision",
        "Browser",
        "Profile",
        "Granted",
        "Expires",
        "Tags",
    ]

    # Column indexes
    COL_ORIGIN = 0
    COL_PERMISSION = 1
    COL_DECISION = 2
    COL_BROWSER = 3
    COL_PROFILE = 4
    COL_GRANTED = 5
    COL_EXPIRES = 6
    COL_TAGS = 7

    ARTIFACT_TYPE = "site_permission"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize permissions model.

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
        self._permission_filter: str = ""

    def load(self, browser_filter: str = "", permission_filter: str = "") -> None:
        """
        Load permissions from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            permission_filter: Permission type filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._permission_filter = permission_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_permissions(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    permission_type=permission_filter or None,
                    limit=5000,
                )
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} permissions")

        except Exception as e:
            logger.error(f"Failed to load permissions: {e}", exc_info=True)
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
        """Get list of browsers that have permission data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM site_permissions WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get permission browsers: {e}", exc_info=True)
            return []

    def get_available_permission_types(self) -> List[str]:
        """Get list of permission types in the data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT permission_type FROM site_permissions WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["permission_type"] for row in cursor.fetchall() if row["permission_type"]]
        except Exception as e:
            logger.error(f"Failed to get permission types: {e}", exc_info=True)
            return []

    def get_decision_counts(self) -> Dict[str, int]:
        """Get counts by decision type."""
        counts: Dict[str, int] = {}
        for row in self._rows:
            decision = row.get("permission_value", "unknown")  # Use permission_value from DB
            counts[decision] = counts.get(decision, 0) + 1
        return counts

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
            if col == self.COL_ORIGIN:
                origin = row_data.get("origin", "")
                if len(origin) > 50:
                    return origin[:47] + "..."
                return origin
            elif col == self.COL_PERMISSION:
                return row_data.get("permission_type", "").replace("_", " ").title()
            elif col == self.COL_DECISION:
                decision = row_data.get("permission_value", "")  # Use permission_value from DB
                return decision.replace("_", " ").title()
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_GRANTED:
                granted = row_data.get("granted_at_utc")  # Use granted_at_utc from DB
                if granted:
                    return granted[:10] if len(granted) > 10 else granted
                return ""
            elif col == self.COL_EXPIRES:
                expires = row_data.get("expires_at_utc")  # Use expires_at_utc from DB
                if expires:
                    return expires[:10] if len(expires) > 10 else expires
                return "Never"
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_ORIGIN:
                return row_data.get("origin", "")
            elif col == self.COL_GRANTED:
                return row_data.get("granted_at_utc", "")  # Use granted_at_utc from DB
            elif col == self.COL_EXPIRES:
                return row_data.get("expires_at_utc") or "Never expires"  # Use expires_at_utc from DB
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ForegroundRole:
            if col == self.COL_DECISION:
                decision = row_data.get("permission_value", "")  # Use permission_value from DB
                from PySide6.QtGui import QColor
                if decision == "allow":
                    return QColor(0, 128, 0)  # Green
                elif decision == "deny":
                    return QColor(192, 0, 0)  # Red
                elif decision == "ask":
                    return QColor(0, 0, 192)  # Blue

        elif role == Qt.TextAlignmentRole:
            if col == self.COL_DECISION:
                return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
