"""
Qt model for browser extensions table.

Displays parsed browser extensions with filtering by browser and risk level.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor

from core.database import get_extensions

logger = logging.getLogger(__name__)


class ExtensionsTableModel(QAbstractTableModel):
    """
    Qt model for browser extensions table.

    Displays browser extensions extracted from manifest.json and Preferences.
    Highlights high-risk extensions with color coding.
    """

    # Column definitions - DB column names
    COLUMNS = [
        "name",
        "extension_id",
        "version",
        "browser",
        "profile",
        "enabled",
        "risk_score",
        "risk_factors",
        "known_category",
        "install_location_text",
        "from_webstore",
        "permissions",
    ]

    HEADERS = [
        "Name",
        "Extension ID",
        "Version",
        "Browser",
        "Profile",
        "Enabled",
        "Risk",
        "Risk Factors",
        "Category",
        "Install Source",
        "Web Store",
        "Permissions",
    ]

    # Column indexes
    COL_NAME = 0
    COL_EXTENSION_ID = 1
    COL_VERSION = 2
    COL_BROWSER = 3
    COL_PROFILE = 4
    COL_ENABLED = 5
    COL_RISK_SCORE = 6
    COL_RISK_FACTORS = 7
    COL_CATEGORY = 8
    COL_INSTALL_SOURCE = 9
    COL_WEB_STORE = 10
    COL_PERMISSIONS = 11

    ARTIFACT_TYPE = "browser_extension"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize extensions model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            case_data: CaseDataAccess for tagging
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
        self._min_risk: int = 0
        self._category_filter: str = ""

    def load(
        self,
        browser_filter: str = "",
        min_risk: int = 0,
        category_filter: str = ""
    ) -> None:
        """
        Load extensions from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            min_risk: Minimum risk score filter (0 = all)
            category_filter: Category filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._min_risk = min_risk
        self._category_filter = category_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_extensions(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    min_risk_score=min_risk,
                    category=category_filter or None,
                    limit=5000,
                )
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} extensions")

        except Exception as e:
            logger.error(f"Failed to load extensions: {e}", exc_info=True)
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
        """Get list of browsers that have extension data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM browser_extensions WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get browser list: {e}")
            return []

    def get_available_categories(self) -> List[str]:
        """Get list of known categories in the data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    """
                    SELECT DISTINCT known_category FROM browser_extensions
                    WHERE evidence_id = ? AND known_category IS NOT NULL
                    """,
                    (self.evidence_id,)
                )
                return [row["known_category"] for row in cursor]
        except Exception as e:
            logger.error(f"Failed to get category list: {e}")
            return []

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
        self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole
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

        record = self._rows[row]

        if role == Qt.DisplayRole:
            col_name = self.COLUMNS[col]
            value = record.get(col_name)

            # Format specific columns
            if col == self.COL_ENABLED:
                return "Yes" if value else "No"
            elif col == self.COL_WEB_STORE:
                if value is None:
                    return "Unknown"
                return "Yes" if value else "No"
            elif col == self.COL_PERMISSIONS:
                # Truncate long permission lists
                if value and len(value) > 100:
                    return value[:100] + "..."
                return value or ""
            elif col == self.COL_RISK_FACTORS:
                # Format risk factors for display
                if value and len(value) > 80:
                    return value[:80] + "..."
                return value or ""

            return value if value is not None else ""

        elif role == Qt.BackgroundRole:
            # Color code by risk score
            risk_score = record.get("risk_score", 0) or 0
            if risk_score >= 80:
                return QColor(255, 200, 200)  # Light red - critical
            elif risk_score >= 60:
                return QColor(255, 230, 200)  # Light orange - high
            elif risk_score >= 40:
                return QColor(255, 255, 200)  # Light yellow - medium

        elif role == Qt.ToolTipRole:
            if col == self.COL_PERMISSIONS:
                return record.get("permissions", "")
            elif col == self.COL_RISK_FACTORS:
                return record.get("risk_factors", "")
            elif col == self.COL_NAME:
                desc = record.get("description", "")
                return desc if desc else None

        elif role == Qt.UserRole:
            # Return full record for detail dialogs
            return record

        return None

    def get_record(self, row: int) -> Optional[Dict[str, Any]]:
        """Get full record for a row."""
        if 0 <= row < len(self._rows):
            return dict(self._rows[row])
        return None

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """Get full row data for given index (compatibility with other models)."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return dict(self._rows[index.row()])

    def get_selected_ids(self, indexes: List[QModelIndex]) -> List[int]:
        """Get IDs for selected rows."""
        rows = set(idx.row() for idx in indexes if idx.isValid())
        return [
            self._rows[row].get("id")
            for row in rows
            if 0 <= row < len(self._rows) and self._rows[row].get("id") is not None
        ]
