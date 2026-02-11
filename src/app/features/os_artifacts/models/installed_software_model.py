"""Qt model for Installed Software table.

Displays installed software extracted from Windows registry Uninstall keys
with full metadata (Publisher, Version, Install Date, etc.).

Features:
- Full software metadata from registry
- Forensic interest highlighting (Deep Freeze, CCleaner, etc.)
- Filtering by name, publisher, forensic category
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


class InstalledSoftwareModel(QAbstractTableModel):
    """
    Qt model for installed software from os_indicators table.

    Queries indicators with type='system:installed_software' and parses
    extra_json for full metadata (Publisher, Version, InstallDate, etc.).
    """

    # Column definitions
    COLUMNS = [
        "name",
        "publisher",
        "version",
        "install_date",
        "install_location",
        "size_kb",
        "forensic_interest",
    ]

    HEADERS = [
        "Software Name",
        "Publisher",
        "Version",
        "Install Date",
        "Install Location",
        "Size (KB)",
        "Forensic",
    ]

    # Column indexes
    COL_NAME = 0
    COL_PUBLISHER = 1
    COL_VERSION = 2
    COL_INSTALL_DATE = 3
    COL_INSTALL_LOCATION = 4
    COL_SIZE = 5
    COL_FORENSIC = 6

    # Forensic interest colors
    COLOR_FORENSIC_RESTORE = QColor(255, 200, 200)  # Light red for system restore tools
    COLOR_FORENSIC_ANTI = QColor(255, 220, 180)  # Light orange for anti-forensic tools
    COLOR_FORENSIC_OTHER = QColor(255, 255, 180)  # Light yellow for other forensic interest

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        parent=None
    ):
        """
        Initialize Installed Software model.

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

        # Data storage
        self._rows: List[Dict[str, Any]] = []

        # Filters
        self._search_text: str = ""
        self._forensic_only: bool = False

        # Initial load
        self._load_data()

    def _load_data(self) -> None:
        """Load data from database with current filters."""
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )
            if not conn:
                logger.warning("No evidence connection for Installed Software")
                self._rows = []
                return

            # Query installed software indicators
            sql = """
                SELECT id, value, path, hive, extra_json
                FROM os_indicators
                WHERE type = 'system:installed_software'
                ORDER BY value COLLATE NOCASE
            """

            cursor = conn.execute(sql)
            raw_rows = cursor.fetchall()

            # Parse extra_json and build row data
            parsed_rows = []
            for row in raw_rows:
                row_id, value, path, hive, extra_json_str = row

                # Parse extra_json
                extra = {}
                if extra_json_str:
                    try:
                        extra = json.loads(extra_json_str)
                    except json.JSONDecodeError:
                        pass

                # Build row dict
                row_data = {
                    "id": row_id,
                    "name": value or extra.get("name", "Unknown"),
                    "publisher": extra.get("publisher", ""),
                    "version": extra.get("version", ""),
                    "install_date": extra.get("install_date_formatted") or extra.get("install_date", ""),
                    "install_location": extra.get("install_location", ""),
                    "size_kb": extra.get("size_kb"),
                    "forensic_interest": extra.get("forensic_interest", False),
                    "forensic_category": extra.get("forensic_category", ""),
                    "registry_key": extra.get("registry_key", ""),
                    "uninstall_command": extra.get("uninstall_command", ""),
                    "install_source": extra.get("install_source", ""),
                    "url": extra.get("url", ""),
                    "comments": extra.get("comments", ""),
                    "architecture": extra.get("architecture", ""),
                    "path": path,
                    "hive": hive,
                    "extra": extra,
                }
                parsed_rows.append(row_data)

            # Apply filters
            filtered_rows = self._apply_filters(parsed_rows)
            self._rows = filtered_rows

        except Exception as e:
            logger.exception("Failed to load installed software: %s", e)
            self._rows = []

    def _apply_filters(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply search and forensic filters."""
        result = rows

        # Search filter (name or publisher)
        if self._search_text:
            search_lower = self._search_text.lower()
            result = [
                r for r in result
                if search_lower in r.get("name", "").lower()
                or search_lower in r.get("publisher", "").lower()
            ]

        # Forensic-only filter
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
            if col == self.COL_NAME:
                return row.get("name", "")
            elif col == self.COL_PUBLISHER:
                return row.get("publisher", "")
            elif col == self.COL_VERSION:
                return row.get("version", "")
            elif col == self.COL_INSTALL_DATE:
                return row.get("install_date", "")
            elif col == self.COL_INSTALL_LOCATION:
                return row.get("install_location", "")
            elif col == self.COL_SIZE:
                size = row.get("size_kb")
                if size:
                    # Format with thousands separator
                    return f"{int(size):,}"
                return ""
            elif col == self.COL_FORENSIC:
                if row.get("forensic_interest"):
                    category = row.get("forensic_category", "")
                    if category == "system_restore":
                        return "⚠️ System Restore"
                    elif category == "anti_forensic":
                        return "⚠️ Anti-Forensic"
                    else:
                        return "⚠️ Interest"
                return ""

        elif role == Qt.BackgroundRole:
            # Highlight forensically interesting software
            if row.get("forensic_interest"):
                category = row.get("forensic_category", "")
                if category == "system_restore":
                    return QBrush(self.COLOR_FORENSIC_RESTORE)
                elif category == "anti_forensic":
                    return QBrush(self.COLOR_FORENSIC_ANTI)
                else:
                    return QBrush(self.COLOR_FORENSIC_OTHER)

        elif role == Qt.ToolTipRole:
            # Show full details in tooltip
            tips = []
            tips.append(f"Name: {row.get('name', 'Unknown')}")
            if row.get("publisher"):
                tips.append(f"Publisher: {row['publisher']}")
            if row.get("version"):
                tips.append(f"Version: {row['version']}")
            if row.get("install_date"):
                tips.append(f"Install Date: {row['install_date']}")
            if row.get("install_location"):
                tips.append(f"Location: {row['install_location']}")
            if row.get("install_source"):
                tips.append(f"Source: {row['install_source']}")
            if row.get("architecture"):
                tips.append(f"Architecture: {row['architecture']}")
            if row.get("registry_key"):
                tips.append(f"Registry Key: {row['registry_key']}")
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
        with_date = sum(1 for r in self._rows if r.get("install_date"))
        with_publisher = sum(1 for r in self._rows if r.get("publisher"))

        return {
            "total": total,
            "forensic_count": forensic_count,
            "with_install_date": with_date,
            "with_publisher": with_publisher,
        }

    def get_publishers(self) -> List[str]:
        """Get distinct publishers for filter dropdown."""
        publishers = set()
        for row in self._rows:
            pub = row.get("publisher")
            if pub:
                publishers.add(pub)
        return sorted(publishers)
