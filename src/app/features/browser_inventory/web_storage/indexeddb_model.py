"""
Qt model for IndexedDB entries table.

Displays IndexedDB database entries in a flat list view with filtering
by origin, database name, and object store.

Added source and partition_index columns.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor, QFont

from core.database import get_indexeddb_databases, get_indexeddb_entries

logger = logging.getLogger(__name__)


class IndexedDBTableModel(QAbstractTableModel):
    """
    Qt model for IndexedDB entries displayed as flat list.

    Shows all IndexedDB entries with database context for forensic analysis.
    """

    # Column definitions
    COLUMNS = [
        "origin",
        "database_name",
        "object_store",
        "key",
        "value",
        "source",
        "browser",
        "profile",
        "value_size",
        "partition_index",
    ]

    HEADERS = [
        "Origin",
        "Database",
        "Object Store",
        "Key",
        "Value",
        "Source",
        "Browser",
        "Profile",
        "Size",
        "Partition",
    ]

    # Column indexes
    COL_ORIGIN = 0
    COL_DATABASE = 1
    COL_OBJECT_STORE = 2
    COL_KEY = 3
    COL_VALUE = 4
    COL_SOURCE = 5
    COL_BROWSER = 6
    COL_PROFILE = 7
    COL_VALUE_SIZE = 8
    COL_PARTITION = 9

    ARTIFACT_TYPE = "indexeddb_entry"

    # Max value length for display (full value in detail dialog)
    MAX_VALUE_DISPLAY = 100

    # Keys that often contain forensically interesting data
    INTERESTING_STORES = {
        "user", "users", "account", "accounts", "auth", "session",
        "token", "tokens", "credential", "credentials", "login",
        "message", "messages", "email", "emails", "draft", "drafts",
        "cache", "offline", "sync", "data",
    }

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize IndexedDB model.

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
        self._databases: Dict[int, Dict[str, Any]] = {}  # Cache database info by ID

        # Filters
        self._origin_filter: str = ""
        self._database_filter: str = ""
        self._object_store_filter: str = ""

        # Stats
        self._database_count: int = 0
        self._entry_count: int = 0

    def load(
        self,
        origin_filter: str = "",
        database_filter: str = "",
        object_store_filter: str = "",
    ) -> None:
        """
        Load IndexedDB entries from database with optional filters.

        Args:
            origin_filter: Origin substring filter
            database_filter: Database name filter
            object_store_filter: Object store name filter
        """
        self._origin_filter = origin_filter
        self._database_filter = database_filter
        self._object_store_filter = object_store_filter

        self.beginResetModel()
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )

            # First, load all databases to get their metadata
            databases = get_indexeddb_databases(
                conn,
                self.evidence_id,
                origin=origin_filter or None,
                limit=10000,
            )

            # Build database lookup and apply database name filter
            self._databases = {}
            filtered_db_ids = []
            for db in databases:
                db_id = db.get("id")
                if db_id is not None:
                    # Apply database name filter
                    db_name = db.get("database_name", "")
                    if database_filter and database_filter.lower() not in db_name.lower():
                        continue
                    self._databases[db_id] = db
                    filtered_db_ids.append(db_id)

            self._database_count = len(filtered_db_ids)

            # Load entries for each database
            combined_rows = []
            for db_id in filtered_db_ids:
                db_info = self._databases[db_id]
                entries = get_indexeddb_entries(
                    conn,
                    self.evidence_id,
                    database_id=db_id,
                    object_store=object_store_filter or None,
                    limit=5000,
                )

                # Enrich entries with database info
                for entry in entries:
                    entry["origin"] = db_info.get("origin", "")
                    entry["database_name"] = db_info.get("database_name", "")
                    entry["browser"] = db_info.get("browser", "")
                    entry["profile"] = db_info.get("profile", "")
                    combined_rows.append(entry)

            # Sort by origin, database, object store, key
            combined_rows.sort(key=lambda r: (
                r.get("origin", ""),
                r.get("database_name", ""),
                r.get("object_store", ""),
                r.get("key", ""),
            ))

            self._rows = combined_rows
            self._entry_count = len(combined_rows)

            logger.debug(
                "Loaded %d IndexedDB entries from %d databases for evidence %d",
                self._entry_count,
                self._database_count,
                self.evidence_id,
            )
        except Exception as e:
            logger.error("Failed to load IndexedDB entries: %s", e)
            self._rows = []
            self._databases = {}
            self._database_count = 0
            self._entry_count = 0
        finally:
            self.endResetModel()

    def get_stats(self) -> Dict[str, int]:
        """Get current IndexedDB statistics."""
        return {
            "databases": self._database_count,
            "entries": self._entry_count,
        }

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
        col_name = self.COLUMNS[col] if col < len(self.COLUMNS) else None

        if role == Qt.DisplayRole:
            return self._get_display_value(row_data, col_name)
        elif role == Qt.BackgroundRole:
            return self._get_background_color(row_data, col_name)
        elif role == Qt.FontRole:
            return self._get_font(row_data, col_name)
        elif role == Qt.ToolTipRole:
            return self._get_tooltip(row_data, col_name)
        elif role == Qt.TextAlignmentRole:
            if col == self.COL_VALUE_SIZE:
                return Qt.AlignRight | Qt.AlignVCenter
        elif role == Qt.UserRole:
            return row_data

        return None

    def _get_display_value(self, row_data: Dict[str, Any], col_name: str) -> str:
        """Get display string for a cell."""
        if col_name == "value":
            value = row_data.get("value") or ""
            if len(value) > self.MAX_VALUE_DISPLAY:
                return value[:self.MAX_VALUE_DISPLAY] + "..."
            return value
        elif col_name == "value_size":
            size = row_data.get("value_size")
            if size is None:
                value = row_data.get("value") or ""
                size = len(value)
            return str(size)
        elif col_name == "browser":
            browser = row_data.get("browser", "")
            return browser.capitalize() if browser else ""
        elif col_name == "profile":
            return row_data.get("profile") or ""
        elif col_name == "source":
            # Show source table (object_data, index_data, legacy_table)
            source = row_data.get("source") or ""
            return source.replace("_", " ").title() if source else ""
        elif col_name == "partition_index":
            partition = row_data.get("partition_index")
            return str(partition) if partition is not None else ""
        else:
            return str(row_data.get(col_name) or "")

    def _get_background_color(self, row_data: Dict[str, Any], col_name: str) -> Optional[QColor]:
        """Get background color for highlighting interesting object stores."""
        object_store = (row_data.get("object_store") or "").lower()

        # Highlight object stores that might contain forensically interesting data
        for interesting in self.INTERESTING_STORES:
            if interesting in object_store:
                return QColor(255, 255, 200)  # Light yellow

        return None

    def _get_font(self, row_data: Dict[str, Any], col_name: str) -> Optional[QFont]:
        """Get font for cell."""
        if col_name == "object_store":
            object_store = (row_data.get("object_store") or "").lower()
            for interesting in self.INTERESTING_STORES:
                if interesting in object_store:
                    font = QFont()
                    font.setBold(True)
                    return font
        return None

    def _get_tooltip(self, row_data: Dict[str, Any], col_name: str) -> Optional[str]:
        """Get tooltip for cell."""
        if col_name == "value":
            value = row_data.get("value") or ""
            if len(value) > self.MAX_VALUE_DISPLAY:
                # Show first 500 chars in tooltip
                return value[:500] + ("..." if len(value) > 500 else "")
        elif col_name == "object_store":
            object_store = (row_data.get("object_store") or "").lower()
            for interesting in self.INTERESTING_STORES:
                if interesting in object_store:
                    return "This object store may contain forensically interesting data"
        elif col_name == "database_name":
            # Show database version if available
            db_id = row_data.get("database_id")
            if db_id and db_id in self._databases:
                db_info = self._databases[db_id]
                version = db_info.get("database_version")
                stores = db_info.get("object_stores")
                parts = []
                if version:
                    parts.append(f"Version: {version}")
                if stores:
                    parts.append(f"Object stores: {stores}")
                if parts:
                    return "\n".join(parts)
        return None

    def get_row_data(self, row: int) -> Optional[Dict[str, Any]]:
        """Get row data dictionary by row index."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_origins(self) -> List[str]:
        """Get unique origins in current dataset."""
        origins = set()
        for db in self._databases.values():
            origin = db.get("origin")
            if origin:
                origins.add(origin)
        return sorted(origins)

    def get_database_names(self) -> List[str]:
        """Get unique database names in current dataset."""
        names = set()
        for db in self._databases.values():
            name = db.get("database_name")
            if name:
                names.add(name)
        return sorted(names)

    def get_object_stores(self) -> List[str]:
        """Get unique object store names in current dataset."""
        stores = set()
        for row in self._rows:
            store = row.get("object_store")
            if store:
                stores.add(store)
        return sorted(stores)
