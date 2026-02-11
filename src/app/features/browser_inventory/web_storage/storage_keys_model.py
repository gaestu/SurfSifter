"""
Qt model for storage keys table.

Unified view of local_storage and session_storage key-value pairs.
Enables forensic analysis of data stored by websites.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor, QFont

from core.database import get_local_storage, get_session_storage

logger = logging.getLogger(__name__)


class StorageKeysTableModel(QAbstractTableModel):
    """
    Qt model for unified storage keys view.

    Shows all local_storage and session_storage key-value pairs with
    filtering by origin, storage type, and browser.

    Added last_access_utc and partition_index columns.
    """

    # Column definitions
    COLUMNS = [
        "origin",
        "storage_type",
        "key",
        "value",
        "browser",
        "profile",
        "value_size",
        "last_access_utc",
        "partition_index",
        "tags",
    ]

    HEADERS = [
        "Origin",
        "Type",
        "Key",
        "Value",
        "Browser",
        "Profile",
        "Size",
        "Last Access",
        "Partition",
        "Tags",
    ]

    # Column indexes
    COL_ORIGIN = 0
    COL_STORAGE_TYPE = 1
    COL_KEY = 2
    COL_VALUE = 3
    COL_BROWSER = 4
    COL_PROFILE = 5
    COL_VALUE_SIZE = 6
    COL_LAST_ACCESS = 7
    COL_PARTITION = 8
    COL_TAGS = 9

    # Artifact types for tagging (one per storage type)
    ARTIFACT_TYPE_LOCAL = "local_storage"
    ARTIFACT_TYPE_SESSION = "session_storage"

    # Max value length for display (full value in detail dialog)
    MAX_VALUE_DISPLAY = 100

    # Keys that often contain forensically interesting data
    INTERESTING_KEYS = {
        "username", "user", "userid", "user_id", "email", "login",
        "session", "sessionid", "session_id", "token", "auth",
        "password", "pwd", "credential", "key", "secret",
        "account", "name", "id", "uid",
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
        Initialize storage keys model.

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

        # Tag maps - separate for local and session storage
        self._local_tag_map: Dict[int, str] = {}
        self._session_tag_map: Dict[int, str] = {}

        # Filters
        self._origin_filter: str = ""
        self._storage_type_filter: str = ""  # "local", "session", or "" for all
        self._browser_filter: str = ""
        self._key_filter: str = ""

        # Stats
        self._local_count: int = 0
        self._session_count: int = 0

    def load(
        self,
        origin_filter: str = "",
        storage_type_filter: str = "",
        browser_filter: str = "",
        key_filter: str = "",
    ) -> None:
        """
        Load storage keys from database with optional filters.

        Args:
            origin_filter: Origin substring filter
            storage_type_filter: "local", "session", or "" for all
            browser_filter: Browser name filter
            key_filter: Key name substring filter
        """
        self._origin_filter = origin_filter
        self._storage_type_filter = storage_type_filter
        self._browser_filter = browser_filter
        self._key_filter = key_filter

        self.beginResetModel()
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )

            combined_rows = []

            # Load local storage if not filtered to session only
            if storage_type_filter != "session":
                local_rows = get_local_storage(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    origin=origin_filter or None,
                    limit=10000,
                )
                self._local_count = len(local_rows)
                for row in local_rows:
                    row["storage_type"] = "local"
                    combined_rows.append(row)

            # Load session storage if not filtered to local only
            if storage_type_filter != "local":
                session_rows = get_session_storage(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    origin=origin_filter or None,
                    limit=10000,
                )
                self._session_count = len(session_rows)
                for row in session_rows:
                    row["storage_type"] = "session"
                    combined_rows.append(row)

            # Apply key filter if specified
            if key_filter:
                key_lower = key_filter.lower()
                combined_rows = [
                    r for r in combined_rows
                    if key_lower in (r.get("key") or "").lower()
                ]

            # Sort by origin, then storage_type, then key
            combined_rows.sort(key=lambda r: (
                r.get("origin", ""),
                r.get("storage_type", ""),
                r.get("key", ""),
            ))

            self._rows = combined_rows

            # Refresh tags for both storage types
            self._refresh_tags()

            logger.debug(
                "Loaded %d storage keys for evidence %d (local=%d, session=%d)",
                len(self._rows),
                self.evidence_id,
                self._local_count,
                self._session_count,
            )
        except Exception as e:
            logger.error("Failed to load storage keys: %s", e)
            self._rows = []
            self._local_count = 0
            self._session_count = 0
            self._local_tag_map = {}
            self._session_tag_map = {}
        finally:
            self.endResetModel()

    def _refresh_tags(self) -> None:
        """Refresh tag strings for current rows."""
        if not self.case_data:
            self._local_tag_map = {}
            self._session_tag_map = {}
            return

        # Collect IDs by storage type
        local_ids = [
            row.get("id") for row in self._rows
            if row.get("storage_type") == "local" and row.get("id") is not None
        ]
        session_ids = [
            row.get("id") for row in self._rows
            if row.get("storage_type") == "session" and row.get("id") is not None
        ]

        # Fetch tags for each type
        if local_ids:
            self._local_tag_map = self.case_data.get_tag_strings_for_artifacts(
                self.evidence_id,
                self.ARTIFACT_TYPE_LOCAL,
                local_ids,
            )
        else:
            self._local_tag_map = {}

        if session_ids:
            self._session_tag_map = self.case_data.get_tag_strings_for_artifacts(
                self.evidence_id,
                self.ARTIFACT_TYPE_SESSION,
                session_ids,
            )
        else:
            self._session_tag_map = {}

    def get_artifact_type_for_row(self, row_data: Dict[str, Any]) -> str:
        """Get the artifact type for a row based on storage type."""
        storage_type = row_data.get("storage_type", "")
        if storage_type == "local":
            return self.ARTIFACT_TYPE_LOCAL
        elif storage_type == "session":
            return self.ARTIFACT_TYPE_SESSION
        return "storage_key"  # Fallback

    def get_stats(self) -> Dict[str, int]:
        """Get current storage key statistics."""
        return {
            "total": len(self._rows),
            "local": self._local_count,
            "session": self._session_count,
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
        elif col_name == "storage_type":
            st = row_data.get("storage_type", "")
            return st.capitalize() if st else ""
        elif col_name == "browser":
            browser = row_data.get("browser", "")
            return browser.capitalize() if browser else ""
        elif col_name == "profile":
            return row_data.get("profile") or ""
        elif col_name == "last_access_utc":
            ts = row_data.get("last_access_utc")
            if ts:
                # Truncate to YYYY-MM-DD HH:MM
                return ts[:16] if len(ts) > 16 else ts
            return ""
        elif col_name == "partition_index":
            partition = row_data.get("partition_index")
            return str(partition) if partition is not None else ""
        elif col_name == "tags":
            # Get tags from appropriate map based on storage type
            artifact_id = row_data.get("id")
            if artifact_id is None:
                return ""
            storage_type = row_data.get("storage_type", "")
            if storage_type == "local":
                return self._local_tag_map.get(artifact_id, "") or ""
            elif storage_type == "session":
                return self._session_tag_map.get(artifact_id, "") or ""
            return ""
        else:
            return str(row_data.get(col_name) or "")

    def _get_background_color(self, row_data: Dict[str, Any], col_name: str) -> Optional[QColor]:
        """Get background color for highlighting interesting keys."""
        key = (row_data.get("key") or "").lower()

        # Highlight keys that might contain forensically interesting data
        for interesting in self.INTERESTING_KEYS:
            if interesting in key:
                return QColor(255, 255, 200)  # Light yellow

        return None

    def _get_font(self, row_data: Dict[str, Any], col_name: str) -> Optional[QFont]:
        """Get font for cell."""
        if col_name == "key":
            key = (row_data.get("key") or "").lower()
            for interesting in self.INTERESTING_KEYS:
                if interesting in key:
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
        elif col_name == "key":
            key = (row_data.get("key") or "").lower()
            for interesting in self.INTERESTING_KEYS:
                if interesting in key:
                    return "This key may contain forensically interesting data"
        return None

    def get_row_data(self, row: int) -> Optional[Dict[str, Any]]:
        """Get row data dictionary by row index."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_origins(self) -> List[str]:
        """Get unique origins in current dataset."""
        origins = set()
        for row in self._rows:
            origin = row.get("origin")
            if origin:
                origins.add(origin)
        return sorted(origins)

    def get_browsers(self) -> List[str]:
        """Get unique browsers in current dataset."""
        browsers = set()
        for row in self._rows:
            browser = row.get("browser")
            if browser:
                browsers.add(browser)
        return sorted(browsers)
