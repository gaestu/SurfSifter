"""
Qt model for autofill_profile_tokens table.

Displays Chromium 100+ modern contact info tokens with decoded field types.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import get_autofill_profile_tokens, CHROMIUM_TOKEN_TYPES, get_token_type_name

logger = logging.getLogger(__name__)


class AutofillProfileTokensTableModel(QAbstractTableModel):
    """
    Qt model for autofill_profile_tokens table.

    Displays modern Chromium contact_info storage tokens (v100+).
    Shows both the raw token type code and decoded human-readable name.
    """

    # Column definitions - DB column names
    COLUMNS = [
        "guid",
        "token_type",
        "token_type_name",  # Virtual column - decoded from type
        "token_value",
        "browser",
        "profile",
        "parent_date_modified_utc",
        "tags",
    ]

    HEADERS = [
        "Profile GUID",
        "Token Type",
        "Field Type",
        "Value",
        "Browser",
        "Profile",
        "Modified",
        "Tags",
    ]

    # Column indexes
    COL_PROFILE_GUID = 0
    COL_TOKEN_TYPE = 1
    COL_TOKEN_TYPE_NAME = 2
    COL_VALUE = 3
    COL_BROWSER = 4
    COL_PROFILE = 5
    COL_DATE_MODIFIED = 6
    COL_TAGS = 7

    ARTIFACT_TYPE = "autofill_profile_token"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize autofill profile tokens model.

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
        self._field_type_filter: str = ""

    def load(self, browser_filter: str = "", field_type_filter: str = "") -> None:
        """
        Load autofill profile tokens from database with optional filters.

        Args:
            browser_filter: Browser name filter (empty = all)
            field_type_filter: Field type name filter (empty = all)
        """
        self._browser_filter = browser_filter
        self._field_type_filter = field_type_filter

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = get_autofill_profile_tokens(
                    conn,
                    self.evidence_id,
                    browser=browser_filter or None,
                    limit=5000,
                )

                # Decode token types and apply filter
                for row in self._rows:
                    token_type = row.get("token_type")
                    row["token_type_name"] = get_token_type_name(token_type)

                if field_type_filter:
                    filter_lower = field_type_filter.lower()
                    self._rows = [
                        r for r in self._rows
                        if filter_lower in r.get("token_type_name", "").lower()
                    ]

                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} autofill profile token entries")

        except Exception as e:
            logger.error(f"Failed to load autofill profile tokens: {e}", exc_info=True)
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
        """Get list of browsers that have profile token data."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT DISTINCT browser FROM autofill_profile_tokens WHERE evidence_id = ?",
                    (self.evidence_id,)
                )
                return [row["browser"] for row in cursor.fetchall() if row["browser"]]
        except Exception as e:
            logger.error(f"Failed to get profile token browsers: {e}", exc_info=True)
            return []

    def get_available_field_types(self) -> List[str]:
        """Get list of distinct field type names in the data."""
        seen = set()
        types = []
        for row in self._rows:
            name = row.get("token_type_name", "UNKNOWN")
            if name not in seen:
                seen.add(name)
                types.append(name)
        return sorted(types)

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
            if col == self.COL_PROFILE_GUID:
                guid = row_data.get("guid", "")
                # Show truncated GUID
                if len(guid) > 12:
                    return guid[:8] + "..."
                return guid
            elif col == self.COL_TOKEN_TYPE:
                return str(row_data.get("token_type", 0))
            elif col == self.COL_TOKEN_TYPE_NAME:
                return row_data.get("token_type_name", "UNKNOWN")
            elif col == self.COL_VALUE:
                value = row_data.get("token_value") or ""
                # Truncate long values
                if len(value) > 50:
                    return value[:47] + "..."
                return value
            elif col == self.COL_BROWSER:
                return row_data.get("browser", "").capitalize()
            elif col == self.COL_PROFILE:
                return row_data.get("profile") or ""
            elif col == self.COL_DATE_MODIFIED:
                modified = row_data.get("parent_date_modified_utc") or row_data.get("date_modified_utc")
                if modified:
                    return modified[:10] if len(modified) > 10 else modified
                return ""
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""

        elif role == Qt.ToolTipRole:
            if col == self.COL_PROFILE_GUID:
                return row_data.get("guid", "")
            elif col == self.COL_VALUE:
                return row_data.get("token_value", "")
            elif col == self.COL_DATE_MODIFIED:
                return row_data.get("parent_date_modified_utc") or row_data.get("date_modified_utc", "")
            elif col == self.COL_TAGS:
                return self._tag_map.get(row_data.get("id"), "") or ""
            elif col == self.COL_TOKEN_TYPE:
                # Show full token type mapping in tooltip
                token_type = row_data.get("token_type", 0)
                return f"Type {token_type}: {get_token_type_name(token_type)}"

        elif role == Qt.TextAlignmentRole:
            if col == self.COL_TOKEN_TYPE:
                return Qt.AlignCenter

        return None

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            return self.HEADERS[section]
        return None
