"""
Qt model for stored sites.

Aggregated view of sites with stored data across all storage types.
Uses the stored_sites table for proper tagging support.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import (
    refresh_stored_sites,
    get_stored_sites,
)

logger = logging.getLogger(__name__)


class StoredSitesTableModel(QAbstractTableModel):
    """
    Qt model for aggregated stored sites view.

    Shows summary of all storage types per site (local storage, session storage,
    IndexedDB) for quick triage. Supports unified tagging.
    """

    # Column definitions
    COLUMNS = [
        "origin",
        "local_storage_count",
        "session_storage_count",
        "indexeddb_count",
        "total_keys",
        "tags",
    ]

    HEADERS = [
        "Site",
        "Local Storage",
        "Session Storage",
        "IndexedDB",
        "Total Keys",
        "Tags",
    ]

    # Column indexes
    COL_ORIGIN = 0
    COL_LOCAL_STORAGE = 1
    COL_SESSION_STORAGE = 2
    COL_INDEXEDDB = 3
    COL_TOTAL = 4
    COL_TAGS = 5

    ARTIFACT_TYPE = "stored_site"

    def __init__(
        self,
        db_manager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize stored sites model.

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

        # Tag cache (id -> [tag_name, ...])
        self._tag_cache: Dict[int, List[str]] = {}

        # Filters
        self._origin_filter: str = ""
        self._min_total: int = 0

    def load(
        self,
        origin_filter: str = "",
        min_total: int = 0,
    ) -> None:
        """
        Load stored sites data from database with optional filters.

        Args:
            origin_filter: Origin substring filter
            min_total: Minimum total keys filter
        """
        self._origin_filter = origin_filter
        self._min_total = min_total

        self.beginResetModel()
        try:
            conn = self.db_manager.get_evidence_conn(
                self.evidence_id, self.evidence_label
            )

            # Refresh the stored_sites table (aggregates from storage tables)
            refresh_stored_sites(conn, self.evidence_id)

            # Fetch the stored sites
            all_rows = get_stored_sites(conn, self.evidence_id)

            # Apply filters
            self._rows = []
            for row in all_rows:
                # Apply origin filter
                origin = row.get("origin", "")
                if origin_filter and origin_filter.lower() not in origin.lower():
                    continue

                # Apply min_total filter
                total = row.get("total_keys", 0)
                if total < min_total:
                    continue

                self._rows.append(row)

            # Sort by total keys descending
            self._rows.sort(key=lambda r: r.get("total_keys", 0), reverse=True)

            # Load tags for all sites
            self._load_tags()

            logger.debug(
                "Loaded %d stored sites for evidence %d",
                len(self._rows),
                self.evidence_id,
            )
        except Exception as e:
            logger.error("Failed to load stored sites: %s", e)
            self._rows = []
        finally:
            self.endResetModel()

    def _load_tags(self) -> None:
        """Load tags for all stored sites."""
        self._tag_cache.clear()

        if not self.case_data:
            return

        # Collect all IDs
        ids = [row["id"] for row in self._rows if row.get("id")]
        if not ids:
            return

        try:
            self._tag_cache = self.case_data.get_tag_strings_for_artifacts(
                self.evidence_id, self.ARTIFACT_TYPE, ids
            )
        except Exception as e:
            logger.warning("Failed to load tags for stored sites: %s", e)

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

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:
        """Return data for the given index and role."""
        if not index.isValid():
            return None

        row = index.row()
        col = index.column()

        if row < 0 or row >= len(self._rows):
            return None

        record = self._rows[row]

        if role == Qt.DisplayRole:
            # Tags column
            if col == self.COL_TAGS:
                row_id = record.get("id")
                # _tag_cache stores comma-separated strings, not lists
                return self._tag_cache.get(row_id, "")

            col_name = self.COLUMNS[col]
            value = record.get(col_name)

            # Format count columns
            if col in (
                self.COL_LOCAL_STORAGE,
                self.COL_SESSION_STORAGE,
                self.COL_INDEXEDDB,
                self.COL_TOTAL,
            ):
                return str(value) if value else "0"

            return value or ""

        elif role == Qt.TextAlignmentRole:
            # Right-align numeric columns
            if col in (
                self.COL_LOCAL_STORAGE,
                self.COL_SESSION_STORAGE,
                self.COL_INDEXEDDB,
                self.COL_TOTAL,
            ):
                return Qt.AlignRight | Qt.AlignVCenter

        elif role == Qt.ToolTipRole:
            if col == self.COL_ORIGIN:
                # Show full breakdown on hover
                parts = []
                for key in ["local_storage_count", "session_storage_count",
                           "indexeddb_count"]:
                    count = record.get(key, 0)
                    if count:
                        name = key.replace("_count", "").replace("_", " ").title()
                        parts.append(f"{name}: {count}")
                return "\n".join(parts) if parts else "No data"
            elif col == self.COL_TAGS:
                row_id = record.get("id")
                tags_str = self._tag_cache.get(row_id, "")
                # Convert comma-separated to newline-separated for tooltip
                return tags_str.replace(", ", "\n") if tags_str else "No tags"

        elif role == Qt.UserRole:
            # Return full record for detail views
            return record

        return None

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole
    ) -> Any:
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def get_record_by_row(self, row: int) -> Optional[Dict[str, Any]]:
        """Get the full record for a given row."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_row_data(self, index: QModelIndex) -> Optional[Dict[str, Any]]:
        """Get row data by model index (compatible with _tag_selection)."""
        if not index.isValid():
            return None
        return self.get_record_by_row(index.row())

    def get_artifact_id_by_row(self, row: int) -> Optional[int]:
        """Get the artifact ID for a given row (for tagging)."""
        if 0 <= row < len(self._rows):
            return self._rows[row].get("id")
        return None

    def get_total_keys(self) -> int:
        """Get sum of all keys across all sites."""
        return sum(r.get("total_keys", 0) for r in self._rows)

    def refresh_tags(self) -> None:
        """Reload tags from database and refresh display."""
        self._load_tags()
        # Emit dataChanged for tags column
        if self._rows:
            top_left = self.index(0, self.COL_TAGS)
            bottom_right = self.index(len(self._rows) - 1, self.COL_TAGS)
            self.dataChanged.emit(top_left, bottom_right)
