"""
Base Qt table model for artifact tables.

Provides a generic base class for all artifact table models, reducing code
duplication across cookies, bookmarks, downloads, autofill, etc. models.
"""
from __future__ import annotations

import logging
import sqlite3
from abc import abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

from core.database import DatabaseManager

logger = logging.getLogger(__name__)


class BaseArtifactTableModel(QAbstractTableModel):
    """
    Base class for artifact table models.

    Subclasses must define:
    - COLUMNS: List of column keys (dict keys from database)
    - HEADERS: List of column display names
    - ARTIFACT_TYPE: Tag association type (e.g., "cookie", "bookmark")

    Subclasses should override:
    - _fetch_data(): Execute query and return list of dicts
    - _format_cell(): Optional custom cell formatting
    - get_available_browsers(): Optional browser list method
    """

    # Subclasses must define these
    COLUMNS: List[str] = []
    HEADERS: List[str] = []
    ARTIFACT_TYPE: str = ""

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        case_data=None,
        parent=None
    ):
        """
        Initialize base artifact model.

        Args:
            db_manager: Database manager instance
            evidence_id: Evidence ID
            evidence_label: Evidence label for database path resolution
            case_data: CaseDataAccess for tag lookups (optional)
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

        # Generic filters (subclasses may add more)
        self._filters: Dict[str, Any] = {}

    @property
    def evidence_db_path(self) -> Path:
        """Get evidence database path."""
        return self.db_manager.evidence_db_path(
            self.evidence_id, label=self.evidence_label
        )

    def load(self, **filters) -> None:
        """
        Load data from database with optional filters.

        Args:
            **filters: Filter parameters passed to _fetch_data()
        """
        self._filters = filters

        self.beginResetModel()
        try:
            with sqlite3.connect(self.evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row
                self._rows = self._fetch_data(conn, **filters)
                self._refresh_tags()

            logger.debug(f"Loaded {len(self._rows)} {self.ARTIFACT_TYPE} records")

        except Exception as e:
            logger.error(f"Failed to load {self.ARTIFACT_TYPE}: {e}", exc_info=True)
            self._rows = []
            self._tag_map = {}

        self.endResetModel()

    @abstractmethod
    def _fetch_data(self, conn: sqlite3.Connection, **filters) -> List[Dict[str, Any]]:
        """
        Fetch data from database.

        Subclasses must implement this to execute their specific query.

        Args:
            conn: Database connection with row_factory set
            **filters: Filter parameters

        Returns:
            List of row dicts
        """
        pass

    def _refresh_tags(self) -> None:
        """Refresh tag strings for current rows."""
        if not self.case_data or not self.ARTIFACT_TYPE:
            self._tag_map = {}
            return
        ids = [row.get("id") for row in self._rows if row.get("id") is not None]
        self._tag_map = self.case_data.get_tag_strings_for_artifacts(
            self.evidence_id,
            self.ARTIFACT_TYPE,
            ids,
        )

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """Get full row data for given index."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return self._rows[index.row()]

    def get_row_id(self, index: QModelIndex) -> Optional[int]:
        """Get row ID for given index."""
        row_data = self.get_row_data(index)
        return row_data.get("id")

    def get_selected_ids(self, indexes: List[QModelIndex]) -> List[int]:
        """Get list of IDs for selected indexes (unique rows only)."""
        seen_rows = set()
        ids = []
        for index in indexes:
            row = index.row()
            if row not in seen_rows:
                seen_rows.add(row)
                row_id = self.get_row_id(index)
                if row_id is not None:
                    ids.append(row_id)
        return ids

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
        col_key = self.COLUMNS[col] if col < len(self.COLUMNS) else None

        if role == Qt.DisplayRole:
            return self._format_cell(row_data, col, col_key)

        elif role == Qt.ToolTipRole:
            return self._format_tooltip(row_data, col, col_key)

        elif role == Qt.TextAlignmentRole:
            return self._get_alignment(col, col_key)

        return None

    def _format_cell(self, row_data: Dict[str, Any], col: int, col_key: str) -> Any:
        """
        Format cell value for display.

        Subclasses can override for custom formatting.
        Default implementation returns the raw value or tags for 'tags' column.

        Args:
            row_data: Row dictionary
            col: Column index
            col_key: Column key (from COLUMNS)

        Returns:
            Formatted display value
        """
        if col_key == "tags":
            return self._tag_map.get(row_data.get("id"), "") or ""

        value = row_data.get(col_key, "")

        # Common value transformations
        if value is None:
            return ""
        if isinstance(value, bool):
            return "Yes" if value else "No"
        if col_key == "browser":
            return str(value).capitalize()

        return value

    def _format_tooltip(self, row_data: Dict[str, Any], col: int, col_key: str) -> Any:
        """
        Format tooltip for cell.

        Override for custom tooltips. Default returns full value.
        """
        if col_key == "tags":
            return self._tag_map.get(row_data.get("id"), "") or ""
        return row_data.get(col_key, "")

    def _get_alignment(self, col: int, col_key: str) -> int:
        """
        Get text alignment for column.

        Override for custom alignment. Default is left-aligned.
        """
        return Qt.AlignLeft | Qt.AlignVCenter

    def headerData(self, section: int, orientation, role: int = Qt.DisplayRole):
        """Return header data."""
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    def refresh(self) -> None:
        """Reload data with current filters."""
        self.load(**self._filters)
