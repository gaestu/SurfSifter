"""
Qt model for Firefox cache index table.

Displays metadata from the Firefox cache2 binary index, including entries
whose cached content has been evicted — proving site visits even without
cached files.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor, QFont

from core.database import DatabaseManager
from core.database.helpers.firefox_cache_index import (
    get_firefox_cache_index_count,
    get_firefox_cache_index_entries,
    get_firefox_cache_index_stats,
)

logger = logging.getLogger(__name__)


class CacheIndexTableModel(QAbstractTableModel):
    """Qt model for Firefox cache index entries with pagination and filtering."""

    COLUMNS = [
        "entry_hash",
        "url",
        "content_type_name",
        "frecency",
        "file_size_kb",
        "entry_source",
        "has_entry_file",
        "is_removed",
        "is_anonymous",
        "is_pinned",
    ]

    HEADERS = [
        "Entry Hash",
        "URL",
        "Content Type",
        "Frecency",
        "Size (KB)",
        "Source",
        "Has File",
        "Removed",
        "Anonymous",
        "Pinned",
    ]

    # Column indexes for formatting
    COL_HASH = 0
    COL_URL = 1
    COL_CONTENT_TYPE = 2
    COL_FRECENCY = 3
    COL_SIZE = 4
    COL_SOURCE = 5
    COL_HAS_FILE = 6
    COL_REMOVED = 7
    COL_ANONYMOUS = 8
    COL_PINNED = 9

    DEFAULT_PAGE_SIZE = 500

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        parent=None,
    ):
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label

        # Data storage
        self._rows: List[Dict[str, Any]] = []

        # Filters
        self._removed_only: bool = False
        self._has_entry_file: Optional[bool] = None
        self._content_type: Optional[int] = None
        self._entry_source: Optional[str] = None

        # Pagination
        self.page: int = 0
        self.page_size: int = self.DEFAULT_PAGE_SIZE
        self._total_count: int = 0

    def load(
        self,
        *,
        removed_only: bool = False,
        has_entry_file: Optional[bool] = None,
        content_type: Optional[int] = None,
        entry_source: Optional[str] = None,
        reset_page: bool = True,
    ) -> None:
        """Load cache index entries from database with optional filters.

        Args:
            removed_only: If True, show only removed entries.
            has_entry_file: Filter by backing file existence.
            content_type: Filter by content type enum value.
            entry_source: Filter by entry source string.
            reset_page: If True, reset to page 0 when filters change.
        """
        if reset_page:
            self.page = 0

        self._removed_only = removed_only
        self._has_entry_file = has_entry_file
        self._content_type = content_type
        self._entry_source = entry_source

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label,
            )

            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row

                filter_kwargs = self._build_filter_kwargs()

                self._total_count = get_firefox_cache_index_count(
                    conn, self.evidence_id, **filter_kwargs,
                )

                offset = self.page * self.page_size
                self._rows = get_firefox_cache_index_entries(
                    conn,
                    self.evidence_id,
                    **filter_kwargs,
                    limit=self.page_size,
                    offset=offset,
                )

            logger.debug(
                "Loaded %d cache index entries (page %d, total %d)",
                len(self._rows), self.page + 1, self._total_count,
            )

        except Exception as e:
            logger.error("Failed to load cache index entries: %s", e, exc_info=True)
            self._rows = []
            self._total_count = 0

        self.endResetModel()

    def _build_filter_kwargs(self) -> Dict[str, Any]:
        """Build keyword arguments for helper query functions."""
        kwargs: Dict[str, Any] = {}
        if self._removed_only:
            kwargs["removed_only"] = True
        if self._has_entry_file is not None:
            kwargs["has_entry_file"] = self._has_entry_file
        if self._content_type is not None:
            kwargs["content_type"] = self._content_type
        if self._entry_source is not None:
            kwargs["entry_source"] = self._entry_source
        return kwargs

    # ─── QAbstractTableModel Interface ──────────────────────────────

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self.COLUMNS)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid() or index.row() >= len(self._rows):
            return None

        row = self._rows[index.row()]
        col = index.column()
        col_name = self.COLUMNS[col]

        if role == Qt.DisplayRole:
            value = row.get(col_name)

            # URL: truncate for display
            if col == self.COL_URL:
                url = str(value) if value else ""
                return url[:100] + "..." if len(url) > 100 else url

            # Hash: show first 12 chars
            if col == self.COL_HASH:
                h = str(value) if value else ""
                return h[:12] + "…" if len(h) > 12 else h

            # Boolean columns
            if col in (self.COL_HAS_FILE, self.COL_REMOVED,
                       self.COL_ANONYMOUS, self.COL_PINNED):
                return "Yes" if value else "No"

            # Size
            if col == self.COL_SIZE:
                return str(value) if value is not None else ""

            # Frecency
            if col == self.COL_FRECENCY:
                return str(value) if value is not None else ""

            # Source
            if col == self.COL_SOURCE:
                return str(value) if value else "index_only"

            return str(value) if value else ""

        if role == Qt.ToolTipRole:
            if col == self.COL_URL:
                return row.get("url", "")
            if col == self.COL_HASH:
                return row.get("entry_hash", "")

        if role == Qt.UserRole:
            return row

        if role == Qt.TextAlignmentRole:
            if col in (self.COL_FRECENCY, self.COL_SIZE):
                return Qt.AlignRight | Qt.AlignVCenter
            if col in (self.COL_HAS_FILE, self.COL_REMOVED,
                       self.COL_ANONYMOUS, self.COL_PINNED):
                return Qt.AlignCenter

        if role == Qt.ForegroundRole:
            is_removed = row.get("is_removed", False)
            entry_source = row.get("entry_source", "")

            # Removed entries: red/dimmed
            if is_removed:
                return QColor(200, 60, 60)

            # Doomed/trash entries: orange
            if entry_source in ("doomed", "trash"):
                return QColor(200, 120, 0)

        if role == Qt.FontRole:
            has_file = row.get("has_entry_file", False)
            if not has_file:
                font = QFont()
                font.setItalic(True)
                return font

        return None

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole,
    ):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if 0 <= section < len(self.HEADERS):
                return self.HEADERS[section]
        return None

    # ─── Data Access ────────────────────────────────────────────────

    def get_row_data(self, row: int) -> Optional[Dict[str, Any]]:
        """Get full row data by row index."""
        if 0 <= row < len(self._rows):
            return self._rows[row]
        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get summary statistics for current evidence."""
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label,
            )
            with sqlite3.connect(evidence_db_path) as conn:
                return get_firefox_cache_index_stats(conn, self.evidence_id)
        except Exception as e:
            logger.error("Failed to get cache index stats: %s", e, exc_info=True)
            return {
                "total": 0, "removed": 0,
                "with_file": 0, "without_file": 0,
                "by_content_type": {}, "by_entry_source": {},
            }

    # ─── Pagination ─────────────────────────────────────────────────

    def total_count(self) -> int:
        """Get total count matching current filters."""
        return self._total_count

    def total_pages(self) -> int:
        if self._total_count == 0:
            return 1
        return (self._total_count + self.page_size - 1) // self.page_size

    def current_page(self) -> int:
        return self.page

    def has_next_page(self) -> bool:
        return self.page < self.total_pages() - 1

    def has_prev_page(self) -> bool:
        return self.page > 0

    def next_page(self) -> None:
        if self.has_next_page():
            self.page += 1
            self.load(
                removed_only=self._removed_only,
                has_entry_file=self._has_entry_file,
                content_type=self._content_type,
                entry_source=self._entry_source,
                reset_page=False,
            )

    def prev_page(self) -> None:
        if self.has_prev_page():
            self.page -= 1
            self.load(
                removed_only=self._removed_only,
                has_entry_file=self._has_entry_file,
                content_type=self._content_type,
                entry_source=self._entry_source,
                reset_page=False,
            )

    def goto_page(self, page: int) -> None:
        max_page = self.total_pages() - 1
        self.page = max(0, min(page, max_page))
        self.load(
            removed_only=self._removed_only,
            has_entry_file=self._has_entry_file,
            content_type=self._content_type,
            entry_source=self._entry_source,
            reset_page=False,
        )

    def reload(self) -> None:
        """Reload current page with current filters."""
        self.load(
            removed_only=self._removed_only,
            has_entry_file=self._has_entry_file,
            content_type=self._content_type,
            entry_source=self._entry_source,
            reset_page=False,
        )
