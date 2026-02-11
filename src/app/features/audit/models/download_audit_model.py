"""
Qt model for download_audit table.

Displays investigator download audit rows for the Audit tab.
"""
from __future__ import annotations

import logging
import sqlite3
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt
from PySide6.QtGui import QColor

from core.database import (
    DatabaseManager,
    get_download_audit,
    get_download_audit_count,
    get_download_audit_summary,
)

logger = logging.getLogger(__name__)


class DownloadAuditTableModel(QAbstractTableModel):
    """Qt model for the per-evidence download audit table."""

    COLUMNS = [
        "ts_utc",
        "url",
        "method",
        "outcome",
        "status_code",
        "attempts",
        "bytes_written",
        "content_type",
        "duration_s",
        "caller_info",
        "reason",
    ]

    HEADERS = [
        "Timestamp (UTC)",
        "URL",
        "Method",
        "Outcome",
        "HTTP",
        "Attempts",
        "Bytes",
        "Content-Type",
        "Duration",
        "Caller",
        "Reason",
    ]

    OUTCOME_COLORS = {
        "success": "#2e7d32",
        "failed": "#c62828",
        "blocked": "#ef6c00",
        "cancelled": "#546e7a",
        "error": "#6a1b9a",
    }

    COL_OUTCOME = 3
    COL_BYTES = 6
    COL_DURATION = 8

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

        self._rows: List[Dict[str, Any]] = []
        self._outcome_filter: str = ""
        self._search_text: str = ""
        self._limit: int = 1000
        self._offset: int = 0
        self._total_count: int = 0
        self._summary: Dict[str, Any] = {"total": 0, "by_outcome": {}}

    def load(
        self,
        *,
        outcome_filter: str = "",
        search_text: str = "",
        limit: int = 1000,
        offset: int = 0,
    ) -> None:
        """Load rows with optional filters and pagination."""
        self._outcome_filter = outcome_filter
        self._search_text = search_text
        self._limit = limit
        self._offset = offset

        self.beginResetModel()
        try:
            evidence_db_path = self.db_manager.evidence_db_path(
                self.evidence_id, label=self.evidence_label
            )
            with sqlite3.connect(evidence_db_path) as conn:
                conn.row_factory = sqlite3.Row

                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if "download_audit" not in tables:
                    self._rows = []
                    self._total_count = 0
                    self._summary = {"total": 0, "by_outcome": {}}
                    self.endResetModel()
                    return

                self._rows = get_download_audit(
                    conn,
                    self.evidence_id,
                    outcome=outcome_filter or None,
                    search_text=search_text or None,
                    limit=limit,
                    offset=offset,
                )
                self._total_count = get_download_audit_count(
                    conn,
                    self.evidence_id,
                    outcome=outcome_filter or None,
                    search_text=search_text or None,
                )
                self._summary = get_download_audit_summary(conn, self.evidence_id)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to load download audit rows: %s", exc, exc_info=True)
            self._rows = []
            self._total_count = 0
            self._summary = {"total": 0, "by_outcome": {}}
        self.endResetModel()

    @property
    def total_count(self) -> int:
        return self._total_count

    def get_summary(self) -> Dict[str, Any]:
        return self._summary

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self.COLUMNS)

    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.DisplayRole,
    ):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self.HEADERS):
            return self.HEADERS[section]
        if orientation == Qt.Vertical:
            return str(section + 1)
        return None

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole):
        if not index.isValid() or index.row() >= len(self._rows):
            return None

        row = self._rows[index.row()]
        col = index.column()
        key = self.COLUMNS[col]
        value = row.get(key)

        if role == Qt.DisplayRole:
            if col == self.COL_BYTES:
                return self._format_size(value)
            if col == self.COL_DURATION:
                if value is None:
                    return ""
                return f"{float(value):.2f}s"
            if value is None:
                return ""
            return str(value)

        if role == Qt.ForegroundRole and col == self.COL_OUTCOME:
            color = self.OUTCOME_COLORS.get(str(value).lower())
            if color:
                return QColor(color)

        if role == Qt.TextAlignmentRole:
            if col in {4, 5, 6, 8}:  # HTTP, attempts, bytes, duration
                return Qt.AlignRight | Qt.AlignVCenter
            return Qt.AlignLeft | Qt.AlignVCenter

        return None

    @staticmethod
    def _format_size(size_bytes: Any) -> str:
        """Format bytes in human-readable units."""
        if size_bytes is None:
            return ""
        try:
            size = int(size_bytes)
        except (TypeError, ValueError):
            return ""

        if size < 1024:
            return f"{size} B"
        if size < 1024 * 1024:
            return f"{size / 1024:.1f} KB"
        if size < 1024 * 1024 * 1024:
            return f"{size / (1024 * 1024):.1f} MB"
        return f"{size / (1024 * 1024 * 1024):.2f} GB"
