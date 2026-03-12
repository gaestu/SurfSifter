"""Qt table model for DPAPI master keys."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

logger = logging.getLogger(__name__)


class MasterKeysModel(QAbstractTableModel):
    """Displays DPAPI master keys for a selected Windows user."""

    HEADERS = [
        "GUID",
        "SID",
        "Username",
        "Source Path",
        "Status",
        "Unlock Method",
    ]

    COL_GUID = 0
    COL_SID = 1
    COL_USERNAME = 2
    COL_SOURCE_PATH = 3
    COL_STATUS = 4
    COL_UNLOCK_METHOD = 5

    _STATUS_ICONS = {
        "unlocked": "\U0001f513",   # 🔓
        "locked": "\U0001f512",     # 🔒
        "failed": "\u274c",         # ❌
    }

    def __init__(self, parent: Optional[Any] = None) -> None:
        super().__init__(parent)
        self._rows: List[Dict[str, Any]] = []

    def load(self, rows: List[Dict[str, Any]]) -> None:
        self.beginResetModel()
        self._rows = list(rows)
        self.endResetModel()

    def clear(self) -> None:
        self.beginResetModel()
        self._rows = []
        self.endResetModel()

    # -- Qt interface ---------------------------------------------------------

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self.HEADERS)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self.HEADERS[section]
        return None

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None

        row = self._rows[index.row()]
        col = index.column()

        if role == Qt.ItemDataRole.DisplayRole:
            if col == self.COL_GUID:
                return row.get("guid", "")
            elif col == self.COL_SID:
                return row.get("sid", "")
            elif col == self.COL_USERNAME:
                return row.get("username", "")
            elif col == self.COL_SOURCE_PATH:
                path = row.get("source_path", "")
                if len(path) > 80:
                    return "..." + path[-77:]
                return path
            elif col == self.COL_STATUS:
                status = row.get("status", "")
                icon = self._STATUS_ICONS.get(status, "")
                return f"{icon} {status}".strip() if status else ""
            elif col == self.COL_UNLOCK_METHOD:
                return row.get("unlock_method", "") or ""

        elif role == Qt.ItemDataRole.ToolTipRole:
            if col == self.COL_SOURCE_PATH:
                return row.get("source_path", "")
            elif col == self.COL_STATUS:
                err = row.get("error_message")
                if err:
                    return f"Error: {err}"

        elif role == Qt.ItemDataRole.TextAlignmentRole:
            if col == self.COL_STATUS:
                return Qt.AlignmentFlag.AlignCenter

        return None
