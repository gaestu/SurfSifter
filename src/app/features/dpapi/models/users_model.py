"""Qt table model for Windows Users in the DPAPI tab."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

logger = logging.getLogger(__name__)


class WindowsUsersModel(QAbstractTableModel):
    """Displays Windows user accounts discovered during DPAPI key extraction."""

    HEADERS = [
        "Username",
        "SID",
        "Profile Path",
        "Keys Found",
        "Keys Unlocked",
        "NTLM",
    ]

    COL_USERNAME = 0
    COL_SID = 1
    COL_PROFILE = 2
    COL_KEYS_FOUND = 3
    COL_KEYS_UNLOCKED = 4
    COL_NTLM = 5

    def __init__(self, parent: Optional[Any] = None) -> None:
        super().__init__(parent)
        self._rows: List[Dict[str, Any]] = []

    def load(self, rows: List[Dict[str, Any]]) -> None:
        """Replace model data with *rows* fetched by the tab."""
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
            if col == self.COL_USERNAME:
                return row.get("username", "")
            elif col == self.COL_SID:
                return row.get("sid", "")
            elif col == self.COL_PROFILE:
                return row.get("profile_path", "")
            elif col == self.COL_KEYS_FOUND:
                return str(row.get("master_keys_found", 0))
            elif col == self.COL_KEYS_UNLOCKED:
                return str(row.get("master_keys_unlocked", 0))
            elif col == self.COL_NTLM:
                return "Yes" if row.get("ntlm_hash_available") else "No"

        elif role == Qt.ItemDataRole.ToolTipRole:
            if col == self.COL_PROFILE:
                return row.get("profile_path", "")
            elif col == self.COL_SID:
                return row.get("sid", "")

        elif role == Qt.ItemDataRole.TextAlignmentRole:
            if col in (self.COL_KEYS_FOUND, self.COL_KEYS_UNLOCKED, self.COL_NTLM):
                return Qt.AlignmentFlag.AlignCenter

        return None

    def get_row_data(self, index: QModelIndex) -> Dict[str, Any]:
        """Return the full row dict for *index*."""
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return {}
        return self._rows[index.row()]
