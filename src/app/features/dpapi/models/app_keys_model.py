"""Qt table model for Chromium application-bound encryption keys."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

logger = logging.getLogger(__name__)


class ChromiumAppKeysModel(QAbstractTableModel):
    """Displays Chromium Local State encryption keys and their DPAPI status."""

    HEADERS = [
        "Browser",
        "SID",
        "Profile Root",
        "Local State Path",
        "Master Key GUID",
        "Status",
    ]

    COL_BROWSER = 0
    COL_SID = 1
    COL_PROFILE_ROOT = 2
    COL_LOCAL_STATE = 3
    COL_MASTER_KEY_GUID = 4
    COL_STATUS = 5

    _STATUS_ICONS = {
        "decrypted": "\U0001f513",    # 🔓
        "encrypted": "\U0001f512",    # 🔒
        "failed": "\u274c",           # ❌
        "no_key": "\u2753",           # ❓
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
            if col == self.COL_BROWSER:
                return (row.get("browser") or "").capitalize()
            elif col == self.COL_SID:
                return row.get("sid", "")
            elif col == self.COL_PROFILE_ROOT:
                path = row.get("profile_root", "")
                if len(path) > 60:
                    return "..." + path[-57:]
                return path
            elif col == self.COL_LOCAL_STATE:
                path = row.get("local_state_path", "")
                if len(path) > 60:
                    return "..." + path[-57:]
                return path
            elif col == self.COL_MASTER_KEY_GUID:
                return row.get("master_key_guid", "") or ""
            elif col == self.COL_STATUS:
                status = row.get("status", "")
                icon = self._STATUS_ICONS.get(status, "")
                return f"{icon} {status}".strip() if status else ""

        elif role == Qt.ItemDataRole.ToolTipRole:
            if col == self.COL_PROFILE_ROOT:
                return row.get("profile_root", "")
            elif col == self.COL_LOCAL_STATE:
                return row.get("local_state_path", "")
            elif col == self.COL_STATUS:
                err = row.get("error_message")
                if err:
                    return f"Error: {err}"

        elif role == Qt.ItemDataRole.TextAlignmentRole:
            if col == self.COL_STATUS:
                return Qt.AlignmentFlag.AlignCenter

        return None
