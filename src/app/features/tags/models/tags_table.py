"""
Tags table model for unified tag management.

Provides filterable tag listing with usage counts and color display.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess


class TagsTableModel(QAbstractTableModel):
    headers = [
        "Tag Name",
        "Description",
        "Color",
        "Created At (UTC)",
        "Usage Count",
    ]

    def __init__(self, case_data: Optional[CaseDataAccess] = None) -> None:
        super().__init__()
        self.case_data = case_data
        self.evidence_id: Optional[int] = None
        self._rows: List[Dict[str, Any]] = []
        self._filter_name: Optional[str] = None

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:  # noqa: N802
        return len(self.headers)

    def data(self, index: QModelIndex, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None
        row = self._rows[index.row()]
        column = index.column()

        if role == Qt.DisplayRole:
            if column == 0:
                return row.get("name", "")
            if column == 1:
                return row.get("description", "")
            if column == 2:
                return row.get("color", "")
            if column == 3:
                return row.get("created_at_utc", "")
            if column == 4:
                return str(row.get("usage_count", 0))

        elif role == Qt.DecorationRole:
            if column == 0:
                color_code = row.get("color")
                if color_code:
                    from PySide6.QtGui import QColor, QIcon, QPixmap, QPainter
                    pixmap = QPixmap(12, 12)
                    pixmap.fill(Qt.transparent)
                    painter = QPainter(pixmap)
                    painter.setBrush(QColor(color_code))
                    painter.setPen(Qt.NoPen)
                    painter.drawEllipse(0, 0, 12, 12)
                    painter.end()
                    return QIcon(pixmap)

        elif role == Qt.UserRole:
            return row

        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            if 0 <= section < len(self.headers):
                return self.headers[section]
        return None

    def set_case_data(self, case_data: Optional[CaseDataAccess]) -> None:
        self.case_data = case_data
        self.evidence_id = None
        self._rows = []
        self.layoutChanged.emit()

    def set_evidence(self, evidence_id: Optional[int]) -> None:
        self.evidence_id = evidence_id
        self.reload()

    def set_filter(self, name: Optional[str] = None) -> None:
        self._filter_name = name
        self.reload()

    def reload(self) -> None:
        if not self.case_data or self.evidence_id is None:
            self.beginResetModel()
            self._rows = []
            self.endResetModel()
            return

        self.beginResetModel()

        all_tags = self.case_data.list_tags(int(self.evidence_id))

        if self._filter_name:
            search = self._filter_name.lower()
            self._rows = [t for t in all_tags if search in t["name"].lower()]
        else:
            self._rows = all_tags

        self.endResetModel()

    def get_tag(self, index: QModelIndex) -> Optional[Dict[str, Any]]:
        if not index.isValid() or not (0 <= index.row() < len(self._rows)):
            return None
        return self._rows[index.row()]
