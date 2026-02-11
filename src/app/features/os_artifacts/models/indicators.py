"""
Indicators table model for registry/system artifact indicators.

Displays OS-level detection indicators with filtering support.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess


class IndicatorsTableModel(QAbstractTableModel):
    headers = [
        "Type",
        "Name",
        "Value",
        "Path",
        "Hive",
        "Confidence",
        "Detected",
        "Provenance",
    ]

    def __init__(self, case_data: Optional[CaseDataAccess] = None) -> None:
        super().__init__()
        self.case_data = case_data
        self.evidence_id: Optional[int] = None
        self._rows: List[Dict[str, Any]] = []
        self._filter_type: Optional[str] = None

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
        if role == Qt.DisplayRole:
            mapping = [
                "type",
                "name",
                "value",
                "path",
                "hive",
                "confidence",
                "detected_at_utc",
                "provenance",
            ]
            key = mapping[index.column()]
            return row.get(key, "")
        if role == Qt.UserRole:
            return row
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self.headers):
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

    def set_filter_type(self, indicator_type: Optional[str]) -> None:
        self._filter_type = indicator_type if indicator_type else None
        self.reload()

    def reload(self) -> None:
        if not self.case_data or self.evidence_id is None:
            self.beginResetModel()
            self._rows = []
            self.endResetModel()
            return
        self.beginResetModel()
        self._rows = self.case_data.iter_indicators(int(self.evidence_id), indicator_type=self._filter_type)
        self.endResetModel()
