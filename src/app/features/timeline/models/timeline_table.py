"""
Timeline table model with confidence-based color coding.

Provides paginated, filterable timeline events with background coloring.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt

if TYPE_CHECKING:
    from app.data.case_data import CaseDataAccess


class TimelineTableModel(QAbstractTableModel):
    """Table model for timeline events with confidence-based color coding."""

    headers = [
        "Timestamp (UTC)",
        "Confidence",
        "Event Type",
        "Note",
        "Source",
        "Ref ID",
        "Tags",
    ]

    # Color palette for confidence levels
    CONFIDENCE_COLORS = {
        "high": (200, 255, 200),      # Light green
        "medium": (255, 255, 200),    # Light yellow
        "low": (255, 220, 220),       # Light red/pink
    }

    def __init__(self, case_data: Optional[CaseDataAccess] = None, page_size: int = 100) -> None:
        super().__init__()
        self.case_data = case_data
        self.evidence_id: Optional[int] = None
        self.page_size = page_size
        self.page = 0
        self._rows: List[Dict[str, Any]] = []
        self._filters: Dict[str, Any] = {
            "kind": None,
            "confidence": None,
            "start_date": None,
            "end_date": None,
            "tag": None,  #
        }

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

        if role in (Qt.DisplayRole, Qt.EditRole):
            if column == 0:
                # Timestamp - format nicely
                ts = row.get("ts_utc", "")
                return ts[:19] if ts else ""  # Strip microseconds
            if column == 1:
                return (row.get("confidence", "") or "").title()
            if column == 2:
                # Format kind as readable
                kind = row.get("kind", "")
                return kind.replace("_", " ").title()
            if column == 3:
                return row.get("note", "")
            if column == 4:
                return row.get("ref_table", "")
            if column == 5:
                return row.get("ref_id", "")
            if column == 6:
                return row.get("tags", "")

        elif role == Qt.BackgroundRole:
            # Color code by confidence
            confidence = (row.get("confidence", "") or "").lower()
            if confidence in self.CONFIDENCE_COLORS:
                from PySide6.QtGui import QColor
                r, g, b = self.CONFIDENCE_COLORS[confidence]
                return QColor(r, g, b)

        elif role == Qt.ToolTipRole:
            # Full details on hover
            ts = row.get("ts_utc", "")
            conf = row.get("confidence", "")
            kind = row.get("kind", "")
            note = row.get("note", "")
            source = row.get("ref_table", "")
            ref_id = row.get("ref_id", "")
            return f"{ts}\n{conf.title()}: {kind.replace('_', ' ').title()}\n{note}\n→ {source}#{ref_id}"

        elif role == Qt.UserRole:
            return row

        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.DisplayRole) -> Any:  # noqa: N802
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal and 0 <= section < len(self.headers):
            return self.headers[section]
        return None

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:  # noqa: N802
        return super().flags(index)

    def set_case_data(self, case_data: Optional[CaseDataAccess]) -> None:
        self.case_data = case_data
        self.page = 0
        self.evidence_id = None
        self._rows.clear()
        self.layoutChanged.emit()

    def set_evidence(self, evidence_id: Optional[int]) -> None:
        self.evidence_id = evidence_id
        self.page = 0
        self.reload()

    def set_filters(
        self,
        *,
        kind: Optional[str] = None,
        confidence: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        tag: Optional[str] = None,  #
    ) -> None:
        """Update filters and reload. Pass None to keep existing value."""
        if kind is not None:
            self._filters["kind"] = kind if kind else None
        if confidence is not None:
            self._filters["confidence"] = confidence if confidence else None
        if start_date is not None:
            self._filters["start_date"] = start_date if start_date else None
        if end_date is not None:
            self._filters["end_date"] = end_date if end_date else None
        if tag is not None:  #
            self._filters["tag"] = tag if tag and tag != "*" else None
        self.page = 0
        self.reload()

    def reload(self) -> None:
        if not self.case_data or self.evidence_id is None:
            self.beginResetModel()
            self._rows = []
            self.endResetModel()
            return

        self.beginResetModel()
        self._rows = self.case_data.iter_timeline(
            int(self.evidence_id),
            filters=self._filters,
            page=self.page + 1,  # DAL uses 1-indexed pages
            page_size=self.page_size,
        )
        self.endResetModel()

    def page_up(self) -> None:
        if self.page == 0:
            return
        self.page -= 1
        self.reload()

    def page_down(self) -> None:
        # Check if there's more data
        if len(self._rows) < self.page_size:
            return  # Already at the end
        self.page += 1
        self.reload()
