"""
Evidence List Widget for case-wide batch operations.

Displays all case evidences with selection checkboxes for batch
extraction and ingestion workflows.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QVBoxLayout,
    QWidget,
)


class EvidenceListWidget(QWidget):
    """
    Widget displaying all case evidences with selection checkboxes.

    Note: This widget displays evidences from list_evidences() for the UI,
    but the case-wide worker uses get_evidence(id) to fetch full data
    including partition_index for proper mounting.

    Signals:
        selection_changed(list[int]): Emitted when selection changes (list of evidence_ids)
    """

    selection_changed = Signal(list)

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._evidences: List[Dict[str, Any]] = []
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Evidence list with checkboxes
        self.evidence_list = QListWidget()
        self.evidence_list.setAlternatingRowColors(True)
        self.evidence_list.itemChanged.connect(self._on_selection_changed)
        layout.addWidget(self.evidence_list)

        # Selection buttons
        btn_layout = QHBoxLayout()
        btn_layout.setContentsMargins(0, 0, 0, 0)

        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.clicked.connect(self._select_all)
        self.select_none_btn = QPushButton("Select None")
        self.select_none_btn.clicked.connect(self._select_none)

        btn_layout.addWidget(self.select_all_btn)
        btn_layout.addWidget(self.select_none_btn)
        btn_layout.addStretch()

        layout.addLayout(btn_layout)

    def load_evidences(self, evidences: List[Dict[str, Any]]) -> None:
        """
        Load evidences from CaseDataAccess.list_evidences().

        Note: list_evidences() returns basic fields (id, label, source_path, partition_info).
        For full evidence data including partition_index, use get_evidence(id).

        Args:
            evidences: List of evidence dictionaries from list_evidences()
        """
        self._evidences = evidences
        self.evidence_list.clear()

        # Block signals during batch update
        self.evidence_list.blockSignals(True)

        for ev in evidences:
            item = QListWidgetItem()
            item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable | Qt.ItemIsSelectable)
            item.setCheckState(Qt.Checked)  # Default: all selected
            item.setData(Qt.UserRole, ev['id'])

            # Build display text
            label = ev.get('label', f"Evidence {ev['id']}")
            source = Path(ev.get('source_path', '')).name if ev.get('source_path') else ''
            partition_count = self._count_partitions(ev.get('partition_info'))

            display = f"{label}"
            if source and source != label:
                display += f" ({source})"
            if partition_count > 0:
                display += f" - {partition_count} partition(s)"

            item.setText(display)
            self.evidence_list.addItem(item)

        # Unblock signals after loading
        self.evidence_list.blockSignals(False)

        # Emit initial selection
        self._on_selection_changed()

    def get_selected_evidence_ids(self) -> List[int]:
        """
        Return list of selected evidence IDs.

        Note: Returns IDs only. Caller should use CaseDataAccess.get_evidence(id)
        to fetch full evidence data including partition_index for mounting.

        Returns:
            List of evidence IDs that are checked
        """
        selected = []
        for i in range(self.evidence_list.count()):
            item = self.evidence_list.item(i)
            if item.checkState() == Qt.Checked:
                selected.append(item.data(Qt.UserRole))
        return selected

    def get_selected_evidences(self) -> List[Dict[str, Any]]:
        """
        Return list of selected evidence dictionaries (basic data only).

        Note: For full evidence data including partition_index,
        use CaseDataAccess.get_evidence(id) for each ID from get_selected_evidence_ids().

        Returns:
            List of evidence dicts for selected items
        """
        selected_ids = set(self.get_selected_evidence_ids())
        return [ev for ev in self._evidences if ev['id'] in selected_ids]

    def evidence_count(self) -> int:
        """Return total number of evidences loaded."""
        return len(self._evidences)

    def selected_count(self) -> int:
        """Return number of selected evidences."""
        return len(self.get_selected_evidence_ids())

    def _count_partitions(self, partition_info: Optional[str]) -> int:
        """Parse partition_info JSON and return partition count."""
        if not partition_info:
            return 0
        try:
            data = json.loads(partition_info)
            if isinstance(data, list):
                return len(data)
            return 0
        except (json.JSONDecodeError, TypeError):
            return 0

    def _select_all(self) -> None:
        """Select all evidences."""
        for i in range(self.evidence_list.count()):
            self.evidence_list.item(i).setCheckState(Qt.Checked)

    def _select_none(self) -> None:
        """Deselect all evidences."""
        for i in range(self.evidence_list.count()):
            self.evidence_list.item(i).setCheckState(Qt.Unchecked)

    def _on_selection_changed(self) -> None:
        """Emit selection_changed signal with current selection."""
        self.selection_changed.emit(self.get_selected_evidence_ids())
