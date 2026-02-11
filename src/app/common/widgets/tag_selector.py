from typing import List, Optional, Set, Dict, Any
from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QListWidget, QListWidgetItem,
    QLineEdit, QLabel, QHBoxLayout
)
from app.data.case_data import CaseDataAccess

class TagSelectorWidget(QWidget):
    selectionChanged = Signal()

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.case_data: Optional[CaseDataAccess] = None
        self.evidence_id: Optional[int] = None
        self._all_tags: List[Dict[str, Any]] = []

        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Filter
        self.filter_edit = QLineEdit()
        self.filter_edit.setPlaceholderText("Filter tags...")
        self.filter_edit.textChanged.connect(self._filter_tags)
        layout.addWidget(self.filter_edit)

        # List
        self.list_widget = QListWidget()
        self.list_widget.setSelectionMode(QListWidget.NoSelection) # We use checkboxes
        self.list_widget.itemChanged.connect(self._on_item_changed)
        layout.addWidget(self.list_widget)

    def set_data(self, case_data: CaseDataAccess, evidence_id: int, selected_tag_ids: Optional[List[int]] = None):
        self.case_data = case_data
        self.evidence_id = evidence_id
        self._load_tags(selected_tag_ids or [])

    def _load_tags(self, selected_ids: List[int]):
        if not self.case_data or self.evidence_id is None:
            return

        self.list_widget.clear()
        self._all_tags = self.case_data.list_tags(self.evidence_id)

        selected_set = set(selected_ids)

        for tag in self._all_tags:
            item = QListWidgetItem(tag["name"])
            item.setData(Qt.UserRole, tag["id"])
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)

            if tag["id"] in selected_set:
                item.setCheckState(Qt.Checked)
            else:
                item.setCheckState(Qt.Unchecked)

            self.list_widget.addItem(item)

    def get_selected_tag_ids(self) -> List[int]:
        selected = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.Checked:
                selected.append(item.data(Qt.UserRole))
        return selected

    def _filter_tags(self, text: str):
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            item.setHidden(text.lower() not in item.text().lower())

    def _on_item_changed(self, item):
        self.selectionChanged.emit()
