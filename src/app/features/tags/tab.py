from __future__ import annotations

from typing import Optional, Dict, Any, List

from PySide6.QtCore import Qt, Signal, Slot
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableView,
    QComboBox,
    QMessageBox,
    QHeaderView,
    QAbstractItemView,
    QDialog,
    QFormLayout,
    QDialogButtonBox,
    QMenu,
)
from PySide6.QtGui import QAction

from app.data.case_data import CaseDataAccess
from app.features.tags.models import TagsTableModel


class TagsTab(QWidget):
    """
    Tags management tab.
    Allows viewing, creating, renaming, deleting, and merging tags.
    """

    def __init__(self, case_data: Optional[CaseDataAccess] = None, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.case_data = case_data
        self.evidence_id: Optional[int] = None

        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)

        # Toolbar
        toolbar_layout = QHBoxLayout()

        # Search
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search tags...")
        self.search_edit.textChanged.connect(self._on_search_changed)
        toolbar_layout.addWidget(self.search_edit)

        # Actions
        self.create_btn = QPushButton("Create Tag")
        self.create_btn.clicked.connect(self._create_tag)
        toolbar_layout.addWidget(self.create_btn)

        self.merge_btn = QPushButton("Merge Selected...")
        self.merge_btn.clicked.connect(self._merge_tags)
        self.merge_btn.setEnabled(False)
        toolbar_layout.addWidget(self.merge_btn)

        self.delete_btn = QPushButton("Delete Selected")
        self.delete_btn.clicked.connect(self._delete_tags)
        self.delete_btn.setEnabled(False)
        toolbar_layout.addWidget(self.delete_btn)

        toolbar_layout.addStretch()
        layout.addLayout(toolbar_layout)

        # Table
        self.model = TagsTableModel(self.case_data)
        self.table_view = QTableView()
        self.table_view.setModel(self.model)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table_view.setSortingEnabled(True)
        self.table_view.selectionModel().selectionChanged.connect(self._on_selection_changed)
        self.table_view.doubleClicked.connect(self._on_double_click)

        # Context Menu
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._show_context_menu)

        layout.addWidget(self.table_view)

        # Details Panel (Placeholder for now, maybe expand later)
        self.details_label = QLabel()
        layout.addWidget(self.details_label)

    def set_case_data(self, case_data: Optional[CaseDataAccess]) -> None:
        self.case_data = case_data
        self.model.set_case_data(case_data)
        self._refresh()

    def set_evidence(self, evidence_id: Optional[int]) -> None:
        self.evidence_id = evidence_id
        self.model.set_evidence(evidence_id)
        self._refresh()

    def _refresh(self) -> None:
        self.model.reload()
        self._update_buttons()

    def showEvent(self, event):
        """Override showEvent to refresh tags when tab becomes visible."""
        super().showEvent(event)
        self._refresh()

    def _on_search_changed(self, text: str) -> None:
        self.model.set_filter(text)

    def _on_selection_changed(self) -> None:
        self._update_buttons()

    def _update_buttons(self) -> None:
        selected = self.table_view.selectionModel().selectedRows()
        count = len(selected)
        self.merge_btn.setEnabled(count >= 2)
        self.delete_btn.setEnabled(count >= 1)

        if count == 1:
            tag = self.model.get_tag(selected[0])
            if tag:
                self.details_label.setText(
                    f"Selected: {tag['name']} (Used {tag['usage_count']} times)"
                )
        else:
            self.details_label.setText(
                f"{count} tags selected"
            )

    def _create_tag(self) -> None:
        if not self.evidence_id:
            return

        dialog = CreateTagDialog(self)
        if dialog.exec() == QDialog.Accepted:
            name = dialog.get_name()
            if name:
                try:
                    self.case_data.create_tag(self.evidence_id, name)
                    self._refresh()
                except Exception as e:
                    QMessageBox.critical(self, "Error", str(e))

    def _rename_tag(self, index) -> None:
        if not self.evidence_id:
            return

        tag = self.model.get_tag(index)
        if not tag:
            return

        dialog = RenameTagDialog(tag['name'], self)
        if dialog.exec() == QDialog.Accepted:
            new_name = dialog.get_name()
            if new_name and new_name != tag['name']:
                try:
                    self.case_data.rename_tag(self.evidence_id, tag['id'], new_name)
                    self._refresh()
                except Exception as e:
                    QMessageBox.critical(self, "Error", str(e))

    def _delete_tags(self) -> None:
        if not self.evidence_id:
            return

        selected = self.table_view.selectionModel().selectedRows()
        if not selected:
            return

        count = len(selected)
        reply = QMessageBox.question(
            self,
            "Confirm Delete",
            f"Are you sure you want to delete {count} tags? This will remove the tag from all artifacts.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )

        if reply == QMessageBox.Yes:
            try:
                for index in selected:
                    tag = self.model.get_tag(index)
                    if tag:
                        self.case_data.delete_tag(self.evidence_id, tag['id'])
                self._refresh()
            except Exception as e:
                QMessageBox.critical(self, "Error", str(e))

    def _merge_tags(self) -> None:
        if not self.evidence_id:
            return

        selected = self.table_view.selectionModel().selectedRows()
        if len(selected) < 2:
            return

        tags = [self.model.get_tag(idx) for idx in selected]
        tags = [t for t in tags if t] # Filter None

        if len(tags) < 2:
            return

        dialog = MergeTagsDialog(tags, self)
        if dialog.exec() == QDialog.Accepted:
            target_tag_id = dialog.get_target_tag_id()
            if target_tag_id:
                source_ids = [t['id'] for t in tags if t['id'] != target_tag_id]
                try:
                    self.case_data.merge_tags(self.evidence_id, source_ids, target_tag_id)
                    self._refresh()
                except Exception as e:
                    QMessageBox.critical(self, "Error", str(e))

    def _on_double_click(self, index) -> None:
        self._rename_tag(index)

    def _show_context_menu(self, pos) -> None:
        index = self.table_view.indexAt(pos)
        if not index.isValid():
            return

        menu = QMenu(self)
        rename_action = QAction("Rename...", self)
        rename_action.triggered.connect(lambda: self._rename_tag(index))
        menu.addAction(rename_action)

        delete_action = QAction("Delete", self)
        delete_action.triggered.connect(self._delete_tags)
        menu.addAction(delete_action)

        selected = self.table_view.selectionModel().selectedRows()
        if len(selected) >= 2:
            merge_action = QAction("Merge Selected...", self)
            merge_action.triggered.connect(self._merge_tags)
            menu.addAction(merge_action)

        menu.exec(self.table_view.viewport().mapToGlobal(pos))


class CreateTagDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Create Tag")

        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.name_edit = QLineEdit()
        form.addRow("Tag Name:", self.name_edit)
        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_name(self) -> str:
        return self.name_edit.text().strip()

class RenameTagDialog(QDialog):
    def __init__(self, current_name: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Rename Tag")

        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.name_edit = QLineEdit(current_name)
        form.addRow("New Name:", self.name_edit)
        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_name(self) -> str:
        return self.name_edit.text().strip()

class MergeTagsDialog(QDialog):
    def __init__(self, tags: List[Dict[str, Any]], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Merge Tags")
        self.tags = tags

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Select the target tag (others will be merged into it):"))

        self.combo = QComboBox()
        for tag in tags:
            self.combo.addItem(f"{tag['name']} ({tag['usage_count']} uses)", userData=tag['id'])

        layout.addWidget(self.combo)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_target_tag_id(self) -> int:
        return self.combo.currentData()
