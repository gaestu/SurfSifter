"""
Reference list selection dialogs for matching against file and hash lists.
"""
from __future__ import annotations

from typing import List, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QVBoxLayout,
    QWidget,
)


class ReferenceListSelectorDialog(QDialog):
    """Dialog for selecting reference lists to match against."""

    def __init__(self, available_filelists: List[str], parent: Optional[QWidget] = None):
        """
        Initialize reference list selector dialog.

        Args:
            available_filelists: List of available file list names
            parent: Parent widget
        """
        super().__init__(parent)
        self.setWindowTitle("Select Reference Lists")
        self.selected_lists = []

        layout = QVBoxLayout()

        # Instructions
        label = QLabel("Select file lists to match against:")
        layout.addWidget(label)

        # List widget for selection
        self.list_widget = QListWidget()
        self.list_widget.setSelectionMode(QListWidget.MultiSelection)

        for name in available_filelists:
            item = QListWidgetItem(name)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked)
            self.list_widget.addItem(item)

        layout.addWidget(self.list_widget)

        # Select/Deselect all buttons
        button_layout = QHBoxLayout()
        select_all_btn = QPushButton("Select All")
        select_all_btn.clicked.connect(self._select_all)
        button_layout.addWidget(select_all_btn)

        deselect_all_btn = QPushButton("Deselect All")
        deselect_all_btn.clicked.connect(self._deselect_all)
        button_layout.addWidget(deselect_all_btn)

        layout.addLayout(button_layout)

        # OK/Cancel buttons
        dialog_buttons = QHBoxLayout()
        ok_btn = QPushButton("Match")
        ok_btn.setDefault(True)
        ok_btn.clicked.connect(self.accept)
        dialog_buttons.addWidget(ok_btn)

        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        dialog_buttons.addWidget(cancel_btn)

        layout.addLayout(dialog_buttons)
        self.setLayout(layout)
        self.resize(400, 500)

    def _select_all(self):
        """Select all reference lists."""
        for i in range(self.list_widget.count()):
            self.list_widget.item(i).setCheckState(Qt.Checked)

    def _deselect_all(self):
        """Deselect all reference lists."""
        for i in range(self.list_widget.count()):
            self.list_widget.item(i).setCheckState(Qt.Unchecked)

    def get_selected_lists(self) -> List[str]:
        """
        Get list of selected reference list names.

        Returns:
            List of selected list names
        """
        selected = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.Checked:
                selected.append(item.text())
        return selected


class HashListSelectorDialog(QDialog):
    """Dialog for selecting hash lists to check images against."""

    def __init__(self, available_hashlists: List[str], parent: Optional[QWidget] = None):
        """
        Initialize hash list selector dialog.

        Args:
            available_hashlists: List of available hash list names
            parent: Parent widget
        """
        super().__init__(parent)
        self.setWindowTitle("Check Known Hashes")
        self.selected_lists = []

        layout = QVBoxLayout()

        # Instructions
        label = QLabel(
            "Select hash lists to check against all images in this evidence.\n"
            "Matched images will be marked with the hash list name."
        )
        label.setWordWrap(True)
        layout.addWidget(label)

        # List widget for selection
        self.list_widget = QListWidget()
        self.list_widget.setSelectionMode(QListWidget.MultiSelection)

        for name in available_hashlists:
            item = QListWidgetItem(name)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked)
            self.list_widget.addItem(item)

        layout.addWidget(self.list_widget)

        # Select/Deselect all buttons
        button_layout = QHBoxLayout()
        select_all_btn = QPushButton("Select All")
        select_all_btn.clicked.connect(self._select_all)
        button_layout.addWidget(select_all_btn)

        deselect_all_btn = QPushButton("Deselect All")
        deselect_all_btn.clicked.connect(self._deselect_all)
        button_layout.addWidget(deselect_all_btn)

        layout.addLayout(button_layout)

        # Info label
        info_label = QLabel(
            "ℹ️ This will check ALL images in the current evidence against selected hash lists."
        )
        info_label.setStyleSheet("color: #666; font-style: italic;")
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        # OK/Cancel buttons
        dialog_buttons = QHBoxLayout()
        ok_btn = QPushButton("Run Check")
        ok_btn.setDefault(True)
        ok_btn.clicked.connect(self.accept)
        dialog_buttons.addWidget(ok_btn)

        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        dialog_buttons.addWidget(cancel_btn)

        layout.addLayout(dialog_buttons)
        self.setLayout(layout)
        self.resize(450, 400)

    def _select_all(self):
        """Select all hash lists."""
        for i in range(self.list_widget.count()):
            self.list_widget.item(i).setCheckState(Qt.Checked)

    def _deselect_all(self):
        """Deselect all hash lists."""
        for i in range(self.list_widget.count()):
            self.list_widget.item(i).setCheckState(Qt.Unchecked)

    def get_selected_lists(self) -> List[str]:
        """
        Get list of selected hash list names.

        Returns:
            List of selected list names
        """
        selected = []
        for i in range(self.list_widget.count()):
            item = self.list_widget.item(i)
            if item.checkState() == Qt.Checked:
                selected.append(item.text())
        return selected
