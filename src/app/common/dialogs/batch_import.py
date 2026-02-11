"""
Batch hash list import dialogs.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QPalette
from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QVBoxLayout,
    QWidget,
)


class BatchHashListImportDialog(QDialog):
    """
    Preview and configure batch hash list import.

    Shows a list of files to import with conflict detection,
    allows user to select which files to import and configure
    conflict handling policy.
    """

    def __init__(
        self,
        files: List[Path],
        existing_names: set,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Batch Import Hash Lists")
        self.setMinimumSize(600, 500)

        self.files = files
        self.existing_names = existing_names
        self._conflict_count = 0

        self._build_ui()
        self._populate_file_list()
        self._update_import_button()

    def _build_ui(self) -> None:
        layout = QVBoxLayout()

        # Header
        self.header_label = QLabel(f"Found {len(self.files)} hash list files")
        self.header_label.setStyleSheet("font-weight: bold; font-size: 14px;")
        layout.addWidget(self.header_label)

        # Conflict info
        self.conflict_label = QLabel()
        self.conflict_label.setStyleSheet("color: #c62828;")  # Red
        layout.addWidget(self.conflict_label)

        # Select All / Deselect All buttons
        btn_layout = QHBoxLayout()
        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.clicked.connect(self._select_all)
        btn_layout.addWidget(self.select_all_btn)

        self.deselect_all_btn = QPushButton("Deselect All")
        self.deselect_all_btn.clicked.connect(self._deselect_all)
        btn_layout.addWidget(self.deselect_all_btn)

        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        # File list
        self.file_list = QListWidget()
        self.file_list.setSelectionMode(QListWidget.NoSelection)
        self.file_list.itemChanged.connect(self._on_item_changed)
        layout.addWidget(self.file_list, 1)

        # Conflict policy group
        policy_group = QGroupBox("If file exists:")
        policy_layout = QHBoxLayout()

        self.policy_skip = QRadioButton("Skip")
        self.policy_skip.setChecked(True)
        self.policy_skip.setToolTip("Skip files that already exist")
        policy_layout.addWidget(self.policy_skip)

        self.policy_overwrite = QRadioButton("Overwrite")
        self.policy_overwrite.setToolTip("Replace existing files")
        policy_layout.addWidget(self.policy_overwrite)

        self.policy_rename = QRadioButton("Rename (_1, _2)")
        self.policy_rename.setToolTip("Create new file with numbered suffix")
        policy_layout.addWidget(self.policy_rename)

        policy_layout.addStretch()
        policy_group.setLayout(policy_layout)
        layout.addWidget(policy_group)

        # Rebuild hash DB checkbox
        self.rebuild_checkbox = QCheckBox("Rebuild hash database after import")
        self.rebuild_checkbox.setChecked(True)
        self.rebuild_checkbox.setToolTip(
            "Rebuild the SQLite hash database for fast matching.\n"
            "Required for hash matching to work with new lists."
        )
        layout.addWidget(self.rebuild_checkbox)

        # Dialog buttons
        button_box = QDialogButtonBox()
        self.cancel_btn = button_box.addButton(QDialogButtonBox.Cancel)
        self.import_btn = button_box.addButton("Import 0 Files", QDialogButtonBox.AcceptRole)
        self.import_btn.setEnabled(False)

        button_box.rejected.connect(self.reject)
        button_box.accepted.connect(self.accept)

        layout.addWidget(button_box)
        self.setLayout(layout)

    def _populate_file_list(self) -> None:
        """Populate the file list with checkboxes and conflict indicators."""
        self._conflict_count = 0

        for file_path in self.files:
            name = file_path.stem
            size_kb = file_path.stat().st_size / 1024

            # Check for conflict
            is_conflict = name in self.existing_names
            if is_conflict:
                self._conflict_count += 1
                display_text = f"⚠ {file_path.name}  →  {name}    EXISTS ({size_kb:.1f} KB)"
            else:
                display_text = f"    {file_path.name}  →  {name}    ({size_kb:.1f} KB)"

            item = QListWidgetItem(display_text)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked)
            item.setData(Qt.UserRole, file_path)

            # Style conflicts differently
            if is_conflict:
                item.setForeground(QPalette().color(QPalette.Link))  # Use link color

            self.file_list.addItem(item)

        # Update conflict label
        if self._conflict_count > 0:
            self.conflict_label.setText(f"⚠ {self._conflict_count} file(s) already exist")
        else:
            self.conflict_label.setText("")

    def _select_all(self) -> None:
        """Check all items."""
        for i in range(self.file_list.count()):
            item = self.file_list.item(i)
            item.setCheckState(Qt.Checked)
        self._update_import_button()

    def _deselect_all(self) -> None:
        """Uncheck all items."""
        for i in range(self.file_list.count()):
            item = self.file_list.item(i)
            item.setCheckState(Qt.Unchecked)
        self._update_import_button()

    def _on_item_changed(self, item: QListWidgetItem) -> None:
        """Handle item checkbox change."""
        self._update_import_button()

    def _update_import_button(self) -> None:
        """Update import button text and enabled state."""
        count = self._get_selected_count()
        self.import_btn.setText(f"Import {count} File{'s' if count != 1 else ''}")
        self.import_btn.setEnabled(count > 0)

    def _get_selected_count(self) -> int:
        """Count selected items."""
        count = 0
        for i in range(self.file_list.count()):
            item = self.file_list.item(i)
            if item.checkState() == Qt.Checked:
                count += 1
        return count

    def get_selected_files(self) -> List[Path]:
        """Get list of selected file paths."""
        selected = []
        for i in range(self.file_list.count()):
            item = self.file_list.item(i)
            if item.checkState() == Qt.Checked:
                selected.append(item.data(Qt.UserRole))
        return selected

    def get_conflict_policy(self) -> str:
        """Get selected conflict policy as string."""
        if self.policy_overwrite.isChecked():
            return "overwrite"
        elif self.policy_rename.isChecked():
            return "rename"
        return "skip"

    def should_rebuild_db(self) -> bool:
        """Check if hash DB should be rebuilt after import."""
        return self.rebuild_checkbox.isChecked()


class BatchImportProgressDialog(QDialog):
    """
    Modal progress dialog during batch import.

    Shows progress bar, current file name, and cancel button.
    """

    cancelled = Signal()

    def __init__(
        self,
        total_files: int,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Importing Hash Lists")
        self.setMinimumWidth(400)
        self.setModal(True)
        # Prevent closing via X button during import
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowCloseButtonHint)

        self.total_files = total_files
        self._is_cancelled = False

        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout()

        # Status label
        self.status_label = QLabel("Preparing import...")
        layout.addWidget(self.status_label)

        # Progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, self.total_files)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        # File counter label
        self.counter_label = QLabel(f"0 / {self.total_files}")
        self.counter_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.counter_label)

        # Cancel button
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self._on_cancel)
        btn_layout.addWidget(self.cancel_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        self.setLayout(layout)

    def update_progress(self, current: int, filename: str) -> None:
        """Update progress display."""
        self.progress_bar.setValue(current)
        self.counter_label.setText(f"{current} / {self.total_files}")
        self.status_label.setText(f"Importing: {filename}")

    def set_complete(self) -> None:
        """Mark import as complete."""
        self.progress_bar.setValue(self.total_files)
        self.status_label.setText("Import complete!")
        self.cancel_btn.setText("Close")
        self.cancel_btn.clicked.disconnect()
        self.cancel_btn.clicked.connect(self.accept)
        # Re-enable close button
        self.setWindowFlags(self.windowFlags() | Qt.WindowCloseButtonHint)
        self.show()  # Re-show to apply flag change

    def _on_cancel(self) -> None:
        """Handle cancel button click."""
        if not self._is_cancelled:
            self._is_cancelled = True
            self.cancel_btn.setEnabled(False)
            self.cancel_btn.setText("Cancelling...")
            self.status_label.setText("Cancelling import...")
            self.cancelled.emit()

    def is_cancelled(self) -> bool:
        """Check if import was cancelled."""
        return self._is_cancelled
