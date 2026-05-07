"""
Batch hash list import dialogs.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QPalette
from PySide6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QTextEdit,
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
        *,
        list_label: str = "hash list",
        window_title: str = "Batch Import Hash Lists",
        show_rebuild_db: bool = True,
        show_url_metadata: bool = False,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(window_title)
        self.setMinimumSize(600, 500)

        self.files = files
        self.existing_names = existing_names
        self.list_label = list_label
        self.show_rebuild_db = show_rebuild_db
        self.show_url_metadata = show_url_metadata
        self._conflict_count = 0

        self._build_ui()
        self._populate_file_list()
        self._update_import_button()

    def _build_ui(self) -> None:
        layout = QVBoxLayout()

        # Header
        self.header_label = QLabel(f"Found {len(self.files)} {self.list_label} files")
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

        if self.show_url_metadata:
            metadata_group = QGroupBox("Shared URL List Metadata")
            form_layout = QFormLayout()

            category_layout = QHBoxLayout()
            self.category_combo = QComboBox()
            self.category_combo.addItems([
                "Gambling",
                "Gaming",
                "Social Media",
                "Adult Content",
                "Malicious",
                "Excluded",
                "Custom",
            ])
            self.category_combo.setCurrentText("Custom")
            self.category_combo.currentTextChanged.connect(self._on_category_changed)
            category_layout.addWidget(self.category_combo)

            self.custom_category_edit = QLineEdit()
            self.custom_category_edit.setPlaceholderText("Enter custom category")
            category_layout.addWidget(self.custom_category_edit)
            form_layout.addRow("Category:", category_layout)

            self.description_edit = QTextEdit()
            self.description_edit.setPlaceholderText(
                "Enter a description for generated URL-list metadata"
            )
            self.description_edit.setMaximumHeight(70)
            form_layout.addRow("Description:", self.description_edit)

            pattern_group = QGroupBox("Pattern Type")
            pattern_layout = QVBoxLayout()
            self.wildcard_radio = QRadioButton("Wildcard Patterns (* and ? supported)")
            self.wildcard_radio.setChecked(True)
            pattern_layout.addWidget(self.wildcard_radio)
            self.regex_radio = QRadioButton("Regular Expressions")
            pattern_layout.addWidget(self.regex_radio)
            self.pattern_button_group = QButtonGroup(self)
            self.pattern_button_group.addButton(self.wildcard_radio, 0)
            self.pattern_button_group.addButton(self.regex_radio, 1)
            pattern_group.setLayout(pattern_layout)

            metadata_layout = QVBoxLayout()
            metadata_layout.addLayout(form_layout)
            metadata_layout.addWidget(pattern_group)
            metadata_group.setLayout(metadata_layout)
            layout.addWidget(metadata_group)

        # Rebuild hash DB checkbox
        if self.show_rebuild_db:
            self.rebuild_checkbox = QCheckBox("Rebuild hash database after import")
            self.rebuild_checkbox.setChecked(True)
            self.rebuild_checkbox.setToolTip(
                "Rebuild the SQLite hash database for fast matching.\n"
                "Required for hash matching to work with new lists."
            )
            layout.addWidget(self.rebuild_checkbox)
        else:
            self.rebuild_checkbox = None

        # Dialog buttons
        button_box = QDialogButtonBox()
        self.cancel_btn = button_box.addButton(QDialogButtonBox.Cancel)
        self.import_btn = button_box.addButton("Import 0 Files", QDialogButtonBox.AcceptRole)
        self.import_btn.setEnabled(False)

        button_box.rejected.connect(self.reject)
        button_box.accepted.connect(self._on_accept)

        layout.addWidget(button_box)
        self.setLayout(layout)

    def _populate_file_list(self) -> None:
        """Populate the file list with checkboxes and conflict indicators."""
        self._conflict_count = 0

        for file_path in self.files:
            name = file_path.stem
            try:
                size_text = f"{file_path.lstat().st_size / 1024:.1f} KB"
            except OSError:
                size_text = "size unavailable"

            # Check for conflict
            is_conflict = name in self.existing_names
            if is_conflict:
                self._conflict_count += 1
                display_text = f"⚠ {file_path.name}  →  {name}    EXISTS ({size_text})"
            else:
                display_text = f"    {file_path.name}  →  {name}    ({size_text})"

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

    def _on_category_changed(self, category: str) -> None:
        """Toggle custom category entry visibility."""
        if not self.show_url_metadata:
            return
        is_custom = category == "Custom"
        self.custom_category_edit.setVisible(is_custom)
        if is_custom:
            self.custom_category_edit.setFocus()

    def _on_accept(self) -> None:
        """Validate URL metadata when present, then accept."""
        if self.show_url_metadata:
            if not self.get_category():
                QMessageBox.warning(
                    self,
                    "Validation Error",
                    "Category is required.",
                )
                self.custom_category_edit.setFocus()
                return

            if not self.get_description():
                QMessageBox.warning(
                    self,
                    "Validation Error",
                    "Description is required.",
                )
                self.description_edit.setFocus()
                return

        self.accept()

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
        return bool(self.rebuild_checkbox and self.rebuild_checkbox.isChecked())

    def get_category(self) -> str:
        """Get the shared URL-list category."""
        if not self.show_url_metadata:
            return ""
        category = self.category_combo.currentText()
        if category == "Custom":
            return self.custom_category_edit.text().strip()
        return category

    def get_description(self) -> str:
        """Get the shared URL-list description."""
        if not self.show_url_metadata:
            return ""
        return self.description_edit.toPlainText().strip()

    def is_regex(self) -> bool:
        """Return True when URL-list regex mode is selected."""
        return bool(self.show_url_metadata and self.regex_radio.isChecked())


class BatchUrlListImportDialog(BatchHashListImportDialog):
    """Preview and configure batch URL list import."""

    def __init__(
        self,
        files: List[Path],
        existing_names: set,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(
            files,
            existing_names,
            parent,
            list_label="URL list",
            window_title="Batch Import URL Lists",
            show_rebuild_db=False,
            show_url_metadata=True,
        )


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
        *,
        title: str = "Importing Hash Lists",
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
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
