"""
Case management dialogs - partition selection and case creation.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class PartitionSelectionDialog(QDialog):
    """Dialog for selecting partitions from an E01 disk image."""

    def __init__(self, partitions: List[Dict[str, Any]], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.partitions = partitions
        self.selected_partitions: List[int] = []

        self.setWindowTitle("Select Partitions")
        self.resize(700, 400)

        layout = QVBoxLayout()

        # Info label
        info_label = QLabel(
            "The E01 image contains multiple partitions. "
            "Select which partition(s) to add as evidence:"
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        # Partition table
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels([
            "Partition",
            "Size",
            "Type",
            "Filesystem",
            "Files"
        ])
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.MultiSelection)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)

        # Populate table
        self.table.setRowCount(len(partitions))
        for row, part in enumerate(partitions):
            # Partition index
            part_item = QTableWidgetItem(str(part['index']))
            part_item.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(row, 0, part_item)

            # Size (in GB)
            size_gb = part['length'] / (1024 ** 3)
            size_item = QTableWidgetItem(f"{size_gb:.2f} GB")
            size_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.table.setItem(row, 1, size_item)

            # Type/Description
            type_item = QTableWidgetItem(part['description'])
            self.table.setItem(row, 2, type_item)

            # Filesystem status
            fs_status = "Readable" if part['filesystem_readable'] else "Unreadable"
            fs_item = QTableWidgetItem(fs_status)
            fs_item.setTextAlignment(Qt.AlignCenter)
            if not part['filesystem_readable']:
                fs_item.setForeground(Qt.red)
            self.table.setItem(row, 3, fs_item)

            # Root file count
            file_count = part.get('root_file_count')
            files_text = str(file_count) if file_count is not None else "—"
            files_item = QTableWidgetItem(files_text)
            files_item.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(row, 4, files_item)

        # Auto-select largest readable partition
        self._auto_select_best_partition()

        layout.addWidget(self.table)

        # Buttons
        button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self.setLayout(layout)

    def _auto_select_best_partition(self) -> None:
        """Auto-select the largest readable partition (likely Windows system partition)."""
        readable_partitions = [
            (i, p) for i, p in enumerate(self.partitions)
            if p['filesystem_readable']
        ]

        if readable_partitions:
            # Select largest readable partition
            largest_idx, _ = max(readable_partitions, key=lambda x: x[1]['length'])
            self.table.selectRow(largest_idx)

    def accept(self) -> None:
        """Store selected partition indices and close dialog."""
        selected_rows = {item.row() for item in self.table.selectedItems()}
        self.selected_partitions = [
            self.partitions[row]['index']
            for row in sorted(selected_rows)
        ]

        if not self.selected_partitions:
            QMessageBox.warning(
                self,
                "No Selection",
                "Please select at least one partition."
            )
            return

        super().accept()

    def get_selected_partitions(self) -> List[int]:
        """Return list of selected partition indices."""
        return self.selected_partitions


class CreateCaseDialog(QDialog):
    """Dialog for creating a new forensic case with metadata."""

    # Invalid characters for filenames on Windows and Linux
    INVALID_CHARS_PATTERN = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

    def __init__(self, parent: Optional[QWidget] = None, base_dir: Optional[Path] = None) -> None:
        super().__init__(parent)
        self.base_dir = base_dir or Path.home()
        self.destination_folder: Optional[Path] = None
        self.case_folder: Optional[Path] = None

        self.setWindowTitle("Create New Case")
        self.setModal(True)
        self.resize(600, 400)

        self._setup_ui()
        self._connect_signals()

    def _setup_ui(self) -> None:
        """Build the dialog UI."""
        layout = QVBoxLayout()

        # Form for case metadata
        form_layout = QFormLayout()

        # Case Number (mandatory)
        self.case_number_edit = QLineEdit()
        self.case_number_edit.setPlaceholderText("e.g., CASE-2026-001")
        case_number_label = QLabel("Case Number:" + " *")
        form_layout.addRow(case_number_label, self.case_number_edit)

        # Case Name (optional)
        self.case_name_edit = QLineEdit()
        self.case_name_edit.setPlaceholderText("e.g., State v. Smith")
        form_layout.addRow("Case Name:", self.case_name_edit)

        # Investigator (optional)
        self.investigator_edit = QLineEdit()
        self.investigator_edit.setPlaceholderText("e.g., Detective John Doe")
        form_layout.addRow("Investigator:", self.investigator_edit)

        # Notes (optional)
        self.notes_edit = QTextEdit()
        self.notes_edit.setPlaceholderText("Investigation notes, legal authority, etc.")
        self.notes_edit.setMaximumHeight(100)
        form_layout.addRow("Notes:", self.notes_edit)

        layout.addLayout(form_layout)

        # Destination folder selection
        dest_layout = QHBoxLayout()
        dest_label = QLabel("Destination Folder:" + " *")
        self.destination_edit = QLineEdit()
        self.destination_edit.setReadOnly(True)
        self.destination_edit.setPlaceholderText("Select where to create the case folder")
        self.browse_button = QPushButton("Select Folder")
        dest_layout.addWidget(self.destination_edit, 1)
        dest_layout.addWidget(self.browse_button)

        layout.addWidget(dest_label)
        layout.addLayout(dest_layout)

        # Info label showing where case will be created
        self.info_label = QLabel()
        self.info_label.setWordWrap(True)
        self.info_label.setStyleSheet("color: gray; font-style: italic;")
        layout.addWidget(self.info_label)

        layout.addStretch()

        # Mandatory field note
        mandatory_note = QLabel("* Required fields")
        mandatory_note.setStyleSheet("color: gray; font-style: italic;")
        layout.addWidget(mandatory_note)

        # Buttons
        self.button_box = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.button_box.button(QDialogButtonBox.Ok).setText("Create Case")
        self.button_box.button(QDialogButtonBox.Ok).setEnabled(False)  # Disabled until valid
        layout.addWidget(self.button_box)

        self.setLayout(layout)

    def _connect_signals(self) -> None:
        """Connect widget signals."""
        self.case_number_edit.textChanged.connect(self._on_case_number_changed)
        self.browse_button.clicked.connect(self._browse_destination)
        self.button_box.accepted.connect(self._on_accept)
        self.button_box.rejected.connect(self.reject)

    def _on_case_number_changed(self, text: str) -> None:
        """Validate case number in real-time and update UI."""
        is_valid, error_msg = self._validate_case_number(text)

        # Update visual feedback
        palette = self.case_number_edit.palette()
        if text and not is_valid:
            # Show red border for invalid input
            palette.setColor(QPalette.Base, QColor(Qt.red).lighter(180))
            self.case_number_edit.setToolTip(error_msg)
        else:
            # Reset to default
            palette.setColor(QPalette.Base, QColor(Qt.white))
            self.case_number_edit.setToolTip("")
        self.case_number_edit.setPalette(palette)

        # Update info label and enable/disable OK button
        self._update_info_label()
        self._update_ok_button()

    def _validate_case_number(self, case_number: str) -> tuple[bool, str]:
        """
        Validate case number for filesystem safety.
        Returns (is_valid, error_message).
        """
        if not case_number:
            return False, "Case number is required"

        # Check for invalid characters
        if self.INVALID_CHARS_PATTERN.search(case_number):
            return False, "Case number contains invalid characters (< > : \" / \\ | ? * or control characters)"

        # Check for spaces
        if ' ' in case_number:
            return False, "Case number cannot contain spaces"

        # Check length (filesystem limits)
        if len(case_number) > 200:  # Conservative limit
            return False, "Case number is too long (max 200 characters)"

        return True, ""

    def _browse_destination(self) -> None:
        """Open folder browser to select destination."""
        folder = QFileDialog.getExistingDirectory(
            self,
            "Select Destination Folder",
            str(self.destination_folder or self.base_dir),
        )
        if folder:
            self.destination_folder = Path(folder)
            self.destination_edit.setText(str(self.destination_folder))
            self._update_info_label()
            self._update_ok_button()

    def _update_info_label(self) -> None:
        """Update the info label showing where case will be created."""
        case_number = self.case_number_edit.text().strip()

        if not case_number or not self.destination_folder:
            self.info_label.setText("")
            self.case_folder = None
            return

        is_valid, _ = self._validate_case_number(case_number)
        if not is_valid:
            self.info_label.setText("")
            self.case_folder = None
            return

        # Generate folder name
        folder_name = f"{case_number}_browser_analyzing"
        self.case_folder = self.destination_folder / folder_name

        self.info_label.setText(
            f"Case will be created at:\n{str(self.case_folder)}"
        )

    def _update_ok_button(self) -> None:
        """Enable OK button only if required fields are valid."""
        case_number = self.case_number_edit.text().strip()
        is_valid, _ = self._validate_case_number(case_number)

        can_create = (
            is_valid and
            self.destination_folder is not None and
            self.case_folder is not None
        )

        self.button_box.button(QDialogButtonBox.Ok).setEnabled(can_create)

    def _on_accept(self) -> None:
        """Validate and accept dialog."""
        case_number = self.case_number_edit.text().strip()

        # Final validation
        is_valid, error_msg = self._validate_case_number(case_number)
        if not is_valid:
            QMessageBox.warning(
                self,
                "Invalid Case Number",
                error_msg
            )
            return

        if not self.destination_folder:
            QMessageBox.warning(
                self,
                "No Destination",
                "Please select a destination folder."
            )
            return

        # Check if case folder already exists
        if self.case_folder and self.case_folder.exists():
            # Ask if user wants to open existing case
            reply = QMessageBox.question(
                self,
                "Case Already Exists",
                f"A case folder with this name already exists:\n{self.case_folder}\n\n"
                "Do you want to open the existing case instead?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )

            if reply == QMessageBox.Yes:
                # User wants to open existing case - accept dialog
                super().accept()
            else:
                # User doesn't want to open or create - stay in dialog
                return
        else:
            # Case folder doesn't exist - proceed with creation
            super().accept()

    def get_case_data(self) -> Dict[str, Any]:
        """Return the entered case data."""
        case_number = self.case_number_edit.text().strip()

        from core.database import CASE_DB_SUFFIX
        return {
            'case_number': case_number,
            'case_name': self.case_name_edit.text().strip() or case_number,
            'investigator': self.investigator_edit.text().strip(),
            'notes': self.notes_edit.toPlainText().strip(),
            'case_folder': self.case_folder,
            'db_filename': f"{case_number}{CASE_DB_SUFFIX}",
        }
