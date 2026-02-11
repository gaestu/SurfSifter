"""
Export and Import dialogs for case packages.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMessageBox,
    QProgressDialog,
    QPushButton,
    QRadioButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from .utils import show_error_dialog


class SizeEstimateWorker(QThread):
    """Background worker for estimating export size."""

    finished = Signal(object)  # Total size in bytes (object to avoid 32-bit overflow)
    error = Signal(str)        # Error message
    progress = Signal(object, object)  # current, total (object to avoid 32-bit overflow)

    def __init__(self, case_folder: Path, options: Any) -> None:
        super().__init__()
        self.case_folder = case_folder
        self.options = options

    def run(self) -> None:
        try:
            from core.export import estimate_export_size

            size = estimate_export_size(
                self.case_folder,
                self.options,
                progress_callback=lambda cur, tot: self.progress.emit(cur, tot)
            )
            self.finished.emit(size)
        except Exception as exc:
            self.error.emit(str(exc))


class ExportWorker(QThread):
    """Background worker for creating export package."""

    finished = Signal(object)  # ExportResult
    error = Signal(str)        # Error message
    progress = Signal(object, object, str)  # current_bytes, total_bytes, filename (object to avoid 32-bit overflow)
    cancelled = Signal()       # Emitted when cancelled

    def __init__(self, case_folder: Path, dest_path: Path, options: Any) -> None:
        super().__init__()
        self.case_folder = case_folder
        self.dest_path = dest_path
        self.options = options
        self._cancel_requested = False

    def request_cancel(self) -> None:
        """Request cancellation (cooperative)."""
        self._cancel_requested = True

    def run(self) -> None:
        try:
            from core.export import create_export_package_cancellable

            result = create_export_package_cancellable(
                self.case_folder,
                self.dest_path,
                self.options,
                cancel_check=lambda: self._cancel_requested,
                progress_callback=lambda cur, tot, file: self.progress.emit(cur, tot, file)
            )

            # Check result for cancellation
            if result.error_message == "Cancelled by user":
                self.cancelled.emit()
            else:
                self.finished.emit(result)
        except Exception as exc:
            self.error.emit(str(exc))


class ExportDialog(QDialog):
    """Dialog for exporting case packages."""

    def __init__(self, case_folder: Path, case_id: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.case_folder = case_folder
        self.case_id = case_id
        self.size_estimate = 0
        self.estimating = False

        self.setWindowTitle("Export Case")
        self.resize(600, 400)

        layout = QVBoxLayout()

        # Header
        header = QLabel(f"<h2>Export Case: {case_id}</h2>")
        layout.addWidget(header)

        info = QLabel(
            "Create a ZIP archive of this case for archival, sharing, or migration. "
            "Select which artifacts to include in the export package."
        )
        info.setWordWrap(True)
        layout.addWidget(info)

        layout.addSpacing(20)

        # Destination selection
        dest_group = QGroupBox("Destination")
        dest_layout = QVBoxLayout()

        dest_row = QHBoxLayout()
        self.dest_edit = QLineEdit()
        self.dest_edit.setPlaceholderText("Select destination for ZIP file...")
        dest_row.addWidget(self.dest_edit)

        self.browse_button = QPushButton("Browse...")
        self.browse_button.clicked.connect(self._on_browse_clicked)
        dest_row.addWidget(self.browse_button)

        dest_layout.addLayout(dest_row)
        dest_group.setLayout(dest_layout)
        layout.addWidget(dest_group)

        # Export options
        options_group = QGroupBox("Export Options")
        options_layout = QVBoxLayout()

        self.include_evidence_checkbox = QCheckBox(
            "Include source evidence files (E01, DD, etc.)"
        )
        self.include_evidence_checkbox.setToolTip(
            "Include original evidence image files. All segments (.E01, .E02, etc.) are included. Warning: May be very large."
        )
        self.include_evidence_checkbox.stateChanged.connect(self._on_options_changed)
        options_layout.addWidget(self.include_evidence_checkbox)

        self.include_artifacts_checkbox = QCheckBox(
            "Include cached artifacts (carved files, cache, thumbnails)"
        )
        self.include_artifacts_checkbox.setToolTip(
            "Include extracted artifacts from carved/, cache/, thumbnails/ folders and evidence-level extracted files."
        )
        self.include_artifacts_checkbox.stateChanged.connect(self._on_options_changed)
        options_layout.addWidget(self.include_artifacts_checkbox)

        # Include reports checkbox (default checked)
        self.include_reports_checkbox = QCheckBox(
            "Include reports"
        )
        self.include_reports_checkbox.setChecked(True)
        self.include_reports_checkbox.setToolTip(
            "Include generated PDF reports from reports/ folder."
        )
        self.include_reports_checkbox.stateChanged.connect(self._on_options_changed)
        options_layout.addWidget(self.include_reports_checkbox)

        # Include logs checkbox
        self.include_logs_checkbox = QCheckBox(
            "Include audit logs"
        )
        self.include_logs_checkbox.setToolTip(
            "Include case audit log and evidence processing logs."
        )
        self.include_logs_checkbox.stateChanged.connect(self._on_options_changed)
        options_layout.addWidget(self.include_logs_checkbox)

        options_group.setLayout(options_layout)
        layout.addWidget(options_group)

        # Size estimate
        size_group = QGroupBox("Estimated Export Size")
        size_layout = QVBoxLayout()

        self.size_label = QLabel("Calculating...")
        self.size_label.setStyleSheet("font-size: 14pt; font-weight: bold;")
        size_layout.addWidget(self.size_label)

        self.size_detail_label = QLabel("Database files only (minimal)")
        self.size_detail_label.setStyleSheet("color: gray;")
        size_layout.addWidget(self.size_detail_label)

        size_group.setLayout(size_layout)
        layout.addWidget(size_group)

        layout.addStretch()

        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        self.export_button = QPushButton("Export")
        self.export_button.setEnabled(False)
        self.export_button.clicked.connect(self._on_export_clicked)
        button_layout.addWidget(self.export_button)

        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(cancel_button)

        layout.addLayout(button_layout)

        self.setLayout(layout)

        # Start size estimation
        self._recalculate_size_estimate()

    def _on_browse_clicked(self) -> None:
        """Handle browse button click."""
        default_name = f"{self.case_id}.zip"
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Export Package",
            default_name,
            "ZIP Archives (*.zip)"
        )

        if file_path:
            self.dest_edit.setText(file_path)
            self.export_button.setEnabled(True)

    def _on_options_changed(self) -> None:
        """Handle export option checkbox changes."""
        if not self.estimating:
            self._recalculate_size_estimate()

    def _recalculate_size_estimate(self) -> None:
        """Recalculate size estimate based on current options."""
        from core.export import ExportOptions

        options = ExportOptions(
            include_source_evidence=self.include_evidence_checkbox.isChecked(),
            include_cached_artifacts=self.include_artifacts_checkbox.isChecked(),
            include_logs=self.include_logs_checkbox.isChecked(),
            include_reports=self.include_reports_checkbox.isChecked()
        )

        self.estimating = True
        self.size_label.setText("Calculating...")

        # Update detail text
        parts = ["Database files"]
        if options.include_source_evidence:
            parts.append("source evidence")
        if options.include_cached_artifacts:
            parts.append("artifacts")
        if options.include_reports:
            parts.append("reports")
        if options.include_logs:
            parts.append("logs")
        self.size_detail_label.setText(", ".join(parts))

        # Start worker
        self.size_worker = SizeEstimateWorker(self.case_folder, options)
        self.size_worker.finished.connect(self._on_size_estimate_finished)
        self.size_worker.error.connect(self._on_size_estimate_error)
        self.size_worker.start()

    def _on_size_estimate_finished(self, size_bytes: int) -> None:
        """Handle size estimation completion."""
        self.estimating = False
        self.size_estimate = size_bytes

        # Format size
        if size_bytes < 1024:
            size_str = f"{size_bytes} bytes"
        elif size_bytes < 1024 * 1024:
            size_str = f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            size_str = f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            size_str = f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

        self.size_label.setText(size_str)

    def _on_size_estimate_error(self, error_msg: str) -> None:
        """Handle size estimation error."""
        self.estimating = False
        self.size_label.setText("Error")
        show_error_dialog(
            self,
            "Size Estimation Failed",
            "Failed to estimate export size.",
            details=error_msg
        )

    def _on_export_clicked(self) -> None:
        """Handle export button click."""
        from core.export import ExportOptions

        dest_path = Path(self.dest_edit.text())
        if not dest_path:
            QMessageBox.warning(
                self,
                "No Destination",
                "Please select a destination for the export package."
            )
            return

        options = ExportOptions(
            include_source_evidence=self.include_evidence_checkbox.isChecked(),
            include_cached_artifacts=self.include_artifacts_checkbox.isChecked(),
            include_logs=self.include_logs_checkbox.isChecked(),
            include_reports=self.include_reports_checkbox.isChecked()
        )

        # Create progress dialog
        progress = QProgressDialog(
            "Exporting case...",
            "Cancel",
            0,
            100,
            self
        )
        progress.setWindowModality(Qt.WindowModal)
        progress.setWindowTitle("Exporting")
        progress.setMinimumDuration(0)
        progress.show()

        # Start export worker
        self.export_worker = ExportWorker(self.case_folder, dest_path, options)

        def update_progress(current_bytes: int, total_bytes: int, filename: str) -> None:
            if total_bytes > 0:
                percent = int((current_bytes / total_bytes) * 100)
                progress.setValue(percent)
                progress.setLabelText(f"Exporting: {filename}")

        self.export_worker.progress.connect(update_progress)
        self.export_worker.finished.connect(lambda result: self._on_export_finished(result, progress))
        self.export_worker.error.connect(lambda err: self._on_export_error(err, progress))
        self.export_worker.cancelled.connect(lambda: self._on_export_cancelled(progress))

        # Use cooperative cancellation instead of terminate()
        progress.canceled.connect(self.export_worker.request_cancel)

        self.export_worker.start()

    def _on_export_finished(self, result: Any, progress: QProgressDialog) -> None:
        """Handle export completion."""
        progress.close()

        if result.success:
            QMessageBox.information(
                self,
                "Export Complete",
                f"Successfully exported {result.exported_files} files "
                f"({result.total_size_bytes / (1024*1024):.1f} MB) "
                f"in {result.duration_seconds:.1f}s\n\n"
                f"Output: {result.export_path}",
            )
            self.accept()
        else:
            show_error_dialog(
                self,
                "Export Failed",
                "Failed to export case.",
                details=result.error_message or "Unknown error"
            )

    def _on_export_error(self, error_msg: str, progress: QProgressDialog) -> None:
        """Handle export error."""
        progress.close()
        show_error_dialog(
            self,
            "Export Failed",
            "An error occurred during export.",
            details=error_msg
        )

    def _on_export_cancelled(self, progress: QProgressDialog) -> None:
        """Handle export cancellation."""
        progress.close()
        QMessageBox.information(
            self,
            "Export Cancelled",
            "Export was cancelled. No file was created."
        )


class ImportWorker(QThread):
    """Background worker for importing case packages."""

    finished = Signal(object)  # ImportResult
    error = Signal(str)        # Error message
    progress = Signal(object, object, str)  # current_bytes, total_bytes, filename (object to avoid 32-bit overflow)
    cancelled = Signal()       # Emitted when cancelled

    def __init__(self, zip_path: Path, dest_cases_dir: Path, options: Any) -> None:
        super().__init__()
        self.zip_path = zip_path
        self.dest_cases_dir = dest_cases_dir
        self.options = options
        self._cancel_requested = False

    def request_cancel(self) -> None:
        """Request cancellation (cooperative)."""
        self._cancel_requested = True

    def run(self) -> None:
        try:
            from core.import_case import import_case

            result = import_case(
                self.zip_path,
                self.dest_cases_dir,
                self.options,
                cancel_check=lambda: self._cancel_requested,
                progress_callback=lambda cur, tot, file: self.progress.emit(cur, tot, file)
            )

            # Check result for cancellation, not the flag
            if result.error_message == "Cancelled by user":
                self.cancelled.emit()
            else:
                self.finished.emit(result)
        except FileExistsError as exc:
            self.error.emit(f"COLLISION:{exc}")
        except ValueError as exc:
            self.error.emit(f"VALIDATION:{exc}")
        except Exception as exc:
            self.error.emit(str(exc))


class ImportDialog(QDialog):
    """Dialog for importing case packages."""

    def __init__(self, cases_dir: Path, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.cases_dir = cases_dir
        self.selected_dest_dir: Optional[Path] = cases_dir
        self.validation_result = None
        self.import_result = None  # Store result for main.py to access

        self.setWindowTitle("Import Case")
        self.resize(600, 450)

        layout = QVBoxLayout()

        # Header
        header = QLabel("<h2>Import Case Package</h2>")
        layout.addWidget(header)

        info = QLabel(
            "Import a case from a ZIP export package. The package will be "
            "validated before import to ensure integrity."
        )
        info.setWordWrap(True)
        layout.addWidget(info)

        layout.addSpacing(20)

        # File selection
        file_group = QGroupBox("Export Package")
        file_layout = QVBoxLayout()

        file_row = QHBoxLayout()
        self.file_edit = QLineEdit()
        self.file_edit.setPlaceholderText("Select ZIP export package...")
        file_row.addWidget(self.file_edit)

        self.browse_button = QPushButton("Browse...")
        self.browse_button.clicked.connect(self._on_browse_clicked)
        file_row.addWidget(self.browse_button)

        file_layout.addLayout(file_row)
        file_group.setLayout(file_layout)
        layout.addWidget(file_group)

        # Validation status
        self.validation_group = QGroupBox("Validation Status")
        validation_layout = QVBoxLayout()

        self.validation_label = QLabel("No package selected")
        self.validation_label.setWordWrap(True)
        validation_layout.addWidget(self.validation_label)

        self.validation_details = QTextEdit()
        self.validation_details.setReadOnly(True)
        self.validation_details.setMaximumHeight(150)
        self.validation_details.hide()
        validation_layout.addWidget(self.validation_details)

        self.validation_group.setLayout(validation_layout)
        layout.addWidget(self.validation_group)

        # Destination selection
        dest_group = QGroupBox("Import Destination")
        dest_layout = QVBoxLayout()

        dest_info = QLabel(
            "Select the parent folder where the case will be imported. "
            "A subfolder will be created using the case ID from the package."
        )
        dest_info.setWordWrap(True)
        dest_layout.addWidget(dest_info)

        dest_row = QHBoxLayout()
        self.dest_edit = QLineEdit()
        self.dest_edit.setText(str(cases_dir))
        self.dest_edit.setPlaceholderText("Select destination folder...")
        self.dest_edit.textChanged.connect(self._validate_destination)
        dest_row.addWidget(self.dest_edit)

        self.dest_browse_button = QPushButton("Browse...")
        self.dest_browse_button.clicked.connect(self._on_dest_browse_clicked)
        dest_row.addWidget(self.dest_browse_button)

        dest_layout.addLayout(dest_row)

        # Destination validation status label
        self.dest_status_label = QLabel()
        self.dest_status_label.setStyleSheet("color: gray;")
        dest_layout.addWidget(self.dest_status_label)

        dest_group.setLayout(dest_layout)
        layout.addWidget(dest_group)

        layout.addStretch()

        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()

        self.import_button = QPushButton("Import")
        self.import_button.setEnabled(False)
        self.import_button.clicked.connect(self._on_import_clicked)
        button_layout.addWidget(self.import_button)

        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(cancel_button)

        layout.addLayout(button_layout)

        self.setLayout(layout)

    def _on_browse_clicked(self) -> None:
        """Handle browse button click."""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Export Package",
            "",
            "ZIP Archives (*.zip)"
        )

        if file_path:
            self.file_edit.setText(file_path)
            self._validate_package(Path(file_path))

    def _on_dest_browse_clicked(self) -> None:
        """Handle destination browse button click."""
        folder = QFileDialog.getExistingDirectory(
            self,
            "Select Destination Folder",
            str(self.selected_dest_dir or Path.home()),
        )
        if folder:
            self.dest_edit.setText(folder)

    def _validate_destination(self, text: str) -> None:
        """Validate the destination folder and update UI state."""
        if not text:
            self.dest_status_label.setText("⚠ Destination required")
            self.dest_status_label.setStyleSheet("color: orange;")
            self.selected_dest_dir = None
            self._update_import_button_state()
            return

        dest_path = Path(text)

        if not dest_path.exists():
            # Check if parent is writable (for creation)
            parent = dest_path.parent
            if not parent.exists():
                self.dest_status_label.setText("✗ Parent folder does not exist")
                self.dest_status_label.setStyleSheet("color: red;")
                self.selected_dest_dir = None
            elif not os.access(parent, os.W_OK):
                self.dest_status_label.setText("✗ Cannot create folder (parent not writable)")
                self.dest_status_label.setStyleSheet("color: red;")
                self.selected_dest_dir = None
            else:
                self.dest_status_label.setText("⚠ Folder will be created")
                self.dest_status_label.setStyleSheet("color: orange;")
                self.selected_dest_dir = dest_path
        elif not dest_path.is_dir():
            self.dest_status_label.setText("✗ Path is not a folder")
            self.dest_status_label.setStyleSheet("color: red;")
            self.selected_dest_dir = None
        elif not os.access(dest_path, os.W_OK):
            self.dest_status_label.setText("✗ Folder is not writable")
            self.dest_status_label.setStyleSheet("color: red;")
            self.selected_dest_dir = None
        else:
            # Check if case subfolder would conflict (after validation)
            if self.validation_result and self.validation_result.valid:
                case_id = self.validation_result.manifest.get("case_id", "")
                case_folder = dest_path / case_id
                if case_folder.exists():
                    self.dest_status_label.setText(f"⚠ Case folder exists: {case_id}")
                    self.dest_status_label.setStyleSheet("color: orange;")
                else:
                    self.dest_status_label.setText("✓ Destination valid")
                    self.dest_status_label.setStyleSheet("color: green;")
            else:
                self.dest_status_label.setText("✓ Folder writable")
                self.dest_status_label.setStyleSheet("color: green;")
            self.selected_dest_dir = dest_path

        self._update_import_button_state()

    def _update_import_button_state(self) -> None:
        """Enable import button only when both package and destination are valid."""
        can_import = (
            self.validation_result is not None
            and self.validation_result.valid
            and self.selected_dest_dir is not None
        )
        self.import_button.setEnabled(can_import)

    def _validate_package(self, zip_path: Path) -> None:
        """Validate the selected export package."""
        from .progress import ValidationWorker

        self.validation_label.setText("Validating package...")
        self.validation_details.hide()
        self.import_button.setEnabled(False)

        # Create progress dialog
        progress = QProgressDialog(
            "Validating export package...",
            None,
            0,
            5,
            self
        )
        progress.setWindowModality(Qt.WindowModal)
        progress.setWindowTitle("Validating")
        progress.setMinimumDuration(0)
        progress.setCancelButton(None)
        progress.show()

        # Start validation worker
        self.validation_worker = ValidationWorker(zip_path)

        def update_progress(current: int, total: int, step_name: str) -> None:
            progress.setValue(current)
            progress.setLabelText(f"Step {current}/{total}: {step_name}")

        self.validation_worker.progress.connect(update_progress)
        self.validation_worker.finished.connect(lambda result: self._on_validation_finished(result, progress))
        self.validation_worker.error.connect(lambda err: self._on_validation_error(err, progress))

        self.validation_worker.start()

    def _on_validation_finished(self, result: Any, progress: QProgressDialog) -> None:
        """Handle validation completion."""
        progress.close()
        self.validation_result = result

        if result.valid:
            case_id = result.manifest.get("case_id", "Unknown")
            case_title = result.manifest.get("case_title", "Untitled")
            evidence_count = result.manifest.get("evidence_count", 0)
            file_count = len(result.manifest.get("file_list", []))

            self.validation_label.setText(
                f"✓ Valid package: {case_id} - {case_title}"
            )

            # Show details
            details = []
            details.append(f"Case ID: {case_id}")
            details.append(f"Title: {case_title}")
            details.append(f"Evidences: {evidence_count}")
            details.append(f"Files: {file_count}")
            details.append("")

            if result.warnings:
                details.append("Warnings:")
                for warning in result.warnings:
                    details.append(f"  • {warning}")

            self.validation_details.setPlainText("\n".join(details))
            self.validation_details.show()

            # Re-validate destination (may show case folder conflict now)
            self._validate_destination(self.dest_edit.text())
        else:
            self.validation_label.setText(
                f"✗ Invalid package: {result.error_message}"
            )

            # Show validation details
            details = []
            details.append(f"Error: {result.error_message}")
            details.append("")
            details.append("Validation steps:")
            details.append(f"  {'✓' if result.zip_valid else '✗'} ZIP integrity")
            details.append(f"  {'✓' if result.manifest_present else '✗'} Manifest present")
            details.append(f"  {'✓' if result.files_present else '✗'} Files present")
            details.append(f"  {'✓' if result.checksums_valid else '✗'} Checksums valid")
            details.append(f"  {'✓' if result.schema_compatible else '✗'} Schema compatible")

            self.validation_details.setPlainText("\n".join(details))
            self.validation_details.show()
            self._update_import_button_state()

    def _on_validation_error(self, error_msg: str, progress: QProgressDialog) -> None:
        """Handle validation error."""
        progress.close()
        self.validation_label.setText("✗ Validation failed")
        show_error_dialog(
            self,
            "Validation Failed",
            "Failed to validate export package.",
            details=error_msg
        )

    def _on_import_clicked(self) -> None:
        """Handle import button click."""
        from core.import_case import ImportOptions, CollisionStrategy, detect_case_collision

        if not self.validation_result or not self.validation_result.valid:
            return

        if not self.selected_dest_dir:
            QMessageBox.warning(self, "No Destination", "Please select a destination folder.")
            return

        zip_path = Path(self.file_edit.text())
        case_id = self.validation_result.manifest.get("case_id")
        dest_cases_dir = self.selected_dest_dir  # Use selected destination

        # Check for collision at chosen destination
        if detect_case_collision(case_id, dest_cases_dir):
            # Show collision resolution dialog
            strategy = self._show_collision_dialog(case_id)
            if strategy is None:
                return  # User canceled

            if strategy == CollisionStrategy.CANCEL:
                return  # User chose to abort

            if strategy == CollisionStrategy.OVERWRITE:
                # Require explicit confirmation by typing case ID
                confirmation, ok = QInputDialog.getText(
                    self,
                    "Confirm Overwrite",
                    f"Type the case ID '{case_id}' to confirm overwrite.\n\n"
                    "WARNING: This will permanently delete the existing case!",
                )
                if not ok or confirmation != case_id:
                    QMessageBox.warning(
                        self,
                        "Overwrite Cancelled",
                        "Case ID confirmation does not match. Import cancelled."
                    )
                    return

                options = ImportOptions(
                    collision_strategy=CollisionStrategy.OVERWRITE,
                    case_id_confirmation=case_id
                )
            else:
                # RENAME strategy
                options = ImportOptions(collision_strategy=strategy)
        else:
            # No collision - use CANCEL strategy (which proceeds normally when no collision)
            options = ImportOptions(collision_strategy=CollisionStrategy.CANCEL)

        # Create progress dialog
        progress = QProgressDialog(
            "Importing case...",
            "Cancel",
            0,
            100,
            self
        )
        progress.setWindowModality(Qt.WindowModal)
        progress.setWindowTitle("Importing")
        progress.setMinimumDuration(0)
        progress.show()

        # Start import worker with user-selected destination
        self.import_worker = ImportWorker(zip_path, dest_cases_dir, options)

        def update_progress(current_bytes: int, total_bytes: int, filename: str) -> None:
            if total_bytes > 0:
                percent = int((current_bytes / total_bytes) * 100)
                progress.setValue(percent)
                progress.setLabelText(f"Importing: {filename}")

        self.import_worker.progress.connect(update_progress)
        self.import_worker.finished.connect(lambda result: self._on_import_finished(result, progress))
        self.import_worker.error.connect(lambda err: self._on_import_error(err, progress))
        self.import_worker.cancelled.connect(lambda: self._on_import_cancelled(progress))

        # Use cooperative cancellation instead of terminate()
        progress.canceled.connect(self.import_worker.request_cancel)

        self.import_worker.start()

    def _on_import_cancelled(self, progress: QProgressDialog) -> None:
        """Handle import cancellation."""
        progress.close()
        QMessageBox.information(
            self,
            "Import Cancelled",
            "Import was cancelled. No changes were made."
        )

    def _show_collision_dialog(self, case_id: str) -> Optional[Any]:
        """Show collision resolution dialog."""
        from core.import_case import CollisionStrategy

        dialog = QDialog(self)
        dialog.setWindowTitle("Case Already Exists")
        dialog.resize(500, 250)

        layout = QVBoxLayout()

        warning = QLabel(
            f"<h3>Case '{case_id}' already exists</h3>"
            f"<p>Choose how to handle this collision:</p>"
        )
        warning.setWordWrap(True)
        layout.addWidget(warning)

        # Radio buttons
        radio_group = QButtonGroup(dialog)

        cancel_radio = QRadioButton(
            "Cancel import (do not modify existing case)"
        )
        cancel_radio.setChecked(True)
        radio_group.addButton(cancel_radio, 0)
        layout.addWidget(cancel_radio)

        rename_radio = QRadioButton(
            "Import with renamed ID (add '-imported' suffix)"
        )
        radio_group.addButton(rename_radio, 1)
        layout.addWidget(rename_radio)

        overwrite_radio = QRadioButton(
            "Overwrite existing case (DESTRUCTIVE - requires confirmation)"
        )
        radio_group.addButton(overwrite_radio, 2)
        layout.addWidget(overwrite_radio)

        layout.addStretch()

        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)

        dialog.setLayout(layout)

        if dialog.exec() == QDialog.Accepted:
            strategy_map = {
                0: CollisionStrategy.CANCEL,
                1: CollisionStrategy.RENAME,
                2: CollisionStrategy.OVERWRITE
            }
            return strategy_map[radio_group.checkedId()]
        return None

    def _on_import_finished(self, result: Any, progress: QProgressDialog) -> None:
        """Handle import completion."""
        progress.close()

        if result.success:
            # Store result for main.py to access
            self.import_result = result

            # Build message with location info
            location_info = ""
            if result.imported_path:
                location_info = f"\nLocation: {result.imported_path}"

            QMessageBox.information(
                self,
                "Import Complete",
                f"Successfully imported {result.imported_files} files "
                f"({result.total_size_bytes / (1024*1024):.1f} MB) "
                f"in {result.duration_seconds:.1f}s\n\n"
                f"Case ID: {result.imported_case_id}{location_info}",
            )
            self.accept()
        else:
            self.import_result = None
            show_error_dialog(
                self,
                "Import Failed",
                "Failed to import case.",
                details=result.error_message or "Unknown error"
            )

    def _on_import_error(self, error_msg: str, progress: QProgressDialog) -> None:
        """Handle import error."""
        progress.close()

        # Check if it's a collision error (shouldn't happen, but handle it)
        if error_msg.startswith("COLLISION:"):
            QMessageBox.warning(
                self,
                "Case Already Exists",
                error_msg[10:]
            )
        elif error_msg.startswith("VALIDATION:"):
            show_error_dialog(
                self,
                "Validation Error",
                "Import validation failed.",
                details=error_msg[11:]
            )
        else:
            show_error_dialog(
                self,
                "Import Failed",
                "An error occurred during import.",
                details=error_msg
            )
