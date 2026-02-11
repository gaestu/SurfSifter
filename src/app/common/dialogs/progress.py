"""
Progress tracking dialogs - enhanced progress display and validation.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QBrush, QColor
from PySide6.QtWidgets import (
    QDialog,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from .utils import show_error_dialog


class EnhancedProgressDialog(QDialog):
    """
    Detailed progress dialog for extractors/analyzers.
    Shows current file, live counts, performance metrics, and ETA.
    """

    def __init__(self, title: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.setMinimumWidth(600)
        self.setMinimumHeight(400)

        layout = QVBoxLayout(self)

        # Current operation
        self.operation_label = QLabel()
        self.operation_label.setStyleSheet("font-size: 12pt; font-weight: bold;")
        layout.addWidget(self.operation_label)

        # Step timeline (- shows discrete extraction steps)
        # Replaces progress bar for better step visibility
        step_group = QGroupBox("Extraction Steps")
        step_layout = QVBoxLayout()
        self.step_list = QListWidget()
        self.step_list.setMaximumHeight(120)
        self.step_list.setStyleSheet("""
            QListWidget::item {
                padding: 4px;
                border-bottom: 1px solid #e0e0e0;
            }
        """)
        self._step_items = {}  # Map step_key -> QListWidgetItem
        step_layout.addWidget(self.step_list)
        step_group.setLayout(step_layout)
        layout.addWidget(step_group)

        # Current file being processed
        current_file_group = QGroupBox("Current File")
        current_file_layout = QVBoxLayout()
        self.file_label = QLabel("—")
        self.file_label.setWordWrap(True)
        self.file_label.setStyleSheet("font-family: monospace;")
        current_file_layout.addWidget(self.file_label)
        current_file_group.setLayout(current_file_layout)
        layout.addWidget(current_file_group)

        # Statistics group
        stats_group = QGroupBox("Live Statistics")
        stats_layout = QFormLayout()

        self.url_count_label = QLabel("0")
        stats_layout.addRow("URLs Found:", self.url_count_label)

        self.image_count_label = QLabel("0")
        stats_layout.addRow("Images Found:", self.image_count_label)

        self.record_count_label = QLabel("0")
        stats_layout.addRow("Records Found:", self.record_count_label)

        self.error_count_label = QLabel("0")
        self.error_count_label.setStyleSheet("color: red;")
        stats_layout.addRow("Errors:", self.error_count_label)

        stats_group.setLayout(stats_layout)
        layout.addWidget(stats_group)

        # Performance group
        perf_group = QGroupBox("Performance")
        perf_layout = QFormLayout()

        self.speed_label = QLabel("—")
        perf_layout.addRow("Speed:", self.speed_label)

        self.elapsed_label = QLabel("00:00:00")
        perf_layout.addRow("Elapsed:", self.elapsed_label)

        self.remaining_label = QLabel("—")
        perf_layout.addRow("Remaining:", self.remaining_label)

        perf_group.setLayout(perf_layout)
        layout.addWidget(perf_group)

        # Log output area (NEW - shows detailed progress messages)
        log_group = QGroupBox("Progress Log")
        log_layout = QVBoxLayout()
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMaximumHeight(150)
        self.log_output.setStyleSheet("font-family: monospace; font-size: 9pt; background-color: #f5f5f5;")
        log_layout.addWidget(self.log_output)
        log_group.setLayout(log_layout)
        layout.addWidget(log_group)

        # Status message
        self.status_label = QLabel()
        self.status_label.setWordWrap(True)
        self.status_label.setStyleSheet("color: gray; font-style: italic;")
        layout.addWidget(self.status_label)

        layout.addStretch()

        # Cancel button
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(self.cancel_button)
        layout.addLayout(button_layout)

    def set_operation(self, operation: str):
        """Set the current operation name (e.g., 'URL Discovery', 'Image Carving')."""
        self.operation_label.setText(operation)

    def update_progress(self, percent: int, current_file: str = ""):
        """
        Update current file being processed.

        Note: Progress bar removed - use step timeline instead.
        This method kept for backward compatibility but only updates file label.

        Args:
            percent: Progress percentage (0-100) - ignored, kept for compatibility
            current_file: Path of file currently being processed
        """
        # Progress bar removed - step timeline shows status instead
        # Just update current file
        if current_file:
            self.file_label.setText(current_file)
        else:
            self.file_label.setText("—")

    def update_stats(
        self,
        urls: Optional[int] = None,
        images: Optional[int] = None,
        records: Optional[int] = None,
        errors: Optional[int] = None
    ):
        """
        Update live count statistics.

        Args:
            urls: Number of URLs found
            images: Number of images found
            records: Number of records found
            errors: Number of errors encountered
        """
        if urls is not None:
            self.url_count_label.setText(str(urls))

        if images is not None:
            self.image_count_label.setText(str(images))

        if records is not None:
            self.record_count_label.setText(str(records))

        if errors is not None:
            self.error_count_label.setText(str(errors))
            if errors > 0:
                self.error_count_label.setStyleSheet("color: red; font-weight: bold;")
            else:
                self.error_count_label.setStyleSheet("color: gray;")

    def update_performance(
        self,
        speed: Optional[float] = None,
        elapsed: Optional[int] = None,
        remaining: Optional[int] = None
    ):
        """
        Update performance metrics.

        Args:
            speed: Processing speed (files/second or MB/second)
            elapsed: Elapsed time in seconds
            remaining: Estimated remaining time in seconds
        """
        if speed is not None:
            if speed >= 1.0:
                self.speed_label.setText(f"{speed:.1f} files/sec")
            else:
                self.speed_label.setText(f"{speed:.2f} files/sec")

        if elapsed is not None:
            hours = elapsed // 3600
            minutes = (elapsed % 3600) // 60
            seconds = elapsed % 60
            self.elapsed_label.setText(f"{hours:02d}:{minutes:02d}:{seconds:02d}")

        if remaining is not None:
            if remaining < 0:
                self.remaining_label.setText("—")
            elif remaining < 60:
                self.remaining_label.setText(f"{remaining} seconds")
            elif remaining < 3600:
                minutes = remaining // 60
                self.remaining_label.setText(f"{minutes} minutes")
            else:
                hours = remaining // 3600
                minutes = (remaining % 3600) // 60
                self.remaining_label.setText(f"{hours}h {minutes}m")

    def set_status(self, message: str):
        """Set the status message at the bottom."""
        self.status_label.setText(message)

    def append_log(self, message: str):
        """
        Append a message to the progress log output.

        Args:
            message: Log message to append
        """
        self.log_output.append(message)
        # Auto-scroll to bottom
        scrollbar = self.log_output.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def set_indeterminate(self, indeterminate: bool = True):
        """
        Set progress bar to indeterminate mode (busy indicator).

        DEPRECATED in Progress bar removed. Method kept for compatibility.

        Args:
            indeterminate: True for indeterminate mode, False for normal
        """
        # Progress bar removed - no-op for backward compatibility
        pass

    def mark_complete(self, success: bool = True):
        """
        Mark the operation as complete.

        Args:
            success: True if completed successfully, False if failed
        """
        # Progress bar removed - just update button and status
        self.cancel_button.setText("Close")

        if success:
            self.set_status("✓ Operation completed successfully")
            self.status_label.setStyleSheet("color: green; font-weight: bold;")
        else:
            self.set_status("✗ Operation failed or was cancelled")
            self.status_label.setStyleSheet("color: red; font-weight: bold;")

    def update_step(self, step_key: str, status: str, message: str = ""):
        """
        Update step status in the timeline.

        Args:
            step_key: Unique step identifier (e.g., "bulk-run", "bulk-ingest")
            status: Step status ("pending" | "running" | "done" | "error" | "skipped")
            message: Optional step message/label
        """
        # Status icons and colors
        status_icons = {
            "pending": "⏳",
            "running": "▶️",
            "done": "✓",
            "error": "✗",
            "skipped": "⊘",
        }
        status_colors = {
            "pending": "#808080",     # Gray
            "running": "#0078d4",     # Blue
            "done": "#107c10",        # Green
            "error": "#d13438",       # Red
            "skipped": "#f7630c",     # Orange
        }

        icon = status_icons.get(status, "•")
        color = status_colors.get(status, "#000000")

        # Build display text
        label = message if message else step_key.replace("-", " ").title()
        display_text = f"{icon} {label}"

        # Update or create item
        if step_key in self._step_items:
            item = self._step_items[step_key]
            item.setText(display_text)
        else:
            item = QListWidgetItem(display_text)
            self._step_items[step_key] = item
            self.step_list.addItem(item)

        # Apply color based on status
        item.setForeground(QBrush(QColor(color)))

        # Auto-scroll to active step
        if status == "running":
            self.step_list.scrollToItem(item)


class ValidationWorker(QThread):
    """Background worker for validating export packages."""

    finished = Signal(object)  # ValidationResult
    error = Signal(str)        # Error message
    progress = Signal(int, int, str)  # current_step, total_steps, step_name

    def __init__(self, zip_path: Path) -> None:
        super().__init__()
        self.zip_path = zip_path

    def run(self) -> None:
        try:
            from core.import_case import validate_export_package

            result = validate_export_package(
                self.zip_path,
                progress_callback=lambda cur, tot, step: self.progress.emit(cur, tot, step)
            )
            self.finished.emit(result)
        except Exception as exc:
            self.error.emit(str(exc))


class ValidationDialog(QDialog):
    """Dialog for displaying case validation results."""

    def __init__(self, validation_report, parent: Optional[QWidget] = None) -> None:
        """
        Initialize the validation dialog.

        Args:
            validation_report: ValidationReport object from core.validation
            parent: Parent widget
        """
        super().__init__(parent)
        self.validation_report = validation_report

        self.setWindowTitle("Case Validation Report")
        self.resize(900, 600)

        layout = QVBoxLayout()

        # Header section
        header_layout = self._create_header()
        layout.addLayout(header_layout)

        # Results table
        self.table = self._create_results_table()
        layout.addWidget(self.table)

        # Status bar at bottom
        status_bar = self._create_status_bar()
        layout.addWidget(status_bar)

        # Buttons
        button_layout = QHBoxLayout()

        export_button = QPushButton("Export Report...")
        export_button.clicked.connect(self._export_report)
        button_layout.addWidget(export_button)

        revalidate_button = QPushButton("Revalidate")
        revalidate_button.clicked.connect(self._revalidate)
        button_layout.addWidget(revalidate_button)

        button_layout.addStretch()

        close_button = QPushButton("Close")
        close_button.clicked.connect(self.accept)
        button_layout.addWidget(close_button)

        layout.addLayout(button_layout)

        self.setLayout(layout)

    def _create_header(self) -> QVBoxLayout:
        """Create header section with case info and overall status."""
        layout = QVBoxLayout()

        # Case folder
        case_label = QLabel(f"<b>{'Case Folder'}:</b> {self.validation_report.case_folder}")
        layout.addWidget(case_label)

        # Validation time
        time_label = QLabel(
            f"<b>{'Validation Time'}:</b> {self.validation_report.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')} "
            f"({self.validation_report.duration_seconds:.3f}s)"
        )
        layout.addWidget(time_label)

        # Overall status with color coding
        status = self.validation_report.overall_status
        status_colors = {
            "pass": ("green", "✓"),
            "warning": ("orange", "⚠"),
            "error": ("red", "✗"),
        }
        color, symbol = status_colors.get(status.value, ("gray", "?"))

        status_label = QLabel(
            f"<b>{'Overall Status'}:</b> "
            f"<span style='color: {color}; font-weight: bold; font-size: 14pt;'>"
            f"{symbol} {status.value.upper()}"
            f"</span>"
        )
        layout.addWidget(status_label)

        return layout

    def _create_results_table(self) -> QTableWidget:
        """Create table showing all validation check results."""
        from core.validation import ValidationStatus

        table = QTableWidget()
        table.setColumnCount(3)
        table.setHorizontalHeaderLabels([
            "Status",
            "Check",
            "Message"
        ])

        # Configure table
        table.setSelectionBehavior(QTableWidget.SelectRows)
        table.setSelectionMode(QTableWidget.SingleSelection)
        table.horizontalHeader().setStretchLastSection(True)
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        table.setEditTriggers(QTableWidget.NoEditTriggers)

        # Populate rows
        results = self.validation_report.results
        table.setRowCount(len(results))

        for row, result in enumerate(results):
            # Status icon
            status_symbols = {
                ValidationStatus.PASS: ("✓", Qt.green),
                ValidationStatus.WARNING: ("⚠", Qt.darkYellow),
                ValidationStatus.ERROR: ("✗", Qt.red),
            }
            symbol, color = status_symbols.get(result.status, ("?", Qt.gray))

            status_item = QTableWidgetItem(symbol)
            status_item.setTextAlignment(Qt.AlignCenter)
            status_item.setForeground(color)
            font = status_item.font()
            font.setPointSize(14)
            font.setBold(True)
            status_item.setFont(font)
            table.setItem(row, 0, status_item)

            # Check name
            check_item = QTableWidgetItem(result.check_name)
            table.setItem(row, 1, check_item)

            # Message
            message_item = QTableWidgetItem(result.message)
            table.setItem(row, 2, message_item)

            # Store full result object for details dialog
            table.item(row, 0).setData(Qt.UserRole, result)

        # Double-click to show details
        table.doubleClicked.connect(self._show_check_details)

        return table

    def _create_status_bar(self) -> QLabel:
        """Create status bar showing summary counts."""
        pass_count = self.validation_report.pass_count
        warning_count = self.validation_report.warning_count
        error_count = self.validation_report.error_count

        status_text = (
            f"<span style='color: green;'>✓ {pass_count} passed</span> • "
            f"<span style='color: orange;'>⚠ {warning_count} warnings</span> • "
            f"<span style='color: red;'>✗ {error_count} errors</span>"
        )

        status_label = QLabel(status_text)
        status_label.setStyleSheet("padding: 8px; background-color: #f0f0f0; border-radius: 4px;")

        return status_label

    def _show_check_details(self, index) -> None:
        """Show detailed information for a validation check."""
        row = index.row()
        result = self.table.item(row, 0).data(Qt.UserRole)

        # Create details dialog
        details_dialog = QDialog(self)
        details_dialog.setWindowTitle("Validation Check Details")
        details_dialog.resize(600, 400)

        layout = QVBoxLayout()

        # Check name
        name_label = QLabel(f"<h3>{result.check_name}</h3>")
        layout.addWidget(name_label)

        # Status
        status_symbols = {
            "pass": ("green", "✓"),
            "warning": ("orange", "⚠"),
            "error": ("red", "✗"),
        }
        color, symbol = status_symbols.get(result.status.value, ("gray", "?"))
        status_label = QLabel(
            f"<b>{'Status'}:</b> "
            f"<span style='color: {color}; font-weight: bold;'>{symbol} {result.status.value.upper()}</span>"
        )
        layout.addWidget(status_label)

        # Message
        message_label = QLabel(f"<b>{'Message'}:</b><br>{result.message}")
        message_label.setWordWrap(True)
        layout.addWidget(message_label)

        # Remediation (if available)
        if result.remediation:
            remediation_label = QLabel(
                f"<b>{'Remediation'}:</b><br>"
                f"<span style='color: blue;'>{result.remediation}</span>"
            )
            remediation_label.setWordWrap(True)
            layout.addWidget(remediation_label)

        # Details (if available)
        if result.details:
            details_label = QLabel(f"<b>{'Details'}:</b>")
            layout.addWidget(details_label)

            details_text = QTextEdit()
            details_text.setReadOnly(True)
            details_text.setPlainText(json.dumps(result.details, indent=2))
            details_text.setMaximumHeight(150)
            layout.addWidget(details_text)

        # Close button
        close_button = QPushButton("Close")
        close_button.clicked.connect(details_dialog.accept)
        layout.addWidget(close_button)

        details_dialog.setLayout(layout)
        details_dialog.exec()

    def _export_report(self) -> None:
        """Export validation report to text file."""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Validation Report",
            f"validation_report_{self.validation_report.timestamp.strftime('%Y%m%d_%H%M%S')}.txt",
            "Text Files (*.txt)"
        )

        if file_path:
            try:
                Path(file_path).write_text(self.validation_report.to_text(), encoding="utf-8")
                QMessageBox.information(
                    self,
                    "Export Successful",
                    f"Validation report exported to:\n{file_path}",
                )
            except Exception as exc:
                show_error_dialog(
                    self,
                    "Export Failed",
                    "Failed to export validation report.",
                    details=str(exc)
                )

    def _revalidate(self) -> None:
        """Rerun validation and update the dialog."""
        from core.validation import validate_case_full

        try:
            # Run validation
            new_report = validate_case_full(self.validation_report.case_folder)

            # Update dialog with new results
            self.validation_report = new_report

            # Rebuild UI
            # Clear and recreate table
            self.table.setParent(None)
            self.table = self._create_results_table()
            self.layout().insertWidget(2, self.table)  # Insert at position 2 (after header)

            # Update status bar
            status_bar = self.layout().itemAt(3).widget()
            if status_bar:
                status_bar.setParent(None)
            new_status_bar = self._create_status_bar()
            self.layout().insertWidget(3, new_status_bar)

            # Update header (rebuild entire header section)
            old_header_layout = self.layout().itemAt(0)
            while old_header_layout.count():
                item = old_header_layout.takeAt(0)
                if item.widget():
                    item.widget().setParent(None)

            # Rebuild header
            new_header = self._create_header()
            for i in range(new_header.count()):
                widget = new_header.itemAt(i).widget()
                old_header_layout.addWidget(widget)

            QMessageBox.information(
                self,
                "Revalidation Complete",
                f"Validation completed in {new_report.duration_seconds:.3f}s\n\n"
                f"✓ {new_report.pass_count} passed\n"
                f"⚠ {new_report.warning_count} warnings\n"
                f"✗ {new_report.error_count} errors",
            )

        except Exception as exc:
            show_error_dialog(
                self,
                "Revalidation Failed",
                "Failed to revalidate case.",
                details=str(exc)
            )
