"""
URL deduplication dialog.

Provides UI for deduplicating URLs based on user-selected constraints.
Merges duplicate entries by consolidating source_paths and aggregating
visit counts, tags, and match information.

Initial implementation for URL debloating.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from PySide6.QtCore import Qt, Signal, QThread
from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

logger = logging.getLogger(__name__)


@dataclass
class DeduplicationConfig:
    """Configuration for URL deduplication."""
    sources: List[str]  # Sources to deduplicate
    unique_by_url: bool = True  # Always True
    unique_by_first_seen: bool = True
    unique_by_last_seen: bool = True
    unique_by_source: bool = False  # discovered_by


class DeduplicationAnalyzeWorker(QThread):
    """Background worker for analyzing duplicates."""

    finished = Signal(dict)  # {total, duplicates, unique_count, preview_groups}
    error = Signal(str)

    def __init__(
        self,
        case_data,  # CaseDataAccess
        evidence_id: int,
        config: DeduplicationConfig,
    ):
        super().__init__()
        self.case_data = case_data
        self.evidence_id = evidence_id
        self.config = config

    def run(self):
        """Analyze duplicates in background thread."""
        try:
            result = self.case_data.analyze_url_duplicates(
                self.evidence_id,
                sources=self.config.sources,
                unique_by_first_seen=self.config.unique_by_first_seen,
                unique_by_last_seen=self.config.unique_by_last_seen,
                unique_by_source=self.config.unique_by_source,
            )
            self.finished.emit(result)
        except Exception as e:
            logger.error("Deduplication analysis error: %s", e, exc_info=True)
            self.error.emit(str(e))


class DeduplicationExecuteWorker(QThread):
    """Background worker for executing deduplication."""

    progress = Signal(int, int)  # current, total
    finished = Signal(object)  # Dict with total_before, total_after, etc.
    error = Signal(str)

    def __init__(
        self,
        case_data,  # CaseDataAccess
        evidence_id: int,
        config: DeduplicationConfig,
    ):
        super().__init__()
        self.case_data = case_data
        self.evidence_id = evidence_id
        self.config = config

    def run(self):
        """Execute deduplication in background thread."""
        try:
            result = self.case_data.deduplicate_urls(
                self.evidence_id,
                sources=self.config.sources,
                unique_by_first_seen=self.config.unique_by_first_seen,
                unique_by_last_seen=self.config.unique_by_last_seen,
                unique_by_source=self.config.unique_by_source,
                progress_callback=self._report_progress,
            )
            self.finished.emit(result)
        except Exception as e:
            logger.error("Deduplication execution error: %s", e, exc_info=True)
            self.error.emit(str(e))

    def _report_progress(self, current: int, total: int) -> None:
        """Report progress to main thread."""
        self.progress.emit(current, total)


class DeduplicateUrlsDialog(QDialog):
    """
    Dialog for deduplicating URLs based on user-selected constraints.

    Allows selecting which sources to deduplicate and which columns
    define uniqueness. Provides analysis preview before executing.

    Initial implementation.
    """

    deduplication_complete = Signal()  # Emitted when deduplication finishes

    def __init__(
        self,
        case_data,  # CaseDataAccess
        evidence_id: int,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.case_data = case_data
        self.evidence_id = evidence_id

        # Workers
        self._analyze_worker: Optional[DeduplicationAnalyzeWorker] = None
        self._execute_worker: Optional[DeduplicationExecuteWorker] = None

        # Analysis result
        self._analysis_result: Optional[Dict[str, Any]] = None

        self.setWindowTitle("Deduplicate URLs")
        self.resize(550, 500)

        self._init_ui()
        self._load_sources()

    def _init_ui(self) -> None:
        """Initialize the UI."""
        layout = QVBoxLayout(self)

        # Sources selection
        sources_group = QGroupBox("Sources to deduplicate")
        sources_layout = QVBoxLayout(sources_group)

        sources_info = QLabel(
            "Select which URL sources to deduplicate. "
            "Only URLs from selected sources will be affected."
        )
        sources_info.setWordWrap(True)
        sources_layout.addWidget(sources_info)

        self.sources_list = QListWidget()
        self.sources_list.setSelectionMode(QListWidget.MultiSelection)
        self.sources_list.itemSelectionChanged.connect(self._on_selection_changed)
        sources_layout.addWidget(self.sources_list)

        # Select all / none buttons
        select_buttons = QHBoxLayout()
        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.clicked.connect(self._select_all_sources)
        self.select_none_btn = QPushButton("Select None")
        self.select_none_btn.clicked.connect(self._select_no_sources)
        select_buttons.addWidget(self.select_all_btn)
        select_buttons.addWidget(self.select_none_btn)
        select_buttons.addStretch()
        sources_layout.addLayout(select_buttons)

        layout.addWidget(sources_group)

        # Uniqueness constraints
        constraints_group = QGroupBox("Keep entries unique by")
        constraints_layout = QVBoxLayout(constraints_group)

        self.url_checkbox = QCheckBox("URL (required)")
        self.url_checkbox.setChecked(True)
        self.url_checkbox.setEnabled(False)  # Always required
        constraints_layout.addWidget(self.url_checkbox)

        self.first_seen_checkbox = QCheckBox("First Seen timestamp")
        self.first_seen_checkbox.setChecked(True)  # Default on per user request
        self.first_seen_checkbox.toggled.connect(self._on_constraint_changed)
        constraints_layout.addWidget(self.first_seen_checkbox)

        self.last_seen_checkbox = QCheckBox("Last Seen timestamp")
        self.last_seen_checkbox.setChecked(True)  # Default on per user request
        self.last_seen_checkbox.toggled.connect(self._on_constraint_changed)
        constraints_layout.addWidget(self.last_seen_checkbox)

        self.source_checkbox = QCheckBox("Source (discovered_by)")
        self.source_checkbox.setChecked(False)
        self.source_checkbox.toggled.connect(self._on_constraint_changed)
        constraints_layout.addWidget(self.source_checkbox)

        layout.addWidget(constraints_group)

        # Analysis result / preview
        preview_group = QGroupBox("Preview")
        preview_layout = QVBoxLayout(preview_group)

        self.preview_label = QLabel("Click 'Analyze' to see how many duplicates will be merged.")
        self.preview_label.setWordWrap(True)
        preview_layout.addWidget(self.preview_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        preview_layout.addWidget(self.progress_bar)

        layout.addWidget(preview_group)

        # Warning
        warning_layout = QHBoxLayout()
        warning_icon = QLabel("⚠️")
        warning_icon.setStyleSheet("font-size: 18px;")
        warning_layout.addWidget(warning_icon)

        warning_text = QLabel(
            "This action is <b>permanent</b> and cannot be undone. "
            "Duplicate rows will be deleted and their source_path values merged."
        )
        warning_text.setWordWrap(True)
        warning_layout.addWidget(warning_text, 1)
        layout.addLayout(warning_layout)

        # Buttons
        button_layout = QHBoxLayout()

        self.analyze_button = QPushButton("Analyze")
        self.analyze_button.clicked.connect(self._analyze)
        self.analyze_button.setEnabled(False)
        button_layout.addWidget(self.analyze_button)

        button_layout.addStretch()

        self.button_box = QDialogButtonBox()
        self.deduplicate_button = self.button_box.addButton(
            "Deduplicate", QDialogButtonBox.AcceptRole
        )
        self.deduplicate_button.setEnabled(False)
        self.cancel_button = self.button_box.addButton(QDialogButtonBox.Cancel)

        self.button_box.accepted.connect(self._execute_deduplication)
        self.button_box.rejected.connect(self.reject)

        button_layout.addWidget(self.button_box)
        layout.addLayout(button_layout)

    def _load_sources(self) -> None:
        """Load available URL sources."""
        try:
            sources = self.case_data.list_url_sources(self.evidence_id)

            for source in sources:
                item = QListWidgetItem(source)
                item.setData(Qt.UserRole, source)
                self.sources_list.addItem(item)

            if not sources:
                self.preview_label.setText("No URL sources found in this evidence.")

        except Exception as e:
            logger.error("Failed to load URL sources: %s", e)
            self.preview_label.setText(f"Error loading sources: {e}")

    def _select_all_sources(self) -> None:
        """Select all sources."""
        self.sources_list.selectAll()

    def _select_no_sources(self) -> None:
        """Deselect all sources."""
        self.sources_list.clearSelection()

    def _on_selection_changed(self) -> None:
        """Handle source selection change."""
        selected = len(self.sources_list.selectedItems()) > 0
        self.analyze_button.setEnabled(selected)
        self.deduplicate_button.setEnabled(False)
        self._analysis_result = None
        self.preview_label.setText("Click 'Analyze' to see how many duplicates will be merged.")

    def _on_constraint_changed(self) -> None:
        """Handle constraint checkbox change."""
        # Invalidate analysis when constraints change
        self.deduplicate_button.setEnabled(False)
        self._analysis_result = None
        self.preview_label.setText("Click 'Analyze' to see how many duplicates will be merged.")

    def _get_config(self) -> DeduplicationConfig:
        """Get current deduplication configuration."""
        sources = [
            item.data(Qt.UserRole)
            for item in self.sources_list.selectedItems()
        ]

        return DeduplicationConfig(
            sources=sources,
            unique_by_url=True,
            unique_by_first_seen=self.first_seen_checkbox.isChecked(),
            unique_by_last_seen=self.last_seen_checkbox.isChecked(),
            unique_by_source=self.source_checkbox.isChecked(),
        )

    def _analyze(self) -> None:
        """Analyze duplicates based on current configuration."""
        config = self._get_config()

        if not config.sources:
            QMessageBox.warning(
                self,
                "No Sources Selected",
                "Please select at least one source to analyze.",
            )
            return

        # Cancel any existing worker
        if self._analyze_worker is not None:
            self._analyze_worker.quit()
            self._analyze_worker.wait(1000)

        # Show progress
        self.analyze_button.setEnabled(False)
        self.preview_label.setText("Analyzing duplicates...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate

        # Start worker
        self._analyze_worker = DeduplicationAnalyzeWorker(
            self.case_data, self.evidence_id, config
        )
        self._analyze_worker.finished.connect(self._on_analysis_complete)
        self._analyze_worker.error.connect(self._on_analysis_error)
        self._analyze_worker.start()

    def _on_analysis_complete(self, result: Dict[str, Any]) -> None:
        """Handle analysis completion."""
        self._analyze_worker = None
        self._analysis_result = result

        self.progress_bar.setVisible(False)
        self.analyze_button.setEnabled(True)

        total = result.get("total", 0)
        unique_count = result.get("unique_count", 0)
        duplicates = result.get("duplicates", 0)

        if duplicates == 0:
            self.preview_label.setText(
                f"<b>No duplicates found.</b>\n\n"
                f"Total URLs: {total:,}\n"
                f"All URLs are already unique based on selected constraints."
            )
            self.deduplicate_button.setEnabled(False)
        else:
            self.preview_label.setText(
                f"<b>Found {duplicates:,} duplicate rows</b>\n\n"
                f"• Total URLs matching sources: {total:,}\n"
                f"• Unique entries after merge: {unique_count:,}\n"
                f"• Rows to be removed: {duplicates:,}"
            )
            self.deduplicate_button.setEnabled(True)

    def _on_analysis_error(self, error: str) -> None:
        """Handle analysis error."""
        self._analyze_worker = None
        self.progress_bar.setVisible(False)
        self.analyze_button.setEnabled(True)

        self.preview_label.setText(f"<b>Analysis failed:</b> {error}")
        logger.error("Deduplication analysis failed: %s", error)

    def _execute_deduplication(self) -> None:
        """Execute the deduplication after confirmation."""
        if self._analysis_result is None:
            return

        config = self._get_config()
        duplicates = self._analysis_result.get("duplicates", 0)

        # Confirm
        reply = QMessageBox.warning(
            self,
            "Confirm Deduplication",
            f"This will permanently remove {duplicates:,} duplicate rows.\n\n"
            f"Sources affected: {', '.join(config.sources)}\n\n"
            "This action cannot be undone.\n\n"
            "Continue?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )

        if reply != QMessageBox.Yes:
            return

        # Disable UI during execution
        self.analyze_button.setEnabled(False)
        self.deduplicate_button.setEnabled(False)
        self.cancel_button.setEnabled(False)
        self.sources_list.setEnabled(False)
        self.first_seen_checkbox.setEnabled(False)
        self.last_seen_checkbox.setEnabled(False)
        self.source_checkbox.setEnabled(False)

        self.preview_label.setText("Deduplicating URLs...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)

        # Start worker
        self._execute_worker = DeduplicationExecuteWorker(
            self.case_data, self.evidence_id, config
        )
        self._execute_worker.progress.connect(self._on_execute_progress)
        self._execute_worker.finished.connect(self._on_execute_complete)
        self._execute_worker.error.connect(self._on_execute_error)
        self._execute_worker.start()

    def _on_execute_progress(self, current: int, total: int) -> None:
        """Handle execution progress update."""
        if total > 0:
            self.progress_bar.setValue(int((current / total) * 100))
            self.preview_label.setText(
                f"Deduplicating URLs... {current:,} / {total:,}"
            )

    def _on_execute_complete(self, result: Dict[str, Any]) -> None:
        """Handle execution completion."""
        self._execute_worker = None
        self.progress_bar.setVisible(False)

        QMessageBox.information(
            self,
            "Deduplication Complete",
            f"Successfully deduplicated URLs.\n\n"
            f"• URLs before: {result['total_before']:,}\n"
            f"• URLs after: {result['total_after']:,}\n"
            f"• Duplicates removed: {result['duplicates_removed']:,}\n"
            f"• Unique URLs affected: {result['unique_urls_affected']:,}",
        )

        self.deduplication_complete.emit()
        self.accept()

    def _on_execute_error(self, error: str) -> None:
        """Handle execution error."""
        self._execute_worker = None
        self.progress_bar.setVisible(False)

        # Re-enable UI
        self.cancel_button.setEnabled(True)
        self.sources_list.setEnabled(True)
        self.first_seen_checkbox.setEnabled(True)
        self.last_seen_checkbox.setEnabled(True)
        self.source_checkbox.setEnabled(True)
        self._on_selection_changed()  # Reset analyze/deduplicate buttons

        QMessageBox.critical(
            self,
            "Deduplication Failed",
            f"Failed to deduplicate URLs:\n\n{error}",
        )
        logger.error("Deduplication execution failed: %s", error)

    def closeEvent(self, event) -> None:
        """Handle dialog close - stop workers."""
        self._stop_workers()
        super().closeEvent(event)

    def reject(self) -> None:
        """Handle dialog rejection - stop workers."""
        self._stop_workers()
        super().reject()

    def _stop_workers(self) -> None:
        """Stop any running workers."""
        if self._analyze_worker is not None:
            self._analyze_worker.quit()
            if not self._analyze_worker.wait(1000):
                self._analyze_worker.terminate()
            self._analyze_worker = None

        if self._execute_worker is not None:
            self._execute_worker.quit()
            if not self._execute_worker.wait(2000):
                self._execute_worker.terminate()
            self._execute_worker = None
