"""
Audit Tab - forensic audit data with subtabs.

Initial implementation with Extraction subtab.
Added Statistics subtab (moved from standalone tab).
Added Warnings subtab for extraction warnings (schema discovery).
Added Logs subtab (moved from standalone tab).
Added Download Audit subtab for investigator download outcomes.

This tab is added as a per-evidence subtab showing forensic audit data.

Subtabs:
- Extraction: View all files extracted by any extractor (extracted_files table)
- Warnings: View extraction warnings (unknown schemas, parse errors)
- Download Audit: View investigator-initiated download request outcomes
- Statistics: View extractor run statistics (summary cards)
- Logs: Per-evidence extraction logs (current and persisted sessions)
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableView,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

if TYPE_CHECKING:
    from core.audit_logging import AuditLogger

from core.database import DatabaseManager
from app.features.audit.models import (
    DownloadAuditTableModel,
    ExtractedFilesTableModel,
    ExtractionWarningsTableModel,
)
from app.features.audit.statistics_subtab import StatisticsSubtab

logger = logging.getLogger(__name__)


class ExtractionSubtab(QWidget):
    """
    Extraction subtab - displays extracted_files table.

    Shows all files extracted by any extractor with:
    - Filter by extractor name
    - Filter by status
    - Summary statistics
    - Paginated table view
    """

    # Available status values for filter
    STATUS_OPTIONS = [
        ("", "All Statuses"),
        ("ok", "OK"),
        ("partial", "Partial"),
        ("error", "Error"),
        ("skipped", "Skipped"),
    ]

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label

        self._model: Optional[ExtractedFilesTableModel] = None
        self._loaded = False

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Set up the UI components."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        # === Summary statistics frame ===
        self._stats_frame = QFrame()
        self._stats_frame.setFrameShape(QFrame.StyledPanel)
        self._stats_frame.setStyleSheet("""
            QFrame {
                background-color: #f5f5f5;
                border: 1px solid #e0e0e0;
                border-radius: 4px;
                padding: 8px;
            }
        """)
        stats_layout = QGridLayout(self._stats_frame)
        stats_layout.setSpacing(16)

        # Row 1: Total files, Total size
        stats_layout.addWidget(QLabel("Total Files:"), 0, 0)
        self._total_files_label = QLabel("0")
        self._total_files_label.setStyleSheet("font-weight: bold;")
        stats_layout.addWidget(self._total_files_label, 0, 1)

        stats_layout.addWidget(QLabel("Total Size:"), 0, 2)
        self._total_size_label = QLabel("0 B")
        self._total_size_label.setStyleSheet("font-weight: bold;")
        stats_layout.addWidget(self._total_size_label, 0, 3)

        stats_layout.addWidget(QLabel("Errors:"), 0, 4)
        self._error_count_label = QLabel("0")
        self._error_count_label.setStyleSheet("font-weight: bold; color: #c62828;")
        stats_layout.addWidget(self._error_count_label, 0, 5)

        # Stretch at end
        stats_layout.setColumnStretch(6, 1)

        layout.addWidget(self._stats_frame)

        # === Filter controls ===
        filter_layout = QHBoxLayout()
        filter_layout.setSpacing(16)

        # Extractor filter
        filter_layout.addWidget(QLabel("Extractor:"))
        self._extractor_combo = QComboBox()
        self._extractor_combo.setMinimumWidth(200)
        self._extractor_combo.addItem("All Extractors", "")
        self._extractor_combo.currentIndexChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self._extractor_combo)

        # Status filter
        filter_layout.addWidget(QLabel("Status:"))
        self._status_combo = QComboBox()
        self._status_combo.setMinimumWidth(120)
        for value, label in self.STATUS_OPTIONS:
            self._status_combo.addItem(label, value)
        self._status_combo.currentIndexChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self._status_combo)

        # Refresh button
        self._refresh_btn = QPushButton("🔄 Refresh")
        self._refresh_btn.clicked.connect(self.load)
        filter_layout.addWidget(self._refresh_btn)

        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        # === Table view ===
        self._table = QTableView()
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableView.SelectRows)
        self._table.setSelectionMode(QTableView.SingleSelection)
        self._table.setSortingEnabled(False)  # Data comes pre-sorted from DB
        self._table.verticalHeader().setVisible(False)

        # Set column resize modes
        header = self._table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.Interactive)

        layout.addWidget(self._table, 1)  # Stretch factor 1

        # === Empty state label ===
        self._empty_label = QLabel(
            "No extracted files found.\n\n"
            "Run extractors to populate this table."
        )
        self._empty_label.setAlignment(Qt.AlignCenter)
        self._empty_label.setStyleSheet("color: #666; font-size: 14px;")
        self._empty_label.hide()
        layout.addWidget(self._empty_label)

        # === Pagination controls ===
        pagination_layout = QHBoxLayout()
        pagination_layout.setSpacing(8)

        self._showing_label = QLabel("Showing 0 of 0")
        pagination_layout.addWidget(self._showing_label)

        pagination_layout.addStretch()

        self._prev_btn = QPushButton("← Previous")
        self._prev_btn.clicked.connect(self._on_prev_page)
        self._prev_btn.setEnabled(False)
        pagination_layout.addWidget(self._prev_btn)

        self._page_label = QLabel("Page 1")
        pagination_layout.addWidget(self._page_label)

        self._next_btn = QPushButton("Next →")
        self._next_btn.clicked.connect(self._on_next_page)
        self._next_btn.setEnabled(False)
        pagination_layout.addWidget(self._next_btn)

        layout.addLayout(pagination_layout)

        # Pagination state
        self._page = 0
        self._page_size = 1000

    def load(self) -> None:
        """Load data from database."""
        if self._model is None:
            self._model = ExtractedFilesTableModel(
                self.db_manager,
                self.evidence_id,
                self.evidence_label,
                parent=self,
            )
            self._table.setModel(self._model)

            # Set initial column widths
            self._table.setColumnWidth(0, 140)  # Extractor
            self._table.setColumnWidth(1, 180)  # Filename
            self._table.setColumnWidth(2, 250)  # Source Path
            self._table.setColumnWidth(3, 80)   # Size
            self._table.setColumnWidth(4, 60)   # Type
            self._table.setColumnWidth(5, 140)  # SHA256
            self._table.setColumnWidth(6, 60)   # Status
            self._table.setColumnWidth(7, 140)  # Extracted At

        # Get current filter values
        extractor = self._extractor_combo.currentData() or ""
        status = self._status_combo.currentData() or ""

        # Load data
        self._model.load(
            extractor_filter=extractor,
            status_filter=status,
            limit=self._page_size,
            offset=self._page * self._page_size,
        )

        # Update extractor dropdown (only on first load)
        if not self._loaded:
            extractors = self._model.get_distinct_extractors()
            # Preserve current selection
            current = self._extractor_combo.currentData()
            self._extractor_combo.blockSignals(True)
            self._extractor_combo.clear()
            self._extractor_combo.addItem("All Extractors", "")
            for ext in extractors:
                display_name = ext.replace("_", " ").title()
                self._extractor_combo.addItem(display_name, ext)
            # Restore selection if still valid
            idx = self._extractor_combo.findData(current)
            if idx >= 0:
                self._extractor_combo.setCurrentIndex(idx)
            self._extractor_combo.blockSignals(False)
            self._loaded = True

        # Update statistics
        stats = self._model.get_stats()
        self._update_stats_display(stats)

        # Update pagination
        self._update_pagination()

        # Show/hide empty state
        if self._model.rowCount() == 0:
            self._table.hide()
            self._empty_label.show()
        else:
            self._empty_label.hide()
            self._table.show()

    def _update_stats_display(self, stats: dict) -> None:
        """Update the statistics display."""
        self._total_files_label.setText(f"{stats.get('total_count', 0):,}")

        total_size = stats.get("total_size_bytes", 0)
        self._total_size_label.setText(self._format_size(total_size))

        error_count = stats.get("error_count", 0)
        self._error_count_label.setText(str(error_count))
        if error_count > 0:
            self._error_count_label.setStyleSheet("font-weight: bold; color: #c62828;")
        else:
            self._error_count_label.setStyleSheet("font-weight: bold; color: #2e7d32;")

    def _update_pagination(self) -> None:
        """Update pagination controls."""
        if self._model is None:
            return

        total = self._model.total_count
        showing = self._model.rowCount()
        start = self._page * self._page_size + 1 if showing > 0 else 0
        end = start + showing - 1 if showing > 0 else 0

        self._showing_label.setText(f"Showing {start:,}-{end:,} of {total:,}")

        total_pages = (total + self._page_size - 1) // self._page_size if total > 0 else 1
        self._page_label.setText(f"Page {self._page + 1} of {total_pages}")

        self._prev_btn.setEnabled(self._page > 0)
        self._next_btn.setEnabled(end < total)

    def _on_filter_changed(self) -> None:
        """Handle filter combo changes."""
        self._page = 0  # Reset to first page on filter change
        self.load()

    def _on_prev_page(self) -> None:
        """Go to previous page."""
        if self._page > 0:
            self._page -= 1
            self.load()

    def _on_next_page(self) -> None:
        """Go to next page."""
        self._page += 1
        self.load()

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format file size in human-readable form."""
        if size_bytes is None or size_bytes == 0:
            return "0 B"
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


class ExtractionWarningsSubtab(QWidget):
    """
    Extraction Warnings subtab - displays extraction_warnings table.

    Shows warnings collected during extraction (unknown schemas, parse errors)
    with filtering by extractor, category, and severity.

    Initial implementation.
    """

    # Severity filter options
    SEVERITY_OPTIONS = [
        ("", "All Severities"),
        ("info", "ℹ️ Info"),
        ("warning", "⚠️ Warning"),
        ("error", "❌ Error"),
    ]

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label

        self._model: Optional[ExtractionWarningsTableModel] = None
        self._loaded = False

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Set up the UI components."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        # === Summary statistics frame ===
        self._stats_frame = QFrame()
        self._stats_frame.setFrameShape(QFrame.StyledPanel)
        self._stats_frame.setStyleSheet("""
            QFrame {
                background-color: #fff8e1;
                border: 1px solid #ffe082;
                border-radius: 4px;
                padding: 8px;
            }
        """)
        stats_layout = QHBoxLayout(self._stats_frame)
        stats_layout.setSpacing(20)

        self._total_label = QLabel("Total: 0")
        self._total_label.setStyleSheet("font-weight: bold;")
        stats_layout.addWidget(self._total_label)

        self._info_label = QLabel("ℹ️ Info: 0")
        self._info_label.setStyleSheet("color: #1976d2;")
        stats_layout.addWidget(self._info_label)

        self._warning_label = QLabel("⚠️ Warning: 0")
        self._warning_label.setStyleSheet("color: #f57f17;")
        stats_layout.addWidget(self._warning_label)

        self._error_label = QLabel("❌ Error: 0")
        self._error_label.setStyleSheet("color: #c62828;")
        stats_layout.addWidget(self._error_label)

        stats_layout.addStretch()
        layout.addWidget(self._stats_frame)

        # === Filter row ===
        filter_layout = QHBoxLayout()
        filter_layout.setSpacing(10)

        # Extractor filter
        filter_layout.addWidget(QLabel("Extractor:"))
        self._extractor_combo = QComboBox()
        self._extractor_combo.setMinimumWidth(150)
        self._extractor_combo.addItem("All Extractors", "")
        self._extractor_combo.currentIndexChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self._extractor_combo)

        # Category filter
        filter_layout.addWidget(QLabel("Category:"))
        self._category_combo = QComboBox()
        self._category_combo.setMinimumWidth(120)
        self._category_combo.addItem("All Categories", "")
        self._category_combo.currentIndexChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self._category_combo)

        # Severity filter
        filter_layout.addWidget(QLabel("Severity:"))
        self._severity_combo = QComboBox()
        self._severity_combo.setMinimumWidth(120)
        for value, label in self.SEVERITY_OPTIONS:
            self._severity_combo.addItem(label, value)
        self._severity_combo.currentIndexChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self._severity_combo)

        filter_layout.addStretch()

        # Refresh button
        self._refresh_btn = QPushButton("🔄 Refresh")
        self._refresh_btn.clicked.connect(self.load)
        filter_layout.addWidget(self._refresh_btn)

        layout.addLayout(filter_layout)

        # === Table view ===
        self._table = QTableView()
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableView.SelectRows)
        self._table.setSelectionMode(QTableView.SingleSelection)
        self._table.setSortingEnabled(True)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self._table.verticalHeader().setVisible(False)
        layout.addWidget(self._table)

    def load(self) -> None:
        """Load warnings data with current filters."""
        self._loaded = True

        # Get current filter values
        extractor = self._extractor_combo.currentData() or ""
        category = self._category_combo.currentData() or ""
        severity = self._severity_combo.currentData() or ""

        # Create model if needed
        if self._model is None:
            self._model = ExtractionWarningsTableModel(
                self.db_manager,
                self.evidence_id,
                self.evidence_label,
                parent=self,
            )
            self._table.setModel(self._model)

        # Load data
        self._model.load(
            extractor_filter=extractor,
            category_filter=category,
            severity_filter=severity,
        )

        # Update filter dropdowns (only on first load)
        if self._extractor_combo.count() == 1:
            extractors = self._model.get_distinct_extractors()
            for ext in extractors:
                display = ext.replace("_", " ").title()
                self._extractor_combo.addItem(display, ext)

        if self._category_combo.count() == 1:
            categories = self._model.get_distinct_categories()
            for cat in categories:
                display = cat.replace("_", " ").title()
                self._category_combo.addItem(display, cat)

        # Update summary stats
        summary = self._model.get_summary()
        self._total_label.setText(f"Total: {summary['total']:,}")
        self._info_label.setText(f"ℹ️ Info: {summary['by_severity'].get('info', 0):,}")
        self._warning_label.setText(f"⚠️ Warning: {summary['by_severity'].get('warning', 0):,}")
        self._error_label.setText(f"❌ Error: {summary['by_severity'].get('error', 0):,}")

    def _on_filter_changed(self) -> None:
        """Reload when filter changes."""
        if self._loaded:
            self.load()


class DownloadAuditSubtab(QWidget):
    """
    Download Audit subtab - displays final investigator download outcomes.

    Shows one final row per requested URL with outcome, status, attempts,
    and timing/size metadata.
    """

    OUTCOME_OPTIONS = [
        ("", "All Outcomes"),
        ("success", "Success"),
        ("failed", "Failed"),
        ("blocked", "Blocked"),
        ("cancelled", "Cancelled"),
        ("error", "Error"),
    ]

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label

        self._model: Optional[DownloadAuditTableModel] = None
        self._page = 0
        self._page_size = 500
        self._loaded = False

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(8)

        self._stats_frame = QFrame()
        self._stats_frame.setFrameShape(QFrame.StyledPanel)
        self._stats_frame.setStyleSheet(
            """
            QFrame {
                background-color: #eef6ff;
                border: 1px solid #d0e3ff;
                border-radius: 4px;
                padding: 8px;
            }
            """
        )
        stats_layout = QHBoxLayout(self._stats_frame)
        stats_layout.setSpacing(20)
        self._total_label = QLabel("Total: 0")
        self._success_label = QLabel("Success: 0")
        self._failed_label = QLabel("Failed: 0")
        self._blocked_label = QLabel("Blocked: 0")
        self._cancelled_label = QLabel("Cancelled: 0")
        self._error_label = QLabel("Error: 0")
        for label in (
            self._total_label,
            self._success_label,
            self._failed_label,
            self._blocked_label,
            self._cancelled_label,
            self._error_label,
        ):
            stats_layout.addWidget(label)
        stats_layout.addStretch()
        layout.addWidget(self._stats_frame)

        filter_layout = QHBoxLayout()
        filter_layout.setSpacing(10)
        filter_layout.addWidget(QLabel("Outcome:"))
        self._outcome_combo = QComboBox()
        self._outcome_combo.setMinimumWidth(150)
        for value, label in self.OUTCOME_OPTIONS:
            self._outcome_combo.addItem(label, value)
        self._outcome_combo.currentIndexChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self._outcome_combo)

        filter_layout.addWidget(QLabel("Search:"))
        self._search_edit = QLineEdit()
        self._search_edit.setPlaceholderText("URL, reason, or caller info")
        self._search_edit.returnPressed.connect(self._on_filter_changed)
        filter_layout.addWidget(self._search_edit, 1)

        self._refresh_btn = QPushButton("🔄 Refresh")
        self._refresh_btn.clicked.connect(self.load)
        filter_layout.addWidget(self._refresh_btn)
        layout.addLayout(filter_layout)

        self._table = QTableView()
        self._table.setAlternatingRowColors(True)
        self._table.setSelectionBehavior(QTableView.SelectRows)
        self._table.setSelectionMode(QTableView.SingleSelection)
        self._table.setSortingEnabled(False)
        self._table.verticalHeader().setVisible(False)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self._table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self._table, 1)

        self._empty_label = QLabel(
            "No download audit rows found.\n\n"
            "Run downloads from the Download tab to populate this view."
        )
        self._empty_label.setAlignment(Qt.AlignCenter)
        self._empty_label.setStyleSheet("color: #666; font-size: 14px;")
        self._empty_label.hide()
        layout.addWidget(self._empty_label)

        pagination_layout = QHBoxLayout()
        self._showing_label = QLabel("Showing 0 of 0")
        pagination_layout.addWidget(self._showing_label)
        pagination_layout.addStretch()
        self._prev_btn = QPushButton("← Previous")
        self._prev_btn.setEnabled(False)
        self._prev_btn.clicked.connect(self._on_prev_page)
        pagination_layout.addWidget(self._prev_btn)
        self._page_label = QLabel("Page 1")
        pagination_layout.addWidget(self._page_label)
        self._next_btn = QPushButton("Next →")
        self._next_btn.setEnabled(False)
        self._next_btn.clicked.connect(self._on_next_page)
        pagination_layout.addWidget(self._next_btn)
        layout.addLayout(pagination_layout)

    def load(self) -> None:
        """Load download audit data with current filters."""
        self._loaded = True

        if self._model is None:
            self._model = DownloadAuditTableModel(
                self.db_manager,
                self.evidence_id,
                self.evidence_label,
                parent=self,
            )
            self._table.setModel(self._model)
            self._table.setColumnWidth(0, 170)  # ts
            self._table.setColumnWidth(1, 360)  # url
            self._table.setColumnWidth(2, 70)   # method
            self._table.setColumnWidth(3, 90)   # outcome
            self._table.setColumnWidth(4, 60)   # http
            self._table.setColumnWidth(5, 70)   # attempts
            self._table.setColumnWidth(6, 90)   # bytes
            self._table.setColumnWidth(7, 140)  # ctype
            self._table.setColumnWidth(8, 90)   # duration
            self._table.setColumnWidth(9, 140)  # caller

        outcome = self._outcome_combo.currentData() or ""
        search_text = self._search_edit.text().strip()
        self._model.load(
            outcome_filter=outcome,
            search_text=search_text,
            limit=self._page_size,
            offset=self._page * self._page_size,
        )

        self._update_summary()
        self._update_pagination()

        if self._model.rowCount() == 0:
            self._table.hide()
            self._empty_label.show()
        else:
            self._empty_label.hide()
            self._table.show()

    def _update_summary(self) -> None:
        if self._model is None:
            return
        summary = self._model.get_summary()
        by_outcome = summary.get("by_outcome", {})
        self._total_label.setText(f"Total: {summary.get('total', 0):,}")
        self._success_label.setText(f"Success: {by_outcome.get('success', 0):,}")
        self._failed_label.setText(f"Failed: {by_outcome.get('failed', 0):,}")
        self._blocked_label.setText(f"Blocked: {by_outcome.get('blocked', 0):,}")
        self._cancelled_label.setText(f"Cancelled: {by_outcome.get('cancelled', 0):,}")
        self._error_label.setText(f"Error: {by_outcome.get('error', 0):,}")

    def _update_pagination(self) -> None:
        if self._model is None:
            return
        total = self._model.total_count
        showing = self._model.rowCount()
        start = self._page * self._page_size + 1 if showing > 0 else 0
        end = start + showing - 1 if showing > 0 else 0
        self._showing_label.setText(f"Showing {start:,}-{end:,} of {total:,}")

        total_pages = (total + self._page_size - 1) // self._page_size if total > 0 else 1
        self._page_label.setText(f"Page {self._page + 1} of {total_pages}")
        self._prev_btn.setEnabled(self._page > 0)
        self._next_btn.setEnabled(end < total)

    def _on_filter_changed(self) -> None:
        self._page = 0
        self.load()

    def _on_prev_page(self) -> None:
        if self._page > 0:
            self._page -= 1
            self.load()

    def _on_next_page(self) -> None:
        self._page += 1
        self.load()


class LogsSubtab(QWidget):
    """
    Logs subtab - displays per-evidence extraction logs.

    Moved from standalone Logs tab to Audit subtab.

    Shows logs from:
    - Current session (in-memory)
    - Previous sessions (loaded from persistent log file)
    """

    def __init__(
        self,
        evidence_id: int,
        evidence_label: str,
        case_path: Optional[Path],
        db_manager: Optional[DatabaseManager],
        audit_logger: Optional["AuditLogger"],
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.case_path = case_path
        self.db_manager = db_manager
        self.audit_logger = audit_logger

        self._setup_ui()
        self._load_persisted_logs()

    def _setup_ui(self) -> None:
        """Set up the UI components."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        # Log text widget
        self._log_widget = QTextEdit()
        self._log_widget.setReadOnly(True)
        self._log_widget.setPlaceholderText(
            "Logs for this evidence will appear here during extraction and ingestion operations."
        )
        layout.addWidget(self._log_widget)

    def _load_persisted_logs(self) -> None:
        """Load persisted logs from evidence log file."""
        if not (self.case_path and self.evidence_label and self.db_manager and self.audit_logger):
            return

        try:
            # Ensure the evidence database exists with migrations applied
            self.db_manager.get_evidence_conn(self.evidence_id, self.evidence_label)
            evidence_db_path = self.db_manager.evidence_db_path(self.evidence_id, self.evidence_label)
            evidence_logger = self.audit_logger.get_evidence_logger(self.evidence_id, evidence_db_path)

            # Load last 500 lines from log file
            persisted_lines = evidence_logger.tail(500)
            if persisted_lines:
                self._log_widget.setPlainText(
                    "--- Previous session logs ---\n" +
                    "\n".join(persisted_lines) +
                    "\n--- Current session ---\n"
                )
        except Exception as e:
            logger.debug(f"Could not load persisted logs for evidence {self.evidence_id}: {e}")

    @property
    def log_widget(self) -> QTextEdit:
        """Access the log widget for external log message routing."""
        return self._log_widget

    def append_log(self, message: str) -> None:
        """Append a log message to the widget."""
        self._log_widget.append(message)
        # Auto-scroll to bottom
        scrollbar = self._log_widget.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())


class AuditTab(QWidget):
    """
    Audit Tab - forensic audit data with subtabs.

    This is a per-evidence tab containing subtabs for different audit views.

    Subtabs:
    - Extraction: View extracted_files table
    - Warnings: View extraction warnings (schema discovery)
    - Download Audit: View investigator download outcomes
    - Statistics: View extractor run statistics (cards)
    - Logs: Per-evidence extraction logs
    """

    def __init__(
        self,
        db_manager: DatabaseManager,
        evidence_id: int,
        evidence_label: str,
        case_path: Optional[Path] = None,
        audit_logger: Optional["AuditLogger"] = None,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.case_path = case_path
        self.audit_logger = audit_logger

        # Lazy loading flags
        self._extraction_loaded = False
        self._statistics_loaded = False
        self._warnings_loaded = False
        self._download_audit_loaded = False
        # Logs subtab loads on creation (persisted logs)

        # Stale data flag for lazy refresh
        self._data_stale = False

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Set up the UI components."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Tab widget for subtabs
        self._tab_widget = QTabWidget()
        self._tab_widget.currentChanged.connect(self._on_subtab_changed)

        # Create subtabs
        # 1. Extraction subtab
        self._extraction_tab = ExtractionSubtab(
            self.db_manager,
            self.evidence_id,
            self.evidence_label,
            parent=self,
        )
        self._tab_widget.addTab(self._extraction_tab, "📦 Extraction")

        # 2. Warnings subtab
        self._warnings_tab = ExtractionWarningsSubtab(
            self.db_manager,
            self.evidence_id,
            self.evidence_label,
            parent=self,
        )
        self._tab_widget.addTab(self._warnings_tab, "⚠️ Warnings")

        # 3. Download Audit subtab
        self._download_audit_tab = DownloadAuditSubtab(
            self.db_manager,
            self.evidence_id,
            self.evidence_label,
            parent=self,
        )
        self._tab_widget.addTab(self._download_audit_tab, "⬇️ Download Audit")

        # 4. Statistics subtab (moved from standalone tab)
        self._statistics_tab = StatisticsSubtab(
            self.evidence_id,
            evidence_label=self.evidence_label,
            parent=self,
        )
        self._tab_widget.addTab(self._statistics_tab, "📈 Statistics")

        # 5. Logs subtab (- moved from standalone Logs tab)
        self._logs_tab = LogsSubtab(
            self.evidence_id,
            self.evidence_label,
            self.case_path,
            self.db_manager,
            self.audit_logger,
            parent=self,
        )
        self._tab_widget.addTab(self._logs_tab, "📜 Logs")

        layout.addWidget(self._tab_widget)

    def _on_subtab_changed(self, index: int) -> None:
        """Handle subtab change for lazy loading."""
        if index == 0 and not self._extraction_loaded:
            self._extraction_tab.load()
            self._extraction_loaded = True
        elif index == 1 and not self._warnings_loaded:
            self._warnings_tab.load()
            self._warnings_loaded = True
        elif index == 2 and not self._download_audit_loaded:
            self._download_audit_tab.load()
            self._download_audit_loaded = True
        elif index == 3 and not self._statistics_loaded:
            self._statistics_tab.refresh()
            self._statistics_loaded = True
        # Logs subtab (index 4) loads on creation, no lazy loading needed

    def load(self) -> None:
        """Load data for current subtab."""
        current_index = self._tab_widget.currentIndex()
        if current_index == 0:
            self._extraction_tab.load()
            self._extraction_loaded = True
        elif current_index == 1:
            self._warnings_tab.load()
            self._warnings_loaded = True
        elif current_index == 2:
            self._download_audit_tab.load()
            self._download_audit_loaded = True
        elif current_index == 3:
            self._statistics_tab.refresh()
            self._statistics_loaded = True

    @property
    def statistics_tab(self) -> StatisticsSubtab:
        """Access the statistics subtab (for external signal connections)."""
        return self._statistics_tab

    @property
    def logs_tab(self) -> LogsSubtab:
        """Access the logs subtab (for external log message routing)."""
        return self._logs_tab

    @property
    def log_widget(self) -> QTextEdit:
        """Access the log widget directly (for backward compatibility)."""
        return self._logs_tab.log_widget

    def mark_stale(self) -> None:
        """Mark data as stale - will refresh on next showEvent.

        Part of lazy refresh pattern to prevent UI freezes.
        Called by main.py when data changes but tab is not visible.
        """
        self._data_stale = True
        # Also mark statistics tab stale
        self._statistics_tab.mark_stale()

    def showEvent(self, event) -> None:
        """Load data on first show, refresh if stale."""
        super().showEvent(event)

        # Load on first show
        current_index = self._tab_widget.currentIndex()
        if current_index == 0 and not self._extraction_loaded:
            self._on_subtab_changed(0)
        elif current_index == 1 and not self._warnings_loaded:
            self._on_subtab_changed(1)
        elif current_index == 2 and not self._download_audit_loaded:
            self._on_subtab_changed(2)
        elif current_index == 3 and not self._statistics_loaded:
            self._on_subtab_changed(3)
        # Logs subtab (index 4) loads on creation

        # Refresh if marked stale
        if self._data_stale:
            self.load()
            self._data_stale = False
