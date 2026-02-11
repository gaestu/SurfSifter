"""
Available Downloads subtab - browse and select URLs for download.

Extracted from downloads/tab.py
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, Signal, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QCheckBox,
    QPushButton,
    QLabel,
    QLineEdit,
    QComboBox,
    QMessageBox,
)

from app.data.case_data import CaseDataAccess
from core.file_classifier import classify_file_type, FILE_TYPE_LABELS
from app.features.downloads.workers import AvailableUrlsWorker

logger = logging.getLogger(__name__)


class AvailableDownloadsPanel(QWidget):
    """Panel for browsing and selecting URLs to download."""

    download_requested = Signal(list)  # List of selected URL dicts

    def __init__(
        self,
        evidence_id: int,
        case_data: Optional[CaseDataAccess] = None,
        case_folder: Optional[Path] = None,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.evidence_id = evidence_id
        self.case_data = case_data
        self.case_folder = case_folder
        self.case_db_path = case_data.db_path if case_data else None

        self._available_items: List[Dict[str, Any]] = []
        self._total_count = 0
        self._worker: Optional[AvailableUrlsWorker] = None
        self._pending_workers: List[AvailableUrlsWorker] = []  # Keep old workers alive until finished
        self._worker_generation = 0  # Track worker generation to ignore stale results
        self._filter_timer = QTimer()
        self._filter_timer.setSingleShot(True)
        self._filter_timer.timeout.connect(self._apply_filters_delayed)

        # Pagination state
        self._current_page = 0
        self._page_size = 500

        # Guards against re-entrant calls and race conditions
        self._populating_filters = False  # Prevent filter signals during population
        self._refresh_pending = False  # Coalesce multiple refresh requests

        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        # Filters row
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel("Domain:"))
        self.domain_filter = QComboBox()
        self.domain_filter.setEditable(True)
        self.domain_filter.setMinimumWidth(150)
        self.domain_filter.addItem("All", "")
        self.domain_filter.currentIndexChanged.connect(self._on_filter_changed)
        self.domain_filter.lineEdit().textChanged.connect(self._on_filter_text_changed)
        filter_layout.addWidget(self.domain_filter)

        # Tag filter
        filter_layout.addWidget(QLabel("Tag:"))
        self.tag_filter = QComboBox()
        self.tag_filter.addItem("All Tags", "")
        self.tag_filter.setMinimumWidth(100)
        self.tag_filter.currentIndexChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self.tag_filter)

        # Match filter
        filter_layout.addWidget(QLabel("Match:"))
        self.match_filter = QComboBox()
        self.match_filter.addItem("All", "")
        self.match_filter.addItem("Matched Only", "matched")
        self.match_filter.addItem("Unmatched Only", "unmatched")
        self.match_filter.setMinimumWidth(100)
        self.match_filter.currentIndexChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self.match_filter)

        filter_layout.addWidget(QLabel("Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Filter by URL...")
        self.search_edit.setClearButtonEnabled(True)
        self.search_edit.textChanged.connect(self._on_filter_text_changed)
        filter_layout.addWidget(self.search_edit, 1)

        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        # Type filter row (radio-like buttons)
        type_layout = QHBoxLayout()
        type_layout.addWidget(QLabel("Type:"))

        self.type_buttons: Dict[str, QPushButton] = {}
        for type_key in ["all", "image", "video", "audio", "document", "archive"]:
            btn = QPushButton(FILE_TYPE_LABELS.get(type_key, type_key.title()))
            btn.setCheckable(True)
            btn.setChecked(type_key == "image")  # Default to images
            btn.clicked.connect(lambda checked, t=type_key: self._on_type_selected(t))
            self.type_buttons[type_key] = btn
            type_layout.addWidget(btn)

        type_layout.addStretch()
        layout.addLayout(type_layout)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels([
            "", "URL", "Domain", "Type", "Source", "Status"
        ])
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.table.setColumnWidth(0, 30)  # Checkbox column
        self.table.setColumnWidth(3, 80)
        self.table.setColumnWidth(4, 100)
        self.table.setColumnWidth(5, 80)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        layout.addWidget(self.table, 1)

        # Pagination row
        page_layout = QHBoxLayout()

        self.prev_btn = QPushButton("< Previous")
        self.prev_btn.clicked.connect(self._prev_page)
        page_layout.addWidget(self.prev_btn)

        self.page_label = QLabel("Page 1 of 1")
        page_layout.addWidget(self.page_label)

        self.next_btn = QPushButton("Next >")
        self.next_btn.clicked.connect(self._next_page)
        page_layout.addWidget(self.next_btn)

        page_layout.addWidget(QLabel("Per page:"))
        self.page_size_combo = QComboBox()
        self.page_size_combo.addItems(["100", "250", "500", "1000"])
        self.page_size_combo.setCurrentText("500")
        self.page_size_combo.currentTextChanged.connect(self._on_page_size_changed)
        page_layout.addWidget(self.page_size_combo)

        page_layout.addStretch()
        layout.addLayout(page_layout)

        # Selection controls
        selection_layout = QHBoxLayout()

        self.select_all_btn = QPushButton("Select All")
        self.select_all_btn.clicked.connect(self._select_all)
        selection_layout.addWidget(self.select_all_btn)

        self.select_none_btn = QPushButton("Select None")
        self.select_none_btn.clicked.connect(self._select_none)
        selection_layout.addWidget(self.select_none_btn)

        self.selected_label = QLabel("0 selected")
        selection_layout.addWidget(self.selected_label)

        selection_layout.addStretch()

        self.download_btn = QPushButton("⬇ Download Selected")
        self.download_btn.setStyleSheet("font-weight: bold; padding: 6px 16px;")
        self.download_btn.clicked.connect(self._on_download_clicked)
        selection_layout.addWidget(self.download_btn)

        layout.addLayout(selection_layout)

        # Status bar
        self.status_label = QLabel("Ready")
        layout.addWidget(self.status_label)

        self.download_btn.setToolTip("")

    def _on_type_selected(self, type_key: str):
        """Handle type filter button click."""
        # Skip if populating filters
        if self._populating_filters:
            return
        # Uncheck all other buttons
        for key, btn in self.type_buttons.items():
            btn.setChecked(key == type_key)
        # Use debounced apply
        self._filter_timer.start(100)

    def _get_selected_type(self) -> Optional[str]:
        """Get currently selected type filter."""
        for key, btn in self.type_buttons.items():
            if btn.isChecked():
                return None if key == "all" else key
        return "image"  # Default

    def _on_filter_changed(self):
        """Handle filter dropdown change."""
        # Skip if we're in the middle of populating filters
        if self._populating_filters:
            return
        # Use same debounce as text filter to prevent rapid-fire worker creation
        self._filter_timer.start(150)

    def _on_filter_text_changed(self):
        """Handle text filter change with debounce."""
        self._filter_timer.start(300)  # 300ms debounce

    def _apply_filters_delayed(self):
        """Apply filters after debounce."""
        self._apply_filters()

    def _apply_filters(self):
        """Load data with current filters."""
        if not self.case_data or not self.case_folder or not self.case_db_path:
            return

        # Increment generation - any in-flight workers with old generation will be ignored
        self._worker_generation += 1
        current_gen = self._worker_generation

        # Keep old worker alive until it finishes (prevents QThread crash)
        if self._worker and self._worker.isRunning():
            self._pending_workers.append(self._worker)

        # Clean up finished workers from pending list
        self._pending_workers = [w for w in self._pending_workers if w.isRunning()]

        # Fix domain filter bug: properly detect typed vs selected values
        domain = self.domain_filter.currentData()
        if domain == "" or domain is None:
            # User may have typed something in the editable combo
            typed = self.domain_filter.currentText().strip()
            if typed and typed != "All":
                domain = typed
            else:
                domain = None

        # Get tag filter
        tag_filter = self.tag_filter.currentData()
        if not tag_filter:
            tag_filter = None

        # Get match filter
        match_filter = self.match_filter.currentData()
        if not match_filter:
            match_filter = None

        self._worker = AvailableUrlsWorker(
            self.case_folder,
            self.case_db_path,
            self.evidence_id,
            file_type=self._get_selected_type(),
            domain_filter=domain if domain else None,
            search_text=self.search_edit.text() or None,
            tag_filter=tag_filter,
            match_filter=match_filter,
            limit=self._page_size,
            offset=self._current_page * self._page_size,
        )
        # Use lambda to capture generation for staleness check
        self._worker.finished.connect(
            lambda rows, count, gen=current_gen: self._on_data_loaded(rows, count, gen)
        )
        self._worker.error.connect(
            lambda err, gen=current_gen: self._on_load_error(err, gen)
        )
        # Show backfill progress during initial indexing
        self._worker.backfill_progress.connect(self._on_backfill_progress)
        self._worker.start()

        self.status_label.setText("Loading...")

    def _on_backfill_progress(self, message: str):
        """Handle backfill progress update from worker."""
        self.status_label.setText(message)

    def _on_data_loaded(self, rows: List[Dict], total: int, generation: int = 0):
        """Handle data loaded from worker."""
        # Ignore stale results from old workers
        if generation != self._worker_generation:
            logger.debug("Ignoring stale AvailableUrlsWorker result (gen %d vs current %d)", generation, self._worker_generation)
            return

        self._available_items = rows
        self._total_count = total
        self._populate_table()
        self._update_pagination()
        self.status_label.setText(f"{total} URLs available")

    def _on_load_error(self, error: str, generation: int = 0):
        """Handle load error."""
        # Ignore errors from stale workers
        if generation != self._worker_generation:
            return

        self.status_label.setText(f"Error: {error}")
        logger.error("Failed to load available URLs: %s", error)

    def _populate_table(self):
        """Populate table with available items.

        Updated to show grouped sources with count.
        """
        self.table.setRowCount(len(self._available_items))

        for row, item in enumerate(self._available_items):
            # Checkbox
            checkbox = QCheckBox()
            checkbox.stateChanged.connect(self._update_selected_count)
            self.table.setCellWidget(row, 0, checkbox)

            # URL
            url_item = QTableWidgetItem(item.get("url", ""))
            url_item.setToolTip(item.get("url", ""))
            url_item.setData(Qt.UserRole, item)  # Store full item data
            self.table.setItem(row, 1, url_item)

            # Domain
            self.table.setItem(row, 2, QTableWidgetItem(item.get("domain") or ""))

            # Type
            url = item.get("url", "")
            file_type = classify_file_type(url)
            self.table.setItem(row, 3, QTableWidgetItem(file_type))

            # Source - show primary source with count if multiple
            # discovered_by is now comma-separated from GROUP_CONCAT
            sources_str = item.get("discovered_by") or ""
            source_count = item.get("source_count", 1)
            source_item = QTableWidgetItem(self._format_sources(sources_str, source_count))
            source_item.setToolTip(sources_str.replace(",", ", ") if sources_str else "")
            self.table.setItem(row, 4, source_item)

            # Status
            status = item.get("download_status") or "—"
            self.table.setItem(row, 5, QTableWidgetItem(status))

    def _format_sources(self, sources_str: str, count: int) -> str:
        """Format sources for display.

        Shows 'primary_source (+N)' format when multiple sources.

        Args:
            sources_str: Comma-separated source names from GROUP_CONCAT
            count: Total number of URL occurrences

        Returns:
            Formatted string like 'bulk_extractor (+2)'
        """
        if not sources_str:
            return "—"

        sources = [s.strip() for s in sources_str.split(",") if s.strip()]
        if not sources:
            return "—"

        primary = sources[0]
        if len(sources) > 1:
            return f"{primary} (+{len(sources) - 1})"
        elif count > 1:
            # Same source found URL multiple times
            return f"{primary} (×{count})"
        return primary

    def _select_all(self):
        """Select all visible rows."""
        for row in range(self.table.rowCount()):
            if not self.table.isRowHidden(row):
                checkbox = self.table.cellWidget(row, 0)
                if checkbox:
                    checkbox.setChecked(True)
        self._update_selected_count()

    def _select_none(self):
        """Deselect all rows."""
        for row in range(self.table.rowCount()):
            checkbox = self.table.cellWidget(row, 0)
            if checkbox:
                checkbox.setChecked(False)
        self._update_selected_count()

    def _update_selected_count(self):
        """Update selected count label."""
        count = sum(
            1 for row in range(self.table.rowCount())
            if (cb := self.table.cellWidget(row, 0)) and cb.isChecked()
        )
        self.selected_label.setText(f"{count} selected")

    def _get_selected_items(self) -> List[Dict[str, Any]]:
        """Get list of selected URL items."""
        selected = []
        for row in range(self.table.rowCount()):
            checkbox = self.table.cellWidget(row, 0)
            if checkbox and checkbox.isChecked():
                url_item = self.table.item(row, 1)
                if url_item:
                    item_data = url_item.data(Qt.UserRole)
                    if item_data:
                        selected.append(item_data)
        return selected

    def _on_download_clicked(self):
        """Handle download button click."""
        selected = self._get_selected_items()
        if not selected:
            QMessageBox.information(
                self,
                "No Selection",
                "Please select at least one URL to download."
            )
            return

        # Confirm
        reply = QMessageBox.question(
            self,
            "Confirm Download",
            f"Download {len(selected)} item(s)?\n\nAll downloads will be logged with full metadata.",
            QMessageBox.Cancel | QMessageBox.Ok,
            QMessageBox.Ok,
        )

        if reply == QMessageBox.Ok:
            self.download_requested.emit(selected)

    # --- Pagination helpers ---

    @property
    def _total_pages(self) -> int:
        """Calculate total pages based on count and page size."""
        return max(1, (self._total_count + self._page_size - 1) // self._page_size)

    def _update_pagination(self):
        """Update pagination controls state."""
        total_pages = self._total_pages
        current = self._current_page + 1  # 1-indexed for display

        self.page_label.setText(f"Page {current} of {total_pages}")
        self.prev_btn.setEnabled(self._current_page > 0)
        self.next_btn.setEnabled(self._current_page < total_pages - 1)

    def _prev_page(self):
        """Go to previous page."""
        if self._current_page > 0:
            self._current_page -= 1
            self._apply_filters()

    def _next_page(self):
        """Go to next page."""
        if self._current_page < self._total_pages - 1:
            self._current_page += 1
            self._apply_filters()

    def _on_page_size_changed(self, text: str):
        """Handle page size change."""
        try:
            self._page_size = int(text)
            self._current_page = 0  # Reset to first page
            self._apply_filters()
        except ValueError:
            pass

    # --- Filter population ---

    def _populate_filters(self):
        """Populate domain, tag, and match filter dropdowns.

        Uses _populating_filters guard to prevent re-entrant signal handling.
        """
        if not self.case_data or self.evidence_id is None:
            return

        # Set guard to prevent filter change handlers from triggering during population
        self._populating_filters = True
        try:
            # Populate domain filter
            try:
                domains = self.case_data.list_url_domains(self.evidence_id, limit=200)
                self.domain_filter.blockSignals(True)
                current_domain = self.domain_filter.currentData()
                current_text = self.domain_filter.currentText()
                self.domain_filter.clear()
                self.domain_filter.addItem("All", "")
                for domain in sorted(domains):
                    if domain:  # Skip empty domains
                        self.domain_filter.addItem(domain, domain)
                # Restore selection if possible
                if current_domain:
                    idx = self.domain_filter.findData(current_domain)
                    if idx >= 0:
                        self.domain_filter.setCurrentIndex(idx)
                elif current_text and current_text != "All":
                    # User typed a custom value, restore it
                    self.domain_filter.setEditText(current_text)
                self.domain_filter.blockSignals(False)
            except Exception as e:
                logger.warning("Failed to load domains: %s", e)

            # Populate tag filter
            try:
                tags = self.case_data.list_tags(self.evidence_id)
                self.tag_filter.blockSignals(True)
                current_tag = self.tag_filter.currentData()
                self.tag_filter.clear()
                self.tag_filter.addItem("All Tags", "")
                for tag in tags:
                    self.tag_filter.addItem(tag["name"], tag["name"])
                # Restore selection if possible
                if current_tag:
                    idx = self.tag_filter.findData(current_tag)
                    if idx >= 0:
                        self.tag_filter.setCurrentIndex(idx)
                self.tag_filter.blockSignals(False)
            except Exception as e:
                logger.warning("Failed to load tags: %s", e)

            # Populate match filter with list names
            try:
                match_lists = self.case_data.list_url_match_lists(self.evidence_id)
                self.match_filter.blockSignals(True)
                current_match = self.match_filter.currentData()
                self.match_filter.clear()
                self.match_filter.addItem("All", "")
                self.match_filter.addItem("Matched Only", "matched")
                self.match_filter.addItem("Unmatched Only", "unmatched")
                for list_name in match_lists:
                    self.match_filter.addItem(f"List: {list_name}", list_name)
                # Restore selection if possible
                if current_match:
                    idx = self.match_filter.findData(current_match)
                    if idx >= 0:
                        self.match_filter.setCurrentIndex(idx)
                self.match_filter.blockSignals(False)
            except Exception as e:
                logger.warning("Failed to load match lists: %s", e)
        finally:
            self._populating_filters = False

    def set_case_data(self, case_data: CaseDataAccess, case_folder: Optional[Path] = None):
        """Update case data reference."""
        self.case_data = case_data
        self.case_db_path = case_data.db_path if case_data else None
        if case_folder:
            self.case_folder = case_folder
        # Populate filters after setting case data
        self._populate_filters()

    def refresh(self):
        """Refresh the data.

        Uses _refresh_pending to coalesce multiple rapid calls.
        """
        # Coalesce rapid refresh calls
        if self._refresh_pending:
            return
        self._refresh_pending = True

        self._current_page = 0  # Reset to first page on refresh
        self._populate_filters()
        self._apply_filters()

        self._refresh_pending = False

    def showEvent(self, event):
        """Refresh when panel becomes visible.

        Added guard to prevent refresh during filter population.
        """
        super().showEvent(event)
        # Skip if already refreshing or populating filters
        if self._populating_filters or self._refresh_pending:
            return
        if self.case_data and self.case_folder and not self._available_items:
            self._populate_filters()
            self._apply_filters()

    def shutdown(self) -> None:
        """Gracefully stop all background workers before widget destruction."""
        # Stop current worker
        if self._worker is not None:
            # Disconnect signals first to prevent callbacks during shutdown
            try:
                self._worker.finished.disconnect()
                self._worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            if self._worker.isRunning():
                self._worker.requestInterruption()
                self._worker.quit()
                if not self._worker.wait(2000):
                    logger.warning("AvailableUrlsWorker did not stop in 2s, terminating")
                    self._worker.terminate()
                    self._worker.wait(500)
            self._worker = None

        # Stop all pending workers
        for worker in self._pending_workers:
            try:
                worker.finished.disconnect()
                worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            if worker.isRunning():
                worker.requestInterruption()
                worker.quit()
                if not worker.wait(1000):
                    logger.warning("Pending AvailableUrlsWorker did not stop, terminating")
                    worker.terminate()
                    worker.wait(500)
        self._pending_workers.clear()

        logger.debug("AvailableDownloadsPanel shutdown complete")
