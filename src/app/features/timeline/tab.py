from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from PySide6.QtCore import Qt, Signal, QDate, QTimer, QThreadPool
from PySide6.QtWidgets import (
    QDateEdit,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QMenu,
    QInputDialog,
    QMessageBox,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
    QComboBox,
    QStackedWidget,
)

from app.data.case_data import CaseDataAccess
from app.features.timeline.models import TimelineTableModel


@dataclass
class TimelineFilters:
    kind: str = ""
    confidence: str = ""
    start_date: str = ""
    end_date: str = ""
    tag: str = "*"  #


class TimelineTab(QWidget):
    """Timeline tab showing unified timeline events from all artifact sources.

     (Phase 5): Added Build Timeline button, empty-state UX, fixed showEvent.
    """

    # Signal emitted when build completes (for main.py to update other tabs if needed)
    timeline_built = Signal(int)  # event count

    def __init__(self, case_data: Optional[CaseDataAccess] = None, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.case_data = case_data
        self.evidence_id: Optional[int] = None
        self.filters = TimelineFilters()

        # Phase 3: Lazy loading state
        self._data_loaded = False
        self._load_pending = False

        # Stale data flag for lazy refresh after ingestion
        self._data_stale = False

        # Phase 5: Build task tracking
        self._build_task = None

        main_layout = QVBoxLayout()

        # === Create stacked widget to switch between empty and data views ===
        self.stacked_widget = QStackedWidget()

        # --- Page 0: Empty state ---
        self.empty_state_widget = QWidget()
        empty_layout = QVBoxLayout()
        empty_layout.addStretch()

        self.empty_label = QLabel(
            "No timeline events found.\n\n"
            "Click 'Build Timeline' to generate a unified timeline\n"
            "from all extracted browser artifacts.\n\n"
            "Supported sources: browser history, cookies, bookmarks,\n"
            "downloads, sessions, autofill, credentials, media history,\n"
            "HSTS entries, and jump lists."
        )
        self.empty_label.setAlignment(Qt.AlignCenter)
        empty_layout.addWidget(self.empty_label)

        self.empty_build_button = QPushButton("Build Timeline")
        self.empty_build_button.clicked.connect(self._on_build_timeline)
        empty_layout.addWidget(self.empty_build_button, alignment=Qt.AlignCenter)

        empty_layout.addStretch()
        self.empty_state_widget.setLayout(empty_layout)

        # --- Page 1: Data view ---
        self.data_widget = QWidget()
        data_layout = QVBoxLayout()

        # Statistics panel at top
        stats_group = QGroupBox()
        stats_layout = QGridLayout()

        self.total_events_label = QLabel("Total Events:")
        stats_layout.addWidget(self.total_events_label, 0, 0)
        self.total_events_value = QLabel("0")
        stats_layout.addWidget(self.total_events_value, 0, 1)

        self.time_range_label = QLabel("Time Range:")
        stats_layout.addWidget(self.time_range_label, 0, 2)
        self.time_range_value = QLabel("—")
        stats_layout.addWidget(self.time_range_value, 0, 3)

        self.kinds_label = QLabel("Event Types:")
        stats_layout.addWidget(self.kinds_label, 1, 0)
        self.kinds_value = QLabel("—")
        self.kinds_value.setWordWrap(True)
        stats_layout.addWidget(self.kinds_value, 1, 1, 1, 3)

        stats_group.setLayout(stats_layout)
        data_layout.addWidget(stats_group)

        # Filter controls
        filter_layout = QGridLayout()

        self.kind_label = QLabel("Event Type:")
        filter_layout.addWidget(self.kind_label, 0, 0)
        self.kind_combo = QComboBox()
        self.kind_combo.addItem("", userData="")  # All
        self.kind_combo.currentIndexChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.kind_combo, 0, 1)

        self.confidence_label = QLabel("Confidence:")
        filter_layout.addWidget(self.confidence_label, 0, 2)
        self.confidence_combo = QComboBox()
        self.confidence_combo.addItem("", userData="")  # All
        self.confidence_combo.currentIndexChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.confidence_combo, 0, 3)

        self.tag_label = QLabel("Tag:")
        filter_layout.addWidget(self.tag_label, 0, 4)
        self.tag_combo = QComboBox()
        self.tag_combo.addItem("", userData="*")
        self.tag_combo.currentIndexChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.tag_combo, 0, 5)

        self.start_date_label = QLabel("Start Date:")
        filter_layout.addWidget(self.start_date_label, 1, 0)
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setCalendarPopup(True)
        self.start_date_edit.setDisplayFormat("yyyy-MM-dd")
        self.start_date_edit.setSpecialValueText(" ")  # Empty when cleared
        self.start_date_edit.dateChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.start_date_edit, 1, 1)

        self.end_date_label = QLabel("End Date:")
        filter_layout.addWidget(self.end_date_label, 1, 2)
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setCalendarPopup(True)
        self.end_date_edit.setDisplayFormat("yyyy-MM-dd")
        self.end_date_edit.setSpecialValueText(" ")
        self.end_date_edit.dateChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.end_date_edit, 1, 3)

        self.clear_dates_button = QPushButton("Clear Dates")
        self.clear_dates_button.clicked.connect(self._clear_date_filters)
        filter_layout.addWidget(self.clear_dates_button, 1, 4)

        data_layout.addLayout(filter_layout)

        # Timeline table
        self.model = TimelineTableModel(case_data)
        self.table_view = QTableView()
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setSelectionMode(QTableView.ExtendedSelection)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setModel(self.model)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        self.table_view.setSortingEnabled(False)  # Already sorted deterministically

        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._show_context_menu)

        data_layout.addWidget(self.table_view)

        # Controls at bottom
        controls_layout = QHBoxLayout()

        # Build Timeline button
        self.build_button = QPushButton("Build Timeline")
        self.build_button.clicked.connect(self._on_build_timeline)
        controls_layout.addWidget(self.build_button)

        self.tag_button = QPushButton("Tag Selected...")
        self.tag_button.clicked.connect(self._tag_selected)
        controls_layout.addWidget(self.tag_button)

        self.prev_button = QPushButton("◄ Previous")
        self.prev_button.clicked.connect(self._page_up)
        controls_layout.addWidget(self.prev_button)

        self.next_button = QPushButton("Next ►")
        self.next_button.clicked.connect(self._page_down)
        controls_layout.addWidget(self.next_button)

        self.page_label = QLabel()
        controls_layout.addWidget(self.page_label)

        controls_layout.addStretch()

        self.export_button = QPushButton("Export CSV...")
        self.export_button.clicked.connect(self._export_csv)
        controls_layout.addWidget(self.export_button)

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.clicked.connect(self._refresh)
        controls_layout.addWidget(self.refresh_button)

        data_layout.addLayout(controls_layout)
        self.data_widget.setLayout(data_layout)

        # --- Add pages to stacked widget ---
        self.stacked_widget.addWidget(self.empty_state_widget)  # Index 0
        self.stacked_widget.addWidget(self.data_widget)         # Index 1

        main_layout.addWidget(self.stacked_widget)
        self.setLayout(main_layout)

        self._update_page_label()
        self._update_filter_combos()

    def set_case_data(self, case_data: Optional[CaseDataAccess], defer_load: bool = False) -> None:
        """
        Set the case data access object.

        Args:
            case_data: CaseDataAccess instance
            defer_load: If True, defer data loading until tab is visible (Phase 3)
        """
        self.case_data = case_data
        self.model.set_case_data(case_data)
        self.evidence_id = None
        if not defer_load:
            self._update_stats()
            self._update_page_label()
        else:
            self._data_loaded = False
            self._load_pending = True

    def set_evidence(self, evidence_id: Optional[int], defer_load: bool = False) -> None:
        """
        Set the current evidence ID.

        Args:
            evidence_id: Evidence ID to display
            defer_load: If True, defer data loading until tab is visible (Phase 3)
        """
        self.evidence_id = evidence_id
        self._data_loaded = False

        if not defer_load:
            # Immediate loading (legacy behavior)
            self.model.set_evidence(evidence_id)
            self._populate_filter_options()
            self._update_stats()
            self._update_page_label()
        else:
            # Deferred loading - just store the ID, load on showEvent
            self._load_pending = True

    def showEvent(self, event):
        """Override showEvent to perform lazy loading when tab becomes visible."""
        super().showEvent(event)

        # Phase 3: Lazy loading - load data on first visibility
        if self._load_pending and not self._data_loaded:
            QTimer.singleShot(10, self._perform_deferred_load)
        # Refresh if data was marked stale while tab was hidden
        elif self._data_stale and self._data_loaded:
            self._data_stale = False
            QTimer.singleShot(10, self.refresh)

    def _perform_deferred_load(self) -> None:
        """Perform the deferred data loading."""
        if self._data_loaded:
            return

        self._data_loaded = True
        self._load_pending = False

        # Now actually load the data
        if self.evidence_id is not None:
            self.model.set_evidence(self.evidence_id)
            self._populate_filter_options()
        self._update_stats()
        self._update_page_label()

    def _populate_filter_options(self) -> None:
        """Populate filter dropdowns with available values from the data."""
        if not self.case_data or self.evidence_id is None:
            return

        # Populate kinds
        self.kind_combo.blockSignals(True)
        self.kind_combo.clear()
        self.kind_combo.addItem("All Types", userData="")

        kinds = self.case_data.get_timeline_kinds(int(self.evidence_id))
        for kind in kinds:
            display_name = self._format_kind_name(kind)
            self.kind_combo.addItem(display_name, userData=kind)

        self.kind_combo.blockSignals(False)

        # Populate confidences
        self.confidence_combo.blockSignals(True)
        self.confidence_combo.clear()
        self.confidence_combo.addItem("All Confidences", userData="")

        confidences = self.case_data.get_timeline_confidences(int(self.evidence_id))
        for conf in confidences:
            self.confidence_combo.addItem(conf.title(), userData=conf)

        self.confidence_combo.blockSignals(False)

        # Populate tags
        self.tag_combo.blockSignals(True)
        self.tag_combo.clear()
        self.tag_combo.addItem("All Tags", userData="*")

        tags = self.case_data.list_tags(int(self.evidence_id))
        for tag in tags:
            self.tag_combo.addItem(tag["name"], userData=tag["name"])

        self.tag_combo.blockSignals(False)

    def _format_kind_name(self, kind: str) -> str:
        """Convert snake_case kind to display name."""
        return kind.replace("_", " ").title()

    def _update_filter_combos(self) -> None:
        """Update combo box labels after translation."""
        # Re-set "All" items with translated text
        if self.kind_combo.count() > 0:
            self.kind_combo.setItemText(0, "All Types")
        if self.confidence_combo.count() > 0:
            self.confidence_combo.setItemText(0, "All Confidences")
        if self.tag_combo.count() > 0:
            self.tag_combo.setItemText(0, "All Tags")

    def _update_stats(self) -> None:
        """Update statistics panel and toggle between empty/data views."""
        if not self.case_data or self.evidence_id is None:
            self.total_events_value.setText("0")
            self.time_range_value.setText("—")
            self.kinds_value.setText("—")
            self.stacked_widget.setCurrentIndex(0)  # Empty state
            return

        stats = self.case_data.get_timeline_stats(int(self.evidence_id))
        total = stats.get("total_events", 0)

        self.total_events_value.setText(str(total))

        earliest = stats.get("earliest")
        latest = stats.get("latest")
        if earliest and latest:
            time_range = f"{earliest[:10]} → {latest[:10]}"
            self.time_range_value.setText(time_range)
        else:
            self.time_range_value.setText("—")

        by_kind = stats.get("by_kind", {})
        if by_kind:
            kind_summary = ", ".join([f"{self._format_kind_name(k)}: {c}" for k, c in sorted(by_kind.items())])
            self.kinds_value.setText(kind_summary)
        else:
            self.kinds_value.setText("—")

        # Toggle between empty state and data view
        if total == 0:
            self.stacked_widget.setCurrentIndex(0)  # Empty state
        else:
            self.stacked_widget.setCurrentIndex(1)  # Data view

    def _update_page_label(self) -> None:
        """Update pagination label."""
        page = self.model.page + 1
        self.page_label.setText(f"Page {page}")

    def _on_filters_changed(self) -> None:
        """Apply filters and reload."""
        kind = self.kind_combo.currentData() or ""
        confidence = self.confidence_combo.currentData() or ""
        tag = self.tag_combo.currentData() or "*"

        start_date = ""
        if self.start_date_edit.date() != QDate(2000, 1, 1):  # Not default
            start_date = self.start_date_edit.date().toString("yyyy-MM-dd")

        end_date = ""
        if self.end_date_edit.date() != QDate(2099, 12, 31):  # Not default
            end_date = self.end_date_edit.date().toString("yyyy-MM-dd")

        self.filters.kind = kind
        self.filters.confidence = confidence
        self.filters.start_date = start_date
        self.filters.end_date = end_date
        self.filters.tag = tag

        self.model.set_filters(
            kind=kind if kind else None,
            confidence=confidence if confidence else None,
            start_date=start_date if start_date else None,
            end_date=end_date if end_date else None,
            tag=tag if tag and tag != "*" else None,
        )

    def _clear_date_filters(self) -> None:
        """Clear date range filters."""
        self.start_date_edit.setDate(QDate(2000, 1, 1))
        self.end_date_edit.setDate(QDate(2099, 12, 31))
        self._on_filters_changed()

    def _page_up(self) -> None:
        self.model.page_up()
        self._update_page_label()

    def _page_down(self) -> None:
        self.model.page_down()
        self._update_page_label()

    def _refresh(self) -> None:
        """Refresh timeline data (internal method called by refresh button)."""
        self.model.reload()
        self._update_stats()

    def refresh(self) -> None:
        """Public refresh method for external calls (matches other tabs pattern)."""
        self._refresh()

    def mark_stale(self) -> None:
        """Mark data as stale - will refresh on next showEvent.

        Part of lazy refresh pattern to prevent UI freezes.
        Called by main.py when data changes but tab is not visible.
        """
        self._data_stale = True

    # NOTE: Duplicate showEvent removed in
    # Only the showEvent in _perform_deferred_load path is used now

    def _show_context_menu(self, pos) -> None:
        """Show context menu for table."""
        index = self.table_view.indexAt(pos)
        if not index.isValid():
            return

        menu = QMenu(self)
        tag_action = menu.addAction("Tag Selected...")
        tag_action.triggered.connect(self._tag_selected)

        menu.exec(self.table_view.viewport().mapToGlobal(pos))

    def _tag_selected(self) -> None:
        """Apply tags to selected rows."""
        if not self.case_data or self.evidence_id is None:
            return

        selection = self.table_view.selectionModel().selectedRows()
        if not selection:
            QMessageBox.warning(self, "Tagging", "No events selected.")
            return

        # Get current tags from first selected item to pre-fill
        first_idx = selection[0]
        # Column 6 is Tags (0-based index)
        current_tags = self.model.data(self.model.index(first_idx.row(), 6), Qt.DisplayRole)

        tags, ok = QInputDialog.getText(
            self,
            "Tag Events",
            "Enter tags (comma separated):",
            text=current_tags
        )

        if ok:
            count = 0
            for idx in selection:
                row_data = self.model.data(idx, Qt.UserRole)
                if row_data and "id" in row_data:
                    self.case_data.update_timeline_tags(
                        int(self.evidence_id),
                        row_data["id"],
                        tags
                    )
                    count += 1

            self._refresh()
            self._populate_filter_options()  # Update tag filter list
            QMessageBox.information(
                self,
                "Tagging Complete",
                f"Updated tags for {count} events.",
            )

    def _export_csv(self) -> None:
        """Export current timeline view to CSV."""
        if not self.case_data or self.evidence_id is None:
            QMessageBox.warning(self, "Export Timeline", "No timeline data to export.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Timeline",
            f"timeline_evidence_{self.evidence_id}.csv",
            "CSV Files (*.csv)"
        )

        if not path:
            return

        try:
            self.case_data.export_timeline_csv(
                int(self.evidence_id),
                Path(path),
                filters={
                    "kind": self.filters.kind if self.filters.kind else None,
                    "confidence": self.filters.confidence if self.filters.confidence else None,
                    "start_date": self.filters.start_date if self.filters.start_date else None,
                    "end_date": self.filters.end_date if self.filters.end_date else None,
                    "tag": self.filters.tag if self.filters.tag and self.filters.tag != "*" else None,
                }
            )
            QMessageBox.information(
                self,
                "Export Complete",
                f"Timeline exported to:\n{path}",
            )
        except Exception as exc:
            QMessageBox.critical(
                self,
                "Export Failed",
                f"Failed to export timeline:\n{exc}",
            )

    def current_filters(self) -> Dict[str, Any]:
        """Return current filter state for exports."""
        return {
            "kind": self.filters.kind if self.filters.kind else None,
            "confidence": self.filters.confidence if self.filters.confidence else None,
            "start_date": self.filters.start_date if self.filters.start_date else None,
            "end_date": self.filters.end_date if self.filters.end_date else None,
            "tag": self.filters.tag if self.filters.tag and self.filters.tag != "*" else None,
        }

    # =========================================================================
    # Build Timeline (- Phase 5)
    # =========================================================================

    def _on_build_timeline(self) -> None:
        """Start timeline build in background thread via QThreadPool."""
        from app.services.workers import TimelineBuildTask, TimelineBuildConfig

        if not self.case_data or self.evidence_id is None:
            QMessageBox.warning(
                self,
                "Build Timeline",
                "Please select an evidence first."
            )
            return

        # Timeline config is now hardcoded, no rules_dir needed
        config = TimelineBuildConfig(
            case_root=self.case_data.case_folder,
            db_path=self.case_data.db_path,
            evidence_id=int(self.evidence_id),
            db_manager=self.case_data.db_manager,
        )

        # Create task and wire signals
        self._build_task = TimelineBuildTask(config)
        self._build_task.signals.progress.connect(self._on_build_progress)
        self._build_task.signals.result.connect(self._on_build_complete)
        self._build_task.signals.error.connect(self._on_build_error)

        # Disable build buttons during build
        self.build_button.setEnabled(False)
        self.build_button.setText("Building...")
        self.empty_build_button.setEnabled(False)
        self.empty_build_button.setText("Building...")

        # Start via QThreadPool
        QThreadPool.globalInstance().start(self._build_task)

    def _on_build_progress(self, percent: int, message: str) -> None:
        """Update progress during timeline build."""
        # Could update a progress bar or status label
        # For now, just update button text with percentage
        self.build_button.setText(f"Building... ({percent}%)")
        self.empty_build_button.setText(f"Building... ({percent}%)")

    def _on_build_complete(self, count: int) -> None:
        """Handle successful timeline build."""
        self.build_button.setEnabled(True)
        self.build_button.setText("Build Timeline")
        self.empty_build_button.setEnabled(True)
        self.empty_build_button.setText("Build Timeline")

        # Reload timeline data
        self.refresh()
        self._populate_filter_options()

        # Emit signal for main.py to react if needed
        self.timeline_built.emit(count)

        QMessageBox.information(
            self,
            "Timeline Built",
            f"Successfully built timeline with {count:,} events."
        )

    def _on_build_error(self, error_msg: str, traceback_str: str) -> None:
        """Handle timeline build failure."""
        self.build_button.setEnabled(True)
        self.build_button.setText("Build Timeline")
        self.empty_build_button.setEnabled(True)
        self.empty_build_button.setText("Build Timeline")

        QMessageBox.critical(
            self,
            "Timeline Build Failed",
            f"Failed to build timeline:\n{error_msg}"
        )
