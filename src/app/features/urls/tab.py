from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, List

from PySide6.QtCore import Qt, Signal, QThread, QTimer
from PySide6.QtWidgets import (
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QTableView,
    QTreeView,
    QVBoxLayout,
    QWidget,
    QComboBox,
    QCheckBox,
    QStackedWidget,
    QProgressDialog,
    QDialog,
    QApplication,
)

from app.data.case_data import CaseDataAccess
from app.features.urls.models import UrlsTableModel, UrlsGroupedModel
from app.services.matching_workers import UrlMatchWorker
from app.common import add_sandbox_url_actions

logger = logging.getLogger(__name__)


class FilterLoadWorker(QThread):
    """
    Background worker for loading filter dropdown values.

    Runs expensive DISTINCT queries off the main thread to prevent UI freezing.
    Performance optimization for large databases.
    """

    finished = Signal(dict)  # {domains: [...], sources: [...], match_lists: [...], tags: [...]}
    error = Signal(str)

    # Limit domain dropdown to top N to prevent UI slowdown with 16k+ domains
    MAX_DOMAINS = 200

    def __init__(self, case_folder: Path, case_db_path: Path, evidence_id: int):
        super().__init__()
        self.case_folder = case_folder
        self.case_db_path = case_db_path
        self.evidence_id = evidence_id

    def run(self):
        """Load filter values in background thread."""
        import time
        start_time = time.time()

        try:
            if self.isInterruptionRequested():
                return

            with CaseDataAccess(self.case_folder, self.case_db_path) as case_data:
                # Load domains with count, limited to top N
                domains = self._load_top_domains(case_data)
                if self.isInterruptionRequested():
                    return

                # Load sources (usually small list)
                sources = case_data.list_url_sources(self.evidence_id)
                if self.isInterruptionRequested():
                    return

                # Load match lists (usually small list)
                try:
                    match_lists = case_data.list_url_match_lists(self.evidence_id)
                except Exception:
                    match_lists = []
                if self.isInterruptionRequested():
                    return

                # Load tags (usually small list)
                tags = case_data.list_tags(self.evidence_id)
                if self.isInterruptionRequested():
                    return

            elapsed = time.time() - start_time
            logger.info(
                "FilterLoadWorker: Loaded filters in %.3fs (domains=%d, sources=%d, match_lists=%d, tags=%d)",
                elapsed, len(domains), len(sources), len(match_lists), len(tags)
            )

            self.finished.emit({
                "domains": domains,
                "sources": sources,
                "match_lists": match_lists,
                "tags": tags,
            })

        except Exception as e:
            logger.error("FilterLoadWorker error: %s", e, exc_info=True)
            self.error.emit(str(e))

    def _load_top_domains(self, case_data: CaseDataAccess) -> List[Dict[str, Any]]:
        """Load top domains by frequency instead of all domains."""
        # Use case_data helper for clean data access
        return case_data.get_top_domains(self.evidence_id, limit=self.MAX_DOMAINS)


class DataLoadWorker(QThread):
    """
    Background worker for loading initial page data.

    Phase 2.1 - Runs count + iter_urls off the main thread to prevent
    UI freezing when loading the first page of data for large databases.
    """

    finished = Signal(list, int)  # (rows, total_count)
    error = Signal(str)

    def __init__(
        self,
        case_folder: Path,
        case_db_path: Path,
        evidence_id: int,
        filters: dict,
        page_size: int = 10000,
        page: int = 0,
    ):
        super().__init__()
        self.case_folder = case_folder
        self.case_db_path = case_db_path
        self.evidence_id = evidence_id
        self.filters = filters
        self.page_size = page_size
        self.page = page

    def run(self):
        """Load page data in background thread."""
        import time
        start_time = time.time()

        try:
            if self.isInterruptionRequested():
                return

            with CaseDataAccess(self.case_folder, self.case_db_path) as case_data:
                # Get total count first (fast query)
                total_count = case_data.count_urls(
                    self.evidence_id,
                    domain_like=self.filters.get("domain", "%"),
                    url_like=self.filters.get("url", "%"),
                    tag_like=self.filters.get("tag", "%"),
                    discovered_by=self.filters.get("sources"),
                    match_filter=self.filters.get("match_filter"),
                )
                if self.isInterruptionRequested():
                    return

                # Load page data
                rows = case_data.iter_urls(
                    self.evidence_id,
                    domain_like=self.filters.get("domain", "%"),
                    url_like=self.filters.get("url", "%"),
                    tag_like=self.filters.get("tag", "%"),
                    discovered_by=self.filters.get("sources"),
                    match_filter=self.filters.get("match_filter"),
                    limit=self.page_size,
                    offset=self.page * self.page_size,
                )
                if self.isInterruptionRequested():
                    return

            elapsed = time.time() - start_time
            logger.info(
                "DataLoadWorker: Loaded %d rows (total=%d) in %.3fs for evidence_id=%s",
                len(rows), total_count, elapsed, self.evidence_id
            )

            self.finished.emit(rows, total_count)

        except Exception as e:
            logger.error("DataLoadWorker error: %s", e, exc_info=True)
            self.error.emit(str(e))


@dataclass
class UrlFilters:
    domain: str = ""
    text: str = ""
    source: str = ""
    match: str = "all"  # "all", "matched", "unmatched", or specific list name
    tag: str = "*"  #


class UrlsTab(QWidget):
    # downloadRequested signal removed - downloads now handled in Download tab

    def __init__(self, case_data: Optional[CaseDataAccess] = None, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.case_data = case_data
        self.evidence_id: Optional[int] = None
        self.filters = UrlFilters()

        # Cache evidence label for forensic context
        self._evidence_label_cache: Optional[str] = None

        # Lazy loading state (Phase 3)
        self._data_loaded = False
        self._load_pending = False

        # Stale data flag for lazy refresh after ingestion
        self._data_stale = False

        # Background filter loading (performance optimization)
        self._filter_worker: Optional[FilterLoadWorker] = None
        self._pending_filter_workers: List[FilterLoadWorker] = []
        self._filter_worker_generation = 0
        self._filters_loading = False

        # Background data loading (Phase 2.1)
        self._data_worker: Optional[DataLoadWorker] = None
        self._pending_data_workers: List[DataLoadWorker] = []
        self._data_worker_generation = 0
        self._data_loading = False

        # URL match worker lifecycle
        self.match_worker: Optional[UrlMatchWorker] = None
        self._pending_match_workers: List[UrlMatchWorker] = []
        self._match_worker_generation = 0
        self.progress_dialog: Optional[QProgressDialog] = None

        layout = QVBoxLayout()

        filter_layout = QGridLayout()
        self.domain_label = QLabel("Domain")
        filter_layout.addWidget(self.domain_label, 0, 0)
        self.domain_combo = QComboBox()
        self.domain_combo.addItem("All domains", userData="*")
        self.domain_combo.currentIndexChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.domain_combo, 0, 1)

        self.search_label = QLabel("Search")
        filter_layout.addWidget(self.search_label, 0, 2)

        # Search field + button layout (like FileListTab)
        search_layout = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Search URLs...")
        self.search_edit.returnPressed.connect(self._on_search_clicked)  # Performance: Trigger on Enter
        search_layout.addWidget(self.search_edit)

        self.search_button = QPushButton("Search")
        self.search_button.setToolTip("Click to apply search filter")
        self.search_button.clicked.connect(self._on_search_clicked)
        search_layout.addWidget(self.search_button)

        filter_layout.addLayout(search_layout, 0, 3)

        self.source_label = QLabel("Source")
        filter_layout.addWidget(self.source_label, 1, 0)
        self.source_combo = QComboBox()
        self.source_combo.addItem("All sources", userData="")
        self.source_combo.currentIndexChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.source_combo, 1, 1)

        self.match_label = QLabel("Match")  # Initialize with text
        filter_layout.addWidget(self.match_label, 1, 2)  #
        self.match_combo = QComboBox()  #
        self.match_combo.addItem("All URLs", userData="all")
        self.match_combo.addItem("Matched", userData="matched")
        self.match_combo.addItem("Not Matched", userData="unmatched")
        self.match_combo.currentIndexChanged.connect(self._on_filters_changed)  #
        filter_layout.addWidget(self.match_combo, 1, 3)  #

        self.tag_label = QLabel("Tag")
        filter_layout.addWidget(self.tag_label, 2, 0)
        self.tag_combo = QComboBox()
        self.tag_combo.addItem("All tags", userData="*")
        self.tag_combo.currentIndexChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.tag_combo, 2, 1)

        self.group_checkbox = QCheckBox("Group by domain")  # Initialize with text
        self.group_checkbox.toggled.connect(self._toggle_grouping)
        filter_layout.addWidget(self.group_checkbox, 2, 2)  # moved to col 2

        self.match_button = QPushButton("Match Against URL Lists")  # Initialize with text
        self.match_button.clicked.connect(self._match_against_url_lists)  #
        filter_layout.addWidget(self.match_button, 2, 3)  # moved to col 3

        layout.addLayout(filter_layout)

        self.model = UrlsTableModel(case_data)
        self.group_model = UrlsGroupedModel(case_data)
        self.group_model.itemChanged.connect(self._on_group_item_changed)

        self.table_view = QTableView()
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setSelectionMode(QTableView.ExtendedSelection)  # Allow multi-select for tagging
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setModel(self.model)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        # Context menu for tagging
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._show_context_menu)

        self.tree_view = QTreeView()
        self.tree_view.setAlternatingRowColors(True)
        self.tree_view.setUniformRowHeights(True)
        self.tree_view.setModel(self.group_model)
        self.tree_view.setItemsExpandable(True)
        self.tree_view.setRootIsDecorated(True)

        self.views = QStackedWidget()
        self.views.addWidget(self.table_view)
        self.views.addWidget(self.tree_view)
        layout.addWidget(self.views)

        controls_layout = QHBoxLayout()
        self.prev_button = QPushButton("Previous")
        self.prev_button.clicked.connect(self._page_up)
        self.next_button = QPushButton("Next")
        self.next_button.clicked.connect(self._page_down)
        self.page_label = QLabel()

        self.export_button = QPushButton("Export CSV")
        self.export_button.clicked.connect(self._export_csv)
        self.tag_selected_button = QPushButton("Tag Selected")  # Tag button
        self.tag_selected_button.clicked.connect(self._tag_selected)
        # Deduplicate button
        self.deduplicate_button = QPushButton("Deduplicate...")
        self.deduplicate_button.setToolTip("Remove duplicate URL entries based on configurable constraints")
        self.deduplicate_button.clicked.connect(self._show_deduplicate_dialog)
        # Download buttons removed - downloads now handled in Download tab

        controls_layout.addWidget(self.prev_button)
        controls_layout.addWidget(self.next_button)
        controls_layout.addWidget(self.page_label)
        controls_layout.addStretch()
        controls_layout.addWidget(self.deduplicate_button)  #
        controls_layout.addWidget(self.tag_selected_button)  #
        controls_layout.addWidget(self.export_button)
        # Download buttons removed
        layout.addLayout(controls_layout)

        self.setLayout(layout)
        self._update_page_label()

    # Public API ---------------------------------------------------------

    def set_case_data(self, case_data: Optional[CaseDataAccess], defer_load: bool = False) -> None:
        """
        Set the case data access object.

        Args:
            case_data: CaseDataAccess instance
            defer_load: If True, defer data loading until tab is visible (Phase 3)
        """
        self.case_data = case_data
        self.model.set_case_data(case_data)
        self.group_model.set_case_data(case_data)
        if not defer_load:
            self._populate_filters()
            self._update_page_label()
        else:
            self._data_loaded = False
            self._load_pending = True

    def set_evidence(self, evidence_id: Optional[int], defer_load: bool = False, evidence_label: Optional[str] = None) -> None:
        """
        Set the current evidence ID.

        Args:
            evidence_id: Evidence ID to display
            defer_load: If True, defer data loading until tab is visible (Phase 3)
            evidence_label: Optional evidence label for forensic context
        """
        self.evidence_id = evidence_id
        self._evidence_label_cache = evidence_label  #
        self._data_loaded = False

        if not defer_load:
            # Immediate loading (legacy behavior)
            self.model.set_evidence(evidence_id)
            self.group_model.set_evidence(evidence_id)
            self._populate_filters()
            self._update_page_label()
        else:
            # Deferred loading - just store the ID, load on showEvent
            self._load_pending = True

    def refresh(self) -> None:
        self.model.reload()
        self.group_model.reload()
        self._update_page_label()

    def _get_evidence_label(self) -> Optional[str]:
        """Get the evidence label for forensic context.

        Required for screenshot saving in sandbox browser.
        """
        if self._evidence_label_cache:
            return self._evidence_label_cache

        # Try to look it up from the case database
        if self.case_data and self.evidence_id is not None:
            try:
                evidence = self.case_data.get_evidence(self.evidence_id)
                if evidence and evidence.get("label"):
                    self._evidence_label_cache = evidence["label"]
                    return self._evidence_label_cache
            except Exception:
                pass

        return None

    def _get_case_folder(self) -> Optional[Path]:
        """Get the case folder path for forensic context.

        Required for screenshot saving in sandbox browser.
        """
        if self.case_data and hasattr(self.case_data, 'case_folder'):
            return self.case_data.case_folder
        return None

    def mark_stale(self) -> None:
        """Mark data as stale - will refresh on next showEvent.

        Part of lazy refresh pattern to prevent UI freezes.
        Called by main.py when data changes but tab is not visible.
        """
        self._data_stale = True

    def showEvent(self, event):
        """Override showEvent to perform lazy loading when tab becomes visible."""
        super().showEvent(event)

        # Phase 3: Lazy loading - load data on first visibility
        if self._load_pending and not self._data_loaded:
            # Use a short timer to let the UI paint first
            QTimer.singleShot(10, self._perform_deferred_load)
        # Refresh if data was marked stale while tab was hidden
        elif self._data_stale and self._data_loaded:
            self._data_stale = False
            QTimer.singleShot(10, self.refresh)

    def _perform_deferred_load(self) -> None:
        """
        Perform the deferred data loading.

        Uses non-blocking pattern:
        1. Set evidence IDs on models WITHOUT triggering reload
        2. Show loading indicators
        3. Start background filter loading (_populate_filters)
        4. When filters load, _on_filters_loaded triggers _apply_filters
        5. _apply_filters now uses background data loading
        """
        if self._data_loaded:
            return

        self._data_loaded = True
        self._load_pending = False

        # Set evidence IDs on models WITHOUT triggering immediate reload
        # The actual data load happens after filters are loaded
        if self.evidence_id is not None:
            self.model.set_evidence(self.evidence_id, reload=False)
            self.group_model.set_evidence(self.evidence_id, reload=False)

        # Start background filter loading - _on_filters_loaded will trigger data load
        self._populate_filters()
        self._update_page_label()

    # Helpers ------------------------------------------------------------

    def _populate_filters(self) -> None:
        """
        Populate filter dropdowns (now uses background loading).

        Shows "Loading..." placeholders while expensive DISTINCT queries run
        in a background thread to prevent UI freezing.
        """
        if not self.case_data or self.evidence_id is None:
            self._set_filters_empty()
            return

        # Increment generation; any stale worker callbacks are ignored.
        self._filter_worker_generation += 1
        current_gen = self._filter_worker_generation

        # Keep old worker alive until it finishes (prevents QThread destruction crash).
        if self._filter_worker is not None:
            if self._filter_worker.isRunning():
                self._pending_filter_workers.append(self._filter_worker)
            else:
                self._filter_worker.deleteLater()
            self._filter_worker = None
        self._pending_filter_workers = [w for w in self._pending_filter_workers if w.isRunning()]

        # Show loading state
        self._set_filters_loading()

        case_folder = getattr(self.case_data, "case_folder", None)
        case_db_path = getattr(self.case_data, "db_path", None)
        if case_folder is None or case_db_path is None:
            self._on_filters_load_error("Case context unavailable for background filter loading", current_gen)
            return

        # Start background loading
        self._filter_worker = FilterLoadWorker(Path(case_folder), Path(case_db_path), int(self.evidence_id))
        self._filter_worker.finished.connect(
            lambda data, gen=current_gen: self._on_filters_loaded(data, gen)
        )
        self._filter_worker.error.connect(
            lambda error, gen=current_gen: self._on_filters_load_error(error, gen)
        )
        self._filter_worker.start()

        logger.debug("Started background filter loading for evidence_id=%s", self.evidence_id)

    def _set_filters_empty(self) -> None:
        """Set filters to empty state (no case data)."""
        self.domain_combo.blockSignals(True)
        self.domain_combo.clear()
        self.domain_combo.addItem("All domains", userData="*")
        self.domain_combo.blockSignals(False)

        self.source_combo.blockSignals(True)
        self.source_combo.clear()
        self.source_combo.addItem("All sources", userData="")
        self.source_combo.blockSignals(False)

        self.match_combo.blockSignals(True)
        self.match_combo.clear()
        self.match_combo.addItem("All", userData="all")
        self.match_combo.addItem("Matched", userData="matched")
        self.match_combo.addItem("Not Matched", userData="unmatched")
        self.match_combo.blockSignals(False)

        self.tag_combo.blockSignals(True)
        self.tag_combo.clear()
        self.tag_combo.addItem("All tags", userData="*")
        self.tag_combo.blockSignals(False)

        self._on_filters_changed()
        self.group_model.set_case_data(None)
        self.group_model.set_evidence(None)

    def _set_filters_loading(self) -> None:
        """Set filters to loading state with placeholders."""
        self._filters_loading = True

        # Show loading placeholder in domain combo (most expensive query)
        self.domain_combo.blockSignals(True)
        self.domain_combo.clear()
        self.domain_combo.addItem("Loading domains...", userData="__loading__")
        self.domain_combo.setEnabled(False)
        self.domain_combo.blockSignals(False)

        # Source, match, and tag combos keep defaults (fast queries)
        self.source_combo.blockSignals(True)
        self.source_combo.clear()
        self.source_combo.addItem("Loading...", userData="__loading__")
        self.source_combo.setEnabled(False)
        self.source_combo.blockSignals(False)

        self.match_combo.blockSignals(True)
        self.match_combo.clear()
        self.match_combo.addItem("All", userData="all")
        self.match_combo.addItem("Matched", userData="matched")
        self.match_combo.addItem("Not Matched", userData="unmatched")
        self.match_combo.blockSignals(False)

        self.tag_combo.blockSignals(True)
        self.tag_combo.clear()
        self.tag_combo.addItem("Loading...", userData="__loading__")
        self.tag_combo.setEnabled(False)
        self.tag_combo.blockSignals(False)

    def _on_filters_loaded(self, data: Dict[str, Any], generation: int = 0) -> None:
        """
        Handle background filter loading completion.

        Args:
            data: Dictionary with domains, sources, match_lists, tags
        """
        if generation != self._filter_worker_generation:
            logger.debug(
                "Ignoring stale FilterLoadWorker result (gen %d vs current %d)",
                generation,
                self._filter_worker_generation,
            )
            return

        self._filters_loading = False
        self._filter_worker = None

        # Populate domains (with count info)
        domains_data = data.get("domains", {})
        domain_items = domains_data.get("items", [])
        total_domains = domains_data.get("total_count", 0)
        truncated = domains_data.get("truncated", False)

        current_domain = "*"  # Default to all
        self.domain_combo.blockSignals(True)
        self.domain_combo.clear()
        self.domain_combo.setEnabled(True)

        if truncated:
            self.domain_combo.addItem(
                f"All domains ({total_domains} total)",
                userData="*"
            )
        else:
            self.domain_combo.addItem("All domains", userData="*")

        for item in domain_items:
            domain = item["domain"]
            count = item["count"]
            label = f"{domain or '<none>'} ({count:,})"
            self.domain_combo.addItem(label, userData=domain)

        if truncated:
            # Add note about more domains available
            remaining = total_domains - len(domain_items)
            self.domain_combo.addItem(
                f"... and {remaining} more (use search)",
                userData="__more__"
            )

        self.domain_combo.blockSignals(False)

        # Populate sources
        sources = data.get("sources", [])
        self.source_combo.blockSignals(True)
        self.source_combo.clear()
        self.source_combo.setEnabled(True)
        self.source_combo.addItem("All sources", userData="")
        for source in sources:
            self.source_combo.addItem(source, userData=source)
        self.source_combo.blockSignals(False)

        # Populate match lists
        match_lists = data.get("match_lists", [])
        self.match_combo.blockSignals(True)
        self.match_combo.clear()
        self.match_combo.addItem("All", userData="all")
        self.match_combo.addItem("Matched", userData="matched")
        self.match_combo.addItem("Not Matched", userData="unmatched")
        for list_name in match_lists:
            self.match_combo.addItem(list_name, userData=list_name)
        self.match_combo.blockSignals(False)

        # Populate tags
        tags = data.get("tags", [])
        self.tag_combo.blockSignals(True)
        self.tag_combo.clear()
        self.tag_combo.setEnabled(True)
        self.tag_combo.addItem("All tags", userData="*")
        for tag in tags:
            self.tag_combo.addItem(tag["name"], userData=tag["name"])
        self.tag_combo.blockSignals(False)

        # Trigger filter application
        self._on_filters_changed()

        logger.debug("Filter loading complete: %d domains, %d sources, %d match_lists, %d tags",
                     len(domain_items), len(sources), len(match_lists), len(tags))

    def _on_filters_load_error(self, error: str, generation: int = 0) -> None:
        """Handle background filter loading error."""
        if generation != self._filter_worker_generation:
            return

        self._filters_loading = False
        self._filter_worker = None

        logger.error("Filter loading failed: %s", error)

        # Reset to basic state
        self.domain_combo.blockSignals(True)
        self.domain_combo.clear()
        self.domain_combo.setEnabled(True)
        self.domain_combo.addItem("All domains", userData="*")
        self.domain_combo.blockSignals(False)

        self.source_combo.blockSignals(True)
        self.source_combo.clear()
        self.source_combo.setEnabled(True)
        self.source_combo.addItem("All sources", userData="")
        self.source_combo.blockSignals(False)

        self.tag_combo.blockSignals(True)
        self.tag_combo.clear()
        self.tag_combo.setEnabled(True)
        self.tag_combo.addItem("All tags", userData="*")
        self.tag_combo.blockSignals(False)

        self._on_filters_changed()

    def _on_search_clicked(self) -> None:
        """Handle explicit search button click (performance optimization)."""
        # Ignore if filters still loading
        if self._filters_loading:
            return
        self.filters.domain = self.domain_combo.currentData() or ""
        self.filters.text = self.search_edit.text().strip()
        self.filters.source = self.source_combo.currentData() or ""
        self.filters.match = self.match_combo.currentData() or "all"
        self.filters.tag = self.tag_combo.currentData() or "*"
        self._apply_filters()

    def _on_filters_changed(self) -> None:
        """Handle filter dropdown changes (but not search text)."""
        # Ignore placeholder values during loading
        domain_data = self.domain_combo.currentData()
        if domain_data in ("__loading__", "__more__"):
            return

        self.filters.domain = domain_data or ""
        # Don't update search text here - only on button click
        self.filters.source = self.source_combo.currentData() or ""
        self.filters.match = self.match_combo.currentData() or "all"  #
        self.filters.tag = self.tag_combo.currentData() or "*"
        self._apply_filters()

    def _apply_filters(self, use_background: bool = True) -> None:
        """
        Apply current filters to the URL list.

        Phase 2.1 - Uses background DataLoadWorker for initial load
        to prevent UI freezing with large databases.

        Args:
            use_background: If True (default), load data in background thread.
                           Set to False for synchronous pagination.
        """
        # Skip if filters are loading
        if self._filters_loading:
            return

        # Skip if no case data or evidence
        if not self.case_data or self.evidence_id is None:
            return

        sources: Optional[Iterable[str]] = None
        if self.filters.source:
            sources = (self.filters.source,)

        # Update filters on model WITHOUT triggering reload
        self.model.set_filters(
            domain=self.filters.domain if self.filters.domain != "*" else "",
            text=self.filters.text,
            sources=sources,
            match_filter=self.filters.match,  #
            tag=self.filters.tag,  #
            reload=not use_background,  # Don't reload if using background worker
        )

        # Update grouped model - only reload if grouped view is visible (performance fix)
        # This prevents expensive 10k-row queries when the flat table view is active
        grouped_view_visible = self.views.currentIndex() == 1
        self.group_model.set_case_data(self.case_data)
        self.group_model.set_evidence(self.evidence_id, reload=False)
        self.group_model.set_filters(
            domain=self.filters.domain if self.filters.domain != "*" else "",
            url=self.filters.text,
            sources=sources,
            match_filter=self.filters.match,  #
            tag=self.filters.tag,  #
            reload=grouped_view_visible,  # Only reload if visible
        )

        if use_background:
            # Start background data loading
            self._start_data_load()
        else:
            self._update_page_label()

    def _start_data_load(self) -> None:
        """
        Start background data loading.

        Phase 2.1 - Loads URL data in background thread.
        """
        # Increment generation; any stale worker callbacks are ignored.
        self._data_worker_generation += 1
        current_gen = self._data_worker_generation

        # Keep old worker alive until it finishes (prevents QThread destruction crash).
        if self._data_worker is not None:
            if self._data_worker.isRunning():
                self._pending_data_workers.append(self._data_worker)
            else:
                self._data_worker.deleteLater()
            self._data_worker = None
        self._pending_data_workers = [w for w in self._pending_data_workers if w.isRunning()]

        # Show loading state
        self._data_loading = True
        self._set_table_loading()

        case_folder = getattr(self.case_data, "case_folder", None)
        case_db_path = getattr(self.case_data, "db_path", None)
        if case_folder is None or case_db_path is None:
            self._on_data_load_error("Case context unavailable for background URL loading", current_gen)
            return

        # Start background worker with current filters
        self._data_worker = DataLoadWorker(
            Path(case_folder),
            Path(case_db_path),
            int(self.evidence_id),
            self.model.get_filters(),
            page_size=self.model.page_size,
            page=self.model.page,
        )
        self._data_worker.finished.connect(
            lambda rows, count, gen=current_gen: self._on_data_loaded(rows, count, gen)
        )
        self._data_worker.error.connect(
            lambda error, gen=current_gen: self._on_data_load_error(error, gen)
        )
        self._data_worker.start()

        logger.debug("Started background data loading for evidence_id=%s", self.evidence_id)

    def _set_table_loading(self) -> None:
        """Show loading state in table view."""
        # Update page label to show loading
        self.page_label.setText("Loading...")
        self.prev_button.setEnabled(False)
        self.next_button.setEnabled(False)

    def _on_data_loaded(self, rows: list, total_count: int, generation: int = 0) -> None:
        """
        Handle background data loading completion.

        Updates model with loaded data.
        """
        if generation != self._data_worker_generation:
            logger.debug(
                "Ignoring stale DataLoadWorker result (gen %d vs current %d)",
                generation,
                self._data_worker_generation,
            )
            return

        self._data_loading = False
        self._data_worker = None

        # Update model with loaded data
        self.model.set_loaded_data(rows, total_count)

        # Update UI
        self._update_page_label()

        logger.debug("Data loading complete: %d rows, %d total", len(rows), total_count)

    def _on_data_load_error(self, error: str, generation: int = 0) -> None:
        """Handle background data loading error."""
        if generation != self._data_worker_generation:
            return

        self._data_loading = False
        self._data_worker = None

        logger.error("Data loading failed: %s", error)

        # Reset to empty state
        self.model.set_loaded_data([], 0)
        self._update_page_label()

    def _page_up(self) -> None:
        self.model.page_up()
        self._update_page_label()

    def _page_down(self) -> None:
        self.model.page_down()
        self._update_page_label()

    def _update_page_label(self) -> None:
        """Update pagination label with 'Showing X-Y of Z URLs' format."""
        total = self.model.total_count()
        page_size = self.model.page_size
        current_page = self.model.current_page()

        if total == 0:
            self.page_label.setText("No URLs")
            self.prev_button.setEnabled(False)
            self.next_button.setEnabled(False)
            return

        start = current_page * page_size + 1
        end = min((current_page + 1) * page_size, total)

        self.page_label.setText(
            f"Showing {start}-{end} of {total} URLs"
        )

        # Update button states
        self.prev_button.setEnabled(current_page > 0)
        self.next_button.setEnabled(end < total)

    def _export_csv(self) -> None:
        if self.evidence_id is None:
            QMessageBox.information(
                self,
                "No data",
                "Select an evidence item before exporting.",
            )
            return
        path_str, _ = QFileDialog.getSaveFileName(
            self,
            "Export URLs",
            str(Path.home() / "urls.csv"),
            "CSV Files (*.csv)",
        )
        if not path_str:
            return
        self.model.export_to_csv(Path(path_str))

    def _show_deduplicate_dialog(self) -> None:
        """Open deduplication dialog."""
        if not self.case_data or self.evidence_id is None:
            QMessageBox.information(
                self,
                "No Evidence",
                "Please select an evidence item first.",
            )
            return

        from app.common.dialogs import DeduplicateUrlsDialog

        dialog = DeduplicateUrlsDialog(
            self.case_data,
            self.evidence_id,
            self,
        )
        dialog.deduplication_complete.connect(self._on_deduplication_complete)
        dialog.exec()

    def _on_deduplication_complete(self) -> None:
        """Handle deduplication completion - refresh data."""
        # Refresh filters to reflect any removed sources
        self._populate_filters()
        # Refresh data
        self.refresh()

    def _toggle_grouping(self, checked: bool) -> None:
        index = 1 if checked else 0
        self.views.setCurrentIndex(index)
        if checked:
            # Reload grouped model with current filters when switching to grouped view
            # The reload was deferred in _apply_filters when view wasn't visible
            self.group_model.reload()

    # _trigger_download method removed - downloads now handled in Download tab

    def _tag_selected(self) -> None:
        """Open tagging dialog for selected URLs."""
        if not self.case_data or self.evidence_id is None:
            return

        selected_urls = self.get_selected_urls()
        if not selected_urls:
            QMessageBox.information(
                self,
                "Tag URLs",
                "Please select one or more URLs to tag.",
            )
            return

        url_ids = [url['id'] for url in selected_urls if url.get('id')]
        if not url_ids:
            return

        from app.common.dialogs import TagArtifactsDialog
        dialog = TagArtifactsDialog(
            self.case_data,
            self.evidence_id,
            "url",
            url_ids,
            self
        )
        dialog.tags_changed.connect(self.refresh)
        dialog.exec()

    def _show_context_menu(self, pos) -> None:
        """Show context menu for table view (, sandbox options)."""
        if not self.case_data or self.evidence_id is None:
            return

        index = self.table_view.indexAt(pos)
        if not index.isValid():
            return

        from PySide6.QtGui import QAction

        menu = QMenu(self)

        # Get selected URLs for context-aware actions
        selected_urls = self.get_selected_urls()
        single_url_selected = len(selected_urls) == 1

        # Get URL for sandbox actions
        url = self._get_selected_url() if single_url_selected else ""

        # Pass forensic context parameters for screenshot saving
        add_sandbox_url_actions(
            menu, url, self, self.evidence_id,
            evidence_label=self._get_evidence_label(),
            workspace_path=self._get_case_folder(),
            case_data=self.case_data,
        )

        menu.addSeparator()

        # Copy URL action
        copy_action = QAction("📋 Copy URL", self)
        copy_action.triggered.connect(self._copy_selected_url)
        copy_action.setEnabled(single_url_selected)
        menu.addAction(copy_action)

        menu.addSeparator()

        tag_action = QAction("🏷️ Tag Selected...", self)
        tag_action.triggered.connect(self._tag_selected)
        menu.addAction(tag_action)

        menu.exec(self.table_view.viewport().mapToGlobal(pos))

    def _get_selected_url(self) -> Optional[str]:
        """Get the URL string from the first selected row."""
        selected_urls = self.get_selected_urls()
        if not selected_urls:
            return None
        return selected_urls[0].get('url', '') or None

    def _copy_selected_url(self) -> None:
        """Copy selected URL to clipboard."""
        selected_urls = self.get_selected_urls()
        if not selected_urls:
            return

        url = selected_urls[0].get('url', '')
        if url:
            QApplication.clipboard().setText(url)

    def current_filters(self) -> Dict[str, str]:
        return {
            "domain": self.filters.domain if self.filters.domain not in ("", "*") else "",
            "text": self.filters.text,
            "source": self.filters.source,
        }

    def get_selected_urls(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if self.views.currentIndex() == 0:
            indexes = self.table_view.selectionModel().selectedRows()
            for idx in indexes:
                row = self.model.get_row(idx.row())
                if row:
                    rows.append(row)
        else:
            indexes = self.tree_view.selectionModel().selectedRows()
            for idx in indexes:
                if not idx.parent().isValid():
                    continue
                item = self.group_model.itemFromIndex(idx)
                data = item.data(Qt.UserRole + 1)
                if isinstance(data, dict):
                    rows.append(data)
        return rows

    def _on_group_item_changed(self, item):  # noqa: ANN001
        url_id = item.data(Qt.UserRole)
        if not url_id or not self.case_data or self.evidence_id is None:
            return
        self.case_data.update_url_tags(int(self.evidence_id), int(url_id), item.text())

    def _match_against_url_lists(self) -> None:
        """Match URLs against selected URL reference lists."""
        if self.evidence_id is None:
            QMessageBox.information(
                self,
                "No Evidence",
                "Please select an evidence item first.",
            )
            return

        # Check if any URLs exist
        if not self.case_data:
            QMessageBox.information(
                self,
                "No Data",
                "No URLs found in this evidence.",
            )
            return

        # Get available URL lists
        from core.matching import ReferenceListManager
        ref_manager = ReferenceListManager()
        available = ref_manager.list_available()
        available_urllists = available.get("urllists", [])

        if not available_urllists:
            QMessageBox.information(
                self,
                "No URL Lists",
                "No URL lists found. Install predefined lists or create custom lists in Preferences first.",
            )
            return

        # Show selection dialog
        from app.common.dialogs import ReferenceListSelectorDialog
        dialog = ReferenceListSelectorDialog(available_urllists, self)
        dialog.setWindowTitle("Select URL Lists")
        dialog.findChild(QLabel).setText("Select URL lists to match against:")

        if dialog.exec() != QDialog.Accepted:
            return

        selected_names = dialog.get_selected_lists()
        if not selected_names:
            QMessageBox.information(
                self,
                "No Selection",
                "Please select at least one URL list to match against.",
            )
            return

        # Convert to (list_name, list_path) tuples
        selected_lists = []
        for name in selected_names:
            list_path = ref_manager.urllists_dir / f"{name}.txt"
            if list_path.exists():
                selected_lists.append((name, str(list_path)))

        if not selected_lists:
            QMessageBox.warning(
                self,
                "Error",
                "Selected URL lists not found.",
            )
            return

        try:
            # Get case database path
            if not self.case_data or not hasattr(self.case_data, 'db_manager'):
                QMessageBox.critical(
                    self,
                    "Error",
                    "Case database not available.",
                )
                return

            # Invalidate stale callbacks from any previous worker.
            self._match_worker_generation += 1
            current_gen = self._match_worker_generation

            # Keep old worker alive until it finishes (prevents QThread destruction crash).
            if self.match_worker is not None:
                if self.match_worker.isRunning():
                    self._pending_match_workers.append(self.match_worker)
                else:
                    self.match_worker.deleteLater()
                self.match_worker = None
            self._pending_match_workers = [w for w in self._pending_match_workers if w.isRunning()]

            # Replace any prior progress dialog safely.
            if self.progress_dialog is not None:
                try:
                    self.progress_dialog.close()
                    self.progress_dialog.deleteLater()
                except RuntimeError:
                    pass
                self.progress_dialog = None

            # Create and start worker
            self.match_worker = UrlMatchWorker(
                self.case_data.db_manager, self.evidence_id, selected_lists
            )

            # Progress dialog
            self.progress_dialog = QProgressDialog(
                "Matching URLs against reference lists...",
                "Cancel",
                0, 100, self
            )
            self.progress_dialog.setWindowModality(Qt.WindowModal)
            self.progress_dialog.setAutoClose(True)

            # Connect signals
            self.match_worker.progress.connect(
                lambda current, total, gen=current_gen: self._update_match_progress(current, total, gen)
            )
            self.match_worker.finished.connect(
                lambda results, gen=current_gen: self._match_finished(results, gen)
            )
            self.match_worker.error.connect(
                lambda error_msg, gen=current_gen: self._match_error(error_msg, gen)
            )
            self.progress_dialog.canceled.connect(self._cancel_match_worker)

            self.match_worker.start()
            self.progress_dialog.show()

        except Exception as e:
            QMessageBox.critical(
                self,
                "Match Error",
                f"Failed to start matching: {str(e)}",
            )

    def _cancel_match_worker(self) -> None:
        """Gracefully cancel match worker when user clicks Cancel."""
        if self.match_worker is None:
            return

        # Invalidate old worker callbacks immediately.
        self._match_worker_generation += 1
        worker = self.match_worker
        self.match_worker = None

        if worker.isRunning():
            worker.requestInterruption()
            worker.quit()
            if not worker.wait(300):
                logger.info("UrlMatchWorker still stopping after cancel; keeping worker alive")
                self._pending_match_workers.append(worker)
        else:
            worker.deleteLater()

        if self.progress_dialog is not None:
            try:
                self.progress_dialog.close()
                self.progress_dialog.deleteLater()
            except RuntimeError:
                pass
            self.progress_dialog = None

    def _update_match_progress(self, current: int, total: int, generation: int = 0) -> None:
        """Update match progress dialog."""
        if generation != self._match_worker_generation or self.progress_dialog is None:
            return

        if total > 0:
            progress = int((current / total) * 100)
            # Set label BEFORE setValue — setValue(100) with autoClose
            # emits canceled, which nulls self.progress_dialog.
            self.progress_dialog.setLabelText(
                f"Matching... {progress}%"
            )
            self.progress_dialog.setValue(progress)

    def _match_finished(self, results: Dict[str, int], generation: int = 0) -> None:
        """Handle match completion."""
        if generation != self._match_worker_generation:
            return

        # Grab local ref: close() emits canceled, which nulls self.progress_dialog.
        dlg = self.progress_dialog
        self.progress_dialog = None
        if dlg is not None:
            dlg.close()
            dlg.deleteLater()
        if self.match_worker is not None:
            self.match_worker.deleteLater()
            self.match_worker = None

        total_matches = sum(results.values())
        message = f"Matching completed!\n\nTotal matches found: {total_matches:,}\n\n"

        # Show results for each list (sorted by name)
        for list_name in sorted(results.keys()):
            count = results[list_name]
            message += f"• {list_name}: {count:,} matches\n"

        QMessageBox.information(
            self,
            "Matching Complete",
            message,
        )

        # Refresh data
        self.refresh()

    def _match_error(self, error_msg: str, generation: int = 0) -> None:
        """Handle match error."""
        if generation != self._match_worker_generation:
            return

        # Grab local ref: close() emits canceled, which nulls self.progress_dialog.
        dlg = self.progress_dialog
        self.progress_dialog = None
        if dlg is not None:
            dlg.close()
            dlg.deleteLater()
        if self.match_worker is not None:
            self.match_worker.deleteLater()
            self.match_worker = None

        QMessageBox.critical(
            self,
            "Match Error",
            f"Matching failed: {error_msg}",
        )

    def shutdown(self) -> None:
        """
        Gracefully stop all background workers before widget destruction.

        Called by MainWindow.closeEvent() and _on_close_evidence_tab() to prevent
        Qt abort from destroying QThread while still running.
        """
        # Invalidate all pending callbacks.
        self._filter_worker_generation += 1
        self._data_worker_generation += 1
        self._match_worker_generation += 1

        def _stop_worker(worker: QThread, worker_name: str, timeout_ms: int = 2000) -> None:
            if worker is None:
                return
            try:
                worker.finished.disconnect()
            except (RuntimeError, TypeError):
                pass
            try:
                worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            try:
                worker.progress.disconnect()  # UrlMatchWorker only
            except (RuntimeError, TypeError, AttributeError):
                pass

            if worker.isRunning():
                worker.requestInterruption()
                worker.quit()
                if not worker.wait(timeout_ms):
                    logger.warning("%s did not stop in %dms, terminating", worker_name, timeout_ms)
                    worker.terminate()
                    worker.wait(500)
            worker.deleteLater()

        # Stop current workers.
        if self._filter_worker is not None:
            _stop_worker(self._filter_worker, "FilterLoadWorker")
            self._filter_worker = None
        if self._data_worker is not None:
            _stop_worker(self._data_worker, "DataLoadWorker")
            self._data_worker = None
        if self.match_worker is not None:
            _stop_worker(self.match_worker, "UrlMatchWorker")
            self.match_worker = None

        # Stop pending workers that were intentionally kept alive.
        for worker in self._pending_filter_workers:
            _stop_worker(worker, "Pending FilterLoadWorker", timeout_ms=1000)
        self._pending_filter_workers.clear()

        for worker in self._pending_data_workers:
            _stop_worker(worker, "Pending DataLoadWorker", timeout_ms=1000)
        self._pending_data_workers.clear()

        for worker in self._pending_match_workers:
            _stop_worker(worker, "Pending UrlMatchWorker", timeout_ms=1000)
        self._pending_match_workers.clear()

        # Close progress dialog if open.
        if self.progress_dialog is not None:
            try:
                self.progress_dialog.close()
                self.progress_dialog.deleteLater()
            except RuntimeError:
                pass
            self.progress_dialog = None

        logger.debug("UrlsTab shutdown complete")


# UrlMatchWorker imported from app.services.matching_workers
