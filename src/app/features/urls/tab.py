from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, List

from PySide6.QtCore import Qt, Signal, QThread, QTimer
from PySide6.QtWidgets import (
    QAbstractItemView,
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

from app.data import UrlFilterSpec, UrlPage, UrlSearchIndexStatus
from app.data.case_data import CaseDataAccess
from app.features.urls.models import UrlsTableModel, UrlsGroupedModel
from app.services.matching_workers import UrlMatchWorker
from app.services.query_loader import (
    CancellableQueryWorker,
    QueryRequest,
    QueryResult,
    background_read_limiter,
)
from app.services.url_search_index_worker import UrlSearchIndexWorker
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


def _load_url_page(case_data: CaseDataAccess, request: QueryRequest) -> UrlPage:
    """Worker-thread handler: load one flat URL page with tag/match maps."""
    return case_data.load_url_page(request.payload)


@dataclass(frozen=True)
class UrlCountRequest:
    """Exact-total request; ``key`` is the model's count-cache key it fills."""

    key: tuple
    filters: UrlFilterSpec


def _count_urls(case_data: CaseDataAccess, request: QueryRequest) -> tuple:
    """Worker-thread handler: ``(key, exact total)`` for one filter spec (issue #61)."""
    payload: UrlCountRequest = request.payload
    return payload.key, case_data.count_urls(request.evidence_id, filters=payload.filters)


@dataclass
class UrlFilters:
    domain: str = ""  # exact domain (dropdown), "" or "*" = all
    domain_contains: str = ""  # literal domain substring
    text: str = ""
    source: str = ""
    match: str = "all"  # "all", "matched", "unmatched", or specific list name
    tag: str = "*"  #


class UrlsTab(QWidget):
    # downloadRequested signal removed - downloads now handled in Download tab

    # Emitted when the user picks a different rows-per-page value (persisted
    # by the main window).
    page_size_changed = Signal(int)

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

        # Background page loading (issue #60): one worker-owned connection per
        # tab; latest request wins, stale results are rejected by generation.
        self._url_loader: Optional[CancellableQueryWorker] = None
        self._data_worker_generation = 0
        # Generation of the last quiet re-check request (issue #67).
        self._quiet_generation: Optional[int] = None
        self._data_loading = False

        # Exact totals load independently of pages (issue #61); results are
        # cached on the model, so page navigation does not recount.
        self._count_loader: Optional[CancellableQueryWorker] = None
        self._count_generation = 0
        self._count_in_flight_key: Optional[tuple] = None
        self._count_failed_key: Optional[tuple] = None
        # A page that arrived while a table cell editor was open; applied
        # when the editor closes (a model reset would discard the edit).
        self._pending_page_result: Optional[QueryResult] = None

        # Only the visible view is refreshed; the other one is marked stale.
        self._flat_stale = False
        self._grouped_stale = True

        # Background URL search index build (issue #62); search never waits
        # for it (URLs not indexed yet are scanned).
        self._index_worker: Optional[UrlSearchIndexWorker] = None
        # True once the running build reported a batch (it committed writes).
        self._index_worker_wrote = False
        # Set by shutdown(): no background work may start afterwards.
        self._shut_down = False
        # Set while the main window needs the evidence DB unchanged (export).
        self._search_index_paused = False
        # Builds stopped without waiting that may still commit a batch.
        self._stopping_index_workers: List[UrlSearchIndexWorker] = []
        # A start requested while a flat page load was in flight (see
        # _start_search_index_build); done once that page is applied.
        self._start_index_after_load = False

        # URL match worker lifecycle
        self.match_worker: Optional[UrlMatchWorker] = None
        self._pending_match_workers: List[UrlMatchWorker] = []
        self._match_worker_generation = 0
        self.progress_dialog: Optional[QProgressDialog] = None

        layout = QVBoxLayout()

        filter_layout = QGridLayout()
        self.domain_label = QLabel("Domain")
        filter_layout.addWidget(self.domain_label, 0, 0)
        # Issue #61: the dropdown selects one exact domain; substring search
        # on domains is a separate field.
        domain_layout = QHBoxLayout()
        self.domain_combo = QComboBox()
        self.domain_combo.addItem("All domains", userData="*")
        self.domain_combo.setToolTip("Exact domain (ASCII case-insensitive)")
        self.domain_combo.currentIndexChanged.connect(self._on_filters_changed)
        domain_layout.addWidget(self.domain_combo, 1)
        self.domain_contains_edit = QLineEdit()
        self.domain_contains_edit.setPlaceholderText("Domain contains...")
        self.domain_contains_edit.setToolTip(
            "Domain substring (ASCII case-insensitive; %, _ and \\ match literally). "
            "Press Enter or Search to apply."
        )
        self.domain_contains_edit.returnPressed.connect(self._on_search_clicked)
        domain_layout.addWidget(self.domain_contains_edit, 1)
        filter_layout.addLayout(domain_layout, 0, 1)

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

        # Search index state (issue #62); hidden while the index is ready.
        self.search_index_label = QLabel()
        self.search_index_label.setStyleSheet("color: palette(mid);")
        self.search_index_label.hide()
        search_layout.addWidget(self.search_index_label)

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
        # Queued: reload only after the editor has finished committing.
        self.model.tags_edited.connect(self._on_tags_changed, Qt.QueuedConnection)
        self.group_model = UrlsGroupedModel(case_data)
        self.group_model.itemChanged.connect(self._on_group_item_changed)

        self.table_view = QTableView()
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setSelectionMode(QTableView.ExtendedSelection)  # Allow multi-select for tagging
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setModel(self.model)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        self.table_view.itemDelegate().closeEditor.connect(
            lambda *_: QTimer.singleShot(0, self._apply_pending_page)
        )
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
        self.page_size_label = QLabel("Rows per page")
        self.page_size_combo = QComboBox()
        for size in self.model.PAGE_SIZE_CHOICES:
            self.page_size_combo.addItem(f"{size:,}", userData=size)
        self.page_size_combo.setCurrentIndex(self.page_size_combo.findData(self.model.page_size))
        self.page_size_combo.currentIndexChanged.connect(self._on_page_size_changed)

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
        controls_layout.addWidget(self.page_size_label)
        controls_layout.addWidget(self.page_size_combo)
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
        if case_data is not self.case_data:
            # Loader connections belong to the previous case.
            self._data_worker_generation += 1
            self._stop_url_loader()
            self._stop_count_loader()
            self._stop_search_index_build()
        self.case_data = case_data
        self.model.set_case_data(case_data)
        self.group_model.set_case_data(case_data)
        self._grouped_stale = True
        if not defer_load:
            # Loaded now (in the background), so showEvent may refresh/re-check.
            self._data_loaded = self.evidence_id is not None
            self._load_pending = False
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
        if evidence_id != self.evidence_id:
            # Reject in-flight results and stop work for the previous evidence.
            self._cancel_flat_load()
            self._cancel_count()
            self._stop_search_index_build()
        self.evidence_id = evidence_id
        self._evidence_label_cache = evidence_label  #
        self._data_loaded = False
        self._grouped_stale = True

        if not defer_load:
            # Data is loaded in the background once filters are populated;
            # loaded state lets showEvent refresh/re-check (issue #67).
            self._data_loaded = evidence_id is not None
            self._load_pending = False
            self.model.set_evidence(evidence_id, reload=False)
            self.group_model.set_evidence(evidence_id, reload=False)
            self._populate_filters()
            self._update_page_label()
        else:
            # Deferred loading - just store the ID, load on showEvent
            self._load_pending = True

    def set_page_size(self, page_size: int) -> None:
        """Apply a rows-per-page value (e.g. from user settings) without emitting ``page_size_changed``."""
        try:
            index = self.page_size_combo.findData(int(page_size))
        except (TypeError, ValueError):
            index = -1
        if index < 0:
            index = self.page_size_combo.findData(UrlsTableModel.PAGE_SIZE)
        self.page_size_combo.blockSignals(True)
        self.page_size_combo.setCurrentIndex(index)
        self.page_size_combo.blockSignals(False)
        if self.model.set_page_size(self.page_size_combo.currentData()) and self._data_loaded:
            self._reload_flat_from_start()

    def refresh(self) -> None:
        """Reload after a data change (ingestion, deduplication, list matching).

        Cached totals are invalidated and the flat view returns to the first
        page, because earlier pages may have changed (issue #61). Only the
        visible view is reloaded; the other one is marked stale.
        """
        self.model.mark_data_changed(keep_position=False)
        self._reload_visible_view(0)
        # New URLs (e.g. after ingestion) are indexed in the background.
        self._start_search_index_build()

    def _on_tags_changed(self) -> None:
        """Reload after tagging (dialog or inline Tags edit): totals are recounted.

        Tagged from the flat view, all changed rows are on the current page,
        so earlier pages and the displayed row range stay valid and the page
        position is kept. Tagged from the grouped view, rows of any flat page
        may have changed, so the flat view returns to its first page.
        """
        keep_position = self.views.currentIndex() == 0
        self.model.mark_data_changed(keep_position=keep_position)
        self._reload_visible_view(self.model.current_page())

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
        elif self._data_loaded:
            QTimer.singleShot(10, self._recheck_flat_page)
        if self._data_loaded or self._load_pending:
            QTimer.singleShot(10, self._start_search_index_build)

    def hideEvent(self, event):
        """Pause the search index build while hidden; showing the tab resumes it (#62).

        Minimizing the window (a spontaneous hide) does not pause it.
        """
        if not event.spontaneous():
            self._stop_search_index_build(timeout_ms=0)
        super().hideEvent(event)

    def _recheck_flat_page(self) -> None:
        """Quietly reload the shown flat page to detect unnotified writes (issue #67).

        Writes through other connections (e.g. a tag rename or merge in the
        Tags tab) are only noticed through the page's ``PRAGMA data_version``.
        Re-requesting the current page (a cheap keyset query) when the tab is
        shown again reveals them, which invalidates the total and recounts.
        The label and buttons are kept until the page arrives; an unchanged
        page keeps the view's selection and scroll position.
        """
        if (
            not self.isVisible()
            or self.views.currentIndex() != 0
            or self._flat_stale
            or self._data_loading
            or self._filters_loading
            or self._load_pending
            or not self.model.has_data_baseline()
            or self.table_view.state() == QAbstractItemView.EditingState
        ):
            return
        self._request_page(self.model.current_page(), quiet=True)

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

        self._retire_filter_worker()
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

    def _retire_filter_worker(self) -> None:
        """Drop the current filter worker, keeping it referenced until its thread ends.

        Its result signal is emitted from inside run(), so the thread may still
        be running when the result arrives; dropping the last reference then
        would destroy a running QThread (Qt aborts).
        """
        worker, self._filter_worker = self._filter_worker, None
        if worker is None:
            return
        if worker.isRunning():
            self._pending_filter_workers.append(worker)
        else:
            worker.deleteLater()

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
        self._retire_filter_worker()

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
                f"... and {remaining} more (use Domain contains)",
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
        self._retire_filter_worker()

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
        domain_data = self.domain_combo.currentData()
        if domain_data not in ("__loading__", "__more__"):
            self.filters.domain = domain_data or ""
        self.filters.domain_contains = self.domain_contains_edit.text().strip()
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

    def _apply_filters(self) -> None:
        """
        Apply current filters to the URL list.

        The flat view is loaded in the background (issue #60); filters are
        only stored on the models here.
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
            domain_contains=self.filters.domain_contains,
            text=self.filters.text,
            sources=sources,
            match_filter=self.filters.match,  #
            tag=self.filters.tag,  #
            reload=False,
        )

        # Grouped model filters are updated without reloading; it is reloaded
        # only while visible (otherwise marked stale).
        self.group_model.set_case_data(self.case_data)
        self.group_model.set_evidence(self.evidence_id, reload=False)
        self.group_model.set_filters(
            domain=self.filters.domain if self.filters.domain != "*" else "",
            domain_contains=self.filters.domain_contains,
            url=self.filters.text,
            sources=sources,
            match_filter=self.filters.match,  #
            tag=self.filters.tag,  #
            reload=False,
        )
        self._grouped_stale = True

        self._reload_visible_view(0)

    def _cancel_flat_load(self) -> None:
        """Drop the running/queued flat page load; its late result is rejected."""
        self._data_worker_generation += 1
        self._data_loading = False
        if self._url_loader is not None:
            self._url_loader.cancel()

    def _invalidate_flat_load(self) -> None:
        """Mark the flat view stale and drop any in-flight flat page load."""
        self._flat_stale = True
        if self._data_loading:
            self._cancel_flat_load()
            # A build start waiting for that load would otherwise never run.
            self._start_deferred_search_index_build()

    def _reload_visible_view(self, page: int) -> None:
        """Reload only the visible view; mark the other view stale."""
        if self.views.currentIndex() == 1:
            self._invalidate_flat_load()
            self._reload_grouped()
        else:
            self._grouped_stale = True
            self._request_page(page)

    def _reload_grouped(self) -> None:
        """Reload the grouped view (still synchronous; async children are #64)."""
        if self._filters_loading or self._load_pending:
            return
        self._grouped_stale = False
        self.group_model.reload()

    def _ensure_url_loader(self) -> Optional[CancellableQueryWorker]:
        if self._url_loader is not None:
            return self._url_loader
        case_folder = getattr(self.case_data, "case_folder", None)
        case_db_path = getattr(self.case_data, "db_path", None)
        if case_folder is None or case_db_path is None:
            return None
        self._url_loader = CancellableQueryWorker(
            Path(case_folder), Path(case_db_path), _load_url_page, name="UrlPageLoader"
        )
        self._url_loader.result_ready.connect(self._on_page_loaded)
        self._url_loader.failed.connect(self._on_page_load_error)
        return self._url_loader

    def _ensure_count_loader(self) -> Optional[CancellableQueryWorker]:
        if self._count_loader is not None:
            return self._count_loader
        case_folder = getattr(self.case_data, "case_folder", None)
        case_db_path = getattr(self.case_data, "db_path", None)
        if case_folder is None or case_db_path is None:
            return None
        # Own limiter: long counts must not hold the interactive read slots.
        self._count_loader = CancellableQueryWorker(
            Path(case_folder), Path(case_db_path), _count_urls,
            limiter=background_read_limiter(), name="UrlCountLoader",
        )
        self._count_loader.result_ready.connect(self._on_count_loaded)
        self._count_loader.failed.connect(self._on_count_error)
        return self._count_loader

    # URL search index (issue #62) ---------------------------------------

    def pause_search_index_build(self, timeout_ms: int = 15000) -> bool:
        """Stop the search index build until ``resume_search_index_build``.

        For actions that need the evidence database unchanged (case export).
        Waits for the running batch to commit or roll back.

        Returns:
            True if no build is running anymore.
        """
        self._search_index_paused = True
        deadline = time.monotonic() + timeout_ms / 1000
        self._stop_search_index_build(timeout_ms=timeout_ms)
        # Also wait for builds stopped earlier without waiting (e.g. on hide).
        for worker in list(self._stopping_index_workers):
            remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
            if worker.isRunning() and remaining_ms:
                worker.wait(remaining_ms)
        self._prune_stopping_index_workers()
        return not self._stopping_index_workers

    def resume_search_index_build(self) -> None:
        """Undo ``pause_search_index_build`` (restarts the build if shown)."""
        self._search_index_paused = False
        self._start_search_index_build()

    def _start_search_index_build(self) -> None:
        """Start or resume the background search index build for the shown evidence.

        Runs while the tab is shown; a finished index makes the worker end
        after one status check. Stopped (after its running batch) when the
        tab is hidden, on evidence/case change, pause and shutdown.
        """
        if (
            self._shut_down
            or self._search_index_paused
            or not self.isVisible()
            or not self.case_data
            or self.evidence_id is None
        ):
            return
        if self._data_loading:
            # A page in flight may have read data_version before an
            # unnotified external write; once the build runs, such a change
            # would be set aside as the build's own. Start after it is applied.
            self._start_index_after_load = True
            return
        worker = self._index_worker
        if (
            worker is not None
            and worker.evidence_id == self.evidence_id
            and not worker.is_cancelled()
            and not worker.isFinished()
        ):
            return  # already running for this evidence
        self._stop_search_index_build()
        case_folder = getattr(self.case_data, "case_folder", None)
        case_db_path = getattr(self.case_data, "db_path", None)
        if case_folder is None or case_db_path is None:
            return
        worker = UrlSearchIndexWorker(Path(case_folder), Path(case_db_path), int(self.evidence_id))
        worker.progress.connect(self._on_search_index_progress)
        worker.completed.connect(self._show_search_index_status)
        worker.failed.connect(self._on_search_index_failed)
        worker.finished.connect(lambda w=worker: self._on_search_index_worker_finished(w))
        self._index_worker = worker
        self._index_worker_wrote = False
        # Its commits change data_version without changing URL data.
        self.model.begin_own_writes()
        worker.start()

    def _stop_search_index_build(self, timeout_ms: int = 0) -> bool:
        """Cancel the build (it stops after the running batch) and hide its state.

        Waits up to ``timeout_ms`` for the running batch (0: do not wait; the
        worker is kept alive until it ends). Returns True if it has ended.
        """
        worker, self._index_worker = self._index_worker, None
        self._start_index_after_load = False
        self.search_index_label.hide()
        if worker is None:
            return True
        self.model.end_own_writes()
        for signal in (worker.progress, worker.completed, worker.failed, worker.finished):
            try:
                signal.disconnect()
            except (RuntimeError, TypeError):
                pass
        if worker.stop(timeout_ms):
            worker.deleteLater()
            return True
        self._prune_stopping_index_workers()
        self._stopping_index_workers.append(worker)
        return False

    def _prune_stopping_index_workers(self) -> None:
        self._stopping_index_workers = [w for w in self._stopping_index_workers if w.isRunning()]

    def _start_deferred_search_index_build(self) -> None:
        if self._start_index_after_load and not self._data_loading:
            self._start_index_after_load = False
            self._start_search_index_build()

    def _on_search_index_worker_finished(self, worker: UrlSearchIndexWorker) -> None:
        if self._index_worker is worker:
            self._index_worker = None
            self.model.end_own_writes()
            if self._index_worker_wrote or self.model.has_set_aside_change():
                # Apply a set-aside change now instead of on the next page load
                # (guarded: no-op while a load is running or the view is hidden).
                self._recheck_flat_page()
        worker.deleteLater()

    def _on_search_index_progress(self, evidence_id: int, status: UrlSearchIndexStatus) -> None:
        if evidence_id != self.evidence_id:
            return
        self._index_worker_wrote = True
        self._show_search_index_status(evidence_id, status)

    def _show_search_index_status(self, evidence_id: int, status: UrlSearchIndexStatus) -> None:
        if evidence_id != self.evidence_id:
            return
        label = self.search_index_label
        if status.state == UrlSearchIndexStatus.READY:
            label.hide()
            return
        if status.state == UrlSearchIndexStatus.BUILDING:
            label.setText(f"Search index: building {int(status.progress * 100)}%")
            label.setToolTip(
                "Indexing URLs for fast text search. Results are complete meanwhile: "
                "URLs not indexed yet are scanned."
            )
        elif status.state == UrlSearchIndexStatus.UNSUPPORTED:
            label.setText("Search index: unavailable")
            label.setToolTip(
                "This SQLite build lacks FTS5 trigram support. URL text search scans "
                "all URLs: results are complete, but slower on large evidence."
            )
        else:
            label.setText("Search index: unavailable")
            label.setToolTip(
                "The search index could not be set up (see the log). URL text search "
                "scans all URLs: results are complete, but slower on large evidence."
            )
        label.show()

    def _on_search_index_failed(self, evidence_id: int, error: str) -> None:
        if evidence_id != self.evidence_id:
            return
        logger.error("URL search index build failed: %s", error)
        self.search_index_label.setText("Search index: build failed")
        self.search_index_label.setToolTip(
            f"Building the search index failed: {error}\n"
            "Results stay complete: URLs not indexed yet are scanned. "
            "The build is retried when the tab is shown again."
        )
        self.search_index_label.show()

    @staticmethod
    def _stop_loader(loader: Optional[CancellableQueryWorker], timeout_ms: int) -> None:
        if loader is None:
            return
        for signal in (loader.result_ready, loader.failed):
            try:
                signal.disconnect()
            except (RuntimeError, TypeError):
                pass
        if loader.stop(timeout_ms):
            loader.deleteLater()

    def _stop_url_loader(self, timeout_ms: int = 2000) -> None:
        loader, self._url_loader = self._url_loader, None
        self._stop_loader(loader, timeout_ms)

    def _stop_count_loader(self, timeout_ms: int = 2000) -> None:
        self._count_generation += 1
        self._count_in_flight_key = None
        loader, self._count_loader = self._count_loader, None
        self._stop_loader(loader, timeout_ms)

    def _cancel_count(self) -> None:
        """Drop the running/queued count; its late result is rejected."""
        self._count_generation += 1
        self._count_in_flight_key = None
        if self._count_loader is not None:
            self._count_loader.cancel()

    def _request_page(self, page: int, *, quiet: bool = False) -> None:
        """Load ``page`` of the flat view in the background (no GUI-thread queries).

        Args:
            page: Page index to load
            quiet: Keep the page label and buttons instead of showing the
                loading state (re-check of the shown page, issue #67)
        """
        # Filter population / deferred load will trigger the load when done.
        if self._filters_loading or self._load_pending:
            return
        if not self.case_data or self.evidence_id is None:
            return
        if self.model.evidence_id != self.evidence_id:
            # e.g. set_case_data() reset the model's evidence.
            self.model.set_evidence(self.evidence_id, reload=False)
            page = 0

        self._data_worker_generation += 1
        current_gen = self._data_worker_generation
        self._flat_stale = False
        self._data_loading = True
        self._quiet_generation = current_gen if quiet else None
        if not quiet:
            self._set_table_loading()

        loader = self._ensure_url_loader()
        if loader is None:
            self._on_page_load_error(
                current_gen, int(self.evidence_id), "Case context unavailable for background URL loading"
            )
            return
        loader.submit(QueryRequest(current_gen, int(self.evidence_id), self.model.build_query(page)))
        logger.debug("Requested URL page %d (gen %d) for evidence_id=%s", page, current_gen, self.evidence_id)
        # The total loads alongside the page; first rows never wait for it.
        # Without a data_version baseline, a count racing a write that lands
        # before the page sets the baseline could be cached stale for good, so
        # it starts from _on_page_loaded instead (after the baseline is set).
        if self.model.has_data_baseline():
            self._request_count()

    def _apply_pending_page(self) -> None:
        """Apply a page held back while a table cell editor was open."""
        result, self._pending_page_result = self._pending_page_result, None
        if result is not None:
            self._on_page_loaded(result)

    def _request_count(self) -> None:
        """Start an exact count unless it is cached, already running, or failed."""
        if not self.case_data or self.evidence_id is None:
            return
        key = self.model.count_key()
        if (
            self.model.total_count() is not None
            or key == self._count_in_flight_key
            or key == self._count_failed_key
        ):
            return
        loader = self._ensure_count_loader()
        if loader is None:
            return
        self._count_generation += 1
        self._count_in_flight_key = key
        self._count_failed_key = None
        loader.submit(
            QueryRequest(self._count_generation, int(self.evidence_id), UrlCountRequest(key, self.model.filter_spec()))
        )

    def _set_table_loading(self) -> None:
        """Show loading state in table view."""
        # Update page label to show loading
        self.page_label.setText("Loading...")
        self.prev_button.setEnabled(False)
        self.next_button.setEnabled(False)

    def _is_current_result(self, generation: int, evidence_id: int) -> bool:
        return generation == self._data_worker_generation and evidence_id == self.evidence_id

    def _on_page_loaded(self, result: QueryResult) -> None:
        """Apply a prepared page from the loader (GUI thread, no DB access)."""
        if not self._is_current_result(result.generation, result.evidence_id):
            logger.debug(
                "Ignoring stale URL page (gen %d vs current %d)",
                result.generation,
                self._data_worker_generation,
            )
            return

        if self.table_view.state() == QAbstractItemView.EditingState:
            # Resetting the model now would close the open Tags editor
            # without committing it; apply the page once it is closed.
            self._pending_page_result = result
            return
        self._pending_page_result = None
        self._data_loading = False
        page: UrlPage = result.payload
        if not self.model.is_current_query(page.query):
            # Filters/page size/data changed without a new request (e.g. a
            # refresh while filters were still loading): this page no longer fits.
            logger.debug("Discarding outdated URL page %d; reloading current page", page.query.page)
            self._request_page(self.model.current_page())
            return
        self.model.set_loaded_page(page)
        if self.model.total_count() is not None and self._count_in_flight_key == self.model.count_key():
            # The page held the complete result; the running count is redundant.
            self._cancel_count()
        else:
            # Recount if the page revealed an unnotified data change.
            self._request_count()
        self._update_page_label()
        self._start_deferred_search_index_build()

        logger.debug("Data loading complete: %d rows, has_more=%s", len(page.rows), page.has_more)

    def _on_page_load_error(self, generation: int, evidence_id: int, error: str) -> None:
        """Handle background page loading error."""
        if not self._is_current_result(generation, evidence_id):
            return

        self._data_loading = False
        if generation == self._quiet_generation:
            # A failed background re-check (issue #67) keeps the shown page.
            logger.warning("URL page re-check failed: %s", error)
            self._update_page_label()
            self._start_deferred_search_index_build()
            return
        logger.error("Data loading failed: %s", error)

        # Reset to empty state
        self.model.clear_page()
        self._update_page_label()
        self._start_deferred_search_index_build()

    def _is_current_count(self, generation: int, evidence_id: int) -> bool:
        return generation == self._count_generation and evidence_id == self.evidence_id

    def _on_count_loaded(self, result: QueryResult) -> None:
        if not self._is_current_count(result.generation, result.evidence_id):
            return
        key, count = result.payload
        if key == self._count_in_flight_key:
            self._count_in_flight_key = None
        # Ignored by the model if filters/data changed meanwhile.
        self.model.store_total_count(key, count)
        if not self._data_loading:
            self._update_page_label()

    def _on_count_error(self, generation: int, evidence_id: int, error: str) -> None:
        if not self._is_current_count(generation, evidence_id):
            return
        logger.error("URL count failed: %s", error)
        self._count_failed_key = self._count_in_flight_key
        self._count_in_flight_key = None
        if not self._data_loading:
            self._update_page_label()

    def _page_up(self) -> None:
        if self.model.has_previous_page():
            self._request_page(self.model.current_page() - 1)

    def _page_down(self) -> None:
        if self.model.has_next_page():
            self._request_page(self.model.current_page() + 1)

    def _on_page_size_changed(self, _index: int) -> None:
        size = self.page_size_combo.currentData()
        if size is None or not self.model.set_page_size(int(size)):
            return
        self.page_size_changed.emit(int(size))
        self._reload_flat_from_start()

    def _reload_flat_from_start(self) -> None:
        """Reload the flat view from page 1 (or mark it stale while hidden)."""
        if self.views.currentIndex() == 0:
            self._request_page(0)
        else:
            self._invalidate_flat_load()

    def _update_page_label(self) -> None:
        """Update pagination label: 'Showing X-Y of Z URLs' (Z pending while counting)."""
        rows = self.model.rowCount()
        page = self.model.current_page()
        self.prev_button.setEnabled(self.model.has_previous_page())
        self.next_button.setEnabled(self.model.has_next_page())

        if rows == 0:
            self.page_label.setText("No URLs" if page == 0 else "No URLs on this page")
            return

        total = self.model.total_count()
        if not self.model.row_numbers_exact():
            # Data changed elsewhere since earlier pages were walked: row
            # positions are unknown until page 1 is shown again.
            if total is not None:
                of_total = f" of {total:,}"
            elif self._count_failed_key == self.model.count_key():
                of_total = " (total unavailable)"
            else:
                of_total = " of … (counting)"
            self.page_label.setText(
                f"Page {page + 1:,}: {rows:,}{of_total} URLs; data changed, row numbers hidden"
            )
            return

        start = self.model.first_row_number()
        end = start + rows - 1
        if total is not None:
            text = f"Showing {start:,}-{end:,} of {total:,} URLs"
        elif self._count_failed_key == self.model.count_key():
            text = f"Showing {start:,}-{end:,} URLs (total unavailable)"
        else:
            # Never show an estimate as exact: the total is pending.
            text = f"Showing {start:,}-{end:,} of … URLs (counting)"
        self.page_label.setText(text)

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
        if checked and self._data_loading:
            # A hidden flat load would be applied after later state changes.
            self._invalidate_flat_load()
        self.views.setCurrentIndex(index)
        # Reload the newly visible view only if it went stale while hidden.
        if checked:
            if self._grouped_stale:
                self._reload_grouped()
        elif self._flat_stale:
            self._request_page(self.model.current_page())

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
        dialog.tags_changed.connect(self._on_tags_changed)
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
        # Cached flat-view tags (and tag-filtered totals) are now outdated;
        # the edited URL may be on any flat page.
        self.model.mark_data_changed(keep_position=False)
        self._invalidate_flat_load()

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
        ref_manager = ReferenceListManager(create_dirs=False)
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
            # Disconnect canceled before hitting 100%: autoClose emits
            # canceled which would bump _match_worker_generation and
            # cause _match_finished to silently discard results (#35).
            if progress >= 100:
                try:
                    self.progress_dialog.canceled.disconnect(self._cancel_match_worker)
                except (RuntimeError, TypeError):
                    pass
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

        # Refresh filters to reflect new match lists, then refresh data
        self._populate_filters()
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
        self._shut_down = True
        # Invalidate all pending callbacks.
        self._filter_worker_generation += 1
        self._data_worker_generation += 1
        self._count_generation += 1
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
        self._stop_url_loader()
        self._stop_count_loader()
        self._stop_search_index_build(timeout_ms=2000)
        if self.match_worker is not None:
            _stop_worker(self.match_worker, "UrlMatchWorker")
            self.match_worker = None

        # Stop pending workers that were intentionally kept alive.
        for worker in self._pending_filter_workers:
            _stop_worker(worker, "Pending FilterLoadWorker", timeout_ms=1000)
        self._pending_filter_workers.clear()

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
