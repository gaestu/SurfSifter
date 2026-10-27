from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, Optional, List, Set, Tuple

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

from app.data import (
    STATS_PROCESS_LOG,
    STATS_TAGS,
    STATS_URL_MATCHES,
    STATS_URLS,
    UrlDomainChildPage,
    UrlDomainGroupPage,
    UrlFilterSpec,
    UrlPage,
    UrlSearchIndexStatus,
    ascii_fold,
)
from app.data.case_data import CaseDataAccess
from app.features.urls.models import UrlsTableModel, UrlsGroupedModel
from app.features.urls.models.urls_grouped import FAILED, LOADED, LOADING, NOT_LOADED, group_key
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


def _load_url_domain_groups(case_data: CaseDataAccess, request: QueryRequest) -> UrlDomainGroupPage:
    """Worker-thread handler: load one page of domain groups (issue #64)."""
    return case_data.load_url_domain_groups(request.payload)


def _load_url_domain_children(case_data: CaseDataAccess, request: QueryRequest) -> UrlDomainChildPage:
    """Worker-thread handler: load one page of a domain group's URLs (issue #64)."""
    return case_data.load_url_domain_children(request.payload)


# First element of the count keys of the grouped tree's total (issue #64);
# the flat view's keys start with the evidence id.
_TREE_COUNT = "tree"

# Item data of the Tag dropdown's fixed "Untagged" entry (issue #72). Tag
# entries hold the tag name (a str), so no tag name can equal this object.
_UNTAGGED_TAG = object()


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
    exclude_text: str = ""  # literal URL substring to hide
    source: str = ""
    match: str = "all"  # "all", "matched", "unmatched", or specific list name
    tag: str = "*"  #
    untagged: bool = False  # Tag dropdown on "Untagged" (tag is then "*")


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

        # Grouped tree (issue #64): group pages and child pages load in the
        # background; results of an older tree generation are rejected. The
        # child loader is latest-wins, so child requests run one at a time
        # from a FIFO queue of group keys (sibling expansions do not cancel
        # each other).
        self._group_loader: Optional[CancellableQueryWorker] = None
        self._child_loader: Optional[CancellableQueryWorker] = None
        self._tree_generation = 0
        self._child_queue: Deque[Optional[str]] = deque()
        self._child_request_active = False
        self._child_request_domain: Optional[str] = None
        # Keys of groups expanded before a tree reload, re-expanded when they
        # appear again.
        self._restore_expanded: Set[Optional[str]] = set()
        # The tree's URL total when the flat view's cached total is unknown.
        # Counted by the count loader but kept apart from the flat view's
        # count cache: without the flat view's data_version baseline, a
        # cached total could not be invalidated by an unnotified write.
        self._tree_count_serial = 0
        self._tree_count_key: Optional[tuple] = None
        self._tree_total: Optional[int] = None

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

        # Issue #72: hides URLs containing a literal substring; applied with
        # Search, like the search text.
        self.exclude_label = QLabel("Exclude")
        search_layout.addWidget(self.exclude_label)
        self.exclude_edit = QLineEdit()
        self.exclude_edit.setPlaceholderText("Exclude URLs containing...")
        self.exclude_edit.setToolTip(
            "Hide URLs containing this text (ASCII case-insensitive; %, _ and \\ match literally). "
            "Press Enter or Search to apply."
        )
        self.exclude_edit.returnPressed.connect(self._on_search_clicked)
        search_layout.addWidget(self.exclude_edit)

        self.search_button = QPushButton("Search")
        self.search_button.setToolTip("Apply Search, Exclude and Domain contains")
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
        self._add_fixed_tag_items(self.tag_combo)
        self.tag_combo.currentIndexChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.tag_combo, 2, 1)

        self.group_checkbox = QCheckBox("Group by domain")  # Initialize with text
        self.group_checkbox.toggled.connect(self._toggle_grouping)
        filter_layout.addWidget(self.group_checkbox, 2, 2)  # moved to col 2

        self.match_button = QPushButton("Match Against URL Lists")  # Initialize with text
        self.match_button.clicked.connect(self._match_against_url_lists)  #
        filter_layout.addWidget(self.match_button, 2, 3)  # moved to col 3

        layout.addLayout(filter_layout)

        # Tooltips restored after the dropdown lists loaded (issue #63)
        self._filter_combo_tooltips = {
            combo: combo.toolTip() for combo in (self.domain_combo, self.source_combo, self.tag_combo)
        }

        self.model = UrlsTableModel(case_data)
        # Queued: reload only after the editor has finished committing.
        self.model.tags_edited.connect(self._on_tags_changed, Qt.QueuedConnection)
        self.group_model = UrlsGroupedModel(case_data)
        self.group_model.tags_edited.connect(self._on_group_tags_edited)

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
        # Children load on expansion; "load more"/retry rows on activation.
        self.tree_view.expanded.connect(self._on_tree_expanded)
        self.tree_view.collapsed.connect(self._on_tree_collapsed)
        self.tree_view.activated.connect(self._on_tree_activated)

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
            self._stop_tree_loaders()
            self._restore_expanded.clear()
            self._stop_search_index_build()
        self.case_data = case_data
        self.model.set_case_data(case_data)
        self.group_model.set_case_data(case_data)
        self._grouped_stale = True
        if not defer_load:
            # Loaded now (in the background), so showEvent may refresh/re-check.
            self._data_loaded = self.evidence_id is not None
            self._load_pending = False
            self._update_page_label()
            # First page and dropdown lists load in parallel (issue #63).
            self._apply_filters()
            self._populate_filters()
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
            self._cancel_tree_loads()
            self._restore_expanded.clear()
            self._stop_search_index_build()
        self.evidence_id = evidence_id
        self._evidence_label_cache = evidence_label  #
        self._data_loaded = False
        self._grouped_stale = True

        if not defer_load:
            # Data is loaded in the background; loaded state lets showEvent
            # refresh/re-check (issue #67).
            self._data_loaded = evidence_id is not None
            self._load_pending = False
            self.model.set_evidence(evidence_id, reload=False)
            self.group_model.set_evidence(evidence_id, reload=False)
            self._update_page_label()
            # The first page does not wait for the dropdown lists (issue #63).
            self._apply_filters()
            self._populate_filters()
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
        if not self._load_pending:
            # Domains, sources, match lists and tags may have changed too
            # (served from the shared statistics cache if not; issue #63).
            self._populate_filters()
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
        # New tags appear in the Tag dropdown (tag writes invalidated the
        # cached tag list; issue #63).
        self._populate_filters()

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
        2. Request the first page in the background with the current filters
        3. Load the dropdown lists in parallel (_populate_filters); the first
           page never waits for them (issue #63)
        """
        if self._data_loaded:
            return

        self._data_loaded = True
        self._load_pending = False

        # Set evidence IDs on models WITHOUT triggering immediate reload
        if self.evidence_id is not None:
            self.model.set_evidence(self.evidence_id, reload=False)
            self.group_model.set_evidence(self.evidence_id, reload=False)

        self._update_page_label()
        self._apply_filters()
        self._populate_filters()

    # Helpers ------------------------------------------------------------

    def _populate_filters(self) -> None:
        """
        Populate filter dropdowns (now uses background loading).

        While the expensive DISTINCT queries run in a background thread, each
        dropdown shows only its current selection and is disabled; rows,
        search and paging do not wait for the lists (issue #63).
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
        self._filters_loading = False
        for combo, tooltip in self._filter_combo_tooltips.items():
            combo.setEnabled(True)
            combo.setToolTip(tooltip)
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
        self._add_fixed_tag_items(self.tag_combo)
        self.tag_combo.blockSignals(False)

        self._on_filters_changed()
        self.group_model.set_case_data(None)
        self.group_model.set_evidence(None)

    def _set_filters_loading(self) -> None:
        """Show the loading state: each dropdown keeps only its current selection.

        The Domain, Source and Tag dropdowns are disabled until the lists
        arrive; reading them still yields the current filter values. The Match
        dropdown keeps its fixed entries (issue #63), and the Tag dropdown keeps
        All tags and Untagged (issue #72).
        """
        self._filters_loading = True
        self._fill_filter_combos(None, loading=True)

    def _fill_filter_combos(self, data: Optional[Dict[str, Any]], *, loading: bool = False) -> None:
        """Refill the dropdowns, keeping the current filters selected (issue #63).

        Args:
            data: Loaded lists (domains, sources, match_lists, tags), or None
                for only the current selections (loading, load error).
            loading: Disable the Domain, Source and Tag dropdowns meanwhile.

        A selected value missing from the new lists (e.g. a domain beyond the
        top domains, a tag that no longer exists) is inserted, so the user's
        filter never changes silently. Signals are blocked: nothing reloads.
        """
        data = data or {}
        filters = self.filters

        # Domains (with count info)
        domains_data = data.get("domains") or {}
        domain_items = domains_data.get("items", [])
        total_domains = domains_data.get("total_count", 0)
        truncated = domains_data.get("truncated", False)
        combo = self.domain_combo
        combo.blockSignals(True)
        combo.clear()
        if truncated:
            combo.addItem(f"All domains ({total_domains} total)", userData="*")
        else:
            combo.addItem("All domains", userData="*")
        for item in domain_items:
            domain = item["domain"]
            count = item["count"]
            combo.addItem(f"{domain or '<none>'} ({count:,})", userData=domain)
        if truncated:
            # Add note about more domains available
            remaining = total_domains - len(domain_items)
            combo.addItem(f"... and {remaining} more (use Domain contains)", userData="__more__")
        domain = filters.domain if filters.domain not in ("", "*") else "*"
        # The domain filter ignores ASCII case and the listed spelling
        # (MIN(domain)) may change after ingestion: select the listed entry.
        self._select_combo_value(combo, domain, domain, ascii_case_insensitive=True)
        combo.blockSignals(False)

        # Sources
        combo = self.source_combo
        combo.blockSignals(True)
        combo.clear()
        combo.addItem("All sources", userData="")
        for source in data.get("sources", []):
            combo.addItem(source, userData=source)
        self._select_combo_value(combo, filters.source or "", filters.source)
        combo.blockSignals(False)

        # Match: fixed entries plus list names
        combo = self.match_combo
        combo.blockSignals(True)
        combo.clear()
        combo.addItem("All", userData="all")
        combo.addItem("Matched", userData="matched")
        combo.addItem("Not Matched", userData="unmatched")
        for list_name in data.get("match_lists", []):
            combo.addItem(list_name, userData=list_name)
        self._select_combo_value(combo, filters.match or "all", filters.match, insert_at=3)
        combo.blockSignals(False)

        # Tags: fixed entries plus tag names
        combo = self.tag_combo
        combo.blockSignals(True)
        combo.clear()
        self._add_fixed_tag_items(combo)
        for tag in data.get("tags", []):
            combo.addItem(tag["name"], userData=tag["name"])
        if filters.untagged:
            combo.setCurrentIndex(combo.findData(_UNTAGGED_TAG))
        else:
            self._select_combo_value(combo, filters.tag or "*", filters.tag, insert_at=2)
        combo.blockSignals(False)

        for combo, tooltip in self._filter_combo_tooltips.items():
            combo.setEnabled(not loading)
            combo.setToolTip("Loading…" if loading else tooltip)

    @staticmethod
    def _add_fixed_tag_items(combo: QComboBox) -> None:
        """Add the Tag dropdown's fixed entries; tag names follow them (issue #72)."""
        combo.addItem("All tags", userData="*")
        combo.addItem("Untagged", userData=_UNTAGGED_TAG)
        # Tells it apart from a tag that is itself named "Untagged".
        combo.setItemData(1, "URLs without any tag", Qt.ToolTipRole)

    @staticmethod
    def _select_combo_value(
        combo: QComboBox,
        value: str,
        label: str,
        insert_at: int = 1,
        *,
        ascii_case_insensitive: bool = False,
    ) -> None:
        """Select the item holding ``value``, inserting it (as ``label``) if missing.

        With ``ascii_case_insensitive`` an item whose value equals ``value``
        ignoring ASCII case is selected if no exact match exists (the Domain
        filter matches with ``COLLATE NOCASE``), instead of inserting a
        differently-cased duplicate.
        """
        index = combo.findData(value)
        if index < 0 and ascii_case_insensitive and isinstance(value, str):
            folded = ascii_fold(value)
            for candidate in range(combo.count()):
                data = combo.itemData(candidate)
                if isinstance(data, str) and ascii_fold(data) == folded:
                    index = candidate
                    break
        if index < 0:
            index = min(insert_at, combo.count())
            combo.insertItem(index, label, userData=value)
        combo.setCurrentIndex(index)

    def _combo_filters_differ(self) -> bool:
        """True if the dropdown selections differ from the applied filters.

        Domains compare ignoring ASCII case, like the Domain filter itself.
        """
        def domain_value(value: Optional[str]) -> str:
            return "" if value in (None, "", "*") else ascii_fold(value)

        domain_data = self.domain_combo.currentData()
        if domain_data != "__more__" and domain_value(domain_data) != domain_value(self.filters.domain):
            return True
        return (
            (self.source_combo.currentData() or "") != (self.filters.source or "")
            or (self.match_combo.currentData() or "all") != (self.filters.match or "all")
            or self._tag_combo_selection() != (self.filters.untagged, self.filters.tag or "*")
        )

    def _tag_combo_selection(self) -> Tuple[bool, str]:
        """The Tag dropdown's selection as ``(untagged, tag)``; tag "*" = all."""
        data = self.tag_combo.currentData()
        if data is _UNTAGGED_TAG:
            return True, "*"
        return False, data or "*"

    def _on_filters_loaded(self, data: Dict[str, Any], generation: int = 0) -> None:
        """
        Handle background filter loading completion.

        Refills the dropdowns and keeps the current selections; rows are not
        reloaded, since the effective filter is unchanged (issue #63).

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
        self._fill_filter_combos(data)

        if self._combo_filters_differ():  # defensive: selections are restored above
            self._on_filters_changed()

        logger.debug(
            "Filter loading complete: %d domains, %d sources, %d match_lists, %d tags",
            len((data.get("domains") or {}).get("items", [])),
            len(data.get("sources", [])),
            len(data.get("match_lists", [])),
            len(data.get("tags", [])),
        )

    def _on_filters_load_error(self, error: str, generation: int = 0) -> None:
        """Handle background filter loading error (current selections are kept)."""
        if generation != self._filter_worker_generation:
            return

        self._filters_loading = False
        self._retire_filter_worker()

        logger.error("Filter loading failed: %s", error)

        self._fill_filter_combos(None)
        if self._combo_filters_differ():  # defensive: selections are kept above
            self._on_filters_changed()

    def _on_search_clicked(self) -> None:
        """Handle explicit search button click (performance optimization)."""
        # Dropdowns still loading hold the current filter values (issue #63)
        domain_data = self.domain_combo.currentData()
        if domain_data not in ("__loading__", "__more__"):
            self.filters.domain = domain_data or ""
        self.filters.domain_contains = self.domain_contains_edit.text().strip()
        self.filters.text = self.search_edit.text().strip()
        self.filters.exclude_text = self.exclude_edit.text().strip()
        self.filters.source = self.source_combo.currentData() or ""
        self.filters.match = self.match_combo.currentData() or "all"
        self.filters.untagged, self.filters.tag = self._tag_combo_selection()
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
        self.filters.untagged, self.filters.tag = self._tag_combo_selection()
        self._apply_filters()

    def _apply_filters(self) -> None:
        """
        Apply current filters to the URL list.

        The flat view is loaded in the background (issue #60); filters are
        only stored on the models here.
        """
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
            exclude_text=self.filters.exclude_text,
            sources=sources,
            match_filter=self.filters.match,  #
            tag=self.filters.tag,  #
            untagged=self.filters.untagged,
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
            exclude=self.filters.exclude_text,
            sources=sources,
            match_filter=self.filters.match,  #
            tag=self.filters.tag,  #
            untagged=self.filters.untagged,
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
            self._invalidate_tree()
            self._request_page(page)

    # Grouped tree (issue #64) -------------------------------------------

    def _invalidate_tree(self) -> None:
        """Mark the grouped tree stale and drop its in-flight loads."""
        self._grouped_stale = True
        self._cancel_tree_loads()

    def _cancel_tree_loads(self) -> None:
        """Reject in-flight tree results and drop queued child requests."""
        self._tree_generation += 1
        self._child_queue.clear()
        self._child_request_active = False
        self._tree_count_key = None
        self._tree_total = None
        for loader in (self._group_loader, self._child_loader):
            if loader is not None:
                loader.cancel()

    def _stop_tree_loaders(self, timeout_ms: int = 2000) -> None:
        self._cancel_tree_loads()
        group_loader, self._group_loader = self._group_loader, None
        child_loader, self._child_loader = self._child_loader, None
        self._stop_loader(group_loader, timeout_ms)
        self._stop_loader(child_loader, timeout_ms)

    def _reload_grouped(self) -> None:
        """Reload the grouped tree: first group page in the background, no children.

        Groups expanded before are re-expanded (their first child page is
        reloaded) when they appear again.
        """
        if self._load_pending:
            return
        self._grouped_stale = False
        self._cancel_tree_loads()
        model = self.group_model
        if model.evidence_id == self.evidence_id:
            expanded = {
                group_key(model.group_domain(row))
                for row in range(model.group_count())
                if self.tree_view.isExpanded(model.index(row, 0))
            }
            # A reload before the first page arrived keeps the pending keys.
            if expanded or model.group_count():
                self._restore_expanded = expanded
        else:
            self._restore_expanded.clear()
        model.reset_tree()
        if not self.case_data or self.evidence_id is None:
            self._update_page_label()
            return
        if model.evidence_id != self.evidence_id:
            model.set_evidence(self.evidence_id, reload=False)
        self._request_group_page()
        self._request_tree_count()
        self._update_page_label()

    def _request_tree_count(self) -> None:
        """Count the tree's URLs unless the flat view's total (same filters) is cached."""
        self._tree_count_key = None
        self._tree_total = None
        if not self.case_data or self.evidence_id is None or self.model.total_count() is not None:
            return
        loader = self._ensure_count_loader()
        if loader is None:
            return
        self._tree_count_serial += 1
        self._tree_count_key = (_TREE_COUNT, self._tree_count_serial)
        self._submit_count(loader, self._tree_count_key, self.group_model.filter_spec())

    def _ensure_group_loader(self) -> Optional[CancellableQueryWorker]:
        if self._group_loader is None:
            self._group_loader = self._create_loader(
                _load_url_domain_groups, "UrlGroupLoader", self._on_group_page_loaded, self._on_group_page_error
            )
        return self._group_loader

    def _ensure_child_loader(self) -> Optional[CancellableQueryWorker]:
        if self._child_loader is None:
            self._child_loader = self._create_loader(
                _load_url_domain_children, "UrlGroupChildLoader", self._on_child_page_loaded, self._on_child_page_error
            )
        return self._child_loader

    def _is_current_tree_result(self, generation: int, evidence_id: int) -> bool:
        return generation == self._tree_generation and evidence_id == self.evidence_id

    def _request_group_page(self) -> None:
        """Load the next group page (first page, more domains, or retry)."""
        if not self.case_data or self.evidence_id is None:
            return
        query = self.group_model.begin_group_load()
        if query is None:
            return
        loader = self._ensure_group_loader()
        if loader is None:
            self.group_model.fail_group_load("Case context unavailable for background URL loading")
        else:
            loader.submit(QueryRequest(self._tree_generation, int(self.evidence_id), query))
        self._update_page_label()

    def _on_group_page_loaded(self, result: QueryResult) -> None:
        if not self._is_current_tree_result(result.generation, result.evidence_id):
            return
        page: UrlDomainGroupPage = result.payload
        if self.group_model.apply_group_page(page):
            self._restore_expanded_groups()
        self._update_page_label()

    def _on_group_page_error(self, generation: int, evidence_id: int, error: str) -> None:
        if not self._is_current_tree_result(generation, evidence_id):
            return
        logger.error("URL domain group loading failed: %s", error)
        self.group_model.fail_group_load(error)
        self._update_page_label()

    def _restore_expanded_groups(self) -> None:
        for key in list(self._restore_expanded):
            row = self.group_model.group_row(key)
            if row is not None:
                self._restore_expanded.discard(key)
                # Emits expanded: the first child page is requested.
                self.tree_view.expand(self.group_model.index(row, 0))

    def _on_tree_expanded(self, index) -> None:  # noqa: ANN001
        model = self.group_model
        if not model.is_group_index(index):
            return
        row = index.row()
        model.group_expanded(row)
        if model.group_state(row) in (NOT_LOADED, FAILED):
            self._queue_child_load(row)

    def _on_tree_collapsed(self, index) -> None:  # noqa: ANN001
        model = self.group_model
        if not model.is_group_index(index):
            return
        row = index.row()
        key = group_key(model.group_domain(row))
        if key in self._child_queue:
            # Not started yet: drop it (an in-flight page is still applied).
            self._child_queue.remove(key)
            model.cancel_child_load(row)
        model.group_collapsed(row)

    def _on_tree_activated(self, index) -> None:  # noqa: ANN001
        """Load more domains/URLs, or retry, from an activated status row."""
        model = self.group_model
        if not model.is_status_index(index):
            return
        parent = index.parent()
        if not parent.isValid():
            self._request_group_page()
        else:
            self._queue_child_load(parent.row())

    def _queue_child_load(self, row: int) -> None:
        if not self.group_model.begin_child_load(row):
            return
        self._child_queue.append(group_key(self.group_model.group_domain(row)))
        self._pump_child_queue()

    def _pump_child_queue(self) -> None:
        """Submit the next queued child request unless one is running."""
        model = self.group_model
        while not self._child_request_active and self._child_queue:
            key = self._child_queue.popleft()
            row = model.group_row(key)
            if row is None or model.group_state(row) != LOADING:
                continue
            domain = model.group_domain(row)
            loader = self._ensure_child_loader()
            if loader is None or self.evidence_id is None:
                model.fail_child_load(domain, "Case context unavailable for background URL loading")
                continue
            self._child_request_active = True
            self._child_request_domain = domain
            loader.submit(QueryRequest(self._tree_generation, int(self.evidence_id), model.build_child_query(row)))

    def _on_child_page_loaded(self, result: QueryResult) -> None:
        if not self._is_current_tree_result(result.generation, result.evidence_id):
            return
        self._child_request_active = False
        page: UrlDomainChildPage = result.payload
        model = self.group_model
        if model.apply_child_page(page):
            row = model.group_row(group_key(page.query.domain))
            if row is not None and not self.tree_view.isExpanded(model.index(row, 0)):
                # Collapsed while loading: keep retained rows bounded.
                model.trim_collapsed()
        self._pump_child_queue()

    def _on_child_page_error(self, generation: int, evidence_id: int, error: str) -> None:
        if not self._is_current_tree_result(generation, evidence_id):
            return
        self._child_request_active = False
        logger.error("URL group child loading failed: %s", error)
        self.group_model.fail_child_load(self._child_request_domain, error)
        self._pump_child_queue()

    def _create_loader(self, handler, name: str, on_result, on_error, limiter=None) -> Optional[CancellableQueryWorker]:
        """Background loader for the current case (None without case context)."""
        case_folder = getattr(self.case_data, "case_folder", None)
        case_db_path = getattr(self.case_data, "db_path", None)
        if case_folder is None or case_db_path is None:
            return None
        loader = CancellableQueryWorker(Path(case_folder), Path(case_db_path), handler, limiter=limiter, name=name)
        loader.result_ready.connect(on_result)
        loader.failed.connect(on_error)
        return loader

    def _ensure_url_loader(self) -> Optional[CancellableQueryWorker]:
        if self._url_loader is None:
            self._url_loader = self._create_loader(
                _load_url_page, "UrlPageLoader", self._on_page_loaded, self._on_page_load_error
            )
        return self._url_loader

    def _ensure_count_loader(self) -> Optional[CancellableQueryWorker]:
        if self._count_loader is None:
            # Own limiter: long counts must not hold the interactive read slots.
            self._count_loader = self._create_loader(
                _count_urls, "UrlCountLoader", self._on_count_loaded, self._on_count_error,
                limiter=background_read_limiter(),
            )
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
        # The deferred load will trigger the load when done. The dropdown lists
        # loading meanwhile never delay rows (issue #63).
        if self._load_pending:
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
        self._submit_count(loader, key, self.model.filter_spec())

    def _submit_count(self, loader: CancellableQueryWorker, key: tuple, filters: UrlFilterSpec) -> None:
        """Start counting ``filters``; the result is delivered with ``key``."""
        self._count_generation += 1
        self._count_in_flight_key = key
        self._count_failed_key = None
        loader.submit(QueryRequest(self._count_generation, int(self.evidence_id), UrlCountRequest(key, filters)))

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
        if self.model.set_loaded_page(page):
            self._on_unnotified_data_change()
        if self.model.total_count() is not None and self._count_in_flight_key == self.model.count_key():
            # The page held the complete result; the running count is redundant.
            self._cancel_count()
        else:
            # Recount if the page revealed an unnotified data change.
            self._request_count()
        self._update_page_label()
        self._start_deferred_search_index_build()

        logger.debug("Data loading complete: %d rows, has_more=%s", len(page.rows), page.has_more)

    def _on_unnotified_data_change(self) -> None:
        """A loaded page revealed a write nobody announced (e.g. a tag merge).

        Safety net of the shared statistics cache (issue #63): URL, tag and
        match statistics of this evidence are marked stale and the dropdown
        lists reload. The tab's own search index writes are set aside by the
        model and never get here.
        """
        if not self.case_data or self.evidence_id is None:
            return
        try:
            self.case_data.invalidate_stats(int(self.evidence_id), (STATS_URLS, STATS_TAGS, STATS_URL_MATCHES))
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to invalidate URL statistics: %s", exc)
        # The hidden grouped tree holds counts, tags and matches read before
        # that write: reload it when it is shown again.
        self._invalidate_tree()
        self._populate_filters()

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
        if key[0] == _TREE_COUNT:
            if key == self._tree_count_key:
                self._tree_total = count
        else:
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
        if self.views.currentIndex() == 1:
            self._update_grouped_label()
            return
        self.page_size_combo.setEnabled(True)
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

    def _update_grouped_label(self) -> None:
        """Grouped view: loaded domains and the exact URL total (issue #64).

        Flat paging controls do not apply to the tree and are disabled.
        """
        self.prev_button.setEnabled(False)
        self.next_button.setEnabled(False)
        self.page_size_combo.setEnabled(False)
        model = self.group_model
        groups = model.group_count()
        if groups == 0:
            state = model.state()
            if state == LOADING:
                text = "Loading domains…"
            elif state == FAILED:
                text = "Could not load domains"
            elif self.evidence_id is not None and state != NOT_LOADED:
                text = "No URLs"
            else:
                text = ""
            self.page_label.setText(text)
            return
        text = f"{groups:,} domain{'s' if groups != 1 else ''} shown"
        if model.has_more_groups():
            text += ", more available"
        total = self.model.total_count()
        if total is None:
            total = self._tree_total
        if total is None and not model.has_more_groups() and model.state() == LOADED:
            # All groups loaded: their exact counts add up to the total.
            total = sum(model.group_url_count(row) for row in range(groups))
        key = self._tree_count_key
        if total is not None:
            text += f" · {total:,} URLs"
        elif key is not None and self._count_in_flight_key == key:
            text += " · … URLs (counting)"
        elif key is not None and self._count_failed_key == key:
            text += " · total unavailable"
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
        # Refresh data and the dropdown lists (removed sources; the dialog
        # invalidated the cached statistics)
        self.refresh()

    def _toggle_grouping(self, checked: bool) -> None:
        index = 1 if checked else 0
        if checked and self._data_loading:
            # A hidden flat load would be applied after later state changes.
            self._invalidate_flat_load()
        self.views.setCurrentIndex(index)
        # Reload the newly visible view only if it went stale while hidden;
        # otherwise it is shown as it was (no queries).
        if checked and self._grouped_stale:
            self._reload_grouped()
        elif not checked and self._flat_stale:
            self._request_page(self.model.current_page())
        else:
            self._update_page_label()

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
            # Loaded URL rows only; group and status rows are skipped.
            indexes = self.tree_view.selectionModel().selectedRows()
            for idx in indexes:
                row = self.group_model.url_row(idx)
                if row:
                    rows.append(row)
        return rows

    def _on_group_tags_edited(self) -> None:
        """An inline Tags edit in the grouped view was written (by the model)."""
        if not self.case_data or self.evidence_id is None:
            return
        # Cached flat-view tags (and tag-filtered totals) are now outdated;
        # the edited URL may be on any flat page.
        self.model.mark_data_changed(keep_position=False)
        self._invalidate_flat_load()
        if self.views.currentIndex() == 1:
            # Tag-filtered totals may have changed.
            self._request_tree_count()
        self._update_page_label()
        # A new tag name appears in the Tag dropdown (issue #63)
        self._populate_filters()

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
        # Lists matched before the cancel are committed (issue #63)
        self._invalidate_match_stats()

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

        # New matches: cached match lists are stale (issue #63). refresh()
        # reloads the data and the dropdown lists (new match lists).
        self._invalidate_match_stats()
        self.refresh()

    def _invalidate_match_stats(self) -> None:
        """URL list matching wrote url_matches (and process_log) (issue #63)."""
        if self.case_data is not None and self.evidence_id is not None:
            self.case_data.invalidate_stats(int(self.evidence_id), (STATS_URL_MATCHES, STATS_PROCESS_LOG))

    def _match_error(self, error_msg: str, generation: int = 0) -> None:
        """Handle match error."""
        if generation != self._match_worker_generation:
            return
        # Lists matched before the error are committed (issue #63)
        self._invalidate_match_stats()

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
        self._tree_generation += 1
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
        self._stop_tree_loaders()
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
