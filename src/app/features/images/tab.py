"""
Images Tab Widget
Workers extracted to separate module.
"""

from __future__ import annotations

import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from PySide6.QtCore import Qt, QSize, Signal, QPoint, QUrl, QModelIndex, QRect, QTimer
from PySide6.QtWidgets import (
    QDialog,
    QFileDialog,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QListView,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableView,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
    QComboBox,
    QProgressBar,
    QProgressDialog,
    QStyle,
    QStyleOptionButton,
    QStyleOptionViewItem,
    QStyledItemDelegate,
    QApplication,
    QMenu,
)
from PySide6.QtGui import QDesktopServices, QPainter

from app.common.dialogs import TagArtifactsDialog, HashListSelectorDialog, ImagePreviewDialog
from app.data.case_data import CaseDataAccess
from app.features.images.models import ImagesListModel, ImageClustersModel, ImagesTableModel
from core.matching import ReferenceListManager
from core.logging import get_logger
from core.enums import BROWSER_IMAGE_SOURCES

# Workers extracted to separate module
from app.features.images.workers import HashCheckWorker, ClusterLoadWorker, ImageFilterLoadWorker

LOGGER = get_logger("app.features.images")


class ImageThumbnailDelegate(QStyledItemDelegate):
    """
    Custom delegate for image thumbnails with checkbox overlay and browser badge.

    Renders a checkbox in the top-left corner and a browser badge in the top-right
    corner for images from browser cache/storage sources.
    """

    CHECKBOX_SIZE = 20
    CHECKBOX_MARGIN = 4
    BADGE_SIZE = 18
    BADGE_MARGIN = 4

    def initStyleOption(self, option: QStyleOptionViewItem, index: QModelIndex) -> None:
        """Initialize style option, suppressing Qt's default checkbox rendering."""
        super().initStyleOption(option, index)
        # Clear the check state so Qt doesn't draw its own checkbox
        # We handle checkbox rendering ourselves in paint()
        option.checkState = Qt.Unchecked
        option.features &= ~QStyleOptionViewItem.HasCheckIndicator

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index: QModelIndex) -> None:
        """Paint the item with checkbox overlay and browser badge."""
        # Draw the base item (thumbnail) - checkbox suppressed via initStyleOption
        super().paint(painter, option, index)

        # Get check state from model
        check_state = index.data(Qt.CheckStateRole)
        if check_state is None:
            return  # No checkbox for this item

        # Calculate checkbox rect in top-left corner
        checkbox_rect = QRect(
            option.rect.left() + self.CHECKBOX_MARGIN,
            option.rect.top() + self.CHECKBOX_MARGIN,
            self.CHECKBOX_SIZE,
            self.CHECKBOX_SIZE,
        )

        # Draw checkbox using style
        style = option.widget.style() if option.widget else QApplication.style()
        checkbox_option = QStyleOptionButton()
        checkbox_option.rect = checkbox_rect
        checkbox_option.state = QStyle.State_Enabled

        if check_state == Qt.Checked:
            checkbox_option.state |= QStyle.State_On
        else:
            checkbox_option.state |= QStyle.State_Off

        # Draw semi-transparent background for checkbox visibility
        painter.save()
        painter.setBrush(Qt.white)
        painter.setPen(Qt.NoPen)
        painter.setOpacity(0.7)
        painter.drawRoundedRect(checkbox_rect.adjusted(-2, -2, 2, 2), 3, 3)
        painter.restore()

        style.drawControl(QStyle.CE_CheckBox, checkbox_option, painter)

        # Draw browser badge in top-right corner for browser-sourced images
        # Uses has_browser_source field from v_image_sources view join
        row = index.data(Qt.UserRole)
        if row and row.get("has_browser_source"):
            badge_rect = QRect(
                option.rect.right() - self.BADGE_SIZE - self.BADGE_MARGIN,
                option.rect.top() + self.BADGE_MARGIN,
                self.BADGE_SIZE,
                self.BADGE_SIZE,
            )

            # Draw badge background (blue circle)
            painter.save()
            from PySide6.QtGui import QColor, QFont
            painter.setRenderHint(QPainter.Antialiasing, True)
            painter.setBrush(QColor(0, 120, 212))  # Windows blue
            painter.setPen(Qt.NoPen)
            painter.drawEllipse(badge_rect)

            # Draw globe icon (🌐 approximation using text)
            painter.setPen(Qt.white)
            font = QFont()
            font.setPixelSize(12)
            font.setBold(True)
            painter.setFont(font)
            painter.drawText(badge_rect, Qt.AlignCenter, "🌐")
            painter.restore()

    def editorEvent(self, event, model, option, index) -> bool:
        """Handle mouse clicks on checkbox."""
        from PySide6.QtCore import QEvent
        from PySide6.QtGui import QMouseEvent

        # Only handle mouse button release
        if event.type() != QEvent.MouseButtonRelease:
            return super().editorEvent(event, model, option, index)

        # Check if click is within checkbox area
        checkbox_rect = QRect(
            option.rect.left() + self.CHECKBOX_MARGIN,
            option.rect.top() + self.CHECKBOX_MARGIN,
            self.CHECKBOX_SIZE,
            self.CHECKBOX_SIZE,
        )

        if isinstance(event, QMouseEvent) and checkbox_rect.contains(event.pos()):
            # Toggle check state
            current_state = index.data(Qt.CheckStateRole)
            if current_state is not None:
                new_state = Qt.Unchecked if current_state == Qt.Checked else Qt.Checked
                model.setData(index, new_state, Qt.CheckStateRole)
                return True

        return super().editorEvent(event, model, option, index)


@dataclass
class ImageFilters:
    tags: str = ""
    source: str = ""
    extension: str = ""
    hash_match: str = ""  # Filter by hash list matches
    url_text: str = ""  # Case-insensitive URL substring filter
    min_size_bytes: Optional[int] = None
    max_size_bytes: Optional[int] = None


# Size filter presets (label, min_bytes, max_bytes)
SIZE_FILTER_PRESETS = [
    ("All sizes", None, None),
    ("< 50 KB", None, 50 * 1024),
    ("50 KB – 500 KB", 50 * 1024, 500 * 1024),
    ("500 KB – 5 MB", 500 * 1024, 5 * 1024 * 1024),
    ("> 5 MB", 5 * 1024 * 1024, None),
]


class ImagesTab(QWidget):
    hashLookupFinished = Signal()
    def __init__(
        self,
        case_data: Optional[CaseDataAccess] = None,
        case_folder: Optional[Path] = None,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.case_data = case_data
        self.case_folder = case_folder
        self.evidence_id: Optional[int] = None
        self.filters = ImageFilters()
        self.hash_db_path: Optional[Path] = None
        self._current_phash: Optional[str] = None
        self._view_mode = "grid"  # "grid", "clusters", or "table"

        # Phase 2: Checkbox tagging - persistent checked state across pagination
        self._checked_image_ids: Set[int] = set()

        # Phase 3: Lazy loading state
        self._data_loaded = False
        self._load_pending = False

        # Stale data flag for lazy refresh after ingestion
        self._data_stale = False

        # Background filter loading worker
        self._filter_worker: Optional[ImageFilterLoadWorker] = None

        # Background cluster loading worker
        self._cluster_worker: Optional[ClusterLoadWorker] = None

        # Total image count for pagination display
        self._total_image_count: int = 0

        layout = QVBoxLayout()

        # Filters row (shared across all tabs)
        filter_layout = QGridLayout()
        self.tags_label = QLabel("Tags")
        filter_layout.addWidget(self.tags_label, 0, 0)
        self.tag_combo = QComboBox()
        self.tag_combo.addItem("All tags", userData="")
        self.tag_combo.currentIndexChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.tag_combo, 0, 1)

        self.source_label = QLabel("Source")
        filter_layout.addWidget(self.source_label, 0, 2)
        self.source_combo = QComboBox()
        self.source_combo.addItem("All sources", userData="")
        self.source_combo.currentIndexChanged.connect(self._on_filters_changed)
        filter_layout.addWidget(self.source_combo, 0, 3)

        # Extension filter
        self.extension_label = QLabel("Extension")
        filter_layout.addWidget(self.extension_label, 0, 4)
        self.extension_combo = QComboBox()
        self.extension_combo.addItem("All extensions", userData="")
        self.extension_combo.currentIndexChanged.connect(self._on_filters_changed)
        self.extension_combo.setToolTip("Filter by file extension (gif, jpg, bmp, etc.)")
        filter_layout.addWidget(self.extension_combo, 0, 5)

        # Phase 3: Size filter
        self.size_label = QLabel("Size")
        filter_layout.addWidget(self.size_label, 0, 6)
        self.size_combo = QComboBox()
        for label, _min, _max in SIZE_FILTER_PRESETS:
            self.size_combo.addItem(label, userData=(_min, _max))
        self.size_combo.currentIndexChanged.connect(self._on_filters_changed)
        self.size_combo.setToolTip("Filter by file size (requires size data)")
        filter_layout.addWidget(self.size_combo, 0, 7)

        # Hash match filter
        self.hash_match_label = QLabel("Hash Match")
        filter_layout.addWidget(self.hash_match_label, 0, 8)
        self.hash_match_combo = QComboBox()
        self.hash_match_combo.addItem("All images", userData="")
        self.hash_match_combo.addItem("Any match", userData="__any__")
        self.hash_match_combo.currentIndexChanged.connect(self._on_filters_changed)
        self.hash_match_combo.setToolTip("Filter by hash list matches (run 'Check Known Hashes' first)")
        filter_layout.addWidget(self.hash_match_combo, 0, 9)

        # URL text filter
        self.url_filter_label = QLabel("URL")
        filter_layout.addWidget(self.url_filter_label, 1, 0)
        self.url_filter_input = QLineEdit()
        self.url_filter_input.setPlaceholderText("Filter URLs (contains...)")
        self.url_filter_input.setToolTip("Case-insensitive URL substring filter on image cache URLs")
        self.url_filter_input.setClearButtonEnabled(True)
        self._url_filter_timer = QTimer(self)
        self._url_filter_timer.setSingleShot(True)
        self._url_filter_timer.setInterval(400)  # Debounce 400ms
        self._url_filter_timer.timeout.connect(self._on_filters_changed)
        self.url_filter_input.textChanged.connect(self._on_url_filter_text_changed)
        filter_layout.addWidget(self.url_filter_input, 1, 1, 1, 7)

        # Reset filters button
        self.reset_filters_button = QPushButton("↻ Reset")
        self.reset_filters_button.setToolTip("Reset all filters to defaults and reload images")
        self.reset_filters_button.clicked.connect(self._reset_filters)
        self.reset_filters_button.setMaximumWidth(80)
        filter_layout.addWidget(self.reset_filters_button, 1, 8)

        layout.addLayout(filter_layout)

        # === Subtabs: Grid, Clusters, Table ===
        self.view_tabs = QTabWidget()
        self.view_tabs.currentChanged.connect(self._on_tab_changed)

        # --- Grid Tab ---
        grid_widget = QWidget()
        grid_layout = QVBoxLayout(grid_widget)
        grid_layout.setContentsMargins(0, 0, 0, 0)

        self.model = ImagesListModel(case_data, case_folder=case_folder)
        self.model.set_checked_ids(self._checked_image_ids)
        self.model.set_check_callback(self.on_image_check_changed)
        self.list_view = QListView()
        self.list_view.setViewMode(QListView.IconMode)
        self.list_view.setResizeMode(QListView.Adjust)
        self.list_view.setIconSize(QSize(200, 200))
        self.list_view.setUniformItemSizes(True)
        self.list_view.setSpacing(8)
        self.list_view.setLayoutMode(QListView.Batched)
        self.list_view.setWordWrap(False)
        self.list_view.setItemDelegate(ImageThumbnailDelegate(self.list_view))
        self.list_view.setModel(self.model)
        self.list_view.selectionModel().currentChanged.connect(self._on_selection_changed)
        self.list_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.list_view.customContextMenuRequested.connect(self._show_context_menu)
        self.list_view.doubleClicked.connect(self._on_image_double_clicked)
        grid_layout.addWidget(self.list_view)
        self.view_tabs.addTab(grid_widget, "🖼️ Grid")

        # --- Clusters Tab ---
        clusters_widget = QWidget()
        clusters_layout = QVBoxLayout(clusters_widget)
        clusters_layout.setContentsMargins(0, 0, 0, 0)

        # Splitter: cluster list on top, member detail on bottom
        self.cluster_splitter = QSplitter(Qt.Vertical)

        self.cluster_model = ImageClustersModel(case_data, case_folder=case_folder)
        self.cluster_model.set_checked_ids(self._checked_image_ids)
        self.cluster_model.set_check_callback(self.on_image_check_changed)
        self.cluster_view = QListView()
        self.cluster_view.setViewMode(QListView.IconMode)
        self.cluster_view.setResizeMode(QListView.Adjust)
        self.cluster_view.setIconSize(QSize(160, 160))
        self.cluster_view.setUniformItemSizes(True)
        self.cluster_view.setSpacing(8)
        self.cluster_view.setLayoutMode(QListView.Batched)
        self.cluster_view.setItemDelegate(ImageThumbnailDelegate(self.cluster_view))
        self.cluster_view.setModel(self.cluster_model)
        self.cluster_view.clicked.connect(self._on_cluster_clicked)
        self.cluster_view.doubleClicked.connect(self._on_cluster_double_clicked)
        self.cluster_splitter.addWidget(self.cluster_view)

        # Cluster member detail view
        self.cluster_members_view = QListView()
        self.cluster_members_view.setViewMode(QListView.IconMode)
        self.cluster_members_view.setResizeMode(QListView.Adjust)
        self.cluster_members_view.setIconSize(QSize(120, 120))
        self.cluster_members_view.setUniformItemSizes(True)
        self.cluster_members_view.setSpacing(4)
        self.cluster_members_model = ImagesListModel(case_data, case_folder=case_folder)
        self.cluster_members_model.set_checked_ids(self._checked_image_ids)
        self.cluster_members_model.set_check_callback(self.on_image_check_changed)
        self.cluster_members_view.setItemDelegate(ImageThumbnailDelegate(self.cluster_members_view))
        self.cluster_members_view.setModel(self.cluster_members_model)
        self.cluster_members_view.doubleClicked.connect(self._on_cluster_member_double_clicked)

        # Wrap members view with a label
        members_container = QWidget()
        members_layout = QVBoxLayout(members_container)
        members_layout.setContentsMargins(0, 0, 0, 0)
        self.cluster_members_label = QLabel("Click a cluster above to see all similar images")
        self.cluster_members_label.setStyleSheet("color: #666; padding: 4px;")
        members_layout.addWidget(self.cluster_members_label)
        members_layout.addWidget(self.cluster_members_view)
        self.cluster_splitter.addWidget(members_container)

        self.cluster_splitter.setSizes([300, 200])
        clusters_layout.addWidget(self.cluster_splitter)
        self.view_tabs.addTab(clusters_widget, "🔗 Clusters")

        # --- Table Tab ---
        table_widget = QWidget()
        table_layout = QVBoxLayout(table_widget)
        table_layout.setContentsMargins(0, 0, 0, 0)

        self.table_model = ImagesTableModel(case_data, case_folder=case_folder)
        self.table_model.set_checked_ids(self._checked_image_ids)
        self.table_model.set_check_callback(self.on_image_check_changed)
        self.table_view = QTableView()
        self.table_view.setModel(self.table_model)
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setSelectionMode(QTableView.ExtendedSelection)
        self.table_view.setSortingEnabled(True)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.verticalHeader().setVisible(False)
        self.table_view.horizontalHeader().setStretchLastSection(True)
        self.table_view.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_view.customContextMenuRequested.connect(self._show_table_context_menu)
        self.table_view.doubleClicked.connect(self._on_table_double_clicked)
        # Set row height for thumbnails and column widths
        self.table_view.verticalHeader().setDefaultSectionSize(40)
        self.table_view.setColumnWidth(0, 50)   # Thumbnail
        self.table_view.setColumnWidth(1, 200)  # Filename
        self.table_view.setColumnWidth(2, 80)   # Size
        self.table_view.setColumnWidth(3, 180)  # Sources
        self.table_view.setColumnWidth(4, 300)  # Original Path
        self.table_view.setColumnWidth(5, 150)  # Timestamp
        self.table_view.setColumnWidth(6, 150)  # MD5
        table_layout.addWidget(self.table_view)
        self.view_tabs.addTab(table_widget, "📋 Table")

        layout.addWidget(self.view_tabs)

        # Phase 2: Checked count label
        checked_layout = QHBoxLayout()
        self.checked_count_label = QLabel()
        self.checked_count_label.setStyleSheet("color: #0066cc; font-weight: bold;")
        checked_layout.addWidget(self.checked_count_label)

        self.check_visible_button = QPushButton("Check Visible")
        self.check_visible_button.setToolTip("Check all currently visible images")
        self.check_visible_button.clicked.connect(self._check_visible)
        checked_layout.addWidget(self.check_visible_button)

        self.uncheck_visible_button = QPushButton("Uncheck Visible")
        self.uncheck_visible_button.setToolTip("Uncheck all currently visible images")
        self.uncheck_visible_button.clicked.connect(self._uncheck_visible)
        checked_layout.addWidget(self.uncheck_visible_button)

        self.clear_checks_button = QPushButton("Clear Checks")
        self.clear_checks_button.clicked.connect(self._clear_checked)
        self.clear_checks_button.setEnabled(False)
        checked_layout.addWidget(self.clear_checks_button)
        checked_layout.addStretch()
        layout.addLayout(checked_layout)
        self._update_checked_count_label()

        controls = QHBoxLayout()
        self.prev_button = QPushButton("Previous")
        self.prev_button.clicked.connect(self._page_up)
        self.next_button = QPushButton("Next")
        self.next_button.clicked.connect(self._page_down)
        self.page_label = QLabel()

        self.export_button = QPushButton("Export Selected")
        self.export_button.clicked.connect(self._export_selected)
        self.export_clusters_button = QPushButton("Export Clusters CSV")
        self.export_clusters_button.clicked.connect(self._export_clusters)
        self.export_clusters_button.hide()  # Only show in cluster mode

        # Tag Checked button (primary batch-tag action)
        self.tag_checked_button = QPushButton("Tag Checked")
        self.tag_checked_button.clicked.connect(self._tag_checked)
        self.tag_checked_button.setToolTip(
            "Tag all checked images (tick checkboxes, then click here)"
        )
        self.tag_checked_button.setEnabled(False)

        # Check Known Hashes button
        self.check_hashes_button = QPushButton("Check Known Hashes")
        self.check_hashes_button.clicked.connect(self._check_known_hashes)
        self.check_hashes_button.setToolTip(
            "Check all images against selected hash lists"
        )

        self.similar_button = QPushButton("Find Similar")
        self.similar_button.clicked.connect(self._find_similar)
        self.similar_button.setEnabled(False)

        # Pagination controls
        controls.addWidget(self.prev_button)
        controls.addWidget(self.next_button)

        # Page size selector
        controls.addWidget(QLabel("Per page:"))
        self.page_size_combo = QComboBox()
        self.page_size_combo.setFixedWidth(70)
        for size in [100, 200, 500, 1000]:
            self.page_size_combo.addItem(str(size), userData=size)
        self.page_size_combo.setCurrentIndex(1)  # Default 200
        self.page_size_combo.currentIndexChanged.connect(self._on_page_size_changed)
        controls.addWidget(self.page_size_combo)

        # Page label with total
        controls.addWidget(self.page_label)

        # Go to page
        controls.addWidget(QLabel("Go to:"))
        self.goto_page_input = QLineEdit()
        self.goto_page_input.setFixedWidth(50)
        self.goto_page_input.setPlaceholderText("#")
        self.goto_page_input.returnPressed.connect(self._goto_page)
        controls.addWidget(self.goto_page_input)

        controls.addStretch()
        controls.addWidget(self.export_button)
        controls.addWidget(self.export_clusters_button)
        controls.addWidget(self.tag_checked_button)
        controls.addWidget(self.check_hashes_button)
        controls.addWidget(self.similar_button)
        layout.addLayout(controls)

        self.setLayout(layout)
        self._update_page_label()

    # Public API ---------------------------------------------------------

    def set_case_data(self, case_data: Optional[CaseDataAccess], case_folder: Optional[Path] = None, defer_load: bool = False) -> None:
        """
        Set the case data access object.

        Args:
            case_data: CaseDataAccess instance
            case_folder: Path to case folder
            defer_load: If True, defer data loading until tab is visible (Phase 3)
        """
        self.case_data = case_data
        if case_folder is not None:
            self.case_folder = case_folder
        self.model.set_case_data(case_data, case_folder=case_folder)
        self.cluster_model.set_case_data(case_data, case_folder=case_folder)
        self.table_model.set_case_data(case_data, case_folder=case_folder)
        self.cluster_members_model.set_case_data(case_data, case_folder=case_folder)
        if not defer_load:
            self._populate_filters()
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
        # Clear checked state on evidence change
        self._checked_image_ids.clear()
        self._update_checked_count_label()

        if not defer_load:
            # Immediate loading (legacy behavior)
            self.model.set_evidence(evidence_id)
            self.cluster_model.set_evidence(evidence_id)
            self.table_model.set_evidence(evidence_id)
            # Set evidence for cluster members model (no reload since it's populated on click)
            self.cluster_members_model.evidence_id = evidence_id
            self._populate_filters()
            self._load_tags()
            self._update_page_label()
        else:
            # Deferred loading - just store the ID, load on showEvent
            self._load_pending = True

    def refresh(self) -> None:
        self.model.reload()
        self._update_page_label()

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

        # Set evidence IDs on models WITHOUT triggering immediate reload.
        # The actual data load will happen in _on_filters_loaded() after
        # the background filter query completes.
        if self.evidence_id is not None:
            self.model.set_evidence(self.evidence_id, reload=False)
            self.cluster_model.set_evidence(self.evidence_id, reload=False)
            self.table_model.set_evidence(self.evidence_id, reload=False)
            # Set evidence for cluster members model (no reload since it's populated on click)
            self.cluster_members_model.evidence_id = self.evidence_id
            self._load_tags()

        # Start background filter loading - this shows loading indicators
        # and triggers _on_filters_loaded() when complete
        self._populate_filters()
        self._update_page_label()

    def current_filters(self) -> Dict[str, str]:
        return {
            "tags": self.filters.tags,
            "source": self.filters.source,
        }

    def selected_image_ids(self) -> List[int]:
        ids: List[int] = []
        for index in self.list_view.selectedIndexes():
            row = self.model.get_row(index)
            if row and row.get("id") is not None:
                ids.append(int(row["id"]))
        return ids

    def set_hash_db_path(self, path: Optional[Path]) -> None:
        """Store hash database path for Check Known Hashes feature."""
        self.hash_db_path = path

    def set_thumbnail_size(self, size: int) -> None:
        self.list_view.setIconSize(QSize(size, size))
        self.model.set_thumbnail_size(size)

    # Helpers ------------------------------------------------------------

    def _populate_filters(self) -> None:
        """
        Populate filter dropdowns with loading placeholders.

        Uses background worker to prevent UI freeze on large datasets.
        """
        if not self.case_data or self.evidence_id is None:
            self.source_combo.blockSignals(True)
            self.source_combo.clear()
            self.source_combo.addItem("All sources", userData="")
            self.source_combo.blockSignals(False)

            self.extension_combo.blockSignals(True)
            self.extension_combo.clear()
            self.extension_combo.addItem("All extensions", userData="")
            self.extension_combo.blockSignals(False)

            self.hash_match_combo.blockSignals(True)
            self.hash_match_combo.clear()
            self.hash_match_combo.addItem("All images", userData="")
            self.hash_match_combo.addItem("Any match", userData="__any__")
            self.hash_match_combo.blockSignals(False)

            self._on_filters_changed()
            return

        # Show loading indicators in dropdowns
        self.source_combo.blockSignals(True)
        self.source_combo.clear()
        self.source_combo.addItem("Loading sources...", userData="__loading__")
        self.source_combo.setEnabled(False)
        self.source_combo.blockSignals(False)

        self.extension_combo.blockSignals(True)
        self.extension_combo.clear()
        self.extension_combo.addItem("Loading extensions...", userData="__loading__")
        self.extension_combo.setEnabled(False)
        self.extension_combo.blockSignals(False)

        self.hash_match_combo.blockSignals(True)
        self.hash_match_combo.clear()
        self.hash_match_combo.addItem("Loading...", userData="__loading__")
        self.hash_match_combo.setEnabled(False)
        self.hash_match_combo.blockSignals(False)

        # Start background worker
        self._filter_worker = ImageFilterLoadWorker(self.case_data, int(self.evidence_id))
        self._filter_worker.finished.connect(self._on_filters_loaded)
        self._filter_worker.start()

    def _on_filters_loaded(self, data: dict) -> None:
        """
        Handle filter data loaded from background worker.

        Args:
            data: Dict with sources, extensions, hash_matches, tags, total_images
        """
        # Update total image count for pagination
        self._total_image_count = data.get("total_images", 0)

        # Restore current selections
        current_source = self.source_combo.currentData()
        current_ext = self.extension_combo.currentData()
        current_hash_match = self.hash_match_combo.currentData()

        # Populate sources
        sources_with_counts = data.get("sources", [])
        self.source_combo.blockSignals(True)
        self.source_combo.clear()
        self.source_combo.addItem("All sources", userData="")

        # Add "Browser Sources" option if any browser sources exist
        browser_source_count = sum(
            count or 0 for source, count in sources_with_counts
            if source in BROWSER_IMAGE_SOURCES
        )
        if browser_source_count > 0:
            self.source_combo.addItem(f"🌐 Browser Sources ({browser_source_count})", userData="__browser__")

        for source, count in sources_with_counts:
            label = self._format_source_label(source, count)
            self.source_combo.addItem(label, userData=source)
        self.source_combo.setEnabled(True)
        self.source_combo.blockSignals(False)
        if current_source and current_source != "__loading__":
            index = self.source_combo.findData(current_source)
            if index != -1:
                self.source_combo.setCurrentIndex(index)

        # Populate extensions
        extensions_with_counts = data.get("extensions", [])
        truncated = data.get("extensions_truncated", False)
        extension_count = data.get("extension_count", 0)

        self.extension_combo.blockSignals(True)
        self.extension_combo.clear()
        if truncated:
            self.extension_combo.addItem(
                f"All extensions (showing {len(extensions_with_counts)} of {extension_count})",
                userData=""
            )
        else:
            self.extension_combo.addItem("All extensions", userData="")
        for ext, count in extensions_with_counts:
            if ext:  # Skip empty extensions
                label = f"{ext} ({count})" if count else ext
                self.extension_combo.addItem(label, userData=ext)
        self.extension_combo.setEnabled(True)
        self.extension_combo.blockSignals(False)
        if current_ext and current_ext != "__loading__":
            index = self.extension_combo.findData(current_ext)
            if index != -1:
                self.extension_combo.setCurrentIndex(index)

        # Populate hash match lists
        hash_match_counts = data.get("hash_matches", [])
        self.hash_match_combo.blockSignals(True)
        self.hash_match_combo.clear()
        self.hash_match_combo.addItem("All images", userData="")
        # Add "Any match" option with total count
        total_matched = sum(count for _, count in hash_match_counts)
        if total_matched > 0:
            self.hash_match_combo.addItem(f"Any match ({total_matched})", userData="__any__")
        else:
            self.hash_match_combo.addItem("Any match", userData="__any__")
        # Add individual hash lists
        for list_name, count in hash_match_counts:
            if list_name:
                label = f"🔒 {list_name} ({count})"
                self.hash_match_combo.addItem(label, userData=list_name)
        self.hash_match_combo.setEnabled(True)
        self.hash_match_combo.blockSignals(False)
        if current_hash_match and current_hash_match != "__loading__":
            index = self.hash_match_combo.findData(current_hash_match)
            if index != -1:
                self.hash_match_combo.setCurrentIndex(index)

        # Populate tags from worker data
        tags = data.get("tags", [])
        current_tag = self.tag_combo.currentData()
        self.tag_combo.blockSignals(True)
        self.tag_combo.clear()
        self.tag_combo.addItem("All tags", userData="")
        for tag in tags:
            name = tag["name"]
            count = tag["usage_count"]
            self.tag_combo.addItem(f"{name} ({count})", userData=name)
        self.tag_combo.blockSignals(False)
        if current_tag:
            index = self.tag_combo.findData(current_tag)
            if index != -1:
                self.tag_combo.setCurrentIndex(index)

        self._on_filters_changed()
        self._update_source_counts_label(sources_with_counts)

    def _update_source_counts_label(self, sources_with_counts: List[Tuple[str, Optional[int]]]) -> None:
        """Update tooltip with provenance counts for quick glance."""
        parts = []
        for source, count in sources_with_counts:
            label = self._format_source_label(source, count)
            parts.append(label)
        tooltip = "<br/>".join(parts) if parts else "No sources found"
        self.source_label.setToolTip(tooltip)

    def _on_url_filter_text_changed(self, text: str) -> None:
        """Debounce URL filter text changes."""
        self._url_filter_timer.start()

    def _on_filters_changed(self) -> None:
        # Process pending events to prevent UI lockup when changing filters quickly
        QApplication.processEvents()

        self.filters.tags = self.tag_combo.currentData() or ""
        self.filters.source = self.source_combo.currentData() or ""
        self.filters.extension = self.extension_combo.currentData() or ""
        self.filters.hash_match = self.hash_match_combo.currentData() or ""
        self.filters.url_text = self.url_filter_input.text().strip()

        # Phase 3: Size filter
        size_data = self.size_combo.currentData()
        if size_data:
            self.filters.min_size_bytes, self.filters.max_size_bytes = size_data
        else:
            self.filters.min_size_bytes = None
            self.filters.max_size_bytes = None

        # Handle __browser__ filter as tuple of all browser sources
        if self.filters.source == "__browser__":
            sources = tuple(BROWSER_IMAGE_SOURCES)
        elif self.filters.source:
            sources = (self.filters.source,)
        else:
            sources = None

        # Update all models with new filters
        filter_kwargs = dict(
            tags=self.filters.tags,
            sources=sources,
            extension=self.filters.extension,
            hash_match=self.filters.hash_match,
            url_text=self.filters.url_text,
            min_size_bytes=self.filters.min_size_bytes,
            max_size_bytes=self.filters.max_size_bytes,
        )
        self.model.set_filters(**filter_kwargs)
        self.table_model.set_filters(**filter_kwargs)
        self._update_page_label()

    def _reset_filters(self) -> None:
        """Reset all filters to defaults and reload images."""
        # Block signals to avoid multiple reloads
        self.tag_combo.blockSignals(True)
        self.source_combo.blockSignals(True)
        self.extension_combo.blockSignals(True)
        self.size_combo.blockSignals(True)
        self.hash_match_combo.blockSignals(True)

        try:
            # Reset all combo boxes to first item (default)
            self.tag_combo.setCurrentIndex(0)
            self.source_combo.setCurrentIndex(0)
            self.extension_combo.setCurrentIndex(0)
            self.size_combo.setCurrentIndex(0)
            self.hash_match_combo.setCurrentIndex(0)

            # Reset filter state
            self.filters.tags = ""
            self.filters.source = ""
            self.filters.extension = ""
            self.filters.hash_match = ""
            self.filters.url_text = ""
            self.filters.min_size_bytes = None
            self.filters.max_size_bytes = None

            # Clear URL filter text
            self.url_filter_input.clear()

            # Clear thumbnail cache to force reload
            self.model._thumb_cache.clear()

            # Reset to page 0 and reload all models
            # Use empty string for filters to ensure they are cleared (None skips update)
            filter_kwargs = dict(
                tags="",
                sources=(),  # Empty tuple to clear sources filter
                extension="",
                hash_match="",
                url_text="",
                min_size_bytes=None,
                max_size_bytes=None,
            )
            self.model.page = 0
            self.model.set_filters(**filter_kwargs)
            self.table_model.page = 0
            self.table_model.set_filters(**filter_kwargs)

            # Update total image count for accurate pagination
            if self.case_data and self.evidence_id is not None:
                sources = self.case_data.list_image_sources_counts(int(self.evidence_id), use_cache=False)
                self._total_image_count = sum(count or 0 for _, count in sources)
        finally:
            # Restore signals
            self.tag_combo.blockSignals(False)
            self.source_combo.blockSignals(False)
            self.extension_combo.blockSignals(False)
            self.size_combo.blockSignals(False)
            self.hash_match_combo.blockSignals(False)

        self._update_page_label()

    def _format_source_label(self, source: str, count: Optional[int] = None) -> str:
        """Return a human-friendly source label with provenance icon and optional count."""
        if not source:
            return "Unknown"
        # Added browser cache/storage source icons
        icon = {
            # Carving tools
            "foremost_carver": "🔨",
            "scalpel": "✂️",
            "bulk_extractor:images": "🔍",
            "bulk_extractor_images": "🔍",
            "image_carving": "🖼️",
            # Filesystem
            "filesystem_images": "📁",
            # Browser cache/storage
            "cache_simple": "🌐",
            "cache_blockfile": "🌐",
            "cache_firefox": "🦊",
            "browser_storage_indexeddb": "💾",
            "safari": "🧭",
        }.get(source, "📁")
        if count is not None:
            return f"{icon} {source} ({count})"
        return f"{icon} {source}"

    def _summarize_exif(self, exif_json: Optional[str]) -> List[str]:
        """Return a concise EXIF summary."""
        if not exif_json:
            return []
        try:
            import json

            exif = json.loads(exif_json)
        except Exception:
            return []
        keys = [
            ("Make", exif.get("Make")),
            ("Model", exif.get("Model")),
            ("DateTime", exif.get("DateTimeOriginal") or exif.get("DateTime")),
            ("Dimensions", f"{exif.get('ImageWidth')}x{exif.get('ImageLength')}" if exif.get("ImageWidth") and exif.get("ImageLength") else None),
        ]
        return [f"{k}: {v}" for k, v in keys if v]

    def _on_tab_changed(self, index: int) -> None:
        """Handle tab changes between Grid, Clusters, and Table views."""
        self._view_mode = ["grid", "clusters", "table"][index]

        # Guard against early signal before buttons are created
        if not hasattr(self, 'prev_button'):
            return

        # Update pagination visibility based on tab
        if self._view_mode == "clusters":
            # Clusters show all - disable pagination
            self.prev_button.setEnabled(False)
            self.next_button.setEnabled(False)
            # Load clusters asynchronously to avoid UI freeze
            self._load_clusters_async()
        else:
            # Grid and Table support pagination
            self.prev_button.setEnabled(True)
            self.next_button.setEnabled(True)

        # Update export buttons visibility
        if hasattr(self, 'export_button'):
            self.export_button.setVisible(self._view_mode != "clusters")
        if hasattr(self, 'export_clusters_button'):
            self.export_clusters_button.setVisible(self._view_mode == "clusters")

        self._update_page_label()

    def _load_clusters_async(self) -> None:
        """Load clusters in background thread to avoid UI freeze."""
        if not self.case_data or self.evidence_id is None:
            return

        # Cancel any running cluster worker
        if self._cluster_worker and self._cluster_worker.isRunning():
            self._cluster_worker.quit()
            self._cluster_worker.wait(1000)

        # Show loading state
        self.cluster_members_label.setText("Loading clusters...")

        # Start background worker
        self._cluster_worker = ClusterLoadWorker(
            self.case_data,
            self.evidence_id,
            threshold=self.cluster_model.threshold
        )
        self._cluster_worker.finished.connect(self._on_clusters_loaded)
        self._cluster_worker.error.connect(self._on_clusters_error)
        self._cluster_worker.start()

    def _on_clusters_loaded(self, clusters: list) -> None:
        """Handle cluster loading completion."""
        self.cluster_model.beginResetModel()
        self.cluster_model._clusters = clusters
        self.cluster_model._thumb_cache.clear()
        self.cluster_model.endResetModel()

        # Update label
        count = len(clusters)
        self.cluster_members_label.setText(f"Click a cluster above to see all similar images ({count} clusters)")
        self._update_page_label()

    def _on_clusters_error(self, error: str) -> None:
        """Handle cluster loading error."""
        self.cluster_members_label.setText(f"Error loading clusters: {error}")

    def _on_cluster_clicked(self, index: QModelIndex) -> None:
        """Handle cluster click to show members in detail view."""
        cluster = self.cluster_model.data(index, Qt.UserRole)
        if not cluster:
            return

        # Get all member image IDs
        members = cluster.get("members", [])
        representative = cluster.get("representative", {})
        all_members = [representative] + members if representative else members

        # Update the members label
        count = len(all_members)
        cluster_id = cluster.get("cluster_id", "?")
        self.cluster_members_label.setText(f"Cluster {cluster_id} — {count} similar images:")

        # Load member images into the members model
        # Need to clear thumb cache and set rows properly for thumbnail loading
        self.cluster_members_model._thumb_cache.clear()
        self.cluster_members_model.beginResetModel()
        self.cluster_members_model._rows = all_members
        self.cluster_members_model.endResetModel()

    def _on_cluster_double_clicked(self, index: QModelIndex) -> None:
        """Handle cluster double-click to open representative image preview."""
        cluster = self.cluster_model.data(index, Qt.UserRole)
        if not cluster:
            return
        representative = cluster.get("representative", {})
        if representative:
            self._show_image_preview(representative)

    def _on_cluster_member_double_clicked(self, index: QModelIndex) -> None:
        """Handle cluster member double-click to open image preview."""
        row = self.cluster_members_model.get_row(index)
        if row:
            self._show_image_preview(row)

    def _on_table_double_clicked(self, index: QModelIndex) -> None:
        """Handle table double-click to open image preview."""
        row = self.table_model.get_row(index)
        if row:
            self._show_image_preview(row)

    def _show_table_context_menu(self, pos: QPoint) -> None:
        """Show context menu for table view."""
        index = self.table_view.indexAt(pos)
        if not index.isValid():
            return

        row = self.table_model.get_row(index)
        if not row:
            return

        menu = QMenu(self)
        preview_action = menu.addAction("Preview Image")
        copy_hash_action = menu.addAction("Copy MD5")
        copy_path_action = menu.addAction("Copy Original Path")
        menu.addSeparator()
        tag_action = menu.addAction("Tag Image")

        action = menu.exec_(self.table_view.viewport().mapToGlobal(pos))

        if action == preview_action:
            self._show_image_preview(row)
        elif action == copy_hash_action:
            md5 = row.get("md5", "")
            if md5:
                QApplication.clipboard().setText(md5)
        elif action == copy_path_action:
            fs_path = row.get("fs_path", "")
            if fs_path:
                QApplication.clipboard().setText(fs_path)
        elif action == tag_action:
            image_id = row.get("id")
            if image_id:
                self._tag_images([image_id])

    def _page_up(self) -> None:
        if self._view_mode == "grid":
            self.model.page_up()
        elif self._view_mode == "table":
            self.table_model.page_up()
        self._update_page_label()
        self._scroll_to_top()

    def _page_down(self) -> None:
        if self._view_mode == "grid":
            self.model.page_down()
        elif self._view_mode == "table":
            self.table_model.page_down()
        self._update_page_label()
        self._scroll_to_top()

    def _scroll_to_top(self) -> None:
        """Scroll the view to the top after pagination."""
        if self._view_mode == "grid":
            self.list_view.scrollToTop()
        elif self._view_mode == "table":
            self.table_view.scrollToTop()
        else:
            self.cluster_view.scrollToTop()

    def _on_page_size_changed(self, index: int) -> None:
        """Handle page size dropdown change."""
        size = self.page_size_combo.currentData()
        if size:
            self.model.page_size = size
            self.model.page = 0
            self.table_model.page_size = size
            self.table_model.page = 0
            # Reload active model
            if self._view_mode == "grid":
                self.model.reload()
            elif self._view_mode == "table":
                self.table_model.reload()
            self._update_page_label()
            self._scroll_to_top()

    def _goto_page(self) -> None:
        """Handle go-to-page input."""
        text = self.goto_page_input.text().strip()
        if not text:
            return
        try:
            page_num = int(text)
            if page_num < 1:
                page_num = 1
            total_pages = self._get_total_pages()
            if total_pages > 0 and page_num > total_pages:
                page_num = total_pages
            # Update active model
            if self._view_mode == "grid":
                self.model.page = page_num - 1
                self.model.reload()
            elif self._view_mode == "table":
                self.table_model.page = page_num - 1
                self.table_model.reload()
            self._update_page_label()
            self._scroll_to_top()
        except ValueError:
            pass
        finally:
            self.goto_page_input.clear()

    def _get_total_pages(self) -> int:
        """Calculate total number of pages."""
        if self._total_image_count == 0:
            return 1
        page_size = self.model.page_size if self._view_mode == "grid" else self.table_model.page_size
        return (self._total_image_count + page_size - 1) // page_size

    def _update_page_label(self) -> None:
        """Update page label with current page and total."""
        if self._view_mode == "grid":
            current = self.model.current_page() + 1
        elif self._view_mode == "table":
            current = self.table_model.page + 1
        else:
            # Clusters mode - no pagination
            self.page_label.setText(f"{self._total_image_count} images in clusters")
            return

        total_pages = self._get_total_pages()
        if self._total_image_count > 0:
            self.page_label.setText(
                f"Page {current} of {total_pages} ({self._total_image_count} images)"
            )
        else:
            self.page_label.setText(f"Page {current}")

    def _on_selection_changed(self, current, previous) -> None:  # noqa: ANN001
        row = self.model.get_row(current)
        if not row:
            self._current_phash = None
            self.similar_button.setEnabled(False)
            return

        phash = row.get("phash")
        self._current_phash = phash if phash else None
        self.similar_button.setEnabled(phash is not None)

    def _export_selected(self) -> None:
        indexes = self.list_view.selectedIndexes()
        if not indexes:
            QMessageBox.information(
                self,
                "No selection",
                "Select an image to export.",
            )
            return
        if not self.case_data:
            QMessageBox.warning(
                self,
                "Unavailable",
                "Case data not available.",
            )
            return
        dest_dir = QFileDialog.getExistingDirectory(
            self,
            "Select Export Folder",
            str(Path.home()),
        )
        if not dest_dir:
            return
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        exported = 0
        for index in indexes:
            row = self.model.get_row(index)
            if not row:
                continue
            rel_path = row.get("rel_path")
            if not rel_path:
                continue
            # Support both aliased discovered_by and raw first_discovered_by
            discovered_by = row.get("discovered_by") or row.get("first_discovered_by")
            source_path = self.case_data.resolve_image_path(
                rel_path,
                evidence_id=self.evidence_id,
                discovered_by=discovered_by,
            )
            if not source_path.exists():
                continue
            target_path = dest / Path(rel_path).name
            target_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_path, target_path)
            exported += 1
        QMessageBox.information(
            self,
            "Export complete",
            f"Exported {exported} images to {dest}",
        )

    def _export_clusters(self) -> None:
        """Export clusters to CSV."""
        if self.evidence_id is None:
            QMessageBox.information(
                self,
                "No data",
                "Select an evidence item before exporting.",
            )
            return

        path_str, _ = QFileDialog.getSaveFileName(
            self,
            "Export Clusters",
            str(Path.home() / "image_clusters.csv"),
            "CSV Files (*.csv)",
        )
        if not path_str:
            return

        self.cluster_model.export_to_csv(Path(path_str))
        QMessageBox.information(
            self,
            "Export complete",
            f"Clusters exported to {path_str}",
        )

    # ---------------------------------------------------------------
    # Check Known Hashes (bulk hash list matching)
    # ---------------------------------------------------------------

    def _check_known_hashes(self) -> None:
        """Open dialog to select hash lists and check all images against them."""
        if not self.case_data or self.evidence_id is None:
            QMessageBox.warning(
                self,
                "Check Known Hashes",
                "Please select an evidence first."
            )
            return

        # Get available hash lists
        ref_manager = ReferenceListManager()
        available = ref_manager.list_available()
        available_hashlists = available.get("hashlists", [])

        if not available_hashlists:
            QMessageBox.information(
                self,
                "No Hash Lists",
                "No hash lists found.\n\n"
                "Add hash lists in Settings → Preferences → Hash Lists tab."
            )
            return

        # Show selection dialog
        dialog = HashListSelectorDialog(available_hashlists, self)
        if dialog.exec() != QDialog.Accepted:
            return

        selected_lists = dialog.get_selected_lists()
        if not selected_lists:
            QMessageBox.information(
                self,
                "No Selection",
                "Please select at least one hash list to check against."
            )
            return

        # Start worker
        self._start_hash_check(selected_lists)

    def _start_hash_check(self, selected_hashlists: List[str]) -> None:
        """Start the hash check worker thread."""
        # Create progress dialog
        self._hash_check_progress = QProgressDialog(
            "Checking images against hash lists...",
            "Cancel",
            0, 100, self
        )
        self._hash_check_progress.setWindowModality(Qt.WindowModal)
        self._hash_check_progress.setAutoClose(False)
        self._hash_check_progress.setMinimumDuration(0)

        # Create and start worker
        self._hash_check_worker = HashCheckWorker(
            self.case_data.db_manager,
            int(self.evidence_id),
            selected_hashlists
        )

        self._hash_check_worker.progress.connect(self._on_hash_check_progress)
        self._hash_check_worker.finished.connect(self._on_hash_check_finished)
        self._hash_check_worker.error.connect(self._on_hash_check_error)
        self._hash_check_progress.canceled.connect(self._cancel_hash_check_worker)

        self._hash_check_worker.start()
        self._hash_check_progress.show()

    def _cancel_hash_check_worker(self) -> None:
        """Gracefully cancel hash check worker when user clicks Cancel."""
        if hasattr(self, '_hash_check_worker') and self._hash_check_worker is not None:
            if self._hash_check_worker.isRunning():
                self._hash_check_worker.requestInterruption()
                self._hash_check_worker.quit()
                if not self._hash_check_worker.wait(1000):
                    import logging
                    logging.getLogger(__name__).warning("HashCheckWorker did not stop on cancel, terminating")
                    self._hash_check_worker.terminate()
                    self._hash_check_worker.wait(500)
            self._hash_check_worker = None

    def _on_hash_check_progress(self, current: int, total: int) -> None:
        """Update hash check progress dialog."""
        if total > 0:
            progress = int((current / total) * 100)
            self._hash_check_progress.setValue(progress)
            self._hash_check_progress.setLabelText(
                f"Checking images... {progress}%"
            )

    def _on_hash_check_finished(self, results: dict) -> None:
        """Handle hash check completion."""
        self._hash_check_progress.close()

        total_matches = sum(
            v for v in results.values() if isinstance(v, int)
        )

        message = f"Hash check completed!\n\nTotal matches found: {total_matches}\n\n"

        # Show results for each list
        for list_name in sorted(results.keys()):
            count = results[list_name]
            if isinstance(count, int):
                message += f"• {list_name}: {count} matches\n"
            else:
                message += f"• {list_name}: {count}\n"

        QMessageBox.information(self, "Hash Check Complete", message)

        # Repopulate filter dropdowns (including hash match options) and refresh data
        # _populate_filters starts a background worker that calls _on_filters_loaded,
        # which preserves current filter selections and triggers model reload.
        self._populate_filters()
        self.hashLookupFinished.emit()

    def _on_hash_check_error(self, error_msg: str) -> None:
        """Handle hash check error."""
        self._hash_check_progress.close()
        QMessageBox.critical(
            self,
            "Hash Check Error",
            f"Hash check failed: {error_msg}"
        )

    def _find_similar(self) -> None:
        """Find images similar to the currently selected image using perceptual hash."""
        if not self._current_phash:
            QMessageBox.information(
                self,
                "Find Similar",
                "The selected image has no perceptual hash.",
            )
            return

        if not self.case_data or self.evidence_id is None:
            QMessageBox.warning(
                self,
                "Find Similar",
                "Case data not available.",
            )
            return

        # Default threshold: 10 bits (similar images)
        threshold = 10
        similar_images = self.case_data.find_similar_images(
            self.evidence_id,
            self._current_phash,
            threshold=threshold
        )

        if not similar_images:
            QMessageBox.information(
                self,
                "Find Similar",
                f"No similar images found (threshold: {threshold} bits).",
            )
            return

        # Build result message
        lines = [
            f"Found {len(similar_images)} similar image(s) (threshold: {threshold} bits):",
            ""
        ]

        for img in similar_images:
            distance = img.get("hamming_distance", "?")
            filename = img.get("filename", "Unknown")
            similarity = self._describe_similarity(distance)
            lines.append(
                f"• {filename} (distance: {distance} bits - {similarity})"
            )

        QMessageBox.information(
            self,
            "Similar Images",
            "\n".join(lines)
        )

    def _describe_similarity(self, distance: int) -> str:
        """Describe the similarity level based on Hamming distance."""
        if distance == 0:
            return "identical"
        elif distance <= 5:
            return "very similar"
        elif distance <= 10:
            return "similar"
        else:
            return "different"

    def _tag_images(self, image_ids: List[int]) -> None:
        """Open dialog to tag specific images by ID."""
        if not self.case_data or self.evidence_id is None:
            return
        if not image_ids:
            return

        dialog = TagArtifactsDialog(self.case_data, int(self.evidence_id), "image", image_ids, self)
        dialog.tags_changed.connect(self._load_tags)
        if dialog.exec():
            self.refresh()

    def _show_context_menu(self, point: QPoint) -> None:
        if not self.case_data:
            return
        index = self.list_view.indexAt(point)
        if not index.isValid():
            return
        row = self.model.get_row(index)
        if not row:
            return
        menu = QMenu(self)

        preview_action = menu.addAction("Preview Image")
        tag_action = menu.addAction("Tag Image")
        menu.addSeparator()
        check_action = menu.addAction("Check Image")
        reveal_action = menu.addAction("Reveal in case folder")

        chosen = menu.exec(self.list_view.mapToGlobal(point))
        if chosen == reveal_action:
            self._reveal_in_case_folder(row)
        elif chosen == tag_action:
            image_id = row.get("id")
            if image_id:
                self._tag_images([image_id])
        elif chosen == preview_action:
            self._show_image_preview(row)
        elif chosen == check_action:
            image_id = row.get("id")
            if image_id is not None:
                self._checked_image_ids.add(image_id)
                self._update_checked_count_label()
                self.model.layoutChanged.emit()

    def _reveal_in_case_folder(self, row: Dict[str, Any]) -> None:
        if not self.case_data:
            return
        rel_path = row.get("rel_path")
        if not rel_path:
            return
        target = self.case_data.resolve_image_path(
            rel_path,
            evidence_id=self.evidence_id,
            discovered_by=row.get("discovered_by"),
        )
        if not target.exists():
            QMessageBox.warning(
                self,
                "Reveal in case folder",
                "Unable to locate image on disk.",
            )
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target.parent)))

    def _load_tags(self) -> None:
        """Load tags into the filter combo box."""
        if not self.case_data or self.evidence_id is None:
            return

        current_tag = self.tag_combo.currentData()
        self.tag_combo.blockSignals(True)
        self.tag_combo.clear()
        self.tag_combo.addItem("All tags", userData="")

        tags = self.case_data.list_tags(self.evidence_id)
        for tag in tags:
            name = tag["name"]
            count = tag["usage_count"]
            self.tag_combo.addItem(f"{name} ({count})", userData=name)

        # Restore selection if possible
        index = self.tag_combo.findData(current_tag)
        if index != -1:
            self.tag_combo.setCurrentIndex(index)

        self.tag_combo.blockSignals(False)

    # ---------------------------------------------------------------
    # Phase 1: Image Preview
    # ---------------------------------------------------------------

    def _on_image_double_clicked(self, index: QModelIndex) -> None:
        """Open image preview dialog on double-click."""
        row = self.model.get_row(index)
        if row:
            self._show_image_preview(row)

    def _show_image_preview(self, row: Dict[str, Any]) -> None:
        """
        Open image preview dialog for the given image row.

        Extracted from _on_image_double_clicked to support calls from
        table view, cluster view, and context menus.
        """
        if not row:
            return

        if not self.case_data:
            return

        # Get thumbnail path
        thumbnail_path = self._get_thumbnail_path(row)

        # Get full image path
        rel_path = row.get("rel_path")
        full_image_path = None
        if rel_path:
            full_image_path = self.case_data.resolve_image_path(
                rel_path,
                evidence_id=self.evidence_id,
                discovered_by=row.get("discovered_by"),
            )

        # Fetch discovery records for multi-source provenance
        discoveries = []
        hash_matches = []
        image_id = row.get("id")
        if image_id and self.evidence_id and self.case_data and self.case_data.db_manager:
            try:
                from core.database import get_image_discoveries, get_hash_matches
                # Get evidence label for connection lookup
                evidence = self.case_data.get_evidence(self.evidence_id)
                label = evidence.get("label") if evidence else None
                conn = self.case_data.db_manager.get_evidence_conn(self.evidence_id, label)
                if conn:
                    discoveries = get_image_discoveries(conn, self.evidence_id, image_id)
                    hash_matches = get_hash_matches(
                        conn, int(self.evidence_id), image_id=int(image_id)
                    )
            except Exception as e:
                # Gracefully handle missing table or connection issues
                LOGGER.debug("Could not fetch image discoveries/hash matches: %s", e)

        # Fetch tags on-demand (removed from iter_images)
        if image_id and self.evidence_id and self.case_data:
            try:
                tags_str = self.case_data.get_artifact_tags_str(
                    int(self.evidence_id), "image", int(image_id)
                )
                row = dict(row)  # Make a copy to avoid modifying cached data
                row["tags"] = tags_str
            except Exception as e:
                LOGGER.debug("Could not fetch image tags: %s", e)

        # Open preview dialog
        dialog = ImagePreviewDialog(
            image_data=row,
            thumbnail_path=thumbnail_path,
            full_image_path=full_image_path,
            parent=self,
            discoveries=discoveries,
            hash_matches=hash_matches,
        )
        dialog.exec()

    def _get_thumbnail_path(self, row: Dict[str, Any]) -> Optional[Path]:
        """
        Get cached thumbnail path for an image.

        Validates thumbnail file has actual content (not empty placeholder).
        """
        if not self.case_data:
            return None

        cache_base = self.case_folder or self.case_data.case_folder
        if cache_base is None:
            return None

        rel_path = row.get("rel_path")
        if not rel_path:
            return None

        # Match thumbnailer's key generation
        image_path = self.case_data.resolve_image_path(
            rel_path,
            evidence_id=self.evidence_id,
            discovered_by=row.get("discovered_by"),
        )
        key = hashlib.md5(str(image_path).encode("utf-8")).hexdigest()
        thumb_path = cache_base / ".thumbs" / f"{key}.jpg"

        # Validate thumbnail has actual content (not empty placeholder)
        if thumb_path.exists():
            try:
                if thumb_path.stat().st_size >= 100:  # Minimum valid JPEG size
                    return thumb_path
            except OSError:
                pass
        return None

    # ---------------------------------------------------------------
    # Phase 2: Checkbox Tagging
    # ---------------------------------------------------------------

    def _update_checked_count_label(self) -> None:
        """Update the label showing checked image count."""
        count = len(self._checked_image_ids)
        if count == 0:
            self.checked_count_label.setText("")
            self.clear_checks_button.setEnabled(False)
            if hasattr(self, 'tag_checked_button'):
                self.tag_checked_button.setEnabled(False)
        else:
            self.checked_count_label.setText(
                f"{count} image(s) checked"
            )
            self.clear_checks_button.setEnabled(True)
            if hasattr(self, 'tag_checked_button'):
                self.tag_checked_button.setEnabled(True)

    def _check_visible(self) -> None:
        """Check all currently visible images."""
        rows = self._get_visible_rows()
        for row in rows:
            image_id = row.get("id")
            if image_id is not None:
                self._checked_image_ids.add(image_id)
        self._update_checked_count_label()
        self._emit_layout_changed_all()

    def _uncheck_visible(self) -> None:
        """Uncheck all currently visible images."""
        rows = self._get_visible_rows()
        for row in rows:
            image_id = row.get("id")
            if image_id is not None:
                self._checked_image_ids.discard(image_id)
        self._update_checked_count_label()
        self._emit_layout_changed_all()

    def _get_visible_rows(self) -> List[Dict[str, Any]]:
        """Return list of row dicts for currently visible items in the active view."""
        if self._view_mode == "grid":
            return list(self.model._rows)
        elif self._view_mode == "table":
            return list(self.table_model._rows)
        elif self._view_mode == "clusters":
            # Include cluster members model rows if populated, else cluster representative rows
            rows: List[Dict[str, Any]] = []
            if self.cluster_members_model._rows:
                rows.extend(self.cluster_members_model._rows)
            else:
                for cluster in self.cluster_model._clusters:
                    rep = cluster.get("representative")
                    if rep:
                        rows.append(rep)
                    for member in cluster.get("members", []):
                        rows.append(member)
            return rows
        return []

    def _emit_layout_changed_all(self) -> None:
        """Emit layoutChanged on all models to refresh checkbox display."""
        self.model.layoutChanged.emit()
        self.table_model.layoutChanged.emit()
        self.cluster_model.layoutChanged.emit()
        self.cluster_members_model.layoutChanged.emit()

    def _clear_checked(self) -> None:
        """Clear all checked images."""
        self._checked_image_ids.clear()
        self._update_checked_count_label()
        # Refresh models to update checkbox display
        self._emit_layout_changed_all()

    def _tag_checked(self) -> None:
        """Open dialog to tag all checked images."""
        if not self.case_data or self.evidence_id is None:
            return

        if not self._checked_image_ids:
            QMessageBox.information(
                self,
                "No Checked Images",
                "Please check at least one image to tag."
            )
            return

        image_ids = list(self._checked_image_ids)

        dialog = TagArtifactsDialog(
            self.case_data, int(self.evidence_id), "image", image_ids, self
        )
        dialog.tags_changed.connect(self._load_tags)
        if dialog.exec():
            # Optionally clear checks after successful tagging
            # self._clear_checked()
            self.refresh()

    def checked_image_ids(self) -> List[int]:
        """Return list of all checked image IDs."""
        return list(self._checked_image_ids)

    def on_image_check_changed(self, image_id: int, checked: bool) -> None:
        """Called by model when an image's checkbox state changes."""
        if checked:
            self._checked_image_ids.add(image_id)
        else:
            self._checked_image_ids.discard(image_id)
        self._update_checked_count_label()

    def shutdown(self) -> None:
        """
        Gracefully stop all background workers before widget destruction.

        Called by MainWindow.closeEvent() and _on_close_evidence_tab() to prevent
        Qt abort from destroying QThread while still running.
        """
        import logging
        logger = logging.getLogger(__name__)

        # Stop filter worker
        if self._filter_worker is not None:
            try:
                self._filter_worker.finished.disconnect()
            except (RuntimeError, TypeError):
                pass
            if self._filter_worker.isRunning():
                self._filter_worker.requestInterruption()
                self._filter_worker.quit()
                if not self._filter_worker.wait(2000):
                    logger.warning("ImageFilterLoadWorker did not stop in 2s, terminating")
                    self._filter_worker.terminate()
                    self._filter_worker.wait(500)
            self._filter_worker = None

        # Stop hash check worker
        if hasattr(self, '_hash_check_worker') and self._hash_check_worker is not None:
            try:
                self._hash_check_worker.progress.disconnect()
                self._hash_check_worker.finished.disconnect()
                self._hash_check_worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            if self._hash_check_worker.isRunning():
                self._hash_check_worker.requestInterruption()
                self._hash_check_worker.quit()
                if not self._hash_check_worker.wait(2000):
                    logger.warning("HashCheckWorker did not stop in 2s, terminating")
                    self._hash_check_worker.terminate()
                    self._hash_check_worker.wait(500)
            self._hash_check_worker = None

        # Close hash check progress dialog if open
        if hasattr(self, '_hash_check_progress') and self._hash_check_progress is not None:
            try:
                self._hash_check_progress.close()
            except RuntimeError:
                pass
            self._hash_check_progress = None

        logger.debug("ImagesTab shutdown complete")
