"""Firefox cache index subtab widget with filtering and pagination."""
from __future__ import annotations

import logging

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMenu,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from app.common import add_sandbox_url_actions
from app.features.browser_inventory._base import SubtabContext
from .index_model import CacheIndexTableModel
from .index_dialog import CacheIndexDetailsDialog

logger = logging.getLogger(__name__)

# Content type enum → display name (matches Firefox cache2 spec)
CONTENT_TYPE_MAP = {
    0: "Unknown",
    1: "Other",
    2: "JavaScript",
    3: "Image",
    4: "Media",
    5: "CSS",
    6: "WASM",
}


class CacheIndexSubtab(QWidget):
    """Firefox cache index entries with filtering and pagination."""

    def __init__(self, ctx: SubtabContext, parent=None):
        super().__init__(parent)
        self.ctx = ctx
        self._loaded = False
        self._model = None
        self._setup_ui()

    @property
    def is_loaded(self):
        return self._loaded

    def load(self):
        try:
            if self._model is None:
                self._model = CacheIndexTableModel(
                    self.ctx.db_manager,
                    self.ctx.evidence_id,
                    self.ctx.get_evidence_label(),
                    parent=self,
                )
                self.table.setModel(self._model)

                self.table.setColumnWidth(0, 100)   # Hash
                self.table.setColumnWidth(1, 280)   # URL
                self.table.setColumnWidth(2, 100)   # Content Type
                self.table.setColumnWidth(3, 70)    # Frecency
                self.table.setColumnWidth(4, 70)    # Size
                self.table.setColumnWidth(5, 80)    # Source
                self.table.setColumnWidth(6, 60)    # Has File
                self.table.setColumnWidth(7, 65)    # Removed
                self.table.setColumnWidth(8, 75)    # Anonymous
                self.table.setColumnWidth(9, 60)    # Pinned

            self._model.load()
            self._update_status()
            self._loaded = True

        except Exception as e:
            logger.error("Failed to load cache index entries: %s", e, exc_info=True)
            self.status_label.setText(f"Error loading cache index: {e}")

    def mark_needs_reload(self):
        self._loaded = False

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # ── Filter controls ──────────────────────────────────────────
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel("Source:"))
        self.source_filter = QComboBox()
        self.source_filter.addItem("All", None)
        self.source_filter.addItem("Entries", "entries")
        self.source_filter.addItem("Doomed", "doomed")
        self.source_filter.addItem("Trash", "trash")
        self.source_filter.addItem("Journal", "journal")
        self.source_filter.setFixedWidth(100)
        filter_layout.addWidget(self.source_filter)

        filter_layout.addWidget(QLabel("Content Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All", None)
        for enum_val, name in CONTENT_TYPE_MAP.items():
            self.type_filter.addItem(name, enum_val)
        self.type_filter.setFixedWidth(110)
        filter_layout.addWidget(self.type_filter)

        self.removed_checkbox = QCheckBox("Removed only")
        filter_layout.addWidget(self.removed_checkbox)

        self.metadata_checkbox = QCheckBox("Metadata-only (no file)")
        filter_layout.addWidget(self.metadata_checkbox)

        apply_btn = QPushButton("Apply")
        apply_btn.clicked.connect(self._apply_filters)
        filter_layout.addWidget(apply_btn)

        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        # ── Table view ────────────────────────────────────────────────
        self.table = QTableView()
        self.table.setSelectionBehavior(QTableView.SelectRows)
        self.table.setSelectionMode(QTableView.ExtendedSelection)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_context_menu)
        self.table.doubleClicked.connect(self._view_details)

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(True)

        layout.addWidget(self.table)

        # ── Pagination and status ────────────────────────────────────
        pagination_layout = QHBoxLayout()

        self.prev_btn = QPushButton("◀ Prev")
        self.prev_btn.setFixedWidth(70)
        self.prev_btn.clicked.connect(self._prev_page)
        pagination_layout.addWidget(self.prev_btn)

        self.next_btn = QPushButton("Next ▶")
        self.next_btn.setFixedWidth(70)
        self.next_btn.clicked.connect(self._next_page)
        pagination_layout.addWidget(self.next_btn)

        pagination_layout.addWidget(QLabel("Per page:"))
        self.page_size_combo = QComboBox()
        self.page_size_combo.setFixedWidth(80)
        for size in [100, 200, 500, 1000, 2000]:
            self.page_size_combo.addItem(str(size), userData=size)
        self.page_size_combo.setCurrentIndex(2)  # Default 500
        self.page_size_combo.currentIndexChanged.connect(self._on_page_size_changed)
        pagination_layout.addWidget(self.page_size_combo)

        self.page_label = QLabel("Page 1 of 1")
        pagination_layout.addWidget(self.page_label)

        pagination_layout.addWidget(QLabel("Go to:"))
        self.goto_input = QLineEdit()
        self.goto_input.setFixedWidth(50)
        self.goto_input.setPlaceholderText("#")
        self.goto_input.returnPressed.connect(self._goto_page)
        pagination_layout.addWidget(self.goto_input)

        pagination_layout.addStretch()

        self.status_label = QLabel("0 index entries")
        pagination_layout.addWidget(self.status_label)

        layout.addLayout(pagination_layout)

    # ─── Filters ──────────────────────────────────────────────────────

    def _apply_filters(self):
        if self._model is None:
            return

        entry_source = self.source_filter.currentData()
        content_type = self.type_filter.currentData()
        removed_only = self.removed_checkbox.isChecked()

        # Metadata-only means has_entry_file=False
        has_entry_file = None
        if self.metadata_checkbox.isChecked():
            has_entry_file = False

        self._model.load(
            removed_only=removed_only,
            has_entry_file=has_entry_file,
            content_type=content_type,
            entry_source=entry_source,
        )
        self._update_status()

    # ─── Status ──────────────────────────────────────────────────────

    def _update_status(self):
        if self._model is None:
            return

        total_count = self._model.total_count()
        stats = self._model.get_stats()

        removed = stats.get("removed", 0)
        without_file = stats.get("without_file", 0)

        parts = [f"{total_count} index entries"]
        if removed > 0:
            parts.append(f"{removed} removed")
        if without_file > 0:
            parts.append(f"{without_file} metadata-only")
        self.status_label.setText(" | ".join(parts))

        current_page = self._model.current_page() + 1
        total_pages = self._model.total_pages()
        self.page_label.setText(f"Page {current_page} of {total_pages}")
        self.prev_btn.setEnabled(self._model.has_prev_page())
        self.next_btn.setEnabled(self._model.has_next_page())

    # ─── Pagination ──────────────────────────────────────────────────

    def _prev_page(self):
        if self._model is None:
            return
        self._model.prev_page()
        self._update_status()
        self.table.scrollToTop()

    def _next_page(self):
        if self._model is None:
            return
        self._model.next_page()
        self._update_status()
        self.table.scrollToTop()

    def _on_page_size_changed(self, index):
        if self._model is None:
            return
        size = self.page_size_combo.currentData()
        if size:
            self._model.page_size = size
            self._model.page = 0
            self._model.reload()
            self._update_status()
            self.table.scrollToTop()

    def _goto_page(self):
        if self._model is None:
            return
        text = self.goto_input.text().strip()
        if not text:
            return
        try:
            page_num = int(text)
            if page_num < 1:
                page_num = 1
            total_pages = self._model.total_pages()
            if page_num > total_pages:
                page_num = total_pages
            self._model.goto_page(page_num - 1)
            self._update_status()
            self.table.scrollToTop()
        except ValueError:
            pass
        finally:
            self.goto_input.clear()

    # ─── Context Menu & Details ──────────────────────────────────────

    def _show_context_menu(self, position):
        if self._model is None:
            return
        index = self.table.indexAt(position)
        if not index.isValid():
            return
        row_data = self._model.get_row_data(index.row())
        if not row_data:
            return

        menu = QMenu(self)

        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        entry_hash = row_data.get("entry_hash", "")
        if entry_hash:
            copy_hash = menu.addAction("📋 Copy Entry Hash")
            copy_hash.triggered.connect(
                lambda: QApplication.clipboard().setText(entry_hash),
            )

        url = row_data.get("url", "")
        if url:
            copy_url = menu.addAction("📋 Copy URL")
            copy_url.triggered.connect(
                lambda: QApplication.clipboard().setText(url),
            )
            menu.addSeparator()
            add_sandbox_url_actions(
                menu, url, self, self.ctx.evidence_id,
                evidence_label=self.ctx.get_evidence_label(),
                workspace_path=self.ctx.case_folder,
                case_data=self.ctx.case_data,
            )

        menu.exec(self.table.viewport().mapToGlobal(position))

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._model.get_row_data(index.row())
        if not row_data:
            return
        CacheIndexDetailsDialog(row_data, parent=self).exec()
