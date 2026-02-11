"""Cache entries subtab widget with pagination."""
from __future__ import annotations

import logging

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
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
from .model import CacheEntriesTableModel
from .dialog import CacheEntryDetailsDialog

logger = logging.getLogger(__name__)


class CacheSubtab(QWidget):
    """Cache entries with pagination and domain/type filtering."""

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
                self._model = CacheEntriesTableModel(
                    self.ctx.db_manager,
                    self.ctx.evidence_id,
                    self.ctx.get_evidence_label(),
                    case_data=self.ctx.case_data,
                    parent=self,
                )
                self.table.setModel(self._model)

                self.table.setColumnWidth(0, 300)  # URL
                self.table.setColumnWidth(1, 120)  # Domain
                self.table.setColumnWidth(2, 80)   # Browser
                self.table.setColumnWidth(3, 50)   # Status
                self.table.setColumnWidth(4, 120)  # Content-Type
                self.table.setColumnWidth(5, 140)  # Last Used
                self.table.setColumnWidth(6, 120)  # Cache File
                self.table.setColumnWidth(7, 200)  # Source Path

                browsers = self._model.get_available_browsers()
                for browser in browsers:
                    self.browser_filter.addItem(browser.capitalize(), browser)

            self._model.load()
            self._update_status()
            self._loaded = True

        except Exception as e:
            logger.error(f"Failed to load cache entries: {e}", exc_info=True)
            self.status_label.setText(f"Error loading cache entries: {e}")

    def mark_needs_reload(self):
        self._loaded = False

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Filter controls
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel("Browser:"))
        self.browser_filter = QComboBox()
        self.browser_filter.addItem("All", "")
        filter_layout.addWidget(self.browser_filter)

        filter_layout.addWidget(QLabel("Domain:"))
        self.domain_filter = QLineEdit()
        self.domain_filter.setPlaceholderText("Filter by domain...")
        self.domain_filter.setMaximumWidth(200)
        filter_layout.addWidget(self.domain_filter)

        filter_layout.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All", "")
        self.type_filter.addItem("Images", "image/")
        self.type_filter.addItem("HTML", "text/html")
        self.type_filter.addItem("JavaScript", "application/javascript")
        self.type_filter.addItem("CSS", "text/css")
        self.type_filter.addItem("JSON", "application/json")
        filter_layout.addWidget(self.type_filter)

        apply_btn = QPushButton("Apply")
        apply_btn.clicked.connect(self._apply_filters)
        filter_layout.addWidget(apply_btn)

        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        # Table view
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

        # Pagination and status row
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

        self.status_label = QLabel("0 cache entries")
        pagination_layout.addWidget(self.status_label)

        layout.addLayout(pagination_layout)

    # ─── Filters ──────────────────────────────────────────────────────

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        domain = self.domain_filter.text().strip()
        content_type = self.type_filter.currentData() or ""
        self._model.load(
            browser_filter=browser,
            domain_filter=domain,
            content_type_filter=content_type,
        )
        self._update_status()

    # ─── Status ──────────────────────────────────────────────────────

    def _update_status(self):
        if self._model is None:
            return

        total_count = self._model.total_count()
        stats = self._model.get_stats()

        parts = [f"{total_count} total"]
        by_status = stats.get("by_status", {})
        ok_count = sum(v for k, v in by_status.items() if k and 200 <= k < 300)
        error_count = sum(v for k, v in by_status.items() if k and k >= 400)
        if ok_count > 0:
            parts.append(f"{ok_count} OK")
        if error_count > 0:
            parts.append(f"{error_count} errors")
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

        url = row_data.get("url", "")
        if url:
            copy_url = menu.addAction("📋 Copy URL")
            copy_url.triggered.connect(lambda: QApplication.clipboard().setText(url))
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
        CacheEntryDetailsDialog(row_data, parent=self).exec()
