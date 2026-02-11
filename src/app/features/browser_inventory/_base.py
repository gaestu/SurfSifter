"""Base class for browser inventory subtab widgets.

Provides common patterns for artifact subtabs: filter bar, table view,
lazy loading, status label, context menu, and tagging integration.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Callable, List, Optional

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMenu,
    QMessageBox,
    QPushButton,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from app.common.dialogs import TagArtifactsDialog

logger = logging.getLogger(__name__)


class SubtabContext:
    """Shared context passed from BrowserInventoryTab to each subtab.

    Avoids each subtab needing its own copy of every shared resource.
    """

    def __init__(
        self,
        case_folder: Path,
        evidence_id: int,
        case_db_path: Path,
        case_data,
        db_manager,
    ):
        self.case_folder = case_folder
        self.evidence_id = evidence_id
        self.case_db_path = case_db_path
        self.case_data = case_data
        self.db_manager = db_manager
        self._evidence_label_cache: Optional[str] = None

    def get_evidence_label(self) -> str:
        """Retrieve the evidence label from the database."""
        if self._evidence_label_cache is not None:
            return self._evidence_label_cache

        with sqlite3.connect(self.case_db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT label FROM evidences WHERE id = ?",
                (self.evidence_id,),
            ).fetchone()
            if row and row["label"]:
                self._evidence_label_cache = row["label"]
            else:
                self._evidence_label_cache = f"EV-{self.evidence_id:03d}"
                logger.warning(
                    "Evidence %d has no label in database, using fallback: %s",
                    self.evidence_id,
                    self._evidence_label_cache,
                )

        return self._evidence_label_cache

    def evidence_db_path(self) -> str:
        """Get the evidence database file path."""
        return self.db_manager.evidence_db_path(
            self.evidence_id, label=self.get_evidence_label()
        )


class BaseArtifactSubtab(QWidget):
    """Base class for a simple artifact subtab with filter bar + table + status.

    Subclasses override:
    - _setup_filters(filter_layout): Add filter widgets to the horizontal layout
    - _create_model(): Create and return the table model
    - _configure_table(): Set column widths after model is assigned
    - _populate_filter_options(): Populate filter dropdowns after model is loaded
    - _apply_filters(): Read filter widget values and call model.load(...)
    - _update_status(): Update the status label text from model stats
    - _view_details(index): Open detail dialog for double-clicked row
    - _build_context_menu(menu, index, row_data): Add actions to the context menu
    """

    def __init__(self, ctx: SubtabContext, parent=None):
        super().__init__(parent)
        self.ctx = ctx
        self._loaded = False
        self._model = None

        self._setup_ui()

    # ─── UI Construction ──────────────────────────────────────────────

    def _setup_ui(self) -> None:
        """Build the standard subtab layout."""
        layout = QVBoxLayout(self)

        # Optional description label (override to provide)
        desc = self._description_text()
        if desc:
            desc_label = QLabel(desc)
            desc_label.setWordWrap(True)
            desc_label.setStyleSheet(
                "QLabel { color: #666; font-style: italic; margin-bottom: 4px; }"
            )
            layout.addWidget(desc_label)

        # Filter bar
        filter_layout = QHBoxLayout()
        self._setup_filters(filter_layout)

        # Apply button
        apply_btn = QPushButton("Apply")
        apply_btn.clicked.connect(self._on_apply_clicked)
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

        # Status label
        self.status_label = QLabel(self._default_status_text())
        layout.addWidget(self.status_label)

    def _description_text(self) -> str:
        """Override to add a description label above filters. Return empty for none."""
        return ""

    def _default_status_text(self) -> str:
        """Override for initial status label text."""
        return "0 records"

    def _setup_filters(self, filter_layout: QHBoxLayout) -> None:
        """Override to add filter widgets to the layout."""
        pass

    # ─── Helpers for filters ──────────────────────────────────────────

    def _add_browser_filter(self, filter_layout: QHBoxLayout) -> QComboBox:
        """Add a standard Browser: combo to the filter layout."""
        filter_layout.addWidget(QLabel("Browser:"))
        combo = QComboBox()
        combo.addItem("All", "")
        filter_layout.addWidget(combo)
        return combo

    # ─── Lazy Loading ─────────────────────────────────────────────────

    def load(self) -> None:
        """Lazy-load data on first visit (or reload if needed)."""
        try:
            first_load = self._model is None
            if first_load:
                self._model = self._create_model()
                self.table.setModel(self._model)
                self._configure_table()

            self._model.load()
            if first_load:
                self._populate_filter_options()
            self._update_status()
            self._loaded = True

        except Exception as e:
            logger.error(f"Failed to load {self.__class__.__name__}: {e}", exc_info=True)
            self.status_label.setText(f"Error: {e}")

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    def mark_needs_reload(self) -> None:
        """Reset so next load() call reloads data."""
        self._loaded = False

    def _create_model(self):
        """Override: create and return the QAbstractTableModel."""
        raise NotImplementedError

    def _configure_table(self) -> None:
        """Override: set column widths after model is assigned."""
        pass

    def _populate_filter_options(self) -> None:
        """Override: populate filter dropdowns from model data."""
        pass

    # ─── Filter Application ──────────────────────────────────────────

    def _on_apply_clicked(self) -> None:
        """Called when Apply button is pressed."""
        self._apply_filters()

    def _apply_filters(self) -> None:
        """Override: read filter values and call self._model.load(...)."""
        if self._model is not None:
            self._model.load()
            self._update_status()

    # ─── Status ──────────────────────────────────────────────────────

    def _update_status(self) -> None:
        """Override: update status label from model stats."""
        if self._model is not None:
            count = self._model.rowCount()
            self.status_label.setText(f"{count} records")

    # ─── Context Menu & Details ──────────────────────────────────────

    def _show_context_menu(self, position) -> None:
        """Standard context menu handler."""
        if self._model is None:
            return

        index = self.table.indexAt(position)
        if not index.isValid():
            return

        row_data = self._get_row_data(index)
        if not row_data:
            return

        # Select the row if not already selected
        if self.table.selectionModel() and not self.table.selectionModel().selectedRows():
            self.table.selectRow(index.row())

        menu = QMenu(self)
        self._build_context_menu(menu, index, row_data)
        menu.exec(self.table.viewport().mapToGlobal(position))

    def _get_row_data(self, index):
        """Get row data from model. Handles both index-based and row-number patterns."""
        if hasattr(self._model, "get_row_data"):
            # Some models take QModelIndex, some take int
            try:
                return self._model.get_row_data(index)
            except TypeError:
                return self._model.get_row_data(index.row())
        elif hasattr(self._model, "get_record_by_row"):
            return self._model.get_record_by_row(index.row())
        elif hasattr(self._model, "get_record"):
            return self._model.get_record(index.row())
        return None

    def _build_context_menu(self, menu: QMenu, index, row_data: dict) -> None:
        """Override: add actions to context menu. Base provides View Details + Tag."""
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)

    def _view_details(self, index) -> None:
        """Override: open detail dialog for double-clicked row."""
        pass

    # ─── Tagging ─────────────────────────────────────────────────────

    def _artifact_type_for_tagging(self) -> str:
        """Override: return the artifact_type string for TagArtifactsDialog."""
        return ""

    def _get_selected_ids(self) -> List[int]:
        """Return IDs of selected rows."""
        if self._model is None or self.table.selectionModel() is None:
            return []
        ids: List[int] = []
        for index in self.table.selectionModel().selectedRows():
            row_data = self._get_row_data(index)
            if row_data and row_data.get("id") is not None:
                ids.append(int(row_data["id"]))
        return ids

    def _tag_selected(self) -> None:
        """Launch tagging dialog for selected rows."""
        if self.ctx.case_data is None:
            QMessageBox.warning(self, "Tagging Unavailable", "Case data is not loaded.")
            return

        artifact_type = self._artifact_type_for_tagging()
        if not artifact_type:
            return

        ids = self._get_selected_ids()
        if not ids:
            QMessageBox.information(self, "No Selection", "Select at least one row to tag.")
            return

        dialog = TagArtifactsDialog(
            self.ctx.case_data, self.ctx.evidence_id, artifact_type, ids, self
        )
        dialog.tags_changed.connect(self._on_tags_changed)
        dialog.exec()

    def _on_tags_changed(self) -> None:
        """Refresh after tag changes."""
        if self.ctx.case_data:
            self.ctx.case_data.invalidate_filter_cache(self.ctx.evidence_id)
        self._apply_filters()
