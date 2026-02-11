"""IndexedDB subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
)

from app.common import add_sandbox_url_actions
from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .storage_keys_dialog import StorageKeyDetailsDialog
from .indexeddb_model import IndexedDBTableModel

logger = logging.getLogger(__name__)


class IndexedDBSubtab(BaseArtifactSubtab):
    """IndexedDB structured database entries with filtering."""

    def _default_status_text(self):
        return "0 entries (0 databases)"

    def _setup_filters(self, fl: QHBoxLayout):
        fl.addWidget(QLabel("Origin:"))
        self.origin_filter = QLineEdit()
        self.origin_filter.setPlaceholderText("Filter by origin...")
        self.origin_filter.setMaximumWidth(200)
        fl.addWidget(self.origin_filter)

        fl.addWidget(QLabel("Database:"))
        self.database_filter = QLineEdit()
        self.database_filter.setPlaceholderText("Filter by database...")
        self.database_filter.setMaximumWidth(150)
        fl.addWidget(self.database_filter)

        fl.addWidget(QLabel("Object Store:"))
        self.store_filter = QComboBox()
        self.store_filter.addItem("All", "")
        self.store_filter.setMinimumWidth(120)
        fl.addWidget(self.store_filter)

    def _create_model(self):
        return IndexedDBTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 160)   # Origin
        t.setColumnWidth(1, 100)   # Database
        t.setColumnWidth(2, 100)   # Object Store
        t.setColumnWidth(3, 100)   # Key
        t.setColumnWidth(4, 180)   # Value
        t.setColumnWidth(5, 70)    # Source
        t.setColumnWidth(6, 60)    # Browser
        t.setColumnWidth(7, 60)    # Profile
        t.setColumnWidth(8, 50)    # Size
        t.setColumnWidth(9, 60)    # Partition

    def _populate_filter_options(self):
        self.store_filter.blockSignals(True)
        for store in self._model.get_object_stores():
            self.store_filter.addItem(store, store)
        self.store_filter.blockSignals(False)

    def _apply_filters(self):
        if self._model is None:
            return
        origin = self.origin_filter.text().strip()
        database = self.database_filter.text().strip()
        object_store = self.store_filter.currentData() or ""
        self._model.load(
            origin_filter=origin,
            database_filter=database,
            object_store_filter=object_store,
        )
        self._update_status()

        # Re-populate store filter preserving current selection
        current_store = self.store_filter.currentData() or ""
        self.store_filter.blockSignals(True)
        self.store_filter.clear()
        self.store_filter.addItem("All", "")
        for store in self._model.get_object_stores():
            self.store_filter.addItem(store, store)
        # Restore selection
        idx = self.store_filter.findData(current_store)
        if idx >= 0:
            self.store_filter.setCurrentIndex(idx)
        self.store_filter.blockSignals(False)

    def _update_status(self):
        if self._model is None:
            return
        stats = self._model.get_stats()
        entries = stats.get("entries", 0)
        databases = stats.get("databases", 0)
        self.status_label.setText(f"{entries} entries ({databases} databases)")

    def _get_row_data(self, index):
        """IndexedDBTableModel uses row number for get_row_data."""
        if self._model is None:
            return None
        return self._model.get_row_data(index.row())

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return
        dialog = StorageKeyDetailsDialog(row_data, parent=self)
        dialog.setWindowTitle("IndexedDB Entry Details")
        dialog.exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        key = row_data.get("key", "") or ""
        value = row_data.get("value", "") or ""

        if key:
            copy_key = menu.addAction("📋 Copy Key")
            copy_key.triggered.connect(
                lambda: QApplication.clipboard().setText(key)
            )
        if value:
            copy_value = menu.addAction("📋 Copy Value")
            copy_value.triggered.connect(
                lambda: QApplication.clipboard().setText(value)
            )

        database_name = row_data.get("database_name", "") or row_data.get("database", "") or ""
        object_store = row_data.get("object_store_name", "") or row_data.get("object_store", "") or ""
        if database_name or object_store:
            menu.addSeparator()
            if database_name:
                copy_db = menu.addAction("📋 Copy Database Name")
                copy_db.triggered.connect(
                    lambda: QApplication.clipboard().setText(database_name)
                )
            if object_store:
                copy_store = menu.addAction("📋 Copy Object Store")
                copy_store.triggered.connect(
                    lambda: QApplication.clipboard().setText(object_store)
                )

        origin = row_data.get("origin", "") or ""
        if origin:
            menu.addSeparator()
            copy_origin = menu.addAction("📋 Copy Origin")
            copy_origin.triggered.connect(
                lambda: QApplication.clipboard().setText(origin)
            )
            clean_origin = origin.split(",")[0].strip() if "," in origin else origin
            if clean_origin.startswith("http://") or clean_origin.startswith("https://"):
                add_sandbox_url_actions(
                    menu,
                    clean_origin,
                    self,
                    self.ctx.evidence_id,
                    evidence_label=self.ctx.get_evidence_label(),
                    workspace_path=self.ctx.case_folder,
                    case_data=self.ctx.case_data,
                )
