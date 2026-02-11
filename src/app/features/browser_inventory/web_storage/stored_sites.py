"""Stored sites subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QSpinBox,
)

from app.common import add_sandbox_url_actions
from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .stored_sites_dialog import StoredSiteDetailsDialog
from .stored_sites_model import StoredSitesTableModel

logger = logging.getLogger(__name__)


class StoredSitesSubtab(BaseArtifactSubtab):
    """Per-site storage summary with origin filtering."""

    view_all_keys_requested = Signal(str)

    def _default_status_text(self):
        return "0 sites"

    def _setup_filters(self, fl: QHBoxLayout):
        fl.addWidget(QLabel("Site:"))
        self.origin_filter = QLineEdit()
        self.origin_filter.setPlaceholderText("Filter by site...")
        self.origin_filter.setMaximumWidth(200)
        fl.addWidget(self.origin_filter)

        fl.addWidget(QLabel("Min Total:"))
        self.min_total = QSpinBox()
        self.min_total.setMinimum(0)
        self.min_total.setMaximum(10000)
        self.min_total.setValue(0)
        self.min_total.setToolTip("Minimum total storage keys to include")
        fl.addWidget(self.min_total)

    def _create_model(self):
        return StoredSitesTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 250)   # Site
        t.setColumnWidth(1, 100)   # Local Storage
        t.setColumnWidth(2, 100)   # Session Storage
        t.setColumnWidth(3, 90)    # IndexedDB
        t.setColumnWidth(4, 90)    # Total Keys
        t.setColumnWidth(5, 150)   # Tags

    def _apply_filters(self):
        if self._model is None:
            return
        origin = self.origin_filter.text().strip()
        min_total = self.min_total.value()
        self._model.load(origin_filter=origin, min_total=min_total)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        total_keys = self._model.get_total_keys()
        self.status_label.setText(f"{count} sites | {total_keys} total keys")

    def _artifact_type_for_tagging(self):
        return "stored_site"

    def _get_row_data(self, index):
        """Use get_record_by_row for StoredSitesTableModel."""
        if self._model is None:
            return None
        return self._model.get_record_by_row(index.row())

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return

        dialog = StoredSiteDetailsDialog(
            row_data,
            db_manager=self.ctx.db_manager,
            evidence_id=self.ctx.evidence_id,
            evidence_label=self.ctx.get_evidence_label(),
            parent=self,
        )
        dialog.view_all_keys_requested.connect(self._on_view_all_keys)
        dialog.exec()

    def _on_view_all_keys(self, origin: str):
        """Forward cross-tab navigation signal."""
        self.view_all_keys_requested.emit(origin)

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected Sites...")
        tag_action.triggered.connect(self._tag_selected)

        origin = row_data.get("origin", "") or ""

        if origin:
            menu.addSeparator()

            copy_origin = menu.addAction("📋 Copy Site Origin")
            copy_origin.triggered.connect(
                lambda: QApplication.clipboard().setText(origin)
            )

            clean_origin = origin.split(",")[0].strip() if "," in origin else origin
            if clean_origin.startswith("http://") or clean_origin.startswith("https://"):
                menu.addSeparator()
                add_sandbox_url_actions(
                    menu,
                    clean_origin,
                    self,
                    self.ctx.evidence_id,
                    evidence_label=self.ctx.get_evidence_label(),
                    workspace_path=self.ctx.case_folder,
                    case_data=self.ctx.case_data,
                )
