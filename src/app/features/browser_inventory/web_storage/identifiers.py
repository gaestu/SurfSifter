"""Storage identifiers subtab widget."""
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
from .identifiers_dialog import StorageIdentifierDetailsDialog
from .identifiers_model import StorageIdentifiersTableModel

logger = logging.getLogger(__name__)


class StorageIdentifiersSubtab(BaseArtifactSubtab):
    """User/device/tracking identifiers extracted from web storage."""

    def _default_status_text(self):
        return "0 identifiers"

    def _setup_filters(self, fl: QHBoxLayout):
        fl.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All", "")
        self.type_filter.addItem("User ID", "user_id")
        self.type_filter.addItem("Device ID", "device_id")
        self.type_filter.addItem("Tracking ID", "tracking_id")
        self.type_filter.addItem("Visitor ID", "visitor_id")
        self.type_filter.addItem("Session ID", "session_id")
        self.type_filter.addItem("Email", "email")
        fl.addWidget(self.type_filter)

        fl.addWidget(QLabel("Origin:"))
        self.origin_filter = QLineEdit()
        self.origin_filter.setPlaceholderText("Filter by origin...")
        self.origin_filter.setMaximumWidth(200)
        fl.addWidget(self.origin_filter)

        fl.addWidget(QLabel("Value:"))
        self.value_filter = QLineEdit()
        self.value_filter.setPlaceholderText("Filter by value...")
        self.value_filter.setMaximumWidth(150)
        fl.addWidget(self.value_filter)

    def _create_model(self):
        return StorageIdentifiersTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 80)    # Type
        t.setColumnWidth(1, 80)    # Name
        t.setColumnWidth(2, 150)   # Value
        t.setColumnWidth(3, 150)   # Origin
        t.setColumnWidth(4, 100)   # Storage Key
        t.setColumnWidth(5, 60)    # Browser
        t.setColumnWidth(6, 60)    # Profile
        t.setColumnWidth(7, 70)    # Storage
        t.setColumnWidth(8, 90)    # First Seen
        t.setColumnWidth(9, 90)    # Last Seen
        t.setColumnWidth(10, 60)   # Partition

    def _apply_filters(self):
        if self._model is None:
            return
        id_type = self.type_filter.currentData() or ""
        origin = self.origin_filter.text().strip()
        value = self.value_filter.text().strip()
        self._model.load(
            identifier_type_filter=id_type,
            origin_filter=origin,
            value_filter=value,
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        stats = self._model.get_stats()
        total = stats.get("total", 0)
        by_type = stats.get("by_type", {})
        type_parts = []
        for id_type, count in sorted(by_type.items(), key=lambda x: -x[1]):
            type_parts.append(f"{id_type.replace('_', ' ')}: {count}")
        type_str = ", ".join(type_parts[:4])
        if len(type_parts) > 4:
            type_str += f", +{len(type_parts) - 4} more"
        if type_str:
            self.status_label.setText(f"{total} identifiers ({type_str})")
        else:
            self.status_label.setText(f"{total} identifiers")

    def _get_row_data(self, index):
        """StorageIdentifiersTableModel uses row number for get_row_data."""
        if self._model is None:
            return None
        return self._model.get_row_data(index.row())

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return
        StorageIdentifierDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        value = row_data.get("identifier_value", "") or ""
        name = row_data.get("identifier_name", "") or ""

        if value:
            copy_value = menu.addAction("📋 Copy Identifier Value")
            copy_value.triggered.connect(
                lambda: QApplication.clipboard().setText(value)
            )
        if name:
            copy_name = menu.addAction("📋 Copy Identifier Name")
            copy_name.triggered.connect(
                lambda: QApplication.clipboard().setText(name)
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
