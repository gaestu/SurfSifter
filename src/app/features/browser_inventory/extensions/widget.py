"""Extensions subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import QApplication, QComboBox, QHBoxLayout, QLabel, QMenu

from app.common import add_sandbox_url_actions
from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .dialog import ExtensionDetailsDialog
from .model import ExtensionsTableModel

logger = logging.getLogger(__name__)


class ExtensionsSubtab(BaseArtifactSubtab):
    """Browser extensions with category and risk score filtering."""

    def _default_status_text(self):
        return "0 extensions"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Category:"))
        self.category_filter = QComboBox()
        self.category_filter.addItem("All", "")
        fl.addWidget(self.category_filter)

        fl.addWidget(QLabel("Risk:"))
        self.risk_filter = QComboBox()
        self.risk_filter.addItem("All", 0)
        self.risk_filter.addItem("Low (20+)", 20)
        self.risk_filter.addItem("Medium (40+)", 40)
        self.risk_filter.addItem("High (60+)", 60)
        self.risk_filter.addItem("Critical (80+)", 80)
        fl.addWidget(self.risk_filter)

    def _create_model(self):
        return ExtensionsTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 180)   # Name
        t.setColumnWidth(1, 280)   # Extension ID
        t.setColumnWidth(2, 60)    # Version
        t.setColumnWidth(3, 80)    # Browser
        t.setColumnWidth(4, 70)    # Profile
        t.setColumnWidth(5, 55)    # Enabled
        t.setColumnWidth(6, 45)    # Risk
        t.setColumnWidth(7, 200)   # Risk Factors
        t.setColumnWidth(8, 90)    # Category
        t.setColumnWidth(9, 100)   # Install Source
        t.setColumnWidth(10, 70)   # Web Store

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)
        for category in self._model.get_available_categories():
            display = category.replace("_", " ").title()
            self.category_filter.addItem(display, category)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        category = self.category_filter.currentData() or ""
        min_risk = self.risk_filter.currentData() or 0
        self._model.load(
            browser_filter=browser,
            min_risk=min_risk,
            category_filter=category,
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        high_risk = sum(
            1
            for i in range(count)
            if (rec := self._model.get_record(i)) and (rec.get("risk_score") or 0) >= 60
        )
        if high_risk > 0:
            self.status_label.setText(f"{count} extensions ({high_risk} high risk)")
        else:
            self.status_label.setText(f"{count} extensions")

    def _artifact_type_for_tagging(self):
        return "browser_extension"

    def _get_row_data(self, index):
        """Override: ExtensionsTableModel uses get_record(row) not get_row_data(index)."""
        if self._model is None:
            return None
        return self._model.get_record(index.row())

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._model.get_record(index.row())
        if not row_data:
            return
        ExtensionDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        ext_id = row_data.get("extension_id", "") or ""
        if ext_id:
            copy_id = menu.addAction("📋 Copy Extension ID")
            copy_id.triggered.connect(
                lambda: QApplication.clipboard().setText(ext_id)
            )

        homepage_url = row_data.get("homepage_url", "") or ""
        if homepage_url:
            add_sandbox_url_actions(
                menu,
                homepage_url,
                self,
                self.ctx.evidence_id,
                evidence_label=self.ctx.get_evidence_label(),
                workspace_path=self.ctx.case_folder,
                case_data=self.ctx.case_data,
            )

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
