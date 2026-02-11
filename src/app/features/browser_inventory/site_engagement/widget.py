"""Site engagement subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import QApplication, QComboBox, QHBoxLayout, QLabel, QMenu

from app.common import add_sandbox_url_actions
from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .dialog import SiteEngagementDetailsDialog
from .model import SiteEngagementTableModel

logger = logging.getLogger(__name__)


class SiteEngagementSubtab(BaseArtifactSubtab):
    """Site and media engagement scores with type and score filtering."""

    def _default_status_text(self):
        return "0 engagement records"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All", "")
        self.type_filter.addItem("Site Engagement", "site_engagement")
        self.type_filter.addItem("Media Engagement", "media_engagement")
        fl.addWidget(self.type_filter)

        fl.addWidget(QLabel("Min Score:"))
        self.min_score = QComboBox()
        self.min_score.addItem("Any", None)
        self.min_score.addItem("1+", 1.0)
        self.min_score.addItem("5+", 5.0)
        self.min_score.addItem("10+", 10.0)
        self.min_score.addItem("25+", 25.0)
        self.min_score.addItem("50+", 50.0)
        fl.addWidget(self.min_score)

    def _create_model(self):
        return SiteEngagementTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 200)   # Origin
        t.setColumnWidth(1, 60)    # Type
        t.setColumnWidth(2, 80)    # Browser
        t.setColumnWidth(3, 70)    # Profile
        t.setColumnWidth(4, 55)    # Score
        t.setColumnWidth(5, 50)    # Visits
        t.setColumnWidth(6, 70)    # Playbacks
        t.setColumnWidth(7, 130)   # Last Engagement
        t.setColumnWidth(8, 120)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        engagement_type = self.type_filter.currentData() or ""
        min_score = self.min_score.currentData()
        self._model.load(
            browser_filter=browser,
            type_filter=engagement_type,
            min_score=min_score,
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        site_count = self._model.get_site_engagement_count()
        media_count = self._model.get_media_engagement_count()
        max_score = self._model.get_max_score()
        parts = [f"{count} records"]
        if site_count > 0:
            parts.append(f"{site_count} site")
        if media_count > 0:
            parts.append(f"{media_count} media")
        if max_score > 0:
            parts.append(f"max score: {max_score:.2f}")
        self.status_label.setText(" | ".join(parts))

    def _artifact_type_for_tagging(self):
        return "site_engagement"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._model.get_row_data(index)
        if not row_data:
            return
        SiteEngagementDetailsDialog(row_data, parent=self).exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        origin = row_data.get("origin", "") or ""
        if origin:
            # Clean multi-valued origins
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
                menu.addSeparator()

            copy_origin = menu.addAction("📋 Copy Origin")
            copy_origin.triggered.connect(
                lambda: QApplication.clipboard().setText(origin)
            )
            menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
