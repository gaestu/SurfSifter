"""Storage keys subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
)

from app.common import add_sandbox_url_actions
from app.common.dialogs import TagArtifactsDialog
from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .storage_keys_dialog import StorageKeyDetailsDialog
from .storage_keys_model import StorageKeysTableModel

logger = logging.getLogger(__name__)


class StorageKeysSubtab(BaseArtifactSubtab):
    """LocalStorage / SessionStorage key-value pairs with filtering."""

    def __init__(self, ctx: SubtabContext, parent=None):
        super().__init__(ctx, parent)
        self._pending_origin_filter: str | None = None

    def _default_status_text(self):
        return "0 keys (local: 0, session: 0)"

    def _setup_filters(self, fl: QHBoxLayout):
        fl.addWidget(QLabel("Origin:"))
        self.origin_filter = QLineEdit()
        self.origin_filter.setPlaceholderText("Filter by origin...")
        self.origin_filter.setMaximumWidth(200)
        fl.addWidget(self.origin_filter)

        fl.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All", "")
        self.type_filter.addItem("Local Storage", "local")
        self.type_filter.addItem("Session Storage", "session")
        fl.addWidget(self.type_filter)

        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Key:"))
        self.key_filter = QLineEdit()
        self.key_filter.setPlaceholderText("Filter by key name...")
        self.key_filter.setMaximumWidth(150)
        fl.addWidget(self.key_filter)

    def _create_model(self):
        return StorageKeysTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 180)   # Origin
        t.setColumnWidth(1, 60)    # Type
        t.setColumnWidth(2, 140)   # Key
        t.setColumnWidth(3, 200)   # Value
        t.setColumnWidth(4, 70)    # Browser
        t.setColumnWidth(5, 70)    # Profile
        t.setColumnWidth(6, 50)    # Size
        t.setColumnWidth(7, 90)    # Last Access
        t.setColumnWidth(8, 60)    # Partition
        t.setColumnWidth(9, 100)   # Tags

    def _populate_filter_options(self):
        self.browser_filter.blockSignals(True)
        for browser in self._model.get_browsers():
            self.browser_filter.addItem(browser.capitalize(), browser)
        self.browser_filter.blockSignals(False)

    def _apply_filters(self):
        if self._model is None:
            return
        origin = self.origin_filter.text().strip()
        storage_type = self.type_filter.currentData() or ""
        browser = self.browser_filter.currentData() or ""
        key_filter = self.key_filter.text().strip()
        self._model.load(
            origin_filter=origin,
            storage_type_filter=storage_type,
            browser_filter=browser,
            key_filter=key_filter,
        )
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        stats = self._model.get_stats()
        total = stats.get("total", 0)
        local = stats.get("local", 0)
        session = stats.get("session", 0)
        self.status_label.setText(f"{total} keys (local: {local}, session: {session})")

    def _get_row_data(self, index):
        """StorageKeysTableModel uses row number for get_row_data."""
        if self._model is None:
            return None
        return self._model.get_row_data(index.row())

    def load(self) -> None:
        """Override base load to honour any pending or active origin filter.

        Three scenarios:
        1. First load with pending filter (model was None when set_origin_filter
           was called): create model via super, then apply the stashed filter.
        2. Reload (stale) with an origin already in the text field (set by
           set_origin_filter before this load): skip the unfiltered super().load()
           and apply filters directly so the filtered view isn't overwritten.
        3. Normal load/reload with no special filter: delegate to super().
        """
        # Resolve pending filter first (stashed when model didn't exist yet)
        pending = self._pending_origin_filter
        if pending is not None:
            self._pending_origin_filter = None

        # Ensure model + table wiring exists (first-load path)
        if self._model is None:
            super().load()
            if pending:
                self.origin_filter.setText(pending)
                self._apply_filters()
            return

        # Model already exists — check whether a filter is active
        active_origin = pending or self.origin_filter.text().strip()
        if active_origin:
            # Re-apply filters so the filtered view survives stale reloads
            if pending:
                self.origin_filter.setText(pending)
            self._apply_filters()
            self._loaded = True
        else:
            # No filter active — normal unfiltered reload
            super().load()

    def set_origin_filter(self, origin: str):
        """Set origin filter text for cross-tab navigation from stored sites."""
        if self._model is None:
            # Model not yet created — stash for first load()
            self.origin_filter.setText(origin)
            self._pending_origin_filter = origin
            return
        self.origin_filter.setText(origin)
        self._apply_filters()
        # Mark as loaded so a subsequent load() from the lazy-load path
        # doesn't overwrite with an unfiltered query.
        self._loaded = True

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return
        StorageKeyDetailsDialog(row_data, parent=self).exec()

    def _tag_selected(self):
        """Tag selected - groups by storage type (local vs session)."""
        from typing import List

        if self.ctx.case_data is None:
            QMessageBox.warning(self, "Tagging Unavailable", "Case data is not loaded.")
            return
        if self._model is None or self.table.selectionModel() is None:
            return

        local_ids: List[int] = []
        session_ids: List[int] = []
        for index in self.table.selectionModel().selectedRows():
            row_data = self._get_row_data(index)
            if row_data and row_data.get("id") is not None:
                storage_type = row_data.get("storage_type", "")
                artifact_id = int(row_data["id"])
                if storage_type == "local":
                    local_ids.append(artifact_id)
                elif storage_type == "session":
                    session_ids.append(artifact_id)

        if not local_ids and not session_ids:
            QMessageBox.information(self, "No Selection", "Select at least one row to tag.")
            return

        if local_ids:
            dialog = TagArtifactsDialog(
                self.ctx.case_data, self.ctx.evidence_id, "local_storage", local_ids, self
            )
            if session_ids:
                dialog.setWindowTitle(f"Tag {len(local_ids)} Local Storage Keys")
            dialog.tags_changed.connect(self._on_tags_changed)
            dialog.exec()

        if session_ids:
            dialog = TagArtifactsDialog(
                self.ctx.case_data, self.ctx.evidence_id, "session_storage", session_ids, self
            )
            if local_ids:
                dialog.setWindowTitle(f"Tag {len(session_ids)} Session Storage Keys")
            dialog.tags_changed.connect(self._on_tags_changed)
            dialog.exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        key = row_data.get("key", "") or ""
        value = row_data.get("value", "") or ""

        if key:
            copy_key = menu.addAction("📋 Copy Key Name")
            copy_key.triggered.connect(
                lambda: QApplication.clipboard().setText(key)
            )
        if value:
            copy_value = menu.addAction("📋 Copy Value")
            copy_value.triggered.connect(
                lambda: QApplication.clipboard().setText(value)
            )
        if key and value:
            copy_kv = menu.addAction("📋 Copy Key=Value")
            copy_kv.triggered.connect(
                lambda: QApplication.clipboard().setText(f"{key}={value}")
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

        source_path = row_data.get("source_path", "") or ""
        if source_path:
            menu.addSeparator()
            related_action = menu.addAction("🔗 View Related URLs/Emails")
            related_action.triggered.connect(
                lambda: self._show_related_urls_emails(source_path, key, origin)
            )

        menu.addSeparator()
        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)

    def _show_related_urls_emails(self, source_path, key, origin):
        """Show related URLs and emails for a storage key."""
        from urllib.parse import urlparse

        from core.database import get_urls, get_emails
        from .related_dialog import RelatedUrlsEmailsDialog

        try:
            conn = self.ctx.db_manager.get_evidence_conn(
                self.ctx.evidence_id, self.ctx.get_evidence_label()
            )
            domain = None
            if origin:
                try:
                    parsed = urlparse(origin)
                    domain = parsed.netloc or origin
                except Exception:
                    domain = origin

            urls = get_urls(
                conn, self.ctx.evidence_id, domain=domain,
                discovered_by="firefox_storage", limit=500,
            )
            emails = get_emails(
                conn, self.ctx.evidence_id, domain=domain, limit=500,
            )

            dialog = RelatedUrlsEmailsDialog(
                urls=urls,
                emails=emails,
                source_context=f"Key: {key}\nOrigin: {origin}\nSource: {source_path}",
                parent=self,
            )
            dialog.exec()
        except Exception as e:
            logger.error(f"Failed to load related URLs/emails: {e}", exc_info=True)
            QMessageBox.warning(self, "Error", f"Failed to load related URLs/emails: {e}")
