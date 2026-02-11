"""Web Storage container with nested subtabs."""
from __future__ import annotations

from PySide6.QtWidgets import QLabel, QTabWidget, QVBoxLayout, QWidget

from app.features.browser_inventory._base import SubtabContext
from app.features.browser_inventory.web_storage.stored_sites import StoredSitesSubtab
from app.features.browser_inventory.web_storage.storage_keys import StorageKeysSubtab
from app.features.browser_inventory.web_storage.indexeddb import IndexedDBSubtab
from app.features.browser_inventory.web_storage.auth_tokens import AuthTokensSubtab
from app.features.browser_inventory.web_storage.identifiers import StorageIdentifiersSubtab


class WebStorageContainer(QWidget):
    """Container with nested tabs for web storage artifact types."""

    def __init__(self, ctx: SubtabContext, parent=None):
        super().__init__(parent)
        self.ctx = ctx
        self._subtabs = []
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        desc_label = QLabel(
            "<b>Web Storage:</b> Data stored by websites in your browser using LocalStorage, "
            "SessionStorage, and IndexedDB APIs. The Overview shows per-site summaries; "
            "Storage Keys shows LocalStorage/SessionStorage key-value pairs; "
            "IndexedDB shows structured database entries; "
            "Auth Tokens shows authentication tokens; "
            "Identifiers shows user/device/tracking IDs extracted from storage."
        )
        desc_label.setWordWrap(True)
        desc_label.setStyleSheet(
            "QLabel { padding: 4px; background-color: #f0f0f0; "
            "border-radius: 4px; margin: 2px; }"
        )
        layout.addWidget(desc_label)

        self.nested_tabs = QTabWidget()
        self.nested_tabs.currentChanged.connect(self._on_nested_tab_changed)

        self._stored_sites = StoredSitesSubtab(self.ctx, parent=self)
        self._storage_keys = StorageKeysSubtab(self.ctx, parent=self)
        self._indexeddb = IndexedDBSubtab(self.ctx, parent=self)
        self._auth_tokens = AuthTokensSubtab(self.ctx, parent=self)
        self._identifiers = StorageIdentifiersSubtab(self.ctx, parent=self)

        self._subtabs = [
            self._stored_sites, self._storage_keys, self._indexeddb,
            self._auth_tokens, self._identifiers,
        ]

        self.nested_tabs.addTab(self._stored_sites, "Sites Overview")
        self.nested_tabs.addTab(self._storage_keys, "Storage Keys")
        self.nested_tabs.addTab(self._indexeddb, "IndexedDB")
        self.nested_tabs.addTab(self._auth_tokens, "Auth Tokens")
        self.nested_tabs.addTab(self._identifiers, "Identifiers")

        layout.addWidget(self.nested_tabs)

    # --- Public interface expected by the orchestrator tab ---
    @property
    def storage_keys_subtab(self):
        """Expose storage keys subtab for cross-tab navigation."""
        return self._storage_keys

    def load(self):
        """Load the currently visible nested subtab."""
        idx = self.nested_tabs.currentIndex()
        subtab = self._subtabs[idx]
        if not subtab.is_loaded:
            subtab.load()

    def mark_needs_reload(self):
        """Mark all nested subtabs as needing reload."""
        for subtab in self._subtabs:
            subtab.mark_needs_reload()

    @property
    def is_loaded(self):
        """True if any nested subtab has been loaded."""
        return any(s.is_loaded for s in self._subtabs)

    def _on_nested_tab_changed(self, index):
        subtab = self._subtabs[index]
        if not subtab.is_loaded:
            subtab.load()
