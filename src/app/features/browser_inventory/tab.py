"""
Browser/Cache Inventory tab — orchestrator.

Delegates to subtab widgets under browser_inventory/{subtab}/.
Each subtab is self-contained with its own widget, model, and dialog.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from PySide6.QtWidgets import QTabWidget, QVBoxLayout, QWidget

from app.features.browser_inventory._base import SubtabContext

# Top-level subtabs
from app.features.browser_inventory.inventory import InventorySubtab
from app.features.browser_inventory.history import HistorySubtab
from app.features.browser_inventory.search_terms import SearchTermsSubtab
from app.features.browser_inventory.cookies import CookiesSubtab
from app.features.browser_inventory.bookmarks import BookmarksSubtab
from app.features.browser_inventory.downloads import DownloadsSubtab
from app.features.browser_inventory.permissions import PermissionsSubtab
from app.features.browser_inventory.media import MediaSubtab
from app.features.browser_inventory.site_engagement import SiteEngagementSubtab
from app.features.browser_inventory.extensions import ExtensionsSubtab

# Container subtabs (with nested tabs inside)
from app.features.browser_inventory.autofill import AutofillContainer
from app.features.browser_inventory.sessions import SessionsContainer
from app.features.browser_inventory.web_storage import WebStorageContainer

# Cache (container with nested tabs: Entries + Index)
from app.features.browser_inventory.cache import CacheContainer

logger = logging.getLogger(__name__)


class BrowserInventoryTab(QWidget):
    """
    Browser/Cache Inventory tab.

    Displays all browser artifacts discovered during extraction with their
    extraction and ingestion status. Provides filtering and context menu
    actions for viewing details, opening files, and tagging.

    Subtabs:
    - Inventory: Raw browser artifact files with extraction status
    - History: Parsed browser history with visit-type filtering
    - Search Terms: Extracted search queries
    - Cookies: Parsed cookies with domain/browser filtering
    - Bookmarks: Parsed bookmarks with folder column
    - Downloads: Browser download history with state/danger info
    - Autofill: Form autofill data, saved logins, credit cards, addresses, etc.
    - Restored Tabs: Browser session tabs and form data
    - Permissions: Site permission settings
    - Media: Media playback history
    - Site Engagement: Chromium site engagement scores
    - Extensions: Browser extensions with risk analysis
    - Web Storage: LocalStorage, SessionStorage, IndexedDB, auth tokens
    - Cache: Browser cache entries with pagination
    """

    def __init__(
        self,
        case_folder: str,
        evidence_id: int,
        case_db_path: Path,
        case_data=None,
        parent=None,
    ):
        super().__init__(parent)
        self.case_folder = Path(case_folder)
        self.evidence_id = evidence_id
        self.case_db_path = case_db_path
        self.case_data = case_data

        # Stale data flag for lazy refresh after ingestion
        self._data_stale = False

        # Build UI
        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout with subtab widgets."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        # Create tab widget for subtabs
        self.subtabs = QTabWidget()
        self.subtabs.currentChanged.connect(self._on_subtab_changed)

        # Create the shared context (inventory subtab creates db_manager)
        self._inventory = InventorySubtab(
            SubtabContext(
                self.case_folder, self.evidence_id,
                self.case_db_path, self.case_data, None,  # db_manager set below
            ),
            parent=self,
        )
        # Use the db_manager created by BrowserInventoryModel inside InventorySubtab
        self._ctx = SubtabContext(
            self.case_folder, self.evidence_id,
            self.case_db_path, self.case_data,
            self._inventory.db_manager,
        )
        # Patch inventory's context to share the same one
        self._inventory.ctx = self._ctx

        # Create all subtab widgets
        self._history = HistorySubtab(self._ctx, parent=self)
        self._search_terms = SearchTermsSubtab(self._ctx, parent=self)
        self._cookies = CookiesSubtab(self._ctx, parent=self)
        self._bookmarks = BookmarksSubtab(self._ctx, parent=self)
        self._downloads = DownloadsSubtab(self._ctx, parent=self)
        self._autofill = AutofillContainer(self._ctx, parent=self)
        self._sessions = SessionsContainer(self._ctx, parent=self)
        self._permissions = PermissionsSubtab(self._ctx, parent=self)
        self._media = MediaSubtab(self._ctx, parent=self)
        self._site_engagement = SiteEngagementSubtab(self._ctx, parent=self)
        self._extensions = ExtensionsSubtab(self._ctx, parent=self)
        self._web_storage = WebStorageContainer(self._ctx, parent=self)
        self._cache = CacheContainer(self._ctx, parent=self)

        # Ordered list for lazy-load dispatch (index matches tab position)
        self._all_subtabs = [
            self._inventory,       # 0
            self._history,         # 1
            self._search_terms,    # 2
            self._cookies,         # 3
            self._bookmarks,       # 4
            self._downloads,       # 5
            self._autofill,        # 6
            self._sessions,        # 7
            self._permissions,     # 8
            self._media,           # 9
            self._site_engagement, # 10
            self._extensions,      # 11
            self._web_storage,     # 12
            self._cache,           # 13
        ]

        # Add tabs
        self.subtabs.addTab(self._inventory, "Inventory")
        self.subtabs.addTab(self._history, "History")
        self.subtabs.addTab(self._search_terms, "Search Terms")
        self.subtabs.addTab(self._cookies, "Cookies")
        self.subtabs.addTab(self._bookmarks, "Bookmarks")
        self.subtabs.addTab(self._downloads, "Downloads")
        self.subtabs.addTab(self._autofill, "Autofill")
        self.subtabs.addTab(self._sessions, "Restored Tabs")
        self.subtabs.addTab(self._permissions, "Permissions")
        self.subtabs.addTab(self._media, "Media")
        self.subtabs.addTab(self._site_engagement, "Site Engagement")
        self.subtabs.addTab(self._extensions, "Extensions")
        self.subtabs.addTab(self._web_storage, "Web Storage")
        self.subtabs.addTab(self._cache, "Cache")

        layout.addWidget(self.subtabs)

        # Connect cross-tab navigation from stored sites → storage keys
        if hasattr(self._web_storage, '_stored_sites'):
            self._web_storage._stored_sites.view_all_keys_requested.connect(
                self._navigate_to_storage_keys_tab
            )

    # ─── Lazy Loading ─────────────────────────────────────────────────

    def _on_subtab_changed(self, index: int) -> None:
        """Load subtab data on first visit."""
        if 0 <= index < len(self._all_subtabs):
            subtab = self._all_subtabs[index]
            if not subtab.is_loaded:
                subtab.load()

    # ─── Cross-Tab Navigation ────────────────────────────────────────

    def _navigate_to_storage_keys_tab(self, origin: str) -> None:
        """Navigate to Storage Keys tab with origin filter pre-filled."""
        storage_keys = self._web_storage.storage_keys_subtab
        storage_keys.set_origin_filter(origin)

        # Switch to Web Storage tab
        self.subtabs.setCurrentIndex(12)

        # Switch to Storage Keys nested tab
        self._web_storage.nested_tabs.setCurrentIndex(1)

    # ─── Public API (called by main.py) ──────────────────────────────

    def load_inventory(self) -> None:
        """Reload inventory data after extraction/ingestion.

        Resets lazy loading flags so subtabs refresh on next visit,
        then refreshes the currently visible subtab.
        """
        # Mark all subtabs as needing reload
        for subtab in self._all_subtabs:
            subtab.mark_needs_reload()

        # Refresh the currently visible subtab
        current_index = self.subtabs.currentIndex()
        if 0 <= current_index < len(self._all_subtabs):
            self._all_subtabs[current_index].load()

    def mark_stale(self) -> None:
        """Mark data as stale — will refresh on next showEvent."""
        self._data_stale = True

    def showEvent(self, event):
        """Override showEvent to refresh data when tab becomes visible."""
        super().showEvent(event)
        if self._data_stale:
            self._data_stale = False
            self.load_inventory()
