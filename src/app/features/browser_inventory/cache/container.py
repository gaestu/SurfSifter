"""Cache container with nested tabs for cache entries and Firefox cache index."""
from __future__ import annotations

from PySide6.QtWidgets import QLabel, QTabWidget, QVBoxLayout, QWidget

from app.features.browser_inventory._base import SubtabContext
from app.features.browser_inventory.cache.widget import CacheSubtab
from app.features.browser_inventory.cache.index_widget import CacheIndexSubtab


class CacheContainer(QWidget):
    """Container with nested tabs for cache data views."""

    def __init__(self, ctx: SubtabContext, parent=None):
        super().__init__(parent)
        self.ctx = ctx
        self._subtabs = []
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        desc_label = QLabel(
            "<b>Browser cache data:</b> Entries shows parsed URLs from "
            "cached content. Index (Firefox) shows the binary cache index "
            "metadata — including entries whose content has been evicted or "
            "deleted, which can prove site visits even without cached files."
        )
        desc_label.setWordWrap(True)
        desc_label.setStyleSheet(
            "QLabel { padding: 4px; background-color: #f0f0f0; "
            "border-radius: 4px; margin: 2px; }"
        )
        layout.addWidget(desc_label)

        self.nested_tabs = QTabWidget()
        self.nested_tabs.currentChanged.connect(self._on_nested_tab_changed)

        self._entries = CacheSubtab(self.ctx, parent=self)
        self._index = CacheIndexSubtab(self.ctx, parent=self)
        self._subtabs = [self._entries, self._index]

        self.nested_tabs.addTab(self._entries, "Entries")
        self.nested_tabs.addTab(self._index, "Index (Firefox)")

        layout.addWidget(self.nested_tabs)

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
