"""Sessions container with nested subtabs for Open Tabs and Form Data."""
from __future__ import annotations

from PySide6.QtWidgets import QLabel, QTabWidget, QVBoxLayout, QWidget

from app.features.browser_inventory._base import SubtabContext
from app.features.browser_inventory.sessions.open_tabs import OpenTabsSubtab
from app.features.browser_inventory.sessions.form_data import SessionFormDataSubtab


class SessionsContainer(QWidget):
    """Container with nested tabs for session artifact types."""

    def __init__(self, ctx: SubtabContext, parent=None):
        super().__init__(parent)
        self.ctx = ctx
        self._subtabs = []
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        desc_label = QLabel(
            "<b>Browser session restore data:</b> Open Tabs shows tabs that were open "
            "when the browser was last closed. Form Data shows user-entered form field "
            "values captured in Firefox session files (forensically valuable for search "
            "queries, partial entries, and user activity)."
        )
        desc_label.setWordWrap(True)
        desc_label.setStyleSheet(
            "QLabel { padding: 4px; background-color: #f0f0f0; "
            "border-radius: 4px; margin: 2px; }"
        )
        layout.addWidget(desc_label)

        self.nested_tabs = QTabWidget()
        self.nested_tabs.currentChanged.connect(self._on_nested_tab_changed)

        self._open_tabs = OpenTabsSubtab(self.ctx, parent=self)
        self._form_data = SessionFormDataSubtab(self.ctx, parent=self)

        self._subtabs = [self._open_tabs, self._form_data]

        self.nested_tabs.addTab(self._open_tabs, "Open Tabs")
        self.nested_tabs.addTab(self._form_data, "Form Data")

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
