"""Autofill container with nested subtabs."""
from __future__ import annotations

from PySide6.QtWidgets import QLabel, QTabWidget, QVBoxLayout, QWidget

from app.features.browser_inventory._base import SubtabContext
from app.features.browser_inventory.autofill.form_data import FormDataSubtab
from app.features.browser_inventory.autofill.saved_logins import SavedLoginsSubtab
from app.features.browser_inventory.autofill.credit_cards import CreditCardsSubtab
from app.features.browser_inventory.autofill.addresses import AddressesSubtab
from app.features.browser_inventory.autofill.search_engines import SearchEnginesSubtab
from app.features.browser_inventory.autofill.deleted_history import DeletedHistorySubtab
from app.features.browser_inventory.autofill.block_list import BlockListSubtab
from app.features.browser_inventory.autofill.ibans import IbansSubtab


class AutofillContainer(QWidget):
    """Container with nested tabs for autofill artifact types."""

    def __init__(self, ctx: SubtabContext, parent=None):
        super().__init__(parent)
        self.ctx = ctx
        self._subtabs = []
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        desc_label = QLabel(
            "<b>Autofill Data:</b> Form field values and saved logins from browsers. "
            "Form Data shows individual field entries; "
            "Saved Logins shows stored website credentials; "
            "Credit Cards shows saved payment methods from Chromium browsers; "
            "Addresses shows Chromium's structured contact info (v100+); "
            "Search Engines shows configured search providers; "
            "Deleted History shows Firefox's deleted form entries; "
            "Block List shows Edge autofill exclusions; "
            "IBANs shows stored banking IBAN entries."
        )
        desc_label.setWordWrap(True)
        desc_label.setStyleSheet(
            "QLabel { padding: 4px; background-color: #f0f0f0; "
            "border-radius: 4px; margin: 2px; }"
        )
        layout.addWidget(desc_label)

        self.nested_tabs = QTabWidget()
        self.nested_tabs.currentChanged.connect(self._on_nested_tab_changed)

        self._form_data = FormDataSubtab(self.ctx, parent=self)
        self._saved_logins = SavedLoginsSubtab(self.ctx, parent=self)
        self._credit_cards = CreditCardsSubtab(self.ctx, parent=self)
        self._addresses = AddressesSubtab(self.ctx, parent=self)
        self._search_engines = SearchEnginesSubtab(self.ctx, parent=self)
        self._deleted_history = DeletedHistorySubtab(self.ctx, parent=self)
        self._block_list = BlockListSubtab(self.ctx, parent=self)
        self._ibans = IbansSubtab(self.ctx, parent=self)

        self._subtabs = [
            self._form_data, self._saved_logins, self._credit_cards,
            self._addresses, self._search_engines, self._deleted_history,
            self._block_list, self._ibans,
        ]

        self.nested_tabs.addTab(self._form_data, "Form Data")
        self.nested_tabs.addTab(self._saved_logins, "Saved Logins")
        self.nested_tabs.addTab(self._credit_cards, "Credit Cards")
        self.nested_tabs.addTab(self._addresses, "Addresses")
        self.nested_tabs.addTab(self._search_engines, "Search Engines")
        self.nested_tabs.addTab(self._deleted_history, "Deleted History")
        self.nested_tabs.addTab(self._block_list, "Block List")
        self.nested_tabs.addTab(self._ibans, "IBANs")

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
