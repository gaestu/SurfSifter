"""IBANs subtab widget."""
from __future__ import annotations

import logging

from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QTextEdit,
    QVBoxLayout,
)

from app.features.browser_inventory._base import BaseArtifactSubtab, SubtabContext
from .ibans_model import AutofillIbansTableModel

logger = logging.getLogger(__name__)


class IbansSubtab(BaseArtifactSubtab):
    """Stored IBAN banking entries with browser/source/term filtering."""

    def _default_status_text(self):
        return "0 IBAN entries"

    def _setup_filters(self, fl: QHBoxLayout):
        self.browser_filter = self._add_browser_filter(fl)

        fl.addWidget(QLabel("Source:"))
        self.source_filter = QComboBox()
        self.source_filter.addItem("All", "")
        self.source_filter.addItem("Local", "local_ibans")
        self.source_filter.addItem("Masked", "masked_ibans")
        fl.addWidget(self.source_filter)

        fl.addWidget(QLabel("Term:"))
        self.term_filter = QLineEdit()
        self.term_filter.setPlaceholderText("Filter by nickname/prefix/suffix...")
        self.term_filter.setMaximumWidth(260)
        fl.addWidget(self.term_filter)

    def _create_model(self):
        return AutofillIbansTableModel(
            self.ctx.db_manager,
            self.ctx.evidence_id,
            self.ctx.get_evidence_label(),
            case_data=self.ctx.case_data,
            parent=self,
        )

    def _configure_table(self):
        t = self.table
        t.setColumnWidth(0, 110)   # Source
        t.setColumnWidth(1, 120)   # Nickname
        t.setColumnWidth(2, 200)   # Value
        t.setColumnWidth(3, 90)    # Prefix
        t.setColumnWidth(4, 90)    # Suffix
        t.setColumnWidth(5, 80)    # Browser
        t.setColumnWidth(6, 80)    # Profile
        t.setColumnWidth(7, 140)   # Last Used
        t.setColumnWidth(8, 140)   # Tags

    def _populate_filter_options(self):
        for b in self._model.get_available_browsers():
            self.browser_filter.addItem(b.capitalize(), b)

    def _apply_filters(self):
        if self._model is None:
            return
        browser = self.browser_filter.currentData() or ""
        source = self.source_filter.currentData() or ""
        term = self.term_filter.text().strip()
        self._model.load(browser_filter=browser, source_filter=source, term_filter=term)
        self._update_status()

    def _update_status(self):
        if self._model is None:
            return
        count = self._model.rowCount()
        self.status_label.setText(f"{count} IBAN entries")

    def _artifact_type_for_tagging(self):
        return "autofill_iban"

    def _view_details(self, index):
        if self._model is None:
            return
        row_data = self._get_row_data(index)
        if not row_data:
            return

        display_names = {
            "source_table": "Source Table",
            "nickname": "Nickname",
            "value": "IBAN Value",
            "prefix": "Prefix",
            "suffix": "Suffix",
            "length": "Length",
            "use_count": "Use Count",
            "use_date_utc": "Last Used (UTC)",
            "date_modified_utc": "Date Modified (UTC)",
            "guid": "GUID",
            "instrument_id": "Instrument ID",
            "browser": "Browser",
            "profile": "Profile",
            "source_path": "Source Path",
            "logical_path": "Logical Path",
            "forensic_path": "Forensic Path",
            "run_id": "Run ID",
            "discovered_by": "Discovered By",
            "partition_index": "Partition Index",
            "fs_type": "Filesystem Type",
            "tags": "Tags",
            "notes": "Notes",
            "created_at_utc": "Created At (UTC)",
        }

        dialog = QDialog(self)
        dialog.setWindowTitle("Autofill IBAN Entry")
        dialog.setMinimumSize(520, 360)
        layout = QVBoxLayout(dialog)
        text = QTextEdit()
        text.setReadOnly(True)
        details = []
        for key, value in row_data.items():
            if value is not None:
                label = display_names.get(key, key)
                details.append(f"<b>{label}:</b> {value}")
        text.setHtml("<br>".join(details))
        layout.addWidget(text)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok)
        buttons.accepted.connect(dialog.accept)
        layout.addWidget(buttons)
        dialog.exec()

    def _build_context_menu(self, menu: QMenu, index, row_data: dict):
        view_action = menu.addAction("View Details")
        view_action.triggered.connect(lambda: self._view_details(index))

        menu.addSeparator()

        tag_action = menu.addAction("🏷️ Tag Selected…")
        tag_action.triggered.connect(self._tag_selected)
