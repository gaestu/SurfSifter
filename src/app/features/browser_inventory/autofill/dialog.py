"""Autofill form data details dialog."""
from __future__ import annotations

import re

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
)


class AutofillDetailsDialog(QDialog):
    """Dialog showing full details for an autofill entry.

    Fixed field name mappings (DB uses 'name', 'count', 'date_created_utc', 'date_last_used_utc').
    Added field_id_hash display for Edge autofill forensic traceability.
    """

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Autofill Details")
        self.setModal(True)
        self.resize(550, 420)

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()

        # DB uses 'name' column, not 'field_name'
        field_name = self.row_data.get("name") or "N/A"
        form.addRow("Field Name:", QLabel(field_name))

        browser = self.row_data.get("browser") or "N/A"
        form.addRow("Browser:", QLabel(browser.capitalize() if browser != "N/A" else browser))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))

        form.addRow("", QLabel(""))

        # DB uses 'count' column, not 'use_count'
        use_count = self.row_data.get("count") or 0
        form.addRow("Use Count:", QLabel(str(use_count)))

        # DB uses 'date_created_utc', not 'first_used_utc'
        first_used = self.row_data.get("date_created_utc") or "N/A"
        form.addRow("First Used:", QLabel(first_used))

        # DB uses 'date_last_used_utc', not 'last_used_utc'
        last_used = self.row_data.get("date_last_used_utc") or "N/A"
        form.addRow("Last Used:", QLabel(last_used))

        # Extract and show domain from notes (Edge autofill context)
        notes = self.row_data.get("notes") or ""
        domain = self._extract_domain(notes)
        if domain:
            form.addRow("", QLabel(""))
            domain_label = QLabel(domain)
            domain_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
            form.addRow("Domain:", domain_label)

        # Show field_id_hash for Edge autofill forensic traceability
        field_id_hash = self.row_data.get("field_id_hash")
        if field_id_hash:
            form.addRow("", QLabel(""))
            hash_label = QLabel(field_id_hash)
            hash_label.setWordWrap(True)
            hash_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
            hash_label.setStyleSheet("QLabel { font-family: monospace; font-size: 9pt; color: #666; }")
            form.addRow("Field ID Hash:", hash_label)

        # Show source info from notes if present
        source = self._extract_source(notes)
        if source:
            source_label = QLabel(source)
            source_label.setStyleSheet("QLabel { color: #666; font-size: 9pt; }")
            form.addRow("Source:", source_label)

        layout.addLayout(form)

        layout.addWidget(QLabel("Value:"))
        value_text = QTextEdit()
        value_text.setReadOnly(True)
        value_text.setMaximumHeight(80)
        value_text.setPlainText(self.row_data.get("value", ""))
        layout.addWidget(value_text)

        button_layout = QHBoxLayout()
        copy_btn = QPushButton("Copy Value")
        copy_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(self.row_data.get("value", ""))
        )
        button_layout.addWidget(copy_btn)
        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)

    def _extract_domain(self, notes: str) -> str:
        """Extract domain from notes field."""
        match = re.search(r'domain:([^;]+)', notes)
        return match.group(1).strip() if match else ""

    def _extract_source(self, notes: str) -> str:
        """Extract source table from notes field."""
        match = re.search(r'source:([^;]+)', notes)
        return match.group(1).strip() if match else ""
