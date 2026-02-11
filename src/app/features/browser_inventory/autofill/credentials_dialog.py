"""Credentials details dialog."""
from __future__ import annotations

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


class CredentialsDetailsDialog(QDialog):
    """Dialog showing full details for a saved credential.

    Fixed bug checking 'encrypted' instead of 'password_value_encrypted'.
    Added display of new security fields (is_insecure, is_breached, password_notes).
    Shows full hex representation of encrypted password blob.
    """

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Credential Details")
        self.setModal(True)
        self.resize(600, 550)

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()

        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))
        form.addRow("", QLabel(""))
        form.addRow("Username Field:", QLabel(self.row_data.get("username_element") or "N/A"))
        form.addRow("Username:", QLabel(self.row_data.get("username_value") or "N/A"))
        form.addRow("Password Field:", QLabel(self.row_data.get("password_element") or "N/A"))

        # Fix: Check password_value_encrypted (BLOB) not 'encrypted'
        encrypted_blob = self.row_data.get("password_value_encrypted")
        has_encrypted = encrypted_blob is not None and len(encrypted_blob) > 0 if isinstance(encrypted_blob, (bytes, bytearray)) else bool(encrypted_blob)
        form.addRow("Has Password:", QLabel("Yes" if has_encrypted else "No"))

        # Security indicators
        form.addRow("", QLabel(""))
        is_insecure = self.row_data.get("is_insecure")
        is_breached = self.row_data.get("is_breached")
        if is_insecure is not None or is_breached is not None:
            form.addRow("<b>Security Status:</b>", QLabel(""))
            if is_insecure:
                label = QLabel("⚠️ INSECURE (reused or weak password)")
                label.setStyleSheet("QLabel { color: orange; }")
                form.addRow("Insecure:", label)
            if is_breached:
                label = QLabel("🚨 BREACHED (found in data breach)")
                label.setStyleSheet("QLabel { color: red; }")
                form.addRow("Breached:", label)

        form.addRow("", QLabel(""))
        form.addRow("Created:", QLabel(self.row_data.get("date_created_utc") or "N/A"))
        form.addRow("Last Used:", QLabel(self.row_data.get("date_last_used_utc") or "N/A"))
        form.addRow("Times Used:", QLabel(str(self.row_data.get("times_used") or 0)))

        layout.addLayout(form)

        layout.addWidget(QLabel("Origin URL:"))
        url_text = QTextEdit()
        url_text.setReadOnly(True)
        url_text.setMaximumHeight(60)
        url_text.setPlainText(self.row_data.get("origin_url", ""))
        layout.addWidget(url_text)

        # Password notes if present
        password_notes = self.row_data.get("password_notes")
        if password_notes:
            layout.addWidget(QLabel("Password Notes:"))
            notes_text = QTextEdit()
            notes_text.setReadOnly(True)
            notes_text.setMaximumHeight(60)
            notes_text.setPlainText(password_notes)
            layout.addWidget(notes_text)

        # Encrypted password hex - show full hex in scrollable area
        if has_encrypted and isinstance(encrypted_blob, (bytes, bytearray)):
            layout.addWidget(QLabel("Encrypted Password (Hex):"))
            hex_text = QTextEdit()
            hex_text.setReadOnly(True)
            hex_text.setMaximumHeight(80)
            hex_text.setPlainText(encrypted_blob.hex())
            hex_text.setStyleSheet("QTextEdit { font-family: monospace; font-size: 10pt; }")
            layout.addWidget(hex_text)

        button_layout = QHBoxLayout()
        url = self.row_data.get("origin_url", "")
        copy_btn = QPushButton("Copy URL")
        copy_btn.setEnabled(bool(url))
        copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(url))
        button_layout.addWidget(copy_btn)
        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)
