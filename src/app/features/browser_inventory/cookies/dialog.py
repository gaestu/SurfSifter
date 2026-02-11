"""Cookie details dialog."""
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


class CookieDetailsDialog(QDialog):
    """Dialog showing full details for a cookie."""

    def __init__(self, row_data: dict, parent=None):
        """
        Initialize cookie details dialog.

        Args:
            row_data: Cookie data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Cookie Details")
        self.setModal(True)
        self.resize(550, 450)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Form layout for fields
        form = QFormLayout()

        # Basic info
        form.addRow("Domain:", QLabel(self.row_data.get("domain", "N/A")))
        form.addRow("Name:", QLabel(self.row_data.get("name", "N/A")))
        form.addRow("Path:", QLabel(self.row_data.get("path") or "/"))

        form.addRow("", QLabel(""))  # Spacer

        # Browser info
        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Security flags
        form.addRow("Secure:", QLabel("Yes" if self.row_data.get("is_secure") else "No"))
        form.addRow("HttpOnly:", QLabel("Yes" if self.row_data.get("is_httponly") else "No"))
        form.addRow("SameSite:", QLabel(self.row_data.get("samesite") or "unset"))
        form.addRow("Encrypted:", QLabel("Yes" if self.row_data.get("encrypted") else "No"))

        form.addRow("", QLabel(""))  # Spacer

        # Timestamps
        form.addRow("Created:", QLabel(self.row_data.get("creation_utc") or "N/A"))
        form.addRow("Last Access:", QLabel(self.row_data.get("last_access_utc") or "N/A"))
        form.addRow("Expires:", QLabel(self.row_data.get("expires_utc") or "Session"))

        layout.addLayout(form)

        # Value section
        layout.addWidget(QLabel("Value:"))
        value_text = QTextEdit()
        value_text.setReadOnly(True)
        value_text.setMaximumHeight(80)
        value = self.row_data.get("value", "")
        if self.row_data.get("encrypted") and not value:
            value_text.setPlainText("[Encrypted - no plaintext value]")
        else:
            value_text.setPlainText(value)
        layout.addWidget(value_text)

        # Encrypted value section (if encrypted)
        encrypted_value = self.row_data.get("encrypted_value")
        if encrypted_value and isinstance(encrypted_value, bytes):
            layout.addWidget(QLabel("Encrypted Value (hex):"))
            enc_text = QTextEdit()
            enc_text.setReadOnly(True)
            enc_text.setMaximumHeight(80)
            enc_text.setPlainText(encrypted_value.hex())
            layout.addWidget(enc_text)

        # Buttons
        button_layout = QHBoxLayout()

        copy_value_btn = QPushButton("Copy Value")
        copy_value_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(self.row_data.get("value", ""))
        )
        button_layout.addWidget(copy_value_btn)

        button_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)
