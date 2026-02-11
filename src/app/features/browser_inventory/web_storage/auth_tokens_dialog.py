"""Storage token details dialog."""
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


class StorageTokenDetailsDialog(QDialog):
    """Dialog showing full details for a storage token."""

    def __init__(self, row_data: dict, parent=None):
        """
        Initialize storage token details dialog.

        Args:
            row_data: Token data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Storage Token Details")
        self.setModal(True)
        self.resize(600, 500)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Form layout for fields
        form = QFormLayout()

        # Token type and risk
        token_type = (self.row_data.get("token_type") or "unknown").replace("_", " ").title()
        risk = (self.row_data.get("risk_level") or "medium").upper()
        form.addRow("Token Type:", QLabel(token_type))
        form.addRow("Risk Level:", QLabel(risk))

        form.addRow("", QLabel(""))  # Spacer

        # Origin and key
        form.addRow("Origin:", QLabel(self.row_data.get("origin") or "N/A"))
        form.addRow("Storage Key:", QLabel(self.row_data.get("storage_key") or "N/A"))
        form.addRow("Storage Type:", QLabel(self.row_data.get("storage_type") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Browser info
        form.addRow("Browser:", QLabel((self.row_data.get("browser") or "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # JWT-specific fields
        issuer = self.row_data.get("issuer")
        subject = self.row_data.get("subject")
        if issuer:
            form.addRow("Issuer (iss):", QLabel(issuer))
        if subject:
            form.addRow("Subject (sub):", QLabel(subject))

        # Associated email
        email = self.row_data.get("associated_email")
        if email:
            form.addRow("Email:", QLabel(email))

        form.addRow("", QLabel(""))  # Spacer

        # Expiration
        form.addRow("Expires:", QLabel(self.row_data.get("expires_at_utc") or "Unknown"))
        form.addRow("Expired:", QLabel("Yes" if self.row_data.get("is_expired") else "No"))

        form.addRow("", QLabel(""))  # Spacer

        # Timestamps
        form.addRow("Created:", QLabel(self.row_data.get("created_at") or "N/A"))

        layout.addLayout(form)

        # Token value section
        layout.addWidget(QLabel("Token Value:"))
        token_text = QTextEdit()
        token_text.setReadOnly(True)
        token_text.setMaximumHeight(100)
        token_value = self.row_data.get("token_value", "")
        token_text.setPlainText(token_value)
        layout.addWidget(token_text)

        # Hash
        token_hash = self.row_data.get("token_hash")
        if token_hash:
            layout.addWidget(QLabel(f"Token Hash (SHA256): {token_hash}"))

        # Buttons
        button_layout = QHBoxLayout()

        copy_btn = QPushButton("Copy Token")
        copy_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(token_value)
        )
        button_layout.addWidget(copy_btn)

        copy_origin_btn = QPushButton("Copy Origin")
        copy_origin_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(self.row_data.get("origin", ""))
        )
        button_layout.addWidget(copy_origin_btn)

        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)
