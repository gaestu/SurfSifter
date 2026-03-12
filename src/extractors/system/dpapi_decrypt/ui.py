"""
DPAPI Decrypt extractor UI components.
"""

from __future__ import annotations

from typing import Optional

from PySide6.QtWidgets import (
    QCheckBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QVBoxLayout,
    QWidget,
)

from core.logging import get_logger

LOGGER = get_logger("extractors.system.dpapi_decrypt.ui")


class DPAPIDecryptConfigWidget(QWidget):
    """Configuration widget for DPAPI decryption."""

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        # Header
        header = QLabel("<b>DPAPI Decrypt Configuration</b>")
        layout.addWidget(header)

        # Auto-SAM checkbox
        self._auto_sam = QCheckBox(
            "Auto-attempt unlock using SAM hashes (recommended)"
        )
        self._auto_sam.setChecked(True)
        layout.addWidget(self._auto_sam)

        # User credentials group
        creds_group = QGroupBox("User Credentials (Optional)")
        creds_layout = QVBoxLayout(creds_group)

        creds_info = QLabel(
            "If you know a Windows user password, enter it below.\n"
            "Most master keys can be unlocked with recovered SAM hashes."
        )
        creds_info.setWordWrap(True)
        creds_info.setStyleSheet("color: gray; font-size: 9pt;")
        creds_layout.addWidget(creds_info)

        pw_layout = QHBoxLayout()
        pw_layout.addWidget(QLabel("Password:"))
        self._password_input = QLineEdit()
        self._password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self._password_input.setPlaceholderText("Enter known Windows password")
        pw_layout.addWidget(self._password_input)
        creds_layout.addLayout(pw_layout)

        layout.addWidget(creds_group)

        # Decryption targets
        targets_group = QGroupBox("Decryption Targets")
        targets_layout = QVBoxLayout(targets_group)

        self._decrypt_passwords = QCheckBox("Decrypt saved passwords (Login Data)")
        self._decrypt_passwords.setChecked(True)
        targets_layout.addWidget(self._decrypt_passwords)

        self._decrypt_cookies = QCheckBox("Decrypt cookies")
        self._decrypt_cookies.setChecked(True)
        targets_layout.addWidget(self._decrypt_cookies)

        self._decrypt_cards = QCheckBox("Decrypt credit card numbers")
        self._decrypt_cards.setChecked(True)
        targets_layout.addWidget(self._decrypt_cards)

        layout.addWidget(targets_group)

        # Mask secrets checkbox
        self._mask_secrets = QCheckBox("Mask decrypted secrets in UI (recommended)")
        self._mask_secrets.setChecked(True)
        layout.addWidget(self._mask_secrets)

        layout.addStretch()

    def get_config(self) -> dict:
        """Return configuration dict from widget state."""
        config: dict = {
            "auto_sam": self._auto_sam.isChecked(),
            "decrypt_passwords": self._decrypt_passwords.isChecked(),
            "decrypt_cookies": self._decrypt_cookies.isChecked(),
            "decrypt_cards": self._decrypt_cards.isChecked(),
            "mask_secrets": self._mask_secrets.isChecked(),
        }
        password = self._password_input.text().strip()
        if password:
            config["user_password"] = password
        return config
