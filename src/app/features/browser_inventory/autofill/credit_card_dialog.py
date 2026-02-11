"""Credit card details dialog."""
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


class CreditCardDetailsDialog(QDialog):
    """Dialog showing full details for a saved credit card entry."""

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Credit Card Details")
        self.setModal(True)
        self.resize(600, 520)

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()
        form.addRow("Browser:", QLabel((self.row_data.get("browser") or "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))
        form.addRow("", QLabel(""))
        form.addRow("Name On Card:", QLabel(self.row_data.get("name_on_card") or "N/A"))
        form.addRow("Nickname:", QLabel(self.row_data.get("nickname") or "N/A"))
        form.addRow("Last Four:", QLabel(self.row_data.get("card_number_last_four") or "N/A"))
        exp_month = self.row_data.get("expiration_month")
        exp_year = self.row_data.get("expiration_year")
        exp_text = f"{int(exp_month):02d}/{exp_year}" if exp_month and exp_year else "N/A"
        form.addRow("Expiration:", QLabel(exp_text))
        form.addRow("", QLabel(""))
        form.addRow("Use Count:", QLabel(str(self.row_data.get("use_count") or 0)))
        form.addRow("Last Used:", QLabel(self.row_data.get("use_date_utc") or "N/A"))
        form.addRow("Modified:", QLabel(self.row_data.get("date_modified_utc") or "N/A"))
        form.addRow("", QLabel(""))
        form.addRow("GUID:", QLabel(self.row_data.get("guid") or "N/A"))
        form.addRow("Billing Address ID:", QLabel(self.row_data.get("billing_address_id") or "N/A"))
        layout.addLayout(form)

        encrypted_blob = self.row_data.get("card_number_encrypted")
        if encrypted_blob and isinstance(encrypted_blob, (bytes, bytearray)):
            layout.addWidget(QLabel("Encrypted Card Number (Hex):"))
            hex_text = QTextEdit()
            hex_text.setReadOnly(True)
            hex_text.setMaximumHeight(100)
            hex_text.setPlainText(encrypted_blob.hex())
            hex_text.setStyleSheet("QTextEdit { font-family: monospace; font-size: 10pt; }")
            layout.addWidget(hex_text)

        button_layout = QHBoxLayout()
        copy_name_btn = QPushButton("Copy Name")
        copy_name_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(self.row_data.get("name_on_card") or "")
        )
        button_layout.addWidget(copy_name_btn)
        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)
