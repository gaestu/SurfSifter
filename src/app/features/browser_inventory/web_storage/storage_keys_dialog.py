"""Storage key details dialog."""
from __future__ import annotations

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


class StorageKeyDetailsDialog(QDialog):
    """Dialog showing full details for a storage key-value pair."""

    def __init__(self, row_data: dict, parent=None):
        """
        Initialize storage key details dialog.

        Args:
            row_data: Storage key data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Storage Key Details")
        self.setModal(True)
        self.resize(650, 500)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Origin header
        origin = self.row_data.get("origin", "Unknown")
        origin_label = QLabel(f"<b>{origin}</b>")
        origin_label.setWordWrap(True)
        layout.addWidget(origin_label)

        layout.addWidget(QLabel(""))  # Spacer

        # Form layout for metadata
        form = QFormLayout()

        # Storage type
        storage_type = self.row_data.get("storage_type", "unknown")
        type_label = QLabel(storage_type.capitalize())
        if storage_type == "session":
            type_label.setStyleSheet("color: orange; font-weight: bold;")
        else:
            type_label.setStyleSheet("color: blue; font-weight: bold;")
        form.addRow("Storage Type:", type_label)

        # Key name
        key = self.row_data.get("key", "")
        key_label = QLabel(key)
        key_label.setWordWrap(True)
        key_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow("Key:", key_label)

        form.addRow("", QLabel(""))  # Spacer

        # Browser info
        browser = self.row_data.get("browser", "unknown")
        form.addRow("Browser:", QLabel(browser.capitalize()))

        profile = self.row_data.get("profile") or "Default"
        form.addRow("Profile:", QLabel(profile))

        form.addRow("", QLabel(""))  # Spacer

        # Value size
        value = self.row_data.get("value") or ""
        value_size = self.row_data.get("value_size") or len(value)
        form.addRow("Value Size:", QLabel(f"{value_size:,} bytes"))

        value_type = self.row_data.get("value_type") or "string"
        form.addRow("Value Type:", QLabel(value_type))

        form.addRow("", QLabel(""))  # Spacer

        # Forensic provenance
        source_path = self.row_data.get("source_path") or "N/A"
        source_label = QLabel(source_path)
        source_label.setWordWrap(True)
        source_label.setStyleSheet("color: gray; font-size: 10px;")
        form.addRow("Source Path:", source_label)

        layout.addLayout(form)

        # Value section with scrollable text area
        layout.addWidget(QLabel("Value:"))

        value_text = QTextEdit()
        value_text.setReadOnly(True)
        value_text.setPlainText(value)
        value_text.setMinimumHeight(150)

        # Try to format as JSON if possible
        if value.startswith("{") or value.startswith("["):
            try:
                import json
                parsed = json.loads(value)
                formatted = json.dumps(parsed, indent=2)
                value_text.setPlainText(formatted)
            except (json.JSONDecodeError, TypeError):
                pass  # Keep original value

        layout.addWidget(value_text)

        # Buttons
        button_layout = QHBoxLayout()

        copy_key_btn = QPushButton("Copy Key")
        copy_key_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(key)
        )
        button_layout.addWidget(copy_key_btn)

        copy_value_btn = QPushButton("Copy Value")
        copy_value_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(value)
        )
        button_layout.addWidget(copy_value_btn)

        copy_all_btn = QPushButton("Copy Key=Value")
        copy_all_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(f"{key}={value}")
        )
        button_layout.addWidget(copy_all_btn)

        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)
