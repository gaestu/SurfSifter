"""Storage identifier details dialog."""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QVBoxLayout,
)


class StorageIdentifierDetailsDialog(QDialog):
    """Dialog showing full details for a storage identifier."""

    def __init__(self, row_data: dict, parent=None):
        """
        Initialize storage identifier details dialog.

        Args:
            row_data: Storage identifier data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Storage Identifier Details")
        self.setModal(True)
        self.resize(600, 450)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Identifier type header with styling
        id_type = self.row_data.get("identifier_type", "unknown")
        type_label = QLabel(f"<b>{id_type.replace('_', ' ').title()}</b>")
        type_label.setStyleSheet("font-size: 14px; padding: 4px;")
        layout.addWidget(type_label)

        # Form layout for fields
        form = QFormLayout()

        # Identifier info
        name = self.row_data.get("identifier_name") or "N/A"
        form.addRow("Identifier Name:", QLabel(name))

        value = self.row_data.get("identifier_value") or ""
        value_label = QLabel(value)
        value_label.setWordWrap(True)
        value_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow("Identifier Value:", value_label)

        form.addRow("", QLabel(""))  # Spacer

        # Source info
        origin = self.row_data.get("origin") or "N/A"
        origin_label = QLabel(origin)
        origin_label.setWordWrap(True)
        form.addRow("Origin:", origin_label)

        storage_key = self.row_data.get("storage_key") or "N/A"
        key_label = QLabel(storage_key)
        key_label.setWordWrap(True)
        form.addRow("Storage Key:", key_label)

        storage_type = self.row_data.get("storage_type") or "N/A"
        form.addRow("Storage Type:", QLabel(storage_type.replace("_", " ").title()))

        form.addRow("", QLabel(""))  # Spacer

        # Browser info
        browser = self.row_data.get("browser", "N/A")
        form.addRow("Browser:", QLabel(browser.capitalize() if browser else "N/A"))

        profile = self.row_data.get("profile") or "N/A"
        form.addRow("Profile:", QLabel(profile))

        form.addRow("", QLabel(""))  # Spacer

        # Timestamps
        first_seen = self.row_data.get("first_seen_utc") or "N/A"
        form.addRow("First Seen:", QLabel(str(first_seen)))

        last_seen = self.row_data.get("last_seen_utc") or "N/A"
        form.addRow("Last Seen:", QLabel(str(last_seen)))

        form.addRow("", QLabel(""))  # Spacer

        # Source path and partition
        source_path = self.row_data.get("source_path") or "N/A"
        source_label = QLabel(source_path)
        source_label.setWordWrap(True)
        source_label.setStyleSheet("color: gray; font-size: 10px;")
        form.addRow("Source Path:", source_label)

        partition = self.row_data.get("partition_index")
        if partition is not None:
            form.addRow("Partition:", QLabel(str(partition)))

        layout.addLayout(form)
        layout.addStretch()

        # Buttons
        button_layout = QHBoxLayout()

        copy_value_btn = QPushButton("Copy Value")
        copy_value_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(value)
        )
        button_layout.addWidget(copy_value_btn)

        copy_name_btn = QPushButton("Copy Name")
        copy_name_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(name)
        )
        button_layout.addWidget(copy_name_btn)

        button_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)
