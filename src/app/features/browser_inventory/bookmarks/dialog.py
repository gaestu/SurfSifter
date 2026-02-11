"""Bookmark details dialog."""
from __future__ import annotations

from PySide6.QtCore import QUrl
from PySide6.QtGui import QDesktopServices
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


class BookmarkDetailsDialog(QDialog):
    """Dialog showing full details for a bookmark."""

    def __init__(self, row_data: dict, parent=None):
        """
        Initialize bookmark details dialog.

        Args:
            row_data: Bookmark data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Bookmark Details")
        self.setModal(True)
        self.resize(550, 400)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Form layout for fields
        form = QFormLayout()

        # Basic info
        form.addRow("Title:", QLabel(self.row_data.get("title") or "N/A"))
        form.addRow("Folder:", QLabel(self.row_data.get("folder_path") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Browser info
        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Timestamps
        form.addRow("Date Added:", QLabel(self.row_data.get("date_added_utc") or "N/A"))
        form.addRow("Date Modified:", QLabel(self.row_data.get("date_modified_utc") or "N/A"))

        layout.addLayout(form)

        # URL section
        layout.addWidget(QLabel("URL:"))
        url_text = QTextEdit()
        url_text.setReadOnly(True)
        url_text.setMaximumHeight(60)
        url_text.setPlainText(self.row_data.get("url", ""))
        layout.addWidget(url_text)

        # Buttons
        button_layout = QHBoxLayout()

        url = self.row_data.get("url", "")

        open_btn = QPushButton("Open URL")
        open_btn.setEnabled(bool(url))
        open_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(url)))
        button_layout.addWidget(open_btn)

        copy_btn = QPushButton("Copy URL")
        copy_btn.setEnabled(bool(url))
        copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(url))
        button_layout.addWidget(copy_btn)

        button_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)
