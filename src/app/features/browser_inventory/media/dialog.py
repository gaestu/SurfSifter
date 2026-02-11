"""Media playback details dialog."""
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

from .model import MediaHistoryTableModel


class MediaDetailsDialog(QDialog):
    """Dialog showing full details for a media playback record."""

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Media Playback Details")
        self.setModal(True)
        self.resize(550, 400)

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()

        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))
        form.addRow("", QLabel(""))

        seconds = self.row_data.get("watch_time_seconds") or 0
        time_str = MediaHistoryTableModel.format_watch_time(seconds)
        form.addRow("Watch Time:", QLabel(f"{time_str} ({seconds:,} seconds)"))
        form.addRow("Has Video:", QLabel("Yes" if self.row_data.get("has_video") else "No"))
        form.addRow("Has Audio:", QLabel("Yes" if self.row_data.get("has_audio") else "No"))
        form.addRow("", QLabel(""))
        form.addRow("Last Updated:", QLabel(self.row_data.get("last_updated") or "N/A"))
        form.addRow("Origin ID:", QLabel(str(self.row_data.get("origin_id") or "N/A")))

        layout.addLayout(form)

        layout.addWidget(QLabel("URL:"))
        url_text = QTextEdit()
        url_text.setReadOnly(True)
        url_text.setMaximumHeight(80)
        url_text.setPlainText(self.row_data.get("url", ""))
        layout.addWidget(url_text)

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
