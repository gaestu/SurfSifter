"""Closed tab details dialog."""
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


class ClosedTabDetailsDialog(QDialog):
    """Dialog showing full details for a recently closed tab."""

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Closed Tab Details")
        self.setModal(True)
        self.resize(600, 400)
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()
        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))
        form.addRow("", QLabel(""))

        closed_at = self.row_data.get("closed_at_utc") or "N/A"
        form.addRow("Closed At:", QLabel(closed_at))

        window_id = self.row_data.get("original_window_id")
        form.addRow("Window ID:", QLabel("N/A" if window_id is None else str(window_id)))

        tab_index = self.row_data.get("original_tab_index")
        form.addRow("Tab Index:", QLabel("N/A" if tab_index is None else str(tab_index)))

        form.addRow("", QLabel(""))

        source_path = self.row_data.get("source_path") or self.row_data.get("logical_path")
        if source_path:
            form.addRow("Source File:", QLabel(source_path))

        run_id = self.row_data.get("run_id")
        if run_id:
            form.addRow("Run ID:", QLabel(run_id))

        layout.addLayout(form)

        # Title
        layout.addWidget(QLabel("Title:"))
        title_text = QTextEdit()
        title_text.setReadOnly(True)
        title_text.setMaximumHeight(40)
        title_text.setPlainText(self.row_data.get("title", ""))
        layout.addWidget(title_text)

        # URL
        layout.addWidget(QLabel("URL:"))
        url_text = QTextEdit()
        url_text.setReadOnly(True)
        url_text.setMaximumHeight(60)
        url_text.setPlainText(self.row_data.get("url", ""))
        layout.addWidget(url_text)

        layout.addStretch()

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
