"""Browser download details dialog."""
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

from .model import BrowserDownloadsTableModel


class BrowserDownloadDetailsDialog(QDialog):
    """Dialog showing full details for a browser download."""

    def __init__(self, row_data: dict, parent=None):
        """
        Initialize browser download details dialog.

        Args:
            row_data: Download data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Browser Download Details")
        self.setModal(True)
        self.resize(600, 500)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Form layout for fields
        form = QFormLayout()

        # File info
        form.addRow("Filename:", QLabel(self.row_data.get("filename") or "N/A"))
        form.addRow("Target Path:", QLabel(self.row_data.get("target_path") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Browser info
        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # State info
        state = self.row_data.get("state") or ""
        form.addRow("State:", QLabel(state.replace("_", " ").title()))

        danger = self.row_data.get("danger_type") or "not_dangerous"
        danger_explanations = {
            "not_dangerous": "No known risk",
            "dangerous_file": "File type is potentially dangerous",
            "dangerous_url": "URL is known to be dangerous",
            "dangerous_content": "Content flagged as dangerous",
            "uncommon_content": "Content is uncommon/suspicious",
            "user_validated": "User validated as safe",
        }
        danger_display = danger.replace("_", " ").title()
        danger_explain = danger_explanations.get(danger, "")
        form.addRow("Danger Type:", QLabel(f"{danger_display} ({danger_explain})"))

        form.addRow("Opened:", QLabel("Yes" if self.row_data.get("opened") else "No"))

        form.addRow("", QLabel(""))  # Spacer

        # Size info
        total_bytes = self.row_data.get("total_bytes") or 0
        received_bytes = self.row_data.get("received_bytes") or 0
        form.addRow("Total Size:", QLabel(BrowserDownloadsTableModel.format_bytes(total_bytes)))
        form.addRow("Received:", QLabel(BrowserDownloadsTableModel.format_bytes(received_bytes)))
        form.addRow("MIME Type:", QLabel(self.row_data.get("mime_type") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Timestamps
        form.addRow("Start Time:", QLabel(self.row_data.get("start_time_utc") or "N/A"))
        form.addRow("End Time:", QLabel(self.row_data.get("end_time_utc") or "N/A"))

        layout.addLayout(form)

        # URL section
        layout.addWidget(QLabel("URL:"))
        url_text = QTextEdit()
        url_text.setReadOnly(True)
        url_text.setMaximumHeight(60)
        url_text.setPlainText(self.row_data.get("url", ""))
        layout.addWidget(url_text)

        # Referrer section
        referrer = self.row_data.get("referrer")
        if referrer:
            layout.addWidget(QLabel("Referrer:"))
            ref_text = QTextEdit()
            ref_text.setReadOnly(True)
            ref_text.setMaximumHeight(40)
            ref_text.setPlainText(referrer)
            layout.addWidget(ref_text)

        # Buttons
        button_layout = QHBoxLayout()

        url = self.row_data.get("url", "")

        copy_btn = QPushButton("Copy URL")
        copy_btn.setEnabled(bool(url))
        copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(url))
        button_layout.addWidget(copy_btn)

        button_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)
