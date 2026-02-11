"""Browser history details dialog."""
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


class HistoryDetailsDialog(QDialog):
    """Dialog showing full details for a browser history record."""

    def __init__(self, row_data: dict, parent=None):
        """
        Initialize history details dialog.

        Args:
            row_data: History record data dictionary (from BrowserHistoryTableModel)
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("History Details")
        self.setModal(True)
        self.resize(600, 500)

        self._setup_ui()

    def _format_duration(self, duration_ms) -> str:
        """Format duration in milliseconds to human-readable format."""
        if duration_ms is None or duration_ms <= 0:
            return "N/A"
        seconds = duration_ms // 1000
        if seconds < 60:
            return f"{seconds}s"
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        if minutes < 60:
            return f"{minutes}m {remaining_seconds}s"
        hours = minutes // 60
        remaining_minutes = minutes % 60
        return f"{hours}h {remaining_minutes}m"

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Form layout for fields
        form = QFormLayout()

        # Basic info
        form.addRow("Title:", QLabel(self.row_data.get("title") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Browser info
        browser = self.row_data.get("browser", "N/A")
        form.addRow("Browser:", QLabel(browser.capitalize() if browser else "N/A"))
        form.addRow("Profile:", QLabel(str(self.row_data.get("profile") or "N/A")))

        form.addRow("", QLabel(""))  # Spacer

        # Visit info - ts_utc is the column name in browser_history table
        visit_time = self.row_data.get("ts_utc") or self.row_data.get("visit_time") or "N/A"
        form.addRow("Visit Time:", QLabel(str(visit_time)))
        form.addRow("Visit Count:", QLabel(str(self.row_data.get("visit_count") or "None")))
        form.addRow("Typed Count:", QLabel(str(self.row_data.get("typed_count") or "None")))

        # Visit type uses transition_type column (Chromium) or visit_type_label enriched by model
        visit_type_label = self.row_data.get("visit_type_label")
        if not visit_type_label:
            # Fallback: compute from transition_type if present
            transition = self.row_data.get("transition_type")
            if transition is not None:
                from .model import get_transition_label
                visit_type_label = get_transition_label(transition)
        form.addRow("Visit Type:", QLabel(visit_type_label or "N/A"))

        # Duration stores visit_duration_ms
        duration_ms = self.row_data.get("visit_duration_ms")
        duration_str = self._format_duration(duration_ms)
        form.addRow("Duration:", QLabel(duration_str))

        # Hidden flag stores hidden as integer column
        hidden = self.row_data.get("hidden")
        if hidden is not None:
            hidden_str = "Yes" if hidden else "No"
        else:
            hidden_str = "N/A"
        form.addRow("Hidden:", QLabel(hidden_str))

        # From visit (navigation chain) stores from_visit column
        from_visit = self.row_data.get("from_visit")
        if from_visit is not None and from_visit > 0:
            form.addRow("From Visit ID:", QLabel(str(from_visit)))

        form.addRow("", QLabel(""))  # Spacer

        # Source info
        form.addRow("Source Path:", QLabel(self.row_data.get("source_path") or "N/A"))
        form.addRow("Run ID:", QLabel(self.row_data.get("run_id") or "N/A"))

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

        copy_url_btn = QPushButton("Copy URL")
        copy_url_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(self.row_data.get("url", ""))
        )
        button_layout.addWidget(copy_url_btn)

        open_url_btn = QPushButton("Open URL")
        open_url_btn.clicked.connect(
            lambda: QDesktopServices.openUrl(QUrl(self.row_data.get("url", "")))
        )
        button_layout.addWidget(open_url_btn)

        button_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)
