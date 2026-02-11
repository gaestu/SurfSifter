"""Cache entry details dialog."""
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
    QVBoxLayout,
)


class CacheEntryDetailsDialog(QDialog):
    """Dialog showing full details for a cache entry."""

    def __init__(self, row_data: dict, parent=None):
        """
        Initialize cache entry details dialog.

        Args:
            row_data: Cache entry data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Cache Entry Details")
        self.setModal(True)
        self.resize(700, 500)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # URL header (truncated with tooltip)
        url = self.row_data.get("url", "Unknown")
        url_display = url[:80] + "..." if len(url) > 80 else url
        url_label = QLabel(f"<b>{url_display}</b>")
        url_label.setWordWrap(True)
        url_label.setToolTip(url)
        layout.addWidget(url_label)

        layout.addWidget(QLabel(""))  # Spacer

        # Form layout for metadata
        form = QFormLayout()

        # Basic info
        domain = self.row_data.get("domain", "N/A")
        browser = self.row_data.get("browser", "unknown")
        form.addRow("Domain:", QLabel(domain))
        form.addRow("Browser:", QLabel(browser.capitalize()))

        form.addRow("", QLabel(""))  # Spacer

        # HTTP info
        response_code = self.row_data.get("response_code")
        if response_code:
            code_label = QLabel(str(response_code))
            if 200 <= response_code < 300:
                code_label.setStyleSheet("color: green; font-weight: bold;")
            elif response_code >= 400:
                code_label.setStyleSheet("color: red; font-weight: bold;")
            form.addRow("HTTP Status:", code_label)
        else:
            form.addRow("HTTP Status:", QLabel("N/A"))

        content_type = self.row_data.get("content_type", "N/A")
        form.addRow("Content-Type:", QLabel(content_type or "N/A"))

        content_encoding = self.row_data.get("content_encoding")
        if content_encoding:
            form.addRow("Content-Encoding:", QLabel(content_encoding))

        form.addRow("", QLabel(""))  # Spacer

        # Cache file info
        cache_filename = self.row_data.get("cache_filename", "N/A")
        form.addRow("Cache File:", QLabel(cache_filename or "N/A"))

        source_path = self.row_data.get("source_path", "N/A")
        source_label = QLabel(source_path or "N/A")
        source_label.setWordWrap(True)
        form.addRow("Source Path:", source_label)

        form.addRow("", QLabel(""))  # Spacer

        # Timestamps
        first_seen = self.row_data.get("first_seen_utc") or "N/A"
        last_seen = self.row_data.get("last_seen_utc") or self.row_data.get("last_used_time") or "N/A"

        first_display = first_seen[:19] if first_seen != "N/A" and len(first_seen) >= 19 else first_seen
        last_display = last_seen[:19] if last_seen != "N/A" and len(str(last_seen)) >= 19 else str(last_seen)

        form.addRow("First Seen:", QLabel(first_display))
        form.addRow("Last Used:", QLabel(last_display))

        form.addRow("", QLabel(""))  # Spacer

        # Stream sizes (from parsed tags)
        stream0_size = self.row_data.get("stream0_size")
        stream1_size = self.row_data.get("stream1_size")

        if stream0_size is not None or stream1_size is not None:
            if stream0_size is not None:
                form.addRow("Headers Size:", QLabel(f"{stream0_size:,} bytes"))
            if stream1_size is not None:
                form.addRow("Body Size:", QLabel(f"{stream1_size:,} bytes"))

        # Run ID (for forensic provenance)
        run_id = self.row_data.get("run_id")
        if run_id:
            form.addRow("", QLabel(""))
            run_label = QLabel(run_id)
            run_label.setStyleSheet("color: gray; font-size: 10px;")
            form.addRow("Run ID:", run_label)

        layout.addLayout(form)
        layout.addStretch()

        # Buttons
        button_layout = QHBoxLayout()

        copy_url_btn = QPushButton("Copy URL")
        copy_url_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(url)
        )
        button_layout.addWidget(copy_url_btn)

        open_btn = QPushButton("Open URL")
        open_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(url)))
        button_layout.addWidget(open_btn)

        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)
