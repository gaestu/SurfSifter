"""Search terms details dialog."""
from __future__ import annotations

from PySide6.QtCore import QUrl
from PySide6.QtGui import QDesktopServices, QFont
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


class SearchTermsDetailsDialog(QDialog):
    """Dialog showing full details for a browser search term record."""

    def __init__(self, row_data: dict, parent=None):
        """
        Initialize search term details dialog.

        Args:
            row_data: Search term record data dictionary
            parent: Parent widget
        """
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Search Term Details")
        self.setModal(True)
        self.resize(600, 500)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Form layout for fields
        form = QFormLayout()

        # Search term (the main content)
        term = self.row_data.get("term") or "N/A"
        term_label = QLabel(term)
        term_label.setWordWrap(True)
        term_label.setFont(QFont("", 12, QFont.Bold))
        form.addRow("Search Term:", term_label)

        # Normalized term
        normalized = self.row_data.get("normalized_term")
        if normalized and normalized != term:
            form.addRow("Normalized:", QLabel(normalized))

        form.addRow("", QLabel(""))  # Spacer

        # Browser info
        browser = self.row_data.get("browser", "N/A")
        form.addRow("Browser:", QLabel(browser.capitalize() if browser else "N/A"))
        form.addRow("Profile:", QLabel(str(self.row_data.get("profile") or "N/A")))

        form.addRow("", QLabel(""))  # Spacer

        # Search context
        search_time = self.row_data.get("search_time_utc") or "N/A"
        form.addRow("Search Time:", QLabel(str(search_time)))

        search_engine = self.row_data.get("search_engine")
        if search_engine:
            form.addRow("Search Engine:", QLabel(search_engine))

        form.addRow("", QLabel(""))  # Spacer

        # Chromium metadata
        keyword_id = self.row_data.get("chromium_keyword_id")
        if keyword_id is not None:
            form.addRow("Keyword ID:", QLabel(str(keyword_id)))

        url_id = self.row_data.get("chromium_url_id")
        if url_id is not None:
            form.addRow("URL ID:", QLabel(str(url_id)))

        form.addRow("", QLabel(""))  # Spacer

        # Source info
        form.addRow("Source Path:", QLabel(self.row_data.get("source_path") or "N/A"))
        form.addRow("Run ID:", QLabel(self.row_data.get("run_id") or "N/A"))

        partition = self.row_data.get("partition_index")
        if partition is not None:
            form.addRow("Partition:", QLabel(str(partition)))

        layout.addLayout(form)

        # URL section
        url = self.row_data.get("url", "")
        if url:
            layout.addWidget(QLabel("Associated URL:"))
            url_text = QTextEdit()
            url_text.setReadOnly(True)
            url_text.setMaximumHeight(60)
            url_text.setPlainText(url)
            layout.addWidget(url_text)

        # Buttons
        button_layout = QHBoxLayout()

        copy_term_btn = QPushButton("Copy Search Term")
        copy_term_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(self.row_data.get("term", ""))
        )
        button_layout.addWidget(copy_term_btn)

        if url:
            copy_url_btn = QPushButton("Copy URL")
            copy_url_btn.clicked.connect(
                lambda: QApplication.clipboard().setText(url)
            )
            button_layout.addWidget(copy_url_btn)

            open_url_btn = QPushButton("Open URL")
            open_url_btn.clicked.connect(
                lambda: QDesktopServices.openUrl(QUrl(url))
            )
            button_layout.addWidget(open_url_btn)

        button_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)
