"""Related URLs and emails dialog for web storage."""
from __future__ import annotations

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QDialog,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)


class RelatedUrlsEmailsDialog(QDialog):
    """Dialog showing URLs and emails extracted from a storage key."""

    def __init__(self, urls: list, emails: list, source_context: str, parent=None):
        """
        Initialize related URLs/emails dialog.

        Args:
            urls: List of URL records
            emails: List of email records
            source_context: Context string describing the source
            parent: Parent widget
        """
        super().__init__(parent)
        self.urls = urls
        self.emails = emails
        self.source_context = source_context

        self.setWindowTitle("Related URLs and Emails")
        self.setModal(True)
        self.resize(750, 500)

        self._setup_ui()

    def _setup_ui(self) -> None:
        """Create UI layout."""
        layout = QVBoxLayout(self)

        # Source context
        context_label = QLabel(self.source_context)
        context_label.setWordWrap(True)
        context_label.setStyleSheet("QLabel { padding: 4px; background-color: #f0f0f0; border-radius: 4px; }")
        layout.addWidget(context_label)

        # Splitter for URLs and emails
        splitter = QSplitter(Qt.Vertical)

        # URLs section
        urls_group = QGroupBox(f"URLs ({len(self.urls)})")
        urls_layout = QVBoxLayout(urls_group)

        self.urls_table = QTableWidget()
        self.urls_table.setColumnCount(4)
        self.urls_table.setHorizontalHeaderLabels(["URL", "Context", "First Seen", "Last Seen"])
        self.urls_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.urls_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.urls_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.urls_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.urls_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.urls_table.setEditTriggers(QTableWidget.NoEditTriggers)

        self.urls_table.setRowCount(len(self.urls))
        for i, url_data in enumerate(self.urls):
            self.urls_table.setItem(i, 0, QTableWidgetItem(url_data.get("url", "")))
            self.urls_table.setItem(i, 1, QTableWidgetItem(url_data.get("context", "")))
            first_seen = url_data.get("first_seen_utc", "")
            self.urls_table.setItem(i, 2, QTableWidgetItem(first_seen[:16] if first_seen else ""))
            last_seen = url_data.get("last_seen_utc", "")
            self.urls_table.setItem(i, 3, QTableWidgetItem(last_seen[:16] if last_seen else ""))

        urls_layout.addWidget(self.urls_table)
        splitter.addWidget(urls_group)

        # Emails section
        emails_group = QGroupBox(f"Emails ({len(self.emails)})")
        emails_layout = QVBoxLayout(emails_group)

        self.emails_table = QTableWidget()
        self.emails_table.setColumnCount(2)
        self.emails_table.setHorizontalHeaderLabels(["Email", "Context"])
        self.emails_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.emails_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.emails_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.emails_table.setEditTriggers(QTableWidget.NoEditTriggers)

        self.emails_table.setRowCount(len(self.emails))
        for i, email_data in enumerate(self.emails):
            self.emails_table.setItem(i, 0, QTableWidgetItem(email_data.get("email", "")))
            self.emails_table.setItem(i, 1, QTableWidgetItem(email_data.get("context", "")))

        emails_layout.addWidget(self.emails_table)
        splitter.addWidget(emails_group)

        layout.addWidget(splitter)

        # Summary
        summary = f"Found {len(self.urls)} URLs and {len(self.emails)} emails from this storage source."
        summary_label = QLabel(summary)
        layout.addWidget(summary_label)

        # Buttons
        button_layout = QHBoxLayout()

        copy_urls_btn = QPushButton("Copy All URLs")
        copy_urls_btn.clicked.connect(self._copy_all_urls)
        button_layout.addWidget(copy_urls_btn)

        copy_emails_btn = QPushButton("Copy All Emails")
        copy_emails_btn.clicked.connect(self._copy_all_emails)
        button_layout.addWidget(copy_emails_btn)

        button_layout.addStretch()

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)

        layout.addLayout(button_layout)

    def _copy_all_urls(self) -> None:
        """Copy all URLs to clipboard."""
        urls_text = "\n".join(url.get("url", "") for url in self.urls)
        QApplication.clipboard().setText(urls_text)

    def _copy_all_emails(self) -> None:
        """Copy all emails to clipboard."""
        emails_text = "\n".join(email.get("email", "") for email in self.emails)
        QApplication.clipboard().setText(emails_text)
