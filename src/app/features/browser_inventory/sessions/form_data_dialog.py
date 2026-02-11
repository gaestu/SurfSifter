"""Session form data details dialog."""
from __future__ import annotations

from PySide6.QtCore import Qt, QUrl
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


class SessionFormDataDetailsDialog(QDialog):
    """Dialog showing full details for a session form data entry.

    Initial implementation for Firefox session form data.
    Displays form field data captured from session restore files including
    field name, value, type, and associated page URL.
    """

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Session Form Data Details")
        self.setModal(True)
        self.resize(600, 480)

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()

        # Browser/Profile info
        browser = self.row_data.get("browser") or "N/A"
        form.addRow("Browser:", QLabel(browser.capitalize() if browser != "N/A" else browser))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))

        form.addRow("", QLabel(""))  # Spacer

        # Form field info
        field_name = self.row_data.get("field_name") or "N/A"
        field_name_label = QLabel(field_name)
        field_name_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        form.addRow("Field Name:", field_name_label)

        field_type = self.row_data.get("field_type") or "text"
        form.addRow("Field Type:", QLabel(field_type))

        # Window/Tab context from session
        window_index = self.row_data.get("window_index")
        if window_index is not None:
            form.addRow("Window Index:", QLabel(str(window_index)))

        tab_index = self.row_data.get("tab_index")
        if tab_index is not None:
            form.addRow("Tab Index:", QLabel(str(tab_index)))

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
            layout.addWidget(QLabel("Page URL:"))
            url_text = QTextEdit()
            url_text.setReadOnly(True)
            url_text.setMaximumHeight(60)
            url_text.setPlainText(url)
            layout.addWidget(url_text)

        # Value section
        layout.addWidget(QLabel("Field Value:"))
        value_text = QTextEdit()
        value_text.setReadOnly(True)
        value_text.setMaximumHeight(100)
        value_text.setPlainText(self.row_data.get("field_value", ""))
        layout.addWidget(value_text)

        # Buttons
        button_layout = QHBoxLayout()

        copy_name_btn = QPushButton("Copy Field Name")
        copy_name_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(self.row_data.get("field_name", ""))
        )
        button_layout.addWidget(copy_name_btn)

        copy_value_btn = QPushButton("Copy Value")
        copy_value_btn.clicked.connect(
            lambda: QApplication.clipboard().setText(self.row_data.get("field_value", ""))
        )
        button_layout.addWidget(copy_value_btn)

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
