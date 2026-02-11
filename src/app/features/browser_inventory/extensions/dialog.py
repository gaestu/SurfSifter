"""Extension details dialog."""
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


class ExtensionDetailsDialog(QDialog):
    """Dialog showing full details for a browser extension."""

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Extension Details")
        self.setModal(True)
        self.resize(650, 550)

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()

        # Basic info
        form.addRow("Name:", QLabel(self.row_data.get("name", "N/A")))
        form.addRow("Extension ID:", QLabel(self.row_data.get("extension_id", "N/A")))
        form.addRow("Version:", QLabel(self.row_data.get("version") or "N/A"))
        form.addRow("Author:", QLabel(self.row_data.get("author") or "N/A"))
        form.addRow("", QLabel(""))

        # Browser/Profile
        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))
        form.addRow("", QLabel(""))

        # Status
        enabled = self.row_data.get("enabled")
        enabled_str = "Yes" if enabled else "No" if enabled is not None else "Unknown"
        form.addRow("Enabled:", QLabel(enabled_str))

        # Install info
        install_loc = self.row_data.get("install_location_text") or "Unknown"
        form.addRow("Install Source:", QLabel(install_loc))

        from_webstore = self.row_data.get("from_webstore")
        webstore_str = "Yes" if from_webstore else "No" if from_webstore is not None else "Unknown"
        form.addRow("From Web Store:", QLabel(webstore_str))

        form.addRow("Install Time:", QLabel(self.row_data.get("install_time") or "N/A"))
        form.addRow("", QLabel(""))

        # Risk info
        risk_score = self.row_data.get("risk_score") or 0
        risk_label = QLabel(f"{risk_score}")
        if risk_score >= 80:
            risk_label.setStyleSheet("color: red; font-weight: bold;")
        elif risk_score >= 60:
            risk_label.setStyleSheet("color: orange; font-weight: bold;")
        elif risk_score >= 40:
            risk_label.setStyleSheet("color: #B8860B;")  # Dark goldenrod
        form.addRow("Risk Score:", risk_label)

        category = self.row_data.get("known_category")
        if category:
            form.addRow("Category:", QLabel(category.replace("_", " ").title()))

        layout.addLayout(form)

        # Risk factors
        risk_factors = self.row_data.get("risk_factors", "")
        if risk_factors:
            layout.addWidget(QLabel("Risk Factors:"))
            rf_text = QTextEdit()
            rf_text.setReadOnly(True)
            rf_text.setMaximumHeight(60)
            rf_text.setPlainText(risk_factors)
            layout.addWidget(rf_text)

        # Description
        description = self.row_data.get("description", "")
        if description:
            layout.addWidget(QLabel("Description:"))
            desc_text = QTextEdit()
            desc_text.setReadOnly(True)
            desc_text.setMaximumHeight(60)
            desc_text.setPlainText(description)
            layout.addWidget(desc_text)

        # Permissions
        permissions = self.row_data.get("permissions", "")
        if permissions:
            layout.addWidget(QLabel("Permissions:"))
            perm_text = QTextEdit()
            perm_text.setReadOnly(True)
            perm_text.setMaximumHeight(80)
            perm_text.setPlainText(permissions)
            layout.addWidget(perm_text)

        # Host permissions
        host_perms = self.row_data.get("host_permissions", "")
        if host_perms:
            layout.addWidget(QLabel("Host Permissions:"))
            host_text = QTextEdit()
            host_text.setReadOnly(True)
            host_text.setMaximumHeight(60)
            host_text.setPlainText(host_perms)
            layout.addWidget(host_text)

        # Buttons
        button_layout = QHBoxLayout()

        ext_id = self.row_data.get("extension_id", "")
        copy_id_btn = QPushButton("Copy Extension ID")
        copy_id_btn.setEnabled(bool(ext_id))
        copy_id_btn.clicked.connect(lambda: QApplication.clipboard().setText(ext_id))
        button_layout.addWidget(copy_id_btn)

        homepage = self.row_data.get("homepage_url", "")
        if homepage:
            open_btn = QPushButton("Open Homepage")
            open_btn.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(homepage)))
            button_layout.addWidget(open_btn)

        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)
