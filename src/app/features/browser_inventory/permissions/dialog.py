"""Permission details dialog."""
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


class PermissionDetailsDialog(QDialog):
    """Dialog showing full details for a site permission."""

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data

        self.setWindowTitle("Permission Details")
        self.setModal(True)
        self.resize(500, 350)

        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()

        form.addRow("Browser:", QLabel(self.row_data.get("browser", "N/A").capitalize()))
        form.addRow("Profile:", QLabel(self.row_data.get("profile") or "N/A"))
        form.addRow("", QLabel(""))
        perm_type = self.row_data.get("permission_type", "")
        form.addRow("Permission:", QLabel(perm_type.replace("_", " ").title()))
        decision = self.row_data.get("decision", "")
        form.addRow("Decision:", QLabel(decision.replace("_", " ").title()))
        form.addRow("", QLabel(""))
        form.addRow("Granted:", QLabel(self.row_data.get("granted_at") or "N/A"))
        form.addRow("Expires:", QLabel(self.row_data.get("expires_at") or "Never"))
        form.addRow("Is Default:", QLabel("Yes" if self.row_data.get("is_default") else "No"))

        layout.addLayout(form)

        layout.addWidget(QLabel("Origin:"))
        origin_text = QTextEdit()
        origin_text.setReadOnly(True)
        origin_text.setMaximumHeight(40)
        origin_text.setPlainText(self.row_data.get("origin", ""))
        layout.addWidget(origin_text)

        secondary = self.row_data.get("secondary_origin")
        if secondary:
            layout.addWidget(QLabel("Secondary Origin:"))
            sec_text = QTextEdit()
            sec_text.setReadOnly(True)
            sec_text.setMaximumHeight(40)
            sec_text.setPlainText(secondary)
            layout.addWidget(sec_text)

        button_layout = QHBoxLayout()
        origin = self.row_data.get("origin", "")
        copy_btn = QPushButton("Copy Origin")
        copy_btn.setEnabled(bool(origin))
        copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(origin))
        button_layout.addWidget(copy_btn)
        button_layout.addStretch()
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.reject)
        button_layout.addWidget(close_btn)
        layout.addLayout(button_layout)
