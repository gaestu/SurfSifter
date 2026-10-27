"""Browser config details dialog."""
from __future__ import annotations

import json
from typing import Any

from PySide6.QtCore import Qt
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


class BrowserConfigDetailsDialog(QDialog):
    """Dialog showing complete browser_config row details."""

    def __init__(self, row_data: dict, parent=None):
        super().__init__(parent)
        self.row_data = row_data
        self.setWindowTitle("Browser Config Details")
        self.setModal(True)
        self.resize(700, 620)
        self._setup_ui()

    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)

        form = QFormLayout()
        for label, key in (
            ("Record ID:", "id"),
            ("Evidence ID:", "evidence_id"),
            ("Browser:", "browser"),
            ("Profile:", "profile"),
            ("Config Type:", "config_type"),
            ("Config Key:", "config_key"),
            ("Value Count:", "value_count"),
            ("Run ID:", "run_id"),
            ("Partition Index:", "partition_index"),
            ("Filesystem Type:", "fs_type"),
            ("Created:", "created_at_utc"),
        ):
            form.addRow(label, self._plain_label(self._display_value(self.row_data.get(key))))
        layout.addLayout(form)

        self._add_text_block(layout, "Config Value:", self.row_data.get("config_value"), 120)
        self._add_text_block(layout, "Source Path:", self.row_data.get("source_path"), 60)
        self._add_text_block(layout, "Logical Path:", self.row_data.get("logical_path"), 60)
        self._add_text_block(layout, "Forensic Path:", self.row_data.get("forensic_path"), 60)
        self._add_text_block(layout, "Notes:", self.row_data.get("notes"), 80)

        buttons = QHBoxLayout()
        copy_key = QPushButton("Copy Key")
        config_key = self.row_data.get("config_key") or ""
        copy_key.setEnabled(bool(config_key))
        copy_key.clicked.connect(lambda: QApplication.clipboard().setText(config_key))
        buttons.addWidget(copy_key)

        copy_record = QPushButton("Copy Record")
        copy_record.clicked.connect(self._copy_record)
        buttons.addWidget(copy_record)

        buttons.addStretch()
        close = QPushButton("Close")
        close.clicked.connect(self.reject)
        buttons.addWidget(close)
        layout.addLayout(buttons)

    def _add_text_block(self, layout: QVBoxLayout, label: str, value: Any, max_height: int) -> None:
        layout.addWidget(self._plain_label(label))
        text = QTextEdit()
        text.setReadOnly(True)
        text.setMaximumHeight(max_height)
        text.setPlainText(self._display_value(value, empty=""))
        layout.addWidget(text)

    def _copy_record(self) -> None:
        QApplication.clipboard().setText(
            json.dumps(self.row_data, indent=2, sort_keys=True, default=str)
        )

    @staticmethod
    def _plain_label(value: str) -> QLabel:
        label = QLabel(value)
        label.setTextFormat(Qt.PlainText)
        return label

    @staticmethod
    def _display_value(value: Any, empty: str = "N/A") -> str:
        if value is None or value == "":
            return empty
        return str(value)