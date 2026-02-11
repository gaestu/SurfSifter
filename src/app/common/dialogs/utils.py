"""
Shared utility functions for dialogs.
"""
from __future__ import annotations

import json
from typing import Callable, Iterable, Optional

from PySide6.QtWidgets import QApplication, QMessageBox, QWidget


def show_error_dialog(
    parent: QWidget,
    title: str,
    message: str,
    details: str = "",
    log_provider: Optional[Callable[[], Iterable[str]]] = None,
) -> None:
    """
    Show an error dialog with copy functionality.

    Args:
        parent: Parent widget
        title: Dialog title
        message: Error message
        details: Additional details (shown in detailed text)
        log_provider: Optional callable returning log lines
    """
    dialog = QMessageBox(parent)
    dialog.setWindowTitle(title)
    dialog.setIcon(QMessageBox.Warning)
    dialog.setText(message)
    if details:
        dialog.setDetailedText(details)
    copy_button = dialog.addButton(parent.tr("Copy details"), QMessageBox.ActionRole)
    copy_json_button = dialog.addButton(parent.tr("Copy as JSON"), QMessageBox.ActionRole)
    dialog.addButton(parent.tr("Close"), QMessageBox.AcceptRole)

    def _copy_payload() -> None:
        payload = message
        if details:
            payload = f"{message}\n\n{details}"
        QApplication.clipboard().setText(payload)

    copy_button.clicked.connect(_copy_payload)
    if log_provider:
        copy_json_button.clicked.connect(
            lambda: QApplication.clipboard().setText(
                json.dumps(
                    {
                        "title": title,
                        "message": message,
                        "details": details,
                        "logs": list(log_provider() or []),
                    },
                    indent=2,
                )
            )
        )
    else:
        copy_json_button.setEnabled(False)
    dialog.exec()
