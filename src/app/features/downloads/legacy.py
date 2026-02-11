from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path
from typing import Dict, List, Optional

from PySide6.QtCore import Qt, QUrl
from PySide6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtWidgets import QApplication
from PySide6.QtGui import QDesktopServices


@dataclass
class DownloadQueueItem:
    item_id: int
    url: str
    domain: str
    filename: str
    status: str = "queued"
    progress: int = 0
    error: str = ""
    dest_path: Optional[Path] = None
    bytes_written: int = 0
    sha256: str = ""
    md5: str = ""  # Added for hash matching
    content_type: str = ""
    duration_s: float = 0.0
    # New fields for database wiring
    url_id: Optional[int] = None  # FK to urls table
    download_id: Optional[int] = None  # FK to downloads table (set after insert)


class DownloadManagerDialog(QDialog):
    _last_snapshot: Dict[int, DownloadQueueItem] = {}

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Download Manager")
        self.resize(720, 360)
        self._items: Dict[int, DownloadQueueItem] = {}
        self._download_root: Optional[Path] = None
        self._last_queue_count: Optional[int] = None
        self._row_ids: List[int] = []

        layout = QVBoxLayout()
        self.info_label = QLabel("Downloads pending…")
        layout.addWidget(self.info_label)

        self.table = QTableWidget(0, 7)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setHorizontalHeaderLabels(
            [
                "URL",
                "Domain",
                "Status",
                "Progress",
                "Bytes",
                "SHA256",
                "Error",
            ]
        )
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.itemSelectionChanged.connect(self._on_selection_changed)
        layout.addWidget(self.table)

        button_layout = QHBoxLayout()
        self.start_button = QPushButton("Start")
        self.pause_button = QPushButton("Pause")
        self.cancel_button = QPushButton("Cancel")
        self.open_folder_button = QPushButton("Open downloads folder")
        self.open_folder_button.setEnabled(False)
        self.open_folder_button.clicked.connect(self._open_downloads_folder)
        self.copy_sha_button = QPushButton("Copy SHA256")
        self.copy_sha_button.setEnabled(False)
        self.copy_sha_button.clicked.connect(self._copy_selected_sha)
        self.copy_button = QPushButton("Copy details")
        self.copy_button.setEnabled(False)
        self.copy_button.clicked.connect(self._copy_selected_details)
        button_layout.addWidget(self.start_button)
        button_layout.addWidget(self.pause_button)
        button_layout.addStretch()
        button_layout.addWidget(self.open_folder_button)
        button_layout.addWidget(self.copy_sha_button)
        button_layout.addWidget(self.copy_button)
        button_layout.addWidget(self.cancel_button)
        layout.addLayout(button_layout)

        self.setLayout(layout)

        self._update_action_states()

    def set_queue(self, items: List[DownloadQueueItem]) -> None:
        merged: List[DownloadQueueItem] = []
        for item in items:
            cached = self._last_snapshot.get(item.item_id)
            if cached:
                cached_copy = replace(cached)
                cached_copy.url = item.url
                cached_copy.domain = item.domain
                cached_copy.filename = item.filename
                merged.append(cached_copy)
            else:
                merged.append(item)
        self._items = {item.item_id: item for item in merged}
        self._row_ids = [item.item_id for item in merged]
        self.table.setRowCount(len(merged))
        for row, item in enumerate(merged):
            self._items[item.item_id] = item
            self._set_row(row, item)
            self._last_snapshot[item.item_id] = replace(item)
        self._last_queue_count = len(merged)
        self.info_label.setText(
            f"{self._last_queue_count} items queued."
        )
        self._update_action_states()

    def update_item(
        self,
        item_id: int,
        *,
        progress: Optional[int] = None,
        status: str = "",
        error: str = "",
        dest_path: Optional[str] = None,
        bytes_written: Optional[int] = None,
        sha256: Optional[str] = None,
        content_type: Optional[str] = None,
        duration_s: Optional[float] = None,
    ) -> None:
        if item_id not in self._items:
            return
        item = self._items[item_id]
        if progress is not None:
            item.progress = progress
        if status:
            item.status = status
        if error:
            item.error = error
        if dest_path:
            item.dest_path = Path(dest_path)
        if bytes_written is not None:
            item.bytes_written = bytes_written
        if sha256 is not None:
            item.sha256 = sha256
        if content_type is not None:
            item.content_type = content_type
        if duration_s is not None:
            item.duration_s = duration_s
        self._last_snapshot[item_id] = replace(item)
        row = self._row_ids.index(item_id)
        self._set_row(row, item)
        self._update_action_states()

    def _set_row(self, row: int, item: DownloadQueueItem) -> None:
        self.table.setItem(row, 0, QTableWidgetItem(item.url))
        self.table.setItem(row, 1, QTableWidgetItem(item.domain))
        self.table.setItem(row, 2, QTableWidgetItem(item.status))
        self.table.setItem(row, 3, QTableWidgetItem(f"{item.progress}%"))
        self.table.setItem(row, 4, QTableWidgetItem(self._format_bytes(item.bytes_written)))
        self.table.setItem(row, 5, QTableWidgetItem(item.sha256 or "—"))
        self.table.setItem(row, 6, QTableWidgetItem(item.error or ""))

    def _on_selection_changed(self) -> None:
        self._update_action_states()

    def _copy_selected_details(self) -> None:
        item = self._selected_item()
        if not item:
            return
        details = [
            f"URL: {item.url}",
            f"Domain: {item.domain}",
            f"Filename: {item.filename}",
            f"Status: {item.status}",
            f"Progress: {item.progress}%",
            f"Bytes: {item.bytes_written}",
            f"SHA256: {item.sha256 or '—'}",
            f"Content-Type: {item.content_type or 'unknown'}",
            f"Duration(s): {item.duration_s:.2f}",
            f"Destination: {item.dest_path or '—'}",
        ]
        if item.error:
            details.append(f"Error: {item.error}")
        clipboard = QApplication.clipboard()
        clipboard.setText("\n".join(details))

    @staticmethod
    def _format_bytes(value: int) -> str:
        if value <= 0:
            return "0 B"
        units = ["B", "KB", "MB", "GB"]
        size = float(value)
        index = 0
        while size >= 1024 and index < len(units) - 1:
            size /= 1024
            index += 1
        return f"{size:.1f} {units[index]}"

    def _selected_item(self) -> Optional[DownloadQueueItem]:
        indexes = self.table.selectedIndexes()
        if not indexes:
            return None
        row = indexes[0].row()
        item_id = self._row_ids[row]
        return self._items.get(item_id)

    def _copy_selected_sha(self) -> None:
        item = self._selected_item()
        if not item or not item.sha256:
            return
        QApplication.clipboard().setText(item.sha256)

    def _open_downloads_folder(self) -> None:
        item = self._selected_item()
        target: Optional[Path] = None
        if item and item.dest_path:
            target = Path(item.dest_path).parent
        elif self._download_root:
            target = self._download_root
        if target and target.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))

    def _update_action_states(self) -> None:
        item = self._selected_item()
        has_selection = item is not None
        self.copy_button.setEnabled(has_selection)
        self.copy_sha_button.setEnabled(bool(item and item.sha256))
        enable_open = bool(self._download_root and self._download_root.exists())
        if item and item.dest_path:
            enable_open = True
        self.open_folder_button.setEnabled(enable_open)

    def set_download_root(self, root: Path) -> None:
        self._download_root = root
        self._update_action_states()
