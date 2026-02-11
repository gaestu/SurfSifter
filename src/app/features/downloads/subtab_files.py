"""
Downloaded Files subtab - view downloaded non-image files.

Extracted from downloads/tab.py
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, Signal, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QCheckBox,
    QPushButton,
    QLabel,
    QLineEdit,
    QComboBox,
    QSpinBox,
    QFormLayout,
    QMessageBox,
    QMenu,
    QApplication,
    QFileDialog,
)
from PySide6.QtGui import QDesktopServices
from PySide6.QtCore import QUrl

from app.data.case_data import CaseDataAccess
from core.file_classifier import FILE_TYPE_LABELS
from app.features.downloads.workers import DownloadsListWorker
from app.features.downloads.helpers import get_downloads_folder

logger = logging.getLogger(__name__)


class DownloadedFilesPanel(QWidget):
    """Panel for viewing downloaded non-image files."""

    def __init__(
        self,
        evidence_id: int,
        case_data: Optional[CaseDataAccess] = None,
        case_folder: Optional[Path] = None,
        parent: Optional[QWidget] = None,
    ):
        super().__init__(parent)
        self.evidence_id = evidence_id
        self.case_data = case_data
        self.case_folder = case_folder
        self.case_db_path = case_data.db_path if case_data else None

        self._downloads: List[Dict[str, Any]] = []
        self._worker: Optional[DownloadsListWorker] = None
        self._pending_workers: List[DownloadsListWorker] = []  # Keep old workers alive until finished
        self._worker_generation = 0  # Track worker generation to ignore stale results

        # Guards against re-entrant calls
        self._refresh_pending = False
        self._filter_timer = QTimer()
        self._filter_timer.setSingleShot(True)
        self._filter_timer.timeout.connect(self.refresh)

        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        # Filters
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel("Type:"))
        self.type_filter = QComboBox()
        self.type_filter.addItem("All", "")
        for ftype in ["video", "audio", "document", "archive", "other"]:
            self.type_filter.addItem(FILE_TYPE_LABELS.get(ftype, ftype), ftype)
        self.type_filter.currentIndexChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self.type_filter)

        filter_layout.addWidget(QLabel("Domain:"))
        self.domain_filter = QComboBox()
        self.domain_filter.addItem("All", "")
        self.domain_filter.currentIndexChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self.domain_filter)

        filter_layout.addWidget(QLabel("Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Filter by filename...")
        self.search_edit.setClearButtonEnabled(True)
        self.search_edit.textChanged.connect(self._on_filter_changed)
        filter_layout.addWidget(self.search_edit, 1)

        filter_layout.addStretch()
        layout.addLayout(filter_layout)

        # Table
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels([
            "Filename", "Type", "Size", "Domain", "Downloaded", "SHA256"
        ])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.setColumnWidth(1, 80)
        self.table.setColumnWidth(2, 80)
        self.table.setColumnWidth(3, 120)
        self.table.setColumnWidth(4, 140)
        self.table.setColumnWidth(5, 120)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_context_menu)
        self.table.doubleClicked.connect(self._on_row_double_clicked)
        layout.addWidget(self.table, 1)

        # Actions
        actions_layout = QHBoxLayout()

        self.tag_btn = QPushButton("Tag Selected")
        self.tag_btn.clicked.connect(self._on_tag_clicked)
        actions_layout.addWidget(self.tag_btn)

        self.export_btn = QPushButton("Export to Folder")
        self.export_btn.clicked.connect(self._on_export_clicked)
        actions_layout.addWidget(self.export_btn)

        self.props_btn = QPushButton("View Properties")
        self.props_btn.clicked.connect(self._on_properties_clicked)
        actions_layout.addWidget(self.props_btn)

        actions_layout.addStretch()

        self.open_folder_btn = QPushButton("Open Folder")
        self.open_folder_btn.clicked.connect(self._on_open_folder_clicked)
        actions_layout.addWidget(self.open_folder_btn)

        layout.addLayout(actions_layout)

        # Status
        self.status_label = QLabel("0 files downloaded")
        layout.addWidget(self.status_label)

    def _on_filter_changed(self):
        """Handle filter change.

        Added debounce to prevent rapid worker creation.
        """
        self._filter_timer.start(150)

    def refresh(self):
        """Refresh the downloads list.

        Added guard to prevent re-entrant calls.
        """
        if not self.case_data or not self.case_folder or not self.case_db_path:
            return

        # Coalesce rapid refresh calls
        if self._refresh_pending:
            return
        self._refresh_pending = True

        try:
            # Increment generation - any in-flight workers with old generation will be ignored
            self._worker_generation += 1
            current_gen = self._worker_generation

            # Keep old worker alive until it finishes (prevents QThread crash)
            if self._worker and self._worker.isRunning():
                self._pending_workers.append(self._worker)

            # Clean up finished workers from pending list
            self._pending_workers = [w for w in self._pending_workers if w.isRunning()]

            file_type = self.type_filter.currentData()
            domain = self.domain_filter.currentData()

            # Non-image types
            self._worker = DownloadsListWorker(
                self.case_folder,
                self.case_db_path,
                self.evidence_id,
                file_type=file_type if file_type else None,
                status_filter="completed",
                domain_filter=domain if domain else None,
                search_text=self.search_edit.text() or None,
            )
            # Use lambda to capture generation for staleness check
            self._worker.finished.connect(
                lambda rows, count, gen=current_gen: self._on_data_loaded(rows, count, gen)
            )
            self._worker.error.connect(
                lambda err, gen=current_gen: self._on_load_error(err, gen)
            )
            self._worker.start()
        finally:
            self._refresh_pending = False

    def _on_data_loaded(self, rows: List[Dict], total: int, generation: int = 0):
        """Handle data loaded."""
        # Ignore stale results from old workers
        if generation != self._worker_generation:
            logger.debug("Ignoring stale DownloadsListWorker result (gen %d vs current %d)", generation, self._worker_generation)
            return

        # Filter out images - they're shown in the Images subtab
        self._downloads = [r for r in rows if r.get("file_type") != "image"]
        self._populate_table()
        self.status_label.setText(f"{len(self._downloads)} files downloaded")

        # Load domains
        self._load_domains()

    def _on_load_error(self, error: str, generation: int = 0):
        """Handle load error."""
        # Ignore errors from stale workers
        if generation != self._worker_generation:
            return

        self.status_label.setText(f"Error: {error}")

    def _load_domains(self):
        """Load unique domains for filter."""
        if not self.case_data:
            return

        try:
            domains = self.case_data.list_download_domains(self.evidence_id)
            current = self.domain_filter.currentData()
            self.domain_filter.clear()
            self.domain_filter.addItem("All", "")
            for domain in domains:
                self.domain_filter.addItem(domain, domain)

            if current:
                idx = self.domain_filter.findData(current)
                if idx >= 0:
                    self.domain_filter.setCurrentIndex(idx)
        except Exception as e:
            logger.warning("Failed to load domains: %s", e)

    def _populate_table(self):
        """Populate table with downloads."""
        self.table.setRowCount(len(self._downloads))

        for row, download in enumerate(self._downloads):
            # Filename
            filename_item = QTableWidgetItem(download.get("filename") or "")
            filename_item.setData(Qt.UserRole, download)
            self.table.setItem(row, 0, filename_item)

            # Type
            self.table.setItem(row, 1, QTableWidgetItem(download.get("file_type") or ""))

            # Size
            size = download.get("size_bytes") or 0
            self.table.setItem(row, 2, QTableWidgetItem(self._format_size(size)))

            # Domain
            self.table.setItem(row, 3, QTableWidgetItem(download.get("domain") or ""))

            # Downloaded timestamp
            completed = download.get("completed_at_utc") or ""
            if completed:
                # Format ISO timestamp
                try:
                    dt = datetime.fromisoformat(completed.replace("Z", "+00:00"))
                    completed = dt.strftime("%Y-%m-%d %H:%M")
                except Exception:
                    pass
            self.table.setItem(row, 4, QTableWidgetItem(completed))

            # SHA256 (truncated)
            sha256 = download.get("sha256") or ""
            if sha256:
                sha256 = sha256[:16] + "..."
            self.table.setItem(row, 5, QTableWidgetItem(sha256))

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        if size_bytes <= 0:
            return "0 B"
        units = ["B", "KB", "MB", "GB"]
        size = float(size_bytes)
        idx = 0
        while size >= 1024 and idx < len(units) - 1:
            size /= 1024
            idx += 1
        return f"{size:.1f} {units[idx]}"

    def _get_selected_downloads(self) -> List[Dict[str, Any]]:
        """Get selected download items."""
        selected = []
        for row in self.table.selectedIndexes():
            if row.column() == 0:  # Only process first column to avoid duplicates
                item = self.table.item(row.row(), 0)
                if item:
                    data = item.data(Qt.UserRole)
                    if data:
                        selected.append(data)
        return selected

    def _on_row_double_clicked(self, index):
        """Handle double-click to open file."""
        row = index.row()
        item = self.table.item(row, 0)
        if not item:
            return

        download = item.data(Qt.UserRole)
        if not download or not self.case_folder:
            return

        dest_path = download.get("dest_path")
        if dest_path:
            full_path = self.case_folder / dest_path
            if full_path.exists():
                QDesktopServices.openUrl(QUrl.fromLocalFile(str(full_path)))

    def _show_context_menu(self, pos):
        """Show context menu."""
        item = self.table.itemAt(pos)
        if not item:
            return

        row = item.row()
        first_col = self.table.item(row, 0)
        if not first_col:
            return

        download = first_col.data(Qt.UserRole)
        if not download:
            return

        menu = QMenu(self)

        open_action = menu.addAction("Open File")
        open_action.triggered.connect(lambda: self._open_download(download))

        folder_action = menu.addAction("Open Containing Folder")
        folder_action.triggered.connect(lambda: self._open_containing_folder(download))

        menu.addSeparator()

        copy_url = menu.addAction("Copy Source URL")
        copy_url.triggered.connect(lambda: QApplication.clipboard().setText(download.get("url", "")))

        if download.get("sha256"):
            copy_sha = menu.addAction("Copy SHA256")
            copy_sha.triggered.connect(lambda: QApplication.clipboard().setText(download.get("sha256", "")))

        menu.exec_(self.table.mapToGlobal(pos))

    def _open_download(self, download: Dict[str, Any]):
        """Open downloaded file."""
        if not self.case_folder:
            return
        dest_path = download.get("dest_path")
        if dest_path:
            full_path = self.case_folder / dest_path
            if full_path.exists():
                QDesktopServices.openUrl(QUrl.fromLocalFile(str(full_path)))

    def _open_containing_folder(self, download: Dict[str, Any]):
        """Open folder containing the file."""
        if not self.case_folder:
            return
        dest_path = download.get("dest_path")
        if dest_path:
            full_path = self.case_folder / dest_path
            if full_path.parent.exists():
                QDesktopServices.openUrl(QUrl.fromLocalFile(str(full_path.parent)))

    def _on_tag_clicked(self):
        """Tag selected downloaded files."""
        selected = self._get_selected_downloads()
        if not selected:
            QMessageBox.information(self, "No Selection", "Select files to tag.")
            return

        if not self.case_data or not self.evidence_id:
            QMessageBox.warning(self, "Error", "No case data available.")
            return

        from app.common.dialogs import TagArtifactsDialog

        # Get download IDs
        artifact_ids = [d["id"] for d in selected]

        dialog = TagArtifactsDialog(
            case_data=self.case_data,
            evidence_id=self.evidence_id,
            artifact_type="download",
            artifact_ids=artifact_ids,
            parent=self,
        )
        dialog.tags_changed.connect(self.refresh)
        dialog.exec()

    def _on_export_clicked(self):
        """Export selected files."""
        selected = self._get_selected_downloads()
        if not selected:
            QMessageBox.information(self, "No Selection", "Select files to export.")
            return

        folder = QFileDialog.getExistingDirectory(self, "Export to Folder")
        if not folder:
            return

        import shutil
        exported = 0
        for download in selected:
            dest_path = download.get("dest_path")
            if dest_path and self.case_folder:
                src = self.case_folder / dest_path
                if src.exists():
                    try:
                        shutil.copy2(src, Path(folder) / src.name)
                        exported += 1
                    except Exception as e:
                        logger.warning("Failed to export %s: %s", src, e)

        QMessageBox.information(self, "Export Complete", f"Exported {exported} files.")

    def _on_properties_clicked(self):
        """Show properties of selected file."""
        selected = self._get_selected_downloads()
        if not selected:
            QMessageBox.information(self, "No Selection", "Select a file to view properties.")
            return

        download = selected[0]

        # Build properties text
        props = []
        props.append(f"Filename: {download.get('filename', 'N/A')}")
        props.append(f"URL: {download.get('url', 'N/A')}")
        props.append(f"Domain: {download.get('domain', 'N/A')}")
        props.append(f"Type: {download.get('file_type', 'N/A')}")
        props.append(f"Size: {self._format_size(download.get('size_bytes', 0))}")
        props.append(f"Content-Type: {download.get('content_type', 'N/A')}")
        props.append(f"MD5: {download.get('md5', 'N/A')}")
        props.append(f"SHA256: {download.get('sha256', 'N/A')}")
        props.append(f"Downloaded: {download.get('completed_at_utc', 'N/A')}")
        props.append(f"Duration: {download.get('duration_seconds', 0):.2f}s")
        props.append(f"Path: {download.get('dest_path', 'N/A')}")

        QMessageBox.information(self, "File Properties", "\n".join(props))

    def _on_open_folder_clicked(self):
        """Open downloads folder."""
        if not self.case_folder:
            return

        downloads_folder = get_downloads_folder(
            self.case_folder, self.evidence_id, self.case_db_path
        )
        if downloads_folder and downloads_folder.exists():
            QDesktopServices.openUrl(QUrl.fromLocalFile(str(downloads_folder)))
        else:
            QMessageBox.information(self, "No Downloads", "Downloads folder does not exist yet.")

    def set_case_data(self, case_data: CaseDataAccess, case_folder: Optional[Path] = None):
        """Update case data reference."""
        self.case_data = case_data
        self.case_db_path = case_data.db_path if case_data else None
        if case_folder:
            self.case_folder = case_folder

    def showEvent(self, event):
        """Refresh when panel becomes visible."""
        super().showEvent(event)
        if self.case_data and self.case_folder and not self._downloads:
            self.refresh()

    def shutdown(self) -> None:
        """Gracefully stop all background workers before widget destruction."""
        # Stop current worker
        if self._worker is not None:
            # Disconnect signals first to prevent callbacks during shutdown
            try:
                self._worker.finished.disconnect()
                self._worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            if self._worker.isRunning():
                self._worker.requestInterruption()
                self._worker.quit()
                if not self._worker.wait(2000):
                    logger.warning("DownloadsListWorker did not stop in 2s, terminating")
                    self._worker.terminate()
                    self._worker.wait(500)
            self._worker = None

        # Stop all pending workers
        for worker in self._pending_workers:
            try:
                worker.finished.disconnect()
                worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            if worker.isRunning():
                worker.requestInterruption()
                worker.quit()
                if not worker.wait(1000):
                    logger.warning("Pending DownloadsListWorker did not stop, terminating")
                    worker.terminate()
                    worker.wait(500)
        self._pending_workers.clear()

        logger.debug("DownloadedFilesPanel shutdown complete")


class DownloadSettingsPanel(QWidget):
    """Collapsible settings panel for downloads."""

    settings_changed = Signal(dict)

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._setup_ui()

    def _setup_ui(self):
        layout = QFormLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)

        # Max concurrent downloads
        self.concurrent_spin = QSpinBox()
        self.concurrent_spin.setRange(1, 10)
        self.concurrent_spin.setValue(3)
        self.concurrent_spin.setToolTip("Maximum number of concurrent downloads")
        layout.addRow("Max Concurrent:", self.concurrent_spin)

        # Max file size
        self.max_size_spin = QSpinBox()
        self.max_size_spin.setRange(1, 1000)
        self.max_size_spin.setValue(100)
        self.max_size_spin.setSuffix(" MB")
        self.max_size_spin.setToolTip("Maximum file size per download")
        layout.addRow("Max File Size:", self.max_size_spin)

        # Timeout
        self.timeout_spin = QSpinBox()
        self.timeout_spin.setRange(5, 300)
        self.timeout_spin.setValue(30)
        self.timeout_spin.setSuffix(" s")
        self.timeout_spin.setToolTip("Timeout for each download request")
        layout.addRow("Timeout:", self.timeout_spin)

        # Generate thumbnails
        self.thumbs_check = QCheckBox("Generate thumbnails for images")
        self.thumbs_check.setChecked(True)
        layout.addRow("", self.thumbs_check)

    def get_settings(self) -> Dict[str, Any]:
        """Get current settings."""
        return {
            "concurrency": self.concurrent_spin.value(),
            "max_size_mb": self.max_size_spin.value(),
            "timeout_s": self.timeout_spin.value(),
            "generate_thumbnails": self.thumbs_check.isChecked(),
        }
