"""
Downloaded Images subtab - view downloaded image files with thumbnails.

Extracted from downloads/tab.py
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from PySide6.QtCore import Qt, Signal, QSize, QModelIndex, QRect, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QCheckBox,
    QPushButton,
    QLabel,
    QLineEdit,
    QComboBox,
    QMessageBox,
    QListView,
    QAbstractItemView,
    QMenu,
    QApplication,
    QStyledItemDelegate,
    QStyleOptionViewItem,
    QStyleOptionButton,
    QStyle,
    QFileDialog,
)
from PySide6.QtGui import QDesktopServices, QPainter, QPixmap, QIcon
from PySide6.QtCore import QUrl

from app.data.case_data import CaseDataAccess
from app.features.downloads.workers import DownloadsListWorker
from app.features.downloads.helpers import get_downloads_folder

logger = logging.getLogger(__name__)


class DownloadThumbnailDelegate(QStyledItemDelegate):
    """Delegate for downloaded image thumbnails with checkbox."""

    CHECKBOX_SIZE = 20
    CHECKBOX_MARGIN = 4

    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index: QModelIndex) -> None:
        opt = QStyleOptionViewItem(option)
        opt.features &= ~QStyleOptionViewItem.HasCheckIndicator
        super().paint(painter, opt, index)

        # Only draw checkbox if item has a check state AND has an icon (valid thumbnail)
        check_state = index.data(Qt.CheckStateRole)
        if check_state is None:
            return

        # Skip checkbox for items without icons (no valid thumbnail)
        icon = index.data(Qt.DecorationRole)
        if icon is None or (hasattr(icon, 'isNull') and icon.isNull()):
            return

        checkbox_rect = QRect(
            option.rect.left() + self.CHECKBOX_MARGIN,
            option.rect.top() + self.CHECKBOX_MARGIN,
            self.CHECKBOX_SIZE,
            self.CHECKBOX_SIZE,
        )

        style = option.widget.style() if option.widget else QApplication.style()
        checkbox_option = QStyleOptionButton()
        checkbox_option.rect = checkbox_rect
        checkbox_option.state = QStyle.State_Enabled

        if check_state == Qt.Checked:
            checkbox_option.state |= QStyle.State_On
        else:
            checkbox_option.state |= QStyle.State_Off

        painter.save()
        painter.setBrush(Qt.white)
        painter.setPen(Qt.NoPen)
        painter.setOpacity(0.7)
        painter.drawRoundedRect(checkbox_rect.adjusted(-2, -2, 2, 2), 3, 3)
        painter.restore()

        style.drawControl(QStyle.CE_CheckBox, checkbox_option, painter)

    def editorEvent(self, event, model, option, index) -> bool:
        from PySide6.QtCore import QEvent
        from PySide6.QtGui import QMouseEvent

        if event.type() != QEvent.MouseButtonRelease:
            return super().editorEvent(event, model, option, index)

        # Skip checkbox interaction for items without icons
        icon = index.data(Qt.DecorationRole)
        if icon is None or (hasattr(icon, 'isNull') and icon.isNull()):
            return super().editorEvent(event, model, option, index)

        checkbox_rect = QRect(
            option.rect.left() + self.CHECKBOX_MARGIN,
            option.rect.top() + self.CHECKBOX_MARGIN,
            self.CHECKBOX_SIZE,
            self.CHECKBOX_SIZE,
        )

        if isinstance(event, QMouseEvent) and checkbox_rect.contains(event.pos()):
            current_state = index.data(Qt.CheckStateRole)
            if current_state is not None:
                new_state = Qt.Unchecked if current_state == Qt.Checked else Qt.Checked
                model.setData(index, new_state, Qt.CheckStateRole)
                return True

        return super().editorEvent(event, model, option, index)


class DownloadedImagesPanel(QWidget):
    """Panel for viewing downloaded images with thumbnails."""

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
        self._total_count = 0
        self._selected_ids: Set[int] = set()
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

        # Thumbnail grid (using QListView in icon mode)
        self.list_view = QListView()
        self.list_view.setViewMode(QListView.IconMode)
        self.list_view.setIconSize(QSize(150, 150))
        self.list_view.setSpacing(8)
        self.list_view.setResizeMode(QListView.Adjust)
        self.list_view.setWrapping(True)
        self.list_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.list_view.setMovement(QListView.Static)
        self.list_view.doubleClicked.connect(self._on_item_double_clicked)
        self.list_view.setContextMenuPolicy(Qt.CustomContextMenu)
        self.list_view.customContextMenuRequested.connect(self._show_context_menu)

        # Use custom delegate for checkbox
        self.list_view.setItemDelegate(DownloadThumbnailDelegate(self.list_view))

        layout.addWidget(self.list_view, 1)

        # Actions bar
        actions_layout = QHBoxLayout()

        self.tag_btn = QPushButton("Tag Selected")
        self.tag_btn.clicked.connect(self._on_tag_clicked)
        actions_layout.addWidget(self.tag_btn)

        self.export_btn = QPushButton("Export")
        self.export_btn.clicked.connect(self._on_export_clicked)
        actions_layout.addWidget(self.export_btn)

        self.hash_btn = QPushButton("Check Hashes")
        self.hash_btn.clicked.connect(self._on_hash_check_clicked)
        actions_layout.addWidget(self.hash_btn)

        self.similar_btn = QPushButton("Find Similar")
        self.similar_btn.clicked.connect(self._on_find_similar_clicked)
        actions_layout.addWidget(self.similar_btn)

        actions_layout.addStretch()

        self.open_folder_btn = QPushButton("Open Folder")
        self.open_folder_btn.clicked.connect(self._on_open_folder_clicked)
        actions_layout.addWidget(self.open_folder_btn)

        layout.addLayout(actions_layout)

        # Status
        self.status_label = QLabel("0 images downloaded")
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

            domain = self.domain_filter.currentData()

            self._worker = DownloadsListWorker(
                self.case_folder,
                self.case_db_path,
                self.evidence_id,
                file_type="image",
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

        self._downloads = rows
        self._total_count = total
        self._populate_grid()
        self.status_label.setText(f"{total} images downloaded")

        # Load domains for filter
        self._load_domains()

    def _on_load_error(self, error: str, generation: int = 0):
        """Handle load error."""
        # Ignore errors from stale workers
        if generation != self._worker_generation:
            return

        self.status_label.setText(f"Error: {error}")

    def _load_domains(self):
        """Load unique domains for filter dropdown."""
        if not self.case_data:
            return

        try:
            domains = self.case_data.list_download_domains(self.evidence_id)
            current = self.domain_filter.currentData()
            self.domain_filter.clear()
            self.domain_filter.addItem("All", "")
            for domain in domains:
                self.domain_filter.addItem(domain, domain)

            # Restore selection
            if current:
                idx = self.domain_filter.findData(current)
                if idx >= 0:
                    self.domain_filter.setCurrentIndex(idx)
        except Exception as e:
            logger.warning("Failed to load domains: %s", e)

    def _populate_grid(self):
        """Populate thumbnail grid."""
        from PySide6.QtGui import QStandardItemModel, QStandardItem

        model = QStandardItemModel()

        for download in self._downloads:
            # Load thumbnail first - only add item if we have a valid thumbnail
            dest_path = download.get("dest_path")
            pixmap = None

            if dest_path and self.case_folder:
                full_path = self.case_folder / dest_path
                if full_path.exists():
                    # Thumbnail stored inside _downloads/thumbnails/ folder
                    downloads_folder = get_downloads_folder(
                        self.case_folder, self.evidence_id, self.case_db_path
                    )
                    if downloads_folder:
                        thumb_dir = downloads_folder / "thumbnails"
                        from app.services.thumbnailer import ensure_thumbnail
                        try:
                            thumb_path = ensure_thumbnail(full_path, thumb_dir)
                            pixmap = QPixmap(str(thumb_path))
                            if pixmap.isNull():
                                pixmap = None
                        except Exception:
                            pass

            # Skip items without valid thumbnails
            if pixmap is None:
                continue

            item = QStandardItem()
            item.setData(download, Qt.UserRole)
            item.setCheckable(True)
            item.setCheckState(Qt.Unchecked)
            item.setIcon(QIcon(pixmap))

            # Set tooltip
            tooltip = f"URL: {download.get('url', '')}\n"
            tooltip += f"Size: {self._format_size(download.get('size_bytes', 0))}\n"
            if download.get("sha256"):
                tooltip += f"SHA256: {download['sha256'][:16]}..."
            item.setToolTip(tooltip)

            model.appendRow(item)

        self.list_view.setModel(model)

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format file size for display."""
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
        model = self.list_view.model()
        if not model:
            return []

        selected = []
        for i in range(model.rowCount()):
            item = model.item(i)
            if item and item.checkState() == Qt.Checked:
                data = item.data(Qt.UserRole)
                if data:
                    selected.append(data)

        return selected

    def _on_item_double_clicked(self, index: QModelIndex):
        """Handle double-click on image."""
        model = self.list_view.model()
        if not model:
            return

        item = model.itemFromIndex(index)
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
        """Show context menu for image."""
        index = self.list_view.indexAt(pos)
        if not index.isValid():
            return

        model = self.list_view.model()
        item = model.itemFromIndex(index)
        if not item:
            return

        download = item.data(Qt.UserRole)
        if not download:
            return

        menu = QMenu(self)

        open_action = menu.addAction("Open Image")
        open_action.triggered.connect(lambda: self._open_download(download))

        folder_action = menu.addAction("Open Containing Folder")
        folder_action.triggered.connect(lambda: self._open_containing_folder(download))

        menu.addSeparator()

        copy_url = menu.addAction("Copy Source URL")
        copy_url.triggered.connect(lambda: QApplication.clipboard().setText(download.get("url", "")))

        if download.get("sha256"):
            copy_sha = menu.addAction("Copy SHA256")
            copy_sha.triggered.connect(lambda: QApplication.clipboard().setText(download.get("sha256", "")))

        menu.exec_(self.list_view.mapToGlobal(pos))

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
        """Tag selected downloaded images."""
        selected = self._get_selected_downloads()
        if not selected:
            QMessageBox.information(self, "No Selection", "Select images to tag.")
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
        """Export selected images."""
        selected = self._get_selected_downloads()
        if not selected:
            QMessageBox.information(self, "No Selection", "Select images to export.")
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

        QMessageBox.information(self, "Export Complete", f"Exported {exported} images.")

    def _on_hash_check_clicked(self):
        """Check selected downloaded images against hash lists."""
        selected = self._get_selected_downloads()
        if not selected:
            QMessageBox.information(self, "No Selection", "Select images to check.")
            return

        if not self.case_data or not self.evidence_id:
            QMessageBox.warning(self, "Error", "No case data available.")
            return

        from core.matching import ReferenceListManager
        from app.common.dialogs import HashListSelectorDialog

        # Get available hash lists
        ref_manager = ReferenceListManager()
        available = ref_manager.list_available()
        available_hashlists = available.get("hashlists", [])

        if not available_hashlists:
            QMessageBox.information(
                self,
                "No Hash Lists",
                "No hash lists found.\n\n"
                "Add hash lists in Settings → Preferences → Hash Lists tab."
            )
            return

        # Show selection dialog
        from PySide6.QtWidgets import QDialog
        dialog = HashListSelectorDialog(available_hashlists, self)
        if dialog.exec() != QDialog.Accepted:
            return

        selected_lists = dialog.get_selected_lists()
        if not selected_lists:
            return

        # Check selected downloads against hash lists
        matched = 0
        for download in selected:
            sha256 = download.get("sha256", "").lower()
            md5 = download.get("md5", "").lower()

            for list_name in selected_lists:
                try:
                    hashes = ref_manager.load_hashlist(list_name)
                    if (sha256 and sha256 in hashes) or (md5 and md5 in hashes):
                        matched += 1
                        break
                except Exception as e:
                    logger.warning("Failed to load hash list %s: %s", list_name, e)

        QMessageBox.information(
            self,
            "Hash Check Complete",
            f"Checked {len(selected)} images.\n"
            f"Found {matched} matching hash list entries."
        )

    def _on_find_similar_clicked(self):
        """Find similar images using pHash."""
        selected = self._get_selected_downloads()
        if not selected:
            QMessageBox.information(self, "No Selection", "Select an image to find similar.")
            return

        if len(selected) > 1:
            QMessageBox.information(
                self,
                "Single Selection Required",
                "Please select only one image to use as the reference for similarity search."
            )
            return

        if not self.case_data or not self.evidence_id:
            QMessageBox.warning(self, "Error", "No case data available.")
            return

        from core.phash import hamming_distance

        target = selected[0]
        target_phash = target.get("phash")

        if not target_phash:
            QMessageBox.warning(
                self,
                "No pHash",
                "The selected image does not have a perceptual hash.\n"
                "Only images processed after download have pHash computed."
            )
            return

        # Find similar among all downloads
        threshold = 10  # Default Hamming distance threshold
        similar = []

        # Get all downloaded images with pHash
        all_downloads = self.case_data.list_downloads(
            self.evidence_id,
            file_type="image",
            status_filter="completed",
            limit=10000,
        )

        for download in all_downloads:
            phash = download.get("phash")
            if phash and phash != target_phash:
                dist = hamming_distance(target_phash, phash)
                if dist <= threshold:
                    download["hamming_distance"] = dist
                    similar.append(download)

        # Sort by distance
        similar.sort(key=lambda x: x["hamming_distance"])

        if not similar:
            QMessageBox.information(
                self,
                "No Similar Images",
                f"No similar images found (threshold: Hamming distance ≤ {threshold})."
            )
            return

        # Show results in a dialog
        msg = f"Found {len(similar)} similar images (Hamming distance ≤ {threshold}):\n\n"
        for i, img in enumerate(similar[:10]):  # Show top 10
            filename = img.get("filename", "unknown")
            dist = img["hamming_distance"]
            msg += f"  • {filename} (distance: {dist})\n"

        if len(similar) > 10:
            msg += f"\n...and {len(similar) - 10} more."

        QMessageBox.information(self, "Similar Images Found", msg)

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

        logger.debug("DownloadedImagesPanel shutdown complete")
