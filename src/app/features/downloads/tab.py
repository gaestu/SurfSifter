"""
Download Tab Widget

Refactored download management with three subtabs.
Workers extracted to separate module.
Subtabs extracted to separate modules.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtCore import Qt, Signal, QTimer
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QTabWidget,
    QPushButton,
    QLabel,
)

from app.data.case_data import CaseDataAccess

# Import subtab panels
from app.features.downloads.subtab_available import AvailableDownloadsPanel
from app.features.downloads.subtab_images import DownloadedImagesPanel, DownloadThumbnailDelegate
from app.features.downloads.subtab_files import DownloadedFilesPanel, DownloadSettingsPanel
from app.features.downloads.workers import (
    TabCountsWorker,
    AvailableUrlsWorker,
    DownloadsListWorker,
)
from app.features.downloads.helpers import get_downloads_folder

logger = logging.getLogger(__name__)

__all__ = ["DownloadTab"]


class DownloadTab(QWidget):
    """
    Download management tab with three subtabs.

    Replaces the old download.py implementation with a self-contained
    download workflow.

    Tab counts now loaded in background to prevent UI freeze.
    """

    # Signals
    download_started = Signal(list)  # List of URL dicts to download
    download_paused = Signal()
    download_cancelled = Signal()

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
        self._counts_worker: Optional[TabCountsWorker] = None
        self._pending_counts_workers: List[TabCountsWorker] = []  # Keep old workers alive until finished
        self._counts_worker_generation = 0  # Track worker generation to ignore stale results

        # Stale data flag for lazy refresh after ingestion
        self._data_stale = False

        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(4, 4, 4, 4)

        # Header with title and settings
        header_layout = QHBoxLayout()

        header_label = QLabel("<b>DOWNLOAD MANAGER</b>")
        header_layout.addWidget(header_label)

        header_layout.addStretch()

        # Settings toggle (expandable)
        self.settings_btn = QPushButton("⚙ Settings")
        self.settings_btn.setCheckable(True)
        self.settings_btn.clicked.connect(self._toggle_settings)
        header_layout.addWidget(self.settings_btn)

        layout.addLayout(header_layout)

        # Settings panel (hidden by default)
        self.settings_panel = DownloadSettingsPanel()
        self.settings_panel.setVisible(False)
        layout.addWidget(self.settings_panel)

        # Tab widget with three subtabs
        self.subtabs = QTabWidget()

        # Subtab 1: Available Downloads
        self.available_panel = AvailableDownloadsPanel(
            self.evidence_id,
            self.case_data,
            self.case_folder,
        )
        self.available_panel.download_requested.connect(self._on_download_requested)
        self.subtabs.addTab(self.available_panel, "Available")

        # Subtab 2: Downloaded Images
        self.images_panel = DownloadedImagesPanel(
            self.evidence_id,
            self.case_data,
            self.case_folder,
        )
        self.subtabs.addTab(self.images_panel, "Images")

        # Subtab 3: Downloaded Others
        self.files_panel = DownloadedFilesPanel(
            self.evidence_id,
            self.case_data,
            self.case_folder,
        )
        self.subtabs.addTab(self.files_panel, "Other Files")

        # Update tab labels with counts
        self._update_tab_counts()

        layout.addWidget(self.subtabs, 1)

    def _toggle_settings(self):
        """Toggle settings panel visibility."""
        self.settings_panel.setVisible(self.settings_btn.isChecked())

    def _update_tab_counts(self):
        """
        Update tab labels with download counts.

        Runs in background to prevent UI freeze on large datasets.
        Uses generation tracking instead of terminate() to avoid deadlocks.
        """
        if not self.case_data or not self.case_folder or not self.case_db_path:
            return

        # Increment generation - any in-flight workers with old generation will be ignored
        self._counts_worker_generation += 1
        current_gen = self._counts_worker_generation

        # Keep old worker alive until it finishes (prevents QThread crash)
        if self._counts_worker and self._counts_worker.isRunning():
            self._pending_counts_workers.append(self._counts_worker)

        # Clean up finished workers from pending list
        self._pending_counts_workers = [w for w in self._pending_counts_workers if w.isRunning()]

        self._counts_worker = TabCountsWorker(
            self.case_folder,
            self.case_db_path,
            self.evidence_id,
        )
        # Use lambda to capture generation for staleness check
        self._counts_worker.finished.connect(
            lambda avail, img, other, gen=current_gen: self._on_tab_counts_loaded(avail, img, other, gen)
        )
        self._counts_worker.error.connect(
            lambda err, gen=current_gen: self._on_tab_counts_error(err, gen)
        )
        self._counts_worker.start()

    def _on_tab_counts_loaded(self, available: int, images: int, other: int, generation: int = 0):
        """Handle tab counts loaded from worker."""
        # Ignore stale results from old workers
        if generation != self._counts_worker_generation:
            logger.debug("Ignoring stale TabCountsWorker result (gen %d vs current %d)", generation, self._counts_worker_generation)
            return

        self.subtabs.setTabText(0, f"Available ({available})")
        self.subtabs.setTabText(1, f"Images ({images})")
        self.subtabs.setTabText(2, f"Other Files ({other})")

    def _on_tab_counts_error(self, error: str, generation: int = 0):
        """Handle tab counts load error."""
        # Ignore errors from stale workers
        if generation != self._counts_worker_generation:
            return

        logger.warning("Failed to update tab counts: %s", error)

    def _on_download_requested(self, selected_items: List[Dict]):
        """Handle download request from Available panel."""
        self.download_started.emit(selected_items)

    def set_case_data(self, case_data: CaseDataAccess, case_folder: Optional[Path] = None):
        """Update case data reference and refresh all panels."""
        self.case_data = case_data
        self.case_db_path = case_data.db_path if case_data else None
        if case_folder:
            self.case_folder = case_folder

        self.available_panel.set_case_data(case_data, case_folder)
        self.images_panel.set_case_data(case_data, case_folder)
        self.files_panel.set_case_data(case_data, case_folder)

        self._update_tab_counts()

    def refresh(self):
        """Refresh all panels."""
        self.available_panel.refresh()
        self.images_panel.refresh()
        self.files_panel.refresh()
        self._update_tab_counts()

    def mark_stale(self) -> None:
        """Mark data as stale - will refresh on next showEvent.

        Part of lazy refresh pattern to prevent UI freezes.
        Called by main.py when data changes but tab is not visible.
        """
        self._data_stale = True

    def showEvent(self, event):
        """Refresh when tab becomes visible."""
        super().showEvent(event)
        # Refresh if data was marked stale while tab was hidden
        if self._data_stale:
            self._data_stale = False
            QTimer.singleShot(10, self.refresh)

    def shutdown(self) -> None:
        """Gracefully stop all background workers before widget destruction."""
        # Stop counts worker
        if self._counts_worker is not None:
            # Disconnect signals first to prevent callbacks during shutdown
            try:
                self._counts_worker.finished.disconnect()
                self._counts_worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            if self._counts_worker.isRunning():
                self._counts_worker.requestInterruption()
                self._counts_worker.quit()
                if not self._counts_worker.wait(2000):
                    logger.warning("TabCountsWorker did not stop in 2s, terminating")
                    self._counts_worker.terminate()
                    self._counts_worker.wait(500)
            self._counts_worker = None

        # Stop all pending counts workers
        for worker in self._pending_counts_workers:
            try:
                worker.finished.disconnect()
                worker.error.disconnect()
            except (RuntimeError, TypeError):
                pass
            if worker.isRunning():
                worker.requestInterruption()
                worker.quit()
                if not worker.wait(1000):
                    logger.warning("Pending TabCountsWorker did not stop, terminating")
                    worker.terminate()
                    worker.wait(500)
        self._pending_counts_workers.clear()

        # Shutdown child panels
        self.available_panel.shutdown()
        self.images_panel.shutdown()
        self.files_panel.shutdown()

        logger.debug("DownloadTab shutdown complete")
