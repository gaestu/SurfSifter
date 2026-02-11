"""
Downloads feature workers.

Provides QThread workers for background data loading operations.

Extracted from features/downloads/tab.py
Added backfill of file_extension column for optimized queries.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QThread, Signal

from app.data.case_data import CaseDataAccess

logger = logging.getLogger(__name__)


class AvailableUrlsWorker(QThread):
    """
    Background worker for loading downloadable URLs.

    Creates own CaseDataAccess instance for thread safety.
    CaseDataAccess._use_evidence_conn is not thread-safe, so each worker
    must have its own instance to avoid race conditions.

    Added tag_filter and match_filter parameters for extended filtering.
    Runs file_extension backfill on first page load for optimized queries.
    """

    finished = Signal(list, int)  # (rows, total_count)
    backfill_progress = Signal(str)  # Status message during backfill
    error = Signal(str)

    def __init__(
        self,
        case_folder: Path,
        case_db_path: Path,
        evidence_id: int,
        file_type: Optional[str],
        domain_filter: Optional[str],
        search_text: Optional[str],
        tag_filter: Optional[str] = None,
        match_filter: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
    ):
        super().__init__()
        self.case_folder = case_folder
        self.case_db_path = case_db_path
        self.evidence_id = evidence_id
        self.file_type = file_type
        self.domain_filter = domain_filter
        self.search_text = search_text
        self.tag_filter = tag_filter
        self.match_filter = match_filter
        self.limit = limit
        self.offset = offset

    def run(self):
        try:
            # Create thread-local CaseDataAccess instance
            # Use context manager to ensure connections are closed
            with CaseDataAccess(self.case_folder, self.case_db_path) as case_data:
                # Run backfill on first page (offset=0) to enable optimized queries
                # This is idempotent - only processes URLs where file_extension IS NULL
                if self.offset == 0:
                    status = case_data.get_extension_backfill_status(self.evidence_id)
                    if status['pending'] > 0:
                        self.backfill_progress.emit(
                            f"Indexing {status['pending']} URLs for fast search..."
                        )
                        updated, elapsed = case_data.backfill_url_extensions(self.evidence_id)
                        if updated > 0:
                            logger.info(
                                "Backfilled %d URL extensions in %.2fs",
                                updated, elapsed
                            )

                rows = case_data.list_downloadable_urls(
                    self.evidence_id,
                    file_type=self.file_type,
                    domain_filter=self.domain_filter,
                    search_text=self.search_text,
                    tag_filter=self.tag_filter,
                    match_filter=self.match_filter,
                    limit=self.limit,
                    offset=self.offset,
                )
                count = case_data.count_downloadable_urls(
                    self.evidence_id,
                    file_type=self.file_type,
                    domain_filter=self.domain_filter,
                    search_text=self.search_text,
                    tag_filter=self.tag_filter,
                    match_filter=self.match_filter,
                )
            self.finished.emit(rows, count)
        except Exception as e:
            logger.error("AvailableUrlsWorker error: %s", e, exc_info=True)
            self.error.emit(str(e))


class DownloadsListWorker(QThread):
    """
    Background worker for loading completed downloads.

    Creates own CaseDataAccess instance for thread safety.
    """

    finished = Signal(list, int)  # (rows, total_count)
    error = Signal(str)

    def __init__(
        self,
        case_folder: Path,
        case_db_path: Path,
        evidence_id: int,
        file_type: Optional[str],
        status_filter: str = "completed",
        domain_filter: Optional[str] = None,
        search_text: Optional[str] = None,
        limit: int = 500,
        offset: int = 0,
    ):
        super().__init__()
        self.case_folder = case_folder
        self.case_db_path = case_db_path
        self.evidence_id = evidence_id
        self.file_type = file_type
        self.status_filter = status_filter
        self.domain_filter = domain_filter
        self.search_text = search_text
        self.limit = limit
        self.offset = offset

    def run(self):
        try:
            # Create thread-local CaseDataAccess instance
            # Use context manager to ensure connections are closed
            with CaseDataAccess(self.case_folder, self.case_db_path) as case_data:
                rows = case_data.list_downloads(
                    self.evidence_id,
                    file_type=self.file_type,
                    status_filter=self.status_filter,
                    domain_filter=self.domain_filter,
                    search_text=self.search_text,
                    limit=self.limit,
                    offset=self.offset,
                )
                count = case_data.count_downloads(
                    self.evidence_id,
                    file_type=self.file_type,
                    status_filter=self.status_filter,
                )
            self.finished.emit(rows, count)
        except Exception as e:
            logger.error("DownloadsListWorker error: %s", e, exc_info=True)
            self.error.emit(str(e))


class TabCountsWorker(QThread):
    """
    Background worker for loading download tab counts.

    Prevents main thread freeze by computing counts in background.
    """

    finished = Signal(int, int, int)  # (available_count, images_count, other_count)
    error = Signal(str)

    def __init__(
        self,
        case_folder: Path,
        case_db_path: Path,
        evidence_id: int,
    ):
        super().__init__()
        self.case_folder = case_folder
        self.case_db_path = case_db_path
        self.evidence_id = evidence_id

    def run(self):
        try:
            # Use context manager to ensure connections are closed
            with CaseDataAccess(self.case_folder, self.case_db_path) as case_data:
                # Get download stats (fast query on downloads table)
                stats = case_data.get_download_stats(self.evidence_id)
                images_count = stats.get("completed_image", 0)
                completed = stats.get("completed", 0)
                other_count = completed - images_count

                # Count downloadable URLs (slower query - avoid if possible)
                # For initial display, we can just show "..." until loaded
                available = case_data.count_downloadable_urls(self.evidence_id)

            self.finished.emit(available, images_count, other_count)
        except Exception as e:
            logger.error("TabCountsWorker error: %s", e, exc_info=True)
            self.error.emit(str(e))
