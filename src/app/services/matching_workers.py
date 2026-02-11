"""
Reference list matching workers.

Provides QThread workers for background reference list matching operations.
Consolidates duplicate MatchWorker patterns from file_list and urls tabs.

Extracted from features/file_list/tab.py and features/urls/tab.py
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from PySide6.QtCore import QThread, Signal

if TYPE_CHECKING:
    from core.database import DatabaseManager

from core.logging import get_logger

logger = get_logger(__name__)


class FileListMatchWorker(QThread):
    """
    Worker thread for file list reference matching.

    Matches file_list entries against hash lists and file lists
    using ReferenceListMatcher.

    Extracted from features/file_list/tab.py
    """

    progress = Signal(int, int)  # current, total
    finished = Signal(dict)  # results: {list_name: match_count}
    error = Signal(str)  # error message

    def __init__(
        self,
        db_manager: "DatabaseManager",
        evidence_id: int,
        selected_lists: List[Tuple[str, str]],
    ):
        """
        Initialize file list match worker.

        Args:
            db_manager: DatabaseManager instance (creates new connection in thread)
            evidence_id: Evidence ID
            selected_lists: List of (list_type, list_name) tuples
                - list_type: "hashlist" or "filelist"
                - list_name: Name of the reference list
        """
        super().__init__()
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.selected_lists = selected_lists

    def run(self):
        """Run matching in background thread."""
        try:
            from core.matching import ReferenceListMatcher

            # Get evidence label from case database
            label = self._get_evidence_label()

            # Get evidence database connection (thread-local)
            evidence_conn = self.db_manager.get_evidence_conn(
                self.evidence_id, label=label
            )

            try:
                matcher = ReferenceListMatcher(evidence_conn, self.evidence_id)
                results = {}

                total_lists = len(self.selected_lists)
                for i, (list_type, list_name) in enumerate(self.selected_lists):
                    def progress_callback(current: int, total: int):
                        # Report progress across all lists
                        if total > 0:
                            overall_current = (i * 1000) + int(current * 1000 / total)
                        else:
                            overall_current = i * 1000
                        overall_total = total_lists * 1000
                        self.progress.emit(overall_current, overall_total)

                    if list_type == "hashlist":
                        matches = matcher.match_hashlist(list_name, progress_callback)
                    else:  # filelist
                        matches = matcher.match_filelist(list_name, progress_callback)

                    results[list_name] = matches

                self.finished.emit(results)

            finally:
                evidence_conn.close()

        except Exception as e:
            logger.exception("FileListMatchWorker error")
            self.error.emit(str(e))

    def _get_evidence_label(self) -> str:
        """Get evidence label from case database."""
        with sqlite3.connect(self.db_manager.case_db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT label FROM evidences WHERE id = ?",
                (self.evidence_id,),
            ).fetchone()
            return row["label"] if row and row["label"] else f"EV-{self.evidence_id:03d}"


class UrlMatchWorker(QThread):
    """
    Worker thread for URL reference list matching.

    Matches URLs against URL lists using URLMatcher.

    Extracted from features/urls/tab.py
    """

    progress = Signal(int, int)  # current, total
    finished = Signal(dict)  # results: {list_name: match_count}
    error = Signal(str)  # error message

    def __init__(
        self,
        db_manager: "DatabaseManager",
        evidence_id: int,
        selected_lists: List[Tuple[str, str]],
    ):
        """
        Initialize URL match worker.

        Args:
            db_manager: DatabaseManager instance
            evidence_id: Evidence ID
            selected_lists: List of (list_name, list_path) tuples
        """
        super().__init__()
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.selected_lists = selected_lists

    def run(self):
        """Run URL matching in background thread."""
        try:
            from core.matching import URLMatcher

            # Get evidence label from case database
            label = self._get_evidence_label()

            # Get evidence database connection (thread-local)
            evidence_conn = self.db_manager.get_evidence_conn(
                self.evidence_id, label=label
            )

            try:
                matcher = URLMatcher(evidence_conn, self.evidence_id)
                results = {}

                total_lists = len(self.selected_lists)
                for i, (list_name, list_path) in enumerate(self.selected_lists):
                    def progress_callback(current: int, total: int):
                        # Report progress across all lists
                        if total > 0:
                            overall_current = (i * 1000) + int(current * 1000 / total)
                        else:
                            overall_current = i * 1000
                        overall_total = total_lists * 1000
                        self.progress.emit(overall_current, overall_total)

                    # Match URLs against this list
                    match_result = matcher.match_urls(list_name, list_path, progress_callback)
                    results[list_name] = match_result["matched"]

                self.finished.emit(results)

            finally:
                evidence_conn.close()

        except Exception as e:
            import traceback
            logger.exception("UrlMatchWorker error")
            error_detail = f"{str(e)}\n{traceback.format_exc()}"
            self.error.emit(error_detail)

    def _get_evidence_label(self) -> str:
        """Get evidence label from case database."""
        with sqlite3.connect(self.db_manager.case_db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT label FROM evidences WHERE id = ?",
                (self.evidence_id,),
            ).fetchone()
            return row["label"] if row and row["label"] else f"EV-{self.evidence_id:03d}"


# Backward compatibility aliases
MatchWorker = FileListMatchWorker
