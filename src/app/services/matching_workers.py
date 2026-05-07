"""Reference list matching service workers."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple

from PySide6.QtCore import QThread, Signal

from core.database import create_process_log, finalize_process_log, insert_hash_matches
from core.logging import get_logger
from core.matching import ReferenceListManager

if TYPE_CHECKING:
    from core.database import DatabaseManager

logger = get_logger(__name__)


class HashCheckWorker(QThread):
    """Worker thread for checking images against text hash lists."""

    progress = Signal(int, int)  # current, total
    finished = Signal(dict)  # results: {list_name: match_count}
    error = Signal(str)  # error message

    def __init__(self, db_manager, evidence_id: int, selected_hashlists: List[str]):
        super().__init__()
        self.db_manager = db_manager
        self.evidence_id = evidence_id
        self.selected_hashlists = selected_hashlists

    def run(self):
        """Run hash checking in background thread."""
        evidence_conn = None
        log_id = None
        finalized = False
        results = {}
        list_errors = []
        try:
            with closing(sqlite3.connect(self.db_manager.case_db_path)) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT label FROM evidences WHERE id = ?",
                    (self.evidence_id,),
                ).fetchone()
                label = row["label"] if row and row["label"] else f"EV-{self.evidence_id:03d}"

            evidence_conn = self.db_manager.get_evidence_conn(self.evidence_id, label=label)
            ref_manager = ReferenceListManager(create_dirs=False)

            cursor = evidence_conn.execute(
                """SELECT id, md5, sha256 FROM images
                WHERE evidence_id = ? AND (md5 IS NOT NULL OR sha256 IS NOT NULL)""",
                (self.evidence_id,),
            )
            images = cursor.fetchall()
            total_images = len(images)

            valid_hashlists = []
            for hashlist_name in self.selected_hashlists:
                try:
                    valid_hashlists.append(ref_manager.validate_list_name(hashlist_name))
                except ValueError as e:
                    results[hashlist_name] = f"Error: {e}"
                    list_errors.append(_format_list_error(hashlist_name, str(e)))

            log_id = create_process_log(
                evidence_conn,
                self.evidence_id,
                "hash_check",
                "lists={lists} images={images}".format(
                    lists=json.dumps(sorted(self.selected_hashlists), ensure_ascii=True),
                    images=total_images,
                ),
            )
            run_id = f"hash_check:{log_id}"
            valid_total = len(valid_hashlists)

            for list_idx, hashlist_name in enumerate(valid_hashlists):
                self._raise_if_interrupted()
                try:
                    hashes, list_version = ref_manager.load_hashlist_with_version(hashlist_name)
                    if not hashes:
                        results[hashlist_name] = 0
                        continue

                    matches = []
                    for img_idx, img_row in enumerate(images):
                        self._raise_if_interrupted()
                        image_id, md5, sha256 = img_row

                        matched = False
                        matched_hash = None
                        if md5 and md5.lower() in hashes:
                            matched = True
                            matched_hash = md5.lower()
                        elif sha256 and sha256.lower() in hashes:
                            matched = True
                            matched_hash = sha256.lower()

                        if matched:
                            matches.append({
                                "image_id": image_id,
                                "db_name": hashlist_name,
                                "db_md5": matched_hash,
                                "list_name": hashlist_name,
                                "list_version": list_version,
                                "note": None,
                                "hash_sha256": sha256.lower() if sha256 else None,
                                "run_id": run_id,
                            })

                        if (img_idx + 1) % 50 == 0:
                            overall_progress = (list_idx * total_images) + img_idx + 1
                            overall_total = valid_total * total_images
                            self.progress.emit(overall_progress, overall_total)

                    if matches:
                        insert_hash_matches(evidence_conn, self.evidence_id, matches)

                    results[hashlist_name] = len(matches)

                except _HashCheckCancelled:
                    raise
                except FileNotFoundError:
                    message = "hash list not found"
                    results[hashlist_name] = f"Error: {message}"
                    list_errors.append(_format_list_error(hashlist_name, message))
                except Exception as e:
                    results[hashlist_name] = f"Error: {e}"
                    list_errors.append(_format_list_error(hashlist_name, str(e)))

            evidence_conn.commit()
            if log_id is not None:
                total_matches = sum(v for v in results.values() if isinstance(v, int))
                finalize_process_log(
                    evidence_conn,
                    log_id,
                    exit_code=1 if list_errors else 0,
                    stdout=f"matches={total_matches}",
                    stderr="\n".join(list_errors),
                )
                finalized = True

            self.finished.emit(results)

        except _HashCheckCancelled:
            if evidence_conn is not None and log_id is not None and not finalized:
                total_matches = sum(v for v in results.values() if isinstance(v, int))
                finalize_process_log(
                    evidence_conn,
                    log_id,
                    exit_code=1,
                    stdout=f"matches={total_matches}",
                    stderr=_format_error("cancelled"),
                )
                finalized = True
        except Exception as e:
            if evidence_conn is not None and log_id is not None and not finalized:
                finalize_process_log(
                    evidence_conn,
                    log_id,
                    exit_code=1,
                    stdout="",
                    stderr=_format_error(str(e)),
                )
                finalized = True
            self.error.emit(str(e))
        finally:
            if evidence_conn is not None:
                evidence_conn.close()

    def _raise_if_interrupted(self) -> None:
        if self.isInterruptionRequested():
            raise _HashCheckCancelled()


class _HashCheckCancelled(Exception):
    """Raised when hash checking is cancelled cooperatively."""


def _format_list_error(list_name: str, message: str) -> str:
    return json.dumps(
        {"list": list_name, "error": message},
        ensure_ascii=True,
        sort_keys=True,
    )


def _format_error(message: str) -> str:
    return json.dumps({"error": message}, ensure_ascii=True, sort_keys=True)


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

            if self.isInterruptionRequested():
                return

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
                    if self.isInterruptionRequested():
                        logger.info("UrlMatchWorker interrupted before list '%s'", list_name)
                        return

                    def progress_callback(current: int, total: int):
                        if self.isInterruptionRequested():
                            raise InterruptedError("URL matching interrupted")

                        # Report progress across all lists
                        if total > 0:
                            overall_current = (i * 1000) + int(current * 1000 / total)
                        else:
                            overall_current = i * 1000
                        overall_total = total_lists * 1000
                        self.progress.emit(overall_current, overall_total)

                    # Match URLs against this list
                    match_result = matcher.match_urls(
                        list_name,
                        list_path,
                        progress_callback,
                        should_interrupt=self.isInterruptionRequested,
                    )
                    results[list_name] = match_result["matched"]

                if self.isInterruptionRequested():
                    logger.info("UrlMatchWorker interrupted after processing lists")
                    return
                self.finished.emit(results)

            finally:
                evidence_conn.close()
                try:
                    self.db_manager.close_thread_connections()
                except Exception:
                    pass

        except InterruptedError:
            logger.info("UrlMatchWorker interrupted")
            return
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
