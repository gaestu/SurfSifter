from __future__ import annotations

import sqlite3
import traceback
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from PySide6.QtCore import QObject, QRunnable, QThread, QThreadPool, Signal, Slot

from core.evidence_fs import MountedFS, PyEwfTskFS, find_ewf_segments
from core.validation import validate_case_quick, validate_case_full
from core.logging import get_logger

if TYPE_CHECKING:
    from core.audit_logging import AuditLogger, EvidenceLogger
from core.extraction_orchestrator import run_extraction_pipeline
from core.database import (
    create_process_log,
    finalize_process_log,
    insert_download_audit,
    insert_hash_matches,
)
from core.database import DatabaseManager, slugify_label
from app.services.net_download import DownloadRequest, download_items, sanitize_filename
from app.config.settings import NetworkSettings
from app.data.case_data import CaseDataAccess

_worker_logger = get_logger("app.services.workers")


# -----------------------------------------------------------------------------
# Validation Worker (moved from main.py )
# -----------------------------------------------------------------------------

class ValidationWorkerSignals(QObject):
    """Signals for ValidationWorker."""
    finished = Signal(object)  # ValidationReport
    error = Signal(str)  # Error message


class ValidationWorker(QRunnable):
    """Worker to run case validation in background thread."""

    def __init__(self, case_path: Path, quick: bool = True):
        super().__init__()
        self.case_path = case_path
        self.quick = quick
        self.signals = ValidationWorkerSignals()

    @Slot()
    def run(self):
        """Run validation and emit results."""
        try:
            if self.quick:
                report = validate_case_quick(self.case_path)
            else:
                report = validate_case_full(self.case_path)
            self.signals.finished.emit(report)
        except Exception as exc:
            _worker_logger.exception("Validation worker failed")
            self.signals.error.emit(str(exc))


# -----------------------------------------------------------------------------
# Task Signals and Base Classes
# -----------------------------------------------------------------------------

class TaskSignals(QObject):
    progress = Signal(int, str)
    result = Signal(object)
    error = Signal(str, str)
    finished = Signal()
    step_update = Signal(str, str, str)  # key, status, message/label


class DownloadTaskSignals(TaskSignals):
    item_progress = Signal(int, int, str)
    # Extended signal with download_id and md5 for database persistence
    # Args: item_id, ok, path, error, bytes_written, sha256, content_type, duration_s, download_id, md5
    item_finished = Signal(int, bool, str, str, int, str, str, float, int, str)


class HashLookupSignals(TaskSignals):
    progress = Signal(int, int)


class BaseTask(QRunnable):
    """Base QRunnable that emits signals for progress, result, and errors."""

    def __init__(self) -> None:
        super().__init__()
        self.signals = TaskSignals()
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True
        # If this is an ExecutorTask, also kill any active subprocess
        if hasattr(self, '_active_process') and self._active_process:
            try:
                import signal
                # Check if process is still running
                if self._active_process.poll() is None:
                    self._active_process.send_signal(signal.SIGTERM)
                    from core.logging import get_logger
                    logger = get_logger("app.workers")
                    logger.info("Sent SIGTERM to subprocess (PID: %s)", self._active_process.pid)
                else:
                    from core.logging import get_logger
                    logger = get_logger("app.workers")
                    logger.debug("Subprocess already exited (returncode: %s)", self._active_process.returncode)
            except Exception as e:
                from core.logging import get_logger
                logger = get_logger("app.workers")
                logger.error("Failed to kill subprocess: %s", e)

    def is_cancelled(self) -> bool:
        return self._cancelled

    def raise_if_cancelled(self) -> None:
        if self._cancelled:
            raise TaskCancelled()

    def report_progress(self, percent: int, message: str) -> None:
        try:
            self.signals.progress.emit(percent, message)
        except RuntimeError:
            # Signal receiver deleted (dialog closed) - silently ignore
            pass

    def report_step(self, key: str, status: str, message: str = "") -> None:
        """Emit structured step updates (key/status/message) for the GUI."""
        try:
            self.signals.step_update.emit(key, status, message)
        except RuntimeError:
            from core.logging import get_logger
            logger = get_logger("app.workers")
            logger.debug("Step signal not emitted - receiver deleted (%s:%s)", key, status)

    def run(self) -> None:  # noqa: D401
        try:
            result = self.run_task()
        except TaskCancelled:
            self._safe_emit_finished()
            return
        except Exception as exc:  # pragma: no cover - defensive
            tb = traceback.format_exc()
            self._safe_emit_error(str(exc), tb)
        else:
            self._safe_emit_result(result)
        finally:
            self._safe_emit_finished()

    def _safe_emit_result(self, result: Any) -> None:
        """Safely emit result signal, ignoring if receiver deleted."""
        try:
            self.signals.result.emit(result)
        except RuntimeError:
            # Signal receiver deleted (dialog closed during cancellation)
            from core.logging import get_logger
            logger = get_logger("app.workers")
            logger.debug("Result signal not emitted - receiver deleted")

    def _safe_emit_error(self, error: str, traceback_str: str) -> None:
        """Safely emit error signal, ignoring if receiver deleted."""
        try:
            self.signals.error.emit(error, traceback_str)
        except RuntimeError:
            # Signal receiver deleted (dialog closed during cancellation)
            from core.logging import get_logger
            logger = get_logger("app.workers")
            logger.warning("Error signal not emitted - receiver deleted: %s", error)

    def _safe_emit_finished(self) -> None:
        """Safely emit finished signal, ignoring if receiver deleted."""
        try:
            self.signals.finished.emit()
        except RuntimeError:
            # Signal receiver deleted (dialog closed during cancellation)
            from core.logging import get_logger
            logger = get_logger("app.workers")
            logger.debug("Finished signal not emitted - receiver deleted")

    def run_task(self) -> Any:
        raise NotImplementedError


@dataclass(frozen=True)
class ExecutorTaskConfig:
    case_root: Path
    db_path: Path
    evidence_id: int
    mount_root: Optional[Path] = None
    ewf_paths: Optional[List[Path]] = None
    partition_index: int = -1  # -1 = auto-detect, 0 = direct FS, 1+ = specific partition
    selected_extractors: Optional[List[str]] = None  # NEW: Filter rules by extractor type
    db_manager: Optional[DatabaseManager] = None
    evidence_label: Optional[str] = None
    # bulk_extractor configuration
    bulk_extractor_scanners: Optional[List[str]] = None  # Scanner list (None/[] = all, ['email'] = URLs-only)
    bulk_extractor_threads: Optional[int] = None  # Thread count (None = auto-detect)
    bulk_extractor_existing_policy: str = "overwrite"  # "overwrite" | "reuse" | "skip"
    bulk_extractor_delete_existing: bool = False  # Whether to delete existing DB records before ingesting


class ExecutorTask(BaseTask):
    """Runs the full rule executor pipeline for a single evidence."""

    def __init__(self, config: ExecutorTaskConfig) -> None:
        super().__init__()
        self.config = config
        self._active_process = None  # Track subprocess for cancellation

    def run_task(self) -> None:
        self.report_progress(0, "Preparing extraction")
        self.raise_if_cancelled()

        self.report_progress(10, "Preparing evidence filesystem")
        self.raise_if_cancelled()

        # Choose filesystem backend based on configuration
        if self.config.ewf_paths:
            # Direct E01 reading via pyewf + pytsk3
            try:
                fs = PyEwfTskFS(self.config.ewf_paths, partition_index=self.config.partition_index)
            except RuntimeError as exc:
                raise RuntimeError(
                    f"Unable to open E01 image directly: {exc}\n"
                    "Please ensure pyewf and pytsk3 are installed, or mount the image manually."
                ) from exc
        elif self.config.mount_root:
            # Traditional mounted filesystem
            fs = MountedFS(self.config.mount_root)
        else:
            raise ValueError("Either ewf_paths or mount_root must be provided")

        self.report_progress(20, "Starting extraction pipeline")
        self.raise_if_cancelled()
        case_conn: Optional[sqlite3.Connection] = None
        evidence_conn: Optional[sqlite3.Connection] = None
        try:
            manager = self.config.db_manager or DatabaseManager(
                self.config.case_root,
                case_db_path=self.config.db_path,
            )
            case_conn = manager.get_case_conn()
            label = self.config.evidence_label
            if label is None:
                case_conn.row_factory = sqlite3.Row
                label_row = case_conn.execute(
                    "SELECT label FROM evidences WHERE id = ?",
                    (self.config.evidence_id,),
                ).fetchone()
                label = label_row["label"] if label_row else None
            evidence_conn = manager.get_evidence_conn(self.config.evidence_id, label)
            evidence_db_path = manager.evidence_db_path(self.config.evidence_id, label, create_dirs=False)
        except Exception as exc:
            raise RuntimeError(f"Failed to open case database: {exc}") from exc

        try:
            # Create a log callback that forwards messages to the GUI via progress signal
            def log_callback(message: str):
                # Also log to system logger
                from core.logging import get_logger
                logger = get_logger("core.extraction_orchestrator")
                logger.info(message)

                # Parse progress from bulk_extractor messages
                # Format: "bulk_extractor: fraction_read: 45.3 %"
                import re
                progress_match = re.search(r'fraction_read:\s+([\d.]+)\s*%', message)
                if progress_match:
                    # Scale 0-100% bulk_extractor progress to 20-80% overall progress
                    # (reserve 0-20% for init, 80-100% for post-processing)
                    bulk_pct = float(progress_match.group(1))
                    overall_pct = 20 + int(bulk_pct * 0.6)  # 20% + (0-100% * 60%)
                    self.report_progress(overall_pct, message)
                else:
                    # For non-progress messages, keep at base 20%
                    self.report_progress(20, message)

            # Create a process callback to track the subprocess
            def process_callback(process):
                self._active_process = process

            def step_callback(step_key: str, status: str, message: str = ""):
                self.report_step(step_key, status, message)

            summary = run_extraction_pipeline(
                fs=fs,
                case_conn=case_conn,
                evidence_conn=evidence_conn,
                evidence_id=self.config.evidence_id,
                case_root=self.config.case_root,
                selected_extractors=self.config.selected_extractors,
                bulk_extractor_scanners=self.config.bulk_extractor_scanners,  #
                bulk_extractor_threads=self.config.bulk_extractor_threads,  #
                bulk_extractor_existing_policy=self.config.bulk_extractor_existing_policy,  #
                bulk_extractor_delete_existing=self.config.bulk_extractor_delete_existing,  #
                evidence_db_path=evidence_db_path,  # For bulk_extractor output location
                log_cb=log_callback,  # Pass GUI logging callback
                process_cb=process_callback,  # Track subprocess for cancellation
                step_cb=step_callback,  # Structured step updates for GUI
            )
            return None
        finally:
            if evidence_conn is not None:
                evidence_conn.close()
            if case_conn is not None:
                case_conn.close()


# =============================================================================
# Timeline Build Task (- Phase 5)
# =============================================================================

@dataclass(frozen=True)
class TimelineBuildConfig:
    """Configuration for timeline build task."""
    case_root: Path
    db_path: Path
    evidence_id: int
    rules_dir: Optional[Path] = None  # Deprecated: ignored since
    db_manager: Optional[DatabaseManager] = None


class TimelineBuildTask(BaseTask):
    """Background task to build timeline from all artifact sources.

    This task builds a unified timeline from 13 artifact sources:
    - browser_history, urls, images, os_indicators
    - cookies, bookmarks, browser_downloads, session_tabs
    - autofill, credentials, media_playback
    - hsts_entries, jump_list_entries

    It generates 19 distinct event kinds from these sources and persists
    them to the timeline table in the evidence database.
    """

    def __init__(self, config: TimelineBuildConfig) -> None:
        super().__init__()
        self.config = config

    def run_task(self) -> int:
        """Build and persist timeline. Returns event count."""
        from app.features.timeline.config import load_timeline_config
        from app.features.timeline.engine import build_timeline, persist_timeline

        self.report_progress(0, "Loading timeline configuration...")

        # Config is now hardcoded, rules_dir is ignored
        config = load_timeline_config()

        self.report_progress(10, "Opening evidence database...")

        # Create or get database connection via manager
        manager = self.config.db_manager or DatabaseManager(
            self.config.case_root,
            case_db_path=self.config.db_path,
        )

        case_conn: Optional[sqlite3.Connection] = None
        evidence_conn: Optional[sqlite3.Connection] = None

        try:
            # Get evidence label from case DB
            case_conn = manager.get_case_conn()
            case_conn.row_factory = sqlite3.Row
            label_row = case_conn.execute(
                "SELECT label FROM evidences WHERE id = ?",
                (self.config.evidence_id,)
            ).fetchone()
            label = label_row["label"] if label_row else None

            evidence_conn = manager.get_evidence_conn(self.config.evidence_id, label)

            self.report_progress(20, "Building timeline from artifact sources...")
            self.raise_if_cancelled()

            # Build with progress callbacks
            def progress_adapter(pct: float, msg: str) -> None:
                # Scale 0.0-1.0 to 20-80%
                scaled = 20 + int(pct * 60)
                self.report_progress(scaled, msg)
                self.raise_if_cancelled()

            events = build_timeline(
                evidence_conn,
                self.config.evidence_id,
                config,
                progress_cb=progress_adapter
            )

            self.report_progress(80, f"Persisting {len(events)} events...")
            self.raise_if_cancelled()

            # Pass evidence_id to ensure stale rows are deleted even if no events
            count = persist_timeline(evidence_conn, events, evidence_id=self.config.evidence_id)

            self.report_progress(100, f"Timeline built: {count} events")
            return count

        finally:
            if evidence_conn is not None:
                evidence_conn.close()
            if case_conn is not None:
                case_conn.close()


def start_task(task: BaseTask, pool: Optional[QThreadPool] = None) -> None:
    thread_pool = pool or QThreadPool.globalInstance()
    thread_pool.start(task)


class TaskCancelled(Exception):
    """Raised when a task is cancelled cooperatively."""


class HashLookupTask(BaseTask):
    """
    Task for looking up image hashes against a hash database.

    Phase 4: Updated to support both:
    - Legacy schema: images(md5, note)
    - New schema: hash_entries(hash_md5, hash_sha256, note) + hash_lists
    """

    def __init__(
        self,
        case_db_path: Path,
        hash_db_path: Path,
        evidence_id: int,
        image_ids: List[int],
        db_manager: Optional[DatabaseManager] = None,
    ) -> None:
        super().__init__()
        self.signals = HashLookupSignals()
        self.case_db_path = case_db_path
        self.hash_db_path = hash_db_path
        self.evidence_id = evidence_id
        self.image_ids = image_ids
        self.db_manager = db_manager

    def _detect_schema(self, hash_conn: sqlite3.Connection) -> str:
        """Detect hash database schema type."""
        cursor = hash_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='hash_entries'"
        )
        if cursor.fetchone():
            return "new"  # Phase 4 schema
        return "legacy"  # Old schema with images(md5, note)

    def _lookup_legacy(
        self, hash_conn: sqlite3.Connection, md5_value: str, sha256_value: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Look up hash using legacy schema: images(md5, note)."""
        hash_row = hash_conn.execute(
            "SELECT note FROM images WHERE md5 = ?",
            (md5_value,),
        ).fetchone()
        if hash_row:
            note = hash_row["note"] if "note" in hash_row.keys() else ""
            return {
                "db_md5": md5_value,
                "note": note,
                "list_name": self.hash_db_path.name,
                "list_version": None,
                "hash_sha256": None,
            }
        return None

    def _lookup_new(
        self, hash_conn: sqlite3.Connection, md5_value: str, sha256_value: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Look up hash using new schema: hash_entries + hash_lists."""
        # Build query for MD5 or SHA256 match
        conditions = []
        params = []

        if md5_value:
            conditions.append("he.hash_md5 = ?")
            params.append(md5_value.lower())
        if sha256_value:
            conditions.append("he.hash_sha256 = ?")
            params.append(sha256_value.lower())

        if not conditions:
            return None

        sql = f"""
            SELECT he.hash_md5, he.hash_sha256, he.note,
                   hl.name as list_name, hl.source_file_hash as list_version
            FROM hash_entries he
            JOIN hash_lists hl ON he.list_id = hl.id
            WHERE {' OR '.join(conditions)}
            LIMIT 1
        """

        hash_row = hash_conn.execute(sql, params).fetchone()
        if hash_row:
            return {
                "db_md5": hash_row["hash_md5"] or md5_value,
                "note": hash_row["note"] or "",
                "list_name": hash_row["list_name"],
                "list_version": hash_row["list_version"],
                "hash_sha256": hash_row["hash_sha256"],
            }
        return None

    def run_task(self) -> List[Tuple[int, str, str]]:
        if not self.hash_db_path.exists():
            raise RuntimeError("Hash database not found")
        if not self.image_ids:
            return []
        manager = self.db_manager or DatabaseManager(
            self.case_db_path.parent,
            case_db_path=self.case_db_path,
        )
        case_conn = manager.get_case_conn()
        case_conn.row_factory = sqlite3.Row
        hash_conn = sqlite3.connect(self.hash_db_path)
        hash_conn.row_factory = sqlite3.Row

        # Detect schema type
        schema_type = self._detect_schema(hash_conn)

        label_row = case_conn.execute(
            "SELECT label FROM evidences WHERE id = ?",
            (self.evidence_id,),
        ).fetchone()
        label = label_row["label"] if label_row else None
        evidence_conn = manager.get_evidence_conn(self.evidence_id, label)
        persisted: List[Dict[str, Any]] = []
        matches: List[Tuple[int, str, str]] = []
        log_id = create_process_log(
            evidence_conn,
            self.evidence_id,
            "hash_lookup",
            f"images={len(self.image_ids)} schema={schema_type}",
        )
        try:
            total = len(self.image_ids)
            self.signals.progress.emit(0, total)
            for idx, image_id in enumerate(self.image_ids, start=1):
                self.raise_if_cancelled()
                self.signals.progress.emit(idx - 1, total)

                # Get image hashes (MD5 and SHA256 if available)
                image_row = evidence_conn.execute(
                    "SELECT id, md5, sha256 FROM images WHERE id = ?",
                    (image_id,),
                ).fetchone()
                if not image_row:
                    continue
                md5_value = image_row["md5"]
                sha256_value = image_row["sha256"] if "sha256" in image_row.keys() else None

                if not md5_value and not sha256_value:
                    continue

                # Look up hash
                if schema_type == "new":
                    match = self._lookup_new(hash_conn, md5_value, sha256_value)
                else:
                    match = self._lookup_legacy(hash_conn, md5_value, sha256_value)

                if match:
                    persisted.append({
                        "image_id": image_id,
                        "db_name": self.hash_db_path.name,
                        "db_md5": match["db_md5"],
                        "note": match["note"],
                        "list_name": match.get("list_name"),
                        "list_version": match.get("list_version"),
                        "hash_sha256": match.get("hash_sha256"),
                    })
                    matches.append((image_id, match["db_md5"], match["note"] or ""))

            if persisted:
                insert_hash_matches(evidence_conn, self.evidence_id, persisted)
            finalize_process_log(
                evidence_conn,
                log_id,
                exit_code=0,
                stdout=f"matches={len(matches)} schema={schema_type}",
                stderr="",
            )
            self.signals.progress.emit(total, total)
            return matches
        except TaskCancelled:
            finalize_process_log(evidence_conn, log_id, exit_code=1, stdout="", stderr="cancelled")
            raise
        except Exception as exc:
            finalize_process_log(evidence_conn, log_id, exit_code=1, stdout="", stderr=str(exc))
            raise
        finally:
            evidence_conn.close()
            case_conn.close()
            hash_conn.close()


@dataclass
class DownloadTaskConfig:
    case_root: Path
    case_db_path: Path
    evidence_id: int
    items: List[Dict[str, Any]]  # each requires url, filename, domain
    network: NetworkSettings
    db_manager: Optional[DatabaseManager] = None
    evidence_label: Optional[str] = None  # Required for evidence DB connection
    caller_info: str = "download_tab"


class DownloadTask(BaseTask):
    def __init__(self, config: DownloadTaskConfig) -> None:
        super().__init__()
        self.signals = DownloadTaskSignals()
        self.config = config

    @staticmethod
    def _map_download_outcome(error: Optional[str], ok: bool, status_code: Optional[int]) -> tuple[str, bool, Optional[str]]:
        """
        Map net_download result fields to canonical audit outcome.

        Returns:
            (outcome, blocked, reason)
        """
        if ok:
            return ("success", False, None)

        reason = (error or "").strip() or None
        lower_reason = (reason or "").lower()

        if lower_reason == "cancelled":
            return ("cancelled", False, reason)

        if lower_reason.startswith("blocked content-type"):
            return ("blocked", True, reason)

        if status_code is not None:
            return ("failed", False, reason or f"HTTP {status_code}")

        if lower_reason.startswith("http ") or lower_reason.startswith("size limit exceeded"):
            return ("failed", False, reason)

        return ("error", False, reason or "unknown error")

    def run_task(self) -> Dict[str, Any]:
        manager = self.config.db_manager or DatabaseManager(
            self.config.case_root,
            case_db_path=self.config.case_db_path,
        )
        # Retrieve evidence label if not provided
        label = self.config.evidence_label
        if label is None:
            case_conn = manager.get_case_conn()
            case_conn.row_factory = sqlite3.Row
            label_row = case_conn.execute(
                "SELECT label FROM evidences WHERE id = ?",
                (self.config.evidence_id,),
            ).fetchone()
            label = label_row["label"] if label_row else None
            case_conn.close()
        conn = manager.get_evidence_conn(self.config.evidence_id, label)
        log_id = create_process_log(
            conn,
            self.config.evidence_id,
            "download",
            f"items={len(self.config.items)}",
        )
        try:
            # Compute evidence slug for correct folder path
            evidence_slug = slugify_label(label, self.config.evidence_id)
            base_dir = (
                self.config.case_root
                / "evidences"
                / evidence_slug
                / "_downloads"
            )
            requests_list: List[DownloadRequest] = []
            # Map item_id to download_id for database updates
            download_id_map: Dict[int, int] = {}

            for idx, item in enumerate(self.config.items, start=1):
                domain = item.get("domain") or "misc"
                domain_safe = sanitize_filename(domain)
                filename = sanitize_filename(item.get("filename") or f"download-{idx}")
                dest = base_dir / domain_safe / filename
                requests_list.append(
                    DownloadRequest(
                        item_id=idx,
                        url=item["url"],
                        dest_path=dest,
                        domain=domain_safe,
                    )
                )
                # Store download_id if provided (for database wiring)
                if item.get("download_id"):
                    download_id_map[idx] = item["download_id"]

            def progress_cb(item_id: int, pct: int, note: str) -> None:
                self.signals.item_progress.emit(item_id, pct, note)
                self.report_progress(pct, note)

            results = download_items(
                requests_list,
                concurrency=self.config.network.concurrency,
                timeout_s=self.config.network.timeout_s,
                retries=self.config.network.retries,
                max_bytes=self.config.network.max_bytes,
                allowed_content_types=self.config.network.allowed_content_types,
                progress_cb=progress_cb,
                should_cancel=self.is_cancelled,
            )
            ok = sum(1 for res in results if res.ok)
            for res in results:
                # Include download_id and md5 in signal
                download_id = download_id_map.get(res.item_id, 0)
                self.signals.item_finished.emit(
                    res.item_id,
                    res.ok,
                    str(res.dest_path) if res.dest_path else "",
                    (res.error or ""),
                    res.bytes_written,
                    res.sha256 or "",
                    res.content_type or "",
                    float(res.duration_s),
                    download_id,
                    res.md5 or "",
                )
                outcome, blocked, reason = self._map_download_outcome(
                    res.error,
                    res.ok,
                    res.status_code,
                )
                insert_download_audit(
                    conn,
                    self.config.evidence_id,
                    res.url,
                    "GET",
                    outcome,
                    blocked=blocked,
                    reason=reason,
                    status_code=res.status_code,
                    attempts=res.attempts,
                    duration_s=res.duration_s,
                    bytes_written=res.bytes_written,
                    content_type=res.content_type,
                    caller_info=self.config.caller_info,
                )
            conn.commit()
            log_lines = []
            for res in results:
                dest_display = str(res.dest_path) if res.dest_path else "-"
                status_display = res.status_code if res.status_code is not None else "NA"
                line = (
                    f"id={res.item_id} url={res.url} status={status_display} "
                    f"bytes={res.bytes_written} sha={res.sha256 or '-'} md5={res.md5 or '-'} "
                    f"ctype={res.content_type or '-'} dur={res.duration_s:.2f}s dest={dest_display}"
                )
                if res.error:
                    line += f" error={res.error}"
                log_lines.append(line)
            summary = f"ok={ok} failed={len(results) - ok}"
            stdout = summary if not log_lines else summary + "\n" + "\n".join(log_lines)
            finalize_process_log(
                conn,
                log_id,
                exit_code=0,
                stdout=stdout,
                stderr="",
            )
            return {
                "results": [
                    {
                        **res.__dict__,
                        "dest_path": str(res.dest_path) if res.dest_path else "",
                    }
                    for res in results
                ]
            }
        finally:
            conn.close()


# --- Case Loading Task (Phase 2: Background Loading) ---


@dataclass(frozen=True)
class CaseLoadTaskConfig:
    """Configuration for background case loading."""
    case_path: Path
    db_path: Optional[Path] = None  # Auto-detected if None


@dataclass
class CaseLoadResult:
    """Result of background case loading."""
    case_path: Path
    db_path: Path
    db_manager: DatabaseManager
    case_metadata: Dict[str, Any]
    evidences: List[Dict[str, Any]]
    error: Optional[str] = None


class CaseLoadTask(BaseTask):
    """
    Background task for loading case data without blocking the UI.

    Phase 2 implementation: Loads case database, metadata, and evidence list
    in a background thread while showing a progress dialog.
    """

    def __init__(self, config: CaseLoadTaskConfig) -> None:
        super().__init__()
        self.config = config

    def run_task(self) -> CaseLoadResult:
        from core.database import DatabaseManager, find_case_database

        case_path = self.config.case_path
        db_path = self.config.db_path

        # Step 1: Find case database
        self.report_progress(10, "Locating case database...")
        self.raise_if_cancelled()

        if db_path is None:
            db_path = find_case_database(case_path)
            if db_path is None:
                return CaseLoadResult(
                    case_path=case_path,
                    db_path=Path(),
                    db_manager=None,
                    case_metadata={},
                    evidences=[],
                    error=f"No case database found in: {case_path}"
                )

        db_path = db_path.resolve()

        # Step 2: Initialize database manager
        self.report_progress(30, "Opening database...")
        self.raise_if_cancelled()

        try:
            db_manager = DatabaseManager(case_path, case_db_path=db_path)
        except Exception as exc:
            return CaseLoadResult(
                case_path=case_path,
                db_path=db_path,
                db_manager=None,
                case_metadata={},
                evidences=[],
                error=f"Failed to open case database: {exc}"
            )

        # Step 3: Load case metadata
        self.report_progress(50, "Loading case metadata...")
        self.raise_if_cancelled()

        try:
            case_data = CaseDataAccess(case_path, db_manager=db_manager)
            case_metadata = case_data.get_case_metadata()
        except Exception as exc:
            return CaseLoadResult(
                case_path=case_path,
                db_path=db_path,
                db_manager=db_manager,
                case_metadata={},
                evidences=[],
                error=f"Failed to load case metadata: {exc}"
            )

        # Step 4: List evidences (metadata only, no artifact loading)
        self.report_progress(70, "Loading evidence list...")
        self.raise_if_cancelled()

        try:
            evidences = case_data.list_evidences()
        except Exception as exc:
            return CaseLoadResult(
                case_path=case_path,
                db_path=db_path,
                db_manager=db_manager,
                case_metadata=case_metadata,
                evidences=[],
                error=f"Failed to list evidences: {exc}"
            )

        # Step 5: Ensure case record exists
        self.report_progress(90, "Finalizing...")
        self.raise_if_cancelled()

        # Check/create case record if needed
        conn = db_manager.get_case_conn()
        try:
            row = conn.execute("SELECT id FROM cases LIMIT 1").fetchone()
            if not row:
                import time
                folder_name = case_path.name
                case_number = folder_name
                case_name = folder_name
                if folder_name.endswith("_browser_analyzing"):
                    case_number = folder_name[:-len("_browser_analyzing")]
                    case_name = case_number
                utc_now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                conn.execute(
                    """
                    INSERT INTO cases(case_id, title, created_at_utc, case_number, case_name)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (case_number, case_name, utc_now, case_number, case_name),
                )
                conn.commit()
                # Reload metadata after insert
                case_metadata = case_data.get_case_metadata()
        finally:
            pass  # Connection managed by DatabaseManager

        self.report_progress(100, "Case loaded successfully")

        return CaseLoadResult(
            case_path=case_path,
            db_path=db_path,
            db_manager=db_manager,
            case_metadata=case_metadata,
            evidences=evidences,
            error=None
        )


# --- Download Post-Processing Task ---


class DownloadPostProcessSignals(TaskSignals):
    """Signals for download post-processing task."""
    # item_id, download_id, success, phash (or empty), error
    item_processed = Signal(int, int, bool, str, str)


@dataclass(frozen=True)
class DownloadPostProcessConfig:
    """Configuration for download post-processing."""
    case_root: Path
    case_db_path: Path
    evidence_id: int
    # List of (item_id, download_id, file_path) tuples to process
    items: List[Tuple[int, int, Path]]
    db_manager: Optional[DatabaseManager] = None
    evidence_label: Optional[str] = None  # For computing thumbnail path


class DownloadPostProcessTask(BaseTask):
    """
    Post-process downloaded images: compute pHash, extract EXIF, get dimensions.

    Runs after successful image downloads to populate image metadata fields.
    """

    def __init__(self, config: DownloadPostProcessConfig) -> None:
        super().__init__()
        self.signals = DownloadPostProcessSignals()
        self.config = config

    def run_task(self) -> Dict[str, Any]:
        import json
        from PIL import Image, UnidentifiedImageError
        from core.image_codecs import ensure_pillow_heif_registered
        from core.phash import compute_phash
        from extractors._shared.carving.exif import extract_exif
        from app.services.thumbnailer import ensure_thumbnail

        manager = self.config.db_manager or DatabaseManager(
            self.config.case_root,
            case_db_path=self.config.case_db_path,
        )

        case_data = CaseDataAccess(
            self.config.case_root,
            self.config.case_db_path,
            db_manager=manager,
        )

        # Compute evidence slug for thumbnail path
        evidence_label = self.config.evidence_label
        if evidence_label is None:
            case_conn = manager.get_case_conn()
            case_conn.row_factory = sqlite3.Row
            label_row = case_conn.execute(
                "SELECT label FROM evidences WHERE id = ?",
                (self.config.evidence_id,),
            ).fetchone()
            evidence_label = label_row["label"] if label_row else None
            case_conn.close()

        evidence_slug = slugify_label(evidence_label, self.config.evidence_id) if evidence_label else f"evidence_{self.config.evidence_id}"

        processed = 0
        failed = 0
        total = len(self.config.items)

        for idx, (item_id, download_id, file_path) in enumerate(self.config.items):
            self.raise_if_cancelled()

            pct = int((idx / total) * 100) if total > 0 else 0
            self.report_progress(pct, f"Processing image {idx + 1}/{total}")

            if not file_path.exists():
                failed += 1
                self.signals.item_processed.emit(item_id, download_id, False, "", "File not found")
                continue

            try:
                # Compute perceptual hash
                phash = compute_phash(file_path)

                # Extract EXIF metadata
                exif_data = extract_exif(file_path)
                exif_json = json.dumps(exif_data, sort_keys=True) if exif_data else "{}"

                # Get image dimensions
                width: Optional[int] = None
                height: Optional[int] = None
                try:
                    ensure_pillow_heif_registered()
                    with Image.open(file_path) as img:
                        width, height = img.size
                except (UnidentifiedImageError, OSError):
                    pass

                # Generate thumbnail (inside _downloads/thumbnails/ folder)
                thumb_dir = self.config.case_root / "evidences" / evidence_slug / "_downloads" / "thumbnails"
                try:
                    ensure_thumbnail(file_path, thumb_dir)
                except Exception:
                    pass  # Non-critical

                # Update database
                case_data.update_download_image_metadata(
                    self.config.evidence_id,
                    download_id,
                    phash=phash,
                    exif_json=exif_json,
                    width=width,
                    height=height,
                )

                processed += 1
                self.signals.item_processed.emit(
                    item_id, download_id, True, phash or "", ""
                )

            except Exception as exc:
                failed += 1
                from core.logging import get_logger
                logger = get_logger("app.workers")
                logger.warning("Failed to post-process image %s: %s", file_path, exc)
                self.signals.item_processed.emit(
                    item_id, download_id, False, "", str(exc)
                )

        self.report_progress(100, f"Processed {processed} images, {failed} failed")

        return {
            "processed": processed,
            "failed": failed,
            "total": total,
        }


# -----------------------------------------------------------------------------
# Batch Hash List Import Task
# -----------------------------------------------------------------------------


class BatchHashListImportSignals(TaskSignals):
    """Signals for batch hash list import progress."""
    # current_index, total_count, current_filename
    file_progress = Signal(int, int, str)


@dataclass(frozen=True)
class BatchHashListImportConfig:
    """Configuration for batch hash list import."""
    files: Tuple[Path, ...]  # Use tuple for frozen dataclass
    conflict_policy: str  # "skip", "overwrite", "rename"
    rebuild_db: bool = True


class BatchHashListImportTask(BaseTask):
    """Background task for batch hash list import."""

    def __init__(self, config: BatchHashListImportConfig) -> None:
        super().__init__()
        self.config = config
        self.signals = BatchHashListImportSignals()
        self._results: List[Any] = []  # Will hold ImportResult objects

    def get_results(self) -> List[Any]:
        """Get import results after task completes."""
        return self._results

    def run_task(self) -> Dict[str, Any]:
        from core.matching import ReferenceListManager, ConflictPolicy, ImportResult, rebuild_hash_db

        ref_manager = ReferenceListManager()

        # Map string to enum
        policy_map = {
            "skip": ConflictPolicy.SKIP,
            "overwrite": ConflictPolicy.OVERWRITE,
            "rename": ConflictPolicy.RENAME,
        }
        conflict_policy = policy_map.get(self.config.conflict_policy, ConflictPolicy.SKIP)

        def progress_cb(current: int, total: int, filename: str) -> None:
            self.report_progress(
                int(100 * current / total) if total > 0 else 0,
                f"Importing {filename}..."
            )
            # Also emit file-level progress
            try:
                self.signals.file_progress.emit(current, total, filename)
            except RuntimeError:
                pass  # Signal receiver deleted

        def cancel_cb() -> bool:
            return self._cancelled

        # Run batch import
        self._results = ref_manager.import_hashlist_batch(
            files=list(self.config.files),
            conflict_policy=conflict_policy,
            progress_callback=progress_cb,
            cancel_check=cancel_cb,
        )

        # Calculate summary
        imported = sum(1 for r in self._results if r.status in ("imported", "overwritten", "renamed"))
        skipped = sum(1 for r in self._results if r.status == "skipped")
        errors = sum(1 for r in self._results if r.status == "error")
        cancelled = sum(1 for r in self._results if r.status == "cancelled")

        # Rebuild hash DB if requested and at least one file was imported
        rebuild_success = False
        rebuild_count = 0
        if self.config.rebuild_db and imported > 0 and not self._cancelled:
            self.report_progress(95, "Rebuilding hash database...")
            try:
                hash_db_path = ref_manager.base_path / "hash_database.sqlite"
                rebuild_count = rebuild_hash_db(ref_manager.hashlists_dir, hash_db_path)
                rebuild_success = True
            except Exception as e:
                from core.logging import get_logger
                logger = get_logger("app.workers")
                logger.error(f"Failed to rebuild hash database: {e}")

        self.report_progress(100, "Complete")

        return {
            "imported": imported,
            "skipped": skipped,
            "errors": errors,
            "cancelled": cancelled,
            "total": len(self.config.files),
            "results": self._results,
            "rebuild_success": rebuild_success,
            "rebuild_count": rebuild_count,
        }


# =============================================================================
# Extract & Ingest Worker (moved from features/extraction/workers.py )
# =============================================================================


class ExtractAndIngestWorker(QThread):
    """
    Background worker for combined extraction and ingestion.

    Runs extraction followed by immediate ingestion for multiple extractors,
    emitting progress signals for both phases.

    Signals:
        extractor_started(int, str, str): index, display_name, phase ("extract"/"ingest")
        extractor_finished(int, str, str, bool, str): index, display_name, phase, success, message
        batch_finished(list, list, list, bool): succeeded, skipped, failed, cancelled
        log_message(str): message for logging

    Moved to services/workers.py to fix dependency direction violation.
    """

    extractor_started = Signal(int, str, str)  # index, display_name, phase
    extractor_finished = Signal(int, str, str, bool, str)  # index, display_name, phase, success, message
    batch_finished = Signal(list, list, list, bool)  # succeeded, skipped, failed, cancelled
    log_message = Signal(str)  # log message

    def __init__(
        self,
        extractors: list,
        evidence_fs,
        evidence_source_path,
        evidence_id: int,
        evidence_label: str,
        workspace_dir: Path,
        db_manager,
        overwrite_mode: str = 'overwrite',
        evidence_logger: Optional["EvidenceLogger"] = None,
        parent=None
    ):
        super().__init__(parent)
        self.extractors = extractors
        self.evidence_fs = evidence_fs
        self.evidence_source_path = evidence_source_path
        self.evidence_id = evidence_id
        self.evidence_label = evidence_label
        self.workspace_dir = workspace_dir
        self.db_manager = db_manager
        self.overwrite_mode = overwrite_mode
        self.evidence_logger = evidence_logger
        self._cancelled = False

    def cancel(self):
        """Request cancellation."""
        self._cancelled = True

    def run(self):
        """Run extraction + ingestion in two phases: extract all, then ingest successes."""
        import inspect
        import json
        import shutil
        import time
        from extractors.workers import WorkerCallbacks

        succeeded = []
        skipped = []
        failed = []
        evidence_slug = slugify_label(self.evidence_label, self.evidence_id)

        # Generate run_id for this batch
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self._current_run_id = run_id  # Store for helper methods
        self.log_message.emit(f"🔄 Extract & Ingest batch started (run_id: {run_id})")

        # === PHASE 1: EXTRACTION ===
        extraction_results = []  # List of (extractor, success, message)

        for i, extractor in enumerate(self.extractors):
            if self._cancelled:
                self.log_message.emit("❌ Extract & Ingest cancelled during extraction phase")
                self.batch_finished.emit(succeeded, skipped, failed, self._cancelled)
                return

            meta = extractor.metadata
            self.log_message.emit(f"🔄 Extracting {meta.display_name} ({i+1}/{len(self.extractors)})")

            self.extractor_started.emit(i, meta.name, "extract")
            extract_success, extract_msg = self._run_single_extraction(extractor, evidence_slug, run_id)

            if extract_success is None:
                # Extraction skipped
                skipped.append((meta.display_name, f"Extraction: {extract_msg}"))
                self.log_message.emit(f"⚠️ Skipped {meta.display_name} - {extract_msg}")
                self.extractor_finished.emit(i, meta.name, "extract", False, extract_msg)
                extraction_results.append((extractor, None, extract_msg))
            elif not extract_success:
                # Extraction failed
                failed.append((meta.display_name, f"Extraction failed: {extract_msg}"))
                self.log_message.emit(f"❌ Extraction failed for {meta.display_name} - {extract_msg}")
                self.extractor_finished.emit(i, meta.name, "extract", False, extract_msg)
                extraction_results.append((extractor, False, extract_msg))
            else:
                # Extraction succeeded
                self.log_message.emit(f"✅ Extraction completed for {meta.display_name}")
                self.extractor_finished.emit(i, meta.name, "extract", True, "")
                extraction_results.append((extractor, True, ""))

        # Check cancellation before ingestion phase
        if self._cancelled:
            self.log_message.emit("❌ Extract & Ingest cancelled after extraction phase")
            self.batch_finished.emit(succeeded, skipped, failed, self._cancelled)
            return

        # === PHASE 2: INGESTION ===
        self.log_message.emit(f"📥 Starting ingestion phase (run_id: {run_id})")

        ingest_index = 0
        for i, (extractor, extract_success, extract_msg) in enumerate(extraction_results):
            if self._cancelled:
                self.log_message.emit("❌ Extract & Ingest cancelled during ingestion phase")
                break

            meta = extractor.metadata

            # Skip if extraction didn't succeed
            if extract_success is not True:
                continue

            # Skip if extractor can't ingest
            if not meta.can_ingest:
                succeeded.append(meta.display_name)
                self.log_message.emit(f"✅ Completed {meta.display_name} (no ingestion)")
                continue

            # Run ingestion
            self.log_message.emit(f"📥 Ingesting {meta.display_name} ({ingest_index+1}/{len([e for e, s, _ in extraction_results if s is True and e.metadata.can_ingest])})")
            self.extractor_started.emit(i, meta.name, "ingest")

            ingest_success, ingest_msg = self._run_single_ingestion(extractor, evidence_slug, run_id)

            if ingest_success is None:
                # Ingestion skipped (but extraction succeeded)
                # Check if this is a "data already exists" skip (count as success) vs a real skip
                if "already in database" in ingest_msg.lower() or "already exists" in ingest_msg.lower():
                    # Extractor wrote directly to DB during extraction - count as success
                    succeeded.append(meta.display_name)
                    self.log_message.emit(f"✅ Completed {meta.display_name} (data written during extraction)")
                    self.extractor_finished.emit(i, meta.name, "ingest", True, "")
                else:
                    skipped.append((meta.display_name, f"Ingestion: {ingest_msg}"))
                    self.log_message.emit(f"⚠️ Ingestion skipped for {meta.display_name} - {ingest_msg}")
                    self.extractor_finished.emit(i, meta.name, "ingest", False, ingest_msg)
            elif not ingest_success:
                # Ingestion failed
                failed.append((meta.display_name, f"Ingestion failed: {ingest_msg}"))
                self.log_message.emit(f"❌ Ingestion failed for {meta.display_name} - {ingest_msg}")
                self.extractor_finished.emit(i, meta.name, "ingest", False, ingest_msg)
            else:
                # Both phases succeeded
                succeeded.append(meta.display_name)
                self.log_message.emit(f"✅ Completed {meta.display_name}")
                self.extractor_finished.emit(i, meta.name, "ingest", True, "")

            ingest_index += 1

        self.batch_finished.emit(succeeded, skipped, failed, self._cancelled)

    def _run_single_extraction(self, extractor, evidence_slug: str, run_id: str) -> tuple:
        """
        Run extraction phase for a single extractor.

        Returns:
            (True, "") - Success
            (False, error_message) - Failed
            (None, reason) - Skipped
        """
        import inspect
        import json
        import shutil
        import time
        from core.logging import get_logger
        from extractors.workers import WorkerCallbacks

        logger = get_logger("app.services.workers")
        meta = extractor.metadata

        # Check if can run
        run_sig = inspect.signature(extractor.run_extraction)
        run_params = list(run_sig.parameters.keys())

        can_run_sig = inspect.signature(extractor.can_run_extraction)
        can_run_params = list(can_run_sig.parameters.keys())

        if 'evidence_source_path' in can_run_params:
            can_run, reason = extractor.can_run_extraction(self.evidence_source_path)
        else:
            can_run, reason = extractor.can_run_extraction(self.evidence_fs)

        if not can_run:
            return (None, reason)

        # Get config and inject standard params
        config = getattr(extractor, '_config', {}).copy()
        config['evidence_id'] = self.evidence_id
        config['evidence_label'] = self.evidence_label
        config['db_manager'] = self.db_manager  # For extractors that need DB access

        # Get output directory
        output_dir = extractor.get_output_dir(self.workspace_dir, evidence_slug)

        # Log extraction start and track timing
        process_log_id = None
        fallback_conn = None  # Used if evidence_logger is unavailable
        if self.evidence_logger and run_id:
            try:
                process_log_id = self.evidence_logger.log_extraction_start(
                    extractor=meta.name,
                    run_id=run_id,
                    config=config
                )
            except Exception as e:
                logger.warning(f"Failed to log extraction start: {e}")
        elif run_id and self.db_manager:
            try:
                from core.audit_logging import create_process_log_enhanced
                fallback_conn = self.db_manager.get_evidence_conn(
                    self.evidence_id,
                    self.evidence_label
                )
                process_log_id = create_process_log_enhanced(
                    fallback_conn,
                    self.evidence_id,
                    task=f"extract:{meta.name}",
                    command=None,
                    run_id=run_id,
                    extractor_name=meta.name,
                    log_file_path=None,
                )
                fallback_conn.commit()
            except Exception as e:
                logger.warning(f"Failed to log extraction start (fallback): {e}")

        start_time = time.time()

        # Smart output reuse: only delete if we need to re-extract
        should_extract = True
        if output_dir.exists():
            try:
                manifest_path = output_dir / "manifest.json"
                if manifest_path.exists() and extractor.has_existing_output(output_dir):
                    # Reuse existing output (skip extraction, still log to process_log)
                    should_extract = False
                    logger.info(f"Reusing existing output for {meta.name}")
                else:
                    shutil.rmtree(output_dir)
                    logger.info(f"Removing incomplete output for {meta.name}")
            except Exception as e:
                logger.warning(f"Failed to check/remove existing output for {meta.name}: {e}")
                try:
                    shutil.rmtree(output_dir)
                except Exception:
                    pass

        # Create callbacks
        callbacks = WorkerCallbacks(
            evidence_logger=self.evidence_logger,
            extractor_name=meta.name
        )
        callbacks.log_message.connect(
            lambda msg, level: self.log_message.emit(f"[{meta.name}] {msg}")
        )
        # Also connect error signal to make errors visible in batch runs
        callbacks.error.connect(
            lambda err, details: self.log_message.emit(f"[{meta.name}] ❌ {err}: {details}" if details else f"[{meta.name}] ❌ {err}")
        )

        evidence_conn = None
        try:
            # Create evidence_conn once - used in both config and kwargs
            # This enables extractors to use file_list index for fast discovery
            if self.db_manager:
                evidence_conn = self.db_manager.get_evidence_conn(
                    self.evidence_id,
                    self.evidence_label
                )
                config['evidence_conn'] = evidence_conn

            # Build kwargs
            kwargs = {
                'output_dir': output_dir,
                'config': config,
                'callbacks': callbacks,
            }

            if 'evidence_fs' in run_params:
                kwargs['evidence_fs'] = self.evidence_fs
            if 'evidence_source_path' in run_params:
                kwargs['evidence_source_path'] = self.evidence_source_path
            if 'evidence_conn' in run_params:
                # Also pass as direct kwarg for extractors that declare it in signature
                kwargs['evidence_conn'] = evidence_conn
            if 'evidence_id' in run_params:
                kwargs['evidence_id'] = self.evidence_id

            # If reusing existing output, short-circuit while still logging
            if not should_extract:
                success = True
                elapsed_sec = time.time() - start_time

                # Track statistics for reused output from manifest
                from core.statistics_collector import StatisticsCollector
                stats = StatisticsCollector.instance()
                if stats and run_id:
                    stats.start_run(self.evidence_id, self.evidence_label, meta.name, run_id)
                    # Read manifest to get discovered counts
                    manifest_path = output_dir / "manifest.json"
                    if manifest_path.exists():
                        try:
                            with open(manifest_path) as f:
                                manifest_data = json.load(f)
                            # Report discovered items from manifest
                            # Different extractors store different data
                            files = manifest_data.get('files', [])
                            if files:
                                stats.report_discovered(self.evidence_id, meta.name, files=len(files))
                            # For registry: count extracted_hives
                            hives = manifest_data.get('extracted_hives', 0)
                            if hives:
                                stats.report_discovered(self.evidence_id, meta.name, hives=hives)
                        except Exception as e:
                            logger.debug(f"Could not read manifest for stats: {e}")
                    stats.finish_run(self.evidence_id, meta.name, "ok")
            else:
                output_dir.mkdir(parents=True, exist_ok=True)
                success = extractor.run_extraction(**kwargs)
                elapsed_sec = time.time() - start_time

            # Count records from manifest if available
            records_extracted = 0
            if success:
                try:
                    manifest_path = output_dir / "manifest.json"
                    if manifest_path.exists():
                        with open(manifest_path) as f:
                            manifest_data = json.load(f)
                            records_extracted = manifest_data.get('record_count', 0)
                except Exception:
                    pass

            # Log extraction result to process_log for card updates
            if self.evidence_logger and run_id:
                try:
                    self.evidence_logger.log_extraction_result(
                        extractor=meta.name,
                        run_id=run_id,
                        records=records_extracted,
                        errors=0 if success else 1,
                        elapsed_sec=elapsed_sec,
                        process_log_id=process_log_id
                    )
                except Exception as e:
                    logger.warning(f"Failed to log extraction result: {e}")
            elif process_log_id and fallback_conn and run_id:
                try:
                    from core.audit_logging import finalize_process_log_enhanced
                    finalize_process_log_enhanced(
                        fallback_conn,
                        process_log_id,
                        exit_code=0 if success else 1,
                        records_extracted=records_extracted,
                        records_ingested=0,
                        warnings_json=None,
                    )
                    fallback_conn.commit()
                except Exception as e:
                    logger.warning(f"Failed to finalize extraction (fallback): {e}")

            if success:
                return (True, "")
            else:
                return (False, "Extraction returned failure")

        except Exception as e:
            logger.exception(f"Extraction error for {meta.name}")
            elapsed_sec = time.time() - start_time

            # Log failed extraction
            if self.evidence_logger and run_id:
                try:
                    self.evidence_logger.log_extraction_result(
                        extractor=meta.name,
                        run_id=run_id,
                        records=0,
                        errors=1,
                        elapsed_sec=elapsed_sec,
                        process_log_id=process_log_id
                    )
                except Exception as ex:
                    logger.warning(f"Failed to log extraction error: {ex}")
            elif process_log_id and fallback_conn and run_id:
                try:
                    from core.audit_logging import finalize_process_log_enhanced
                    finalize_process_log_enhanced(
                        fallback_conn,
                        process_log_id,
                        exit_code=1,
                        records_extracted=0,
                        records_ingested=0,
                        warnings_json=str(e),
                    )
                    fallback_conn.commit()
                except Exception as ex:
                    logger.warning(f"Failed to finalize extraction error (fallback): {ex}")
            return (False, str(e))
        finally:
            # Always close evidence_conn if we created one
            if evidence_conn is not None:
                try:
                    evidence_conn.close()
                except Exception as e:
                    logger.warning(f"Failed to close evidence_conn: {e}")
            if fallback_conn is not None:
                try:
                    fallback_conn.close()
                except Exception:
                    pass

    def _run_single_ingestion(self, extractor, evidence_slug: str, run_id: str) -> tuple:
        """
        Run ingestion phase for a single extractor.

        Args:
            extractor: Extractor instance
            evidence_slug: Evidence slug for output directory
            run_id: Run ID for this batch operation

        Returns:
            (True, "") - Success
            (False, error_message) - Failed
            (None, reason) - Skipped
        """
        import inspect
        import time
        from core.logging import get_logger
        from extractors.workers import WorkerCallbacks

        logger = get_logger("app.services.workers")
        meta = extractor.metadata
        evidence_conn = None

        # Get output directory
        output_dir = extractor.get_output_dir(self.workspace_dir, evidence_slug)

        # Check if can run ingestion
        can_run_sig = inspect.signature(extractor.can_run_ingestion)
        can_run_params = list(can_run_sig.parameters.keys())

        if 'output_dir' in can_run_params:
            can_run, reason = extractor.can_run_ingestion(output_dir)
        else:
            # Fallback: check if has output
            try:
                can_run = extractor.has_existing_output(output_dir)
                reason = "No output to ingest" if not can_run else ""
            except Exception as e:
                return (False, f"Failed to check output: {e}")

        if not can_run:
            return (None, reason)

        # Check if should skip based on mode
        if self.overwrite_mode == 'skip_existing':
            try:
                evidence_conn = self.db_manager.get_evidence_conn(
                    self.evidence_id,
                    self.evidence_label
                )
                has_data = self._has_existing_data(extractor, evidence_conn)
                evidence_conn.close()
                evidence_conn = None

                if has_data:
                    return (None, "Data already exists (skip mode)")
            except Exception as e:
                logger.warning(f"Failed to check existing data: {e}")
                if evidence_conn:
                    evidence_conn.close()
                    evidence_conn = None

        # Get config and add overwrite_mode + run_id + evidence context
        config = dict(getattr(extractor, '_config', {}))
        config['overwrite_mode'] = self.overwrite_mode
        config['run_id'] = run_id
        config['evidence_id'] = self.evidence_id
        config['evidence_label'] = self.evidence_label

        # Create callbacks
        callbacks = WorkerCallbacks()
        callbacks.log_message.connect(
            lambda msg, level: self.log_message.emit(f"[{meta.name}] {msg}")
        )

        # Add evidence logger if available
        if self.evidence_logger:
            callbacks.on_log = lambda msg, level="info": self.evidence_logger.log_message(
                msg, level=level, extractor=meta.name
            )

        start_time = time.time()
        evidence_conn = None
        process_log_id = None

        try:
            # Get evidence connection FIRST to ensure database exists with migrations
            evidence_conn = self.db_manager.get_evidence_conn(
                self.evidence_id,
                self.evidence_label
            )

            # Now we can log ingestion start (database exists with process_log table)
            if self.evidence_logger and run_id:
                try:
                    process_log_id = self.evidence_logger.log_extraction_start(
                        extractor=f"{meta.name}:ingest",
                        run_id=run_id,
                        config=config
                    )
                except Exception as e:
                    logger.warning(f"Failed to log ingestion start: {e}")

            success = extractor.run_ingestion(
                output_dir=output_dir,
                evidence_conn=evidence_conn,
                evidence_id=self.evidence_id,
                config=config,
                callbacks=callbacks
            )
            elapsed_sec = time.time() - start_time

            # Count ingested records - check manifest or DB
            records_ingested = 0
            if success:
                try:
                    manifest_path = output_dir / "manifest.json"
                    if manifest_path.exists():
                        import json
                        with open(manifest_path) as f:
                            manifest_data = json.load(f)
                            records_ingested = manifest_data.get('record_count', 0)
                except Exception:
                    pass

            # Log ingestion result to process_log for card updates
            if self.evidence_logger and run_id:
                try:
                    self.evidence_logger.log_ingestion_complete(
                        extractor=f"{meta.name}:ingest",
                        run_id=run_id,
                        records_ingested=records_ingested,
                        errors=0 if success else 1,
                        elapsed_sec=elapsed_sec,
                        process_log_id=process_log_id
                    )
                except Exception as e:
                    logger.warning(f"Failed to log ingestion result: {e}")

            return (success, "")
        except Exception as e:
            logger.exception(f"Ingestion failed for {meta.name}")
            elapsed_sec = time.time() - start_time

            # Log failed ingestion
            if self.evidence_logger and run_id:
                try:
                    self.evidence_logger.log_ingestion_complete(
                        extractor=f"{meta.name}:ingest",
                        run_id=run_id,
                        records_ingested=0,
                        errors=1,
                        elapsed_sec=elapsed_sec,
                        process_log_id=process_log_id
                    )
                except Exception as ex:
                    logger.warning(f"Failed to log ingestion error: {ex}")
            return (False, str(e))
        finally:
            if evidence_conn:
                try:
                    evidence_conn.close()
                except Exception as e:
                    logger.warning(f"Failed to close evidence_conn: {e}")

    def _has_existing_data(self, extractor, evidence_conn) -> bool:
        """Check if extractor has data in database."""
        from core.logging import get_logger
        logger = get_logger("app.services.workers")
        meta = extractor.metadata

        # Map extractor names to their primary tables
        table_map = {
            'browser_history': 'browser_history',
            'browser_cookies': 'cookies',
            'browser_bookmarks': 'bookmarks',
            'browser_downloads': 'browser_downloads',
            'browser_autofill': 'autofill',
            'browser_credentials': 'credentials',
            'browser_sessions': 'session_windows',
            'browser_permissions': 'site_permissions',
            'browser_media': 'media_playback',
            'browser_extensions': 'browser_extensions',
            'browser_storage': 'local_storage',
            'sync_data': 'sync_data',
            'transport_security': 'hsts_entries',
            'browser_carver': 'browser_history',
            'jump_lists': 'jump_list_entries',
            'browser_favicons': 'favicons',
            'safari': 'browser_history',
            'cache_simple': 'browser_cache_inventory',
            'file_list_importer': 'file_list',
            'registry': 'os_indicators',
            'bulk_extractor': 'urls',
            'foremost_carver': 'images',
            'scalpel': 'images',
            'bulk_extractor_images': 'images',
        }

        table = table_map.get(meta.name)
        if not table:
            return False

        try:
            cursor = evidence_conn.cursor()
            cursor.execute(
                f"SELECT COUNT(*) FROM {table} WHERE evidence_id = ?",
                (self.evidence_id,)
            )
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logger.warning(f"Failed to check existing data for {meta.name}: {e}")
            return False


# =============================================================================
# Case-Wide Extract & Ingest Worker
# =============================================================================


class CaseWideExtractAndIngestWorker(QThread):
    """
    Background worker for case-wide extraction and ingestion.

    Orchestrates ExtractAndIngestWorker for each evidence sequentially.
    Each evidence is fully processed (extract + ingest all extractors) before
    moving to the next, maintaining resource efficiency and evidence isolation.

    Signals:
        evidence_started(int, str): evidence_id, label
        evidence_finished(int, str, bool, str): evidence_id, label, success, message
        progress(int, int, str): current_phase, total_phases, message
        batch_finished(dict): Summary {succeeded: [], failed: [], skipped: [], run_id: str}
        log_message(int, str): evidence_id, message
    """

    evidence_started = Signal(int, str)  # evidence_id, label
    evidence_finished = Signal(int, str, bool, str)  # evidence_id, label, success, message
    progress = Signal(int, int, str)  # current_phase, total_phases, message
    batch_finished = Signal(dict)  # Summary dict
    log_message = Signal(int, str)  # evidence_id, message

    def __init__(
        self,
        evidence_ids: List[int],
        extractor_names: List[str],
        extractor_configs: Optional[Dict[str, Dict[str, Any]]],
        case_data: CaseDataAccess,
        case_path: Path,
        db_manager: DatabaseManager,
        overwrite_mode: str = 'overwrite',  # 'overwrite', 'append', 'skip_existing'
        audit_logger: Optional["AuditLogger"] = None,
        parent=None
    ):
        super().__init__(parent)
        self.evidence_ids = evidence_ids
        self.extractor_names = extractor_names
        self.extractor_configs = extractor_configs or {}
        self.case_data = case_data
        self.case_path = case_path
        self.db_manager = db_manager
        self.overwrite_mode = overwrite_mode
        self.audit_logger = audit_logger
        self._cancelled = False
        self._current_sub_worker = None
        self._progress_current = 0
        self._progress_total = 0

    def cancel(self):
        """Request cancellation - also cancels current sub-worker."""
        self._cancelled = True
        if self._current_sub_worker:
            self._current_sub_worker.cancel()

    def run(self):
        """Run extraction + ingestion for all evidences."""
        from core.logging import get_logger
        from extractors import ExtractorRegistry

        # ExtractAndIngestWorker is now in this same module

        logger = get_logger("app.workers.case_wide")
        registry = ExtractorRegistry()
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        # Resolve extractor names once and compute total phases
        resolved_names = []
        phases_per_evidence = 0
        for name in self.extractor_names:
            ext = registry.get(name)
            if ext and ext.metadata.can_extract:
                resolved_names.append(name)
                # Each extractor = 1 extract phase + 1 ingest phase (if can_ingest)
                phases_per_evidence += 1 + (1 if ext.metadata.can_ingest else 0)

        self._progress_total = max(phases_per_evidence * len(self.evidence_ids), 1)

        results = {
            'succeeded': [],
            'failed': [],
            'skipped': [],
            'run_id': run_id
        }

        total_evidences = len(self.evidence_ids)
        logger.info(f"Case-wide extract & ingest starting: {total_evidences} evidences, {len(resolved_names)} extractors")

        for ev_idx, evidence_id in enumerate(self.evidence_ids):
            if self._cancelled:
                self.log_message.emit(0, "❌ Case-wide operation cancelled")
                break

            # Fetch FULL evidence data (includes partition_index for mounting)
            evidence = self.case_data.get_evidence(evidence_id)
            if not evidence:
                results['skipped'].append({
                    'evidence_id': evidence_id,
                    'reason': 'Evidence not found'
                })
                continue

            evidence_label = evidence.get('label', f'Evidence {evidence_id}')

            self.evidence_started.emit(evidence_id, evidence_label)
            self.progress.emit(
                self._progress_current,
                self._progress_total,
                f"Processing {evidence_label} ({ev_idx + 1}/{total_evidences})"
            )
            self.log_message.emit(evidence_id,
                f"🔄 Starting case-wide processing for: {evidence_label}")

            # Mount evidence filesystem
            evidence_fs = self._mount_evidence(evidence)
            if evidence_fs is None:
                self.log_message.emit(evidence_id,
                    f"⚠️ Could not mount filesystem - continuing without FS access")

            # Build extractor instances and apply case-wide config overrides
            extractors = []
            name_to_display = {}
            for name in resolved_names:
                ext = registry.get(name)
                if ext and ext.metadata.can_extract:
                    if name in self.extractor_configs:
                        ext._config = deepcopy(self.extractor_configs[name])
                    extractors.append(ext)
                    name_to_display[name] = ext.metadata.display_name

            if not extractors:
                results['skipped'].append({
                    'evidence_id': evidence_id,
                    'label': evidence_label,
                    'reason': 'No valid extractors selected'
                })
                self._unmount_evidence(evidence_fs)
                continue

            # Get evidence logger for audit trail (same as per-evidence runs)
            evidence_logger = None
            if self.audit_logger:
                try:
                    # Ensure evidence DB exists/migrations applied before logger use
                    conn = self.db_manager.get_evidence_conn(evidence_id, evidence_label)
                    conn.close()
                    evidence_db_path = self.db_manager.evidence_db_path(evidence_id, evidence_label)
                    evidence_logger = self.audit_logger.get_evidence_logger(evidence_id, evidence_db_path)
                except Exception as e:
                    logger.warning(f"Could not get evidence logger: {e}")
                    self.log_message.emit(evidence_id, f"⚠️ Could not get evidence logger: {e}")

            # Create sub-worker for this evidence (reuses existing ExtractAndIngestWorker)
            source_path = Path(evidence.get('source_path', ''))

            self._current_sub_worker = ExtractAndIngestWorker(
                extractors=extractors,
                evidence_fs=evidence_fs,
                evidence_source_path=source_path,
                evidence_id=evidence_id,
                evidence_label=evidence_label,
                workspace_dir=self.case_path,
                db_manager=self.db_manager,
                overwrite_mode=self.overwrite_mode,
                evidence_logger=evidence_logger,
                parent=None  # No parent - we manage lifecycle
            )

            sub_results = {
                "succeeded": [],
                "failed": [],
                "skipped": [],
                "cancelled": False,
            }

            # Connect sub-worker signals for logging, progress, and results
            # NOTE: ExtractAndIngestWorker.log_message is Signal(str), not (int, str)
            self._current_sub_worker.log_message.connect(
                lambda msg, eid=evidence_id: self.log_message.emit(eid, msg)
            )

            # extractor_started(int index, str name, str phase)
            self._current_sub_worker.extractor_started.connect(
                lambda idx, name, phase, eid=evidence_id, lbl=evidence_label, n2d=name_to_display: (
                    self._on_extractor_started(eid, lbl, n2d.get(name, name), phase)
                )
            )

            # extractor_finished(int index, str name, str phase, bool success, str message)
            self._current_sub_worker.extractor_finished.connect(
                lambda idx, name, phase, ok, msg, eid=evidence_id, lbl=evidence_label, n2d=name_to_display: (
                    self._on_extractor_finished(eid, lbl, n2d.get(name, name), phase, ok, msg)
                )
            )

            # batch_finished(list succeeded, list skipped, list failed, bool cancelled)
            # succeeded: list of display_name strings
            # skipped/failed: list of (display_name, reason) tuples
            self._current_sub_worker.batch_finished.connect(
                lambda succ, skip, fail, cancel: sub_results.update({
                    "succeeded": succ,  # List of display_name strings
                    "skipped": skip,    # List of (display_name, reason) tuples
                    "failed": fail,     # List of (display_name, reason) tuples
                    "cancelled": cancel,
                })
            )

            # Run sub-worker synchronously (we're already in a thread)
            # Note: ExtractAndIngestWorker.run() can be called directly
            self._current_sub_worker.run()

            # Collect results from sub-worker (accurate evidence-level status)
            if self._cancelled or sub_results["cancelled"]:
                results['skipped'].append({
                    'evidence_id': evidence_id,
                    'label': evidence_label,
                    'reason': 'Cancelled'
                })
                self.evidence_finished.emit(evidence_id, evidence_label, False, "Cancelled")
                self._unmount_evidence(evidence_fs)
                break
            elif sub_results["failed"]:
                results['failed'].append({
                    'evidence_id': evidence_id,
                    'label': evidence_label,
                    'succeeded': sub_results["succeeded"],
                    'failed': sub_results["failed"],
                    'skipped': sub_results["skipped"],
                })
                self.evidence_finished.emit(
                    evidence_id,
                    evidence_label,
                    False,
                    f"{len(sub_results['failed'])} extractor(s) failed"
                )
            elif sub_results["succeeded"]:
                results['succeeded'].append({
                    'evidence_id': evidence_id,
                    'label': evidence_label,
                    'succeeded': sub_results["succeeded"],
                    'skipped': sub_results["skipped"],
                })
                self.evidence_finished.emit(
                    evidence_id,
                    evidence_label,
                    True,
                    f"{len(sub_results['succeeded'])} extractor(s) completed"
                )
            else:
                results['skipped'].append({
                    'evidence_id': evidence_id,
                    'label': evidence_label,
                    'reason': 'All extractors skipped'
                })
                self.evidence_finished.emit(evidence_id, evidence_label, False, "All extractors skipped")

            self._current_sub_worker = None

            # Cleanup
            self._unmount_evidence(evidence_fs)
            self.log_message.emit(evidence_id,
                f"✅ Finished processing: {evidence_label}")

        logger.info(f"Case-wide processing complete: {len(results['succeeded'])} succeeded, "
                   f"{len(results['failed'])} failed, {len(results['skipped'])} skipped")
        self.batch_finished.emit(results)

    def _on_extractor_started(self, evidence_id: int, evidence_label: str, display_name: str, phase: str):
        """Handle extractor start - update progress message."""
        msg = f"{evidence_label}: {phase.title()} {display_name}"
        self.progress.emit(self._progress_current, self._progress_total, msg)

    def _on_extractor_finished(self, evidence_id: int, evidence_label: str, display_name: str, phase: str, ok: bool, message: str):
        """Handle extractor finish - update progress counter."""
        self._progress_current += 1
        status = "✅" if ok else "❌"
        msg = f"{status} {evidence_label}: {phase.title()} {display_name}"
        self.progress.emit(self._progress_current, self._progress_total, msg)

    def _mount_evidence(self, evidence: Dict[str, Any]):
        """Mount evidence filesystem using partition_index from evidence data."""
        source_path_str = evidence.get('source_path', '')
        if not source_path_str:
            return None

        source_path = Path(source_path_str)
        if not source_path.exists():
            return None

        if source_path.is_dir():
            try:
                return MountedFS(source_path)
            except Exception:
                return None

        if source_path.suffix.lower() in ['.e01', '.e02', '.e03']:
            try:
                segments = find_ewf_segments(source_path)
                # Use partition_index from evidence data (from get_evidence())
                partition_index = evidence.get('partition_index')
                if partition_index is None:
                    partition_index = -1  # Auto-detect
                return PyEwfTskFS(segments, partition_index=partition_index)
            except Exception as e:
                from core.logging import get_logger
                logger = get_logger("app.workers.case_wide")
                logger.warning(f"Mount error for evidence {evidence.get('id')}: {e}")
                return None

        return None

    def _unmount_evidence(self, evidence_fs):
        """Cleanup evidence filesystem."""
        if evidence_fs and hasattr(evidence_fs, 'close'):
            try:
                evidence_fs.close()
            except Exception:
                pass
