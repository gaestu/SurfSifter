"""
Forensic audit logging system with three-layer architecture.

Provides:
- AuditLogger: Main interface for logging operations
- CaseLogger: Case-level logging (file + database)
- EvidenceLogger: Evidence-level logging (file + database process_log)
- ExtractorLoggerAdapter: Safe logging with default extractor field
- Configured RotatingFileHandler with size-based rotation (no auto-delete)

Key Design:
- propagate=False on case/evidence loggers to prevent duplicate writes to global log
- ExtractorLoggerAdapter provides default 'extractor' field to avoid KeyError
- EvidenceLogger writes to BOTH file AND process_log table
- WorkerCallbacks routes through EvidenceLogger for persistence

Version:
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

from .logging import get_logger

LOGGER = get_logger("core.audit_logging")


class ExtractorLoggerAdapter(logging.LoggerAdapter):
    """
    LoggerAdapter that provides default 'extractor' field.

    Prevents KeyError when formatter uses %(extractor)s but caller
    doesn't provide extra={'extractor': ...}.

    Usage:
        logger = ExtractorLoggerAdapter(base_logger, {"extractor": "unknown"})
        logger.info("message")  # Uses default extractor
        logger.info("message", extra={"extractor": "browser_history"})  # Override
    """

    def process(self, msg: str, kwargs: Dict[str, Any]) -> tuple:
        # Merge default extra with any provided extra
        extra = self.extra.copy()
        if "extra" in kwargs:
            extra.update(kwargs["extra"])
        kwargs["extra"] = extra
        return msg, kwargs


class AuditLogger:
    """
    Central audit logging coordinator.

    Manages case and evidence loggers with proper lifecycle.
    Initialized once at app startup, updated when case opens.
    """

    def __init__(self, logging_config: Optional[Dict[str, Any]] = None):
        """
        Initialize audit logger.

        Args:
            logging_config: Optional config dict from AppConfig.logging
                Keys: app_log_max_mb, case_log_max_mb, evidence_log_max_mb,
                      app_log_backup_count, case_log_backup_count, evidence_log_backup_count,
                      level
        """
        self.config = logging_config or {}
        self.case_path: Optional[Path] = None
        self._case_logger: Optional[CaseLogger] = None
        self._evidence_loggers: Dict[int, EvidenceLogger] = {}

    def set_case(self, case_path: Path, case_number: str, case_db_path: Path) -> CaseLogger:
        """
        Initialize case-level logging when case is opened.

        Creates logs/ subdirectory in case folder.
        Logs case open event.

        Args:
            case_path: Path to case folder
            case_number: Case identifier for logger name
            case_db_path: Path to case database for audit log table writes

        Returns:
            CaseLogger instance
        """
        # Close existing loggers if switching cases
        self.close()

        self.case_path = case_path

        # Ensure logs/ directory exists
        logs_dir = case_path / "logs"
        logs_dir.mkdir(exist_ok=True)

        self._case_logger = CaseLogger(
            case_path,
            case_number,
            case_db_path,
            max_bytes=self.config.get("case_log_max_mb", 50) * 1024 * 1024,
            backup_count=self.config.get("case_log_backup_count", 5)
        )
        self._case_logger.log_case_opened()
        return self._case_logger

    @property
    def case_logger(self) -> Optional[CaseLogger]:
        """Get current case logger (may be None if no case open)."""
        return self._case_logger

    def get_evidence_logger(self, evidence_id: int, evidence_db_path: Path) -> EvidenceLogger:
        """
        Get or create evidence-level logger.

        Args:
            evidence_id: Evidence ID
            evidence_db_path: Path to evidence database for process_log writes

        Returns:
            EvidenceLogger instance (cached per evidence_id)
        """
        if self.case_path is None:
            raise RuntimeError("Cannot create evidence logger without case")

        if evidence_id not in self._evidence_loggers:
            self._evidence_loggers[evidence_id] = EvidenceLogger(
                self.case_path,
                evidence_id,
                evidence_db_path,
                max_bytes=self.config.get("evidence_log_max_mb", 100) * 1024 * 1024,
                backup_count=self.config.get("evidence_log_backup_count", 5)
            )
        return self._evidence_loggers[evidence_id]

    def close(self):
        """Close all loggers (call on case close or app shutdown)."""
        if self._case_logger:
            self._case_logger.log_case_closed()
            self._case_logger.close()
            self._case_logger = None

        for logger in self._evidence_loggers.values():
            logger.close()
        self._evidence_loggers.clear()
        self.case_path = None


class CaseLogger:
    """
    Case-level audit logging with file + database.

    Writes to:
    - {case_folder}/case_audit.log (rotating file)
    - case_audit_log table in case database

    Logger has propagate=False to prevent duplicate writes to global log.
    """

    def __init__(
        self,
        case_path: Path,
        case_number: str,
        case_db_path: Path,
        max_bytes: int = 50 * 1024 * 1024,
        backup_count: int = 5
    ):
        self.case_path = case_path
        self.case_number = case_number
        self.case_db_path = case_db_path
        self.log_path = case_path / "case_audit.log"
        self._logger = self._setup_logger(max_bytes, backup_count)

    def _setup_logger(self, max_bytes: int, backup_count: int) -> logging.Logger:
        # Create unique logger name to avoid conflicts
        logger = logging.getLogger(f"audit.case.{self.case_number}.{id(self)}")
        logger.setLevel(logging.INFO)
        logger.propagate = False  # Prevent duplicate writes to global log
        logger.handlers.clear()

        handler = RotatingFileHandler(
            self.log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8"
        )
        # Use UTC formatter consistent with global logging
        handler.setFormatter(logging.Formatter(
            "%(asctime)sZ %(levelname)-5s [case.%(case_number)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        ))
        handler.formatter.converter = time.gmtime
        handler.addFilter(_CaseContextFilter(self.case_number))
        logger.addHandler(handler)
        return logger

    def _write_to_db(
        self,
        level: str,
        category: str,
        action: str,
        target_type: Optional[str] = None,
        target_id: Optional[int] = None,
        details: Optional[dict] = None,
        investigator: Optional[str] = None
    ):
        """Write audit entry to case_audit_log table."""
        try:
            conn = sqlite3.connect(self.case_db_path)
            ts_utc = datetime.now(timezone.utc).isoformat()
            details_json = json.dumps(details) if details else None
            conn.execute(
                """
                INSERT INTO case_audit_log (ts_utc, level, category, action,
                                            target_type, target_id, details_json, investigator)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts_utc, level, category, action, target_type, target_id, details_json, investigator)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            # Log DB write failure but don't fail the operation
            self._logger.warning(f"Failed to write to case_audit_log: {e}")

    def log_evidence_added(self, evidence_id: int, label: str, source_path: str, size_bytes: Optional[int] = None):
        size_str = self._format_size(size_bytes) if size_bytes else "unknown"
        msg = f"Evidence added: {label} (id={evidence_id}, size={size_str}, path={source_path})"
        self._logger.info(msg)
        self._write_to_db("INFO", "evidence", "added", "evidence", evidence_id,
                          {"label": label, "source_path": source_path, "size_bytes": size_bytes})

    def log_evidence_removed(self, evidence_id: int, label: str):
        msg = f"Evidence removed: {label} (id={evidence_id})"
        self._logger.info(msg)
        self._write_to_db("INFO", "evidence", "removed", "evidence", evidence_id, {"label": label})

    def log_case_opened(self):
        msg = f"Case opened: {self.case_number}"
        self._logger.info(msg)
        self._write_to_db("INFO", "case", "opened", "case", None, {"case_number": self.case_number})

    def log_case_closed(self):
        msg = f"Case closed: {self.case_number}"
        self._logger.info(msg)
        self._write_to_db("INFO", "case", "closed", "case", None, {"case_number": self.case_number})

    def log_settings_changed(self, setting_name: str, old_value: Any, new_value: Any):
        msg = f"Setting changed: {setting_name} = {new_value} (was: {old_value})"
        self._logger.info(msg)
        self._write_to_db("INFO", "settings", "changed", "setting", None,
                          {"setting": setting_name, "old_value": str(old_value), "new_value": str(new_value)})

    def log_report_generated(self, report_type: str, output_path: str, evidence_id: Optional[int] = None):
        """Log report generation at case level."""
        msg = f"Report generated: {report_type} -> {output_path}"
        if evidence_id:
            msg += f" (evidence_id={evidence_id})"
        self._logger.info(msg)
        self._write_to_db("INFO", "report", "generated", "report", evidence_id,
                          {"report_type": report_type, "output_path": output_path})

    def _format_size(self, size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.1f}{unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f}PB"

    def close(self):
        for handler in self._logger.handlers[:]:
            handler.close()
            self._logger.removeHandler(handler)


class _CaseContextFilter(logging.Filter):
    """Filter that adds case_number to log records."""

    def __init__(self, case_number: str):
        super().__init__()
        self.case_number = case_number

    def filter(self, record: logging.LogRecord) -> bool:
        record.case_number = self.case_number
        return True


class EvidenceLogger:
    """
    Evidence-level audit logging with file + process_log table.

    Writes to:
    - {case_folder}/logs/evidence_{id}.log (rotating file)
    - process_log table in evidence database (for structured queries)

    Logger has propagate=False to prevent duplicate writes.
    Uses ExtractorLoggerAdapter for safe %(extractor)s formatting.

    Integration with WorkerCallbacks:
    - WorkerCallbacks.on_log() should call evidence_logger.log_message()
    - WorkerCallbacks.on_step() should call evidence_logger.log_step()
    - This routes UI signals through persistent storage FIRST
    """

    def __init__(
        self,
        case_path: Path,
        evidence_id: int,
        evidence_db_path: Path,
        max_bytes: int = 100 * 1024 * 1024,
        backup_count: int = 5
    ):
        self.case_path = case_path
        self.evidence_id = evidence_id
        self.evidence_db_path = evidence_db_path

        # Log file in logs/ subdirectory
        logs_dir = case_path / "logs"
        logs_dir.mkdir(exist_ok=True)
        self.log_path = logs_dir / f"evidence_{evidence_id}.log"

        self._logger = self._setup_logger(max_bytes, backup_count)

        # Track active process_log entries for enrichment
        self._active_process_log_id: Optional[int] = None
        self._active_run_id: Optional[str] = None

    def _setup_logger(self, max_bytes: int, backup_count: int) -> ExtractorLoggerAdapter:
        # Create unique logger name to avoid conflicts
        base_logger = logging.getLogger(f"audit.evidence.{self.evidence_id}.{id(self)}")
        base_logger.setLevel(logging.INFO)
        base_logger.propagate = False  # Prevent duplicate writes
        base_logger.handlers.clear()

        handler = RotatingFileHandler(
            self.log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8"
        )
        handler.setFormatter(logging.Formatter(
            "%(asctime)sZ %(levelname)-5s [evidence.%(evidence_id)s.%(extractor)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        ))
        handler.formatter.converter = time.gmtime
        base_logger.addHandler(handler)

        # Wrap in adapter with default extractor field
        return ExtractorLoggerAdapter(base_logger, {"extractor": "general", "evidence_id": self.evidence_id})

    def _get_db_conn(self) -> sqlite3.Connection:
        """
        Get connection to evidence database.

        Note: Migrations should have been applied when the database was created
        by DatabaseManager.get_evidence_conn(). This method creates a simple
        connection for writing to the existing schema.
        """
        conn = sqlite3.connect(self.evidence_db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.row_factory = sqlite3.Row
        return conn

    # --- Extraction lifecycle methods (write to both file and process_log) ---

    def log_extraction_start(
        self,
        extractor: str,
        run_id: str,
        command: Optional[str] = None,
        config: Optional[dict] = None
    ) -> int:
        """
        Log extraction phase start.

        Creates process_log entry and returns log_id for later enrichment.

        Args:
            extractor: Extractor name
            run_id: Unique run ID
            command: Optional command string for external tools
            config: Optional config dict

        Returns:
            process_log row ID for use with finalize_extraction()
        """
        extra = {"extractor": extractor}
        config_str = f", config={config}" if config else ""
        self._logger.info(f"Extraction started: run_id={run_id}{config_str}", extra=extra)

        # Create process_log entry with enhanced fields
        conn = self._get_db_conn()
        try:
            log_id = create_process_log_enhanced(
                conn,
                self.evidence_id,
                task=f"extract:{extractor}",
                command=command,
                run_id=run_id,
                extractor_name=extractor,
                log_file_path=str(self.log_path)
            )
            conn.commit()
            self._active_process_log_id = log_id
            self._active_run_id = run_id
            return log_id
        finally:
            conn.close()

    def log_extraction_result(
        self,
        extractor: str,
        run_id: str,
        records: int,
        errors: int,
        elapsed_sec: float,
        process_log_id: Optional[int] = None
    ):
        """
        Log extraction phase completion and finalize process_log entry.

        Args:
            extractor: Extractor name
            run_id: Run ID from log_extraction_start
            records: Number of records extracted
            errors: Number of errors
            elapsed_sec: Elapsed time in seconds
            process_log_id: process_log row ID (uses active if not provided)
        """
        extra = {"extractor": extractor}
        self._logger.info(
            f"Extraction completed: run_id={run_id}, {records} records, {errors} errors, {elapsed_sec:.1f}s elapsed",
            extra=extra
        )

        # Finalize process_log entry
        log_id = process_log_id or self._active_process_log_id
        if log_id:
            conn = self._get_db_conn()
            try:
                finalize_process_log_enhanced(
                    conn, log_id,
                    exit_code=0 if errors == 0 else 1,
                    records_extracted=records,
                    records_ingested=0,  # Updated by log_ingestion_result
                    warnings_json=None
                )
                conn.commit()
            finally:
                conn.close()

        self._active_process_log_id = None
        self._active_run_id = None

    def log_ingestion_result(self, extractor: str, table: str, rows_inserted: int, process_log_id: Optional[int] = None):
        """Log ingestion completion and update process_log."""
        extra = {"extractor": extractor}
        self._logger.info(f"Ingested {rows_inserted} rows into {table}", extra=extra)

        # Update process_log with ingestion count
        log_id = process_log_id or self._active_process_log_id
        if log_id:
            conn = self._get_db_conn()
            try:
                conn.execute(
                    "UPDATE process_log SET records_ingested = COALESCE(records_ingested, 0) + ? WHERE id = ?",
                    (rows_inserted, log_id)
                )
                conn.commit()
            finally:
                conn.close()

    def log_ingestion_complete(
        self,
        extractor: str,
        run_id: str,
        records_ingested: int,
        errors: int,
        elapsed_sec: float,
        process_log_id: Optional[int] = None
    ):
        """
        Log ingestion phase completion and finalize process_log entry.

        Similar to log_extraction_result but writes to records_ingested column.

        Args:
            extractor: Extractor name (will have :ingest suffix)
            run_id: Run ID from log_extraction_start
            records_ingested: Total records ingested
            errors: Number of errors (0 = success)
            elapsed_sec: Elapsed time in seconds
            process_log_id: process_log row ID (uses active if not provided)
        """
        extra = {"extractor": extractor}
        self._logger.info(
            f"Ingestion completed: run_id={run_id}, {records_ingested} records ingested, {errors} errors, {elapsed_sec:.1f}s elapsed",
            extra=extra
        )

        # Finalize process_log entry with ingestion counts
        log_id = process_log_id or self._active_process_log_id
        if log_id:
            conn = self._get_db_conn()
            try:
                finalize_process_log_enhanced(
                    conn, log_id,
                    exit_code=0 if errors == 0 else 1,
                    records_extracted=0,  # Ingestion doesn't extract
                    records_ingested=records_ingested,
                    warnings_json=None
                )
                conn.commit()
            finally:
                conn.close()

        self._active_process_log_id = None
        self._active_run_id = None

    # --- General logging methods ---

    def log_message(self, message: str, level: str = "info", extractor: str = "general"):
        """
        Log a general message (called by WorkerCallbacks.on_log).

        This is the main integration point for existing extraction code.
        """
        extra = {"extractor": extractor}
        log_method = getattr(self._logger, level.lower(), self._logger.info)
        log_method(message, extra=extra)

    def log_step(self, step_name: str, extractor: str = "general"):
        """Log a step transition (called by WorkerCallbacks.on_step)."""
        extra = {"extractor": extractor}
        self._logger.info(f"Step: {step_name}", extra=extra)

    def log_tool_invocation(self, extractor: str, command: str):
        extra = {"extractor": extractor}
        self._logger.info(f"Tool invocation: {command}", extra=extra)

    def log_tool_result(self, extractor: str, exit_code: int, elapsed_sec: float):
        extra = {"extractor": extractor}
        status = "completed" if exit_code == 0 else f"failed (exit={exit_code})"
        self._logger.info(f"Tool {status}, {elapsed_sec:.1f}s elapsed", extra=extra)

    def log_artifact_found(self, extractor: str, artifact_type: str, count: int, source: Optional[str] = None):
        extra = {"extractor": extractor}
        source_str = f" from {source}" if source else ""
        self._logger.info(f"Found {count} {artifact_type}{source_str}", extra=extra)

    def log_warning(self, extractor: str, message: str):
        extra = {"extractor": extractor}
        self._logger.warning(message, extra=extra)

    def log_error(self, extractor: str, message: str):
        extra = {"extractor": extractor}
        self._logger.error(message, extra=extra)

    # --- Investigator action methods (per-evidence scoped) ---

    def log_report_generated(self, report_type: str, output_path: str):
        """Log report generation for this evidence."""
        extra = {"extractor": "reports"}
        self._logger.info(f"Report generated: {report_type} -> {output_path}", extra=extra)

    def log_tag_applied(self, artifact_type: str, artifact_id: int, tag_name: str):
        """Log tag applied to an artifact."""
        extra = {"extractor": "tagging"}
        self._logger.info(f"Tag applied: '{tag_name}' to {artifact_type} id={artifact_id}", extra=extra)

    def log_tag_removed(self, artifact_type: str, artifact_id: int, tag_name: str):
        """Log tag removed from an artifact."""
        extra = {"extractor": "tagging"}
        self._logger.info(f"Tag removed: '{tag_name}' from {artifact_type} id={artifact_id}", extra=extra)

    def log_note_added(self, artifact_type: str, artifact_id: int):
        """Log note added to an artifact."""
        extra = {"extractor": "notes"}
        self._logger.info(f"Note added to {artifact_type} id={artifact_id}", extra=extra)

    def log_download_acquired(self, url: str, local_path: str, sha256: Optional[str] = None):
        """Log investigator download acquisition."""
        extra = {"extractor": "downloads"}
        hash_str = f", sha256={sha256[:16]}..." if sha256 else ""
        self._logger.info(f"Download acquired: {url} -> {local_path}{hash_str}", extra=extra)

    def tail(self, lines: int = 100) -> List[str]:
        """
        Read last N lines from log file for UI display.

        Called by _build_logs_tab to load persisted logs on tab open.
        """
        if not self.log_path.exists():
            return []

        try:
            with open(self.log_path, "r", encoding="utf-8") as f:
                all_lines = f.readlines()
                return [line.rstrip() for line in all_lines[-lines:]]
        except Exception:
            return []

    def close(self):
        base_logger = self._logger.logger  # Get underlying logger from adapter
        for handler in base_logger.handlers[:]:
            handler.close()
            base_logger.removeHandler(handler)


# --- Enhanced process_log helpers (backward compatible) ---

def create_process_log_enhanced(
    conn: sqlite3.Connection,
    evidence_id: Optional[int],
    task: str,
    command: Optional[str],
    run_id: Optional[str] = None,
    extractor_name: Optional[str] = None,
    extractor_version: Optional[str] = None,
    log_file_path: Optional[str] = None,
) -> int:
    """
    Create process_log entry with enhanced fields.

    Backward compatible: new columns are optional and default to NULL.
    Existing code using create_process_log() continues to work.
    """
    ts_utc = datetime.now(timezone.utc).isoformat()
    cursor = conn.execute(
        """
        INSERT INTO process_log (
            evidence_id, task, command, started_at_utc,
            run_id, extractor_name, extractor_version, log_file_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (evidence_id, task, command, ts_utc, run_id, extractor_name, extractor_version, log_file_path)
    )
    return cursor.lastrowid


def finalize_process_log_enhanced(
    conn: sqlite3.Connection,
    log_id: int,
    *,
    exit_code: int,
    stdout: Optional[str] = None,
    stderr: Optional[str] = None,
    records_extracted: Optional[int] = None,
    records_ingested: Optional[int] = None,
    warnings_json: Optional[str] = None,
) -> None:
    """
    Finalize process_log entry with enhanced fields.

    Backward compatible: new columns are optional.
    """
    ts_utc = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        UPDATE process_log
        SET finished_at_utc = ?, exit_code = ?, stdout = ?, stderr = ?,
            records_extracted = ?, records_ingested = ?, warnings_json = ?
        WHERE id = ?
        """,
        (ts_utc, exit_code, stdout, stderr, records_extracted, records_ingested, warnings_json, log_id)
    )
