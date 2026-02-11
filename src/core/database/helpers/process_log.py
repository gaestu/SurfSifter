"""
Process log database helper functions.

This module provides CRUD operations for the process_log table.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

__all__ = [
    "insert_process_log",
    "get_process_logs",
    "create_process_log",
    "finalize_process_log",
]


def _utc_now() -> str:
    """Return current UTC time as ISO 8601 string (without microseconds)."""
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def insert_process_log(
    conn: sqlite3.Connection,
    evidence_id: int,
    tool_name: str,
    command_line: str,
    *,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
    exit_code: Optional[int] = None,
    output_path: Optional[str] = None,
    run_id: Optional[str] = None,
    extractor_version: Optional[str] = None,
    record_count: Optional[int] = None,
    metadata: Optional[str] = None,
) -> int:
    """
    Insert a process log entry (forensic audit trail).

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        tool_name: Name of the tool/extractor
        command_line: Full command line or invocation details
        started_at: ISO 8601 start timestamp
        finished_at: ISO 8601 finish timestamp
        exit_code: Process exit code (0=success)
        output_path: Path to output files
        run_id: Extraction run ID
        extractor_version: Extractor version string
        record_count: Number of records processed
        metadata: JSON-serialized additional metadata

    Returns:
        Process log row ID
    """
    if started_at is None:
        started_at = datetime.utcnow().isoformat()

    cursor = conn.execute(
        """
        INSERT INTO process_log (
            evidence_id, tool_name, command_line, started_at, finished_at,
            exit_code, output_path, run_id, extractor_version, record_count, metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence_id, tool_name, command_line, started_at, finished_at,
            exit_code, output_path, run_id, extractor_version, record_count, metadata
        )
    )
    conn.commit()
    return cursor.lastrowid


def get_process_logs(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    tool_name: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve process log entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        tool_name: Optional tool name filter
        run_id: Optional run_id filter
        limit: Maximum rows to return

    Returns:
        List of process log records ordered by started_at DESC
    """
    conditions = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]

    if tool_name:
        conditions.append("tool_name = ?")
        params.append(tool_name)
    if run_id:
        conditions.append("run_id = ?")
        params.append(run_id)

    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT * FROM process_log
        WHERE {' AND '.join(conditions)}
        ORDER BY started_at DESC
        LIMIT ?
        """,
        params
    ).fetchall()

    return [dict(row) for row in rows]


def create_process_log(
    conn: sqlite3.Connection,
    evidence_id: Optional[int],
    task: str,
    command: Optional[str],
) -> int:
    """
    Create a new process log entry (for tracking task start).

    Args:
        conn: SQLite connection
        evidence_id: Evidence ID (can be None for case-level tasks)
        task: Task/tool name
        command: Command line or invocation details

    Returns:
        Process log row ID
    """
    with conn:
        cur = conn.execute(
            """
            INSERT INTO process_log(
                evidence_id, task, command, started_at_utc
            ) VALUES (?, ?, ?, ?)
            """,
            (evidence_id, task, command, _utc_now()),
        )
    return int(cur.lastrowid)


def finalize_process_log(
    conn: sqlite3.Connection,
    log_id: int,
    *,
    exit_code: int,
    stdout: Optional[str],
    stderr: Optional[str],
) -> None:
    """
    Finalize a process log entry with completion status.

    Args:
        conn: SQLite connection
        log_id: Process log row ID from create_process_log()
        exit_code: Process exit code (0=success)
        stdout: Standard output content
        stderr: Standard error content
    """
    with conn:
        conn.execute(
            """
            UPDATE process_log
            SET finished_at_utc = ?, exit_code = ?, stdout = ?, stderr = ?
            WHERE id = ?
            """,
            (_utc_now(), exit_code, stdout, stderr, log_id),
        )