"""
Download audit database helper functions.

This module stores and retrieves investigator-initiated download audit events.
Rows are final outcomes only (one row per requested URL result).
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

__all__ = [
    "insert_download_audit",
    "get_download_audit",
    "get_download_audit_count",
    "get_download_audit_summary",
]


def _utc_now() -> str:
    """Return current UTC time as ISO-8601."""
    return datetime.now(timezone.utc).isoformat()


def insert_download_audit(
    conn: sqlite3.Connection,
    evidence_id: int,
    url: str,
    method: str,
    outcome: str,
    *,
    blocked: bool = False,
    reason: Optional[str] = None,
    status_code: Optional[int] = None,
    attempts: Optional[int] = None,
    duration_s: Optional[float] = None,
    bytes_written: Optional[int] = None,
    content_type: Optional[str] = None,
    caller_info: Optional[str] = None,
    ts_utc: Optional[str] = None,
) -> int:
    """
    Insert a final download audit row.

    Returns the inserted row ID.
    """
    if ts_utc is None:
        ts_utc = _utc_now()

    cursor = conn.execute(
        """
        INSERT INTO download_audit (
            evidence_id, ts_utc, url, method, outcome, blocked, reason,
            status_code, attempts, duration_s, bytes_written, content_type, caller_info
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence_id,
            ts_utc,
            url,
            method,
            outcome,
            1 if blocked else 0,
            reason,
            status_code,
            attempts,
            duration_s,
            bytes_written,
            content_type,
            caller_info,
        ),
    )
    return int(cursor.lastrowid)


def get_download_audit(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    outcome: Optional[str] = None,
    search_text: Optional[str] = None,
    limit: int = 1000,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Get download audit rows with optional outcome/text filtering.
    """
    conditions = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]

    if outcome:
        conditions.append("outcome = ?")
        params.append(outcome)

    if search_text:
        token = f"%{search_text}%"
        conditions.append("(url LIKE ? OR COALESCE(reason, '') LIKE ? OR COALESCE(caller_info, '') LIKE ?)")
        params.extend([token, token, token])

    params.extend([limit, offset])
    cursor = conn.execute(
        f"""
        SELECT
            id, evidence_id, ts_utc, url, method, outcome, blocked, reason,
            status_code, attempts, duration_s, bytes_written, content_type,
            caller_info, created_at_utc
        FROM download_audit
        WHERE {' AND '.join(conditions)}
        ORDER BY ts_utc DESC, id DESC
        LIMIT ? OFFSET ?
        """,
        params,
    )
    return [dict(row) for row in cursor.fetchall()]


def get_download_audit_count(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    outcome: Optional[str] = None,
    search_text: Optional[str] = None,
) -> int:
    """Get filtered row count for pagination."""
    conditions = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]

    if outcome:
        conditions.append("outcome = ?")
        params.append(outcome)

    if search_text:
        token = f"%{search_text}%"
        conditions.append("(url LIKE ? OR COALESCE(reason, '') LIKE ? OR COALESCE(caller_info, '') LIKE ?)")
        params.extend([token, token, token])

    row = conn.execute(
        f"SELECT COUNT(*) FROM download_audit WHERE {' AND '.join(conditions)}",
        params,
    ).fetchone()
    return int(row[0]) if row else 0


def get_download_audit_summary(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Any]:
    """Return total count and per-outcome counts."""
    rows = conn.execute(
        """
        SELECT outcome, COUNT(*) AS cnt
        FROM download_audit
        WHERE evidence_id = ?
        GROUP BY outcome
        """,
        (evidence_id,),
    ).fetchall()

    by_outcome: Dict[str, int] = {str(row[0]): int(row[1]) for row in rows}
    total = sum(by_outcome.values())
    return {
        "total": total,
        "by_outcome": by_outcome,
    }
