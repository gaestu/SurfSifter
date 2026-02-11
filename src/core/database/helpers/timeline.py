"""
Timeline database helper functions.

This module provides CRUD operations for the timeline table.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, OrderColumn, TABLE_SCHEMAS
from .generic import delete_by_run, get_distinct_values, get_rows, insert_row, insert_rows

__all__ = [
    "insert_timeline_event",
    "insert_timeline_events",
    "get_timeline_events",
    "get_timeline_stats",
    "delete_timeline_by_run",
    "clear_timeline",
    "iter_timeline",
    "get_timeline_kinds",
    "get_timeline_confidences",
]


def insert_timeline_event(
    conn: sqlite3.Connection,
    evidence_id: int,
    ts_utc: str,
    kind: str,
    *,
    ref_table: Optional[str] = None,
    ref_id: Optional[int] = None,
    confidence: Optional[str] = None,
    note: Optional[str] = None,
    run_id: Optional[str] = None,
) -> None:
    """
    Insert a single timeline event.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        ts_utc: ISO 8601 timestamp
        kind: Event type (visit, download, cookie_create, etc.)
        ref_table: Source table name
        ref_id: Row ID in source table
        confidence: Confidence level
        note: Additional notes
        run_id: Extraction run ID
    """
    record = {
        "ts_utc": ts_utc,
        "kind": kind,
        "ref_table": ref_table,
        "ref_id": ref_id,
        "confidence": confidence,
        "note": note,
        "run_id": run_id,
    }
    insert_row(conn, TABLE_SCHEMAS["timeline"], evidence_id, record)


def insert_timeline_events(conn: sqlite3.Connection, evidence_id: int, records: Iterable[Dict[str, Any]]) -> None:
    """
    Insert timeline events in batch.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        records: Iterable of event records with keys:
            - ts_utc: ISO timestamp
            - kind: Event kind/type
            - ref_table: Source table name
            - ref_id: Source record ID
            - confidence: Optional confidence level
            - note: Optional note/description
            - run_id: Extraction run ID (optional)
    """
    insert_rows(conn, TABLE_SCHEMAS["timeline"], evidence_id, records)


def get_timeline_events(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    kind: Optional[str] = None,
    ref_table: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 10000,
) -> List[Dict[str, Any]]:
    """
    Retrieve timeline events for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        kind: Optional event type filter
        ref_table: Optional source table filter
        start_time: Optional start time filter (ISO 8601)
        end_time: Optional end time filter (ISO 8601)
        limit: Maximum rows to return

    Returns:
        List of timeline events ordered by ts_utc DESC
    """
    conditions = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]

    if kind:
        conditions.append("kind = ?")
        params.append(kind)
    if ref_table:
        conditions.append("ref_table = ?")
        params.append(ref_table)
    if start_time:
        conditions.append("ts_utc >= ?")
        params.append(start_time)
    if end_time:
        conditions.append("ts_utc <= ?")
        params.append(end_time)

    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT * FROM timeline
        WHERE {' AND '.join(conditions)}
        ORDER BY ts_utc DESC
        LIMIT ?
        """,
        params
    ).fetchall()

    return [dict(row) for row in rows]


def iter_timeline(
    conn: sqlite3.Connection,
    evidence_id: int,
    filters: Optional[Dict[str, Any]] = None,
    page: int = 1,
    page_size: int = 100,
) -> List[sqlite3.Row]:
    """
    Retrieve timeline events with filtering and paging.
    Returns events in deterministic order (ts_utc, kind, ref_table, ref_id).
    """
    where_clauses = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]

    if filters:
        if "kind" in filters and filters["kind"]:
            where_clauses.append("kind = ?")
            params.append(filters["kind"])

        if "confidence" in filters and filters["confidence"]:
            where_clauses.append("confidence = ?")
            params.append(filters["confidence"])

        if "ref_table" in filters and filters["ref_table"]:
            where_clauses.append("ref_table = ?")
            params.append(filters["ref_table"])

        if "start_date" in filters and filters["start_date"]:
            where_clauses.append("ts_utc >= ?")
            params.append(filters["start_date"])

        if "end_date" in filters and filters["end_date"]:
            where_clauses.append("ts_utc <= ?")
            params.append(filters["end_date"])

    where_sql = " AND ".join(where_clauses)
    offset = (page - 1) * page_size

    query = f"""
        SELECT id, evidence_id, ts_utc, kind, ref_table, ref_id, confidence, note
        FROM timeline
        WHERE {where_sql}
        ORDER BY ts_utc, kind, ref_table, ref_id
        LIMIT ? OFFSET ?
    """
    params.extend([page_size, offset])

    return conn.execute(query, params).fetchall()


def get_timeline_stats(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, Any]:
    """
    Get timeline statistics for an evidence.
    Returns counts by kind, confidence, source table, and temporal range.
    """
    stats: Dict[str, Any] = {
        "total_events": 0,
        "by_kind": {},
        "by_confidence": {},
        "by_source": {},
        "earliest": None,
        "latest": None
    }

    # Total count
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM timeline WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()
    stats["total_events"] = row["cnt"] if row else 0

    # By kind
    for row in conn.execute(
        """
        SELECT kind, COUNT(*) as cnt
        FROM timeline
        WHERE evidence_id = ?
        GROUP BY kind
        ORDER BY kind
        """,
        (evidence_id,)
    ):
        stats["by_kind"][row["kind"]] = row["cnt"]

    # By confidence
    for row in conn.execute(
        """
        SELECT confidence, COUNT(*) as cnt
        FROM timeline
        WHERE evidence_id = ?
        GROUP BY confidence
        ORDER BY confidence DESC
        """,
        (evidence_id,)
    ):
        stats["by_confidence"][row["confidence"]] = row["cnt"]

    # By source table
    for row in conn.execute(
        """
        SELECT ref_table, COUNT(*) as cnt
        FROM timeline
        WHERE evidence_id = ?
        GROUP BY ref_table
        ORDER BY ref_table
        """,
        (evidence_id,)
    ):
        stats["by_source"][row["ref_table"]] = row["cnt"]

    # Temporal range
    row = conn.execute(
        """
        SELECT MIN(ts_utc) as earliest, MAX(ts_utc) as latest
        FROM timeline
        WHERE evidence_id = ?
        """,
        (evidence_id,)
    ).fetchone()
    if row:
        stats["earliest"] = row["earliest"]
        stats["latest"] = row["latest"]

    return stats


def get_timeline_kinds(conn: sqlite3.Connection, evidence_id: int) -> List[str]:
    """Get distinct event kinds in timeline."""
    rows = conn.execute(
        "SELECT DISTINCT kind FROM timeline WHERE evidence_id = ? ORDER BY kind",
        (evidence_id,)
    ).fetchall()
    return [row[0] for row in rows]


def get_timeline_confidences(conn: sqlite3.Connection, evidence_id: int) -> List[str]:
    """Get distinct confidence levels from timeline (sorted high->medium->low)."""
    confidences = get_distinct_values(conn, TABLE_SCHEMAS["timeline"], evidence_id, "confidence")
    has_null = conn.execute(
        "SELECT 1 FROM timeline WHERE evidence_id = ? AND confidence IS NULL LIMIT 1",
        (evidence_id,),
    ).fetchone()
    if has_null:
        confidences.append(None)

    # Sort with high > medium > low
    confidence_order = {"high": 0, "medium": 1, "low": 2}
    return sorted(confidences, key=lambda c: confidence_order.get(c, 99))


def delete_timeline_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete timeline events from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["timeline"], evidence_id, run_id)


def clear_timeline(conn: sqlite3.Connection, evidence_id: int) -> int:
    """Clear all timeline events for an evidence."""
    cursor = conn.execute(
        "DELETE FROM timeline WHERE evidence_id = ?",
        (evidence_id,)
    )
    return cursor.rowcount
