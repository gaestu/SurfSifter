"""
Browser history database helper functions.

This module provides CRUD operations for the browser_history table.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, OrderColumn, TABLE_SCHEMAS
from .generic import delete_by_run, get_distinct_values, get_rows, insert_row, insert_rows

__all__ = [
    "insert_browser_history",
    "insert_browser_history_row",
    "insert_browser_history_rows",
    "get_browser_history",
    "get_browser_history_by_id",
    "get_distinct_history_browsers",
    "get_distinct_history_profiles",
    "get_browser_history_stats",
    "delete_browser_history_by_run",
]


def insert_browser_history(conn: sqlite3.Connection, evidence_id: int, records: Iterable[Dict[str, Any]]) -> None:
    """
    Legacy helper for rule-based browser history extraction.

    Inserts 8-column rows (url, title, ts_utc, browser, profile, source_path).
    Used by rules/extractors/sqlite_browser_history.yml until deprecated in.

    Note: id and evidence_id are auto-filled by database.
    """
    insert_rows(conn, TABLE_SCHEMAS["browser_history"], evidence_id, records)


def insert_browser_history_row(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    url: str,
    *,
    title: Optional[str] = None,
    visit_time_utc: Optional[str] = None,
    visit_count: Optional[int] = None,
    typed_count: Optional[int] = None,
    last_visit_time_utc: Optional[str] = None,
    source_path: str,
    discovered_by: str,
    run_id: str,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    tags: Optional[str] = None,
    notes: Optional[str] = None,
    # Forensic visit metadata
    transition_type: Optional[int] = None,
    from_visit: Optional[int] = None,
    visit_duration_ms: Optional[int] = None,
    hidden: Optional[int] = None,
    chromium_visit_id: Optional[int] = None,
    chromium_url_id: Optional[int] = None,
) -> None:
    """
    Insert single browser history row with full forensic provenance.

    Used by modular browser_history extractor. Includes all forensic context fields
    present in the consolidated baseline schema.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name ('chrome', 'firefox', 'edge', 'safari')
        url: Visited URL
        title: Page title
        visit_time_utc: ISO 8601 timestamp (maps to legacy ts_utc column)
        visit_count: Total visits to this URL
        typed_count: Times user typed URL
        last_visit_time_utc: Last visit timestamp
        source_path: Original path in evidence (legacy column, kept for compatibility)
        discovered_by: Extractor signature (e.g., 'browser_history:0.46.0:run_abc123')
        run_id: Extraction run ID for idempotent re-ingestion
        partition_index: E01 partition number
        fs_type: Filesystem type ('ntfs', 'ext4', etc.)
        logical_path: Windows-style path (C:\\Users\\...)
        forensic_path: Canonical E01 identifier
        tags: JSON-serialized BrowserHistoryTags
        notes: Investigator notes
        transition_type: Chromium page transition code
        from_visit: Referrer visit ID for navigation chain
        visit_duration_ms: Time spent on page in milliseconds
        hidden: Whether visit is hidden (subframes, errors)
        chromium_visit_id: Original visit.id from Chromium database
        chromium_url_id: Original urls.id from Chromium database
    """
    record = {
        "browser": browser,
        "url": url,
        "title": title,
        "visit_time_utc": visit_time_utc,
        "visit_count": visit_count,
        "typed_count": typed_count,
        "last_visit_time_utc": last_visit_time_utc,
        "source_path": source_path,
        "discovered_by": discovered_by,
        "run_id": run_id,
        "partition_index": partition_index,
        "fs_type": fs_type,
        "logical_path": logical_path,
        "forensic_path": forensic_path,
        "tags": tags,
        "notes": notes,
        "transition_type": transition_type,
        "from_visit": from_visit,
        "visit_duration_ms": visit_duration_ms,
        "hidden": hidden,
        "chromium_visit_id": chromium_visit_id,
        "chromium_url_id": chromium_url_id,
    }
    insert_row(conn, TABLE_SCHEMAS["browser_history"], evidence_id, record)


def insert_browser_history_rows(
    conn: sqlite3.Connection,
    evidence_id: int,
    records: Iterable[Dict[str, Any]],
) -> int:
    """
    Batch insert browser history rows in single transaction.

    Significantly faster than calling insert_browser_history_row() per record.
    Use this for bulk inserts (50K+ rows see 10-50x speedup).

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        records: Iterable of dicts with keys: browser, url, title, visit_time_utc,
                 visit_count, typed_count, last_visit_time_utc, source_path,
                 discovered_by, run_id, partition_index, fs_type, logical_path,
                 forensic_path, tags, notes, transition_type, from_visit,
                 visit_duration_ms, hidden, chromium_visit_id, chromium_url_id

    Returns:
        Number of rows inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["browser_history"], evidence_id, records)


def get_browser_history(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    profile: Optional[str] = None,
    url_filter: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """
    Retrieve browser history records for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter (exact match)
        profile: Optional profile filter (exact match)
        url_filter: Optional URL substring filter (LIKE)
        limit: Maximum rows to return

    Returns:
        List of browser history records as dicts, ordered by ts_utc DESC
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if profile:
        filters["profile"] = (FilterOp.EQ, profile)
    if url_filter:
        filters["url"] = (FilterOp.LIKE, f"%{url_filter}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["browser_history"],
        evidence_id,
        filters=filters or None,
        limit=limit,
        order_by=[OrderColumn("ts_utc", "DESC")],
    )


def get_browser_history_by_id(conn: sqlite3.Connection, history_id: int) -> Optional[Dict[str, Any]]:
    """
    Get a single browser history record by ID.

    Args:
        conn: SQLite connection to evidence database
        history_id: Browser history row ID

    Returns:
        Browser history record as dict, or None if not found
    """
    row = conn.execute(
        """
        SELECT id, evidence_id, url, title, ts_utc, browser, profile, source_path,
               visit_count, typed_count, last_visit_time_utc, discovered_by,
               tags, notes, run_id, partition_index, fs_type, logical_path, forensic_path,
               transition_type, from_visit, visit_duration_ms, hidden,
               chromium_visit_id, chromium_url_id
        FROM browser_history WHERE id = ?
        """,
        (history_id,)
    ).fetchone()
    return dict(row) if row else None


def get_distinct_history_browsers(conn: sqlite3.Connection, evidence_id: int) -> List[str]:
    """
    Get distinct browsers that have history for an evidence.

    Returns:
        Sorted list of browser names
    """
    return get_distinct_values(conn, TABLE_SCHEMAS["browser_history"], evidence_id, "browser")


def get_distinct_history_profiles(conn: sqlite3.Connection, evidence_id: int, browser: Optional[str] = None) -> List[str]:
    """
    Get distinct profiles that have history for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter

    Returns:
        Sorted list of profile names
    """
    if browser:
        rows = conn.execute(
            "SELECT DISTINCT profile FROM browser_history WHERE evidence_id = ? AND browser = ? ORDER BY profile",
            (evidence_id, browser)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT profile FROM browser_history WHERE evidence_id = ? ORDER BY profile",
            (evidence_id,)
        ).fetchall()
    return [row[0] for row in rows if row[0]]


def get_browser_history_stats(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, Any]:
    """
    Get browser history statistics for an evidence.

    Returns:
        Dict with total_visits, unique_urls, browsers, earliest_visit, latest_visit
    """
    row = conn.execute(
        """
        SELECT COUNT(*) as total_visits,
               COUNT(DISTINCT url) as unique_urls,
               COUNT(DISTINCT browser) as browser_count,
               MIN(ts_utc) as earliest_visit,
               MAX(ts_utc) as latest_visit
        FROM browser_history
        WHERE evidence_id = ?
        """,
        (evidence_id,)
    ).fetchone()

    return {
        "total_visits": row[0] if row else 0,
        "unique_urls": row[1] if row else 0,
        "browser_count": row[2] if row else 0,
        "earliest_visit": row[3] if row else None,
        "latest_visit": row[4] if row else None,
    }


def delete_browser_history_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """
    Delete browser history from a specific extraction run.

    Used for idempotent re-ingestion.

    Returns:
        Number of rows deleted
    """
    return delete_by_run(conn, TABLE_SCHEMAS["browser_history"], evidence_id, run_id)
