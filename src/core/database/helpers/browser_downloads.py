"""
Browser downloads database helper functions.

This module provides CRUD operations for the browser_downloads table.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_distinct_values, get_rows, insert_row, insert_rows

__all__ = [
    "insert_browser_download_row",
    "insert_browser_downloads",
    "get_browser_downloads",
    "get_browser_download_stats",
    "delete_browser_downloads_by_run",
    "get_distinct_download_browsers",
]


def insert_browser_download_row(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    url: str,
    **kwargs,
) -> None:
    """
    Insert a single browser download record.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name (chrome, edge, firefox, etc.)
        url: Download URL
        **kwargs: Optional fields (profile, filename, target_path, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "url": url,
        "target_path": kwargs.get("target_path"),
        "filename": kwargs.get("filename"),
        "start_time_utc": kwargs.get("start_time_utc"),
        "end_time_utc": kwargs.get("end_time_utc"),
        "total_bytes": kwargs.get("total_bytes"),
        "received_bytes": kwargs.get("received_bytes"),
        "mime_type": kwargs.get("mime_type"),
        "referrer": kwargs.get("referrer"),
        "state": kwargs.get("state"),
        "danger_type": kwargs.get("danger_type"),
        "opened": kwargs.get("opened"),
        "run_id": kwargs.get("run_id"),
        "source_path": kwargs.get("source_path"),
        "discovered_by": kwargs.get("discovered_by"),
        "partition_index": kwargs.get("partition_index"),
        "fs_type": kwargs.get("fs_type"),
        "logical_path": kwargs.get("logical_path"),
        "forensic_path": kwargs.get("forensic_path"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["browser_downloads"], evidence_id, record)


def insert_browser_downloads(conn: sqlite3.Connection, evidence_id: int, downloads: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple browser downloads in batch.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        downloads: Iterable of download records

    Returns:
        Number of downloads inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["browser_downloads"], evidence_id, downloads)


def get_browser_downloads(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    state: Optional[str] = None,
    filename: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve browser downloads for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        state: Optional state filter (complete, interrupted, etc.)
        filename: Optional filename filter (partial match)
        limit: Maximum rows to return

    Returns:
        List of download records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if state:
        filters["state"] = (FilterOp.EQ, state)
    if filename:
        filters["filename"] = (FilterOp.LIKE, f"%{filename}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["browser_downloads"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_browser_download_stats(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, Any]:
    """
    Get browser download statistics for an evidence.

    Returns:
        Dict with total_count, by_browser, by_state, total_bytes
    """
    # Total count
    total = conn.execute(
        "SELECT COUNT(*) FROM browser_downloads WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0]

    # By browser
    by_browser = {}
    for row in conn.execute(
        """
        SELECT browser, COUNT(*) as count
        FROM browser_downloads
        WHERE evidence_id = ?
        GROUP BY browser
        """,
        (evidence_id,)
    ):
        by_browser[row["browser"]] = row["count"]

    # By state
    by_state = {}
    for row in conn.execute(
        """
        SELECT state, COUNT(*) as count
        FROM browser_downloads
        WHERE evidence_id = ?
        GROUP BY state
        """,
        (evidence_id,)
    ):
        by_state[row["state"] or "unknown"] = row["count"]

    # Total bytes
    total_bytes = conn.execute(
        "SELECT SUM(total_bytes) FROM browser_downloads WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0] or 0

    return {
        "total_count": total,
        "by_browser": by_browser,
        "by_state": by_state,
        "total_bytes": total_bytes,
    }


def delete_browser_downloads_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """
    Delete browser downloads from a specific extraction run.

    Used for idempotent re-ingestion.

    Returns:
        Number of rows deleted
    """
    return delete_by_run(conn, TABLE_SCHEMAS["browser_downloads"], evidence_id, run_id)


def get_distinct_download_browsers(conn: sqlite3.Connection, evidence_id: int) -> List[str]:
    """
    Get distinct browsers that have downloads for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        Sorted list of browser names
    """
    return get_distinct_values(conn, TABLE_SCHEMAS["browser_downloads"], evidence_id, "browser")
