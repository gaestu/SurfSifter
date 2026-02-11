"""
Bookmark database helper functions.

This module provides CRUD operations for the bookmarks table.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_distinct_values, get_rows, insert_row, insert_rows

__all__ = [
    "insert_bookmark_row",
    "insert_bookmarks",
    "get_bookmarks",
    "get_bookmark_folders",
    "delete_bookmarks_by_run",
    "get_distinct_bookmark_browsers",
]


def insert_bookmark_row(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    url: str,
    *,
    profile: Optional[str] = None,
    title: Optional[str] = None,
    folder_path: Optional[str] = None,
    bookmark_type: str = "url",
    guid: Optional[str] = None,
    date_added_utc: Optional[str] = None,
    date_modified_utc: Optional[str] = None,
    run_id: Optional[str] = None,
    source_path: Optional[str] = None,
    discovered_by: Optional[str] = None,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    tags: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Insert a single bookmark row with full forensic provenance.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name ('chrome', 'firefox', 'edge', 'opera', 'brave')
        url: Bookmark URL
        profile: Browser profile name
        title: Bookmark title
        folder_path: Folder hierarchy path (e.g., "Bookmarks Bar/Gambling")
        bookmark_type: 'url' or 'folder'
        guid: Browser-assigned GUID
        date_added_utc: ISO 8601 creation timestamp
        date_modified_utc: ISO 8601 modification timestamp
        run_id: Extraction run ID
        source_path: Original path in evidence
        discovered_by: Extractor signature
        partition_index: E01 partition number
        fs_type: Filesystem type
        logical_path: Windows-style path
        forensic_path: Canonical E01 identifier
        tags: JSON-serialized tags
        notes: Investigator notes
    """
    record = {
        "browser": browser,
        "profile": profile,
        "url": url,
        "title": title,
        "folder_path": folder_path,
        "bookmark_type": bookmark_type,
        "guid": guid,
        "date_added_utc": date_added_utc,
        "date_modified_utc": date_modified_utc,
        "run_id": run_id,
        "source_path": source_path,
        "discovered_by": discovered_by,
        "partition_index": partition_index,
        "fs_type": fs_type,
        "logical_path": logical_path,
        "forensic_path": forensic_path,
        "tags": tags,
        "notes": notes,
    }
    insert_row(conn, TABLE_SCHEMAS["bookmarks"], evidence_id, record)


def insert_bookmarks(conn: sqlite3.Connection, evidence_id: int, bookmarks: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple bookmarks in batch.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        bookmarks: Iterable of bookmark records

    Returns:
        Number of bookmarks inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["bookmarks"], evidence_id, bookmarks)


def get_bookmarks(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    folder_path: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve bookmarks for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        folder_path: Optional folder path filter (prefix match)
        limit: Maximum rows to return

    Returns:
        List of bookmark records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if folder_path:
        filters["folder_path"] = (FilterOp.LIKE, f"{folder_path}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["bookmarks"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_bookmark_folders(conn: sqlite3.Connection, evidence_id: int) -> List[Dict[str, Any]]:
    """
    Get bookmark folder statistics for an evidence.

    Returns:
        List of dicts with folder_path, bookmark_count, browsers
    """
    rows = conn.execute(
        """
        SELECT folder_path, COUNT(*) as bookmark_count,
               GROUP_CONCAT(DISTINCT browser) as browsers
        FROM bookmarks
        WHERE evidence_id = ? AND bookmark_type = 'url'
        GROUP BY folder_path
        ORDER BY bookmark_count DESC
        """,
        (evidence_id,)
    ).fetchall()

    return [dict(row) for row in rows]


def delete_bookmarks_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """
    Delete bookmarks from a specific extraction run.

    Used for idempotent re-ingestion.

    Returns:
        Number of rows deleted
    """
    return delete_by_run(conn, TABLE_SCHEMAS["bookmarks"], evidence_id, run_id)


def get_distinct_bookmark_browsers(conn: sqlite3.Connection, evidence_id: int) -> List[str]:
    """
    Get distinct browsers that have bookmarks for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        Sorted list of browser names
    """
    return get_distinct_values(conn, TABLE_SCHEMAS["bookmarks"], evidence_id, "browser")
