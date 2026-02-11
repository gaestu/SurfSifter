"""
Jump list database helper functions.

This module provides CRUD operations for the jump_list_entries table.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    "insert_jump_list_entry",
    "insert_jump_list_entries",
    "get_jump_list_entries",
    "get_jump_list_stats",
    "delete_jump_list_by_run",
    "delete_jump_lists_by_run",  # legacy alias
]


def insert_jump_list_entry(
    conn: sqlite3.Connection,
    evidence_id: int,
    app_id: str,
    **kwargs,
) -> None:
    """
    Insert a single jump list entry.

    Jump lists track recently/frequently used items in Windows taskbar.
    Browser jump lists contain visited URLs.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        app_id: Application ID (browser AppUserModelId)
        **kwargs: Optional fields (lnk_path, target_path, url, arguments, etc.)
    """
    record = {
        "app_id": app_id,
        "lnk_path": kwargs.get("lnk_path"),
        "target_path": kwargs.get("target_path"),
        "url": kwargs.get("url"),
        "arguments": kwargs.get("arguments"),
        "working_directory": kwargs.get("working_directory"),
        "icon_location": kwargs.get("icon_location"),
        "access_time_utc": kwargs.get("access_time_utc"),
        "creation_time_utc": kwargs.get("creation_time_utc"),
        "modification_time_utc": kwargs.get("modification_time_utc"),
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
    insert_row(conn, TABLE_SCHEMAS["jump_list_entries"], evidence_id, record)


def insert_jump_list_entries(conn: sqlite3.Connection, evidence_id: int, entries: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple jump list entries in batch.

    Returns:
        Number of entries inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["jump_list_entries"], evidence_id, entries)


def get_jump_list_entries(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    app_id: Optional[str] = None,
    url: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """
    Retrieve jump list entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        app_id: Optional app ID filter
        url: Optional URL filter (partial match)
        limit: Maximum rows to return

    Returns:
        List of jump list entry records as dicts
    """
    filters: Dict[str, Any] = {}
    if app_id:
        filters["app_id"] = (FilterOp.EQ, app_id)
    if url:
        filters["url"] = (FilterOp.LIKE, f"%{url}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["jump_list_entries"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_jump_list_stats(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, Any]:
    """
    Get jump list entry statistics for an evidence.

    Returns:
        Dict with total_count, by_app_id, url_count
    """
    total = conn.execute(
        "SELECT COUNT(*) FROM jump_list_entries WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0]

    by_app_id = {}
    for row in conn.execute(
        """
        SELECT app_id, COUNT(*) as count
        FROM jump_list_entries
        WHERE evidence_id = ?
        GROUP BY app_id
        """,
        (evidence_id,)
    ):
        by_app_id[row["app_id"]] = row["count"]

    url_count = conn.execute(
        "SELECT COUNT(*) FROM jump_list_entries WHERE evidence_id = ? AND url IS NOT NULL",
        (evidence_id,)
    ).fetchone()[0]

    return {
        "total_count": total,
        "by_app_id": by_app_id,
        "url_count": url_count,
    }


def delete_jump_list_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete jump list entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["jump_list_entries"], evidence_id, run_id)


# Legacy alias (deprecated; target removal)
delete_jump_lists_by_run = delete_jump_list_by_run
