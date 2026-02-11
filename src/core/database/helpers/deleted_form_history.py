"""
Deleted form history database helper functions.

This module provides CRUD operations for the deleted_form_history table,
which stores Firefox moz_deleted_formhistory entries.

Added for autofill enhancement feature.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_distinct_values, get_rows, insert_row, insert_rows

__all__ = [
    "insert_deleted_form_history",
    "insert_deleted_form_history_entries",
    "get_deleted_form_history",
    "get_distinct_deleted_form_history_browsers",
    "delete_deleted_form_history_by_run",
]


def insert_deleted_form_history(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    **kwargs,
) -> None:
    """
    Insert a single deleted form history entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        **kwargs: Optional fields (guid, time_deleted_utc, original_fieldname, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "guid": kwargs.get("guid"),
        "time_deleted_utc": kwargs.get("time_deleted_utc"),
        "original_fieldname": kwargs.get("original_fieldname"),
        "original_value": kwargs.get("original_value"),
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
    insert_row(conn, TABLE_SCHEMAS["deleted_form_history"], evidence_id, record)


def insert_deleted_form_history_entries(
    conn: sqlite3.Connection,
    evidence_id: int,
    entries: Iterable[Dict[str, Any]],
) -> int:
    """
    Insert multiple deleted form history entries in batch.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        entries: Iterable of deleted form history records

    Returns:
        Number of records inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["deleted_form_history"], evidence_id, entries)


def get_deleted_form_history(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    guid: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve deleted form history entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        guid: Optional GUID filter (exact match)
        limit: Maximum rows to return

    Returns:
        List of deleted form history records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if guid:
        filters["guid"] = (FilterOp.EQ, guid)

    return get_rows(
        conn,
        TABLE_SCHEMAS["deleted_form_history"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_distinct_deleted_form_history_browsers(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[str]:
    """Get distinct browser names from deleted_form_history table."""
    return get_distinct_values(
        conn,
        TABLE_SCHEMAS["deleted_form_history"],
        evidence_id,
        "browser",
    )


def delete_deleted_form_history_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete deleted form history entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["deleted_form_history"], evidence_id, run_id)
