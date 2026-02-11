"""
Autofill IBAN database helper functions.

This module provides CRUD operations for the autofill_ibans table, storing
Chromium/Edge IBAN artifacts from Web Data (local_ibans and masked_ibans).
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_distinct_values, get_rows, insert_row, insert_rows

__all__ = [
    "insert_autofill_iban",
    "insert_autofill_ibans",
    "get_autofill_ibans",
    "get_distinct_autofill_iban_browsers",
    "delete_autofill_ibans_by_run",
]


def insert_autofill_iban(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    source_table: str,
    **kwargs,
) -> None:
    """
    Insert a single autofill IBAN entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        source_table: Source table name (local_ibans/masked_ibans)
        **kwargs: Optional IBAN and provenance fields
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "source_table": source_table,
        "guid": kwargs.get("guid"),
        "instrument_id": kwargs.get("instrument_id"),
        "nickname": kwargs.get("nickname"),
        "value": kwargs.get("value"),
        "value_encrypted": kwargs.get("value_encrypted"),
        "prefix": kwargs.get("prefix"),
        "suffix": kwargs.get("suffix"),
        "length": kwargs.get("length"),
        "use_count": kwargs.get("use_count"),
        "use_date_utc": kwargs.get("use_date_utc"),
        "date_modified_utc": kwargs.get("date_modified_utc"),
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
    insert_row(conn, TABLE_SCHEMAS["autofill_ibans"], evidence_id, record)


def insert_autofill_ibans(
    conn: sqlite3.Connection,
    evidence_id: int,
    ibans: Iterable[Dict[str, Any]],
) -> int:
    """Insert multiple autofill IBAN entries in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["autofill_ibans"], evidence_id, ibans)


def get_autofill_ibans(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    source_table: Optional[str] = None,
    nickname: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve autofill IBAN entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        source_table: Optional source table filter
        nickname: Optional nickname substring filter
        limit: Maximum rows to return

    Returns:
        List of autofill IBAN records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if source_table:
        filters["source_table"] = (FilterOp.EQ, source_table)
    if nickname:
        filters["nickname"] = (FilterOp.LIKE, f"%{nickname}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["autofill_ibans"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_distinct_autofill_iban_browsers(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[str]:
    """Get distinct browser names from autofill_ibans table."""
    return get_distinct_values(
        conn,
        TABLE_SCHEMAS["autofill_ibans"],
        evidence_id,
        "browser",
    )


def delete_autofill_ibans_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete autofill IBAN entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["autofill_ibans"], evidence_id, run_id)
