"""
Autofill block list database helper functions.

This module provides CRUD operations for the autofill_block_list table,
which stores Edge-specific data about sites where autofill is disabled.

Initial implementation for Edge autofill_edge_block_list support.

Forensic value:
- Shows financial/sensitive sites user accessed (banking, brokerage, tax)
- Reveals user security awareness (blocking autofill on sensitive sites)
- Device correlation via device_model field
- Timeline context via date_created
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    "insert_autofill_block_list_entry",
    "insert_autofill_block_list_entries",
    "get_autofill_block_list",
    "delete_autofill_block_list_by_run",
]


# Block value type constants (from Edge internal definitions)
BLOCK_TYPE_URL = 0
BLOCK_TYPE_DOMAIN = 1
BLOCK_TYPE_FIELD_SPECIFIC = 3
BLOCK_TYPE_PATTERN = 4

BLOCK_TYPE_NAMES = {
    BLOCK_TYPE_URL: "URL",
    BLOCK_TYPE_DOMAIN: "Domain",
    BLOCK_TYPE_FIELD_SPECIFIC: "Field-specific",
    BLOCK_TYPE_PATTERN: "Pattern",
}


def get_block_type_name(block_type: int | None) -> str:
    """Get human-readable name for block value type."""
    if block_type is None:
        return "Unknown"
    return BLOCK_TYPE_NAMES.get(block_type, f"Type {block_type}")


def insert_autofill_block_list_entry(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    block_value: str,
    **kwargs,
) -> None:
    """
    Insert a single autofill block list entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name (typically "edge")
        block_value: The site/domain/pattern being blocked
        **kwargs: Optional fields (guid, block_value_type, meta_data, device_model, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "guid": kwargs.get("guid"),
        "block_value": block_value,
        "block_value_type": kwargs.get("block_value_type"),
        "attribute_flag": kwargs.get("attribute_flag"),
        "meta_data": kwargs.get("meta_data"),
        "device_model": kwargs.get("device_model"),
        "date_created_utc": kwargs.get("date_created_utc"),
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
    insert_row(conn, TABLE_SCHEMAS["autofill_block_list"], evidence_id, record)


def insert_autofill_block_list_entries(
    conn: sqlite3.Connection,
    evidence_id: int,
    entries: Iterable[Dict[str, Any]]
) -> int:
    """
    Insert multiple autofill block list entries in batch.

    Returns:
        Number of entries inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["autofill_block_list"], evidence_id, entries)


def get_autofill_block_list(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    block_value: Optional[str] = None,
    block_value_type: Optional[int] = None,
    device_model: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve autofill block list entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        block_value: Optional block value substring filter
        block_value_type: Optional block type filter (0=URL, 1=domain, 3=field, 4=pattern)
        device_model: Optional device model filter
        limit: Maximum rows to return

    Returns:
        List of block list records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if block_value:
        filters["block_value"] = (FilterOp.LIKE, f"%{block_value}%")
    if block_value_type is not None:
        filters["block_value_type"] = (FilterOp.EQ, block_value_type)
    if device_model:
        filters["device_model"] = (FilterOp.EQ, device_model)

    return get_rows(
        conn,
        TABLE_SCHEMAS["autofill_block_list"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_autofill_block_list_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str
) -> int:
    """Delete autofill block list entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["autofill_block_list"], evidence_id, run_id)
