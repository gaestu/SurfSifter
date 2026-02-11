"""
Permissions database helper functions.

This module provides CRUD operations for the site_permissions table.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_distinct_values, get_rows, insert_row, insert_rows

__all__ = [
    "insert_permission",
    "insert_permissions",
    "insert_site_permissions",  # legacy alias
    "get_permissions",
    "get_site_permissions",  # legacy alias
    "get_distinct_permission_types",
    "delete_permissions_by_run",
]


def insert_permission(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    origin: str,
    permission_type: str,
    **kwargs,
) -> None:
    """
    Insert a single site permission entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        origin: Site origin URL
        permission_type: Permission type (notifications, geolocation, camera, etc.)
        **kwargs: Optional fields (profile, setting, expiration_utc, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "origin": origin,
        "permission_type": permission_type,
        "setting": kwargs.get("setting"),
        "raw_value": kwargs.get("raw_value"),
        "expiration_utc": kwargs.get("expiration_utc"),
        "last_modified_utc": kwargs.get("last_modified_utc"),
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
    insert_row(conn, TABLE_SCHEMAS["site_permissions"], evidence_id, record)


def insert_permissions(conn: sqlite3.Connection, evidence_id: int, perms: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple site permissions in batch.

    Returns:
        Number of permissions inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["site_permissions"], evidence_id, perms)


# Legacy alias (deprecated; target removal)
insert_site_permissions = insert_permissions


def get_permissions(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    permission_type: Optional[str] = None,
    origin: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve site permissions for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        permission_type: Optional permission type filter
        origin: Optional origin filter (partial match)
        limit: Maximum rows to return

    Returns:
        List of permission records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if permission_type:
        filters["permission_type"] = (FilterOp.EQ, permission_type)
    if origin:
        filters["origin"] = (FilterOp.LIKE, f"%{origin}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["site_permissions"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


# Legacy alias (deprecated; target removal)
get_site_permissions = get_permissions


def get_distinct_permission_types(conn: sqlite3.Connection, evidence_id: int) -> List[str]:
    """Get distinct permission types for an evidence (sorted)."""
    return get_distinct_values(conn, TABLE_SCHEMAS["site_permissions"], evidence_id, "permission_type")


def delete_permissions_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete site permissions from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["site_permissions"], evidence_id, run_id)
