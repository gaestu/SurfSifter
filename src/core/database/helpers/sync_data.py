"""
Sync data database helper functions.

This module provides CRUD operations for sync_data and synced_devices tables.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    # Sync data
    "insert_sync_data",
    "insert_sync_datas",
    "insert_sync_data_row",  # alias
    "get_sync_data",
    "delete_sync_data_by_run",
    # Synced devices
    "insert_synced_device",
    "insert_synced_devices",
    "insert_synced_device_row",  # alias
    "get_synced_devices",
    "delete_synced_devices_by_run",
    # Stats
    "get_sync_stats",
]


# ============================================================================
# Sync Data
# ============================================================================

def insert_sync_data(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    account_email: str,
    **kwargs,
) -> None:
    """
    Insert a single sync data entry.

    Sync data tracks browser sync account info and last sync timestamps.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        account_email: Sync account email
        **kwargs: Optional fields (profile, account_id, sync_enabled, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "account_email": account_email,
        "account_id": kwargs.get("account_id"),
        "sync_enabled": kwargs.get("sync_enabled"),
        "last_sync_utc": kwargs.get("last_sync_utc"),
        "sync_types": kwargs.get("sync_types"),  # JSON array
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
    insert_row(conn, TABLE_SCHEMAS["sync_data"], evidence_id, record)


def insert_sync_datas(conn: sqlite3.Connection, evidence_id: int, entries: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple sync data entries in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["sync_data"], evidence_id, entries)


def get_sync_data(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    account_email: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Retrieve sync data entries for an evidence."""
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if account_email:
        filters["account_email"] = (FilterOp.LIKE, f"%{account_email}%")
    return get_rows(conn, TABLE_SCHEMAS["sync_data"], evidence_id, filters=filters or None, limit=limit)


def delete_sync_data_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete sync data from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["sync_data"], evidence_id, run_id)


# ============================================================================
# Synced Devices
# ============================================================================

def insert_synced_device(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    device_name: str,
    **kwargs,
) -> None:
    """
    Insert a single synced device entry.

    Synced devices track other devices in the browser sync network.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        device_name: Device name
        **kwargs: Optional fields (profile, device_id, device_type, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "device_id": kwargs.get("device_id"),
        "device_name": device_name,
        "device_type": kwargs.get("device_type"),  # desktop, phone, tablet
        "last_updated_utc": kwargs.get("last_updated_utc"),
        "chrome_version": kwargs.get("chrome_version"),
        "os_type": kwargs.get("os_type"),
        "is_this_device": kwargs.get("is_this_device"),
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
    insert_row(conn, TABLE_SCHEMAS["synced_devices"], evidence_id, record)


def insert_synced_devices(conn: sqlite3.Connection, evidence_id: int, devices: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple synced devices in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["synced_devices"], evidence_id, devices)


def get_synced_devices(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    device_type: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Retrieve synced devices for an evidence."""
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if device_type:
        filters["device_type"] = (FilterOp.EQ, device_type)
    return get_rows(conn, TABLE_SCHEMAS["synced_devices"], evidence_id, filters=filters or None, limit=limit)


def delete_synced_devices_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete synced devices from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["synced_devices"], evidence_id, run_id)


# ============================================================================
# Stats Functions
# ============================================================================

def get_sync_stats(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Any]:
    """Get sync data statistics.

    Returns counts of sync entries, synced devices by type, and accounts.
    """
    result: Dict[str, Any] = {}

    # Sync data count
    cur = conn.execute(
        "SELECT COUNT(*) FROM sync_data WHERE evidence_id = ?",
        (evidence_id,),
    )
    result["sync_data_count"] = cur.fetchone()[0]

    # Synced devices by type
    cur = conn.execute(
        """
        SELECT device_type, COUNT(*) as count
        FROM synced_devices
        WHERE evidence_id = ?
        GROUP BY device_type
        ORDER BY count DESC
        """,
        (evidence_id,),
    )
    result["devices_by_type"] = {row[0]: row[1] for row in cur.fetchall()}

    # Total synced devices
    cur = conn.execute(
        "SELECT COUNT(*) FROM synced_devices WHERE evidence_id = ?",
        (evidence_id,),
    )
    result["total_devices"] = cur.fetchone()[0]

    # Unique accounts
    cur = conn.execute(
        "SELECT COUNT(DISTINCT account_email) FROM sync_data WHERE evidence_id = ?",
        (evidence_id,),
    )
    result["unique_accounts"] = cur.fetchone()[0]

    return result


# ============================================================================
# Legacy aliases
# ============================================================================
insert_sync_data_row = insert_sync_data
insert_synced_device_row = insert_synced_device
