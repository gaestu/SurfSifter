"""
HSTS database helper functions.

This module provides CRUD operations for the hsts_entries table.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    "insert_hsts_entry",
    "insert_hsts_entries",
    "get_hsts_entries",
    "get_hsts_stats",
    "delete_hsts_by_run",
]


def insert_hsts_entry(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    domain: str,
    **kwargs,
) -> None:
    """
    Insert a single HSTS entry.

    HSTS entries indicate sites the user has visited that enforce HTTPS.
    HIGH FORENSIC VALUE: Survives browser history clearing.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        domain: HSTS domain
        **kwargs: Optional fields (profile, expiry_utc, include_subdomains, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "domain": domain,
        "expiry_utc": kwargs.get("expiry_utc"),
        "include_subdomains": kwargs.get("include_subdomains"),
        "mode": kwargs.get("mode"),
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
    insert_row(conn, TABLE_SCHEMAS["hsts_entries"], evidence_id, record)


def insert_hsts_entries(conn: sqlite3.Connection, evidence_id: int, entries: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple HSTS entries in batch.

    Returns:
        Number of entries inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["hsts_entries"], evidence_id, entries)


def get_hsts_entries(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    domain: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """
    Retrieve HSTS entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        domain: Optional domain filter (partial match)
        limit: Maximum rows to return

    Returns:
        List of HSTS entry records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if domain:
        filters["domain"] = (FilterOp.LIKE, f"%{domain}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["hsts_entries"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_hsts_stats(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, Any]:
    """
    Get HSTS entry statistics for an evidence.

    Returns:
        Dict with total_count, by_browser, unique_domains
    """
    total = conn.execute(
        "SELECT COUNT(*) FROM hsts_entries WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0]

    by_browser = {}
    for row in conn.execute(
        """
        SELECT browser, COUNT(*) as count
        FROM hsts_entries
        WHERE evidence_id = ?
        GROUP BY browser
        """,
        (evidence_id,)
    ):
        by_browser[row["browser"]] = row["count"]

    unique_domains = conn.execute(
        "SELECT COUNT(DISTINCT domain) FROM hsts_entries WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0]

    return {
        "total_count": total,
        "by_browser": by_browser,
        "unique_domains": unique_domains,
    }


def delete_hsts_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete HSTS entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["hsts_entries"], evidence_id, run_id)
