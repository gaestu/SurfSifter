"""
Browser extensions database helper functions.

This module provides CRUD operations for the browser_extensions table.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    "insert_extension",
    "insert_extensions",
    "insert_browser_extension_row",  # legacy alias
    "insert_browser_extensions",  # legacy alias
    "get_extensions",
    "get_browser_extensions",  # legacy alias
    "get_extension_stats",
    "get_browser_extension_stats",  # legacy alias
    "delete_extensions_by_run",
    "delete_browser_extensions_by_run",  # legacy alias
]


def insert_extension(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    extension_id: str,
    name: str,
    **kwargs,
) -> None:
    """
    Insert a single browser extension entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        extension_id: Extension ID (Chrome Store ID, Firefox UUID, etc.)
        name: Extension name
        **kwargs: Optional fields (profile, version, description, permissions, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "extension_id": extension_id,
        "name": name,
        "version": kwargs.get("version"),
        "description": kwargs.get("description"),
        "enabled": kwargs.get("enabled"),
        "permissions": kwargs.get("permissions"),  # JSON array
        "risk_level": kwargs.get("risk_level"),  # critical, high, medium, low
        "known_extension": kwargs.get("known_extension"),  # Matched from reference list
        "install_time_utc": kwargs.get("install_time_utc"),
        "update_time_utc": kwargs.get("update_time_utc"),
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
    insert_row(conn, TABLE_SCHEMAS["browser_extensions"], evidence_id, record)


def insert_extensions(conn: sqlite3.Connection, evidence_id: int, extensions: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple browser extensions in batch.

    Returns:
        Number of extensions inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["browser_extensions"], evidence_id, extensions)


def get_extensions(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    risk_level: Optional[str] = None,
    min_risk_score: Optional[int] = None,
    category: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve browser extensions for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        risk_level: Optional risk level filter (deprecated, use min_risk_score)
        min_risk_score: Optional minimum risk score filter (0-100)
        category: Optional known_category filter
        limit: Maximum rows to return

    Returns:
        List of extension records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if risk_level:
        # Legacy support - filter by exact risk_level text if stored
        filters["risk_level"] = (FilterOp.EQ, risk_level)
    if min_risk_score is not None and min_risk_score > 0:
        filters["risk_score"] = (FilterOp.GTE, min_risk_score)
    if category:
        filters["known_category"] = (FilterOp.EQ, category)

    return get_rows(
        conn,
        TABLE_SCHEMAS["browser_extensions"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_extension_stats(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, Any]:
    """
    Get browser extension statistics for an evidence.

    Returns:
        Dict with total_count, by_browser, by_risk_level, known_count
    """
    total = conn.execute(
        "SELECT COUNT(*) FROM browser_extensions WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0]

    by_browser = {}
    for row in conn.execute(
        """
        SELECT browser, COUNT(*) as count
        FROM browser_extensions
        WHERE evidence_id = ?
        GROUP BY browser
        """,
        (evidence_id,)
    ):
        by_browser[row["browser"]] = row["count"]

    by_risk_level = {}
    for row in conn.execute(
        """
        SELECT risk_level, COUNT(*) as count
        FROM browser_extensions
        WHERE evidence_id = ?
        GROUP BY risk_level
        """,
        (evidence_id,)
    ):
        by_risk_level[row["risk_level"] or "unknown"] = row["count"]

    known_count = conn.execute(
        "SELECT COUNT(*) FROM browser_extensions WHERE evidence_id = ? AND known_extension IS NOT NULL",
        (evidence_id,)
    ).fetchone()[0]

    return {
        "total_count": total,
        "by_browser": by_browser,
        "by_risk_level": by_risk_level,
        "known_count": known_count,
    }


def delete_extensions_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete browser extensions from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["browser_extensions"], evidence_id, run_id)


# ============================================================================
# Legacy Aliases (backward compatibility with db.py)
# Deprecated compatibility shims for older import paths.
# Planned removal window: first stable major release after 0.2.x (target ).
# ============================================================================

insert_browser_extension_row = insert_extension
insert_browser_extensions = insert_extensions
get_browser_extensions = get_extensions
get_browser_extension_stats = get_extension_stats
delete_browser_extensions_by_run = delete_extensions_by_run
