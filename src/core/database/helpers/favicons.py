"""
Favicon database helper functions.

This module provides CRUD operations for favicons, favicon_mappings, and top_sites tables.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    # Favicons
    "insert_favicon",
    "insert_favicons",
    "get_favicons",
    "get_favicon_by_hash",
    "get_favicon_by_id",
    "delete_favicons_by_run",
    "get_favicon_stats",
    # Favicon mappings
    "insert_favicon_mapping",
    "insert_favicon_mappings",
    "get_favicon_mappings",
    "delete_favicon_mappings_by_run",
    # Top sites
    "insert_top_site",
    "insert_top_sites",
    "get_top_sites",
    "get_top_sites_stats",
    "delete_top_sites_by_run",
]


# ============================================================================
# Favicons
# ============================================================================

def insert_favicon(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    icon_url: str,
    **kwargs,
) -> int:
    """
    Insert a single favicon entry.

    Favicons are deduplicated by SHA256 hash within each evidence/browser.
    Icon data is stored on disk only (not in DB) - use extracted_path for location.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        icon_url: Favicon URL
        **kwargs: Optional fields:
            - profile: Browser profile name
            - icon_md5: MD5 hash of icon data
            - icon_sha256: SHA256 hash of icon data
            - icon_type: 1=favicon, 2=touch_icon, 4=touch_precomposed
            - width, height: Icon dimensions in pixels
            - last_updated_utc, last_requested_utc: Timestamps
            - run_id, source_path, partition_index, fs_type
            - logical_path, forensic_path, notes

    Returns:
        Row ID of inserted favicon (or existing if deduplicated)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "icon_url": icon_url,
        "icon_md5": kwargs.get("icon_md5"),
        "icon_sha256": kwargs.get("icon_sha256"),
        "icon_type": kwargs.get("icon_type"),
        "width": kwargs.get("width"),
        "height": kwargs.get("height"),
        "icon_data": None,  # Not stored in DB - icons saved to disk only
        "last_updated_utc": kwargs.get("last_updated_utc"),
        "last_requested_utc": kwargs.get("last_requested_utc"),
        "run_id": kwargs.get("run_id"),
        "source_path": kwargs.get("source_path"),
        "partition_index": kwargs.get("partition_index"),
        "fs_type": kwargs.get("fs_type"),
        "logical_path": kwargs.get("logical_path"),
        "forensic_path": kwargs.get("forensic_path"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["favicons"], evidence_id, record)

    # Return the ID (may be existing row if deduplicated by sha256)
    sha256 = kwargs.get("icon_sha256")
    if sha256:
        row = conn.execute(
            "SELECT id FROM favicons WHERE evidence_id = ? AND icon_sha256 = ?",
            (evidence_id, sha256)
        ).fetchone()
        return row[0] if row else conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def insert_favicons(conn: sqlite3.Connection, evidence_id: int, favicons: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple favicons in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["favicons"], evidence_id, favicons)


def get_favicons(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    icon_type: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Retrieve favicons for an evidence."""
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if icon_type:
        filters["icon_type"] = (FilterOp.EQ, icon_type)
    return get_rows(conn, TABLE_SCHEMAS["favicons"], evidence_id, filters=filters or None, limit=limit)


def get_favicon_by_hash(conn: sqlite3.Connection, evidence_id: int, sha256: str) -> Optional[Dict[str, Any]]:
    """Get favicon by SHA256 hash."""
    row = conn.execute(
        "SELECT * FROM favicons WHERE evidence_id = ? AND icon_sha256 = ?",
        (evidence_id, sha256)
    ).fetchone()
    return dict(row) if row else None


def delete_favicons_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete favicons from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["favicons"], evidence_id, run_id)


# ============================================================================
# Favicon Mappings
# ============================================================================

def insert_favicon_mapping(
    conn: sqlite3.Connection,
    evidence_id: int,
    favicon_id: int,
    page_url: str,
    **kwargs,
) -> None:
    """
    Insert a single favicon mapping entry.

    Maps page URLs to their favicon icons.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        favicon_id: Parent favicon ID (FK to favicons.id)
        page_url: Page URL that uses this favicon
        **kwargs: Optional fields (run_id, etc.)
    """
    record = {
        "favicon_id": favicon_id,
        "page_url": page_url,
        "run_id": kwargs.get("run_id"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["favicon_mappings"], evidence_id, record)


def insert_favicon_mappings(conn: sqlite3.Connection, evidence_id: int, mappings: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple favicon mappings in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["favicon_mappings"], evidence_id, mappings)


def get_favicon_mappings(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    favicon_id: Optional[int] = None,
    page_url: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """Retrieve favicon mappings for an evidence."""
    filters: Dict[str, Any] = {}
    if favicon_id is not None:
        filters["favicon_id"] = (FilterOp.EQ, favicon_id)
    if page_url:
        filters["page_url"] = (FilterOp.LIKE, f"%{page_url}%")
    return get_rows(conn, TABLE_SCHEMAS["favicon_mappings"], evidence_id, filters=filters or None, limit=limit)


def delete_favicon_mappings_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete favicon mappings from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["favicon_mappings"], evidence_id, run_id)


# ============================================================================
# Top Sites
# ============================================================================

def insert_top_site(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    url: str,
    **kwargs,
) -> None:
    """
    Insert a single top site entry.

    Top sites are frequently visited sites shown on new tab pages.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        url: Top site URL
        **kwargs: Optional fields (profile, title, url_rank, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "url": url,
        "title": kwargs.get("title"),
        "url_rank": kwargs.get("url_rank"),
        "redirect_urls": kwargs.get("redirect_urls"),  # JSON array
        "last_forced_time_utc": kwargs.get("last_forced_time_utc"),
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
    insert_row(conn, TABLE_SCHEMAS["top_sites"], evidence_id, record)


def insert_top_sites(conn: sqlite3.Connection, evidence_id: int, sites: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple top sites in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["top_sites"], evidence_id, sites)


def get_top_sites(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Retrieve top sites for an evidence, ordered by rank."""
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)

    # Custom query to order by url_rank
    conditions = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]
    if browser:
        conditions.append("browser = ?")
        params.append(browser)
    params.append(limit)

    rows = conn.execute(
        f"SELECT * FROM top_sites WHERE {' AND '.join(conditions)} ORDER BY url_rank LIMIT ?",
        params
    ).fetchall()
    return [dict(row) for row in rows]


def delete_top_sites_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete top sites from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["top_sites"], evidence_id, run_id)


# ============================================================================
# Additional Functions
# ============================================================================

def get_favicon_by_id(conn: sqlite3.Connection, evidence_id: int, favicon_id: int) -> Optional[Dict[str, Any]]:
    """Get favicon by ID."""
    row = conn.execute(
        "SELECT * FROM favicons WHERE evidence_id = ? AND id = ?",
        (evidence_id, favicon_id)
    ).fetchone()
    return dict(row) if row else None


def get_favicon_stats(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Any]:
    """Get favicon statistics.

    Returns counts of favicons by browser and type.
    """
    result: Dict[str, Any] = {}

    # Total favicons
    cur = conn.execute(
        "SELECT COUNT(*) FROM favicons WHERE evidence_id = ?",
        (evidence_id,),
    )
    result["total_favicons"] = cur.fetchone()[0]

    # By browser
    cur = conn.execute(
        """
        SELECT browser, COUNT(*) as count
        FROM favicons
        WHERE evidence_id = ?
        GROUP BY browser
        ORDER BY count DESC
        """,
        (evidence_id,),
    )
    result["by_browser"] = {row[0]: row[1] for row in cur.fetchall()}

    # By icon type
    cur = conn.execute(
        """
        SELECT icon_type, COUNT(*) as count
        FROM favicons
        WHERE evidence_id = ?
        GROUP BY icon_type
        ORDER BY count DESC
        """,
        (evidence_id,),
    )
    result["by_type"] = {row[0]: row[1] for row in cur.fetchall()}

    # Total mappings
    cur = conn.execute(
        "SELECT COUNT(*) FROM favicon_mappings WHERE evidence_id = ?",
        (evidence_id,),
    )
    result["total_mappings"] = cur.fetchone()[0]

    return result


def get_top_sites_stats(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Any]:
    """Get top sites statistics.

    Returns counts of top sites by browser.
    """
    result: Dict[str, Any] = {}

    # Total top sites
    cur = conn.execute(
        "SELECT COUNT(*) FROM top_sites WHERE evidence_id = ?",
        (evidence_id,),
    )
    result["total_top_sites"] = cur.fetchone()[0]

    # By browser
    cur = conn.execute(
        """
        SELECT browser, COUNT(*) as count
        FROM top_sites
        WHERE evidence_id = ?
        GROUP BY browser
        ORDER BY count DESC
        """,
        (evidence_id,),
    )
    result["by_browser"] = {row[0]: row[1] for row in cur.fetchall()}

    return result
