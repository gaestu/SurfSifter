"""
Database helper functions for stored_sites table.

Provides CRUD operations for the stored_sites materialized view,
which aggregates web storage data by origin for tagging support.

Created for unified tagging of stored sites.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional

__all__ = [
    "insert_stored_site",
    "upsert_stored_site",
    "get_stored_sites",
    "get_stored_site_by_id",
    "get_stored_site_by_origin",
    "refresh_stored_sites",
    "delete_stored_sites_by_evidence",
    "get_stored_sites_for_report",
]


def insert_stored_site(
    conn: sqlite3.Connection,
    evidence_id: int,
    origin: str,
    local_storage_count: int = 0,
    session_storage_count: int = 0,
    indexeddb_count: int = 0,
    browsers: Optional[List[str]] = None,
    **kwargs,
) -> int:
    """
    Insert a single stored site record.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        origin: Site origin (e.g., "https://example.com")
        local_storage_count: Number of local storage keys
        session_storage_count: Number of session storage keys
        indexeddb_count: Number of IndexedDB databases
        browsers: List of browser names that have data for this site
        **kwargs: Optional fields (tags, notes)

    Returns:
        ID of inserted row
    """
    total = local_storage_count + session_storage_count + indexeddb_count
    now = datetime.utcnow().isoformat()

    cursor = conn.execute(
        """
        INSERT INTO stored_sites (
            evidence_id, origin, local_storage_count, session_storage_count,
            indexeddb_count, total_keys, browsers, first_seen_utc,
            last_updated_utc, tags, notes, created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence_id,
            origin,
            local_storage_count,
            session_storage_count,
            indexeddb_count,
            total,
            json.dumps(browsers) if browsers else None,
            now,
            now,
            kwargs.get("tags"),
            kwargs.get("notes"),
            now,
        ),
    )
    conn.commit()
    return cursor.lastrowid


def upsert_stored_site(
    conn: sqlite3.Connection,
    evidence_id: int,
    origin: str,
    local_storage_count: int = 0,
    session_storage_count: int = 0,
    indexeddb_count: int = 0,
    browsers: Optional[List[str]] = None,
) -> int:
    """
    Insert or update a stored site record.

    Preserves existing tags and notes on update.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        origin: Site origin
        local_storage_count: Number of local storage keys
        session_storage_count: Number of session storage keys
        indexeddb_count: Number of IndexedDB databases
        browsers: List of browser names

    Returns:
        ID of upserted row
    """
    total = local_storage_count + session_storage_count + indexeddb_count
    now = datetime.utcnow().isoformat()

    cursor = conn.execute(
        """
        INSERT INTO stored_sites (
            evidence_id, origin, local_storage_count, session_storage_count,
            indexeddb_count, total_keys, browsers, first_seen_utc,
            last_updated_utc, created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(evidence_id, origin) DO UPDATE SET
            local_storage_count = excluded.local_storage_count,
            session_storage_count = excluded.session_storage_count,
            indexeddb_count = excluded.indexeddb_count,
            total_keys = excluded.total_keys,
            browsers = excluded.browsers,
            last_updated_utc = excluded.last_updated_utc
        """,
        (
            evidence_id,
            origin,
            local_storage_count,
            session_storage_count,
            indexeddb_count,
            total,
            json.dumps(browsers) if browsers else None,
            now,
            now,
            now,
        ),
    )
    conn.commit()

    # Get the ID (either new or existing)
    row = conn.execute(
        "SELECT id FROM stored_sites WHERE evidence_id = ? AND origin = ?",
        (evidence_id, origin),
    ).fetchone()
    return row[0] if row else 0


def get_stored_sites(
    conn: sqlite3.Connection,
    evidence_id: int,
    origin_filter: Optional[str] = None,
    min_total: int = 0,
    limit: int = 10000,
) -> List[Dict[str, Any]]:
    """
    Retrieve stored sites for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        origin_filter: Origin substring filter
        min_total: Minimum total keys filter
        limit: Maximum rows to return

    Returns:
        List of stored site records as dicts
    """
    query = """
        SELECT id, evidence_id, origin, local_storage_count, session_storage_count,
               indexeddb_count, total_keys, browsers, first_seen_utc,
               last_updated_utc, tags, notes, created_at_utc
        FROM stored_sites
        WHERE evidence_id = ?
    """
    params: List[Any] = [evidence_id]

    if origin_filter:
        query += " AND origin LIKE ?"
        params.append(f"%{origin_filter}%")

    if min_total > 0:
        query += " AND total_keys >= ?"
        params.append(min_total)

    query += " ORDER BY total_keys DESC LIMIT ?"
    params.append(limit)

    conn.row_factory = sqlite3.Row
    cursor = conn.execute(query, params)

    rows = []
    for row in cursor.fetchall():
        record = dict(row)
        # Parse browsers JSON
        if record.get("browsers"):
            try:
                record["browsers"] = json.loads(record["browsers"])
            except (json.JSONDecodeError, TypeError):
                record["browsers"] = []
        else:
            record["browsers"] = []
        rows.append(record)

    return rows


def get_stored_site_by_id(
    conn: sqlite3.Connection,
    site_id: int,
) -> Optional[Dict[str, Any]]:
    """Get a single stored site by ID."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT id, evidence_id, origin, local_storage_count, session_storage_count,
               indexeddb_count, total_keys, browsers, first_seen_utc,
               last_updated_utc, tags, notes, created_at_utc
        FROM stored_sites WHERE id = ?
        """,
        (site_id,),
    ).fetchone()

    if not row:
        return None

    record = dict(row)
    if record.get("browsers"):
        try:
            record["browsers"] = json.loads(record["browsers"])
        except (json.JSONDecodeError, TypeError):
            record["browsers"] = []
    else:
        record["browsers"] = []

    return record


def get_stored_site_by_origin(
    conn: sqlite3.Connection,
    evidence_id: int,
    origin: str,
) -> Optional[Dict[str, Any]]:
    """Get a single stored site by origin."""
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT id, evidence_id, origin, local_storage_count, session_storage_count,
               indexeddb_count, total_keys, browsers, first_seen_utc,
               last_updated_utc, tags, notes, created_at_utc
        FROM stored_sites WHERE evidence_id = ? AND origin = ?
        """,
        (evidence_id, origin),
    ).fetchone()

    if not row:
        return None

    record = dict(row)
    if record.get("browsers"):
        try:
            record["browsers"] = json.loads(record["browsers"])
        except (json.JSONDecodeError, TypeError):
            record["browsers"] = []
    else:
        record["browsers"] = []

    return record


def refresh_stored_sites(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> int:
    """
    Refresh stored_sites table from underlying storage tables.

    This aggregates data from local_storage, session_storage, and
    indexeddb_databases tables to create/update stored_sites records.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        Number of sites created/updated
    """
    # Get local storage counts per origin
    local_counts: Dict[str, int] = {}
    local_browsers: Dict[str, set] = {}
    cursor = conn.execute(
        """
        SELECT origin, browser, COUNT(*) as count
        FROM local_storage
        WHERE evidence_id = ?
        GROUP BY origin, browser
        """,
        (evidence_id,),
    )
    for row in cursor.fetchall():
        origin, browser, count = row
        local_counts[origin] = local_counts.get(origin, 0) + count
        if origin not in local_browsers:
            local_browsers[origin] = set()
        if browser:
            local_browsers[origin].add(browser)

    # Get session storage counts per origin
    session_counts: Dict[str, int] = {}
    session_browsers: Dict[str, set] = {}
    cursor = conn.execute(
        """
        SELECT origin, browser, COUNT(*) as count
        FROM session_storage
        WHERE evidence_id = ?
        GROUP BY origin, browser
        """,
        (evidence_id,),
    )
    for row in cursor.fetchall():
        origin, browser, count = row
        session_counts[origin] = session_counts.get(origin, 0) + count
        if origin not in session_browsers:
            session_browsers[origin] = set()
        if browser:
            session_browsers[origin].add(browser)

    # Get IndexedDB counts per origin
    indexeddb_counts: Dict[str, int] = {}
    indexeddb_browsers: Dict[str, set] = {}
    cursor = conn.execute(
        """
        SELECT origin, browser, COUNT(*) as count
        FROM indexeddb_databases
        WHERE evidence_id = ?
        GROUP BY origin, browser
        """,
        (evidence_id,),
    )
    for row in cursor.fetchall():
        origin, browser, count = row
        indexeddb_counts[origin] = indexeddb_counts.get(origin, 0) + count
        if origin not in indexeddb_browsers:
            indexeddb_browsers[origin] = set()
        if browser:
            indexeddb_browsers[origin].add(browser)

    # Collect all unique origins
    all_origins = set(local_counts.keys()) | set(session_counts.keys()) | set(indexeddb_counts.keys())

    # Upsert each origin
    count = 0
    for origin in all_origins:
        # Merge browsers from all sources
        browsers = set()
        browsers.update(local_browsers.get(origin, set()))
        browsers.update(session_browsers.get(origin, set()))
        browsers.update(indexeddb_browsers.get(origin, set()))

        upsert_stored_site(
            conn,
            evidence_id,
            origin,
            local_storage_count=local_counts.get(origin, 0),
            session_storage_count=session_counts.get(origin, 0),
            indexeddb_count=indexeddb_counts.get(origin, 0),
            browsers=sorted(browsers) if browsers else None,
        )
        count += 1

    return count


def delete_stored_sites_by_evidence(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> int:
    """Delete all stored sites for an evidence."""
    cursor = conn.execute(
        "DELETE FROM stored_sites WHERE evidence_id = ?",
        (evidence_id,),
    )
    conn.commit()
    return cursor.rowcount


def get_stored_sites_for_report(
    conn: sqlite3.Connection,
    evidence_id: int,
    site_ids: Optional[List[int]] = None,
    tagged_only: bool = False,
) -> List[Dict[str, Any]]:
    """
    Get stored sites with full details for report generation.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        site_ids: Optional list of specific site IDs to include
        tagged_only: If True, only return sites with tags

    Returns:
        List of stored sites with full details
    """
    query = """
        SELECT id, evidence_id, origin, local_storage_count, session_storage_count,
               indexeddb_count, total_keys, browsers, first_seen_utc,
               last_updated_utc, tags, notes
        FROM stored_sites
        WHERE evidence_id = ?
    """
    params: List[Any] = [evidence_id]

    if site_ids:
        placeholders = ",".join("?" * len(site_ids))
        query += f" AND id IN ({placeholders})"
        params.extend(site_ids)

    if tagged_only:
        query += " AND tags IS NOT NULL AND tags != ''"

    query += " ORDER BY total_keys DESC"

    conn.row_factory = sqlite3.Row
    cursor = conn.execute(query, params)

    sites = []
    for row in cursor.fetchall():
        site = dict(row)

        # Parse browsers
        if site.get("browsers"):
            try:
                site["browsers"] = json.loads(site["browsers"])
            except (json.JSONDecodeError, TypeError):
                site["browsers"] = []
        else:
            site["browsers"] = []

        sites.append(site)

    return sites
