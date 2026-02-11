"""
Cookie database helper functions.

This module provides CRUD operations for the cookies table.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_distinct_values, get_rows, insert_row, insert_rows

__all__ = [
    "insert_cookie_row",
    "insert_cookies",
    "get_cookies",
    "get_cookie_domains",
    "get_cookie_by_id",
    "get_distinct_cookie_browsers",
    "delete_cookies_by_run",
]


def insert_cookie_row(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    name: str,
    domain: str,
    *,
    profile: Optional[str] = None,
    value: Optional[str] = None,
    path: Optional[str] = None,
    expires_utc: Optional[str] = None,
    is_secure: Optional[int] = None,
    is_httponly: Optional[int] = None,
    samesite: Optional[str] = None,
    samesite_raw: Optional[int] = None,
    creation_utc: Optional[str] = None,
    last_access_utc: Optional[str] = None,
    encrypted: int = 0,
    encrypted_value: Optional[bytes] = None,
    # Firefox originAttributes: Container tabs, private browsing, FPI, state partitioning
    origin_attributes: Optional[str] = None,
    user_context_id: Optional[int] = None,
    private_browsing_id: Optional[int] = None,
    first_party_domain: Optional[str] = None,
    partition_key: Optional[str] = None,
    run_id: Optional[str] = None,
    source_path: Optional[str] = None,
    discovered_by: Optional[str] = None,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    tags: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Insert a single cookie row with full forensic provenance.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name ('chrome', 'firefox', 'edge', 'opera', 'brave')
        name: Cookie name
        domain: Cookie domain
        profile: Browser profile name
        value: Cookie value (may be None if encrypted)
        path: Cookie path
        expires_utc: ISO 8601 expiration timestamp
        is_secure: 1 if Secure flag set, 0 otherwise
        is_httponly: 1 if HttpOnly flag set, 0 otherwise
        samesite: SameSite attribute ('Strict', 'Lax', 'None')
        samesite_raw: Original integer SameSite value from Firefox
        creation_utc: ISO 8601 creation timestamp
        last_access_utc: ISO 8601 last access timestamp
        encrypted: 1 if value is encrypted (Chromium DPAPI)
        encrypted_value: Raw encrypted bytes for forensic record
        origin_attributes: Raw Firefox originAttributes string
        user_context_id: Firefox container tab ID (0=default, 1+=containers)
        private_browsing_id: Firefox private browsing indicator (0=normal, 1=private)
        first_party_domain: Firefox First-Party Isolation domain
        partition_key: Firefox State Partitioning key
        run_id: Extraction run ID
        source_path: Original path in evidence
        discovered_by: Extractor signature
        partition_index: E01 partition number
        fs_type: Filesystem type
        logical_path: Windows-style path
        forensic_path: Canonical E01 identifier
        tags: JSON-serialized tags
        notes: Investigator notes
    """
    record = {
        "browser": browser,
        "profile": profile,
        "name": name,
        "value": value,
        "domain": domain,
        "path": path,
        "expires_utc": expires_utc,
        "is_secure": is_secure,
        "is_httponly": is_httponly,
        "samesite": samesite,
        "samesite_raw": samesite_raw,
        "creation_utc": creation_utc,
        "last_access_utc": last_access_utc,
        "encrypted": encrypted,
        "encrypted_value": encrypted_value,
        "origin_attributes": origin_attributes,
        "user_context_id": user_context_id,
        "private_browsing_id": private_browsing_id,
        "first_party_domain": first_party_domain,
        "partition_key": partition_key,
        "run_id": run_id,
        "source_path": source_path,
        "discovered_by": discovered_by,
        "partition_index": partition_index,
        "fs_type": fs_type,
        "logical_path": logical_path,
        "forensic_path": forensic_path,
        "tags": tags,
        "notes": notes,
    }
    insert_row(conn, TABLE_SCHEMAS["cookies"], evidence_id, record)


def insert_cookies(conn: sqlite3.Connection, evidence_id: int, cookies: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple cookies in batch.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        cookies: Iterable of cookie records

    Returns:
        Number of cookies inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["cookies"], evidence_id, cookies)


def get_cookies(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    domain: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve cookies for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        domain: Optional domain filter
        limit: Maximum rows to return

    Returns:
        List of cookie records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if domain:
        filters["domain"] = (FilterOp.LIKE, f"%{domain}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["cookies"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_cookie_domains(conn: sqlite3.Connection, evidence_id: int) -> List[Dict[str, Any]]:
    """
    Get cookie domain statistics for an evidence.

    Returns:
        List of dicts with domain, cookie_count, browsers
    """
    rows = conn.execute(
        """
        SELECT domain, COUNT(*) as cookie_count,
               GROUP_CONCAT(DISTINCT browser) as browsers
        FROM cookies
        WHERE evidence_id = ?
        GROUP BY domain
        ORDER BY cookie_count DESC
        """,
        (evidence_id,)
    ).fetchall()

    return [dict(row) for row in rows]


def get_cookie_by_id(conn: sqlite3.Connection, cookie_id: int) -> Optional[Dict[str, Any]]:
    """
    Get a single cookie by ID, including encrypted_value blob.

    Args:
        conn: SQLite connection to evidence database
        cookie_id: Cookie row ID

    Returns:
        Cookie record as dict with encrypted_value, or None if not found
    """
    row = conn.execute(
        """
        SELECT id, evidence_id, browser, profile, name, value, domain, path,
               expires_utc, is_secure, is_httponly, samesite,
               creation_utc, last_access_utc, encrypted, encrypted_value,
               run_id, source_path, discovered_by, tags, notes
        FROM cookies WHERE id = ?
        """,
        (cookie_id,)
    ).fetchone()
    return dict(row) if row else None


def get_distinct_cookie_browsers(conn: sqlite3.Connection, evidence_id: int) -> List[str]:
    """
    Get distinct browsers that have cookies for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        Sorted list of browser names
    """
    return get_distinct_values(conn, TABLE_SCHEMAS["cookies"], evidence_id, "browser")


def delete_cookies_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """
    Delete cookies from a specific extraction run.

    Used for idempotent re-ingestion.

    Returns:
        Number of rows deleted
    """
    return delete_by_run(conn, TABLE_SCHEMAS["cookies"], evidence_id, run_id)
