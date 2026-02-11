"""
Hash matches database helper functions.

This module provides CRUD operations for the hash_matches and url_matches tables.
These tables don't have entries in TABLE_SCHEMAS so use raw SQL.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows

__all__ = [
    # Hash matches
    "insert_hash_match",
    "insert_hash_matches",
    "get_hash_matches",
    "delete_hash_matches_by_run",
    # URL matches
    "insert_url_match",
    "insert_url_matches",
    "get_url_matches",
    "delete_url_matches_by_run",
]


# ============================================================================
# Internal Helpers
# ============================================================================

def _insert_rows_with_columns(
    conn: sqlite3.Connection,
    table: str,
    columns: List[str],
    rows: List[tuple],
) -> None:
    """Insert rows with specific column set (raw SQL)."""
    if not rows:
        return
    placeholders = ", ".join("?" * len(columns))
    column_names = ", ".join(columns)
    sql = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"
    with conn:
        conn.executemany(sql, rows)


# ============================================================================
# Hash Matches
# ============================================================================

def insert_hash_match(
    conn: sqlite3.Connection,
    evidence_id: int,
    image_id: int,
    db_name: str,
    db_md5: str,
    **kwargs,
) -> None:
    """
    Insert a single hash match entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        image_id: ID of the matching image
        db_name: Hash database name
        db_md5: MD5 hash value
        **kwargs: Optional fields (matched_at_utc, list_name, list_version, note, hash_sha256, run_id)
    """
    insert_hash_matches(conn, evidence_id, [{
        "image_id": image_id,
        "db_name": db_name,
        "db_md5": db_md5,
        **kwargs,
    }])


def insert_hash_matches(
    conn: sqlite3.Connection,
    evidence_id: int,
    matches: Iterable[Dict[str, Any]],
) -> None:
    """
    Insert hash match records.

    Phase 4: Extended to support new columns (list_name, list_version, note, hash_sha256).
    Batch Operations: Added run_id support for overwrite operations.
    Backward compatible - new columns are optional.
    """
    row_dicts = []
    for match in matches:
        row_dicts.append({
            "evidence_id": evidence_id,
            "image_id": match.get("image_id"),
            "db_name": match.get("db_name"),
            "db_md5": match.get("db_md5"),
            "matched_at_utc": match.get("matched_at_utc"),
            "list_name": match.get("list_name"),
            "list_version": match.get("list_version"),
            "note": match.get("note"),
            "hash_sha256": match.get("hash_sha256"),
            "run_id": match.get("run_id"),
        })
    if not row_dicts:
        return

    column_sets = [
        [
            "evidence_id",
            "image_id",
            "db_name",
            "db_md5",
            "matched_at_utc",
            "list_name",
            "list_version",
            "note",
            "hash_sha256",
            "run_id",
        ],
        [
            "evidence_id",
            "image_id",
            "db_name",
            "db_md5",
            "matched_at_utc",
            "list_name",
            "list_version",
            "note",
            "hash_sha256",
        ],
        [
            "evidence_id",
            "image_id",
            "db_name",
            "db_md5",
            "matched_at_utc",
        ],
    ]

    last_error: Optional[sqlite3.OperationalError] = None
    for columns in column_sets:
        rows = [tuple(row.get(col) for col in columns) for row in row_dicts]
        try:
            _insert_rows_with_columns(conn, "hash_matches", columns, rows)
            return
        except sqlite3.OperationalError as exc:
            last_error = exc

    if last_error:
        raise last_error


def get_hash_matches(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    image_id: Optional[int] = None,
    db_name: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """Retrieve hash matches for an evidence."""
    conditions = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]

    if image_id is not None:
        conditions.append("image_id = ?")
        params.append(image_id)
    if db_name:
        conditions.append("db_name = ?")
        params.append(db_name)

    params.append(limit)
    query = f"""
        SELECT * FROM hash_matches
        WHERE {' AND '.join(conditions)}
        ORDER BY id
        LIMIT ?
    """
    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def delete_hash_matches_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete hash matches from a specific run."""
    cur = conn.execute(
        "DELETE FROM hash_matches WHERE evidence_id = ? AND run_id = ?",
        (evidence_id, run_id),
    )
    return cur.rowcount


# ============================================================================
# URL Matches
# ============================================================================

def insert_url_match(
    conn: sqlite3.Connection,
    evidence_id: int,
    url: str,
    list_name: str,
    **kwargs,
) -> None:
    """
    Insert a single URL match entry.

    URL matches track URLs that match known URL lists.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        url: The matching URL
        list_name: Name of the URL list
        **kwargs: Optional fields (list_version, category, run_id, etc.)
    """
    record = {
        "evidence_id": evidence_id,
        "url": url,
        "list_name": list_name,
        "list_version": kwargs.get("list_version"),
        "category": kwargs.get("category"),
        "matched_at_utc": kwargs.get("matched_at_utc"),
        "run_id": kwargs.get("run_id"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }

    columns = [k for k, v in record.items() if v is not None]
    values = [v for v in record.values() if v is not None]

    placeholders = ", ".join("?" * len(columns))
    column_names = ", ".join(columns)
    conn.execute(
        f"INSERT INTO url_matches ({column_names}) VALUES ({placeholders})",
        values,
    )


def insert_url_matches(
    conn: sqlite3.Connection,
    evidence_id: int,
    matches: Iterable[Dict[str, Any]],
    run_id: Optional[str] = None,
) -> int:
    """Insert multiple URL matches in batch."""
    count = 0
    for match in matches:
        if run_id:
            match = {**match, "run_id": run_id}
        insert_url_match(
            conn,
            evidence_id,
            url=match["url"],
            list_name=match["list_name"],
            **{k: v for k, v in match.items() if k not in ("url", "list_name")},
        )
        count += 1
    return count


def get_url_matches(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    list_name: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """Retrieve URL matches for an evidence."""
    return get_rows(
        conn,
        TABLE_SCHEMAS["url_matches"],
        evidence_id,
        filters={
            "list_name": (FilterOp.EQ, list_name) if list_name else None,
            "category": (FilterOp.EQ, category) if category else None,
        } if list_name or category else None,
        limit=limit,
    )


def delete_url_matches_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete URL matches from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["url_matches"], evidence_id, run_id)
