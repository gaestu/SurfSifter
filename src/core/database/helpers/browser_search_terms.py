"""
Browser search terms database helper functions.

This module provides CRUD operations for the browser_search_terms table,
which stores search queries extracted from browser history databases.

Initial implementation for Chromium keyword_search_terms extraction.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, OrderColumn, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    "insert_search_term",
    "insert_search_terms",
    "get_search_terms",
    "get_search_term_by_id",
    "get_search_terms_stats",
    "delete_search_terms_by_run",
]


# Define table schema for browser_search_terms
# This mirrors the migration but allows generic helpers to work
SEARCH_TERMS_SCHEMA = {
    "name": "browser_search_terms",
    "columns": [
        "evidence_id",
        "term",
        "normalized_term",
        "url",
        "browser",
        "profile",
        "search_engine",
        "search_time_utc",
        "source_path",
        "discovered_by",
        "run_id",
        "partition_index",
        "logical_path",
        "forensic_path",
        "chromium_keyword_id",
        "chromium_url_id",
        "tags",
        "notes",
    ],
}


def insert_search_term(
    conn: sqlite3.Connection,
    evidence_id: int,
    term: str,
    *,
    normalized_term: Optional[str] = None,
    url: Optional[str] = None,
    browser: Optional[str] = None,
    profile: Optional[str] = None,
    search_engine: Optional[str] = None,
    search_time_utc: Optional[str] = None,
    source_path: Optional[str] = None,
    discovered_by: Optional[str] = None,
    run_id: Optional[str] = None,
    partition_index: Optional[int] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    chromium_keyword_id: Optional[int] = None,
    chromium_url_id: Optional[int] = None,
    tags: Optional[str] = None,
    notes: Optional[str] = None,
) -> int:
    """
    Insert a single browser search term record.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        term: The search term as typed by user
        normalized_term: Lowercase/normalized version
        url: URL that contained the search
        browser: Browser name (chrome, edge, brave, opera)
        profile: Browser profile name
        search_engine: Detected search engine
        search_time_utc: ISO 8601 timestamp
        source_path: Path in evidence image
        discovered_by: Extractor signature
        run_id: Extraction run ID
        partition_index: E01 partition number
        logical_path: Windows-style path
        forensic_path: Canonical E01 identifier
        chromium_keyword_id: Original keyword_id from Chromium
        chromium_url_id: Original url_id from Chromium
        tags: JSON-serialized tags
        notes: Investigator notes

    Returns:
        ID of inserted row
    """
    cursor = conn.execute(
        """
        INSERT INTO browser_search_terms (
            evidence_id, term, normalized_term, url, browser, profile,
            search_engine, search_time_utc, source_path, discovered_by,
            run_id, partition_index, logical_path, forensic_path,
            chromium_keyword_id, chromium_url_id, tags, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence_id, term, normalized_term, url, browser, profile,
            search_engine, search_time_utc, source_path, discovered_by,
            run_id, partition_index, logical_path, forensic_path,
            chromium_keyword_id, chromium_url_id, tags, notes,
        ),
    )
    return cursor.lastrowid


def insert_search_terms(
    conn: sqlite3.Connection,
    evidence_id: int,
    records: Iterable[Dict[str, Any]],
) -> int:
    """
    Batch insert browser search term records.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        records: Iterable of dicts with search term data

    Returns:
        Number of rows inserted
    """
    columns = [
        "evidence_id", "term", "normalized_term", "url", "browser", "profile",
        "search_engine", "search_time_utc", "source_path", "discovered_by",
        "run_id", "partition_index", "logical_path", "forensic_path",
        "chromium_keyword_id", "chromium_url_id", "tags", "notes",
    ]

    placeholders = ", ".join(["?"] * len(columns))
    column_names = ", ".join(columns)

    sql = f"INSERT INTO browser_search_terms ({column_names}) VALUES ({placeholders})"

    count = 0
    for record in records:
        values = [evidence_id] + [record.get(col) for col in columns[1:]]
        conn.execute(sql, values)
        count += 1

    return count


def get_search_terms(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    term_filter: Optional[str] = None,
    browser: Optional[str] = None,
    search_engine: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """
    Retrieve browser search terms for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        term_filter: Optional term substring filter (LIKE)
        browser: Optional browser filter (exact match)
        search_engine: Optional search engine filter (exact match)
        run_id: Optional run_id filter (exact match)
        limit: Maximum rows to return

    Returns:
        List of search term records as dicts, ordered by search_time_utc DESC
    """
    conditions = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]

    if term_filter:
        conditions.append("(term LIKE ? OR normalized_term LIKE ?)")
        params.extend([f"%{term_filter}%", f"%{term_filter}%"])

    if browser:
        conditions.append("browser = ?")
        params.append(browser)

    if search_engine:
        conditions.append("search_engine = ?")
        params.append(search_engine)

    if run_id:
        conditions.append("run_id = ?")
        params.append(run_id)

    where_clause = " AND ".join(conditions)

    rows = conn.execute(
        f"""
        SELECT * FROM browser_search_terms
        WHERE {where_clause}
        ORDER BY search_time_utc DESC
        LIMIT ?
        """,
        params + [limit],
    ).fetchall()

    return [dict(row) for row in rows]


def get_search_term_by_id(
    conn: sqlite3.Connection,
    search_term_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Get a single search term record by ID.

    Args:
        conn: SQLite connection to evidence database
        search_term_id: Search term row ID

    Returns:
        Search term record as dict, or None if not found
    """
    row = conn.execute(
        "SELECT * FROM browser_search_terms WHERE id = ?",
        (search_term_id,),
    ).fetchone()
    return dict(row) if row else None


def get_search_terms_stats(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Any]:
    """
    Get search terms statistics for an evidence.

    Returns:
        Dict with total_count, unique_terms, by_browser, by_search_engine
    """
    total = conn.execute(
        "SELECT COUNT(*) FROM browser_search_terms WHERE evidence_id = ?",
        (evidence_id,),
    ).fetchone()[0]

    unique_terms = conn.execute(
        "SELECT COUNT(DISTINCT normalized_term) FROM browser_search_terms WHERE evidence_id = ?",
        (evidence_id,),
    ).fetchone()[0]

    by_browser = {}
    for row in conn.execute(
        """
        SELECT browser, COUNT(*) as count
        FROM browser_search_terms
        WHERE evidence_id = ?
        GROUP BY browser
        """,
        (evidence_id,),
    ):
        by_browser[row["browser"] or "unknown"] = row["count"]

    by_search_engine = {}
    for row in conn.execute(
        """
        SELECT search_engine, COUNT(*) as count
        FROM browser_search_terms
        WHERE evidence_id = ?
        GROUP BY search_engine
        """,
        (evidence_id,),
    ):
        by_search_engine[row["search_engine"] or "unknown"] = row["count"]

    return {
        "total_count": total,
        "unique_terms": unique_terms,
        "by_browser": by_browser,
        "by_search_engine": by_search_engine,
    }


def delete_search_terms_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """
    Delete search terms for a specific extraction run.

    Used for idempotent re-ingestion.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        run_id: Extraction run ID to delete

    Returns:
        Number of rows deleted
    """
    cursor = conn.execute(
        "DELETE FROM browser_search_terms WHERE evidence_id = ? AND run_id = ?",
        (evidence_id, run_id),
    )
    return cursor.rowcount
