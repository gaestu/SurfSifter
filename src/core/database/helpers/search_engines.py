"""
Search engines database helper functions.

This module provides CRUD operations for the search_engines table,
which stores browser search engine configurations from Chromium's keywords table.

Added for autofill enhancement feature.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_distinct_values, get_rows, insert_row, insert_rows

__all__ = [
    "insert_search_engine",
    "insert_search_engines",
    "get_search_engines",
    "get_distinct_search_engine_browsers",
    "delete_search_engines_by_run",
]


def insert_search_engine(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    **kwargs,
) -> None:
    """
    Insert a single search engine entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        **kwargs: Optional fields (keyword, short_name, url, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "short_name": kwargs.get("short_name"),
        "keyword": kwargs.get("keyword"),
        "url": kwargs.get("url"),
        "favicon_url": kwargs.get("favicon_url"),
        "suggest_url": kwargs.get("suggest_url"),
        "prepopulate_id": kwargs.get("prepopulate_id"),
        "usage_count": kwargs.get("usage_count", 0),
        "date_created_utc": kwargs.get("date_created_utc"),
        "last_modified_utc": kwargs.get("last_modified_utc"),
        "last_visited_utc": kwargs.get("last_visited_utc"),
        "is_default": kwargs.get("is_default", 0),
        "is_active": kwargs.get("is_active", 1),
        "new_tab_url": kwargs.get("new_tab_url"),
        "image_url": kwargs.get("image_url"),
        "search_url_post_params": kwargs.get("search_url_post_params"),
        "suggest_url_post_params": kwargs.get("suggest_url_post_params"),
        "token_mappings": kwargs.get("token_mappings"),
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
    insert_row(conn, TABLE_SCHEMAS["search_engines"], evidence_id, record)


def insert_search_engines(
    conn: sqlite3.Connection,
    evidence_id: int,
    engines: Iterable[Dict[str, Any]],
) -> int:
    """
    Insert multiple search engines in batch.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        engines: Iterable of search engine records

    Returns:
        Number of records inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["search_engines"], evidence_id, engines)


def get_search_engines(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    keyword: Optional[str] = None,
    is_default: Optional[bool] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve search engines for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        keyword: Optional keyword filter (partial match)
        is_default: Optional filter for default search engines
        limit: Maximum rows to return

    Returns:
        List of search engine records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if keyword:
        filters["keyword"] = (FilterOp.LIKE, f"%{keyword}%")
    if is_default is not None:
        filters["is_default"] = (FilterOp.EQ, 1 if is_default else 0)

    return get_rows(
        conn,
        TABLE_SCHEMAS["search_engines"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_distinct_search_engine_browsers(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[str]:
    """Get distinct browser names from search_engines table."""
    return get_distinct_values(
        conn,
        TABLE_SCHEMAS["search_engines"],
        evidence_id,
        "browser",
    )


def delete_search_engines_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete search engines from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["search_engines"], evidence_id, run_id)
