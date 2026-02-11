"""
Session database helper functions.

This module provides CRUD operations for session_windows, session_tabs,
session_tab_history, closed_tabs, and session_form_data tables.

Extracted from db.py during database refactor.
Added session_form_data helpers.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    # Session windows
    "insert_session_window",
    "insert_session_windows",
    "get_session_windows",
    "get_session_window_by_id",
    "delete_session_windows_by_run",
    # Session tabs
    "insert_session_tab",
    "insert_session_tabs",
    "get_session_tabs",
    "delete_session_tabs_by_run",
    # Session tab history
    "insert_session_tab_history",
    "insert_session_tab_histories",
    "get_session_tab_history",
    "delete_session_tab_history_by_run",
    # Closed tabs
    "insert_closed_tab",
    "insert_closed_tabs",
    "get_closed_tabs",
    "delete_closed_tabs_by_run",
    # Session form data
    "insert_session_form_data",
    "insert_session_form_datas",
    "get_session_form_data",
    "delete_session_form_data_by_run",
    # Combined delete
    "delete_sessions_by_run",
]


# ============================================================================
# Session Windows
# ============================================================================

def insert_session_window(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    **kwargs,
) -> int:
    """
    Insert a single session window.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        **kwargs: Optional fields (profile, window_id, state, bounds_*, etc.)

    Returns:
        Row ID of inserted window
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "window_id": kwargs.get("window_id"),
        "state": kwargs.get("state"),
        "bounds_x": kwargs.get("bounds_x"),
        "bounds_y": kwargs.get("bounds_y"),
        "bounds_width": kwargs.get("bounds_width"),
        "bounds_height": kwargs.get("bounds_height"),
        "selected_tab_index": kwargs.get("selected_tab_index"),
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
    insert_row(conn, TABLE_SCHEMAS["session_windows"], evidence_id, record)

    # Return the inserted ID
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def insert_session_windows(conn: sqlite3.Connection, evidence_id: int, windows: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple session windows in batch.

    Returns:
        Number of windows inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["session_windows"], evidence_id, windows)


def get_session_windows(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    window_id: Optional[int] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve session windows for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        window_id: Optional window_id filter (the browser's internal window ID)
        limit: Maximum rows to return

    Returns:
        List of session window records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if window_id is not None:
        filters["window_id"] = (FilterOp.EQ, window_id)

    return get_rows(
        conn,
        TABLE_SCHEMAS["session_windows"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_session_window_by_id(
    conn: sqlite3.Connection,
    evidence_id: int,
    window_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Retrieve a single session window by its browser window_id.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        window_id: Browser's internal window ID

    Returns:
        Session window record as dict, or None if not found
    """
    results = get_session_windows(conn, evidence_id, window_id=window_id, limit=1)
    return results[0] if results else None


def delete_session_windows_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete session windows from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["session_windows"], evidence_id, run_id)


# ============================================================================
# Session Tabs
# ============================================================================

def insert_session_tab(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    url: str,
    **kwargs,
) -> int:
    """
    Insert a single session tab.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        url: Tab URL
        **kwargs: Optional fields (profile, window_id, tab_index, title, etc.)

    Returns:
        Row ID of inserted tab
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "window_id": kwargs.get("window_id"),
        "tab_index": kwargs.get("tab_index"),
        "url": url,
        "title": kwargs.get("title"),
        "pinned": kwargs.get("pinned"),
        "last_accessed_utc": kwargs.get("last_accessed_utc"),
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
    insert_row(conn, TABLE_SCHEMAS["session_tabs"], evidence_id, record)

    # Return the inserted ID
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def insert_session_tabs(conn: sqlite3.Connection, evidence_id: int, tabs: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple session tabs in batch.

    Returns:
        Number of tabs inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["session_tabs"], evidence_id, tabs)


def get_session_tabs(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    window_id: Optional[int] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """
    Retrieve session tabs for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        window_id: Optional window ID filter
        limit: Maximum rows to return

    Returns:
        List of session tab records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if window_id is not None:
        filters["window_id"] = (FilterOp.EQ, window_id)

    return get_rows(
        conn,
        TABLE_SCHEMAS["session_tabs"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_session_tabs_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete session tabs from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["session_tabs"], evidence_id, run_id)


# ============================================================================
# Session Tab History
# ============================================================================

def insert_session_tab_history(
    conn: sqlite3.Connection,
    evidence_id: int,
    tab_id: int,
    url: str,
    **kwargs,
) -> None:
    """
    Insert a single session tab history entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        tab_id: Parent tab ID (FK to session_tabs.id)
        url: Navigation URL
        **kwargs: Optional fields (title, index, timestamp_utc, etc.)
    """
    record = {
        "tab_id": tab_id,
        "index": kwargs.get("index"),
        "url": url,
        "title": kwargs.get("title"),
        "timestamp_utc": kwargs.get("timestamp_utc"),
        "run_id": kwargs.get("run_id"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["session_tab_history"], evidence_id, record)


def insert_session_tab_histories(conn: sqlite3.Connection, evidence_id: int, entries: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple session tab history entries in batch.

    Returns:
        Number of entries inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["session_tab_history"], evidence_id, entries)


def get_session_tab_history(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    tab_id: Optional[int] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """
    Retrieve session tab history for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        tab_id: Optional tab ID filter
        limit: Maximum rows to return

    Returns:
        List of session tab history records as dicts
    """
    filters: Dict[str, Any] = {}
    if tab_id is not None:
        filters["tab_id"] = (FilterOp.EQ, tab_id)

    return get_rows(
        conn,
        TABLE_SCHEMAS["session_tab_history"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_session_tab_history_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete session tab history from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["session_tab_history"], evidence_id, run_id)


# ============================================================================
# Closed Tabs
# ============================================================================

def insert_closed_tab(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    url: str,
    **kwargs,
) -> None:
    """
    Insert a single closed tab entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        url: Tab URL
        **kwargs: Optional fields (profile, title, closed_time_utc, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "url": url,
        "title": kwargs.get("title"),
        "closed_time_utc": kwargs.get("closed_time_utc"),
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
    insert_row(conn, TABLE_SCHEMAS["closed_tabs"], evidence_id, record)


def insert_closed_tabs(conn: sqlite3.Connection, evidence_id: int, tabs: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple closed tabs in batch.

    Returns:
        Number of tabs inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["closed_tabs"], evidence_id, tabs)


def get_closed_tabs(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve closed tabs for an evidence.

    Returns:
        List of closed tab records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)

    return get_rows(
        conn,
        TABLE_SCHEMAS["closed_tabs"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_closed_tabs_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete closed tabs from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["closed_tabs"], evidence_id, run_id)


# ============================================================================
# Session Form Data
# ============================================================================

def insert_session_form_data(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    field_name: str,
    **kwargs,
) -> None:
    """
    Insert a single session form data entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        field_name: Form field name/id
        **kwargs: Optional fields (url, field_value, field_type, xpath, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "url": kwargs.get("url"),
        "field_name": field_name,
        "field_value": kwargs.get("field_value"),
        "field_type": kwargs.get("field_type"),
        "xpath": kwargs.get("xpath"),
        "window_id": kwargs.get("window_id"),
        "tab_id": kwargs.get("tab_id"),
        "nav_index": kwargs.get("nav_index"),
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
    insert_row(conn, TABLE_SCHEMAS["session_form_data"], evidence_id, record)


def insert_session_form_datas(conn: sqlite3.Connection, evidence_id: int, entries: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple session form data entries in batch.

    Returns:
        Number of entries inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["session_form_data"], evidence_id, entries)


def get_session_form_data(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    url: Optional[str] = None,
    field_name: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve session form data entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        url: Optional URL filter
        field_name: Optional field name filter
        limit: Maximum rows to return

    Returns:
        List of session form data records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if url:
        filters["url"] = (FilterOp.EQ, url)
    if field_name:
        filters["field_name"] = (FilterOp.EQ, field_name)

    return get_rows(
        conn,
        TABLE_SCHEMAS["session_form_data"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_session_form_data_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete session form data from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["session_form_data"], evidence_id, run_id)


def delete_sessions_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete session data from a specific extraction run (all session tables)."""
    total = 0
    for table in ("session_windows", "session_tabs", "session_tab_history", "closed_tabs", "session_form_data"):
        total += delete_by_run(conn, TABLE_SCHEMAS[table], evidence_id, run_id)
    return total