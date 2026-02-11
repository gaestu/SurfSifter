"""
Browser configuration database helper functions.

This module provides CRUD operations for browser_config and tor_state tables.
Used for storing parsed configuration from:
- Tor Browser: torrc, state files
- Firefox: prefs.js, user.js (future)
- Chromium: Preferences JSON keys (future)

Initial implementation for Tor state extractor.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

__all__ = [
    # Browser config
    "insert_browser_config",
    "insert_browser_configs",
    "get_browser_configs",
    "get_browser_config_keys",
    "delete_browser_config_by_run",
    # Tor state
    "insert_tor_state",
    "insert_tor_states",
    "get_tor_states",
    "delete_tor_state_by_run",
]


# ============================================================================
# Browser Config
# ============================================================================

def insert_browser_config(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    config_type: str,
    config_key: str,
    config_value: Optional[str],
    source_path: str,
    *,
    run_id: str,
    profile: Optional[str] = None,
    value_count: int = 1,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    notes: Optional[str] = None,
) -> int:
    """
    Insert a single browser config entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser identifier (tor, firefox, chrome, etc.)
        config_type: Type of config file (torrc, prefs_js, etc.)
        config_key: Configuration key/directive name
        config_value: Configuration value
        source_path: Full path to config file on evidence
        run_id: Extraction run ID
        profile: Browser profile name
        value_count: Number of times key appears (for multi-value)
        partition_index: Partition index
        fs_type: Filesystem type
        logical_path: Logical path
        forensic_path: Forensic path
        notes: Additional notes

    Returns:
        Inserted row ID
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO browser_config (
            evidence_id, run_id, browser, profile, config_type,
            config_key, config_value, value_count,
            source_path, partition_index, fs_type, logical_path, forensic_path,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence_id, run_id, browser, profile, config_type,
            config_key, config_value, value_count,
            source_path, partition_index, fs_type, logical_path, forensic_path,
            notes,
        ),
    )
    return cursor.lastrowid


def insert_browser_configs(
    conn: sqlite3.Connection,
    evidence_id: int,
    entries: Iterable[Dict[str, Any]],
) -> int:
    """
    Insert multiple browser config entries in batch.

    Args:
        conn: SQLite connection
        evidence_id: Evidence ID
        entries: Iterable of config entry dicts

    Returns:
        Number of rows inserted
    """
    cursor = conn.cursor()
    count = 0

    for entry in entries:
        cursor.execute(
            """
            INSERT INTO browser_config (
                evidence_id, run_id, browser, profile, config_type,
                config_key, config_value, value_count,
                source_path, partition_index, fs_type, logical_path, forensic_path,
                notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evidence_id,
                entry.get("run_id"),
                entry.get("browser"),
                entry.get("profile"),
                entry.get("config_type"),
                entry.get("config_key"),
                entry.get("config_value"),
                entry.get("value_count", 1),
                entry.get("source_path"),
                entry.get("partition_index"),
                entry.get("fs_type"),
                entry.get("logical_path"),
                entry.get("forensic_path"),
                entry.get("notes"),
            ),
        )
        count += 1

    return count


def get_browser_configs(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    config_type: Optional[str] = None,
    config_key: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve browser config entries.

    Args:
        conn: SQLite connection
        evidence_id: Evidence ID
        browser: Filter by browser
        config_type: Filter by config type
        config_key: Filter by config key
        run_id: Filter by run ID
        limit: Maximum results

    Returns:
        List of config entries
    """
    query = "SELECT * FROM browser_config WHERE evidence_id = ?"
    params: List[Any] = [evidence_id]

    if browser:
        query += " AND browser = ?"
        params.append(browser)
    if config_type:
        query += " AND config_type = ?"
        params.append(config_type)
    if config_key:
        query += " AND config_key = ?"
        params.append(config_key)
    if run_id:
        query += " AND run_id = ?"
        params.append(run_id)

    query += " ORDER BY id LIMIT ?"
    params.append(limit)

    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute(query, params)

    return [dict(row) for row in cursor.fetchall()]


def get_browser_config_keys(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    config_type: Optional[str] = None,
) -> List[str]:
    """
    Get distinct config keys for an evidence.

    Returns:
        List of unique config key names
    """
    query = "SELECT DISTINCT config_key FROM browser_config WHERE evidence_id = ?"
    params: List[Any] = [evidence_id]

    if browser:
        query += " AND browser = ?"
        params.append(browser)
    if config_type:
        query += " AND config_type = ?"
        params.append(config_type)

    query += " ORDER BY config_key"

    cursor = conn.cursor()
    cursor.execute(query, params)

    return [row[0] for row in cursor.fetchall()]


def delete_browser_config_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """
    Delete browser config entries for a specific run.

    Returns:
        Number of rows deleted
    """
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM browser_config WHERE evidence_id = ? AND run_id = ?",
        (evidence_id, run_id),
    )
    return cursor.rowcount


# ============================================================================
# Tor State
# ============================================================================

def insert_tor_state(
    conn: sqlite3.Connection,
    evidence_id: int,
    state_key: str,
    state_value: Optional[str],
    source_path: str,
    *,
    run_id: str,
    profile: Optional[str] = None,
    timestamp_utc: Optional[str] = None,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    notes: Optional[str] = None,
) -> int:
    """
    Insert a single Tor state entry.

    Args:
        conn: SQLite connection
        evidence_id: Evidence ID
        state_key: State key name (TorVersion, LastWritten, Guard, etc.)
        state_value: State value
        source_path: Path to state file
        run_id: Extraction run ID
        profile: Profile name
        timestamp_utc: ISO 8601 timestamp if value is temporal
        partition_index: Partition index
        fs_type: Filesystem type
        logical_path: Logical path
        forensic_path: Forensic path
        notes: Additional notes

    Returns:
        Inserted row ID
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO tor_state (
            evidence_id, run_id, profile, state_key, state_value, timestamp_utc,
            source_path, partition_index, fs_type, logical_path, forensic_path, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence_id, run_id, profile, state_key, state_value, timestamp_utc,
            source_path, partition_index, fs_type, logical_path, forensic_path, notes,
        ),
    )
    return cursor.lastrowid


def insert_tor_states(
    conn: sqlite3.Connection,
    evidence_id: int,
    entries: Iterable[Dict[str, Any]],
) -> int:
    """
    Insert multiple Tor state entries in batch.

    Returns:
        Number of rows inserted
    """
    cursor = conn.cursor()
    count = 0

    for entry in entries:
        cursor.execute(
            """
            INSERT INTO tor_state (
                evidence_id, run_id, profile, state_key, state_value, timestamp_utc,
                source_path, partition_index, fs_type, logical_path, forensic_path, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evidence_id,
                entry.get("run_id"),
                entry.get("profile"),
                entry.get("state_key"),
                entry.get("state_value"),
                entry.get("timestamp_utc"),
                entry.get("source_path"),
                entry.get("partition_index"),
                entry.get("fs_type"),
                entry.get("logical_path"),
                entry.get("forensic_path"),
                entry.get("notes"),
            ),
        )
        count += 1

    return count


def get_tor_states(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    state_key: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve Tor state entries.

    Returns:
        List of state entries
    """
    query = "SELECT * FROM tor_state WHERE evidence_id = ?"
    params: List[Any] = [evidence_id]

    if state_key:
        query += " AND state_key = ?"
        params.append(state_key)
    if run_id:
        query += " AND run_id = ?"
        params.append(run_id)

    query += " ORDER BY id LIMIT ?"
    params.append(limit)

    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row
    cursor.execute(query, params)

    return [dict(row) for row in cursor.fetchall()]


def delete_tor_state_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """
    Delete Tor state entries for a specific run.

    Returns:
        Number of rows deleted
    """
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM tor_state WHERE evidence_id = ? AND run_id = ?",
        (evidence_id, run_id),
    )
    return cursor.rowcount
