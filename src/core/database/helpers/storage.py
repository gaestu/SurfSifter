"""
Browser storage database helper functions.

This module provides CRUD operations for local_storage, session_storage,
indexeddb_databases, indexeddb_entries, storage_tokens, and storage_identifiers tables.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    # Local storage
    "insert_local_storage",
    "insert_local_storages",
    "get_local_storage",
    "get_local_storage_origins",
    "delete_local_storage_by_run",
    # Session storage
    "insert_session_storage",
    "insert_session_storages",
    "get_session_storage",
    "delete_session_storage_by_run",
    # IndexedDB databases
    "insert_indexeddb_database",
    "insert_indexeddb_databases",
    "get_indexeddb_databases",
    "delete_indexeddb_databases_by_run",
    # IndexedDB entries
    "insert_indexeddb_entry",
    "insert_indexeddb_entries",
    "get_indexeddb_entries",
    "delete_indexeddb_entries_by_run",
    # Storage tokens
    "insert_storage_token",
    "insert_storage_tokens",
    "get_storage_tokens",
    "get_storage_token_stats",
    "delete_storage_tokens_by_run",
    # Storage identifiers
    "insert_storage_identifier",
    "insert_storage_identifiers",
    "get_storage_identifiers",
    "get_storage_identifier_stats",
    "delete_storage_identifiers_by_run",
    # Aggregate
    "get_stored_sites_summary",
]


# ============================================================================
# Local Storage
# ============================================================================

def insert_local_storage(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    origin: str,
    key: str,
    **kwargs,
) -> None:
    """
    Insert a single local storage entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        origin: Storage origin
        key: Storage key
        **kwargs: Optional fields (profile, value, value_type, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "origin": origin,
        "key": key,
        "value": kwargs.get("value"),
        "value_type": kwargs.get("value_type"),
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
    insert_row(conn, TABLE_SCHEMAS["local_storage"], evidence_id, record)


def insert_local_storages(conn: sqlite3.Connection, evidence_id: int, entries: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple local storage entries in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["local_storage"], evidence_id, entries)


def get_local_storage(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    origin: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """Retrieve local storage entries for an evidence."""
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if origin:
        filters["origin"] = (FilterOp.LIKE, f"%{origin}%")
    return get_rows(conn, TABLE_SCHEMAS["local_storage"], evidence_id, filters=filters or None, limit=limit)


def delete_local_storage_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete local storage entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["local_storage"], evidence_id, run_id)


# ============================================================================
# Session Storage
# ============================================================================

def insert_session_storage(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    origin: str,
    key: str,
    **kwargs,
) -> None:
    """
    Insert a single session storage entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        origin: Storage origin
        key: Storage key
        **kwargs: Optional fields (profile, value, value_type, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "origin": origin,
        "key": key,
        "value": kwargs.get("value"),
        "value_type": kwargs.get("value_type"),
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
    insert_row(conn, TABLE_SCHEMAS["session_storage"], evidence_id, record)


def insert_session_storages(conn: sqlite3.Connection, evidence_id: int, entries: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple session storage entries in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["session_storage"], evidence_id, entries)


def get_session_storage(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    origin: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """Retrieve session storage entries for an evidence."""
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if origin:
        filters["origin"] = (FilterOp.LIKE, f"%{origin}%")
    return get_rows(conn, TABLE_SCHEMAS["session_storage"], evidence_id, filters=filters or None, limit=limit)


def delete_session_storage_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete session storage entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["session_storage"], evidence_id, run_id)


# ============================================================================
# IndexedDB Databases
# ============================================================================

def insert_indexeddb_database(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    origin: str,
    database_name: str,
    **kwargs,
) -> int:
    """
    Insert a single IndexedDB database entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        origin: Storage origin
        database_name: Database name
        **kwargs: Optional fields (profile, version, object_stores, etc.)

    Returns:
        Row ID of inserted database
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "origin": origin,
        "database_name": database_name,
        "version": kwargs.get("version"),
        "object_stores": kwargs.get("object_stores"),  # JSON array
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
    insert_row(conn, TABLE_SCHEMAS["indexeddb_databases"], evidence_id, record)
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def insert_indexeddb_databases(conn: sqlite3.Connection, evidence_id: int, databases: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple IndexedDB databases in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["indexeddb_databases"], evidence_id, databases)


def get_indexeddb_databases(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    origin: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Retrieve IndexedDB databases for an evidence."""
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if origin:
        filters["origin"] = (FilterOp.LIKE, f"%{origin}%")
    return get_rows(conn, TABLE_SCHEMAS["indexeddb_databases"], evidence_id, filters=filters or None, limit=limit)


def delete_indexeddb_databases_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete IndexedDB databases from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["indexeddb_databases"], evidence_id, run_id)


# ============================================================================
# IndexedDB Entries
# ============================================================================

def insert_indexeddb_entry(
    conn: sqlite3.Connection,
    evidence_id: int,
    database_id: int,
    object_store: str,
    key: str,
    **kwargs,
) -> None:
    """
    Insert a single IndexedDB entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        database_id: Parent database ID (FK to indexeddb_databases.id)
        object_store: Object store name
        key: Entry key
        **kwargs: Optional fields (value, value_type, etc.)
    """
    record = {
        "database_id": database_id,
        "object_store": object_store,
        "key": key,
        "value": kwargs.get("value"),
        "value_type": kwargs.get("value_type"),
        "run_id": kwargs.get("run_id"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["indexeddb_entries"], evidence_id, record)


def insert_indexeddb_entries(conn: sqlite3.Connection, evidence_id: int, entries: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple IndexedDB entries in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["indexeddb_entries"], evidence_id, entries)


def get_indexeddb_entries(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    database_id: Optional[int] = None,
    object_store: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """Retrieve IndexedDB entries for an evidence."""
    filters: Dict[str, Any] = {}
    if database_id is not None:
        filters["database_id"] = (FilterOp.EQ, database_id)
    if object_store:
        filters["object_store"] = (FilterOp.EQ, object_store)
    return get_rows(conn, TABLE_SCHEMAS["indexeddb_entries"], evidence_id, filters=filters or None, limit=limit)


def delete_indexeddb_entries_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete IndexedDB entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["indexeddb_entries"], evidence_id, run_id)


# ============================================================================
# Storage Tokens
# ============================================================================

def insert_storage_token(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    origin: str,
    token_type: str,
    token_value: str,
    **kwargs,
) -> None:
    """
    Insert a single storage token entry.

    Storage tokens include OAuth tokens, session tokens, API keys, etc.
    found in browser storage.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        origin: Storage origin
        token_type: Token type (oauth, session, api_key, etc.)
        token_value: Token value (may be redacted)
        **kwargs: Optional fields (profile, storage_type, key_name, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "origin": origin,
        "storage_type": kwargs.get("storage_type"),  # local, session, cookie, indexeddb
        "key_name": kwargs.get("key_name"),
        "token_type": token_type,
        "token_value": token_value,
        "expires_utc": kwargs.get("expires_utc"),
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
    insert_row(conn, TABLE_SCHEMAS["storage_tokens"], evidence_id, record)


def insert_storage_tokens(conn: sqlite3.Connection, evidence_id: int, tokens: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple storage tokens in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["storage_tokens"], evidence_id, tokens)


def get_storage_tokens(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    origin: Optional[str] = None,
    token_type: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Retrieve storage tokens for an evidence."""
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if origin:
        filters["origin"] = (FilterOp.LIKE, f"%{origin}%")
    if token_type:
        filters["token_type"] = (FilterOp.EQ, token_type)
    if run_id:
        filters["run_id"] = (FilterOp.EQ, run_id)
    return get_rows(conn, TABLE_SCHEMAS["storage_tokens"], evidence_id, filters=filters or None, limit=limit)


def delete_storage_tokens_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete storage tokens from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["storage_tokens"], evidence_id, run_id)


# ============================================================================
# Storage Identifiers
# ============================================================================

def insert_storage_identifier(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    origin: str,
    identifier_type: str,
    identifier_value: str,
    **kwargs,
) -> None:
    """
    Insert a single storage identifier entry.

    Storage identifiers include user IDs, device IDs, analytics IDs, etc.
    found in browser storage.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        origin: Storage origin
        identifier_type: Identifier type (user_id, device_id, analytics_id, etc.)
        identifier_value: Identifier value
        **kwargs: Optional fields (profile, storage_type, key_name, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "origin": origin,
        "storage_type": kwargs.get("storage_type"),
        "key_name": kwargs.get("key_name"),
        "identifier_type": identifier_type,
        "identifier_value": identifier_value,
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
    insert_row(conn, TABLE_SCHEMAS["storage_identifiers"], evidence_id, record)


def insert_storage_identifiers(conn: sqlite3.Connection, evidence_id: int, identifiers: Iterable[Dict[str, Any]]) -> int:
    """Insert multiple storage identifiers in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["storage_identifiers"], evidence_id, identifiers)


def get_storage_identifiers(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    origin: Optional[str] = None,
    identifier_type: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Retrieve storage identifiers for an evidence."""
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if origin:
        filters["origin"] = (FilterOp.LIKE, f"%{origin}%")
    if identifier_type:
        filters["identifier_type"] = (FilterOp.EQ, identifier_type)
    return get_rows(conn, TABLE_SCHEMAS["storage_identifiers"], evidence_id, filters=filters or None, limit=limit)


def delete_storage_identifiers_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete storage identifiers from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["storage_identifiers"], evidence_id, run_id)


# ============================================================================
# Aggregate/Stats Functions
# ============================================================================

def get_local_storage_origins(conn: sqlite3.Connection, evidence_id: int) -> List[str]:
    """Get distinct origins from local storage."""
    cur = conn.execute(
        "SELECT DISTINCT origin FROM local_storage WHERE evidence_id = ? ORDER BY origin",
        (evidence_id,),
    )
    return [row[0] for row in cur.fetchall()]


def get_storage_token_stats(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Any]:
    """Get storage token statistics.

    Returns:
        Dict with:
        - total: Total token count
        - by_type: Dict mapping token_type to count
        - unique_origins: Count of unique origins
    """
    result: Dict[str, Any] = {}

    # Total count
    cur = conn.execute(
        "SELECT COUNT(*) FROM storage_tokens WHERE evidence_id = ?",
        (evidence_id,),
    )
    result["total"] = cur.fetchone()[0]

    # By type
    cur = conn.execute(
        """
        SELECT token_type, COUNT(*) as count
        FROM storage_tokens
        WHERE evidence_id = ?
        GROUP BY token_type
        ORDER BY count DESC
        """,
        (evidence_id,),
    )
    result["by_type"] = {row[0]: row[1] for row in cur.fetchall()}

    # Unique origins
    cur = conn.execute(
        "SELECT COUNT(DISTINCT origin) FROM storage_tokens WHERE evidence_id = ?",
        (evidence_id,),
    )
    result["unique_origins"] = cur.fetchone()[0]

    return result


def get_storage_identifier_stats(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Any]:
    """Get storage identifier statistics.

    Returns:
        Dict with:
        - total: Total identifier count
        - by_type: Dict mapping identifier_type to count
        - unique_origins: Count of unique origins
    """
    result: Dict[str, Any] = {}

    # Total count
    cur = conn.execute(
        "SELECT COUNT(*) FROM storage_identifiers WHERE evidence_id = ?",
        (evidence_id,),
    )
    result["total"] = cur.fetchone()[0]

    # By type
    cur = conn.execute(
        """
        SELECT identifier_type, COUNT(*) as count
        FROM storage_identifiers
        WHERE evidence_id = ?
        GROUP BY identifier_type
        ORDER BY count DESC
        """,
        (evidence_id,),
    )
    result["by_type"] = {row[0]: row[1] for row in cur.fetchall()}

    # Unique origins
    cur = conn.execute(
        "SELECT COUNT(DISTINCT origin) FROM storage_identifiers WHERE evidence_id = ?",
        (evidence_id,),
    )
    result["unique_origins"] = cur.fetchone()[0]

    return result


def get_stored_sites_summary(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Dict[str, int]]:
    """Get summary of stored sites across all storage tables.

    Returns a dict mapping storage type to origin counts.
    """
    result: Dict[str, Dict[str, int]] = {}

    # Local storage origins
    cur = conn.execute(
        """
        SELECT origin, COUNT(*) as count
        FROM local_storage
        WHERE evidence_id = ?
        GROUP BY origin
        ORDER BY count DESC
        """,
        (evidence_id,),
    )
    result["local_storage"] = {row[0]: row[1] for row in cur.fetchall()}

    # Session storage origins
    cur = conn.execute(
        """
        SELECT origin, COUNT(*) as count
        FROM session_storage
        WHERE evidence_id = ?
        GROUP BY origin
        ORDER BY count DESC
        """,
        (evidence_id,),
    )
    result["session_storage"] = {row[0]: row[1] for row in cur.fetchall()}

    # IndexedDB origins
    cur = conn.execute(
        """
        SELECT origin, COUNT(*) as count
        FROM indexeddb_databases
        WHERE evidence_id = ?
        GROUP BY origin
        ORDER BY count DESC
        """,
        (evidence_id,),
    )
    result["indexeddb"] = {row[0]: row[1] for row in cur.fetchall()}

    return result
