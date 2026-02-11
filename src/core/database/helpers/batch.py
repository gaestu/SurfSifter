"""
Batch operations database helper functions.

This module provides functions for batch data operations like purging
and getting table counts.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List

__all__ = [
    "get_evidence_table_counts",
    "purge_evidence_data",
]


# All evidence tables that can be purged (in FK-safe deletion order)
PURGEABLE_TABLES = [
    # Tables with FKs must be deleted first
    "session_tab_history",
    "session_form_data",
    "favicon_mappings",
    "indexeddb_entries",
    "image_discoveries",
    # Tables with no FK dependencies
    "browser_history",
    "cookies",
    "bookmarks",
    "browser_downloads",
    "images",
    "urls",
    "url_groups",
    "timeline",
    "process_log",
    "bitcoin_addresses",
    "ethereum_addresses",
    "emails",
    "domains",
    "ip_addresses",
    "telephone_numbers",
    "autofill",
    "autofill_profiles",
    "credentials",
    "credit_cards",
    "session_windows",
    "session_tabs",
    "closed_tabs",
    "site_permissions",
    "media_playback",
    "media_sessions",
    "hsts_entries",
    "jump_list_entries",
    "browser_extensions",
    "local_storage",
    "session_storage",
    "indexeddb_databases",
    "storage_tokens",
    "storage_identifiers",
    "sync_data",
    "synced_devices",
    "favicons",
    "top_sites",
    "hash_matches",
    "os_indicators",
    "platform_detections",
    "file_list",
    "file_list_tags",
    "file_list_matches",
    "tags",
    "tag_associations",
    "url_matches",
    "extracted_files",
]


def get_evidence_table_counts(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, int]:
    """
    Get row counts for all evidence tables.

    Used by batch operations dialogs to show data preview.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        Dict mapping table name to row count
    """
    counts = {}
    for table in PURGEABLE_TABLES:
        try:
            row = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE evidence_id = ?",
                (evidence_id,)
            ).fetchone()
            counts[table] = row[0] if row else 0
        except sqlite3.OperationalError:
            # Table may not exist
            counts[table] = 0

    return counts


def purge_evidence_data(
    conn: sqlite3.Connection,
    evidence_id: int,
    tables: List[str] | None = None,
) -> int:
    """
    Purge data from evidence tables.

    Performs FK-safe deletion by deleting child tables first.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        tables: Optional list of specific tables to purge (default: all)

    Returns:
        Total number of rows deleted across all tables
    """
    if tables is None:
        tables = PURGEABLE_TABLES

    # Filter to only valid purgeable tables and maintain FK-safe order
    ordered_tables = [t for t in PURGEABLE_TABLES if t in tables]

    total_deleted = 0
    for table in ordered_tables:
        try:
            cursor = conn.execute(
                f"DELETE FROM {table} WHERE evidence_id = ?",
                (evidence_id,)
            )
            total_deleted += cursor.rowcount
        except sqlite3.OperationalError:
            # Table may not exist
            pass

    conn.commit()
    return total_deleted
