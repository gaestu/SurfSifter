"""
Browser inventory database helper functions.

This module provides CRUD operations for browser inventory detection.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

__all__ = [
    "insert_browser_inventory",
    "update_inventory_ingestion_status",
    "get_browser_inventory",
]


def insert_browser_inventory(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    artifact_type: str,
    run_id: str,
    extracted_path: str,
    extraction_status: str,
    extraction_timestamp_utc: str,
    logical_path: str,
    **kwargs,
) -> int:
    """
    Insert row into browser_cache_inventory table.

    Tracks discovered browser artifacts with extraction/ingestion status.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name ('chrome', 'firefox', etc.)
        artifact_type: Artifact type ('history', 'cache_simple', 'cache_firefox')
        run_id: Extraction run ID (UUID4 with timestamp prefix)
        extracted_path: Relative path in extracted_artifacts/
        extraction_status: Extraction status ('ok', 'partial', 'error', 'skipped')
        extraction_timestamp_utc: ISO 8601 extraction timestamp
        logical_path: Windows path (C:\\Users\\...)
        **kwargs: Optional fields (profile, partition_index, fs_type, etc.)

    Returns:
        Inventory row ID (for updating ingestion status later)
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO browser_cache_inventory (
            evidence_id,
            browser,
            artifact_type,
            profile,
            partition_index,
            fs_type,
            logical_path,
            forensic_path,
            run_id,
            extracted_path,
            extraction_status,
            extraction_timestamp_utc,
            extraction_tool,
            extraction_notes,
            file_size_bytes,
            file_md5,
            file_sha256,
            ingestion_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
        """,
        (
            evidence_id,
            browser,
            artifact_type,
            kwargs.get("profile"),
            kwargs.get("partition_index"),
            kwargs.get("fs_type"),
            logical_path,
            kwargs.get("forensic_path"),
            run_id,
            extracted_path,
            extraction_status,
            extraction_timestamp_utc,
            kwargs.get("extraction_tool"),
            kwargs.get("extraction_notes"),
            kwargs.get("file_size_bytes"),
            kwargs.get("file_md5"),
            kwargs.get("file_sha256"),
        ),
    )
    conn.commit()  # Persist inventory record immediately
    return cursor.lastrowid


def update_inventory_ingestion_status(
    conn: sqlite3.Connection,
    inventory_id: int,
    status: str,
    urls_parsed: int = 0,
    records_parsed: int = 0,
    notes: Optional[str] = None,
) -> None:
    """
    Update ingestion status in browser_cache_inventory.

    Called after parsing manifest and ingesting artifacts into database.

    Args:
        conn: SQLite connection to evidence database
        inventory_id: Row ID from insert_browser_inventory()
        status: Ingestion status ('ok', 'failed', 'skipped')
        urls_parsed: Count of URLs extracted
        records_parsed: Total records (history rows, cache entries)
        notes: Ingestion warnings/errors
    """
    conn.execute(
        """
        UPDATE browser_cache_inventory
        SET ingestion_status = ?,
            ingestion_timestamp_utc = datetime('now'),
            urls_parsed = ?,
            records_parsed = ?,
            ingestion_notes = ?,
            updated_at_utc = datetime('now')
        WHERE id = ?
        """,
        (status, urls_parsed, records_parsed, notes, inventory_id),
    )
    conn.commit()  # Persist ingestion status update immediately


def get_browser_inventory(conn: sqlite3.Connection, evidence_id: int) -> List[Dict[str, Any]]:
    """
    Get browser inventory for an evidence.

    Aggregates browser presence information from multiple artifact tables.

    Returns:
        List of dicts with browser name, profile count, and artifact counts
    """
    # Get distinct browsers from browser_history
    browsers = set()

    # From browser_history
    for row in conn.execute(
        "SELECT DISTINCT browser FROM browser_history WHERE evidence_id = ?",
        (evidence_id,)
    ):
        if row[0]:
            browsers.add(row[0])

    # From cookies
    for row in conn.execute(
        "SELECT DISTINCT browser FROM cookies WHERE evidence_id = ?",
        (evidence_id,)
    ):
        if row[0]:
            browsers.add(row[0])

    # From bookmarks
    for row in conn.execute(
        "SELECT DISTINCT browser FROM bookmarks WHERE evidence_id = ?",
        (evidence_id,)
    ):
        if row[0]:
            browsers.add(row[0])

    # From browser_downloads
    for row in conn.execute(
        "SELECT DISTINCT browser FROM browser_downloads WHERE evidence_id = ?",
        (evidence_id,)
    ):
        if row[0]:
            browsers.add(row[0])

    # Build inventory
    inventory = []
    for browser in sorted(browsers):
        # Count profiles
        profiles = set()
        for row in conn.execute(
            "SELECT DISTINCT profile FROM browser_history WHERE evidence_id = ? AND browser = ?",
            (evidence_id, browser)
        ):
            if row[0]:
                profiles.add(row[0])

        # Count artifacts
        history_count = conn.execute(
            "SELECT COUNT(*) FROM browser_history WHERE evidence_id = ? AND browser = ?",
            (evidence_id, browser)
        ).fetchone()[0]

        cookie_count = conn.execute(
            "SELECT COUNT(*) FROM cookies WHERE evidence_id = ? AND browser = ?",
            (evidence_id, browser)
        ).fetchone()[0]

        bookmark_count = conn.execute(
            "SELECT COUNT(*) FROM bookmarks WHERE evidence_id = ? AND browser = ?",
            (evidence_id, browser)
        ).fetchone()[0]

        download_count = conn.execute(
            "SELECT COUNT(*) FROM browser_downloads WHERE evidence_id = ? AND browser = ?",
            (evidence_id, browser)
        ).fetchone()[0]

        inventory.append({
            "browser": browser,
            "profile_count": len(profiles),
            "profiles": sorted(profiles),
            "history_count": history_count,
            "cookie_count": cookie_count,
            "bookmark_count": bookmark_count,
            "download_count": download_count,
        })

    return inventory
