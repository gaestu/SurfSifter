"""
Report settings database helpers.

This module provides CRUD operations for the report_settings table.
Settings are stored per-evidence and include author info, branding, and preferences.

Initial implementation.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _ensure_settings_table(conn: sqlite3.Connection) -> None:
    """Create report_settings table if it doesn't exist and migrate if needed."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS report_settings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL UNIQUE,
            -- Report Title
            title TEXT,
            -- Report Created By
            author_function TEXT,
            author_name TEXT,
            author_date TEXT,
            -- Branding
            branding_org_name TEXT,
            branding_footer_text TEXT,
            branding_logo_path TEXT,
            -- Preferences
            locale TEXT NOT NULL DEFAULT 'en',
            date_format TEXT NOT NULL DEFAULT 'eu',
            -- UI State (collapsed sections)
            collapsed_title INTEGER NOT NULL DEFAULT 0,
            collapsed_author INTEGER NOT NULL DEFAULT 1,
            collapsed_branding INTEGER NOT NULL DEFAULT 1,
            collapsed_appendix INTEGER NOT NULL DEFAULT 1,
            -- Timestamps
            updated_at_utc TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_report_settings_evidence
        ON report_settings(evidence_id)
    """)

    # Migrate existing tables: add missing columns
    cursor = conn.execute("PRAGMA table_info(report_settings)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    migrations = [
        ("collapsed_title", "INTEGER NOT NULL DEFAULT 0"),
        ("collapsed_author", "INTEGER NOT NULL DEFAULT 1"),
        ("collapsed_branding", "INTEGER NOT NULL DEFAULT 1"),
        ("collapsed_appendix", "INTEGER NOT NULL DEFAULT 1"),
        ("title", "TEXT"),
    ]

    for col_name, col_def in migrations:
        if col_name not in existing_columns:
            conn.execute(f"ALTER TABLE report_settings ADD COLUMN {col_name} {col_def}")

    conn.commit()


def _utc_now() -> str:
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def get_report_settings(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Get report settings for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        Dictionary with settings, or None if no settings exist
    """
    _ensure_settings_table(conn)

    cursor = conn.execute(
        """
        SELECT
            title,
            author_function,
            author_name,
            author_date,
            branding_org_name,
            branding_footer_text,
            branding_logo_path,
            locale,
            date_format,
            collapsed_title,
            collapsed_author,
            collapsed_branding,
            collapsed_appendix,
            updated_at_utc
        FROM report_settings
        WHERE evidence_id = ?
        """,
        (evidence_id,)
    )
    row = cursor.fetchone()

    if row is None:
        return None

    return {
        "title": row[0],
        "author_function": row[1],
        "author_name": row[2],
        "author_date": row[3],
        "branding_org_name": row[4],
        "branding_footer_text": row[5],
        "branding_logo_path": row[6],
        "locale": row[7],
        "date_format": row[8],
        "collapsed_title": bool(row[9]),
        "collapsed_author": bool(row[10]),
        "collapsed_branding": bool(row[11]),
        "collapsed_appendix": bool(row[12]),
        "updated_at_utc": row[13],
    }


def save_report_settings(
    conn: sqlite3.Connection,
    evidence_id: int,
    title: Optional[str] = None,
    author_function: Optional[str] = None,
    author_name: Optional[str] = None,
    author_date: Optional[str] = None,
    branding_org_name: Optional[str] = None,
    branding_footer_text: Optional[str] = None,
    branding_logo_path: Optional[str] = None,
    locale: str = "en",
    date_format: str = "eu",
    collapsed_title: bool = False,
    collapsed_author: bool = True,
    collapsed_branding: bool = True,
    collapsed_appendix: bool = True,
) -> None:
    """
    Save report settings for an evidence (upsert).

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        title: Report title
        author_function: Author function/role
        author_name: Author name
        author_date: Author date (ISO format or dd.mm.yyyy)
        branding_org_name: Organization name
        branding_footer_text: Footer text
        branding_logo_path: Path to logo (relative to case folder)
        locale: Report locale ("en" or "de")
        date_format: Date format ("eu" or "us")
        collapsed_title: Whether title section is collapsed
        collapsed_author: Whether author section is collapsed
        collapsed_branding: Whether branding section is collapsed
        collapsed_appendix: Whether appendix section is collapsed
    """
    _ensure_settings_table(conn)

    now = _utc_now()

    # Use INSERT OR REPLACE for upsert behavior
    conn.execute(
        """
        INSERT OR REPLACE INTO report_settings (
            evidence_id,
            title,
            author_function,
            author_name,
            author_date,
            branding_org_name,
            branding_footer_text,
            branding_logo_path,
            locale,
            date_format,
            collapsed_title,
            collapsed_author,
            collapsed_branding,
            collapsed_appendix,
            updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence_id,
            title or None,
            author_function or None,
            author_name or None,
            author_date or None,
            branding_org_name or None,
            branding_footer_text or None,
            branding_logo_path or None,
            locale,
            date_format,
            int(collapsed_title),
            int(collapsed_author),
            int(collapsed_branding),
            int(collapsed_appendix),
            now,
        )
    )
    conn.commit()


def delete_report_settings(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> bool:
    """
    Delete report settings for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        True if settings were deleted, False if not found
    """
    _ensure_settings_table(conn)

    cursor = conn.execute(
        "DELETE FROM report_settings WHERE evidence_id = ?",
        (evidence_id,)
    )
    conn.commit()
    return cursor.rowcount > 0
