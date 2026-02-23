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
        # New fields for title page customization
        ("branding_department", "TEXT"),
        ("show_title_case_number", "INTEGER NOT NULL DEFAULT 1"),
        ("show_title_evidence", "INTEGER NOT NULL DEFAULT 1"),
        ("show_title_investigator", "INTEGER NOT NULL DEFAULT 1"),
        ("show_title_date", "INTEGER NOT NULL DEFAULT 1"),
        ("show_footer_date", "INTEGER NOT NULL DEFAULT 1"),
        ("footer_evidence_label", "TEXT"),
        ("hide_appendix_page_numbers", "INTEGER NOT NULL DEFAULT 0"),
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
            branding_department,
            branding_footer_text,
            branding_logo_path,
            locale,
            date_format,
            collapsed_title,
            collapsed_author,
            collapsed_branding,
            collapsed_appendix,
            show_title_case_number,
            show_title_evidence,
            show_title_investigator,
            show_title_date,
            show_footer_date,
            footer_evidence_label,
            hide_appendix_page_numbers,
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
        "branding_department": row[5],
        "branding_footer_text": row[6],
        "branding_logo_path": row[7],
        "locale": row[8],
        "date_format": row[9],
        "collapsed_title": bool(row[10]),
        "collapsed_author": bool(row[11]),
        "collapsed_branding": bool(row[12]),
        "collapsed_appendix": bool(row[13]),
        "show_title_case_number": bool(row[14]) if row[14] is not None else True,
        "show_title_evidence": bool(row[15]) if row[15] is not None else True,
        "show_title_investigator": bool(row[16]) if row[16] is not None else True,
        "show_title_date": bool(row[17]) if row[17] is not None else True,
        "show_footer_date": bool(row[18]) if row[18] is not None else True,
        "footer_evidence_label": row[19],
        "hide_appendix_page_numbers": bool(row[20]) if row[20] is not None else False,
        "updated_at_utc": row[21],
    }


def save_report_settings(
    conn: sqlite3.Connection,
    evidence_id: int,
    title: Optional[str] = None,
    author_function: Optional[str] = None,
    author_name: Optional[str] = None,
    author_date: Optional[str] = None,
    branding_org_name: Optional[str] = None,
    branding_department: Optional[str] = None,
    branding_footer_text: Optional[str] = None,
    branding_logo_path: Optional[str] = None,
    locale: str = "en",
    date_format: str = "eu",
    collapsed_title: bool = False,
    collapsed_author: bool = True,
    collapsed_branding: bool = True,
    collapsed_appendix: bool = True,
    show_title_case_number: bool = True,
    show_title_evidence: bool = True,
    show_title_investigator: bool = True,
    show_title_date: bool = True,
    show_footer_date: bool = True,
    footer_evidence_label: Optional[str] = None,
    hide_appendix_page_numbers: bool = False,
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
        branding_department: Department name
        branding_footer_text: Footer text
        branding_logo_path: Path to logo (relative to case folder)
        locale: Report locale ("en" or "de")
        date_format: Date format ("eu" or "us")
        collapsed_title: Whether title section is collapsed
        collapsed_author: Whether author section is collapsed
        collapsed_branding: Whether branding section is collapsed
        collapsed_appendix: Whether appendix section is collapsed
        show_title_case_number: Show case number on title page
        show_title_evidence: Show evidence on title page
        show_title_investigator: Show investigator on title page
        show_title_date: Show date on title page
        show_footer_date: Show generation date in footer
        footer_evidence_label: Custom evidence label for footer
        hide_appendix_page_numbers: Hide page numbers in appendix
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
            branding_department,
            branding_footer_text,
            branding_logo_path,
            locale,
            date_format,
            collapsed_title,
            collapsed_author,
            collapsed_branding,
            collapsed_appendix,
            show_title_case_number,
            show_title_evidence,
            show_title_investigator,
            show_title_date,
            show_footer_date,
            footer_evidence_label,
            hide_appendix_page_numbers,
            updated_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence_id,
            title or None,
            author_function or None,
            author_name or None,
            author_date or None,
            branding_org_name or None,
            branding_department or None,
            branding_footer_text or None,
            branding_logo_path or None,
            locale,
            date_format,
            int(collapsed_title),
            int(collapsed_author),
            int(collapsed_branding),
            int(collapsed_appendix),
            int(show_title_case_number),
            int(show_title_evidence),
            int(show_title_investigator),
            int(show_title_date),
            int(show_footer_date),
            footer_evidence_label or None,
            int(hide_appendix_page_numbers),
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
