"""
Custom report sections database helpers.

This module provides CRUD operations for the custom_report_sections table.
Sections are stored per-evidence and support ordering via sort_order field.

Initial implementation.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _ensure_table(conn: sqlite3.Connection) -> None:
    """Create custom_report_sections table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS custom_report_sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            content TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_custom_sections_evidence
        ON custom_report_sections(evidence_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_custom_sections_order
        ON custom_report_sections(evidence_id, sort_order)
    """)
    conn.commit()


def _utc_now() -> str:
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def insert_custom_section(
    conn: sqlite3.Connection,
    evidence_id: int,
    title: str,
    content: Optional[str] = None,
    sort_order: Optional[int] = None,
) -> int:
    """
    Insert a new custom report section.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        title: Section title (required)
        content: Section content (optional, supports basic HTML formatting)
        sort_order: Position in section list (auto-assigned if None)

    Returns:
        ID of the newly inserted section
    """
    _ensure_table(conn)

    # Auto-assign sort_order if not provided
    if sort_order is None:
        cursor = conn.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM custom_report_sections WHERE evidence_id = ?",
            (evidence_id,)
        )
        sort_order = cursor.fetchone()[0]

    now = _utc_now()
    cursor = conn.execute(
        """
        INSERT INTO custom_report_sections
        (evidence_id, title, content, sort_order, created_at_utc, updated_at_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (evidence_id, title, content, sort_order, now, now)
    )
    conn.commit()
    return cursor.lastrowid


def update_custom_section(
    conn: sqlite3.Connection,
    section_id: int,
    *,
    title: Optional[str] = None,
    content: Optional[str] = None,
) -> bool:
    """
    Update an existing custom report section.

    Args:
        conn: SQLite connection to evidence database
        section_id: Section ID to update
        title: New title (if provided)
        content: New content (if provided)

    Returns:
        True if section was updated, False if not found
    """
    _ensure_table(conn)

    updates = []
    params: List[Any] = []

    if title is not None:
        updates.append("title = ?")
        params.append(title)

    if content is not None:
        updates.append("content = ?")
        params.append(content)

    if not updates:
        return False

    updates.append("updated_at_utc = ?")
    params.append(_utc_now())
    params.append(section_id)

    cursor = conn.execute(
        f"UPDATE custom_report_sections SET {', '.join(updates)} WHERE id = ?",
        params
    )
    conn.commit()
    return cursor.rowcount > 0


def delete_custom_section(conn: sqlite3.Connection, section_id: int) -> bool:
    """
    Delete a custom report section.

    Args:
        conn: SQLite connection to evidence database
        section_id: Section ID to delete

    Returns:
        True if section was deleted, False if not found
    """
    _ensure_table(conn)

    cursor = conn.execute(
        "DELETE FROM custom_report_sections WHERE id = ?",
        (section_id,)
    )
    conn.commit()
    return cursor.rowcount > 0


def get_custom_sections(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[Dict[str, Any]]:
    """
    Get all custom report sections for an evidence, ordered by sort_order.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        List of section dictionaries with id, title, content, sort_order, timestamps
    """
    _ensure_table(conn)

    cursor = conn.execute(
        """
        SELECT id, evidence_id, title, content, sort_order, created_at_utc, updated_at_utc
        FROM custom_report_sections
        WHERE evidence_id = ?
        ORDER BY sort_order ASC
        """,
        (evidence_id,)
    )

    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_custom_section_by_id(
    conn: sqlite3.Connection,
    section_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Get a single custom report section by ID.

    Args:
        conn: SQLite connection to evidence database
        section_id: Section ID

    Returns:
        Section dictionary or None if not found
    """
    _ensure_table(conn)

    cursor = conn.execute(
        """
        SELECT id, evidence_id, title, content, sort_order, created_at_utc, updated_at_utc
        FROM custom_report_sections
        WHERE id = ?
        """,
        (section_id,)
    )

    row = cursor.fetchone()
    if row is None:
        return None

    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def reorder_custom_section(
    conn: sqlite3.Connection,
    section_id: int,
    new_order: int,
) -> bool:
    """
    Move a section to a new position, adjusting other sections accordingly.

    Args:
        conn: SQLite connection to evidence database
        section_id: Section ID to move
        new_order: New sort_order value

    Returns:
        True if section was reordered, False if not found
    """
    _ensure_table(conn)

    # Get current section info
    section = get_custom_section_by_id(conn, section_id)
    if section is None:
        return False

    old_order = section["sort_order"]
    evidence_id = section["evidence_id"]

    if old_order == new_order:
        return True

    # Shift other sections to make room
    if new_order < old_order:
        # Moving up: shift sections between new and old down
        conn.execute(
            """
            UPDATE custom_report_sections
            SET sort_order = sort_order + 1
            WHERE evidence_id = ? AND sort_order >= ? AND sort_order < ?
            """,
            (evidence_id, new_order, old_order)
        )
    else:
        # Moving down: shift sections between old and new up
        conn.execute(
            """
            UPDATE custom_report_sections
            SET sort_order = sort_order - 1
            WHERE evidence_id = ? AND sort_order > ? AND sort_order <= ?
            """,
            (evidence_id, old_order, new_order)
        )

    # Update the target section
    conn.execute(
        "UPDATE custom_report_sections SET sort_order = ?, updated_at_utc = ? WHERE id = ?",
        (new_order, _utc_now(), section_id)
    )
    conn.commit()
    return True
