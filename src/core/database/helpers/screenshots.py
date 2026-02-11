"""
Screenshots database helper functions.

This module provides CRUD operations for the screenshots table.

Initial implementation for investigator screenshot documentation.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import get_rows, insert_row

__all__ = [
    "insert_screenshot",
    "update_screenshot",
    "delete_screenshot",
    "get_screenshot",
    "get_screenshots",
    "get_screenshot_count",
    "get_sequences",
    "reorder_sequence",
    "get_screenshot_stats",
]


def insert_screenshot(
    conn: sqlite3.Connection,
    evidence_id: int,
    dest_path: str,
    filename: str,
    *,
    captured_url: Optional[str] = None,
    size_bytes: Optional[int] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    md5: Optional[str] = None,
    sha256: Optional[str] = None,
    title: Optional[str] = None,
    caption: Optional[str] = None,
    notes: Optional[str] = None,
    sequence_name: Optional[str] = None,
    sequence_order: int = 0,
    source: str = "sandbox",
    captured_at_utc: Optional[str] = None,
) -> int:
    """
    Insert a screenshot record.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        dest_path: Relative path to screenshot file
        filename: Screenshot filename
        captured_url: URL that was captured (optional)
        size_bytes: File size in bytes
        width: Image width in pixels
        height: Image height in pixels
        md5: MD5 hash of file
        sha256: SHA-256 hash of file
        title: Short title for report headers
        caption: Description shown under image in report
        notes: Internal investigator notes (not in report)
        sequence_name: Sequence group name
        sequence_order: Order within sequence
        source: Source type ('sandbox' or 'upload')
        captured_at_utc: Capture timestamp (defaults to now)

    Returns:
        ID of inserted screenshot
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    record = {
        "captured_url": captured_url,
        "dest_path": dest_path,
        "filename": filename,
        "size_bytes": size_bytes,
        "width": width,
        "height": height,
        "md5": md5,
        "sha256": sha256,
        "title": title,
        "caption": caption,
        "notes": notes,
        "sequence_name": sequence_name,
        "sequence_order": sequence_order,
        "source": source,
        "captured_at_utc": captured_at_utc or now_utc,
        "created_at_utc": now_utc,
        "updated_at_utc": None,
    }

    insert_row(conn, TABLE_SCHEMAS["screenshots"], evidence_id, record)

    # Get the inserted ID
    cursor = conn.execute("SELECT last_insert_rowid()")
    return cursor.fetchone()[0]


def update_screenshot(
    conn: sqlite3.Connection,
    evidence_id: int,
    screenshot_id: int,
    **kwargs,
) -> bool:
    """
    Update a screenshot record.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        screenshot_id: Screenshot ID to update
        **kwargs: Fields to update (title, caption, notes, sequence_name, sequence_order)

    Returns:
        True if updated, False if not found
    """
    # Allowed fields to update
    allowed_fields = {"title", "caption", "notes", "sequence_name", "sequence_order", "captured_url"}
    update_fields = {k: v for k, v in kwargs.items() if k in allowed_fields}

    if not update_fields:
        return False

    # Add updated timestamp
    update_fields["updated_at_utc"] = datetime.now(timezone.utc).isoformat()

    set_clause = ", ".join(f"{k} = ?" for k in update_fields.keys())
    values = list(update_fields.values()) + [evidence_id, screenshot_id]

    cursor = conn.execute(
        f"UPDATE screenshots SET {set_clause} WHERE evidence_id = ? AND id = ?",
        values
    )
    conn.commit()

    return cursor.rowcount > 0


def delete_screenshot(
    conn: sqlite3.Connection,
    evidence_id: int,
    screenshot_id: int,
) -> bool:
    """
    Delete a screenshot record.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        screenshot_id: Screenshot ID to delete

    Returns:
        True if deleted, False if not found
    """
    cursor = conn.execute(
        "DELETE FROM screenshots WHERE evidence_id = ? AND id = ?",
        (evidence_id, screenshot_id)
    )
    conn.commit()

    return cursor.rowcount > 0


def get_screenshot(
    conn: sqlite3.Connection,
    evidence_id: int,
    screenshot_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Get a single screenshot by ID.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        screenshot_id: Screenshot ID

    Returns:
        Screenshot record as dict, or None if not found
    """
    old_factory = conn.row_factory
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM screenshots WHERE evidence_id = ? AND id = ?",
            (evidence_id, screenshot_id)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.row_factory = old_factory


def get_screenshots(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    sequence_name: Optional[str] = None,
    source: Optional[str] = None,
    limit: int = 1000,
    offset: int = 0,
    order_by: str = "captured_at_utc",
    order_dir: str = "DESC",
) -> List[Dict[str, Any]]:
    """
    Get screenshots for an evidence with optional filtering.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        sequence_name: Optional sequence name filter
        source: Optional source filter ('sandbox', 'upload')
        limit: Maximum rows to return
        offset: Number of rows to skip
        order_by: Column to order by
        order_dir: Order direction ('ASC' or 'DESC')

    Returns:
        List of screenshot records as dicts
    """
    filters: Dict[str, Any] = {}
    if sequence_name is not None:
        filters["sequence_name"] = (FilterOp.EQ, sequence_name)
    if source is not None:
        filters["source"] = (FilterOp.EQ, source)

    return get_rows(
        conn,
        TABLE_SCHEMAS["screenshots"],
        evidence_id,
        filters=filters or None,
        limit=limit,
        offset=offset,
        order_by=order_by,
        order_dir=order_dir,
    )


def get_screenshot_count(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> int:
    """
    Get total screenshot count for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        Number of screenshots
    """
    cursor = conn.execute(
        "SELECT COUNT(*) FROM screenshots WHERE evidence_id = ?",
        (evidence_id,)
    )
    return cursor.fetchone()[0]


def get_sequences(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[str]:
    """
    Get all unique sequence names for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        List of sequence names (excludes NULL/empty)
    """
    cursor = conn.execute(
        """
        SELECT DISTINCT sequence_name
        FROM screenshots
        WHERE evidence_id = ? AND sequence_name IS NOT NULL AND sequence_name != ''
        ORDER BY sequence_name
        """,
        (evidence_id,)
    )
    return [row[0] for row in cursor.fetchall()]


def reorder_sequence(
    conn: sqlite3.Connection,
    evidence_id: int,
    sequence_name: str,
    screenshot_ids: List[int],
) -> bool:
    """
    Reorder screenshots within a sequence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        sequence_name: Sequence to reorder
        screenshot_ids: Screenshot IDs in desired order

    Returns:
        True if reordered successfully
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    for order, screenshot_id in enumerate(screenshot_ids):
        conn.execute(
            """
            UPDATE screenshots
            SET sequence_order = ?, updated_at_utc = ?
            WHERE evidence_id = ? AND id = ? AND sequence_name = ?
            """,
            (order, now_utc, evidence_id, screenshot_id, sequence_name)
        )

    conn.commit()
    return True


def get_screenshot_stats(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, Any]:
    """
    Get screenshot statistics for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        Dict with total_count, by_source, by_sequence, total_size_bytes
    """
    # Total count
    total = conn.execute(
        "SELECT COUNT(*) FROM screenshots WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0]

    # By source
    by_source = {}
    for row in conn.execute(
        """
        SELECT source, COUNT(*) as count
        FROM screenshots
        WHERE evidence_id = ?
        GROUP BY source
        """,
        (evidence_id,)
    ):
        by_source[row[0]] = row[1]

    # By sequence
    by_sequence = {}
    for row in conn.execute(
        """
        SELECT COALESCE(sequence_name, '(ungrouped)'), COUNT(*) as count
        FROM screenshots
        WHERE evidence_id = ?
        GROUP BY sequence_name
        """,
        (evidence_id,)
    ):
        by_sequence[row[0]] = row[1]

    # Total size
    total_size = conn.execute(
        "SELECT COALESCE(SUM(size_bytes), 0) FROM screenshots WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0]

    return {
        "total_count": total,
        "by_source": by_source,
        "by_sequence": by_sequence,
        "total_size_bytes": total_size,
    }
