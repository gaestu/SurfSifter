"""
URL tag database helpers.

Provides functions for managing URL tags via the unified tagging system.
Tags are stored in the `tags` table with associations in `tag_associations`.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import TABLE_SCHEMAS
from .generic import insert_row, insert_rows


def insert_url_tags(
    conn: sqlite3.Connection, evidence_id: int, tags: Iterable[Dict[str, Any]]
) -> None:
    """
    Insert URL tags (user-created or auto-generated).

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        tags: Iterable of tag records with keys:
            - url_id: Foreign key to urls.id
            - tag: Tag string (e.g., 'gambling', 'suspicious')
            - auto_tag: Boolean (0=manual, 1=auto-generated)
            - created_at_utc: Optional explicit tag creation timestamp
            - tagged_at_utc: Optional explicit tag association timestamp

    Note:
        Silently ignores duplicates (UNIQUE constraint on url_id, tag).
    """
    association_records: List[Dict[str, Any]] = []
    tag_ids: Dict[str, int] = {}
    for record in tags:
        tag_name = record.get("tag")
        if not tag_name:
            continue
        normalized = tag_name.lower()
        created_at_utc = record.get("created_at_utc")
        tagged_at_utc = record.get("tagged_at_utc", created_at_utc)

        tag_id = tag_ids.get(normalized)
        if tag_id is None:
            tag_record = {
                "name": tag_name,
                "name_normalized": normalized,
                "created_by": "pattern_detection" if record.get("auto_tag") else "manual",
                "created_at_utc": created_at_utc,
            }
            insert_row(conn, TABLE_SCHEMAS["tags"], evidence_id, tag_record)

            tag_id_row = conn.execute(
                "SELECT id FROM tags WHERE evidence_id = ? AND name_normalized = ?",
                (evidence_id, normalized),
            ).fetchone()
            if not tag_id_row:
                continue
            tag_id = tag_id_row[0]
            tag_ids[normalized] = tag_id

        association_records.append({
            "tag_id": tag_id,
            "artifact_type": "url",
            "artifact_id": record.get("url_id"),
            "tagged_at_utc": tagged_at_utc,
            "tagged_by": "auto" if record.get("auto_tag") else "manual",
        })

    if not association_records:
        return

    insert_rows(conn, TABLE_SCHEMAS["tag_associations"], evidence_id, association_records)


def get_url_tags(
    conn: sqlite3.Connection, evidence_id: int, url_id: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Retrieve URL tags for an evidence.

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        url_id: Optional filter by specific URL

    Returns:
        List of dicts with tag records
    """
    if url_id is not None:
        rows = conn.execute(
            """
            SELECT
                ta.id,
                ta.evidence_id,
                ta.artifact_id as url_id,
                t.name as tag,
                CASE WHEN ta.tagged_by = 'auto' THEN 1 ELSE 0 END as auto_tag,
                ta.tagged_at_utc as created_at_utc
            FROM tag_associations ta
            JOIN tags t ON t.id = ta.tag_id
            WHERE ta.evidence_id = ? AND ta.artifact_type = 'url' AND ta.artifact_id = ?
            ORDER BY t.name
            """,
            (evidence_id, url_id)
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                ta.id,
                ta.evidence_id,
                ta.artifact_id as url_id,
                t.name as tag,
                CASE WHEN ta.tagged_by = 'auto' THEN 1 ELSE 0 END as auto_tag,
                ta.tagged_at_utc as created_at_utc
            FROM tag_associations ta
            JOIN tags t ON t.id = ta.tag_id
            WHERE ta.evidence_id = ? AND ta.artifact_type = 'url'
            ORDER BY t.name
            """,
            (evidence_id,)
        ).fetchall()

    return [dict(row) for row in rows]


__all__ = [
    "insert_url_tags",
    "get_url_tags",
]
