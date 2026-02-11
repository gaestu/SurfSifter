"""
Appendix modules database helpers.

This module provides CRUD operations for the appendix_modules table.
Appendix modules are stored per-evidence and support ordering via sort_order.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _ensure_appendix_table(conn: sqlite3.Connection) -> None:
    """Create appendix_modules table if it doesn't exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS appendix_modules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            module_id TEXT NOT NULL,
            title TEXT,
            config TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_appendix_modules_evidence
        ON appendix_modules(evidence_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_appendix_modules_order
        ON appendix_modules(evidence_id, sort_order)
        """
    )
    conn.commit()


def _utc_now() -> str:
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def insert_appendix_module(
    conn: sqlite3.Connection,
    evidence_id: int,
    module_id: str,
    title: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    sort_order: Optional[int] = None,
) -> int:
    """Insert a new appendix module instance."""
    _ensure_appendix_table(conn)

    if sort_order is None:
        cursor = conn.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM appendix_modules WHERE evidence_id = ?",
            (evidence_id,),
        )
        sort_order = cursor.fetchone()[0]

    config_json = json.dumps(config) if config else None
    now = _utc_now()
    cursor = conn.execute(
        """
        INSERT INTO appendix_modules
        (evidence_id, module_id, title, config, sort_order, created_at_utc, updated_at_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (evidence_id, module_id, title, config_json, sort_order, now, now),
    )
    conn.commit()
    return cursor.lastrowid


def update_appendix_module(
    conn: sqlite3.Connection,
    module_instance_id: int,
    *,
    title: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
) -> bool:
    """Update an existing appendix module instance."""
    _ensure_appendix_table(conn)

    updates = []
    params: List[Any] = []

    if title is not None:
        updates.append("title = ?")
        params.append(title)

    if config is not None:
        updates.append("config = ?")
        params.append(json.dumps(config))

    if not updates:
        return False

    updates.append("updated_at_utc = ?")
    params.append(_utc_now())
    params.append(module_instance_id)

    cursor = conn.execute(
        f"UPDATE appendix_modules SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    conn.commit()
    return cursor.rowcount > 0


def delete_appendix_module(conn: sqlite3.Connection, module_instance_id: int) -> bool:
    """Delete an appendix module instance."""
    _ensure_appendix_table(conn)
    cursor = conn.execute(
        "DELETE FROM appendix_modules WHERE id = ?",
        (module_instance_id,),
    )
    conn.commit()
    return cursor.rowcount > 0


def get_appendix_modules(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[Dict[str, Any]]:
    """Get appendix modules for an evidence, ordered by sort_order."""
    _ensure_appendix_table(conn)
    cursor = conn.execute(
        """
        SELECT id, evidence_id, module_id, title, config, sort_order, created_at_utc, updated_at_utc
        FROM appendix_modules
        WHERE evidence_id = ?
        ORDER BY sort_order ASC
        """,
        (evidence_id,),
    )
    results = []
    for row in cursor.fetchall():
        results.append(
            {
                "id": row[0],
                "evidence_id": row[1],
                "module_id": row[2],
                "title": row[3] or "",
                "config": json.loads(row[4]) if row[4] else {},
                "sort_order": row[5],
                "created_at_utc": row[6],
                "updated_at_utc": row[7],
            }
        )
    return results


def get_appendix_module_by_id(
    conn: sqlite3.Connection,
    module_instance_id: int,
) -> Optional[Dict[str, Any]]:
    """Get a single appendix module instance by ID."""
    _ensure_appendix_table(conn)
    cursor = conn.execute(
        """
        SELECT id, evidence_id, module_id, title, config, sort_order, created_at_utc, updated_at_utc
        FROM appendix_modules
        WHERE id = ?
        """,
        (module_instance_id,),
    )
    row = cursor.fetchone()
    if row is None:
        return None

    return {
        "id": row[0],
        "evidence_id": row[1],
        "module_id": row[2],
        "title": row[3] or "",
        "config": json.loads(row[4]) if row[4] else {},
        "sort_order": row[5],
        "created_at_utc": row[6],
        "updated_at_utc": row[7],
    }


def reorder_appendix_module(
    conn: sqlite3.Connection,
    module_instance_id: int,
    new_order: int,
) -> bool:
    """Move an appendix module to a new position."""
    _ensure_appendix_table(conn)

    module = get_appendix_module_by_id(conn, module_instance_id)
    if module is None:
        return False

    old_order = module["sort_order"]
    evidence_id = module["evidence_id"]

    if old_order == new_order:
        return True

    if new_order < old_order:
        conn.execute(
            """
            UPDATE appendix_modules
            SET sort_order = sort_order + 1
            WHERE evidence_id = ? AND sort_order >= ? AND sort_order < ?
            """,
            (evidence_id, new_order, old_order),
        )
    else:
        conn.execute(
            """
            UPDATE appendix_modules
            SET sort_order = sort_order - 1
            WHERE evidence_id = ? AND sort_order > ? AND sort_order <= ?
            """,
            (evidence_id, old_order, new_order),
        )

    conn.execute(
        "UPDATE appendix_modules SET sort_order = ?, updated_at_utc = ? WHERE id = ?",
        (new_order, _utc_now(), module_instance_id),
    )
    conn.commit()
    return True
