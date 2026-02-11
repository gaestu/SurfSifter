"""
Section modules database helpers.

This module provides CRUD operations for the section_modules table,
which stores module configurations attached to custom report sections.

Initial implementation.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _ensure_modules_table(conn: sqlite3.Connection) -> None:
    """Create section_modules table if it doesn't exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS section_modules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section_id INTEGER NOT NULL,
            module_id TEXT NOT NULL,
            config TEXT,
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            FOREIGN KEY (section_id) REFERENCES custom_report_sections(id) ON DELETE CASCADE
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_section_modules_section
        ON section_modules(section_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_section_modules_order
        ON section_modules(section_id, sort_order)
    """)
    conn.commit()


def _utc_now() -> str:
    """Return current UTC timestamp in ISO format."""
    return datetime.now(timezone.utc).isoformat()


def insert_section_module(
    conn: sqlite3.Connection,
    section_id: int,
    module_id: str,
    config: Optional[Dict[str, Any]] = None,
    sort_order: Optional[int] = None,
) -> int:
    """
    Insert a new module instance into a section.

    Args:
        conn: SQLite connection to evidence database
        section_id: ID of the parent section
        module_id: Module type identifier (e.g., "tagged_urls")
        config: Module configuration dictionary (filters, options)
        sort_order: Position in module list (auto-assigned if None)

    Returns:
        ID of the newly inserted module instance
    """
    _ensure_modules_table(conn)

    # Auto-assign sort_order if not provided
    if sort_order is None:
        cursor = conn.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 FROM section_modules WHERE section_id = ?",
            (section_id,)
        )
        sort_order = cursor.fetchone()[0]

    config_json = json.dumps(config) if config else None
    now = _utc_now()

    cursor = conn.execute(
        """
        INSERT INTO section_modules
        (section_id, module_id, config, sort_order, created_at_utc, updated_at_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (section_id, module_id, config_json, sort_order, now, now)
    )
    conn.commit()
    return cursor.lastrowid


def update_section_module(
    conn: sqlite3.Connection,
    module_instance_id: int,
    *,
    config: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Update an existing module instance configuration.

    Args:
        conn: SQLite connection to evidence database
        module_instance_id: ID of the module instance to update
        config: New configuration dictionary

    Returns:
        True if module was updated, False if not found
    """
    _ensure_modules_table(conn)

    config_json = json.dumps(config) if config is not None else None

    cursor = conn.execute(
        """
        UPDATE section_modules
        SET config = ?, updated_at_utc = ?
        WHERE id = ?
        """,
        (config_json, _utc_now(), module_instance_id)
    )
    conn.commit()
    return cursor.rowcount > 0


def delete_section_module(conn: sqlite3.Connection, module_instance_id: int) -> bool:
    """
    Delete a module instance from a section.

    Args:
        conn: SQLite connection to evidence database
        module_instance_id: ID of the module instance to delete

    Returns:
        True if module was deleted, False if not found
    """
    _ensure_modules_table(conn)

    cursor = conn.execute(
        "DELETE FROM section_modules WHERE id = ?",
        (module_instance_id,)
    )
    conn.commit()
    return cursor.rowcount > 0


def get_section_modules(
    conn: sqlite3.Connection,
    section_id: int,
) -> List[Dict[str, Any]]:
    """
    Get all module instances for a section, ordered by sort_order.

    Args:
        conn: SQLite connection to evidence database
        section_id: Section ID

    Returns:
        List of module instance dictionaries with id, module_id, config, sort_order, timestamps
    """
    _ensure_modules_table(conn)

    cursor = conn.execute(
        """
        SELECT id, section_id, module_id, config, sort_order, created_at_utc, updated_at_utc
        FROM section_modules
        WHERE section_id = ?
        ORDER BY sort_order ASC
        """,
        (section_id,)
    )

    results = []
    for row in cursor.fetchall():
        record = {
            "id": row[0],
            "section_id": row[1],
            "module_id": row[2],
            "config": json.loads(row[3]) if row[3] else {},
            "sort_order": row[4],
            "created_at_utc": row[5],
            "updated_at_utc": row[6],
        }
        results.append(record)

    return results


def get_section_module_by_id(
    conn: sqlite3.Connection,
    module_instance_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Get a single module instance by ID.

    Args:
        conn: SQLite connection to evidence database
        module_instance_id: Module instance ID

    Returns:
        Module instance dictionary or None if not found
    """
    _ensure_modules_table(conn)

    cursor = conn.execute(
        """
        SELECT id, section_id, module_id, config, sort_order, created_at_utc, updated_at_utc
        FROM section_modules
        WHERE id = ?
        """,
        (module_instance_id,)
    )

    row = cursor.fetchone()
    if row is None:
        return None

    return {
        "id": row[0],
        "section_id": row[1],
        "module_id": row[2],
        "config": json.loads(row[3]) if row[3] else {},
        "sort_order": row[4],
        "created_at_utc": row[5],
        "updated_at_utc": row[6],
    }


def reorder_section_module(
    conn: sqlite3.Connection,
    module_instance_id: int,
    new_order: int,
) -> bool:
    """
    Move a module instance to a new position within its section.

    Args:
        conn: SQLite connection to evidence database
        module_instance_id: Module instance ID to move
        new_order: New sort_order value

    Returns:
        True if module was reordered, False if not found
    """
    _ensure_modules_table(conn)

    # Get current module info
    module = get_section_module_by_id(conn, module_instance_id)
    if module is None:
        return False

    old_order = module["sort_order"]
    section_id = module["section_id"]

    if old_order == new_order:
        return True

    # Shift other modules to make room
    if new_order < old_order:
        # Moving up: shift modules between new and old down
        conn.execute(
            """
            UPDATE section_modules
            SET sort_order = sort_order + 1
            WHERE section_id = ? AND sort_order >= ? AND sort_order < ?
            """,
            (section_id, new_order, old_order)
        )
    else:
        # Moving down: shift modules between old and new up
        conn.execute(
            """
            UPDATE section_modules
            SET sort_order = sort_order - 1
            WHERE section_id = ? AND sort_order > ? AND sort_order <= ?
            """,
            (section_id, old_order, new_order)
        )

    # Update the target module
    conn.execute(
        "UPDATE section_modules SET sort_order = ?, updated_at_utc = ? WHERE id = ?",
        (new_order, _utc_now(), module_instance_id)
    )
    conn.commit()
    return True


def delete_modules_by_section(conn: sqlite3.Connection, section_id: int) -> int:
    """
    Delete all module instances for a section.

    Args:
        conn: SQLite connection to evidence database
        section_id: Section ID

    Returns:
        Number of modules deleted
    """
    _ensure_modules_table(conn)

    cursor = conn.execute(
        "DELETE FROM section_modules WHERE section_id = ?",
        (section_id,)
    )
    conn.commit()
    return cursor.rowcount


def get_modules_count_by_section(conn: sqlite3.Connection, section_id: int) -> int:
    """
    Get count of modules in a section.

    Args:
        conn: SQLite connection to evidence database
        section_id: Section ID

    Returns:
        Number of modules in the section
    """
    _ensure_modules_table(conn)

    cursor = conn.execute(
        "SELECT COUNT(*) FROM section_modules WHERE section_id = ?",
        (section_id,)
    )
    return cursor.fetchone()[0]
