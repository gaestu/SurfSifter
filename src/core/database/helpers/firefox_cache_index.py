"""
Database helper functions for Firefox cache index entries.

Provides CRUD operations for the ``firefox_cache_index`` table which stores
metadata parsed from the Firefox cache2 binary index file, including entries
whose cached content has been evicted or moved to doomed/trash.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional

__all__ = [
    "insert_firefox_cache_index_entry",
    "insert_firefox_cache_index_entries",
    "get_firefox_cache_index_entries",
    "get_firefox_cache_index_count",
    "get_firefox_cache_index_stats",
    "delete_firefox_cache_index_by_run",
]


def insert_firefox_cache_index_entry(
    conn: sqlite3.Connection,
    evidence_id: int,
    entry: Dict[str, Any],
) -> int:
    """Insert a single Firefox cache index entry.

    Args:
        conn: Evidence database connection.
        evidence_id: Evidence ID.
        entry: Dict with cache index entry fields.

    Returns:
        Row ID of the inserted entry.
    """
    cursor = conn.execute(
        """
        INSERT INTO firefox_cache_index (
            run_id, evidence_id, partition_index, source_path,
            entry_hash, frecency, origin_attrs_hash,
            on_start_time, on_stop_time,
            content_type, content_type_name, file_size_kb, raw_flags,
            is_initialized, is_anonymous, is_removed, is_pinned, has_alt_data,
            index_version, index_timestamp, index_dirty,
            has_entry_file, entry_source, url,
            browser, profile_path, os_user
        ) VALUES (
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?
        )
        """,
        (
            entry["run_id"],
            evidence_id,
            entry.get("partition_index", 0),
            entry.get("source_path", ""),
            entry["entry_hash"],
            entry.get("frecency"),
            entry.get("origin_attrs_hash"),
            entry.get("on_start_time"),
            entry.get("on_stop_time"),
            entry.get("content_type"),
            entry.get("content_type_name"),
            entry.get("file_size_kb"),
            entry.get("raw_flags"),
            entry.get("is_initialized", False),
            entry.get("is_anonymous", False),
            entry.get("is_removed", False),
            entry.get("is_pinned", False),
            entry.get("has_alt_data", False),
            entry.get("index_version"),
            entry.get("index_timestamp"),
            entry.get("index_dirty", False),
            entry.get("has_entry_file", False),
            entry.get("entry_source"),
            entry.get("url"),
            entry.get("browser", "firefox"),
            entry.get("profile_path"),
            entry.get("os_user"),
        ),
    )
    row_id = cursor.lastrowid
    if row_id is None:
        raise RuntimeError("Insert succeeded but sqlite3 did not return lastrowid")
    return int(row_id)


def insert_firefox_cache_index_entries(
    conn: sqlite3.Connection,
    evidence_id: int,
    entries: List[Dict[str, Any]],
) -> int:
    """Batch-insert Firefox cache index entries.

    Args:
        conn: Evidence database connection.
        evidence_id: Evidence ID.
        entries: List of entry dicts.

    Returns:
        Number of entries inserted.
    """
    if not entries:
        return 0

    rows = [
        (
            e["run_id"],
            evidence_id,
            e.get("partition_index", 0),
            e.get("source_path", ""),
            e["entry_hash"],
            e.get("frecency"),
            e.get("origin_attrs_hash"),
            e.get("on_start_time"),
            e.get("on_stop_time"),
            e.get("content_type"),
            e.get("content_type_name"),
            e.get("file_size_kb"),
            e.get("raw_flags"),
            e.get("is_initialized", False),
            e.get("is_anonymous", False),
            e.get("is_removed", False),
            e.get("is_pinned", False),
            e.get("has_alt_data", False),
            e.get("index_version"),
            e.get("index_timestamp"),
            e.get("index_dirty", False),
            e.get("has_entry_file", False),
            e.get("entry_source"),
            e.get("url"),
            e.get("browser", "firefox"),
            e.get("profile_path"),
            e.get("os_user"),
        )
        for e in entries
    ]

    conn.executemany(
        """
        INSERT INTO firefox_cache_index (
            run_id, evidence_id, partition_index, source_path,
            entry_hash, frecency, origin_attrs_hash,
            on_start_time, on_stop_time,
            content_type, content_type_name, file_size_kb, raw_flags,
            is_initialized, is_anonymous, is_removed, is_pinned, has_alt_data,
            index_version, index_timestamp, index_dirty,
            has_entry_file, entry_source, url,
            browser, profile_path, os_user
        ) VALUES (
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?
        )
        """,
        rows,
    )
    return len(rows)


def get_firefox_cache_index_entries(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    run_id: Optional[str] = None,
    removed_only: bool = False,
    has_entry_file: Optional[bool] = None,
    content_type: Optional[int] = None,
    entry_source: Optional[str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """Query Firefox cache index entries with optional filters.

    Args:
        conn: Evidence database connection.
        evidence_id: Evidence ID.
        run_id: Optional run ID filter.
        removed_only: If True, return only entries flagged as removed.
        has_entry_file: Filter by whether a backing entry file exists.
        content_type: Filter by content type enum value.
        entry_source: Filter by entry source ('entries', 'doomed', 'trash', 'journal').
        limit: Maximum number of rows to return.
        offset: Row offset for pagination.

    Returns:
        List of entry dicts.
    """
    conditions = ["evidence_id = ?"]
    params: list = [evidence_id]

    if run_id is not None:
        conditions.append("run_id = ?")
        params.append(run_id)

    if removed_only:
        conditions.append("is_removed = 1")

    if has_entry_file is not None:
        conditions.append("has_entry_file = ?")
        params.append(int(has_entry_file))

    if content_type is not None:
        conditions.append("content_type = ?")
        params.append(content_type)

    if entry_source is not None:
        conditions.append("entry_source = ?")
        params.append(entry_source)

    where = " AND ".join(conditions)
    query = f"SELECT * FROM firefox_cache_index WHERE {where} ORDER BY id"

    if limit is not None:
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

    conn.row_factory = sqlite3.Row
    cursor = conn.execute(query, params)
    return [dict(row) for row in cursor.fetchall()]


def get_firefox_cache_index_count(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    run_id: Optional[str] = None,
    removed_only: bool = False,
    has_entry_file: Optional[bool] = None,
    content_type: Optional[int] = None,
    entry_source: Optional[str] = None,
) -> int:
    """Count Firefox cache index entries matching filters.

    Args:
        conn: Evidence database connection.
        evidence_id: Evidence ID.
        run_id: Optional run ID filter.
        removed_only: If True, count only entries flagged as removed.
        has_entry_file: Filter by whether a backing entry file exists.
        content_type: Filter by content type enum value.
        entry_source: Filter by entry source ('entries', 'doomed', 'trash', 'journal').

    Returns:
        Number of matching entries.
    """
    conditions = ["evidence_id = ?"]
    params: list = [evidence_id]

    if run_id is not None:
        conditions.append("run_id = ?")
        params.append(run_id)

    if removed_only:
        conditions.append("is_removed = 1")

    if has_entry_file is not None:
        conditions.append("has_entry_file = ?")
        params.append(int(has_entry_file))

    if content_type is not None:
        conditions.append("content_type = ?")
        params.append(content_type)

    if entry_source is not None:
        conditions.append("entry_source = ?")
        params.append(entry_source)

    where = " AND ".join(conditions)
    row = conn.execute(
        f"SELECT COUNT(*) FROM firefox_cache_index WHERE {where}", params,
    ).fetchone()
    return int(row[0] if row is not None else 0)


def get_firefox_cache_index_stats(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Get summary statistics for Firefox cache index entries.

    Args:
        conn: Evidence database connection.
        evidence_id: Evidence ID.
        run_id: Optional run ID filter.

    Returns:
        Dict with counts: total, removed, with_file, without_file,
        by_content_type, by_entry_source.
    """
    conditions = ["evidence_id = ?"]
    params: list = [evidence_id]
    if run_id is not None:
        conditions.append("run_id = ?")
        params.append(run_id)
    where = " AND ".join(conditions)

    # Total count
    total = conn.execute(
        f"SELECT COUNT(*) FROM firefox_cache_index WHERE {where}", params,
    ).fetchone()[0]

    # Removed count
    removed = conn.execute(
        f"SELECT COUNT(*) FROM firefox_cache_index WHERE {where} AND is_removed = 1",
        params,
    ).fetchone()[0]

    # With / without file
    with_file = conn.execute(
        f"SELECT COUNT(*) FROM firefox_cache_index WHERE {where} AND has_entry_file = 1",
        params,
    ).fetchone()[0]

    # By content type
    ct_rows = conn.execute(
        f"SELECT content_type_name, COUNT(*) AS cnt "
        f"FROM firefox_cache_index WHERE {where} "
        f"GROUP BY content_type_name ORDER BY cnt DESC",
        params,
    ).fetchall()
    by_content_type = {r[0]: r[1] for r in ct_rows}

    # By entry source
    es_rows = conn.execute(
        f"SELECT entry_source, COUNT(*) AS cnt "
        f"FROM firefox_cache_index WHERE {where} "
        f"GROUP BY entry_source ORDER BY cnt DESC",
        params,
    ).fetchall()
    by_entry_source = {r[0] or "index_only": r[1] for r in es_rows}

    return {
        "total": total,
        "removed": removed,
        "with_file": with_file,
        "without_file": total - with_file,
        "by_content_type": by_content_type,
        "by_entry_source": by_entry_source,
    }


def delete_firefox_cache_index_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete Firefox cache index entries for a given run.

    Args:
        conn: Evidence database connection.
        evidence_id: Evidence ID.
        run_id: Run ID whose entries should be deleted.

    Returns:
        Number of rows deleted.
    """
    cursor = conn.execute(
        "DELETE FROM firefox_cache_index WHERE evidence_id = ? AND run_id = ?",
        (evidence_id, run_id),
    )
    return cursor.rowcount
