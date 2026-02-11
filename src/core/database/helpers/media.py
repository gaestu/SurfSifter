"""
Media history database helper functions.

This module provides CRUD operations for media_playback, media_sessions,
and media_origins tables.

Added media_origins helpers
Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    # Media playback
    "insert_media_playback",
    "insert_media_playbacks",
    "get_media_playback",
    "delete_media_playback_by_run",
    # Media sessions
    "insert_media_session",
    "insert_media_sessions",
    "get_media_sessions",
    "delete_media_sessions_by_run",
    # Media origins
    "insert_media_origin",
    "insert_media_origins",
    "get_media_origins",
    "delete_media_origins_by_run",
    # Stats and combined delete
    "get_media_stats",
    "delete_media_by_run",
]


# ============================================================================
# Media Playback
# ============================================================================

def insert_media_playback(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    url: str,
    **kwargs,
) -> None:
    """
    Insert a single media playback entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        url: Media URL
        **kwargs: Optional fields (profile, title, artist, watch_time_s, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "url": url,
        "title": kwargs.get("title"),
        "artist": kwargs.get("artist"),
        "album": kwargs.get("album"),
        "source_title": kwargs.get("source_title"),
        "watch_time_s": kwargs.get("watch_time_s"),
        "has_audio": kwargs.get("has_audio"),
        "has_video": kwargs.get("has_video"),
        "last_updated_utc": kwargs.get("last_updated_utc"),
        "run_id": kwargs.get("run_id"),
        "source_path": kwargs.get("source_path"),
        "discovered_by": kwargs.get("discovered_by"),
        "partition_index": kwargs.get("partition_index"),
        "fs_type": kwargs.get("fs_type"),
        "logical_path": kwargs.get("logical_path"),
        "forensic_path": kwargs.get("forensic_path"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["media_playback"], evidence_id, record)


def insert_media_playbacks(conn: sqlite3.Connection, evidence_id: int, playbacks: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple media playback entries in batch.

    Returns:
        Number of entries inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["media_playback"], evidence_id, playbacks)


def get_media_playback(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    url: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve media playback entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        url: Optional URL filter (partial match)
        limit: Maximum rows to return

    Returns:
        List of media playback records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if url:
        filters["url"] = (FilterOp.LIKE, f"%{url}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["media_playback"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_media_playback_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete media playback entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["media_playback"], evidence_id, run_id)


# ============================================================================
# Media Sessions
# ============================================================================

def insert_media_session(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    origin: str,
    **kwargs,
) -> None:
    """
    Insert a single media session entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        origin: Site origin
        **kwargs: Optional fields (profile, position_ms, duration_ms, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "origin": origin,
        "position_ms": kwargs.get("position_ms"),
        "duration_ms": kwargs.get("duration_ms"),
        "title": kwargs.get("title"),
        "artist": kwargs.get("artist"),
        "album": kwargs.get("album"),
        "source_url": kwargs.get("source_url"),
        "last_updated_utc": kwargs.get("last_updated_utc"),
        "run_id": kwargs.get("run_id"),
        "source_path": kwargs.get("source_path"),
        "discovered_by": kwargs.get("discovered_by"),
        "partition_index": kwargs.get("partition_index"),
        "fs_type": kwargs.get("fs_type"),
        "logical_path": kwargs.get("logical_path"),
        "forensic_path": kwargs.get("forensic_path"),
        "tags": kwargs.get("tags"),
        "notes": kwargs.get("notes"),
    }
    insert_row(conn, TABLE_SCHEMAS["media_sessions"], evidence_id, record)


def insert_media_sessions(conn: sqlite3.Connection, evidence_id: int, sessions: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple media session entries in batch.

    Returns:
        Number of entries inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["media_sessions"], evidence_id, sessions)


def get_media_sessions(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    origin: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve media session entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        origin: Optional origin filter (partial match)
        limit: Maximum rows to return

    Returns:
        List of media session records as dicts
    """
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if origin:
        filters["origin"] = (FilterOp.LIKE, f"%{origin}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["media_sessions"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_media_sessions_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete media session entries from a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["media_sessions"], evidence_id, run_id)


def get_media_stats(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, Any]:
    """Get media history statistics for an evidence."""
    # Total watch time
    total_watch = conn.execute(
        "SELECT SUM(watch_time_seconds) FROM media_playback WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0] or 0

    # By browser
    by_browser = {}
    for row in conn.execute(
        """
        SELECT browser, COUNT(*) as count, SUM(watch_time_seconds) as total_time
        FROM media_playback WHERE evidence_id = ?
        GROUP BY browser
        """,
        (evidence_id,)
    ):
        by_browser[row["browser"]] = {
            "count": row["count"],
            "total_time": row["total_time"] or 0
        }

    # Video vs Audio
    video_count = conn.execute(
        "SELECT COUNT(*) FROM media_playback WHERE evidence_id = ? AND has_video = 1",
        (evidence_id,)
    ).fetchone()[0]

    audio_only_count = conn.execute(
        "SELECT COUNT(*) FROM media_playback WHERE evidence_id = ? AND has_video = 0 AND has_audio = 1",
        (evidence_id,)
    ).fetchone()[0]

    return {
        "total_watch_time_seconds": total_watch,
        "by_browser": by_browser,
        "video_count": video_count,
        "audio_only_count": audio_only_count,
    }


def delete_media_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete media history from a specific extraction run."""
    total = 0
    for table in ("media_playback", "media_sessions", "media_origins"):
        try:
            cursor = conn.execute(
                f"DELETE FROM {table} WHERE evidence_id = ? AND run_id = ?",
                (evidence_id, run_id),
            )
            total += cursor.rowcount
        except sqlite3.OperationalError:
            # Table may not exist in older databases
            pass
    return total


# ============================================================================
# Media Origins
# ============================================================================

def insert_media_origin(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    origin: str,
    **kwargs,
) -> None:
    """
    Insert a single media origin entry.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Browser name
        origin: Origin URL (e.g., "https://youtube.com")
        **kwargs: Optional fields (profile, origin_id_source, last_updated_utc, etc.)
    """
    record = {
        "browser": browser,
        "profile": kwargs.get("profile"),
        "origin": origin,
        "origin_id_source": kwargs.get("origin_id_source"),
        "last_updated_utc": kwargs.get("last_updated_utc"),
        "run_id": kwargs.get("run_id"),
        "source_path": kwargs.get("source_path"),
        "discovered_by": kwargs.get("discovered_by"),
        "partition_index": kwargs.get("partition_index"),
        "fs_type": kwargs.get("fs_type"),
        "logical_path": kwargs.get("logical_path"),
        "forensic_path": kwargs.get("forensic_path"),
    }

    # Build insert statement (media_origins not in TABLE_SCHEMAS)
    columns = ["evidence_id"] + [k for k, v in record.items() if v is not None]
    values = [evidence_id] + [v for v in record.values() if v is not None]
    placeholders = ", ".join("?" * len(columns))
    column_names = ", ".join(columns)

    conn.execute(
        f"INSERT INTO media_origins ({column_names}) VALUES ({placeholders})",
        values,
    )


def insert_media_origins(
    conn: sqlite3.Connection,
    evidence_id: int,
    origins: Iterable[Dict[str, Any]]
) -> int:
    """
    Insert multiple media origin entries in batch.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        origins: Iterable of origin records

    Returns:
        Number of entries inserted
    """
    count = 0
    for record in origins:
        # Build insert statement
        columns = ["evidence_id"] + [k for k, v in record.items() if v is not None]
        values = [evidence_id] + [v for v in record.values() if v is not None]
        placeholders = ", ".join("?" * len(columns))
        column_names = ", ".join(columns)

        conn.execute(
            f"INSERT INTO media_origins ({column_names}) VALUES ({placeholders})",
            values,
        )
        count += 1

    return count


def get_media_origins(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    origin: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve media origin entries for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        browser: Optional browser filter
        origin: Optional origin filter (partial match)
        limit: Maximum rows to return

    Returns:
        List of media origin records as dicts
    """
    conditions = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]

    if browser:
        conditions.append("browser = ?")
        params.append(browser)
    if origin:
        conditions.append("origin LIKE ?")
        params.append(f"%{origin}%")

    params.append(limit)

    rows = conn.execute(
        f"SELECT * FROM media_origins WHERE {' AND '.join(conditions)} LIMIT ?",
        params
    ).fetchall()

    return [dict(row) for row in rows]


def delete_media_origins_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete media origin entries from a specific run."""
    cursor = conn.execute(
        "DELETE FROM media_origins WHERE evidence_id = ? AND run_id = ?",
        (evidence_id, run_id),
    )
    return cursor.rowcount