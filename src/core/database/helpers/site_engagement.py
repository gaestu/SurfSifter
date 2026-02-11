"""
Site Engagement database helpers.

Functions for storing and retrieving site engagement data from Chromium browsers.
Site engagement includes both site_engagement and media_engagement data from
the profile.content_settings.exceptions section of Preferences.

Initial implementation
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def insert_site_engagement(
    conn,
    evidence_id: int,
    record: Dict[str, Any],
) -> int:
    """
    Insert a single site engagement record.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        record: Engagement record dict

    Returns:
        Inserted row ID
    """
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO site_engagement (
            evidence_id, browser, profile, origin, engagement_type,
            raw_score, points_added_today, last_engagement_time_utc,
            last_shortcut_launch_time_utc, has_high_score, media_playbacks,
            visits, last_media_playback_time_utc, last_modified_webkit,
            expiration, model, run_id, source_path, discovered_by,
            partition_index, fs_type, logical_path, forensic_path
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?
        )
    """, (
        evidence_id,
        record.get("browser"),
        record.get("profile"),
        record.get("origin"),
        record.get("engagement_type"),
        record.get("raw_score"),
        record.get("points_added_today"),
        record.get("last_engagement_time_utc"),
        record.get("last_shortcut_launch_time_utc"),
        record.get("has_high_score"),
        record.get("media_playbacks"),
        record.get("visits"),
        record.get("last_media_playback_time_utc"),
        record.get("last_modified_webkit"),
        record.get("expiration"),
        record.get("model"),
        record.get("run_id"),
        record.get("source_path"),
        record.get("discovered_by"),
        record.get("partition_index"),
        record.get("fs_type"),
        record.get("logical_path"),
        record.get("forensic_path"),
    ))

    return cursor.lastrowid


def insert_site_engagements(
    conn,
    evidence_id: int,
    records: List[Dict[str, Any]],
) -> int:
    """
    Insert multiple site engagement records.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        records: List of engagement record dicts

    Returns:
        Number of records inserted
    """
    if not records:
        return 0

    cursor = conn.cursor()

    values = []
    for record in records:
        values.append((
            evidence_id,
            record.get("browser"),
            record.get("profile"),
            record.get("origin"),
            record.get("engagement_type"),
            record.get("raw_score"),
            record.get("points_added_today"),
            record.get("last_engagement_time_utc"),
            record.get("last_shortcut_launch_time_utc"),
            record.get("has_high_score"),
            record.get("media_playbacks"),
            record.get("visits"),
            record.get("last_media_playback_time_utc"),
            record.get("last_modified_webkit"),
            record.get("expiration"),
            record.get("model"),
            record.get("run_id"),
            record.get("source_path"),
            record.get("discovered_by"),
            record.get("partition_index"),
            record.get("fs_type"),
            record.get("logical_path"),
            record.get("forensic_path"),
        ))

    cursor.executemany("""
        INSERT INTO site_engagement (
            evidence_id, browser, profile, origin, engagement_type,
            raw_score, points_added_today, last_engagement_time_utc,
            last_shortcut_launch_time_utc, has_high_score, media_playbacks,
            visits, last_media_playback_time_utc, last_modified_webkit,
            expiration, model, run_id, source_path, discovered_by,
            partition_index, fs_type, logical_path, forensic_path
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?
        )
    """, values)

    return cursor.rowcount


def get_site_engagements(
    conn,
    evidence_id: int,
    engagement_type: Optional[str] = None,
    browser: Optional[str] = None,
    origin_like: Optional[str] = None,
    min_score: Optional[float] = None,
    limit: int = 1000,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Get site engagement records with optional filtering.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        engagement_type: Filter by type ("site_engagement" or "media_engagement")
        browser: Filter by browser
        origin_like: Filter by origin (SQL LIKE pattern)
        min_score: Minimum raw_score (for site_engagement)
        limit: Maximum records to return
        offset: Record offset for pagination

    Returns:
        List of engagement record dicts
    """
    cursor = conn.cursor()
    cursor.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))

    query = "SELECT * FROM site_engagement WHERE evidence_id = ?"
    params: List[Any] = [evidence_id]

    if engagement_type:
        query += " AND engagement_type = ?"
        params.append(engagement_type)

    if browser:
        query += " AND browser = ?"
        params.append(browser)

    if origin_like:
        query += " AND origin LIKE ?"
        params.append(origin_like)

    if min_score is not None:
        query += " AND raw_score >= ?"
        params.append(min_score)

    query += " ORDER BY raw_score DESC NULLS LAST, id"
    query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    cursor.execute(query, params)
    return cursor.fetchall()


def get_site_engagement_stats(
    conn,
    evidence_id: int,
) -> Dict[str, Any]:
    """
    Get summary statistics for site engagement data.

    Args:
        conn: Database connection
        evidence_id: Evidence ID

    Returns:
        Dict with statistics
    """
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN engagement_type = 'site_engagement' THEN 1 ELSE 0 END) as site_count,
            SUM(CASE WHEN engagement_type = 'media_engagement' THEN 1 ELSE 0 END) as media_count,
            COUNT(DISTINCT origin) as unique_origins,
            COUNT(DISTINCT browser) as unique_browsers,
            MAX(raw_score) as max_score,
            AVG(raw_score) as avg_score
        FROM site_engagement
        WHERE evidence_id = ?
    """, (evidence_id,))

    row = cursor.fetchone()
    return {
        "total": row[0] or 0,
        "site_engagement_count": row[1] or 0,
        "media_engagement_count": row[2] or 0,
        "unique_origins": row[3] or 0,
        "unique_browsers": row[4] or 0,
        "max_score": row[5],
        "avg_score": row[6],
    }


def delete_site_engagement_by_run(
    conn,
    evidence_id: int,
    run_id: str,
) -> int:
    """
    Delete site engagement records for a specific run.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        run_id: Run ID to delete

    Returns:
        Number of records deleted
    """
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM site_engagement WHERE evidence_id = ? AND run_id = ?",
        (evidence_id, run_id)
    )
    return cursor.rowcount


def delete_site_engagement_by_evidence(
    conn,
    evidence_id: int,
) -> int:
    """
    Delete all site engagement records for an evidence.

    Args:
        conn: Database connection
        evidence_id: Evidence ID

    Returns:
        Number of records deleted
    """
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM site_engagement WHERE evidence_id = ?",
        (evidence_id,)
    )
    return cursor.rowcount


def get_top_engaged_sites(
    conn,
    evidence_id: int,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    Get top engaged sites by raw_score.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        limit: Maximum sites to return

    Returns:
        List of engagement records ordered by score
    """
    cursor = conn.cursor()
    cursor.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))

    cursor.execute("""
        SELECT * FROM site_engagement
        WHERE evidence_id = ?
          AND engagement_type = 'site_engagement'
          AND raw_score IS NOT NULL
        ORDER BY raw_score DESC
        LIMIT ?
    """, (evidence_id, limit))

    return cursor.fetchall()
