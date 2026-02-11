"""
Investigator downloads database helper functions.

This module provides CRUD operations for the downloads table (investigator-acquired files).
Distinct from browser_downloads which stores browser download history.

Extracted from case_data.py during modular repository refactor.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

__all__ = [
    "insert_download",
    "update_download_status",
    "update_download_image_metadata",
    "get_download",
    "get_download_by_path",
    "get_downloads",
    "get_download_count",
    "get_download_stats",
    "get_download_domains",
    "get_url_download_status",
    "find_url_by_filename_domain",
]


def insert_download(
    conn: sqlite3.Connection,
    evidence_id: int,
    url: str,
    domain: str,
    file_type: str,
    file_extension: str,
    *,
    url_id: Optional[int] = None,
    status: str = "pending",
    filename: Optional[str] = None,
    queued_at_utc: Optional[str] = None,
) -> int:
    """
    Insert a new download record.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        url: Source URL
        domain: URL domain
        file_type: File type classification (image, video, document, etc.)
        file_extension: File extension
        url_id: Optional reference to urls table
        status: Initial status (default: "pending")
        filename: Optional filename
        queued_at_utc: Optional timestamp (defaults to now)

    Returns:
        The download ID (lastrowid)
    """
    if queued_at_utc is None:
        queued_at_utc = datetime.now(timezone.utc).isoformat()

    sql = """
        INSERT INTO downloads (
            evidence_id, url_id, url, domain, file_type, file_extension,
            status, filename, queued_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor = conn.execute(
        sql,
        (evidence_id, url_id, url, domain, file_type, file_extension, status, filename, queued_at_utc),
    )
    return cursor.lastrowid


def update_download_status(
    conn: sqlite3.Connection,
    evidence_id: int,
    download_id: int,
    status: str,
    *,
    dest_path: Optional[str] = None,
    filename: Optional[str] = None,
    size_bytes: Optional[int] = None,
    md5: Optional[str] = None,
    sha256: Optional[str] = None,
    content_type: Optional[str] = None,
    response_code: Optional[int] = None,
    error_message: Optional[str] = None,
    duration_seconds: Optional[float] = None,
    attempts: Optional[int] = None,
) -> None:
    """
    Update download status and metadata.

    Automatically sets started_at_utc when status="downloading"
    and completed_at_utc when status in (completed, failed, cancelled).

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        download_id: Download record ID
        status: New status
        dest_path: Destination file path
        filename: Downloaded filename
        size_bytes: File size in bytes
        md5: MD5 hash
        sha256: SHA256 hash
        content_type: HTTP Content-Type header
        response_code: HTTP response code
        error_message: Error message (for failed status)
        duration_seconds: Download duration
        attempts: Number of download attempts
    """
    updates = ["status = ?"]
    params: List[Any] = [status]

    if dest_path is not None:
        updates.append("dest_path = ?")
        params.append(dest_path)
    if filename is not None:
        updates.append("filename = ?")
        params.append(filename)
    if size_bytes is not None:
        updates.append("size_bytes = ?")
        params.append(size_bytes)
    if md5 is not None:
        updates.append("md5 = ?")
        params.append(md5)
    if sha256 is not None:
        updates.append("sha256 = ?")
        params.append(sha256)
    if content_type is not None:
        updates.append("content_type = ?")
        params.append(content_type)
    if response_code is not None:
        updates.append("response_code = ?")
        params.append(response_code)
    if error_message is not None:
        updates.append("error_message = ?")
        params.append(error_message)
    if duration_seconds is not None:
        updates.append("duration_seconds = ?")
        params.append(duration_seconds)
    if attempts is not None:
        updates.append("attempts = ?")
        params.append(attempts)

    # Set timestamp based on status
    now_utc = datetime.now(timezone.utc).isoformat()
    if status == "downloading":
        updates.append("started_at_utc = ?")
        params.append(now_utc)
    elif status in ("completed", "failed", "cancelled"):
        updates.append("completed_at_utc = ?")
        params.append(now_utc)

    params.extend([download_id, evidence_id])

    sql = f"UPDATE downloads SET {', '.join(updates)} WHERE id = ? AND evidence_id = ?"
    conn.execute(sql, params)


def update_download_image_metadata(
    conn: sqlite3.Connection,
    evidence_id: int,
    download_id: int,
    *,
    phash: Optional[str] = None,
    exif_json: Optional[str] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> None:
    """
    Update image-specific metadata for a download.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        download_id: Download record ID
        phash: Perceptual hash (16-char hex)
        exif_json: JSON-serialized EXIF data
        width: Image width in pixels
        height: Image height in pixels
    """
    updates = []
    params: List[Any] = []

    if phash is not None:
        updates.append("phash = ?")
        params.append(phash)
    if exif_json is not None:
        updates.append("exif_json = ?")
        params.append(exif_json)
    if width is not None:
        updates.append("width = ?")
        params.append(width)
    if height is not None:
        updates.append("height = ?")
        params.append(height)

    if not updates:
        return

    params.extend([download_id, evidence_id])
    sql = f"UPDATE downloads SET {', '.join(updates)} WHERE id = ? AND evidence_id = ?"
    conn.execute(sql, params)


def get_download(
    conn: sqlite3.Connection,
    evidence_id: int,
    download_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Get a single download by ID.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        download_id: Download record ID

    Returns:
        Download record as dict, or None if not found
    """
    row = conn.execute(
        "SELECT * FROM downloads WHERE id = ? AND evidence_id = ?",
        (download_id, evidence_id),
    ).fetchone()
    return dict(row) if row else None


def get_download_by_path(
    conn: sqlite3.Connection,
    evidence_id: int,
    dest_path: str,
) -> Optional[Dict[str, Any]]:
    """
    Get a download by destination path (for backfill dedup).

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        dest_path: Destination file path

    Returns:
        Download record as dict, or None if not found
    """
    row = conn.execute(
        "SELECT * FROM downloads WHERE evidence_id = ? AND dest_path = ?",
        (evidence_id, dest_path),
    ).fetchone()
    return dict(row) if row else None


def get_downloads(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    file_type: Optional[str] = None,
    status: Optional[str] = None,
    domain: Optional[str] = None,
    search_text: Optional[str] = None,
    limit: int = 500,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    List downloads with optional filters.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        file_type: Filter by file type ('image', 'video', etc.)
        status: Filter by status ('completed', 'failed', etc.)
        domain: Filter by domain (partial match)
        search_text: Search in filename or URL
        limit: Max results
        offset: Pagination offset

    Returns:
        List of download dicts
    """
    conditions = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]

    if file_type:
        conditions.append("file_type = ?")
        params.append(file_type)

    if status:
        conditions.append("status = ?")
        params.append(status)

    if domain:
        conditions.append("domain LIKE ?")
        params.append(f"%{domain}%")

    if search_text:
        conditions.append("(url LIKE ? OR filename LIKE ?)")
        params.extend([f"%{search_text}%", f"%{search_text}%"])

    where_clause = " AND ".join(conditions)

    sql = f"""
        SELECT
            id, evidence_id, url_id, url, domain, file_type, file_extension,
            status, dest_path, filename, size_bytes, md5, sha256, content_type,
            phash, exif_json, width, height,
            queued_at_utc, started_at_utc, completed_at_utc,
            response_code, error_message, attempts, duration_seconds, notes
        FROM downloads
        WHERE {where_clause}
        ORDER BY queued_at_utc DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])

    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def get_download_count(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    file_type: Optional[str] = None,
    status: Optional[str] = None,
) -> int:
    """
    Count downloads with optional filters.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        file_type: Filter by file type
        status: Filter by status

    Returns:
        Count of matching downloads
    """
    conditions = ["evidence_id = ?"]
    params: List[Any] = [evidence_id]

    if file_type:
        conditions.append("file_type = ?")
        params.append(file_type)

    if status:
        conditions.append("status = ?")
        params.append(status)

    where_clause = " AND ".join(conditions)

    row = conn.execute(
        f"SELECT COUNT(*) FROM downloads WHERE {where_clause}",
        params,
    ).fetchone()
    return row[0] if row else 0


def get_download_stats(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> Dict[str, int]:
    """
    Get download statistics for the evidence.

    Returns status counts and file type breakdown for completed downloads.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        Dict with status counts (e.g., {"completed": 10, "failed": 2, "completed_image": 5})
    """
    cursor = conn.execute(
        """
        SELECT status, COUNT(*) as count
        FROM downloads
        WHERE evidence_id = ?
        GROUP BY status
        """,
        (evidence_id,),
    )
    stats = {row["status"]: row["count"] for row in cursor.fetchall()}

    # Also get file type breakdown for completed downloads
    cursor = conn.execute(
        """
        SELECT file_type, COUNT(*) as count
        FROM downloads
        WHERE evidence_id = ? AND status = 'completed'
        GROUP BY file_type
        """,
        (evidence_id,),
    )
    for row in cursor.fetchall():
        stats[f"completed_{row['file_type']}"] = row["count"]

    return stats


def get_download_domains(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[str]:
    """
    List unique domains from downloads.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        Sorted list of unique domains
    """
    cursor = conn.execute(
        """
        SELECT DISTINCT domain FROM downloads
        WHERE evidence_id = ? AND domain IS NOT NULL AND domain != ''
        ORDER BY domain
        """,
        (evidence_id,),
    )
    return [row["domain"] for row in cursor.fetchall()]


def get_url_download_status(
    conn: sqlite3.Connection,
    evidence_id: int,
    url_id: int,
) -> Optional[str]:
    """
    Check if a URL has already been downloaded.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        url_id: URL record ID

    Returns:
        Download status string, or None if not downloaded
    """
    row = conn.execute(
        "SELECT status FROM downloads WHERE evidence_id = ? AND url_id = ?",
        (evidence_id, url_id),
    ).fetchone()
    return row["status"] if row else None


def find_url_by_filename_domain(
    conn: sqlite3.Connection,
    evidence_id: int,
    filename: str,
    domain: str,
) -> Optional[int]:
    """
    Try to find a URL ID by filename and domain (best-effort linkage for backfill).

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        filename: Filename to search for in URL path
        domain: Domain to match

    Returns:
        URL ID if found, None otherwise
    """
    row = conn.execute(
        """
        SELECT id FROM urls
        WHERE evidence_id = ? AND domain = ? AND url LIKE ?
        LIMIT 1
        """,
        (evidence_id, domain, f"%/{filename}"),
    ).fetchone()
    return row["id"] if row else None
