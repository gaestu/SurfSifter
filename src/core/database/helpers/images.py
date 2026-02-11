"""
Image database helper functions.

This module provides CRUD operations for the images and image_discoveries tables.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, OrderColumn, TABLE_SCHEMAS
from .generic import delete_by_run, get_count, get_rows, insert_row, insert_rows

__all__ = [
    "insert_image",
    "insert_images",
    "get_images",
    "get_image_by_id",
    "get_image_by_md5",
    "get_image_by_sha256",
    "get_image_stats",
    "delete_images_by_run",
    "update_image_tags",
    "update_image_notes",
    # Image discoveries
    "insert_image_discovery",
    "get_image_discoveries",
    "insert_image_with_discovery",
    "get_image_sources",
    "get_image_fs_path",
    "delete_discoveries_by_run",
]


def insert_image(
    conn: sqlite3.Connection,
    evidence_id: int,
    md5: str,
    *,
    sha256: Optional[str] = None,
    phash: Optional[str] = None,
    file_type: Optional[str] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    size_bytes: Optional[int] = None,
    exif_json: Optional[str] = None,
    thumbnail_path: Optional[str] = None,
    source_path: Optional[str] = None,
    discovered_by: Optional[str] = None,
    run_id: Optional[str] = None,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    extracted_path: Optional[str] = None,
    tags: Optional[str] = None,
    notes: Optional[str] = None,
) -> int:
    """
    Insert an image record with hash-based deduplication.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        md5: MD5 hash (required)
        sha256: SHA256 hash
        phash: Perceptual hash (16-char hex)
        file_type: Image format (jpeg, png, gif, etc.)
        width: Image width in pixels
        height: Image height in pixels
        size_bytes: File size in bytes
        exif_json: JSON-serialized EXIF data
        thumbnail_path: Path to generated thumbnail
        source_path: Original path in evidence
        discovered_by: Extractor name
        run_id: Extraction run ID
        partition_index: E01 partition number
        fs_type: Filesystem type
        logical_path: Windows-style path
        forensic_path: Canonical E01 identifier
        extracted_path: Path to extracted file in workspace
        tags: JSON-serialized tags
        notes: Investigator notes

    Returns:
        Image row ID (new or existing)
    """
    record = {
        "md5": md5,
        "sha256": sha256,
        "phash": phash,
        "file_type": file_type,
        "width": width,
        "height": height,
        "size_bytes": size_bytes,
        "exif_json": exif_json,
        "thumbnail_path": thumbnail_path,
        "source_path": source_path,
        "discovered_by": discovered_by,
        "run_id": run_id,
        "partition_index": partition_index,
        "fs_type": fs_type,
        "logical_path": logical_path,
        "forensic_path": forensic_path,
        "extracted_path": extracted_path,
        "tags": tags,
        "notes": notes,
    }
    insert_row(conn, TABLE_SCHEMAS["images"], evidence_id, record)

    # Return the ID of the inserted/existing row
    row = conn.execute(
        "SELECT id FROM images WHERE evidence_id = ? AND md5 = ?",
        (evidence_id, md5)
    ).fetchone()
    return row[0] if row else -1


def insert_images(conn: sqlite3.Connection, evidence_id: int, records: Iterable[Dict[str, Any]]) -> int:
    """
    Insert multiple images in batch.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        records: Iterable of image records (must have 'md5' key)

    Returns:
        Number of images inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["images"], evidence_id, records)


def get_images(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    file_type: Optional[str] = None,
    min_size: Optional[int] = None,
    max_size: Optional[int] = None,
    phash_prefix: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve images for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        file_type: Optional file type filter
        min_size: Minimum file size in bytes
        max_size: Maximum file size in bytes
        phash_prefix: pHash prefix for similarity search (SQL prefix filter)
        limit: Maximum rows to return

    Returns:
        List of image records as dicts
    """
    filters: Dict[str, Any] = {}
    if file_type:
        filters["file_type"] = (FilterOp.EQ, file_type)
    if min_size is not None:
        filters["size_bytes_min"] = (FilterOp.GTE, min_size)
    if max_size is not None:
        filters["size_bytes_max"] = (FilterOp.LTE, max_size)
    if phash_prefix:
        filters["phash"] = (FilterOp.LIKE, f"{phash_prefix}%")

    # Custom query for size range filters
    if min_size is not None or max_size is not None:
        # Build manual query for range filters
        conditions = ["evidence_id = ?"]
        params: List[Any] = [evidence_id]

        if file_type:
            conditions.append("file_type = ?")
            params.append(file_type)
        if min_size is not None:
            conditions.append("size_bytes >= ?")
            params.append(min_size)
        if max_size is not None:
            conditions.append("size_bytes <= ?")
            params.append(max_size)
        if phash_prefix:
            conditions.append("phash LIKE ?")
            params.append(f"{phash_prefix}%")

        params.append(limit)
        rows = conn.execute(
            f"SELECT * FROM images WHERE {' AND '.join(conditions)} LIMIT ?",
            params
        ).fetchall()
        return [dict(row) for row in rows]

    return get_rows(
        conn,
        TABLE_SCHEMAS["images"],
        evidence_id,
        filters=filters if filters else None,
        limit=limit,
    )


def get_image_by_id(conn: sqlite3.Connection, image_id: int) -> Optional[Dict[str, Any]]:
    """
    Get a single image by ID.

    Returns:
        Image record as dict, or None if not found
    """
    row = conn.execute("SELECT * FROM images WHERE id = ?", (image_id,)).fetchone()
    return dict(row) if row else None


def get_image_by_md5(conn: sqlite3.Connection, evidence_id: int, md5: str) -> Optional[Dict[str, Any]]:
    """
    Get image by MD5 hash.

    Returns:
        Image record as dict, or None if not found
    """
    row = conn.execute(
        "SELECT * FROM images WHERE evidence_id = ? AND md5 = ?",
        (evidence_id, md5)
    ).fetchone()
    return dict(row) if row else None


def get_image_by_sha256(conn: sqlite3.Connection, evidence_id: int, sha256: str) -> Optional[Dict[str, Any]]:
    """
    Get image by SHA256 hash.

    Returns:
        Image record as dict, or None if not found
    """
    row = conn.execute(
        "SELECT * FROM images WHERE evidence_id = ? AND sha256 = ?",
        (evidence_id, sha256)
    ).fetchone()
    return dict(row) if row else None


def get_image_stats(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, Any]:
    """
    Get image statistics for an evidence.

    Returns:
        Dict with total_count, by_type, total_size, unique_phash
    """
    total = conn.execute(
        "SELECT COUNT(*) FROM images WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0]

    by_type = {}
    for row in conn.execute(
        """
        SELECT file_type, COUNT(*) as count
        FROM images
        WHERE evidence_id = ?
        GROUP BY file_type
        """,
        (evidence_id,)
    ):
        by_type[row["file_type"] or "unknown"] = row["count"]

    total_size = conn.execute(
        "SELECT SUM(size_bytes) FROM images WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0] or 0

    unique_phash = conn.execute(
        "SELECT COUNT(DISTINCT phash) FROM images WHERE evidence_id = ? AND phash IS NOT NULL",
        (evidence_id,)
    ).fetchone()[0]

    return {
        "total_count": total,
        "by_type": by_type,
        "total_size": total_size,
        "unique_phash": unique_phash,
    }


def delete_images_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """
    Delete images from a specific extraction run.

    Used for idempotent re-ingestion.

    Returns:
        Number of rows deleted
    """
    return delete_by_run(conn, TABLE_SCHEMAS["images"], evidence_id, run_id)


def update_image_tags(conn: sqlite3.Connection, image_id: int, tags: str) -> None:
    """
    Update tags for an image.

    Args:
        conn: SQLite connection to evidence database
        image_id: Image row ID
        tags: JSON-serialized tags
    """
    conn.execute("UPDATE images SET tags = ? WHERE id = ?", (tags, image_id))
    conn.commit()


def update_image_notes(conn: sqlite3.Connection, image_id: int, notes: str) -> None:
    """
    Update notes for an image.

    Args:
        conn: SQLite connection to evidence database
        image_id: Image row ID
        notes: Investigator notes
    """
    conn.execute("UPDATE images SET notes = ? WHERE id = ?", (notes, image_id))
    conn.commit()


# ============================================================================
# Image Discoveries
# ============================================================================

def insert_image_discovery(
    conn: sqlite3.Connection,
    evidence_id: int,
    image_id: int,
    discovered_by: str,
    run_id: str,
    *,
    extractor_version: Optional[str] = None,
    fs_path: Optional[str] = None,
    fs_mtime_epoch: Optional[float] = None,
    fs_crtime_epoch: Optional[float] = None,
    fs_atime_epoch: Optional[float] = None,
    fs_ctime_epoch: Optional[float] = None,
    fs_inode: Optional[int] = None,
    carved_offset_bytes: Optional[int] = None,
    carved_block_size: Optional[int] = None,
    carved_tool_output: Optional[str] = None,
    # Cache context
    cache_url: Optional[str] = None,
    cache_key: Optional[str] = None,
    cache_filename: Optional[str] = None,
    cache_response_time: Optional[str] = None,
    source_metadata_json: Optional[str | dict] = None,
    # Legacy parameters (deprecated, kept for backward compat; target removal )
    source_path: Optional[str] = None,
    offset: Optional[int] = None,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    mtime: Optional[str] = None,
    atime: Optional[str] = None,
    ctime: Optional[str] = None,
    crtime: Optional[str] = None,
) -> bool:
    """
    Insert discovery record for an image.

    Uses INSERT OR IGNORE to handle duplicate paths/offsets per D2.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        image_id: Image ID (FK to images.id)
        discovered_by: Extractor name (e.g., "filesystem_images", "foremost_carver")
        run_id: Extraction run identifier
        extractor_version: Optional extractor version string
        fs_path: Original filesystem path (for filesystem_images)
        fs_mtime_epoch: Modification time (Unix epoch)
        fs_crtime_epoch: Creation time (Unix epoch)
        fs_atime_epoch: Access time (Unix epoch)
        fs_ctime_epoch: Metadata change time (Unix epoch)
        fs_inode: Inode/MFT entry number
        carved_offset_bytes: Byte offset in evidence (for carvers)
        carved_block_size: Block size used for offset
        carved_tool_output: Path in carver output directory
        cache_url: Original URL from cache metadata
        cache_key: Cache entry key/hash
        cache_filename: Cache entry filename
        cache_response_time: Cache timestamp (ISO8601)
        source_metadata_json: JSON string or dict for browser-specific context

    Returns:
        True if inserted, False if duplicate (ignored)
    """
    import json

    # Handle JSON serialization for source_metadata_json
    if source_metadata_json is not None and isinstance(source_metadata_json, dict):
        source_metadata_json = json.dumps(source_metadata_json)

    record = {
        "image_id": image_id,
        "discovered_by": discovered_by,
        "run_id": run_id,
        "extractor_version": extractor_version,
        "fs_path": fs_path or source_path,  # Legacy compat
        "fs_mtime_epoch": fs_mtime_epoch,
        "fs_crtime_epoch": fs_crtime_epoch,
        "fs_atime_epoch": fs_atime_epoch,
        "fs_ctime_epoch": fs_ctime_epoch,
        "fs_inode": fs_inode,
        "carved_offset_bytes": carved_offset_bytes or offset,  # Legacy compat
        "carved_block_size": carved_block_size,
        "carved_tool_output": carved_tool_output,
        "cache_url": cache_url,
        "cache_key": cache_key,
        "cache_filename": cache_filename,
        "cache_response_time": cache_response_time,
        "source_metadata_json": source_metadata_json,
    }

    # Build column list dynamically (only include non-None values)
    columns = ["evidence_id"] + [k for k, v in record.items() if v is not None]
    values = [evidence_id] + [v for v in record.values() if v is not None]

    placeholders = ", ".join("?" * len(columns))
    column_names = ", ".join(columns)

    try:
        conn.execute(
            f"INSERT OR IGNORE INTO image_discoveries ({column_names}) VALUES ({placeholders})",
            values,
        )
        return True
    except Exception:
        return False


def get_image_discoveries(
    conn: sqlite3.Connection,
    evidence_id: int,
    image_id: int,
) -> List[Dict[str, Any]]:
    """
    Get all discovery records for an image.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        image_id: Image ID

    Returns:
        List of discovery records showing where/how the image was found
    """
    rows = conn.execute(
        """
        SELECT * FROM image_discoveries
        WHERE evidence_id = ? AND image_id = ?
        ORDER BY discovered_by, run_id
        """,
        (evidence_id, image_id),
    ).fetchall()
    return [dict(row) for row in rows]


def get_image_sources(
    conn: sqlite3.Connection,
    evidence_id: int,
    image_id: int,
) -> List[str]:
    """
    Get list of sources that discovered this image within an evidence scope.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        image_id: Image ID

    Returns:
        List of distinct discovered_by values
    """
    cursor = conn.execute(
        "SELECT DISTINCT discovered_by FROM image_discoveries WHERE evidence_id = ? AND image_id = ?",
        (evidence_id, image_id)
    )
    return [row[0] for row in cursor.fetchall()]


def get_image_fs_path(
    conn: sqlite3.Connection,
    evidence_id: int,
    image_id: int,
) -> Optional[str]:
    """
    Get filesystem path for image (if discovered by FS extractor).

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        image_id: Image ID

    Returns:
        Original filesystem path or None if only carved
    """
    cursor = conn.execute(
        """
        SELECT fs_path FROM image_discoveries
        WHERE evidence_id = ? AND image_id = ? AND fs_path IS NOT NULL
        LIMIT 1
        """,
        (evidence_id, image_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def delete_discoveries_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """Delete image discoveries from a specific run."""
    cursor = conn.execute(
        "DELETE FROM image_discoveries WHERE evidence_id = ? AND run_id = ?",
        (evidence_id, run_id),
    )
    return cursor.rowcount


# =============================================================================
# Helper functions for image discovery
# =============================================================================

def _utc_now() -> str:
    """Return current UTC time as ISO 8601 string (without microseconds)."""
    from datetime import datetime, timezone
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def insert_image_with_discovery(
    conn: sqlite3.Connection,
    evidence_id: int,
    image_data: Dict[str, Any],
    discovery_data: Dict[str, Any],
) -> tuple:
    """
    Insert or find image, then add discovery record.

    Uses existing insert_images() helper which handles INSERT OR IGNORE.
    Populates first_discovered_by/first_discovered_at on new images.

    NOTE: source_count is computed on demand via v_image_sources view,
    not stored or maintained here.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        image_data: Image record dict (must include sha256)
        discovery_data: Discovery record dict (must include discovered_by, run_id)
            Optional discovery_data keys:
            - extractor_version: Extractor version string
            - fs_path, fs_mtime_epoch, fs_crtime_epoch, fs_atime_epoch, fs_ctime_epoch, fs_inode (filesystem)
            - carved_offset_bytes, carved_block_size, carved_tool_output (carving)
            - cache_url, cache_key, cache_filename, cache_response_time, source_metadata_json (cache)

    Returns:
        (image_id, was_inserted) - was_inserted=False means enriched existing

    Raises:
        ValueError: If required fields are missing
    """
    sha256 = image_data.get("sha256")
    if not sha256:
        raise ValueError("image_data must include sha256 for deduplication")

    discovered_by = discovery_data.get("discovered_by")
    run_id = discovery_data.get("run_id")
    if not discovered_by or not run_id:
        raise ValueError("discovery_data must include discovered_by and run_id")

    # 1. Check if SHA256 exists
    existing = get_image_by_sha256(conn, evidence_id, sha256)

    if existing:
        # 2a. Image exists → add discovery (enrichment)
        image_id = existing["id"]
        was_inserted = False
    else:
        # 2b. Image new → insert image record with first_discovered_* columns
        now = _utc_now()
        image_record = dict(image_data)
        image_record["first_discovered_by"] = discovered_by
        image_record["first_discovered_at"] = now
        # NOTE: Do NOT set discovered_by/run_id/cache_key - those columns are removed
        # Remove any legacy keys that may have been passed
        image_record.pop("discovered_by", None)
        image_record.pop("run_id", None)
        image_record.pop("cache_key", None)

        # Insert using existing helper
        inserted_count = insert_images(conn, evidence_id, [image_record])

        if inserted_count == 0:
            # Race condition: another thread inserted while we checked
            # Re-fetch and treat as enrichment
            existing = get_image_by_sha256(conn, evidence_id, sha256)
            if existing:
                image_id = existing["id"]
                was_inserted = False
            else:
                raise RuntimeError(f"Failed to insert image with sha256={sha256}")
        else:
            # Get the inserted image ID
            cursor = conn.execute(
                "SELECT id FROM images WHERE evidence_id = ? AND sha256 = ?",
                (evidence_id, sha256)
            )
            row = cursor.fetchone()
            if not row:
                raise RuntimeError(f"Failed to retrieve inserted image with sha256={sha256}")
            image_id = row[0]
            was_inserted = True

    # 3. Always add discovery record (UNIQUE constraint handles duplicates)
    insert_image_discovery(
        conn, evidence_id, image_id,
        discovered_by=discovered_by,
        run_id=run_id,
        extractor_version=discovery_data.get("extractor_version"),
        # Filesystem context
        fs_path=discovery_data.get("fs_path"),
        fs_mtime_epoch=discovery_data.get("fs_mtime_epoch"),
        fs_crtime_epoch=discovery_data.get("fs_crtime_epoch"),
        fs_atime_epoch=discovery_data.get("fs_atime_epoch"),
        fs_ctime_epoch=discovery_data.get("fs_ctime_epoch"),
        fs_inode=discovery_data.get("fs_inode"),
        # Carving context
        carved_offset_bytes=discovery_data.get("carved_offset_bytes"),
        carved_block_size=discovery_data.get("carved_block_size"),
        carved_tool_output=discovery_data.get("carved_tool_output"),
        # Cache context
        cache_url=discovery_data.get("cache_url"),
        cache_key=discovery_data.get("cache_key"),
        cache_filename=discovery_data.get("cache_filename"),
        cache_response_time=discovery_data.get("cache_response_time"),
        source_metadata_json=discovery_data.get("source_metadata_json"),
    )

    return image_id, was_inserted


def get_image_sources(
    conn: sqlite3.Connection,
    evidence_id: int,
    image_id: int,
) -> List[str]:
    """
    Get list of sources that discovered this image within an evidence scope.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        image_id: Image ID

    Returns:
        List of distinct discovered_by values
    """
    cursor = conn.execute(
        "SELECT DISTINCT discovered_by FROM image_discoveries WHERE evidence_id = ? AND image_id = ?",
        (evidence_id, image_id)
    )
    return [row[0] for row in cursor.fetchall()]


def get_image_fs_path(
    conn: sqlite3.Connection,
    evidence_id: int,
    image_id: int,
) -> Optional[str]:
    """
    Get filesystem path for image (if discovered by FS extractor).

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        image_id: Image ID

    Returns:
        Original filesystem path or None if only carved
    """
    cursor = conn.execute(
        """
        SELECT fs_path FROM image_discoveries
        WHERE evidence_id = ? AND image_id = ? AND fs_path IS NOT NULL
        LIMIT 1
        """,
        (evidence_id, image_id)
    )
    row = cursor.fetchone()
    return row[0] if row else None


def delete_discoveries_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """
    Delete discovery records for a specific run (cleanup before re-ingest).

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        run_id: Run ID to delete

    Returns:
        Number of rows deleted
    """
    cursor = conn.execute(
        "DELETE FROM image_discoveries WHERE evidence_id = ? AND run_id = ?",
        (evidence_id, run_id)
    )
    return cursor.rowcount
