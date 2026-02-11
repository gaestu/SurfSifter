"""
Extracted files database helper functions.

This module provides CRUD operations for the extracted_files audit table.
This table tracks ALL files extracted by ANY extractor with forensic provenance.

Initial implementation as part of universal extraction audit feature.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from core.enums import ExtractionStatus
from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_count, get_rows, insert_row, insert_rows

__all__ = [
    # Insert functions
    "insert_extracted_file",
    "insert_extracted_files",
    "insert_extracted_files_batch",
    # Query functions
    "get_extracted_files",
    "get_extracted_file_by_id",
    "get_extracted_file_by_sha256",
    "get_extraction_stats",
    "get_distinct_extractors",
    "get_distinct_run_ids",
    # Delete functions
    "delete_extracted_files_by_run",
    "delete_extracted_files_by_extractor",
]


def insert_extracted_file(
    conn: sqlite3.Connection,
    evidence_id: int,
    extractor_name: str,
    run_id: str,
    dest_rel_path: str,
    dest_filename: str,
    *,
    source_path: Optional[str] = None,
    source_inode: Optional[str] = None,
    partition_index: Optional[int] = None,
    source_offset_bytes: Optional[int] = None,
    source_block_size: Optional[int] = None,
    size_bytes: Optional[int] = None,
    file_type: Optional[str] = None,
    mime_type: Optional[str] = None,
    md5: Optional[str] = None,
    sha256: Optional[str] = None,
    status: str = ExtractionStatus.OK,
    error_message: Optional[str] = None,
    extractor_version: Optional[str] = None,
    metadata_json: Optional[str] = None,
) -> int:
    """
    Insert a single extracted file record.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        extractor_name: Name of the extractor (e.g., "filesystem_images", "cache_firefox")
        run_id: Unique run ID for this extraction invocation
        dest_rel_path: Relative path from extractor output dir
        dest_filename: Just the filename
        source_path: Original path in evidence
        source_inode: TSK inode string (e.g., "292163-128-4")
        partition_index: Partition number (0-based)
        source_offset_bytes: Byte offset in raw evidence (for carving)
        source_block_size: Block size used for offset calculation
        size_bytes: File size in bytes
        file_type: Detected type (JPEG, PNG, SQLite, etc.)
        mime_type: MIME type if known
        md5: MD5 hash
        sha256: SHA256 hash
        status: Extraction status (ok, partial, error, skipped)
        error_message: Error details if status != ok
        extractor_version: Version of the extractor
        metadata_json: JSON blob for extractor-specific metadata

    Returns:
        Row ID of inserted record
    """
    record = {
        "extractor_name": extractor_name,
        "extractor_version": extractor_version,
        "run_id": run_id,
        "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_path": source_path,
        "source_inode": source_inode,
        "partition_index": partition_index,
        "source_offset_bytes": source_offset_bytes,
        "source_block_size": source_block_size,
        "dest_rel_path": dest_rel_path,
        "dest_filename": dest_filename,
        "size_bytes": size_bytes,
        "file_type": file_type,
        "mime_type": mime_type,
        "md5": md5,
        "sha256": sha256,
        "status": status,
        "error_message": error_message,
        "metadata_json": metadata_json,
    }
    insert_row(conn, TABLE_SCHEMAS["extracted_files"], evidence_id, record)
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def insert_extracted_files(
    conn: sqlite3.Connection,
    evidence_id: int,
    records: Iterable[Dict[str, Any]],
) -> int:
    """
    Insert multiple extracted file records in batch.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        records: Iterable of record dicts (must have extractor_name, run_id, dest_rel_path, dest_filename)

    Returns:
        Number of records inserted
    """
    return insert_rows(conn, TABLE_SCHEMAS["extracted_files"], evidence_id, records)


def insert_extracted_files_batch(
    conn: sqlite3.Connection,
    evidence_id: int,
    extractor_name: str,
    run_id: str,
    files: List[Dict[str, Any]],
    extractor_version: Optional[str] = None,
) -> int:
    """
    Batch insert for performance - adds common fields to each record.

    This is the preferred method for extractors to use, as it:
    1. Adds extractor_name, run_id, extractor_version to each record
    2. Sets extracted_at_utc timestamp
    3. Uses executemany for efficient bulk insert

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        extractor_name: Name of the extractor
        run_id: Unique run ID for this extraction
        files: List of file records (each must have dest_rel_path, dest_filename)
        extractor_version: Version of the extractor

    Returns:
        Number of records inserted

    Example:
        files = [
            {"dest_rel_path": "extracted/img1.jpg", "dest_filename": "img1.jpg",
             "source_path": "Users/John/Pictures/img1.jpg", "sha256": "abc123..."},
            {"dest_rel_path": "extracted/img2.png", "dest_filename": "img2.png",
             "source_path": "Users/John/Pictures/img2.png", "sha256": "def456..."},
        ]
        count = insert_extracted_files_batch(conn, evidence_id, "filesystem_images", run_id, files)
    """
    if not files:
        return 0

    now = datetime.now(timezone.utc).isoformat()

    # Prepare records with common fields
    records = []
    for f in files:
        record = {
            "extractor_name": extractor_name,
            "extractor_version": extractor_version,
            "run_id": run_id,
            "extracted_at_utc": now,
            "source_path": f.get("source_path"),
            "source_inode": f.get("source_inode") or f.get("inode"),  # Support both key names
            "partition_index": f.get("partition_index"),
            "source_offset_bytes": f.get("source_offset_bytes"),
            "source_block_size": f.get("source_block_size"),
            "dest_rel_path": f["dest_rel_path"],
            "dest_filename": f["dest_filename"],
            "size_bytes": f.get("size_bytes"),
            "file_type": f.get("file_type") or f.get("detected_type"),  # Support both
            "mime_type": f.get("mime_type"),
            "md5": f.get("md5"),
            "sha256": f.get("sha256"),
            "status": f.get("status", ExtractionStatus.OK),
            "error_message": f.get("error_message"),
            "metadata_json": f.get("metadata_json"),
        }
        records.append(record)

    return insert_rows(conn, TABLE_SCHEMAS["extracted_files"], evidence_id, records)


def get_extracted_files(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    extractor_name: Optional[str] = None,
    run_id: Optional[str] = None,
    status: Optional[str] = None,
    file_type: Optional[str] = None,
    partition_index: Optional[int] = None,
    limit: int = 1000,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Query extracted files with filters.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        extractor_name: Filter by extractor name
        run_id: Filter by run ID
        status: Filter by status (ok, partial, error, skipped)
        file_type: Filter by detected file type
        partition_index: Filter by partition
        limit: Maximum rows to return
        offset: Offset for pagination

    Returns:
        List of extracted file records as dicts
    """
    filters: Dict[str, Any] = {}
    if extractor_name:
        filters["extractor_name"] = (FilterOp.EQ, extractor_name)
    if run_id:
        filters["run_id"] = (FilterOp.EQ, run_id)
    if status:
        filters["status"] = (FilterOp.EQ, status)
    if file_type:
        filters["file_type"] = (FilterOp.EQ, file_type)
    if partition_index is not None:
        filters["partition_index"] = (FilterOp.EQ, partition_index)

    return get_rows(
        conn,
        TABLE_SCHEMAS["extracted_files"],
        evidence_id,
        filters=filters if filters else None,
        limit=limit,
        offset=offset,
    )


def get_extracted_file_by_id(
    conn: sqlite3.Connection,
    evidence_id: int,
    file_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Get a single extracted file record by ID.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        file_id: Record ID

    Returns:
        Extracted file record as dict, or None if not found
    """
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM extracted_files WHERE evidence_id = ? AND id = ?",
        (evidence_id, file_id)
    ).fetchone()
    return dict(row) if row else None


def get_extracted_file_by_sha256(
    conn: sqlite3.Connection,
    evidence_id: int,
    sha256: str,
) -> Optional[Dict[str, Any]]:
    """
    Find first extracted file by SHA256 hash.

    Note: Multiple records may exist for the same SHA256 (different runs).
    This returns the most recent one (highest ID).

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        sha256: SHA256 hash to search for

    Returns:
        Most recent extracted file record with matching hash, or None
    """
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT * FROM extracted_files
        WHERE evidence_id = ? AND sha256 = ?
        ORDER BY id DESC LIMIT 1
        """,
        (evidence_id, sha256)
    ).fetchone()
    return dict(row) if row else None


def get_extraction_stats(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get extraction statistics for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        run_id: Optional filter by specific run

    Returns:
        Dict with statistics:
        - total_count: Total number of extracted files
        - total_size_bytes: Sum of all file sizes
        - by_extractor: {extractor_name: count}
        - by_status: {status: count}
        - by_file_type: {file_type: count}
        - error_count: Number of files with status != ok
    """
    run_filter = "AND run_id = ?" if run_id else ""
    params: List[Any] = [evidence_id]
    if run_id:
        params.append(run_id)

    # Total count
    total = conn.execute(
        f"SELECT COUNT(*) FROM extracted_files WHERE evidence_id = ? {run_filter}",
        params
    ).fetchone()[0]

    # Total size
    total_size = conn.execute(
        f"SELECT COALESCE(SUM(size_bytes), 0) FROM extracted_files WHERE evidence_id = ? {run_filter}",
        params
    ).fetchone()[0]

    # By extractor
    by_extractor = {}
    for row in conn.execute(
        f"""
        SELECT extractor_name, COUNT(*) as count
        FROM extracted_files
        WHERE evidence_id = ? {run_filter}
        GROUP BY extractor_name
        """,
        params
    ):
        by_extractor[row[0]] = row[1]

    # By status
    by_status = {}
    for row in conn.execute(
        f"""
        SELECT status, COUNT(*) as count
        FROM extracted_files
        WHERE evidence_id = ? {run_filter}
        GROUP BY status
        """,
        params
    ):
        by_status[row[0]] = row[1]

    # By file type (top 20)
    by_file_type = {}
    for row in conn.execute(
        f"""
        SELECT file_type, COUNT(*) as count
        FROM extracted_files
        WHERE evidence_id = ? AND file_type IS NOT NULL {run_filter}
        GROUP BY file_type
        ORDER BY count DESC
        LIMIT 20
        """,
        params
    ):
        by_file_type[row[0]] = row[1]

    # Error count
    error_params = [evidence_id, ExtractionStatus.OK]
    if run_id:
        error_params.append(run_id)
    error_count = conn.execute(
        f"""
        SELECT COUNT(*) FROM extracted_files
        WHERE evidence_id = ? AND status != ? {run_filter}
        """,
        error_params
    ).fetchone()[0]

    return {
        "total_count": total,
        "total_size_bytes": total_size,
        "by_extractor": by_extractor,
        "by_status": by_status,
        "by_file_type": by_file_type,
        "error_count": error_count,
    }


def get_distinct_extractors(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[str]:
    """
    Get list of distinct extractor names that have records.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID

    Returns:
        List of extractor names
    """
    rows = conn.execute(
        """
        SELECT DISTINCT extractor_name
        FROM extracted_files
        WHERE evidence_id = ?
        ORDER BY extractor_name
        """,
        (evidence_id,)
    ).fetchall()
    return [row[0] for row in rows]


def get_distinct_run_ids(
    conn: sqlite3.Connection,
    evidence_id: int,
    extractor_name: Optional[str] = None,
) -> List[str]:
    """
    Get all run_ids for evidence, optionally filtered by extractor.

    Useful for UI dropdowns and batch cleanup operations.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        extractor_name: Optional filter by extractor

    Returns:
        List of run_ids, most recent first
    """
    if extractor_name:
        rows = conn.execute(
            """
            SELECT DISTINCT run_id
            FROM extracted_files
            WHERE evidence_id = ? AND extractor_name = ?
            ORDER BY run_id DESC
            """,
            (evidence_id, extractor_name)
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT DISTINCT run_id
            FROM extracted_files
            WHERE evidence_id = ?
            ORDER BY run_id DESC
            """,
            (evidence_id,)
        ).fetchall()
    return [row[0] for row in rows]


def delete_extracted_files_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """
    Delete records for a specific run (for re-extraction).

    Note: run_id is per-extractor-invocation, so this deletes
    only one extractor's output from one run.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        run_id: Run ID to delete

    Returns:
        Number of records deleted
    """
    return delete_by_run(conn, TABLE_SCHEMAS["extracted_files"], evidence_id, run_id)


def delete_extracted_files_by_extractor(
    conn: sqlite3.Connection,
    evidence_id: int,
    extractor_name: str,
) -> int:
    """
    Delete ALL records for an extractor (all runs).

    Use before complete re-extraction of an artifact type.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        extractor_name: Extractor name to delete

    Returns:
        Number of records deleted
    """
    cursor = conn.execute(
        "DELETE FROM extracted_files WHERE evidence_id = ? AND extractor_name = ?",
        (evidence_id, extractor_name)
    )
    conn.commit()
    return cursor.rowcount
