"""
URL database helper functions.

This module provides CRUD operations for the urls and url_groups tables.

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from ..schema import FilterOp, OrderColumn, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

__all__ = [
    "insert_url_row",
    "insert_urls",
    "get_urls",
    "get_url_by_id",
    "get_url_stats",
    "delete_urls_by_run",
    # URL groups
    "insert_url_groups",
    "get_url_groups",
    # Deduplication
    "analyze_url_duplicates",
    "deduplicate_urls",
]


def insert_url_row(
    conn: sqlite3.Connection,
    evidence_id: int,
    url: str,
    *,
    domain: Optional[str] = None,
    first_seen_utc: Optional[str] = None,
    last_seen_utc: Optional[str] = None,
    source_path: Optional[str] = None,
    discovered_by: Optional[str] = None,
    run_id: Optional[str] = None,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
    logical_path: Optional[str] = None,
    forensic_path: Optional[str] = None,
    tags: Optional[str] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Insert a single URL row with provenance.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        url: Discovered URL
        domain: Extracted domain
        first_seen_utc: First observation timestamp
        last_seen_utc: Last observation timestamp
        source_path: Original path in evidence
        discovered_by: Extractor signature
        run_id: Extraction run ID
        partition_index: E01 partition number
        fs_type: Filesystem type
        logical_path: Windows-style path
        forensic_path: Canonical E01 identifier
        tags: JSON-serialized tags
        notes: Investigator notes
    """
    record = {
        "url": url,
        "domain": domain,
        "first_seen_utc": first_seen_utc,
        "last_seen_utc": last_seen_utc,
        "source_path": source_path,
        "discovered_by": discovered_by,
        "run_id": run_id,
        "partition_index": partition_index,
        "fs_type": fs_type,
        "logical_path": logical_path,
        "forensic_path": forensic_path,
        "tags": tags,
        "notes": notes,
    }
    insert_row(conn, TABLE_SCHEMAS["urls"], evidence_id, record)


def insert_urls(conn: sqlite3.Connection, evidence_id: int, urls: Iterable[Dict[str, Any]], run_id: Optional[str] = None) -> int:
    """
    Insert multiple URLs in batch.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        urls: Iterable of URL records
        run_id: Optional run_id to inject into all records

    Returns:
        Number of URLs inserted
    """
    if run_id:
        urls = [{**u, "run_id": run_id} for u in urls]
    return insert_rows(conn, TABLE_SCHEMAS["urls"], evidence_id, urls)


def get_urls(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    domain: Optional[str] = None,
    url_filter: Optional[str] = None,
    discovered_by: Optional[str] = None,
    limit: int = 5000,
) -> List[Dict[str, Any]]:
    """
    Retrieve URLs for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        domain: Optional domain filter (exact match)
        url_filter: Optional URL substring filter
        discovered_by: Optional extractor filter
        limit: Maximum rows to return

    Returns:
        List of URL records as dicts
    """
    filters: Dict[str, Any] = {}
    if domain:
        filters["domain"] = (FilterOp.EQ, domain)
    if url_filter:
        filters["url"] = (FilterOp.LIKE, f"%{url_filter}%")
    if discovered_by:
        filters["discovered_by"] = (FilterOp.LIKE, f"%{discovered_by}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["urls"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def get_url_by_id(conn: sqlite3.Connection, url_id: int) -> Optional[Dict[str, Any]]:
    """
    Get a single URL by ID.

    Returns:
        URL record as dict, or None if not found
    """
    row = conn.execute("SELECT * FROM urls WHERE id = ?", (url_id,)).fetchone()
    return dict(row) if row else None


def get_url_stats(conn: sqlite3.Connection, evidence_id: int) -> Dict[str, Any]:
    """
    Get URL statistics for an evidence.

    Returns:
        Dict with total_count, unique_domains, by_extractor
    """
    total = conn.execute(
        "SELECT COUNT(*) FROM urls WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0]

    unique_domains = conn.execute(
        "SELECT COUNT(DISTINCT domain) FROM urls WHERE evidence_id = ?",
        (evidence_id,)
    ).fetchone()[0]

    by_extractor = {}
    for row in conn.execute(
        """
        SELECT discovered_by, COUNT(*) as count
        FROM urls
        WHERE evidence_id = ?
        GROUP BY discovered_by
        """,
        (evidence_id,)
    ):
        by_extractor[row["discovered_by"] or "unknown"] = row["count"]

    return {
        "total_count": total,
        "unique_domains": unique_domains,
        "by_extractor": by_extractor,
    }


def delete_urls_by_run(conn: sqlite3.Connection, evidence_id: int, run_id: str) -> int:
    """
    Delete URLs from a specific extraction run.

    Used for idempotent re-ingestion.

    Returns:
        Number of rows deleted
    """
    return delete_by_run(conn, TABLE_SCHEMAS["urls"], evidence_id, run_id)


# ============================================================================
# URL Groups
# ============================================================================

def insert_url_groups(conn: sqlite3.Connection, evidence_id: int, groups: Iterable[Dict[str, Any]], run_id: Optional[str] = None) -> int:
    """
    Insert URL group records (domain aggregations).

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        groups: Iterable of group records
        run_id: Optional run_id to inject into all records

    Returns:
        Number of groups inserted
    """
    if run_id:
        groups = [{**g, "run_id": run_id} for g in groups]
    return insert_rows(conn, TABLE_SCHEMAS["url_groups"], evidence_id, groups)


def get_url_groups(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    domain: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    Retrieve URL groups for an evidence.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        domain: Optional domain filter
        limit: Maximum rows to return

    Returns:
        List of URL group records as dicts
    """
    filters: Dict[str, Any] = {}
    if domain:
        filters["domain"] = (FilterOp.LIKE, f"%{domain}%")

    return get_rows(
        conn,
        TABLE_SCHEMAS["url_groups"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


# ============================================================================
# URL Deduplication
# ============================================================================

def _build_unique_key_columns(
    unique_by_first_seen: bool,
    unique_by_last_seen: bool,
    unique_by_source: bool,
) -> List[str]:
    """Build list of columns that define uniqueness."""
    columns = ["url"]  # URL is always required
    if unique_by_first_seen:
        columns.append("first_seen_utc")
    if unique_by_last_seen:
        columns.append("last_seen_utc")
    if unique_by_source:
        columns.append("discovered_by")
    return columns


def analyze_url_duplicates(
    conn: sqlite3.Connection,
    evidence_id: int,
    sources: List[str],
    *,
    unique_by_first_seen: bool = True,
    unique_by_last_seen: bool = True,
    unique_by_source: bool = False,
) -> Dict[str, Any]:
    """
    Analyze URL duplicates based on uniqueness constraints.

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        sources: List of discovered_by values to include
        unique_by_first_seen: Include first_seen_utc in uniqueness
        unique_by_last_seen: Include last_seen_utc in uniqueness
        unique_by_source: Include discovered_by in uniqueness

    Returns:
        Dict with:
            - total: Total URL count for selected sources
            - unique_count: Count after deduplication
            - duplicates: Number of duplicate rows to remove

    Initial implementation.
    """
    if not sources:
        return {"total": 0, "unique_count": 0, "duplicates": 0}

    # Build source filter
    placeholders = ", ".join("?" * len(sources))

    # Count total URLs for selected sources
    total_sql = f"""
        SELECT COUNT(*) FROM urls
        WHERE evidence_id = ? AND discovered_by IN ({placeholders})
    """
    total = conn.execute(total_sql, (evidence_id, *sources)).fetchone()[0]

    if total == 0:
        return {"total": 0, "unique_count": 0, "duplicates": 0}

    # Build uniqueness key columns
    key_columns = _build_unique_key_columns(
        unique_by_first_seen, unique_by_last_seen, unique_by_source
    )
    key_cols_sql = ", ".join(key_columns)

    # Count unique combinations
    unique_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT {key_cols_sql}
            FROM urls
            WHERE evidence_id = ? AND discovered_by IN ({placeholders})
            GROUP BY {key_cols_sql}
        )
    """
    unique_count = conn.execute(unique_sql, (evidence_id, *sources)).fetchone()[0]

    duplicates = total - unique_count

    return {
        "total": total,
        "unique_count": unique_count,
        "duplicates": duplicates,
    }


def deduplicate_urls(
    conn: sqlite3.Connection,
    evidence_id: int,
    sources: List[str],
    *,
    unique_by_first_seen: bool = True,
    unique_by_last_seen: bool = True,
    unique_by_source: bool = False,
    progress_callback: Optional[callable] = None,
) -> Dict[str, Any]:
    """
    Deduplicate URLs based on uniqueness constraints.

    Merges duplicate rows by:
    - Keeping earliest first_seen_utc (if not unique by it)
    - Keeping latest last_seen_utc (if not unique by it)
    - Concatenating unique source_path values (comma-separated)
    - Merging tags (union of all)
    - Keeping match_list if any duplicate matched

    Args:
        conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        sources: List of discovered_by values to include
        unique_by_first_seen: Include first_seen_utc in uniqueness
        unique_by_last_seen: Include last_seen_utc in uniqueness
        unique_by_source: Include discovered_by in uniqueness
        progress_callback: Optional callback(current, total) for progress

    Returns:
        Dict with:
            - total_before: URL count before deduplication
            - total_after: URL count after deduplication
            - duplicates_removed: Number of rows deleted
            - unique_urls_affected: Number of unique URL groups merged

    Initial implementation.
    """
    import json
    from datetime import datetime, timezone

    if not sources:
        return {
            "total_before": 0,
            "total_after": 0,
            "duplicates_removed": 0,
            "unique_urls_affected": 0,
        }

    # Build source filter
    placeholders = ", ".join("?" * len(sources))

    # Count total before
    total_before = conn.execute(
        f"SELECT COUNT(*) FROM urls WHERE evidence_id = ? AND discovered_by IN ({placeholders})",
        (evidence_id, *sources)
    ).fetchone()[0]

    if total_before == 0:
        return {
            "total_before": 0,
            "total_after": 0,
            "duplicates_removed": 0,
            "unique_urls_affected": 0,
        }

    # Build uniqueness key columns
    key_columns = _build_unique_key_columns(
        unique_by_first_seen, unique_by_last_seen, unique_by_source
    )
    key_cols_sql = ", ".join(key_columns)

    # Find all duplicate groups (groups with count > 1)
    # Returns the key columns and all IDs in the group
    groups_sql = f"""
        SELECT {key_cols_sql}, GROUP_CONCAT(id) as ids, COUNT(*) as cnt
        FROM urls
        WHERE evidence_id = ? AND discovered_by IN ({placeholders})
        GROUP BY {key_cols_sql}
        HAVING cnt > 1
    """

    groups = conn.execute(groups_sql, (evidence_id, *sources)).fetchall()
    total_groups = len(groups)

    if total_groups == 0:
        return {
            "total_before": total_before,
            "total_after": total_before,
            "duplicates_removed": 0,
            "unique_urls_affected": 0,
        }

    duplicates_removed = 0

    # SQLite variable limit (safe batch size, well under 999 limit)
    BATCH_SIZE = 500

    for i, group in enumerate(groups):
        if progress_callback:
            progress_callback(i + 1, total_groups)

        ids_str = group["ids"]
        ids = [int(x) for x in ids_str.split(",")]

        if len(ids) < 2:
            continue

        # Get all rows for this group (batched to avoid SQLite variable limit)
        rows = []
        for batch_start in range(0, len(ids), BATCH_SIZE):
            batch_ids = ids[batch_start:batch_start + BATCH_SIZE]
            id_placeholders = ", ".join("?" * len(batch_ids))
            batch_rows = conn.execute(
                f"SELECT * FROM urls WHERE id IN ({id_placeholders})",
                batch_ids
            ).fetchall()
            rows.extend([dict(r) for r in batch_rows])

        # Merge into first row (keep the one with lowest ID)
        rows.sort(key=lambda r: r["id"])
        keeper = rows[0]
        duplicates = rows[1:]

        # Merge source_path (comma-separated, unique values)
        all_paths = set()
        for r in rows:
            if r.get("source_path"):
                # Handle already comma-separated paths
                for p in r["source_path"].split(", "):
                    if p.strip():
                        all_paths.add(p.strip())
        merged_paths = ", ".join(sorted(all_paths)) if all_paths else None

        # Merge first_seen_utc (earliest) if not unique by it
        if not unique_by_first_seen:
            timestamps = [r.get("first_seen_utc") for r in rows if r.get("first_seen_utc")]
            if timestamps:
                keeper["first_seen_utc"] = min(timestamps)

        # Merge last_seen_utc (latest) if not unique by it
        if not unique_by_last_seen:
            timestamps = [r.get("last_seen_utc") for r in rows if r.get("last_seen_utc")]
            if timestamps:
                keeper["last_seen_utc"] = max(timestamps)

        # Merge tags (union)
        all_tags = set()
        for r in rows:
            if r.get("tags"):
                try:
                    tags = json.loads(r["tags"])
                    if isinstance(tags, list):
                        all_tags.update(tags)
                    elif isinstance(tags, str):
                        all_tags.add(tags)
                except (json.JSONDecodeError, TypeError):
                    # Plain string tag
                    if r["tags"]:
                        all_tags.add(r["tags"])
        merged_tags = json.dumps(sorted(all_tags)) if all_tags else None

        # Merge notes (concatenate unique)
        all_notes = set()
        for r in rows:
            if r.get("notes"):
                all_notes.add(r["notes"])
        merged_notes = "; ".join(sorted(all_notes)) if all_notes else None

        # Calculate total occurrence count (sum of all, with default 1 for rows without it)
        total_occurrences = sum(r.get("occurrence_count") or 1 for r in rows)

        # Update the keeper row
        conn.execute(
            """
            UPDATE urls SET
                source_path = ?,
                first_seen_utc = ?,
                last_seen_utc = ?,
                tags = ?,
                notes = ?,
                occurrence_count = ?
            WHERE id = ?
            """,
            (
                merged_paths,
                keeper.get("first_seen_utc"),
                keeper.get("last_seen_utc"),
                merged_tags,
                merged_notes,
                total_occurrences,
                keeper["id"],
            )
        )

        # Delete duplicate rows (batched to avoid SQLite variable limit)
        duplicate_ids = [r["id"] for r in duplicates]
        for batch_start in range(0, len(duplicate_ids), BATCH_SIZE):
            batch_ids = duplicate_ids[batch_start:batch_start + BATCH_SIZE]
            dup_placeholders = ", ".join("?" * len(batch_ids))
            conn.execute(
                f"DELETE FROM urls WHERE id IN ({dup_placeholders})",
                batch_ids
            )

        duplicates_removed += len(duplicate_ids)

    conn.commit()

    # Rebuild indexes for performance
    conn.execute("REINDEX idx_urls_evidence")
    conn.execute("REINDEX idx_urls_domain")
    conn.execute("REINDEX idx_urls_url")
    conn.execute("REINDEX idx_urls_evidence_domain")
    conn.execute("REINDEX idx_urls_evidence_source")
    conn.execute("REINDEX idx_urls_evidence_first_seen")
    conn.execute("REINDEX idx_urls_evidence_last_seen")
    # Reindex occurrence count if it exists
    try:
        conn.execute("REINDEX idx_urls_evidence_occurrence")
    except Exception:
        pass  # Index may not exist in older databases

    # Count total after
    total_after = conn.execute(
        f"SELECT COUNT(*) FROM urls WHERE evidence_id = ? AND discovered_by IN ({placeholders})",
        (evidence_id, *sources)
    ).fetchone()[0]

    return {
        "total_before": total_before,
        "total_after": total_after,
        "duplicates_removed": duplicates_removed,
        "unique_urls_affected": total_groups,
    }
