"""
Tag database helpers.

Provides stateless CRUD operations for tags and tag associations.
Tags are stored in the `tags` table with associations in `tag_associations`.

Created as part of CaseDataAccess refactor (Block 8).
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Sequence


# -----------------------------------------------------------------------------
# Tag CRUD Operations
# -----------------------------------------------------------------------------

def get_all_tags(
    conn: sqlite3.Connection,
    evidence_id: int,
) -> List[Dict[str, Any]]:
    """
    Get all tags for an evidence with usage counts.

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID

    Returns:
        List of tag dicts with keys: id, name, name_normalized, usage_count,
        created_by, created_at_utc
    """
    sql = """
        SELECT id, name, name_normalized, usage_count, created_by, created_at_utc
        FROM tags
        WHERE evidence_id = ?
        ORDER BY usage_count DESC, name ASC
    """
    cursor = conn.execute(sql, (evidence_id,))
    return [dict(row) for row in cursor.fetchall()]


def get_tag_by_name(
    conn: sqlite3.Connection,
    evidence_id: int,
    name: str,
) -> Optional[Dict[str, Any]]:
    """
    Get a tag by name (case-insensitive).

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        name: Tag name (case-insensitive lookup)

    Returns:
        Tag dict or None if not found
    """
    sql = """
        SELECT id, name, name_normalized, usage_count, created_by, created_at_utc
        FROM tags
        WHERE evidence_id = ? AND name_normalized = ?
    """
    row = conn.execute(sql, (evidence_id, name.lower())).fetchone()
    return dict(row) if row else None


def get_tag_by_id(
    conn: sqlite3.Connection,
    tag_id: int,
) -> Optional[Dict[str, Any]]:
    """
    Get a tag by ID.

    Args:
        conn: Evidence database connection
        tag_id: Tag ID

    Returns:
        Tag dict or None if not found
    """
    sql = """
        SELECT id, name, name_normalized, usage_count, created_by, created_at_utc, evidence_id
        FROM tags
        WHERE id = ?
    """
    row = conn.execute(sql, (tag_id,)).fetchone()
    return dict(row) if row else None


def insert_tag(
    conn: sqlite3.Connection,
    evidence_id: int,
    name: str,
    created_by: str = "manual",
) -> int:
    """
    Insert a new tag.

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        name: Tag name (display name)
        created_by: Creator identifier (e.g., 'manual', 'pattern_detection')

    Returns:
        New tag ID

    Raises:
        sqlite3.IntegrityError: If tag already exists (use get_or_create_tag instead)
    """
    name_normalized = name.lower()
    sql = """
        INSERT INTO tags (evidence_id, name, name_normalized, created_by)
        VALUES (?, ?, ?, ?)
    """
    cursor = conn.execute(sql, (evidence_id, name, name_normalized, created_by))
    return cursor.lastrowid


def get_or_create_tag(
    conn: sqlite3.Connection,
    evidence_id: int,
    name: str,
    created_by: str = "manual",
) -> int:
    """
    Get existing tag ID or create new tag.

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        name: Tag name
        created_by: Creator identifier

    Returns:
        Tag ID (existing or newly created)
    """
    existing = get_tag_by_name(conn, evidence_id, name)
    if existing:
        return existing["id"]

    try:
        return insert_tag(conn, evidence_id, name, created_by)
    except sqlite3.IntegrityError:
        # Race condition: tag was created by another thread
        existing = get_tag_by_name(conn, evidence_id, name)
        if existing:
            return existing["id"]
        raise


def update_tag_name(
    conn: sqlite3.Connection,
    tag_id: int,
    evidence_id: int,
    new_name: str,
) -> None:
    """
    Rename a tag.

    Args:
        conn: Evidence database connection
        tag_id: Tag ID
        evidence_id: Evidence ID (for safety check)
        new_name: New tag name
    """
    name_normalized = new_name.lower()
    sql = """
        UPDATE tags
        SET name = ?, name_normalized = ?
        WHERE id = ? AND evidence_id = ?
    """
    conn.execute(sql, (new_name, name_normalized, tag_id, evidence_id))


def delete_tag(
    conn: sqlite3.Connection,
    tag_id: int,
    evidence_id: int,
) -> None:
    """
    Delete a tag and all its associations.

    Note: Associations are cascade-deleted via FK constraint.

    Args:
        conn: Evidence database connection
        tag_id: Tag ID
        evidence_id: Evidence ID (for safety check)
    """
    sql = "DELETE FROM tags WHERE id = ? AND evidence_id = ?"
    conn.execute(sql, (tag_id, evidence_id))


# -----------------------------------------------------------------------------
# Tag Association Operations
# -----------------------------------------------------------------------------

def insert_tag_association(
    conn: sqlite3.Connection,
    tag_id: int,
    evidence_id: int,
    artifact_type: str,
    artifact_id: int,
    tagged_by: str = "manual",
) -> None:
    """
    Create a tag association (tag an artifact).

    Uses INSERT OR IGNORE to handle duplicates silently.

    Args:
        conn: Evidence database connection
        tag_id: Tag ID
        evidence_id: Evidence ID
        artifact_type: Artifact type ('url', 'image', 'file_list', etc.)
        artifact_id: Artifact ID
        tagged_by: Tagger identifier
    """
    sql = """
        INSERT OR IGNORE INTO tag_associations
        (tag_id, evidence_id, artifact_type, artifact_id, tagged_by)
        VALUES (?, ?, ?, ?, ?)
    """
    conn.execute(sql, (tag_id, evidence_id, artifact_type, artifact_id, tagged_by))


def delete_tag_association(
    conn: sqlite3.Connection,
    tag_id: int,
    artifact_type: str,
    artifact_id: int,
) -> None:
    """
    Remove a tag from an artifact.

    Args:
        conn: Evidence database connection
        tag_id: Tag ID
        artifact_type: Artifact type
        artifact_id: Artifact ID
    """
    sql = """
        DELETE FROM tag_associations
        WHERE tag_id = ? AND artifact_type = ? AND artifact_id = ?
    """
    conn.execute(sql, (tag_id, artifact_type, artifact_id))


def get_artifact_tags(
    conn: sqlite3.Connection,
    evidence_id: int,
    artifact_type: str,
    artifact_id: int,
) -> List[Dict[str, Any]]:
    """
    Get all tags for a specific artifact.

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        artifact_type: Artifact type
        artifact_id: Artifact ID

    Returns:
        List of tag dicts with keys: id, name, name_normalized, tagged_at_utc, tagged_by
    """
    sql = """
        SELECT t.id, t.name, t.name_normalized, ta.tagged_at_utc, ta.tagged_by
        FROM tags t
        JOIN tag_associations ta ON t.id = ta.tag_id
        WHERE ta.evidence_id = ? AND ta.artifact_type = ? AND ta.artifact_id = ?
        ORDER BY t.name
    """
    cursor = conn.execute(sql, (evidence_id, artifact_type, artifact_id))
    return [dict(row) for row in cursor.fetchall()]


def get_artifact_tags_str(
    conn: sqlite3.Connection,
    evidence_id: int,
    artifact_type: str,
    artifact_id: int,
) -> str:
    """
    Get tags for a specific artifact as a comma-separated string.

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        artifact_type: Artifact type
        artifact_id: Artifact ID

    Returns:
        Comma-separated list of tag names, or empty string if no tags
    """
    sql = """
        SELECT GROUP_CONCAT(t.name, ', ')
        FROM tag_associations ta
        JOIN tags t ON ta.tag_id = t.id
        WHERE ta.evidence_id = ? AND ta.artifact_type = ? AND ta.artifact_id = ?
    """
    result = conn.execute(sql, (evidence_id, artifact_type, artifact_id)).fetchone()
    return result[0] if result and result[0] else ""


def get_tag_strings_for_artifacts(
    conn: sqlite3.Connection,
    evidence_id: int,
    artifact_type: str,
    artifact_ids: Sequence[int],
) -> Dict[int, str]:
    """
    Get tags for multiple artifacts in a single query.

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        artifact_type: Artifact type
        artifact_ids: Sequence of artifact IDs

    Returns:
        Mapping of artifact_id -> comma-separated tag names
    """
    if not artifact_ids:
        return {}

    placeholders = ",".join("?" for _ in artifact_ids)
    sql = f"""
        SELECT ta.artifact_id, GROUP_CONCAT(t.name, ', ') AS tags
        FROM tag_associations ta
        JOIN tags t ON ta.tag_id = t.id
        WHERE ta.evidence_id = ?
          AND ta.artifact_type = ?
          AND ta.artifact_id IN ({placeholders})
        GROUP BY ta.artifact_id
    """
    params = [evidence_id, artifact_type, *artifact_ids]
    rows = conn.execute(sql, params).fetchall()
    return {row["artifact_id"]: row["tags"] or "" for row in rows}


def get_artifacts_by_tag_id(
    conn: sqlite3.Connection,
    tag_id: int,
) -> Dict[str, List[int]]:
    """
    Get all artifact IDs associated with a tag, grouped by type.

    Args:
        conn: Evidence database connection
        tag_id: Tag ID

    Returns:
        Dict mapping artifact_type -> list of artifact IDs
    """
    sql = """
        SELECT artifact_type, artifact_id
        FROM tag_associations
        WHERE tag_id = ?
    """
    results: Dict[str, List[int]] = {}
    cursor = conn.execute(sql, (tag_id,))
    for row in cursor:
        atype = row["artifact_type"]
        aid = row["artifact_id"]
        if atype not in results:
            results[atype] = []
        results[atype].append(aid)
    return results


def merge_tag_associations(
    conn: sqlite3.Connection,
    evidence_id: int,
    source_tag_ids: List[int],
    target_tag_id: int,
) -> None:
    """
    Merge associations from source tags into target tag, then delete sources.

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        source_tag_ids: List of tag IDs to merge from
        target_tag_id: Tag ID to merge into
    """
    if not source_tag_ids:
        return

    placeholders = ",".join("?" for _ in source_tag_ids)

    # Get all associations from source tags
    select_sql = f"""
        SELECT artifact_type, artifact_id, tagged_at_utc, tagged_by
        FROM tag_associations
        WHERE tag_id IN ({placeholders})
    """
    cursor = conn.execute(select_sql, source_tag_ids)
    associations = cursor.fetchall()

    # Re-point associations to target tag (ignore duplicates)
    for assoc in associations:
        try:
            conn.execute(
                """
                INSERT INTO tag_associations
                (tag_id, evidence_id, artifact_type, artifact_id, tagged_at_utc, tagged_by)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    target_tag_id,
                    evidence_id,
                    assoc["artifact_type"],
                    assoc["artifact_id"],
                    assoc["tagged_at_utc"],
                    assoc["tagged_by"],
                ),
            )
        except sqlite3.IntegrityError:
            # Already tagged with target tag, ignore
            pass

    # Delete source tags (cascade deletes old associations)
    delete_sql = f"DELETE FROM tags WHERE id IN ({placeholders})"
    conn.execute(delete_sql, source_tag_ids)


# -----------------------------------------------------------------------------
# Tag-Based Artifact Queries
# -----------------------------------------------------------------------------

def query_artifacts_by_tags(
    conn: sqlite3.Connection,
    evidence_id: int,
    artifact_type: str,
    table_name: str,
    tag_ids: List[int],
    tag_mode: str = "all",
    limit: Optional[int] = None,
    order_by: Optional[str] = None,
) -> List[sqlite3.Row]:
    """
    Query artifacts by tags with AND/OR logic.

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        artifact_type: Artifact type in tag_associations
        table_name: Table to query
        tag_ids: Tag IDs to filter by
        tag_mode: 'all' (AND) or 'any' (OR)
        limit: Optional max results
        order_by: Optional ORDER BY clause (without keyword)

    Returns:
        List of matching rows
    """
    if not tag_ids:
        return []

    placeholders = ",".join("?" for _ in tag_ids)
    params: List[Any] = [artifact_type, evidence_id, *tag_ids]
    having_clause = (
        f"HAVING COUNT(DISTINCT ta.tag_id) = {len(tag_ids)}" if tag_mode == "all" else ""
    )

    sql = f"""
        SELECT a.*
        FROM {table_name} a
        JOIN tag_associations ta
          ON ta.artifact_type = ?
         AND ta.artifact_id = a.id
        WHERE a.evidence_id = ?
          AND ta.tag_id IN ({placeholders})
        GROUP BY a.id
        {having_clause}
    """

    if order_by:
        sql += f" ORDER BY {order_by}"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    return conn.execute(sql, params).fetchall()


def query_all_tagged_artifacts(
    conn: sqlite3.Connection,
    evidence_id: int,
    artifact_type: str,
    table_name: str,
    limit: Optional[int] = None,
    order_by: Optional[str] = None,
) -> List[sqlite3.Row]:
    """
    Query all artifacts of a type that have at least one tag.

    Args:
        conn: Evidence database connection
        evidence_id: Evidence ID
        artifact_type: Artifact type in tag_associations
        table_name: Table to query
        limit: Optional max results
        order_by: Optional ORDER BY clause (without keyword)

    Returns:
        List of matching rows
    """
    params: List[Any] = [artifact_type, evidence_id]
    sql = f"""
        SELECT DISTINCT a.*
        FROM {table_name} a
        JOIN tag_associations ta
          ON ta.artifact_type = ?
         AND ta.artifact_id = a.id
        WHERE a.evidence_id = ?
    """

    if order_by:
        sql += f" ORDER BY {order_by}"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)

    return conn.execute(sql, params).fetchall()


__all__ = [
    # Tag CRUD
    "get_all_tags",
    "get_tag_by_name",
    "get_tag_by_id",
    "insert_tag",
    "get_or_create_tag",
    "update_tag_name",
    "delete_tag",
    # Tag associations
    "insert_tag_association",
    "delete_tag_association",
    "get_artifact_tags",
    "get_artifact_tags_str",
    "get_tag_strings_for_artifacts",
    "get_artifacts_by_tag_id",
    "merge_tag_associations",
    # Tag-based queries
    "query_artifacts_by_tags",
    "query_all_tagged_artifacts",
]
