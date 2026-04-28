"""
Database helper functions for extractor statistics.

Initial implementation
Moved to database/helpers/ during refactor
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from core.statistics_collector import ExtractorRunStats

LOGGER = logging.getLogger(__name__)


def _compute_json_total(
    json_data: Any,
    fallback_keys: tuple[str, ...] = ("records", "files"),
) -> int:
    """Parse a statistics JSON field and compute a total count.

    Priority:
    1. If a key from *fallback_keys* exists, use its value directly.
    2. Otherwise sum all numeric values in the dict.
    """
    try:
        d = json.loads(json_data) if isinstance(json_data, str) else json_data
        if not d:
            return 0
        for key in fallback_keys:
            if key in d:
                v = d[key]
                return int(v) if isinstance(v, (int, float)) else 0
        return sum(v for v in d.values() if isinstance(v, (int, float)))
    except (json.JSONDecodeError, TypeError, AttributeError):
        return 0


def _escape_like(value: str) -> str:
    """Escape SQL LIKE metacharacters for safe use in parameterised queries."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def upsert_extractor_statistics(db_manager, stats: "ExtractorRunStats") -> None:
    """
    Insert or update extractor statistics (latest run wins).

    Uses INSERT OR REPLACE with UNIQUE constraint on (evidence_id, extractor_name).
    """
    conn = db_manager.get_evidence_conn(stats.evidence_id, stats.evidence_label)
    data = stats.to_dict()

    conn.execute("""
        INSERT INTO extractor_statistics (
            evidence_id, extractor_name, run_id, started_at, finished_at,
            duration_seconds, status, discovered, ingested, failed, skipped
        ) VALUES (
            :evidence_id, :extractor_name, :run_id, :started_at, :finished_at,
            :duration_seconds, :status, :discovered, :ingested, :failed, :skipped
        )
        ON CONFLICT(evidence_id, extractor_name) DO UPDATE SET
            run_id = excluded.run_id,
            started_at = excluded.started_at,
            finished_at = excluded.finished_at,
            duration_seconds = excluded.duration_seconds,
            status = excluded.status,
            discovered = excluded.discovered,
            ingested = excluded.ingested,
            failed = excluded.failed,
            skipped = excluded.skipped
    """, data)
    conn.commit()


def get_extractor_statistics_by_evidence(
    db_manager,
    evidence_id: int,
    evidence_label: str
) -> List[Dict[str, Any]]:
    """Get all extractor statistics for a specific evidence."""
    conn = db_manager.get_evidence_conn(evidence_id, evidence_label)

    # Check if table exists (handles pre-migration databases)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "extractor_statistics" not in tables:
        return []

    cursor = conn.execute("""
        SELECT * FROM extractor_statistics
        WHERE evidence_id = ?
        ORDER BY extractor_name
    """, (evidence_id,))

    # Convert rows to dicts
    columns = [desc[0] for desc in cursor.description]
    rows = []
    for row in cursor.fetchall():
        rows.append(dict(zip(columns, row)))

    return rows


def delete_extractor_statistics_by_evidence(
    db_manager,
    evidence_id: int,
    evidence_label: str
) -> int:
    """Delete all statistics for an evidence. Returns count deleted."""
    conn = db_manager.get_evidence_conn(evidence_id, evidence_label)

    # Check if table exists
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "extractor_statistics" not in tables:
        return 0

    cursor = conn.execute("""
        DELETE FROM extractor_statistics WHERE evidence_id = ?
    """, (evidence_id,))
    conn.commit()
    return cursor.rowcount


def delete_extractor_statistics_by_run(
    db_manager,
    evidence_id: int,
    evidence_label: str,
    extractor_name: str
) -> int:
    """Delete statistics for a specific extractor. Returns count deleted."""
    conn = db_manager.get_evidence_conn(evidence_id, evidence_label)

    # Check if table exists
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "extractor_statistics" not in tables:
        return 0

    cursor = conn.execute("""
        DELETE FROM extractor_statistics
        WHERE evidence_id = ? AND extractor_name = ?
    """, (evidence_id, extractor_name))
    conn.commit()
    return cursor.rowcount


def get_extractor_statistics_by_name(
    db_manager,
    evidence_id: int,
    evidence_label: str,
    extractor_name: str
) -> Dict[str, Any] | None:
    """
    Get statistics for a specific extractor.

    Returns None if not found or table doesn't exist.
    """
    conn = db_manager.get_evidence_conn(evidence_id, evidence_label)

    # Check if table exists
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "extractor_statistics" not in tables:
        return None

    cursor = conn.execute("""
        SELECT * FROM extractor_statistics
        WHERE evidence_id = ? AND extractor_name = ?
    """, (evidence_id, extractor_name))

    row = cursor.fetchone()
    if not row:
        return None

    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def sync_process_log_from_statistics(
    db_manager,
    evidence_id: int,
    evidence_label: str,
    extractor_name: str,
) -> bool:
    """
    Sync process_log records_ingested from extractor_statistics.

    This ensures process_log audit entries have accurate counts by copying
    the total ingested count from extractor_statistics.ingested JSON.

    Args:
        db_manager: DatabaseManager instance
        evidence_id: Evidence ID
        evidence_label: Evidence label for db access
        extractor_name: Extractor name to sync

    Returns:
        True if sync succeeded, False if no matching records found

    Added for process_log/extractor_statistics audit sync
    """
    conn = db_manager.get_evidence_conn(evidence_id, evidence_label)

    # Get statistics for this extractor
    stats = get_extractor_statistics_by_name(
        db_manager, evidence_id, evidence_label, extractor_name
    )
    if not stats:
        return False

    total_ingested = _compute_json_total(stats.get("ingested", "{}"), ("records",))
    total_discovered = _compute_json_total(stats.get("discovered", "{}"), ("records", "files"))

    run_id = stats.get("run_id")
    if not run_id:
        return False

    # Update process_log entries for this extractor run
    # Match by run_id and extractor_name (or task containing extractor_name)
    escaped_name = _escape_like(extractor_name)
    cursor = conn.execute("""
        UPDATE process_log
        SET records_extracted = ?,
            records_ingested = ?
        WHERE evidence_id = ?
          AND run_id = ?
          AND (extractor_name = ? OR task LIKE ? ESCAPE '\\')
    """, (total_discovered, total_ingested, evidence_id, run_id,
          extractor_name, f"%{escaped_name}%"))

    conn.commit()
    return cursor.rowcount > 0


def sync_process_log_counters(
    evidence_conn,
    evidence_id: int,
    extractor_name: str,
) -> bool:
    """
    Sync process_log records from extractor_statistics using evidence_conn directly.

    Variant of sync_process_log_from_statistics that works without db_manager,
    suitable for use in the extraction orchestrator.

    Args:
        evidence_conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        extractor_name: Extractor name to sync

    Returns:
        True if sync succeeded, False if no matching records found
    """
    # Get statistics directly from evidence_conn
    try:
        cursor = evidence_conn.execute(
            "SELECT * FROM extractor_statistics WHERE evidence_id = ? AND extractor_name = ?",
            (evidence_id, extractor_name),
        )
        row = cursor.fetchone()
        if not row:
            return False
        columns = [desc[0] for desc in cursor.description]
        stats = dict(zip(columns, row))
    except Exception as exc:
        LOGGER.debug("Failed to read extractor_statistics for %s: %s", extractor_name, exc)
        return False

    total_ingested = _compute_json_total(stats.get("ingested", "{}"), ("records",))
    total_discovered = _compute_json_total(stats.get("discovered", "{}"), ("records", "files"))

    run_id = stats.get("run_id")
    if not run_id:
        return False

    escaped_name = _escape_like(extractor_name)
    cursor = evidence_conn.execute("""
        UPDATE process_log
        SET records_extracted = ?,
            records_ingested = ?
        WHERE evidence_id = ?
          AND run_id = ?
          AND (extractor_name = ? OR task LIKE ? ESCAPE '\\')
    """, (total_discovered, total_ingested, evidence_id, run_id,
          extractor_name, f"%{escaped_name}%"))

    evidence_conn.commit()
    return cursor.rowcount > 0
