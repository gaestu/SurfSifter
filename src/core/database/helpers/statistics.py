"""
Database helper functions for extractor statistics.

Initial implementation
Moved to database/helpers/ during refactor
"""
from __future__ import annotations

from typing import Any, Dict, List, TYPE_CHECKING

if TYPE_CHECKING:
    from core.statistics_collector import ExtractorRunStats


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
    import json

    conn = db_manager.get_evidence_conn(evidence_id, evidence_label)

    # Get statistics for this extractor
    stats = get_extractor_statistics_by_name(
        db_manager, evidence_id, evidence_label, extractor_name
    )
    if not stats:
        return False

    # Parse ingested JSON and compute total
    # Priority: use 'records' key if present (represents actual DB rows inserted)
    # Otherwise sum other count fields (excluding duplicates like urls/images if records exists)
    ingested_json = stats.get("ingested", "{}")
    try:
        ingested_dict = json.loads(ingested_json) if isinstance(ingested_json, str) else ingested_json
        if ingested_dict:
            # Use 'records' if present (this is the canonical count of DB rows inserted)
            if "records" in ingested_dict:
                total_ingested = ingested_dict["records"]
            else:
                # Fallback: sum all values
                total_ingested = sum(ingested_dict.values())
        else:
            total_ingested = 0
    except (json.JSONDecodeError, TypeError):
        total_ingested = 0

    # Parse discovered JSON and compute total (same logic)
    discovered_json = stats.get("discovered", "{}")
    try:
        discovered_dict = json.loads(discovered_json) if isinstance(discovered_json, str) else discovered_json
        if discovered_dict:
            if "records" in discovered_dict:
                total_discovered = discovered_dict["records"]
            elif "files" in discovered_dict:
                # For extraction phase, 'files' is the canonical count
                total_discovered = discovered_dict["files"]
            else:
                total_discovered = sum(discovered_dict.values())
        else:
            total_discovered = 0
    except (json.JSONDecodeError, TypeError):
        total_discovered = 0

    run_id = stats.get("run_id")
    if not run_id:
        return False

    # Update process_log entries for this extractor run
    # Match by run_id and extractor_name (or task containing extractor_name)
    cursor = conn.execute("""
        UPDATE process_log
        SET records_extracted = ?,
            records_ingested = ?
        WHERE evidence_id = ?
          AND run_id = ?
          AND (extractor_name = ? OR task LIKE ?)
    """, (total_discovered, total_ingested, evidence_id, run_id,
          extractor_name, f"%{extractor_name}%"))

    conn.commit()
    return cursor.rowcount > 0
