"""
Process Log Service - Read helpers for extractor run status.

Provides read-only access to process_log table to display extraction/ingestion
status in the UI. All queries use existing indexes and are optimized for performance.

Initial implementation.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, TypedDict

from .logging import get_logger

LOGGER = get_logger(__name__)


class RunInfo(TypedDict, total=False):
    """Type hint for run information returned by helpers."""
    finished_at: Optional[str]
    started_at: Optional[str]
    run_id: Optional[str]
    records_ingested: Optional[int]


class ExtractorRunStatus(TypedDict, total=False):
    """Type hint for combined extractor run status."""
    extraction: Optional[RunInfo]
    ingestion: Optional[RunInfo]


def get_last_successful_extraction(
    db_path: Path,
    extractor_name: str,
    evidence_id: int,
) -> Optional[RunInfo]:
    """
    Get the last successful extraction run for an extractor.

    Args:
        db_path: Path to the evidence or case database.
        extractor_name: Name of the extractor (e.g., 'browser_history').
        evidence_id: Evidence ID to filter by.

    Returns:
        Dict with 'finished_at', 'started_at', 'run_id' if found, None otherwise.

    Note:
        Uses extractor_name column which is indexed (idx_process_log_extractor_name).
        Falls back to started_at_utc when finished_at_utc is NULL.
    """
    if not db_path.exists():
        LOGGER.debug("Database not found: %s", db_path)
        return None

    try:
        # Open read-only via SQLite URI to prevent accidental writes
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(
                """
                SELECT
                    COALESCE(finished_at_utc, started_at_utc) as effective_ts,
                    finished_at_utc,
                    started_at_utc,
                    run_id
                FROM process_log
                WHERE evidence_id = ?
                  AND extractor_name = ?
                  AND exit_code = 0
                ORDER BY COALESCE(finished_at_utc, started_at_utc) DESC
                LIMIT 1
                """,
                (evidence_id, extractor_name)
            )
            row = cursor.fetchone()
            if row:
                return {
                    "finished_at": row["finished_at_utc"] or row["started_at_utc"],
                    "started_at": row["started_at_utc"],
                    "run_id": row["run_id"],
                }
            return None
        finally:
            conn.close()
    except sqlite3.Error as e:
        LOGGER.debug("Error querying process_log for extraction: %s", e)
        return None


def get_last_successful_ingestion(
    db_path: Path,
    extractor_name: str,
    evidence_id: int,
) -> Optional[RunInfo]:
    """
    Get the last successful ingestion run for an extractor.

    Args:
        db_path: Path to the evidence or case database.
        extractor_name: Name of the extractor (e.g., 'browser_history').
                        Will be suffixed with ':ingest' for lookup.
        evidence_id: Evidence ID to filter by.

    Returns:
        Dict with 'finished_at', 'started_at', 'run_id', 'records_ingested' if found,
        None otherwise.

    Note:
        Ingestion tasks are stored with extractor_name = '{name}:ingest'.
    """
    if not db_path.exists():
        LOGGER.debug("Database not found: %s", db_path)
        return None

    # Ingestion uses the pattern '{name}:ingest' in extractor_name column
    ingest_name = f"{extractor_name}:ingest"

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            cursor = conn.execute(
                """
                SELECT
                    COALESCE(finished_at_utc, started_at_utc) as effective_ts,
                    finished_at_utc,
                    started_at_utc,
                    run_id,
                    records_ingested
                FROM process_log
                WHERE evidence_id = ?
                  AND extractor_name = ?
                  AND exit_code = 0
                ORDER BY COALESCE(finished_at_utc, started_at_utc) DESC
                LIMIT 1
                """,
                (evidence_id, ingest_name)
            )
            row = cursor.fetchone()
            if row:
                return {
                    "finished_at": row["finished_at_utc"] or row["started_at_utc"],
                    "started_at": row["started_at_utc"],
                    "run_id": row["run_id"],
                    "records_ingested": row["records_ingested"],
                }
            return None
        finally:
            conn.close()
    except sqlite3.Error as e:
        LOGGER.debug("Error querying process_log for ingestion: %s", e)
        return None


def get_extractor_run_status(
    db_path: Path,
    extractor_name: str,
    evidence_id: int,
) -> ExtractorRunStatus:
    """
    Get combined extraction and ingestion status for an extractor.

    Convenience wrapper that calls both get_last_successful_extraction
    and get_last_successful_ingestion.

    Args:
        db_path: Path to the evidence or case database.
        extractor_name: Name of the extractor (e.g., 'browser_history').
        evidence_id: Evidence ID to filter by.

    Returns:
        Dict with 'extraction' and 'ingestion' keys, each containing
        RunInfo or None if no successful run found.

    Example:
        >>> status = get_extractor_run_status(db_path, "browser_history", 1)
        >>> if status["extraction"]:
        ...     print(f"Last extraction: {status['extraction']['finished_at']}")
        >>> if status["ingestion"]:
        ...     print(f"Ingested {status['ingestion']['records_ingested']} records")
    """
    return {
        "extraction": get_last_successful_extraction(db_path, extractor_name, evidence_id),
        "ingestion": get_last_successful_ingestion(db_path, extractor_name, evidence_id),
    }


def format_timestamp_for_display(ts: Optional[str]) -> str:
    """
    Format ISO timestamp for compact UI display.

    Args:
        ts: ISO timestamp string or None.

    Returns:
        Formatted string like "2025-01-15 14:30" or "N/A".
    """
    if not ts:
        return "N/A"

    # Handle ISO format timestamps (2025-01-15T14:30:45.123456+00:00)
    try:
        # Truncate to datetime without microseconds/timezone for display
        if "T" in ts:
            date_part, time_part = ts.split("T")
            time_part = time_part.split(".")[0]  # Remove microseconds
            time_part = time_part.split("+")[0]  # Remove timezone
            time_part = time_part.split("Z")[0]  # Remove Z suffix
            return f"{date_part} {time_part[:5]}"  # Only HH:MM
        return ts[:16]  # Fallback: first 16 chars
    except Exception:
        return ts[:16] if len(ts) > 16 else ts
