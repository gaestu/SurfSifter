"""
Unsupported artifact discovery.

Scans file_list for known browser artifacts that have no parser,
and records them as extraction_warnings so investigators know
evidence exists but is not structurally parsed.
"""
from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from core.logging import get_logger

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector
    from extractors.callbacks import ExtractorCallbacks

LOGGER = get_logger("extractors._shared.unsupported_artifacts")

# Known Chrome/Chromium auxiliary artifacts that we don't parse yet.
# Each entry: (filename, description, forensic_value)
CHROMIUM_UNSUPPORTED_ARTIFACTS = [
    ("Visited Links", "Bloom filter of visited URLs", "medium"),
    ("Network Action Predictor", "URL autocomplete prediction data", "medium"),
    ("Origin Bound Certs", "TLS channel ID certificates (deprecated)", "low"),
    ("History Provider Cache", "URL completion cache from history", "low"),
    ("Shortcuts", "Omnibox shortcut URLs with visit counts", "medium"),
    ("TransportSecurity", "HSTS preload/pin data (separate from HSTS table)", "low"),
    ("Reporting and NEL", "Network Error Logging reports", "low"),
    ("heavy_ad_intervention", "Heavy ad intervention decisions", "low"),
]

# Service Worker patterns (need path matching, not just filename)
CHROMIUM_SW_PATTERNS = [
    "%Service Worker%CacheStorage%",
    "%Service Worker%ScriptCache%",
    "%Service Worker%Database%",
]


def discover_unsupported_chromium_artifacts(
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
    warning_collector: ExtractionWarningCollector,
    *,
    callbacks: ExtractorCallbacks | None = None,
) -> int:
    """
    Scan file_list for known but unsupported Chromium artifacts.

    Records each discovery as an extraction_warning with type
    'unsupported_artifact' so it appears in the warnings table.

    Args:
        evidence_conn: Evidence database connection
        evidence_id: Evidence ID
        run_id: Current extraction run ID
        warning_collector: ExtractionWarningCollector instance
        callbacks: Optional ExtractorCallbacks for logging

    Returns:
        Number of unsupported artifacts discovered
    """
    count = 0

    # 1. Check for known unsupported filenames
    for filename, description, value in CHROMIUM_UNSUPPORTED_ARTIFACTS:
        try:
            row = evidence_conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM file_list
                WHERE evidence_id = ?
                  AND LOWER(file_name) = LOWER(?)
                  AND COALESCE(deleted, 0) = 0
                """,
                (evidence_id, filename),
            ).fetchone()
            if row and row[0] > 0:
                warning_collector.add_warning(
                    warning_type="unsupported_artifact",
                    item_name=filename,
                    severity="info",
                    category="chromium_auxiliary",
                    item_value=f"{row[0]} file(s) found",
                    context_json={
                        "description": description,
                        "forensic_value": value,
                        "file_count": row[0],
                    },
                )
                count += row[0]
                if callbacks:
                    callbacks.on_log(
                        f"Unsupported artifact present: {filename} ({row[0]} files)",
                        "info",
                    )
        except sqlite3.OperationalError:
            pass  # file_list table may not exist
        except Exception as e:
            LOGGER.warning("Failed to check for %s: %s", filename, e)

    # 2. Check for Service Worker patterns (path-based)
    for pattern in CHROMIUM_SW_PATTERNS:
        try:
            row = evidence_conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM file_list
                WHERE evidence_id = ?
                  AND file_path LIKE ?
                  AND COALESCE(deleted, 0) = 0
                """,
                (evidence_id, pattern),
            ).fetchone()
            if row and row[0] > 0:
                # Extract a readable name from the pattern
                name = pattern.strip("%").replace("%", "/")
                warning_collector.add_warning(
                    warning_type="unsupported_artifact",
                    item_name=name,
                    severity="info",
                    category="chromium_service_worker",
                    item_value=f"{row[0]} file(s) found",
                    context_json={
                        "description": "Service Worker cached content",
                        "forensic_value": "medium",
                        "file_count": row[0],
                    },
                )
                count += row[0]
                if callbacks:
                    callbacks.on_log(
                        f"Unsupported artifact present: {name} ({row[0]} files)",
                        "info",
                    )
        except Exception as e:
            LOGGER.debug("Failed to check for %s: %s", pattern, e)

    return count
