"""
Browser usage indicator database helper functions.

This module provides CRUD operations for the browser_indicators table,
which stores soft browser usage indicators aggregated from multiple
forensic sources (os_indicators, jump_list_entries, urls, etc.).

These are NOT parsed profile data — they are execution traces, URL
patterns, and other residual indicators that suggest browser usage
even when no profile/state files are found.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, List, Optional

from core.logging import get_logger

from ..schema import FilterOp, TABLE_SCHEMAS
from .generic import delete_by_run, get_rows, insert_row, insert_rows

LOGGER = get_logger("core.database.helpers.browser_indicators")

__all__ = [
    "insert_browser_indicator",
    "insert_browser_indicators",
    "get_browser_indicators",
    "delete_browser_indicators_by_run",
    "aggregate_tor_indicators",
]


def insert_browser_indicator(
    conn: sqlite3.Connection,
    evidence_id: int,
    browser: str,
    indicator_type: str,
    source_table: str,
    indicator_value: str,
    *,
    run_id: str,
    source_id: Optional[int] = None,
    source_path: Optional[str] = None,
    timestamp_utc: Optional[str] = None,
    confidence: str = "medium",
    notes: Optional[str] = None,
) -> None:
    """Insert a single browser usage indicator."""
    record = {
        "run_id": run_id,
        "browser": browser,
        "indicator_type": indicator_type,
        "source_table": source_table,
        "source_id": source_id,
        "indicator_value": indicator_value,
        "source_path": source_path,
        "timestamp_utc": timestamp_utc,
        "confidence": confidence,
        "notes": notes,
    }
    insert_row(conn, TABLE_SCHEMAS["browser_indicators"], evidence_id, record)


def insert_browser_indicators(
    conn: sqlite3.Connection,
    evidence_id: int,
    indicators: Iterable[Dict[str, Any]],
) -> int:
    """Insert multiple browser indicator records in batch."""
    return insert_rows(conn, TABLE_SCHEMAS["browser_indicators"], evidence_id, indicators)


def get_browser_indicators(
    conn: sqlite3.Connection,
    evidence_id: int,
    *,
    browser: Optional[str] = None,
    indicator_type: Optional[str] = None,
    confidence: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """Query browser indicators with optional filters."""
    filters: Dict[str, Any] = {}
    if browser:
        filters["browser"] = (FilterOp.EQ, browser)
    if indicator_type:
        filters["indicator_type"] = (FilterOp.EQ, indicator_type)
    if confidence:
        filters["confidence"] = (FilterOp.EQ, confidence)

    return get_rows(
        conn,
        TABLE_SCHEMAS["browser_indicators"],
        evidence_id,
        filters=filters or None,
        limit=limit,
    )


def delete_browser_indicators_by_run(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete browser indicators for a specific run."""
    return delete_by_run(conn, TABLE_SCHEMAS["browser_indicators"], evidence_id, run_id)


def aggregate_tor_indicators(
    conn: sqlite3.Connection,
    evidence_id: int,
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Scan existing artifact tables for Tor Browser usage indicators.

    Searches:
    - os_indicators: UserAssist references to Tor Browser executables/LNKs
    - jump_list_entries: Tor Browser executable references with arguments
    - urls: Tor-related URLs (torproject.org, .onion, torbrowser-install)

    Args:
        conn: Evidence DB connection
        evidence_id: Evidence ID
        run_id: Run ID for the indicator records

    Returns:
        List of indicator records (ready for insert_browser_indicators)
    """
    indicators: List[Dict[str, Any]] = []
    conn.row_factory = sqlite3.Row

    # Tor-related patterns
    TOR_EXE_PATTERNS = [
        "%Tor Browser%firefox.exe%",
        "%Start Tor Browser%",
        "%Tor Browser%Browser%",
        "%torbrowser%",
    ]
    TOR_URL_PATTERNS = [
        "%torproject.org%",
        "%torbrowser-install%",
        "%.onion%",
        "%.onion/%",
    ]

    # 1. Scan os_indicators (UserAssist, etc.)
    for pattern in TOR_EXE_PATTERNS:
        try:
            rows = conn.execute(
                """
                SELECT id, type, name, value, path, detected_at_utc
                FROM os_indicators
                WHERE evidence_id = ? AND (
                    value LIKE ? OR name LIKE ? OR path LIKE ?
                )
                """,
                (evidence_id, pattern, pattern, pattern),
            ).fetchall()
            for row in rows:
                indicators.append({
                    "run_id": run_id,
                    "browser": "tor",
                    "indicator_type": f"os_indicator_{row['type']}",
                    "source_table": "os_indicators",
                    "source_id": row["id"],
                    "indicator_value": row["value"] or row["name"],
                    "source_path": row["path"],
                    "timestamp_utc": row["detected_at_utc"],
                    "confidence": "high",
                    "notes": f"UserAssist/registry: {row['type']}",
                })
        except sqlite3.OperationalError:
            pass  # Table may not exist yet
        except Exception as e:
            LOGGER.warning("Failed to scan os_indicators for Tor pattern %r: %s", pattern, e)

    # 2. Scan jump_list_entries
    for pattern in TOR_EXE_PATTERNS:
        try:
            rows = conn.execute(
                """
                SELECT id, target_path, arguments, lnk_access_time, source_path
                FROM jump_list_entries
                WHERE evidence_id = ? AND (
                    target_path LIKE ? OR arguments LIKE ?
                )
                """,
                (evidence_id, pattern, pattern),
            ).fetchall()
            for row in rows:
                value = row["target_path"] or ""
                if row["arguments"]:
                    value += f" {row['arguments']}"
                indicators.append({
                    "run_id": run_id,
                    "browser": "tor",
                    "indicator_type": "jump_list_entry",
                    "source_table": "jump_list_entries",
                    "source_id": row["id"],
                    "indicator_value": value.strip(),
                    "source_path": row["source_path"],
                    "timestamp_utc": row["lnk_access_time"],
                    "confidence": "high",
                    "notes": "Jump list reference to Tor Browser",
                })
        except sqlite3.OperationalError:
            pass  # Table may not exist yet
        except Exception as e:
            LOGGER.warning("Failed to scan jump_list_entries for Tor pattern %r: %s", pattern, e)

    # 3. Scan urls for Tor-related patterns
    for pattern in TOR_URL_PATTERNS:
        try:
            rows = conn.execute(
                """
                SELECT id, url, discovered_by, first_seen_utc
                FROM urls
                WHERE evidence_id = ? AND url LIKE ?
                """,
                (evidence_id, pattern),
            ).fetchall()
            for row in rows:
                # .onion URLs are high confidence; download/project URLs are medium
                is_onion = ".onion" in (row["url"] or "")
                indicators.append({
                    "run_id": run_id,
                    "browser": "tor",
                    "indicator_type": "url_pattern",
                    "source_table": "urls",
                    "source_id": row["id"],
                    "indicator_value": row["url"],
                    "timestamp_utc": row["first_seen_utc"],
                    "confidence": "high" if is_onion else "medium",
                    "notes": f"Tor-related URL from {row['discovered_by'] or 'unknown'}",
                })
        except sqlite3.OperationalError:
            pass  # Table may not exist yet
        except Exception as e:
            LOGGER.warning("Failed to scan urls for Tor pattern %r: %s", pattern, e)

    # Deduplicate by (source_table, source_id) to avoid duplicate indicators
    seen = set()
    unique_indicators: List[Dict[str, Any]] = []
    for ind in indicators:
        key = (ind["source_table"], ind.get("source_id"))
        if key not in seen:
            seen.add(key)
            unique_indicators.append(ind)

    return unique_indicators
