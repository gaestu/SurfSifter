"""
Chromium History SQLite database parser.

Parses the History database from Chromium-based browsers (Chrome, Edge, Brave, Opera).
All Chromium browsers use an identical schema, so one parser works for all.

Features:
- Per-visit history records (not aggregated per-URL)
- Keyword search terms extraction
- URL-level aggregates
- WebKit timestamp conversion

Moved from chromium/_parsers.py for consistency with cookies/_parsers.py

Usage:
    from extractors.browser.chromium.history._parser import (
        parse_history_visits,
        parse_keyword_search_terms,
        HistoryVisit,
        SearchTerm,
    )

    with safe_sqlite_connect(history_path) as conn:
        for visit in parse_history_visits(conn):
            print(f"{visit.url} at {visit.visit_time_iso}")
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterator, Optional

from extractors._shared.timestamps import webkit_to_datetime, webkit_to_iso
from extractors._shared.sqlite_helpers import safe_execute, table_exists


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class HistoryVisit:
    """A single browser history visit record."""
    url: str
    title: Optional[str]
    visit_time: Optional[datetime]
    visit_time_iso: Optional[str]
    visit_count: int
    typed_count: int
    from_visit: int
    transition: int
    visit_duration_ms: int
    hidden: bool  # Whether URL is hidden (subframes, errors)

    # For forensic context
    url_id: int
    visit_id: int


@dataclass
class SearchTerm:
    """A single keyword search term record from Chromium History."""
    term: str                         # Original search term
    normalized_term: Optional[str]    # Normalized (lowercase) term
    url: Optional[str]                # Associated URL
    search_time: Optional[datetime]   # When the search was performed
    search_time_iso: Optional[str]    # ISO 8601 timestamp
    keyword_id: int                   # Original keyword_id (search engine)
    url_id: int                       # Original url_id reference


# =============================================================================
# History Visits Parsing
# =============================================================================

def parse_history_visits(conn: sqlite3.Connection) -> Iterator[HistoryVisit]:
    """
    Parse Chromium History database visits.

    Queries the visits table joined with urls to get per-visit records,
    not just per-URL aggregates. This ensures timeline accuracy.

    Args:
        conn: SQLite connection to History database

    Yields:
        HistoryVisit records ordered by visit_time DESC

    Note:
        Chromium stores timestamps in WebKit format (microseconds since 1601-01-01).
    """
    # Verify required tables exist
    if not table_exists(conn, "visits") or not table_exists(conn, "urls"):
        return

    # Join visits with urls to get per-visit records
    # visit_duration is in microseconds, convert to milliseconds
    query = """
        SELECT
            u.id as url_id,
            u.url,
            u.title,
            u.visit_count,
            u.typed_count,
            COALESCE(u.hidden, 0) as hidden,
            v.id as visit_id,
            v.visit_time,
            v.from_visit,
            v.transition,
            COALESCE(v.visit_duration, 0) as visit_duration
        FROM visits v
        JOIN urls u ON v.url = u.id
        WHERE v.visit_time > 0
        ORDER BY v.visit_time DESC
    """

    try:
        rows = safe_execute(conn, query)
    except Exception:
        # If query fails (corrupted DB), return empty
        return

    for row in rows:
        visit_time_webkit = row["visit_time"]
        visit_dt = webkit_to_datetime(visit_time_webkit)
        visit_iso = webkit_to_iso(visit_time_webkit)

        yield HistoryVisit(
            url=row["url"],
            title=row["title"],
            visit_time=visit_dt,
            visit_time_iso=visit_iso,
            visit_count=row["visit_count"],
            typed_count=row["typed_count"],
            from_visit=row["from_visit"] or 0,
            transition=row["transition"] or 0,
            visit_duration_ms=row["visit_duration"] // 1000,  # μs → ms
            hidden=bool(row["hidden"]),
            url_id=row["url_id"],
            visit_id=row["visit_id"],
        )


def parse_history_urls(conn: sqlite3.Connection) -> Iterator[Dict[str, Any]]:
    """
    Parse Chromium History database URLs (aggregated, not per-visit).

    Use parse_history_visits() for forensically accurate per-visit records.
    This function returns URL-level aggregates (visit_count, last_visit_time).

    Args:
        conn: SQLite connection to History database

    Yields:
        Dict with url, title, visit_count, typed_count, last_visit_time_iso
    """
    if not table_exists(conn, "urls"):
        return

    query = """
        SELECT
            id,
            url,
            title,
            visit_count,
            typed_count,
            last_visit_time,
            hidden
        FROM urls
        WHERE last_visit_time > 0
        ORDER BY last_visit_time DESC
    """

    try:
        rows = safe_execute(conn, query)
    except Exception:
        return

    for row in rows:
        last_visit_iso = webkit_to_iso(row["last_visit_time"])

        yield {
            "url_id": row["id"],
            "url": row["url"],
            "title": row["title"],
            "visit_count": row["visit_count"],
            "typed_count": row["typed_count"],
            "last_visit_time_iso": last_visit_iso,
            "hidden": bool(row["hidden"]),
        }


def get_history_stats(conn: sqlite3.Connection) -> Dict[str, int]:
    """
    Get quick statistics from History database.

    Args:
        conn: SQLite connection to History database

    Returns:
        Dict with url_count, visit_count, download_count
    """
    stats = {
        "url_count": 0,
        "visit_count": 0,
        "download_count": 0,
    }

    if table_exists(conn, "urls"):
        rows = safe_execute(conn, "SELECT COUNT(*) as cnt FROM urls")
        stats["url_count"] = rows[0]["cnt"] if rows else 0

    if table_exists(conn, "visits"):
        rows = safe_execute(conn, "SELECT COUNT(*) as cnt FROM visits")
        stats["visit_count"] = rows[0]["cnt"] if rows else 0

    if table_exists(conn, "downloads"):
        rows = safe_execute(conn, "SELECT COUNT(*) as cnt FROM downloads")
        stats["download_count"] = rows[0]["cnt"] if rows else 0

    return stats


# =============================================================================
# Keyword Search Terms Parsing
# =============================================================================

def parse_keyword_search_terms(conn: sqlite3.Connection) -> Iterator[SearchTerm]:
    """
    Parse Chromium History database keyword_search_terms.

    The keyword_search_terms table stores user search queries typed into
    the omnibox (URL bar). High forensic value as it captures actual
    user intent/searches.

    Args:
        conn: SQLite connection to History database

    Yields:
        SearchTerm records ordered by visit time DESC

    Note:
        Requires joining with urls table to get timestamps.
        The keyword_id references the keyword (search engine) table.
    """
    if not table_exists(conn, "keyword_search_terms"):
        return

    # Check if urls table exists for timestamp lookup
    has_urls = table_exists(conn, "urls")

    if has_urls:
        # Join with urls to get the search timestamp
        query = """
            SELECT
                kst.keyword_id,
                kst.url_id,
                kst.term,
                kst.normalized_term,
                u.url,
                u.last_visit_time
            FROM keyword_search_terms kst
            LEFT JOIN urls u ON kst.url_id = u.id
            ORDER BY u.last_visit_time DESC
        """
    else:
        # Fallback: no timestamp available
        query = """
            SELECT
                keyword_id,
                url_id,
                term,
                normalized_term,
                NULL as url,
                NULL as last_visit_time
            FROM keyword_search_terms
            ORDER BY keyword_id
        """

    try:
        rows = safe_execute(conn, query)
    except Exception:
        return

    for row in rows:
        visit_time = row["last_visit_time"]
        search_dt = webkit_to_datetime(visit_time) if visit_time else None
        search_iso = webkit_to_iso(visit_time) if visit_time else None

        yield SearchTerm(
            term=row["term"] or "",
            normalized_term=row["normalized_term"],
            url=row["url"],
            search_time=search_dt,
            search_time_iso=search_iso,
            keyword_id=row["keyword_id"] or 0,
            url_id=row["url_id"] or 0,
        )


def get_search_terms_stats(conn: sqlite3.Connection) -> Dict[str, int]:
    """Get quick statistics from keyword_search_terms table."""
    stats = {"search_count": 0, "unique_terms": 0}

    if not table_exists(conn, "keyword_search_terms"):
        return stats

    rows = safe_execute(conn, "SELECT COUNT(*) as cnt FROM keyword_search_terms")
    stats["search_count"] = rows[0]["cnt"] if rows else 0

    rows = safe_execute(conn, "SELECT COUNT(DISTINCT normalized_term) as cnt FROM keyword_search_terms")
    stats["unique_terms"] = rows[0]["cnt"] if rows else 0

    return stats


# =============================================================================
# Path Utilities (re-exported from chromium/_parsers.py)
# =============================================================================
# Path utilities moved to chromium/_parsers.py as they are Chromium-wide.
# These re-exports maintain backward compatibility for direct imports.

from .._parsers import extract_profile_from_path, detect_browser_from_path
