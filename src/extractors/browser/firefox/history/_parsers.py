"""
Firefox history database parsing utilities.

Pure functions for parsing Firefox places.sqlite history database:
- Per-visit records from moz_historyvisits + moz_places
- Input history from moz_inputhistory (typed URL autocomplete)
- Statistics without full parsing

Firefox uses PRTime timestamps (microseconds since 1970-01-01 UTC).
All parsers return dataclasses with typed fields.

This module is specific to the Firefox history extractor. Other Firefox
artifact parsers (cookies, bookmarks, etc.) remain in the family-level
_parsers.py module.

Moved from firefox/_parsers.py for extractor isolation
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, Optional, Set, TYPE_CHECKING

from extractors._shared.timestamps import prtime_to_iso
from ._schemas import VISIT_TYPES, get_visit_type_label

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector


# =============================================================================
# History Dataclasses
# =============================================================================


@dataclass
class FirefoxVisit:
    """Single visit record from Firefox history."""

    url: str
    title: Optional[str]
    visit_time_utc: Optional[str]  # ISO 8601
    visit_count: int
    typed: int  # 1 if URL was typed, 0 otherwise
    last_visit_time_utc: Optional[str]  # ISO 8601 (URL-level aggregate)

    # Visit-level details
    from_visit: Optional[int]  # Referrer visit ID
    visit_type: int  # 1=link, 2=typed, 3=bookmark, etc.

    # Raw timestamp for forensic purposes
    visit_date_raw: int  # PRTime (microseconds since 1970)

    # Fields with defaults (must come after required fields)
    visit_type_label: str = ""  # Human-readable visit type
    frecency: int = 0  # Firefox importance score
    hidden: bool = False  # Internal/redirect URL indicator
    typed_input: Optional[str] = None  # What user typed from moz_inputhistory


@dataclass
class FirefoxHistoryStats:
    """Statistics from a Firefox history file."""

    visit_count: int = 0
    unique_urls: int = 0
    earliest_visit: Optional[str] = None  # ISO 8601
    latest_visit: Optional[str] = None  # ISO 8601


# =============================================================================
# Input History Loading
# =============================================================================


def _load_inputhistory(conn: sqlite3.Connection) -> Dict[int, str]:
    """
    Load moz_inputhistory data for typed URL autocomplete context.

    moz_inputhistory tracks what users typed in the address bar that led
    to URL selection. Forensically valuable for showing user intent.

    Args:
        conn: SQLite connection to places.sqlite

    Returns:
        Dict mapping place_id -> most frequent typed input
    """
    inputhistory: Dict[int, str] = {}

    try:
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='moz_inputhistory'"
        )
        if not cursor.fetchone():
            return inputhistory

        # Get most frequently used input for each place_id
        # (same URL may have multiple typed inputs)
        cursor.execute("""
            SELECT place_id, input, use_count
            FROM moz_inputhistory
            ORDER BY place_id, use_count DESC
        """)

        for row in cursor:
            place_id = row[0]
            typed_input = row[1]
            # Keep only the most frequently used input per place_id
            if place_id not in inputhistory:
                inputhistory[place_id] = typed_input

    except sqlite3.Error:
        pass  # Table may not exist in older Firefox versions

    return inputhistory


# =============================================================================
# History Parsing
# =============================================================================


def parse_history_visits(
    db_path: Path,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Iterator[FirefoxVisit]:
    """
    Parse Firefox history visits from places.sqlite.

    Joins moz_historyvisits with moz_places to get per-visit records,
    not just per-URL aggregates. This ensures timeline accuracy.

    Also loads moz_inputhistory for typed URL autocomplete context,
    which shows what the user actually typed in the address bar.

    Args:
        db_path: Path to places.sqlite
        warning_collector: Optional collector for schema warnings

    Yields:
        FirefoxVisit records ordered by visit_date DESC
    """
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return

    # Track found visit types for warning collection
    found_visit_types: Set[int] = set()

    try:
        cursor = conn.cursor()

        # Check if tables exist
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('moz_places', 'moz_historyvisits')"
        )
        tables = {row[0] for row in cursor.fetchall()}

        if not tables.issuperset({'moz_places', 'moz_historyvisits'}):
            return

        # Discover unknown tables if warning collector provided
        if warning_collector:
            _check_unknown_tables(conn, warning_collector, str(db_path))

        # Load typed input history for URL context
        inputhistory = _load_inputhistory(conn)

        # Join visits with places for per-visit records
        # Include frecency and hidden for forensic context
        cursor.execute("""
            SELECT
                p.id AS place_id,
                p.url,
                p.title,
                p.visit_count,
                p.typed,
                p.last_visit_date,
                p.frecency,
                p.hidden,
                v.visit_date,
                v.from_visit,
                v.visit_type
            FROM moz_historyvisits v
            JOIN moz_places p ON v.place_id = p.id
            WHERE v.visit_date IS NOT NULL
            ORDER BY v.visit_date DESC
        """)

        for row in cursor:
            visit_time = prtime_to_iso(row["visit_date"]) if row["visit_date"] else None
            last_visit = prtime_to_iso(row["last_visit_date"]) if row["last_visit_date"] else None
            visit_type = row["visit_type"] or 1
            place_id = row["place_id"]

            # Track visit types for warning collection
            found_visit_types.add(visit_type)

            yield FirefoxVisit(
                url=row["url"],
                title=row["title"],
                visit_time_utc=visit_time,
                visit_count=row["visit_count"] or 0,
                typed=row["typed"] or 0,
                last_visit_time_utc=last_visit,
                from_visit=row["from_visit"],
                visit_type=visit_type,
                visit_type_label=get_visit_type_label(visit_type),
                visit_date_raw=row["visit_date"],
                frecency=row["frecency"] or 0,
                hidden=bool(row["hidden"]),
                typed_input=inputhistory.get(place_id),
            )

        # Report unknown visit types
        if warning_collector:
            _check_unknown_visit_types(
                found_visit_types, warning_collector, str(db_path)
            )

    finally:
        conn.close()


def get_history_stats(db_path: Path) -> FirefoxHistoryStats:
    """
    Get statistics from Firefox places.sqlite without full parsing.

    Args:
        db_path: Path to places.sqlite

    Returns:
        FirefoxHistoryStats with counts and date range
    """
    stats = FirefoxHistoryStats()

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return stats

    try:
        cursor = conn.cursor()

        # Count visits
        cursor.execute("SELECT COUNT(*) FROM moz_historyvisits")
        stats.visit_count = cursor.fetchone()[0]

        # Count unique URLs
        cursor.execute("SELECT COUNT(DISTINCT place_id) FROM moz_historyvisits")
        stats.unique_urls = cursor.fetchone()[0]

        # Date range
        cursor.execute("SELECT MIN(visit_date), MAX(visit_date) FROM moz_historyvisits")
        row = cursor.fetchone()
        if row[0]:
            stats.earliest_visit = prtime_to_iso(row[0])
        if row[1]:
            stats.latest_visit = prtime_to_iso(row[1])
    except sqlite3.Error:
        pass
    finally:
        conn.close()

    return stats


# =============================================================================
# Search Query Parsing (Firefox 75+)
# =============================================================================


@dataclass
class FirefoxSearchQuery:
    """Search query from Firefox places metadata."""

    term: str                           # The search query text
    normalized_term: Optional[str]      # Lowercase normalized term
    url: Optional[str]                  # URL of the page visited from search
    title: Optional[str]                # Page title
    search_time_utc: Optional[str]      # ISO 8601 timestamp (from metadata created_at)

    # Metadata context
    place_id: Optional[int] = None      # FK to moz_places
    search_query_id: int = 0            # Original ID in moz_places_metadata_search_queries

    # User interaction metrics (forensically interesting)
    total_view_time_ms: int = 0         # Time spent viewing page
    typing_time_ms: int = 0             # Time spent typing on page
    key_presses: int = 0                # Number of key presses


def parse_search_queries(
    db_path: Path,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Iterator[FirefoxSearchQuery]:
    """
    Parse Firefox search queries from places.sqlite metadata tables.

    Firefox 75+ stores search queries in moz_places_metadata_search_queries,
    linked to pages via moz_places_metadata. This captures actual user
    search intent - high forensic value.

    Args:
        db_path: Path to places.sqlite
        warning_collector: Optional collector for schema warnings

    Yields:
        FirefoxSearchQuery records
    """
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return

    try:
        cursor = conn.cursor()

        # Check if search query tables exist (Firefox 75+)
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('moz_places_metadata_search_queries', 'moz_places_metadata')"
        )
        tables = {row[0] for row in cursor.fetchall()}

        if 'moz_places_metadata_search_queries' not in tables:
            # Table doesn't exist - older Firefox version
            return

        has_metadata = 'moz_places_metadata' in tables

        if has_metadata:
            # Join search queries with metadata and places for full context
            # This gives us the URL, title, and timestamps
            cursor.execute("""
                SELECT
                    sq.id AS search_query_id,
                    sq.terms AS term,
                    p.url,
                    p.title,
                    m.place_id,
                    m.created_at,
                    m.total_view_time,
                    m.typing_time,
                    m.key_presses
                FROM moz_places_metadata_search_queries sq
                LEFT JOIN moz_places_metadata m ON m.search_query_id = sq.id
                LEFT JOIN moz_places p ON m.place_id = p.id
                ORDER BY m.created_at DESC
            """)
        else:
            # Fallback: just the search terms without context
            cursor.execute("""
                SELECT
                    id AS search_query_id,
                    terms AS term,
                    NULL AS url,
                    NULL AS title,
                    NULL AS place_id,
                    NULL AS created_at,
                    NULL AS total_view_time,
                    NULL AS typing_time,
                    NULL AS key_presses
                FROM moz_places_metadata_search_queries
                ORDER BY id
            """)

        for row in cursor:
            # Convert PRTime to ISO if available
            search_time = None
            if row["created_at"]:
                search_time = prtime_to_iso(row["created_at"])

            # Normalize term (lowercase, strip whitespace)
            term = row["term"] or ""
            normalized = term.lower().strip() if term else None

            yield FirefoxSearchQuery(
                term=term,
                normalized_term=normalized,
                url=row["url"],
                title=row["title"],
                search_time_utc=search_time,
                place_id=row["place_id"],
                search_query_id=row["search_query_id"] or 0,
                total_view_time_ms=row["total_view_time"] or 0,
                typing_time_ms=row["typing_time"] or 0,
                key_presses=row["key_presses"] or 0,
            )

    except sqlite3.Error:
        pass
    finally:
        conn.close()


def get_search_query_stats(db_path: Path) -> Dict[str, int]:
    """
    Get quick statistics from Firefox search query tables.

    Args:
        db_path: Path to places.sqlite

    Returns:
        Dict with search_count and unique_terms
    """
    stats = {"search_count": 0, "unique_terms": 0}

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return stats

    try:
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='moz_places_metadata_search_queries'"
        )
        if not cursor.fetchone():
            return stats

        # Count total search queries
        cursor.execute("SELECT COUNT(*) FROM moz_places_metadata_search_queries")
        stats["search_count"] = cursor.fetchone()[0]

        # Unique terms (table has UNIQUE constraint, so same as count)
        stats["unique_terms"] = stats["search_count"]

    except sqlite3.Error:
        pass
    finally:
        conn.close()

    return stats


# =============================================================================
# Schema Warning Helpers
# =============================================================================


def _check_unknown_tables(
    conn: sqlite3.Connection,
    warning_collector: "ExtractionWarningCollector",
    source_file: str,
) -> None:
    """
    Check for unknown tables and report as warnings.

    Args:
        conn: SQLite connection
        warning_collector: Warning collector instance
        source_file: Source file path for warning context
    """
    from extractors._shared.extraction_warnings import discover_unknown_tables
    from ._schemas import KNOWN_PLACES_TABLES, HISTORY_TABLE_PATTERNS

    unknown_tables = discover_unknown_tables(
        conn, KNOWN_PLACES_TABLES, HISTORY_TABLE_PATTERNS
    )

    for table_info in unknown_tables:
        warning_collector.add_unknown_table(
            table_name=table_info["name"],
            columns=table_info["columns"],
            source_file=source_file,
            artifact_type="history",
        )


def _check_unknown_visit_types(
    found_types: Set[int],
    warning_collector: "ExtractionWarningCollector",
    source_file: str,
) -> None:
    """
    Check for unknown visit types and report as warnings.

    Args:
        found_types: Set of visit type integers found during parsing
        warning_collector: Warning collector instance
        source_file: Source file path for warning context
    """
    from extractors._shared.extraction_warnings import track_unknown_values

    track_unknown_values(
        warning_collector=warning_collector,
        known_mapping=VISIT_TYPES,
        found_values=found_types,
        value_name="visit_type",
        source_file=source_file,
        artifact_type="history",
    )
