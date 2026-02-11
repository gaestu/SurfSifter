"""
Firefox History schema definitions for extraction warning support.

This module defines known tables, columns, and enum values for Firefox
places.sqlite history database. Unknown items discovered during extraction
are logged as warnings for investigator review and extractor improvement.

Schema Evolution:
- Firefox 3+: moz_places, moz_historyvisits, moz_bookmarks core tables
- Firefox 40+: moz_hosts for domain tracking
- Firefox 55+: moz_origins for origin-based storage
- Firefox 75+: moz_places_metadata for page metadata

References:
- Mozilla source: toolkit/components/places/
- Schema: toolkit/components/places/Database.cpp
- Visit types: toolkit/components/places/nsINavHistoryService.idl

Initial implementation for schema warning support
"""

from __future__ import annotations

from typing import Dict, List, Set


# =============================================================================
# Known Tables in places.sqlite
# =============================================================================
# Tables we know about and either parse or intentionally skip.
# Unknown tables trigger warnings for investigator review.

KNOWN_PLACES_TABLES: Set[str] = {
    # Core history tables we extract
    "moz_places",           # URL storage
    "moz_historyvisits",    # Visit records (per-visit, not per-URL)
    "moz_inputhistory",     # Address bar autocomplete typed input

    # Bookmark tables (extracted by bookmarks extractor)
    "moz_bookmarks",
    "moz_bookmarks_deleted",
    "moz_keywords",         # Bookmark keywords

    # Annotation tables (download metadata, etc.)
    "moz_annos",            # Annotations (downloads metadata)
    "moz_anno_attributes",  # Annotation attribute names
    "moz_items_annos",      # Item annotations

    # Domain/origin tables (lower forensic value)
    "moz_hosts",            # Legacy hostname tracking (Firefox 40-54)
    "moz_origins",          # Origin-based storage (Firefox 55+)

    # Frecency/segment tables
    "moz_frecency_scores",  # Frecency calculation cache

    # Metadata tables (Firefox 75+)
    "moz_places_metadata",
    "moz_places_metadata_search_queries",
    "moz_places_metadata_snapshots",
    "moz_places_metadata_snapshots_extra",
    "moz_places_metadata_snapshots_groups",
    "moz_places_metadata_groups_to_snapshots",
    "moz_session_metadata",
    "moz_session_to_places",

    # Preview images (Firefox 78+)
    "moz_previews_tombstones",

    # SQLite internal tables
    "sqlite_sequence",
    "sqlite_stat1",

    # Meta tables
    "moz_meta",             # Database metadata (schema version, etc.)
}

# Patterns that suggest history-related tables (for discovery filtering)
# Used to filter which unknown tables are reported (reduce noise)
HISTORY_TABLE_PATTERNS: List[str] = [
    "moz_",
    "places",
    "visit",
    "history",
    "url",
    "bookmark",
    "anno",
    "host",
    "origin",
]


# =============================================================================
# Known Columns in Core Tables
# =============================================================================
# Define known columns for high-value tables to detect schema changes.

KNOWN_MOZ_PLACES_COLUMNS: Set[str] = {
    "id",
    "url",
    "url_hash",             # URL hash for fast lookup (Firefox 49+)
    "title",
    "rev_host",             # Reversed hostname for sorting
    "visit_count",
    "hidden",               # 1 = internal URL (redirect, frame, etc.)
    "typed",                # 1 = user typed this URL
    "frecency",             # Frecency score (popularity metric)
    "last_visit_date",      # PRTime (microseconds since 1970)
    "guid",                 # GUID for sync
    "foreign_count",        # Count of foreign key references
    "preview_image_url",    # Preview image URL (Firefox 78+)
    "description",          # Page description (Firefox 78+)
    "site_name",            # Site name (Firefox 78+)
    "origin_id",            # FK to moz_origins (Firefox 55+)
    "recalc_frecency",      # Flag for frecency recalculation
    "alt_frecency",         # Alternative frecency (Firefox 118+)
    "recalc_alt_frecency",  # Flag for alt frecency recalc (Firefox 118+)
}

KNOWN_MOZ_HISTORYVISITS_COLUMNS: Set[str] = {
    "id",
    "from_visit",           # Referrer visit ID (navigation chain)
    "place_id",             # FK to moz_places
    "visit_date",           # PRTime (microseconds since 1970)
    "visit_type",           # Visit type code (1-9, see below)
    "session",              # Session ID (legacy, removed in newer Firefox)
    "source",               # Visit source (Firefox 89+): 0=local, 1=synced
    "triggeringPlaceId",    # Place that triggered this visit (Firefox 110+)
}

KNOWN_MOZ_INPUTHISTORY_COLUMNS: Set[str] = {
    "place_id",             # FK to moz_places
    "input",                # What user typed in address bar
    "use_count",            # Number of times this input led to this URL
}


# =============================================================================
# Search Query Tables (Firefox 75+)
# =============================================================================
# Firefox stores search queries in metadata tables, linked to places.

KNOWN_MOZ_PLACES_METADATA_SEARCH_QUERIES_COLUMNS: Set[str] = {
    "id",                   # Primary key
    "terms",                # The actual search query text (UNIQUE)
}

KNOWN_MOZ_PLACES_METADATA_COLUMNS: Set[str] = {
    "id",                   # Primary key
    "place_id",             # FK to moz_places
    "referrer_place_id",    # FK to referring page's moz_places entry
    "created_at",           # PRTime when metadata was created
    "updated_at",           # PRTime when metadata was last updated
    "total_view_time",      # Total time spent viewing page (ms)
    "typing_time",          # Time spent typing on page (ms)
    "key_presses",          # Number of key presses on page
    "scrolling_time",       # Time spent scrolling (ms)
    "scrolling_distance",   # Total scroll distance (pixels)
    "document_type",        # Document type enum
    "search_query_id",      # FK to moz_places_metadata_search_queries
}


# =============================================================================
# Visit Type Mapping
# =============================================================================
# Firefox visit_type values from nsINavHistoryService.idl
# https://searchfox.org/mozilla-central/source/toolkit/components/places/nsINavHistoryService.idl

VISIT_TYPES: Dict[int, str] = {
    1: "link",                    # TRANSITION_LINK - User clicked a link
    2: "typed",                   # TRANSITION_TYPED - User typed URL in address bar
    3: "bookmark",                # TRANSITION_BOOKMARK - Navigation from bookmark
    4: "embed",                   # TRANSITION_EMBED - Subframe navigation
    5: "redirect_permanent",      # TRANSITION_REDIRECT_PERMANENT - 301 redirect
    6: "redirect_temporary",      # TRANSITION_REDIRECT_TEMPORARY - 302/307 redirect
    7: "download",                # TRANSITION_DOWNLOAD - Download link
    8: "framed_link",             # TRANSITION_FRAMED_LINK - Link in subframe
    9: "reload",                  # TRANSITION_RELOAD - Page reload
}


def get_visit_type_label(visit_type: int) -> str:
    """
    Convert Firefox visit_type integer to human-readable label.

    Args:
        visit_type: Integer visit type from moz_historyvisits

    Returns:
        Label string, or "unknown_{value}" for unknown types
    """
    return VISIT_TYPES.get(visit_type, f"unknown_{visit_type}")


# =============================================================================
# Hidden Flag Values
# =============================================================================
# The 'hidden' column in moz_places indicates URLs that shouldn't appear
# in normal history views. Forensically valuable to detect.

HIDDEN_FLAG_VALUES: Dict[int, str] = {
    0: "visible",           # Normal URL shown in history
    1: "hidden",            # Internal URL (redirect target, frame, etc.)
}


# =============================================================================
# Frecency Constants
# =============================================================================
# Frecency is Firefox's "frequency + recency" ranking. Special values:

FRECENCY_SPECIAL_VALUES: Dict[int, str] = {
    -1: "unvisited_bookmark",    # Bookmarked but never visited
    0: "unknown",                # Default/unset
}
