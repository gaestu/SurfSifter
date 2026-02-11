"""
Firefox Downloads schema definitions for schema warning support.

This module defines known tables, columns, annotation attributes, and enum values
for Firefox downloads stored in places.sqlite. Used by the extractor to detect
unknown schemas that may contain forensically valuable data we're not capturing.

Firefox Download Storage:
- Modern Firefox (v26+): moz_annos annotations with downloads/* attributes
- Legacy Firefox (< v26): moz_downloads table

References:
- Mozilla source: toolkit/components/downloads/DownloadHistory.sys.mjs
- Places database: toolkit/components/places/Database.cpp
"""

from __future__ import annotations

from typing import Dict, Set, List


# =============================================================================
# Known Tables
# =============================================================================
# Tables we expect in a Firefox places.sqlite database related to downloads.
# Used to detect if new download-related tables have been added.

KNOWN_PLACES_TABLES: Set[str] = {
    # Core places tables
    "moz_places",
    "moz_historyvisits",
    "moz_bookmarks",
    "moz_bookmarks_deleted",
    "moz_keywords",
    "moz_anno_attributes",
    "moz_annos",
    "moz_items_annos",
    "moz_inputhistory",
    "moz_hosts",
    "moz_origins",
    "moz_meta",
    # Legacy downloads table (Firefox < v26)
    "moz_downloads",
    # Sync tables
    "moz_bookmarks_synced",
    "moz_bookmarks_synced_structure",
    "moz_bookmarks_synced_tag_relation",
    # FTS tables (internal)
    "moz_places_url_hashindex",
    # SQLite internal
    "sqlite_stat1",
    "sqlite_sequence",
}

# Patterns to filter relevant unknown tables (download-related)
DOWNLOADS_TABLE_PATTERNS: List[str] = [
    "download",
    "annos",
    "anno",
]


# =============================================================================
# Known Annotation Attributes
# =============================================================================
# Annotation attribute names we parse for downloads.
# Unknown download-related attributes will be reported as warnings.

KNOWN_DOWNLOAD_ANNOTATIONS: Set[str] = {
    "downloads/destinationFileURI",
    "downloads/metaData",
}

# All known annotation attributes (for filtering unknowns)
KNOWN_ANNOTATION_ATTRIBUTES: Set[str] = {
    # Download annotations
    "downloads/destinationFileURI",
    "downloads/metaData",
    # Bookmark annotations (for reference, not parsed here)
    "bookmarkProperties/description",
    "livemark/feedURI",
    "livemark/siteURI",
    # Page annotations
    "places/pageGuid",
}


# =============================================================================
# Known Metadata JSON Keys
# =============================================================================
# Keys we parse from the downloads/metaData JSON.
# Unknown keys will be reported as warnings.

KNOWN_METADATA_KEYS: Set[str] = {
    "state",
    "endTime",
    "fileSize",
    "deleted",
    "reputationCheckVerdict",
    # Optional/rare keys that may appear
    "startTime",  # Not commonly used but possible
    "referrer",   # Sometimes duplicated in metadata
}


# =============================================================================
# Download State Mapping
# =============================================================================
# Maps Firefox download state codes to human-readable strings.
# Unknown state codes will be reported as warnings.
#
# Source: toolkit/components/downloads/DownloadHistory.sys.mjs

FIREFOX_STATE_MAP: Dict[int, str] = {
    0: "in_progress",
    1: "complete",
    2: "failed",
    3: "cancelled",
    4: "paused",
    5: "blocked_parental",         # METADATA_STATE_BLOCKED_PARENTAL
    6: "dirty",                    # Legacy (see state 8)
    7: "blocked_policy",           # METADATA_STATE_BLOCKED_POLICY
    8: "dirty",                    # METADATA_STATE_DIRTY - blocked by reputation
    9: "blocked_content_analysis", # METADATA_STATE_BLOCKED_CONTENT_ANALYSIS (v115+)
}


# =============================================================================
# Legacy moz_downloads Columns
# =============================================================================
# Columns in the legacy moz_downloads table (Firefox < v26).

KNOWN_LEGACY_DOWNLOADS_COLUMNS: Set[str] = {
    "id",
    "name",
    "source",
    "target",
    "startTime",
    "endTime",
    "state",
    "referrer",
    "entityID",
    "currBytes",
    "maxBytes",
    "mimeType",
    "preferredApplication",
    "preferredAction",
    "autoResume",
}


# =============================================================================
# Reputation Check Verdicts
# =============================================================================
# Known values for reputationCheckVerdict in metaData JSON.
# Unknown verdicts will be reported as warnings.

KNOWN_REPUTATION_VERDICTS: Set[str] = {
    "MALWARE",
    "POTENTIALLY_UNWANTED",
    "UNCOMMON",
    "INSECURE",
    "DOWNLOAD_SPAM",
}


def get_state_label(state_code: int) -> str:
    """
    Convert Firefox download state code to human-readable label.

    Args:
        state_code: Integer state code from metaData JSON

    Returns:
        Human-readable state string, or "unknown_{code}" for unknown codes
    """
    return FIREFOX_STATE_MAP.get(state_code, f"unknown_{state_code}")
