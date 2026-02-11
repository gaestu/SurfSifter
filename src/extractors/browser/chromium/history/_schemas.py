"""
Chromium History schema definitions for extraction warning support.

This module defines known tables, columns, and enum values for the Chromium
History database. Unknown items discovered during extraction are logged as
warnings for investigator review and extractor improvement.

Schema Evolution:
- Chromium 1-50: Basic urls, visits tables
- Chromium 51+: downloads_url_chains for redirect tracking
- Chromium 70+: keyword_search_terms for omnibox searches
- Chromium 80+: clusters for tab grouping (not yet extracted)
- Chromium 100+: content_annotations for AI features (not yet extracted)

References:
- Chromium source: components/history/core/browser/
- History schema: components/history/core/browser/history_database.cc
"""

from __future__ import annotations

from typing import Dict, List, Set


# =============================================================================
# Known Tables in History Database
# =============================================================================
# Tables we know about and either parse or intentionally skip.
# Unknown tables trigger warnings for investigator review.

KNOWN_HISTORY_TABLES: Set[str] = {
    # Core tables we extract
    "urls",
    "visits",
    "downloads",
    "downloads_url_chains",
    "downloads_slices",
    "keyword_search_terms",

    # Metadata tables (not extracted, but known)
    "meta",
    "sqlite_sequence",

    # Segment/frecency tables (lower forensic value)
    "segments",
    "segment_usage",

    # Sync-related tables
    "typed_url_sync_metadata",
    "history_sync_metadata",

    # Cluster/annotation tables (Chromium 100+, not yet extracted)
    "clusters",
    "clusters_and_visits",
    "context_annotations",
    "content_annotations",
    "visit_context_annotations",

    # Cluster keyword tables (Chromium 115+, )
    "cluster_keywords",
    "cluster_visit_duplicates",

    # Visit source table (Chromium 110+, )
    # Tracks where visits originated from (import, sync, etc.)
    "visit_source",

    # Foreign visits (synced from other devices)
    "foreign_visits",
}

# Patterns that suggest history-related tables (for discovery filtering)
HISTORY_TABLE_PATTERNS: List[str] = [
    "url",
    "visit",
    "download",
    "segment",
    "keyword",
    "search",
    "cluster",
    "annotation",
    "sync",
    "history",
]


# =============================================================================
# Known Columns in Core Tables
# =============================================================================
# Define known columns for high-value tables to detect schema changes.

KNOWN_URLS_COLUMNS: Set[str] = {
    "id",
    "url",
    "title",
    "visit_count",
    "typed_count",
    "last_visit_time",
    "hidden",
}

KNOWN_VISITS_COLUMNS: Set[str] = {
    "id",
    "url",  # References urls.id
    "visit_time",
    "from_visit",
    "transition",
    "segment_id",
    "visit_duration",
    "incremented_omnibox_typed_score",
    # Chromium 100+ additions
    "opener_visit",
    "originator_cache_guid",
    "originator_visit_id",
    "originator_from_visit",
    "originator_opener_visit",
    "is_known_to_sync",
    # Chromium 110+ additions
    "consider_for_ntp_most_visited",
    "publicly_routable",
    # Chromium 120+ additions
    "floc_allowed",
    "app_id",
    "external_referrer_url",
}

KNOWN_KEYWORD_SEARCH_TERMS_COLUMNS: Set[str] = {
    "keyword_id",
    "url_id",
    "term",
    "normalized_term",
}


# =============================================================================
# Page Transition Types
# =============================================================================
# Chromium page transition is a bitmask:
# - Bits 0-7: Core transition type (what caused the navigation)
# - Bits 8+: Qualifier flags (additional context)
#
# Source: ui/base/page_transition_types.h

# Core transition types (bits 0-7, masked with 0xFF)
TRANSITION_CORE_TYPES: Dict[int, str] = {
    0: "LINK",              # User clicked a link
    1: "TYPED",             # User typed URL in omnibox
    2: "AUTO_BOOKMARK",     # Auto-generated from bookmark
    3: "AUTO_SUBFRAME",     # Subframe navigation (ads, iframes)
    4: "MANUAL_SUBFRAME",   # User-initiated subframe nav
    5: "GENERATED",         # Generated (e.g., from JS)
    6: "AUTO_TOPLEVEL",     # Auto navigation at top level
    7: "FORM_SUBMIT",       # Form submission
    8: "RELOAD",            # Page reload
    9: "KEYWORD",           # Omnibox keyword search
    10: "KEYWORD_GENERATED", # Generated from keyword
}

# Transition qualifier flags (bits 8+)
# These can be OR'd together with the core type
TRANSITION_QUALIFIERS: Dict[int, str] = {
    0x00800000: "FORWARD_BACK",      # Forward/back navigation
    0x01000000: "FROM_ADDRESS_BAR",  # Typed in address bar
    0x02000000: "HOME_PAGE",         # Home page navigation
    0x04000000: "FROM_API",          # Initiated by API
    0x08000000: "CHAIN_START",       # Start of redirect chain
    0x10000000: "CHAIN_END",         # End of redirect chain
    0x20000000: "CLIENT_REDIRECT",   # Client-side redirect (JS)
    0x40000000: "SERVER_REDIRECT",   # Server-side redirect (302)
    0x80000000: "IS_REDIRECT_MASK",  # Any redirect
}

# Mask to extract core type from full transition value
TRANSITION_CORE_MASK = 0xFF


def decode_transition_type(transition: int) -> str:
    """
    Decode Chromium page transition to human-readable string.

    Args:
        transition: Raw transition value from visits table

    Returns:
        Human-readable transition name (e.g., "TYPED", "LINK|FROM_ADDRESS_BAR")

    Example:
        >>> decode_transition_type(1)
        "TYPED"
        >>> decode_transition_type(0x01000001)
        "TYPED|FROM_ADDRESS_BAR"
    """
    if transition is None:
        return "UNKNOWN"

    # Extract core type (bits 0-7)
    core_type = transition & TRANSITION_CORE_MASK
    core_name = TRANSITION_CORE_TYPES.get(core_type, f"CORE_{core_type}")

    # Extract qualifier flags
    qualifiers = []
    for flag, name in TRANSITION_QUALIFIERS.items():
        if transition & flag:
            qualifiers.append(name)

    if qualifiers:
        return f"{core_name}|{'|'.join(qualifiers)}"
    return core_name


def get_transition_core_name(transition: int) -> str:
    """
    Get just the core transition type name (without qualifiers).

    Args:
        transition: Raw transition value from visits table

    Returns:
        Core transition name (e.g., "TYPED", "LINK")
    """
    if transition is None:
        return "UNKNOWN"

    core_type = transition & TRANSITION_CORE_MASK
    return TRANSITION_CORE_TYPES.get(core_type, f"UNKNOWN_{core_type}")


# =============================================================================
# Download State and Danger Type Mappings
# =============================================================================
# These are also in _parsers.py but duplicated here for schema warning support

DOWNLOAD_STATE_TYPES: Dict[int, str] = {
    0: "IN_PROGRESS",
    1: "COMPLETE",
    2: "CANCELLED",
    3: "INTERRUPTED",
    4: "INTERRUPTED_NETWORK",
}

DOWNLOAD_DANGER_TYPES: Dict[int, str] = {
    0: "NOT_DANGEROUS",
    1: "DANGEROUS_FILE",
    2: "DANGEROUS_URL",
    3: "DANGEROUS_CONTENT",
    4: "MAYBE_DANGEROUS_CONTENT",
    5: "UNCOMMON_CONTENT",
    6: "USER_VALIDATED",
    7: "DANGEROUS_HOST",
    8: "POTENTIALLY_UNWANTED",
    9: "ALLOWLISTED_BY_POLICY",
    10: "ASYNC_SCANNING",
    11: "BLOCKED_PASSWORD_PROTECTED",
    12: "BLOCKED_TOO_LARGE",
    13: "SENSITIVE_CONTENT_WARNING",
    14: "SENSITIVE_CONTENT_BLOCK",
    15: "DEEP_SCANNED_SAFE",
    16: "DEEP_SCANNED_OPENED_DANGEROUS",
    17: "PROMPT_FOR_SCANNING",
    18: "BLOCKED_UNSUPPORTED_FILETYPE",
    19: "DANGEROUS_ACCOUNT_COMPROMISE",
}
