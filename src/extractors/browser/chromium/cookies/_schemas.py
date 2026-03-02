"""
Chromium Cookie schema definitions for schema warning support.

This module defines known tables, columns, and enum values for the Chromium
cookie database. Used by the extractor to detect unknown schemas that may
contain forensically valuable data we're not capturing.

Schema Evolution:
- Chromium <80: Basic columns (host_key, name, value, path, timestamps, flags)
- Chromium 80+: samesite column added
- Chromium 86+: source_scheme, source_port columns added
- Chromium 100+: top_frame_site_key for partitioned cookies
- Chromium 115+: last_update_utc column added

References:
- Chromium source: net/extras/sqlite/sqlite_persistent_cookie_store.cc
- Cookie partitioning: chromium.org/updates/chips
"""

from __future__ import annotations

from typing import Dict, Set, List


# =============================================================================
# Known Tables
# =============================================================================
# Tables we expect in a Chromium Cookies database.
# Used to detect if new tables have been added.

KNOWN_COOKIES_TABLES: Set[str] = {
    "cookies",
    "meta",  # Schema version tracking
}

# Patterns to filter relevant unknown tables (cookie-related)
COOKIES_TABLE_PATTERNS: List[str] = [
    "cookie",
    "site",
    "partition",
]


# =============================================================================
# Known Columns (cookies table)
# =============================================================================
# Columns we currently parse from the cookies table.
# Unknown columns will be reported as warnings.

KNOWN_COOKIES_COLUMNS: Set[str] = {
    # Core cookie data
    "host_key",
    "name",
    "value",
    "path",

    # Timestamps (WebKit format)
    "creation_utc",
    "expires_utc",
    "last_access_utc",

    # Security flags (modern names)
    "is_secure",
    "is_httponly",
    "samesite",

    # Security flags (legacy names, Chromium <67 / CefSharp)
    "secure",       # → is_secure
    "httponly",      # → is_httponly
    "firstpartyonly",  # → samesite

    # Persistence
    "is_persistent",
    "persistent",   # Legacy name for is_persistent
    "has_expires",
    "priority",

    # Encryption
    "encrypted_value",
}

# Columns we know exist but don't currently extract
# (documented for future reference, will show as warnings)
KNOWN_BUT_NOT_PARSED_COLUMNS: Set[str] = {
    "source_scheme",      # Chromium 86+: 0=unset, 1=nonsecure, 2=secure
    "source_port",        # Chromium 86+: Port cookie was set from (-1 if unknown)
    "last_update_utc",    # Chromium 115+: Last modification time
    "top_frame_site_key", # Chromium 100+: Partitioned cookies (CHIPS)
    # Edge-specific columns
    "browser_provenance", # Edge: Tracks if cookie was synced from other browser
    "is_edgelegacycookie",# Edge: Flag for cookies migrated from EdgeHTML
}

# Legacy column name aliases.
# Old Chromium (<67) and CefSharp/CEF embedded browsers used shorter names
# that were later prefixed with "is_" in newer Chromium versions.
# Key = modern name expected by our parser, Values = legacy alternatives.
LEGACY_COLUMN_ALIASES: Dict[str, List[str]] = {
    "is_secure": ["secure"],           # Chromium <67
    "is_httponly": ["httponly"],        # Chromium <67
    "is_persistent": ["persistent"],   # Chromium <67
    "samesite": ["firstpartyonly"],    # Chromium <76
}

# All legacy names (flat set for schema detection)
LEGACY_COLUMN_NAMES: Set[str] = {
    alias
    for aliases in LEGACY_COLUMN_ALIASES.values()
    for alias in aliases
}


# =============================================================================
# SameSite Attribute Mapping
# =============================================================================
# Maps Chromium samesite integer values to human-readable strings.
# Source: net/cookies/cookie_constants.h

SAMESITE_VALUES: Dict[int, str] = {
    -1: "unspecified",     # No SameSite attribute set
    0: "no_restriction",   # SameSite=None
    1: "lax",              # SameSite=Lax (default since Chrome 80)
    2: "strict",           # SameSite=Strict
}

# Set of known values for tracking unknown ones
KNOWN_SAMESITE_VALUES: Set[int] = set(SAMESITE_VALUES.keys())


# =============================================================================
# Priority Mapping
# =============================================================================
# Cookie priority values (rarely used, mainly for first-party sets)

PRIORITY_VALUES: Dict[int, str] = {
    0: "low",
    1: "medium",  # Default
    2: "high",
}

KNOWN_PRIORITY_VALUES: Set[int] = set(PRIORITY_VALUES.keys())


# =============================================================================
# Source Scheme Mapping (Chromium 86+)
# =============================================================================
# We don't extract these yet, but document for future use

SOURCE_SCHEME_VALUES: Dict[int, str] = {
    0: "unset",
    1: "non_secure",  # HTTP
    2: "secure",      # HTTPS
}


# =============================================================================
# Helper Functions
# =============================================================================

def get_samesite_name(value: int) -> str:
    """
    Convert samesite integer to human-readable name.

    Args:
        value: Chromium samesite integer value

    Returns:
        Human-readable string, or "unknown_{value}" if not recognized
    """
    return SAMESITE_VALUES.get(value, f"unknown_{value}")


def get_priority_name(value: int) -> str:
    """
    Convert priority integer to human-readable name.

    Args:
        value: Chromium priority integer value

    Returns:
        Human-readable string, or "unknown_{value}" if not recognized
    """
    return PRIORITY_VALUES.get(value, f"unknown_{value}")
