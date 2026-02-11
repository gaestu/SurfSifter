"""
Firefox Cookies schema definitions for extraction warning support.

This module defines known tables, columns, and enum values for Firefox
cookies databases (cookies.sqlite). Used by the extractor to detect
unknown schemas that may contain valuable forensic data.

Schema Documentation:
- moz_cookies table: Primary cookie storage (Firefox 3+)
- cookies table: Legacy name (Firefox 2 and earlier)

Column Evolution:
- Firefox 3+: host, name, value, path, expiry, isSecure, isHttpOnly, sameSite
- Firefox 60+: originAttributes (container tabs, private browsing, FPI)
- Firefox 86+: schemeMap (for SameSite cookie fixes)

SameSite Values:
- 0 = None (cross-site allowed)
- 1 = Lax (cross-site on navigation)
- 2 = Strict (same-site only)

originAttributes Format:
- Caret-prefixed query string: ^userContextId=1&privateBrowsingId=0
- Components: userContextId, privateBrowsingId, firstPartyDomain, partitionKey

Initial implementation for schema warning support
"""

from __future__ import annotations

from typing import Dict, List, Set


# =============================================================================
# Known Tables
# =============================================================================

# Tables we expect in cookies.sqlite
KNOWN_COOKIES_TABLES: Set[str] = {
    # Modern Firefox (3+)
    "moz_cookies",
    # Legacy Firefox (2 and earlier)
    "cookies",
    # SQLite internal tables (always present)
    "sqlite_sequence",
    "sqlite_stat1",
}

# Patterns for detecting potentially cookie-related unknown tables
# Used to filter which unknown tables are reported (avoid noise)
COOKIES_TABLE_PATTERNS: List[str] = [
    "cookie",
    "moz_",
    "session",
    "storage",
]


# =============================================================================
# Known Columns
# =============================================================================

# Expected columns in moz_cookies table (Firefox 86+)
# Note: Older versions may have fewer columns
KNOWN_MOZ_COOKIES_COLUMNS: Set[str] = {
    # Core cookie data
    "id",
    "name",
    "value",
    "host",
    "path",

    # Timestamps
    "expiry",           # Unix timestamp (seconds)
    "creationTime",     # PRTime (microseconds since 1970)
    "lastAccessed",     # PRTime (microseconds since 1970)

    # Security flags
    "isSecure",
    "isHttpOnly",
    "sameSite",

    # Firefox 60+ privacy features
    "originAttributes",

    # Firefox 68+ (inBrowserElement for web extensions)
    "inBrowserElement",

    # Firefox 86+ (SameSite scheme handling)
    "schemeMap",

    # Firefox 91+ raw SameSite value (for forensic preservation)
    "rawSameSite",

    # Legacy column names (Firefox 2 era)
    "baseDomain",       # Alternative to host in older versions
    "appId",            # App ID for Firefox OS (deprecated)
}


# =============================================================================
# SameSite Value Mapping
# =============================================================================

# Maps Firefox sameSite integer to human-readable value
# Source: https://searchfox.org/mozilla-central/source/netwerk/cookie/nsICookie.idl
SAMESITE_VALUES: Dict[int, str] = {
    0: "None",      # SAMESITE_NONE - Cross-site allowed
    1: "Lax",       # SAMESITE_LAX - Cross-site on navigation
    2: "Strict",    # SAMESITE_STRICT - Same-site only
}


def get_samesite_label(value: int) -> str:
    """
    Convert sameSite integer to human-readable label.

    Args:
        value: Integer sameSite value from moz_cookies

    Returns:
        Label string, or "unknown_{value}" for unknown values
    """
    return SAMESITE_VALUES.get(value, f"unknown_{value}")


# =============================================================================
# originAttributes Keys
# =============================================================================

# Known keys in Firefox originAttributes string
# Format: ^key1=value1&key2=value2
KNOWN_ORIGIN_ATTRIBUTES_KEYS: Set[str] = {
    # Container tabs (Multi-Account Containers extension or built-in)
    "userContextId",        # 0=default, 1+=container tabs

    # Private browsing indicator
    "privateBrowsingId",    # 0=normal, 1=private browsing

    # First-Party Isolation (FPI) - privacy.firstparty.isolate
    "firstPartyDomain",     # Domain for isolation context

    # State Partitioning (Firefox 86+)
    "partitionKey",         # Cross-site tracking protection key

    # Deprecated keys (still may appear in older profiles)
    "appId",                # Firefox OS app ID (deprecated)
    "inIsolatedMozBrowser", # Legacy isolation flag
}


# =============================================================================
# schemeMap Values (Firefox 86+)
# =============================================================================

# schemeMap is a bitmask for SameSite scheme handling
# Bit 0 (1): HTTP scheme set
# Bit 1 (2): HTTPS scheme set
SCHEME_MAP_VALUES: Dict[int, str] = {
    0: "unset",
    1: "http_only",
    2: "https_only",
    3: "both",
}


def get_scheme_map_label(value: int) -> str:
    """
    Convert schemeMap integer to human-readable label.

    Args:
        value: Integer schemeMap value from moz_cookies

    Returns:
        Label string, or "unknown_{value}" for unknown values
    """
    return SCHEME_MAP_VALUES.get(value, f"unknown_{value}")
