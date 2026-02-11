"""
Firefox Favicons schema definitions.

This module defines known database tables and columns for Firefox favicons.sqlite
databases across different Firefox versions. Used for schema warning discovery
to detect unknown tables/columns that may contain forensic data.

Schema Evolution:
- Firefox 55+ (2017): moz_icons, moz_pages_w_icons, moz_icons_to_pages
- Firefox < 55: moz_favicons (legacy table)

References:
- Mozilla source: toolkit/components/places/FaviconHelpers.cpp
- Schema: toolkit/components/places/nsPlacesTables.h
"""

from __future__ import annotations

from typing import Dict, Set, List


# =============================================================================
# Known Tables
# =============================================================================

KNOWN_FAVICONS_TABLES: Set[str] = {
    # Modern schema (Firefox 55+)
    "moz_icons",
    "moz_pages_w_icons",
    "moz_icons_to_pages",
    # Legacy schema (Firefox < 55)
    "moz_favicons",
    # SQLite internal
    "sqlite_sequence",
    "sqlite_stat1",
}

# Table patterns for filtering relevant unknown tables
# (tables matching these patterns are likely favicon-related)
FAVICONS_TABLE_PATTERNS: List[str] = [
    "icon",
    "favicon",
    "moz_",
]


# =============================================================================
# Known Columns by Table
# =============================================================================

# Modern schema: moz_icons (Firefox 55+)
# Stores actual icon data with deduplication via fixed_icon_url_hash
KNOWN_MOZ_ICONS_COLUMNS: Set[str] = {
    "id",
    "icon_url",
    "fixed_icon_url_hash",  # Numeric hash for deduplication, NOT a URL
    "width",
    "root",  # 1 = touch icon (apple-touch-icon), 0 = standard favicon
    "color",  # Theme color (rarely populated)
    "expire_ms",  # Expiration timestamp in milliseconds
    "data",  # Icon binary data (BLOB)
    # Columns added in later Firefox versions
    "flags",  # Firefox 78+
}

# Modern schema: moz_pages_w_icons (Firefox 55+)
# Maps page URLs to icons
KNOWN_MOZ_PAGES_W_ICONS_COLUMNS: Set[str] = {
    "id",
    "page_url",
    "page_url_hash",  # Numeric hash for fast lookup
}

# Modern schema: moz_icons_to_pages (Firefox 55+)
# Many-to-many relationship between icons and pages
KNOWN_MOZ_ICONS_TO_PAGES_COLUMNS: Set[str] = {
    "icon_id",
    "page_id",
    "expire_ms",  # Per-mapping expiration (can differ from icon's expire_ms)
}

# Legacy schema: moz_favicons (Firefox < 55)
# Single table with icon data and URL
KNOWN_MOZ_FAVICONS_COLUMNS: Set[str] = {
    "id",
    "url",
    "data",
    "mime_type",
    "expiration",
    # Some versions had additional columns
    "guid",
}


# =============================================================================
# Icon Type Mappings
# =============================================================================

# Root flag values in moz_icons
ICON_ROOT_TYPES: Dict[int, str] = {
    0: "favicon",  # Standard favicon (16x16, 32x32)
    1: "touch_icon",  # Apple touch icon (larger, typically 180x180)
}


# =============================================================================
# Utility Functions
# =============================================================================

def get_icon_type_name(root_value: int) -> str:
    """Get human-readable icon type name from root flag value."""
    return ICON_ROOT_TYPES.get(root_value, f"UNKNOWN_{root_value}")


def get_known_columns_for_table(table_name: str) -> Set[str]:
    """Get known columns for a specific table."""
    mapping = {
        "moz_icons": KNOWN_MOZ_ICONS_COLUMNS,
        "moz_pages_w_icons": KNOWN_MOZ_PAGES_W_ICONS_COLUMNS,
        "moz_icons_to_pages": KNOWN_MOZ_ICONS_TO_PAGES_COLUMNS,
        "moz_favicons": KNOWN_MOZ_FAVICONS_COLUMNS,
    }
    return mapping.get(table_name, set())
