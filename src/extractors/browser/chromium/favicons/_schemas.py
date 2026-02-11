"""
Chromium Favicons & Top Sites Schema Definitions.

This module defines known tables, columns, and constants for schema discovery
and warning generation. Unknown items are reported to help investigators
understand what data might not be fully parsed.

Initial schema warning support
"""
from __future__ import annotations

from typing import Dict, Set


# =============================================================================
# Icon Type Constants
# =============================================================================
# From Chromium source: components/favicon_base/favicon_types.h
# These are bitmasks that can be combined.

ICON_TYPES: Dict[int, str] = {
    0: "INVALID",
    1: "FAVICON",           # Standard favicon.ico
    2: "TOUCH_ICON",        # Apple touch icon
    4: "TOUCH_PRECOMPOSED", # Apple touch icon (precomposed)
    8: "WEB_MANIFEST_ICON", # Web manifest icon (Chrome 70+)
}


def get_icon_type_name(icon_type: int) -> str:
    """
    Get human-readable icon type name.

    Args:
        icon_type: Icon type code from favicons table

    Returns:
        Human-readable name, or "UNKNOWN_{code}" for unmapped types
    """
    if icon_type in ICON_TYPES:
        return ICON_TYPES[icon_type]

    # Check if it's a combination of known flags
    names = []
    for bit, name in ICON_TYPES.items():
        if bit > 0 and (icon_type & bit):
            names.append(name)

    if names:
        return "|".join(names)

    return f"UNKNOWN_{icon_type}"


# =============================================================================
# Known Tables for Schema Discovery
# =============================================================================
# These sets define tables we know about. Any tables NOT in these sets
# will be flagged as unknown for investigator review.

KNOWN_FAVICONS_DB_TABLES: Set[str] = {
    # SQLite internal
    "sqlite_sequence",
    "sqlite_stat1",

    # Schema versioning
    "meta",

    # Core favicon tables
    "favicons",           # Icon URLs and types
    "favicon_bitmaps",    # Actual icon image data
    "icon_mapping",       # Page URL -> icon URL mapping

    # Legacy tables (older Chromium versions)
    "thumbnails",         # Deprecated, superseded by favicon_bitmaps
}

KNOWN_TOP_SITES_DB_TABLES: Set[str] = {
    # SQLite internal
    "sqlite_sequence",
    "sqlite_stat1",

    # Schema versioning
    "meta",

    # Core top sites tables
    "top_sites",          # Frequently visited sites

    # Legacy/auxiliary tables
    "thumbnails",         # Site thumbnails (older versions)
    "most_visited_tiles", # Alternative table name (some versions)
}


# =============================================================================
# Table Discovery Patterns
# =============================================================================
# Patterns to identify tables that MIGHT be relevant even if unknown.
# These help filter sqlite_master results to find potentially interesting tables.

FAVICON_TABLE_PATTERNS: list[str] = [
    "favicon",
    "icon",
    "bitmap",
    "mapping",
    "thumb",
]

TOP_SITES_TABLE_PATTERNS: list[str] = [
    "top_site",
    "top_sites",
    "thumbnail",
    "most_visited",
    "frequent",
    "tile",
]


# =============================================================================
# Known Columns for Schema Discovery
# =============================================================================
# These sets define columns we parse from each table. Unknown columns will be
# flagged as potentially containing unextracted forensic data.

# Favicons database: favicons table
KNOWN_FAVICONS_TABLE_COLUMNS: Set[str] = {
    "id",
    "url",
    "icon_type",
}

# Favicons database: favicon_bitmaps table
KNOWN_FAVICON_BITMAPS_COLUMNS: Set[str] = {
    "id",
    "icon_id",
    "last_updated",
    "image_data",
    "width",
    "height",
    "last_requested",  # Added in later Chromium versions
}

# Favicons database: icon_mapping table
KNOWN_ICON_MAPPING_COLUMNS: Set[str] = {
    "id",
    "page_url",
    "icon_id",
}

# Top Sites database: top_sites table
KNOWN_TOP_SITES_TABLE_COLUMNS: Set[str] = {
    "url",
    "url_rank",
    "title",
    "redirects",       # JSON array of redirect URLs
}

# Top Sites database: thumbnails table (legacy)
KNOWN_THUMBNAILS_COLUMNS: Set[str] = {
    "url",
    "url_rank",
    "title",
    "thumbnail",
    "redirects",
    "boring_score",
    "good_clipping",
    "at_top",
    "last_updated",
    "load_completed",
    "last_forced",
}


# =============================================================================
# Convenience Mappings
# =============================================================================
# Map table names to their known column sets for easy lookup

FAVICONS_DB_COLUMN_MAP: Dict[str, Set[str]] = {
    "favicons": KNOWN_FAVICONS_TABLE_COLUMNS,
    "favicon_bitmaps": KNOWN_FAVICON_BITMAPS_COLUMNS,
    "icon_mapping": KNOWN_ICON_MAPPING_COLUMNS,
}

TOP_SITES_DB_COLUMN_MAP: Dict[str, Set[str]] = {
    "top_sites": KNOWN_TOP_SITES_TABLE_COLUMNS,
    "thumbnails": KNOWN_THUMBNAILS_COLUMNS,
}
