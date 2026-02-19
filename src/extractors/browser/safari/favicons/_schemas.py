"""Schema constants for Safari Favicons.db parsing and warning discovery."""

from __future__ import annotations

from typing import Dict, Set

# Safari favicon icon type bit flags (aligned with existing favicon schema usage)
ICON_TYPE_FAVICON = 1
ICON_TYPE_TOUCH_ICON = 2
ICON_TYPE_MASK_ICON = 4

KNOWN_TABLES: Set[str] = {
    "icon_info",
    "page_url",
    "rejected_resources",
    "sqlite_sequence",
    "sqlite_stat1",
    "meta",
}

KNOWN_ICON_INFO_COLUMNS: Set[str] = {
    "uuid",
    "url",
    "timestamp",
    "width",
    "height",
    "has_generated_representations",
}

KNOWN_PAGE_URL_COLUMNS: Set[str] = {
    "uuid",
    "url",
}

KNOWN_COLUMNS_BY_TABLE: Dict[str, Set[str]] = {
    "icon_info": KNOWN_ICON_INFO_COLUMNS,
    "page_url": KNOWN_PAGE_URL_COLUMNS,
}

RELEVANT_TABLE_PATTERNS = (
    "icon",
    "favicon",
    "page",
    "touch",
    "template",
)
