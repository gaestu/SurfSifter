"""
Extractor Section Definitions — Shared across UI components.

Defines the logical grouping and ordering of extractors for:
- Extractors Tab (main view)
- Batch Extraction Dialog
- Extract & Ingest Dialog

Each section has:
- name: Display name (shown in UI)
- icon: Emoji icon for visual identification
- extractors: List of extractor internal names (metadata.name)
- collapsed: Default collapsed state (True = collapsed by default)
- auto_populate: If True, collects any unmapped extractors (only for "Other" section)
"""

from typing import List, TypedDict, Optional


class ExtractorSection(TypedDict):
    """Type definition for an extractor section."""
    name: str
    icon: str
    extractors: List[str]
    collapsed: bool
    auto_populate: Optional[bool]


EXTRACTOR_SECTIONS: List[ExtractorSection] = [
    {
        "name": "System Tools",
        "icon": "🖥️",
        "extractors": [
            "file_list",
            "system_registry",
            "system_jump_lists",
        ],
        "collapsed": True,
        "auto_populate": False,
    },
    {
        "name": "General Forensic Tools",
        "icon": "🔍",
        "extractors": ["bulk_extractor"],
        "collapsed": True,
        "auto_populate": False,
    },
    {
        "name": "Media Extraction",
        "icon": "🖼️",
        "extractors": ["filesystem_images", "foremost_carver", "scalpel"],
        "collapsed": True,
        "auto_populate": False,
    },
    {
        "name": "Chromium Browsers",
        "icon": "🔷",
        "extractors": [
            # Common artifacts
            "chromium_history",
            "chromium_bookmarks",
            "chromium_cookies",
            "chromium_downloads",
            "chromium_autofill",
            "chromium_sessions",
            "chromium_permissions",
            # Advanced artifacts
            "chromium_extensions",
            "chromium_favicons",
            "chromium_browser_storage",
            "chromium_sync_data",
            "chromium_transport_security",
            "chromium_site_engagement",
            # Chromium-specific (cache, media)
            "cache_simple",
            "media_history",
        ],
        "collapsed": True,
        "auto_populate": False,
    },
    {
        "name": "Firefox Browser",
        "icon": "🦊",
        "extractors": [
            # Common artifacts
            "firefox_history",
            "firefox_bookmarks",
            "firefox_cookies",
            "firefox_downloads",
            "firefox_autofill",
            "firefox_sessions",
            "firefox_permissions",
            # Advanced artifacts
            "firefox_extensions",
            "firefox_favicons",
            "firefox_browser_storage",
            "firefox_sync_data",
            "firefox_transport_security",
            # Firefox-specific (cache)
            "cache_firefox",
        ],
        "collapsed": True,
        "auto_populate": False,
    },
    {
        "name": "Safari Browser (macOS)",
        "icon": "🍎",
        "extractors": [
            "safari_history",
            "safari_bookmarks",
            "safari_cookies",
            "safari_downloads",
            "safari_favicons",
            "safari_sessions",
            "safari_cache",
        ],
        "collapsed": True,
        "auto_populate": False,
    },
    {
        "name": "Internet Explorer / Legacy Edge",
        "icon": "🌐",
        "extractors": [
            # WebCache-based (shared database)
            "ie_webcache",
            "ie_history",
            "ie_cookies",
            "ie_downloads",
            "ie_cache_metadata",
            # File-based artifacts
            "ie_inetcookies",
            "ie_dom_storage",
            "ie_favorites",
            "ie_typed_urls",
            "ie_tab_recovery",
            # Legacy Edge specific
            "edge_legacy_container",
            "edge_reading_list",
        ],
        "collapsed": True,
        "auto_populate": False,
    },
    {
        "name": "Advanced Forensics (Experimental)",
        "icon": "🧪",
        "extractors": [
            "browser_carver",
        ],
        "collapsed": True,
        "auto_populate": False,
    },
    {
        "name": "Other",
        "icon": "❓",
        "extractors": [],  # Auto-populated with unmapped extractors
        "collapsed": True,
        "auto_populate": True,
    },
]


def get_extractor_section(extractor_name: str) -> Optional[str]:
    """
    Get the section name for a given extractor.

    Args:
        extractor_name: Internal name of the extractor (metadata.name).

    Returns:
        Section name, or None if not found (will go to "Other").
    """
    for section in EXTRACTOR_SECTIONS:
        if extractor_name in section["extractors"]:
            return section["name"]
    return None


def get_all_mapped_extractors() -> set:
    """Return a set of all extractor names that are explicitly mapped to sections."""
    mapped = set()
    for section in EXTRACTOR_SECTIONS:
        if not section.get("auto_populate", False):
            mapped.update(section["extractors"])
    return mapped


def group_extractors_by_section(extractors: list) -> dict:
    """
    Group a list of extractor instances by section.

    Args:
        extractors: List of extractor instances (with .metadata.name attribute).

    Returns:
        Dict mapping section names to lists of extractors.
        Sections are in the order defined by EXTRACTOR_SECTIONS.
    """
    # Build mapping of extractor name to section index
    name_to_section_idx = {}
    for idx, section in enumerate(EXTRACTOR_SECTIONS):
        for ext_name in section["extractors"]:
            name_to_section_idx[ext_name] = idx

    # Group extractors
    grouped = {section["name"]: [] for section in EXTRACTOR_SECTIONS}

    for extractor in extractors:
        name = extractor.metadata.name
        section_idx = name_to_section_idx.get(name)

        if section_idx is not None:
            section_name = EXTRACTOR_SECTIONS[section_idx]["name"]
            grouped[section_name].append(extractor)
        else:
            # Unmapped extractors go to "Other"
            grouped["Other"].append(extractor)

    return grouped


def get_section_by_name(name: str) -> Optional[ExtractorSection]:
    """Get a section definition by its name."""
    for section in EXTRACTOR_SECTIONS:
        if section["name"] == name:
            return section
    return None
