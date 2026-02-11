"""
Internet Explorer & Legacy Edge file path patterns.

Covers:
- Internet Explorer 10/11
- Legacy Edge (EdgeHTML/UWP) - Pre-Chromium Microsoft Edge (2015-2020)

These browsers share the WebCacheV01.dat database but have different
paths for other artifacts (cookies, favorites, cache files).

Usage:
    from extractors.browser.ie_legacy._patterns import (
        IE_BROWSERS,
        IE_ARTIFACTS,
        get_patterns,
    )

    # Get all webcache patterns for IE
    patterns = get_patterns("ie", "webcache")

    # Get all patterns for all IE-family browsers
    all_patterns = get_all_patterns("webcache")
"""

from __future__ import annotations

from typing import Dict, List, Any


# Browser definitions
# Keys are browser identifiers, values contain metadata and paths
IE_BROWSERS: Dict[str, Dict[str, Any]] = {
    # =========================================================================
    # Internet Explorer
    # =========================================================================
    "ie": {
        "display_name": "Internet Explorer",
        "description": "Microsoft Internet Explorer 10/11",
    },
    "ie_system": {
        "display_name": "Internet Explorer (System)",
        "description": "System profile IE artifacts",
    },
    "ie_old_windows": {
        "display_name": "Internet Explorer (Windows.old)",
        "description": "IE artifacts from previous Windows installation",
    },
    # =========================================================================
    # Legacy Edge (EdgeHTML/UWP)
    # =========================================================================
    "edge_legacy": {
        "display_name": "Microsoft Edge (Legacy)",
        "description": "Pre-Chromium EdgeHTML/UWP browser (2015-2020)",
    },
    "edge_legacy_old_windows": {
        "display_name": "Microsoft Edge Legacy (Windows.old)",
        "description": "Legacy Edge artifacts from previous Windows installation",
    },
}


# Artifact type definitions with path patterns
# Each artifact type has patterns for both IE and Legacy Edge
IE_ARTIFACTS: Dict[str, Dict[str, List[str]]] = {
    # =========================================================================
    # WebCache Database (ESE format)
    # Contains: History, Cookies, Downloads, Cache metadata, DOM Storage
    # =========================================================================
    "webcache": {
        "ie": [
            # Primary user WebCache
            "Users/*/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat",
            # Backup/temp files
            "Users/*/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.tmp",
        ],
        "ie_system": [
            # System profile (services, scheduled tasks)
            "Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat",
            "Windows/SysWOW64/config/systemprofile/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat",
        ],
        "ie_old_windows": [
            # Previous Windows installation (upgrade scenarios)
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat",
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.tmp",
            # System profile in old Windows
            "Windows.old/Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat",
        ],
        "edge_legacy": [
            # Legacy Edge uses the same WebCache database as IE
            "Users/*/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.dat",
        ],
    },

    # =========================================================================
    # WebCache Journal/Log Files
    # These are ESE transaction logs needed for database recovery
    # =========================================================================
    "webcache_logs": {
        "ie": [
            "Users/*/AppData/Local/Microsoft/Windows/WebCache/V01.log",
            "Users/*/AppData/Local/Microsoft/Windows/WebCache/V01.chk",
            "Users/*/AppData/Local/Microsoft/Windows/WebCache/V01*.log",
            "Users/*/AppData/Local/Microsoft/Windows/WebCache/V01res*.jrs",
            "Users/*/AppData/Local/Microsoft/Windows/WebCache/V01tmp.log",
            "Users/*/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.jfm",
        ],
        "ie_system": [
            "Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/WebCache/V01*.log",
            "Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/WebCache/V01.chk",
            "Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/WebCache/V01res*.jrs",
            "Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.jfm",
        ],
        "ie_old_windows": [
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/WebCache/V01*.log",
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/WebCache/V01.chk",
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/WebCache/V01res*.jrs",
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/WebCache/WebCacheV01.jfm",
        ],
        "edge_legacy": [
            "Users/*/AppData/Local/Microsoft/Windows/WebCache/V01*.log",
        ],
    },

    # =========================================================================
    # History (Legacy Edge container.dat format)
    # Note: IE history is in WebCache, but Legacy Edge also has container.dat
    # =========================================================================
    "history": {
        "ie": [
            # IE history is stored in WebCache, no separate files
        ],
        "edge_legacy": [
            # UWP package container files
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/History/container.dat",
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/#!*/MicrosoftEdge/History/container.dat",
        ],
    },

    # =========================================================================
    # Cookies
    # =========================================================================
    "cookies": {
        "ie": [
            # Individual cookie text files
            "Users/*/AppData/Local/Microsoft/Windows/INetCookies/*.cookie",
            "Users/*/AppData/Local/Microsoft/Windows/INetCookies/**/*.cookie",
            "Users/*/AppData/Roaming/Microsoft/Windows/Cookies/*.txt",
            "Users/*/AppData/Roaming/Microsoft/Windows/Cookies/Low/*.txt",
            # Legacy paths (older Windows)
            "Users/*/Cookies/*.txt",
        ],
        "edge_legacy": [
            # UWP package cookie files
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/Cookies/*.cookie",
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/#!*/MicrosoftEdge/Cookies/*.cookie",
        ],
    },

    # =========================================================================
    # Favorites / Bookmarks (.url files)
    # =========================================================================
    "favorites": {
        "ie": [
            "Users/*/Favorites/*.url",
            "Users/*/Favorites/**/*.url",
        ],
        "ie_old_windows": [
            "Windows.old/Users/*/Favorites/*.url",
            "Windows.old/Users/*/Favorites/**/*.url",
        ],
        "edge_legacy": [
            # Legacy Edge shares IE favorites
            "Users/*/Favorites/*.url",
            "Users/*/Favorites/**/*.url",
        ],
    },

    # =========================================================================
    # Cache Files
    # =========================================================================
    "cache": {
        "ie": [
            # Temporary Internet Files
            "Users/*/AppData/Local/Microsoft/Windows/INetCache/IE/**/*",
            "Users/*/AppData/Local/Microsoft/Windows/Temporary Internet Files/Content.IE5/**/*",
            # Low integrity cache
            "Users/*/AppData/Local/Microsoft/Windows/INetCache/Low/**/*",
        ],
        "ie_system": [
            # System profile cache
            "Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/INetCache/IE/**/*",
        ],
        "ie_old_windows": [
            # Previous Windows installation cache
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/INetCache/IE/**/*",
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/Temporary Internet Files/Content.IE5/**/*",
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/INetCache/Low/**/*",
        ],
        "edge_legacy": [
            # UWP package cache
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/Cache/**/*",
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/#!*/MicrosoftEdge/Cache/**/*",
        ],
    },

    # =========================================================================
    # Downloads (iedownload container in WebCache)
    # =========================================================================
    "downloads": {
        "ie": [
            # Downloads are in WebCache database, no separate files
        ],
        "edge_legacy": [
            # Legacy Edge downloads also in WebCache
        ],
    },

    # =========================================================================
    # Typed URLs (Registry-based)
    # Note: These require registry hive parsing
    # =========================================================================
    "typed_urls": {
        "ie": [
            # NTUSER.DAT contains HKCU\Software\Microsoft\Internet Explorer\TypedURLs
            "Users/*/NTUSER.DAT",
        ],
        "ie_old_windows": [
            # Previous Windows installation registry
            "Windows.old/Users/*/NTUSER.DAT",
        ],
        "edge_legacy": [
            # Same registry location
            "Users/*/NTUSER.DAT",
        ],
    },

    # =========================================================================
    # Reading List (Legacy Edge only)
    # =========================================================================
    "reading_list": {
        "ie": [],
        "edge_legacy": [
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/User/Default/ReadingList/*",
        ],
    },

    # =========================================================================
    # DOM Storage / Web Storage
    # =========================================================================
    "dom_storage": {
        "ie": [
            # IE DOMStore is in WebCache database (container-based)
            # No separate files for IE
        ],
        "edge_legacy": [
            # Edge Legacy file-based DOMStore
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/User/Default/DOMStore/**/*",
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/#!*/MicrosoftEdge/User/Default/DOMStore/**/*",
        ],
    },

    # =========================================================================
    # File-based Cookies (INetCookies - separate from WebCache)
    # These are individual .cookie files outside the WebCache database
    # =========================================================================
    "inetcookies": {
        "ie": [
            # Windows 10+ INetCookies folder
            "Users/*/AppData/Local/Microsoft/Windows/INetCookies/*.cookie",
            "Users/*/AppData/Local/Microsoft/Windows/INetCookies/**/*.cookie",
            # Low integrity cookies (sandboxed)
            "Users/*/AppData/Local/Microsoft/Windows/INetCookies/Low/*.cookie",
            "Users/*/AppData/Local/Microsoft/Windows/INetCookies/Low/**/*.cookie",
            # Legacy text-based cookies (Windows 7/8)
            "Users/*/AppData/Roaming/Microsoft/Windows/Cookies/*.txt",
            "Users/*/AppData/Roaming/Microsoft/Windows/Cookies/Low/*.txt",
            # Very old Windows XP-style paths
            "Users/*/Cookies/*.txt",
        ],
        "ie_system": [
            "Windows/System32/config/systemprofile/AppData/Local/Microsoft/Windows/INetCookies/*.cookie",
        ],
        "ie_old_windows": [
            # Previous Windows installation cookies
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/INetCookies/*.cookie",
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/INetCookies/**/*.cookie",
            "Windows.old/Users/*/AppData/Local/Microsoft/Windows/INetCookies/Low/*.cookie",
            "Windows.old/Users/*/AppData/Roaming/Microsoft/Windows/Cookies/*.txt",
        ],
        "edge_legacy": [
            # Edge Legacy file-based cookies (UWP)
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/Cookies/*.cookie",
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/#!*/MicrosoftEdge/Cookies/*.cookie",
        ],
        "edge_legacy_old_windows": [
            # Previous Windows installation Legacy Edge cookies
            "Windows.old/Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/Cookies/*.cookie",
            "Windows.old/Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/#!*/MicrosoftEdge/Cookies/*.cookie",
        ],
    },

    # =========================================================================
    # Flash LSO (Local Shared Objects) - Legacy plugin storage
    # High forensic value for older evidence images
    # =========================================================================
    "flash_lso": {
        "ie": [
            "Users/*/AppData/Roaming/Macromedia/Flash Player/#SharedObjects/**/*.sol",
            "Users/*/AppData/Roaming/Macromedia/Flash Player/macromedia.com/support/flashplayer/sys/**/*",
        ],
        "ie_system": [],
        "edge_legacy": [],  # Edge Legacy did not support Flash
    },

    # =========================================================================
    # Silverlight Isolated Storage - Legacy plugin storage
    # =========================================================================
    "silverlight_storage": {
        "ie": [
            "Users/*/AppData/LocalLow/Microsoft/Silverlight/is/**/*",
            "Users/*/AppData/LocalLow/Microsoft/Silverlight/outofbrowser/**/*",
        ],
        "ie_system": [],
        "edge_legacy": [],  # Edge Legacy dropped Silverlight
    },

    # =========================================================================
    # RSS/Atom Feed Subscriptions
    # =========================================================================
    "feeds": {
        "ie": [
            "Users/*/Feeds/*.feed-ms",
            "Users/*/Feeds/**/*.feed-ms",
            # Common Feed Store (Windows indexing)
            "Users/*/AppData/Local/Microsoft/Feeds/**/*",
        ],
        "ie_system": [],
        "edge_legacy": [
            # Edge Legacy reading list (similar purpose)
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/User/Default/ReadingList/*",
        ],
    },

    # =========================================================================
    # Protected Mode / Low Integrity Cache
    # Sandboxed content has forensic significance
    # =========================================================================
    "protected_mode_cache": {
        "ie": [
            # Low integrity (Protected Mode) cache
            "Users/*/AppData/Local/Microsoft/Windows/INetCache/Low/**/*",
            "Users/*/AppData/Local/Microsoft/Windows/INetCache/Low/Content.IE5/**/*",
            # Virtualized paths for sandboxed IE
            "Users/*/AppData/Local/Microsoft/Windows/INetCache/Virtualized/**/*",
        ],
        "ie_system": [],
        "edge_legacy": [
            # AppContainer isolation paths
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/INetCache/**/*",
        ],
    },

    # =========================================================================
    # InPrivate / Private Browsing Indicators
    # Recovery of supposedly deleted private sessions
    # =========================================================================
    "inprivate": {
        "ie": [
            # InPrivate Filtering settings
            "Users/*/AppData/Local/Microsoft/Internet Explorer/Recovery/InPrivate/**/*",
            # Recovery files may contain InPrivate session remnants
            "Users/*/AppData/Local/Microsoft/Internet Explorer/Recovery/**/*",
        ],
        "ie_system": [],
        "edge_legacy": [
            # Edge Legacy InPrivate
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/User/Default/Recovery/**/*",
        ],
    },

    # =========================================================================
    # Browser Recovery / Session Restore
    # =========================================================================
    "recovery": {
        "ie": [
            "Users/*/AppData/Local/Microsoft/Internet Explorer/Recovery/Active/**/*",
            "Users/*/AppData/Local/Microsoft/Internet Explorer/Recovery/Last Active/**/*",
            "Users/*/AppData/Local/Microsoft/Internet Explorer/Recovery/Immersive/**/*",
        ],
        "ie_system": [],
        "edge_legacy": [
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/User/Default/Recovery/Active/**/*",
        ],
    },

    # =========================================================================
    # Toolbar and Add-on Data
    # Third-party toolbars often store browsing data
    # =========================================================================
    "addons": {
        "ie": [
            # Browser Helper Objects (BHOs)
            "Users/*/AppData/Local/Microsoft/Internet Explorer/BrowserExtensions/**/*",
            # Common toolbar data locations
            "Users/*/AppData/LocalLow/Microsoft/Internet Explorer/**/*",
        ],
        "ie_system": [],
        "edge_legacy": [
            "Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/Extensions/**/*",
        ],
    },
}


def get_patterns(browser: str, artifact_type: str) -> List[str]:
    """
    Get file path patterns for a specific browser and artifact type.

    Args:
        browser: Browser identifier (ie, ie_system, edge_legacy)
        artifact_type: Artifact type (webcache, cookies, favorites, etc.)

    Returns:
        List of glob patterns for the specified browser/artifact combination

    Raises:
        ValueError: If browser or artifact_type is unknown
    """
    if browser not in IE_BROWSERS:
        raise ValueError(f"Unknown browser: {browser}. Valid: {list(IE_BROWSERS.keys())}")

    if artifact_type not in IE_ARTIFACTS:
        raise ValueError(f"Unknown artifact type: {artifact_type}. Valid: {list(IE_ARTIFACTS.keys())}")

    return IE_ARTIFACTS[artifact_type].get(browser, [])


def get_all_patterns(artifact_type: str) -> List[str]:
    """
    Get all file path patterns for an artifact type across all browsers.

    Args:
        artifact_type: Artifact type (webcache, cookies, favorites, etc.)

    Returns:
        Combined list of patterns from all browsers (deduplicated)
    """
    if artifact_type not in IE_ARTIFACTS:
        raise ValueError(f"Unknown artifact type: {artifact_type}. Valid: {list(IE_ARTIFACTS.keys())}")

    all_patterns = []
    for browser_patterns in IE_ARTIFACTS[artifact_type].values():
        all_patterns.extend(browser_patterns)

    # Remove duplicates while preserving order
    seen = set()
    unique_patterns = []
    for pattern in all_patterns:
        if pattern not in seen:
            seen.add(pattern)
            unique_patterns.append(pattern)

    return unique_patterns


def get_all_browsers() -> List[str]:
    """Return list of all browser identifiers."""
    return list(IE_BROWSERS.keys())


def get_browser_display_name(browser: str) -> str:
    """Get human-readable display name for a browser."""
    if browser not in IE_BROWSERS:
        return browser.replace("_", " ").title()
    return IE_BROWSERS[browser]["display_name"]


def detect_browser_from_path(file_path: str) -> str:
    """
    Detect which browser a file path belongs to.

    Args:
        file_path: Logical path from evidence filesystem

    Returns:
        Browser identifier (ie, ie_system, ie_old_windows, edge_legacy)
    """
    path_lower = file_path.lower()

    # Legacy Edge UWP package
    if "microsoft.microsoftedge_" in path_lower:
        return "edge_legacy"

    # Windows.old paths (previous Windows installation)
    if "windows.old" in path_lower or "windows.old/" in path_lower:
        # Check if system profile within old Windows
        if "config/systemprofile" in path_lower or "config\\systemprofile" in path_lower:
            return "ie_old_windows"  # Could differentiate further if needed
        return "ie_old_windows"

    # System profile (current Windows)
    if "config/systemprofile" in path_lower or "config\\systemprofile" in path_lower:
        return "ie_system"

    # Default to IE
    return "ie"


def extract_user_from_path(file_path: str) -> str:
    """
    Extract username from a file path.

    Args:
        file_path: Logical path from evidence filesystem

    Returns:
        Username or "SYSTEM" for system profile
    """
    # Normalize path separators
    path = file_path.replace("\\", "/")

    # System profile
    if "config/systemprofile" in path.lower():
        return "SYSTEM"

    # Check for Users/<username>/
    parts = path.split("/")
    for i, part in enumerate(parts):
        if part.lower() == "users" and i + 1 < len(parts):
            return parts[i + 1]

    return "Unknown"
