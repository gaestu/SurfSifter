"""
Safari browser family file path patterns.

Safari is macOS-only and uses Apple-specific formats:
- History: History.db (SQLite with Cocoa timestamps)
- Cookies: Cookies.binarycookies (binary format)
- Bookmarks: Bookmarks.plist (binary/XML plist)
- Downloads: Downloads.plist (plist)
- Cache: WebKitCache (Blobs + Records), NetworkCache, Cache.db
- Spotlight metadata: .webhistory / .webbookmark files (persist after clearing)

Note: Safari is marked as EXPERIMENTAL due to its unique formats.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Literal, TypedDict
from pathlib import Path


def _with_absolute_variants(paths: List[str]) -> List[str]:
    """
    Expand root patterns to include both relative and absolute forms.

    file_list rows from SleuthKit are typically absolute ("/Users/..."),
    while some scanners may use relative roots.
    """
    expanded: List[str] = []
    seen: set[str] = set()

    for path in paths:
        for candidate in (path, f"/{path}" if not path.startswith("/") else path):
            if candidate not in seen:
                seen.add(candidate)
                expanded.append(candidate)

    return expanded


# Safari browser definitions
# Only one browser in this family (unlike Chromium with Chrome, Edge, etc.)
SAFARI_BROWSERS: Dict[str, Dict] = {
    "safari": {
        "display_name": "Apple Safari",
        "engine": "webkit",
        "platform": "macos",
        "profile_roots": [
            # macOS user Library - primary location
            "Users/*/Library/Safari",
            # Containerized Safari data (sandbox/container context)
            "Users/*/Library/Containers/com.apple.Safari/Data/Library/Safari",
            # Safari Technology Preview container
            "Users/*/Library/Containers/com.apple.SafariTechnologyPreview/Data/Library/Safari",
            # System Library (rare, but possible)
            "Library/Safari",
        ],
        # Cookies are stored separately
        "cookies_roots": [
            "Users/*/Library/Cookies",
            "Users/*/Library/Containers/com.apple.Safari/Data/Library/Cookies",
            "Users/*/Library/Containers/com.apple.SafariTechnologyPreview/Data/Library/Cookies",
            "Library/Cookies",
        ],
        # Cache is in a different location
        "cache_roots": [
            "Users/*/Library/Caches/com.apple.Safari",
            "Users/*/Library/Containers/com.apple.Safari/Data/Library/Caches/com.apple.Safari",
            "Users/*/Library/Containers/com.apple.SafariTechnologyPreview/Data/Library/Caches/com.apple.SafariTechnologyPreview",
            "Library/Caches/com.apple.Safari",
        ],
        # Spotlight metadata caches (persist even after Safari history/bookmarks are cleared)
        "metadata_roots": [
            "Users/*/Library/Caches/Metadata/Safari",
            "Users/*/Library/Containers/com.apple.Safari/Data/Library/Caches/Metadata/Safari",
        ],
    },
}

for browser_info in SAFARI_BROWSERS.values():
    browser_info["profile_roots"] = _with_absolute_variants(browser_info["profile_roots"])
    browser_info["cookies_roots"] = _with_absolute_variants(browser_info["cookies_roots"])
    browser_info["cache_roots"] = _with_absolute_variants(browser_info["cache_roots"])
    browser_info["metadata_roots"] = _with_absolute_variants(browser_info["metadata_roots"])


SafariRootType = Literal["profile", "cookies", "cache", "metadata"]


class SafariArtifactInfo(TypedDict):
    patterns: List[str]
    root_type: SafariRootType


# Safari artifact patterns relative to profile roots
SAFARI_ARTIFACTS: Dict[str, SafariArtifactInfo] = {
    "history": {
        # History.db is SQLite with Cocoa timestamps (seconds since 2001-01-01)
        "patterns": [
            "History.db",
            "History.db-wal",
            "History.db-journal",
            "History.db-shm",
            "History.db-lock",
        ],
        "root_type": "profile",
    },
    "cookies": {
        # Cookies.binarycookies requires special parser
        "patterns": [
            "Cookies.binarycookies",
        ],
        "root_type": "cookies",
    },
    "bookmarks": {
        # Bookmarks.plist is binary or XML plist
        "patterns": [
            "Bookmarks.plist",
        ],
        "root_type": "profile",
    },
    "downloads": {
        # Downloads.plist tracks download history
        "patterns": [
            "Downloads.plist",
        ],
        "root_type": "profile",
    },
    "cache": {
        # Safari cache — multiple storage locations:
        # - Cache.db: SQLite index of cached resources
        # - WebKitCache/Version */Blobs/*: raw cached response bodies
        # - WebKitCache/Version */Records/<partition>/<Resource|SubResources>/<hash>: cache records
        # - WebKit/NetworkCache/*: modern NetworkProcess cache
        # - fsCachedData/*: legacy iOS-style cached data (rare on desktop)
        #
        # NOTE: Records are 3 levels deep under Records/ so we need
        # explicit depth globs (not ** which triggers slow full-walk).
        "patterns": [
            "Cache.db",
            "Cache.db-wal",
            "Cache.db-journal",
            "Cache.db-shm",
            "fsCachedData/*",
            # WebKitCache blobs (response bodies) and salt
            "WebKitCache/Version */Blobs/*",
            "WebKitCache/Version */salt",
            # WebKitCache Records: <partition>/Resource/<sha1> and <partition>/SubResources/<sha1>
            "WebKitCache/Version */Records/*/*/*",
            # NetworkCache (same structure)
            "WebKit/NetworkCache/Version */Blobs/*",
            "WebKit/NetworkCache/Version */salt",
            "WebKit/NetworkCache/Version */Records/*/*/*",
            # CacheStorage
            "WebKit/CacheStorage/*",
        ],
        "root_type": "cache",
    },
    "sessions": {
        # Session state for recovery
        "patterns": [
            "LastSession.plist",
        ],
        "root_type": "profile",
    },
    "extensions": {
        # Safari extensions (App Extensions)
        "patterns": [
            "Extensions/*.safariextz",
            "Extensions/Extensions.plist",
        ],
        "root_type": "profile",
    },
    "local_storage": {
        # Safari Local Storage
        "patterns": [
            "LocalStorage/*",
        ],
        "root_type": "profile",
    },
    "favicons": {
        # Favicon and touch icon caches
        "patterns": [
            "Favicon Cache/*",
            "Profiles/*/Favicon Cache/*",
            "Touch Icons Cache/*",
            "Profiles/*/Touch Icons Cache/*",
            "Template Icons/*",
            "Profiles/*/Template Icons/*",
        ],
        "root_type": "profile",
    },
    "top_sites": {
        # Frequently visited sites
        "patterns": [
            "TopSites.plist",
        ],
        "root_type": "profile",
    },
    "recently_closed_tabs": {
        # Recently closed tabs — direct evidence of browsing activity
        "patterns": [
            "RecentlyClosedTabs.plist",
        ],
        "root_type": "profile",
    },
    "autofill": {
        # Safari form autofill data and corrections
        "patterns": [
            "Form Values",
            "AutoFillCorrections.db",
            "AutoFillCorrections.db-wal",
            "AutoFillCorrections.db-shm",
            "CloudAutoFillCorrections.db",
            "CloudAutoFillCorrections.db-wal",
            "CloudAutoFillCorrections.db-shm",
        ],
        "root_type": "profile",
    },
    "per_site_preferences": {
        # Per-site permission and preference settings — lists visited sites
        "patterns": [
            "PerSitePreferences.db",
            "PerSitePreferences.db-wal",
            "PerSitePreferences.db-shm",
            "PerSiteZoomPreferences.plist",
        ],
        "root_type": "profile",
    },
    "history_index": {
        # Safari search index for history — may contain terms not in History.db
        "patterns": [
            "HistoryIndex.sk",
        ],
        "root_type": "profile",
    },
    "permissions": {
        # Site-level permission grants (media, notifications, plugins)
        "patterns": [
            "UserMediaPermissions.plist",
            "UserNotificationPermissions.plist",
            "PlugInOrigins.plist",
            "SitesAllowedToAutoplay.plist",
        ],
        "root_type": "profile",
    },
    "cloud_history": {
        # iCloud Safari sync configuration — indicates sync was active
        "patterns": [
            "CloudHistoryRemoteConfiguration.plist",
        ],
        "root_type": "profile",
    },
    "search_descriptions": {
        # Installed search engine definitions
        "patterns": [
            "SearchDescriptions.plist",
        ],
        "root_type": "profile",
    },
    "webpage_icons": {
        # Legacy favicon database (pre-macOS 12)
        "patterns": [
            "WebpageIcons.db",
        ],
        "root_type": "profile",
    },
    "spotlight_metadata": {
        # Spotlight-indexed Safari metadata — HIGH forensic value!
        # These .webhistory and .webbookmark files persist even after
        # Safari history/bookmarks are cleared, providing evidence
        # recovery capabilities.
        "patterns": [
            "History/*.webhistory",
            "History/.tracked filenames.plist",
            "Bookmarks/*.webbookmark",
        ],
        "root_type": "metadata",
    },
}


def get_patterns(artifact: str) -> List[str]:
    """
    Generate full glob patterns for a Safari artifact type.

    Args:
        artifact: Artifact type (history, cookies, bookmarks, downloads, etc.)

    Returns:
        List of glob patterns for the artifact

    Raises:
        ValueError: If artifact type is not recognized
    """
    if artifact not in SAFARI_ARTIFACTS:
        raise ValueError(f"Unknown Safari artifact type: {artifact}")

    artifact_info = SAFARI_ARTIFACTS[artifact]
    root_type = artifact_info["root_type"]
    file_patterns = artifact_info["patterns"]

    # Select appropriate root paths
    browser = SAFARI_BROWSERS["safari"]
    if root_type == "profile":
        roots = browser["profile_roots"]
    elif root_type == "cookies":
        roots = browser["cookies_roots"]
    elif root_type == "cache":
        roots = browser["cache_roots"]
    elif root_type == "metadata":
        roots = browser["metadata_roots"]
    else:
        roots = browser["profile_roots"]

    patterns = []
    for root in roots:
        for pattern in file_patterns:
            patterns.append(f"{root}/{pattern}")

    return patterns


def get_all_patterns() -> Dict[str, List[str]]:
    """
    Get all Safari artifact patterns.

    Returns:
        Dictionary mapping artifact type to list of glob patterns
    """
    return {
        artifact: get_patterns(artifact)
        for artifact in SAFARI_ARTIFACTS
    }


def get_browser_display_name() -> str:
    """Get Safari display name."""
    return SAFARI_BROWSERS["safari"]["display_name"]


def extract_user_from_path(path: str) -> Optional[str]:
    """
    Extract macOS username from Safari artifact path.

    Args:
        path: File path like 'Users/johndoe/Library/Safari/History.db'

    Returns:
        Username or None if not found

    Examples:
        >>> extract_user_from_path('Users/johndoe/Library/Safari/History.db')
        'johndoe'
        >>> extract_user_from_path('/Library/Safari/History.db')
        None
    """
    path_obj = Path(path)
    parts = path_obj.parts

    # Windows-style paths
    for i, part in enumerate(parts):
        if part.lower() == "users" and i + 1 < len(parts):
            username = parts[i + 1]
            # Skip wildcard and reserved names
            if username not in ("*", "Default", "Public", "All Users"):
                return username

    return None


def is_safari_path(path: str) -> bool:
    """
    Check if a path appears to be a Safari artifact.

    Args:
        path: File path to check

    Returns:
        True if path contains Safari-related directories
    """
    path_lower = path.lower()
    safari_indicators = [
        "safari",
        "com.apple.safari",
        "cookies.binarycookies",
        "bookmarks.plist",
    ]
    return any(indicator in path_lower for indicator in safari_indicators)
