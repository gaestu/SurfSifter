"""
Safari browser family file path patterns.

Safari is macOS-only and uses Apple-specific formats:
- History: History.db (SQLite with Cocoa timestamps)
- Cookies: Cookies.binarycookies (binary format)
- Bookmarks: Bookmarks.plist (binary/XML plist)
- Downloads: Downloads.plist (plist)
- Cache: WebKitCache with fsCachedData

Note: Safari is marked as EXPERIMENTAL due to its unique formats.
"""

from __future__ import annotations

from typing import Dict, List, Optional
from pathlib import Path

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
            # System Library (rare, but possible)
            "Library/Safari",
        ],
        # Cookies are stored separately
        "cookies_roots": [
            "Users/*/Library/Cookies",
            "Library/Cookies",
        ],
        # Cache is in a different location
        "cache_roots": [
            "Users/*/Library/Caches/com.apple.Safari",
            "Library/Caches/com.apple.Safari",
        ],
    },
}

# Safari artifact patterns relative to profile roots
SAFARI_ARTIFACTS: Dict[str, Dict[str, List[str]]] = {
    "history": {
        # History.db is SQLite with Cocoa timestamps (seconds since 2001-01-01)
        "patterns": [
            "History.db",
            "History.db-wal",
            "History.db-journal",
            "History.db-shm",
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
        # Safari cache is complex with multiple components
        "patterns": [
            "Cache.db",
            "Cache.db-wal",
            "Cache.db-journal",
            "fsCachedData/*",
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
            "Touch Icons Cache/*",
            "Template Icons/*",
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
