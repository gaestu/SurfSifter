"""
Chromium browser family file path patterns.

Covers all Chromium-based browsers:
- Google Chrome (Stable, Beta, Dev, Canary)
- Chromium (open-source)
- Microsoft Edge (Stable, Beta, Dev, Canary)
- Brave (Stable, Beta, Nightly)
- Opera (Stable, GX) — Note: Opera uses flat profile structure (no Default/ subdir)

Each browser has different install paths. Most use the same internal structure
with Default/Profile * subdirectories, except Opera which stores artifacts
directly under the profile root.

Usage:
    from extractors.browser.chromium._patterns import (
        CHROMIUM_BROWSERS,
        CHROMIUM_ARTIFACTS,
        get_patterns,
    )

    # Get all history patterns for Chrome
    patterns = get_patterns("chrome", "history")

    # Get all history patterns for all Chromium browsers
    all_patterns = get_all_patterns("history")
"""

from __future__ import annotations

from typing import Dict, List, Any


# Browser-specific profile root paths
# Keys are browser identifiers, values are display name, profile roots, and structure type
#
# flat_profile: If True, artifacts are stored directly under profile_root (Opera style)
#               If False/missing, artifacts are under profile_root/Default/ or profile_root/Profile */
CHROMIUM_BROWSERS: Dict[str, Dict[str, Any]] = {
    # =========================================================================
    # Google Chrome (all channels)
    # =========================================================================
    "chrome": {
        "display_name": "Google Chrome",
        "profile_roots": [
            # Windows
            "Users/*/AppData/Local/Google/Chrome/User Data",
            # macOS
            "Users/*/Library/Application Support/Google/Chrome",
            # Linux
            "home/*/.config/google-chrome",
        ],
    },
    "chrome_beta": {
        "display_name": "Google Chrome Beta",
        "profile_roots": [
            # Windows
            "Users/*/AppData/Local/Google/Chrome Beta/User Data",
            # macOS
            "Users/*/Library/Application Support/Google/Chrome Beta",
            # Linux
            "home/*/.config/google-chrome-beta",
        ],
    },
    "chrome_dev": {
        "display_name": "Google Chrome Dev",
        "profile_roots": [
            # Windows
            "Users/*/AppData/Local/Google/Chrome Dev/User Data",
            # macOS
            "Users/*/Library/Application Support/Google/Chrome Dev",
            # Linux (uses "unstable" suffix)
            "home/*/.config/google-chrome-unstable",
        ],
    },
    "chrome_canary": {
        "display_name": "Google Chrome Canary",
        "profile_roots": [
            # Windows (uses SxS = Side-by-Side)
            "Users/*/AppData/Local/Google/Chrome SxS/User Data",
            # macOS
            "Users/*/Library/Application Support/Google/Chrome Canary",
            # Linux (rare, but exists)
            "home/*/.config/google-chrome-canary",
        ],
    },
    # =========================================================================
    # Chromium (open-source browser)
    # =========================================================================
    "chromium": {
        "display_name": "Chromium",
        "profile_roots": [
            # Windows
            "Users/*/AppData/Local/Chromium/User Data",
            # macOS
            "Users/*/Library/Application Support/Chromium",
            # Linux
            "home/*/.config/chromium",
        ],
    },
    # =========================================================================
    # Microsoft Edge (all channels)
    # =========================================================================
    "edge": {
        "display_name": "Microsoft Edge",
        "profile_roots": [
            # Windows
            "Users/*/AppData/Local/Microsoft/Edge/User Data",
            # macOS
            "Users/*/Library/Application Support/Microsoft Edge",
            # Linux
            "home/*/.config/microsoft-edge",
        ],
    },
    "edge_beta": {
        "display_name": "Microsoft Edge Beta",
        "profile_roots": [
            # Windows
            "Users/*/AppData/Local/Microsoft/Edge Beta/User Data",
            # macOS
            "Users/*/Library/Application Support/Microsoft Edge Beta",
            # Linux
            "home/*/.config/microsoft-edge-beta",
        ],
    },
    "edge_dev": {
        "display_name": "Microsoft Edge Dev",
        "profile_roots": [
            # Windows
            "Users/*/AppData/Local/Microsoft/Edge Dev/User Data",
            # macOS
            "Users/*/Library/Application Support/Microsoft Edge Dev",
            # Linux
            "home/*/.config/microsoft-edge-dev",
        ],
    },
    "edge_canary": {
        "display_name": "Microsoft Edge Canary",
        "profile_roots": [
            # Windows (uses SxS = Side-by-Side)
            "Users/*/AppData/Local/Microsoft/Edge SxS/User Data",
            # macOS
            "Users/*/Library/Application Support/Microsoft Edge Canary",
            # Linux (rare)
            "home/*/.config/microsoft-edge-canary",
        ],
    },
    # =========================================================================
    # Brave Browser (all channels)
    # =========================================================================
    "brave": {
        "display_name": "Brave",
        "profile_roots": [
            # Windows
            "Users/*/AppData/Local/BraveSoftware/Brave-Browser/User Data",
            # macOS
            "Users/*/Library/Application Support/BraveSoftware/Brave-Browser",
            # Linux
            "home/*/.config/BraveSoftware/Brave-Browser",
        ],
    },
    "brave_beta": {
        "display_name": "Brave Beta",
        "profile_roots": [
            # Windows
            "Users/*/AppData/Local/BraveSoftware/Brave-Browser-Beta/User Data",
            # macOS
            "Users/*/Library/Application Support/BraveSoftware/Brave-Browser-Beta",
            # Linux
            "home/*/.config/BraveSoftware/Brave-Browser-Beta",
        ],
    },
    "brave_nightly": {
        "display_name": "Brave Nightly",
        "profile_roots": [
            # Windows
            "Users/*/AppData/Local/BraveSoftware/Brave-Browser-Nightly/User Data",
            # macOS
            "Users/*/Library/Application Support/BraveSoftware/Brave-Browser-Nightly",
            # Linux
            "home/*/.config/BraveSoftware/Brave-Browser-Nightly",
        ],
    },
    # =========================================================================
    # Opera Browser
    # IMPORTANT: Opera uses FLAT profile structure - artifacts are stored
    # directly under the profile root, NOT in Default/ or Profile */ subdirs
    # =========================================================================
    "opera": {
        "display_name": "Opera",
        "flat_profile": True,  # No Default/Profile * subdirectories
        "profile_roots": [
            # Windows - Opera Stable
            "Users/*/AppData/Roaming/Opera Software/Opera Stable",
            # macOS
            "Users/*/Library/Application Support/com.operasoftware.Opera",
            # Linux
            "home/*/.config/opera",
        ],
    },
    "opera_gx": {
        "display_name": "Opera GX",
        "flat_profile": True,  # No Default/Profile * subdirectories
        "profile_roots": [
            # Windows - Opera GX Stable
            "Users/*/AppData/Roaming/Opera Software/Opera GX Stable",
            # macOS
            "Users/*/Library/Application Support/com.operasoftware.OperaGX",
            # Linux (rare)
            "home/*/.config/opera-gx",
        ],
    },
}


# Profile subdirectory patterns for browsers with multi-profile support
# Used by get_patterns() for non-flat browsers
PROFILE_PATTERNS: List[str] = [
    "Default",
    "Profile *",
    "Guest Profile",
    "System Profile",
]


# Artifact paths relative to profile directory
# For flat_profile browsers, these are relative to profile_root directly
# For other browsers, these are prefixed with each PROFILE_PATTERN
CHROMIUM_ARTIFACTS: Dict[str, List[str]] = {
    "history": [
        "History",
    ],
    "cookies": [
        "Cookies",
        # Chrome 96+ moved cookies to Network/ subdirectory
        "Network/Cookies",
    ],
    "bookmarks": [
        "Bookmarks",
    ],
    "downloads": [
        # Downloads are stored in the History database
        "History",
    ],
    "autofill": [
        "Web Data",
        "Login Data",
    ],
    "sessions": [
        # Legacy location (Chrome < 100)
        "Current Session",
        "Current Tabs",
        "Last Session",
        "Last Tabs",
        # New location (Chrome 100+) - timestamped files in Sessions/ subdirectory
        "Sessions/Session_*",
        "Sessions/Tabs_*",
    ],
    "permissions": [
        "Preferences",
    ],
    "preferences": [
        # Alias for Preferences file - used by sync_data and other extractors
        # that need the full Preferences JSON (not just permissions section)
        "Preferences",
    ],
    "extensions": [
        "Extensions",
        "Secure Preferences",
    ],
    "local_storage": [
        "Local Storage/leveldb",
    ],
    "session_storage": [
        "Session Storage",
    ],
    "indexeddb": [
        "IndexedDB",
    ],
    "media_history": [
        "Media History",
    ],
    "favicons": [
        "Favicons",
        "Favicons-journal",
    ],
    "top_sites": [
        "Top Sites",
        "Top Sites-journal",
    ],
    "sync_data": [
        "Sync Data",
    ],
    "transport_security": [
        "TransportSecurity",
    ],
    "cache": [
        # Modern Simple Cache format (Cache_Data/)
        "Cache/Cache_Data",
        # Legacy Blockfile format (Cache/)
        "Cache",
    ],
}


def get_patterns(browser: str, artifact: str) -> List[str]:
    """
    Generate full glob patterns for a browser/artifact combination.

    Handles both standard Chromium browsers (Chrome, Edge, Brave) which use
    Default/Profile * subdirectories, and Opera which stores artifacts
    directly under the profile root (flat_profile=True).

    Args:
        browser: Browser key (chrome, edge, brave, opera, chromium, etc.)
        artifact: Artifact key (history, cookies, bookmarks, etc.)

    Returns:
        List of glob patterns for the browser/artifact combination

    Example:
        >>> patterns = get_patterns("chrome", "history")
        >>> # Returns:
        >>> # [
        >>> #   "Users/*/AppData/Local/Google/Chrome/User Data/Default/History",
        >>> #   "Users/*/AppData/Local/Google/Chrome/User Data/Profile */History",
        >>> #   "Users/*/AppData/Local/Google/Chrome/User Data/Guest Profile/History",
        >>> #   "Users/*/Library/Application Support/Google/Chrome/Default/History",
        >>> #   ...
        >>> # ]

        >>> patterns = get_patterns("opera", "history")
        >>> # Returns (flat profile - no Default/ prefix):
        >>> # [
        >>> #   "Users/*/AppData/Roaming/Opera Software/Opera Stable/History",
        >>> #   "Users/*/Library/Application Support/com.operasoftware.Opera/History",
        >>> #   ...
        >>> # ]
    """
    if browser not in CHROMIUM_BROWSERS:
        raise ValueError(f"Unknown browser: {browser}. Valid: {list(CHROMIUM_BROWSERS.keys())}")

    if artifact not in CHROMIUM_ARTIFACTS:
        raise ValueError(f"Unknown artifact: {artifact}. Valid: {list(CHROMIUM_ARTIFACTS.keys())}")

    browser_info = CHROMIUM_BROWSERS[browser]
    artifact_paths = CHROMIUM_ARTIFACTS[artifact]
    is_flat = browser_info.get("flat_profile", False)

    patterns = []
    for profile_root in browser_info["profile_roots"]:
        if is_flat:
            # Opera-style: artifacts directly under profile root
            for artifact_path in artifact_paths:
                patterns.append(f"{profile_root}/{artifact_path}")
        else:
            # Chrome/Edge/Brave-style: artifacts under Default/Profile */etc.
            for profile_pattern in PROFILE_PATTERNS:
                for artifact_path in artifact_paths:
                    patterns.append(f"{profile_root}/{profile_pattern}/{artifact_path}")

    return patterns


def get_all_patterns(artifact: str) -> List[str]:
    """
    Generate full glob patterns for all Chromium browsers for an artifact.

    Args:
        artifact: Artifact key (history, cookies, bookmarks, etc.)

    Returns:
        List of glob patterns for all Chromium browsers
    """
    patterns = []
    for browser in CHROMIUM_BROWSERS:
        patterns.extend(get_patterns(browser, artifact))
    return patterns


def get_browser_display_name(browser: str) -> str:
    """Get human-readable browser name."""
    if browser not in CHROMIUM_BROWSERS:
        return browser.title()
    return CHROMIUM_BROWSERS[browser]["display_name"]


def get_all_browsers() -> List[str]:
    """Get list of all supported Chromium browser keys."""
    return list(CHROMIUM_BROWSERS.keys())


def get_artifact_patterns(browser: str, artifact: str) -> List[str]:
    """
    Alias for get_patterns() for consistency with other modules.

    Args:
        browser: Browser key (chrome, edge, brave, opera, chromium, etc.)
        artifact: Artifact key (history, cookies, bookmarks, autofill, etc.)

    Returns:
        List of glob patterns for the browser/artifact combination
    """
    return get_patterns(browser, artifact)


def get_stable_browsers() -> List[str]:
    """
    Get list of stable channel browser keys only.

    Useful when you want to avoid scanning beta/dev/canary channels.

    Returns:
        List of browser keys for stable releases only
    """
    return [
        "chrome", "chromium", "edge", "brave", "opera", "opera_gx"
    ]


def is_flat_profile_browser(browser: str) -> bool:
    """
    Check if a browser uses flat profile structure (no Default/ subdir).

    Args:
        browser: Browser key

    Returns:
        True if browser stores artifacts directly under profile root (Opera style)
    """
    if browser not in CHROMIUM_BROWSERS:
        return False
    return CHROMIUM_BROWSERS[browser].get("flat_profile", False)
