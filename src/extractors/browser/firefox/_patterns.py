"""
Firefox browser family file path patterns.

Covers: Firefox, Firefox ESR, Tor Browser (all use Gecko engine).
Firefox uses randomized profile names (e.g., abc123.default-release).

Note: Firefox stores different data in different locations:
- Profile data (history, cookies, etc.) -> AppData/Roaming
- Cache data -> AppData/Local

ESR Detection:
Firefox ESR (Extended Support Release) shares identical profile paths with
regular Firefox. The 'firefox_esr' entry exists ONLY for labeling purposes
(detect_browser_from_path() returns 'firefox_esr' for ESR profiles).
It has NO patterns - all Firefox artifacts are discovered via the 'firefox'
entry's patterns, then labeled appropriately based on profile naming:
- Profile names ending with ".default-esr"
- Installation paths containing "Firefox ESR" (enterprise deployments)

Future Consideration (Firefox Forks):
Other Gecko-based browsers could be added if forensic need arises:
- Waterfox (privacy-focused fork, uses ~/.waterfox or AppData/Waterfox)
- LibreWolf (hardened Firefox fork, uses ~/.librewolf)
- Pale Moon (legacy fork, uses ~/.moonchild productions/pale moon)
- GNU IceCat (FSF rebranding)
These share the Firefox database format but use distinct profile paths.

Usage:
    from extractors.browser.firefox._patterns import (
        FIREFOX_BROWSERS,
        FIREFOX_ARTIFACTS,
        get_patterns,
    )

    # Get all history patterns for Firefox
    patterns = get_patterns("firefox", "history")

    # Get all history patterns for all Firefox browsers (with unique patterns)
    all_patterns = get_all_patterns("history")
"""

from __future__ import annotations

import re
from typing import Dict, List, Any


# Browser-specific profile root paths
# Keys are browser identifiers, values are display name and profile roots
FIREFOX_BROWSERS: Dict[str, Dict[str, Any]] = {
    "firefox": {
        "display_name": "Mozilla Firefox",
        "profile_roots": [
            # Windows (profile data in Roaming)
            "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles",
            # macOS
            "Users/*/Library/Application Support/Firefox/Profiles",
            # Linux
            "home/*/.mozilla/firefox",
        ],
        # Cache is stored in Local, not Roaming on Windows
        "cache_roots": [
            # Windows (cache in Local)
            "Users/*/AppData/Local/Mozilla/Firefox/Profiles",
            # macOS (cache in Caches directory)
            "Users/*/Library/Caches/Firefox/Profiles",
            # Linux (cache typically with profile)
            "home/*/.cache/mozilla/firefox",
            "home/*/.mozilla/firefox",  # Fallback
        ],
    },
    "firefox_esr": {
        "display_name": "Firefox ESR",
        # LABEL-ONLY ENTRY: ESR uses identical paths to regular Firefox.
        # This entry exists so detect_browser_from_path() can return "firefox_esr"
        # for profiles with .default-esr naming or "Firefox ESR" in installation path.
        # All Firefox family artifacts are discovered via the "firefox" patterns.
        "profile_roots": [],  # Empty - no patterns, label only
        "cache_roots": [],    # Empty - no patterns, label only
    },
    "tor": {
        "display_name": "Tor Browser",
        "profile_roots": [
            # Windows (portable bundle)
            "Users/*/Desktop/Tor Browser/Browser/TorBrowser/Data/Browser",
            "Users/*/Downloads/Tor Browser/Browser/TorBrowser/Data/Browser",
            "Users/*/AppData/Local/Tor Browser/Browser/TorBrowser/Data/Browser",
            # Windows portable (generic)
            "*/Tor Browser/Browser/TorBrowser/Data/Browser",
            "*/TorBrowser/Browser/TorBrowser/Data/Browser",
            # macOS
            "Applications/Tor Browser.app/Contents/Resources/TorBrowser/Data/Browser",
            "Users/*/Applications/Tor Browser.app/Contents/Resources/TorBrowser/Data/Browser",
            # Linux
            "home/*/tor-browser*/Browser/TorBrowser/Data/Browser",
            "home/*/.tor-browser/Browser/TorBrowser/Data/Browser",
        ],
        # Tor Browser keeps cache with profile (same roots)
        "cache_roots": None,  # Use profile_roots
    },
}


# Artifact paths relative to profile root
# Firefox uses wildcard (*) for randomized profile names (abc123.default-release)
# Tor Browser uses profile.default/profile.default-release under TorBrowser/Data/Browser/
FIREFOX_ARTIFACTS: Dict[str, List[str]] = {
    "history": [
        # places.sqlite contains both history and bookmarks
        "*/places.sqlite",
        "*/places.sqlite-wal",  # WAL mode journal
        "*/places.sqlite-shm",  # Shared memory index
        # For Tor Browser (fixed profile path)
        "places.sqlite",
    ],
    "cookies": [
        "*/cookies.sqlite",
        "*/cookies.sqlite-wal",
        "*/cookies.sqlite-shm",
        "cookies.sqlite",  # Tor Browser
    ],
    "bookmarks": [
        # Bookmarks stored in same database as history
        "*/places.sqlite",
        "places.sqlite",  # Tor Browser
    ],
    "bookmark_backups": [
        # Automatic bookmark backups (jsonlz4 compressed JSON)
        # Contains historical/deleted bookmarks not in current places.sqlite
        # Filename format: bookmarks-YYYY-MM-DD_####_<hash>.jsonlz4
        "*/bookmarkbackups/*.jsonlz4",
        "bookmarkbackups/*.jsonlz4",  # Tor Browser
    ],
    "downloads": [
        # Downloads tracked via annotations in places.sqlite
        # Modern Firefox uses moz_annos, legacy uses moz_downloads
        "*/places.sqlite",
        "places.sqlite",  # Tor Browser
    ],
    "autofill": [
        "*/formhistory.sqlite",
        "*/logins.json",
        "*/key4.db",
        "*/key3.db",  # Legacy NSS key store (< Firefox 58)
        "*/signons.sqlite",  # Legacy credential store (< Firefox 32)
        "formhistory.sqlite",  # Tor Browser
        "logins.json",
        "key4.db",  # Tor Browser
    ],
    "sessions": [
        "*/sessionstore.jsonlz4",
        "*/sessionstore-backups/recovery.jsonlz4",
        "*/sessionstore-backups/recovery.baklz4",
        "*/sessionstore-backups/previous.jsonlz4",
        "*/sessionstore-backups/upgrade.jsonlz4-*",  # Post-upgrade session backup
        # Legacy uncompressed (Firefox < 56)
        "*/sessionstore.js",
        "*/sessionstore-backups/recovery.js",
        "*/sessionstore-backups/previous.js",
        # Tor Browser (fixed profile path)
        "sessionstore.jsonlz4",
        "sessionstore-backups/recovery.jsonlz4",
        "sessionstore-backups/recovery.baklz4",
        "sessionstore-backups/previous.jsonlz4",
        "sessionstore-backups/upgrade.jsonlz4-*",
    ],
    "permissions": [
        "*/permissions.sqlite",
        "*/content-prefs.sqlite",
        "permissions.sqlite",  # Tor Browser
    ],
    "extensions": [
        "*/extensions.json",
        "*/addons.json",
        "extensions.json",  # Tor Browser
    ],
    "local_storage": [
        "*/webappsstore.sqlite",
        "*/storage/default/*/ls/*.sqlite",
        "webappsstore.sqlite",  # Tor Browser
    ],
    "session_storage": [
        # Firefox session storage is mostly ephemeral
    ],
    "indexeddb": [
        "*/storage/default/*/idb/*.sqlite",
    ],
    "transport_security": [
        # HSTS preload data - HIGH forensic value!
        # Contains cleartext domains even after history clearing
        "*/SiteSecurityServiceState.txt",
        "SiteSecurityServiceState.txt",  # Tor Browser
    ],
    "favicons": [
        "*/favicons.sqlite",
        "*/favicons.sqlite-wal",
        "*/favicons.sqlite-shm",
        "favicons.sqlite",  # Tor Browser
    ],
    "sync_data": [
        "*/signedInUser.json",
        "*/weave/*",
        "signedInUser.json",  # Tor Browser
    ],
    "cache": [
        # Firefox cache2 format (main entry files only)
        # doomed/* and trash/* are handled separately as supporting files
        "*/cache2/entries/*",
        "cache2/entries/*",  # Tor Browser
    ],
    "cache_index": [
        # Cache2 binary index — contains metadata for ALL cache entries
        # including evicted/deleted, HIGH forensic value
        "*/cache2/index",
        "cache2/index",  # Tor Browser
    ],
    "cache_journal": [
        # Journal (dirty/removed entries since last index flush)
        "*/cache2/index.log",
        "cache2/index.log",  # Tor Browser
    ],
    "cache_doomed": [
        # Entries marked for deletion but not yet removed
        "*/cache2/doomed/*",
        "cache2/doomed/*",  # Tor Browser
    ],
    "cache_trash": [
        # Recently deleted cache files (trash has numbered sub-dirs)
        "*/cache2/trash/*/*",
        "cache2/trash/*/*",  # Tor Browser
    ],
}

# Artifacts that use cache_roots instead of profile_roots
CACHE_ARTIFACTS = {"cache", "cache_index", "cache_journal", "cache_doomed", "cache_trash"}


def get_patterns(browser: str, artifact: str) -> List[str]:
    """
    Generate full glob patterns for a browser/artifact combination.

    Args:
        browser: Browser key (firefox, firefox_esr, tor)
        artifact: Artifact key (history, cookies, bookmarks, etc.)

    Returns:
        List of glob patterns for the browser/artifact combination

    Example:
        >>> patterns = get_patterns("firefox", "history")
        >>> # Returns:
        >>> # [
        >>> #   "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/places.sqlite",
        >>> #   "Users/*/Library/Application Support/Firefox/Profiles/*/places.sqlite",
        >>> #   ...
        >>> # ]

        >>> patterns = get_patterns("firefox", "cache")
        >>> # Returns cache patterns using AppData/Local (not Roaming):
        >>> # [
        >>> #   "Users/*/AppData/Local/Mozilla/Firefox/Profiles/*/cache2/entries/*",
        >>> #   ...
        >>> # ]
    """
    if browser not in FIREFOX_BROWSERS:
        raise ValueError(f"Unknown browser: {browser}. Valid: {list(FIREFOX_BROWSERS.keys())}")

    if artifact not in FIREFOX_ARTIFACTS:
        raise ValueError(f"Unknown artifact: {artifact}. Valid: {list(FIREFOX_ARTIFACTS.keys())}")

    browser_info = FIREFOX_BROWSERS[browser]

    # Use cache_roots for cache artifacts, profile_roots for everything else
    if artifact in CACHE_ARTIFACTS:
        roots = browser_info.get("cache_roots") or browser_info["profile_roots"]
    else:
        roots = browser_info["profile_roots"]

    patterns = []
    for root in roots:
        for artifact_path in FIREFOX_ARTIFACTS[artifact]:
            patterns.append(f"{root}/{artifact_path}")

    return patterns


def get_all_patterns(artifact: str) -> List[str]:
    """
    Generate full glob patterns for all Firefox browsers for an artifact.

    Skips browsers with empty profile_roots (label-only entries like firefox_esr).
    Uses dict.fromkeys() to preserve order while removing any duplicates.

    Args:
        artifact: Artifact key (history, cookies, bookmarks, etc.)

    Returns:
        List of unique glob patterns for all Firefox browsers with patterns
    """
    patterns = []
    for browser in FIREFOX_BROWSERS:
        browser_patterns = get_patterns(browser, artifact)
        patterns.extend(browser_patterns)
    # Deduplicate (preserves order)
    return list(dict.fromkeys(patterns))


def get_browser_display_name(browser: str) -> str:
    """Get human-readable browser name."""
    if browser not in FIREFOX_BROWSERS:
        return browser.title()
    return FIREFOX_BROWSERS[browser]["display_name"]


def get_all_browsers() -> List[str]:
    """Get list of all supported Firefox browser keys."""
    return list(FIREFOX_BROWSERS.keys())


def extract_profile_from_path(path: str) -> str:
    """
    Extract Firefox profile name from a path.

    Firefox profiles have randomized names like:
    - abc123def.default-release
    - xyz789.default

    Args:
        path: Full path containing Firefox profile

    Returns:
        Profile name or "Default" if not found

    Example:
        >>> extract_profile_from_path(
        ...     "Users/John/AppData/Roaming/Mozilla/Firefox/Profiles/abc123.default-release/places.sqlite"
        ... )
        'abc123.default-release'
    """
    import re

    # Look for profile pattern after Profiles/ or .mozilla/firefox/
    # Profile names: <random>.<profile_type> (e.g., abc123.default-release)
    patterns = [
        r"Profiles/([^/]+)/",
        r"\.mozilla/firefox/([^/]+)/",
        r"TorBrowser/Data/Browser/([^/]+)/",
        r"(profile\.[^/]+)",  # Tor Browser profile.* (default, default-release, etc.)
    ]

    for pattern in patterns:
        match = re.search(pattern, path)
        if match:
            return match.group(1) if match.lastindex else "default"

    return "Default"


def detect_browser_from_path(path: str) -> str:
    """
    Detect which Firefox-family browser from a file path.

    Detection logic:
    - Tor Browser: Identified by "tor browser", "tor-browser", or "torbrowser" in path
    - Firefox ESR: Identified by:
      - Profile name ending with ".default-esr" or containing "-esr"
      - Installation path containing "Firefox ESR" (enterprise deployments)
    - Firefox: Default for all other Firefox paths

    Note: Firefox and Firefox ESR share identical profile root paths
    (e.g., ~/.mozilla/firefox, AppData/Roaming/Mozilla/Firefox/Profiles).
    ESR detection relies on heuristics and is best-effort. The profile
    suffix ".default-esr" is common but not guaranteed for ESR installs.

    Args:
        path: Full file path

    Returns:
        Browser key (firefox, firefox_esr, tor)
    """
    path_lower = path.lower()

    # Tor Browser detection - most specific patterns first
    if "tor browser" in path_lower or "tor-browser" in path_lower or "torbrowser" in path_lower:
        return "tor"

    # Firefox ESR detection - check installation path and profile naming
    # Enterprise/managed installs may use "Firefox ESR" directory
    if "firefox esr" in path_lower:
        return "firefox_esr"

    # Check for ESR profile naming convention
    # ESR profiles typically have names like "abc123.default-esr" or "profile.default-esr"
    # Also check for "-esr" anywhere in profile name (e.g., "abc123.custom-esr-profile")
    esr_profile_patterns = [
        r"profiles/[^/]*\.default-esr[^/]*/",  # Windows/macOS: .default-esr
        r"profiles/[^/]*-esr[^/]*/",            # Windows/macOS: -esr anywhere in profile
        r"\.mozilla/firefox/[^/]*\.default-esr[^/]*/",  # Linux: .default-esr
        r"\.mozilla/firefox/[^/]*-esr[^/]*/",           # Linux: -esr anywhere
    ]
    for pattern in esr_profile_patterns:
        if re.search(pattern, path_lower):
            return "firefox_esr"

    # Default to regular Firefox
    return "firefox"


def get_artifact_patterns(browser: str, artifact: str) -> List[str]:
    """
    Alias for get_patterns() for consistency with other modules.

    Args:
        browser: Browser key (firefox, firefox_esr, tor)
        artifact: Artifact key (history, cookies, bookmarks, autofill, etc.)

    Returns:
        List of glob patterns for the browser/artifact combination
    """
    return get_patterns(browser, artifact)


# -------------------------------------------------------------------------
# Cache path classification and discovery helpers
# -------------------------------------------------------------------------

# Maps FIREFOX_ARTIFACTS cache keys → extractor artifact_type values.
# The ``cache`` key uses ``cache_firefox`` as its artifact_type to distinguish
# regular entry files from other cache artifacts in manifests and the DB.
_CACHE_ARTIFACT_TYPE_MAP: Dict[str, str] = {
    "cache": "cache_firefox",
    "cache_index": "cache_index",
    "cache_journal": "cache_journal",
    "cache_doomed": "cache_doomed",
    "cache_trash": "cache_trash",
}


def classify_cache_path(file_path: str) -> str:
    """Classify a cache2 file path into its extractor artifact type.

    Determines which ``FIREFOX_ARTIFACTS`` cache key a given path belongs to
    and returns the corresponding artifact type used in manifests and the
    evidence database.

    The mapping (see ``_CACHE_ARTIFACT_TYPE_MAP``):

    ================== ================= ======================================
    Artifact key       Artifact type     Distinctive segment
    ================== ================= ======================================
    ``cache``          ``cache_firefox`` ``cache2/entries/…``
    ``cache_index``    ``cache_index``   path ends with ``cache2/index``
    ``cache_journal``  ``cache_journal`` path ends with ``cache2/index.log``
    ``cache_doomed``   ``cache_doomed``  path contains ``cache2/doomed/``
    ``cache_trash``    ``cache_trash``   path contains ``cache2/trash/``
    ================== ================= ======================================

    Args:
        file_path: Full path (e.g. from ``file_list`` or manifest).

    Returns:
        One of the artifact type strings listed above.
    """
    # Normalise separators for cross-platform matching
    norm = file_path.replace("\\", "/").lower()

    # Most-specific checks first — order matters.
    if norm.endswith("/cache2/index"):
        return "cache_index"
    if norm.endswith("/cache2/index.log"):
        return "cache_journal"
    if "/cache2/doomed/" in norm:
        return "cache_doomed"
    if "/cache2/trash/" in norm:
        return "cache_trash"
    # Default: regular entry in cache2/entries/
    return "cache_firefox"


def get_cache_discovery_patterns() -> List[str]:
    """Return broad SQL LIKE patterns for discovering all cache2 files.

    Derives patterns from the ``CACHE_ARTIFACTS`` keys in
    ``FIREFOX_ARTIFACTS``.  Each artifact's glob patterns are converted to
    SQL LIKE wildcards (``*`` → ``%``) with a leading ``%/`` so they match
    any prefix path.

    The returned patterns are suitable for passing as ``path_patterns`` to
    ``extractors._shared.file_list_discovery.discover_from_file_list()``.

    Returns:
        De-duplicated list of SQL LIKE patterns, e.g.::

            ["%/cache2/entries/%", "%/cache2/index", "%/cache2/index.log",
             "%/cache2/doomed/%", "%/cache2/trash/%"]
    """
    seen: set[str] = set()
    patterns: List[str] = []

    for key in CACHE_ARTIFACTS:
        for artifact_glob in FIREFOX_ARTIFACTS.get(key, []):
            norm = artifact_glob.replace("\\", "/")
            idx = norm.lower().find("cache2/")
            if idx < 0:
                idx = norm.lower().find("cache2")
                if idx < 0:
                    continue

            suffix = norm[idx:]
            # Glob → SQL LIKE
            sql_suffix = suffix.replace("*", "%")
            # Collapse redundant %/% sequences (e.g. trash/%/% → trash/%)
            while "/%/%" in sql_suffix:
                sql_suffix = sql_suffix.replace("/%/%", "/%")
            sql_pattern = f"%/{sql_suffix}"

            if sql_pattern not in seen:
                seen.add(sql_pattern)
                patterns.append(sql_pattern)

    return patterns
