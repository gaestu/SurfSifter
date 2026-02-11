"""
Chromium SQLite database parsers.

Shared parsing logic for Chromium browser databases (History, Cookies, etc.).
All Chromium browsers use identical schemas, so one parser works for all.

History and Search Terms parsing moved to history/_parser.py for modularity.
        Cookie parsing moved to cookies/_parsers.py.
        Bookmark parsing moved to bookmarks/_parser.py.
        This module re-exports for backward compatibility.

Usage:
    from extractors.browser.chromium._parsers import (
        parse_history_visits,
        parse_cookies,
        parse_keyword_search_terms,
    )

    with safe_sqlite_connect(history_path) as conn:
        visits = list(parse_history_visits(conn))
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, Optional, Dict, Any, List

from ..._shared.sqlite_helpers import safe_execute, table_exists
from ..._shared.timestamps import webkit_to_datetime, webkit_to_iso


# ===========================================================================
# Path Utilities (Chromium-wide)
# ===========================================================================

from typing import Optional as _Optional


def detect_browser_from_path(path) -> _Optional[str]:
    """
    Detect which Chromium browser a file belongs to based on path.

    Args:
        path: Full path to a file (str or Path)

    Returns:
        Browser key (chrome, edge, brave, opera, opera_gx, vivaldi) or None if not recognized
    """
    path_lower = str(path).lower().replace("\\", "/")

    # Check for browser-specific path components
    if "/google/chrome/" in path_lower or "google-chrome" in path_lower:
        return "chrome"
    elif "/microsoft/edge/" in path_lower or "microsoft-edge" in path_lower:
        return "edge"
    elif "/bravesoftware/brave-browser/" in path_lower or "/brave-browser" in path_lower:
        return "brave"
    elif "opera gx" in path_lower or "operagx" in path_lower:
        return "opera_gx"
    elif "/opera software/" in path_lower or "/.config/opera" in path_lower or "com.operasoftware.opera" in path_lower:
        return "opera"
    elif "/vivaldi/" in path_lower or "vivaldi" in path_lower:
        return "vivaldi"

    return None


def extract_profile_from_path(path: str) -> _Optional[str]:
    """
    Extract Chromium profile name from a file path.

    Args:
        path: Full path to a file in a Chromium profile (str or Path)

    Returns:
        Profile name (e.g., "Default", "Profile 1"), or None if not detected

    Examples:
        >>> extract_profile_from_path("Users/john/AppData/Local/Google/Chrome/User Data/Profile 1/History")
        "Profile 1"
        >>> extract_profile_from_path("home/user/.config/google-chrome/Default/History")
        "Default"
    """
    # Convert Path to string if needed
    path_str = str(path).replace("\\", "/")
    parts = path_str.split("/")

    # Look for browser-specific markers and extract profile
    try:
        # Windows/macOS: "User Data/Default/History" or "User Data/Profile 1/History"
        if "User Data" in parts:
            idx = parts.index("User Data")
            if idx + 1 < len(parts):
                return parts[idx + 1]

        # Linux Chrome: ".config/google-chrome/Default/History"
        if "google-chrome" in parts:
            idx = parts.index("google-chrome")
            if idx + 1 < len(parts):
                return parts[idx + 1]

        # Linux Edge: ".config/microsoft-edge/Default/History"
        if "microsoft-edge" in parts:
            idx = parts.index("microsoft-edge")
            if idx + 1 < len(parts):
                return parts[idx + 1]

        # Linux Brave: ".config/BraveSoftware/Brave-Browser/Default/History"
        if "Brave-Browser" in parts:
            idx = parts.index("Brave-Browser")
            if idx + 1 < len(parts):
                return parts[idx + 1]

        # Linux Opera: ".config/opera/Default/History"
        if "opera" in parts:
            idx = parts.index("opera")
            if idx + 1 < len(parts):
                return parts[idx + 1]

        # macOS Chrome: "Google/Chrome/Default/History"
        if "Chrome" in parts:
            idx = parts.index("Chrome")
            if idx + 1 < len(parts):
                return parts[idx + 1]

        # macOS Edge: "Microsoft Edge/Default/History"
        if "Microsoft Edge" in parts:
            idx = parts.index("Microsoft Edge")
            if idx + 1 < len(parts):
                return parts[idx + 1]

        # Windows Opera: "Opera Stable/History" or "Opera GX Stable/History"
        for opera_dir in ["Opera Stable", "Opera GX Stable"]:
            if opera_dir in parts:
                return opera_dir

        # macOS Opera: "com.operasoftware.Opera/History" or "com.operasoftware.OperaGX/History"
        # These are single-profile browsers, so return "Default"
        for opera_bundle in ["com.operasoftware.Opera", "com.operasoftware.OperaGX"]:
            if opera_bundle in parts:
                return "Default"

    except (ValueError, IndexError):
        pass

    return None


# ===========================================================================
# History Parsing (delegated to history/_parser.py)
# ===========================================================================
# History parsing moved to history/_parser.py for modularity.
# These re-exports maintain backward compatibility.

from .history._parser import (
    HistoryVisit,
    SearchTerm,
    parse_history_visits,
    parse_history_urls,
    get_history_stats,
    parse_keyword_search_terms,
    get_search_terms_stats,
)


# ===========================================================================
# Cookie Parsing (delegated to cookies/_parsers.py)
# ===========================================================================
# Cookie parsing moved to cookies/_parsers.py for modularity.
# These re-exports maintain backward compatibility.

from .cookies._parsers import (
    ChromiumCookie,
    parse_cookies,
    get_cookie_stats,
)

# Re-export SameSite mapping for backward compatibility
from .cookies._schemas import SAMESITE_VALUES as SAMESITE_MAP


# ===========================================================================
# Bookmark Parsing (JSON format)
# ===========================================================================
# NOTE: Bookmark parsing has been moved to bookmarks/_parser.py for modularity.
# These re-exports maintain backward compatibility.

from .bookmarks._parser import (
    ChromiumBookmark,
    parse_bookmarks_json,
    get_bookmark_stats as get_bookmark_stats_json,
)

# Alias for backward compatibility
__all_bookmark_exports__ = ["ChromiumBookmark", "parse_bookmarks_json", "get_bookmark_stats_json"]


# ===========================================================================
# Download Parsing
# ===========================================================================

@dataclass
class ChromiumDownload:
    """A single Chromium download record."""
    id: int
    target_path: str
    start_time: Optional[datetime]
    start_time_iso: Optional[str]
    end_time: Optional[datetime]
    end_time_iso: Optional[str]
    received_bytes: int
    total_bytes: int
    state: str
    danger_type: str
    opened: bool
    last_access_time: Optional[datetime]
    last_access_time_iso: Optional[str]
    referrer: Optional[str]
    tab_url: Optional[str]
    tab_referrer_url: Optional[str]
    mime_type: Optional[str]
    original_mime_type: Optional[str]
    url_chain: List[str]


# Chromium download state mapping
DOWNLOAD_STATE_MAP = {
    0: "in_progress",
    1: "complete",
    2: "cancelled",
    3: "interrupted",
    4: "interrupted_network",
}

# Chromium danger type mapping
DANGER_TYPE_MAP = {
    0: "not_dangerous",
    1: "dangerous_file",
    2: "dangerous_url",
    3: "dangerous_content",
    4: "maybe_dangerous_content",
    5: "uncommon_content",
    6: "user_validated",
    7: "dangerous_host",
    8: "potentially_unwanted",
    9: "allowlisted_by_policy",
}


def parse_downloads(conn: sqlite3.Connection) -> Iterator[ChromiumDownload]:
    """
    Parse Chromium History database downloads.

    Args:
        conn: SQLite connection to History database

    Yields:
        ChromiumDownload records ordered by start_time DESC

    Note:
        Downloads are stored in the History database, not a separate file.
        URL chain provides redirect history for forensic analysis.
    """
    if not table_exists(conn, "downloads"):
        return

    # Check if downloads_url_chains exists (modern Chromium)
    has_url_chains = table_exists(conn, "downloads_url_chains")

    # Query downloads
    query = """
        SELECT
            id,
            target_path,
            start_time,
            end_time,
            received_bytes,
            total_bytes,
            state,
            danger_type,
            COALESCE(opened, 0) as opened,
            COALESCE(last_access_time, 0) as last_access_time,
            referrer,
            tab_url,
            tab_referrer_url,
            mime_type,
            original_mime_type
        FROM downloads
        ORDER BY start_time DESC
    """

    try:
        rows = safe_execute(conn, query)
    except Exception:
        return

    for row in rows:
        # Get URL chain if available
        url_chain = []
        if has_url_chains:
            chain_query = """
                SELECT url FROM downloads_url_chains
                WHERE id = ? ORDER BY chain_index
            """
            try:
                chain_rows = safe_execute(conn, chain_query, (row["id"],))
                url_chain = [r["url"] for r in chain_rows]
            except Exception:
                pass

        # Convert timestamps
        start_dt = webkit_to_datetime(row["start_time"])
        start_iso = webkit_to_iso(row["start_time"])
        end_dt = webkit_to_datetime(row["end_time"]) if row["end_time"] else None
        end_iso = webkit_to_iso(row["end_time"]) if row["end_time"] else None
        last_access_dt = webkit_to_datetime(row["last_access_time"]) if row["last_access_time"] else None
        last_access_iso = webkit_to_iso(row["last_access_time"]) if row["last_access_time"] else None

        yield ChromiumDownload(
            id=row["id"],
            target_path=row["target_path"] or "",
            start_time=start_dt,
            start_time_iso=start_iso,
            end_time=end_dt,
            end_time_iso=end_iso,
            received_bytes=row["received_bytes"] or 0,
            total_bytes=row["total_bytes"] or 0,
            state=DOWNLOAD_STATE_MAP.get(row["state"], f"unknown_{row['state']}"),
            danger_type=DANGER_TYPE_MAP.get(row["danger_type"], f"unknown_{row['danger_type']}"),
            opened=bool(row["opened"]),
            last_access_time=last_access_dt,
            last_access_time_iso=last_access_iso,
            referrer=row["referrer"],
            tab_url=row["tab_url"],
            tab_referrer_url=row["tab_referrer_url"],
            mime_type=row["mime_type"],
            original_mime_type=row["original_mime_type"],
            url_chain=url_chain,
        )


def get_download_stats(conn: sqlite3.Connection) -> Dict[str, int]:
    """Get quick statistics from downloads table."""
    stats = {"download_count": 0, "complete_count": 0, "dangerous_count": 0}

    if not table_exists(conn, "downloads"):
        return stats

    rows = safe_execute(conn, "SELECT COUNT(*) as cnt FROM downloads")
    stats["download_count"] = rows[0]["cnt"] if rows else 0

    rows = safe_execute(conn, "SELECT COUNT(*) as cnt FROM downloads WHERE state = 1")
    stats["complete_count"] = rows[0]["cnt"] if rows else 0

    rows = safe_execute(conn, "SELECT COUNT(*) as cnt FROM downloads WHERE danger_type > 0")
    stats["dangerous_count"] = rows[0]["cnt"] if rows else 0

    return stats

