"""
Safari artifact parsers.

Safari uses Apple-specific formats:
- History.db: SQLite with Cocoa timestamps (seconds since 2001-01-01)
- Cookies.binarycookies: Binary format (requires binarycookies library)
- Bookmarks.plist: Binary/XML plist format
- Downloads.plist: Plist format

Key Differences from Chromium/Firefox:
- Timestamps: Cocoa epoch (Jan 1, 2001) not Unix or WebKit
- Cookies: Binary format, not SQLite
- Bookmarks: Plist format, not JSON
- Downloads: Plist format, not SQLite
"""

from __future__ import annotations

import sqlite3
import plistlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any, Iterator

# Cocoa epoch: January 1, 2001 00:00:00 UTC
# Cocoa timestamps are seconds (float) since this date
COCOA_EPOCH_OFFSET = 978307200  # Seconds between Unix epoch (1970) and Cocoa epoch (2001)


# =============================================================================
# Timestamp Conversion
# =============================================================================

def cocoa_to_datetime(cocoa_time: Optional[float]) -> Optional[datetime]:
    """
    Convert Cocoa timestamp to datetime.

    Cocoa timestamps are seconds since January 1, 2001 00:00:00 UTC.
    This is NSDate's reference date.

    Args:
        cocoa_time: Cocoa timestamp (seconds since 2001-01-01)

    Returns:
        Datetime in UTC, or None if conversion fails

    Examples:
        >>> cocoa_to_datetime(0)
        datetime.datetime(2001, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        >>> cocoa_to_datetime(759398400)  # Jan 1, 2025
        datetime.datetime(2025, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    """
    if cocoa_time is None:
        return None
    try:
        unix_seconds = cocoa_time + COCOA_EPOCH_OFFSET
        return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def cocoa_to_iso(cocoa_time: Optional[float]) -> Optional[str]:
    """
    Convert Cocoa timestamp to ISO 8601 string.

    Args:
        cocoa_time: Cocoa timestamp (seconds since 2001-01-01)

    Returns:
        ISO 8601 string or None
    """
    dt = cocoa_to_datetime(cocoa_time)
    return dt.isoformat() if dt else None


# =============================================================================
# History Parsing
# =============================================================================

@dataclass
class SafariVisit:
    """Safari history visit record."""
    url: str
    title: Optional[str]
    visit_time: Optional[datetime]
    visit_time_utc: Optional[str]
    redirect_source: Optional[int]
    redirect_destination: Optional[int]
    history_item_id: int


def parse_history_visits(db_path: Path) -> List[SafariVisit]:
    """
    Parse Safari History.db for visit records.

    Safari stores history in two tables:
    - history_items: URLs with their metadata
    - history_visits: Individual visit records with timestamps

    Args:
        db_path: Path to History.db

    Returns:
        List of SafariVisit objects sorted by visit time (newest first)
    """
    visits: List[SafariVisit] = []

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Check if required tables exist
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('history_items', 'history_visits')"
        )
        tables = {row[0] for row in cursor.fetchall()}

        if "history_items" not in tables or "history_visits" not in tables:
            conn.close()
            return visits

        # Safari schema: history_items + history_visits
        # visit_time is Cocoa timestamp (seconds since 2001-01-01)
        cursor.execute("""
            SELECT
                hi.id,
                hi.url,
                hv.title,
                hv.visit_time,
                hv.redirect_source,
                hv.redirect_destination
            FROM history_items hi
            LEFT JOIN history_visits hv ON hi.id = hv.history_item
            WHERE hv.visit_time IS NOT NULL
            ORDER BY hv.visit_time DESC
        """)

        for row in cursor:
            visit_time = cocoa_to_datetime(row["visit_time"])
            visits.append(SafariVisit(
                url=row["url"] or "",
                title=row["title"],
                visit_time=visit_time,
                visit_time_utc=visit_time.isoformat() if visit_time else None,
                redirect_source=row["redirect_source"],
                redirect_destination=row["redirect_destination"],
                history_item_id=row["id"],
            ))

        conn.close()

    except sqlite3.Error:
        pass

    return visits


def get_history_stats(visits: List[SafariVisit]) -> Dict[str, Any]:
    """
    Get statistics about parsed Safari history.

    Args:
        visits: List of SafariVisit objects

    Returns:
        Statistics dictionary
    """
    if not visits:
        return {
            "total_visits": 0,
            "unique_urls": 0,
            "date_range": None,
        }

    urls = {v.url for v in visits}
    times = [v.visit_time for v in visits if v.visit_time]

    return {
        "total_visits": len(visits),
        "unique_urls": len(urls),
        "date_range": {
            "earliest": min(times).isoformat() if times else None,
            "latest": max(times).isoformat() if times else None,
        },
    }


# =============================================================================
# Cookies Parsing
# =============================================================================

@dataclass
class SafariCookie:
    """Safari cookie record."""
    domain: str
    name: str
    value: str
    path: str
    expires: Optional[datetime]
    expires_utc: Optional[str]
    creation_time: Optional[datetime]
    creation_time_utc: Optional[str]
    is_secure: bool
    is_httponly: bool
    # Safari cookies are NOT encrypted locally (unlike Chromium)
    is_encrypted: bool = False


def parse_cookies(file_path: Path) -> List[SafariCookie]:
    """
    Parse Safari Cookies.binarycookies file.

    Requires the 'binarycookies' library to be installed.
    Returns empty list if library is not available.

    Args:
        file_path: Path to Cookies.binarycookies

    Returns:
        List of SafariCookie objects
    """
    cookies: List[SafariCookie] = []

    try:
        import binarycookies
    except ImportError:
        # Library not installed - return empty list
        return cookies

    try:
        with open(file_path, 'rb') as f:
            jar = binarycookies.parse(f)

        for cookie in jar:
            # Convert expiry timestamp
            expires = None
            expires_utc = None
            if hasattr(cookie, 'expires') and cookie.expires:
                try:
                    expires = datetime.fromtimestamp(
                        cookie.expires, tz=timezone.utc
                    )
                    expires_utc = expires.isoformat()
                except (ValueError, OSError, OverflowError):
                    pass

            # Creation time (if available)
            creation = None
            creation_utc = None
            if hasattr(cookie, 'creation') and cookie.creation:
                try:
                    creation = datetime.fromtimestamp(
                        cookie.creation, tz=timezone.utc
                    )
                    creation_utc = creation.isoformat()
                except (ValueError, OSError, OverflowError):
                    pass

            cookies.append(SafariCookie(
                domain=getattr(cookie, 'domain', '') or '',
                name=getattr(cookie, 'name', '') or '',
                value=getattr(cookie, 'value', '') or '',
                path=getattr(cookie, 'path', '/') or '/',
                expires=expires,
                expires_utc=expires_utc,
                creation_time=creation,
                creation_time_utc=creation_utc,
                is_secure=getattr(cookie, 'secure', False),
                is_httponly=getattr(cookie, 'http_only', False),
            ))

    except Exception:
        pass

    return cookies


def get_cookie_stats(cookies: List[SafariCookie]) -> Dict[str, Any]:
    """
    Get statistics about parsed Safari cookies.

    Args:
        cookies: List of SafariCookie objects

    Returns:
        Statistics dictionary
    """
    if not cookies:
        return {
            "total_cookies": 0,
            "unique_domains": 0,
            "secure_count": 0,
            "httponly_count": 0,
        }

    domains = {c.domain for c in cookies}

    return {
        "total_cookies": len(cookies),
        "unique_domains": len(domains),
        "secure_count": sum(1 for c in cookies if c.is_secure),
        "httponly_count": sum(1 for c in cookies if c.is_httponly),
    }


# =============================================================================
# Bookmarks Parsing
# =============================================================================

@dataclass
class SafariBookmark:
    """Safari bookmark record."""
    url: str
    title: str
    folder_path: str
    date_added: Optional[datetime]
    date_added_utc: Optional[str]
    bookmark_type: str  # "leaf" for bookmark, "list" for folder


def parse_bookmarks(file_path: Path) -> List[SafariBookmark]:
    """
    Parse Safari Bookmarks.plist file.

    Safari uses plist format (binary or XML) for bookmarks.
    Structure is hierarchical with folders containing children.

    Args:
        file_path: Path to Bookmarks.plist

    Returns:
        List of SafariBookmark objects
    """
    bookmarks: List[SafariBookmark] = []

    try:
        with open(file_path, 'rb') as f:
            plist_data = plistlib.load(f)

        # Recursively extract bookmarks
        _extract_bookmarks_recursive(plist_data, bookmarks, "")

    except Exception:
        pass

    return bookmarks


def _extract_bookmarks_recursive(
    node: Any,
    bookmarks: List[SafariBookmark],
    folder_path: str
) -> None:
    """
    Recursively extract bookmarks from plist structure.

    Safari bookmark plist structure:
    - WebBookmarkType: "WebBookmarkTypeLeaf" (bookmark) or "WebBookmarkTypeList" (folder)
    - URLString: URL for bookmarks
    - URIDictionary: Contains "title" for bookmarks
    - Title: Folder name for folders
    - Children: Array of child items for folders
    """
    if not isinstance(node, dict):
        return

    node_type = node.get("WebBookmarkType", "")

    if node_type == "WebBookmarkTypeLeaf":
        # This is a bookmark
        url_dict = node.get("URIDictionary", {})
        title = url_dict.get("title", "") if isinstance(url_dict, dict) else ""

        bookmarks.append(SafariBookmark(
            url=node.get("URLString", "") or "",
            title=title or "",
            folder_path=folder_path,
            date_added=None,  # Safari doesn't store date in plist
            date_added_utc=None,
            bookmark_type="leaf",
        ))

    elif node_type == "WebBookmarkTypeList":
        # This is a folder
        folder_name = node.get("Title", "")
        new_path = f"{folder_path}/{folder_name}" if folder_path else folder_name

        children = node.get("Children", [])
        if isinstance(children, list):
            for child in children:
                _extract_bookmarks_recursive(child, bookmarks, new_path)

    # Handle root-level Children without explicit type
    elif "Children" in node:
        children = node.get("Children", [])
        if isinstance(children, list):
            for child in children:
                _extract_bookmarks_recursive(child, bookmarks, folder_path)


def get_bookmark_stats(bookmarks: List[SafariBookmark]) -> Dict[str, Any]:
    """
    Get statistics about parsed Safari bookmarks.

    Args:
        bookmarks: List of SafariBookmark objects

    Returns:
        Statistics dictionary
    """
    if not bookmarks:
        return {
            "total_bookmarks": 0,
            "unique_folders": 0,
        }

    # Only count actual bookmarks (not folder entries)
    actual_bookmarks = [b for b in bookmarks if b.bookmark_type == "leaf"]
    folders = {b.folder_path for b in actual_bookmarks if b.folder_path}

    return {
        "total_bookmarks": len(actual_bookmarks),
        "unique_folders": len(folders),
    }


# =============================================================================
# Downloads Parsing
# =============================================================================

@dataclass
class SafariDownload:
    """Safari download record."""
    url: str
    target_path: str
    filename: str
    total_bytes: int
    received_bytes: int
    state: str  # Safari only stores completed downloads
    identifier: Optional[str]


def parse_downloads(file_path: Path) -> List[SafariDownload]:
    """
    Parse Safari Downloads.plist file.

    Safari stores download history in plist format.
    Structure can be either a list or a dictionary with "DownloadHistory" key.

    Args:
        file_path: Path to Downloads.plist

    Returns:
        List of SafariDownload objects
    """
    downloads: List[SafariDownload] = []

    try:
        with open(file_path, 'rb') as f:
            plist_data = plistlib.load(f)

        # Handle both formats: direct list or dictionary with DownloadHistory key
        if isinstance(plist_data, list):
            download_list = plist_data
        elif isinstance(plist_data, dict):
            download_list = plist_data.get("DownloadHistory", [])
        else:
            return downloads

        for dl in download_list:
            if not isinstance(dl, dict):
                continue

            # Extract download info - multiple possible key names
            url = (
                dl.get("DownloadEntryURL") or
                dl.get("DownloadURL") or
                ""
            )

            target_path = (
                dl.get("DownloadEntryPath") or
                dl.get("DownloadPath") or
                ""
            )

            # Extract filename from path
            filename = Path(target_path).name if target_path else ""

            # Byte counts
            total_bytes = (
                dl.get("DownloadEntryProgressTotalToLoad") or
                dl.get("DownloadTotalBytes") or
                0
            )
            received_bytes = (
                dl.get("DownloadEntryProgressBytesSoFar") or
                dl.get("DownloadReceivedBytes") or
                total_bytes  # Assume complete if not specified
            )

            # Identifier (UUID-like)
            identifier = dl.get("DownloadEntryIdentifier")

            downloads.append(SafariDownload(
                url=url,
                target_path=target_path,
                filename=filename,
                total_bytes=total_bytes,
                received_bytes=received_bytes,
                state="complete",  # Safari only stores completed downloads
                identifier=identifier,
            ))

    except Exception:
        pass

    return downloads


def get_download_stats(downloads: List[SafariDownload]) -> Dict[str, Any]:
    """
    Get statistics about parsed Safari downloads.

    Args:
        downloads: List of SafariDownload objects

    Returns:
        Statistics dictionary
    """
    if not downloads:
        return {
            "total_downloads": 0,
            "total_bytes": 0,
        }

    return {
        "total_downloads": len(downloads),
        "total_bytes": sum(d.total_bytes for d in downloads),
    }
