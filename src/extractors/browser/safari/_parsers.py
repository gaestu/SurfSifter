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
from typing import List, Optional, Dict, Any, Iterator, Set
from urllib.parse import urljoin

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


# =============================================================================
# Sessions Parsing
# =============================================================================

@dataclass
class SafariSessionTab:
    """Safari session tab record from LastSession.plist."""
    tab_url: str
    tab_title: str
    last_visit_time: Optional[datetime]
    tab_index: int
    window_index: int
    is_pinned: bool
    tab_uuid: Optional[str]
    back_forward_entries: List[Dict[str, Any]]


@dataclass
class SafariSessionWindow:
    """Safari session window record from LastSession.plist."""
    window_index: int
    selected_tab_index: int
    is_private: bool
    tab_count: int


@dataclass
class SafariClosedTab:
    """Safari recently closed tab record from RecentlyClosedTabs.plist."""
    tab_url: str
    tab_title: str
    date_closed: Optional[datetime]


def parse_session_plist(file_path: Path) -> Dict[str, Any]:
    """
    Parse Safari LastSession.plist.

    Returns:
        Dict with keys: windows, tabs, history, closed_tabs
    """
    result: Dict[str, Any] = {
        "windows": [],
        "tabs": [],
        "history": [],
        "closed_tabs": [],
    }

    try:
        with open(file_path, "rb") as f:
            plist_data = plistlib.load(f)
    except Exception:
        return result

    if not isinstance(plist_data, dict):
        return result

    windows = plist_data.get("SessionWindows", [])
    if not isinstance(windows, list):
        return result

    for window_index, window_data in enumerate(windows):
        if not isinstance(window_data, dict):
            continue

        tab_states = window_data.get("TabStates", [])
        if not isinstance(tab_states, list):
            tab_states = []

        selected_tab_index = _coerce_int(window_data.get("SelectedTabIndex"), default=0)
        is_private = bool(window_data.get("IsPrivateWindow", False))

        result["windows"].append(
            SafariSessionWindow(
                window_index=window_index,
                selected_tab_index=selected_tab_index,
                is_private=is_private,
                tab_count=len(tab_states),
            )
        )

        for tab_index, tab_data in enumerate(tab_states):
            if not isinstance(tab_data, dict):
                continue

            tab_url = str(tab_data.get("TabURL") or "").strip()
            if not _is_non_blank_url(tab_url):
                continue

            tab_title = str(tab_data.get("TabTitle") or "").strip()
            tab_uuid = tab_data.get("TabUUID")
            last_visit = cocoa_to_datetime(_coerce_float(tab_data.get("LastVisitTime")))
            is_pinned = bool(tab_data.get("IsAppTab", False))

            history_entries = _parse_back_forward_list(tab_data)
            if not history_entries:
                state_blob = tab_data.get("SessionState") or tab_data.get("SessionStateData")
                if isinstance(state_blob, (bytes, bytearray)):
                    history_entries = _parse_session_state_archive(bytes(state_blob))

            result["tabs"].append(
                SafariSessionTab(
                    tab_url=tab_url,
                    tab_title=tab_title,
                    last_visit_time=last_visit,
                    tab_index=tab_index,
                    window_index=window_index,
                    is_pinned=is_pinned,
                    tab_uuid=tab_uuid if isinstance(tab_uuid, str) else None,
                    back_forward_entries=history_entries,
                )
            )

            for nav_index, entry in enumerate(history_entries):
                history_url = str(entry.get("url") or "").strip()
                if not _is_non_blank_url(history_url):
                    continue

                result["history"].append(
                    {
                        "window_index": window_index,
                        "tab_index": tab_index,
                        "nav_index": _coerce_int(entry.get("nav_index"), default=nav_index),
                        "url": history_url,
                        "title": str(entry.get("title") or ""),
                        "timestamp_utc": last_visit.isoformat() if last_visit else None,
                    }
                )

    return result


def parse_recently_closed_tabs(file_path: Path) -> List[SafariClosedTab]:
    """
    Parse Safari RecentlyClosedTabs.plist.

    Returns:
        List of SafariClosedTab records.
    """
    closed_tabs: List[SafariClosedTab] = []

    try:
        with open(file_path, "rb") as f:
            plist_data = plistlib.load(f)
    except Exception:
        return closed_tabs

    tab_entries: List[Any] = []
    if isinstance(plist_data, list):
        tab_entries = plist_data
    elif isinstance(plist_data, dict):
        for key in ("RecentlyClosedTabs", "ClosedTabOrWindowPersistentStates"):
            candidate = plist_data.get(key)
            if isinstance(candidate, list):
                tab_entries = candidate
                break
        if not tab_entries:
            for value in plist_data.values():
                if isinstance(value, list):
                    tab_entries = value
                    break

    for entry in tab_entries:
        if not isinstance(entry, dict):
            continue

        tab_url = str(entry.get("TabURL") or "").strip()
        if not _is_non_blank_url(tab_url):
            continue

        tab_title = str(entry.get("TabTitle") or "").strip()
        date_closed = cocoa_to_datetime(_coerce_float(entry.get("DateClosed")))

        closed_tabs.append(
            SafariClosedTab(
                tab_url=tab_url,
                tab_title=tab_title,
                date_closed=date_closed,
            )
        )

    return closed_tabs


def _parse_back_forward_list(tab_dict: dict) -> List[Dict[str, Any]]:
    """Extract tab navigation entries from BackForwardList (Safari <= 12)."""
    entries: List[Dict[str, Any]] = []
    back_forward = tab_dict.get("BackForwardList")
    if not isinstance(back_forward, dict):
        return entries

    raw_entries = back_forward.get("Entries", [])
    if not isinstance(raw_entries, list):
        return entries

    for nav_index, item in enumerate(raw_entries):
        if not isinstance(item, dict):
            continue

        url = str(item.get("URL") or item.get("url") or "").strip()
        if not _is_non_blank_url(url):
            continue

        title = str(item.get("Title") or item.get("title") or "").strip()
        entries.append({"url": url, "title": title, "nav_index": nav_index})

    return entries


def _parse_session_state_archive(blob: bytes) -> List[Dict[str, Any]]:
    """
    Best-effort parse for Safari SessionState NSKeyedArchive blobs (Safari 13+).

    Returns:
        List of {url, title, nav_index} dicts. Empty list on parse failures.
    """
    if not blob:
        return []

    try:
        archive = plistlib.loads(blob)
    except Exception:
        return []

    objects = archive.get("$objects") if isinstance(archive, dict) else None
    object_list = objects if isinstance(objects, list) else []

    collected: List[Dict[str, Any]] = []
    seen: Set[int] = set()
    _collect_archive_entries(archive, object_list, collected, seen)

    # Normalize nav_index and remove duplicates while preserving order.
    normalized: List[Dict[str, Any]] = []
    seen_keys: Set[tuple[str, str]] = set()
    for item in collected:
        url = str(item.get("url") or "").strip()
        if not _is_non_blank_url(url):
            continue

        title = str(item.get("title") or "").strip()
        key = (url, title)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        normalized.append(
            {
                "url": url,
                "title": title,
                "nav_index": len(normalized),
            }
        )

    return normalized


def get_session_stats(
    windows: List[SafariSessionWindow],
    tabs: List[SafariSessionTab],
) -> Dict[str, Any]:
    """Get statistics about parsed Safari session data."""
    if not windows and not tabs:
        return {
            "total_windows": 0,
            "total_tabs": 0,
            "private_windows": 0,
            "pinned_tabs": 0,
            "date_range": None,
        }

    tab_times = [tab.last_visit_time for tab in tabs if tab.last_visit_time]
    private_windows = sum(1 for window in windows if window.is_private)
    pinned_tabs = sum(1 for tab in tabs if tab.is_pinned)

    return {
        "total_windows": len(windows),
        "total_tabs": len(tabs),
        "private_windows": private_windows,
        "pinned_tabs": pinned_tabs,
        "date_range": {
            "earliest": min(tab_times).isoformat() if tab_times else None,
            "latest": max(tab_times).isoformat() if tab_times else None,
        },
    }


def _collect_archive_entries(
    node: Any,
    objects: List[Any],
    out: List[Dict[str, Any]],
    seen: Set[int],
) -> None:
    """Recursively collect URL/title candidates from NSKeyedArchive object graph."""
    node = _resolve_archive_object(node, objects)

    if isinstance(node, (dict, list, tuple)):
        marker = id(node)
        if marker in seen:
            return
        seen.add(marker)

    if isinstance(node, dict):
        entry = _extract_url_title_from_mapping(node, objects)
        if entry:
            out.append(entry)

        decoded = _decode_ns_keyed_dict(node, objects)
        if decoded:
            decoded_entry = _extract_url_title_from_mapping(decoded, objects)
            if decoded_entry:
                out.append(decoded_entry)
            for value in decoded.values():
                _collect_archive_entries(value, objects, out, seen)

        for value in node.values():
            _collect_archive_entries(value, objects, out, seen)
        return

    if isinstance(node, list):
        for item in node:
            _collect_archive_entries(item, objects, out, seen)
        return

    if isinstance(node, tuple):
        for item in node:
            _collect_archive_entries(item, objects, out, seen)


def _extract_url_title_from_mapping(mapping: Dict[Any, Any], objects: List[Any]) -> Optional[Dict[str, Any]]:
    """Extract URL/title pair from a mapping if present."""
    url_candidates: List[str] = []
    title_value: Optional[str] = None

    for key, value in mapping.items():
        if not isinstance(key, str):
            continue

        key_lower = key.lower()
        resolved_value = _stringify_archive_value(value, objects)

        if "title" in key_lower and not title_value:
            title_value = resolved_value
            continue

        if "url" in key_lower and resolved_value:
            url_candidates.append(resolved_value)
            continue

        if key in ("NS.relative", "NS.string") and resolved_value:
            url_candidates.append(resolved_value)

    # Build URL from NSURL-style {NS.base, NS.relative} pairs when needed.
    if not url_candidates:
        relative = _stringify_archive_value(mapping.get("NS.relative"), objects)
        base = _stringify_archive_value(mapping.get("NS.base"), objects)
        if relative:
            if "://" in relative or relative.startswith(("about:", "file:", "data:", "safari-")):
                url_candidates.append(relative)
            elif base:
                url_candidates.append(urljoin(base, relative))

    for url in url_candidates:
        if _is_non_blank_url(url):
            return {"url": url, "title": title_value or ""}

    return None


def _decode_ns_keyed_dict(node: Dict[Any, Any], objects: List[Any]) -> Dict[str, Any]:
    """Decode NSKeyedArchive NSDictionary-like nodes (NS.keys / NS.objects)."""
    raw_keys = node.get("NS.keys")
    raw_values = node.get("NS.objects")

    if not isinstance(raw_keys, list) or not isinstance(raw_values, list):
        return {}

    decoded: Dict[str, Any] = {}
    for key_obj, value_obj in zip(raw_keys, raw_values):
        key = _stringify_archive_value(key_obj, objects)
        if not key:
            continue
        decoded[key] = _resolve_archive_object(value_obj, objects)

    return decoded


def _resolve_archive_object(value: Any, objects: List[Any]) -> Any:
    """Resolve plistlib.UID references to their underlying archive object."""
    if isinstance(value, plistlib.UID):
        index = _coerce_int(value.data, default=-1)
        if 0 <= index < len(objects):
            return objects[index]
    return value


def _stringify_archive_value(value: Any, objects: List[Any]) -> str:
    """Convert a potential archive value into string when possible."""
    resolved = _resolve_archive_object(value, objects)

    if isinstance(resolved, str):
        return resolved.strip()

    if isinstance(resolved, bytes):
        try:
            return resolved.decode("utf-8", errors="ignore").strip()
        except Exception:
            return ""

    if isinstance(resolved, dict):
        # Common NSURL representation in keyed archives.
        relative = _stringify_archive_value(resolved.get("NS.relative"), objects)
        if relative:
            return relative
        string_value = _stringify_archive_value(resolved.get("NS.string"), objects)
        if string_value:
            return string_value
        for key in ("URL", "url", "OriginalURL", "originalURL"):
            candidate = _stringify_archive_value(resolved.get(key), objects)
            if candidate:
                return candidate

    if isinstance(resolved, (int, float)):
        return str(resolved)

    return ""


def _coerce_int(value: Any, default: int = 0) -> int:
    """Safely coerce value to int."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any) -> Optional[float]:
    """Safely coerce value to float."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_non_blank_url(url: str) -> bool:
    """Return True for non-empty URLs excluding about:blank."""
    candidate = (url or "").strip()
    if not candidate:
        return False
    if candidate.lower() == "about:blank":
        return False
    return True
