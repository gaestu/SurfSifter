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
from dataclasses import dataclass, field
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
            jar = binarycookies.load(f)

        for cookie in jar:
            # The binarycookies library Cookie model uses:
            #   url (domain), name, value, path,
            #   expiry_datetime (datetime|None), create_datetime (datetime|None),
            #   flag (Flag enum: SECURE, HTTPONLY, SECURE_HTTPONLY, UNKNOWN)

            # Expiry timestamp
            expires = None
            expires_utc = None
            expiry_dt = getattr(cookie, 'expiry_datetime', None)
            if expiry_dt is not None:
                try:
                    if isinstance(expiry_dt, datetime):
                        expires = expiry_dt if expiry_dt.tzinfo else expiry_dt.replace(tzinfo=timezone.utc)
                    else:
                        expires = datetime.fromtimestamp(float(expiry_dt), tz=timezone.utc)
                    expires_utc = expires.isoformat()
                except (ValueError, OSError, OverflowError, TypeError):
                    pass

            # Creation time
            creation = None
            creation_utc = None
            create_dt = getattr(cookie, 'create_datetime', None)
            if create_dt is not None:
                try:
                    if isinstance(create_dt, datetime):
                        creation = create_dt if create_dt.tzinfo else create_dt.replace(tzinfo=timezone.utc)
                    else:
                        creation = datetime.fromtimestamp(float(create_dt), tz=timezone.utc)
                    creation_utc = creation.isoformat()
                except (ValueError, OSError, OverflowError, TypeError):
                    pass

            # Flag-based secure/httponly detection
            flag = getattr(cookie, 'flag', None)
            flag_str = str(flag).upper() if flag is not None else ""
            is_secure = "SECURE" in flag_str
            is_httponly = "HTTPONLY" in flag_str

            cookies.append(SafariCookie(
                domain=getattr(cookie, 'url', '') or '',
                name=getattr(cookie, 'name', '') or '',
                value=getattr(cookie, 'value', '') or '',
                path=getattr(cookie, 'path', '/') or '/',
                expires=expires,
                expires_utc=expires_utc,
                creation_time=creation,
                creation_time_utc=creation_utc,
                is_secure=is_secure,
                is_httponly=is_httponly,
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
# Top Sites Parsing
# =============================================================================

@dataclass
class SafariTopSite:
    """Safari top site record from TopSites.plist."""
    url: str
    title: str
    rank: int
    is_built_in: bool
    is_banned: bool = False


def parse_top_sites(file_path: Path) -> List[SafariTopSite]:
    """
    Parse Safari TopSites.plist file.

    Supports known plist layouts:
    - {"TopSites": [ ... ]}
    - {"BannerList": [ ... ]}
    - {"BannedURLStrings": [ ... ]}  (user-removed sites, forensically relevant)
    - [ ... ] (root list fallback)
    """
    sites: List[SafariTopSite] = []

    try:
        with open(file_path, "rb") as f:
            plist_data = plistlib.load(f)
    except Exception:
        return sites

    entries: List[Any] = []
    if isinstance(plist_data, dict):
        for key in ("TopSites", "BannerList"):
            value = plist_data.get(key)
            if isinstance(value, list):
                entries = value
                break
    elif isinstance(plist_data, list):
        entries = plist_data

    for rank, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue

        url = str(entry.get("TopSiteURLString") or entry.get("URLString") or "").strip()
        if not url:
            continue

        title = str(entry.get("TopSiteTitle") or entry.get("Title") or "").strip()
        is_built_in = bool(entry.get("TopSiteIsBuiltIn", False))

        sites.append(
            SafariTopSite(
                url=url,
                title=title,
                rank=rank,
                is_built_in=is_built_in,
            )
        )

    # Parse BannedURLStrings — these are URLs the user explicitly removed
    # from Top Sites.  Forensically significant as indicators of user intent.
    if isinstance(plist_data, dict):
        banned = plist_data.get("BannedURLStrings")
        if isinstance(banned, list):
            for idx, raw_url in enumerate(banned):
                url = str(raw_url or "").strip()
                if not url:
                    continue
                sites.append(
                    SafariTopSite(
                        url=url,
                        title="",
                        rank=-1,
                        is_built_in=False,
                        is_banned=True,
                    )
                )

    return sites


def get_top_site_stats(sites: List[SafariTopSite]) -> Dict[str, Any]:
    """Get statistics about parsed Safari top sites."""
    if not sites:
        return {
            "total_sites": 0,
            "unique_urls": 0,
            "built_in_count": 0,
            "banned_count": 0,
        }

    return {
        "total_sites": len(sites),
        "unique_urls": len({site.url for site in sites}),
        "built_in_count": sum(1 for site in sites if site.is_built_in),
        "banned_count": sum(1 for site in sites if site.is_banned),
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

    Handles multiple Safari plist formats:
    - Flat list: ``[{TabURL, TabTitle, DateClosed}, ...]``
    - Dict with ``RecentlyClosedTabs`` key (older Safari)
    - Dict with ``ClosedTabOrWindowPersistentStates`` key (Safari 14+):
      Each entry wraps data in ``PersistentState``.
      ``PersistentStateType`` 0 = single closed tab,
      ``PersistentStateType`` 1 = closed window containing ``TabStates``.

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

    # Flatten entries: unwrap ClosedTabOrWindowPersistentStates wrappers.
    flat_tabs = _flatten_closed_tab_entries(tab_entries)

    for tab_dict in flat_tabs:
        tab_url = str(tab_dict.get("TabURL") or "").strip()
        if not _is_non_blank_url(tab_url):
            continue

        tab_title = str(tab_dict.get("TabTitle") or "").strip()
        date_closed = _coerce_datetime(tab_dict.get("DateClosed"))

        closed_tabs.append(
            SafariClosedTab(
                tab_url=tab_url,
                tab_title=tab_title,
                date_closed=date_closed,
            )
        )

    return closed_tabs


def _flatten_closed_tab_entries(entries: List[Any]) -> List[dict]:
    """Flatten ``ClosedTabOrWindowPersistentStates`` wrappers into tab dicts.

    Each wrapper entry may contain ``PersistentState`` with:
    - ``PersistentStateType`` 1: closed *window* — tabs live inside
      ``PersistentState.TabStates``, with ``DateClosed`` inherited from
      the window if individual tabs lack it.
    - ``PersistentStateType`` 0 (or absent): single closed tab — tab fields
      live directly inside ``PersistentState``.

    Entries that already carry ``TabURL`` at the top level (legacy flat
    format) pass through unchanged.
    """
    flat: List[dict] = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue

        # Legacy flat format: TabURL directly on entry.
        if entry.get("TabURL"):
            flat.append(entry)
            continue

        # ClosedTabOrWindowPersistentStates wrapper.
        persistent_state = entry.get("PersistentState")
        if not isinstance(persistent_state, dict):
            continue

        state_type = _coerce_int(entry.get("PersistentStateType"), default=-1)

        if state_type == 1:
            # Closed window — iterate TabStates.
            window_date = persistent_state.get("DateClosed")
            tab_states = persistent_state.get("TabStates", [])
            if not isinstance(tab_states, list):
                continue
            for tab_data in tab_states:
                if not isinstance(tab_data, dict):
                    continue
                # Inherit window DateClosed if the tab doesn't have its own.
                if "DateClosed" not in tab_data and window_date is not None:
                    tab_data = {**tab_data, "DateClosed": window_date}
                flat.append(tab_data)
        else:
            # Single closed tab (type 0 or unknown) — tab data is inside
            # PersistentState itself, or directly on the entry.
            if persistent_state.get("TabURL"):
                flat.append(persistent_state)
            elif entry.get("TabURL"):
                flat.append(entry)

    return flat


def _coerce_datetime(value: Any) -> Optional[datetime]:
    """Coerce a value to a UTC datetime.

    Handles:
    - ``datetime`` (returned by plistlib for ``<date>`` tags)
    - ``float``/``int`` (Cocoa timestamp — seconds since 2001-01-01)
    - ``None`` and unsupported types → ``None``
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    return cocoa_to_datetime(_coerce_float(value))


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

    # Normalize nav_index while preserving order.
    normalized: List[Dict[str, Any]] = []
    for item in collected:
        url = str(item.get("url") or "").strip()
        if not _is_non_blank_url(url):
            continue

        title = str(item.get("title") or "").strip()
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


# =============================================================================
# Extension Parsing
# =============================================================================

@dataclass
class SafariExtension:
    """Safari extension metadata — unified across all 3 eras."""
    bundle_identifier: str       # → extension_id
    name: str                    # display name
    version: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None
    website: Optional[str] = None
    enabled: Optional[bool] = None         # Legacy only
    added_date: Optional[datetime] = None  # Legacy only (Cocoa NSDate)
    added_date_utc: Optional[str] = None
    apple_signed: Optional[bool] = None    # Legacy only
    developer_identifier: Optional[str] = None
    archive_file_name: Optional[str] = None  # Legacy .safariextz filename
    bundle_directory_name: Optional[str] = None
    permissions: Optional[str] = None       # JSON: domains/permissions
    host_permissions: Optional[str] = None  # Web Extension host_permissions
    manifest_version: Optional[int] = None  # Web Extension
    content_scripts: Optional[str] = None   # Web Extension content_scripts JSON
    extension_era: str = "legacy"           # "legacy" | "app_extension" | "web_extension"
    extension_point: Optional[str] = None   # NSExtensionPointIdentifier
    unknown_keys: list = field(default_factory=list)  # keys not in known sets


def parse_extensions_plist(file_path: Path) -> List[SafariExtension]:
    """Parse Safari Extensions.plist for installed extension entries.

    Args:
        file_path: Path to Extensions.plist (binary or XML plist)

    Returns:
        List of SafariExtension objects (may be empty on error/no extensions)
    """
    from .extensions._schemas import KNOWN_EXTENSION_ENTRY_KEYS

    try:
        with open(file_path, "rb") as f:
            data = plistlib.load(f)
    except Exception:
        return []

    if not isinstance(data, dict):
        return []

    extensions_list = data.get("Installed Extensions", [])
    if not isinstance(extensions_list, list):
        return []

    results = []
    for entry in extensions_list:
        if not isinstance(entry, dict):
            continue

        bundle_id = entry.get("Bundle Identifier", "")
        if not bundle_id:
            continue

        # Collect unknown keys
        unknown = [k for k in entry if k not in KNOWN_EXTENSION_ENTRY_KEYS]

        # Parse Added Date (Cocoa NSDate)
        added_date_raw = entry.get("Added Date")
        added_dt = None
        added_utc = None
        if isinstance(added_date_raw, datetime):
            added_dt = added_date_raw.replace(tzinfo=timezone.utc) if added_date_raw.tzinfo is None else added_date_raw
            added_utc = added_dt.isoformat()
        elif isinstance(added_date_raw, (int, float)):
            added_dt = cocoa_to_datetime(added_date_raw)
            added_utc = cocoa_to_iso(added_date_raw)

        name = entry.get("Archive File Name", "")
        if name and name.endswith(".safariextz"):
            name = name[:-len(".safariextz")]
        if not name:
            name = entry.get("Bundle Directory Name", bundle_id)

        results.append(SafariExtension(
            bundle_identifier=bundle_id,
            name=name,
            version=entry.get("Bundle Version"),
            enabled=entry.get("Enabled"),
            added_date=added_dt,
            added_date_utc=added_utc,
            apple_signed=entry.get("Apple-signed"),
            developer_identifier=entry.get("Developer Identifier"),
            archive_file_name=entry.get("Archive File Name"),
            bundle_directory_name=entry.get("Bundle Directory Name"),
            extension_era="legacy",
            unknown_keys=unknown,
        ))

    return results


def parse_safariextz_info(info_plist_path: Path) -> Optional[SafariExtension]:
    """Parse Info.plist from inside a .safariextz bundle for metadata enrichment.

    Args:
        info_plist_path: Path to the extracted Info.plist from .safariextz

    Returns:
        SafariExtension with metadata or None if unreadable
    """
    from .extensions._schemas import KNOWN_SAFARIEXTZ_INFO_KEYS

    try:
        with open(info_plist_path, "rb") as f:
            data = plistlib.load(f)
    except Exception:
        return None

    if not isinstance(data, dict):
        return None

    bundle_id = data.get("CFBundleIdentifier", "")
    if not bundle_id:
        return None

    unknown = [k for k in data if k not in KNOWN_SAFARIEXTZ_INFO_KEYS]

    # Extract permissions
    permissions_data = data.get("Permissions")
    permissions_json = None
    if permissions_data:
        import json as _json
        try:
            permissions_json = _json.dumps(permissions_data)
        except (TypeError, ValueError):
            permissions_json = str(permissions_data)

    return SafariExtension(
        bundle_identifier=bundle_id,
        name=data.get("CFBundleDisplayName", data.get("CFBundleName", bundle_id)),
        version=data.get("CFBundleShortVersionString", data.get("CFBundleVersion")),
        description=data.get("Description"),
        author=data.get("Author"),
        website=data.get("Website"),
        permissions=permissions_json,
        extension_era="legacy",
        unknown_keys=unknown,
    )


def parse_appex_info_plist(info_plist_path: Path) -> Optional[SafariExtension]:
    """Parse .appex/Contents/Info.plist for App Extension metadata.

    Returns None if this is not a Safari extension (checks NSExtensionPointIdentifier).

    Args:
        info_plist_path: Path to Info.plist inside .appex/Contents/

    Returns:
        SafariExtension or None if not Safari-related
    """
    from .extensions._schemas import (
        KNOWN_NSEXTENSION_KEYS, KNOWN_APPEX_INFO_KEYS,
        SAFARI_EXTENSION_POINT_IDENTIFIERS,
    )

    try:
        with open(info_plist_path, "rb") as f:
            data = plistlib.load(f)
    except Exception:
        return None

    if not isinstance(data, dict):
        return None

    ns_ext = data.get("NSExtension", {})
    if not isinstance(ns_ext, dict):
        return None

    ext_point = ns_ext.get("NSExtensionPointIdentifier", "")
    if ext_point not in SAFARI_EXTENSION_POINT_IDENTIFIERS:
        return None

    bundle_id = data.get("CFBundleIdentifier", "")
    if not bundle_id:
        return None

    # Collect unknown keys
    unknown_top = [k for k in data if k not in KNOWN_APPEX_INFO_KEYS]
    unknown_ns = [k for k in ns_ext if k not in KNOWN_NSEXTENSION_KEYS]
    unknown = unknown_top + [f"NSExtension.{k}" for k in unknown_ns]

    # Extract SFSafariWebsiteAccess permissions
    website_access = ns_ext.get("SFSafariWebsiteAccess", {})
    permissions_json = None
    if website_access:
        import json as _json
        try:
            permissions_json = _json.dumps(website_access)
        except (TypeError, ValueError):
            pass

    return SafariExtension(
        bundle_identifier=bundle_id,
        name=data.get("CFBundleDisplayName", data.get("CFBundleName", bundle_id)),
        version=data.get("CFBundleShortVersionString", data.get("CFBundleVersion")),
        description=data.get("NSHumanReadableCopyright"),
        extension_era="app_extension",
        extension_point=ext_point,
        permissions=permissions_json,
        unknown_keys=unknown,
    )


def parse_webextension_manifest(
    manifest_path: Path,
    bundle_info: Optional[SafariExtension] = None,
) -> Optional[SafariExtension]:
    """Parse manifest.json from a Safari Web Extension inside .appex.

    If bundle_info is provided (from parent .appex Info.plist), merges
    bundle metadata with manifest data.

    Args:
        manifest_path: Path to manifest.json
        bundle_info: Optional SafariExtension from parent .appex Info.plist

    Returns:
        SafariExtension or None if unreadable
    """
    import json as _json
    from .extensions._schemas import KNOWN_MANIFEST_KEYS

    try:
        with open(manifest_path, "r", encoding="utf-8") as f:
            data = _json.load(f)
    except Exception:
        return None

    if not isinstance(data, dict):
        return None

    unknown = [k for k in data if k not in KNOWN_MANIFEST_KEYS]

    # Build permissions strings
    permissions = data.get("permissions", [])
    host_permissions = data.get("host_permissions", [])
    content_scripts = data.get("content_scripts")

    permissions_json = _json.dumps(permissions) if permissions else None
    host_perms_json = _json.dumps(host_permissions) if host_permissions else None
    content_scripts_json = _json.dumps(content_scripts) if content_scripts else None

    bundle_id = (bundle_info.bundle_identifier if bundle_info else None) or data.get("name", "")
    name = data.get("name", (bundle_info.name if bundle_info else ""))

    return SafariExtension(
        bundle_identifier=bundle_id,
        name=name,
        version=data.get("version", (bundle_info.version if bundle_info else None)),
        description=data.get("description"),
        author=data.get("author"),
        website=data.get("homepage_url"),
        permissions=permissions_json,
        host_permissions=host_perms_json,
        manifest_version=data.get("manifest_version"),
        content_scripts=content_scripts_json,
        extension_era="web_extension",
        extension_point=(bundle_info.extension_point if bundle_info else "com.apple.Safari.web-extension"),
        unknown_keys=unknown,
    )


def get_extension_stats(extensions: List[SafariExtension]) -> Dict[str, Any]:
    """Compute summary statistics for a list of Safari extensions.

    Returns dict with:
        total_count, enabled_count, apple_signed_count,
        by_era (legacy/app_extension/web_extension counts)
    """
    by_era: Dict[str, int] = {"legacy": 0, "app_extension": 0, "web_extension": 0}
    enabled_count = 0
    apple_signed_count = 0

    for ext in extensions:
        by_era[ext.extension_era] = by_era.get(ext.extension_era, 0) + 1
        if ext.enabled is True:
            enabled_count += 1
        if ext.apple_signed is True:
            apple_signed_count += 1

    return {
        "total_count": len(extensions),
        "enabled_count": enabled_count,
        "apple_signed_count": apple_signed_count,
        "by_era": by_era,
    }
