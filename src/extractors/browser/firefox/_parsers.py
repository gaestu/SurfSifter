"""
Firefox database parsing utilities.

Pure functions for parsing Firefox SQLite databases:
- places.sqlite: History (moz_historyvisits + moz_places)
- places.sqlite: Bookmarks (moz_bookmarks + moz_places)
- places.sqlite: Downloads (moz_annos or legacy moz_downloads)
- cookies.sqlite: Cookies (moz_cookies)

Firefox uses PRTime timestamps (microseconds since 1970-01-01).
All parsers return dataclasses with typed fields.

Usage:
    from extractors.browser.firefox._parsers import (
        parse_history_visits,
        parse_cookies,
        parse_bookmarks,
        parse_downloads,
    )
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional, Dict, Any
from urllib.parse import unquote

from extractors._shared.timestamps import prtime_to_iso, unix_milliseconds_to_iso


# =============================================================================
# Constants
# =============================================================================

# Firefox visit_type mapping (from Mozilla source: toolkit/components/places)
# https://searchfox.org/mozilla-central/source/toolkit/components/places/nsINavHistoryService.idl
FIREFOX_VISIT_TYPES: Dict[int, str] = {
    1: "link",                    # TRANSITION_LINK - User clicked a link
    2: "typed",                   # TRANSITION_TYPED - User typed URL in address bar
    3: "bookmark",                # TRANSITION_BOOKMARK - Navigation from bookmark
    4: "embed",                   # TRANSITION_EMBED - Subframe navigation
    5: "redirect_permanent",      # TRANSITION_REDIRECT_PERMANENT - 301 redirect
    6: "redirect_temporary",      # TRANSITION_REDIRECT_TEMPORARY - 302/307 redirect
    7: "download",                # TRANSITION_DOWNLOAD - Download link
    8: "framed_link",             # TRANSITION_FRAMED_LINK - Link in subframe
    9: "reload",                  # TRANSITION_RELOAD - Page reload
}


def get_visit_type_label(visit_type: int) -> str:
    """Convert Firefox visit_type integer to human-readable label."""
    return FIREFOX_VISIT_TYPES.get(visit_type, f"unknown_{visit_type}")


# =============================================================================
# History Dataclasses
# =============================================================================


@dataclass
class FirefoxVisit:
    """Single visit record from Firefox history."""

    url: str
    title: Optional[str]
    visit_time_utc: Optional[str]  # ISO 8601
    visit_count: int
    typed: int  # 1 if URL was typed, 0 otherwise
    last_visit_time_utc: Optional[str]  # ISO 8601 (URL-level aggregate)

    # Visit-level details
    from_visit: Optional[int]  # Referrer visit ID
    visit_type: int  # 1=link, 2=typed, 3=bookmark, etc.

    # Raw timestamp for forensic purposes
    visit_date_raw: int  # PRTime (microseconds since 1970)

    # Fields with defaults (must come after required fields)
    visit_type_label: str = ""  # Human-readable visit type
    frecency: int = 0  # Firefox importance score
    hidden: bool = False  # Internal/redirect URL indicator
    typed_input: Optional[str] = None  # What user typed from moz_inputhistory


@dataclass
class FirefoxHistoryStats:
    """Statistics from a Firefox history file."""

    visit_count: int = 0
    unique_urls: int = 0
    earliest_visit: Optional[str] = None  # ISO 8601
    latest_visit: Optional[str] = None  # ISO 8601


# =============================================================================
# Cookie Dataclasses
# =============================================================================


@dataclass
class FirefoxCookie:
    """Single cookie record from Firefox."""

    name: str
    value: str
    domain: str
    path: str

    # Timestamps (ISO 8601)
    expires_utc: Optional[str]
    creation_utc: Optional[str]
    last_access_utc: Optional[str]

    # Flags
    is_secure: bool
    is_httponly: bool
    samesite: Optional[str]  # "None", "Lax", "Strict"

    # Firefox cookies are NOT encrypted (unlike Chromium)
    encrypted: bool = False

    # Firefox originAttributes (container tabs, private browsing, FPI, state partitioning)
    # Raw string from Firefox (e.g., "^userContextId=1&privateBrowsingId=0")
    origin_attributes: Optional[str] = None
    # Parsed components
    user_context_id: Optional[int] = None  # Container tab ID (0=default, 1+=containers)
    private_browsing_id: Optional[int] = None  # 0=normal, 1=private browsing
    first_party_domain: Optional[str] = None  # FPI partitioning domain
    partition_key: Optional[str] = None  # State partitioning key

    # Raw SameSite value for forensic preservation
    samesite_raw: Optional[int] = None


@dataclass
class FirefoxCookieStats:
    """Statistics from a Firefox cookies file."""

    cookie_count: int = 0
    unique_domains: int = 0
    secure_count: int = 0
    httponly_count: int = 0


# =============================================================================
# Bookmark Dataclasses
# =============================================================================


@dataclass
class FirefoxBookmark:
    """Single bookmark record from Firefox."""

    url: str
    title: Optional[str]
    folder_path: str  # Human-readable path like "Bookmarks Menu/Tech"
    guid: Optional[str]

    # Timestamps (ISO 8601)
    date_added_utc: Optional[str]
    date_modified_utc: Optional[str]

    # Type: url, folder, separator
    bookmark_type: str = "url"


@dataclass
class FirefoxBookmarkStats:
    """Statistics from a Firefox bookmarks extraction."""

    bookmark_count: int = 0
    folder_count: int = 0


# =============================================================================
# Download Dataclasses
# =============================================================================


@dataclass
class FirefoxDownload:
    """Single download record from Firefox."""

    url: str
    target_path: str
    filename: str

    # Timestamps (ISO 8601)
    start_time_utc: Optional[str]
    end_time_utc: Optional[str]

    # Size info
    total_bytes: Optional[int]
    received_bytes: Optional[int]

    # Status
    state: str  # complete, in_progress, cancelled, failed, etc.
    mime_type: Optional[str]
    referrer: Optional[str]

    # Forensic fields
    deleted: bool = False  # File was manually removed (from metaData.deleted)
    danger_type: Optional[str] = None  # reputationCheckVerdict if blocked


@dataclass
class FirefoxDownloadStats:
    """Statistics from a Firefox downloads extraction."""

    download_count: int = 0
    complete_count: int = 0
    failed_count: int = 0
    total_bytes: int = 0


# =============================================================================
# History Parsing
# =============================================================================


def _load_inputhistory(conn: sqlite3.Connection) -> Dict[int, str]:
    """
    Load moz_inputhistory data for typed URL autocomplete context.

    moz_inputhistory tracks what users typed in the address bar that led
    to URL selection. Forensically valuable for showing user intent.

    Args:
        conn: SQLite connection to places.sqlite

    Returns:
        Dict mapping place_id -> most frequent typed input
    """
    inputhistory: Dict[int, str] = {}

    try:
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='moz_inputhistory'"
        )
        if not cursor.fetchone():
            return inputhistory

        # Get most frequently used input for each place_id
        # (same URL may have multiple typed inputs)
        cursor.execute("""
            SELECT place_id, input, use_count
            FROM moz_inputhistory
            ORDER BY place_id, use_count DESC
        """)

        for row in cursor:
            place_id = row[0]
            typed_input = row[1]
            # Keep only the most frequently used input per place_id
            if place_id not in inputhistory:
                inputhistory[place_id] = typed_input

    except sqlite3.Error:
        pass  # Table may not exist in older Firefox versions

    return inputhistory


def parse_history_visits(db_path: Path) -> Iterator[FirefoxVisit]:
    """
    Parse Firefox history visits from places.sqlite.

    Joins moz_historyvisits with moz_places to get per-visit records,
    not just per-URL aggregates. This ensures timeline accuracy.

    Also loads moz_inputhistory for typed URL autocomplete context,
    which shows what the user actually typed in the address bar.

    Args:
        db_path: Path to places.sqlite

    Yields:
        FirefoxVisit records ordered by visit_date DESC
    """
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return

    try:
        cursor = conn.cursor()

        # Check if tables exist
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('moz_places', 'moz_historyvisits')"
        )
        tables = {row[0] for row in cursor.fetchall()}

        if not tables.issuperset({'moz_places', 'moz_historyvisits'}):
            return

        # Load typed input history for URL context
        inputhistory = _load_inputhistory(conn)

        # Join visits with places for per-visit records
        # Include frecency and hidden for forensic context
        cursor.execute("""
            SELECT
                p.id AS place_id,
                p.url,
                p.title,
                p.visit_count,
                p.typed,
                p.last_visit_date,
                p.frecency,
                p.hidden,
                v.visit_date,
                v.from_visit,
                v.visit_type
            FROM moz_historyvisits v
            JOIN moz_places p ON v.place_id = p.id
            WHERE v.visit_date IS NOT NULL
            ORDER BY v.visit_date DESC
        """)

        for row in cursor:
            visit_time = prtime_to_iso(row["visit_date"]) if row["visit_date"] else None
            last_visit = prtime_to_iso(row["last_visit_date"]) if row["last_visit_date"] else None
            visit_type = row["visit_type"] or 1
            place_id = row["place_id"]

            yield FirefoxVisit(
                url=row["url"],
                title=row["title"],
                visit_time_utc=visit_time,
                visit_count=row["visit_count"] or 0,
                typed=row["typed"] or 0,
                last_visit_time_utc=last_visit,
                from_visit=row["from_visit"],
                visit_type=visit_type,
                visit_type_label=get_visit_type_label(visit_type),
                visit_date_raw=row["visit_date"],
                frecency=row["frecency"] or 0,
                hidden=bool(row["hidden"]),
                typed_input=inputhistory.get(place_id),
            )
    finally:
        conn.close()


def get_history_stats(db_path: Path) -> FirefoxHistoryStats:
    """
    Get statistics from Firefox places.sqlite without full parsing.

    Args:
        db_path: Path to places.sqlite

    Returns:
        FirefoxHistoryStats with counts and date range
    """
    stats = FirefoxHistoryStats()

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return stats

    try:
        cursor = conn.cursor()

        # Count visits
        cursor.execute("SELECT COUNT(*) FROM moz_historyvisits")
        stats.visit_count = cursor.fetchone()[0]

        # Count unique URLs
        cursor.execute("SELECT COUNT(DISTINCT place_id) FROM moz_historyvisits")
        stats.unique_urls = cursor.fetchone()[0]

        # Date range
        cursor.execute("SELECT MIN(visit_date), MAX(visit_date) FROM moz_historyvisits")
        row = cursor.fetchone()
        if row[0]:
            stats.earliest_visit = prtime_to_iso(row[0])
        if row[1]:
            stats.latest_visit = prtime_to_iso(row[1])
    except sqlite3.Error:
        pass
    finally:
        conn.close()

    return stats


# =============================================================================
# Cookie Parsing
# =============================================================================


# SameSite mapping (Firefox stores as integer)
# https://searchfox.org/mozilla-central/source/netwerk/cookie/nsICookie.idl
SAMESITE_MAP: Dict[int, str] = {
    0: "None",    # SAMESITE_NONE - Cross-site allowed
    1: "Lax",     # SAMESITE_LAX - Cross-site on navigation
    2: "Strict",  # SAMESITE_STRICT - Same-site only
}


def _parse_origin_attributes(origin_attrs: Optional[str]) -> Dict[str, Any]:
    """
    Parse Firefox originAttributes string into components.

    Firefox stores originAttributes as a caret-prefixed query string:
    - "^userContextId=1&privateBrowsingId=0"
    - "^firstPartyDomain=example.com"
    - "^partitionKey=(https,example.com)"

    Components:
    - userContextId: Container tab ID (0=default, 1+=containers like Personal/Work/Banking)
    - privateBrowsingId: Private browsing indicator (0=normal, 1=private)
    - firstPartyDomain: First-Party Isolation domain (FPI)
    - partitionKey: State Partitioning key for cross-site tracking protection

    Args:
        origin_attrs: Raw originAttributes string from moz_cookies

    Returns:
        Dict with parsed components (user_context_id, private_browsing_id,
        first_party_domain, partition_key)
    """
    result: Dict[str, Any] = {
        "user_context_id": None,
        "private_browsing_id": None,
        "first_party_domain": None,
        "partition_key": None,
    }

    if not origin_attrs:
        return result

    # Strip leading caret if present
    attrs = origin_attrs.lstrip("^")
    if not attrs:
        return result

    # Parse as query string (key=value&key=value)
    for pair in attrs.split("&"):
        if "=" not in pair:
            continue
        key, value = pair.split("=", 1)

        if key == "userContextId":
            try:
                result["user_context_id"] = int(value)
            except ValueError:
                pass
        elif key == "privateBrowsingId":
            try:
                result["private_browsing_id"] = int(value)
            except ValueError:
                pass
        elif key == "firstPartyDomain":
            result["first_party_domain"] = value if value else None
        elif key == "partitionKey":
            result["partition_key"] = value if value else None

    return result


def parse_cookies(db_path: Path) -> Iterator[FirefoxCookie]:
    """
    Parse Firefox cookies from cookies.sqlite.

    Handles both moz_cookies (modern) and cookies (older) table names.
    Extracts originAttributes for container tabs, private browsing, and
    First-Party Isolation context when available.

    Args:
        db_path: Path to cookies.sqlite

    Yields:
        FirefoxCookie records with parsed originAttributes
    """
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return

    try:
        cursor = conn.cursor()

        # Check which table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('moz_cookies', 'cookies')"
        )
        tables = [row[0] for row in cursor.fetchall()]

        if "moz_cookies" in tables:
            table_name = "moz_cookies"
        elif "cookies" in tables:
            table_name = "cookies"
        else:
            return

        # Get columns
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = {row[1] for row in cursor.fetchall()}

        # Build query with available columns
        select_cols = ["name", "value"]

        # Domain column
        domain_col = "host" if "host" in columns else "baseDomain" if "baseDomain" in columns else None
        if domain_col:
            select_cols.append(domain_col)

        # Standard columns
        for col in ["path", "expiry", "isSecure", "isHttpOnly", "sameSite", "creationTime", "lastAccessed"]:
            if col in columns:
                select_cols.append(col)

        # Firefox originAttributes column (containers, private browsing, FPI, state partitioning)
        has_origin_attributes = "originAttributes" in columns
        if has_origin_attributes:
            select_cols.append("originAttributes")

        cursor.execute(f"SELECT {', '.join(select_cols)} FROM {table_name}")

        for row in cursor:
            # Extract values with fallbacks
            name = row["name"]
            value = row["value"]
            domain = row[domain_col] if domain_col else "unknown"
            path = row["path"] if "path" in columns else "/"

            # Timestamps
            expires_utc = None
            if "expiry" in columns and row["expiry"]:
                # expiry is Unix timestamp (seconds)
                from extractors._shared.timestamps import unix_to_iso
                expires_utc = unix_to_iso(row["expiry"])

            creation_utc = None
            if "creationTime" in columns and row["creationTime"]:
                creation_utc = prtime_to_iso(row["creationTime"])

            last_access_utc = None
            if "lastAccessed" in columns and row["lastAccessed"]:
                last_access_utc = prtime_to_iso(row["lastAccessed"])

            # SameSite - preserve raw value and map to string
            samesite_raw = None
            samesite = None
            if "sameSite" in columns and row["sameSite"] is not None:
                samesite_raw = row["sameSite"]
                samesite = SAMESITE_MAP.get(samesite_raw)
                # If unknown value, create descriptive string
                if samesite is None:
                    samesite = f"unknown_{samesite_raw}"

            # Parse originAttributes for container/privacy context
            origin_attrs_raw = None
            origin_attrs_parsed: Dict[str, Any] = {
                "user_context_id": None,
                "private_browsing_id": None,
                "first_party_domain": None,
                "partition_key": None,
            }
            if has_origin_attributes:
                origin_attrs_raw = row["originAttributes"]
                origin_attrs_parsed = _parse_origin_attributes(origin_attrs_raw)

            yield FirefoxCookie(
                name=name,
                value=value,
                domain=domain,
                path=path,
                expires_utc=expires_utc,
                creation_utc=creation_utc,
                last_access_utc=last_access_utc,
                is_secure=bool(row["isSecure"]) if "isSecure" in columns else False,
                is_httponly=bool(row["isHttpOnly"]) if "isHttpOnly" in columns else False,
                samesite=samesite,
                samesite_raw=samesite_raw,
                encrypted=False,  # Firefox never encrypts cookies
                origin_attributes=origin_attrs_raw,
                user_context_id=origin_attrs_parsed["user_context_id"],
                private_browsing_id=origin_attrs_parsed["private_browsing_id"],
                first_party_domain=origin_attrs_parsed["first_party_domain"],
                partition_key=origin_attrs_parsed["partition_key"],
            )
    finally:
        conn.close()


def get_cookie_stats(db_path: Path) -> FirefoxCookieStats:
    """
    Get statistics from Firefox cookies.sqlite without full parsing.

    Args:
        db_path: Path to cookies.sqlite

    Returns:
        FirefoxCookieStats with counts
    """
    stats = FirefoxCookieStats()

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.Error:
        return stats

    try:
        cursor = conn.cursor()

        # Find table name
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('moz_cookies', 'cookies')"
        )
        tables = [row[0] for row in cursor.fetchall()]
        table_name = "moz_cookies" if "moz_cookies" in tables else "cookies" if tables else None

        if not table_name:
            return stats

        # Get columns
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = {row[1] for row in cursor.fetchall()}
        domain_col = "host" if "host" in columns else "baseDomain"

        # Count cookies
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        stats.cookie_count = cursor.fetchone()[0]

        # Count unique domains
        cursor.execute(f"SELECT COUNT(DISTINCT {domain_col}) FROM {table_name}")
        stats.unique_domains = cursor.fetchone()[0]

        # Count secure cookies
        if "isSecure" in columns:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE isSecure = 1")
            stats.secure_count = cursor.fetchone()[0]

        # Count httponly cookies
        if "isHttpOnly" in columns:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE isHttpOnly = 1")
            stats.httponly_count = cursor.fetchone()[0]
    except sqlite3.Error:
        pass
    finally:
        conn.close()

    return stats


# =============================================================================
# Bookmark Parsing
# =============================================================================


def parse_bookmarks(db_path: Path) -> Iterator[FirefoxBookmark]:
    """
    Parse Firefox bookmarks from places.sqlite.

    Joins moz_bookmarks with moz_places to get URLs.
    Builds folder hierarchy for human-readable paths.

    Args:
        db_path: Path to places.sqlite

    Yields:
        FirefoxBookmark records
    """
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return

    try:
        cursor = conn.cursor()

        # Check if tables exist
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('moz_bookmarks', 'moz_places')"
        )
        tables = {row[0] for row in cursor.fetchall()}

        if not tables.issuperset({'moz_bookmarks', 'moz_places'}):
            return

        # Build folder paths first
        folder_paths = _build_folder_paths(conn)

        # Query bookmarks (type=1 is bookmark, type=2 is folder)
        cursor.execute("""
            SELECT
                b.id,
                b.type,
                b.title,
                b.parent,
                b.dateAdded,
                b.lastModified,
                b.guid,
                p.url
            FROM moz_bookmarks b
            LEFT JOIN moz_places p ON b.fk = p.id
            WHERE b.type = 1
            ORDER BY b.parent, b.position
        """)

        for row in cursor:
            url = row["url"]
            if not url:
                continue

            title = row["title"] or ""
            parent_id = row["parent"]
            folder_path = folder_paths.get(parent_id, "")

            # Convert PRTime timestamps
            date_added = prtime_to_iso(row["dateAdded"]) if row["dateAdded"] else None
            date_modified = prtime_to_iso(row["lastModified"]) if row["lastModified"] else None

            yield FirefoxBookmark(
                url=url,
                title=title,
                folder_path=folder_path,
                guid=row["guid"],
                date_added_utc=date_added,
                date_modified_utc=date_modified,
                bookmark_type="url",
            )
    finally:
        conn.close()


def _build_folder_paths(conn: sqlite3.Connection) -> Dict[int, str]:
    """
    Build mapping of folder IDs to human-readable paths.

    Firefox uses parent IDs to create folder hierarchy.
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, title, parent
        FROM moz_bookmarks
        WHERE type = 2
    """)

    folders = {}
    for row in cursor:
        folders[row["id"]] = {
            "title": row["title"] or "",
            "parent": row["parent"],
        }

    # Map internal folder names to display names
    title_map = {
        "": "",
        "menu": "Bookmarks Menu",
        "toolbar": "Bookmarks Toolbar",
        "unfiled": "Other Bookmarks",
        "mobile": "Mobile Bookmarks",
    }

    def get_path(folder_id: int) -> str:
        if folder_id not in folders:
            return ""

        folder = folders[folder_id]
        parent_path = get_path(folder["parent"]) if folder["parent"] else ""
        title = folder["title"]
        display_title = title_map.get(title.lower(), title) if title else ""

        if parent_path and display_title:
            return f"{parent_path}/{display_title}"
        return display_title or parent_path

    folder_paths = {}
    for folder_id in folders:
        folder_paths[folder_id] = get_path(folder_id)

    return folder_paths


def get_bookmark_stats(db_path: Path) -> FirefoxBookmarkStats:
    """
    Get statistics from Firefox places.sqlite bookmarks without full parsing.

    Args:
        db_path: Path to places.sqlite

    Returns:
        FirefoxBookmarkStats with counts
    """
    stats = FirefoxBookmarkStats()

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    except sqlite3.Error:
        return stats

    try:
        cursor = conn.cursor()

        # Count bookmarks (type=1)
        cursor.execute("SELECT COUNT(*) FROM moz_bookmarks WHERE type = 1")
        stats.bookmark_count = cursor.fetchone()[0]

        # Count folders (type=2)
        cursor.execute("SELECT COUNT(*) FROM moz_bookmarks WHERE type = 2")
        stats.folder_count = cursor.fetchone()[0]
    except sqlite3.Error:
        pass
    finally:
        conn.close()

    return stats


# =============================================================================
# Bookmark Backup Parsing (jsonlz4)
# =============================================================================


def decompress_mozlz4(data: bytes) -> bytes:
    """
    Decompress Mozilla LZ4 format data.

    Mozilla uses a custom LZ4 variant with "mozLz40\x00" magic header.
    The actual LZ4 compressed data follows the 8-byte header.

    Args:
        data: Raw bytes from a .jsonlz4 file

    Returns:
        Decompressed bytes

    Raises:
        ValueError: If magic header is invalid
        ImportError: If lz4 module not available
    """
    if len(data) < 8:
        raise ValueError("Data too short for mozLz4 format")

    if data[:8] != b"mozLz40\x00":
        raise ValueError(f"Invalid mozLz4 magic: {data[:8]!r}")

    try:
        import lz4.block
        return lz4.block.decompress(data[8:])
    except ImportError:
        raise ImportError("lz4 module required for jsonlz4 parsing. Install with: pip install lz4")


def parse_bookmark_backup(file_path: Path) -> Iterator[FirefoxBookmark]:
    """
    Parse Firefox bookmark backup file (jsonlz4 format).

    Firefox automatically creates bookmark backups in bookmarkbackups/ folder.
    Filename format: bookmarks-YYYY-MM-DD_####_<hash>.jsonlz4

    The JSON structure contains:
    - root: Object with "children" array containing bookmark tree
    - Each bookmark has: type, uri, title, dateAdded, lastModified, guid
    - type: "text/x-moz-place" (bookmark), "text/x-moz-place-container" (folder),
            "text/x-moz-place-separator" (separator)

    Args:
        file_path: Path to .jsonlz4 bookmark backup file

    Yields:
        FirefoxBookmark records (only URL bookmarks, not folders/separators)
    """
    try:
        data = file_path.read_bytes()
    except Exception as e:
        raise ValueError(f"Failed to read bookmark backup: {e}")

    # Decompress
    try:
        json_data = decompress_mozlz4(data)
    except (ImportError, ValueError) as e:
        raise ValueError(f"Failed to decompress bookmark backup: {e}")

    # Parse JSON
    try:
        backup = json.loads(json_data)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse bookmark backup JSON: {e}")

    # Traverse bookmark tree
    yield from _traverse_bookmark_tree(backup, "")


def _traverse_bookmark_tree(
    node: Dict[str, Any],
    parent_path: str
) -> Iterator[FirefoxBookmark]:
    """
    Recursively traverse Firefox bookmark JSON tree.

    Args:
        node: Current node in bookmark tree
        parent_path: Human-readable folder path to this node

    Yields:
        FirefoxBookmark records
    """
    node_type = node.get("type", "")
    title = node.get("title", "")

    # Map internal folder names to display names
    title_map = {
        "": "",
        "menu": "Bookmarks Menu",
        "toolbar": "Bookmarks Toolbar",
        "unfiled": "Other Bookmarks",
        "mobile": "Mobile Bookmarks",
    }

    # Build current path
    if node_type == "text/x-moz-place-container":
        # It's a folder
        display_title = title_map.get(title.lower(), title) if title else ""
        if parent_path and display_title:
            current_path = f"{parent_path}/{display_title}"
        else:
            current_path = display_title or parent_path
    else:
        current_path = parent_path

    # Process URL bookmarks
    if node_type == "text/x-moz-place":
        uri = node.get("uri", "")
        if uri:
            # Convert timestamps (PRTime in JSON = microseconds since epoch)
            date_added = prtime_to_iso(node.get("dateAdded")) if node.get("dateAdded") else None
            date_modified = prtime_to_iso(node.get("lastModified")) if node.get("lastModified") else None

            yield FirefoxBookmark(
                url=uri,
                title=title,
                folder_path=parent_path,
                guid=node.get("guid"),
                date_added_utc=date_added,
                date_modified_utc=date_modified,
                bookmark_type="url",
            )

    # Recurse into children
    children = node.get("children", [])
    for child in children:
        yield from _traverse_bookmark_tree(child, current_path)


def get_bookmark_backup_stats(file_path: Path) -> FirefoxBookmarkStats:
    """
    Get statistics from Firefox bookmark backup without full parsing.

    Args:
        file_path: Path to .jsonlz4 bookmark backup file

    Returns:
        FirefoxBookmarkStats with counts
    """
    stats = FirefoxBookmarkStats()

    try:
        data = file_path.read_bytes()
        json_data = decompress_mozlz4(data)
        backup = json.loads(json_data)

        def count_nodes(node):
            node_type = node.get("type", "")
            if node_type == "text/x-moz-place":
                stats.bookmark_count += 1
            elif node_type == "text/x-moz-place-container":
                stats.folder_count += 1

            for child in node.get("children", []):
                count_nodes(child)

        count_nodes(backup)
    except Exception:
        pass

    return stats


def extract_backup_timestamp(filename: str) -> Optional[str]:
    """
    Extract timestamp from bookmark backup filename.

    Firefox backup filenames follow the pattern:
    bookmarks-YYYY-MM-DD_####_<hash>.jsonlz4

    Args:
        filename: Backup filename (e.g., "bookmarks-2024-01-15_1234_abcd1234.jsonlz4")

    Returns:
        ISO 8601 date string (e.g., "2024-01-15") or None if not parseable
    """
    import re
    match = re.match(r"bookmarks-(\d{4}-\d{2}-\d{2})_", filename)
    if match:
        return match.group(1)
    return None


# =============================================================================
# Download Parsing
# =============================================================================


# Firefox download state codes
# From: https://searchfox.org/mozilla-central/source/toolkit/components/downloads/DownloadHistory.sys.mjs
FIREFOX_STATE_MAP = {
    0: "in_progress",
    1: "complete",
    2: "failed",
    3: "cancelled",
    4: "paused",
    5: "blocked_parental",      # METADATA_STATE_BLOCKED_PARENTAL
    6: "dirty",                 # Legacy (see state 8)
    7: "blocked_policy",        # METADATA_STATE_BLOCKED_POLICY (never seen in history)
    8: "dirty",                 # METADATA_STATE_DIRTY - blocked by reputation check
    9: "blocked_content_analysis",  # METADATA_STATE_BLOCKED_CONTENT_ANALYSIS (v115+)
}


def parse_downloads(db_path: Path) -> Iterator[FirefoxDownload]:
    """
    Parse Firefox downloads from places.sqlite.

    Modern Firefox (v26+) uses moz_annos annotations.
    Legacy Firefox (< v26) uses moz_downloads table.

    Args:
        db_path: Path to places.sqlite

    Yields:
        FirefoxDownload records
    """
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return

    try:
        cursor = conn.cursor()

        # Check which download storage is used
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('moz_downloads', 'moz_annos', 'moz_places')"
        )
        tables = {row[0] for row in cursor.fetchall()}

        if "moz_downloads" in tables:
            # Legacy Firefox
            yield from _parse_legacy_downloads(conn)
        elif tables.issuperset({'moz_annos', 'moz_places'}):
            # Modern Firefox with annotations
            yield from _parse_annotation_downloads(conn)
    finally:
        conn.close()


def _parse_annotation_downloads(conn: sqlite3.Connection) -> Iterator[FirefoxDownload]:
    """
    Parse downloads from modern Firefox moz_annos annotations (v26+).

    Firefox stores download metadata in two annotations:
    - downloads/destinationFileURI: file:/// path to downloaded file
    - downloads/metaData: JSON with state, endTime, fileSize, deleted, reputationCheckVerdict

    The startTime in metaData (when available) is more accurate than last_visit_date.
    Referrer can be extracted from moz_historyvisits via from_visit chain.
    """
    cursor = conn.cursor()

    # Check for moz_anno_attributes
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='moz_anno_attributes'"
    )
    if not cursor.fetchone():
        return

    # Get annotation attribute IDs
    cursor.execute(
        "SELECT id, name FROM moz_anno_attributes "
        "WHERE name IN ('downloads/destinationFileURI', 'downloads/metaData')"
    )
    attr_map = {row["name"]: row["id"] for row in cursor}

    dest_attr_id = attr_map.get('downloads/destinationFileURI')
    meta_attr_id = attr_map.get('downloads/metaData')

    if not dest_attr_id:
        return

    # Build referrer lookup from moz_historyvisits if available
    # Downloads create a visit with transition=7 (TRANSITION_DOWNLOAD)
    # The from_visit points to the page that initiated the download
    referrer_map: Dict[str, str] = {}
    try:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='moz_historyvisits'"
        )
        if cursor.fetchone():
            cursor.execute("""
                SELECT
                    p.url AS download_url,
                    p2.url AS referrer_url
                FROM moz_historyvisits v
                JOIN moz_places p ON v.place_id = p.id
                JOIN moz_historyvisits v2 ON v.from_visit = v2.id
                JOIN moz_places p2 ON v2.place_id = p2.id
                WHERE v.visit_type = 7
            """)
            for row in cursor:
                referrer_map[row["download_url"]] = row["referrer_url"]
    except sqlite3.Error:
        pass  # moz_historyvisits may not exist or have different schema

    # Query downloads
    cursor.execute("""
        SELECT
            p.url,
            p.last_visit_date,
            dest.content as dest_uri,
            meta.content as metadata
        FROM moz_places p
        JOIN moz_annos dest ON p.id = dest.place_id AND dest.anno_attribute_id = ?
        LEFT JOIN moz_annos meta ON p.id = meta.place_id AND meta.anno_attribute_id = ?
    """, (dest_attr_id, meta_attr_id))

    for row in cursor:
        url = row["url"]
        dest_uri = row["dest_uri"]

        # Parse file path from file:/// URI
        target_path = ""
        if dest_uri and dest_uri.startswith("file:///"):
            target_path = unquote(dest_uri[7:])

        filename = _extract_filename(target_path)

        # Parse metadata JSON
        # See: https://searchfox.org/mozilla-central/source/toolkit/components/downloads/DownloadHistory.sys.mjs
        state = "unknown"
        end_time_utc = None
        start_time_utc = None
        total_bytes = None
        deleted = False
        danger_type = None

        if row["metadata"]:
            try:
                meta = json.loads(row["metadata"])
                state = FIREFOX_STATE_MAP.get(meta.get("state", -1), "unknown")

                # endTime: when download finished/stopped (JavaScript milliseconds, not PRTime microseconds)
                # See: https://searchfox.org/mozilla-central/source/toolkit/components/downloads/DownloadHistory.sys.mjs#144
                if "endTime" in meta:
                    end_time_utc = unix_milliseconds_to_iso(meta["endTime"])

                # fileSize: final size on disk (only for completed downloads)
                if "fileSize" in meta:
                    total_bytes = meta["fileSize"]

                # deleted: file was manually removed via browser UI
                if meta.get("deleted"):
                    deleted = True

                # reputationCheckVerdict: block reason if download was flagged
                # Values: MALWARE, POTENTIALLY_UNWANTED, UNCOMMON, INSECURE, DOWNLOAD_SPAM
                if "reputationCheckVerdict" in meta:
                    danger_type = meta["reputationCheckVerdict"]

            except json.JSONDecodeError:
                pass

        # Start time: prefer last_visit_date (when download was initiated)
        # Note: Firefox doesn't store startTime in metaData annotations,
        # but Places records the visit time when the download started
        if row["last_visit_date"]:
            start_time_utc = prtime_to_iso(row["last_visit_date"])

        # Referrer from moz_historyvisits from_visit chain
        referrer = referrer_map.get(url)

        yield FirefoxDownload(
            url=url,
            target_path=target_path,
            filename=filename,
            start_time_utc=start_time_utc,
            end_time_utc=end_time_utc,
            total_bytes=total_bytes,
            received_bytes=total_bytes,  # Annotations don't track partial downloads
            state=state,
            mime_type=None,  # Not stored in annotations
            referrer=referrer,
            deleted=deleted,
            danger_type=danger_type,
        )


def _parse_legacy_downloads(conn: sqlite3.Connection) -> Iterator[FirefoxDownload]:
    """Parse downloads from legacy Firefox moz_downloads table."""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            name,
            source,
            target,
            startTime,
            endTime,
            state,
            referrer,
            currBytes,
            maxBytes,
            mimeType
        FROM moz_downloads
    """)

    for row in cursor:
        url = row["source"]
        target_uri = row["target"] or ""

        # Parse file path from file:/// URI
        target_path = ""
        if target_uri.startswith("file:///"):
            target_path = unquote(target_uri[7:])

        filename = row["name"] or _extract_filename(target_path)

        # Convert timestamps (PRTime)
        start_time = prtime_to_iso(row["startTime"]) if row["startTime"] else None
        end_time = prtime_to_iso(row["endTime"]) if row["endTime"] else None

        state = FIREFOX_STATE_MAP.get(row["state"], f"unknown_{row['state']}")

        yield FirefoxDownload(
            url=url,
            target_path=target_path,
            filename=filename,
            start_time_utc=start_time,
            end_time_utc=end_time,
            total_bytes=row["maxBytes"],
            received_bytes=row["currBytes"],
            state=state,
            mime_type=row["mimeType"],
            referrer=row["referrer"],
        )


def _extract_filename(path: str) -> str:
    """Extract filename from a path (Windows or Unix)."""
    if not path:
        return ""

    # Handle both Windows backslashes and Unix forward slashes
    if '\\' in path:
        parts = path.split('\\')
    else:
        parts = path.split('/')

    return parts[-1] if parts else ""


def get_download_stats(db_path: Path) -> FirefoxDownloadStats:
    """
    Get statistics from Firefox places.sqlite downloads without full parsing.

    Args:
        db_path: Path to places.sqlite

    Returns:
        FirefoxDownloadStats with counts
    """
    stats = FirefoxDownloadStats()

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return stats

    try:
        cursor = conn.cursor()

        # Check for moz_downloads (legacy)
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name='moz_downloads'"
        )
        if cursor.fetchone():
            cursor.execute("SELECT COUNT(*) FROM moz_downloads")
            stats.download_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM moz_downloads WHERE state = 1")
            stats.complete_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM moz_downloads WHERE state = 2")
            stats.failed_count = cursor.fetchone()[0]

            cursor.execute("SELECT SUM(maxBytes) FROM moz_downloads WHERE state = 1")
            result = cursor.fetchone()[0]
            stats.total_bytes = result if result else 0
        else:
            # Modern annotations
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='moz_anno_attributes'"
            )
            if not cursor.fetchone():
                return stats

            cursor.execute(
                "SELECT id FROM moz_anno_attributes "
                "WHERE name = 'downloads/destinationFileURI'"
            )
            row = cursor.fetchone()
            if row:
                dest_id = row[0]
                cursor.execute(
                    "SELECT COUNT(*) FROM moz_annos WHERE anno_attribute_id = ?",
                    (dest_id,)
                )
                stats.download_count = cursor.fetchone()[0]
    except sqlite3.Error:
        pass
    finally:
        conn.close()

    return stats
