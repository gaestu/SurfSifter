"""
Chromium SNSS Session File Parser

Parses the binary SNSS format used by Chromium browsers for session restore.

File Format (based on Chromium source):
- FileHeader: 8 bytes (signature: 0x53534E53 "SSNS", version: int32)
- Commands: sequence of (size: uint16, command_id: uint8, payload: bytes)

Command Types (from session_service_commands.cc):
- 0: kCommandSetTabWindow
- 2: kCommandSetTabIndexInWindow
- 6: kCommandUpdateTabNavigation (contains URL, title, timestamp)
- 7: kCommandSetSelectedNavigationIndex
- 12: kCommandSetPinnedState
- 16: kCommandTabClosed
- 17: kCommandWindowClosed
- 21: kCommandLastActiveTime
- 255: kInitialStateMarkerCommandId

Navigation Entry Pickle Format (from serialized_navigation_entry.cc):
1. index (int32)
2. virtual_url (string - length-prefixed)
3. title (string16 - length-prefixed UTF-16)
4. encoded_page_state (string)
5. transition_type (int32)
6. type_mask (int32) - optional
7. referrer_url (string) - optional
8. referrer_policy (int32) - optional/deprecated
9. original_request_url (string) - optional
10. is_overriding_user_agent (bool) - optional
11. timestamp (int64) - microseconds since Windows epoch - optional
... more optional fields

References:
- https://chromium.googlesource.com/chromium/src/+/refs/heads/main/components/sessions/core/command_storage_backend.cc
- https://chromium.googlesource.com/chromium/src/+/refs/heads/main/components/sessions/core/session_service_commands.cc
- https://chromium.googlesource.com/chromium/src/+/refs/heads/main/components/sessions/core/serialized_navigation_entry.cc
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Iterator
from pathlib import Path

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.chromium.sessions.snss_parser")

# File signature (SSNS = Sessions)
SNSS_SIGNATURE = 0x53534E53

# File versions
FILE_VERSION_1 = 1
ENCRYPTED_FILE_VERSION = 2
FILE_VERSION_WITH_MARKER = 3
ENCRYPTED_FILE_VERSION_WITH_MARKER = 4

# Command IDs (from session_service_commands.cc)
# Note: Session files and Tab files use different command sets:
# - Session files (Session_*): Use CMD_UPDATE_TAB_NAVIGATION (6) for nav entries
# - Tab files (Tabs_*): Use CMD_TAB_UPDATE_TAB_NAVIGATION (1) for nav entries
CMD_SET_TAB_WINDOW = 0
CMD_TAB_UPDATE_TAB_NAVIGATION = 1  # Used in Tabs_* files (tab_restore_service_commands.cc)
CMD_SET_TAB_INDEX_IN_WINDOW = 2
CMD_TAB_SET_TAB_WINDOW = 4  # Used in Tabs_* files for tab-window association
CMD_TAB_NAVIGATION_PATH_PRUNED_FROM_BACK = 5
CMD_UPDATE_TAB_NAVIGATION = 6  # Used in Session_* files
CMD_SET_SELECTED_NAVIGATION_INDEX = 7
CMD_SET_SELECTED_TAB_IN_INDEX = 8
CMD_SET_WINDOW_TYPE = 9
CMD_TAB_NAVIGATION_PATH_PRUNED_FROM_FRONT = 11
CMD_SET_PINNED_STATE = 12
CMD_SET_EXTENSION_APP_ID = 13
CMD_SET_WINDOW_BOUNDS3 = 14
CMD_SET_WINDOW_APP_NAME = 15
CMD_TAB_CLOSED = 16
CMD_WINDOW_CLOSED = 17
CMD_SET_TAB_USER_AGENT_OVERRIDE = 18
CMD_SESSION_STORAGE_ASSOCIATED = 19
CMD_SET_ACTIVE_WINDOW = 20
CMD_LAST_ACTIVE_TIME = 21
CMD_SET_WINDOW_WORKSPACE2 = 23
CMD_TAB_NAVIGATION_PATH_PRUNED = 24
CMD_SET_TAB_GROUP = 25
CMD_SET_TAB_GROUP_METADATA2 = 27
CMD_SET_TAB_GUID = 28
CMD_SET_TAB_USER_AGENT_OVERRIDE2 = 29
CMD_SET_TAB_DATA = 30
CMD_SET_WINDOW_USER_TITLE = 31
CMD_SET_WINDOW_VISIBLE_ON_ALL_WORKSPACES = 32
CMD_ADD_TAB_EXTRA_DATA = 33
CMD_ADD_WINDOW_EXTRA_DATA = 34
CMD_SET_PLATFORM_SESSION_ID = 35
CMD_SET_SPLIT_TAB = 36
CMD_SET_SPLIT_TAB_DATA = 37
CMD_INITIAL_STATE_MARKER = 255

# Set of all known command IDs (for schema warning detection)
KNOWN_COMMAND_IDS = {
    CMD_SET_TAB_WINDOW,
    CMD_TAB_UPDATE_TAB_NAVIGATION,
    CMD_SET_TAB_INDEX_IN_WINDOW,
    CMD_TAB_SET_TAB_WINDOW,
    CMD_TAB_NAVIGATION_PATH_PRUNED_FROM_BACK,
    CMD_UPDATE_TAB_NAVIGATION,
    CMD_SET_SELECTED_NAVIGATION_INDEX,
    CMD_SET_SELECTED_TAB_IN_INDEX,
    CMD_SET_WINDOW_TYPE,
    CMD_TAB_NAVIGATION_PATH_PRUNED_FROM_FRONT,
    CMD_SET_PINNED_STATE,
    CMD_SET_EXTENSION_APP_ID,
    CMD_SET_WINDOW_BOUNDS3,
    CMD_SET_WINDOW_APP_NAME,
    CMD_TAB_CLOSED,
    CMD_WINDOW_CLOSED,
    CMD_SET_TAB_USER_AGENT_OVERRIDE,
    CMD_SESSION_STORAGE_ASSOCIATED,
    CMD_SET_ACTIVE_WINDOW,
    CMD_LAST_ACTIVE_TIME,
    CMD_SET_WINDOW_WORKSPACE2,
    CMD_TAB_NAVIGATION_PATH_PRUNED,
    CMD_SET_TAB_GROUP,
    CMD_SET_TAB_GROUP_METADATA2,
    CMD_SET_TAB_GUID,
    CMD_SET_TAB_USER_AGENT_OVERRIDE2,
    CMD_SET_TAB_DATA,
    CMD_SET_WINDOW_USER_TITLE,
    CMD_SET_WINDOW_VISIBLE_ON_ALL_WORKSPACES,
    CMD_ADD_TAB_EXTRA_DATA,
    CMD_ADD_WINDOW_EXTRA_DATA,
    CMD_SET_PLATFORM_SESSION_ID,
    CMD_SET_SPLIT_TAB,
    CMD_SET_SPLIT_TAB_DATA,
    CMD_INITIAL_STATE_MARKER,
}

# Windows epoch offset (microseconds from 1601-01-01 to 1970-01-01)
WINDOWS_EPOCH_OFFSET_MICROS = 11644473600000000


@dataclass
class NavigationEntry:
    """Parsed navigation entry from SNSS UpdateTabNavigation command."""
    index: int = 0
    url: str = ""
    title: str = ""
    referrer_url: str = ""
    original_request_url: str = ""
    timestamp: Optional[datetime] = None
    transition_type: int = 0
    http_status_code: int = 0
    has_post_data: bool = False
    is_overriding_user_agent: bool = False


@dataclass
class TabInfo:
    """Aggregated tab information from multiple SNSS commands."""
    tab_id: int = 0
    window_id: int = 0
    index_in_window: int = 0
    pinned: bool = False
    last_active_time: Optional[datetime] = None
    navigations: List[NavigationEntry] = field(default_factory=list)
    current_navigation_index: int = 0
    group_id: Optional[str] = None
    guid: Optional[str] = None


@dataclass
class WindowInfo:
    """Aggregated window information from SNSS commands."""
    window_id: int = 0
    selected_tab_index: int = 0
    window_type: int = 0
    bounds: tuple = (0, 0, 0, 0)  # x, y, w, h
    workspace: str = ""
    app_name: str = ""
    user_title: str = ""


@dataclass
class SNSSParseResult:
    """Complete parsed result from SNSS file."""
    is_valid: bool = False
    version: int = 0
    is_encrypted: bool = False
    tabs: List[TabInfo] = field(default_factory=list)
    windows: List[WindowInfo] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    total_commands: int = 0
    navigation_entries: List[NavigationEntry] = field(default_factory=list)
    # Schema warning support: track unknown command IDs for reporting
    unknown_commands: set = field(default_factory=set)


class PickleReader:
    """
    Reader for Chromium's base::Pickle serialization format.

    Pickle format:
    - Strings: 4-byte length, followed by data (padded to 4 bytes)
    - Integers: 4 bytes (int32)
    - Int64: 8 bytes
    - Bool: 1 byte (but aligned)
    """

    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.pos

    def read_int32(self) -> Optional[int]:
        """Read a 4-byte signed integer."""
        if self.remaining < 4:
            return None
        value = struct.unpack_from('<i', self.data, self.pos)[0]
        self.pos += 4
        return value

    def read_uint32(self) -> Optional[int]:
        """Read a 4-byte unsigned integer."""
        if self.remaining < 4:
            return None
        value = struct.unpack_from('<I', self.data, self.pos)[0]
        self.pos += 4
        return value

    def read_int64(self) -> Optional[int]:
        """Read an 8-byte signed integer."""
        if self.remaining < 8:
            return None
        value = struct.unpack_from('<q', self.data, self.pos)[0]
        self.pos += 8
        return value

    def read_bool(self) -> Optional[bool]:
        """Read a boolean (aligned to 4 bytes in pickle)."""
        # In Chromium pickle, bool is stored as 4-byte int
        value = self.read_int32()
        if value is None:
            return None
        return value != 0

    def read_string(self) -> Optional[str]:
        """Read a length-prefixed UTF-8 string."""
        length = self.read_int32()
        if length is None or length < 0:
            return None
        if length == 0:
            return ""
        if self.remaining < length:
            return None
        try:
            value = self.data[self.pos:self.pos + length].decode('utf-8', errors='replace')
        except Exception:
            value = ""
        self.pos += length
        # Align to 4 bytes
        padding = (4 - (length % 4)) % 4
        self.pos += padding
        return value

    def read_string16(self) -> Optional[str]:
        """Read a length-prefixed UTF-16LE string."""
        # Length is number of char16_t, so byte length is 2x
        char_count = self.read_int32()
        if char_count is None or char_count < 0:
            return None
        if char_count == 0:
            return ""
        byte_length = char_count * 2
        if self.remaining < byte_length:
            return None
        try:
            value = self.data[self.pos:self.pos + byte_length].decode('utf-16-le', errors='replace')
        except Exception:
            value = ""
        self.pos += byte_length
        # Align to 4 bytes
        padding = (4 - (byte_length % 4)) % 4
        self.pos += padding
        return value


def _windows_time_to_datetime(microseconds: int) -> Optional[datetime]:
    """Convert Windows epoch microseconds to datetime."""
    if microseconds <= 0:
        return None
    try:
        # Convert from Windows epoch (1601) to Unix epoch (1970)
        unix_micros = microseconds - WINDOWS_EPOCH_OFFSET_MICROS
        if unix_micros < 0:
            return None
        return datetime.fromtimestamp(unix_micros / 1_000_000, tz=timezone.utc)
    except (OSError, OverflowError, ValueError):
        return None


def parse_navigation_entry(payload: bytes, tab_id: int) -> Optional[tuple[int, NavigationEntry]]:
    """
    Parse a navigation entry from UpdateTabNavigation command payload.

    The payload format is:
    - 4 bytes: pickle payload size (header)
    - 4 bytes: tab_id
    - SerializedNavigationEntry pickle data:
      - 4 bytes: nav_index
      - 4 bytes: url_length, then url bytes (padded to 4 bytes)
      - 4 bytes: title_length (char count), then UTF-16LE bytes (padded)
      - 4 bytes: page_state_length, then bytes (padded)
      - 4 bytes: transition_type
      - ... optional fields

    Returns (tab_id, NavigationEntry) or None on error.
    """
    if len(payload) < 16:  # Minimum: header + tab_id + index + url_len
        return None

    reader = PickleReader(payload)

    # Skip pickle header (payload size) - this is NOT tab_id
    pickle_size = reader.read_int32()
    if pickle_size is None:
        return None

    # Read actual tab_id
    payload_tab_id = reader.read_int32()
    if payload_tab_id is None:
        return None

    entry = NavigationEntry()

    # index (navigation entry index within the tab)
    entry.index = reader.read_int32() or 0

    # virtual_url (this is the actual URL)
    url = reader.read_string()
    if url is None:
        return None
    entry.url = url

    # title (UTF-16)
    title = reader.read_string16()
    if title is not None:
        entry.title = title

    # encoded_page_state (skip it, can be large)
    page_state = reader.read_string()

    # transition_type
    transition = reader.read_int32()
    if transition is not None:
        entry.transition_type = transition

    # From here on, fields are optional and may not be present

    # type_mask (contains has_post_data)
    type_mask = reader.read_int32()
    if type_mask is not None:
        entry.has_post_data = bool(type_mask & 1)

        # referrer_url
        referrer = reader.read_string()
        if referrer:
            entry.referrer_url = referrer

        # referrer_policy (deprecated, skip)
        reader.read_int32()

        # original_request_url
        original_url = reader.read_string()
        if original_url:
            entry.original_request_url = original_url

        # is_overriding_user_agent
        override_ua = reader.read_bool()
        if override_ua is not None:
            entry.is_overriding_user_agent = override_ua

        # timestamp (int64 - microseconds since Windows epoch)
        timestamp_value = reader.read_int64()
        if timestamp_value is not None:
            entry.timestamp = _windows_time_to_datetime(timestamp_value)

        # search_terms (removed, but still in format - skip)
        reader.read_string16()

        # http_status_code
        status_code = reader.read_int32()
        if status_code is not None:
            entry.http_status_code = status_code

    return (payload_tab_id, entry)


def parse_last_active_time(payload: bytes) -> Optional[tuple[int, datetime]]:
    """Parse LastActiveTime command payload.

    Format (Chrome 100+):
    - 4 bytes: tab_id
    - 4 bytes: padding/unknown (always 0)
    - 8 bytes: timestamp (Windows epoch microseconds)
    """
    if len(payload) < 16:  # 4 bytes tab_id + 4 bytes padding + 8 bytes timestamp
        return None

    tab_id = struct.unpack_from('<i', payload, 0)[0]
    # Skip 4 bytes of padding at offset 4
    timestamp_value = struct.unpack_from('<q', payload, 8)[0]

    dt = _windows_time_to_datetime(timestamp_value)
    if dt:
        return (tab_id, dt)
    return None


def parse_set_tab_window(payload: bytes) -> Optional[tuple[int, int]]:
    """Parse SetTabWindow command payload. Returns (window_id, tab_id)."""
    if len(payload) < 8:
        return None
    window_id = struct.unpack_from('<i', payload, 0)[0]
    tab_id = struct.unpack_from('<i', payload, 4)[0]
    return (window_id, tab_id)


def parse_set_tab_index_in_window(payload: bytes) -> Optional[tuple[int, int]]:
    """Parse SetTabIndexInWindow command payload. Returns (tab_id, index)."""
    if len(payload) < 8:
        return None
    tab_id = struct.unpack_from('<i', payload, 0)[0]
    index = struct.unpack_from('<i', payload, 4)[0]
    return (tab_id, index)


def parse_set_pinned_state(payload: bytes) -> Optional[tuple[int, bool]]:
    """Parse SetPinnedState command payload. Returns (tab_id, is_pinned)."""
    if len(payload) < 5:
        return None
    tab_id = struct.unpack_from('<i', payload, 0)[0]
    is_pinned = payload[4] != 0
    return (tab_id, is_pinned)


def parse_selected_navigation_index(payload: bytes) -> Optional[tuple[int, int]]:
    """Parse SetSelectedNavigationIndex command payload. Returns (tab_id, index)."""
    if len(payload) < 8:
        return None
    tab_id = struct.unpack_from('<i', payload, 0)[0]
    index = struct.unpack_from('<i', payload, 4)[0]
    return (tab_id, index)


def parse_selected_tab_in_index(payload: bytes) -> Optional[tuple[int, int]]:
    """Parse SetSelectedTabInIndex command payload. Returns (window_id, index)."""
    if len(payload) < 8:
        return None
    window_id = struct.unpack_from('<i', payload, 0)[0]
    index = struct.unpack_from('<i', payload, 4)[0]
    return (window_id, index)


def parse_closed(payload: bytes) -> Optional[tuple[int, Optional[datetime]]]:
    """Parse TabClosed/WindowClosed command payload. Returns (id, close_time)."""
    if len(payload) < 12:
        return None
    item_id = struct.unpack_from('<i', payload, 0)[0]
    # close_time is at offset 4, but there may be padding
    close_time_value = struct.unpack_from('<q', payload, 4)[0]
    close_time = _windows_time_to_datetime(close_time_value)
    return (item_id, close_time)


def _read_commands(data: bytes) -> Iterator[tuple[int, bytes]]:
    """
    Read commands from SNSS file data (after header).

    Yields (command_id, payload) tuples.
    """
    pos = 0

    while pos < len(data):
        # Command size (2 bytes)
        if pos + 2 > len(data):
            break
        command_size = struct.unpack_from('<H', data, pos)[0]
        pos += 2

        if command_size == 0:
            # Empty command indicates end or error
            break

        if pos + command_size > len(data):
            # Incomplete command
            break

        # Command ID is first byte of payload
        command_id = data[pos]
        payload = data[pos + 1:pos + command_size]

        pos += command_size

        yield (command_id, payload)


def parse_snss_file(file_path: Path) -> SNSSParseResult:
    """
    Parse a Chromium SNSS session file.

    Args:
        file_path: Path to the SNSS file

    Returns:
        SNSSParseResult with parsed data
    """
    result = SNSSParseResult()

    try:
        data = file_path.read_bytes()
    except Exception as e:
        result.errors.append(f"Failed to read file: {e}")
        return result

    return parse_snss_data(data)


def parse_snss_data(data: bytes) -> SNSSParseResult:
    """
    Parse SNSS data from bytes.

    Args:
        data: Raw SNSS file data

    Returns:
        SNSSParseResult with parsed data
    """
    result = SNSSParseResult()

    if len(data) < 8:
        result.errors.append("File too small for header")
        return result

    # Parse header
    signature, version = struct.unpack_from('<II', data, 0)

    if signature != SNSS_SIGNATURE:
        result.errors.append(f"Invalid signature: 0x{signature:08X}, expected 0x{SNSS_SIGNATURE:08X}")
        return result

    result.version = version
    result.is_encrypted = version in (ENCRYPTED_FILE_VERSION, ENCRYPTED_FILE_VERSION_WITH_MARKER)

    if result.is_encrypted:
        result.errors.append("Encrypted SNSS files are not supported")
        return result

    if version not in (FILE_VERSION_1, FILE_VERSION_WITH_MARKER, ENCRYPTED_FILE_VERSION, ENCRYPTED_FILE_VERSION_WITH_MARKER):
        result.errors.append(f"Unknown SNSS version: {version}")
        return result

    result.is_valid = True

    # Data structures for aggregation
    tabs: dict[int, TabInfo] = {}
    windows: dict[int, WindowInfo] = {}
    all_navigation_entries: List[NavigationEntry] = []
    unknown_commands: set = set()

    # Parse commands
    command_data = data[8:]  # Skip header

    for command_id, payload in _read_commands(command_data):
        result.total_commands += 1

        # Track unknown command IDs for schema warnings
        if command_id not in KNOWN_COMMAND_IDS:
            unknown_commands.add(command_id)

        try:
            # Navigation entries: command 6 in Session_* files, command 1 in Tabs_* files
            if command_id in (CMD_UPDATE_TAB_NAVIGATION, CMD_TAB_UPDATE_TAB_NAVIGATION):
                parsed = parse_navigation_entry(payload, 0)
                if parsed:
                    tab_id, entry = parsed
                    if tab_id not in tabs:
                        tabs[tab_id] = TabInfo(tab_id=tab_id)
                    tabs[tab_id].navigations.append(entry)
                    all_navigation_entries.append(entry)

            # Tab-window association: command 0 in Session_* files, command 4 in Tabs_* files
            elif command_id in (CMD_SET_TAB_WINDOW, CMD_TAB_SET_TAB_WINDOW):
                parsed = parse_set_tab_window(payload)
                if parsed:
                    window_id, tab_id = parsed
                    if tab_id not in tabs:
                        tabs[tab_id] = TabInfo(tab_id=tab_id)
                    tabs[tab_id].window_id = window_id
                    if window_id not in windows:
                        windows[window_id] = WindowInfo(window_id=window_id)

            elif command_id == CMD_SET_TAB_INDEX_IN_WINDOW:
                parsed = parse_set_tab_index_in_window(payload)
                if parsed:
                    tab_id, index = parsed
                    if tab_id not in tabs:
                        tabs[tab_id] = TabInfo(tab_id=tab_id)
                    tabs[tab_id].index_in_window = index

            elif command_id == CMD_SET_PINNED_STATE:
                parsed = parse_set_pinned_state(payload)
                if parsed:
                    tab_id, is_pinned = parsed
                    if tab_id not in tabs:
                        tabs[tab_id] = TabInfo(tab_id=tab_id)
                    tabs[tab_id].pinned = is_pinned

            elif command_id == CMD_SET_SELECTED_NAVIGATION_INDEX:
                parsed = parse_selected_navigation_index(payload)
                if parsed:
                    tab_id, index = parsed
                    if tab_id not in tabs:
                        tabs[tab_id] = TabInfo(tab_id=tab_id)
                    tabs[tab_id].current_navigation_index = index

            elif command_id == CMD_SET_SELECTED_TAB_IN_INDEX:
                parsed = parse_selected_tab_in_index(payload)
                if parsed:
                    window_id, index = parsed
                    if window_id not in windows:
                        windows[window_id] = WindowInfo(window_id=window_id)
                    windows[window_id].selected_tab_index = index

            elif command_id == CMD_LAST_ACTIVE_TIME:
                parsed = parse_last_active_time(payload)
                if parsed:
                    tab_id, last_active = parsed
                    if tab_id not in tabs:
                        tabs[tab_id] = TabInfo(tab_id=tab_id)
                    tabs[tab_id].last_active_time = last_active

            elif command_id == CMD_TAB_CLOSED:
                parsed = parse_closed(payload)
                if parsed:
                    tab_id, _ = parsed
                    tabs.pop(tab_id, None)

            elif command_id == CMD_WINDOW_CLOSED:
                parsed = parse_closed(payload)
                if parsed:
                    window_id, _ = parsed
                    windows.pop(window_id, None)

        except Exception as e:
            LOGGER.debug("Error parsing command %d: %s", command_id, e)

    # Convert to lists and populate result
    result.tabs = list(tabs.values())
    result.windows = list(windows.values())
    result.navigation_entries = all_navigation_entries
    result.unknown_commands = unknown_commands

    return result


def extract_urls_with_metadata(data: bytes, max_urls: int = 0) -> List[dict]:
    """
    Extract URLs with metadata from SNSS data.

    Convenience function that returns a list of dicts with url, title, timestamp.

    Args:
        data: Raw SNSS file data
        max_urls: Maximum URLs to return (0 = unlimited)

    Returns:
        List of dicts with 'url', 'title', 'timestamp', 'referrer_url' keys
    """
    result = parse_snss_data(data)

    urls = []
    seen = set()

    for entry in result.navigation_entries:
        if not entry.url:
            continue

        # Skip duplicates by URL
        if entry.url in seen:
            continue
        seen.add(entry.url)

        urls.append({
            'url': entry.url,
            'title': entry.title,
            'timestamp': entry.timestamp,
            'referrer_url': entry.referrer_url,
            'http_status_code': entry.http_status_code,
            'transition_type': entry.transition_type,
        })

        if max_urls > 0 and len(urls) >= max_urls:
            break

    return urls
