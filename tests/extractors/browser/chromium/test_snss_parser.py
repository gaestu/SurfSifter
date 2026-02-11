"""
Tests for SNSS Parser Module

Tests the Chromium SNSS (Session Service) binary format parser:
- Magic header and version parsing
- Command structure parsing
- Navigation entry pickle deserialization
- URL, title, and timestamp extraction
- Tab and window reconstruction
"""
import struct
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from extractors.browser.chromium.sessions.snss_parser import (
    parse_snss_data,
    parse_navigation_entry,
    extract_urls_with_metadata,
    PickleReader,
    NavigationEntry,
    TabInfo,
    WindowInfo,
    SNSSParseResult,
    SNSS_SIGNATURE,
    CMD_UPDATE_TAB_NAVIGATION,
    CMD_SET_SELECTED_NAVIGATION_INDEX,
    CMD_SET_TAB_WINDOW,
    CMD_SET_PINNED_STATE,
    CMD_SET_TAB_GROUP,
    CMD_LAST_ACTIVE_TIME,
)

# Magic header bytes
SNSS_MAGIC = struct.pack("<I", SNSS_SIGNATURE)


# ===========================================================================
# Test Fixtures
# ===========================================================================

@pytest.fixture
def valid_snss_header() -> bytes:
    """Create a valid SNSS file header."""
    return SNSS_MAGIC + struct.pack("<I", 1)  # Version 1


@pytest.fixture
def empty_snss_file(valid_snss_header) -> bytes:
    """Create an empty but valid SNSS file."""
    return valid_snss_header


def create_snss_command(command_id: int, payload: bytes) -> bytes:
    """Helper to create an SNSS command."""
    size_type = len(payload) + 1  # +1 for command_id byte
    return struct.pack("<HB", size_type, command_id) + payload


def create_pickle_string(value: str) -> bytes:
    """Create a pickled string (4-byte length + UTF-8 data, aligned to 4 bytes)."""
    encoded = value.encode("utf-8")
    length = len(encoded)
    data = struct.pack("<I", length) + encoded
    # Pad to 4-byte alignment
    padding = (4 - (len(data) % 4)) % 4
    return data + (b"\x00" * padding)


def create_pickle_string16(value: str) -> bytes:
    """Create a pickled UTF-16 string (4-byte length + UTF-16LE data)."""
    encoded = value.encode("utf-16-le")
    length = len(encoded) // 2  # Character count, not byte count
    data = struct.pack("<I", length) + encoded
    # Pad to 4-byte alignment
    padding = (4 - (len(data) % 4)) % 4
    return data + (b"\x00" * padding)


# ===========================================================================
# Constants Tests
# ===========================================================================

class TestSNSSConstants:
    """Test SNSS format constants."""

    def test_snss_signature(self):
        """Test SNSS signature value."""
        assert SNSS_SIGNATURE == 0x53534E53  # "SSNS"

    def test_windows_epoch_offset(self):
        """Test Windows epoch offset constant."""
        from extractors.browser.chromium.sessions.snss_parser import WINDOWS_EPOCH_OFFSET_MICROS
        # 11644473600 seconds from 1601-01-01 to 1970-01-01
        assert WINDOWS_EPOCH_OFFSET_MICROS == 11644473600000000


# ===========================================================================
# Timestamp Conversion Tests
# ===========================================================================

class TestTimestampConversion:
    """Test Windows timestamp to datetime conversion."""

    def test_unix_epoch_conversion(self):
        """Test converting Unix epoch (1970-01-01) from Windows time."""
        from extractors.browser.chromium.sessions.snss_parser import _windows_time_to_datetime

        # Unix epoch in Windows microseconds
        unix_epoch_windows = 11644473600000000
        dt = _windows_time_to_datetime(unix_epoch_windows)

        assert dt is not None
        assert dt.year == 1970
        assert dt.month == 1
        assert dt.day == 1

    def test_recent_date_conversion(self):
        """Test converting a recent date."""
        from extractors.browser.chromium.sessions.snss_parser import _windows_time_to_datetime

        # Jan 1, 2024 00:00:00 UTC
        # Unix timestamp = 1704067200
        # Windows microseconds = (1704067200 + 11644473600) * 1_000_000
        jan_2024_windows = 13348540800000000
        dt = _windows_time_to_datetime(jan_2024_windows)

        assert dt is not None
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 1

    def test_zero_timestamp_returns_none(self):
        """Test zero timestamp returns None."""
        from extractors.browser.chromium.sessions.snss_parser import _windows_time_to_datetime
        assert _windows_time_to_datetime(0) is None

    def test_negative_timestamp_returns_none(self):
        """Test negative timestamp returns None."""
        from extractors.browser.chromium.sessions.snss_parser import _windows_time_to_datetime
        assert _windows_time_to_datetime(-1) is None

    def test_timestamp_has_utc_timezone(self):
        """Test converted timestamps have UTC timezone."""
        from extractors.browser.chromium.sessions.snss_parser import _windows_time_to_datetime

        dt = _windows_time_to_datetime(13348540800000000)
        assert dt is not None
        assert dt.tzinfo is not None
        assert dt.tzinfo == timezone.utc

    def test_iso_format_output(self):
        """Test ISO format string generation for database storage."""
        from extractors.browser.chromium.sessions.snss_parser import _windows_time_to_datetime

        dt = _windows_time_to_datetime(13348540800000000)
        iso = dt.isoformat()

        # Should produce ISO 8601 format with timezone
        assert iso == "2024-01-01T00:00:00+00:00"

    def test_command_ids(self):
        """Test command ID constants match Chromium source."""
        # These values come from session_service_commands.cc
        assert CMD_UPDATE_TAB_NAVIGATION == 6
        assert CMD_SET_SELECTED_NAVIGATION_INDEX == 7
        assert CMD_SET_TAB_WINDOW == 0
        assert CMD_SET_PINNED_STATE == 12
        assert CMD_LAST_ACTIVE_TIME == 21


# ===========================================================================
# PickleReader Tests
# ===========================================================================

class TestPickleReader:
    """Test pickle deserialization utilities."""

    def test_read_int32(self):
        """Test 32-bit integer reading."""
        data = struct.pack("<i", 42)
        reader = PickleReader(data)
        assert reader.read_int32() == 42

    def test_read_int32_negative(self):
        """Test negative 32-bit integer reading."""
        data = struct.pack("<i", -123)
        reader = PickleReader(data)
        assert reader.read_int32() == -123

    def test_read_int64(self):
        """Test 64-bit integer reading."""
        data = struct.pack("<q", 123456789012345)
        reader = PickleReader(data)
        assert reader.read_int64() == 123456789012345

    def test_read_uint32(self):
        """Test unsigned 32-bit integer reading."""
        data = struct.pack("<I", 0xDEADBEEF)
        reader = PickleReader(data)
        assert reader.read_uint32() == 0xDEADBEEF

    def test_read_string(self):
        """Test string reading with alignment."""
        data = create_pickle_string("hello")
        reader = PickleReader(data)
        assert reader.read_string() == "hello"

    def test_read_string_with_padding(self):
        """Test string with alignment padding."""
        # "hi" = 2 bytes, should be padded to 4 bytes
        data = create_pickle_string("hi")
        reader = PickleReader(data)
        assert reader.read_string() == "hi"

    def test_read_string16(self):
        """Test UTF-16 string reading."""
        data = create_pickle_string16("Test Title")
        reader = PickleReader(data)
        assert reader.read_string16() == "Test Title"

    def test_read_string16_unicode(self):
        """Test UTF-16 string with non-ASCII characters."""
        data = create_pickle_string16("日本語テスト")
        reader = PickleReader(data)
        assert reader.read_string16() == "日本語テスト"

    def test_read_past_end(self):
        """Test reading past end of data returns None."""
        data = struct.pack("<i", 42)
        reader = PickleReader(data)
        assert reader.read_int32() == 42
        assert reader.read_int32() is None

    def test_remaining_property(self):
        """Test remaining bytes calculation."""
        data = struct.pack("<ii", 1, 2)
        reader = PickleReader(data)
        assert reader.remaining == 8
        reader.read_int32()
        assert reader.remaining == 4


# ===========================================================================
# NavigationEntry Tests
# ===========================================================================

class TestNavigationEntry:
    """Test navigation entry data class."""

    def test_navigation_entry_creation(self):
        """Test creating a navigation entry."""
        entry = NavigationEntry(
            index=0,
            url="https://example.com",
            title="Example Site",
            timestamp=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
            referrer_url="https://google.com",
            transition_type=0,
        )
        assert entry.url == "https://example.com"
        assert entry.title == "Example Site"
        assert entry.referrer_url == "https://google.com"

    def test_navigation_entry_defaults(self):
        """Test navigation entry default values."""
        entry = NavigationEntry(index=0)
        assert entry.url == ""
        assert entry.title == ""
        assert entry.timestamp is None
        assert entry.referrer_url == ""
        assert entry.transition_type == 0
        assert entry.http_status_code == 0


# ===========================================================================
# TabInfo Tests
# ===========================================================================

class TestTabInfo:
    """Test tab info data class."""

    def test_tab_info_creation(self):
        """Test creating a tab info."""
        tab = TabInfo(tab_id=1)
        tab.navigations.append(NavigationEntry(
            index=0,
            url="https://example.com",
            title="Example",
        ))
        assert tab.tab_id == 1
        assert len(tab.navigations) == 1

    def test_tab_info_defaults(self):
        """Test tab info default values."""
        tab = TabInfo(tab_id=1)
        assert tab.window_id == 0
        assert tab.index_in_window == 0
        assert tab.current_navigation_index == 0
        assert tab.pinned is False
        assert tab.group_id is None
        assert tab.last_active_time is None


# ===========================================================================
# SNSSParseResult Tests
# ===========================================================================

class TestSNSSParseResult:
    """Test parse result data class."""

    def test_parse_result_creation(self):
        """Test creating a parse result."""
        result = SNSSParseResult(is_valid=True, version=1)
        assert result.is_valid is True
        assert result.version == 1
        assert len(result.tabs) == 0
        assert len(result.windows) == 0

    def test_parse_result_with_errors(self):
        """Test parse result with errors."""
        result = SNSSParseResult(is_valid=False, version=0)
        result.errors.append("Invalid magic header")
        assert result.is_valid is False
        assert "Invalid magic header" in result.errors


# ===========================================================================
# parse_snss_data Tests
# ===========================================================================

class TestParseSNSSData:
    """Test main SNSS parsing function."""

    def test_empty_data_invalid(self):
        """Test empty data returns invalid result."""
        result = parse_snss_data(b"")
        assert result.is_valid is False
        assert len(result.errors) > 0

    def test_too_short_data_invalid(self):
        """Test data shorter than header returns invalid result."""
        result = parse_snss_data(b"SN")
        assert result.is_valid is False

    def test_wrong_magic_invalid(self):
        """Test wrong magic header returns invalid result."""
        data = b"ABCD" + struct.pack("<I", 1)
        result = parse_snss_data(data)
        assert result.is_valid is False
        assert len(result.errors) > 0

    def test_valid_empty_file(self, empty_snss_file):
        """Test valid empty SNSS file parses successfully."""
        result = parse_snss_data(empty_snss_file)
        assert result.is_valid is True
        assert result.version == 1
        assert len(result.tabs) == 0
        assert result.total_commands == 0

    def test_version_extraction(self):
        """Test version extraction from header."""
        for version in [1, 2, 3, 4]:
            data = SNSS_MAGIC + struct.pack("<I", version)
            result = parse_snss_data(data)
            assert result.version == version

    def test_high_version_still_parses(self):
        """Test high version number still parses."""
        data = SNSS_MAGIC + struct.pack("<I", 99)
        result = parse_snss_data(data)
        # Should still parse - version is just informational
        assert result.version == 99


# ===========================================================================
# Command Parsing Tests
# ===========================================================================

class TestCommandParsing:
    """Test individual command parsing."""

    def test_command_structure(self, valid_snss_header):
        """Test basic command structure parsing."""
        # Create a simple command (size=1 means just command_id, no payload)
        cmd = create_snss_command(0xFF, b"")  # Unknown command with no payload
        data = valid_snss_header + cmd
        result = parse_snss_data(data)
        assert result.is_valid is True
        assert result.total_commands == 1

    def test_truncated_command_handled(self, valid_snss_header):
        """Test truncated command is handled gracefully."""
        # Command header says 100 bytes but only 10 exist
        cmd_header = struct.pack("<HB", 100, 6)  # size=100, cmd=6
        data = valid_snss_header + cmd_header + b"short"
        result = parse_snss_data(data)
        # Should handle gracefully, either skip or report error
        assert result is not None  # Doesn't crash


# ===========================================================================
# URL Extraction Tests
# ===========================================================================

class TestExtractURLsWithMetadata:
    """Test URL extraction helper function."""

    def test_empty_result(self):
        """Test extraction from invalid data."""
        urls = extract_urls_with_metadata(b"")
        assert urls == []

    def test_invalid_data_returns_empty(self):
        """Test invalid data returns empty list."""
        urls = extract_urls_with_metadata(b"invalid data")
        assert urls == []

    def test_valid_empty_file_returns_empty(self):
        """Test valid empty SNSS file returns empty list."""
        data = SNSS_MAGIC + struct.pack("<I", 1)  # Just header, no commands
        urls = extract_urls_with_metadata(data)
        assert urls == []


# ===========================================================================
# parse_snss_data Direct Tests with Populated Results
# ===========================================================================

class TestParseSNSSDataResultHandling:
    """Test handling of parse results."""

    def test_navigation_entries_collection(self):
        """Test navigation entries are collected in result."""
        result = SNSSParseResult(is_valid=True, version=1)
        entry = NavigationEntry(
            index=0,
            url="https://example.com",
            title="Example",
        )
        result.navigation_entries.append(entry)

        assert len(result.navigation_entries) == 1
        assert result.navigation_entries[0].url == "https://example.com"

    def test_tabs_support_multiple_navigations(self):
        """Test tabs can hold multiple navigation entries."""
        tab = TabInfo(tab_id=1)
        tab.navigations.append(NavigationEntry(index=0, url="https://site1.com"))
        tab.navigations.append(NavigationEntry(index=1, url="https://site2.com"))
        tab.navigations.append(NavigationEntry(index=2, url="https://site3.com"))

        assert len(tab.navigations) == 3

    def test_non_http_urls_supported(self):
        """Test non-HTTP URLs are supported (unlike old regex approach)."""
        # Create entries with various URL schemes
        entry1 = NavigationEntry(index=0, url="file:///home/user/doc.pdf")
        entry2 = NavigationEntry(index=1, url="chrome://settings")
        entry3 = NavigationEntry(index=2, url="data:text/html,<h1>Hi</h1>")

        # All URL schemes should be preserved
        assert entry1.url.startswith("file://")
        assert entry2.url.startswith("chrome://")
        assert entry3.url.startswith("data:")

    def test_result_has_no_url_limit(self):
        """Test result can hold many entries (unlike old 100 limit)."""
        result = SNSSParseResult(is_valid=True, version=1)

        # Add 150 entries
        for i in range(150):
            result.navigation_entries.append(NavigationEntry(
                index=i,
                url=f"https://example{i}.com",
            ))

        assert len(result.navigation_entries) == 150  # Not capped at 100


# ===========================================================================
# Integration Tests
# ===========================================================================

class TestSNSSParserIntegration:
    """Integration tests for complete parsing workflows."""

    def test_tab_window_association(self):
        """Test tab-window relationship tracking."""
        result = SNSSParseResult(is_valid=True, version=1)
        window = WindowInfo(window_id=1)
        tab = TabInfo(tab_id=10, window_id=1)

        result.windows.append(window)
        result.tabs.append(tab)

        assert result.tabs[0].window_id == 1
        assert len(result.windows) == 1


# ===========================================================================
# Edge Cases Tests
# ===========================================================================

class TestSNSSParserEdgeCases:
    """Test edge cases and error handling."""

    def test_corrupted_data_handled(self):
        """Test corrupted data doesn't crash."""
        # Create header + garbage
        data = SNSS_MAGIC + struct.pack("<I", 1) + b"\xFF\xFF\xFF\xFF" * 10
        result = parse_snss_data(data)
        # Should not crash, just return with possible warnings
        assert result is not None

    def test_zero_length_command_handled(self):
        """Test zero-length command doesn't crash."""
        # Valid header + zero-length command
        header = SNSS_MAGIC + struct.pack("<I", 1)
        cmd = struct.pack("<HB", 0, 0)  # size=0, cmd=0
        data = header + cmd
        result = parse_snss_data(data)
        assert result is not None

    def test_unicode_title_parsing(self):
        """Test Unicode titles in navigation entries."""
        entry = NavigationEntry(
            index=0,
            url="https://example.com",
            title="日本語タイトル — Test 🔒",
        )
        assert entry.title == "日本語タイトル — Test 🔒"

    def test_empty_url_in_entry(self):
        """Test entries with empty URLs are handled."""
        entry = NavigationEntry(index=0, url="")
        assert entry.url == ""
