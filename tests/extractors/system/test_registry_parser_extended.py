import codecs
import json
import struct
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock, patch
from pathlib import Path
from extractors.system.registry.parser import (
    _process_registry_key,
    _process_typed_urls,
    _process_recent_docs_extension,
    _process_word_wheel_query,
    _process_user_assist,
    _extract_utf16le_filename,
    RegistryFinding,
    filetime_to_datetime,
    unix_timestamp_to_datetime,
    systemtime_to_datetime,
    format_datetime,
    CUSTOM_HANDLERS,
)


# =============================================================================
# Timestamp Conversion Tests
# =============================================================================

class TestFiletimeConversion:
    """Tests for Windows FILETIME to datetime conversion."""

    def test_filetime_bytes_to_datetime(self):
        """Test converting FILETIME bytes to datetime."""
        # Known FILETIME: 2024-07-29 22:09:26 UTC
        # FILETIME value: 133667645663557812
        filetime_int = 133667645663557812
        filetime_bytes = struct.pack('<Q', filetime_int)

        result = filetime_to_datetime(filetime_bytes)

        assert result is not None
        assert result.year == 2024
        assert result.month == 7
        assert result.day == 29
        assert result.hour == 22
        assert result.minute == 9
        assert result.second == 26

    def test_filetime_int_to_datetime(self):
        """Test converting FILETIME integer to datetime."""
        filetime_int = 133667645663557812

        result = filetime_to_datetime(filetime_int)

        assert result is not None
        assert result.year == 2024
        assert result.month == 7
        assert result.day == 29

    def test_filetime_zero_returns_none(self):
        """Test that FILETIME of 0 returns None."""
        assert filetime_to_datetime(0) is None
        assert filetime_to_datetime(b'\x00\x00\x00\x00\x00\x00\x00\x00') is None

    def test_filetime_invalid_bytes_length(self):
        """Test that invalid byte length returns None."""
        assert filetime_to_datetime(b'\x00\x00\x00\x00') is None
        assert filetime_to_datetime(b'\x00') is None

    def test_filetime_known_date(self):
        """Test with a known date: 2023-04-25 16:46:38 UTC."""
        # This corresponds to Unix timestamp 1682437598
        unix_ts = 1682437598
        # Convert to FILETIME
        filetime = (unix_ts * 10000000) + 116444736000000000

        result = filetime_to_datetime(filetime)

        assert result is not None
        assert result.year == 2023
        assert result.month == 4
        assert result.day == 25


class TestUnixTimestampConversion:
    """Tests for Unix timestamp to datetime conversion."""

    def test_unix_timestamp_to_datetime(self):
        """Test converting Unix timestamp to datetime."""
        # Known timestamp: 2023-04-25 16:46:38 UTC
        unix_ts = 1682441198

        result = unix_timestamp_to_datetime(unix_ts)

        assert result is not None
        assert result.year == 2023
        assert result.month == 4
        assert result.day == 25

    def test_unix_timestamp_zero_returns_none(self):
        """Test that Unix timestamp of 0 returns None."""
        assert unix_timestamp_to_datetime(0) is None

    def test_unix_timestamp_epoch(self):
        """Test Unix epoch (1970-01-01)."""
        result = unix_timestamp_to_datetime(1)

        assert result is not None
        assert result.year == 1970
        assert result.month == 1
        assert result.day == 1


class TestSystemtimeConversion:
    """Tests for Windows SYSTEMTIME structure to datetime conversion."""

    def test_systemtime_to_datetime_known_date(self):
        """Test converting SYSTEMTIME bytes to datetime with known date."""
        # SYSTEMTIME for 2023-09-05 23:45:56.385 UTC
        # Based on the raw bytes shown in the issue: b'\xe7\x07\t\x00\x02\x00\x05\x00\x17\x00-\x008\x00\x81\x01'
        # wYear=0x07e7=2023, wMonth=9, wDayOfWeek=2, wDay=5, wHour=23, wMinute=45, wSecond=56, wMs=385
        systemtime_bytes = struct.pack('<8H', 2023, 9, 2, 5, 23, 45, 56, 385)

        result = systemtime_to_datetime(systemtime_bytes)

        assert result is not None
        assert result.year == 2023
        assert result.month == 9
        assert result.day == 5
        assert result.hour == 23
        assert result.minute == 45
        assert result.second == 56
        assert result.microsecond == 385000  # milliseconds converted to microseconds

    def test_systemtime_to_datetime_real_bytes(self):
        """Test with actual bytes from registry (the reported issue)."""
        # These are the actual bytes reported: b'\xe7\x07\t\x00\x02\x00\x05\x00\x17\x00-\x008\x00\x81\x01'
        raw_bytes = b'\xe7\x07\t\x00\x02\x00\x05\x00\x17\x00-\x008\x00\x81\x01'

        result = systemtime_to_datetime(raw_bytes)

        assert result is not None
        assert result.year == 2023
        assert result.month == 9
        assert result.day == 5
        assert result.hour == 23
        assert result.minute == 45
        assert result.second == 56

    def test_systemtime_invalid_length_returns_none(self):
        """Test that invalid byte length returns None."""
        assert systemtime_to_datetime(b'\x00\x00\x00\x00') is None
        assert systemtime_to_datetime(b'\x00') is None
        assert systemtime_to_datetime(b'') is None

    def test_systemtime_invalid_month_returns_none(self):
        """Test that invalid month returns None."""
        # Month = 13 (invalid)
        invalid_bytes = struct.pack('<8H', 2023, 13, 0, 1, 0, 0, 0, 0)
        assert systemtime_to_datetime(invalid_bytes) is None

        # Month = 0 (invalid)
        invalid_bytes = struct.pack('<8H', 2023, 0, 0, 1, 0, 0, 0, 0)
        assert systemtime_to_datetime(invalid_bytes) is None

    def test_systemtime_invalid_day_returns_none(self):
        """Test that invalid day returns None."""
        # Day = 32 (invalid)
        invalid_bytes = struct.pack('<8H', 2023, 1, 0, 32, 0, 0, 0, 0)
        assert systemtime_to_datetime(invalid_bytes) is None

        # Day = 0 (invalid)
        invalid_bytes = struct.pack('<8H', 2023, 1, 0, 0, 0, 0, 0, 0)
        assert systemtime_to_datetime(invalid_bytes) is None

    def test_systemtime_invalid_year_returns_none(self):
        """Test that invalid year returns None."""
        # Year = 1600 (before FILETIME epoch)
        invalid_bytes = struct.pack('<8H', 1600, 1, 0, 1, 0, 0, 0, 0)
        assert systemtime_to_datetime(invalid_bytes) is None

    def test_systemtime_not_bytes_returns_none(self):
        """Test that non-bytes input returns None."""
        assert systemtime_to_datetime("not bytes") is None
        assert systemtime_to_datetime(123) is None
        assert systemtime_to_datetime(None) is None


class TestFormatDatetime:
    """Tests for datetime formatting."""

    def test_format_datetime_default(self):
        """Test default datetime formatting."""
        dt = datetime(2024, 7, 29, 22, 9, 26, tzinfo=timezone.utc)

        result = format_datetime(dt)

        assert result == "2024-07-29 22:09:26"

    def test_format_datetime_custom_format(self):
        """Test custom datetime formatting."""
        dt = datetime(2024, 7, 29, 22, 9, 26, tzinfo=timezone.utc)

        result = format_datetime(dt, "%d.%m.%Y %H:%M:%S")

        assert result == "29.07.2024 22:09:26"

    def test_format_datetime_none_returns_empty(self):
        """Test that None input returns empty string."""
        assert format_datetime(None) == ""


# =============================================================================
# Registry Key Processing Tests
# =============================================================================

def test_extract_all_values():
    """Test extracting all values from a registry key."""
    # Mock RegistryKey
    key = MagicMock()
    key.name = "Run"
    key.header.last_modified = "2023-01-01 12:00:00"

    # Mock iter_values
    val1 = Mock()
    val1.name = "Program1"
    val1.value = "C:\\Program1.exe"

    val2 = Mock()
    val2.name = "Program2"
    val2.value = "C:\\Program2.exe"

    key.iter_values.return_value = [val1, val2]

    # Define target and action
    target = {"name": "test_target"}
    action = {"provenance": "test_prov"}
    key_def = {
        "path": "Run",
        "extract_all_values": True,
        "indicator": "startup:run_key"
    }

    findings = []
    _process_registry_key(
        key, key_def, target, action, "test.hive", findings, "Run"
    )

    assert len(findings) == 2
    assert findings[0].name == "startup:run_key"
    assert findings[0].value == "C:\\Program1.exe"
    assert findings[0].path == "Run\\Program1"

    extra = json.loads(findings[0].extra_json)
    assert extra["type"] == "startup:run_key"
    assert extra["value_name"] == "Program1"
    assert extra["raw_value"] == "C:\\Program1.exe"


def test_profile_path_type_extracts_username():
    """Test that profile_path type extracts username from path."""
    key = MagicMock()
    key.name = "S-1-5-21-123456-1001"
    key.header.last_modified = "2023-01-01 12:00:00"
    key.get_value.return_value = "C:\\Users\\HP"

    target = {"name": "system_info_software"}
    action = {"provenance": "registry_system_info"}
    key_def = {
        "path": "ProfileList\\*",
        "values": [{
            "name": "ProfileImagePath",
            "indicator": "system:user_profile",
            "type": "profile_path",
        }]
    }

    findings = []
    _process_registry_key(
        key, key_def, target, action, "SOFTWARE.hive", findings, "ProfileList\\S-1-5-21-123456-1001"
    )

    assert len(findings) == 1
    assert findings[0].name == "system:user_profile"
    assert findings[0].value == "HP"  # Username extracted from path

    extra = json.loads(findings[0].extra_json)
    assert extra["profile_path"] == "C:\\Users\\HP"
    assert extra["username"] == "HP"


def test_profile_path_type_handles_system_profiles():
    """Test profile_path type with system profile paths."""
    key = MagicMock()
    key.name = "S-1-5-18"
    key.header.last_modified = "2023-01-01 12:00:00"
    key.get_value.return_value = "%systemroot%\\system32\\config\\systemprofile"

    target = {"name": "system_info_software"}
    action = {"provenance": "registry_system_info"}
    key_def = {
        "path": "ProfileList\\*",
        "values": [{
            "name": "ProfileImagePath",
            "indicator": "system:user_profile",
            "type": "profile_path",
        }]
    }

    findings = []
    _process_registry_key(
        key, key_def, target, action, "SOFTWARE.hive", findings, "ProfileList\\S-1-5-18"
    )

    assert len(findings) == 1
    assert findings[0].value == "systemprofile"  # Last path component


def test_filetime_bytes_type_parses_shutdown_time():
    """Test that filetime_bytes type parses ShutdownTime correctly."""
    key = MagicMock()
    key.name = "Windows"
    key.header.last_modified = "2024-07-29 22:00:00"

    # FILETIME bytes for 2024-07-29 22:09:26 UTC
    filetime_int = 133667645663557812
    filetime_bytes = struct.pack('<Q', filetime_int)
    key.get_value.return_value = filetime_bytes

    target = {"name": "system_info_system"}
    action = {"provenance": "registry_system_info"}
    key_def = {
        "path": "ControlSet001\\Control\\Windows",
        "values": [{
            "name": "ShutdownTime",
            "indicator": "system:last_shutdown",
            "type": "filetime_bytes",
        }]
    }

    findings = []
    _process_registry_key(
        key, key_def, target, action, "SYSTEM.hive", findings, "ControlSet001\\Control\\Windows"
    )

    assert len(findings) == 1
    assert findings[0].name == "system:last_shutdown"
    assert "2024-07-29 22:09:26" in findings[0].value

    extra = json.loads(findings[0].extra_json)
    assert "timestamp_utc" in extra
    assert "2024-07-29" in extra["timestamp_utc"]


def test_unix_timestamp_type_parses_install_date():
    """Test that unix_timestamp type parses InstallDate correctly."""
    key = MagicMock()
    key.name = "CurrentVersion"
    key.header.last_modified = "2023-04-25 16:00:00"
    key.get_value.return_value = 1682441198  # 2023-04-25 16:46:38 UTC

    target = {"name": "system_info_software"}
    action = {"provenance": "registry_system_info"}
    key_def = {
        "path": "Microsoft\\Windows NT\\CurrentVersion",
        "values": [{
            "name": "InstallDate",
            "indicator": "system:install_date",
            "type": "unix_timestamp",
        }]
    }

    findings = []
    _process_registry_key(
        key, key_def, target, action, "SOFTWARE.hive", findings, "Microsoft\\Windows NT\\CurrentVersion"
    )

    assert len(findings) == 1
    assert findings[0].name == "system:install_date"
    assert "2023-04-25" in findings[0].value

    extra = json.loads(findings[0].extra_json)
    assert "timestamp_utc" in extra


# =============================================================================
# Custom Handler Dispatch Tests
# =============================================================================

class TestCustomHandlerDispatch:
    """Tests for the custom handler dispatch mechanism."""

    def test_custom_handler_dispatches_correctly(self):
        """Test that custom_handler in key_def routes to the right handler."""
        key = MagicMock()
        key.name = "TestKey"
        key.header.last_modified = "2024-01-01 00:00:00"
        key.iter_values.return_value = []

        target = {"name": "test_target"}
        action = {"provenance": "test_prov"}
        key_def = {
            "path": "Test\\Path",
            "custom_handler": "word_wheel_query",
            "indicator": "user_activity:explorer_search",
        }

        findings = []
        # Should not raise, and should not fall through to extract_all_values etc.
        _process_registry_key(
            key, key_def, target, action, "test.hive", findings, "Test\\Path"
        )
        # No values to iterate, so no findings — but it should not error
        assert isinstance(findings, list)

    def test_unknown_handler_falls_through(self):
        """Test that an unknown handler name logs a warning and falls through."""
        key = MagicMock()
        key.name = "TestKey"
        key.header.last_modified = "2024-01-01 00:00:00"

        target = {"name": "test_target"}
        action = {"provenance": "test_prov"}
        key_def = {
            "path": "Test\\Path",
            "custom_handler": "nonexistent_handler",
            "indicator": "test:indicator",
            "extract": True,
        }

        findings = []
        _process_registry_key(
            key, key_def, target, action, "test.hive", findings, "Test\\Path"
        )
        # Should fall through to normal processing (extract=True -> key existence)
        assert len(findings) == 1

    def test_all_handlers_registered(self):
        """Test that all expected handlers are in CUSTOM_HANDLERS."""
        assert "typed_urls" in CUSTOM_HANDLERS
        assert "recent_docs_extension" in CUSTOM_HANDLERS
        assert "word_wheel_query" in CUSTOM_HANDLERS
        assert "user_assist" in CUSTOM_HANDLERS


# =============================================================================
# TypedURLs Handler Tests
# =============================================================================

class TestTypedUrlsHandler:
    """Tests for the TypedURLs custom handler."""

    def test_typed_urls_basic(self):
        """Test TypedURLs handler with basic URL values."""
        key = MagicMock()
        key.name = "TypedURLs"
        key.header.last_modified = "2024-01-15 10:00:00"

        val1 = Mock()
        val1.name = "url1"
        val1.value = "https://www.google.com"

        val2 = Mock()
        val2.name = "url2"
        val2.value = "https://www.example.com"

        key.iter_values.return_value = [val1, val2]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "Software\\Microsoft\\Internet Explorer\\TypedURLs",
            "custom_handler": "typed_urls",
            "indicator": "browser:typed_url",
            "confidence": 0.85,
        }

        findings = []
        # Patch RegistryHive to avoid needing a real hive file
        with patch("regipy.registry.RegistryHive", side_effect=Exception("no hive"), create=True):
            _process_typed_urls(
                key, key_def, target, action, "NTUSER.DAT",
                findings, "Software\\Microsoft\\Internet Explorer\\TypedURLs"
            )

        assert len(findings) == 2
        assert findings[0].name == "browser:typed_url"
        assert findings[0].value == "https://www.google.com"
        assert findings[1].value == "https://www.example.com"

        extra = json.loads(findings[0].extra_json)
        assert extra["url"] == "https://www.google.com"
        assert extra["value_name"] == "url1"

    def test_typed_urls_skips_default(self):
        """Test that TypedURLs handler skips (Default) value."""
        key = MagicMock()
        key.name = "TypedURLs"
        key.header.last_modified = "2024-01-15 10:00:00"

        val_default = Mock()
        val_default.name = "(Default)"
        val_default.value = ""

        val1 = Mock()
        val1.name = "url1"
        val1.value = "https://www.google.com"

        key.iter_values.return_value = [val_default, val1]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "Software\\Microsoft\\Internet Explorer\\TypedURLs",
            "custom_handler": "typed_urls",
            "indicator": "browser:typed_url",
            "confidence": 0.85,
        }

        findings = []
        with patch("regipy.registry.RegistryHive", side_effect=Exception("no hive"), create=True):
            _process_typed_urls(
                key, key_def, target, action, "NTUSER.DAT",
                findings, "Software\\Microsoft\\Internet Explorer\\TypedURLs"
            )

        assert len(findings) == 1
        assert findings[0].value == "https://www.google.com"


# =============================================================================
# RecentDocs Extension Handler Tests
# =============================================================================

class TestRecentDocsExtensionHandler:
    """Tests for the RecentDocs per-extension custom handler."""

    def test_recent_docs_extracts_filename(self):
        """Test extracting filename from binary MRU value."""
        key = MagicMock()
        key.name = ".jpg"
        key.header.last_modified = "2024-03-10 14:30:00"

        # Build binary value: UTF-16LE "photo.jpg" + null terminator + junk
        filename = "photo.jpg"
        binary_data = filename.encode("utf-16-le") + b'\x00\x00' + b'\xab\xcd\xef\x01' * 10

        val0 = Mock()
        val0.name = "0"
        val0.value = binary_data

        key.iter_values.return_value = [val0]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\RecentDocs\\.jpg",
            "custom_handler": "recent_docs_extension",
            "indicator": "recent_documents:image",
            "confidence": 0.75,
        }

        findings = []
        _process_recent_docs_extension(
            key, key_def, target, action, "NTUSER.DAT",
            findings, "Software\\Microsoft\\Windows\\CurrentVersion\\Explorer\\RecentDocs\\.jpg"
        )

        assert len(findings) == 1
        assert findings[0].name == "recent_documents:image"
        assert findings[0].value == "photo.jpg"

        extra = json.loads(findings[0].extra_json)
        assert extra["filename"] == "photo.jpg"
        assert extra["extension"] == ".jpg"
        assert extra["MRU_position"] == "0"

    def test_recent_docs_multiple_entries(self):
        """Test extracting multiple filenames from MRU list."""
        key = MagicMock()
        key.name = ".png"
        key.header.last_modified = "2024-03-10 14:30:00"

        def make_binary(filename):
            return filename.encode("utf-16-le") + b'\x00\x00' + b'\x01\x02' * 5

        val0 = Mock()
        val0.name = "0"
        val0.value = make_binary("screenshot.png")

        val1 = Mock()
        val1.name = "1"
        val1.value = make_binary("diagram.png")

        # MRUListEx should be skipped
        val_mru = Mock()
        val_mru.name = "MRUListEx"
        val_mru.value = b'\x00\x00\x00\x00\x01\x00\x00\x00'

        key.iter_values.return_value = [val0, val1, val_mru]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\.png",
            "custom_handler": "recent_docs_extension",
            "indicator": "recent_documents:image",
            "confidence": 0.75,
        }

        findings = []
        _process_recent_docs_extension(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\.png"
        )

        assert len(findings) == 2
        assert findings[0].value == "screenshot.png"
        assert findings[1].value == "diagram.png"

    def test_recent_docs_skips_short_binary(self):
        """Test that values with too-short binary data are skipped."""
        key = MagicMock()
        key.name = ".jpg"
        key.header.last_modified = "2024-03-10 14:30:00"

        val0 = Mock()
        val0.name = "0"
        val0.value = b'\x00\x01'  # Too short

        key.iter_values.return_value = [val0]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\.jpg",
            "custom_handler": "recent_docs_extension",
            "indicator": "recent_documents:image",
        }

        findings = []
        _process_recent_docs_extension(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\.jpg"
        )

        assert len(findings) == 0


class TestExtractUtf16leFilename:
    """Tests for the UTF-16LE filename extraction helper."""

    def test_basic_filename(self):
        """Test extracting a basic filename."""
        data = "test.jpg".encode("utf-16-le") + b'\x00\x00' + b'\xff' * 10
        assert _extract_utf16le_filename(data) == "test.jpg"

    def test_unicode_filename(self):
        """Test extracting a filename with unicode characters."""
        data = "photo_2024.jpg".encode("utf-16-le") + b'\x00\x00'
        assert _extract_utf16le_filename(data) == "photo_2024.jpg"

    def test_empty_data_returns_none(self):
        """Test that empty data at start returns None."""
        data = b'\x00\x00' + b'\xff' * 10
        assert _extract_utf16le_filename(data) is None

    def test_no_null_terminator_returns_none(self):
        """Test that data without null terminator returns None."""
        # All non-zero bytes, no null pair on 2-byte boundary
        data = b'\x41\x00\x42\x00\x43\x00'  # "ABC" in UTF-16LE, no terminator
        # At i=0: data[0]=0x41, data[1]=0x00 -> not both zero
        # At i=2: data[2]=0x42, data[3]=0x00 -> not both zero
        # At i=4: data[4]=0x43, data[5]=0x00 -> not both zero
        # No null terminator found
        assert _extract_utf16le_filename(data) is None


# =============================================================================
# WordWheelQuery Handler Tests
# =============================================================================

class TestWordWheelQueryHandler:
    """Tests for the WordWheelQuery custom handler."""

    def test_word_wheel_query_basic(self):
        """Test decoding UTF-16LE search terms."""
        key = MagicMock()
        key.name = "WordWheelQuery"
        key.header.last_modified = "2024-06-20 08:15:00"

        val0 = Mock()
        val0.name = "0"
        val0.value = "secret photos".encode("utf-16-le") + b'\x00\x00'

        val1 = Mock()
        val1.name = "1"
        val1.value = "bank statement".encode("utf-16-le") + b'\x00\x00'

        key.iter_values.return_value = [val0, val1]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\WordWheelQuery",
            "custom_handler": "word_wheel_query",
            "indicator": "user_activity:explorer_search",
            "confidence": 0.80,
        }

        findings = []
        _process_word_wheel_query(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\WordWheelQuery"
        )

        assert len(findings) == 2
        assert findings[0].name == "user_activity:explorer_search"
        assert findings[0].value == "secret photos"
        assert findings[1].value == "bank statement"

        extra = json.loads(findings[0].extra_json)
        assert extra["search_term"] == "secret photos"
        assert extra["MRU_position"] == "0"

    def test_word_wheel_query_skips_mrulistex(self):
        """Test that MRUListEx value is skipped."""
        key = MagicMock()
        key.name = "WordWheelQuery"
        key.header.last_modified = "2024-06-20 08:15:00"

        val_mru = Mock()
        val_mru.name = "MRUListEx"
        val_mru.value = b'\x01\x00\x00\x00\x00\x00\x00\x00'

        val0 = Mock()
        val0.name = "0"
        val0.value = "test search".encode("utf-16-le") + b'\x00\x00'

        key.iter_values.return_value = [val_mru, val0]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\WordWheelQuery",
            "custom_handler": "word_wheel_query",
            "indicator": "user_activity:explorer_search",
        }

        findings = []
        _process_word_wheel_query(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\WordWheelQuery"
        )

        assert len(findings) == 1
        assert findings[0].value == "test search"

    def test_word_wheel_query_string_values(self):
        """Test handling of string values (already decoded by regipy)."""
        key = MagicMock()
        key.name = "WordWheelQuery"
        key.header.last_modified = "2024-06-20 08:15:00"

        val0 = Mock()
        val0.name = "0"
        val0.value = "already a string"

        key.iter_values.return_value = [val0]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\WordWheelQuery",
            "custom_handler": "word_wheel_query",
            "indicator": "user_activity:explorer_search",
        }

        findings = []
        _process_word_wheel_query(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\WordWheelQuery"
        )

        assert len(findings) == 1
        assert findings[0].value == "already a string"


# =============================================================================
# UserAssist Handler Tests
# =============================================================================

class TestUserAssistHandler:
    """Tests for the UserAssist custom handler."""

    def _make_v5_data(self, run_count=5, focus_count=10, focus_time_ms=30000, last_run_filetime=0):
        """Build a 72-byte version 5 UserAssist data blob."""
        data = bytearray(72)
        struct.pack_into('<I', data, 4, run_count)
        struct.pack_into('<I', data, 8, focus_count)
        struct.pack_into('<I', data, 12, focus_time_ms)
        struct.pack_into('<Q', data, 60, last_run_filetime)
        return bytes(data)

    def _make_v3_data(self, run_count=10, last_run_filetime=0):
        """Build a 16-byte version 3 (XP) UserAssist data blob."""
        data = bytearray(16)
        # For XP, raw count + 5 is stored
        struct.pack_into('<I', data, 4, run_count + 5)
        struct.pack_into('<Q', data, 8, last_run_filetime)
        return bytes(data)

    def test_user_assist_v5_basic(self):
        """Test parsing version 5 (Win7+) UserAssist data."""
        key = MagicMock()
        key.name = "Count"
        key.header.last_modified = "2024-08-01 12:00:00"

        # ROT13 of "C:\\Program Files\\Google\\Chrome\\chrome.exe"
        original_path = "C:\\Program Files\\Google\\Chrome\\chrome.exe"
        rot13_name = codecs.encode(original_path, "rot_13")

        # Known FILETIME for 2024-07-29 22:09:26 UTC
        filetime_int = 133667645663557812

        val = Mock()
        val.name = rot13_name
        val.value = self._make_v5_data(
            run_count=42,
            focus_count=15,
            focus_time_ms=120000,
            last_run_filetime=filetime_int,
        )

        key.iter_values.return_value = [val]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\UserAssist\\*\\Count",
            "custom_handler": "user_assist",
            "indicator": "execution:user_assist",
            "confidence": 0.90,
        }

        findings = []
        _process_user_assist(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\UserAssist\\{GUID}\\Count"
        )

        assert len(findings) == 1
        assert findings[0].name == "execution:user_assist"

        extra = json.loads(findings[0].extra_json)
        assert extra["decoded_path"] == original_path
        assert extra["rot13_name"] == rot13_name
        assert extra["run_count"] == 42
        assert extra["focus_count"] == 15
        assert extra["focus_time_ms"] == 120000
        assert "last_run_utc" in extra
        assert "2024-07-29" in extra["last_run_utc"]

    def test_user_assist_v3_xp(self):
        """Test parsing version 3 (Windows XP) UserAssist data."""
        key = MagicMock()
        key.name = "Count"
        key.header.last_modified = "2010-06-15 09:00:00"

        original_path = "C:\\Windows\\notepad.exe"
        rot13_name = codecs.encode(original_path, "rot_13")

        filetime_int = 133667645663557812  # Some known time

        val = Mock()
        val.name = rot13_name
        val.value = self._make_v3_data(run_count=7, last_run_filetime=filetime_int)

        key.iter_values.return_value = [val]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\UserAssist\\*\\Count",
            "custom_handler": "user_assist",
            "indicator": "execution:user_assist",
            "confidence": 0.90,
        }

        findings = []
        _process_user_assist(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\UserAssist\\{GUID}\\Count"
        )

        assert len(findings) == 1
        extra = json.loads(findings[0].extra_json)
        assert extra["decoded_path"] == original_path
        assert extra["run_count"] == 7  # stored as 12 (7+5), corrected to 7

    def test_user_assist_forensic_browser(self):
        """Test that browser paths are flagged as forensic interest."""
        key = MagicMock()
        key.name = "Count"
        key.header.last_modified = "2024-08-01 12:00:00"

        original_path = "C:\\Program Files\\Mozilla Firefox\\firefox.exe"
        rot13_name = codecs.encode(original_path, "rot_13")

        val = Mock()
        val.name = rot13_name
        val.value = self._make_v5_data(run_count=100)

        key.iter_values.return_value = [val]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\UserAssist\\*\\Count",
            "custom_handler": "user_assist",
            "indicator": "execution:user_assist",
            "confidence": 0.90,
        }

        findings = []
        _process_user_assist(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\UserAssist\\{GUID}\\Count"
        )

        assert len(findings) == 1
        extra = json.loads(findings[0].extra_json)
        assert extra["forensic_interest"] is True
        assert extra["forensic_category"] == "browser"

    def test_user_assist_forensic_wiping_tool(self):
        """Test that wiping tool paths are flagged."""
        key = MagicMock()
        key.name = "Count"
        key.header.last_modified = "2024-08-01 12:00:00"

        original_path = "C:\\Program Files\\CCleaner\\CCleaner64.exe"
        rot13_name = codecs.encode(original_path, "rot_13")

        val = Mock()
        val.name = rot13_name
        val.value = self._make_v5_data(run_count=3)

        key.iter_values.return_value = [val]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\UserAssist\\*\\Count",
            "custom_handler": "user_assist",
            "indicator": "execution:user_assist",
        }

        findings = []
        _process_user_assist(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\UserAssist\\{GUID}\\Count"
        )

        assert len(findings) == 1
        extra = json.loads(findings[0].extra_json)
        assert extra["forensic_interest"] is True
        assert extra["forensic_category"] == "wiping_tool"

    def test_user_assist_forensic_tor(self):
        """Test that Tor Browser paths are flagged."""
        key = MagicMock()
        key.name = "Count"
        key.header.last_modified = "2024-08-01 12:00:00"

        original_path = "C:\\Users\\Suspect\\Desktop\\Tor Browser\\Browser\\firefox.exe"
        rot13_name = codecs.encode(original_path, "rot_13")

        val = Mock()
        val.name = rot13_name
        val.value = self._make_v5_data(run_count=50)

        key.iter_values.return_value = [val]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\UserAssist\\*\\Count",
            "custom_handler": "user_assist",
            "indicator": "execution:user_assist",
        }

        findings = []
        _process_user_assist(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\UserAssist\\{GUID}\\Count"
        )

        assert len(findings) == 1
        extra = json.loads(findings[0].extra_json)
        assert extra["forensic_interest"] is True
        assert extra["forensic_category"] == "tor"

    def test_user_assist_skips_default_and_ueme(self):
        """Test that (Default) and UEME_* values are skipped.

        Note: In real registry hives, UserAssist value names are ROT13-encoded.
        UEME_CTLSESSION encodes to HRZR_PGYFRFFVBA. We filter on the encoded
        prefix 'HRZR_' since that's what appears in the raw registry data.
        """
        key = MagicMock()
        key.name = "Count"
        key.header.last_modified = "2024-08-01 12:00:00"

        val_default = Mock()
        val_default.name = "(Default)"
        val_default.value = b''

        # ROT13 of "UEME_CTLSESSION" -> "HRZR_PGYFRFFVBA"
        val_ueme = Mock()
        val_ueme.name = "HRZR_PGYFRFFVBA"
        val_ueme.value = b'\x00' * 72

        original_path = "C:\\Windows\\notepad.exe"
        rot13_name = codecs.encode(original_path, "rot_13")
        val_real = Mock()
        val_real.name = rot13_name
        val_real.value = self._make_v5_data(run_count=1)

        key.iter_values.return_value = [val_default, val_ueme, val_real]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\UserAssist\\*\\Count",
            "custom_handler": "user_assist",
            "indicator": "execution:user_assist",
        }

        findings = []
        _process_user_assist(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\UserAssist\\{GUID}\\Count"
        )

        assert len(findings) == 1
        extra = json.loads(findings[0].extra_json)
        assert extra["decoded_path"] == original_path

    def test_user_assist_skips_short_data(self):
        """Test that values with too-short data are skipped."""
        key = MagicMock()
        key.name = "Count"
        key.header.last_modified = "2024-08-01 12:00:00"

        rot13_name = codecs.encode("C:\\short.exe", "rot_13")
        val = Mock()
        val.name = rot13_name
        val.value = b'\x00' * 8  # Only 8 bytes, too short for any version

        key.iter_values.return_value = [val]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\UserAssist\\*\\Count",
            "custom_handler": "user_assist",
            "indicator": "execution:user_assist",
        }

        findings = []
        _process_user_assist(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\UserAssist\\{GUID}\\Count"
        )

        assert len(findings) == 0

    def test_user_assist_non_bytes_skipped(self):
        """Test that non-hex-string, non-bytes values are skipped."""
        key = MagicMock()
        key.name = "Count"
        key.header.last_modified = "2024-08-01 12:00:00"

        rot13_name = codecs.encode("C:\\test.exe", "rot_13")
        val = Mock()
        val.name = rot13_name
        val.value = "not valid hex"

        key.iter_values.return_value = [val]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\UserAssist\\*\\Count",
            "custom_handler": "user_assist",
            "indicator": "execution:user_assist",
        }

        findings = []
        _process_user_assist(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\UserAssist\\{GUID}\\Count"
        )

        assert len(findings) == 0

    def test_user_assist_hex_string_values(self):
        """Test that UserAssist handles hex-encoded string values from regipy.

        Regipy may return binary registry data as hex-encoded strings instead of
        raw bytes. The handler must convert these to bytes before parsing.
        """
        key = MagicMock()
        key.name = "Count"
        key.header.last_modified = "2024-08-01 12:00:00"

        original_path = "C:\\Windows\\notepad.exe"
        rot13_name = codecs.encode(original_path, "rot_13")
        val = Mock()
        val.name = rot13_name
        # Create V5 data as hex string (how regipy actually returns it)
        v5_data = self._make_v5_data(run_count=5, focus_count=10, focus_time_ms=30000)
        val.value = v5_data.hex()  # Hex-encoded string

        key.iter_values.return_value = [val]

        target = {"name": "user_activity"}
        action = {"provenance": "registry_user_activity"}
        key_def = {
            "path": "...\\UserAssist\\*\\Count",
            "custom_handler": "user_assist",
            "indicator": "execution:user_assist",
            "confidence": 0.90,
        }

        findings = []
        _process_user_assist(
            key, key_def, target, action, "NTUSER.DAT", findings, "...\\UserAssist\\{GUID}\\Count"
        )

        assert len(findings) == 1
        extra = json.loads(findings[0].extra_json)
        assert extra["decoded_path"] == original_path
        assert extra["run_count"] == 5
        assert extra["focus_count"] == 10
        assert extra["focus_time_ms"] == 30000


# =============================================================================
# Wildcard Path Resolution Tests
# =============================================================================

class TestWildcardPathResolution:
    """Tests for _resolve_wildcard_path and _expand_wildcard_parts."""

    def test_no_wildcard_resolves_directly(self):
        """Test that a path without wildcards resolves to a single key."""
        from extractors.system.registry.parser import _resolve_wildcard_path

        # Build a mock hive with root -> A -> B
        mock_b = MagicMock()
        mock_b.name = "B"
        mock_a = MagicMock()
        mock_a.name = "A"
        mock_a.iter_subkeys.return_value = [mock_b]
        mock_root = MagicMock()
        mock_root.iter_subkeys.return_value = [mock_a]

        mock_hive = MagicMock()
        mock_hive.root = mock_root
        mock_hive.get_key.return_value = mock_b

        results = _resolve_wildcard_path(mock_hive, "A\\B")
        assert len(results) == 1
        assert results[0][0] == "A\\B"

    def test_trailing_wildcard_expands_subkeys(self):
        """Test that a trailing wildcard expands to all subkeys."""
        from extractors.system.registry.parser import _resolve_wildcard_path

        # Root -> Parent -> (Child1, Child2)
        child1 = MagicMock()
        child1.name = "Child1"
        child1.iter_subkeys.return_value = []
        child2 = MagicMock()
        child2.name = "Child2"
        child2.iter_subkeys.return_value = []
        parent = MagicMock()
        parent.name = "Parent"
        parent.iter_subkeys.return_value = [child1, child2]
        root = MagicMock()
        root.iter_subkeys.return_value = [parent]

        mock_hive = MagicMock()
        mock_hive.root = root

        results = _resolve_wildcard_path(mock_hive, "Parent\\*")
        assert len(results) == 2
        paths = [r[0] for r in results]
        assert "Parent\\Child1" in paths
        assert "Parent\\Child2" in paths

    def test_mid_path_wildcard_expands_correctly(self):
        """Test UserAssist-style pattern: Parent\\*\\Count."""
        from extractors.system.registry.parser import _resolve_wildcard_path

        # Root -> Parent -> (GUID1 -> Count, GUID2 -> Count)
        count1 = MagicMock()
        count1.name = "Count"
        count1.iter_subkeys.return_value = []
        guid1 = MagicMock()
        guid1.name = "{GUID1}"
        guid1.iter_subkeys.return_value = [count1]

        count2 = MagicMock()
        count2.name = "Count"
        count2.iter_subkeys.return_value = []
        guid2 = MagicMock()
        guid2.name = "{GUID2}"
        guid2.iter_subkeys.return_value = [count2]

        parent = MagicMock()
        parent.name = "Parent"
        parent.iter_subkeys.return_value = [guid1, guid2]
        root = MagicMock()
        root.iter_subkeys.return_value = [parent]

        mock_hive = MagicMock()
        mock_hive.root = root

        results = _resolve_wildcard_path(mock_hive, "Parent\\*\\Count")
        assert len(results) == 2
        paths = [r[0] for r in results]
        assert "Parent\\{GUID1}\\Count" in paths
        assert "Parent\\{GUID2}\\Count" in paths

    def test_wildcard_skips_missing_subpath(self):
        """Test that wildcard expansion skips GUIDs that don't have the subpath."""
        from extractors.system.registry.parser import _resolve_wildcard_path

        # Root -> Parent -> (GUID1 -> Count, GUID2 -> NoCount)
        count1 = MagicMock()
        count1.name = "Count"
        count1.iter_subkeys.return_value = []
        guid1 = MagicMock()
        guid1.name = "{GUID1}"
        guid1.iter_subkeys.return_value = [count1]

        no_count = MagicMock()
        no_count.name = "Settings"
        guid2 = MagicMock()
        guid2.name = "{GUID2}"
        guid2.iter_subkeys.return_value = [no_count]

        parent = MagicMock()
        parent.name = "Parent"
        parent.iter_subkeys.return_value = [guid1, guid2]
        root = MagicMock()
        root.iter_subkeys.return_value = [parent]

        mock_hive = MagicMock()
        mock_hive.root = root

        results = _resolve_wildcard_path(mock_hive, "Parent\\*\\Count")
        assert len(results) == 1
        assert results[0][0] == "Parent\\{GUID1}\\Count"