import json
import struct
from datetime import datetime, timezone
from unittest.mock import MagicMock, Mock
from pathlib import Path
from extractors.system.registry.parser import (
    _process_registry_key,
    RegistryFinding,
    filetime_to_datetime,
    unix_timestamp_to_datetime,
    systemtime_to_datetime,
    format_datetime,
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