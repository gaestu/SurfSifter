"""
Tests for src/extractors/_shared/ utilities.

Tests cover:
- timestamps.py: WebKit, PRTime, Unix, Cocoa timestamp conversions
- sqlite_helpers.py: Safe read-only SQLite access
- path_utils.py: Glob patterns, profile enumeration, path normalization
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

from extractors._shared import (
    # Timestamps
    webkit_to_datetime,
    webkit_to_iso,
    prtime_to_datetime,
    prtime_to_iso,
    unix_to_datetime,
    unix_to_iso,
    cocoa_to_datetime,
    cocoa_to_iso,
    WEBKIT_EPOCH_DIFF,
    COCOA_EPOCH_DIFF,
    # SQLite helpers
    safe_sqlite_connect,
    safe_execute,
    copy_sqlite_for_reading,
    SQLiteReadError,
    get_table_names,
    table_exists,
    get_row_count,
    # Path utils
    expand_windows_env_vars,
    glob_pattern_to_regex,
    find_matching_paths,
    enumerate_browser_profiles,
    normalize_evidence_path,
    extract_username_from_path,
    WINDOWS_ENV_DEFAULTS,
)


# =============================================================================
# Timestamp Tests
# =============================================================================

class TestWebkitTimestamps:
    """Tests for WebKit (Chromium) timestamp conversion."""

    def test_webkit_to_datetime_known_value(self):
        """Test conversion of known WebKit timestamp."""
        # 2020-01-01 00:00:00 UTC in WebKit format
        # WebKit epoch: 1601-01-01, microseconds
        webkit_ts = 13222310400000000  # 2020-01-01 00:00:00 UTC
        result = webkit_to_datetime(webkit_ts)
        assert result.year == 2020
        assert result.month == 1
        assert result.day == 1
        assert result.tzinfo == timezone.utc

    def test_webkit_to_datetime_zero(self):
        """Test WebKit timestamp of 0 returns None (invalid)."""
        result = webkit_to_datetime(0)
        assert result is None

    def test_webkit_to_datetime_none(self):
        """Test None input returns None."""
        result = webkit_to_datetime(None)
        assert result is None

    def test_webkit_to_iso_format(self):
        """Test WebKit timestamp to ISO string conversion."""
        webkit_ts = 13222310400000000  # 2020-01-01 00:00:00 UTC
        result = webkit_to_iso(webkit_ts)
        assert "2020-01-01" in result

    def test_webkit_epoch_diff_constant(self):
        """Verify WebKit epoch difference is correct."""
        # Seconds from 1601-01-01 to 1970-01-01
        assert WEBKIT_EPOCH_DIFF == 11644473600


class TestPrTimeTimestamps:
    """Tests for PRTime (Firefox) timestamp conversion."""

    def test_prtime_to_datetime_known_value(self):
        """Test conversion of known PRTime timestamp."""
        # PRTime is Unix microseconds
        # 2020-01-01 00:00:00 UTC = 1577836800 seconds
        prtime = 1577836800000000  # microseconds
        result = prtime_to_datetime(prtime)
        assert result.year == 2020
        assert result.month == 1
        assert result.day == 1
        assert result.tzinfo == timezone.utc

    def test_prtime_to_datetime_zero(self):
        """Test PRTime of 0 returns None."""
        result = prtime_to_datetime(0)
        assert result is None

    def test_prtime_to_iso_format(self):
        """Test PRTime to ISO string conversion."""
        prtime = 1577836800000000
        result = prtime_to_iso(prtime)
        assert "2020-01-01" in result


class TestUnixTimestamps:
    """Tests for Unix timestamp conversion."""

    def test_unix_to_datetime_known_value(self):
        """Test conversion of known Unix timestamp."""
        unix_ts = 1577836800  # 2020-01-01 00:00:00 UTC
        result = unix_to_datetime(unix_ts)
        assert result.year == 2020
        assert result.month == 1
        assert result.day == 1

    def test_unix_to_datetime_zero(self):
        """Test Unix timestamp of 0 returns None."""
        result = unix_to_datetime(0)
        assert result is None

    def test_unix_to_iso_format(self):
        """Test Unix to ISO string conversion."""
        unix_ts = 1577836800
        result = unix_to_iso(unix_ts)
        assert "2020-01-01" in result


class TestCocoaTimestamps:
    """Tests for Cocoa (Safari/macOS) timestamp conversion."""

    def test_cocoa_to_datetime_known_value(self):
        """Test conversion of known Cocoa timestamp."""
        # Cocoa epoch: 2001-01-01 00:00:00 UTC
        # 2020-01-01 = 599529600 seconds after Cocoa epoch
        cocoa_ts = 599529600.0
        result = cocoa_to_datetime(cocoa_ts)
        assert result.year == 2020
        assert result.month == 1
        assert result.day == 1

    def test_cocoa_to_datetime_zero(self):
        """Test Cocoa timestamp of 0 returns None."""
        result = cocoa_to_datetime(0)
        assert result is None

    def test_cocoa_epoch_diff_constant(self):
        """Verify Cocoa epoch difference is correct."""
        # Seconds from 1970-01-01 to 2001-01-01
        assert COCOA_EPOCH_DIFF == 978307200


# =============================================================================
# SQLite Helper Tests
# =============================================================================

class TestSafeSQLiteConnect:
    """Tests for safe SQLite connection helper."""

    def test_connect_read_only(self, tmp_path):
        """Test that connection is read-only."""
        db_path = tmp_path / "test.db"

        # Create a test database
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.execute("INSERT INTO test VALUES (1)")
        conn.commit()
        conn.close()

        # Connect read-only and try to write
        with safe_sqlite_connect(db_path) as conn:
            rows = conn.execute("SELECT * FROM test").fetchall()
            assert len(rows) == 1

            # Writing should fail (read-only)
            with pytest.raises(sqlite3.OperationalError):
                conn.execute("INSERT INTO test VALUES (2)")

    def test_connect_with_copy(self, tmp_path):
        """Test connection with copy_first option."""
        db_path = tmp_path / "test.db"

        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.execute("INSERT INTO test VALUES (1)")
        conn.commit()
        conn.close()

        with safe_sqlite_connect(db_path, copy_first=True) as conn:
            rows = conn.execute("SELECT * FROM test").fetchall()
            assert len(rows) == 1

    def test_connect_nonexistent_file(self, tmp_path):
        """Test that FileNotFoundError is raised for missing file."""
        with pytest.raises(FileNotFoundError):
            with safe_sqlite_connect(tmp_path / "nonexistent.db"):
                pass

    def test_connect_row_factory(self, tmp_path):
        """Test that Row factory is set for column access."""
        db_path = tmp_path / "test.db"

        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO test VALUES (1, 'test')")
        conn.commit()
        conn.close()

        with safe_sqlite_connect(db_path) as conn:
            row = conn.execute("SELECT * FROM test").fetchone()
            assert row["id"] == 1
            assert row["name"] == "test"


class TestCopySQLiteForReading:
    """Tests for SQLite copy helper."""

    def test_copy_basic(self, tmp_path):
        """Test basic database copy."""
        db_path = tmp_path / "source" / "test.db"
        db_path.parent.mkdir(parents=True)

        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.commit()
        conn.close()

        dest_dir = tmp_path / "dest"
        copied = copy_sqlite_for_reading(db_path, dest_dir=dest_dir)

        assert copied.exists()
        assert copied.name == "test.db"

    def test_copy_with_wal(self, tmp_path):
        """Test that WAL files are copied."""
        db_path = tmp_path / "test.db"

        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.commit()
        conn.close()

        # Create fake WAL file
        wal_path = tmp_path / "test.db-wal"
        wal_path.write_text("fake wal")

        dest_dir = tmp_path / "dest"
        copied = copy_sqlite_for_reading(db_path, include_wal=True, dest_dir=dest_dir)

        assert copied.exists()
        assert (dest_dir / "test.db-wal").exists()


class TestSafeExecute:
    """Tests for safe query execution."""

    def test_execute_basic(self, tmp_path):
        """Test basic query execution."""
        db_path = tmp_path / "test.db"

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO test VALUES (1, 'alice')")
        conn.execute("INSERT INTO test VALUES (2, 'bob')")
        conn.commit()

        rows = safe_execute(conn, "SELECT * FROM test ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["name"] == "alice"
        assert rows[1]["name"] == "bob"

        conn.close()

    def test_execute_with_params(self, tmp_path):
        """Test query with parameters."""
        db_path = tmp_path / "test.db"

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        conn.execute("INSERT INTO test VALUES (1, 'alice')")
        conn.execute("INSERT INTO test VALUES (2, 'bob')")
        conn.commit()

        rows = safe_execute(conn, "SELECT * FROM test WHERE id = ?", (1,))
        assert len(rows) == 1
        assert rows[0]["name"] == "alice"

        conn.close()


class TestTableHelpers:
    """Tests for table utility functions."""

    def test_get_table_names(self, tmp_path):
        """Test getting table names."""
        db_path = tmp_path / "test.db"

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE alpha (id INTEGER)")
        conn.execute("CREATE TABLE beta (id INTEGER)")
        conn.commit()

        names = get_table_names(conn)
        assert "alpha" in names
        assert "beta" in names

        conn.close()

    def test_table_exists(self, tmp_path):
        """Test table existence check."""
        db_path = tmp_path / "test.db"

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE existing (id INTEGER)")
        conn.commit()

        assert table_exists(conn, "existing")
        assert not table_exists(conn, "nonexistent")

        conn.close()

    def test_get_row_count(self, tmp_path):
        """Test row count."""
        db_path = tmp_path / "test.db"

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE test (id INTEGER)")
        conn.execute("INSERT INTO test VALUES (1)")
        conn.execute("INSERT INTO test VALUES (2)")
        conn.execute("INSERT INTO test VALUES (3)")
        conn.commit()

        count = get_row_count(conn, "test")
        assert count == 3

        conn.close()


# =============================================================================
# Path Utils Tests
# =============================================================================

class TestExpandWindowsEnvVars:
    """Tests for Windows environment variable expansion."""

    def test_expand_localappdata(self):
        """Test LOCALAPPDATA expansion."""
        result = expand_windows_env_vars(
            "%LOCALAPPDATA%/Google/Chrome",
            user_home="Users/john"
        )
        assert result == "Users/john/AppData/Local/Google/Chrome"

    def test_expand_appdata(self):
        """Test APPDATA expansion."""
        result = expand_windows_env_vars(
            "%APPDATA%/Mozilla/Firefox",
            user_home="Users/jane"
        )
        assert result == "Users/jane/AppData/Roaming/Mozilla/Firefox"

    def test_expand_userprofile(self):
        """Test USERPROFILE expansion."""
        result = expand_windows_env_vars(
            "%USERPROFILE%/Documents",
            user_home="Users/admin"
        )
        assert result == "Users/admin/Documents"

    def test_no_vars(self):
        """Test path without variables."""
        result = expand_windows_env_vars("Users/john/Documents")
        assert result == "Users/john/Documents"

    def test_backslash_normalization(self):
        """Test that backslashes are converted to forward slashes."""
        result = expand_windows_env_vars(
            "%LOCALAPPDATA%\\Google\\Chrome",
            user_home="Users/john"
        )
        assert "\\" not in result


class TestGlobPatternToRegex:
    """Tests for glob pattern to regex conversion."""

    def test_single_star(self):
        """Test single * matches non-slash characters."""
        regex = glob_pattern_to_regex("Users/*/AppData")
        assert regex.match("Users/john/AppData")
        assert not regex.match("Users/john/doe/AppData")  # * doesn't match /

    def test_double_star(self):
        """Test ** matches everything including slashes."""
        regex = glob_pattern_to_regex("Users/**/History")
        assert regex.match("Users/john/AppData/Local/Google/Chrome/Default/History")
        assert regex.match("Users/History")  # ** can match zero chars

    def test_question_mark(self):
        """Test ? matches single character."""
        regex = glob_pattern_to_regex("Profile?")
        assert regex.match("Profile1")
        assert regex.match("ProfileA")
        assert not regex.match("Profile10")

    def test_character_class(self):
        """Test [abc] character class."""
        regex = glob_pattern_to_regex("file[123].txt")
        assert regex.match("file1.txt")
        assert regex.match("file2.txt")
        assert not regex.match("file4.txt")


class TestNormalizeEvidencePath:
    """Tests for evidence path normalization."""

    def test_backslash_to_forward(self):
        """Test backslash conversion."""
        result = normalize_evidence_path("Users\\john\\Documents")
        assert result == "Users/john/Documents"

    def test_double_slash_removal(self):
        """Test double slash removal."""
        result = normalize_evidence_path("Users//john//Documents")
        assert result == "Users/john/Documents"

    def test_path_object(self):
        """Test Path object input."""
        result = normalize_evidence_path(Path("Users/john/Documents"))
        assert result == "Users/john/Documents"


class TestExtractUsernameFromPath:
    """Tests for username extraction."""

    def test_basic_users_path(self):
        """Test basic Users/username path."""
        result = extract_username_from_path("Users/john/Documents")
        assert result == "john"

    def test_full_path_with_drive(self):
        """Test full path with drive letter."""
        result = extract_username_from_path("C/Users/jane.doe/AppData")
        assert result == "jane.doe"

    def test_filter_public(self):
        """Test that Public user is filtered."""
        result = extract_username_from_path("Users/Public/Documents")
        assert result is None

    def test_filter_default(self):
        """Test that Default user is filtered."""
        result = extract_username_from_path("Users/Default/NTUSER.DAT")
        assert result is None

    def test_no_users_directory(self):
        """Test path without Users directory."""
        result = extract_username_from_path("Program Files/Application")
        assert result is None


class TestEnumerateBrowserProfiles:
    """Tests for browser profile enumeration."""

    def test_chromium_profiles(self, tmp_path):
        """Test enumeration of Chromium-style profiles."""
        user_data = tmp_path / "User Data"
        user_data.mkdir()

        # Create Chromium profile directories
        (user_data / "Default").mkdir()
        (user_data / "Profile 1").mkdir()
        (user_data / "Profile 2").mkdir()
        (user_data / "Safe Browsing").mkdir()  # Not a profile

        profiles = list(enumerate_browser_profiles(user_data))
        profile_names = {p.name for p in profiles}

        assert "Default" in profile_names
        assert "Profile 1" in profile_names
        assert "Profile 2" in profile_names
        assert "Safe Browsing" not in profile_names

    def test_custom_patterns(self, tmp_path):
        """Test with custom profile patterns."""
        user_data = tmp_path / "Profiles"
        user_data.mkdir()

        (user_data / "profile1").mkdir()
        (user_data / "profile2").mkdir()

        profiles = list(enumerate_browser_profiles(
            user_data,
            profile_patterns=["profile*"]
        ))

        assert len(profiles) == 2

    def test_nonexistent_directory(self, tmp_path):
        """Test with nonexistent directory returns empty."""
        profiles = list(enumerate_browser_profiles(tmp_path / "nonexistent"))
        assert len(profiles) == 0


class TestFindMatchingPaths:
    """Tests for path matching with globs."""

    def test_local_filesystem(self, tmp_path):
        """Test matching on local filesystem."""
        # Create test structure
        (tmp_path / "Users" / "john" / "AppData").mkdir(parents=True)
        (tmp_path / "Users" / "jane" / "AppData").mkdir(parents=True)
        (tmp_path / "Program Files").mkdir()

        matches = list(find_matching_paths(tmp_path, "Users/*/AppData"))
        assert len(matches) == 2

    def test_custom_file_lister(self, tmp_path):
        """Test with custom file lister function."""
        # Simulate evidence filesystem
        file_list = [
            "Users/john/AppData/Local/Google/Chrome/Default/History",
            "Users/john/AppData/Local/Microsoft/Edge/Default/History",
            "Windows/System32/config/SYSTEM",
        ]

        def mock_lister(root):
            return file_list

        matches = list(find_matching_paths(
            tmp_path,
            "Users/*/AppData/Local/*/Chrome/**",
            file_lister=mock_lister
        ))

        assert len(matches) == 1
        assert "Chrome" in str(matches[0])


# =============================================================================
# Module Import Tests
# =============================================================================

class TestModuleImports:
    """Tests to verify module structure and imports."""

    def test_timestamps_import(self):
        """Test that timestamps module is importable."""
        from extractors._shared import timestamps
        assert hasattr(timestamps, "webkit_to_datetime")

    def test_sqlite_helpers_import(self):
        """Test that sqlite_helpers module is importable."""
        from extractors._shared import sqlite_helpers
        assert hasattr(sqlite_helpers, "safe_sqlite_connect")

    def test_path_utils_import(self):
        """Test that path_utils module is importable."""
        from extractors._shared import path_utils
        assert hasattr(path_utils, "expand_windows_env_vars")

    def test_all_exports(self):
        """Test that __all__ exports are accessible."""
        import extractors._shared as shared

        for name in shared.__all__:
            assert hasattr(shared, name), f"Missing export: {name}"
