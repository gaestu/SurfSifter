"""Tests for Firefox cookies extractor."""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extractors.browser.firefox.cookies import FirefoxCookiesExtractor
from extractors.browser.firefox._parsers import (
    parse_cookies,
    get_cookie_stats,
    FirefoxCookie,
    FirefoxCookieStats,
    _parse_origin_attributes,
)


# =============================================================================
# Parser Tests
# =============================================================================


class TestFirefoxCookieParsers:
    """Tests for Firefox cookie database parsers."""

    @pytest.fixture
    def cookies_db(self, tmp_path):
        """Create a mock cookies.sqlite database."""
        db_path = tmp_path / "cookies.sqlite"
        conn = sqlite3.connect(db_path)

        # Create schema (moz_cookies table)
        conn.executescript("""
            CREATE TABLE moz_cookies (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value TEXT,
                host TEXT NOT NULL,
                path TEXT DEFAULT '/',
                expiry INTEGER,
                isSecure INTEGER DEFAULT 0,
                isHttpOnly INTEGER DEFAULT 0,
                sameSite INTEGER DEFAULT 0,
                creationTime INTEGER,
                lastAccessed INTEGER
            );
        """)

        # Insert test data
        # PRTime is microseconds since 1970
        creation_time = 1704067200000000  # 2024-01-01 00:00:00 UTC

        conn.execute(
            """INSERT INTO moz_cookies
               (name, value, host, path, expiry, isSecure, isHttpOnly, sameSite, creationTime, lastAccessed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("session_id", "abc123", ".example.com", "/", 1735689600, 1, 1, 1, creation_time, creation_time)
        )
        conn.execute(
            """INSERT INTO moz_cookies
               (name, value, host, path, expiry, isSecure, isHttpOnly, sameSite, creationTime, lastAccessed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("user_pref", "dark_mode", ".test.org", "/app", 1735689600, 0, 0, 0, creation_time, creation_time)
        )

        conn.commit()
        conn.close()

        return db_path

    def test_parse_cookies_returns_cookies(self, cookies_db):
        """Test parse_cookies returns cookie records."""
        cookies = list(parse_cookies(cookies_db))

        assert len(cookies) == 2

    def test_parse_cookies_dataclass_fields(self, cookies_db):
        """Test parsed cookies have correct dataclass fields."""
        cookies = list(parse_cookies(cookies_db))
        cookie = cookies[0]

        assert isinstance(cookie, FirefoxCookie)
        assert hasattr(cookie, "name")
        assert hasattr(cookie, "value")
        assert hasattr(cookie, "domain")
        assert hasattr(cookie, "path")
        assert hasattr(cookie, "expires_utc")
        assert hasattr(cookie, "is_secure")
        assert hasattr(cookie, "is_httponly")
        assert hasattr(cookie, "samesite")
        assert hasattr(cookie, "encrypted")

    def test_parse_cookies_firefox_not_encrypted(self, cookies_db):
        """Test Firefox cookies are never encrypted."""
        cookies = list(parse_cookies(cookies_db))

        for cookie in cookies:
            assert cookie.encrypted is False

    def test_parse_cookies_secure_flag(self, cookies_db):
        """Test secure flag parsing."""
        cookies = list(parse_cookies(cookies_db))

        # First cookie is secure
        session_cookie = next(c for c in cookies if c.name == "session_id")
        assert session_cookie.is_secure is True

        # Second cookie is not secure
        pref_cookie = next(c for c in cookies if c.name == "user_pref")
        assert pref_cookie.is_secure is False

    def test_parse_cookies_samesite_mapping(self, cookies_db):
        """Test SameSite attribute mapping."""
        cookies = list(parse_cookies(cookies_db))

        # sameSite=1 → Lax
        session_cookie = next(c for c in cookies if c.name == "session_id")
        assert session_cookie.samesite == "Lax"

        # sameSite=0 → None
        pref_cookie = next(c for c in cookies if c.name == "user_pref")
        assert pref_cookie.samesite == "None"

    def test_parse_cookies_timestamp_conversion(self, cookies_db):
        """Test PRTime timestamps are converted to ISO 8601."""
        cookies = list(parse_cookies(cookies_db))
        cookie = cookies[0]

        # Creation time should be ISO format
        assert cookie.creation_utc is not None
        assert "2024-01-01" in cookie.creation_utc

    def test_parse_cookies_empty_db(self, tmp_path):
        """Test parse_cookies handles empty database."""
        db_path = tmp_path / "empty.sqlite"
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE moz_cookies (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value TEXT,
                host TEXT NOT NULL,
                path TEXT DEFAULT '/',
                expiry INTEGER,
                isSecure INTEGER DEFAULT 0,
                isHttpOnly INTEGER DEFAULT 0,
                sameSite INTEGER DEFAULT 0,
                creationTime INTEGER,
                lastAccessed INTEGER
            );
        """)
        conn.close()

        cookies = list(parse_cookies(db_path))
        assert cookies == []

    def test_parse_cookies_missing_table(self, tmp_path):
        """Test parse_cookies handles missing table."""
        db_path = tmp_path / "invalid.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE other (id INTEGER)")
        conn.close()

        cookies = list(parse_cookies(db_path))
        assert cookies == []

    def test_get_cookie_stats(self, cookies_db):
        """Test get_cookie_stats returns correct counts."""
        stats = get_cookie_stats(cookies_db)

        assert isinstance(stats, FirefoxCookieStats)
        assert stats.cookie_count == 2
        assert stats.unique_domains == 2
        assert stats.secure_count == 1
        assert stats.httponly_count == 1


# =============================================================================
# Origin Attributes Tests
# =============================================================================


class TestFirefoxOriginAttributes:
    """Tests for Firefox originAttributes parsing (containers, private browsing, FPI)."""

    def test_parse_origin_attributes_empty(self):
        """Test parsing empty originAttributes."""
        result = _parse_origin_attributes(None)

        assert result["user_context_id"] is None
        assert result["private_browsing_id"] is None
        assert result["first_party_domain"] is None
        assert result["partition_key"] is None

        result = _parse_origin_attributes("")
        assert result["user_context_id"] is None

    def test_parse_origin_attributes_caret_only(self):
        """Test parsing just caret prefix."""
        result = _parse_origin_attributes("^")

        assert result["user_context_id"] is None
        assert result["private_browsing_id"] is None

    def test_parse_origin_attributes_container(self):
        """Test parsing container tab ID (userContextId)."""
        # Container tab (e.g., Personal container)
        result = _parse_origin_attributes("^userContextId=1")
        assert result["user_context_id"] == 1

        # Default container
        result = _parse_origin_attributes("^userContextId=0")
        assert result["user_context_id"] == 0

        # Banking container
        result = _parse_origin_attributes("^userContextId=3")
        assert result["user_context_id"] == 3

    def test_parse_origin_attributes_private_browsing(self):
        """Test parsing private browsing indicator."""
        result = _parse_origin_attributes("^privateBrowsingId=1")
        assert result["private_browsing_id"] == 1

        result = _parse_origin_attributes("^privateBrowsingId=0")
        assert result["private_browsing_id"] == 0

    def test_parse_origin_attributes_first_party_domain(self):
        """Test parsing First-Party Isolation domain."""
        result = _parse_origin_attributes("^firstPartyDomain=example.com")
        assert result["first_party_domain"] == "example.com"

        result = _parse_origin_attributes("^firstPartyDomain=sub.example.com")
        assert result["first_party_domain"] == "sub.example.com"

    def test_parse_origin_attributes_partition_key(self):
        """Test parsing State Partitioning key."""
        result = _parse_origin_attributes("^partitionKey=(https,example.com)")
        assert result["partition_key"] == "(https,example.com)"

        result = _parse_origin_attributes("^partitionKey=(https,site.org,8443)")
        assert result["partition_key"] == "(https,site.org,8443)"

    def test_parse_origin_attributes_combined(self):
        """Test parsing combined attributes."""
        # Container + private browsing
        result = _parse_origin_attributes("^userContextId=2&privateBrowsingId=0")
        assert result["user_context_id"] == 2
        assert result["private_browsing_id"] == 0

        # Container + FPI
        result = _parse_origin_attributes("^userContextId=1&firstPartyDomain=tracker.com")
        assert result["user_context_id"] == 1
        assert result["first_party_domain"] == "tracker.com"

        # All four attributes
        result = _parse_origin_attributes(
            "^userContextId=1&privateBrowsingId=0&firstPartyDomain=example.com&partitionKey=(https,example.com)"
        )
        assert result["user_context_id"] == 1
        assert result["private_browsing_id"] == 0
        assert result["first_party_domain"] == "example.com"
        assert result["partition_key"] == "(https,example.com)"

    def test_parse_origin_attributes_invalid_values(self):
        """Test parsing handles invalid values gracefully."""
        # Non-integer userContextId
        result = _parse_origin_attributes("^userContextId=abc")
        assert result["user_context_id"] is None

        # Malformed pair
        result = _parse_origin_attributes("^invalidattr")
        assert result["user_context_id"] is None

    @pytest.fixture
    def cookies_db_with_origin_attrs(self, tmp_path):
        """Create cookies.sqlite with originAttributes column."""
        db_path = tmp_path / "cookies.sqlite"
        conn = sqlite3.connect(db_path)

        conn.executescript("""
            CREATE TABLE moz_cookies (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value TEXT,
                host TEXT NOT NULL,
                path TEXT DEFAULT '/',
                expiry INTEGER,
                isSecure INTEGER DEFAULT 0,
                isHttpOnly INTEGER DEFAULT 0,
                sameSite INTEGER DEFAULT 0,
                creationTime INTEGER,
                lastAccessed INTEGER,
                originAttributes TEXT
            );
        """)

        creation_time = 1704067200000000  # 2024-01-01 00:00:00 UTC

        # Default container cookie (no originAttributes)
        conn.execute(
            """INSERT INTO moz_cookies
               (name, value, host, path, expiry, isSecure, isHttpOnly, sameSite, creationTime, lastAccessed, originAttributes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("default_cookie", "value1", ".example.com", "/", 1735689600, 0, 0, 0, creation_time, creation_time, "")
        )

        # Personal container cookie
        conn.execute(
            """INSERT INTO moz_cookies
               (name, value, host, path, expiry, isSecure, isHttpOnly, sameSite, creationTime, lastAccessed, originAttributes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("container_cookie", "value2", ".example.com", "/", 1735689600, 1, 1, 2, creation_time, creation_time, "^userContextId=1")
        )

        # Private browsing cookie
        conn.execute(
            """INSERT INTO moz_cookies
               (name, value, host, path, expiry, isSecure, isHttpOnly, sameSite, creationTime, lastAccessed, originAttributes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("private_cookie", "value3", ".test.org", "/", 1735689600, 1, 1, 1, creation_time, creation_time, "^privateBrowsingId=1")
        )

        # FPI cookie with partition key
        conn.execute(
            """INSERT INTO moz_cookies
               (name, value, host, path, expiry, isSecure, isHttpOnly, sameSite, creationTime, lastAccessed, originAttributes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("fpi_cookie", "value4", ".tracker.com", "/", 1735689600, 1, 0, 0, creation_time, creation_time,
             "^firstPartyDomain=example.com&partitionKey=(https,example.com)")
        )

        conn.commit()
        conn.close()

        return db_path

    def test_parse_cookies_with_origin_attrs(self, cookies_db_with_origin_attrs):
        """Test parse_cookies extracts originAttributes."""
        cookies = list(parse_cookies(cookies_db_with_origin_attrs))

        assert len(cookies) == 4

        # Check default cookie has no origin context
        default = next(c for c in cookies if c.name == "default_cookie")
        assert default.origin_attributes == ""
        assert default.user_context_id is None

        # Check container cookie
        container = next(c for c in cookies if c.name == "container_cookie")
        assert container.origin_attributes == "^userContextId=1"
        assert container.user_context_id == 1
        assert container.private_browsing_id is None

        # Check private browsing cookie
        private = next(c for c in cookies if c.name == "private_cookie")
        assert private.origin_attributes == "^privateBrowsingId=1"
        assert private.private_browsing_id == 1
        assert private.user_context_id is None

        # Check FPI cookie
        fpi = next(c for c in cookies if c.name == "fpi_cookie")
        assert fpi.first_party_domain == "example.com"
        assert fpi.partition_key == "(https,example.com)"

    def test_parse_cookies_dataclass_has_origin_fields(self, cookies_db_with_origin_attrs):
        """Test FirefoxCookie dataclass has all originAttributes fields."""
        cookies = list(parse_cookies(cookies_db_with_origin_attrs))
        cookie = cookies[0]

        assert hasattr(cookie, "origin_attributes")
        assert hasattr(cookie, "user_context_id")
        assert hasattr(cookie, "private_browsing_id")
        assert hasattr(cookie, "first_party_domain")
        assert hasattr(cookie, "partition_key")
        assert hasattr(cookie, "samesite_raw")

    def test_parse_cookies_samesite_raw_preserved(self, cookies_db_with_origin_attrs):
        """Test samesite_raw preserves original integer value."""
        cookies = list(parse_cookies(cookies_db_with_origin_attrs))

        # sameSite=2 → Strict, raw should be 2
        container = next(c for c in cookies if c.name == "container_cookie")
        assert container.samesite == "Strict"
        assert container.samesite_raw == 2

        # sameSite=0 → None, raw should be 0
        default = next(c for c in cookies if c.name == "default_cookie")
        assert default.samesite == "None"
        assert default.samesite_raw == 0

    @pytest.fixture
    def cookies_db_unknown_samesite(self, tmp_path):
        """Create cookies.sqlite with unknown sameSite value."""
        db_path = tmp_path / "cookies.sqlite"
        conn = sqlite3.connect(db_path)

        conn.executescript("""
            CREATE TABLE moz_cookies (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value TEXT,
                host TEXT NOT NULL,
                path TEXT DEFAULT '/',
                expiry INTEGER,
                isSecure INTEGER DEFAULT 0,
                isHttpOnly INTEGER DEFAULT 0,
                sameSite INTEGER DEFAULT 0,
                creationTime INTEGER,
                lastAccessed INTEGER
            );
        """)

        creation_time = 1704067200000000

        # Cookie with unknown sameSite value (e.g., future Firefox version)
        conn.execute(
            """INSERT INTO moz_cookies
               (name, value, host, path, expiry, isSecure, isHttpOnly, sameSite, creationTime, lastAccessed)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("future_cookie", "value", ".example.com", "/", 1735689600, 0, 0, 99, creation_time, creation_time)
        )

        conn.commit()
        conn.close()

        return db_path

    def test_parse_cookies_unknown_samesite_preserved(self, cookies_db_unknown_samesite):
        """Test unknown sameSite values are preserved with descriptive string."""
        cookies = list(parse_cookies(cookies_db_unknown_samesite))
        cookie = cookies[0]

        # Unknown value should create descriptive string
        assert cookie.samesite == "unknown_99"
        assert cookie.samesite_raw == 99


# =============================================================================
# Extractor Tests
# =============================================================================


class TestFirefoxCookiesExtractor:
    """Tests for FirefoxCookiesExtractor class."""

    def test_extractor_metadata(self):
        """Test extractor has correct metadata."""
        extractor = FirefoxCookiesExtractor()
        meta = extractor.metadata

        assert meta.name == "firefox_cookies"
        assert meta.display_name == "Firefox Cookies"
        assert meta.category == "browser"
        assert meta.version
        assert "." in meta.version
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_with_fs(self):
        """Test can_run_extraction returns True with filesystem."""
        extractor = FirefoxCookiesExtractor()
        mock_fs = MagicMock()

        can_run, reason = extractor.can_run_extraction(mock_fs)

        assert can_run is True
        assert reason == ""

    def test_can_run_extraction_without_fs(self):
        """Test can_run_extraction returns False without filesystem."""
        extractor = FirefoxCookiesExtractor()

        can_run, reason = extractor.can_run_extraction(None)

        assert can_run is False
        assert "No evidence filesystem" in reason

    def test_get_output_dir(self, tmp_path):
        """Test get_output_dir returns correct path."""
        extractor = FirefoxCookiesExtractor()

        output_dir = extractor.get_output_dir(tmp_path, "evidence_001")

        assert output_dir == tmp_path / "evidences" / "evidence_001" / "firefox_cookies"

    def test_run_extraction_creates_manifest(self, tmp_path):
        """Test extraction creates manifest.json."""
        extractor = FirefoxCookiesExtractor()
        output_dir = tmp_path / "output"

        mock_fs = MagicMock()
        mock_fs.iter_paths.return_value = []
        mock_fs.source_path = "/test/image.e01"
        mock_fs.fs_type = "NTFS"

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        with patch.object(extractor, '_get_statistics_collector', return_value=None):
            result = extractor.run_extraction(
                mock_fs,
                output_dir,
                {"evidence_id": 1},
                callbacks
            )

        assert result is True
        assert (output_dir / "manifest.json").exists()

        manifest = json.loads((output_dir / "manifest.json").read_text())
        assert manifest["extractor"] == "firefox_cookies"
        assert "run_id" in manifest
