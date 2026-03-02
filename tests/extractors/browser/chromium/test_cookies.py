"""
Tests for ChromiumCookiesExtractor.

Tests cover:
- Metadata: name, version, category, capabilities
- Registry: discovery
- Patterns: cookie file patterns for all Chromium browsers
- Parsers: Cookie dataclass, SameSite mapping, encryption detection
- Extraction: filesystem discovery, manifest creation
- Ingestion: database inserts, statistics
- Error handling: cancellation, empty config, missing files
"""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import pytest

from extractors.browser.chromium.cookies import ChromiumCookiesExtractor
from extractors.browser.chromium._parsers import (
    ChromiumCookie,
    parse_cookies,
    get_cookie_stats,
)
from extractors.browser.chromium.cookies._parsers import _resolve_column_names
from extractors.browser.chromium._patterns import (
    CHROMIUM_BROWSERS,
    CHROMIUM_ARTIFACTS,
    get_patterns,
)


# =============================================================================
# Test Metadata
# =============================================================================

class TestMetadata:
    """Test extractor metadata."""

    def test_metadata_name(self):
        """Extractor has correct name."""
        ext = ChromiumCookiesExtractor()
        assert ext.metadata.name == "chromium_cookies"

    def test_metadata_display_name(self):
        """Extractor has human-readable display name."""
        ext = ChromiumCookiesExtractor()
        assert ext.metadata.display_name == "Chromium Cookies"

    def test_metadata_version(self):
        """Extractor has a version."""
        ext = ChromiumCookiesExtractor()
        assert ext.metadata.version
        assert "." in ext.metadata.version

    def test_metadata_category(self):
        """Extractor is in browser category."""
        ext = ChromiumCookiesExtractor()
        assert ext.metadata.category == "browser"

    def test_metadata_capabilities(self):
        """Extractor can extract and ingest."""
        ext = ChromiumCookiesExtractor()
        assert ext.metadata.can_extract is True
        assert ext.metadata.can_ingest is True

    def test_metadata_description_mentions_browsers(self):
        """Description lists supported browsers."""
        ext = ChromiumCookiesExtractor()
        desc = ext.metadata.description.lower()
        assert "chrome" in desc
        assert "edge" in desc


# =============================================================================
# Test Patterns
# =============================================================================

class TestPatterns:
    """Test browser path patterns."""

    def test_cookies_artifact_in_chromium_artifacts(self):
        """Cookies artifact is defined."""
        assert "cookies" in CHROMIUM_ARTIFACTS

    def test_cookies_patterns_are_list(self):
        """Cookies patterns resolve to a list."""
        patterns = CHROMIUM_ARTIFACTS.get("cookies", [])
        assert isinstance(patterns, list)
        assert len(patterns) > 0

    def test_get_patterns_chrome(self):
        """Get Chrome cookies patterns."""
        patterns = get_patterns("chrome", "cookies")
        assert len(patterns) > 0
        # All patterns should include Cookies file
        for p in patterns:
            assert "Cookies" in p

    def test_get_patterns_edge(self):
        """Get Edge cookies patterns."""
        patterns = get_patterns("edge", "cookies")
        assert len(patterns) > 0
        for p in patterns:
            assert "Cookies" in p or "Microsoft" in p

    def test_get_patterns_opera(self):
        """Get Opera cookies patterns."""
        patterns = get_patterns("opera", "cookies")
        assert len(patterns) > 0


# =============================================================================
# Test Parsers
# =============================================================================

class TestParsers:
    """Test parser functions and dataclasses."""

    def test_cookie_dataclass(self):
        """ChromiumCookie dataclass has all required fields."""
        from datetime import datetime

        cookie = ChromiumCookie(
            host_key=".example.com",
            name="session",
            value="abc123",
            path="/",
            creation_utc=datetime(2023, 1, 1),
            creation_utc_iso="2023-01-01T00:00:00+00:00",
            expires_utc=datetime(2024, 1, 1),
            expires_utc_iso="2024-01-01T00:00:00+00:00",
            last_access_utc=datetime(2023, 6, 1),
            last_access_utc_iso="2023-06-01T00:00:00+00:00",
            is_secure=True,
            is_httponly=True,
            samesite="lax",
            samesite_raw=1,
            is_persistent=True,
            has_expires=True,
            priority=1,
            encrypted_value=None,
            is_encrypted=False,
        )
        assert cookie.host_key == ".example.com"
        assert cookie.name == "session"
        assert cookie.is_secure is True
        assert cookie.samesite == "lax"
        assert cookie.samesite_raw == 1

    def test_cookie_samesite_values(self):
        """SameSite values are strings (already mapped by parser)."""
        from datetime import datetime

        # Parser produces string samesite values
        cookie = ChromiumCookie(
            host_key=".example.com", name="c", value="v",
            path="/", creation_utc=None, creation_utc_iso=None,
            expires_utc=None, expires_utc_iso=None,
            last_access_utc=None, last_access_utc_iso=None,
            is_secure=False, is_httponly=False, samesite="strict",
            samesite_raw=2,
            is_persistent=False, has_expires=False, priority=0,
            encrypted_value=None, is_encrypted=False,
        )
        assert cookie.samesite == "strict"
        assert cookie.samesite_raw == 2

    def test_cookie_encryption_detection(self):
        """Encrypted cookies are flagged correctly."""
        from datetime import datetime

        # No encryption
        cookie_plain = ChromiumCookie(
            host_key=".example.com", name="c", value="plaintext",
            path="/", creation_utc=None, creation_utc_iso=None,
            expires_utc=None, expires_utc_iso=None,
            last_access_utc=None, last_access_utc_iso=None,
            is_secure=False, is_httponly=False, samesite="unspecified",
            samesite_raw=-1,
            is_persistent=False, has_expires=False, priority=0,
            encrypted_value=None, is_encrypted=False,
        )
        assert cookie_plain.is_encrypted is False

        # With encrypted value
        cookie_encrypted = ChromiumCookie(
            host_key=".example.com", name="c", value="",
            path="/", creation_utc=None, creation_utc_iso=None,
            expires_utc=None, expires_utc_iso=None,
            last_access_utc=None, last_access_utc_iso=None,
            is_secure=False, is_httponly=False, samesite="unspecified",
            samesite_raw=-1,
            is_persistent=False, has_expires=False, priority=0,
            encrypted_value=b"v10\x00\x01\x02\x03", is_encrypted=True,
        )
        assert cookie_encrypted.is_encrypted is True


class TestParsersWithDatabase:
    """Test parsers with real SQLite data."""

    @pytest.fixture
    def cookies_db(self, tmp_path):
        """Create a test Cookies database."""
        db_path = tmp_path / "Cookies"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE cookies (
                host_key TEXT NOT NULL,
                name TEXT NOT NULL,
                value TEXT,
                path TEXT NOT NULL,
                creation_utc INTEGER NOT NULL,
                expires_utc INTEGER NOT NULL,
                last_access_utc INTEGER NOT NULL,
                is_secure INTEGER NOT NULL,
                is_httponly INTEGER NOT NULL,
                samesite INTEGER NOT NULL DEFAULT -1,
                is_persistent INTEGER NOT NULL DEFAULT 1,
                has_expires INTEGER NOT NULL DEFAULT 1,
                priority INTEGER NOT NULL DEFAULT 1,
                encrypted_value BLOB
            )
        """)

        # Insert test data
        conn.execute("""
            INSERT INTO cookies VALUES
            ('.google.com', 'NID', 'abc123', '/', 13350000000000000, 13400000000000000, 13350000000000000, 1, 1, 1, 1, 1, 1, NULL),
            ('.example.com', 'session', '', '/', 13300000000000000, 13350000000000000, 13320000000000000, 0, 0, 2, 0, 0, 0, X'763130001234')
        """)
        conn.commit()
        conn.close()
        return db_path

    def test_parse_cookies(self, cookies_db):
        """Parse cookies from database."""
        conn = sqlite3.connect(cookies_db)
        conn.row_factory = sqlite3.Row

        cookies = list(parse_cookies(conn))
        conn.close()

        assert len(cookies) == 2

        # First cookie
        google_cookie = next(c for c in cookies if c.host_key == ".google.com")
        assert google_cookie.name == "NID"
        assert google_cookie.value == "abc123"
        assert google_cookie.is_secure is True
        assert google_cookie.samesite == "lax"  # Already string from parser

        # Second cookie with encrypted value
        example_cookie = next(c for c in cookies if c.host_key == ".example.com")
        assert example_cookie.value == ""
        assert example_cookie.is_encrypted is True
        assert example_cookie.encrypted_value is not None

    def test_get_cookie_stats(self, cookies_db):
        """Get cookie statistics."""
        conn = sqlite3.connect(cookies_db)
        conn.row_factory = sqlite3.Row

        stats = get_cookie_stats(conn)
        conn.close()

        assert stats["cookie_count"] == 2
        assert stats["domain_count"] == 2
        assert "encrypted_count" in stats


# =============================================================================
# Test Extraction
# =============================================================================

class TestExtraction:
    """Test extraction workflow."""

    def test_can_run_extraction_no_fs(self):
        """Extraction requires filesystem."""
        ext = ChromiumCookiesExtractor()
        can_run, msg = ext.can_run_extraction(None)
        assert can_run is False
        assert "filesystem" in msg.lower() or "mounted" in msg.lower()

    def test_can_run_extraction_with_fs(self):
        """Extraction can run with filesystem."""
        ext = ChromiumCookiesExtractor()
        mock_fs = MagicMock()
        can_run, msg = ext.can_run_extraction(mock_fs)
        assert can_run is True

    def test_extraction_creates_manifest(self, tmp_path):
        """Extraction creates manifest.json."""
        ext = ChromiumCookiesExtractor()

        # Mock filesystem with no files
        mock_fs = MagicMock()
        mock_fs.iter_paths.return_value = []

        # Mock callbacks
        callbacks = MagicMock()

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        config = {"browsers": ["chrome"]}

        ext.run_extraction(
            evidence_fs=mock_fs,
            output_dir=output_dir,
            callbacks=callbacks,
            config=config,
        )

        manifest_path = output_dir / "manifest.json"
        assert manifest_path.exists()

        with open(manifest_path) as f:
            manifest = json.load(f)

        assert manifest["extractor"] == "chromium_cookies"
        assert "run_id" in manifest
        assert manifest["files"] == []  # No files found


# =============================================================================
# Test Ingestion
# =============================================================================

class TestIngestion:
    """Test ingestion workflow."""

    def test_can_run_ingestion_no_manifest(self, tmp_path):
        """Ingestion requires manifest."""
        ext = ChromiumCookiesExtractor()
        can_run, msg = ext.can_run_ingestion(tmp_path)
        assert can_run is False
        assert "manifest" in msg.lower()

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        """Ingestion can run with manifest."""
        ext = ChromiumCookiesExtractor()

        manifest = {"extractor_name": "chromium_cookies", "extracted_files": []}
        (tmp_path / "manifest.json").write_text(json.dumps(manifest))

        can_run, msg = ext.can_run_ingestion(tmp_path)
        assert can_run is True


# =============================================================================
# Test Error Handling
# =============================================================================

class TestErrorHandling:
    """Test error handling."""

    def test_handles_cancelled_extraction(self, tmp_path):
        """Extraction handles cancellation gracefully."""
        ext = ChromiumCookiesExtractor()

        mock_fs = MagicMock()
        mock_fs.iter_paths.side_effect = lambda p: iter([])

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = True  # Cancelled immediately

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Should not raise
        ext.run_extraction(
            evidence_fs=mock_fs,
            output_dir=output_dir,
            callbacks=callbacks,
            config={"browsers": ["chrome"]},
        )

        # Should have created manifest even if cancelled
        assert (output_dir / "manifest.json").exists()

    def test_handles_empty_config(self, tmp_path):
        """Empty config uses default browsers."""
        ext = ChromiumCookiesExtractor()

        mock_fs = MagicMock()
        mock_fs.iter_paths.return_value = []

        callbacks = MagicMock()

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Empty config should default to all browsers
        ext.run_extraction(
            evidence_fs=mock_fs,
            output_dir=output_dir,
            callbacks=callbacks,
            config={},  # Empty config
        )

        # Should have queried multiple browsers
        assert mock_fs.iter_paths.call_count > 0


# =============================================================================
# Test Legacy Schema Support (Chromium <67 / CefSharp)
# =============================================================================

class TestResolveColumnNames:
    """Test _resolve_column_names with various schema layouts."""

    def _make_db(self, tmp_path, schema_sql):
        """Helper to create a cookie db with a given schema."""
        db_path = tmp_path / "Cookies"
        conn = sqlite3.connect(db_path)
        conn.execute(schema_sql)
        conn.commit()
        return conn

    def test_modern_schema(self, tmp_path):
        """Modern schema returns modern names."""
        conn = self._make_db(tmp_path, """
            CREATE TABLE cookies (
                host_key TEXT, name TEXT, value TEXT, path TEXT,
                creation_utc INTEGER, expires_utc INTEGER, last_access_utc INTEGER,
                is_secure INTEGER, is_httponly INTEGER, samesite INTEGER,
                is_persistent INTEGER, has_expires INTEGER, priority INTEGER,
                encrypted_value BLOB
            )
        """)
        result = _resolve_column_names(conn)
        conn.close()
        assert result is not None
        assert result["is_secure"] == "is_secure"
        assert result["is_httponly"] == "is_httponly"
        assert result["is_persistent"] == "is_persistent"
        assert result["samesite"] == "samesite"

    def test_legacy_schema(self, tmp_path):
        """Old CefSharp schema maps legacy names correctly."""
        conn = self._make_db(tmp_path, """
            CREATE TABLE cookies (
                host_key TEXT, name TEXT, value TEXT, path TEXT,
                creation_utc INTEGER, expires_utc INTEGER, last_access_utc INTEGER,
                secure INTEGER, httponly INTEGER, firstpartyonly INTEGER,
                persistent INTEGER, has_expires INTEGER, priority INTEGER,
                encrypted_value BLOB
            )
        """)
        result = _resolve_column_names(conn)
        conn.close()
        assert result is not None
        assert result["is_secure"] == "secure"
        assert result["is_httponly"] == "httponly"
        assert result["is_persistent"] == "persistent"
        assert result["samesite"] == "firstpartyonly"

    def test_no_cookies_table(self, tmp_path):
        """Returns empty mapping when cookies table is missing."""
        db_path = tmp_path / "Cookies"
        conn = sqlite3.connect(db_path)
        # No table at all — PRAGMA table_info returns nothing
        result = _resolve_column_names(conn)
        conn.close()
        # Even with no matching columns, it returns a dict (not None)
        assert result is not None

    def test_mixed_columns_prefers_modern(self, tmp_path):
        """If both modern and legacy names exist, modern wins."""
        conn = self._make_db(tmp_path, """
            CREATE TABLE cookies (
                host_key TEXT, name TEXT, value TEXT, path TEXT,
                creation_utc INTEGER, expires_utc INTEGER, last_access_utc INTEGER,
                is_secure INTEGER, secure INTEGER,
                is_httponly INTEGER, httponly INTEGER,
                samesite INTEGER, firstpartyonly INTEGER,
                is_persistent INTEGER, persistent INTEGER,
                has_expires INTEGER, priority INTEGER,
                encrypted_value BLOB
            )
        """)
        result = _resolve_column_names(conn)
        conn.close()
        assert result["is_secure"] == "is_secure"
        assert result["is_httponly"] == "is_httponly"
        assert result["is_persistent"] == "is_persistent"
        assert result["samesite"] == "samesite"


class TestLegacySchemaParsing:
    """Test parse_cookies against old Chromium / CefSharp cookie schemas."""

    @pytest.fixture
    def legacy_cookies_db(self, tmp_path):
        """Create a test Cookies database with legacy (Chromium <67) schema.

        This mimics old CefSharp embedded browsers that used:
        - secure instead of is_secure
        - httponly instead of is_httponly
        - persistent instead of is_persistent
        - firstpartyonly instead of samesite
        """
        db_path = tmp_path / "Cookies"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE cookies (
                creation_utc INTEGER NOT NULL,
                host_key TEXT NOT NULL,
                name TEXT NOT NULL,
                value TEXT NOT NULL DEFAULT '',
                path TEXT NOT NULL,
                expires_utc INTEGER NOT NULL,
                secure INTEGER NOT NULL DEFAULT 0,
                httponly INTEGER NOT NULL DEFAULT 0,
                last_access_utc INTEGER NOT NULL,
                has_expires INTEGER NOT NULL DEFAULT 1,
                persistent INTEGER NOT NULL DEFAULT 1,
                priority INTEGER NOT NULL DEFAULT 1,
                encrypted_value BLOB DEFAULT '',
                firstpartyonly INTEGER NOT NULL DEFAULT 0
            )
        """)

        # Insert test data mimicking old CefSharp cookies
        conn.execute("""
            INSERT INTO cookies (
                creation_utc, host_key, name, value, path, expires_utc,
                secure, httponly, last_access_utc, has_expires, persistent,
                priority, encrypted_value, firstpartyonly
            ) VALUES
            (13100000000000000, '.example.com', 'session', 'abc123', '/', 13200000000000000, 1, 1, 13150000000000000, 1, 1, 1, NULL, 0),
            (13050000000000000, '.test.org', 'pref', 'dark', '/settings', 13150000000000000, 0, 0, 13100000000000000, 1, 0, 1, NULL, 0),
            (13000000000000000, 'localhost', 'token', '', '/', 13100000000000000, 0, 1, 13050000000000000, 0, 0, 0, X'763130001234', 0)
        """)
        conn.commit()
        conn.close()
        return db_path

    def test_parse_legacy_cookies_count(self, legacy_cookies_db):
        """Legacy schema cookies are parsed without error."""
        conn = sqlite3.connect(legacy_cookies_db)
        conn.row_factory = sqlite3.Row
        cookies = list(parse_cookies(conn))
        conn.close()
        assert len(cookies) == 3

    def test_parse_legacy_cookies_secure_flag(self, legacy_cookies_db):
        """is_secure maps from legacy 'secure' column."""
        conn = sqlite3.connect(legacy_cookies_db)
        conn.row_factory = sqlite3.Row
        cookies = list(parse_cookies(conn))
        conn.close()

        session = next(c for c in cookies if c.name == "session")
        pref = next(c for c in cookies if c.name == "pref")
        assert session.is_secure is True
        assert pref.is_secure is False

    def test_parse_legacy_cookies_httponly_flag(self, legacy_cookies_db):
        """is_httponly maps from legacy 'httponly' column."""
        conn = sqlite3.connect(legacy_cookies_db)
        conn.row_factory = sqlite3.Row
        cookies = list(parse_cookies(conn))
        conn.close()

        session = next(c for c in cookies if c.name == "session")
        pref = next(c for c in cookies if c.name == "pref")
        assert session.is_httponly is True
        assert pref.is_httponly is False

    def test_parse_legacy_cookies_persistent_flag(self, legacy_cookies_db):
        """is_persistent maps from legacy 'persistent' column."""
        conn = sqlite3.connect(legacy_cookies_db)
        conn.row_factory = sqlite3.Row
        cookies = list(parse_cookies(conn))
        conn.close()

        session = next(c for c in cookies if c.name == "session")
        pref = next(c for c in cookies if c.name == "pref")
        assert session.is_persistent is True
        assert pref.is_persistent is False

    def test_parse_legacy_cookies_samesite(self, legacy_cookies_db):
        """samesite maps from legacy 'firstpartyonly' column (defaults to 0)."""
        conn = sqlite3.connect(legacy_cookies_db)
        conn.row_factory = sqlite3.Row
        cookies = list(parse_cookies(conn))
        conn.close()

        session = next(c for c in cookies if c.name == "session")
        # firstpartyonly=0 → samesite=0 → "no_restriction"
        assert session.samesite_raw == 0
        assert session.samesite == "no_restriction"

    def test_parse_legacy_cookies_encryption(self, legacy_cookies_db):
        """Encrypted cookies are detected in legacy schema."""
        conn = sqlite3.connect(legacy_cookies_db)
        conn.row_factory = sqlite3.Row
        cookies = list(parse_cookies(conn))
        conn.close()

        token = next(c for c in cookies if c.name == "token")
        assert token.is_encrypted is True
        assert token.encrypted_value is not None

    def test_parse_legacy_cookies_timestamps(self, legacy_cookies_db):
        """Timestamps are correctly converted in legacy schema."""
        conn = sqlite3.connect(legacy_cookies_db)
        conn.row_factory = sqlite3.Row
        cookies = list(parse_cookies(conn))
        conn.close()

        session = next(c for c in cookies if c.name == "session")
        assert session.creation_utc is not None
        assert session.creation_utc_iso is not None
        assert session.last_access_utc is not None

    def test_get_cookie_stats_legacy(self, legacy_cookies_db):
        """Cookie stats work with legacy schema."""
        conn = sqlite3.connect(legacy_cookies_db)
        conn.row_factory = sqlite3.Row
        stats = get_cookie_stats(conn)
        conn.close()

        assert stats["cookie_count"] == 3
        assert stats["domain_count"] == 3
        assert stats["encrypted_count"] >= 1
