"""
Tests for FirefoxFaviconsExtractor.

Tests cover:
- Extractor metadata and capabilities
- Firefox favicons.sqlite parsing (moz_icons, moz_pages_w_icons, moz_icons_to_pages)
- Legacy Firefox support (moz_favicons table for Firefox < 55)
- Icon deduplication via SHA256
- Size guardrails
- StatisticsCollector integration
- Browser pattern coverage
- Hash pollution fix - NULL icon_url not replaced with numeric hash
- URL integration - icon URLs and page URLs added to urls table
- Image integration - icons >= 64px added to images table with pHash
"""

import hashlib
import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extractors.browser.firefox.favicons import FirefoxFaviconsExtractor
from extractors.browser.firefox.favicons._parsers import (
    parse_favicons_database,
    parse_moz_icons,
    parse_moz_favicons_legacy,
    parse_page_mappings,
)
from extractors.browser.firefox._patterns import (
    FIREFOX_BROWSERS,
    FIREFOX_ARTIFACTS,
    extract_profile_from_path,
)


# =============================================================================
# Module-Level Fixtures
# =============================================================================

@pytest.fixture
def favicons_db_with_large_icons(tmp_path):
    """Create favicons database with large and small icons.

    Available at module level for use across multiple test classes.
    """
    db_path = tmp_path / "favicons.sqlite"
    conn = sqlite3.connect(str(db_path))

    conn.execute("""
        CREATE TABLE moz_icons (
            id INTEGER PRIMARY KEY,
            icon_url TEXT,
            fixed_icon_url_hash INTEGER,
            width INTEGER DEFAULT 16,
            root INTEGER DEFAULT 0,
            expire_ms INTEGER DEFAULT 0,
            data BLOB
        )
    """)
    conn.execute("CREATE TABLE moz_pages_w_icons (id INTEGER PRIMARY KEY, page_url TEXT)")
    conn.execute("CREATE TABLE moz_icons_to_pages (page_id INTEGER, icon_id INTEGER)")

    # PNG icon data
    icon_data = bytes([
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
        0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
        0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x10,
        0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x91, 0x68, 0x36
    ])

    # Insert icons of varying sizes
    conn.execute(
        "INSERT INTO moz_icons (id, icon_url, width, data) VALUES (1, ?, 16, ?)",
        ("https://example.com/small.ico", icon_data)
    )
    conn.execute(
        "INSERT INTO moz_icons (id, icon_url, width, data) VALUES (2, ?, 32, ?)",
        ("https://example.com/medium.ico", icon_data)
    )
    conn.execute(
        "INSERT INTO moz_icons (id, icon_url, width, data) VALUES (3, ?, 64, ?)",
        ("https://example.com/large.ico", icon_data)  # >= 64px threshold
    )
    conn.execute(
        "INSERT INTO moz_icons (id, icon_url, width, data) VALUES (4, ?, 128, ?)",
        ("https://example.com/xlarge.ico", icon_data)  # >= 64px threshold
    )

    conn.commit()
    conn.close()
    return db_path


# =============================================================================
# Metadata Tests
# =============================================================================


class TestFirefoxFaviconsMetadata:
    """Test extractor metadata."""

    def test_metadata_name(self):
        """Test extractor name."""
        extractor = FirefoxFaviconsExtractor()
        assert extractor.metadata.name == "firefox_favicons"

    def test_metadata_display_name(self):
        """Test extractor display name."""
        extractor = FirefoxFaviconsExtractor()
        assert "Firefox" in extractor.metadata.display_name
        assert "Favicons" in extractor.metadata.display_name

    def test_metadata_category(self):
        """Test extractor category."""
        extractor = FirefoxFaviconsExtractor()
        assert extractor.metadata.category == "browser"

    def test_metadata_version(self):
        """Test extractor version."""
        extractor = FirefoxFaviconsExtractor()
        assert extractor.metadata.version
        assert "." in extractor.metadata.version

    def test_metadata_capabilities(self):
        """Test extractor capabilities."""
        extractor = FirefoxFaviconsExtractor()
        assert extractor.metadata.can_extract is True
        assert extractor.metadata.can_ingest is True

    def test_metadata_requires_no_tools(self):
        """Test extractor requires no external tools."""
        extractor = FirefoxFaviconsExtractor()
        assert extractor.metadata.requires_tools == []

    def test_supported_browsers(self):
        """Test supported browser list."""
        extractor = FirefoxFaviconsExtractor()
        assert "firefox" in extractor.SUPPORTED_BROWSERS
        assert "tor" in extractor.SUPPORTED_BROWSERS
        assert "chrome" not in extractor.SUPPORTED_BROWSERS


# =============================================================================
# Pattern Tests
# =============================================================================


class TestFirefoxFaviconsPatterns:
    """Test browser pattern configuration."""

    def test_favicons_artifact_exists(self):
        """Test favicons artifact is defined."""
        assert "favicons" in FIREFOX_ARTIFACTS

    def test_favicons_patterns_include_wildcard_profile(self):
        """Test favicons patterns include wildcard profile."""
        patterns = FIREFOX_ARTIFACTS["favicons"]
        assert any("*/favicons.sqlite" in p for p in patterns)

    def test_favicons_patterns_include_wal(self):
        """Test favicons patterns include WAL files."""
        patterns = FIREFOX_ARTIFACTS["favicons"]
        assert any("-wal" in p for p in patterns)

    def test_all_firefox_browsers_defined(self):
        """Test all expected browsers are defined."""
        expected = {"firefox", "firefox_esr", "tor"}
        assert expected.issubset(set(FIREFOX_BROWSERS.keys()))


# =============================================================================
# Capability Tests
# =============================================================================


class TestFirefoxFaviconsCapabilities:
    """Test extractor capability checks."""

    def test_can_run_extraction_with_filesystem(self):
        """Test extraction can run with filesystem."""
        extractor = FirefoxFaviconsExtractor()
        mock_fs = MagicMock()
        can_run, reason = extractor.can_run_extraction(mock_fs)
        assert can_run is True
        assert reason == ""

    def test_cannot_run_extraction_without_filesystem(self):
        """Test extraction cannot run without filesystem."""
        extractor = FirefoxFaviconsExtractor()
        can_run, reason = extractor.can_run_extraction(None)
        assert can_run is False
        assert "No evidence filesystem" in reason

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        """Test ingestion can run with manifest."""
        extractor = FirefoxFaviconsExtractor()
        manifest = tmp_path / "manifest.json"
        manifest.write_text("{}")

        can_run, reason = extractor.can_run_ingestion(tmp_path)
        assert can_run is True
        assert reason == ""

    def test_cannot_run_ingestion_without_manifest(self, tmp_path):
        """Test ingestion cannot run without manifest."""
        extractor = FirefoxFaviconsExtractor()
        can_run, reason = extractor.can_run_ingestion(tmp_path)
        assert can_run is False
        assert "No manifest.json found" in reason

    def test_has_existing_output_with_manifest(self, tmp_path):
        """Test output detection with manifest."""
        extractor = FirefoxFaviconsExtractor()
        manifest = tmp_path / "manifest.json"
        manifest.write_text("{}")

        assert extractor.has_existing_output(tmp_path) is True

    def test_has_no_existing_output_without_manifest(self, tmp_path):
        """Test output detection without manifest."""
        extractor = FirefoxFaviconsExtractor()
        assert extractor.has_existing_output(tmp_path) is False


# =============================================================================
# Output Directory Tests
# =============================================================================


class TestFirefoxFaviconsOutputDir:
    """Test output directory generation."""

    def test_get_output_dir(self, tmp_path):
        """Test output directory path."""
        extractor = FirefoxFaviconsExtractor()
        output_dir = extractor.get_output_dir(tmp_path, "evidence_001")

        assert output_dir == tmp_path / "evidences" / "evidence_001" / "firefox_favicons"


# =============================================================================
# Firefox Favicons Database Parsing Tests
# =============================================================================


class TestFirefoxFaviconsDbParsing:
    """Test Firefox favicons.sqlite parsing."""

    @pytest.fixture
    def firefox_favicons_db(self, tmp_path):
        """Create a test Firefox favicons.sqlite database."""
        db_path = tmp_path / "favicons.sqlite"
        conn = sqlite3.connect(str(db_path))

        # Create Firefox schema
        conn.execute("""
            CREATE TABLE moz_icons (
                id INTEGER PRIMARY KEY,
                icon_url TEXT,
                fixed_icon_url_hash INTEGER,
                width INTEGER DEFAULT 16,
                root INTEGER DEFAULT 0,
                expire_ms INTEGER DEFAULT 0,
                data BLOB
            )
        """)
        conn.execute("""
            CREATE TABLE moz_pages_w_icons (
                id INTEGER PRIMARY KEY,
                page_url TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE moz_icons_to_pages (
                page_id INTEGER NOT NULL,
                icon_id INTEGER NOT NULL,
                expire_ms INTEGER DEFAULT 0
            )
        """)

        # Insert test data
        # Simple PNG header
        icon_data = bytes([
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
            0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
            0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x10,
            0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x91, 0x68, 0x36
        ])

        conn.execute("INSERT INTO moz_icons (id, icon_url, width, root, data) VALUES (1, 'https://example.com/favicon.ico', 16, 0, ?)", (icon_data,))
        conn.execute("INSERT INTO moz_pages_w_icons (id, page_url) VALUES (1, 'https://example.com/')")
        conn.execute("INSERT INTO moz_icons_to_pages (page_id, icon_id) VALUES (1, 1)")

        conn.commit()
        conn.close()

        return db_path

    def test_parse_moz_icons_table(self, firefox_favicons_db, tmp_path):
        """Test parsing moz_icons table."""
        conn = sqlite3.connect(str(firefox_favicons_db))
        conn.row_factory = sqlite3.Row

        # Parse using the new parser function
        icons, mappings, is_legacy = parse_favicons_database(
            conn, "/path/to/favicons.sqlite"
        )

        conn.close()

        # Should have found 1 icon from moz_icons
        assert len(icons) == 1
        assert icons[0]["icon_url"] == "https://example.com/favicon.ico"
        assert icons[0]["width"] == 16

        # Should have page mappings
        assert len(mappings) == 1
        assert mappings[0]["page_url"] == "https://example.com/"

        # Should NOT be legacy
        assert is_legacy is False


# =============================================================================
# Profile Extraction Tests
# =============================================================================


class TestFirefoxFaviconsProfileExtraction:
    """Test profile name extraction from paths."""

    def test_extract_profile_with_profiles_dir(self):
        """Test profile extraction from Profiles directory."""
        path = "Users/TestUser/AppData/Roaming/Mozilla/Firefox/Profiles/abc123.default/favicons.sqlite"
        profile = extract_profile_from_path(path)
        assert profile == "abc123.default"

    def test_extract_profile_linux_path(self):
        """Test profile extraction from Linux path."""
        path = "home/testuser/.mozilla/firefox/xyz789.default-release/favicons.sqlite"
        profile = extract_profile_from_path(path)
        assert profile == "xyz789.default-release"

    def test_extract_profile_tor_browser(self):
        """Test profile extraction from Tor Browser path."""
        path = "Users/TestUser/Desktop/Tor Browser/Browser/TorBrowser/Data/Browser/profile.default/favicons.sqlite"
        profile = extract_profile_from_path(path)
        assert profile == "profile.default"

    def test_extract_profile_fallback(self):
        """Test profile extraction fallback for unknown paths."""
        path = "some/unknown/path/favicons.sqlite"
        profile = extract_profile_from_path(path)
        assert profile == "Default"


# =============================================================================
# StatisticsCollector Integration Tests
# =============================================================================


class TestFirefoxFaviconsStatistics:
    """Test StatisticsCollector integration."""

    def test_extraction_starts_statistics_run(self, tmp_path):
        """Test that extraction starts a statistics run."""
        extractor = FirefoxFaviconsExtractor()

        mock_fs = MagicMock()
        mock_fs.iter_paths.return_value = []
        mock_fs.source_path = "/test/image.E01"
        mock_fs.fs_type = "NTFS"

        callbacks = MagicMock()
        config = {"evidence_id": 1}
        output_dir = tmp_path / "output"

        # Patch at core.statistics_collector since the import happens inside _get_statistics_collector
        with patch("core.statistics_collector.StatisticsCollector") as mock_stats_class:
            mock_stats = MagicMock()
            mock_stats_class.get_instance.return_value = mock_stats

            extractor.run_extraction(mock_fs, output_dir, config, callbacks)

            mock_stats.start_run.assert_called_once()
            mock_stats.finish_run.assert_called_once()


# =============================================================================
# Run ID Generation Tests
# =============================================================================


class TestFirefoxFaviconsRunId:
    """Test run ID generation."""

    def test_generate_run_id_format(self):
        """Test run ID format."""
        extractor = FirefoxFaviconsExtractor()
        run_id = extractor._generate_run_id()

        # Format: YYYYMMDDTHHMMSS_xxxxxxxx
        assert "_" in run_id
        parts = run_id.split("_")
        assert len(parts) == 2
        assert len(parts[0]) == 15  # YYYYMMDDTHHMMSS
        assert len(parts[1]) == 8   # UUID first 8 chars

    def test_generate_run_id_unique(self):
        """Test run IDs are unique."""
        extractor = FirefoxFaviconsExtractor()
        run_ids = {extractor._generate_run_id() for _ in range(10)}
        assert len(run_ids) == 10


# =============================================================================
# Backward Compatibility Tests
# =============================================================================


class TestFirefoxFaviconsBackwardCompat:
    """Test backward compatibility exports."""

    def test_import_from_new_module(self):
        """Test import from new module path."""
        from extractors.browser.firefox.favicons import FirefoxFaviconsExtractor as NewFirefox
        assert NewFirefox is FirefoxFaviconsExtractor


# =============================================================================
# Hash Pollution Fix Tests
# =============================================================================


class TestFirefoxFaviconsHashPollutionFix:
    """Test that fixed_icon_url_hash is not used as icon_url fallback."""

    @pytest.fixture
    def db_with_null_icon_url(self, tmp_path):
        """Create database with NULL icon_url but valid fixed_icon_url_hash."""
        db_path = tmp_path / "favicons.sqlite"
        conn = sqlite3.connect(str(db_path))

        conn.execute("""
            CREATE TABLE moz_icons (
                id INTEGER PRIMARY KEY,
                icon_url TEXT,
                fixed_icon_url_hash INTEGER,
                width INTEGER DEFAULT 16,
                root INTEGER DEFAULT 0,
                expire_ms INTEGER DEFAULT 0,
                data BLOB
            )
        """)
        conn.execute("""
            CREATE TABLE moz_pages_w_icons (
                id INTEGER PRIMARY KEY,
                page_url TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE moz_icons_to_pages (
                page_id INTEGER NOT NULL,
                icon_id INTEGER NOT NULL
            )
        """)

        # PNG header bytes for valid icon data
        icon_data = bytes([
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
            0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
            0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x10,
            0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x91, 0x68, 0x36
        ])

        # Insert with NULL icon_url but valid hash (simulating Firefox behavior)
        conn.execute(
            "INSERT INTO moz_icons (id, icon_url, fixed_icon_url_hash, width, data) "
            "VALUES (1, NULL, 12345678901234567890, 16, ?)",
            (icon_data,)
        )

        conn.commit()
        conn.close()
        return db_path

    def test_null_icon_url_not_replaced_with_hash(self, db_with_null_icon_url, tmp_path):
        """Test that NULL icon_url stays NULL, not replaced with hash."""
        conn = sqlite3.connect(str(db_with_null_icon_url))
        conn.row_factory = sqlite3.Row

        # Parse using the new parser function
        icons, mappings, is_legacy = parse_favicons_database(
            conn, "/path/to/favicons.sqlite"
        )

        conn.close()

        # Verify icon_url is None, not a numeric hash
        assert len(icons) == 1
        assert icons[0]["icon_url"] is None


# =============================================================================
# Legacy moz_favicons Schema Tests
# =============================================================================


class TestFirefoxLegacyFaviconsSchema:
    """Test legacy moz_favicons schema support (Firefox < 55)."""

    @pytest.fixture
    def legacy_favicons_db(self, tmp_path):
        """Create database with legacy moz_favicons schema."""
        db_path = tmp_path / "favicons.sqlite"
        conn = sqlite3.connect(str(db_path))

        # Legacy Firefox schema (pre-Firefox 55)
        conn.execute("""
            CREATE TABLE moz_favicons (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                data BLOB,
                mime_type TEXT,
                expiration INTEGER DEFAULT 0
            )
        """)

        # PNG header bytes
        icon_data = bytes([
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
            0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
            0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x10,
            0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x91, 0x68, 0x36
        ])

        conn.execute(
            "INSERT INTO moz_favicons (id, url, data, mime_type, expiration) "
            "VALUES (1, 'https://legacy.example.com/favicon.ico', ?, 'image/x-icon', 0)",
            (icon_data,)
        )
        conn.execute(
            "INSERT INTO moz_favicons (id, url, data, mime_type, expiration) "
            "VALUES (2, 'https://another.example.com/icon.png', ?, 'image/png', 1234567890)",
            (icon_data,)
        )

        conn.commit()
        conn.close()
        return db_path

    def test_legacy_moz_favicons_parsing(self, legacy_favicons_db, tmp_path):
        """Test parsing legacy moz_favicons table."""
        conn = sqlite3.connect(str(legacy_favicons_db))
        conn.row_factory = sqlite3.Row

        # Parse using the new parser function
        icons, mappings, is_legacy = parse_favicons_database(
            conn, "/path/to/favicons.sqlite"
        )

        conn.close()

        # Should have found 2 favicons from legacy table
        assert len(icons) == 2
        assert is_legacy is True

        # Verify URLs are preserved correctly
        urls = {icon["icon_url"] for icon in icons}
        assert "https://legacy.example.com/favicon.ico" in urls
        assert "https://another.example.com/icon.png" in urls

        # Legacy schema has no page mappings in favicons.sqlite
        assert len(mappings) == 0

    def test_modern_schema_preferred_over_legacy(self, tmp_path):
        """Test that modern moz_icons is used when both schemas exist."""
        db_path = tmp_path / "favicons.sqlite"
        conn = sqlite3.connect(str(db_path))

        # Create both modern and legacy tables
        conn.execute("""
            CREATE TABLE moz_icons (
                id INTEGER PRIMARY KEY,
                icon_url TEXT,
                fixed_icon_url_hash INTEGER,
                width INTEGER DEFAULT 16,
                root INTEGER DEFAULT 0,
                expire_ms INTEGER DEFAULT 0,
                data BLOB
            )
        """)
        conn.execute("""
            CREATE TABLE moz_pages_w_icons (
                id INTEGER PRIMARY KEY,
                page_url TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE moz_icons_to_pages (
                page_id INTEGER NOT NULL,
                icon_id INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE moz_favicons (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                data BLOB,
                mime_type TEXT
            )
        """)

        icon_data = bytes([
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
            0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
            0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x10,
            0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x91, 0x68, 0x36
        ])

        # Insert into both tables
        conn.execute(
            "INSERT INTO moz_icons (id, icon_url, width, root, data) "
            "VALUES (1, 'https://modern.example.com/favicon.ico', 16, 0, ?)",
            (icon_data,)
        )
        conn.execute(
            "INSERT INTO moz_favicons (id, url, data, mime_type) "
            "VALUES (1, 'https://legacy.example.com/favicon.ico', ?, 'image/x-icon')",
            (icon_data,)
        )

        conn.commit()
        conn.close()

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        # Parse using the new parser function
        icons, mappings, is_legacy = parse_favicons_database(
            conn, "/path/to/favicons.sqlite"
        )

        conn.close()

        # Only modern schema should be used (not legacy)
        assert is_legacy is False
        assert len(icons) == 1
        assert icons[0]["icon_url"] == "https://modern.example.com/favicon.ico"


# =============================================================================
# URL and Image Integration Tests
# =============================================================================


class TestFirefoxFaviconsUrlIntegration:
    """Test URL integration - icon URLs and page URLs added to urls table."""

    @pytest.fixture
    def favicons_db_with_mappings(self, tmp_path):
        """Create favicons database with icons and page mappings."""
        db_path = tmp_path / "favicons.sqlite"
        conn = sqlite3.connect(str(db_path))

        conn.execute("""
            CREATE TABLE moz_icons (
                id INTEGER PRIMARY KEY,
                icon_url TEXT,
                fixed_icon_url_hash INTEGER,
                width INTEGER DEFAULT 16,
                root INTEGER DEFAULT 0,
                expire_ms INTEGER DEFAULT 0,
                data BLOB
            )
        """)
        conn.execute("""
            CREATE TABLE moz_pages_w_icons (
                id INTEGER PRIMARY KEY,
                page_url TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE moz_icons_to_pages (
                page_id INTEGER NOT NULL,
                icon_id INTEGER NOT NULL
            )
        """)

        # Small PNG icon (16x16)
        icon_data = bytes([
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
            0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
            0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x10,
            0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x91, 0x68, 0x36
        ])

        # Insert icons
        conn.execute(
            "INSERT INTO moz_icons (id, icon_url, width, data) VALUES (1, ?, 16, ?)",
            ("https://example.com/favicon.ico", icon_data)
        )
        conn.execute(
            "INSERT INTO moz_icons (id, icon_url, width, data) VALUES (2, ?, 32, ?)",
            ("https://other.com/icon.png", icon_data)
        )

        # Insert page mappings
        conn.execute("INSERT INTO moz_pages_w_icons (id, page_url) VALUES (1, 'https://example.com/page1')")
        conn.execute("INSERT INTO moz_pages_w_icons (id, page_url) VALUES (2, 'https://example.com/page2')")
        conn.execute("INSERT INTO moz_pages_w_icons (id, page_url) VALUES (3, 'https://other.com/')")

        # Map icons to pages
        conn.execute("INSERT INTO moz_icons_to_pages (page_id, icon_id) VALUES (1, 1)")
        conn.execute("INSERT INTO moz_icons_to_pages (page_id, icon_id) VALUES (2, 1)")
        conn.execute("INSERT INTO moz_icons_to_pages (page_id, icon_id) VALUES (3, 2)")

        conn.commit()
        conn.close()
        return db_path

    def test_icon_urls_inserted(self, favicons_db_with_mappings, tmp_path):
        """Test that icon URLs are added to urls table."""
        extractor = FirefoxFaviconsExtractor()

        evidence_conn = sqlite3.connect(str(tmp_path / "evidence.sqlite"))

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        inserted_urls = []

        def capture_urls(conn, evidence_id, urls):
            inserted_urls.extend(urls)

        with patch("core.database.insert_favicon", return_value=1):
            with patch("core.database.insert_favicon_mappings", return_value=1):
                with patch("core.database.insert_urls", side_effect=capture_urls):
                    with patch("core.database.insert_image_with_discovery", return_value=(1, True)):
                        callbacks = MagicMock()
                        file_info = {
                            "source_path": "/path/to/favicons.sqlite",
                            "local_path": str(favicons_db_with_mappings),
                        }

                        favicon_count, mapping_count, url_count, image_count = extractor._ingest_favicons(
                            favicons_db_with_mappings, evidence_conn, 1, "test_run",
                            "firefox", "test.default", file_info, callbacks, output_dir
                        )

        evidence_conn.close()

        # Should have 5 URLs: 2 icon URLs + 3 page URLs
        assert len(inserted_urls) == 5

        # Check icon URLs (context starts with "favicon_icon:")
        icon_urls = [u for u in inserted_urls if u["context"].startswith("favicon_icon:")]
        assert len(icon_urls) == 2
        assert {u["url"] for u in icon_urls} == {
            "https://example.com/favicon.ico",
            "https://other.com/icon.png"
        }

        # Check page URLs (context starts with "favicon_page:")
        page_urls = [u for u in inserted_urls if u["context"].startswith("favicon_page:")]
        assert len(page_urls) == 3
        assert {u["url"] for u in page_urls} == {
            "https://example.com/page1",
            "https://example.com/page2",
            "https://other.com/"
        }

    def test_url_record_contains_required_fields(self, favicons_db_with_mappings, tmp_path):
        """Test that URL records have all required fields."""
        extractor = FirefoxFaviconsExtractor()

        evidence_conn = sqlite3.connect(str(tmp_path / "evidence.sqlite"))

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        inserted_urls = []

        def capture_urls(conn, evidence_id, urls):
            inserted_urls.extend(urls)

        with patch("core.database.insert_favicon", return_value=1):
            with patch("core.database.insert_favicon_mappings", return_value=1):
                with patch("core.database.insert_urls", side_effect=capture_urls):
                    with patch("core.database.insert_image_with_discovery", return_value=(1, True)):
                        callbacks = MagicMock()
                        file_info = {
                            "source_path": "/path/to/favicons.sqlite",
                            "local_path": str(favicons_db_with_mappings),
                        }

                        extractor._ingest_favicons(
                            favicons_db_with_mappings, evidence_conn, 1, "test_run",
                            "firefox", "test.default", file_info, callbacks, output_dir
                        )

        evidence_conn.close()

        # Verify first URL record has all required fields
        assert len(inserted_urls) > 0
        url_record = inserted_urls[0]

        assert "url" in url_record
        assert "domain" in url_record
        assert "scheme" in url_record
        assert "discovered_by" in url_record
        assert "run_id" in url_record
        assert "source_path" in url_record
        assert "context" in url_record
        assert "first_seen_utc" in url_record

        # Check specific values
        assert url_record["discovered_by"].startswith("firefox_favicons:")
        assert url_record["run_id"] == "test_run"


class TestFirefoxFaviconsImageIntegration:
    """Test image integration - icons >= 64px added to images table."""

    def test_large_icons_added_to_images_table(self, favicons_db_with_large_icons, tmp_path):
        """Test that icons >= 64px are added to images table."""
        extractor = FirefoxFaviconsExtractor()

        evidence_conn = sqlite3.connect(str(tmp_path / "evidence.sqlite"))

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        image_inserts = []

        def capture_image(conn, evidence_id, image_data, discovery_data):
            image_inserts.append({"image": image_data, "discovery": discovery_data})
            return (len(image_inserts), True)

        with patch("core.database.insert_favicon", return_value=1):
            with patch("core.database.insert_favicon_mappings", return_value=0):
                with patch("core.database.insert_urls"):
                    with patch("core.database.insert_image_with_discovery", side_effect=capture_image):
                        with patch("extractors._shared.carving.exif.generate_thumbnail"):
                            callbacks = MagicMock()
                            file_info = {
                                "source_path": "/path/to/favicons.sqlite",
                                "local_path": str(favicons_db_with_large_icons),
                            }

                            favicon_count, mapping_count, url_count, image_count = extractor._ingest_favicons(
                                favicons_db_with_large_icons, evidence_conn, 1, "test_run",
                                "firefox", "test.default", file_info, callbacks, output_dir
                            )

        evidence_conn.close()

        # Should have 2 images (64px and 128px icons)
        assert image_count == 2
        assert len(image_inserts) == 2

    def test_small_icons_not_added_to_images_table(self, favicons_db_with_large_icons, tmp_path):
        """Test that icons < 64px are NOT added to images table."""
        extractor = FirefoxFaviconsExtractor()

        evidence_conn = sqlite3.connect(str(tmp_path / "evidence.sqlite"))

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        image_inserts = []

        def capture_image(conn, evidence_id, image_data, discovery_data):
            image_inserts.append({"image": image_data, "discovery": discovery_data})
            return (len(image_inserts), True)

        with patch("core.database.insert_favicon", return_value=1):
            with patch("core.database.insert_favicon_mappings", return_value=0):
                with patch("core.database.insert_urls"):
                    with patch("core.database.insert_image_with_discovery", side_effect=capture_image):
                        with patch("extractors._shared.carving.exif.generate_thumbnail"):
                            callbacks = MagicMock()
                            file_info = {
                                "source_path": "/path/to/favicons.sqlite",
                                "local_path": str(favicons_db_with_large_icons),
                            }

                            extractor._ingest_favicons(
                                favicons_db_with_large_icons, evidence_conn, 1, "test_run",
                                "firefox", "test.default", file_info, callbacks, output_dir
                            )

        evidence_conn.close()

        # Check that small icons (16px, 32px) were not added
        # We should only have 2 inserts (64px and 128px)
        assert len(image_inserts) == 2

    def test_image_record_contains_phash(self, favicons_db_with_large_icons, tmp_path):
        """Test that image records contain pHash for similarity search."""
        extractor = FirefoxFaviconsExtractor()

        evidence_conn = sqlite3.connect(str(tmp_path / "evidence.sqlite"))

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        image_inserts = []

        def capture_image(conn, evidence_id, image_data, discovery_data):
            image_inserts.append({"image": image_data, "discovery": discovery_data})
            return (len(image_inserts), True)

        with patch("core.database.insert_favicon", return_value=1):
            with patch("core.database.insert_favicon_mappings", return_value=0):
                with patch("core.database.insert_urls"):
                    with patch("core.database.insert_image_with_discovery", side_effect=capture_image):
                        with patch("extractors._shared.carving.exif.generate_thumbnail"):
                            callbacks = MagicMock()
                            file_info = {
                                "source_path": "/path/to/favicons.sqlite",
                                "local_path": str(favicons_db_with_large_icons),
                            }

                            extractor._ingest_favicons(
                                favicons_db_with_large_icons, evidence_conn, 1, "test_run",
                                "firefox", "test.default", file_info, callbacks, output_dir
                            )

        evidence_conn.close()

        # Check image record fields
        assert len(image_inserts) > 0
        image_data = image_inserts[0]["image"]

        assert "sha256" in image_data
        assert "md5" in image_data
        assert "rel_path" in image_data
        assert "filename" in image_data
        assert "size_bytes" in image_data
        # pHash may be None if imagehash not available, but key should exist
        assert "phash" in image_data
        assert "phash_prefix" in image_data

    def test_image_discovery_record_has_provenance(self, favicons_db_with_large_icons, tmp_path):
        """Test that discovery records have proper provenance."""
        extractor = FirefoxFaviconsExtractor()

        evidence_conn = sqlite3.connect(str(tmp_path / "evidence.sqlite"))

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        image_inserts = []

        def capture_image(conn, evidence_id, image_data, discovery_data):
            image_inserts.append({"image": image_data, "discovery": discovery_data})
            return (len(image_inserts), True)

        with patch("core.database.insert_favicon", return_value=1):
            with patch("core.database.insert_favicon_mappings", return_value=0):
                with patch("core.database.insert_urls"):
                    with patch("core.database.insert_image_with_discovery", side_effect=capture_image):
                        with patch("extractors._shared.carving.exif.generate_thumbnail"):
                            callbacks = MagicMock()
                            file_info = {
                                "source_path": "/path/to/favicons.sqlite",
                                "local_path": str(favicons_db_with_large_icons),
                            }

                            extractor._ingest_favicons(
                                favicons_db_with_large_icons, evidence_conn, 1, "test_run",
                                "firefox", "test.default", file_info, callbacks, output_dir
                            )

        evidence_conn.close()

        # Check discovery record fields
        assert len(image_inserts) > 0
        discovery_data = image_inserts[0]["discovery"]

        assert discovery_data["discovered_by"] == "firefox_favicons"
        assert discovery_data["run_id"] == "test_run"
        assert "extractor_version" in discovery_data
        assert "source_path" in discovery_data
        # Icon URL should be stored for provenance
        assert "cache_url" in discovery_data


class TestFirefoxFaviconsDiskStorage:
    """Tests for disk storage and thumbnail generation."""

    def test_large_icons_written_to_disk(self, favicons_db_with_large_icons, tmp_path):
        """Test that large icons (>=64px) are written to disk files."""
        from extractors.browser.firefox.favicons.extractor import FirefoxFaviconsExtractor

        extractor = FirefoxFaviconsExtractor()

        evidence_conn = sqlite3.connect(str(tmp_path / "evidence.sqlite"))

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with patch("core.database.insert_favicon", return_value=1):
            with patch("core.database.insert_favicon_mappings", return_value=0):
                with patch("core.database.insert_urls"):
                    with patch("core.database.insert_image_with_discovery", return_value=(1, True)):
                        with patch("extractors._shared.carving.exif.generate_thumbnail"):
                            callbacks = MagicMock()
                            file_info = {
                                "source_path": "/path/to/favicons.sqlite",
                                "local_path": str(favicons_db_with_large_icons),
                            }

                            extractor._ingest_favicons(
                                favicons_db_with_large_icons, evidence_conn, 1, "test_run",
                                "firefox", "test.default", file_info, callbacks, output_dir
                            )

        evidence_conn.close()

        # Check that favicons directory was created with icon files
        favicons_dir = output_dir / "favicons" / "firefox"
        assert favicons_dir.exists(), "Favicons output directory should be created"

        # Find image files. Note: only 1 file because both 64px and 128px icons
        # have identical content (same sha256 hash) - deduplication works correctly
        image_files = list(favicons_dir.rglob("*.png"))
        assert len(image_files) == 1, f"Expected 1 unique icon file, found {len(image_files)}"

    def test_thumbnails_generated_for_large_icons(self, favicons_db_with_large_icons, tmp_path):
        """Test that thumbnails are generated for large icons."""
        from extractors.browser.firefox.favicons.extractor import FirefoxFaviconsExtractor

        extractor = FirefoxFaviconsExtractor()

        evidence_conn = sqlite3.connect(str(tmp_path / "evidence.sqlite"))

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        thumbnail_calls = []

        def capture_thumbnail(image_path, output_path, size=256):
            thumbnail_calls.append({"image": image_path, "output": output_path, "size": size})

        with patch("core.database.insert_favicon", return_value=1):
            with patch("core.database.insert_favicon_mappings", return_value=0):
                with patch("core.database.insert_urls"):
                    with patch("core.database.insert_image_with_discovery", return_value=(1, True)):
                        with patch("extractors._shared.carving.exif.generate_thumbnail", side_effect=capture_thumbnail):
                            callbacks = MagicMock()
                            file_info = {
                                "source_path": "/path/to/favicons.sqlite",
                                "local_path": str(favicons_db_with_large_icons),
                            }

                            extractor._ingest_favicons(
                                favicons_db_with_large_icons, evidence_conn, 1, "test_run",
                                "firefox", "test.default", file_info, callbacks, output_dir
                            )

        evidence_conn.close()

        # Thumbnail is generated only once per unique image hash (deduplication)
        # Both 64px and 128px icons have the same content, so only 1 thumbnail call
        assert len(thumbnail_calls) == 1, f"Expected 1 thumbnail call (deduplication), got {len(thumbnail_calls)}"

        # Thumbnails should be in thumbnails/ subdirectory
        for call in thumbnail_calls:
            assert "/thumbnails/" in str(call["output"]), "Thumbnail should be in thumbnails directory"

    def test_icon_extension_detection(self, tmp_path):
        """Test that icon file extensions are detected from magic bytes."""
        from extractors.browser.firefox.favicons.extractor import FirefoxFaviconsExtractor

        extractor = FirefoxFaviconsExtractor()

        # PNG magic bytes
        png_data = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100
        assert extractor._detect_image_extension(png_data) == "png"

        # JPEG magic bytes
        jpeg_data = b'\xff\xd8\xff' + b'\x00' * 100
        assert extractor._detect_image_extension(jpeg_data) == "jpg"

        # GIF magic bytes
        gif_data = b'GIF89a' + b'\x00' * 100
        assert extractor._detect_image_extension(gif_data) == "gif"

        # WebP magic bytes
        webp_data = b'RIFF' + b'\x00\x00\x00\x00' + b'WEBP' + b'\x00' * 100
        assert extractor._detect_image_extension(webp_data) == "webp"

        # SVG (XML-based vector graphics)
        svg_data = b"<svg xmlns='http://www.w3.org/2000/svg'>" + b'\x00' * 100
        assert extractor._detect_image_extension(svg_data) == "svg"

        # SVG with XML declaration
        svg_xml_data = b"<?xml version='1.0'?><svg>" + b'\x00' * 100
        assert extractor._detect_image_extension(svg_xml_data) == "svg"

        # Unknown format defaults to bin
        unknown_data = b'\x00\x01\x02\x03' + b'\x00' * 100
        assert extractor._detect_image_extension(unknown_data) == "bin"

        # ICO magic bytes (actual ICO format)
        ico_data = b'\x00\x00\x01\x00' + b'\x00' * 100
        assert extractor._detect_image_extension(ico_data) == "ico"

    def test_rel_path_uses_hash_prefix_structure(self, favicons_db_with_large_icons, tmp_path):
        """Test that rel_path uses hash-based directory structure for scalability."""
        from extractors.browser.firefox.favicons.extractor import FirefoxFaviconsExtractor

        extractor = FirefoxFaviconsExtractor()

        evidence_conn = sqlite3.connect(str(tmp_path / "evidence.sqlite"))

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        image_data_captured = []

        def capture_image(conn, evidence_id, image_data, discovery_data):
            image_data_captured.append(image_data)
            return (len(image_data_captured), True)

        with patch("core.database.insert_favicon", return_value=1):
            with patch("core.database.insert_favicon_mappings", return_value=0):
                with patch("core.database.insert_urls"):
                    with patch("core.database.insert_image_with_discovery", side_effect=capture_image):
                        with patch("extractors._shared.carving.exif.generate_thumbnail"):
                            callbacks = MagicMock()
                            file_info = {
                                "source_path": "/path/to/favicons.sqlite",
                                "local_path": str(favicons_db_with_large_icons),
                            }

                            extractor._ingest_favicons(
                                favicons_db_with_large_icons, evidence_conn, 1, "test_run",
                                "firefox", "test.default", file_info, callbacks, output_dir
                            )

        evidence_conn.close()

        assert len(image_data_captured) > 0

        for image_data in image_data_captured:
            rel_path = image_data["rel_path"]
            # Structure: favicons/{browser}/{hash_prefix}/{filename}
            parts = rel_path.split("/")
            assert parts[0] == "favicons", f"rel_path should start with 'favicons': {rel_path}"
            assert parts[1] == "firefox", f"rel_path should contain browser name: {rel_path}"
            # Hash prefix is 2 characters
            assert len(parts[2]) == 2, f"Hash prefix should be 2 chars: {rel_path}"
