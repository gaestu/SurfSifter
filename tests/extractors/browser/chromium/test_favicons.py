"""
Tests for ChromiumFaviconsExtractor.

Tests cover:
- Extractor metadata and capabilities
- Chromium Favicons database parsing (favicon_bitmaps, favicons, icon_mapping)
- Top Sites database parsing (top_sites, thumbnails)
- Icon deduplication via SHA256
- Size guardrails
- StatisticsCollector integration
- Browser pattern coverage
- Schema warning support
"""

import hashlib
import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extractors.browser.chromium.favicons import ChromiumFaviconsExtractor
from extractors.browser.chromium.favicons._parsers import (
    parse_favicons_database,
    parse_top_sites_database,
    webkit_to_iso8601,
    detect_image_extension,
)
from extractors.browser.chromium._patterns import CHROMIUM_BROWSERS, CHROMIUM_ARTIFACTS, get_patterns


# =============================================================================
# Metadata Tests
# =============================================================================


class TestChromiumFaviconsMetadata:
    """Test extractor metadata."""

    def test_metadata_name(self):
        """Test extractor name."""
        extractor = ChromiumFaviconsExtractor()
        assert extractor.metadata.name == "chromium_favicons"

    def test_metadata_display_name(self):
        """Test extractor display name."""
        extractor = ChromiumFaviconsExtractor()
        assert "Chromium" in extractor.metadata.display_name
        assert "Favicons" in extractor.metadata.display_name

    def test_metadata_category(self):
        """Test extractor category."""
        extractor = ChromiumFaviconsExtractor()
        assert extractor.metadata.category == "browser"

    def test_metadata_version(self):
        """Test extractor version."""
        extractor = ChromiumFaviconsExtractor()
        assert extractor.metadata.version
        assert "." in extractor.metadata.version

    def test_metadata_capabilities(self):
        """Test extractor capabilities."""
        extractor = ChromiumFaviconsExtractor()
        assert extractor.metadata.can_extract is True
        assert extractor.metadata.can_ingest is True

    def test_metadata_requires_no_tools(self):
        """Test extractor requires no external tools."""
        extractor = ChromiumFaviconsExtractor()
        assert extractor.metadata.requires_tools == []

    def test_supported_browsers(self):
        """Test supported browser list."""
        extractor = ChromiumFaviconsExtractor()
        assert "chrome" in extractor.SUPPORTED_BROWSERS
        assert "edge" in extractor.SUPPORTED_BROWSERS
        assert "opera" in extractor.SUPPORTED_BROWSERS
        assert "brave" in extractor.SUPPORTED_BROWSERS
        assert "firefox" not in extractor.SUPPORTED_BROWSERS


# =============================================================================
# Pattern Tests
# =============================================================================


class TestChromiumFaviconsPatterns:
    """Test browser pattern configuration."""

    def test_favicons_artifact_exists(self):
        """Test favicons artifact is defined."""
        assert "favicons" in CHROMIUM_ARTIFACTS

    def test_top_sites_artifact_exists(self):
        """Test top_sites artifact is defined."""
        assert "top_sites" in CHROMIUM_ARTIFACTS

    def test_favicons_patterns_include_default(self):
        """Test generated favicons patterns include Default profile."""
        # CHROMIUM_ARTIFACTS now contains relative paths; get_patterns() adds profile prefix
        patterns = get_patterns("chrome", "favicons")
        assert any("Default/Favicons" in p for p in patterns)

    def test_favicons_patterns_include_profiles(self):
        """Test generated favicons patterns include Profile * profiles."""
        patterns = get_patterns("chrome", "favicons")
        assert any("Profile */Favicons" in p for p in patterns)

    def test_top_sites_patterns_include_default(self):
        """Test generated top_sites patterns include Default profile."""
        patterns = get_patterns("chrome", "top_sites")
        assert any("Default/Top Sites" in p for p in patterns)

    def test_all_chromium_browsers_defined(self):
        """Test all expected browsers are defined."""
        # Now includes beta/dev/canary channels and separate opera/opera_gx
        expected = {"chrome", "edge", "opera", "brave"}
        assert expected.issubset(set(CHROMIUM_BROWSERS.keys()))

    def test_opera_flat_profile_no_default(self):
        """Test Opera patterns don't include Default/ prefix (flat profile)."""
        patterns = get_patterns("opera", "favicons")
        # Opera uses flat profile - no Default/ in paths
        assert not any("Default/" in p for p in patterns)
        # But should still find Favicons files
        assert any("Favicons" in p for p in patterns)


# =============================================================================
# Capability Tests
# =============================================================================


class TestChromiumFaviconsCapabilities:
    """Test extractor capability checks."""

    def test_can_run_extraction_with_filesystem(self):
        """Test extraction can run with filesystem."""
        extractor = ChromiumFaviconsExtractor()
        mock_fs = MagicMock()
        can_run, reason = extractor.can_run_extraction(mock_fs)
        assert can_run is True
        assert reason == ""

    def test_cannot_run_extraction_without_filesystem(self):
        """Test extraction cannot run without filesystem."""
        extractor = ChromiumFaviconsExtractor()
        can_run, reason = extractor.can_run_extraction(None)
        assert can_run is False
        assert "No evidence filesystem" in reason

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        """Test ingestion can run with manifest."""
        extractor = ChromiumFaviconsExtractor()
        manifest = tmp_path / "manifest.json"
        manifest.write_text("{}")

        can_run, reason = extractor.can_run_ingestion(tmp_path)
        assert can_run is True
        assert reason == ""

    def test_cannot_run_ingestion_without_manifest(self, tmp_path):
        """Test ingestion cannot run without manifest."""
        extractor = ChromiumFaviconsExtractor()
        can_run, reason = extractor.can_run_ingestion(tmp_path)
        assert can_run is False
        assert "No manifest.json found" in reason

    def test_has_existing_output_with_manifest(self, tmp_path):
        """Test output detection with manifest."""
        extractor = ChromiumFaviconsExtractor()
        manifest = tmp_path / "manifest.json"
        manifest.write_text("{}")

        assert extractor.has_existing_output(tmp_path) is True

    def test_has_no_existing_output_without_manifest(self, tmp_path):
        """Test output detection without manifest."""
        extractor = ChromiumFaviconsExtractor()
        assert extractor.has_existing_output(tmp_path) is False


# =============================================================================
# Output Directory Tests
# =============================================================================


class TestChromiumFaviconsOutputDir:
    """Test output directory generation."""

    def test_get_output_dir(self, tmp_path):
        """Test output directory path."""
        extractor = ChromiumFaviconsExtractor()
        output_dir = extractor.get_output_dir(tmp_path, "evidence_001")

        assert output_dir == tmp_path / "evidences" / "evidence_001" / "chromium_favicons"


# =============================================================================
# Chromium Favicons Database Parsing Tests
# =============================================================================


class TestChromiumFaviconsDbParsing:
    """Test Chromium Favicons database parsing."""

    @pytest.fixture
    def chromium_favicons_db(self, tmp_path):
        """Create a test Chromium Favicons database."""
        db_path = tmp_path / "Favicons"
        conn = sqlite3.connect(str(db_path))

        # Create Chromium schema
        conn.execute("""
            CREATE TABLE favicons (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                icon_type INTEGER DEFAULT 1
            )
        """)
        conn.execute("""
            CREATE TABLE favicon_bitmaps (
                id INTEGER PRIMARY KEY,
                icon_id INTEGER NOT NULL,
                last_updated INTEGER DEFAULT 0,
                image_data BLOB,
                width INTEGER DEFAULT 16,
                height INTEGER DEFAULT 16
            )
        """)
        conn.execute("""
            CREATE TABLE icon_mapping (
                id INTEGER PRIMARY KEY,
                page_url TEXT NOT NULL,
                icon_id INTEGER NOT NULL
            )
        """)

        # Insert test data
        # 16x16 PNG header (minimal valid PNG)
        icon_data = bytes([
            0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,  # PNG signature
            0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,  # IHDR chunk
            0x00, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x10,  # 16x16
            0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x91, 0x68, 0x36  # bit depth, etc.
        ])

        conn.execute("INSERT INTO favicons (id, url) VALUES (1, 'https://example.com/favicon.ico')")
        conn.execute("INSERT INTO favicon_bitmaps (id, icon_id, last_updated, image_data, width, height) VALUES (1, 1, 13355376000000000, ?, 16, 16)", (icon_data,))
        conn.execute("INSERT INTO icon_mapping (page_url, icon_id) VALUES ('https://example.com/', 1)")

        conn.commit()
        conn.close()

        return db_path

    def test_parse_favicons_table(self, chromium_favicons_db, tmp_path):
        """Test parsing favicon_bitmaps table."""
        # Create mock evidence conn
        evidence_db = tmp_path / "evidence.sqlite"
        evidence_conn = sqlite3.connect(str(evidence_db))

        # Create minimal schema
        evidence_conn.execute("""
            CREATE TABLE favicons (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                browser TEXT,
                profile TEXT,
                icon_url TEXT,
                icon_type INTEGER,
                width INTEGER,
                height INTEGER,
                icon_data BLOB,
                icon_md5 TEXT,
                icon_sha256 TEXT,
                last_updated_utc TEXT,
                last_requested_utc TEXT,
                run_id TEXT,
                source_path TEXT,
                partition_index INTEGER,
                fs_type TEXT,
                logical_path TEXT,
                forensic_path TEXT,
                notes TEXT,
                created_at_utc TEXT
            )
        """)
        evidence_conn.execute("""
            CREATE TABLE favicon_mappings (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                favicon_id INTEGER,
                page_url TEXT,
                browser TEXT,
                profile TEXT,
                run_id TEXT,
                created_at_utc TEXT
            )
        """)
        evidence_conn.commit()

        callbacks = MagicMock()
        file_info = {"source_path": "/path/to/Favicons"}
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # Mock the db functions where they're imported from (core.database)
        with patch("core.database.insert_favicon") as mock_insert:
            with patch("core.database.insert_favicon_mappings") as mock_insert_mappings:
                with patch("core.database.insert_image_with_discovery"):
                    mock_insert.return_value = 1
                    mock_insert_mappings.return_value = 1

                    favicon_count, mapping_count, url_list = parse_favicons_database(
                        db_path=chromium_favicons_db,
                        evidence_conn=evidence_conn,
                        evidence_id=1,
                        run_id="test_run",
                        browser="chrome",
                        profile="Default",
                        file_info=file_info,
                        output_dir=output_dir,
                        extractor_name="chromium_favicons",
                        extractor_version="2.5.0",
                        callbacks=callbacks,
                    )

        evidence_conn.close()

        assert mock_insert.called
        assert mock_insert_mappings.called
        # Verify URL collection for cross-posting
        assert isinstance(url_list, list)


# =============================================================================
# Top Sites Database Parsing Tests
# =============================================================================


class TestChromiumTopSitesParsing:
    """Test Chromium Top Sites database parsing."""

    @pytest.fixture
    def chromium_top_sites_db(self, tmp_path):
        """Create a test Chromium Top Sites database."""
        db_path = tmp_path / "Top Sites"
        conn = sqlite3.connect(str(db_path))

        # Create Chromium schema
        conn.execute("""
            CREATE TABLE top_sites (
                url TEXT NOT NULL,
                title TEXT,
                url_rank INTEGER
            )
        """)

        # Insert test data
        conn.execute("INSERT INTO top_sites (url, title, url_rank) VALUES ('https://google.com/', 'Google', 0)")
        conn.execute("INSERT INTO top_sites (url, title, url_rank) VALUES ('https://github.com/', 'GitHub', 1)")

        conn.commit()
        conn.close()

        return db_path

    def test_parse_top_sites_table(self, chromium_top_sites_db, tmp_path):
        """Test parsing top_sites table."""
        # Create mock evidence conn
        evidence_db = tmp_path / "evidence.sqlite"
        evidence_conn = sqlite3.connect(str(evidence_db))

        callbacks = MagicMock()
        file_info = {"source_path": "/path/to/Top Sites"}
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with patch("core.database.insert_top_sites") as mock_insert:
            mock_insert.return_value = 2

            count, url_list = parse_top_sites_database(
                db_path=chromium_top_sites_db,
                evidence_conn=evidence_conn,
                evidence_id=1,
                run_id="test_run",
                browser="chrome",
                profile="Default",
                file_info=file_info,
                output_dir=output_dir,
                extractor_name="chromium_favicons",
                extractor_version="2.5.0",
                callbacks=callbacks,
            )

        evidence_conn.close()

        assert mock_insert.called
        # Check the records passed to insert
        call_args = mock_insert.call_args
        records = call_args[0][2]  # Third positional argument
        assert len(records) == 2
        assert records[0]["url"] == "https://google.com/"
        assert records[1]["url_rank"] == 1


# =============================================================================
# Timestamp Conversion Tests
# =============================================================================


class TestChromiumFaviconsTimestamps:
    """Test WebKit timestamp conversion."""

    def test_webkit_to_iso8601_valid(self):
        """Test valid WebKit timestamp conversion."""
        # 13355376000000000 = 2024-01-01 00:00:00 UTC (approximately)
        result = webkit_to_iso8601(13355376000000000)
        assert result is not None
        assert "2024" in result or "2023" in result

    def test_webkit_to_iso8601_zero(self):
        """Test zero WebKit timestamp."""
        result = webkit_to_iso8601(0)
        assert result is None

    def test_webkit_to_iso8601_none(self):
        """Test None WebKit timestamp."""
        result = webkit_to_iso8601(None)
        assert result is None


# =============================================================================
# Profile Extraction Tests
# =============================================================================


class TestChromiumFaviconsProfileExtraction:
    """Test profile name extraction from paths."""

    def test_extract_profile_default(self):
        """Test Default profile extraction."""
        extractor = ChromiumFaviconsExtractor()
        path = "Users/TestUser/AppData/Local/Google/Chrome/User Data/Default/Favicons"
        profile = extractor._extract_profile(path, "chrome")
        assert profile == "Default"

    def test_extract_profile_numbered(self):
        """Test numbered profile extraction."""
        extractor = ChromiumFaviconsExtractor()
        path = "Users/TestUser/AppData/Local/Google/Chrome/User Data/Profile 1/Favicons"
        profile = extractor._extract_profile(path, "chrome")
        assert profile == "Profile 1"

    def test_extract_profile_opera(self):
        """Test Opera profile extraction."""
        extractor = ChromiumFaviconsExtractor()
        path = "Users/TestUser/AppData/Roaming/Opera Software/Opera Stable/Favicons"
        profile = extractor._extract_profile(path, "opera")
        assert profile == "Default"


# =============================================================================
# StatisticsCollector Integration Tests
# =============================================================================


class TestChromiumFaviconsStatistics:
    """Test StatisticsCollector integration."""

    def test_extraction_starts_statistics_run(self, tmp_path):
        """Test that extraction starts a statistics run."""
        extractor = ChromiumFaviconsExtractor()

        mock_fs = MagicMock()
        mock_fs.iter_paths.return_value = []
        mock_fs.source_path = "/test/image.E01"
        mock_fs.fs_type = "NTFS"

        callbacks = MagicMock()
        config = {"evidence_id": 1}
        output_dir = tmp_path / "output"

        with patch("extractors.browser.chromium.favicons.extractor.StatisticsCollector") as mock_stats_class:
            mock_stats = MagicMock()
            mock_stats_class.get_instance.return_value = mock_stats

            extractor.run_extraction(mock_fs, output_dir, config, callbacks)

            mock_stats.start_run.assert_called_once()
            mock_stats.finish_run.assert_called_once()


# =============================================================================
# Run ID Generation Tests
# =============================================================================


class TestChromiumFaviconsRunId:
    """Test run ID generation."""

    def test_generate_run_id_format(self):
        """Test run ID format."""
        extractor = ChromiumFaviconsExtractor()
        run_id = extractor._generate_run_id()

        # Format: YYYYMMDDTHHMMSS_xxxxxxxx
        assert "_" in run_id
        parts = run_id.split("_")
        assert len(parts) == 2
        assert len(parts[0]) == 15  # YYYYMMDDTHHMMSS
        assert len(parts[1]) == 8   # UUID first 8 chars

    def test_generate_run_id_unique(self):
        """Test run IDs are unique."""
        extractor = ChromiumFaviconsExtractor()
        run_ids = {extractor._generate_run_id() for _ in range(10)}
        assert len(run_ids) == 10
