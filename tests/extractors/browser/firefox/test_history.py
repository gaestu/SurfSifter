"""Tests for Firefox history extractor."""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extractors.browser.firefox.history import FirefoxHistoryExtractor
from extractors.browser.firefox._patterns import (
    FIREFOX_BROWSERS,
    FIREFOX_ARTIFACTS,
    get_patterns,
    get_all_patterns,
    get_browser_display_name,
    get_all_browsers,
    extract_profile_from_path,
    detect_browser_from_path,
)
from extractors.browser.firefox._parsers import (
    parse_history_visits,
    get_history_stats,
    FirefoxVisit,
    FirefoxHistoryStats,
)


# =============================================================================
# Pattern Tests
# =============================================================================


class TestFirefoxPatterns:
    """Tests for Firefox path patterns."""

    def test_firefox_browsers_structure(self):
        """Test FIREFOX_BROWSERS has expected keys."""
        assert "firefox" in FIREFOX_BROWSERS
        assert "firefox_esr" in FIREFOX_BROWSERS
        assert "tor" in FIREFOX_BROWSERS

        # Each browser has required fields
        for browser_key, browser_data in FIREFOX_BROWSERS.items():
            assert "display_name" in browser_data
            assert "profile_roots" in browser_data
            # firefox_esr is label-only (empty patterns, used for detect_browser_from_path())
            if browser_key != "firefox_esr":
                assert len(browser_data["profile_roots"]) > 0

    def test_firefox_esr_is_label_only(self):
        """Test firefox_esr has empty patterns (label-only entry)."""
        # firefox_esr exists for detect_browser_from_path() labeling only
        # All Firefox artifacts are discovered via "firefox" patterns
        assert "firefox_esr" in FIREFOX_BROWSERS
        assert FIREFOX_BROWSERS["firefox_esr"]["profile_roots"] == []
        assert FIREFOX_BROWSERS["firefox_esr"]["cache_roots"] == []
        # get_patterns should return empty list for label-only browser
        patterns = get_patterns("firefox_esr", "history")
        assert patterns == []

    def test_firefox_artifacts_structure(self):
        """Test FIREFOX_ARTIFACTS has expected keys."""
        expected_artifacts = ["history", "cookies", "bookmarks", "downloads", "autofill"]
        for artifact in expected_artifacts:
            assert artifact in FIREFOX_ARTIFACTS, f"Missing artifact: {artifact}"

    def test_get_patterns_firefox_history(self):
        """Test get_patterns returns correct paths for Firefox history."""
        patterns = get_patterns("firefox", "history")

        assert len(patterns) > 0

        # Check for places.sqlite patterns
        places_patterns = [p for p in patterns if "places.sqlite" in p]
        assert len(places_patterns) > 0

        # Check platform coverage
        windows = [p for p in patterns if "AppData" in p]
        macos = [p for p in patterns if "Library" in p]
        linux = [p for p in patterns if ".mozilla" in p]

        assert len(windows) > 0, "Missing Windows patterns"
        assert len(macos) > 0, "Missing macOS patterns"
        assert len(linux) > 0, "Missing Linux patterns"

    def test_get_patterns_invalid_browser(self):
        """Test get_patterns raises for invalid browser."""
        with pytest.raises(ValueError, match="Unknown browser"):
            get_patterns("invalid_browser", "history")

    def test_get_patterns_invalid_artifact(self):
        """Test get_patterns raises for invalid artifact."""
        with pytest.raises(ValueError, match="Unknown artifact"):
            get_patterns("firefox", "invalid_artifact")

    def test_get_all_patterns(self):
        """Test get_all_patterns returns patterns for all Firefox-family browsers with patterns."""
        all_patterns = get_all_patterns("history")

        # firefox_esr is label-only (empty patterns), so only firefox and tor contribute
        # All patterns should include firefox + tor (firefox_esr adds nothing, no duplicates)
        firefox_patterns = get_patterns("firefox", "history")
        tor_patterns = get_patterns("tor", "history")

        # All patterns should be superset of firefox patterns
        assert len(all_patterns) >= len(firefox_patterns)

        # Tor patterns should add unique paths (not overlapping with Firefox)
        # so total should be firefox + tor (minus any duplicates, which shouldn't exist)
        assert len(all_patterns) == len(firefox_patterns) + len(tor_patterns)

    def test_get_browser_display_name(self):
        """Test get_browser_display_name returns correct names."""
        assert get_browser_display_name("firefox") == "Mozilla Firefox"
        assert get_browser_display_name("tor") == "Tor Browser"
        assert get_browser_display_name("unknown") == "Unknown"  # Fallback

    def test_get_all_browsers(self):
        """Test get_all_browsers returns all supported browsers."""
        browsers = get_all_browsers()

        assert "firefox" in browsers
        assert "firefox_esr" in browsers
        assert "tor" in browsers

    def test_extract_profile_from_path_linux(self):
        """Test profile extraction from Linux paths."""
        path = "home/john/.mozilla/firefox/abc123.default-release/places.sqlite"
        profile = extract_profile_from_path(path)
        assert profile == "abc123.default-release"

    def test_extract_profile_from_path_windows(self):
        """Test profile extraction from Windows paths."""
        path = "Users/John/AppData/Roaming/Mozilla/Firefox/Profiles/xyz789.default/places.sqlite"
        profile = extract_profile_from_path(path)
        assert profile == "xyz789.default"

    def test_detect_browser_from_path_firefox(self):
        """Test browser detection returns firefox for standard paths."""
        path = "Users/John/AppData/Roaming/Mozilla/Firefox/Profiles/abc.default/places.sqlite"
        assert detect_browser_from_path(path) == "firefox"

    def test_detect_browser_from_path_tor(self):
        """Test browser detection returns tor for Tor Browser paths."""
        path = "Users/John/Desktop/Tor Browser/Browser/TorBrowser/Data/Browser/profile.default/places.sqlite"
        assert detect_browser_from_path(path) == "tor"

    def test_detect_browser_from_path_esr_profile_suffix(self):
        """Test browser detection returns firefox_esr for .default-esr profile."""
        path = "Users/John/AppData/Roaming/Mozilla/Firefox/Profiles/abc123.default-esr/places.sqlite"
        assert detect_browser_from_path(path) == "firefox_esr"

    def test_detect_browser_from_path_esr_linux(self):
        """Test browser detection returns firefox_esr for Linux ESR profile."""
        path = "home/john/.mozilla/firefox/xyz789.default-esr/places.sqlite"
        assert detect_browser_from_path(path) == "firefox_esr"

    def test_detect_browser_from_path_esr_installation(self):
        """Test browser detection returns firefox_esr for enterprise installation path."""
        path = "Program Files/Mozilla Firefox ESR/browser/features/places.sqlite"
        assert detect_browser_from_path(path) == "firefox_esr"

    def test_detect_browser_from_path_regular_default_release(self):
        """Test browser detection returns firefox for .default-release profile (not ESR)."""
        path = "Users/John/AppData/Roaming/Mozilla/Firefox/Profiles/abc123.default-release/places.sqlite"
        assert detect_browser_from_path(path) == "firefox"


# =============================================================================
# Parser Tests
# =============================================================================


class TestFirefoxParsers:
    """Tests for Firefox database parsers."""

    @pytest.fixture
    def places_db(self, tmp_path):
        """Create a mock places.sqlite database."""
        db_path = tmp_path / "places.sqlite"
        conn = sqlite3.connect(db_path)

        # Create schema (including frecency and hidden columns)
        conn.executescript("""
            CREATE TABLE moz_places (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                title TEXT,
                visit_count INTEGER DEFAULT 0,
                typed INTEGER DEFAULT 0,
                last_visit_date INTEGER,
                frecency INTEGER DEFAULT 0,
                hidden INTEGER DEFAULT 0
            );

            CREATE TABLE moz_historyvisits (
                id INTEGER PRIMARY KEY,
                place_id INTEGER NOT NULL,
                visit_date INTEGER,
                from_visit INTEGER DEFAULT 0,
                visit_type INTEGER DEFAULT 1,
                FOREIGN KEY (place_id) REFERENCES moz_places(id)
            );
        """)

        # Insert test data
        # PRTime is microseconds since 1970
        visit_time = 1704067200000000  # 2024-01-01 00:00:00 UTC

        conn.execute(
            "INSERT INTO moz_places (id, url, title, visit_count, typed, last_visit_date, frecency, hidden) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (1, "https://example.com", "Example", 5, 1, visit_time, 100, 0)
        )
        conn.execute(
            "INSERT INTO moz_places (id, url, title, visit_count, typed, last_visit_date, frecency, hidden) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (2, "https://test.org", "Test Site", 3, 0, visit_time + 1000000, 50, 1)
        )

        conn.execute(
            "INSERT INTO moz_historyvisits (place_id, visit_date, from_visit, visit_type) VALUES (?, ?, ?, ?)",
            (1, visit_time, 0, 1)
        )
        conn.execute(
            "INSERT INTO moz_historyvisits (place_id, visit_date, from_visit, visit_type) VALUES (?, ?, ?, ?)",
            (1, visit_time + 500000, 0, 2)
        )
        conn.execute(
            "INSERT INTO moz_historyvisits (place_id, visit_date, from_visit, visit_type) VALUES (?, ?, ?, ?)",
            (2, visit_time + 1000000, 1, 1)
        )

        conn.commit()
        conn.close()

        return db_path

    def test_parse_history_visits_returns_visits(self, places_db):
        """Test parse_history_visits returns visit records."""
        visits = list(parse_history_visits(places_db))

        assert len(visits) == 3

    def test_parse_history_visits_dataclass_fields(self, places_db):
        """Test parsed visits have correct dataclass fields."""
        visits = list(parse_history_visits(places_db))
        visit = visits[0]

        assert isinstance(visit, FirefoxVisit)
        assert hasattr(visit, "url")
        assert hasattr(visit, "title")
        assert hasattr(visit, "visit_time_utc")
        assert hasattr(visit, "visit_count")
        assert hasattr(visit, "typed")
        assert hasattr(visit, "from_visit")
        assert hasattr(visit, "visit_type")
        assert hasattr(visit, "visit_date_raw")

    def test_parse_history_visits_timestamp_conversion(self, places_db):
        """Test PRTime timestamps are converted to ISO 8601."""
        visits = list(parse_history_visits(places_db))
        visit = visits[0]

        # Should be ISO format
        assert visit.visit_time_utc is not None
        assert "2024-01-01" in visit.visit_time_utc

    def test_parse_history_visits_sorted_by_date(self, places_db):
        """Test visits are sorted by date descending."""
        visits = list(parse_history_visits(places_db))

        # Most recent visit should be first
        assert visits[0].visit_date_raw >= visits[-1].visit_date_raw

    def test_parse_history_visits_empty_db(self, tmp_path):
        """Test parse_history_visits handles empty database with no rows."""
        db_path = tmp_path / "empty.sqlite"
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE moz_places (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                title TEXT,
                visit_count INTEGER DEFAULT 0,
                typed INTEGER DEFAULT 0,
                last_visit_date INTEGER,
                frecency INTEGER DEFAULT 0,
                hidden INTEGER DEFAULT 0
            );
            CREATE TABLE moz_historyvisits (
                id INTEGER PRIMARY KEY,
                place_id INTEGER NOT NULL,
                visit_date INTEGER,
                from_visit INTEGER DEFAULT 0,
                visit_type INTEGER DEFAULT 1
            );
        """)
        conn.close()

        visits = list(parse_history_visits(db_path))
        assert visits == []

    def test_parse_history_visits_missing_tables(self, tmp_path):
        """Test parse_history_visits handles missing tables."""
        db_path = tmp_path / "invalid.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE other (id INTEGER)")
        conn.close()

        visits = list(parse_history_visits(db_path))
        assert visits == []

    def test_get_history_stats(self, places_db):
        """Test get_history_stats returns correct counts."""
        stats = get_history_stats(places_db)

        assert isinstance(stats, FirefoxHistoryStats)
        assert stats.visit_count == 3
        assert stats.unique_urls == 2
        assert stats.earliest_visit is not None
        assert stats.latest_visit is not None

    def test_parse_history_visits_extended_fields(self, places_db):
        """Test parsed visits include frecency, hidden, and visit_type_label."""
        visits = list(parse_history_visits(places_db))
        visit = visits[0]

        # New fields from
        assert hasattr(visit, "frecency")
        assert hasattr(visit, "hidden")
        assert hasattr(visit, "visit_type_label")
        assert hasattr(visit, "typed_input")

    def test_parse_history_visits_frecency_values(self, places_db):
        """Test frecency values are correctly extracted."""
        visits = list(parse_history_visits(places_db))

        # Find visit for example.com (frecency=100)
        example_visits = [v for v in visits if "example.com" in v.url]
        assert len(example_visits) > 0
        assert example_visits[0].frecency == 100

        # Find visit for test.org (frecency=50)
        test_visits = [v for v in visits if "test.org" in v.url]
        assert len(test_visits) > 0
        assert test_visits[0].frecency == 50

    def test_parse_history_visits_hidden_flag(self, places_db):
        """Test hidden flag is correctly extracted."""
        visits = list(parse_history_visits(places_db))

        # example.com has hidden=0, test.org has hidden=1
        example_visits = [v for v in visits if "example.com" in v.url]
        assert example_visits[0].hidden is False

        test_visits = [v for v in visits if "test.org" in v.url]
        assert test_visits[0].hidden is True

    def test_parse_history_visits_visit_type_label(self, places_db):
        """Test visit_type is mapped to human-readable label."""
        visits = list(parse_history_visits(places_db))

        # First visit has visit_type=1 (link)
        link_visits = [v for v in visits if v.visit_type == 1]
        assert len(link_visits) > 0
        assert link_visits[0].visit_type_label == "link"

        # Second visit has visit_type=2 (typed)
        typed_visits = [v for v in visits if v.visit_type == 2]
        assert len(typed_visits) > 0
        assert typed_visits[0].visit_type_label == "typed"

    def test_parse_history_visits_with_inputhistory(self, tmp_path):
        """Test moz_inputhistory data is captured when available."""
        db_path = tmp_path / "places_with_input.sqlite"
        conn = sqlite3.connect(db_path)

        # Create schema with moz_inputhistory
        conn.executescript("""
            CREATE TABLE moz_places (
                id INTEGER PRIMARY KEY,
                url TEXT NOT NULL,
                title TEXT,
                visit_count INTEGER DEFAULT 0,
                typed INTEGER DEFAULT 0,
                last_visit_date INTEGER,
                frecency INTEGER DEFAULT 0,
                hidden INTEGER DEFAULT 0
            );

            CREATE TABLE moz_historyvisits (
                id INTEGER PRIMARY KEY,
                place_id INTEGER NOT NULL,
                visit_date INTEGER,
                from_visit INTEGER DEFAULT 0,
                visit_type INTEGER DEFAULT 1
            );

            CREATE TABLE moz_inputhistory (
                place_id INTEGER NOT NULL,
                input TEXT NOT NULL,
                use_count INTEGER DEFAULT 0,
                PRIMARY KEY (place_id, input)
            );
        """)

        visit_time = 1704067200000000
        conn.execute(
            "INSERT INTO moz_places (id, url, title, visit_count, typed, last_visit_date, frecency) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, "https://github.com/mozilla/firefox", "Firefox Repository", 10, 1, visit_time, 200)
        )
        conn.execute(
            "INSERT INTO moz_historyvisits (place_id, visit_date, visit_type) VALUES (?, ?, ?)",
            (1, visit_time, 2)  # typed visit
        )
        # User typed "github firefox" to find this URL
        conn.execute(
            "INSERT INTO moz_inputhistory (place_id, input, use_count) VALUES (?, ?, ?)",
            (1, "github firefox", 5)
        )
        conn.commit()
        conn.close()

        visits = list(parse_history_visits(db_path))
        assert len(visits) == 1
        assert visits[0].typed_input == "github firefox"


# =============================================================================
# Visit Type Mapping Tests
# =============================================================================


class TestVisitTypeMapping:
    """Tests for Firefox visit_type to label mapping."""

    def test_visit_type_mapping_completeness(self):
        """Test all known visit types are mapped."""
        from extractors.browser.firefox._parsers import FIREFOX_VISIT_TYPES, get_visit_type_label

        # Firefox defines visit types 1-9
        expected_types = {1, 2, 3, 4, 5, 6, 7, 8, 9}
        assert set(FIREFOX_VISIT_TYPES.keys()) == expected_types

    def test_visit_type_mapping_values(self):
        """Test visit type mapping returns expected labels."""
        from extractors.browser.firefox._parsers import get_visit_type_label

        assert get_visit_type_label(1) == "link"
        assert get_visit_type_label(2) == "typed"
        assert get_visit_type_label(3) == "bookmark"
        assert get_visit_type_label(4) == "embed"
        assert get_visit_type_label(5) == "redirect_permanent"
        assert get_visit_type_label(6) == "redirect_temporary"
        assert get_visit_type_label(7) == "download"
        assert get_visit_type_label(8) == "framed_link"
        assert get_visit_type_label(9) == "reload"

    def test_visit_type_unknown_fallback(self):
        """Test unknown visit types return fallback label."""
        from extractors.browser.firefox._parsers import get_visit_type_label

        assert get_visit_type_label(0) == "unknown_0"
        assert get_visit_type_label(99) == "unknown_99"
        assert get_visit_type_label(-1) == "unknown_-1"


# =============================================================================
# Extractor Tests
# =============================================================================


class TestFirefoxHistoryExtractor:
    """Tests for FirefoxHistoryExtractor class."""

    def test_extractor_metadata(self):
        """Test extractor has correct metadata."""
        extractor = FirefoxHistoryExtractor()
        meta = extractor.metadata

        assert meta.name == "firefox_history"
        assert meta.display_name == "Firefox History"
        assert meta.category == "browser"
        assert meta.version
        assert "." in meta.version
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_with_fs(self):
        """Test can_run_extraction returns True with filesystem."""
        extractor = FirefoxHistoryExtractor()
        mock_fs = MagicMock()

        can_run, reason = extractor.can_run_extraction(mock_fs)

        assert can_run is True
        assert reason == ""

    def test_can_run_extraction_without_fs(self):
        """Test can_run_extraction returns False without filesystem."""
        extractor = FirefoxHistoryExtractor()

        can_run, reason = extractor.can_run_extraction(None)

        assert can_run is False
        assert "No evidence filesystem" in reason

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        """Test can_run_ingestion returns True with manifest."""
        extractor = FirefoxHistoryExtractor()
        (tmp_path / "manifest.json").write_text("{}")

        can_run, reason = extractor.can_run_ingestion(tmp_path)

        assert can_run is True
        assert reason == ""

    def test_can_run_ingestion_without_manifest(self, tmp_path):
        """Test can_run_ingestion returns False without manifest."""
        extractor = FirefoxHistoryExtractor()

        can_run, reason = extractor.can_run_ingestion(tmp_path)

        assert can_run is False
        assert "No manifest.json" in reason

    def test_has_existing_output(self, tmp_path):
        """Test has_existing_output detects manifest."""
        extractor = FirefoxHistoryExtractor()

        assert extractor.has_existing_output(tmp_path) is False

        (tmp_path / "manifest.json").write_text("{}")

        assert extractor.has_existing_output(tmp_path) is True

    def test_get_output_dir(self, tmp_path):
        """Test get_output_dir returns correct path."""
        extractor = FirefoxHistoryExtractor()

        output_dir = extractor.get_output_dir(tmp_path, "evidence_001")

        assert output_dir == tmp_path / "evidences" / "evidence_001" / "firefox_history"

    def test_run_extraction_creates_manifest(self, tmp_path):
        """Test extraction creates manifest.json."""
        extractor = FirefoxHistoryExtractor()
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
        assert manifest["extractor"] == "firefox_history"
        assert "run_id" in manifest

    def test_run_extraction_discovers_files(self, tmp_path):
        """Test extraction discovers Firefox history files."""
        extractor = FirefoxHistoryExtractor()
        output_dir = tmp_path / "output"

        mock_fs = MagicMock()
        mock_fs.iter_paths.side_effect = lambda pattern: (
            ["Users/John/AppData/Roaming/Mozilla/Firefox/Profiles/abc.default/places.sqlite"]
            if "places.sqlite" in pattern else []
        )
        mock_fs.read_file.return_value = b"mock database content"
        mock_fs.source_path = "/test/image.e01"
        mock_fs.fs_type = "NTFS"
        mock_fs.partition_index = 0

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        with patch.object(extractor, '_get_statistics_collector', return_value=None):
            result = extractor.run_extraction(
                mock_fs,
                output_dir,
                {"evidence_id": 1, "browsers": ["firefox"]},
                callbacks
            )

        assert result is True

        manifest = json.loads((output_dir / "manifest.json").read_text())
        assert len(manifest["files"]) == 1
        assert manifest["files"][0]["browser"] == "firefox"
