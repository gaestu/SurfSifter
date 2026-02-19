"""
Tests for Safari History Extractor.

Tests cover:
- Safari patterns (_patterns.py)
- Cocoa timestamp parsing (_parsers.py)
- SafariHistoryExtractor metadata and methods
- Registry discovery
"""

import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extractors.browser.safari._patterns import (
    SAFARI_BROWSERS,
    SAFARI_ARTIFACTS,
    get_patterns,
    get_all_patterns,
    get_browser_display_name,
    extract_user_from_path,
    is_safari_path,
)
from extractors.browser.safari._parsers import (
    COCOA_EPOCH_OFFSET,
    cocoa_to_datetime,
    cocoa_to_iso,
    parse_history_visits,
    get_history_stats,
    SafariVisit,
)
from extractors.browser.safari.history import SafariHistoryExtractor


# =============================================================================
# Pattern Tests
# =============================================================================

class TestSafariPatterns:
    """Test Safari pattern definitions."""

    def test_safari_browsers_structure(self):
        """SAFARI_BROWSERS has expected structure."""
        assert "safari" in SAFARI_BROWSERS
        safari = SAFARI_BROWSERS["safari"]
        assert "display_name" in safari
        assert "engine" in safari
        assert "profile_roots" in safari
        assert "cookies_roots" in safari
        assert "cache_roots" in safari

    def test_safari_artifacts_structure(self):
        """SAFARI_ARTIFACTS has expected artifact types."""
        expected_artifacts = ["history", "cookies", "bookmarks", "downloads", "cache"]
        for artifact in expected_artifacts:
            assert artifact in SAFARI_ARTIFACTS

    def test_get_patterns_history(self):
        """get_patterns returns history patterns."""
        patterns = get_patterns("history")
        assert len(patterns) > 0
        assert any("History.db" in p for p in patterns)

    def test_get_patterns_cookies(self):
        """get_patterns returns cookies patterns."""
        patterns = get_patterns("cookies")
        assert len(patterns) > 0
        assert any("binarycookies" in p for p in patterns)

    def test_get_patterns_bookmarks(self):
        """get_patterns returns bookmarks patterns."""
        patterns = get_patterns("bookmarks")
        assert len(patterns) > 0
        assert any("Bookmarks.plist" in p for p in patterns)

    def test_get_patterns_downloads(self):
        """get_patterns returns downloads patterns."""
        patterns = get_patterns("downloads")
        assert len(patterns) > 0
        assert any("Downloads.plist" in p for p in patterns)

    def test_get_patterns_favicons_include_profiles(self):
        """Safari favicon patterns include Safari 17+ multi-profile roots."""
        patterns = get_patterns("favicons")
        assert any("Profiles/*/Favicon Cache/*" in p for p in patterns)
        assert any("Profiles/*/Touch Icons Cache/*" in p for p in patterns)
        assert any("Profiles/*/Template Icons/*" in p for p in patterns)

    def test_get_patterns_invalid_artifact(self):
        """get_patterns raises ValueError for unknown artifact."""
        with pytest.raises(ValueError, match="Unknown Safari artifact"):
            get_patterns("nonexistent")

    def test_get_all_patterns(self):
        """get_all_patterns returns dict of all patterns."""
        all_patterns = get_all_patterns()
        assert isinstance(all_patterns, dict)
        assert "history" in all_patterns
        assert "cookies" in all_patterns

    def test_get_browser_display_name(self):
        """get_browser_display_name returns Safari name."""
        name = get_browser_display_name()
        assert "Safari" in name

    def test_extract_user_from_path_macos(self):
        """extract_user_from_path extracts macOS username."""
        path = "Users/johndoe/Library/Safari/History.db"
        assert extract_user_from_path(path) == "johndoe"

    def test_extract_user_from_path_no_user(self):
        """extract_user_from_path returns None for system paths."""
        path = "/Library/Safari/History.db"
        assert extract_user_from_path(path) is None

    def test_is_safari_path_true(self):
        """is_safari_path detects Safari paths."""
        assert is_safari_path("Users/test/Library/Safari/History.db") is True
        assert is_safari_path("Library/Caches/com.apple.Safari/Cache.db") is True
        assert is_safari_path("Cookies.binarycookies") is True

    def test_is_safari_path_false(self):
        """is_safari_path returns False for non-Safari paths."""
        assert is_safari_path("Users/test/AppData/Chrome/History") is False


# =============================================================================
# Parser Tests
# =============================================================================

class TestSafariParsers:
    """Test Safari history parsers."""

    def test_cocoa_to_datetime_basic(self):
        """cocoa_to_datetime converts valid timestamps."""
        # Cocoa epoch is 2001-01-01 00:00:00 UTC
        result = cocoa_to_datetime(0)
        assert result is not None
        assert result.year == 2001
        assert result.month == 1
        assert result.day == 1

    def test_cocoa_to_datetime_recent(self):
        """cocoa_to_datetime handles recent dates."""
        # 759398400 seconds after 2001-01-01 = ~2025
        result = cocoa_to_datetime(759398400)
        assert result is not None
        assert result.year == 2025

    def test_cocoa_to_datetime_none(self):
        """cocoa_to_datetime returns None for invalid input."""
        assert cocoa_to_datetime(None) is None
        # Note: 0 is a valid Cocoa timestamp (Jan 1, 2001 00:00:00 UTC)

    def test_cocoa_to_iso_basic(self):
        """cocoa_to_iso returns ISO string."""
        result = cocoa_to_iso(0)
        assert result is not None
        assert "2001-01-01" in result

    def test_cocoa_to_iso_none(self):
        """cocoa_to_iso returns None for invalid input."""
        assert cocoa_to_iso(None) is None

    def test_parse_history_visits_returns_visits(self, tmp_path):
        """parse_history_visits returns SafariVisit objects."""
        db_path = tmp_path / "History.db"

        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE history_items (
                id INTEGER PRIMARY KEY,
                url TEXT,
                domain_expansion TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE history_visits (
                id INTEGER PRIMARY KEY,
                history_item INTEGER,
                visit_time REAL,
                title TEXT,
                redirect_source INTEGER,
                redirect_destination INTEGER
            )
        """)

        # Insert test data (Cocoa timestamp for ~2024)
        conn.execute("INSERT INTO history_items (id, url) VALUES (1, 'https://example.com/')")
        conn.execute("""
            INSERT INTO history_visits (history_item, visit_time, title)
            VALUES (1, 730000000, 'Example')
        """)
        conn.commit()
        conn.close()

        visits = parse_history_visits(db_path)
        assert len(visits) == 1
        assert visits[0].url == "https://example.com/"
        assert visits[0].title == "Example"

    def test_parse_history_visits_empty_db(self, tmp_path):
        """parse_history_visits returns empty for empty tables."""
        db_path = tmp_path / "History.db"

        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE history_items (
                id INTEGER PRIMARY KEY,
                url TEXT,
                domain_expansion TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE history_visits (
                id INTEGER PRIMARY KEY,
                history_item INTEGER,
                visit_time REAL,
                title TEXT,
                redirect_source INTEGER,
                redirect_destination INTEGER
            )
        """)
        conn.commit()
        conn.close()

        visits = parse_history_visits(db_path)
        assert visits == []

    def test_parse_history_visits_missing_tables(self, tmp_path):
        """parse_history_visits handles missing tables."""
        db_path = tmp_path / "History.db"

        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE other_table (id INTEGER)")
        conn.commit()
        conn.close()

        visits = parse_history_visits(db_path)
        assert visits == []

    def test_get_history_stats_with_data(self):
        """get_history_stats calculates correct statistics."""
        dt = datetime(2024, 1, 15, tzinfo=timezone.utc)
        visits = [
            SafariVisit(
                url="https://example.com/",
                title="Example",
                visit_time=dt,
                visit_time_utc=dt.isoformat(),
                redirect_source=None,
                redirect_destination=None,
                history_item_id=1,
            ),
            SafariVisit(
                url="https://example.com/page",
                title="Page",
                visit_time=dt,
                visit_time_utc=dt.isoformat(),
                redirect_source=None,
                redirect_destination=None,
                history_item_id=2,
            ),
        ]

        stats = get_history_stats(visits)
        assert stats["total_visits"] == 2
        assert stats["unique_urls"] == 2

    def test_get_history_stats_empty(self):
        """get_history_stats handles empty list."""
        stats = get_history_stats([])
        assert stats["total_visits"] == 0
        assert stats["unique_urls"] == 0


# =============================================================================
# Extractor Tests
# =============================================================================

class TestSafariHistoryExtractor:
    """Test SafariHistoryExtractor class."""

    def test_extractor_metadata(self):
        """Extractor has correct metadata."""
        extractor = SafariHistoryExtractor()
        meta = extractor.metadata

        assert meta.name == "safari_history"
        assert "Safari" in meta.display_name
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_with_fs(self):
        """can_run_extraction returns True with evidence filesystem."""
        extractor = SafariHistoryExtractor()
        mock_fs = MagicMock()

        can_run, msg = extractor.can_run_extraction(mock_fs)
        assert can_run is True

    def test_can_run_extraction_without_fs(self):
        """can_run_extraction returns False without evidence filesystem."""
        extractor = SafariHistoryExtractor()

        can_run, msg = extractor.can_run_extraction(None)
        assert can_run is False
        assert "No evidence" in msg

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        """can_run_ingestion returns True when manifest exists."""
        extractor = SafariHistoryExtractor()

        manifest = tmp_path / "manifest.json"
        manifest.write_text('{"run_id": "test"}')

        can_run, msg = extractor.can_run_ingestion(tmp_path)
        assert can_run is True

    def test_can_run_ingestion_without_manifest(self, tmp_path):
        """can_run_ingestion returns False without manifest."""
        extractor = SafariHistoryExtractor()

        can_run, msg = extractor.can_run_ingestion(tmp_path)
        assert can_run is False
        assert "manifest" in msg.lower()

    def test_has_existing_output(self, tmp_path):
        """has_existing_output detects manifest."""
        extractor = SafariHistoryExtractor()

        assert extractor.has_existing_output(tmp_path) is False

        manifest = tmp_path / "manifest.json"
        manifest.write_text('{}')

        assert extractor.has_existing_output(tmp_path) is True

    def test_get_output_dir(self, tmp_path):
        """get_output_dir returns correct path."""
        extractor = SafariHistoryExtractor()

        output = extractor.get_output_dir(tmp_path, "evidence1")
        assert "safari_history" in str(output)
        assert "evidence1" in str(output)

    def test_run_extraction_creates_manifest(self, tmp_path):
        """run_extraction creates manifest file."""
        extractor = SafariHistoryExtractor()
        output_dir = tmp_path / "output"

        # Mock filesystem with no files
        mock_fs = MagicMock()
        mock_fs.iter_paths = MagicMock(return_value=[])

        callbacks = MagicMock()
        config = {"evidence_id": 1, "evidence_label": "test"}

        result = extractor.run_extraction(mock_fs, output_dir, config, callbacks)

        assert result is True
        assert (output_dir / "manifest.json").exists()


# =============================================================================
# Dual-Write Tests
# =============================================================================


class TestSafariHistoryDualWrite:
    """Tests for dual-write to urls table."""

    def test_extractor_imports_insert_urls(self):
        """Test that extractor imports insert_urls."""
        import inspect
        from extractors.browser.safari.history import extractor as module

        source = inspect.getsource(module)
        assert "insert_urls" in source
        assert "insert_urls" in source  # multi-line import block

    def test_ingestion_collects_url_records(self):
        """Test that run_ingestion collects URLs for dual-write."""
        import inspect
        from extractors.browser.safari.history.extractor import SafariHistoryExtractor

        source = inspect.getsource(SafariHistoryExtractor.run_ingestion)

        # Should have url_records list
        assert "url_records" in source
        # Should collect URLs with proper schema
        assert "domain" in source
        assert "scheme" in source
        assert "context" in source
        assert "first_seen_utc" in source
        # Should cross-post to urls table
        assert "insert_urls" in source
        assert "Cross-posted" in source

    def test_url_record_has_history_context(self):
        """Test that URL records have history context provenance."""
        import inspect
        from extractors.browser.safari.history.extractor import SafariHistoryExtractor

        source = inspect.getsource(SafariHistoryExtractor.run_ingestion)

        # Should have history context for Safari
        assert 'context": f"history:safari' in source or "context\": f\"history:safari" in source
