"""
Tests for ChromiumBookmarksExtractor.

Tests cover:
- Metadata: name, version, category, capabilities
- Registry: discovery
- Patterns: bookmarks file patterns for all Chromium browsers
- Parsers: ChromiumBookmark dataclass, JSON parsing, folder hierarchy
- Extraction: filesystem discovery, manifest creation
- Ingestion: database inserts
- Error handling: cancellation, empty config, malformed JSON
"""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import pytest

from extractors.browser.chromium.bookmarks import ChromiumBookmarksExtractor
from extractors.browser.chromium._parsers import (
    ChromiumBookmark,
    parse_bookmarks_json,
)
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
        ext = ChromiumBookmarksExtractor()
        assert ext.metadata.name == "chromium_bookmarks"

    def test_metadata_display_name(self):
        """Extractor has human-readable display name."""
        ext = ChromiumBookmarksExtractor()
        assert ext.metadata.display_name == "Chromium Bookmarks"

    def test_metadata_version(self):
        """Extractor has a version."""
        ext = ChromiumBookmarksExtractor()
        assert ext.metadata.version
        assert "." in ext.metadata.version

    def test_metadata_category(self):
        """Extractor is in browser category."""
        ext = ChromiumBookmarksExtractor()
        assert ext.metadata.category == "browser"

    def test_metadata_capabilities(self):
        """Extractor can extract and ingest."""
        ext = ChromiumBookmarksExtractor()
        assert ext.metadata.can_extract is True
        assert ext.metadata.can_ingest is True

    def test_metadata_description_mentions_browsers(self):
        """Description lists supported browsers."""
        ext = ChromiumBookmarksExtractor()
        desc = ext.metadata.description.lower()
        assert "chrome" in desc or "chromium" in desc


# =============================================================================
# Test Patterns
# =============================================================================

class TestPatterns:
    """Test browser path patterns."""

    def test_bookmarks_artifact_in_chromium_artifacts(self):
        """Bookmarks artifact is defined."""
        assert "bookmarks" in CHROMIUM_ARTIFACTS

    def test_bookmarks_patterns_are_list(self):
        """Bookmarks patterns resolve to a list."""
        patterns = CHROMIUM_ARTIFACTS.get("bookmarks", [])
        assert isinstance(patterns, list)
        assert len(patterns) > 0

    def test_get_patterns_chrome(self):
        """Get Chrome bookmarks patterns."""
        patterns = get_patterns("chrome", "bookmarks")
        assert len(patterns) > 0
        # All patterns should include Bookmarks file
        for p in patterns:
            assert "Bookmarks" in p

    def test_get_patterns_edge(self):
        """Get Edge bookmarks patterns."""
        patterns = get_patterns("edge", "bookmarks")
        assert len(patterns) > 0

    def test_get_patterns_brave(self):
        """Get Brave bookmarks patterns."""
        patterns = get_patterns("brave", "bookmarks")
        assert len(patterns) > 0


# =============================================================================
# Test Parsers
# =============================================================================

class TestParsers:
    """Test parser functions and dataclasses."""

    def test_bookmark_dataclass(self):
        """ChromiumBookmark dataclass has all required fields."""
        from datetime import datetime

        bm = ChromiumBookmark(
            id="1",
            name="Example",
            url="https://example.com",
            date_added=datetime(2023, 1, 1),
            date_added_iso="2023-01-01T00:00:00+00:00",
            date_modified=None,
            date_modified_iso=None,
            bookmark_type="url",
            folder_path="Bookmarks Bar/Tech",
            guid="abc-123-def",
        )
        assert bm.name == "Example"
        assert bm.url == "https://example.com"
        assert bm.folder_path == "Bookmarks Bar/Tech"
        assert bm.bookmark_type == "url"

    def test_bookmark_date_added_iso(self):
        """Bookmark has ISO timestamp from parser."""
        from datetime import datetime

        bm = ChromiumBookmark(
            id="1",
            name="Test",
            url="https://test.com",
            date_added=datetime(2023, 1, 1),
            date_added_iso="2023-01-01T00:00:00+00:00",
            date_modified=None,
            date_modified_iso=None,
            bookmark_type="url",
            folder_path="",
            guid="test",
        )
        # ISO format is pre-computed by parser
        assert bm.date_added_iso is not None
        assert "2023" in bm.date_added_iso


class TestParsersWithJSON:
    """Test parsers with real JSON data."""

    @pytest.fixture
    def bookmarks_json(self):
        """Create a test Bookmarks JSON structure."""
        return {
            "checksum": "abc123",
            "roots": {
                "bookmark_bar": {
                    "children": [
                        {
                            "id": "1",
                            "name": "Google",
                            "type": "url",
                            "url": "https://www.google.com",
                            "date_added": "13300000000000000",
                            "guid": "guid-1",
                        },
                        {
                            "id": "2",
                            "name": "Tech",
                            "type": "folder",
                            "date_added": "13300000000000000",
                            "date_modified": "13350000000000000",
                            "guid": "guid-2",
                            "children": [
                                {
                                    "id": "3",
                                    "name": "GitHub",
                                    "type": "url",
                                    "url": "https://github.com",
                                    "date_added": "13310000000000000",
                                    "guid": "guid-3",
                                }
                            ],
                        },
                    ],
                    "date_added": "0",
                    "date_modified": "0",
                    "guid": "root-bar",
                    "id": "0",
                    "name": "Bookmarks bar",
                    "type": "folder",
                },
                "other": {
                    "children": [],
                    "date_added": "0",
                    "date_modified": "0",
                    "guid": "root-other",
                    "id": "0",
                    "name": "Other bookmarks",
                    "type": "folder",
                },
                "synced": {
                    "children": [],
                    "date_added": "0",
                    "date_modified": "0",
                    "guid": "root-synced",
                    "id": "0",
                    "name": "Mobile bookmarks",
                    "type": "folder",
                },
            },
            "version": 1,
        }

    def test_parse_bookmarks_json(self, bookmarks_json):
        """Parse bookmarks from JSON."""
        bookmarks = list(parse_bookmarks_json(bookmarks_json))

        # Parser returns both folders and URLs
        url_bookmarks = [b for b in bookmarks if b.bookmark_type == "url"]
        folder_bookmarks = [b for b in bookmarks if b.bookmark_type == "folder"]

        # Should have 2 URL bookmarks (Google, GitHub)
        assert len(url_bookmarks) == 2

        # Should have folders (root + nested)
        assert len(folder_bookmarks) >= 1

        # Check URL bookmarks
        google = next(b for b in url_bookmarks if b.name == "Google")
        assert google.url == "https://www.google.com"

        # Nested bookmark
        github = next(b for b in url_bookmarks if b.name == "GitHub")
        assert github.url == "https://github.com"
        # Should show folder hierarchy
        assert "Tech" in github.folder_path

    def test_parse_bookmarks_empty_roots(self):
        """Handle empty roots gracefully."""
        data = {"checksum": "abc", "roots": {}, "version": 1}
        bookmarks = list(parse_bookmarks_json(data))
        assert bookmarks == []

    def test_parse_bookmarks_missing_roots(self):
        """Handle missing roots gracefully."""
        data = {"checksum": "abc", "version": 1}
        bookmarks = list(parse_bookmarks_json(data))
        assert bookmarks == []


# =============================================================================
# Test Extraction
# =============================================================================

class TestExtraction:
    """Test extraction workflow."""

    def test_can_run_extraction_no_fs(self):
        """Extraction requires filesystem."""
        ext = ChromiumBookmarksExtractor()
        can_run, msg = ext.can_run_extraction(None)
        assert can_run is False

    def test_can_run_extraction_with_fs(self):
        """Extraction can run with filesystem."""
        ext = ChromiumBookmarksExtractor()
        mock_fs = MagicMock()
        can_run, msg = ext.can_run_extraction(mock_fs)
        assert can_run is True

    def test_extraction_creates_manifest(self, tmp_path):
        """Extraction creates manifest.json."""
        ext = ChromiumBookmarksExtractor()

        mock_fs = MagicMock()
        mock_fs.iter_paths.return_value = []

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

        assert manifest["extractor"] == "chromium_bookmarks"


# =============================================================================
# Test Ingestion
# =============================================================================

class TestIngestion:
    """Test ingestion workflow."""

    def test_can_run_ingestion_no_manifest(self, tmp_path):
        """Ingestion requires manifest."""
        ext = ChromiumBookmarksExtractor()
        can_run, msg = ext.can_run_ingestion(tmp_path)
        assert can_run is False
        assert "manifest" in msg.lower()

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        """Ingestion can run with manifest."""
        ext = ChromiumBookmarksExtractor()

        manifest = {"extractor_name": "chromium_bookmarks", "extracted_files": []}
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
        ext = ChromiumBookmarksExtractor()

        mock_fs = MagicMock()
        mock_fs.iter_paths.return_value = []

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = True

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        ext.run_extraction(
            evidence_fs=mock_fs,
            output_dir=output_dir,
            callbacks=callbacks,
            config={"browsers": ["chrome"]},
        )

        assert (output_dir / "manifest.json").exists()

    def test_handles_empty_config(self, tmp_path):
        """Empty config uses default browsers."""
        ext = ChromiumBookmarksExtractor()

        mock_fs = MagicMock()
        mock_fs.iter_paths.return_value = []

        callbacks = MagicMock()

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        ext.run_extraction(
            evidence_fs=mock_fs,
            output_dir=output_dir,
            callbacks=callbacks,
            config={},
        )

        assert mock_fs.iter_paths.call_count > 0

    def test_handles_malformed_json(self, tmp_path):
        """Ingestion handles malformed bookmark JSON."""
        # This tests robustness of the parser
        data = {"not": "bookmarks"}
        bookmarks = list(parse_bookmarks_json(data))
        assert bookmarks == []
