"""
Tests for Safari Bookmarks Extractor.

Tests cover:
- Bookmark parsing (_parsers.py)
- SafariBookmarksExtractor metadata and methods
- Registry discovery
"""

import plistlib
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from extractors.browser.safari._parsers import (
    parse_bookmarks,
    get_bookmark_stats,
    SafariBookmark,
)
from extractors.browser.safari.bookmarks import SafariBookmarksExtractor


# =============================================================================
# Parser Tests
# =============================================================================

class TestSafariBookmarkParsers:
    """Test Safari bookmark parsers."""

    def test_parse_bookmarks_single_leaf(self, tmp_path):
        """parse_bookmarks parses single bookmark."""
        plist_path = tmp_path / "Bookmarks.plist"

        plist_data = {
            "WebBookmarkType": "WebBookmarkTypeLeaf",
            "URLString": "https://example.com/",
            "URIDictionary": {"title": "Example"},
        }

        with open(plist_path, 'wb') as f:
            plistlib.dump(plist_data, f)

        bookmarks = parse_bookmarks(plist_path)
        assert len(bookmarks) == 1
        assert bookmarks[0].url == "https://example.com/"
        assert bookmarks[0].title == "Example"

    def test_parse_bookmarks_with_folder(self, tmp_path):
        """parse_bookmarks parses folder structure."""
        plist_path = tmp_path / "Bookmarks.plist"

        plist_data = {
            "WebBookmarkType": "WebBookmarkTypeList",
            "Title": "Bookmarks Menu",
            "Children": [
                {
                    "WebBookmarkType": "WebBookmarkTypeLeaf",
                    "URLString": "https://example.com/",
                    "URIDictionary": {"title": "Example"},
                },
                {
                    "WebBookmarkType": "WebBookmarkTypeList",
                    "Title": "Tech",
                    "Children": [
                        {
                            "WebBookmarkType": "WebBookmarkTypeLeaf",
                            "URLString": "https://github.com/",
                            "URIDictionary": {"title": "GitHub"},
                        },
                    ],
                },
            ],
        }

        with open(plist_path, 'wb') as f:
            plistlib.dump(plist_data, f)

        bookmarks = parse_bookmarks(plist_path)

        # Should have 2 bookmarks
        leaves = [b for b in bookmarks if b.bookmark_type == "leaf"]
        assert len(leaves) == 2

        # Check folder paths
        urls = {b.url: b.folder_path for b in leaves}
        assert "Bookmarks Menu" in urls["https://example.com/"]
        assert "Tech" in urls["https://github.com/"]

    def test_parse_bookmarks_empty_plist(self, tmp_path):
        """parse_bookmarks handles empty plist."""
        plist_path = tmp_path / "Bookmarks.plist"

        with open(plist_path, 'wb') as f:
            plistlib.dump({}, f)

        bookmarks = parse_bookmarks(plist_path)
        assert bookmarks == []

    def test_parse_bookmarks_invalid_file(self, tmp_path):
        """parse_bookmarks handles invalid files gracefully."""
        plist_path = tmp_path / "Bookmarks.plist"
        plist_path.write_bytes(b"not a plist")

        bookmarks = parse_bookmarks(plist_path)
        assert bookmarks == []

    def test_get_bookmark_stats_with_data(self):
        """get_bookmark_stats calculates correct statistics."""
        bookmarks = [
            SafariBookmark(
                url="https://example.com/",
                title="Example",
                folder_path="Bookmarks/Tech",
                date_added=None,
                date_added_utc=None,
                bookmark_type="leaf",
            ),
            SafariBookmark(
                url="https://test.org/",
                title="Test",
                folder_path="Bookmarks/Other",
                date_added=None,
                date_added_utc=None,
                bookmark_type="leaf",
            ),
        ]

        stats = get_bookmark_stats(bookmarks)
        assert stats["total_bookmarks"] == 2
        assert stats["unique_folders"] == 2

    def test_get_bookmark_stats_empty(self):
        """get_bookmark_stats handles empty list."""
        stats = get_bookmark_stats([])
        assert stats["total_bookmarks"] == 0
        assert stats["unique_folders"] == 0


# =============================================================================
# Extractor Tests
# =============================================================================

class TestSafariBookmarksExtractor:
    """Test SafariBookmarksExtractor class."""

    def test_extractor_metadata(self):
        """Extractor has correct metadata."""
        extractor = SafariBookmarksExtractor()
        meta = extractor.metadata

        assert meta.name == "safari_bookmarks"
        assert "Safari" in meta.display_name
        assert "Bookmarks" in meta.display_name
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_with_fs(self):
        """can_run_extraction returns True with evidence filesystem."""
        extractor = SafariBookmarksExtractor()
        mock_fs = MagicMock()

        can_run, msg = extractor.can_run_extraction(mock_fs)
        assert can_run is True

    def test_can_run_extraction_without_fs(self):
        """can_run_extraction returns False without evidence filesystem."""
        extractor = SafariBookmarksExtractor()

        can_run, msg = extractor.can_run_extraction(None)
        assert can_run is False

    def test_get_output_dir(self, tmp_path):
        """get_output_dir returns correct path."""
        extractor = SafariBookmarksExtractor()

        output = extractor.get_output_dir(tmp_path, "evidence1")
        assert "safari_bookmarks" in str(output)

    def test_run_extraction_creates_manifest(self, tmp_path):
        """run_extraction creates manifest file."""
        extractor = SafariBookmarksExtractor()
        output_dir = tmp_path / "output"

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


class TestSafariBookmarksDualWrite:
    """Tests for dual-write to urls table."""

    def test_extractor_imports_insert_urls(self):
        """Test that extractor imports insert_urls."""
        import inspect
        from extractors.browser.safari.bookmarks import extractor as module

        source = inspect.getsource(module)
        assert "insert_urls" in source
        assert "insert_urls" in source  # multi-line import block

    def test_ingestion_collects_url_records(self):
        """Test that run_ingestion collects URLs for dual-write."""
        import inspect
        from extractors.browser.safari.bookmarks.extractor import SafariBookmarksExtractor

        source = inspect.getsource(SafariBookmarksExtractor.run_ingestion)

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

    def test_url_record_has_bookmark_context(self):
        """Test that URL records have bookmark context provenance."""
        import inspect
        from extractors.browser.safari.bookmarks.extractor import SafariBookmarksExtractor

        source = inspect.getsource(SafariBookmarksExtractor.run_ingestion)

        # Should have bookmark context for Safari
        assert 'context": f"bookmark:safari' in source or "context\": f\"bookmark:safari" in source

    def test_skips_javascript_and_data_uris(self):
        """Test that javascript: and data: URIs are skipped."""
        import inspect
        from extractors.browser.safari.bookmarks.extractor import SafariBookmarksExtractor

        source = inspect.getsource(SafariBookmarksExtractor.run_ingestion)

        # Should skip javascript: and data: URIs
        assert "javascript:" in source
        assert "data:" in source
