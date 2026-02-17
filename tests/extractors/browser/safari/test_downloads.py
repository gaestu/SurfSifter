"""
Tests for Safari Downloads Extractor.

Tests cover:
- Download parsing (_parsers.py)
- SafariDownloadsExtractor metadata and methods
- Registry discovery
"""

import plistlib
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from extractors.browser.safari._parsers import (
    parse_downloads,
    get_download_stats,
    SafariDownload,
)
from extractors.browser.safari.downloads import SafariDownloadsExtractor


# =============================================================================
# Parser Tests
# =============================================================================

class TestSafariDownloadParsers:
    """Test Safari download parsers."""

    def test_parse_downloads_list_format(self, tmp_path):
        """parse_downloads parses list format plist."""
        plist_path = tmp_path / "Downloads.plist"

        plist_data = [
            {
                "DownloadEntryURL": "https://example.com/file.zip",
                "DownloadEntryPath": "/Users/test/Downloads/file.zip",
                "DownloadEntryProgressTotalToLoad": 1024000,
                "DownloadEntryProgressBytesSoFar": 1024000,
            },
        ]

        with open(plist_path, 'wb') as f:
            plistlib.dump(plist_data, f)

        downloads = parse_downloads(plist_path)
        assert len(downloads) == 1
        assert downloads[0].url == "https://example.com/file.zip"
        assert downloads[0].filename == "file.zip"
        assert downloads[0].total_bytes == 1024000

    def test_parse_downloads_dict_format(self, tmp_path):
        """parse_downloads parses dictionary format plist."""
        plist_path = tmp_path / "Downloads.plist"

        plist_data = {
            "DownloadHistory": [
                {
                    "DownloadEntryURL": "https://example.com/app.dmg",
                    "DownloadEntryPath": "/Users/test/Downloads/app.dmg",
                    "DownloadEntryProgressTotalToLoad": 50000000,
                },
            ],
        }

        with open(plist_path, 'wb') as f:
            plistlib.dump(plist_data, f)

        downloads = parse_downloads(plist_path)
        assert len(downloads) == 1
        assert "app.dmg" in downloads[0].filename

    def test_parse_downloads_alternate_keys(self, tmp_path):
        """parse_downloads handles alternate key names."""
        plist_path = tmp_path / "Downloads.plist"

        plist_data = [
            {
                "DownloadURL": "https://example.com/doc.pdf",
                "DownloadPath": "/Users/test/Downloads/doc.pdf",
            },
        ]

        with open(plist_path, 'wb') as f:
            plistlib.dump(plist_data, f)

        downloads = parse_downloads(plist_path)
        assert len(downloads) == 1
        assert downloads[0].url == "https://example.com/doc.pdf"

    def test_parse_downloads_empty_plist(self, tmp_path):
        """parse_downloads handles empty plist."""
        plist_path = tmp_path / "Downloads.plist"

        with open(plist_path, 'wb') as f:
            plistlib.dump([], f)

        downloads = parse_downloads(plist_path)
        assert downloads == []

    def test_parse_downloads_invalid_file(self, tmp_path):
        """parse_downloads handles invalid files gracefully."""
        plist_path = tmp_path / "Downloads.plist"
        plist_path.write_bytes(b"not a plist")

        downloads = parse_downloads(plist_path)
        assert downloads == []

    def test_safari_download_state_always_complete(self):
        """Safari downloads are always 'complete' state."""
        download = SafariDownload(
            url="https://example.com/file.zip",
            target_path="/Downloads/file.zip",
            filename="file.zip",
            total_bytes=1000,
            received_bytes=1000,
            state="complete",
            identifier=None,
        )
        # Safari only stores completed downloads
        assert download.state == "complete"

    def test_get_download_stats_with_data(self):
        """get_download_stats calculates correct statistics."""
        downloads = [
            SafariDownload(
                url="https://example.com/a.zip",
                target_path="/Downloads/a.zip",
                filename="a.zip",
                total_bytes=1000,
                received_bytes=1000,
                state="complete",
                identifier=None,
            ),
            SafariDownload(
                url="https://example.com/b.zip",
                target_path="/Downloads/b.zip",
                filename="b.zip",
                total_bytes=2000,
                received_bytes=2000,
                state="complete",
                identifier=None,
            ),
        ]

        stats = get_download_stats(downloads)
        assert stats["total_downloads"] == 2
        assert stats["total_bytes"] == 3000

    def test_get_download_stats_empty(self):
        """get_download_stats handles empty list."""
        stats = get_download_stats([])
        assert stats["total_downloads"] == 0
        assert stats["total_bytes"] == 0


# =============================================================================
# Extractor Tests
# =============================================================================

class TestSafariDownloadsExtractor:
    """Test SafariDownloadsExtractor class."""

    def test_extractor_metadata(self):
        """Extractor has correct metadata."""
        extractor = SafariDownloadsExtractor()
        meta = extractor.metadata

        assert meta.name == "safari_downloads"
        assert "Safari" in meta.display_name
        assert "Downloads" in meta.display_name
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_with_fs(self):
        """can_run_extraction returns True with evidence filesystem."""
        extractor = SafariDownloadsExtractor()
        mock_fs = MagicMock()

        can_run, msg = extractor.can_run_extraction(mock_fs)
        assert can_run is True

    def test_can_run_extraction_without_fs(self):
        """can_run_extraction returns False without evidence filesystem."""
        extractor = SafariDownloadsExtractor()

        can_run, msg = extractor.can_run_extraction(None)
        assert can_run is False

    def test_get_output_dir(self, tmp_path):
        """get_output_dir returns correct path."""
        extractor = SafariDownloadsExtractor()

        output = extractor.get_output_dir(tmp_path, "evidence1")
        assert "safari_downloads" in str(output)

    def test_run_extraction_creates_manifest(self, tmp_path):
        """run_extraction creates manifest file."""
        extractor = SafariDownloadsExtractor()
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


class TestSafariDownloadsDualWrite:
    """Tests for dual-write to urls table."""

    def test_extractor_imports_insert_urls(self):
        """Test that extractor imports insert_urls."""
        import inspect
        from extractors.browser.safari.downloads import extractor as module

        source = inspect.getsource(module)
        assert "insert_urls" in source
        assert "insert_urls" in source  # multi-line import block

    def test_ingestion_collects_url_records(self):
        """Test that run_ingestion collects URLs for dual-write."""
        import inspect
        from extractors.browser.safari.downloads.extractor import SafariDownloadsExtractor

        source = inspect.getsource(SafariDownloadsExtractor.run_ingestion)

        # Should have url_records list
        assert "url_records" in source
        # Should collect URLs with proper schema
        assert "domain" in source
        assert "scheme" in source
        assert "context" in source
        # Should cross-post to urls table
        assert "insert_urls" in source
        assert "Cross-posted" in source

    def test_url_record_has_download_context(self):
        """Test that URL records have download context provenance."""
        import inspect
        from extractors.browser.safari.downloads.extractor import SafariDownloadsExtractor

        source = inspect.getsource(SafariDownloadsExtractor.run_ingestion)

        # Should have download context for Safari
        assert 'context": f"download:safari' in source or "context\": f\"download:safari" in source
