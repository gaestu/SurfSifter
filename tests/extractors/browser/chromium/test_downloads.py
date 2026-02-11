"""
Tests for ChromiumDownloadsExtractor.

Tests cover:
- Metadata: name, version, category, capabilities
- Registry: discovery
- Patterns: History file patterns (downloads are in History database)
- Parsers: ChromiumDownload dataclass, state mapping, danger types
- Extraction: filesystem discovery, manifest creation
- Ingestion: database inserts, statistics
- Error handling: cancellation, empty config
"""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import pytest

from extractors.browser.chromium.downloads import ChromiumDownloadsExtractor
from extractors.browser.chromium._parsers import (
    ChromiumDownload,
    parse_downloads,
    get_download_stats,
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
        ext = ChromiumDownloadsExtractor()
        assert ext.metadata.name == "chromium_downloads"

    def test_metadata_display_name(self):
        """Extractor has human-readable display name."""
        ext = ChromiumDownloadsExtractor()
        assert ext.metadata.display_name == "Chromium Downloads"

    def test_metadata_version(self):
        """Extractor has a version."""
        ext = ChromiumDownloadsExtractor()
        assert ext.metadata.version
        assert "." in ext.metadata.version

    def test_metadata_category(self):
        """Extractor is in browser category."""
        ext = ChromiumDownloadsExtractor()
        assert ext.metadata.category == "browser"

    def test_metadata_capabilities(self):
        """Extractor can extract and ingest."""
        ext = ChromiumDownloadsExtractor()
        assert ext.metadata.can_extract is True
        assert ext.metadata.can_ingest is True

    def test_metadata_description_mentions_downloads(self):
        """Description mentions downloads."""
        ext = ChromiumDownloadsExtractor()
        desc = ext.metadata.description.lower()
        assert "download" in desc


# =============================================================================
# Test Patterns
# =============================================================================

class TestPatterns:
    """Test browser path patterns."""

    def test_history_artifact_exists(self):
        """History artifact is defined (downloads are in History db)."""
        assert "history" in CHROMIUM_ARTIFACTS

    def test_get_patterns_chrome(self):
        """Get Chrome history patterns (for downloads)."""
        patterns = get_patterns("chrome", "history")
        assert len(patterns) > 0
        for p in patterns:
            assert "History" in p

    def test_get_patterns_edge(self):
        """Get Edge history patterns."""
        patterns = get_patterns("edge", "history")
        assert len(patterns) > 0

    def test_get_patterns_brave(self):
        """Get Brave history patterns."""
        patterns = get_patterns("brave", "history")
        assert len(patterns) > 0


# =============================================================================
# Test Parsers
# =============================================================================

class TestParsers:
    """Test parser functions and dataclasses."""

    def test_download_dataclass(self):
        """ChromiumDownload dataclass has all required fields."""
        from datetime import datetime

        dl = ChromiumDownload(
            id=1,
            target_path="/home/user/Downloads/file.pdf",
            start_time=datetime(2023, 1, 1),
            start_time_iso="2023-01-01T00:00:00+00:00",
            end_time=datetime(2023, 1, 1, 0, 1),
            end_time_iso="2023-01-01T00:01:00+00:00",
            received_bytes=1024,
            total_bytes=1024,
            state="complete",  # Pre-mapped string
            danger_type="not_dangerous",  # Pre-mapped string
            opened=False,
            last_access_time=None,
            last_access_time_iso=None,
            referrer="https://example.com",
            tab_url="https://example.com/download",
            tab_referrer_url="https://example.com",
            mime_type="application/pdf",
            original_mime_type="application/pdf",
            url_chain=["https://example.com/file.pdf"],
        )
        assert dl.target_path == "/home/user/Downloads/file.pdf"
        assert dl.state == "complete"
        assert dl.danger_type == "not_dangerous"

    def test_download_state_values(self):
        """Download state is pre-mapped string."""
        from datetime import datetime

        # COMPLETE
        dl_complete = ChromiumDownload(
            id=1, target_path="", start_time=None, start_time_iso=None,
            end_time=None, end_time_iso=None, received_bytes=0, total_bytes=0,
            state="complete", danger_type="not_dangerous", opened=False,
            last_access_time=None, last_access_time_iso=None,
            referrer="", tab_url="", tab_referrer_url="", mime_type="",
            original_mime_type="", url_chain=[],
        )
        assert dl_complete.state == "complete"

        # IN_PROGRESS
        dl_progress = ChromiumDownload(
            id=1, target_path="", start_time=None, start_time_iso=None,
            end_time=None, end_time_iso=None, received_bytes=0, total_bytes=0,
            state="in_progress", danger_type="not_dangerous", opened=False,
            last_access_time=None, last_access_time_iso=None,
            referrer="", tab_url="", tab_referrer_url="", mime_type="",
            original_mime_type="", url_chain=[],
        )
        assert dl_progress.state == "in_progress"

        # CANCELLED
        dl_cancelled = ChromiumDownload(
            id=1, target_path="", start_time=None, start_time_iso=None,
            end_time=None, end_time_iso=None, received_bytes=0, total_bytes=0,
            state="cancelled", danger_type="not_dangerous", opened=False,
            last_access_time=None, last_access_time_iso=None,
            referrer="", tab_url="", tab_referrer_url="", mime_type="",
            original_mime_type="", url_chain=[],
        )
        assert dl_cancelled.state == "cancelled"

    def test_download_danger_type_values(self):
        """Download danger type is pre-mapped string."""
        from datetime import datetime

        # NOT_DANGEROUS
        dl_safe = ChromiumDownload(
            id=1, target_path="", start_time=None, start_time_iso=None,
            end_time=None, end_time_iso=None, received_bytes=0, total_bytes=0,
            state="complete", danger_type="not_dangerous", opened=False,
            last_access_time=None, last_access_time_iso=None,
            referrer="", tab_url="", tab_referrer_url="", mime_type="",
            original_mime_type="", url_chain=[],
        )
        assert dl_safe.danger_type == "not_dangerous"

        # DANGEROUS_FILE
        dl_dangerous = ChromiumDownload(
            id=1, target_path="", start_time=None, start_time_iso=None,
            end_time=None, end_time_iso=None, received_bytes=0, total_bytes=0,
            state="complete", danger_type="dangerous_file", opened=False,
            last_access_time=None, last_access_time_iso=None,
            referrer="", tab_url="", tab_referrer_url="", mime_type="",
            original_mime_type="", url_chain=[],
        )
        assert dl_dangerous.danger_type == "dangerous_file"


class TestParsersWithDatabase:
    """Test parsers with real SQLite data."""

    @pytest.fixture
    def history_db(self, tmp_path):
        """Create a test History database with downloads table."""
        db_path = tmp_path / "History"
        conn = sqlite3.connect(db_path)

        # Create downloads table (Chromium schema)
        conn.execute("""
            CREATE TABLE downloads (
                id INTEGER PRIMARY KEY,
                target_path TEXT NOT NULL,
                start_time INTEGER NOT NULL,
                end_time INTEGER NOT NULL,
                received_bytes INTEGER NOT NULL,
                total_bytes INTEGER NOT NULL,
                state INTEGER NOT NULL,
                danger_type INTEGER NOT NULL DEFAULT 0,
                opened INTEGER NOT NULL DEFAULT 0,
                last_access_time INTEGER DEFAULT 0,
                referrer TEXT DEFAULT '',
                tab_url TEXT DEFAULT '',
                tab_referrer_url TEXT DEFAULT '',
                mime_type TEXT DEFAULT '',
                original_mime_type TEXT DEFAULT ''
            )
        """)

        # Insert test data
        conn.execute("""
            INSERT INTO downloads (id, target_path, start_time, end_time, received_bytes,
                total_bytes, state, danger_type, opened, last_access_time,
                referrer, tab_url, tab_referrer_url, mime_type, original_mime_type) VALUES
            (1, '/home/user/Downloads/report.pdf', 13350000000000000, 13350000060000000,
             102400, 102400, 1, 0, 1, 0,
             'https://example.com', 'https://example.com/download', '', 'application/pdf', 'application/pdf'),
            (2, '/home/user/Downloads/malware.exe', 13360000000000000, 13360000030000000,
             51200, 51200, 1, 1, 0, 0,
             'https://bad.com', 'https://bad.com/file', '', 'application/octet-stream', 'application/octet-stream')
        """)
        conn.commit()
        conn.close()
        return db_path

    def test_parse_downloads(self, history_db):
        """Parse downloads from database."""
        conn = sqlite3.connect(history_db)
        conn.row_factory = sqlite3.Row

        downloads = list(parse_downloads(conn))
        conn.close()

        assert len(downloads) == 2

        # First download - safe PDF
        pdf_dl = next(d for d in downloads if d.id == 1)
        assert "report.pdf" in pdf_dl.target_path
        assert pdf_dl.state == "complete"
        assert pdf_dl.danger_type == "not_dangerous"
        assert pdf_dl.opened is True

        # Second download - dangerous file
        exe_dl = next(d for d in downloads if d.id == 2)
        assert "malware.exe" in exe_dl.target_path
        assert exe_dl.danger_type == "dangerous_file"
        assert exe_dl.opened is False

    def test_get_download_stats(self, history_db):
        """Get download statistics."""
        conn = sqlite3.connect(history_db)
        conn.row_factory = sqlite3.Row

        stats = get_download_stats(conn)
        conn.close()

        assert stats["download_count"] == 2
        assert stats["complete_count"] >= 1
        assert stats["dangerous_count"] >= 1


# =============================================================================
# Test Extraction
# =============================================================================

class TestExtraction:
    """Test extraction workflow."""

    def test_can_run_extraction_no_fs(self):
        """Extraction requires filesystem."""
        ext = ChromiumDownloadsExtractor()
        can_run, msg = ext.can_run_extraction(None)
        assert can_run is False

    def test_can_run_extraction_with_fs(self):
        """Extraction can run with filesystem."""
        ext = ChromiumDownloadsExtractor()
        mock_fs = MagicMock()
        can_run, msg = ext.can_run_extraction(mock_fs)
        assert can_run is True

    def test_extraction_creates_manifest(self, tmp_path):
        """Extraction creates manifest.json."""
        ext = ChromiumDownloadsExtractor()

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

        assert manifest["extractor"] == "chromium_downloads"


# =============================================================================
# Test Ingestion
# =============================================================================

class TestIngestion:
    """Test ingestion workflow."""

    def test_can_run_ingestion_no_manifest(self, tmp_path):
        """Ingestion requires manifest."""
        ext = ChromiumDownloadsExtractor()
        can_run, msg = ext.can_run_ingestion(tmp_path)
        assert can_run is False
        assert "manifest" in msg.lower()

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        """Ingestion can run with manifest."""
        ext = ChromiumDownloadsExtractor()

        manifest = {"extractor_name": "chromium_downloads", "extracted_files": []}
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
        ext = ChromiumDownloadsExtractor()

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
        ext = ChromiumDownloadsExtractor()

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
