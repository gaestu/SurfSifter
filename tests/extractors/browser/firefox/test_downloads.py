"""Tests for Firefox downloads extractor."""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extractors.browser.firefox.downloads import FirefoxDownloadsExtractor
from extractors.browser.firefox._parsers import (
    parse_downloads,
    get_download_stats,
    FirefoxDownload,
    FirefoxDownloadStats,
    FIREFOX_STATE_MAP,
)


# =============================================================================
# Parser Tests
# =============================================================================


class TestFirefoxDownloadParsers:
    """Tests for Firefox download database parsers."""

    @pytest.fixture
    def legacy_downloads_db(self, tmp_path):
        """Create a mock places.sqlite with legacy moz_downloads table."""
        db_path = tmp_path / "places.sqlite"
        conn = sqlite3.connect(db_path)

        # Create legacy schema (Firefox < v26)
        conn.executescript("""
            CREATE TABLE moz_downloads (
                id INTEGER PRIMARY KEY,
                name TEXT,
                source TEXT,
                target TEXT,
                startTime INTEGER,
                endTime INTEGER,
                state INTEGER,
                referrer TEXT,
                currBytes INTEGER,
                maxBytes INTEGER,
                mimeType TEXT
            );
        """)

        # PRTime is microseconds since 1970
        start_time = 1704067200000000  # 2024-01-01 00:00:00 UTC
        end_time = start_time + 5000000  # 5 seconds later

        conn.execute(
            """INSERT INTO moz_downloads
               (name, source, target, startTime, endTime, state, referrer, currBytes, maxBytes, mimeType)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("file.zip", "https://example.com/file.zip", "file:///home/user/Downloads/file.zip",
             start_time, end_time, 1, "https://example.com", 1024000, 1024000, "application/zip")
        )
        conn.execute(
            """INSERT INTO moz_downloads
               (name, source, target, startTime, endTime, state, referrer, currBytes, maxBytes, mimeType)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("doc.pdf", "https://test.org/doc.pdf", "file:///home/user/Downloads/doc.pdf",
             start_time + 1000000, None, 3, None, 512000, 1024000, "application/pdf")  # Cancelled
        )

        conn.commit()
        conn.close()

        return db_path

    @pytest.fixture
    def annotation_downloads_db(self, tmp_path):
        """Create a mock places.sqlite with modern annotation-based downloads."""
        db_path = tmp_path / "places.sqlite"
        conn = sqlite3.connect(db_path)

        # Create modern schema (Firefox v26+)
        conn.executescript("""
            CREATE TABLE moz_places (
                id INTEGER PRIMARY KEY,
                url TEXT,
                title TEXT,
                last_visit_date INTEGER
            );

            CREATE TABLE moz_anno_attributes (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE TABLE moz_annos (
                id INTEGER PRIMARY KEY,
                place_id INTEGER NOT NULL,
                anno_attribute_id INTEGER NOT NULL,
                content TEXT
            );
        """)

        # PRTime is microseconds since 1970
        visit_time = 1704067200000000  # 2024-01-01 00:00:00 UTC in microseconds
        end_time_ms = 1704067205000  # 2024-01-01 00:00:05 UTC in milliseconds (JavaScript Date.now() format)

        # Insert annotation attributes
        conn.execute("INSERT INTO moz_anno_attributes (id, name) VALUES (1, 'downloads/destinationFileURI')")
        conn.execute("INSERT INTO moz_anno_attributes (id, name) VALUES (2, 'downloads/metaData')")

        # Insert places (download URLs)
        conn.execute("INSERT INTO moz_places (id, url, last_visit_date) VALUES (1, 'https://example.com/file.zip', ?)", (visit_time,))
        conn.execute("INSERT INTO moz_places (id, url, last_visit_date) VALUES (2, 'https://test.org/doc.pdf', ?)", (visit_time + 1000000,))

        # Insert annotations for download 1 (complete)
        # Note: endTime in metaData is JavaScript milliseconds (Date.now()), not PRTime microseconds
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (1, 1, 'file:///home/user/Downloads/file.zip')")
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (1, 2, ?)",
                     ('{"state": 1, "endTime": ' + str(end_time_ms) + ', "fileSize": 1024000}',))

        # Insert annotations for download 2 (no metadata - incomplete)
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (2, 1, 'file:///home/user/Downloads/doc.pdf')")

        conn.commit()
        conn.close()

        return db_path

    def test_parse_downloads_legacy_returns_downloads(self, legacy_downloads_db):
        """Test parse_downloads returns download records from legacy table."""
        downloads = list(parse_downloads(legacy_downloads_db))

        assert len(downloads) == 2

    def test_parse_downloads_annotation_returns_downloads(self, annotation_downloads_db):
        """Test parse_downloads returns download records from annotations."""
        downloads = list(parse_downloads(annotation_downloads_db))

        assert len(downloads) == 2

    def test_parse_downloads_dataclass_fields(self, legacy_downloads_db):
        """Test parsed downloads have correct dataclass fields."""
        downloads = list(parse_downloads(legacy_downloads_db))
        download = downloads[0]

        assert isinstance(download, FirefoxDownload)
        assert hasattr(download, "url")
        assert hasattr(download, "target_path")
        assert hasattr(download, "filename")
        assert hasattr(download, "start_time_utc")
        assert hasattr(download, "end_time_utc")
        assert hasattr(download, "total_bytes")
        assert hasattr(download, "received_bytes")
        assert hasattr(download, "state")
        assert hasattr(download, "mime_type")
        assert hasattr(download, "referrer")

    def test_parse_downloads_state_mapping(self, legacy_downloads_db):
        """Test download state codes are mapped to strings."""
        downloads = list(parse_downloads(legacy_downloads_db))

        # First download complete (state=1)
        complete = next(d for d in downloads if "file.zip" in d.url)
        assert complete.state == "complete"

        # Second download cancelled (state=3)
        cancelled = next(d for d in downloads if "doc.pdf" in d.url)
        assert cancelled.state == "cancelled"

    def test_parse_downloads_file_uri_parsing(self, legacy_downloads_db):
        """Test file:/// URI is parsed to path."""
        downloads = list(parse_downloads(legacy_downloads_db))
        download = downloads[0]

        assert download.target_path == "/home/user/Downloads/file.zip"
        assert download.filename == "file.zip"

    def test_parse_downloads_timestamp_conversion(self, legacy_downloads_db):
        """Test PRTime timestamps are converted to ISO 8601."""
        downloads = list(parse_downloads(legacy_downloads_db))
        download = downloads[0]

        assert download.start_time_utc is not None
        assert "2024-01-01" in download.start_time_utc

    def test_parse_downloads_annotation_metadata_parsing(self, annotation_downloads_db):
        """Test annotation metadata JSON is correctly parsed."""
        downloads = list(parse_downloads(annotation_downloads_db))

        # First download has complete metadata
        complete = next(d for d in downloads if "file.zip" in d.url)
        assert complete.state == "complete"
        assert complete.total_bytes == 1024000
        assert complete.end_time_utc is not None

        # Second download has no metadata - state unknown
        incomplete = next(d for d in downloads if "doc.pdf" in d.url)
        assert incomplete.state == "unknown"

    def test_parse_downloads_empty_db(self, tmp_path):
        """Test parse_downloads handles empty database."""
        db_path = tmp_path / "empty.sqlite"
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE moz_places (id INTEGER PRIMARY KEY, url TEXT, last_visit_date INTEGER);
            CREATE TABLE moz_anno_attributes (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE moz_annos (id INTEGER PRIMARY KEY, place_id INTEGER, anno_attribute_id INTEGER, content TEXT);
        """)
        conn.close()

        downloads = list(parse_downloads(db_path))
        assert downloads == []

    def test_parse_downloads_missing_tables(self, tmp_path):
        """Test parse_downloads handles missing tables."""
        db_path = tmp_path / "invalid.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE other (id INTEGER)")
        conn.close()

        downloads = list(parse_downloads(db_path))
        assert downloads == []

    def test_get_download_stats_legacy(self, legacy_downloads_db):
        """Test get_download_stats with legacy table."""
        stats = get_download_stats(legacy_downloads_db)

        assert isinstance(stats, FirefoxDownloadStats)
        assert stats.download_count == 2
        assert stats.complete_count == 1
        assert stats.failed_count == 0  # Cancelled is state 3, not 2

    def test_firefox_state_map(self):
        """Test FIREFOX_STATE_MAP has expected mappings."""
        assert FIREFOX_STATE_MAP[0] == "in_progress"
        assert FIREFOX_STATE_MAP[1] == "complete"
        assert FIREFOX_STATE_MAP[2] == "failed"
        assert FIREFOX_STATE_MAP[3] == "cancelled"
        assert FIREFOX_STATE_MAP[4] == "paused"


# =============================================================================
# Enhanced Metadata Parsing Tests
# =============================================================================


class TestFirefoxDownloadEnhancedMetadata:
    """Tests for enhanced metadata extraction (deleted, danger_type, referrer)."""

    @pytest.fixture
    def enhanced_annotation_db(self, tmp_path):
        """Create database with enhanced metadata fields."""
        db_path = tmp_path / "places_enhanced.sqlite"
        conn = sqlite3.connect(db_path)

        conn.executescript("""
            CREATE TABLE moz_places (
                id INTEGER PRIMARY KEY,
                url TEXT,
                title TEXT,
                last_visit_date INTEGER
            );

            CREATE TABLE moz_anno_attributes (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE TABLE moz_annos (
                id INTEGER PRIMARY KEY,
                place_id INTEGER NOT NULL,
                anno_attribute_id INTEGER NOT NULL,
                content TEXT
            );

            CREATE TABLE moz_historyvisits (
                id INTEGER PRIMARY KEY,
                place_id INTEGER,
                from_visit INTEGER,
                visit_type INTEGER,
                visit_date INTEGER
            );
        """)

        # PRTime is microseconds since 1970
        visit_time = 1704067200000000  # 2024-01-01 00:00:00 UTC in microseconds
        # endTime in metaData is JavaScript milliseconds (Date.now()), not PRTime microseconds
        end_time_base_ms = 1704067200000  # 2024-01-01 00:00:00 UTC in milliseconds

        # Insert annotation attributes
        conn.execute("INSERT INTO moz_anno_attributes (id, name) VALUES (1, 'downloads/destinationFileURI')")
        conn.execute("INSERT INTO moz_anno_attributes (id, name) VALUES (2, 'downloads/metaData')")

        # Insert places for referrer page and download
        conn.execute("INSERT INTO moz_places (id, url, last_visit_date) VALUES (1, 'https://example.com/page.html', ?)", (visit_time,))
        conn.execute("INSERT INTO moz_places (id, url, last_visit_date) VALUES (2, 'https://example.com/file.zip', ?)", (visit_time + 1000,))
        conn.execute("INSERT INTO moz_places (id, url, last_visit_date) VALUES (3, 'https://malware.example/bad.exe', ?)", (visit_time + 2000,))
        conn.execute("INSERT INTO moz_places (id, url, last_visit_date) VALUES (4, 'https://test.org/deleted.pdf', ?)", (visit_time + 3000,))

        # Create visit chain: page -> download (visit_type=7 is TRANSITION_DOWNLOAD)
        conn.execute("INSERT INTO moz_historyvisits (id, place_id, from_visit, visit_type, visit_date) VALUES (1, 1, 0, 1, ?)", (visit_time,))
        conn.execute("INSERT INTO moz_historyvisits (id, place_id, from_visit, visit_type, visit_date) VALUES (2, 2, 1, 7, ?)", (visit_time + 1000,))

        # Download 1: Complete with referrer
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (2, 1, 'file:///home/user/Downloads/file.zip')")
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (2, 2, ?)",
                     ('{"state": 1, "endTime": ' + str(end_time_base_ms + 5000) + ', "fileSize": 1024000}',))

        # Download 2: Blocked by reputation check (danger_type)
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (3, 1, 'file:///home/user/Downloads/bad.exe')")
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (3, 2, ?)",
                     ('{"state": 8, "endTime": ' + str(end_time_base_ms + 6000) + ', "reputationCheckVerdict": "MALWARE"}',))

        # Download 3: Deleted file
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (4, 1, 'file:///home/user/Downloads/deleted.pdf')")
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (4, 2, ?)",
                     ('{"state": 1, "endTime": ' + str(end_time_base_ms + 7000) + ', "fileSize": 2048, "deleted": true}',))

        conn.commit()
        conn.close()

        return db_path

    def test_referrer_extraction_from_historyvisits(self, enhanced_annotation_db):
        """Test referrer URL is extracted from moz_historyvisits from_visit chain."""
        downloads = list(parse_downloads(enhanced_annotation_db))

        # Find the download that has a referrer
        download = next(d for d in downloads if "file.zip" in d.url)

        assert download.referrer == "https://example.com/page.html"

    def test_deleted_flag_extraction(self, enhanced_annotation_db):
        """Test deleted flag is extracted from metaData.deleted."""
        downloads = list(parse_downloads(enhanced_annotation_db))

        # Find the deleted download
        deleted = next(d for d in downloads if "deleted.pdf" in d.url)
        assert deleted.deleted is True

        # Non-deleted downloads should have deleted=False
        normal = next(d for d in downloads if "file.zip" in d.url)
        assert normal.deleted is False

    def test_danger_type_extraction(self, enhanced_annotation_db):
        """Test danger_type is extracted from metaData.reputationCheckVerdict."""
        downloads = list(parse_downloads(enhanced_annotation_db))

        # Find the blocked download
        blocked = next(d for d in downloads if "bad.exe" in d.url)

        assert blocked.danger_type == "MALWARE"
        assert blocked.state == "dirty"  # State 8 = blocked_dirty

    def test_no_referrer_when_no_historyvisits(self, tmp_path):
        """Test referrer is None when moz_historyvisits table doesn't exist."""
        db_path = tmp_path / "no_visits.sqlite"
        conn = sqlite3.connect(db_path)

        conn.executescript("""
            CREATE TABLE moz_places (id INTEGER PRIMARY KEY, url TEXT, last_visit_date INTEGER);
            CREATE TABLE moz_anno_attributes (id INTEGER PRIMARY KEY, name TEXT);
            CREATE TABLE moz_annos (id INTEGER PRIMARY KEY, place_id INTEGER, anno_attribute_id INTEGER, content TEXT);
        """)

        visit_time = 1704067200000000
        conn.execute("INSERT INTO moz_anno_attributes (id, name) VALUES (1, 'downloads/destinationFileURI')")
        conn.execute("INSERT INTO moz_anno_attributes (id, name) VALUES (2, 'downloads/metaData')")
        conn.execute("INSERT INTO moz_places (id, url, last_visit_date) VALUES (1, 'https://example.com/file.zip', ?)", (visit_time,))
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (1, 1, 'file:///home/user/Downloads/file.zip')")
        conn.execute("INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES (1, 2, '{\"state\": 1}')")
        conn.commit()
        conn.close()

        downloads = list(parse_downloads(db_path))
        assert len(downloads) == 1
        assert downloads[0].referrer is None

    def test_dataclass_has_new_fields(self):
        """Test FirefoxDownload dataclass has deleted and danger_type fields."""
        download = FirefoxDownload(
            url="https://example.com/file.zip",
            target_path="/downloads/file.zip",
            filename="file.zip",
            start_time_utc="2024-01-01T00:00:00Z",
            end_time_utc="2024-01-01T00:05:00Z",
            total_bytes=1024,
            received_bytes=1024,
            state="complete",
            mime_type="application/zip",
            referrer="https://example.com/page.html",
            deleted=True,
            danger_type="UNCOMMON",
        )

        assert download.deleted is True
        assert download.danger_type == "UNCOMMON"

    def test_default_values_for_new_fields(self):
        """Test new fields have correct default values."""
        download = FirefoxDownload(
            url="https://example.com/file.zip",
            target_path="/downloads/file.zip",
            filename="file.zip",
            start_time_utc=None,
            end_time_utc=None,
            total_bytes=None,
            received_bytes=None,
            state="unknown",
            mime_type=None,
            referrer=None,
        )

        # New fields should have defaults
        assert download.deleted is False
        assert download.danger_type is None


# =============================================================================
# Extractor Tests
# =============================================================================


class TestFirefoxDownloadsExtractor:
    """Tests for FirefoxDownloadsExtractor class."""

    def test_extractor_metadata(self):
        """Test extractor has correct metadata."""
        extractor = FirefoxDownloadsExtractor()
        meta = extractor.metadata

        assert meta.name == "firefox_downloads"
        assert meta.display_name == "Firefox Downloads"
        assert meta.category == "browser"
        assert meta.version
        assert "." in meta.version
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_with_fs(self):
        """Test can_run_extraction returns True with filesystem."""
        extractor = FirefoxDownloadsExtractor()
        mock_fs = MagicMock()

        can_run, reason = extractor.can_run_extraction(mock_fs)

        assert can_run is True
        assert reason == ""

    def test_get_output_dir(self, tmp_path):
        """Test get_output_dir returns correct path."""
        extractor = FirefoxDownloadsExtractor()

        output_dir = extractor.get_output_dir(tmp_path, "evidence_001")

        assert output_dir == tmp_path / "evidences" / "evidence_001" / "firefox_downloads"

    def test_run_extraction_creates_manifest(self, tmp_path):
        """Test extraction creates manifest.json."""
        extractor = FirefoxDownloadsExtractor()
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
        assert manifest["extractor"] == "firefox_downloads"

    def test_ingestion_passes_enhanced_metadata_to_db(self, tmp_path):
        """Test ingestion passes danger_type and deleted to database."""
        # Create places.sqlite with annotation containing enhanced metadata
        db_path = tmp_path / "places.sqlite"
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE moz_places (
                id INTEGER PRIMARY KEY,
                url TEXT,
                last_visit_date INTEGER
            );
            CREATE TABLE moz_annos (
                id INTEGER PRIMARY KEY,
                place_id INTEGER,
                anno_attribute_id INTEGER,
                content TEXT
            );
            CREATE TABLE moz_anno_attributes (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
            CREATE TABLE moz_historyvisits (
                id INTEGER PRIMARY KEY,
                place_id INTEGER,
                from_visit INTEGER,
                visit_type INTEGER,
                visit_date INTEGER
            );

            INSERT INTO moz_places (id, url, last_visit_date) VALUES
                (1, 'https://example.com/malware.exe', 1704067200000000);

            INSERT INTO moz_anno_attributes (id, name) VALUES
                (1, 'downloads/destinationFileURI'),
                (2, 'downloads/metaData');

            INSERT INTO moz_annos (place_id, anno_attribute_id, content) VALUES
                (1, 1, 'file:///C:/Downloads/malware.exe'),
                (1, 2, '{"state":1,"endTime":1704070800000,"fileSize":1024,"deleted":true,"reputationCheckVerdict":"DANGEROUS_HOST"}');

            -- Add history visit for referrer chain
            INSERT INTO moz_places (id, url, last_visit_date) VALUES
                (2, 'https://malicious-site.com/page', 1704067100000000);
            INSERT INTO moz_historyvisits (id, place_id, from_visit, visit_type, visit_date) VALUES
                (1, 2, 0, 1, 1704067100000000),
                (2, 1, 1, 7, 1704067200000000);
        """)
        conn.close()

        # Parse downloads and verify enhanced fields
        downloads = list(parse_downloads(db_path))
        assert len(downloads) == 1

        download = downloads[0]
        assert download.deleted is True
        assert download.danger_type == "DANGEROUS_HOST"
        assert download.referrer == "https://malicious-site.com/page"

        # Now test ingestion with mock database
        extractor = FirefoxDownloadsExtractor()

        # Track inserted records
        inserted_records = []

        def mock_insert(conn, evidence_id, browser, url, **kwargs):
            inserted_records.append({
                "browser": browser,
                "url": url,
                **kwargs
            })

        with patch('extractors.browser.firefox.downloads.extractor.insert_browser_download_row', mock_insert):
            mock_conn = MagicMock()
            callbacks = MagicMock()
            callbacks.is_cancelled.return_value = False

            result = extractor._parse_and_insert(
                db_path,
                {"browser": "firefox", "profile": "default", "logical_path": "/test/places.sqlite"},
                "run123",
                1,
                mock_conn,
                callbacks
            )

        assert result["total"] == 1
        assert len(inserted_records) == 1

        record = inserted_records[0]
        assert record["danger_type"] == "DANGEROUS_HOST"
        assert record["referrer"] == "https://malicious-site.com/page"
        assert record["notes"] == "deleted=true"


# =============================================================================
# Dual-Write Tests
# =============================================================================


class TestFirefoxDownloadsDualWrite:
    """Tests for dual-write to urls table."""

    def test_extractor_imports_insert_urls(self):
        """Test that extractor imports insert_urls."""
        import inspect
        from extractors.browser.firefox.downloads import extractor as module

        source = inspect.getsource(module)
        assert "insert_urls" in source
        assert "from core.database import" in source

    def test_parse_and_insert_collects_url_records(self):
        """Test that _parse_and_insert collects URLs for dual-write."""
        import inspect
        from extractors.browser.firefox.downloads.extractor import FirefoxDownloadsExtractor

        source = inspect.getsource(FirefoxDownloadsExtractor._parse_and_insert)

        # Should have url_records list
        assert "url_records" in source
        # Should collect URLs with proper schema
        assert "first_seen_utc" in source
        assert "domain" in source
        assert "scheme" in source
        assert "context" in source
        assert "content_type" in source  # Downloads have mime type
        # Should cross-post to urls table
        assert "insert_urls" in source
        assert "Cross-posted" in source

    def test_url_record_has_download_context(self):
        """Test that URL records have download context provenance."""
        import inspect
        from extractors.browser.firefox.downloads.extractor import FirefoxDownloadsExtractor

        source = inspect.getsource(FirefoxDownloadsExtractor._parse_and_insert)

        # Should have download context
        assert 'context": f"download:' in source or "context\": f\"download:" in source
