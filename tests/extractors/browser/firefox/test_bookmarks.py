"""Tests for Firefox bookmarks extractor."""

import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extractors.browser.firefox.bookmarks import FirefoxBookmarksExtractor
from extractors.browser.firefox._parsers import (
    parse_bookmarks,
    get_bookmark_stats,
    parse_bookmark_backup,
    get_bookmark_backup_stats,
    extract_backup_timestamp,
    decompress_mozlz4,
    FirefoxBookmark,
    FirefoxBookmarkStats,
)


# =============================================================================
# Parser Tests
# =============================================================================


class TestFirefoxBookmarkParsers:
    """Tests for Firefox bookmark database parsers."""

    @pytest.fixture
    def places_db(self, tmp_path):
        """Create a mock places.sqlite database with bookmarks."""
        db_path = tmp_path / "places.sqlite"
        conn = sqlite3.connect(db_path)

        # Create schema
        conn.executescript("""
            CREATE TABLE moz_places (
                id INTEGER PRIMARY KEY,
                url TEXT,
                title TEXT,
                visit_count INTEGER DEFAULT 0
            );

            CREATE TABLE moz_bookmarks (
                id INTEGER PRIMARY KEY,
                type INTEGER NOT NULL,
                fk INTEGER,
                parent INTEGER,
                position INTEGER,
                title TEXT,
                dateAdded INTEGER,
                lastModified INTEGER,
                guid TEXT
            );
        """)

        # PRTime is microseconds since 1970
        date_added = 1704067200000000  # 2024-01-01 00:00:00 UTC

        # Insert folders (type=2)
        conn.execute(
            "INSERT INTO moz_bookmarks (id, type, parent, position, title, dateAdded, lastModified, guid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (1, 2, 0, 0, "", date_added, date_added, "root________")  # Root
        )
        conn.execute(
            "INSERT INTO moz_bookmarks (id, type, parent, position, title, dateAdded, lastModified, guid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (2, 2, 1, 0, "toolbar", date_added, date_added, "toolbar_____")  # Toolbar
        )
        conn.execute(
            "INSERT INTO moz_bookmarks (id, type, parent, position, title, dateAdded, lastModified, guid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (3, 2, 2, 0, "Tech", date_added, date_added, "tech_folder_")  # Tech folder
        )

        # Insert places
        conn.execute(
            "INSERT INTO moz_places (id, url, title) VALUES (?, ?, ?)",
            (1, "https://example.com", "Example Site")
        )
        conn.execute(
            "INSERT INTO moz_places (id, url, title) VALUES (?, ?, ?)",
            (2, "https://github.com", "GitHub")
        )

        # Insert bookmarks (type=1)
        conn.execute(
            "INSERT INTO moz_bookmarks (id, type, fk, parent, position, title, dateAdded, lastModified, guid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (4, 1, 1, 2, 0, "Example", date_added, date_added, "bookmark1___")
        )
        conn.execute(
            "INSERT INTO moz_bookmarks (id, type, fk, parent, position, title, dateAdded, lastModified, guid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (5, 1, 2, 3, 0, "GitHub", date_added, date_added, "bookmark2___")
        )

        conn.commit()
        conn.close()

        return db_path

    def test_parse_bookmarks_returns_bookmarks(self, places_db):
        """Test parse_bookmarks returns bookmark records."""
        bookmarks = list(parse_bookmarks(places_db))

        assert len(bookmarks) == 2

    def test_parse_bookmarks_dataclass_fields(self, places_db):
        """Test parsed bookmarks have correct dataclass fields."""
        bookmarks = list(parse_bookmarks(places_db))
        bookmark = bookmarks[0]

        assert isinstance(bookmark, FirefoxBookmark)
        assert hasattr(bookmark, "url")
        assert hasattr(bookmark, "title")
        assert hasattr(bookmark, "folder_path")
        assert hasattr(bookmark, "guid")
        assert hasattr(bookmark, "date_added_utc")
        assert hasattr(bookmark, "date_modified_utc")
        assert hasattr(bookmark, "bookmark_type")

    def test_parse_bookmarks_folder_hierarchy(self, places_db):
        """Test folder hierarchy is correctly built."""
        bookmarks = list(parse_bookmarks(places_db))

        # First bookmark in toolbar
        toolbar_bookmark = next(b for b in bookmarks if b.title == "Example")
        assert "Bookmarks Toolbar" in toolbar_bookmark.folder_path

        # Second bookmark in Tech subfolder
        tech_bookmark = next(b for b in bookmarks if b.title == "GitHub")
        assert "Tech" in tech_bookmark.folder_path

    def test_parse_bookmarks_timestamp_conversion(self, places_db):
        """Test PRTime timestamps are converted to ISO 8601."""
        bookmarks = list(parse_bookmarks(places_db))
        bookmark = bookmarks[0]

        assert bookmark.date_added_utc is not None
        assert "2024-01-01" in bookmark.date_added_utc

    def test_parse_bookmarks_empty_db(self, tmp_path):
        """Test parse_bookmarks handles empty database."""
        db_path = tmp_path / "empty.sqlite"
        conn = sqlite3.connect(db_path)
        conn.executescript("""
            CREATE TABLE moz_places (id INTEGER PRIMARY KEY, url TEXT);
            CREATE TABLE moz_bookmarks (id INTEGER PRIMARY KEY, type INTEGER, fk INTEGER, parent INTEGER, position INTEGER, title TEXT, dateAdded INTEGER, lastModified INTEGER, guid TEXT);
        """)
        conn.close()

        bookmarks = list(parse_bookmarks(db_path))
        assert bookmarks == []

    def test_parse_bookmarks_missing_tables(self, tmp_path):
        """Test parse_bookmarks handles missing tables."""
        db_path = tmp_path / "invalid.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE other (id INTEGER)")
        conn.close()

        bookmarks = list(parse_bookmarks(db_path))
        assert bookmarks == []

    def test_get_bookmark_stats(self, places_db):
        """Test get_bookmark_stats returns correct counts."""
        stats = get_bookmark_stats(places_db)

        assert isinstance(stats, FirefoxBookmarkStats)
        assert stats.bookmark_count == 2
        assert stats.folder_count == 3  # Root, Toolbar, Tech


# =============================================================================
# Extractor Tests
# =============================================================================


class TestFirefoxBookmarksExtractor:
    """Tests for FirefoxBookmarksExtractor class."""

    def test_extractor_metadata(self):
        """Test extractor has correct metadata."""
        extractor = FirefoxBookmarksExtractor()
        meta = extractor.metadata

        assert meta.name == "firefox_bookmarks"
        assert meta.display_name == "Firefox Bookmarks"
        assert meta.category == "browser"
        assert meta.version
        assert "." in meta.version
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_with_fs(self):
        """Test can_run_extraction returns True with filesystem."""
        extractor = FirefoxBookmarksExtractor()
        mock_fs = MagicMock()

        can_run, reason = extractor.can_run_extraction(mock_fs)

        assert can_run is True
        assert reason == ""

    def test_get_output_dir(self, tmp_path):
        """Test get_output_dir returns correct path."""
        extractor = FirefoxBookmarksExtractor()

        output_dir = extractor.get_output_dir(tmp_path, "evidence_001")

        assert output_dir == tmp_path / "evidences" / "evidence_001" / "firefox_bookmarks"

    def test_run_extraction_creates_manifest(self, tmp_path):
        """Test extraction creates manifest.json."""
        extractor = FirefoxBookmarksExtractor()
        output_dir = tmp_path / "output"

        mock_fs = MagicMock()
        mock_fs.source_path = "/test/image.e01"
        mock_fs.fs_type = "NTFS"
        mock_fs.partition_index = 0

        # Mock evidence_conn for file_list check - uses execute() not cursor()
        mock_evidence_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)  # Empty file_list
        mock_cursor.fetchall.return_value = []    # No files found
        mock_evidence_conn.execute.return_value = mock_cursor

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        with patch('core.statistics_collector.StatisticsCollector.instance', return_value=None):
            result = extractor.run_extraction(
                mock_fs,
                output_dir,
                {"evidence_id": 1, "evidence_conn": mock_evidence_conn},
                callbacks
            )

        assert result is True
        assert (output_dir / "manifest.json").exists()

        manifest = json.loads((output_dir / "manifest.json").read_text())
        assert manifest["extractor"] == "firefox_bookmarks"


# =============================================================================
# Bookmark Backup Tests (jsonlz4)
# =============================================================================


class TestBookmarkBackupParser:
    """Tests for Firefox bookmark backup (jsonlz4) parsing."""

    @pytest.fixture
    def bookmark_backup_json(self):
        """Create sample bookmark backup JSON structure."""
        # PRTime is microseconds since 1970
        date_added = 1704067200000000  # 2024-01-01 00:00:00 UTC

        return {
            "type": "text/x-moz-place-container",
            "title": "",
            "guid": "root________",
            "children": [
                {
                    "type": "text/x-moz-place-container",
                    "title": "menu",
                    "guid": "menu________",
                    "children": [
                        {
                            "type": "text/x-moz-place",
                            "uri": "https://example.com",
                            "title": "Example Site",
                            "dateAdded": date_added,
                            "lastModified": date_added,
                            "guid": "bookmark1___"
                        }
                    ]
                },
                {
                    "type": "text/x-moz-place-container",
                    "title": "toolbar",
                    "guid": "toolbar_____",
                    "children": [
                        {
                            "type": "text/x-moz-place-container",
                            "title": "Tech",
                            "guid": "techfolder__",
                            "children": [
                                {
                                    "type": "text/x-moz-place",
                                    "uri": "https://github.com",
                                    "title": "GitHub",
                                    "dateAdded": date_added,
                                    "lastModified": date_added,
                                    "guid": "bookmark2___"
                                }
                            ]
                        }
                    ]
                },
                {
                    "type": "text/x-moz-place-container",
                    "title": "unfiled",
                    "guid": "unfiled_____",
                    "children": [
                        {
                            "type": "text/x-moz-place",
                            "uri": "https://deleted-bookmark.com",
                            "title": "Deleted Bookmark",
                            "dateAdded": date_added - 86400000000,  # 1 day earlier
                            "lastModified": date_added - 86400000000,
                            "guid": "deleted_____"
                        }
                    ]
                }
            ]
        }

    @pytest.fixture
    def backup_jsonlz4_file(self, tmp_path, bookmark_backup_json):
        """Create a mock jsonlz4 bookmark backup file."""
        pytest.importorskip("lz4.block")
        import lz4.block

        json_bytes = json.dumps(bookmark_backup_json).encode("utf-8")
        compressed = lz4.block.compress(json_bytes)
        data = b"mozLz40\x00" + compressed

        file_path = tmp_path / "bookmarks-2024-01-15_1234_abcd1234.jsonlz4"
        file_path.write_bytes(data)

        return file_path

    def test_decompress_mozlz4_valid(self, tmp_path):
        """Test decompressing valid mozLz4 data."""
        pytest.importorskip("lz4.block")
        import lz4.block

        original = b'{"test": "data"}'
        compressed = lz4.block.compress(original)
        data = b"mozLz40\x00" + compressed

        result = decompress_mozlz4(data)
        assert result == original

    def test_decompress_mozlz4_invalid_magic(self):
        """Test decompress_mozlz4 raises on invalid magic."""
        with pytest.raises(ValueError, match="Invalid mozLz4 magic"):
            decompress_mozlz4(b"invalid\x00data")

    def test_decompress_mozlz4_too_short(self):
        """Test decompress_mozlz4 raises on too short data."""
        with pytest.raises(ValueError, match="Data too short"):
            decompress_mozlz4(b"short")

    def test_parse_bookmark_backup_returns_bookmarks(self, backup_jsonlz4_file):
        """Test parse_bookmark_backup returns bookmark records."""
        bookmarks = list(parse_bookmark_backup(backup_jsonlz4_file))

        # Should find 3 bookmarks (Example, GitHub, Deleted)
        assert len(bookmarks) == 3

    def test_parse_bookmark_backup_dataclass_fields(self, backup_jsonlz4_file):
        """Test parsed backup bookmarks have correct dataclass fields."""
        bookmarks = list(parse_bookmark_backup(backup_jsonlz4_file))
        bookmark = bookmarks[0]

        assert isinstance(bookmark, FirefoxBookmark)
        assert hasattr(bookmark, "url")
        assert hasattr(bookmark, "title")
        assert hasattr(bookmark, "folder_path")
        assert hasattr(bookmark, "guid")
        assert hasattr(bookmark, "date_added_utc")
        assert hasattr(bookmark, "date_modified_utc")
        assert hasattr(bookmark, "bookmark_type")

    def test_parse_bookmark_backup_folder_hierarchy(self, backup_jsonlz4_file):
        """Test folder hierarchy is correctly built from backup."""
        bookmarks = list(parse_bookmark_backup(backup_jsonlz4_file))

        # Find GitHub bookmark (in Toolbar/Tech)
        github_bookmark = next(b for b in bookmarks if b.title == "GitHub")
        assert "Bookmarks Toolbar" in github_bookmark.folder_path
        assert "Tech" in github_bookmark.folder_path

        # Find Example bookmark (in Bookmarks Menu)
        example_bookmark = next(b for b in bookmarks if b.title == "Example Site")
        assert "Bookmarks Menu" in example_bookmark.folder_path

        # Find deleted bookmark (in Other Bookmarks)
        deleted_bookmark = next(b for b in bookmarks if b.title == "Deleted Bookmark")
        assert "Other Bookmarks" in deleted_bookmark.folder_path

    def test_parse_bookmark_backup_timestamp_conversion(self, backup_jsonlz4_file):
        """Test PRTime timestamps are converted to ISO 8601."""
        bookmarks = list(parse_bookmark_backup(backup_jsonlz4_file))
        bookmark = bookmarks[0]

        assert bookmark.date_added_utc is not None
        # Timestamp should be valid ISO 8601
        assert "20" in bookmark.date_added_utc  # Year starts with 20xx

    def test_parse_bookmark_backup_invalid_file(self, tmp_path):
        """Test parse_bookmark_backup handles invalid file."""
        invalid_file = tmp_path / "invalid.jsonlz4"
        invalid_file.write_bytes(b"not valid mozlz4")

        with pytest.raises(ValueError, match="Failed to decompress"):
            list(parse_bookmark_backup(invalid_file))

    def test_parse_bookmark_backup_missing_file(self, tmp_path):
        """Test parse_bookmark_backup handles missing file."""
        missing_file = tmp_path / "missing.jsonlz4"

        with pytest.raises(ValueError, match="Failed to read"):
            list(parse_bookmark_backup(missing_file))

    def test_get_bookmark_backup_stats(self, backup_jsonlz4_file):
        """Test get_bookmark_backup_stats returns correct counts."""
        stats = get_bookmark_backup_stats(backup_jsonlz4_file)

        assert isinstance(stats, FirefoxBookmarkStats)
        assert stats.bookmark_count == 3  # 3 URL bookmarks
        assert stats.folder_count >= 4  # Root, Menu, Toolbar, Tech, Unfiled

    def test_extract_backup_timestamp_valid(self):
        """Test extracting timestamp from backup filename."""
        assert extract_backup_timestamp("bookmarks-2024-01-15_1234_abcd1234.jsonlz4") == "2024-01-15"
        assert extract_backup_timestamp("bookmarks-2023-12-31_5678_xyz.jsonlz4") == "2023-12-31"

    def test_extract_backup_timestamp_invalid(self):
        """Test extracting timestamp from invalid filename."""
        assert extract_backup_timestamp("invalid_filename.jsonlz4") is None
        assert extract_backup_timestamp("bookmarks.jsonlz4") is None
        assert extract_backup_timestamp("") is None


class TestFirefoxBookmarksExtractorBackups:
    """Tests for bookmark backup discovery and extraction."""

    def test_extractor_metadata_version(self):
        """Test extractor metadata reflects backup support."""
        extractor = FirefoxBookmarksExtractor()
        meta = extractor.metadata

        assert meta.version
        assert "." in meta.version
        assert "backup" in meta.description.lower()

    def test_discover_multi_partition_method_exists(self):
        """Test _discover_files_multi_partition method exists."""
        extractor = FirefoxBookmarksExtractor()

        # Method should exist
        assert hasattr(extractor, '_discover_files_multi_partition')
        assert callable(extractor._discover_files_multi_partition)

    def test_extract_file_backup(self, tmp_path):
        """Test _extract_file handles backup files correctly."""
        extractor = FirefoxBookmarksExtractor()

        # Create mock backup file
        backup_content = b"mozLz40\x00test_compressed_data"

        mock_fs = MagicMock()
        mock_fs.read_file.return_value = backup_content

        file_info = {
            "logical_path": "Users/test/.mozilla/firefox/abc123.default/bookmarkbackups/bookmarks-2024-01-15_1234_abcd.jsonlz4",
            "browser": "firefox",
            "profile": "abc123.default",
            "artifact_type": "bookmark_backup",
            "source_type": "backup",
            "backup_date": "2024-01-15",
        }

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        callbacks = MagicMock()

        result = extractor._extract_file(mock_fs, file_info, output_dir, callbacks)

        assert result["copy_status"] == "ok"
        assert result["artifact_type"] == "bookmark_backup"
        assert result["source_type"] == "backup"
        assert result["backup_date"] == "2024-01-15"
        assert "backup" in result["extracted_path"]
        assert "companion_files" not in result  # Backups don't have companions


# =============================================================================
# Dual-Write Tests
# =============================================================================

class TestFirefoxBookmarksDualWrite:
    """Tests for dual-write to urls table."""

    def test_extractor_imports_insert_urls(self):
        """Test that extractor imports insert_urls."""
        import inspect
        from extractors.browser.firefox.bookmarks import extractor as module

        source = inspect.getsource(module)
        assert "insert_urls" in source
        assert "from core.database import" in source

    def test_parse_and_insert_collects_url_records(self):
        """Test that _parse_and_insert collects URLs for dual-write."""
        import inspect
        from extractors.browser.firefox.bookmarks.extractor import FirefoxBookmarksExtractor

        source = inspect.getsource(FirefoxBookmarksExtractor._parse_and_insert)

        # Should have url_records list
        assert "url_records" in source
        # Should collect URLs with proper schema
        assert "first_seen_utc" in source
        assert "domain" in source
        assert "scheme" in source
        assert "context" in source
        # Should cross-post to urls table
        assert "insert_urls" in source
        assert "Cross-posted" in source

    def test_url_record_has_bookmark_context(self):
        """Test that URL records have bookmark context provenance."""
        import inspect
        from extractors.browser.firefox.bookmarks.extractor import FirefoxBookmarksExtractor

        source = inspect.getsource(FirefoxBookmarksExtractor._parse_and_insert)

        # Should have bookmark context
        assert 'context": f"bookmark:' in source or "context\": f\"bookmark:" in source

    def test_skips_javascript_and_data_uris(self):
        """Test that javascript: and data: URIs are skipped."""
        import inspect
        from extractors.browser.firefox.bookmarks.extractor import FirefoxBookmarksExtractor

        source = inspect.getsource(FirefoxBookmarksExtractor._parse_and_insert)

        # Should skip javascript: and data: URIs
        assert "javascript:" in source
        assert "data:" in source
