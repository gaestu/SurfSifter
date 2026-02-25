"""
Tests for Chromium Browser Storage parser — old-format .localstorage handling.

Covers:
- Pre-LevelDB .localstorage SQLite format detection and parsing
- Origin URL reconstruction from .localstorage filenames
- UTF-16LE value decoding
- Error handling for corrupt .localstorage files
- LevelDB directory not falsely detected as old format
"""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from extractors.browser.chromium.storage._parsers import (
    parse_leveldb_storage,
    _parse_origin_from_localstorage_filename,
    _parse_old_localstorage_files,
    format_kv_record,
)


# =============================================================================
# Origin URL parsing from .localstorage filenames
# =============================================================================

class TestParseOriginFromFilename:
    """Test origin URL reconstruction from old-format .localstorage filenames."""

    def test_https_default_port(self):
        assert _parse_origin_from_localstorage_filename(
            "https_test.net_0.localstorage"
        ) == "https://test.net"

    def test_https_subdomain(self):
        assert _parse_origin_from_localstorage_filename(
            "https_www.youtube-nocookie.com_0.localstorage"
        ) == "https://www.youtube-nocookie.com"

    def test_http_scheme(self):
        assert _parse_origin_from_localstorage_filename(
            "http_example.com_0.localstorage"
        ) == "http://example.com"

    def test_non_default_port(self):
        assert _parse_origin_from_localstorage_filename(
            "http_localhost_8080.localstorage"
        ) == "http://localhost:8080"

    def test_host_with_underscore(self):
        """Rare but valid: underscores in hostname."""
        assert _parse_origin_from_localstorage_filename(
            "http_my_server_9999.localstorage"
        ) == "http://my_server:9999"

    def test_minimal_unparseable(self):
        """Fewer than 3 parts after split returns raw stem."""
        assert _parse_origin_from_localstorage_filename(
            "garbage.localstorage"
        ) == "garbage"


# =============================================================================
# Old .localstorage SQLite file parsing
# =============================================================================

def _create_localstorage_sqlite(path: Path, records: dict[str, str]):
    """Helper: create a .localstorage SQLite file with ItemTable and UTF-16LE values."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE ItemTable (key TEXT UNIQUE ON CONFLICT REPLACE, "
        "value BLOB NOT NULL ON CONFLICT FAIL)"
    )
    for key, value in records.items():
        conn.execute(
            "INSERT INTO ItemTable (key, value) VALUES (?, ?)",
            (key, value.encode("utf-16-le")),
        )
    conn.commit()
    conn.close()


class TestOldLocalStorageFileParsing:
    """Test actual data extraction from old .localstorage SQLite files."""

    @pytest.fixture
    def localstorage_dir(self, tmp_path):
        """Directory containing two .localstorage files with real data."""
        ls_dir = tmp_path / "Local Storage"
        ls_dir.mkdir()

        _create_localstorage_sqlite(
            ls_dir / "https_example.com_0.localstorage",
            {"theme": "dark", "lang": "en"},
        )
        _create_localstorage_sqlite(
            ls_dir / "http_app.local_8080.localstorage",
            {"token": "abc123"},
        )
        return ls_dir

    def _make_loc(self, path):
        return {
            "browser": "chromium_embedded",
            "profile": "Default",
            "logical_path": str(path),
            "partition_index": 3,
            "fs_type": "NTFS",
        }

    def test_parses_records_from_files(self, localstorage_dir):
        """Records are extracted with correct origin, key, value."""
        loc = self._make_loc(localstorage_dir)
        records = parse_leveldb_storage(
            localstorage_dir,
            loc,
            run_id="run1",
            evidence_id=1,
            storage_type="local_storage",
            excerpt_size=1000,
            include_deleted=False,
        )
        assert len(records) == 3  # 2 + 1

        # Check origins
        origins = {r["origin"] for r in records}
        assert "https://example.com" in origins
        assert "http://app.local:8080" in origins

        # Check specific values
        example_recs = [r for r in records if r["origin"] == "https://example.com"]
        keys = {r["key"] for r in example_recs}
        assert keys == {"theme", "lang"}
        theme_rec = next(r for r in example_recs if r["key"] == "theme")
        assert theme_rec["value"] == "dark"

    def test_fields_populated_correctly(self, localstorage_dir):
        """Record dicts have all expected metadata fields."""
        loc = self._make_loc(localstorage_dir)
        records = parse_leveldb_storage(
            localstorage_dir, loc,
            run_id="run_meta", evidence_id=2,
            storage_type="local_storage",
            excerpt_size=1000, include_deleted=False,
        )
        rec = records[0]
        assert rec["run_id"] == "run_meta"
        assert rec["browser"] == "chromium_embedded"
        assert rec["profile"] == "Default"
        assert rec["partition_index"] == 3
        assert rec["fs_type"] == "NTFS"
        assert rec["value_type"] in ("string", "json", "number", "boolean", "empty")

    def test_excerpt_size_truncates_value(self, tmp_path):
        """Long values are truncated to excerpt_size."""
        ls_dir = tmp_path / "ls"
        ls_dir.mkdir()
        _create_localstorage_sqlite(
            ls_dir / "https_big.com_0.localstorage",
            {"bigkey": "x" * 5000},
        )
        loc = self._make_loc(ls_dir)
        records = parse_leveldb_storage(
            ls_dir, loc,
            run_id="run_trunc", evidence_id=1,
            storage_type="local_storage",
            excerpt_size=100, include_deleted=False,
        )
        assert len(records) == 1
        assert len(records[0]["value"]) == 100

    def test_warning_collector_gets_info_warning(self, localstorage_dir):
        """Warning collector receives old_localstorage_format info warning."""
        loc = self._make_loc(localstorage_dir)
        wc = MagicMock()
        parse_leveldb_storage(
            localstorage_dir, loc,
            run_id="run_warn", evidence_id=1,
            storage_type="local_storage",
            excerpt_size=1000, include_deleted=False,
            warning_collector=wc,
        )
        wc.add_warning.assert_called_once()
        kwargs = wc.add_warning.call_args[1]
        assert kwargs["warning_type"] == "old_localstorage_format"
        assert "Parsing via" in kwargs["item_value"]

    def test_corrupt_file_handled_gracefully(self, tmp_path):
        """Corrupt .localstorage file does not crash, adds warning."""
        ls_dir = tmp_path / "ls"
        ls_dir.mkdir()
        (ls_dir / "https_bad.com_0.localstorage").write_bytes(b"not a sqlite db")
        loc = self._make_loc(ls_dir)
        wc = MagicMock()

        records = parse_leveldb_storage(
            ls_dir, loc,
            run_id="run_bad", evidence_id=1,
            storage_type="local_storage",
            excerpt_size=1000, include_deleted=False,
            warning_collector=wc,
        )
        # Info warning for detection + error warning for parse failure
        assert wc.add_warning.call_count == 2
        types = [c[1]["warning_type"] for c in wc.add_warning.call_args_list]
        assert "old_localstorage_format" in types
        assert records == []

    def test_mixed_good_and_bad_files(self, tmp_path):
        """Good files are parsed even if one file is corrupt."""
        ls_dir = tmp_path / "ls"
        ls_dir.mkdir()
        _create_localstorage_sqlite(
            ls_dir / "https_good.com_0.localstorage",
            {"ok": "yes"},
        )
        (ls_dir / "https_bad.com_0.localstorage").write_bytes(b"corrupt")
        loc = self._make_loc(ls_dir)

        records = parse_leveldb_storage(
            ls_dir, loc,
            run_id="run_mix", evidence_id=1,
            storage_type="local_storage",
            excerpt_size=1000, include_deleted=False,
        )
        assert len(records) == 1
        assert records[0]["origin"] == "https://good.com"

    def test_empty_localstorage_file(self, tmp_path):
        """A .localstorage file with an empty ItemTable returns no records."""
        ls_dir = tmp_path / "ls"
        ls_dir.mkdir()
        _create_localstorage_sqlite(
            ls_dir / "https_empty.com_0.localstorage", {},
        )
        loc = self._make_loc(ls_dir)
        records = parse_leveldb_storage(
            ls_dir, loc,
            run_id="run_empty", evidence_id=1,
            storage_type="local_storage",
            excerpt_size=1000, include_deleted=False,
        )
        assert records == []


# =============================================================================
# Old .localstorage format detection (pre-existing; kept for regression)
# =============================================================================

class TestOldLocalStorageDetection:
    """Test that old .localstorage SQLite directories are detected and parsed."""

    @pytest.fixture
    def old_localstorage_dir(self, tmp_path):
        """Create a directory mimicking old CefSharp Local Storage layout."""
        ls_dir = tmp_path / "Local Storage"
        ls_dir.mkdir()

        # Create .localstorage SQLite files (old CefSharp format)
        _create_localstorage_sqlite(
            ls_dir / "http_example.com_0.localstorage",
            {"k1": "v1"},
        )
        _create_localstorage_sqlite(
            ls_dir / "http_localhost_0.localstorage",
            {"k2": "v2"},
        )
        (ls_dir / "http_app.local_0.localstorage-journal").write_bytes(b"journal")

        return ls_dir

    @pytest.fixture
    def leveldb_dir(self, tmp_path):
        """Create a directory mimicking modern LevelDB Local Storage layout."""
        ls_dir = tmp_path / "Local Storage" / "leveldb"
        ls_dir.mkdir(parents=True)

        # LevelDB files
        (ls_dir / "MANIFEST-000001").write_bytes(b"\x00")
        (ls_dir / "000001.ldb").write_bytes(b"\x00")
        (ls_dir / "CURRENT").write_text("MANIFEST-000001\n")

        return ls_dir

    def _make_loc(self, path):
        """Build a minimal location dict."""
        return {
            "browser": "chromium_embedded",
            "profile": "Default",
            "logical_path": str(path),
            "partition_index": 3,
            "fs_type": "NTFS",
        }

    def test_old_localstorage_returns_records(self, old_localstorage_dir):
        """Old .localstorage directory returns parsed records, no exception."""
        loc = self._make_loc(old_localstorage_dir)
        records = parse_leveldb_storage(
            old_localstorage_dir,
            loc,
            run_id="test_run",
            evidence_id=1,
            storage_type="local_storage",
            excerpt_size=1000,
            include_deleted=False,
        )
        # 2 valid .localstorage files with 1 record each
        assert len(records) == 2

    def test_old_localstorage_adds_warning(self, old_localstorage_dir):
        """Old .localstorage directory adds informative warning."""
        loc = self._make_loc(old_localstorage_dir)
        warning_collector = MagicMock()

        parse_leveldb_storage(
            old_localstorage_dir,
            loc,
            run_id="test_run",
            evidence_id=1,
            storage_type="local_storage",
            excerpt_size=1000,
            include_deleted=False,
            warning_collector=warning_collector,
        )

        warning_collector.add_warning.assert_called_once()
        call_kwargs = warning_collector.add_warning.call_args
        assert call_kwargs[1]["warning_type"] == "old_localstorage_format"
        assert "Pre-LevelDB" in call_kwargs[1]["item_value"]

    def test_leveldb_dir_not_detected_as_old_format(self, leveldb_dir):
        """Modern LevelDB directory is NOT flagged as old format.

        Note: This test verifies the pre-check doesn't falsely trigger.
        The actual LevelDB parse may fail since we create dummy files,
        but it should NOT be caught by the old-format check.
        """
        loc = self._make_loc(leveldb_dir)
        warning_collector = MagicMock()

        # Will likely fail at LevelDB parsing but should not be
        # caught by the .localstorage pre-check
        parse_leveldb_storage(
            leveldb_dir,
            loc,
            run_id="test_run",
            evidence_id=1,
            storage_type="local_storage",
            excerpt_size=1000,
            include_deleted=False,
            warning_collector=warning_collector,
        )

        # If any warning was added, it should NOT be old_localstorage_format
        for call in warning_collector.add_warning.call_args_list:
            assert call[1].get("warning_type") != "old_localstorage_format"

    def test_empty_dir_not_detected_as_old_format(self, tmp_path):
        """Empty directory is NOT flagged as old format."""
        empty_dir = tmp_path / "Local Storage"
        empty_dir.mkdir()

        loc = self._make_loc(empty_dir)
        warning_collector = MagicMock()

        parse_leveldb_storage(
            empty_dir,
            loc,
            run_id="test_run",
            evidence_id=1,
            storage_type="local_storage",
            excerpt_size=1000,
            include_deleted=False,
            warning_collector=warning_collector,
        )

        # Should not trigger old-format warning
        for call in warning_collector.add_warning.call_args_list:
            assert call[1].get("warning_type") != "old_localstorage_format"
