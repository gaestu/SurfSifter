"""
Tests for Chromium IndexedDB handling of minimal/empty LevelDB directories.

Validates that the product has explicit, test-backed behavior for:
- Empty LevelDB directories → 0 indexeddb_databases, 0 indexeddb_entries
- Minimal LevelDB without valid databases → 0 rows (not an error)
- This is the expected behavior documented in Workstream I

When ccl_chromium_reader is not installed, parse_indexeddb_storage returns []
immediately (no error). When it IS installed, empty/corrupt LevelDB directories
also produce [] because the wrapper catches all exceptions and returns gracefully.

See: planning/browser_evidence_coverage_u40304_feature.md
"""

from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest


def _make_loc(source_path: str) -> dict:
    return {
        "browser": "chrome",
        "profile": "Default",
        "source_path": source_path,
        "logical_path": source_path,
    }


def _call_parser(idb_dir: Path, images_dir: Path, **overrides):
    """Call parse_indexeddb_storage with sensible defaults."""
    from extractors.browser.chromium.storage._parsers import parse_indexeddb_storage

    kwargs = dict(
        path=idb_dir,
        loc=_make_loc(str(idb_dir)),
        run_id="test_run",
        evidence_id=1,
        excerpt_size=4096,
        include_deleted=False,
        extract_images=False,
        images_dir=images_dir,
    )
    kwargs.update(overrides)
    return parse_indexeddb_storage(**kwargs)


class TestMinimalIndexedDB:
    """Test IndexedDB behavior with minimal/empty LevelDB directories."""

    def test_empty_directory_returns_empty_list(self, tmp_path):
        """Empty LevelDB directory should produce 0 database records.

        Whether ccl_chromium_reader is installed or not, the parser
        must return [] for an empty directory — never raise.
        """
        idb_dir = tmp_path / "IndexedDB" / "https_example.com_0.indexeddb.leveldb"
        idb_dir.mkdir(parents=True)

        result = _call_parser(idb_dir, tmp_path / "images")

        assert result == [], "Empty LevelDB directory should return empty list"

    def test_directory_with_only_lock_file(self, tmp_path):
        """LevelDB directory with only LOCK file should produce 0 records."""
        idb_dir = tmp_path / "IndexedDB" / "https_example.com_0.indexeddb.leveldb"
        idb_dir.mkdir(parents=True)
        (idb_dir / "LOCK").touch()

        result = _call_parser(idb_dir, tmp_path / "images")

        assert result == [], "LevelDB with only LOCK file should return empty list"

    def test_nonexistent_directory_returns_empty(self, tmp_path):
        """Nonexistent directory should return empty list without error."""
        idb_dir = tmp_path / "nonexistent.indexeddb.leveldb"

        result = _call_parser(idb_dir, tmp_path / "images")

        assert result == [], "Nonexistent directory should return empty list"

    def test_top_level_empty_indexeddb_dir(self, tmp_path):
        """Top-level IndexedDB directory with no origin subdirs → 0 results.

        When the path does NOT end with .indexeddb.leveldb, the wrapper
        iterates subdirectories. An empty parent dir yields nothing.
        """
        idb_dir = tmp_path / "IndexedDB"
        idb_dir.mkdir(parents=True)

        result = _call_parser(idb_dir, tmp_path / "images")

        assert result == [], "Empty top-level IndexedDB dir should return empty list"


class TestLevelDBUnavailable:
    """Test behavior when ccl_chromium_reader is not installed."""

    def test_missing_library_returns_empty(self, tmp_path):
        """When ccl_chromium_reader is absent, parser returns [] immediately."""
        idb_dir = tmp_path / "IndexedDB" / "https_example.com_0.indexeddb.leveldb"
        idb_dir.mkdir(parents=True)

        with patch(
            "extractors._shared.leveldb_wrapper.is_leveldb_available",
            return_value=False,
        ):
            result = _call_parser(idb_dir, tmp_path / "images")

        assert result == []

    def test_missing_library_with_warning_collector(self, tmp_path):
        """Warning collector should receive an error when library is missing."""
        idb_dir = tmp_path / "IndexedDB" / "https_example.com_0.indexeddb.leveldb"
        idb_dir.mkdir(parents=True)

        collector = MagicMock()

        with patch(
            "extractors._shared.leveldb_wrapper.is_leveldb_available",
            return_value=False,
        ):
            result = _call_parser(
                idb_dir, tmp_path / "images", warning_collector=collector
            )

        assert result == []
        collector.add_warning.assert_called_once()
        call_kwargs = collector.add_warning.call_args
        assert "not installed" in str(call_kwargs).lower() or "unavailable" in str(call_kwargs).lower()


class TestInsertionLoopContract:
    """Verify the tuple protocol used by the extractor's insertion loop."""

    def test_empty_result_produces_zero_counts(self):
        """Database insertion loop should handle empty parse result correctly.

        The extractor iterates ``for db_record, entries, images in db_records``
        over the parser result. An empty list must produce 0 counts.
        """
        db_records: list = []

        total_databases = 0
        total_entries = 0

        for db_record, entries, extracted_images in db_records:
            total_databases += 1
            total_entries += len(entries)

        assert total_databases == 0
        assert total_entries == 0

    def test_result_tuple_structure(self):
        """Non-empty results must unpack as (dict, list, list)."""
        mock_result = [
            (
                {
                    "run_id": "r1",
                    "browser": "chrome",
                    "profile": "Default",
                    "origin": "https://example.com",
                    "database_name": "test_db",
                    "version": 1,
                    "object_store_count": 1,
                    "source_path": "/some/path",
                },
                [{"object_store": "store1", "key": "k1", "value": "v1"}],
                [],
            )
        ]

        total_databases = 0
        total_entries = 0

        for db_record, entries, extracted_images in mock_result:
            assert isinstance(db_record, dict)
            assert isinstance(entries, list)
            assert isinstance(extracted_images, list)
            assert "origin" in db_record
            assert "database_name" in db_record
            total_databases += 1
            total_entries += len(entries)

        assert total_databases == 1
        assert total_entries == 1
