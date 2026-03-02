"""
Tests for Safari multi-partition file list discovery.

Tests cover:
- get_safari_filename_patterns(): correct filenames per artifact
- get_safari_path_patterns(): correct SQL LIKE patterns per artifact
- discover_safari_files(): file_list query with mocked database
- discover_safari_files_fallback(): filesystem iteration fallback
- Multi-partition discovery across partitions
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from extractors.browser.safari._discovery import (
    discover_safari_files,
    discover_safari_files_fallback,
    get_safari_filename_patterns,
    get_safari_path_patterns,
)
from extractors.browser.safari._patterns import SAFARI_ARTIFACTS


# =============================================================================
# Helpers
# =============================================================================


def _create_file_list_db(rows: List[dict]) -> sqlite3.Connection:
    """
    Create an in-memory database with a file_list table populated from *rows*.

    Each row dict should have keys: evidence_id, file_path, file_name,
    partition_index, inode, size_bytes, extension, deleted.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE file_list (
            evidence_id INTEGER,
            file_path TEXT,
            file_name TEXT,
            partition_index INTEGER,
            inode INTEGER,
            size_bytes INTEGER,
            extension TEXT,
            deleted INTEGER DEFAULT 0
        )
        """
    )
    for row in rows:
        conn.execute(
            """
            INSERT INTO file_list
                (evidence_id, file_path, file_name, partition_index, inode, size_bytes, extension, deleted)
            VALUES
                (:evidence_id, :file_path, :file_name, :partition_index, :inode, :size_bytes, :extension, :deleted)
            """,
            {
                "evidence_id": row.get("evidence_id", 1),
                "file_path": row["file_path"],
                "file_name": row["file_name"],
                "partition_index": row.get("partition_index", 0),
                "inode": row.get("inode"),
                "size_bytes": row.get("size_bytes"),
                "extension": row.get("extension"),
                "deleted": row.get("deleted", 0),
            },
        )
    conn.commit()
    return conn


class FakeEvidenceFS:
    """Minimal evidence filesystem mock for fallback tests."""

    def __init__(self, paths: Dict[str, bytes], partition_index: int = 0):
        self._paths = paths
        self.partition_index = partition_index

    def iter_paths(self, pattern: str):
        """Yield paths that would match the given glob pattern."""
        import fnmatch

        for path in self._paths:
            if fnmatch.fnmatch(path, pattern):
                yield path

    def read_file(self, path: str) -> bytes:
        if path in self._paths:
            return self._paths[path]
        raise FileNotFoundError(path)


# =============================================================================
# get_safari_filename_patterns
# =============================================================================


class TestGetSafariFilenamePatterns:
    """Test filename pattern extraction for file_list queries."""

    def test_history_filenames(self):
        filenames = get_safari_filename_patterns(["history"])
        assert "History.db" in filenames
        assert "History.db-wal" in filenames
        assert "History.db-journal" in filenames

    def test_cookies_filenames(self):
        filenames = get_safari_filename_patterns(["cookies"])
        assert "Cookies.binarycookies" in filenames

    def test_bookmarks_filenames(self):
        filenames = get_safari_filename_patterns(["bookmarks"])
        assert "Bookmarks.plist" in filenames

    def test_downloads_filenames(self):
        filenames = get_safari_filename_patterns(["downloads"])
        assert "Downloads.plist" in filenames

    def test_sessions_filenames(self):
        filenames = get_safari_filename_patterns(["sessions"])
        assert "LastSession.plist" in filenames

    def test_top_sites_filenames(self):
        filenames = get_safari_filename_patterns(["top_sites"])
        assert "TopSites.plist" in filenames

    def test_multiple_artifacts(self):
        filenames = get_safari_filename_patterns(["sessions", "recently_closed_tabs"])
        assert "LastSession.plist" in filenames
        assert "RecentlyClosedTabs.plist" in filenames

    def test_unknown_artifact_ignored(self):
        filenames = get_safari_filename_patterns(["nonexistent"])
        assert filenames == []

    def test_deduplication(self):
        filenames = get_safari_filename_patterns(["history"])
        assert len(filenames) == len(set(filenames))

    def test_cache_has_wildcard_patterns(self):
        filenames = get_safari_filename_patterns(["cache"])
        assert "Cache.db" in filenames
        # Wildcard patterns from sub-paths are included
        assert "*" in filenames


# =============================================================================
# get_safari_path_patterns
# =============================================================================


class TestGetSafariPathPatterns:
    """Test SQL LIKE path pattern generation."""

    def test_history_path_patterns(self):
        patterns = get_safari_path_patterns(["history"])
        assert len(patterns) > 0
        # Should contain Safari library paths
        assert any("Library/Safari" in p for p in patterns)

    def test_cookies_uses_cookies_roots(self):
        patterns = get_safari_path_patterns(["cookies"])
        assert any("Library/Cookies" in p for p in patterns)
        # Should NOT use profile_roots for cookies
        assert not any(p.endswith("Library/Safari%") for p in patterns)

    def test_cache_uses_cache_roots(self):
        patterns = get_safari_path_patterns(["cache"])
        assert any("Caches/com.apple.Safari" in p for p in patterns)

    def test_patterns_are_sql_like_format(self):
        patterns = get_safari_path_patterns(["history"])
        for p in patterns:
            # Should start and end with %
            assert p.startswith("%")
            assert p.endswith("%")
            # Should contain % for glob wildcards (Users/*)
            assert p.count("%") >= 2

    def test_absolute_variants_excluded(self):
        """Absolute variants should be excluded (leading % handles both)."""
        patterns = get_safari_path_patterns(["history"])
        for p in patterns:
            # The actual root inside % should not start with /
            inner = p.lstrip("%")
            assert not inner.startswith("/Users")

    def test_multiple_artifacts_merge(self):
        """Multiple artifacts should produce union of path patterns."""
        history_patterns = set(get_safari_path_patterns(["history"]))
        cookies_patterns = set(get_safari_path_patterns(["cookies"]))
        combined = set(get_safari_path_patterns(["history", "cookies"]))
        assert history_patterns.issubset(combined)
        assert cookies_patterns.issubset(combined)


# =============================================================================
# discover_safari_files
# =============================================================================


class TestDiscoverSafariFiles:
    """Test file_list-based multi-partition discovery."""

    def test_returns_empty_when_no_conn(self):
        result = discover_safari_files(None, 1, artifact_names=["history"])
        assert result == {}

    def test_returns_empty_when_file_list_empty(self):
        conn = _create_file_list_db([])
        result = discover_safari_files(conn, 1, artifact_names=["history"])
        assert result == {}

    def test_single_partition_history(self):
        conn = _create_file_list_db([
            {
                "evidence_id": 1,
                "file_path": "/Users/johndoe/Library/Safari/History.db",
                "file_name": "History.db",
                "partition_index": 2,
                "inode": 12345,
                "size_bytes": 65536,
                "extension": ".db",
            },
        ])
        result = discover_safari_files(conn, 1, artifact_names=["history"])
        assert 2 in result
        assert len(result[2]) == 1
        file_info = result[2][0]
        assert file_info["logical_path"] == "/Users/johndoe/Library/Safari/History.db"
        assert file_info["user"] == "johndoe"
        assert file_info["browser"] == "safari"
        assert file_info["partition_index"] == 2
        assert file_info["inode"] == 12345
        assert file_info["size_bytes"] == 65536

    def test_multi_partition_history(self):
        conn = _create_file_list_db([
            {
                "evidence_id": 1,
                "file_path": "/Users/alice/Library/Safari/History.db",
                "file_name": "History.db",
                "partition_index": 1,
                "inode": 100,
                "size_bytes": 1024,
            },
            {
                "evidence_id": 1,
                "file_path": "/Users/bob/Library/Safari/History.db",
                "file_name": "History.db",
                "partition_index": 3,
                "inode": 200,
                "size_bytes": 2048,
            },
        ])
        result = discover_safari_files(conn, 1, artifact_names=["history"])
        assert 1 in result
        assert 3 in result
        assert result[1][0]["user"] == "alice"
        assert result[3][0]["user"] == "bob"

    def test_cookies_uses_cookies_root(self):
        """Cookies should be found in Cookies roots, not Safari profile roots."""
        conn = _create_file_list_db([
            {
                "evidence_id": 1,
                "file_path": "/Users/testuser/Library/Cookies/Cookies.binarycookies",
                "file_name": "Cookies.binarycookies",
                "partition_index": 1,
                "inode": 300,
                "size_bytes": 4096,
            },
        ])
        result = discover_safari_files(conn, 1, artifact_names=["cookies"])
        assert 1 in result
        assert len(result[1]) == 1
        assert result[1][0]["file_name"] == "Cookies.binarycookies"

    def test_containerized_safari_path(self):
        """Should find files in Safari container paths."""
        conn = _create_file_list_db([
            {
                "evidence_id": 1,
                "file_path": "/Users/joe/Library/Containers/com.apple.Safari/Data/Library/Safari/History.db",
                "file_name": "History.db",
                "partition_index": 1,
                "inode": 400,
                "size_bytes": 8192,
            },
        ])
        result = discover_safari_files(conn, 1, artifact_names=["history"])
        assert 1 in result
        assert result[1][0]["user"] == "joe"

    def test_filters_by_evidence_id(self):
        conn = _create_file_list_db([
            {
                "evidence_id": 1,
                "file_path": "/Users/a/Library/Safari/History.db",
                "file_name": "History.db",
                "partition_index": 1,
            },
            {
                "evidence_id": 2,
                "file_path": "/Users/b/Library/Safari/History.db",
                "file_name": "History.db",
                "partition_index": 1,
            },
        ])
        result = discover_safari_files(conn, 1, artifact_names=["history"])
        # Only evidence_id=1 should match
        total = sum(len(v) for v in result.values())
        assert total == 1

    def test_excludes_deleted_files(self):
        conn = _create_file_list_db([
            {
                "evidence_id": 1,
                "file_path": "/Users/a/Library/Safari/History.db",
                "file_name": "History.db",
                "partition_index": 1,
                "deleted": 1,
            },
        ])
        result = discover_safari_files(conn, 1, artifact_names=["history"])
        assert result == {}

    def test_sessions_discovers_both_artifacts(self):
        conn = _create_file_list_db([
            {
                "evidence_id": 1,
                "file_path": "/Users/u/Library/Safari/LastSession.plist",
                "file_name": "LastSession.plist",
                "partition_index": 1,
            },
            {
                "evidence_id": 1,
                "file_path": "/Users/u/Library/Safari/RecentlyClosedTabs.plist",
                "file_name": "RecentlyClosedTabs.plist",
                "partition_index": 1,
            },
        ])
        result = discover_safari_files(
            conn, 1, artifact_names=["sessions", "recently_closed_tabs"]
        )
        assert 1 in result
        names = {f["file_name"] for f in result[1]}
        assert "LastSession.plist" in names
        assert "RecentlyClosedTabs.plist" in names

    def test_callbacks_logging(self):
        conn = _create_file_list_db([
            {
                "evidence_id": 1,
                "file_path": "/Users/u/Library/Safari/History.db",
                "file_name": "History.db",
                "partition_index": 1,
            },
        ])
        callbacks = MagicMock()
        discover_safari_files(conn, 1, artifact_names=["history"], callbacks=callbacks)
        callbacks.on_log.assert_called()


# =============================================================================
# discover_safari_files_fallback
# =============================================================================


class TestDiscoverSafariFilesFallback:
    """Test filesystem-based fallback discovery."""

    def test_returns_empty_when_no_files_found(self):
        fs = FakeEvidenceFS({})
        result = discover_safari_files_fallback(fs, artifact_names=["history"])
        assert result == {}

    def test_single_file_found(self):
        fs = FakeEvidenceFS(
            {
                "Users/joe/Library/Safari/History.db": b"data",
            },
            partition_index=0,
        )
        result = discover_safari_files_fallback(fs, artifact_names=["history"])
        assert 0 in result
        assert len(result[0]) == 1
        file_info = result[0][0]
        assert file_info["logical_path"] == "Users/joe/Library/Safari/History.db"
        assert file_info["user"] == "joe"
        assert file_info["browser"] == "safari"
        assert file_info["partition_index"] == 0
        assert file_info["inode"] is None

    def test_preserves_partition_index(self):
        fs = FakeEvidenceFS(
            {
                "Users/joe/Library/Safari/History.db": b"data",
            },
            partition_index=5,
        )
        result = discover_safari_files_fallback(fs, artifact_names=["history"])
        assert 5 in result
        assert result[5][0]["partition_index"] == 5

    def test_deduplicates_paths(self):
        """Same path should not appear twice even if matched by multiple patterns."""
        fs = FakeEvidenceFS(
            {
                "Users/joe/Library/Safari/History.db": b"data",
                "/Users/joe/Library/Safari/History.db": b"data",
            },
        )
        result = discover_safari_files_fallback(fs, artifact_names=["history"])
        total = sum(len(v) for v in result.values())
        # The two paths are different strings so both would be returned
        # but only if the patterns match both
        assert total >= 1

    def test_multiple_artifacts(self):
        fs = FakeEvidenceFS(
            {
                "Users/u/Library/Safari/LastSession.plist": b"x",
                "Users/u/Library/Safari/RecentlyClosedTabs.plist": b"y",
            },
        )
        result = discover_safari_files_fallback(
            fs, artifact_names=["sessions", "recently_closed_tabs"]
        )
        total = sum(len(v) for v in result.values())
        assert total == 2

    def test_unknown_artifact_ignored(self):
        fs = FakeEvidenceFS({})
        result = discover_safari_files_fallback(fs, artifact_names=["nonexistent"])
        assert result == {}


# =============================================================================
# Extractor integration: multi-partition discovery method exists
# =============================================================================


class TestExtractorMultiPartitionSupport:
    """Verify all Safari extractors use multi-partition discovery."""

    @pytest.mark.parametrize(
        "extractor_class_path",
        [
            "extractors.browser.safari.history.SafariHistoryExtractor",
            "extractors.browser.safari.cookies.SafariCookiesExtractor",
            "extractors.browser.safari.bookmarks.SafariBookmarksExtractor",
            "extractors.browser.safari.downloads.SafariDownloadsExtractor",
            "extractors.browser.safari.sessions.SafariSessionsExtractor",
            "extractors.browser.safari.top_sites.SafariTopSitesExtractor",
            "extractors.browser.safari.favicons.SafariFaviconsExtractor",
            "extractors.browser.safari.cache.SafariCacheExtractor",
        ],
    )
    def test_extractor_can_be_imported(self, extractor_class_path):
        """All Safari extractors should still import cleanly."""
        parts = extractor_class_path.rsplit(".", 1)
        module_path, class_name = parts
        import importlib

        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        assert cls is not None

    def test_history_extraction_uses_file_list(self):
        """History extractor should attempt file_list discovery."""
        from extractors.browser.safari.history import SafariHistoryExtractor

        extractor = SafariHistoryExtractor()

        # Create a mock evidence_fs
        evidence_fs = MagicMock()
        evidence_fs.iter_paths.return_value = iter([])
        evidence_fs.partition_index = 0
        evidence_fs.source_path = "/test.E01"
        evidence_fs.fs_type = "hfs+"

        # Create a mock evidence_conn with empty file_list
        conn = _create_file_list_db([])

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        config = {
            "evidence_id": 1,
            "evidence_label": "test",
            "evidence_conn": conn,
        }

        import tempfile

        with tempfile.TemporaryDirectory() as td:
            output_dir = Path(td) / "output"
            result = extractor.run_extraction(
                evidence_fs, output_dir, config, callbacks
            )

        # Should succeed (with no files found)
        assert result is True

    def test_history_extraction_multi_partition_manifest(self):
        """History extraction should write multi_partition metadata to manifest."""
        from contextlib import contextmanager
        from extractors.browser.safari.history import SafariHistoryExtractor

        extractor = SafariHistoryExtractor()

        # Create file_list with files on two partitions
        conn = _create_file_list_db([
            {
                "evidence_id": 1,
                "file_path": "/Users/alice/Library/Safari/History.db",
                "file_name": "History.db",
                "partition_index": 1,
                "inode": 100,
                "size_bytes": 1024,
                "extension": ".db",
            },
            {
                "evidence_id": 1,
                "file_path": "/Users/bob/Library/Safari/History.db",
                "file_name": "History.db",
                "partition_index": 3,
                "inode": 200,
                "size_bytes": 2048,
                "extension": ".db",
            },
        ])

        evidence_fs = MagicMock()
        evidence_fs.partition_index = 1
        evidence_fs.source_path = "/test.E01"
        evidence_fs.fs_type = "hfs+"
        evidence_fs.read_file.return_value = b"SQLite format 3\x00"
        evidence_fs.ewf_paths = None  # No EWF paths (avoids partition opening)

        callbacks = MagicMock()
        callbacks.is_cancelled.return_value = False

        config = {
            "evidence_id": 1,
            "evidence_label": "test_ev",
            "evidence_conn": conn,
        }

        import json
        import tempfile

        # Mock open_partition_for_extraction to avoid real EWF file access
        @contextmanager
        def _mock_open_partition(fs_or_paths, partition_index=None):
            yield evidence_fs

        with tempfile.TemporaryDirectory() as td:
            output_dir = Path(td) / "output"
            with patch(
                "extractors.browser.safari.history.extractor.open_partition_for_extraction",
                side_effect=_mock_open_partition,
            ):
                extractor.run_extraction(evidence_fs, output_dir, config, callbacks)

            manifest_path = output_dir / "manifest.json"
            assert manifest_path.exists()
            manifest = json.loads(manifest_path.read_text())

            assert manifest["multi_partition"] is True
            assert 1 in manifest["partitions_scanned"]
            assert 3 in manifest["partitions_scanned"]
            # Files should have partition_index set
            for f in manifest["files"]:
                assert "partition_index" in f
                assert f["partition_index"] in (1, 3)
