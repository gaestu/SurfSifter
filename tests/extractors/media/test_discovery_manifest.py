"""
Tests for filesystem images discovery manifest functionality.

The discovery manifest (files_to_extract.csv + discovery_summary.json)
helps debug extraction issues by showing what files will be extracted
before extraction begins.
"""

from __future__ import annotations

import csv
import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from extractors.media.filesystem_images.extractor import (
    FilesystemImagesExtractor,
    _format_size,
)


class TestFormatSize:
    """Tests for _format_size helper function."""

    def test_bytes(self):
        assert _format_size(0) == "0 B"
        assert _format_size(100) == "100 B"
        assert _format_size(1023) == "1023 B"

    def test_kilobytes(self):
        assert _format_size(1024) == "1.00 KB"
        assert _format_size(1536) == "1.50 KB"
        assert _format_size(10240) == "10.00 KB"

    def test_megabytes(self):
        assert _format_size(1024 * 1024) == "1.00 MB"
        assert _format_size(1024 * 1024 * 5) == "5.00 MB"
        assert _format_size(1024 * 1024 * 500) == "500.00 MB"

    def test_gigabytes(self):
        assert _format_size(1024 * 1024 * 1024) == "1.00 GB"
        assert _format_size(1024 * 1024 * 1024 * 2) == "2.00 GB"


class TestDiscoveryManifest:
    """Tests for _write_discovery_manifest method."""

    def test_writes_csv_file(self, tmp_path):
        """Test that CSV file is created with correct headers."""
        from extractors.media.filesystem_images.parallel_extractor import ExtractionTask

        tasks = [
            ExtractionTask(
                fs_path="Users/test/Pictures/photo.jpg",
                filename="photo.jpg",
                size_bytes=1024,
                mtime_epoch=None,
                crtime_epoch=None,
                atime_epoch=None,
                ctime_epoch=None,
                inode=12345,
            ),
        ]
        tasks_by_partition = {0: tasks}

        callbacks = MagicMock()
        callbacks.on_step = MagicMock()
        callbacks.on_log = MagicMock()

        FilesystemImagesExtractor._write_discovery_manifest(
            tmp_path, tasks, tasks_by_partition, callbacks
        )

        csv_path = tmp_path / "files_to_extract.csv"
        assert csv_path.exists()

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            headers = next(reader)
            assert headers == ["fs_path", "filename", "extension", "size_bytes", "partition_index", "inode"]

            row = next(reader)
            assert row[0] == "Users/test/Pictures/photo.jpg"
            assert row[1] == "photo.jpg"
            assert row[2] == ".jpg"
            assert row[3] == "1024"
            assert row[4] == "0"  # partition_index
            assert row[5] == "12345"  # inode

    def test_writes_summary_json(self, tmp_path):
        """Test that summary JSON file is created with statistics."""
        from extractors.media.filesystem_images.parallel_extractor import ExtractionTask

        tasks = [
            ExtractionTask(
                fs_path="Users/test/Pictures/photo1.jpg",
                filename="photo1.jpg",
                size_bytes=1024,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None,
                inode=100,
            ),
            ExtractionTask(
                fs_path="Users/test/Pictures/photo2.jpg",
                filename="photo2.jpg",
                size_bytes=2048,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None,
                inode=101,
            ),
            ExtractionTask(
                fs_path="Users/test/Pictures/image.png",
                filename="image.png",
                size_bytes=5120,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None,
                inode=102,
            ),
        ]
        tasks_by_partition = {0: tasks}

        callbacks = MagicMock()
        callbacks.on_step = MagicMock()
        callbacks.on_log = MagicMock()

        FilesystemImagesExtractor._write_discovery_manifest(
            tmp_path, tasks, tasks_by_partition, callbacks
        )

        summary_path = tmp_path / "discovery_summary.json"
        assert summary_path.exists()

        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)

        assert summary["total_files"] == 3
        assert summary["total_size_bytes"] == 8192
        assert summary["total_size_human"] == "8.00 KB"

        # Extension breakdown
        assert summary["by_extension"]["counts"][".jpg"] == 2
        assert summary["by_extension"]["counts"][".png"] == 1

        # Partition breakdown
        assert summary["by_partition"]["counts"]["partition_0"] == 3

        # Size distribution
        assert summary["size_distribution"]["1KB_to_10KB"] == 3

    def test_multiple_partitions(self, tmp_path):
        """Test that multiple partitions are tracked separately."""
        from extractors.media.filesystem_images.parallel_extractor import ExtractionTask

        task1 = ExtractionTask(
            fs_path="Users/test/pic1.jpg",
            filename="pic1.jpg",
            size_bytes=1024,
            mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None,
            inode=100,
        )
        task2 = ExtractionTask(
            fs_path="Users/test/pic2.jpg",
            filename="pic2.jpg",
            size_bytes=2048,
            mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None,
            inode=200,
        )

        tasks = [task1, task2]
        tasks_by_partition = {0: [task1], 2: [task2]}

        callbacks = MagicMock()
        callbacks.on_step = MagicMock()
        callbacks.on_log = MagicMock()

        FilesystemImagesExtractor._write_discovery_manifest(
            tmp_path, tasks, tasks_by_partition, callbacks
        )

        summary_path = tmp_path / "discovery_summary.json"
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)

        assert summary["by_partition"]["counts"]["partition_0"] == 1
        assert summary["by_partition"]["counts"]["partition_2"] == 1

    def test_size_distribution_buckets(self, tmp_path):
        """Test that size distribution buckets are calculated correctly."""
        from extractors.media.filesystem_images.parallel_extractor import ExtractionTask

        tasks = [
            ExtractionTask(
                fs_path="zero.jpg", filename="zero.jpg", size_bytes=0,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None, inode=1,
            ),
            ExtractionTask(
                fs_path="tiny.jpg", filename="tiny.jpg", size_bytes=500,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None, inode=2,
            ),
            ExtractionTask(
                fs_path="small.jpg", filename="small.jpg", size_bytes=5 * 1024,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None, inode=3,
            ),
            ExtractionTask(
                fs_path="medium.jpg", filename="medium.jpg", size_bytes=50 * 1024,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None, inode=4,
            ),
            ExtractionTask(
                fs_path="large.jpg", filename="large.jpg", size_bytes=500 * 1024,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None, inode=5,
            ),
            ExtractionTask(
                fs_path="huge.jpg", filename="huge.jpg", size_bytes=5 * 1024 * 1024,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None, inode=6,
            ),
            ExtractionTask(
                fs_path="giant.jpg", filename="giant.jpg", size_bytes=50 * 1024 * 1024,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None, inode=7,
            ),
            ExtractionTask(
                fs_path="massive.jpg", filename="massive.jpg", size_bytes=500 * 1024 * 1024,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None, inode=8,
            ),
        ]
        tasks_by_partition = {0: tasks}

        callbacks = MagicMock()
        callbacks.on_step = MagicMock()
        callbacks.on_log = MagicMock()

        FilesystemImagesExtractor._write_discovery_manifest(
            tmp_path, tasks, tasks_by_partition, callbacks
        )

        summary_path = tmp_path / "discovery_summary.json"
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)

        dist = summary["size_distribution"]
        assert dist["0_bytes"] == 1
        assert dist["1B_to_1KB"] == 1
        assert dist["1KB_to_10KB"] == 1
        assert dist["10KB_to_100KB"] == 1
        assert dist["100KB_to_1MB"] == 1
        assert dist["1MB_to_10MB"] == 1
        assert dist["10MB_to_100MB"] == 1
        assert dist["100MB_plus"] == 1

    def test_auto_partition_label(self, tmp_path):
        """Test that partition -1 is labeled as partition_auto."""
        from extractors.media.filesystem_images.parallel_extractor import ExtractionTask

        task = ExtractionTask(
            fs_path="pic.jpg", filename="pic.jpg", size_bytes=1024,
            mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None, inode=1,
        )
        tasks = [task]
        tasks_by_partition = {-1: [task]}

        callbacks = MagicMock()
        callbacks.on_step = MagicMock()
        callbacks.on_log = MagicMock()

        FilesystemImagesExtractor._write_discovery_manifest(
            tmp_path, tasks, tasks_by_partition, callbacks
        )

        summary_path = tmp_path / "discovery_summary.json"
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)

        assert "partition_auto" in summary["by_partition"]["counts"]
        assert summary["by_partition"]["counts"]["partition_auto"] == 1

    def test_callbacks_called(self, tmp_path):
        """Test that progress callbacks are invoked."""
        from extractors.media.filesystem_images.parallel_extractor import ExtractionTask

        tasks = [
            ExtractionTask(
                fs_path="pic.jpg", filename="pic.jpg", size_bytes=1024,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None, inode=1,
            ),
        ]
        tasks_by_partition = {0: tasks}

        callbacks = MagicMock()
        callbacks.on_step = MagicMock()
        callbacks.on_log = MagicMock()

        FilesystemImagesExtractor._write_discovery_manifest(
            tmp_path, tasks, tasks_by_partition, callbacks
        )

        callbacks.on_step.assert_called()
        callbacks.on_log.assert_called()

        # Check log message mentions the files
        log_call = callbacks.on_log.call_args
        assert "files_to_extract.csv" in log_call[0][0]
        assert "discovery_summary.json" in log_call[0][0]

    def test_no_extension_file(self, tmp_path):
        """Test handling of files without extension."""
        from extractors.media.filesystem_images.parallel_extractor import ExtractionTask

        tasks = [
            ExtractionTask(
                fs_path="Users/test/README",
                filename="README",
                size_bytes=1024,
                mtime_epoch=None, crtime_epoch=None, atime_epoch=None, ctime_epoch=None,
                inode=100,
            ),
        ]
        tasks_by_partition = {0: tasks}

        callbacks = MagicMock()
        callbacks.on_step = MagicMock()
        callbacks.on_log = MagicMock()

        FilesystemImagesExtractor._write_discovery_manifest(
            tmp_path, tasks, tasks_by_partition, callbacks
        )

        summary_path = tmp_path / "discovery_summary.json"
        with open(summary_path, "r", encoding="utf-8") as f:
            summary = json.load(f)

        # Files without extension get empty string in CSV, "(no extension)" in summary
        # The CSV uses raw extension, summary uses labeled version
        assert "(no extension)" in summary["by_extension"]["counts"]


class TestMatchesPatterns:
    """Tests for _matches_patterns helper method."""

    def test_no_patterns_matches_all(self):
        """Without patterns, all paths should match."""
        assert FilesystemImagesExtractor._matches_patterns("any/path.jpg", [], []) is True
        assert FilesystemImagesExtractor._matches_patterns("Users/test/photo.png", [], []) is True

    def test_include_pattern_matches(self):
        """Include patterns should filter paths correctly."""
        # Match with wildcard
        assert FilesystemImagesExtractor._matches_patterns(
            "Users/test/Pictures/photo.jpg",
            ["*/Pictures/*"],
            []
        ) is True

        # No match
        assert FilesystemImagesExtractor._matches_patterns(
            "Users/test/Documents/file.jpg",
            ["*/Pictures/*"],
            []
        ) is False

    def test_exclude_pattern_excludes(self):
        """Exclude patterns should filter out matching paths."""
        # Path matches exclude - rejected
        assert FilesystemImagesExtractor._matches_patterns(
            "Users/test/AppData/cache.jpg",
            [],
            ["*/AppData/*"]
        ) is False

        # Path doesn't match exclude - accepted
        assert FilesystemImagesExtractor._matches_patterns(
            "Users/test/Pictures/photo.jpg",
            [],
            ["*/AppData/*"]
        ) is True

    def test_include_and_exclude_combined(self):
        """Both include and exclude patterns should work together."""
        include = ["*/Pictures/*"]
        exclude = ["*/Pictures/thumbnails/*"]

        # Matches include, not exclude - accepted
        assert FilesystemImagesExtractor._matches_patterns(
            "Users/test/Pictures/photo.jpg",
            include,
            exclude
        ) is True

        # Matches include AND exclude - rejected (exclude wins)
        assert FilesystemImagesExtractor._matches_patterns(
            "Users/test/Pictures/thumbnails/thumb.jpg",
            include,
            exclude
        ) is False

        # Doesn't match include - rejected
        assert FilesystemImagesExtractor._matches_patterns(
            "Users/test/Documents/doc.jpg",
            include,
            exclude
        ) is False

    def test_case_insensitive_matching(self):
        """Pattern matching should be case-insensitive."""
        assert FilesystemImagesExtractor._matches_patterns(
            "USERS/TEST/PICTURES/PHOTO.JPG",
            ["*/pictures/*"],
            []
        ) is True

        assert FilesystemImagesExtractor._matches_patterns(
            "users/test/pictures/photo.jpg",
            ["*/PICTURES/*"],
            []
        ) is True

    def test_backslash_normalization_for_windows_paths(self):
        """Windows backslash paths should be normalized for glob matching.

        FTK/EnCase CSV imports may contain Windows-style paths like:
            C:\\Users\\John\\Pictures\\photo.jpg

        These should match patterns like:
            */Users/*/Pictures/*
        """
        # Windows path with backslashes
        windows_path = r"C:\Users\John\Pictures\photo.jpg"

        # Pattern uses forward slashes (standard glob style)
        include_pattern = ["*/Users/*/Pictures/*"]

        # Should match after normalization
        assert FilesystemImagesExtractor._matches_patterns(
            windows_path,
            include_pattern,
            []
        ) is True

        # Exclude pattern should also work
        exclude_pattern = ["*/Users/*/Pictures/*"]
        assert FilesystemImagesExtractor._matches_patterns(
            windows_path,
            [],
            exclude_pattern
        ) is False

    def test_mixed_slash_paths(self):
        """Paths with mixed slashes should still match correctly."""
        # Mixed slashes (shouldn't happen but be robust)
        mixed_path = r"Users\test/Pictures\subdir/photo.jpg"

        assert FilesystemImagesExtractor._matches_patterns(
            mixed_path,
            ["*/Pictures/*"],
            []
        ) is True

    def test_double_star_glob_pattern(self):
        """Double-star (**) patterns should match any depth."""
        # fnmatch doesn't support ** natively, but we can test * behavior
        # Note: fnmatch uses shell-style wildcards, not full glob
        assert FilesystemImagesExtractor._matches_patterns(
            "Users/test/deep/nested/path/photo.jpg",
            ["*photo.jpg"],
            []
        ) is True

