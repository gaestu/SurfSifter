"""
Tests for cache directory isolation during extraction.

Verifies that multiple source cache directories sharing the same
(partition, browser, profile) produce separate output subdirectories,
preventing file-name collisions (e.g. data_0, index) and ensuring
blockfile ingestion treats each as an independent cache.
"""
import hashlib
import struct
from pathlib import Path

import pytest

from extractors.browser.chromium.cache._workers import cache_dir_id
from extractors.browser.chromium.cache._blockfile_ingestion import (
    find_blockfile_directories,
)
from extractors.browser.chromium.cache.blockfile import BLOCKFILE_INDEX_MAGIC


# ---------------------------------------------------------------------------
# cache_dir_id tests
# ---------------------------------------------------------------------------


class TestCacheDirId:
    """Tests for the cache_dir_id helper function."""

    def test_returns_8char_hex(self):
        """Should return an 8-character lowercase hex string."""
        result = cache_dir_id("Users/PC/AppData/Local/Cache")
        assert len(result) == 8
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic(self):
        """Same input should always produce the same output."""
        path = "Application/cache"
        assert cache_dir_id(path) == cache_dir_id(path)

    def test_different_paths_different_ids(self):
        """Different source paths should (overwhelmingly) produce different IDs."""
        id1 = cache_dir_id("Application/cache")
        id2 = cache_dir_id("Users/PC/AppData/Local/EBWebView/Default/Cache/Cache_Data")
        assert id1 != id2

    def test_empty_path(self):
        """Empty string should still produce a valid ID."""
        result = cache_dir_id("")
        assert len(result) == 8
        assert all(c in "0123456789abcdef" for c in result)

    def test_unicode_path(self):
        """Unicode characters in the path should not raise."""
        result = cache_dir_id("Users/Ünïcödé/Cache")
        assert len(result) == 8

    def test_matches_md5_prefix(self):
        """Should be the first 8 chars of MD5 hex digest."""
        path = "some/cache/path"
        expected = hashlib.md5(path.encode("utf-8")).hexdigest()[:8]
        assert cache_dir_id(path) == expected


# ---------------------------------------------------------------------------
# find_blockfile_directories — isolation tests
# ---------------------------------------------------------------------------

def _make_blockfile_index(num_entries: int = 0) -> bytes:
    """
    Create a minimal valid blockfile index file.

    Has the correct magic number and header but empty hash table.
    """
    # Index header: 256 bytes
    # magic(4), version(4), num_entries(4), num_bytes(4), last_file(4),
    # this_id(4), stats_addr(4), table_len(4), crash(4), experiment(4),
    # create_time(8) = 44 bytes of fields, padded to 256
    header = struct.pack(
        "<IIIIIIIII",
        BLOCKFILE_INDEX_MAGIC,  # magic
        0x20000,  # version 2.0
        num_entries,  # num_entries
        0,  # num_bytes
        0,  # last_file
        0,  # this_id
        0,  # stats_addr
        0x10000,  # table_len (65536)
        0,  # crash
    )
    header += b"\x00" * (256 - len(header))
    # Empty hash table
    header += b"\x00\x00\x00\x00" * 0x10000
    return header


def _make_block_file(block_size: int = 256) -> bytes:
    """Create a minimal valid data_N block file with header."""
    from extractors.browser.chromium.cache._schemas import BLOCKFILE_BLOCK_MAGIC

    # Block header: 8192 bytes
    header = struct.pack(
        "<IIIII",
        BLOCKFILE_BLOCK_MAGIC,  # magic
        0x20000,  # version
        0,  # this_file_index
        0,  # next_file_index
        block_size,  # block_size
    )
    header += b"\x00" * (8192 - len(header))
    # A few empty blocks
    header += b"\x00" * (block_size * 4)
    return header


class TestFindBlockfileDirectoriesIsolation:
    """Test that find_blockfile_directories groups by extracted parent dir."""

    def test_separate_subdirs_produce_separate_groups(self, tmp_path):
        """
        When files from two different source caches are in separate
        subdirectories (as they should be with the dir_id fix), they
        produce two independent blockfile groups.
        """
        # Create two blockfile cache subdirectories under the same profile
        dir_a = tmp_path / "run" / "p3_chromium_embedded_Default" / "aabbccdd"
        dir_b = tmp_path / "run" / "p3_chromium_embedded_Default" / "11223344"
        dir_a.mkdir(parents=True)
        dir_b.mkdir(parents=True)

        # Write valid blockfile data to both directories
        for d in (dir_a, dir_b):
            (d / "index").write_bytes(_make_blockfile_index())
            (d / "data_1").write_bytes(_make_block_file(256))
            (d / "data_0").write_bytes(_make_block_file(36))

        # Manifest file entries pointing to separate subdirectories
        files = [
            {"extracted_path": str(dir_a / "index")},
            {"extracted_path": str(dir_a / "data_1")},
            {"extracted_path": str(dir_a / "data_0")},
            {"extracted_path": str(dir_b / "index")},
            {"extracted_path": str(dir_b / "data_1")},
            {"extracted_path": str(dir_b / "data_0")},
        ]

        blockfile_dirs = find_blockfile_directories(files, tmp_path)

        assert len(blockfile_dirs) == 2, (
            "Two separate subdirectories should produce two blockfile groups"
        )
        found_paths = {bd["path"] for bd in blockfile_dirs}
        assert dir_a in found_paths
        assert dir_b in found_paths

    def test_flat_dir_produces_single_group(self, tmp_path):
        """
        When all blockfile files are in the same directory (old behavior),
        find_blockfile_directories returns a single group.
        """
        flat_dir = tmp_path / "run" / "p3_chromium_embedded_Default"
        flat_dir.mkdir(parents=True)
        (flat_dir / "index").write_bytes(_make_blockfile_index())
        (flat_dir / "data_1").write_bytes(_make_block_file(256))
        (flat_dir / "data_0").write_bytes(_make_block_file(36))
        (flat_dir / "f_000001").write_bytes(b"\x00" * 100)

        files = [
            {"extracted_path": str(flat_dir / "index")},
            {"extracted_path": str(flat_dir / "data_1")},
            {"extracted_path": str(flat_dir / "data_0")},
            {"extracted_path": str(flat_dir / "f_000001")},
        ]

        blockfile_dirs = find_blockfile_directories(files, tmp_path)
        assert len(blockfile_dirs) == 1

    def test_non_blockfile_dir_excluded(self, tmp_path):
        """Directory without index or data_1 should not be detected."""
        non_cache = tmp_path / "run" / "some_dir"
        non_cache.mkdir(parents=True)
        (non_cache / "f_000001").write_bytes(b"\x00" * 100)
        (non_cache / "random.txt").write_bytes(b"hello")

        files = [
            {"extracted_path": str(non_cache / "f_000001")},
            {"extracted_path": str(non_cache / "random.txt")},
        ]

        blockfile_dirs = find_blockfile_directories(files, tmp_path)
        assert len(blockfile_dirs) == 0
