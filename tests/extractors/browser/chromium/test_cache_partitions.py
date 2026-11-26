"""Chromium Cache: which partitions are opened, for the disk cache and for CacheStorage.

The partitions to open come from the file list. Before #88, only the disk
cache was looked up there, so with only Service Worker CacheStorage selected
the extractor searched the open partition only.
"""

from __future__ import annotations

import fnmatch
import json

import pytest

from extractors._shared.file_list_discovery import FileListDiscoveryResult, FileListQueryError
from extractors.browser.chromium._patterns import get_all_browsers
from extractors.browser.chromium.cache import extractor as cache_module
from extractors.browser.chromium.cache._discovery import (
    CACHE_STORAGE_FILE_LIST_NAMES,
    CACHE_STORAGE_FILE_PATTERNS,
)
from extractors.browser.chromium.cache.extractor import CacheSimpleExtractor
from tests.fixtures.file_list_discovery import (
    EWF_PATHS,
    PartitionFS,
    add_to_file_list,
    make_partition_roots,
    mock_callbacks,
    patch_open_ewf_partition,
    write_file,
)

CHROME = "Users/alice/AppData/Local/Google/Chrome/User Data/Default"
DISK_CACHE_DIR = f"{CHROME}/Cache/Cache_Data"
# Service Worker/CacheStorage/{origin hash}/{cache id}/{files}
STORAGE_CACHE_DIR = f"{CHROME}/Service Worker/CacheStorage/0a1b2c3d4e5f/7f6e5d4c-cache"
STORAGE_FILES = ("index", "0123456789abcdef_0")


@pytest.fixture
def callbacks():
    return mock_callbacks()


@pytest.fixture
def roots(tmp_path):
    """Directories of partitions 1 and 2."""
    return make_partition_roots(tmp_path)


@pytest.fixture
def opened_partitions(monkeypatch, roots):
    """Replace open_ewf_partition; the dict maps partition index -> opened filesystem."""
    return patch_open_ewf_partition(monkeypatch, roots)


def add_files(conn, evidence_id, root, directory, names, partition_index):
    """Create files on a partition and list them in the file list."""
    for name in names:
        write_file(root, f"{directory}/{name}", b"cache " + name.encode())
        add_to_file_list(conn, evidence_id, f"/{directory}/{name}", partition_index)


def discover(conn, evidence_id, *, disk, storage, browsers=("chrome",)):
    return CacheSimpleExtractor()._discover_cache_partitions(
        evidence_conn=conn,
        evidence_id=evidence_id,
        browsers=list(browsers),
        include_disk_cache=disk,
        include_cache_storage=storage,
    )


class TestDiscoverCachePartitions:
    """_discover_cache_partitions() with the disk cache, CacheStorage or both."""

    def test_cache_storage_only_on_second_partition(self, evidence_db, case_context):
        """The case of #88: before, [None] (the open partition)."""
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{STORAGE_CACHE_DIR}/index", 2)

        assert discover(evidence_db, case_context.evidence_id, disk=False, storage=True) == [2]

    @pytest.mark.parametrize(
        "name", ["f_000001", "data_1", "0123456789abcdef_0", "0123456789abcdef_1", "0123456789abcdef_s"]
    )
    def test_cache_storage_files_without_index_are_found(self, name, evidence_db, case_context):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{STORAGE_CACHE_DIR}/{name}", 2)

        assert discover(evidence_db, case_context.evidence_id, disk=False, storage=True) == [2]

    @pytest.mark.parametrize("path", [
        "0a1b2c3d4e5f/index.txt",
        "0a1b2c3d4e5f/7f6e5d4c-cache/index-dir/the-real-index",
    ])
    def test_files_the_scan_does_not_extract_do_not_count(self, path, evidence_db, case_context):
        add_to_file_list(
            evidence_db, case_context.evidence_id, f"/{CHROME}/Service Worker/CacheStorage/{path}", 2
        )

        assert discover(evidence_db, case_context.evidence_id, disk=False, storage=True) == [None]

    def test_both_kinds_give_both_partitions(self, evidence_db, case_context):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{DISK_CACHE_DIR}/index", 1)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{STORAGE_CACHE_DIR}/index", 2)

        assert discover(evidence_db, case_context.evidence_id, disk=True, storage=True) == [1, 2]

    def test_disk_cache_only_ignores_cache_storage(self, evidence_db, case_context, monkeypatch):
        """Unchanged: without CacheStorage selected its files do not select a partition."""
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{STORAGE_CACHE_DIR}/index", 2)

        def no_query(*args, **kwargs):
            raise AssertionError("no CacheStorage query expected")

        monkeypatch.setattr(cache_module, "discover_from_file_list", no_query)

        assert discover(evidence_db, case_context.evidence_id, disk=True, storage=False) == [None]

    def test_disk_cache_only_is_unchanged(self, evidence_db, case_context):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{DISK_CACHE_DIR}/index", 2)
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{STORAGE_CACHE_DIR}/index", 1)

        assert discover(evidence_db, case_context.evidence_id, disk=True, storage=False) == [2]

    def test_cache_storage_only_does_not_search_disk_cache(self, evidence_db, case_context):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{DISK_CACHE_DIR}/index", 2)

        assert discover(evidence_db, case_context.evidence_id, disk=False, storage=True) == [None]

    def test_default_argument_does_not_search_cache_storage(self, evidence_db, case_context):
        """Callers that do not pass include_cache_storage keep the old behaviour."""
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{STORAGE_CACHE_DIR}/index", 2)

        partitions = CacheSimpleExtractor()._discover_cache_partitions(
            evidence_conn=evidence_db,
            evidence_id=case_context.evidence_id,
            browsers=["chrome"],
            include_disk_cache=True,
        )

        assert partitions == [None]

    def test_cache_storage_with_all_browsers(self, evidence_db, case_context):
        opera = "Users/alice/AppData/Roaming/Opera Software/Opera Stable/Service Worker/CacheStorage/ab/cd"
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{opera}/index", 2)

        partitions = discover(
            evidence_db, case_context.evidence_id, disk=False, storage=True, browsers=get_all_browsers()
        )

        assert partitions == [2]

    def test_cache_storage_with_all_browsers_and_no_match(self, evidence_db, case_context):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{CHROME}/History", 2)

        partitions = discover(
            evidence_db, case_context.evidence_id, disk=False, storage=True, browsers=get_all_browsers()
        )

        assert partitions == [None]

    def test_unselected_browser_is_not_searched(self, evidence_db, case_context):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{STORAGE_CACHE_DIR}/index", 2)

        assert discover(evidence_db, case_context.evidence_id, disk=False, storage=True, browsers=["edge"]) == [None]

    def test_deleted_files_are_ignored(self, evidence_db, case_context):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{STORAGE_CACHE_DIR}/index", 2)
        evidence_db.execute("UPDATE file_list SET deleted = 1")
        evidence_db.commit()

        assert discover(evidence_db, case_context.evidence_id, disk=False, storage=True) == [None]

    def test_failed_cache_storage_query_raises(self, evidence_db, case_context, monkeypatch):
        add_to_file_list(evidence_db, case_context.evidence_id, f"/{STORAGE_CACHE_DIR}/index", 2)
        monkeypatch.setattr(
            cache_module, "discover_from_file_list",
            lambda *args, **kwargs: FileListDiscoveryResult(error="database is locked"),
        )

        with pytest.raises(FileListQueryError, match="database is locked"):
            discover(evidence_db, case_context.evidence_id, disk=True, storage=True)


class TestFileListNamesMatchTheScan:
    """The file list lookup and the CacheStorage scan look for the same files."""

    def test_every_scanned_file_is_looked_up(self):
        assert len(CACHE_STORAGE_FILE_LIST_NAMES) == len(CACHE_STORAGE_FILE_PATTERNS)
        for scan_pattern, list_name in zip(CACHE_STORAGE_FILE_PATTERNS, CACHE_STORAGE_FILE_LIST_NAMES):
            assert "[" not in list_name, "file list name patterns have no character classes"
            assert fnmatch.fnmatchcase(scan_pattern.replace("[0-9a-f]", "a"), list_name)

    def test_extractor_uses_the_shared_names(self):
        assert cache_module.CACHE_STORAGE_FILE_LIST_NAMES is CACHE_STORAGE_FILE_LIST_NAMES


class TestCacheStorageRun:
    """run_extraction() with only Service Worker CacheStorage selected."""

    def config(self, evidence_db, case_context, **extra):
        return {
            "evidence_conn": evidence_db,
            "evidence_id": case_context.evidence_id,
            "browsers": ["chrome"],
            "include_disk_cache": False,
            "include_cache_storage": True,
            **extra,
        }

    def test_cache_storage_on_second_partition_is_extracted(
        self, roots, tmp_path, callbacks, evidence_db, case_context, opened_partitions, monkeypatch
    ):
        add_files(evidence_db, case_context.evidence_id, roots[2], STORAGE_CACHE_DIR, STORAGE_FILES, 2)
        handed_over = []

        def fake_concurrent(self, evidence_fs, files_to_extract, *args, **kwargs):
            handed_over.append((
                evidence_fs.partition_index,
                sorted((f["path"], d["cache_type"]) for f, d in files_to_extract),
            ))

        monkeypatch.setattr(CacheSimpleExtractor, "_run_concurrent_extraction", fake_concurrent)
        fs = PartitionFS(roots[1], 1, EWF_PATHS)

        ok = CacheSimpleExtractor().run_extraction(
            fs, tmp_path / "output", self.config(evidence_db, case_context), callbacks
        )

        assert ok is True
        assert list(opened_partitions) == [2]
        assert fs.scanned_patterns == [], "the open partition has no cache in the file list"
        assert handed_over == [(2, [
            (f"{STORAGE_CACHE_DIR}/{name}", "cache_storage") for name in sorted(STORAGE_FILES)
        ])]

    def test_both_kinds_on_different_partitions_are_extracted(
        self, roots, tmp_path, callbacks, evidence_db, case_context, opened_partitions, monkeypatch
    ):
        add_files(evidence_db, case_context.evidence_id, roots[1], DISK_CACHE_DIR, ("index", "f_000001"), 1)
        add_files(evidence_db, case_context.evidence_id, roots[2], STORAGE_CACHE_DIR, STORAGE_FILES, 2)
        handed_over = {}

        def fake_concurrent(self, evidence_fs, files_to_extract, *args, **kwargs):
            handed_over[evidence_fs.partition_index] = {d["cache_type"] for _, d in files_to_extract}

        monkeypatch.setattr(CacheSimpleExtractor, "_run_concurrent_extraction", fake_concurrent)

        ok = CacheSimpleExtractor().run_extraction(
            PartitionFS(roots[1], 1, EWF_PATHS), tmp_path / "output",
            self.config(evidence_db, case_context, include_disk_cache=True), callbacks,
        )

        assert ok is True
        assert sorted(opened_partitions) == [1, 2]
        assert handed_over == {1: {"disk_cache"}, 2: {"cache_storage"}}

    def test_failed_cache_storage_query_ends_the_run_with_error(
        self, roots, tmp_path, callbacks, evidence_db, case_context, monkeypatch
    ):
        # CacheStorage is on the open partition: a search would find it.
        add_files(evidence_db, case_context.evidence_id, roots[1], STORAGE_CACHE_DIR, STORAGE_FILES, 1)
        monkeypatch.setattr(
            cache_module, "discover_from_file_list",
            lambda *args, **kwargs: FileListDiscoveryResult(error="database is locked"),
        )
        fs = PartitionFS(roots[1], 1, EWF_PATHS)
        output_dir = tmp_path / "output"

        ok = CacheSimpleExtractor().run_extraction(
            fs, output_dir, self.config(evidence_db, case_context), callbacks
        )

        assert ok is False
        manifest = json.loads(next(output_dir.glob("*/manifest.json")).read_text())
        assert manifest["status"] == "error"
        assert manifest["files"] == []
        assert "database is locked" in manifest["notes"][0]
        assert fs.scanned_patterns == []
