"""
Unit tests for Application Cache (AppCache) ingestion.

Tests the _appcache_ingestion.py module which handles:
- Discovery of Application Cache directories (find_appcache_directories)
- Reading the SQLite Index (Groups, Caches, Entries)
- Resolving numeric blockfile keys to real URLs
- Ingesting into evidence database
"""

from __future__ import annotations

import json
import os
import sqlite3
import struct
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from extractors.browser.chromium.cache._appcache_ingestion import (
    find_appcache_directories,
    ingest_appcache_directory,
    _is_appcache_index,
    _read_appcache_index,
    _webkit_to_datetime,
)
from extractors.browser.chromium.cache.blockfile import (
    BLOCKFILE_INDEX_MAGIC,
    BLOCKFILE_BLOCK_MAGIC,
    BLOCK_HEADER_SIZE,
    ENTRY_STORE_SIZE,
)
from core.database import DatabaseManager


# ---------------------------------------------------------------------------
# Helpers for building test fixtures
# ---------------------------------------------------------------------------

def _create_appcache_index(path: Path, groups=None, entries=None):
    """Create a minimal Application Cache SQLite Index database."""
    conn = sqlite3.connect(str(path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Groups (
            group_id INTEGER PRIMARY KEY,
            origin TEXT,
            manifest_url TEXT,
            creation_time INTEGER DEFAULT 0,
            last_access_time INTEGER DEFAULT 0,
            last_full_update_check_time INTEGER DEFAULT 0,
            first_evictable_error_time INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Caches (
            cache_id INTEGER PRIMARY KEY,
            group_id INTEGER,
            online INTEGER DEFAULT 1,
            update_time INTEGER DEFAULT 0,
            cache_size INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Entries (
            cache_id INTEGER,
            url TEXT,
            flags INTEGER,
            response_id INTEGER,
            response_size INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Namespaces (
            cache_id INTEGER,
            origin TEXT,
            type INTEGER,
            namespace_url TEXT,
            target_url TEXT,
            is_pattern INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS OnlineWhiteLists (
            cache_id INTEGER,
            namespace_url TEXT,
            is_pattern INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS DeletableResponseIds (
            response_id INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT,
            value TEXT
        )
    """)

    if groups is None:
        groups = [
            (1, "https://example.com/", "https://example.com/app/cache.manifest",
             13394403536764131, 13394403536764131),
        ]
    for g in groups:
        conn.execute(
            "INSERT INTO Groups (group_id, origin, manifest_url, creation_time, last_access_time) "
            "VALUES (?, ?, ?, ?, ?)",
            g,
        )
        conn.execute(
            "INSERT INTO Caches (cache_id, group_id, online, update_time, cache_size) "
            "VALUES (?, ?, 1, ?, 0)",
            (g[0], g[0], g[3]),
        )

    if entries is None:
        entries = [
            (1, "https://example.com/app/cache.manifest", 2, 1, 3937),
            (1, "https://example.com/app/main.html", 1, 2, 215479),
            (1, "https://example.com/app/style.css", 4, 3, 44420),
        ]
    for e in entries:
        conn.execute(
            "INSERT INTO Entries (cache_id, url, flags, response_id, response_size) "
            "VALUES (?, ?, ?, ?, ?)",
            e,
        )

    conn.commit()
    conn.close()


def _create_minimal_blockfile(cache_dir: Path, entry_count: int = 0):
    """
    Create a minimal valid blockfile cache directory.

    Creates index, data_0, data_1 files with valid headers.
    Entries can be added separately if needed.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)

    # table_len must be a power of 2 and >= 0x10000
    table_len = 0x10000

    # Create index file
    # IndexHeader: magic, version, num_entries, num_bytes, last_file,
    #              this_id, stats_addr, table_len, crash, experiment, create_time
    index_header = struct.pack(
        "<IIIIIIIII",
        BLOCKFILE_INDEX_MAGIC,   # magic
        0x20000,                 # version 2.0
        entry_count,             # num_entries
        0,                       # num_bytes
        0,                       # last_file
        0,                       # this_id (unused)
        0,                       # stats_addr
        table_len,               # table_len
        0,                       # crash
    )
    # Pad header to 256 bytes + hash table
    header_data = index_header.ljust(256, b'\x00')
    hash_table = b'\x00' * (table_len * 4)
    (cache_dir / "index").write_bytes(header_data + hash_table)

    # Create data_0 (rankings, 36-byte blocks)
    _create_block_file(cache_dir / "data_0", file_index=0, block_size=36)

    # Create data_1 (entries, 256-byte blocks)
    _create_block_file(cache_dir / "data_1", file_index=1, block_size=256)

    # Create data_2 (1K blocks) and data_3 (4K blocks) as empty
    _create_block_file(cache_dir / "data_2", file_index=2, block_size=1024)
    _create_block_file(cache_dir / "data_3", file_index=3, block_size=4096)


def _create_block_file(path: Path, file_index: int, block_size: int):
    """Create a minimal block file with valid header."""
    max_entries = (BLOCK_HEADER_SIZE - 20) * 8  # Based on allocation bitmap size
    header = struct.pack(
        "<IIIIIII",
        BLOCKFILE_BLOCK_MAGIC,  # magic
        0x20000,                # version
        file_index,             # this_file
        file_index + 1,         # next_file
        block_size,             # entry_size
        0,                      # num_entries
        max_entries,            # max_entries
    )
    data = header.ljust(BLOCK_HEADER_SIZE, b'\x00')
    path.write_bytes(data)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWebkitToDatetime:
    """Test WebKit timestamp conversion."""

    def test_valid_timestamp(self):
        dt = _webkit_to_datetime(13394403536764131)
        assert dt is not None
        assert dt.year >= 2020

    def test_zero(self):
        assert _webkit_to_datetime(0) is None

    def test_negative(self):
        assert _webkit_to_datetime(-1) is None

    def test_overflow(self):
        assert _webkit_to_datetime(10**30) is None


class TestIsAppcacheIndex:
    """Test _is_appcache_index() validation."""

    def test_valid_appcache_index(self, tmp_path):
        index_path = tmp_path / "Index"
        _create_appcache_index(index_path)
        assert _is_appcache_index(index_path) is True

    def test_not_sqlite(self, tmp_path):
        index_path = tmp_path / "Index"
        index_path.write_bytes(b"not a sqlite file")
        assert _is_appcache_index(index_path) is False

    def test_sqlite_without_appcache_tables(self, tmp_path):
        index_path = tmp_path / "Index"
        conn = sqlite3.connect(str(index_path))
        conn.execute("CREATE TABLE foo (id INTEGER)")
        conn.commit()
        conn.close()
        assert _is_appcache_index(index_path) is False

    def test_missing_file(self, tmp_path):
        assert _is_appcache_index(tmp_path / "nonexistent") is False


class TestReadAppcacheIndex:
    """Test _read_appcache_index() parsing."""

    def test_basic_read(self, tmp_path):
        index_path = tmp_path / "Index"
        _create_appcache_index(index_path)

        result = _read_appcache_index(index_path)

        assert result["total_entries"] == 3
        assert len(result["groups"]) == 1
        assert result["groups"][0]["manifest_url"] == "https://example.com/app/cache.manifest"

        # Check response_id -> url mapping
        assert result["response_id_to_url"][1] == "https://example.com/app/cache.manifest"
        assert result["response_id_to_url"][2] == "https://example.com/app/main.html"
        assert result["response_id_to_url"][3] == "https://example.com/app/style.css"

    def test_multiple_groups(self, tmp_path):
        index_path = tmp_path / "Index"
        groups = [
            (1, "https://a.com/", "https://a.com/manifest", 13394403536764131, 13394403536764131),
            (2, "https://b.com/", "https://b.com/manifest", 13395686363891318, 13395686363891318),
        ]
        entries = [
            (1, "https://a.com/page.html", 1, 1, 1000),
            (2, "https://b.com/page.html", 1, 2, 2000),
        ]
        _create_appcache_index(index_path, groups=groups, entries=entries)

        result = _read_appcache_index(index_path)

        assert result["total_entries"] == 2
        assert len(result["groups"]) == 2
        assert result["response_id_to_url"][1] == "https://a.com/page.html"
        assert result["response_id_to_url"][2] == "https://b.com/page.html"

        # Check entry metadata includes group info
        assert result["response_id_to_entry"][1]["origin"] == "https://a.com/"
        assert result["response_id_to_entry"][2]["origin"] == "https://b.com/"

    def test_empty_index(self, tmp_path):
        index_path = tmp_path / "Index"
        _create_appcache_index(index_path, groups=[], entries=[])

        result = _read_appcache_index(index_path)

        assert result["total_entries"] == 0
        assert len(result["groups"]) == 0


class TestFindAppcacheDirectories:
    """Test find_appcache_directories() discovery."""

    def test_discovers_valid_appcache(self, tmp_path):
        """Should find Application Cache with SQLite Index + blockfile Cache/."""
        appcache_root = tmp_path / "Application Cache"
        appcache_root.mkdir(parents=True)

        # Create SQLite Index
        index_path = appcache_root / "Index"
        _create_appcache_index(index_path)

        # Create blockfile Cache/ directory
        cache_dir = appcache_root / "Cache"
        _create_minimal_blockfile(cache_dir)

        # Build file entries as the manifest would contain them
        files = []
        for fpath in cache_dir.iterdir():
            files.append({
                "extracted_path": str(fpath),
                "browser": "chromium_embedded",
                "profile": "Application",
            })
        # Also include Index file
        files.append({
            "extracted_path": str(index_path),
            "browser": "chromium_embedded",
            "profile": "Application",
        })

        result = find_appcache_directories(files, tmp_path)

        assert len(result) == 1
        assert result[0]["path"] == cache_dir
        assert result[0]["index_path"] == index_path
        assert result[0]["browser"] == "chromium_embedded"
        assert result[0]["profile"] == "Application"

    def test_ignores_regular_blockfile(self, tmp_path):
        """Should not match a regular blockfile cache without SQLite Index."""
        cache_dir = tmp_path / "Cache"
        _create_minimal_blockfile(cache_dir)

        files = []
        for fpath in cache_dir.iterdir():
            files.append({"extracted_path": str(fpath)})

        result = find_appcache_directories(files, tmp_path)
        assert len(result) == 0

    def test_ignores_missing_blockfile(self, tmp_path):
        """Should not match if Index exists but Cache/ has no blockfile data."""
        appcache_root = tmp_path / "Application Cache"
        appcache_root.mkdir(parents=True)

        index_path = appcache_root / "Index"
        _create_appcache_index(index_path)

        # Empty Cache/ directory (no blockfile files)
        cache_dir = appcache_root / "Cache"
        cache_dir.mkdir()
        (cache_dir / "somefile.txt").write_bytes(b"not a blockfile")

        files = [
            {"extracted_path": str(index_path)},
            {"extracted_path": str(cache_dir / "somefile.txt")},
        ]

        result = find_appcache_directories(files, tmp_path)
        assert len(result) == 0


class TestIngestAppcacheDirectory:
    """Test ingest_appcache_directory() end-to-end."""

    @pytest.fixture
    def evidence_conn(self, tmp_path):
        """Create evidence database with all migrations applied."""
        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test_surfsifter.sqlite"

        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        conn = db_manager.get_evidence_conn(1, "test_evidence")
        yield conn
        db_manager.close_all()

    @pytest.fixture
    def appcache_fixture(self, tmp_path):
        """Create a minimal Application Cache with SQLite Index and blockfile."""
        appcache_root = tmp_path / "appcache" / "Application Cache"
        appcache_root.mkdir(parents=True)

        # Create SQLite Index with entries
        index_path = appcache_root / "Index"
        _create_appcache_index(index_path)

        # Create blockfile Cache/ directory (empty — no parsed entries)
        cache_dir = appcache_root / "Cache"
        _create_minimal_blockfile(cache_dir)

        return {
            "cache_dir": cache_dir,
            "index_path": index_path,
            "root": appcache_root,
        }

    @pytest.fixture
    def mock_callbacks(self):
        """Create mock callbacks."""
        cb = MagicMock()
        cb.is_cancelled.return_value = False
        return cb

    def test_ingests_urls_from_index(self, evidence_conn, appcache_fixture, mock_callbacks, tmp_path):
        """Should insert URLs from the SQLite Index even without blockfile entries."""
        result = ingest_appcache_directory(
            evidence_conn=evidence_conn,
            evidence_id=1,
            run_id="test_run_001",
            cache_dir=appcache_fixture["cache_dir"],
            index_path=appcache_fixture["index_path"],
            browser="chromium_embedded",
            profile="Application",
            extraction_dir=tmp_path / "output",
            callbacks=mock_callbacks,
            extractor_version="1.0.0",
        )

        # Should have 3 URLs from the SQLite Index
        assert result["urls"] == 3
        assert result["groups"] == 1
        assert result["records"] == 3

        # Verify URLs in evidence database
        cursor = evidence_conn.cursor()
        cursor.execute("SELECT url FROM urls ORDER BY url")
        urls = [row[0] for row in cursor.fetchall()]

        assert "https://example.com/app/cache.manifest" in urls
        assert "https://example.com/app/main.html" in urls
        assert "https://example.com/app/style.css" in urls

    def test_url_metadata(self, evidence_conn, appcache_fixture, mock_callbacks, tmp_path):
        """Should include AppCache metadata in URL records."""
        ingest_appcache_directory(
            evidence_conn=evidence_conn,
            evidence_id=1,
            run_id="test_run_001",
            cache_dir=appcache_fixture["cache_dir"],
            index_path=appcache_fixture["index_path"],
            browser="chromium_embedded",
            profile="Application",
            extraction_dir=tmp_path / "output",
            callbacks=mock_callbacks,
            extractor_version="1.0.0",
        )

        cursor = evidence_conn.cursor()
        cursor.execute(
            "SELECT discovered_by, notes, context, tags FROM urls WHERE url = ?",
            ("https://example.com/app/main.html",),
        )
        row = cursor.fetchone()
        assert row is not None

        discovered_by, notes, context, tags_json = row
        assert "cache_appcache" in discovered_by
        assert "response_id=2" in notes
        assert context == "https://example.com/app/cache.manifest"

        tags = json.loads(tags_json)
        assert tags["cache_backend"] == "appcache"
        assert tags["response_id"] == 2
        assert tags["origin"] == "https://example.com/"
        assert tags["manifest_url"] == "https://example.com/app/cache.manifest"

    def test_empty_index_returns_zero(self, evidence_conn, tmp_path, mock_callbacks):
        """Should handle empty Index gracefully."""
        appcache_root = tmp_path / "empty_appcache"
        appcache_root.mkdir(parents=True)

        index_path = appcache_root / "Index"
        _create_appcache_index(index_path, groups=[], entries=[])

        cache_dir = appcache_root / "Cache"
        _create_minimal_blockfile(cache_dir)

        result = ingest_appcache_directory(
            evidence_conn=evidence_conn,
            evidence_id=1,
            run_id="test_run_002",
            cache_dir=cache_dir,
            index_path=index_path,
            browser="chrome",
            profile="Default",
            extraction_dir=tmp_path / "output",
            callbacks=mock_callbacks,
            extractor_version="1.0.0",
        )

        assert result["urls"] == 0
        assert result["images"] == 0
        assert result["groups"] == 0

    def test_inventory_registration(self, evidence_conn, appcache_fixture, mock_callbacks, tmp_path):
        """Should register inventory entries with cache_appcache artifact type."""
        manifest_data = {
            "run_id": "test_run_003",
            "status": "ok",
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        }
        file_entries = [
            {
                "extracted_path": str(appcache_fixture["index_path"]),
                "logical_path": "C:/Users/test/Application Cache/Index",
            },
        ]
        # Make the extracted_path of Index look like "Index" file name
        # so inventory gets the correct name
        file_entries[0]["extracted_path"] = str(appcache_fixture["index_path"])

        result = ingest_appcache_directory(
            evidence_conn=evidence_conn,
            evidence_id=1,
            run_id="test_run_003",
            cache_dir=appcache_fixture["cache_dir"],
            index_path=appcache_fixture["index_path"],
            browser="chromium_embedded",
            profile="Application",
            extraction_dir=tmp_path / "output",
            callbacks=mock_callbacks,
            extractor_version="1.0.0",
            manifest_data=manifest_data,
            file_entries=file_entries,
        )

        assert result["inventory_entries"] == 1

        cursor = evidence_conn.cursor()
        cursor.execute(
            "SELECT artifact_type, ingestion_status FROM browser_cache_inventory "
            "WHERE run_id = 'test_run_003'"
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "cache_appcache"
        assert row[1] == "ok"

    def test_multiple_groups(self, evidence_conn, tmp_path, mock_callbacks):
        """Should handle multiple groups (different web apps) correctly."""
        appcache_root = tmp_path / "multi_appcache"
        appcache_root.mkdir(parents=True)

        groups = [
            (1, "https://games.example.com/", "https://games.example.com/g1/manifest",
             13394403536764131, 13394403536764131),
            (2, "https://games.example.com/", "https://games.example.com/g2/manifest",
             13395686363891318, 13395686363891318),
        ]
        entries = [
            (1, "https://games.example.com/g1/manifest", 2, 1, 1000),
            (1, "https://games.example.com/g1/main.html", 1, 2, 2000),
            (2, "https://games.example.com/g2/manifest", 2, 3, 1000),
            (2, "https://games.example.com/g2/game.js", 4, 4, 50000),
        ]

        index_path = appcache_root / "Index"
        _create_appcache_index(index_path, groups=groups, entries=entries)

        cache_dir = appcache_root / "Cache"
        _create_minimal_blockfile(cache_dir)

        result = ingest_appcache_directory(
            evidence_conn=evidence_conn,
            evidence_id=1,
            run_id="test_run_multi",
            cache_dir=cache_dir,
            index_path=index_path,
            browser="chrome",
            profile="Default",
            extraction_dir=tmp_path / "output",
            callbacks=mock_callbacks,
            extractor_version="1.0.0",
        )

        assert result["urls"] == 4
        assert result["groups"] == 2


class TestAppcacheWithApplicationData:
    """Integration test using real Application Application Cache data if available."""

    Application_APPCACHE = Path("test_cases/Application/cache/Application Cache")

    @pytest.fixture
    def evidence_conn(self, tmp_path):
        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test_surfsifter.sqlite"
        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        conn = db_manager.get_evidence_conn(1, "test_evidence")
        yield conn
        db_manager.close_all()

    @pytest.fixture
    def mock_callbacks(self):
        cb = MagicMock()
        cb.is_cancelled.return_value = False
        return cb

    @pytest.mark.skipif(
        not Path("test_cases/Application/cache/Application Cache/Index").exists(),
        reason="Application test data not available",
    )
    def test_read_Application_index(self):
        """Read the real Application Application Cache Index."""
        result = _read_appcache_index(self.Application_APPCACHE / "Index")

        assert result["total_entries"] == 18
        assert len(result["groups"]) == 5

        # Verify known URLs
        urls = set(result["response_id_to_url"].values())
        assert any("mythicmaiden" in u for u in urls)
        assert any("dazzleme" in u for u in urls)
        assert any("fruitshopchristmas" in u for u in urls)
        assert any("stickers" in u for u in urls)
        assert any("bollywoodstory" in u for u in urls)

        # All entries should be from cdnc4.example.org
        for url in urls:
            assert "cdnc4.example.org" in url

    @pytest.mark.skipif(
        not Path("test_cases/Application/cache/Application Cache/Index").exists(),
        reason="Application test data not available",
    )
    def test_ingest_Application_appcache(self, evidence_conn, mock_callbacks, tmp_path):
        """Full ingestion of Application Application Cache data."""
        result = ingest_appcache_directory(
            evidence_conn=evidence_conn,
            evidence_id=1,
            run_id="test_Application_appcache",
            cache_dir=self.Application_APPCACHE / "Cache",
            index_path=self.Application_APPCACHE / "Index",
            browser="chromium_embedded",
            profile="Application",
            extraction_dir=tmp_path / "output",
            callbacks=mock_callbacks,
            extractor_version="1.0.0",
        )

        # Should have all 18 URLs from the Index
        assert result["urls"] == 18
        assert result["groups"] == 5

        # Verify URLs in DB
        cursor = evidence_conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT url) FROM urls")
        url_count = cursor.fetchone()[0]
        assert url_count == 18

        # Verify all URLs are from cdnc4.example.org
        cursor.execute("SELECT DISTINCT domain FROM urls")
        domains = {row[0] for row in cursor.fetchall()}
        assert domains == {"cdnc4.example.org"}

        # Verify manifest URLs
        cursor.execute("SELECT url FROM urls WHERE url LIKE '%cache.manifest%'")
        manifests = [row[0] for row in cursor.fetchall()]
        assert len(manifests) == 5

        # Verify game names are present
        cursor.execute("SELECT url FROM urls")
        all_urls = [row[0] for row in cursor.fetchall()]
        games = ["mythicmaiden", "dazzleme", "fruitshopchristmas", "stickers", "bollywoodstory"]
        for game in games:
            assert any(game in url for url in all_urls), f"Game {game} not found in URLs"
