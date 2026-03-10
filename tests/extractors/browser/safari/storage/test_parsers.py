"""
Tests for Safari Storage parsers.

Tests cover:
- LocalStorage parser (parse_safari_localstorage)
- IndexedDB parser (parse_safari_indexeddb)
- Origin extraction from filenames and paths
- UTF-16LE BLOB decoding with fallback
- Empty/corrupt file handling
- Schema warning integration
"""

import sqlite3
from pathlib import Path
from typing import Any, Dict, List

import pytest

from extractors.browser.safari.storage._parsers import (
    parse_safari_localstorage,
    parse_safari_indexeddb,
    parse_origin_from_localstorage_filename,
    parse_origin_from_indexeddb_path,
    _classify_value_type,
    _decode_origin_dir,
)


# =============================================================================
# Origin Extraction Tests
# =============================================================================


class TestOriginExtraction:
    """Test origin extraction from filenames and paths."""

    def test_localstorage_https_default_port(self):
        """Parse standard HTTPS origin with port 0."""
        origin = parse_origin_from_localstorage_filename(
            "https_example.com_0.localstorage"
        )
        assert origin == "https://example.com"

    def test_localstorage_http_custom_port(self):
        """Parse HTTP origin with custom port."""
        origin = parse_origin_from_localstorage_filename(
            "http_localhost_8080.localstorage"
        )
        assert origin == "http://localhost:8080"

    def test_localstorage_subdomain(self):
        """Parse origin with subdomain."""
        origin = parse_origin_from_localstorage_filename(
            "https_www.youtube-nocookie.com_0.localstorage"
        )
        assert origin == "https://www.youtube-nocookie.com"

    def test_localstorage_host_with_underscore(self):
        """Parse origin with underscore in host (edge case)."""
        origin = parse_origin_from_localstorage_filename(
            "https_my_server.local_443.localstorage"
        )
        assert origin == "https://my_server.local:443"

    def test_localstorage_unparseable(self):
        """Unparseable filename returns stem as-is."""
        origin = parse_origin_from_localstorage_filename(
            "weird.localstorage"
        )
        assert origin == "weird"

    def test_indexeddb_path_v1(self):
        """Parse origin from IndexedDB v1 path."""
        path = "/Users/john/Library/Safari/Databases/___IndexedDB/v1/https_example.com_0/MyDB/IndexedDB.sqlite3"
        origin = parse_origin_from_indexeddb_path(path)
        assert origin == "https://example.com"

    def test_indexeddb_path_v2(self):
        """Parse origin from IndexedDB v2 path."""
        path = "/Users/john/Library/WebKit/com.apple.Safari/WebsiteData/IndexedDB/v2/http_localhost_3000/db.sqlite"
        origin = parse_origin_from_indexeddb_path(path)
        assert origin == "http://localhost:3000"

    def test_indexeddb_path_no_version(self):
        """Parse origin from IndexedDB path without version dir."""
        path = "/Users/john/Library/Safari/Databases/___IndexedDB/https_test.org_0/db.sqlite"
        origin = parse_origin_from_indexeddb_path(path)
        assert origin == "https://test.org"

    def test_indexeddb_path_unknown(self):
        """Return empty string for unrecognizable path."""
        origin = parse_origin_from_indexeddb_path("/some/random/path.sqlite")
        assert origin == ""

    def test_decode_origin_dir_default_port(self):
        """Decode origin directory with port 0."""
        assert _decode_origin_dir("https_example.com_0") == "https://example.com"

    def test_decode_origin_dir_custom_port(self):
        """Decode origin directory with custom port."""
        assert _decode_origin_dir("http_localhost_8080") == "http://localhost:8080"

    def test_decode_origin_dir_short(self):
        """Decode unparseable directory name returns as-is."""
        assert _decode_origin_dir("unparseable") == "unparseable"


# =============================================================================
# Value Type Classification Tests
# =============================================================================


class TestValueTypeClassification:
    """Test value type classification."""

    def test_empty_value(self):
        assert _classify_value_type("") == "empty"

    def test_json_object(self):
        assert _classify_value_type('{"key": "value"}') == "json"

    def test_json_array(self):
        assert _classify_value_type('[1, 2, 3]') == "json"

    def test_boolean_true(self):
        assert _classify_value_type("true") == "boolean"

    def test_boolean_false(self):
        assert _classify_value_type("false") == "boolean"

    def test_number_int(self):
        assert _classify_value_type("42") == "number"

    def test_number_float(self):
        assert _classify_value_type("3.14") == "number"

    def test_string_value(self):
        assert _classify_value_type("hello world") == "string"

    def test_broken_json(self):
        assert _classify_value_type("{broken") == "string"


# =============================================================================
# LocalStorage Parser Tests
# =============================================================================


def _create_localstorage_db(
    path: Path,
    entries: List[Dict[str, Any]],
) -> Path:
    """
    Create a synthetic Safari .localstorage SQLite file.

    Args:
        path: Path for the database file.
        entries: List of dicts with 'key' and 'value' (str) fields.
            Values will be encoded as UTF-16LE BLOBs.

    Returns:
        Path to the created database.
    """
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE ItemTable (key TEXT UNIQUE, value BLOB NOT NULL)")
    for entry in entries:
        value_blob = entry["value"].encode("utf-16-le")
        conn.execute(
            "INSERT INTO ItemTable (key, value) VALUES (?, ?)",
            (entry["key"], value_blob),
        )
    conn.commit()
    conn.close()
    return path


class TestLocalStorageParser:
    """Test Safari LocalStorage parser."""

    def test_basic_parse(self, tmp_path):
        """Parse a simple .localstorage file with known key-value pairs."""
        db_path = tmp_path / "https_example.com_0.localstorage"
        _create_localstorage_db(
            db_path,
            [
                {"key": "theme", "value": "dark"},
                {"key": "lang", "value": "en"},
                {"key": "count", "value": "42"},
            ],
        )

        loc = {
            "browser": "safari",
            "profile": "johndoe",
            "logical_path": "/Users/johndoe/Library/Safari/LocalStorage/https_example.com_0.localstorage",
            "partition_index": 0,
            "fs_type": "APFS",
        }

        records = parse_safari_localstorage(
            db_path, loc, run_id="test_run_001", evidence_id=1
        )

        assert len(records) == 3
        # Check origin was parsed from filename
        assert all(r["origin"] == "https://example.com" for r in records)
        assert all(r["browser"] == "safari" for r in records)
        assert all(r["run_id"] == "test_run_001" for r in records)

        keys = {r["key"] for r in records}
        assert keys == {"theme", "lang", "count"}

        # Verify value decoding
        theme_record = next(r for r in records if r["key"] == "theme")
        assert theme_record["value"] == "dark"
        assert theme_record["value_type"] == "string"

        count_record = next(r for r in records if r["key"] == "count")
        assert count_record["value"] == "42"
        assert count_record["value_type"] == "number"

    def test_utf16le_decoding(self, tmp_path):
        """Verify UTF-16LE BLOB values are properly decoded."""
        db_path = tmp_path / "https_example.com_0.localstorage"
        unicode_value = "Héllo Wörld! 日本語テスト"
        _create_localstorage_db(
            db_path,
            [{"key": "unicode_test", "value": unicode_value}],
        )

        loc = {"browser": "safari", "logical_path": str(db_path)}
        records = parse_safari_localstorage(
            db_path, loc, run_id="test_run", evidence_id=1
        )

        assert len(records) == 1
        assert records[0]["value"] == unicode_value

    def test_json_value_classification(self, tmp_path):
        """JSON values are classified correctly."""
        db_path = tmp_path / "https_example.com_0.localstorage"
        _create_localstorage_db(
            db_path,
            [{"key": "settings", "value": '{"fontSize": 14, "theme": "dark"}'}],
        )

        loc = {"browser": "safari", "logical_path": str(db_path)}
        records = parse_safari_localstorage(
            db_path, loc, run_id="test", evidence_id=1
        )

        assert len(records) == 1
        assert records[0]["value_type"] == "json"

    def test_empty_database(self, tmp_path):
        """Empty ItemTable returns no records."""
        db_path = tmp_path / "https_empty.com_0.localstorage"
        _create_localstorage_db(db_path, [])

        loc = {"browser": "safari", "logical_path": str(db_path)}
        records = parse_safari_localstorage(
            db_path, loc, run_id="test", evidence_id=1
        )

        assert records == []

    def test_missing_item_table(self, tmp_path):
        """File without ItemTable returns empty list."""
        db_path = tmp_path / "https_broken.com_0.localstorage"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE other_table (id INTEGER)")
        conn.commit()
        conn.close()

        loc = {"browser": "safari", "logical_path": str(db_path)}
        records = parse_safari_localstorage(
            db_path, loc, run_id="test", evidence_id=1
        )

        assert records == []

    def test_corrupt_file(self, tmp_path):
        """Corrupt file returns empty list without crashing."""
        db_path = tmp_path / "https_corrupt.com_0.localstorage"
        db_path.write_bytes(b"this is not a sqlite file")

        loc = {"browser": "safari", "logical_path": str(db_path)}
        records = parse_safari_localstorage(
            db_path, loc, run_id="test", evidence_id=1
        )

        assert records == []

    def test_value_truncation(self, tmp_path):
        """Long values are truncated to excerpt_size."""
        db_path = tmp_path / "https_example.com_0.localstorage"
        long_value = "x" * 10000
        _create_localstorage_db(
            db_path,
            [{"key": "big", "value": long_value}],
        )

        loc = {"browser": "safari", "logical_path": str(db_path)}
        records = parse_safari_localstorage(
            db_path, loc, run_id="test", evidence_id=1, excerpt_size=100
        )

        assert len(records) == 1
        assert len(records[0]["value"]) == 100
        assert records[0]["value_size"] == len(long_value.encode("utf-8"))

    def test_origin_from_loc_override(self, tmp_path):
        """Origin from loc dict takes precedence over filename."""
        db_path = tmp_path / "weird_name.localstorage"
        _create_localstorage_db(db_path, [{"key": "k", "value": "v"}])

        loc = {
            "browser": "safari",
            "logical_path": str(db_path),
            "origin": "https://override.example.com",
        }
        records = parse_safari_localstorage(
            db_path, loc, run_id="test", evidence_id=1
        )

        assert len(records) == 1
        assert records[0]["origin"] == "https://override.example.com"

    def test_forensic_metadata_preserved(self, tmp_path):
        """Forensic metadata (partition_index, fs_type, paths) is preserved."""
        db_path = tmp_path / "https_example.com_0.localstorage"
        _create_localstorage_db(db_path, [{"key": "k", "value": "v"}])

        loc = {
            "browser": "safari",
            "profile": "testuser",
            "logical_path": "/Users/testuser/Library/Safari/LocalStorage/https_example.com_0.localstorage",
            "partition_index": 2,
            "fs_type": "HFS+",
        }
        records = parse_safari_localstorage(
            db_path, loc, run_id="run42", evidence_id=5
        )

        assert len(records) == 1
        r = records[0]
        assert r["partition_index"] == 2
        assert r["fs_type"] == "HFS+"
        assert r["profile"] == "testuser"
        assert "Safari/LocalStorage" in r["source_path"]


# =============================================================================
# IndexedDB Parser Tests
# =============================================================================


def _create_indexeddb_db(
    path: Path,
    db_name: str = "TestDB",
    db_version: int = 1,
    object_stores: Dict[int, str] = None,
    records: List[Dict[str, Any]] = None,
) -> Path:
    """
    Create a synthetic Safari/WebKit IndexedDB SQLite file.

    Args:
        path: Path for the database file.
        db_name: Database name for DatabaseInfo.
        db_version: Database version for DatabaseInfo.
        object_stores: Mapping of store_id -> store_name.
        records: List of dicts with 'store_id', 'key', 'value' fields.

    Returns:
        Path to the created database.
    """
    if object_stores is None:
        object_stores = {1: "users", 2: "settings"}
    if records is None:
        records = []

    conn = sqlite3.connect(str(path))

    # Create WebKit IndexedDB schema
    conn.execute(
        "CREATE TABLE DatabaseInfo (key TEXT NOT NULL, value TEXT)"
    )
    conn.execute(
        "INSERT INTO DatabaseInfo (key, value) VALUES ('DatabaseName', ?)",
        (db_name,),
    )
    conn.execute(
        "INSERT INTO DatabaseInfo (key, value) VALUES ('DatabaseVersion', ?)",
        (str(db_version),),
    )

    conn.execute(
        'CREATE TABLE ObjectStoreInfo ('
        'id INTEGER PRIMARY KEY, name TEXT, keyPath TEXT, "autoIncrement" INTEGER, '
        'maxIndexID INTEGER)'
    )
    for store_id, store_name in object_stores.items():
        conn.execute(
            'INSERT INTO ObjectStoreInfo (id, name, keyPath, "autoIncrement") '
            'VALUES (?, ?, ?, ?)',
            (store_id, store_name, "id", 1),
        )

    conn.execute(
        "CREATE TABLE Records ("
        "objectStoreID INTEGER, key TEXT, value BLOB, recordPaddingSize INTEGER)"
    )
    for rec in records:
        value = rec["value"]
        if isinstance(value, str):
            value = value.encode("utf-8")
        conn.execute(
            "INSERT INTO Records (objectStoreID, key, value) VALUES (?, ?, ?)",
            (rec["store_id"], rec["key"], value),
        )

    conn.execute(
        "CREATE TABLE IndexInfo ("
        "id INTEGER PRIMARY KEY, name TEXT, objectStoreID INTEGER, "
        "keyPath TEXT, \"unique\" INTEGER, multiEntry INTEGER)"
    )

    conn.execute(
        "CREATE TABLE IndexRecords ("
        "indexID INTEGER, key TEXT, value BLOB, objectStoreID INTEGER)"
    )

    conn.execute(
        "CREATE TABLE KeyGenerators (objectStoreID INTEGER, currentNumber INTEGER)"
    )

    conn.commit()
    conn.close()
    return path


class TestIndexedDBParser:
    """Test Safari IndexedDB parser."""

    def test_basic_parse(self, tmp_path):
        """Parse an IndexedDB file with known records."""
        db_path = tmp_path / "v1" / "https_example.com_0" / "TestDB.sqlite"
        db_path.parent.mkdir(parents=True)

        _create_indexeddb_db(
            db_path,
            db_name="MyAppDB",
            db_version=3,
            object_stores={1: "users", 2: "settings"},
            records=[
                {"store_id": 1, "key": "user_1", "value": '{"name": "Alice", "age": 30}'},
                {"store_id": 1, "key": "user_2", "value": '{"name": "Bob", "age": 25}'},
                {"store_id": 2, "key": "theme", "value": "dark"},
            ],
        )

        loc = {
            "browser": "safari",
            "profile": "johndoe",
            "logical_path": str(db_path),
            "partition_index": 0,
            "fs_type": "APFS",
        }

        results = parse_safari_indexeddb(
            db_path, loc, run_id="test_run_001", evidence_id=1
        )

        assert len(results) == 1
        db_record, entries = results[0]

        # Verify database metadata
        assert db_record["database_name"] == "MyAppDB"
        assert db_record["database_version"] == 3
        assert db_record["browser"] == "safari"
        assert db_record["origin"] == "https://example.com"
        assert db_record["total_entries"] == 3

        # Verify entries
        assert len(entries) == 3

        user_entries = [e for e in entries if e["object_store"] == "users"]
        assert len(user_entries) == 2

        settings_entries = [e for e in entries if e["object_store"] == "settings"]
        assert len(settings_entries) == 1
        assert settings_entries[0]["key"] == "theme"

    def test_object_store_mapping(self, tmp_path):
        """Object store IDs are mapped to names correctly."""
        db_path = tmp_path / "v1" / "https_test.io_0" / "db.sqlite"
        db_path.parent.mkdir(parents=True)

        _create_indexeddb_db(
            db_path,
            object_stores={10: "emails", 20: "attachments"},
            records=[
                {"store_id": 10, "key": "msg_1", "value": "test email"},
                {"store_id": 20, "key": "file_1", "value": b"\x89PNG"},
            ],
        )

        loc = {"browser": "safari", "logical_path": str(db_path)}
        results = parse_safari_indexeddb(
            db_path, loc, run_id="test", evidence_id=1
        )

        assert len(results) == 1
        _, entries = results[0]
        stores = {e["object_store"] for e in entries}
        assert "emails" in stores
        assert "attachments" in stores

    def test_unknown_store_id_fallback(self, tmp_path):
        """Unknown store IDs get fallback names."""
        db_path = tmp_path / "v1" / "https_test.io_0" / "db.sqlite"
        db_path.parent.mkdir(parents=True)

        _create_indexeddb_db(
            db_path,
            object_stores={1: "known"},
            records=[
                {"store_id": 99, "key": "orphan", "value": "data"},
            ],
        )

        loc = {"browser": "safari", "logical_path": str(db_path)}
        results = parse_safari_indexeddb(
            db_path, loc, run_id="test", evidence_id=1
        )

        _, entries = results[0]
        assert len(entries) == 1
        assert entries[0]["object_store"] == "object_store_99"

    def test_empty_database(self, tmp_path):
        """IndexedDB with no records returns empty entries."""
        db_path = tmp_path / "v1" / "https_empty.org_0" / "db.sqlite"
        db_path.parent.mkdir(parents=True)

        _create_indexeddb_db(db_path, records=[])

        loc = {"browser": "safari", "logical_path": str(db_path)}
        results = parse_safari_indexeddb(
            db_path, loc, run_id="test", evidence_id=1
        )

        assert len(results) == 1
        db_record, entries = results[0]
        assert entries == []
        assert db_record["total_entries"] == 0

    def test_corrupt_file(self, tmp_path):
        """Corrupt file returns empty results without crashing."""
        db_path = tmp_path / "corrupt.sqlite"
        db_path.write_bytes(b"NOT SQLITE DATA")

        loc = {"browser": "safari", "logical_path": str(db_path)}
        results = parse_safari_indexeddb(
            db_path, loc, run_id="test", evidence_id=1
        )

        assert results == []

    def test_blob_value_handling(self, tmp_path):
        """Binary BLOB values are decoded with replace strategy."""
        db_path = tmp_path / "v1" / "https_test.io_0" / "db.sqlite"
        db_path.parent.mkdir(parents=True)

        binary_data = bytes(range(256))  # All byte values
        _create_indexeddb_db(
            db_path,
            records=[{"store_id": 1, "key": "binary", "value": binary_data}],
        )

        loc = {"browser": "safari", "logical_path": str(db_path)}
        results = parse_safari_indexeddb(
            db_path, loc, run_id="test", evidence_id=1
        )

        _, entries = results[0]
        assert len(entries) == 1
        # Should not crash — replacement chars are acceptable
        assert entries[0]["value_type"] == "string"

    def test_value_truncation(self, tmp_path):
        """Long values are truncated to excerpt_size."""
        db_path = tmp_path / "v1" / "https_test.io_0" / "db.sqlite"
        db_path.parent.mkdir(parents=True)

        big_value = "x" * 50000
        _create_indexeddb_db(
            db_path,
            records=[{"store_id": 1, "key": "big", "value": big_value}],
        )

        loc = {"browser": "safari", "logical_path": str(db_path)}
        results = parse_safari_indexeddb(
            db_path, loc, run_id="test", evidence_id=1, excerpt_size=200
        )

        _, entries = results[0]
        assert len(entries[0]["value"]) == 200
        assert entries[0]["value_size"] == len(big_value.encode("utf-8"))

    def test_origin_from_loc_override(self, tmp_path):
        """Origin from loc dict takes precedence over path parsing."""
        db_path = tmp_path / "db.sqlite"
        _create_indexeddb_db(db_path, records=[])

        loc = {
            "browser": "safari",
            "logical_path": str(db_path),
            "origin": "https://override.example.com",
        }
        results = parse_safari_indexeddb(
            db_path, loc, run_id="test", evidence_id=1
        )

        db_record, _ = results[0]
        assert db_record["origin"] == "https://override.example.com"


# =============================================================================
# Discovery Classification Tests
# =============================================================================


class TestDiscoveryClassification:
    """Test storage file classification in discovery."""

    def test_localstorage_file_classified(self):
        """LocalStorage .localstorage files are correctly classified."""
        from extractors.browser.safari.storage._discovery import (
            _classify_storage_file,
        )

        file_info = {
            "logical_path": "/Users/test/Library/Safari/LocalStorage/https_example.com_0.localstorage",
            "file_name": "https_example.com_0.localstorage",
        }
        result = _classify_storage_file(file_info)
        assert result is not None
        assert result["storage_type"] == "local_storage"
        assert result["origin"] == "https://example.com"

    def test_indexeddb_sqlite_classified(self):
        """IndexedDB .sqlite files are correctly classified."""
        from extractors.browser.safari.storage._discovery import (
            _classify_storage_file,
        )

        file_info = {
            "logical_path": "/Users/test/Library/Safari/Databases/___IndexedDB/v1/https_test.org_0/MyDB.sqlite",
            "file_name": "MyDB.sqlite",
        }
        result = _classify_storage_file(file_info)
        assert result is not None
        assert result["storage_type"] == "indexeddb"
        assert result["origin"] == "https://test.org"

    def test_wal_file_skipped(self):
        """WAL files are skipped (return None)."""
        from extractors.browser.safari.storage._discovery import (
            _classify_storage_file,
        )

        file_info = {
            "logical_path": "/path/to/db.sqlite-wal",
            "file_name": "db.sqlite-wal",
        }
        result = _classify_storage_file(file_info)
        assert result is None

    def test_shm_file_skipped(self):
        """SHM files are skipped (return None)."""
        from extractors.browser.safari.storage._discovery import (
            _classify_storage_file,
        )

        file_info = {
            "logical_path": "/path/to/db.sqlite-shm",
            "file_name": "db.sqlite-shm",
        }
        result = _classify_storage_file(file_info)
        assert result is None

    def test_unrelated_file_skipped(self):
        """Non-storage files return None."""
        from extractors.browser.safari.storage._discovery import (
            _classify_storage_file,
        )

        file_info = {
            "logical_path": "/Users/test/Library/Safari/History.db",
            "file_name": "History.db",
        }
        result = _classify_storage_file(file_info)
        assert result is None


# =============================================================================
# Extractor Metadata Tests
# =============================================================================


class TestSafariStorageExtractorMetadata:
    """Test SafariStorageExtractor metadata and lifecycle methods."""

    def test_metadata(self):
        """Verify extractor metadata."""
        from extractors.browser.safari.storage import SafariStorageExtractor

        extractor = SafariStorageExtractor()
        meta = extractor.metadata

        assert meta.name == "safari_storage"
        assert "Safari" in meta.display_name
        assert meta.category == "browser"
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_no_fs(self):
        """Extraction requires evidence filesystem."""
        from extractors.browser.safari.storage import SafariStorageExtractor

        extractor = SafariStorageExtractor()
        can_run, msg = extractor.can_run_extraction(None)
        assert can_run is False

    def test_can_run_ingestion_no_manifest(self, tmp_path):
        """Ingestion requires manifest.json."""
        from extractors.browser.safari.storage import SafariStorageExtractor

        extractor = SafariStorageExtractor()
        can_run, msg = extractor.can_run_ingestion(tmp_path)
        assert can_run is False

    def test_has_existing_output(self, tmp_path):
        """has_existing_output checks for manifest.json."""
        from extractors.browser.safari.storage import SafariStorageExtractor

        extractor = SafariStorageExtractor()
        assert extractor.has_existing_output(tmp_path) is False

        (tmp_path / "manifest.json").write_text("{}")
        assert extractor.has_existing_output(tmp_path) is True

    def test_output_dir(self, tmp_path):
        """Output directory uses correct structure."""
        from extractors.browser.safari.storage import SafariStorageExtractor

        extractor = SafariStorageExtractor()
        output = extractor.get_output_dir(tmp_path, "evidence_1")
        assert "safari_storage" in str(output)

    def test_registry_discovery(self, extractor_registry):
        """SafariStorageExtractor is discovered by the registry."""
        names = extractor_registry.list_names()
        assert "safari_storage" in names
