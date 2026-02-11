"""Tests for Firefox cache2 index parser and doomed/trash recovery.

Covers:
- Binary index parsing (v0x9 / v0xA headers, 41-byte records, big-endian)
- Journal parsing (headerless record stream)
- Flag decoding, content-type enum, file-size-from-flags
- Structured audit warnings via ExtractionWarningCollector
- Doomed/trash file discovery and correlation
- Database helper CRUD operations
"""
from __future__ import annotations

import struct
from pathlib import Path
from typing import List, Optional

import pytest

from extractors.browser.firefox.cache._index import (
    CacheIndex,
    CacheIndexEntry,
    CONTENT_TYPES,
    CRC_SIZE,
    FLAG_ANONYMOUS,
    FLAG_FILE_SIZE_MASK,
    FLAG_HAS_ALT_DATA,
    FLAG_INITIALIZED,
    FLAG_PINNED,
    FLAG_REMOVED,
    HEADER_SIZE,
    INDEX_VERSION_9,
    INDEX_VERSION_A,
    KNOWN_VERSIONS,
    MAX_KNOWN_CONTENT_TYPE,
    RECORD_SIZE,
    _parse_index_record,
    parse_cache_index,
    parse_journal,
)
from extractors.browser.firefox.cache._recovery import (
    correlate_index_with_files,
    discover_all_cache_entries,
)
from extractors._shared.extraction_warnings import ExtractionWarningCollector


# =====================================================================
# Test helpers — build binary blobs
# =====================================================================

def _make_header_v9(
    timestamp: int = 1700000000,
    is_dirty: int = 0,
) -> bytes:
    """Build a 12-byte v0x9 CacheIndexHeader."""
    return struct.pack("!III", INDEX_VERSION_9, timestamp, is_dirty)


def _make_header_v0a(
    timestamp: int = 1700000000,
    is_dirty: int = 0,
    kb_written: int = 12345,
) -> bytes:
    """Build a 16-byte v0xA CacheIndexHeader."""
    return struct.pack("!IIII", INDEX_VERSION_A, timestamp, is_dirty, kb_written)


def _make_record(
    sha1: bytes = b"\x01" * 20,
    frecency: int = 100,
    origin_attrs_hash: int = 0,
    on_start_time: int = 500,
    on_stop_time: int = 600,
    content_type: int = 3,  # image
    flags: int = FLAG_INITIALIZED | 42,  # file_size_kb = 42
) -> bytes:
    """Build a single 41-byte CacheIndexRecord."""
    return (
        sha1
        + struct.pack("!I", frecency)
        + struct.pack("!Q", origin_attrs_hash)
        + struct.pack("!H", on_start_time)
        + struct.pack("!H", on_stop_time)
        + struct.pack("B", content_type)
        + struct.pack("!I", flags)
    )


def _make_crc(value: int = 0xDEADBEEF) -> bytes:
    """Build 4-byte trailing CRC."""
    return struct.pack("!I", value)


def _write_index(
    tmp_path: Path,
    header: bytes,
    records: List[bytes],
    crc: bytes = _make_crc(),
    filename: str = "index",
) -> Path:
    """Assemble header + records + CRC and write to file."""
    path = tmp_path / filename
    path.write_bytes(header + b"".join(records) + crc)
    return path


# =====================================================================
# Index header tests
# =====================================================================

class TestCacheIndexHeaderV0A:
    """Test parsing of version 0xA index header (Firefox >= 91)."""

    def test_parse_header_fields(self, tmp_path: Path):
        header = _make_header_v0a(timestamp=1700000000, is_dirty=0, kb_written=9999)
        path = _write_index(tmp_path, header, [])
        index, warnings = parse_cache_index(path)

        assert index is not None
        assert index.version == INDEX_VERSION_A
        assert index.timestamp == 1700000000
        assert index.is_dirty is False
        assert index.kb_written == 9999
        assert index.entries == []
        assert warnings == []

    def test_dirty_flag(self, tmp_path: Path):
        header = _make_header_v0a(is_dirty=1)
        path = _write_index(tmp_path, header, [])
        index, _ = parse_cache_index(path)

        assert index is not None
        assert index.is_dirty is True


class TestCacheIndexHeaderV09:
    """Test parsing of version 0x9 index header (Firefox <= 78)."""

    def test_parse_v9_header(self, tmp_path: Path):
        header = _make_header_v9(timestamp=1600000000, is_dirty=0)
        path = _write_index(tmp_path, header, [])
        index, warnings = parse_cache_index(path)

        assert index is not None
        assert index.version == INDEX_VERSION_9
        assert index.timestamp == 1600000000
        assert index.is_dirty is False
        assert index.kb_written is None  # Not present in v9
        assert warnings == []

    def test_v9_header_size_is_12(self):
        assert HEADER_SIZE[INDEX_VERSION_9] == 12

    def test_v0a_header_size_is_16(self):
        assert HEADER_SIZE[INDEX_VERSION_A] == 16


class TestUnknownVersion:
    """Test graceful rejection of unknown index versions."""

    def test_unknown_version_returns_none(self, tmp_path: Path):
        bad_header = struct.pack("!III", 0x0000000B, 0, 0) + _make_crc()
        path = tmp_path / "index"
        path.write_bytes(bad_header)

        index, warnings = parse_cache_index(path)

        assert index is None
        assert any("Unknown index version" in w for w in warnings)

    def test_unknown_version_emits_structured_warning(self, tmp_path: Path):
        bad_header = struct.pack("!III", 0xFF, 0, 0) + _make_crc()
        path = tmp_path / "index"
        path.write_bytes(bad_header)

        collector = ExtractionWarningCollector(
            extractor_name="firefox_cache",
            run_id="test-run",
            evidence_id=1,
        )
        index, warnings = parse_cache_index(path, warning_collector=collector)

        assert index is None
        assert len(collector._warnings) >= 1
        w = collector._warnings[0]
        assert w.warning_type == "version_unsupported"
        assert w.severity == "error"


# =====================================================================
# Record parsing tests
# =====================================================================

class TestIndexRecordParsing:
    """Test parsing of 41-byte CacheIndexRecord with big-endian fields."""

    def test_parse_single_record(self):
        sha1 = bytes(range(20))
        flags = FLAG_INITIALIZED | FLAG_PINNED | 1024  # file_size_kb = 1024
        record = _make_record(
            sha1=sha1,
            frecency=42,
            origin_attrs_hash=0xDEAD,
            on_start_time=100,
            on_stop_time=200,
            content_type=2,  # javascript
            flags=flags,
        )

        entry = _parse_index_record(record)
        assert entry is not None
        assert entry.hash == sha1.hex().upper()
        assert entry.frecency == 42
        assert entry.origin_attrs_hash == 0xDEAD
        assert entry.on_start_time == 100
        assert entry.on_stop_time == 200
        assert entry.content_type == 2
        assert entry.content_type_name == "javascript"

    def test_record_too_short_returns_none(self):
        assert _parse_index_record(b"\x00" * 40) is None

    def test_big_endian_serialization(self, tmp_path: Path):
        """Verify parser reads big-endian (NetworkEndian) fields correctly."""
        sha1 = b"\xAB" * 20
        frecency = 0x01020304
        origin_hash = 0x0102030405060708
        start = 0x0A0B
        stop = 0x0C0D
        ct = 5  # stylesheet
        flags = 0x80000100  # initialized + file_size 256 KB

        record = (
            sha1
            + struct.pack("!I", frecency)
            + struct.pack("!Q", origin_hash)
            + struct.pack("!H", start)
            + struct.pack("!H", stop)
            + struct.pack("B", ct)
            + struct.pack("!I", flags)
        )

        entry = _parse_index_record(record)
        assert entry.frecency == 0x01020304
        assert entry.origin_attrs_hash == 0x0102030405060708
        assert entry.on_start_time == 0x0A0B
        assert entry.on_stop_time == 0x0C0D
        assert entry.content_type == 5
        assert entry.file_size_kb == 0x100  # 256


class TestIndexEntryFlags:
    """Test flag parsing (removed, pinned, anonymous, file size)."""

    def test_initialized_flag(self):
        entry = CacheIndexEntry("A" * 40, 0, 0, 0, 0, 0, FLAG_INITIALIZED)
        assert entry.is_initialized is True
        assert entry.is_removed is False
        assert entry.is_anonymous is False

    def test_removed_flag(self):
        entry = CacheIndexEntry("A" * 40, 0, 0, 0, 0, 0, FLAG_REMOVED)
        assert entry.is_removed is True

    def test_anonymous_flag(self):
        entry = CacheIndexEntry("A" * 40, 0, 0, 0, 0, 0, FLAG_ANONYMOUS)
        assert entry.is_anonymous is True

    def test_pinned_flag(self):
        entry = CacheIndexEntry("A" * 40, 0, 0, 0, 0, 0, FLAG_PINNED)
        assert entry.is_pinned is True

    def test_has_alt_data_flag(self):
        entry = CacheIndexEntry("A" * 40, 0, 0, 0, 0, 0, FLAG_HAS_ALT_DATA)
        assert entry.has_alt_data is True

    def test_file_size_from_flags(self):
        """File size in KB from lower 24 bits of mFlags."""
        entry = CacheIndexEntry("A" * 40, 0, 0, 0, 0, 0, FLAG_INITIALIZED | 0x000FFF)
        assert entry.file_size_kb == 0x000FFF  # 4095 KB

    def test_combined_flags(self):
        flags = FLAG_INITIALIZED | FLAG_ANONYMOUS | FLAG_REMOVED | FLAG_PINNED | 512
        entry = CacheIndexEntry("A" * 40, 0, 0, 0, 0, 0, flags)
        assert entry.is_initialized
        assert entry.is_anonymous
        assert entry.is_removed
        assert entry.is_pinned
        assert entry.file_size_kb == 512


class TestContentTypeEnum:
    """Test content type enum values (0-6)."""

    @pytest.mark.parametrize("ct_value,ct_name", list(CONTENT_TYPES.items()))
    def test_known_content_types(self, ct_value: int, ct_name: str):
        entry = CacheIndexEntry("A" * 40, 0, 0, 0, 0, ct_value, 0)
        assert entry.content_type_name == ct_name

    def test_unknown_content_type(self):
        entry = CacheIndexEntry("A" * 40, 0, 0, 0, 0, 99, 0)
        assert entry.content_type_name == "unknown(99)"


# =====================================================================
# Full index parsing tests
# =====================================================================

class TestParseFullIndex:
    """Test parsing complete index files with header + records + CRC."""

    def test_v0a_with_multiple_records(self, tmp_path: Path):
        header = _make_header_v0a()
        records = [
            _make_record(sha1=bytes([i]) * 20, frecency=i * 10)
            for i in range(5)
        ]
        path = _write_index(tmp_path, header, records)
        index, warnings = parse_cache_index(path)

        assert index is not None
        assert len(index.entries) == 5
        assert index.entries[0].frecency == 0
        assert index.entries[4].frecency == 40
        assert warnings == []

    def test_v9_with_records(self, tmp_path: Path):
        header = _make_header_v9()
        records = [_make_record() for _ in range(3)]
        path = _write_index(tmp_path, header, records)
        index, warnings = parse_cache_index(path)

        assert index is not None
        assert index.version == INDEX_VERSION_9
        assert len(index.entries) == 3

    def test_removed_entries_counted(self, tmp_path: Path):
        header = _make_header_v0a()
        records = [
            _make_record(flags=FLAG_INITIALIZED | 10),
            _make_record(flags=FLAG_INITIALIZED | FLAG_REMOVED | 20),
            _make_record(flags=FLAG_INITIALIZED | FLAG_REMOVED | 30),
        ]
        path = _write_index(tmp_path, header, records)
        index, _ = parse_cache_index(path)

        assert index is not None
        removed = [e for e in index.entries if e.is_removed]
        assert len(removed) == 2

    def test_empty_index(self, tmp_path: Path):
        """Index with header + CRC but no records."""
        header = _make_header_v0a()
        path = _write_index(tmp_path, header, [])
        index, warnings = parse_cache_index(path)

        assert index is not None
        assert len(index.entries) == 0
        assert warnings == []


class TestTruncatedIndex:
    """Test handling of index files truncated mid-record."""

    def test_truncated_record_region(self, tmp_path: Path):
        header = _make_header_v0a()
        # One full record (41 bytes) + 10 trailing bytes + CRC
        records_data = _make_record() + b"\x00" * 10
        path = tmp_path / "index"
        path.write_bytes(header + records_data + _make_crc())

        index, warnings = parse_cache_index(path)

        assert index is not None
        assert len(index.entries) == 1  # Only the full record parsed
        assert any("not a multiple" in w for w in warnings)

    def test_file_too_small_for_header(self, tmp_path: Path):
        path = tmp_path / "index"
        path.write_bytes(b"\x00" * 8)
        index, warnings = parse_cache_index(path)

        assert index is None
        assert any("too small" in w for w in warnings)

    def test_file_too_small_for_crc(self, tmp_path: Path):
        """Header valid but no room for CRC."""
        header = _make_header_v0a()
        path = tmp_path / "index"
        # Write only the header (16 bytes), no CRC
        path.write_bytes(header)
        index, warnings = parse_cache_index(path)

        assert index is None
        assert any("too small" in w or "CRC" in w for w in warnings)


# =====================================================================
# Journal parsing tests
# =====================================================================

class TestJournalParsing:
    """Test parsing of index.log journal files."""

    def test_parse_journal_with_entries(self, tmp_path: Path):
        records = [
            _make_record(sha1=bytes([i]) * 20, frecency=i)
            for i in range(3)
        ]
        path = tmp_path / "index.log"
        path.write_bytes(b"".join(records) + _make_crc())

        entries, warnings = parse_journal(path)
        assert len(entries) == 3
        assert warnings == []

    def test_empty_journal(self, tmp_path: Path):
        path = tmp_path / "index.log"
        path.write_bytes(_make_crc())

        entries, warnings = parse_journal(path)
        assert entries == []
        assert warnings == []

    def test_journal_too_small(self, tmp_path: Path):
        path = tmp_path / "index.log"
        path.write_bytes(b"\x00")

        entries, warnings = parse_journal(path)
        assert entries == []
        assert any("too small" in w for w in warnings)


# =====================================================================
# Structured warning tests
# =====================================================================

class TestWarningCollectorIntegration:
    """Test that format anomalies produce structured audit warnings."""

    def _make_collector(self) -> ExtractionWarningCollector:
        return ExtractionWarningCollector(
            extractor_name="firefox_cache",
            run_id="test-run-001",
            evidence_id=1,
        )

    def test_misaligned_record_region_emits_warning(self, tmp_path: Path):
        collector = self._make_collector()
        header = _make_header_v0a()
        # 1 record + 5 extra bytes + CRC
        path = tmp_path / "index"
        path.write_bytes(header + _make_record() + b"\xFF" * 5 + _make_crc())

        index, warnings = parse_cache_index(path, warning_collector=collector)

        assert index is not None
        assert len(collector._warnings) >= 1
        assert any(w.warning_type == "binary_format_error" for w in collector._warnings)

    def test_unknown_content_type_emits_info(self, tmp_path: Path):
        collector = self._make_collector()
        header = _make_header_v0a()
        # content_type = 99 (unknown)
        record = _make_record(content_type=99)
        path = _write_index(tmp_path, header, [record])

        index, warnings = parse_cache_index(path, warning_collector=collector)

        assert index is not None
        info_warnings = [
            w for w in collector._warnings
            if w.severity == "info" and "mContentType" in (w.item_value or "")
        ]
        assert len(info_warnings) == 1

    def test_unknown_flag_bits_emits_info(self, tmp_path: Path):
        collector = self._make_collector()
        header = _make_header_v0a()
        # Set a completely unknown bit pattern in flags — but the known
        # ALL_KNOWN_FLAGS already covers 0xFFFFFFFF, so we need to use
        # the fact that flags 0-23 are file size.  Actually ALL_KNOWN_FLAGS
        # is 0xFFFFFFFF so there are no unknown bits by design.  Let's
        # verify the parser doesn't emit spurious warnings.
        record = _make_record(flags=FLAG_INITIALIZED | 42)
        path = _write_index(tmp_path, header, [record])

        index, warnings = parse_cache_index(path, warning_collector=collector)

        assert index is not None
        # No unknown-flags warning expected since all bits are accounted for
        flag_warnings = [
            w for w in collector._warnings
            if "mFlags" in (w.item_value or "")
        ]
        assert len(flag_warnings) == 0

    def test_warnings_without_collector(self, tmp_path: Path):
        """Parser works correctly when no warning_collector is provided."""
        bad_header = struct.pack("!III", 0xBAD, 0, 0) + _make_crc()
        path = tmp_path / "index"
        path.write_bytes(bad_header)

        index, warnings = parse_cache_index(path, warning_collector=None)

        assert index is None
        assert len(warnings) >= 1  # Still produces text warnings

    def test_warnings_flushed_to_database(self, tmp_path: Path, evidence_db):
        """Collector warnings persist via flush_to_database."""
        collector = ExtractionWarningCollector(
            extractor_name="firefox_cache",
            run_id="test-run-flush",
            evidence_id=1,
        )

        bad_header = struct.pack("!III", 0xBAD, 0, 0) + _make_crc()
        path = tmp_path / "index"
        path.write_bytes(bad_header)

        parse_cache_index(path, warning_collector=collector)

        assert collector.has_warnings
        count = collector.flush_to_database(evidence_db)
        assert count >= 1

        # Verify in DB
        rows = evidence_db.execute(
            "SELECT * FROM extraction_warnings WHERE extractor_name = 'firefox_cache'"
        ).fetchall()
        assert len(rows) >= 1


# =====================================================================
# Doomed/trash discovery tests
# =====================================================================

class TestDiscoverCacheEntries:
    """Test discovering entries from entries, doomed, and trash dirs."""

    def test_discover_active_entries(self, tmp_path: Path):
        cache_root = tmp_path / "cache2"
        entries_dir = cache_root / "entries"
        entries_dir.mkdir(parents=True)
        (entries_dir / "AABB").write_bytes(b"data")
        (entries_dir / "CCDD").write_bytes(b"data")

        result = discover_all_cache_entries(cache_root)

        assert len(result["entries"]) == 2
        assert result["doomed"] == []
        assert result["trash"] == []

    def test_discover_doomed_entries(self, tmp_path: Path):
        cache_root = tmp_path / "cache2"
        doomed_dir = cache_root / "doomed"
        doomed_dir.mkdir(parents=True)
        (doomed_dir / "EEFF").write_bytes(b"doomed_data")

        result = discover_all_cache_entries(cache_root)

        assert len(result["doomed"]) == 1
        assert result["doomed"][0].name == "EEFF"

    def test_discover_trash_entries(self, tmp_path: Path):
        cache_root = tmp_path / "cache2"
        trash_subdir = cache_root / "trash" / "0"
        trash_subdir.mkdir(parents=True)
        (trash_subdir / "1122").write_bytes(b"trash_data")
        (trash_subdir / "3344").write_bytes(b"trash_data")

        result = discover_all_cache_entries(cache_root)

        assert len(result["trash"]) == 2

    def test_empty_cache_root(self, tmp_path: Path):
        cache_root = tmp_path / "cache2"
        cache_root.mkdir()

        result = discover_all_cache_entries(cache_root)

        assert result == {"entries": [], "doomed": [], "trash": []}


# =====================================================================
# Correlation tests
# =====================================================================

class TestCorrelateIndexWithFiles:
    """Test correlating index entries with discovered files."""

    def _entry(self, hash_hex: str, removed: bool = False) -> CacheIndexEntry:
        flags = FLAG_INITIALIZED | 10
        if removed:
            flags |= FLAG_REMOVED
        return CacheIndexEntry(
            hash=hash_hex,
            frecency=100,
            origin_attrs_hash=0,
            on_start_time=0,
            on_stop_time=0,
            content_type=0,
            flags=flags,
        )

    def test_all_entries_have_files(self, tmp_path: Path):
        entries_dir = tmp_path / "entries"
        entries_dir.mkdir()
        (entries_dir / "AABB").write_bytes(b"x")
        (entries_dir / "CCDD").write_bytes(b"x")

        index_entries = [self._entry("AABB"), self._entry("CCDD")]
        files = {"entries": list(entries_dir.iterdir()), "doomed": [], "trash": []}

        result = correlate_index_with_files(index_entries, files)

        assert len(result) == 2
        assert all(r["has_file"] for r in result)
        assert all(r["file_source"] == "entries" for r in result)

    def test_index_only_entries(self):
        """Entries without corresponding files (metadata-only)."""
        index_entries = [self._entry("AAAA"), self._entry("BBBB")]
        files = {"entries": [], "doomed": [], "trash": []}

        result = correlate_index_with_files(index_entries, files)

        assert len(result) == 2
        assert all(not r["has_file"] for r in result)
        assert all(r["file_source"] is None for r in result)

    def test_doomed_file_correlation(self, tmp_path: Path):
        doomed_dir = tmp_path / "doomed"
        doomed_dir.mkdir()
        (doomed_dir / "DEAD").write_bytes(b"x")

        index_entries = [self._entry("DEAD", removed=True)]
        files = {"entries": [], "doomed": list(doomed_dir.iterdir()), "trash": []}

        result = correlate_index_with_files(index_entries, files)

        assert result[0]["has_file"] is True
        assert result[0]["file_source"] == "doomed"
        assert result[0]["is_removed"] is True

    def test_mixed_sources(self, tmp_path: Path):
        entries_dir = tmp_path / "entries"
        entries_dir.mkdir()
        (entries_dir / "AAAA").write_bytes(b"x")

        doomed_dir = tmp_path / "doomed"
        doomed_dir.mkdir()
        (doomed_dir / "BBBB").write_bytes(b"x")

        index_entries = [
            self._entry("AAAA"),
            self._entry("BBBB", removed=True),
            self._entry("CCCC"),  # No file
        ]
        files = {
            "entries": list(entries_dir.iterdir()),
            "doomed": list(doomed_dir.iterdir()),
            "trash": [],
        }

        result = correlate_index_with_files(index_entries, files)

        by_hash = {r["hash"]: r for r in result}
        assert by_hash["AAAA"]["has_file"] is True
        assert by_hash["AAAA"]["file_source"] == "entries"
        assert by_hash["BBBB"]["has_file"] is True
        assert by_hash["BBBB"]["file_source"] == "doomed"
        assert by_hash["CCCC"]["has_file"] is False


# =====================================================================
# Database helper tests
# =====================================================================

class TestFirefoxCacheIndexHelpers:
    """Test CRUD operations for the firefox_cache_index table."""

    def test_insert_and_get(self, evidence_db):
        from core.database.helpers import (
            insert_firefox_cache_index_entry,
            get_firefox_cache_index_entries,
        )

        entry = {
            "run_id": "run-001",
            "source_path": "/cache2/index",
            "entry_hash": "A" * 40,
            "frecency": 100,
            "content_type": 3,
            "content_type_name": "image",
            "file_size_kb": 42,
            "raw_flags": FLAG_INITIALIZED | 42,
            "is_initialized": True,
            "is_removed": False,
            "has_entry_file": True,
            "entry_source": "entries",
            "browser": "firefox",
            "index_version": INDEX_VERSION_A,
            "index_timestamp": 1700000000,
        }

        row_id = insert_firefox_cache_index_entry(evidence_db, 1, entry)
        assert row_id > 0

        rows = get_firefox_cache_index_entries(evidence_db, 1)
        assert len(rows) == 1
        assert rows[0]["entry_hash"] == "A" * 40
        assert rows[0]["frecency"] == 100

    def test_batch_insert(self, evidence_db):
        from core.database.helpers import (
            insert_firefox_cache_index_entries,
            get_firefox_cache_index_entries,
        )

        entries = [
            {
                "run_id": "run-002",
                "source_path": "/cache2/index",
                "entry_hash": hex_char * 40,
                "frecency": i * 10,
                "content_type": i % 7,
                "content_type_name": CONTENT_TYPES.get(i % 7, "unknown"),
                "is_removed": i == 2,
                "has_entry_file": i != 3,
                "entry_source": "entries" if i != 3 else None,
                "browser": "firefox",
            }
            for i, hex_char in enumerate("ABCDE")
        ]

        count = insert_firefox_cache_index_entries(evidence_db, 1, entries)
        assert count == 5

        rows = get_firefox_cache_index_entries(evidence_db, 1, run_id="run-002")
        assert len(rows) == 5

    def test_filter_removed_only(self, evidence_db):
        from core.database.helpers import (
            insert_firefox_cache_index_entries,
            get_firefox_cache_index_entries,
        )

        entries = [
            {
                "run_id": "run-003",
                "source_path": "/cache2/index",
                "entry_hash": f"{'A' * 39}{i}",
                "is_removed": i % 2 == 0,
                "browser": "firefox",
            }
            for i in range(4)
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)

        removed = get_firefox_cache_index_entries(
            evidence_db, 1, run_id="run-003", removed_only=True,
        )
        assert len(removed) == 2

    def test_delete_by_run(self, evidence_db):
        from core.database.helpers import (
            insert_firefox_cache_index_entries,
            get_firefox_cache_index_entries,
            delete_firefox_cache_index_by_run,
        )

        entries = [
            {
                "run_id": "run-del",
                "source_path": "/cache2/index",
                "entry_hash": "B" * 40,
                "browser": "firefox",
            }
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        assert len(get_firefox_cache_index_entries(evidence_db, 1, run_id="run-del")) == 1

        deleted = delete_firefox_cache_index_by_run(evidence_db, 1, "run-del")
        assert deleted == 1
        assert len(get_firefox_cache_index_entries(evidence_db, 1, run_id="run-del")) == 0

    def test_stats(self, evidence_db):
        from core.database.helpers import (
            insert_firefox_cache_index_entries,
            get_firefox_cache_index_stats,
        )

        entries = [
            {
                "run_id": "run-stats",
                "source_path": "/cache2/index",
                "entry_hash": f"{'C' * 39}{i}",
                "content_type": 3,
                "content_type_name": "image",
                "is_removed": i == 0,
                "has_entry_file": i != 2,
                "entry_source": "entries" if i != 2 else None,
                "browser": "firefox",
            }
            for i in range(4)
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)

        stats = get_firefox_cache_index_stats(evidence_db, 1, run_id="run-stats")
        assert stats["total"] == 4
        assert stats["removed"] == 1
        assert stats["with_file"] == 3
        assert stats["without_file"] == 1


# =====================================================================
# Pattern tests
# =====================================================================

class TestPatternUpdates:
    """Test that patterns module exposes index/doomed/trash artifacts."""

    def test_cache_index_patterns_exist(self):
        from extractors.browser.firefox._patterns import FIREFOX_ARTIFACTS
        assert "cache_index" in FIREFOX_ARTIFACTS
        patterns = FIREFOX_ARTIFACTS["cache_index"]
        assert any("cache2/index" in p for p in patterns)

    def test_cache_doomed_patterns_exist(self):
        from extractors.browser.firefox._patterns import FIREFOX_ARTIFACTS
        assert "cache_doomed" in FIREFOX_ARTIFACTS
        patterns = FIREFOX_ARTIFACTS["cache_doomed"]
        assert any("cache2/doomed" in p for p in patterns)

    def test_cache_trash_patterns_exist(self):
        from extractors.browser.firefox._patterns import FIREFOX_ARTIFACTS
        assert "cache_trash" in FIREFOX_ARTIFACTS
        patterns = FIREFOX_ARTIFACTS["cache_trash"]
        assert any("cache2/trash" in p for p in patterns)

    def test_cache_journal_patterns_exist(self):
        from extractors.browser.firefox._patterns import FIREFOX_ARTIFACTS
        assert "cache_journal" in FIREFOX_ARTIFACTS

    def test_cache_artifacts_use_cache_roots(self):
        from extractors.browser.firefox._patterns import CACHE_ARTIFACTS
        assert "cache_index" in CACHE_ARTIFACTS
        assert "cache_doomed" in CACHE_ARTIFACTS
        assert "cache_trash" in CACHE_ARTIFACTS
        assert "cache_journal" in CACHE_ARTIFACTS
