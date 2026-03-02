"""Tests for Chromium simple cache index file parser (_index.py).

Regression tests for the V9 24-byte entry format fix, plus V7/V8
backward compatibility.
"""

from __future__ import annotations

import struct
from datetime import datetime, timezone
from pathlib import Path

import pytest

from extractors.browser.chromium.cache._index import (
    IndexEntry,
    IndexMetadata,
    parse_index_file,
)
from extractors.browser.chromium.cache._schemas import (
    SIMPLE_INDEX_MAGIC,
    SIMPLE_INDEX_MAGIC_V7,
    WINDOWS_EPOCH_OFFSET_MICROSECONDS,
)


# ---------------------------------------------------------------------------
# Helpers to build synthetic index files
# ---------------------------------------------------------------------------

# Seconds between 1601 and 1970
_WINDOWS_EPOCH_OFFSET_SECONDS = 11_644_473_600

# Reference timestamp: 2025-10-18 23:13:42 UTC
_REF_DT = datetime(2025, 10, 18, 23, 13, 42, tzinfo=timezone.utc)
_REF_UNIX_S = int(_REF_DT.timestamp())
_REF_US_SINCE_1601 = (_REF_UNIX_S * 1_000_000) + WINDOWS_EPOCH_OFFSET_MICROSECONDS
_REF_S_SINCE_1601 = _REF_UNIX_S + _WINDOWS_EPOCH_OFFSET_SECONDS


def _build_index(
    magic: int,
    version: int,
    entries: list[tuple[int, ...]],
    entry_struct_fmt: str,
    cache_size: int = 0,
    write_reason: int = 0,
    cache_last_modified_us: int = 0,
) -> bytes:
    """
    Build a minimal Chromium simple-cache index file from parts.

    Parameters
    ----------
    magic : int
        8-byte magic for the header.
    version : int
        4-byte version.
    entries : list[tuple]
        Each tuple is packed using *entry_struct_fmt*.
    entry_struct_fmt : str
        ``struct`` format for one entry (e.g. ``'<QqII'`` for V9).
    cache_size : int
        Value for the cache_size metadata field.
    write_reason : int
        Value for the write_reason metadata field.
    cache_last_modified_us : int
        8-byte trailing timestamp (microseconds since 1601-01-01).
    """
    # Build the *payload* (everything after the 8-byte pickle header)
    payload = struct.pack(
        '<QIQQi',          # magic(Q), version(I), count(Q), cache_size(Q), reason(i)
        magic, version, len(entries), cache_size, write_reason,
    )
    for entry_fields in entries:
        payload += struct.pack(entry_struct_fmt, *entry_fields)
    payload += struct.pack('<q', cache_last_modified_us)

    # Pickle header: payload_size (4 bytes) + CRC placeholder (4 bytes)
    pickle_header = struct.pack('<II', len(payload), 0x00000000)
    return pickle_header + payload


# ---------------------------------------------------------------------------
# V9 format (24-byte entries)
# ---------------------------------------------------------------------------

class TestV9IndexParsing:
    """V9 index files with 24-byte entries."""

    def test_single_entry_timestamp(self, tmp_path: Path):
        """V9 entry timestamp decodes to the correct date."""
        hash_val = 0x87527CAAB1C16077
        size_chunks = 512  # 512 * 256 = 131072 bytes
        in_memory = 0

        data = _build_index(
            magic=SIMPLE_INDEX_MAGIC,
            version=9,
            entries=[(hash_val, _REF_US_SINCE_1601, size_chunks, in_memory)],
            entry_struct_fmt='<QqII',
            cache_last_modified_us=_REF_US_SINCE_1601,
        )
        index_file = tmp_path / "the-real-index"
        index_file.write_bytes(data)

        metadata, entries = parse_index_file(index_file)

        assert metadata is not None
        assert metadata.version == 9
        assert metadata.entry_count == 1
        assert len(entries) == 1

        e = entries[0]
        assert e.entry_hash == hash_val
        assert e.entry_size == 131072
        # Allow 1-second tolerance for rounding
        assert abs((e.last_used_time - _REF_DT).total_seconds()) < 1

    def test_multiple_entries_alignment(self, tmp_path: Path):
        """Multiple V9 entries stay correctly aligned (no 16-byte drift)."""
        ts1_us = _REF_US_SINCE_1601
        ts2_us = _REF_US_SINCE_1601 - 60_000_000  # 60 s earlier

        entries_raw = [
            (0xAAAAAAAABBBBBBBB, ts1_us, 512, 0),
            (0xCCCCCCCCDDDDDDDD, ts2_us, 1024, 0),
        ]

        data = _build_index(
            magic=SIMPLE_INDEX_MAGIC,
            version=9,
            entries=entries_raw,
            entry_struct_fmt='<QqII',
        )
        index_file = tmp_path / "the-real-index"
        index_file.write_bytes(data)

        metadata, entries = parse_index_file(index_file)

        assert metadata is not None
        assert len(entries) == 2
        assert entries[0].entry_hash == 0xAAAAAAAABBBBBBBB
        assert entries[1].entry_hash == 0xCCCCCCCCDDDDDDDD
        # Second entry should be ~60 s earlier
        delta = (entries[0].last_used_time - entries[1].last_used_time).total_seconds()
        assert abs(delta - 60) < 1

    def test_cache_last_modified(self, tmp_path: Path):
        """Trailing cache_last_modified is parsed correctly."""
        data = _build_index(
            magic=SIMPLE_INDEX_MAGIC,
            version=9,
            entries=[],
            entry_struct_fmt='<QqII',
            cache_last_modified_us=_REF_US_SINCE_1601,
        )
        index_file = tmp_path / "the-real-index"
        index_file.write_bytes(data)

        metadata, _ = parse_index_file(index_file)
        assert metadata is not None
        assert metadata.cache_last_modified is not None
        assert abs((metadata.cache_last_modified - _REF_DT).total_seconds()) < 1


# ---------------------------------------------------------------------------
# V7/V8 format (16-byte entries) — backward compatibility
# ---------------------------------------------------------------------------

class TestV7IndexParsing:
    """V7/V8 index files with 16-byte entries and legacy magic."""

    def test_v7_entry_timestamp(self, tmp_path: Path):
        """V7 entry parses correctly (uint32 seconds since 1601).

        Note: uint32 overflows for modern dates (~1737+), so V7/V8
        timestamps for 2025 dates are inherently lossy in Chromium.
        We test with a small value that fits uint32.
        """
        hash_val = 0x1234567890ABCDEF
        # Use a small value that fits uint32 — too small to be a valid
        # post-1970 date, but the parser should handle it gracefully
        timestamp_seconds = 100_000_000  # ~1604 AD
        size_flags = 512  # chunks, no flags

        data = _build_index(
            magic=SIMPLE_INDEX_MAGIC_V7,
            version=7,
            entries=[(hash_val, timestamp_seconds, size_flags)],
            entry_struct_fmt='<QII',
        )
        index_file = tmp_path / "index"
        index_file.write_bytes(data)

        metadata, entries = parse_index_file(index_file)

        assert metadata is not None
        assert metadata.version == 7
        assert len(entries) == 1

        e = entries[0]
        assert e.entry_hash == hash_val
        assert e.entry_size == 512 * 256
        # Timestamp is before Unix epoch, should be epoch fallback
        assert e.last_used_time is not None

    def test_v8_parses_same_as_v7(self, tmp_path: Path):
        """V8 uses same 16-byte entry format as V7."""
        data = _build_index(
            magic=SIMPLE_INDEX_MAGIC_V7,
            version=8,
            entries=[(0xDEADBEEF, 100000, 256)],
            entry_struct_fmt='<QII',
        )
        index_file = tmp_path / "index"
        index_file.write_bytes(data)

        metadata, entries = parse_index_file(index_file)
        assert metadata is not None
        assert metadata.version == 8
        assert len(entries) == 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestIndexEdgeCases:
    """Malformed, empty, or truncated index files."""

    def test_empty_file(self, tmp_path: Path):
        f = tmp_path / "index"
        f.write_bytes(b"")
        assert parse_index_file(f) == (None, [])

    def test_too_small_file(self, tmp_path: Path):
        f = tmp_path / "index"
        f.write_bytes(b"\x00" * 7)
        assert parse_index_file(f) == (None, [])

    def test_bad_magic(self, tmp_path: Path):
        data = _build_index(
            magic=0xDEADDEADDEADDEAD,
            version=9,
            entries=[],
            entry_struct_fmt='<QqII',
        )
        f = tmp_path / "index"
        f.write_bytes(data)
        meta, entries = parse_index_file(f)
        assert meta is None
        assert entries == []

    def test_truncated_entry(self, tmp_path: Path):
        """File declares 1 entry but data is truncated."""
        # Build a normal V9 index with 1 entry, then chop the entry data
        data = _build_index(
            magic=SIMPLE_INDEX_MAGIC,
            version=9,
            entries=[(0xAA, _REF_US_SINCE_1601, 1, 0)],
            entry_struct_fmt='<QqII',
        )
        # Remove the last 10 bytes so entry data is incomplete
        f = tmp_path / "index"
        f.write_bytes(data[:-10])
        meta, entries = parse_index_file(f)
        # Should get metadata but fail on the entry
        assert meta is not None
        assert len(entries) == 0

    def test_zero_entries(self, tmp_path: Path):
        """Valid header with zero entries."""
        data = _build_index(
            magic=SIMPLE_INDEX_MAGIC,
            version=9,
            entries=[],
            entry_struct_fmt='<QqII',
        )
        f = tmp_path / "index"
        f.write_bytes(data)
        meta, entries = parse_index_file(f)
        assert meta is not None
        assert meta.entry_count == 0
        assert entries == []

    def test_nonexistent_file(self, tmp_path: Path):
        """Missing file returns gracefully."""
        meta, entries = parse_index_file(tmp_path / "nonexistent")
        assert meta is None
        assert entries == []
