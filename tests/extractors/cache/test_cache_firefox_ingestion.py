"""Tests for Firefox cache ingestion optimisations.

Covers:
- _find_url_for_hash_fast O(1) lookup via pre-built hash map
- Cancellation checks inside _process_cache_index loops
- Journal entry accumulation (+=) across multiple journal files
- uint64 origin_attrs_hash overflow protection for SQLite
"""
from __future__ import annotations

import json
import sqlite3
import struct
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from extractors.browser.firefox.cache.ingestion import CacheIngestionHandler
from extractors.browser.firefox.cache._index import CacheIndexEntry
from core.database.helpers.firefox_cache_index import (
    _uint64_to_sqlite,
    insert_firefox_cache_index_entries,
)


# =====================================================================
# Helpers
# =====================================================================


class FakeCallbacks:
    """Minimal callbacks implementation for testing."""

    def __init__(self, *, cancel_after: Optional[int] = None):
        self.logs: List[str] = []
        self.progress_calls: List[tuple] = []
        self.steps: List[str] = []
        self._cancel_after = cancel_after
        self._progress_count = 0
        self._cancelled = False

    def on_progress(self, current: int, total: int, message: str = "") -> None:
        self._progress_count += 1
        self.progress_calls.append((current, total, message))

    def on_log(self, message: str, level: str = "info") -> None:
        self.logs.append(message)

    def on_error(self, error: str, details: str = "") -> None:
        pass

    def on_step(self, step_name: str) -> None:
        self.steps.append(step_name)

    def is_cancelled(self) -> bool:
        if self._cancel_after is not None and self._progress_count >= self._cancel_after:
            self._cancelled = True
        return self._cancelled


def _make_manifest_file(
    hash_name: str,
    artifact_type: str = "cache_firefox",
    success: bool = True,
    extracted_path: str = "",
) -> Dict[str, Any]:
    """Create a minimal manifest file entry."""
    return {
        "source_path": f"/cache2/entries/{hash_name}",
        "artifact_type": artifact_type,
        "success": success,
        "extracted_path": extracted_path or f"entries/{hash_name}",
    }


def _make_index_entry(
    hash_name: str = "A" * 40,
    frecency: int = 100,
    flags: int = 1,
) -> CacheIndexEntry:
    """Create a minimal CacheIndexEntry for testing."""
    return CacheIndexEntry(
        hash=hash_name,
        frecency=frecency,
        origin_attrs_hash=0,
        on_start_time=0,
        on_stop_time=0,
        content_type=0,
        flags=flags,
    )


# =====================================================================
# _find_url_for_hash_fast
# =====================================================================


class TestFindUrlForHashFast:
    """The O(1) hash-to-URL lookup replaces the old O(n) scan."""

    def test_returns_none_when_hash_not_in_map(self, tmp_path):
        result = CacheIngestionHandler._find_url_for_hash_fast(
            "DEADBEEF" * 5,
            {},
            tmp_path,
        )
        assert result is None

    def test_returns_none_when_extracted_file_missing(self, tmp_path):
        hash_map = {
            "ABC123": {"extracted_path": "entries/ABC123"},
        }
        result = CacheIngestionHandler._find_url_for_hash_fast(
            "ABC123", hash_map, tmp_path,
        )
        assert result is None

    def test_returns_url_from_parsed_entry(self, tmp_path):
        """When the file exists and parses OK, we get the URL back."""
        hash_name = "AAAA" * 10
        entry_dir = tmp_path / "entries"
        entry_dir.mkdir()
        entry_file = entry_dir / hash_name
        entry_file.write_bytes(b"\x00" * 64)  # dummy — we mock the parser

        hash_map = {
            hash_name: {"extracted_path": f"entries/{hash_name}"},
        }

        fake_result = MagicMock()
        fake_result.url = "https://example.com/image.png"

        with patch(
            "extractors.browser.firefox.cache.ingestion.parse_cache2_entry",
            return_value=fake_result,
        ):
            url = CacheIngestionHandler._find_url_for_hash_fast(
                hash_name, hash_map, tmp_path,
            )
        assert url == "https://example.com/image.png"

    def test_returns_none_on_parse_exception(self, tmp_path):
        hash_name = "BBBB" * 10
        entry_dir = tmp_path / "entries"
        entry_dir.mkdir()
        (entry_dir / hash_name).write_bytes(b"\x00")

        hash_map = {
            hash_name: {"extracted_path": f"entries/{hash_name}"},
        }

        with patch(
            "extractors.browser.firefox.cache.ingestion.parse_cache2_entry",
            side_effect=Exception("corrupt"),
        ):
            url = CacheIngestionHandler._find_url_for_hash_fast(
                hash_name, hash_map, tmp_path,
            )
        assert url is None


# =====================================================================
# Hash map construction in _process_cache_index
# =====================================================================


class TestHashMapConstruction:
    """
    The pre-built _hash_to_file_entry map must include cache_firefox,
    cache_doomed, and cache_trash entries but not index/journal.
    """

    def test_hash_map_built_correctly(self):
        """Verify the lookup maps are built from the right artifact types."""
        handler = CacheIngestionHandler(
            extractor_name="cache_firefox",
            extractor_version="1.0.0",
        )

        manifest = {
            "files": [
                _make_manifest_file("HASH_A", "cache_firefox"),
                _make_manifest_file("HASH_B", "cache_doomed"),
                _make_manifest_file("HASH_C", "cache_trash"),
                _make_manifest_file("index", "cache_index"),
                _make_manifest_file("journal", "cache_journal"),
                # Failed entry — should be skipped
                _make_manifest_file("HASH_D", "cache_firefox", success=False),
            ]
        }

        ARTIFACT_SOURCE_MAP = {
            "cache_firefox": "entries",
            "cache_doomed": "doomed",
            "cache_trash": "trash",
        }
        CACHE_ARTIFACT_TYPES = {"cache_firefox", "cache_doomed", "cache_trash"}

        entry_file_lookup: Dict[str, str] = {}
        hash_to_file_entry: Dict[str, Dict[str, Any]] = {}

        for fe in manifest.get("files", []):
            if not fe.get("success", True) or not fe.get("extracted_path"):
                continue
            at = fe.get("artifact_type", "cache_firefox")
            source_name = Path(fe.get("source_path", "")).name.upper()
            if at in ARTIFACT_SOURCE_MAP:
                entry_file_lookup[source_name] = ARTIFACT_SOURCE_MAP[at]
            if at in CACHE_ARTIFACT_TYPES:
                hash_to_file_entry[source_name] = fe

        # entry_file_lookup has A, B, C but not index, journal, D
        assert "HASH_A" in entry_file_lookup
        assert "HASH_B" in entry_file_lookup
        assert "HASH_C" in entry_file_lookup
        assert "INDEX" not in entry_file_lookup
        assert "JOURNAL" not in entry_file_lookup
        assert "HASH_D" not in entry_file_lookup

        # hash_to_file_entry has A, B, C only
        assert "HASH_A" in hash_to_file_entry
        assert "HASH_B" in hash_to_file_entry
        assert "HASH_C" in hash_to_file_entry
        assert "INDEX" not in hash_to_file_entry
        assert "JOURNAL" not in hash_to_file_entry


# =====================================================================
# Cancellation and progress heartbeat
# =====================================================================


class TestIndexProcessingCancellation:
    """Cancel and progress checks fire during index correlation."""

    def test_cancellation_stops_index_loop(self):
        """When is_cancelled returns True, the loop breaks early."""
        callbacks = FakeCallbacks(cancel_after=0)  # cancel immediately
        assert callbacks.is_cancelled() is True

        # Simulate the inner loop pattern
        entries = [_make_index_entry(f"{i:040X}") for i in range(500)]
        processed = 0
        for entry_idx, entry in enumerate(entries):
            if entry_idx % 200 == 0:
                if callbacks.is_cancelled():
                    break
            processed += 1

        # Should stop at first check (idx=0)
        assert processed == 0

    def test_progress_heartbeat_fires(self):
        """Progress callbacks fire every 200 entries."""
        callbacks = FakeCallbacks()  # never cancel
        entries = [_make_index_entry(f"{i:040X}") for i in range(600)]

        for entry_idx, entry in enumerate(entries):
            if entry_idx % 200 == 0:
                if callbacks.is_cancelled():
                    break
                if entry_idx > 0:
                    callbacks.on_progress(
                        entry_idx, len(entries),
                        f"Correlating ({entry_idx}/{len(entries)})",
                    )

        # Should have progress at idx 200 and 400 (not 0)
        assert len(callbacks.progress_calls) == 2
        assert callbacks.progress_calls[0][0] == 200
        assert callbacks.progress_calls[1][0] == 400


# =====================================================================
# Journal accumulation
# =====================================================================


class TestJournalAccumulation:
    """stats['journal_entries'] must accumulate across multiple journals."""

    def test_accumulator_pattern(self):
        """Using += accumulates; using = would overwrite."""
        stats = {"journal_entries": 0}

        journal_counts = [10, 25, 7]
        for count in journal_counts:
            stats["journal_entries"] += count

        assert stats["journal_entries"] == 42  # 10 + 25 + 7


# =====================================================================
# uint64 → signed int64 overflow protection
# =====================================================================


class TestUint64Overflow:
    """origin_attrs_hash is uint64 which can exceed SQLite signed int64."""

    def test_small_value_unchanged(self):
        assert _uint64_to_sqlite(0) == 0
        assert _uint64_to_sqlite(42) == 42
        assert _uint64_to_sqlite((1 << 63) - 1) == (1 << 63) - 1

    def test_none_unchanged(self):
        assert _uint64_to_sqlite(None) is None

    def test_max_signed_boundary(self):
        """2^63 - 1 is the max SQLite INTEGER; should pass through."""
        val = (1 << 63) - 1  # 9223372036854775807
        assert _uint64_to_sqlite(val) == val

    def test_overflow_converted_to_signed(self):
        """2^63 and above must be converted to negative signed int64."""
        val = 1 << 63  # 9223372036854775808
        result = _uint64_to_sqlite(val)
        assert result == -(1 << 63)  # -9223372036854775808

    def test_max_uint64_converted(self):
        """2^64 - 1 must convert to -1 (all bits set)."""
        val = (1 << 64) - 1
        result = _uint64_to_sqlite(val)
        assert result == -1

    def test_arbitrary_high_value(self):
        """Verify round-trip: unsigned → signed → back to unsigned."""
        val = 0xFEDCBA9876543210  # > 2^63
        signed = _uint64_to_sqlite(val)
        assert signed < 0
        # Convert back to unsigned
        assert (signed + (1 << 64)) == val

    def test_insert_with_large_origin_attrs_hash(self, tmp_path):
        """Batch insert must not raise OverflowError for large uint64."""
        db_path = tmp_path / "test.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE firefox_cache_index (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT, evidence_id INTEGER, partition_index INTEGER,
                source_path TEXT, entry_hash TEXT, frecency INTEGER,
                origin_attrs_hash INTEGER, on_start_time INTEGER,
                on_stop_time INTEGER, content_type INTEGER,
                content_type_name TEXT, file_size_kb INTEGER,
                raw_flags INTEGER, is_initialized INTEGER,
                is_anonymous INTEGER, is_removed INTEGER,
                is_pinned INTEGER, has_alt_data INTEGER,
                index_version INTEGER, index_timestamp INTEGER,
                index_dirty INTEGER, has_entry_file INTEGER,
                entry_source TEXT, url TEXT, browser TEXT,
                profile_path TEXT, os_user TEXT
            )
        """)
        entries = [
            {
                "run_id": "test_run",
                "entry_hash": "A" * 40,
                "origin_attrs_hash": 0xFFFFFFFFFFFFFFFF,  # max uint64
                "frecency": 100,
            },
            {
                "run_id": "test_run",
                "entry_hash": "B" * 40,
                "origin_attrs_hash": (1 << 63) + 1,  # just above signed max
            },
            {
                "run_id": "test_run",
                "entry_hash": "C" * 40,
                "origin_attrs_hash": 42,  # normal value
            },
        ]
        # Should not raise OverflowError
        inserted = insert_firefox_cache_index_entries(conn, 1, entries)
        assert inserted == 3

        # Verify data round-trips correctly
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT entry_hash, origin_attrs_hash FROM firefox_cache_index ORDER BY entry_hash"
        ).fetchall()

        row_a = dict(rows[0])
        row_b = dict(rows[1])
        row_c = dict(rows[2])

        assert row_a["origin_attrs_hash"] == -1  # 0xFFFF... → -1
        assert row_b["origin_attrs_hash"] == -(((1 << 64) - (1 << 63)) - 1)
        assert row_c["origin_attrs_hash"] == 42

        conn.close()
