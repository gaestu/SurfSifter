"""Tests for Firefox cache index database helpers — count and entry_source filter."""
from __future__ import annotations

import pytest

from core.database.helpers import (
    get_firefox_cache_index_count,
    get_firefox_cache_index_entries,
    get_firefox_cache_index_stats,
    insert_firefox_cache_index_entries,
)


def _make_entry(hash_char: str, **overrides) -> dict:
    """Build a minimal valid firefox_cache_index entry dict."""
    entry = {
        "run_id": "run-test",
        "source_path": "/cache2/index",
        "entry_hash": hash_char * 40,
        "browser": "firefox",
    }
    entry.update(overrides)
    return entry


class TestGetFirefoxCacheIndexCount:
    """Tests for the get_firefox_cache_index_count function."""

    def test_count_empty_db(self, evidence_db):
        count = get_firefox_cache_index_count(evidence_db, 1)
        assert count == 0

    def test_count_with_data(self, evidence_db):
        entries = [_make_entry(c) for c in "ABCDE"]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        count = get_firefox_cache_index_count(evidence_db, 1)
        assert count == 5

    def test_count_filter_removed_only(self, evidence_db):
        entries = [
            _make_entry("A", is_removed=True),
            _make_entry("B", is_removed=False),
            _make_entry("C", is_removed=True),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        count = get_firefox_cache_index_count(evidence_db, 1, removed_only=True)
        assert count == 2

    def test_count_filter_has_entry_file(self, evidence_db):
        entries = [
            _make_entry("A", has_entry_file=True),
            _make_entry("B", has_entry_file=False),
            _make_entry("C", has_entry_file=True),
            _make_entry("D", has_entry_file=False),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        assert get_firefox_cache_index_count(evidence_db, 1, has_entry_file=True) == 2
        assert get_firefox_cache_index_count(evidence_db, 1, has_entry_file=False) == 2

    def test_count_filter_content_type(self, evidence_db):
        entries = [
            _make_entry("A", content_type=3, content_type_name="image"),
            _make_entry("B", content_type=3, content_type_name="image"),
            _make_entry("C", content_type=2, content_type_name="javascript"),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        assert get_firefox_cache_index_count(evidence_db, 1, content_type=3) == 2
        assert get_firefox_cache_index_count(evidence_db, 1, content_type=2) == 1
        assert get_firefox_cache_index_count(evidence_db, 1, content_type=5) == 0

    def test_count_filter_entry_source(self, evidence_db):
        entries = [
            _make_entry("A", entry_source="entries"),
            _make_entry("B", entry_source="doomed"),
            _make_entry("C", entry_source="entries"),
            _make_entry("D", entry_source="trash"),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        assert get_firefox_cache_index_count(evidence_db, 1, entry_source="entries") == 2
        assert get_firefox_cache_index_count(evidence_db, 1, entry_source="doomed") == 1
        assert get_firefox_cache_index_count(evidence_db, 1, entry_source="trash") == 1
        assert get_firefox_cache_index_count(evidence_db, 1, entry_source="journal") == 0

    def test_count_combined_filters(self, evidence_db):
        entries = [
            _make_entry("A", is_removed=True, content_type=3, entry_source="entries"),
            _make_entry("B", is_removed=True, content_type=3, entry_source="doomed"),
            _make_entry("C", is_removed=False, content_type=3, entry_source="entries"),
            _make_entry("D", is_removed=True, content_type=2, entry_source="entries"),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        count = get_firefox_cache_index_count(
            evidence_db, 1, removed_only=True, content_type=3,
        )
        assert count == 2

        count = get_firefox_cache_index_count(
            evidence_db, 1, removed_only=True, entry_source="entries",
        )
        assert count == 2


class TestEntrySourceFilter:
    """Test entry_source filter on get_firefox_cache_index_entries."""

    def test_filter_by_entry_source(self, evidence_db):
        entries = [
            _make_entry("A", entry_source="entries"),
            _make_entry("B", entry_source="doomed"),
            _make_entry("C", entry_source="trash"),
            _make_entry("D", entry_source="entries"),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        results = get_firefox_cache_index_entries(
            evidence_db, 1, entry_source="entries",
        )
        assert len(results) == 2
        assert all(r["entry_source"] == "entries" for r in results)

    def test_filter_doomed(self, evidence_db):
        entries = [
            _make_entry("A", entry_source="entries"),
            _make_entry("B", entry_source="doomed"),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        results = get_firefox_cache_index_entries(
            evidence_db, 1, entry_source="doomed",
        )
        assert len(results) == 1
        assert results[0]["entry_hash"] == "B" * 40


class TestPagination:
    """Test pagination with count for the cache index entries."""

    def test_pagination_limit_offset(self, evidence_db):
        entries = [_make_entry(chr(ord("A") + i)) for i in range(10)]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        count = get_firefox_cache_index_count(evidence_db, 1)
        assert count == 10

        page1 = get_firefox_cache_index_entries(evidence_db, 1, limit=3, offset=0)
        assert len(page1) == 3

        page2 = get_firefox_cache_index_entries(evidence_db, 1, limit=3, offset=3)
        assert len(page2) == 3

        # No overlap
        hashes_1 = {r["entry_hash"] for r in page1}
        hashes_2 = {r["entry_hash"] for r in page2}
        assert hashes_1.isdisjoint(hashes_2)

    def test_count_matches_unfiltered_entries(self, evidence_db):
        entries = [_make_entry(chr(ord("A") + i)) for i in range(5)]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        count = get_firefox_cache_index_count(evidence_db, 1)
        all_entries = get_firefox_cache_index_entries(evidence_db, 1)
        assert count == len(all_entries)

    def test_filtered_count_matches_filtered_entries(self, evidence_db):
        entries = [
            _make_entry("A", is_removed=True),
            _make_entry("B", is_removed=False),
            _make_entry("C", is_removed=True),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        count = get_firefox_cache_index_count(evidence_db, 1, removed_only=True)
        filtered = get_firefox_cache_index_entries(evidence_db, 1, removed_only=True)
        assert count == len(filtered) == 2


class TestStatsConsistency:
    """Test that stats figures are consistent."""

    def test_stats_by_content_type(self, evidence_db):
        entries = [
            _make_entry("A", content_type=3, content_type_name="image"),
            _make_entry("B", content_type=3, content_type_name="image"),
            _make_entry("C", content_type=2, content_type_name="javascript"),
            _make_entry("D", content_type=5, content_type_name="css"),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        stats = get_firefox_cache_index_stats(evidence_db, 1)
        assert stats["total"] == 4
        assert stats["by_content_type"]["image"] == 2
        assert stats["by_content_type"]["javascript"] == 1
        assert stats["by_content_type"]["css"] == 1

    def test_stats_by_entry_source(self, evidence_db):
        entries = [
            _make_entry("A", entry_source="entries"),
            _make_entry("B", entry_source="doomed"),
            _make_entry("C", entry_source=None),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        stats = get_firefox_cache_index_stats(evidence_db, 1)
        assert stats["by_entry_source"]["entries"] == 1
        assert stats["by_entry_source"]["doomed"] == 1
        assert stats["by_entry_source"]["index_only"] == 1

    def test_stats_removed_and_file_counts(self, evidence_db):
        entries = [
            _make_entry("A", is_removed=True, has_entry_file=True),
            _make_entry("B", is_removed=False, has_entry_file=True),
            _make_entry("C", is_removed=False, has_entry_file=False),
        ]
        insert_firefox_cache_index_entries(evidence_db, 1, entries)
        evidence_db.commit()

        stats = get_firefox_cache_index_stats(evidence_db, 1)
        assert stats["total"] == 3
        assert stats["removed"] == 1
        assert stats["with_file"] == 2
        assert stats["without_file"] == 1
