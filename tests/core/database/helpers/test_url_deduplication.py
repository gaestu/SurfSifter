"""Tests for URL deduplication functionality.

Initial tests for URL deduplication feature.
"""
import json
import sqlite3
from pathlib import Path

import pytest

from core.database.helpers.urls import (
    analyze_url_duplicates,
    deduplicate_urls,
    insert_urls,
)


@pytest.fixture
def evidence_db(tmp_path):
    """Create a test evidence database with URLs."""
    db_path = tmp_path / "evidence.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Create minimal schema
    conn.execute("""
        CREATE TABLE urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            url TEXT NOT NULL,
            domain TEXT,
            scheme TEXT,
            discovered_by TEXT NOT NULL,
            first_seen_utc TEXT,
            last_seen_utc TEXT,
            source_path TEXT,
            tags TEXT,
            notes TEXT,
            context TEXT,
            run_id TEXT,
            cache_key TEXT,
            cache_filename TEXT,
            response_code INTEGER,
            content_type TEXT,
            occurrence_count INTEGER DEFAULT 1
        )
    """)

    # Create indexes for REINDEX
    conn.execute("CREATE INDEX idx_urls_evidence ON urls(evidence_id)")
    conn.execute("CREATE INDEX idx_urls_domain ON urls(domain)")
    conn.execute("CREATE INDEX idx_urls_url ON urls(url)")
    conn.execute("CREATE INDEX idx_urls_evidence_domain ON urls(evidence_id, domain)")
    conn.execute("CREATE INDEX idx_urls_evidence_source ON urls(evidence_id, discovered_by)")
    conn.execute("CREATE INDEX idx_urls_evidence_first_seen ON urls(evidence_id, first_seen_utc DESC)")
    conn.execute("CREATE INDEX idx_urls_evidence_last_seen ON urls(evidence_id, last_seen_utc DESC)")
    conn.execute("CREATE INDEX idx_urls_evidence_occurrence ON urls(evidence_id, occurrence_count DESC)")

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def evidence_id():
    """Evidence ID for tests."""
    return 1


def insert_test_urls(conn, evidence_id, urls):
    """Helper to insert test URL rows."""
    for url_data in urls:
        conn.execute(
            """
            INSERT INTO urls (
                evidence_id, url, domain, scheme, discovered_by,
                first_seen_utc, last_seen_utc, source_path, tags, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evidence_id,
                url_data.get("url"),
                url_data.get("domain"),
                url_data.get("scheme"),
                url_data.get("discovered_by"),
                url_data.get("first_seen_utc"),
                url_data.get("last_seen_utc"),
                url_data.get("source_path"),
                url_data.get("tags"),
                url_data.get("notes"),
            )
        )
    conn.commit()


class TestAnalyzeUrlDuplicates:
    """Tests for analyze_url_duplicates function."""

    def test_no_sources_returns_empty(self, evidence_db, evidence_id):
        """Empty sources list returns zero counts."""
        result = analyze_url_duplicates(
            evidence_db, evidence_id, sources=[]
        )
        assert result == {"total": 0, "unique_count": 0, "duplicates": 0}

    def test_no_urls_returns_zeros(self, evidence_db, evidence_id):
        """No URLs in database returns zero counts."""
        result = analyze_url_duplicates(
            evidence_db, evidence_id, sources=["bulk_extractor:url"]
        )
        assert result["total"] == 0
        assert result["unique_count"] == 0
        assert result["duplicates"] == 0

    def test_all_unique_urls(self, evidence_db, evidence_id):
        """All unique URLs returns zero duplicates."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://example1.com", "domain": "example1.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:100"},
            {"url": "http://example2.com", "domain": "example2.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:200"},
            {"url": "http://example3.com", "domain": "example3.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:300"},
        ])

        result = analyze_url_duplicates(
            evidence_db, evidence_id, sources=["bulk_extractor:url"]
        )
        assert result["total"] == 3
        assert result["unique_count"] == 3
        assert result["duplicates"] == 0

    def test_duplicate_urls_same_source(self, evidence_db, evidence_id):
        """Duplicate URLs from same source detected."""
        # Same URL appearing 3 times with different source_paths
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://euro-slot.com/", "domain": "euro-slot.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:100"},
            {"url": "http://euro-slot.com/", "domain": "euro-slot.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:200"},
            {"url": "http://euro-slot.com/", "domain": "euro-slot.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:300"},
            {"url": "http://other.com/", "domain": "other.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:400"},
        ])

        result = analyze_url_duplicates(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )
        assert result["total"] == 4
        assert result["unique_count"] == 2  # euro-slot and other
        assert result["duplicates"] == 2  # 3 - 1 = 2 duplicates

    def test_unique_by_first_seen(self, evidence_db, evidence_id):
        """URLs with different first_seen are considered unique."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "first_seen_utc": "2024-01-01T10:00:00", "source_path": "url.txt:100"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "first_seen_utc": "2024-01-02T10:00:00", "source_path": "url.txt:200"},
        ])

        # With unique_by_first_seen=True, these are unique
        result = analyze_url_duplicates(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=True,
            unique_by_last_seen=False,
        )
        assert result["duplicates"] == 0

        # With unique_by_first_seen=False, these are duplicates
        result = analyze_url_duplicates(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )
        assert result["duplicates"] == 1

    def test_source_filter(self, evidence_db, evidence_id):
        """Only URLs from selected sources are analyzed."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:100"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:200"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "chromium_history", "source_path": "History"},
        ])

        result = analyze_url_duplicates(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )
        assert result["total"] == 2  # Only bulk_extractor URLs
        assert result["duplicates"] == 1


class TestDeduplicateUrls:
    """Tests for deduplicate_urls function."""

    def test_no_sources_returns_empty(self, evidence_db, evidence_id):
        """Empty sources list returns zeros and does nothing."""
        result = deduplicate_urls(
            evidence_db, evidence_id, sources=[]
        )
        assert result["total_before"] == 0
        assert result["duplicates_removed"] == 0

    def test_no_duplicates_no_changes(self, evidence_db, evidence_id):
        """No duplicates means no changes."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://example1.com", "domain": "example1.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:100"},
            {"url": "http://example2.com", "domain": "example2.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:200"},
        ])

        result = deduplicate_urls(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )

        assert result["total_before"] == 2
        assert result["total_after"] == 2
        assert result["duplicates_removed"] == 0

        # Verify no rows deleted
        count = evidence_db.execute(
            "SELECT COUNT(*) FROM urls WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()[0]
        assert count == 2

    def test_basic_deduplication(self, evidence_db, evidence_id):
        """Basic deduplication merges duplicate URLs."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://euro-slot.com/", "domain": "euro-slot.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:100"},
            {"url": "http://euro-slot.com/", "domain": "euro-slot.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:200"},
            {"url": "http://euro-slot.com/", "domain": "euro-slot.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:300"},
            {"url": "http://other.com/", "domain": "other.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:400"},
        ])

        result = deduplicate_urls(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )

        assert result["total_before"] == 4
        assert result["total_after"] == 2
        assert result["duplicates_removed"] == 2
        assert result["unique_urls_affected"] == 1  # Only euro-slot had duplicates

        # Verify correct number of rows remain
        count = evidence_db.execute(
            "SELECT COUNT(*) FROM urls WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()[0]
        assert count == 2

    def test_source_path_merge(self, evidence_db, evidence_id):
        """Source paths are merged into comma-separated list."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:100"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:200"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:300"},
        ])

        deduplicate_urls(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )

        # Check merged source_path
        row = evidence_db.execute(
            "SELECT source_path FROM urls WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()

        paths = row["source_path"].split(", ")
        assert len(paths) == 3
        assert "url.txt:100" in paths
        assert "url.txt:200" in paths
        assert "url.txt:300" in paths

    def test_timestamp_merge(self, evidence_db, evidence_id):
        """Timestamps are merged (earliest first_seen, latest last_seen)."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url",
             "first_seen_utc": "2024-01-02T10:00:00", "last_seen_utc": "2024-01-02T15:00:00", "source_path": "url.txt:100"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url",
             "first_seen_utc": "2024-01-01T10:00:00", "last_seen_utc": "2024-01-03T15:00:00", "source_path": "url.txt:200"},
        ])

        deduplicate_urls(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )

        row = evidence_db.execute(
            "SELECT first_seen_utc, last_seen_utc FROM urls WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()

        assert row["first_seen_utc"] == "2024-01-01T10:00:00"  # Earliest
        assert row["last_seen_utc"] == "2024-01-03T15:00:00"  # Latest

    def test_tags_merge(self, evidence_db, evidence_id):
        """Tags are merged (union of all)."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url",
             "tags": '["suspicious"]', "source_path": "url.txt:100"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url",
             "tags": '["malware", "phishing"]', "source_path": "url.txt:200"},
        ])

        deduplicate_urls(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )

        row = evidence_db.execute(
            "SELECT tags FROM urls WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()

        tags = json.loads(row["tags"])
        assert set(tags) == {"suspicious", "malware", "phishing"}

    def test_progress_callback(self, evidence_db, evidence_id):
        """Progress callback is called during deduplication."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://example1.com/", "domain": "example1.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:100"},
            {"url": "http://example1.com/", "domain": "example1.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:200"},
            {"url": "http://example2.com/", "domain": "example2.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:300"},
            {"url": "http://example2.com/", "domain": "example2.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:400"},
        ])

        progress_calls = []
        def callback(current, total):
            progress_calls.append((current, total))

        deduplicate_urls(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
            progress_callback=callback,
        )

        assert len(progress_calls) == 2  # Two duplicate groups
        assert progress_calls[-1][0] == progress_calls[-1][1]  # Final call shows completion

    def test_preserves_other_sources(self, evidence_db, evidence_id):
        """URLs from non-selected sources are not affected."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:100"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:200"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "chromium_history", "source_path": "History"},
        ])

        deduplicate_urls(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )

        # Should have 2 rows: 1 merged bulk_extractor + 1 chromium_history
        count = evidence_db.execute(
            "SELECT COUNT(*) FROM urls WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()[0]
        assert count == 2

        # Chromium URL should be untouched
        chromium = evidence_db.execute(
            "SELECT source_path FROM urls WHERE evidence_id = ? AND discovered_by = 'chromium_history'", (evidence_id,)
        ).fetchone()
        assert chromium["source_path"] == "History"  # Not modified

    def test_occurrence_count(self, evidence_db, evidence_id):
        """Occurrence count is updated to reflect merged duplicates."""
        insert_test_urls(evidence_db, evidence_id, [
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:100"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:200"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:300"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:400"},
            {"url": "http://example.com/", "domain": "example.com", "discovered_by": "bulk_extractor:url", "source_path": "url.txt:500"},
        ])

        deduplicate_urls(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )

        # Should have 1 row with occurrence_count = 5
        row = evidence_db.execute(
            "SELECT occurrence_count FROM urls WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()
        assert row["occurrence_count"] == 5

    def test_occurrence_count_preserves_existing(self, evidence_db, evidence_id):
        """Occurrence count sums existing counts when deduplicating already-merged rows."""
        # Insert URL with existing occurrence_count of 3
        evidence_db.execute(
            """
            INSERT INTO urls (evidence_id, url, domain, discovered_by, source_path, occurrence_count)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "http://example.com/", "example.com", "bulk_extractor:url", "url.txt:100", 3)
        )
        # Insert another duplicate with occurrence_count of 2
        evidence_db.execute(
            """
            INSERT INTO urls (evidence_id, url, domain, discovered_by, source_path, occurrence_count)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "http://example.com/", "example.com", "bulk_extractor:url", "url.txt:200", 2)
        )
        evidence_db.commit()

        deduplicate_urls(
            evidence_db, evidence_id,
            sources=["bulk_extractor:url"],
            unique_by_first_seen=False,
            unique_by_last_seen=False,
        )

        # Should have 1 row with occurrence_count = 3 + 2 = 5
        row = evidence_db.execute(
            "SELECT occurrence_count FROM urls WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()
        assert row["occurrence_count"] == 5
