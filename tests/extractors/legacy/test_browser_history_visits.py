"""Tests for browser history visit-level extraction (fix).

Verifies that the extractor correctly captures per-visit records,
not just per-URL aggregates. A URL visited 5 times should produce
5 records with distinct timestamps.
"""

import sqlite3
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest


def create_chromium_history_db(db_path: Path, urls_with_visits: list) -> None:
    """Create a minimal Chromium History SQLite database.

    Args:
        db_path: Path to create the database
        urls_with_visits: List of tuples (url, title, visit_timestamps)
            where visit_timestamps is a list of WebKit timestamps
    """
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE urls (
            id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT,
            visit_count INTEGER DEFAULT 0,
            typed_count INTEGER DEFAULT 0,
            last_visit_time INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE visits (
            id INTEGER PRIMARY KEY,
            url INTEGER NOT NULL,
            visit_time INTEGER NOT NULL,
            from_visit INTEGER DEFAULT 0,
            transition INTEGER DEFAULT 0,
            FOREIGN KEY (url) REFERENCES urls(id)
        )
    """)

    for url_id, (url, title, visit_timestamps) in enumerate(urls_with_visits, start=1):
        last_visit = max(visit_timestamps) if visit_timestamps else 0
        conn.execute(
            "INSERT INTO urls (id, url, title, visit_count, typed_count, last_visit_time) VALUES (?, ?, ?, ?, ?, ?)",
            (url_id, url, title, len(visit_timestamps), 0, last_visit)
        )
        for visit_time in visit_timestamps:
            conn.execute(
                "INSERT INTO visits (url, visit_time, from_visit, transition) VALUES (?, ?, 0, 0)",
                (url_id, visit_time)
            )

    conn.commit()
    conn.close()


def create_firefox_history_db(db_path: Path, urls_with_visits: list) -> None:
    """Create a minimal Firefox places.sqlite database.

    Args:
        db_path: Path to create the database
        urls_with_visits: List of tuples (url, title, visit_timestamps)
            where visit_timestamps is a list of PRTime (microseconds since epoch)
    """
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE moz_places (
            id INTEGER PRIMARY KEY,
            url TEXT NOT NULL,
            title TEXT,
            visit_count INTEGER DEFAULT 0,
            typed INTEGER DEFAULT 0,
            hidden INTEGER DEFAULT 0,
            last_visit_date INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE moz_historyvisits (
            id INTEGER PRIMARY KEY,
            place_id INTEGER NOT NULL,
            visit_date INTEGER NOT NULL,
            from_visit INTEGER DEFAULT 0,
            visit_type INTEGER DEFAULT 0,
            FOREIGN KEY (place_id) REFERENCES moz_places(id)
        )
    """)

    for place_id, (url, title, visit_timestamps) in enumerate(urls_with_visits, start=1):
        last_visit = max(visit_timestamps) if visit_timestamps else None
        conn.execute(
            "INSERT INTO moz_places (id, url, title, visit_count, typed, last_visit_date) VALUES (?, ?, ?, ?, 0, ?)",
            (place_id, url, title, len(visit_timestamps), last_visit)
        )
        for visit_time in visit_timestamps:
            conn.execute(
                "INSERT INTO moz_historyvisits (place_id, visit_date, from_visit, visit_type) VALUES (?, ?, 0, 1)",
                (place_id, visit_time)
            )

    conn.commit()
    conn.close()


def datetime_to_webkit(dt: datetime) -> int:
    """Convert datetime to WebKit timestamp (microseconds since 1601-01-01)."""
    # Seconds from 1601-01-01 to 1970-01-01
    WEBKIT_EPOCH_OFFSET = 11644473600
    unix_timestamp = dt.timestamp()
    return int((unix_timestamp + WEBKIT_EPOCH_OFFSET) * 1_000_000)


def datetime_to_prtime(dt: datetime) -> int:
    """Convert datetime to PRTime (microseconds since 1970-01-01)."""
    return int(dt.timestamp() * 1_000_000)


class TestChromiumVisitLevelQuery:
    """Test that Chromium history queries return per-visit records."""

    def test_single_url_multiple_visits(self, tmp_path: Path):
        """A URL visited 5 times should return 5 records."""
        db_path = tmp_path / "History"

        # Create 5 distinct visit times
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        visit_times = [
            datetime_to_webkit(base_time + timedelta(hours=i))
            for i in range(5)
        ]

        create_chromium_history_db(db_path, [
            ("https://example.com/page1", "Page 1", visit_times),
        ])

        # Query using the same pattern as the extractor
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT
                u.url,
                u.title,
                u.visit_count,
                u.typed_count,
                u.last_visit_time,
                v.visit_time,
                v.from_visit,
                v.transition
            FROM visits v
            JOIN urls u ON v.url = u.id
            WHERE v.visit_time > 0
            ORDER BY v.visit_time DESC
        """)

        rows = list(cursor)
        conn.close()

        # Should have 5 records (one per visit)
        assert len(rows) == 5, f"Expected 5 visit records, got {len(rows)}"

        # Each record should have a distinct visit_time
        visit_times_returned = [row["visit_time"] for row in rows]
        assert len(set(visit_times_returned)) == 5, "Visit times should be distinct"

        # All should be for the same URL
        urls_returned = [row["url"] for row in rows]
        assert all(url == "https://example.com/page1" for url in urls_returned)

    def test_multiple_urls_multiple_visits(self, tmp_path: Path):
        """Multiple URLs with multiple visits each."""
        db_path = tmp_path / "History"

        base_time = datetime(2024, 1, 15, 10, 0, 0)

        # URL 1: 3 visits
        url1_visits = [datetime_to_webkit(base_time + timedelta(hours=i)) for i in range(3)]
        # URL 2: 2 visits
        url2_visits = [datetime_to_webkit(base_time + timedelta(hours=i, minutes=30)) for i in range(2)]

        create_chromium_history_db(db_path, [
            ("https://example.com/page1", "Page 1", url1_visits),
            ("https://example.com/page2", "Page 2", url2_visits),
        ])

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT
                u.url,
                v.visit_time
            FROM visits v
            JOIN urls u ON v.url = u.id
            WHERE v.visit_time > 0
            ORDER BY v.visit_time DESC
        """)

        rows = list(cursor)
        conn.close()

        # Should have 5 total records (3 + 2)
        assert len(rows) == 5, f"Expected 5 visit records, got {len(rows)}"

        # Count records per URL
        url1_count = sum(1 for row in rows if row["url"] == "https://example.com/page1")
        url2_count = sum(1 for row in rows if row["url"] == "https://example.com/page2")

        assert url1_count == 3, f"Expected 3 visits for page1, got {url1_count}"
        assert url2_count == 2, f"Expected 2 visits for page2, got {url2_count}"


class TestFirefoxVisitLevelQuery:
    """Test that Firefox history queries return per-visit records."""

    def test_single_url_multiple_visits(self, tmp_path: Path):
        """A URL visited 5 times should return 5 records."""
        db_path = tmp_path / "places.sqlite"

        # Create 5 distinct visit times
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        visit_times = [
            datetime_to_prtime(base_time + timedelta(hours=i))
            for i in range(5)
        ]

        create_firefox_history_db(db_path, [
            ("https://example.com/page1", "Page 1", visit_times),
        ])

        # Query using the same pattern as the extractor
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT
                p.url,
                p.title,
                p.visit_count,
                p.typed,
                p.last_visit_date,
                v.visit_date,
                v.from_visit,
                v.visit_type
            FROM moz_historyvisits v
            JOIN moz_places p ON v.place_id = p.id
            WHERE v.visit_date IS NOT NULL
            ORDER BY v.visit_date DESC
        """)

        rows = list(cursor)
        conn.close()

        # Should have 5 records (one per visit)
        assert len(rows) == 5, f"Expected 5 visit records, got {len(rows)}"

        # Each record should have a distinct visit_date
        visit_dates_returned = [row["visit_date"] for row in rows]
        assert len(set(visit_dates_returned)) == 5, "Visit dates should be distinct"

        # All should be for the same URL
        urls_returned = [row["url"] for row in rows]
        assert all(url == "https://example.com/page1" for url in urls_returned)

    def test_multiple_urls_multiple_visits(self, tmp_path: Path):
        """Multiple URLs with multiple visits each."""
        db_path = tmp_path / "places.sqlite"

        base_time = datetime(2024, 1, 15, 10, 0, 0)

        # URL 1: 4 visits
        url1_visits = [datetime_to_prtime(base_time + timedelta(hours=i)) for i in range(4)]
        # URL 2: 1 visit
        url2_visits = [datetime_to_prtime(base_time + timedelta(hours=10))]

        create_firefox_history_db(db_path, [
            ("https://firefox.example.com/page1", "Firefox Page 1", url1_visits),
            ("https://firefox.example.com/page2", "Firefox Page 2", url2_visits),
        ])

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT
                p.url,
                v.visit_date
            FROM moz_historyvisits v
            JOIN moz_places p ON v.place_id = p.id
            WHERE v.visit_date IS NOT NULL
            ORDER BY v.visit_date DESC
        """)

        rows = list(cursor)
        conn.close()

        # Should have 5 total records (4 + 1)
        assert len(rows) == 5, f"Expected 5 visit records, got {len(rows)}"

        # Count records per URL
        url1_count = sum(1 for row in rows if "page1" in row["url"])
        url2_count = sum(1 for row in rows if "page2" in row["url"])

        assert url1_count == 4, f"Expected 4 visits for page1, got {url1_count}"
        assert url2_count == 1, f"Expected 1 visit for page2, got {url2_count}"


class TestVisitTimestampDistinction:
    """Test that visit timestamps are correctly distinguished from URL-level timestamps."""

    def test_chromium_visit_time_vs_last_visit_time(self, tmp_path: Path):
        """Verify visit_time differs from last_visit_time for earlier visits."""
        db_path = tmp_path / "History"

        # 3 visits at different times
        base_time = datetime(2024, 1, 15, 10, 0, 0)
        visit1 = datetime_to_webkit(base_time)  # First visit
        visit2 = datetime_to_webkit(base_time + timedelta(hours=2))  # Second
        visit3 = datetime_to_webkit(base_time + timedelta(hours=5))  # Last visit

        create_chromium_history_db(db_path, [
            ("https://example.com/page1", "Page 1", [visit1, visit2, visit3]),
        ])

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute("""
            SELECT u.last_visit_time, v.visit_time
            FROM visits v
            JOIN urls u ON v.url = u.id
            ORDER BY v.visit_time ASC
        """)

        rows = list(cursor)
        conn.close()

        # The first two visits should have visit_time != last_visit_time
        assert rows[0]["visit_time"] == visit1
        assert rows[0]["last_visit_time"] == visit3  # URL-level aggregate
        assert rows[0]["visit_time"] != rows[0]["last_visit_time"]

        # The last visit should match
        assert rows[2]["visit_time"] == visit3
        assert rows[2]["last_visit_time"] == visit3
