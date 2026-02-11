"""Tests for Timeline Report Modules.

Tests for:
- activity_summary: System activity overview with gaps detection
- url_activity_timeline: URL/domain activity time ranges
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def evidence_db(tmp_path: Path) -> sqlite3.Connection:
    """Create an in-memory evidence database with timeline data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create minimal schema for timeline
    conn.executescript("""
        CREATE TABLE timeline (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            ts_utc TEXT,
            kind TEXT,
            ref_table TEXT,
            ref_id INTEGER,
            confidence TEXT,
            note TEXT,
            run_id TEXT
        );

        CREATE TABLE browser_history (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            url TEXT,
            title TEXT,
            ts_utc TEXT,
            browser TEXT,
            profile TEXT
        );

        CREATE TABLE urls (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            url TEXT,
            first_seen_utc TEXT,
            discovered_by TEXT
        );

        CREATE TABLE tags (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            color TEXT
        );

        CREATE TABLE tag_associations (
            id INTEGER PRIMARY KEY,
            tag_id INTEGER,
            artifact_type TEXT,
            artifact_id INTEGER
        );

        CREATE TABLE url_matches (
            id INTEGER PRIMARY KEY,
            url_id INTEGER,
            list_name TEXT
        );
    """)

    return conn


@pytest.fixture
def populated_db(evidence_db: sqlite3.Connection) -> sqlite3.Connection:
    """Populate the evidence database with test timeline data."""
    evidence_id = 1

    # Insert browser history records
    browser_data = [
        (1, evidence_id, "https://example.com/page1", "Example Page", "2024-01-15T10:30:00", "chrome", "Default"),
        (2, evidence_id, "https://example.com/page2", "Example Page 2", "2024-01-15T14:00:00", "chrome", "Default"),
        (3, evidence_id, "https://test.org/index", "Test Site", "2024-01-16T09:00:00", "firefox", "default"),
        (4, evidence_id, "https://example.com/page3", "Example Page 3", "2024-01-17T22:30:00", "chrome", "Default"),
        (5, evidence_id, "https://casino-site.com/games", "Casino Games", "2024-01-18T23:00:00", "chrome", "Default"),
        (6, evidence_id, "https://casino-site.com/slots", "Slots", "2024-01-18T23:30:00", "chrome", "Default"),
    ]
    evidence_db.executemany(
        "INSERT INTO browser_history (id, evidence_id, url, title, ts_utc, browser, profile) VALUES (?, ?, ?, ?, ?, ?, ?)",
        browser_data,
    )

    # Insert timeline events (matching browser_history)
    timeline_data = [
        (1, evidence_id, "2024-01-15T10:30:00", "browser_visit", "browser_history", 1, "high", "chrome visit: Example Page"),
        (2, evidence_id, "2024-01-15T14:00:00", "browser_visit", "browser_history", 2, "high", "chrome visit: Example Page 2"),
        (3, evidence_id, "2024-01-16T09:00:00", "browser_visit", "browser_history", 3, "high", "firefox visit: Test Site"),
        (4, evidence_id, "2024-01-17T22:30:00", "browser_visit", "browser_history", 4, "high", "chrome visit: Example Page 3"),
        (5, evidence_id, "2024-01-18T23:00:00", "browser_visit", "browser_history", 5, "high", "chrome visit: Casino Games"),
        (6, evidence_id, "2024-01-18T23:30:00", "browser_visit", "browser_history", 6, "high", "chrome visit: Slots"),
        # Add some non-browser events
        (7, evidence_id, "2024-01-15T11:00:00", "download_started", "browser_downloads", 1, "high", "Download started: file.zip"),
        (8, evidence_id, "2024-01-15T11:05:00", "download_completed", "browser_downloads", 1, "high", "Download completed: file.zip"),
        (9, evidence_id, "2024-01-16T15:00:00", "cookie_created", "cookies", 1, "medium", "Cookie created: example.com"),
    ]
    evidence_db.executemany(
        "INSERT INTO timeline (id, evidence_id, ts_utc, kind, ref_table, ref_id, confidence, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        timeline_data,
    )

    # Add tags
    evidence_db.execute("INSERT INTO tags (id, name, color) VALUES (1, 'important', '#ff0000')")
    evidence_db.execute("INSERT INTO tag_associations (tag_id, artifact_type, artifact_id) VALUES (1, 'url', 1)")

    evidence_db.commit()
    return evidence_db


@pytest.fixture
def db_with_gaps(evidence_db: sqlite3.Connection) -> sqlite3.Connection:
    """Create database with significant gaps in activity."""
    evidence_id = 1

    # Events with 48+ hour gaps
    timeline_data = [
        (1, evidence_id, "2024-01-10T10:00:00", "browser_visit", "browser_history", 1, "high", "Visit 1"),
        (2, evidence_id, "2024-01-10T12:00:00", "browser_visit", "browser_history", 2, "high", "Visit 2"),
        # 48 hour gap
        (3, evidence_id, "2024-01-12T12:00:00", "browser_visit", "browser_history", 3, "high", "Visit 3"),
        (4, evidence_id, "2024-01-12T14:00:00", "browser_visit", "browser_history", 4, "high", "Visit 4"),
        # 72 hour gap
        (5, evidence_id, "2024-01-15T14:00:00", "browser_visit", "browser_history", 5, "high", "Visit 5"),
    ]
    evidence_db.executemany(
        "INSERT INTO timeline (id, evidence_id, ts_utc, kind, ref_table, ref_id, confidence, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        timeline_data,
    )
    evidence_db.commit()
    return evidence_db


# =============================================================================
# Activity Summary Module Tests
# =============================================================================


class TestActivitySummaryModule:
    """Tests for the ActivitySummaryModule."""

    def test_module_metadata(self):
        """Test module metadata is correctly defined."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        meta = module.metadata

        assert meta.module_id == "activity_summary"
        assert meta.name == "Activity Summary"
        assert meta.category == "Timeline"
        assert meta.icon == "📊"

    def test_filter_fields(self):
        """Test filter fields are correctly defined."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        fields = module.get_filter_fields()

        assert len(fields) >= 5
        field_keys = [f.key for f in fields]
        assert "event_group" in field_keys
        assert "min_gap_hours" in field_keys
        assert "show_daily_breakdown" in field_keys
        assert "confidence_filter" in field_keys

    def test_calculate_stats_empty(self):
        """Test stats calculation with no events."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        stats = module._calculate_stats([], "eu")

        assert stats["total_events"] == 0
        assert stats["earliest"] is None
        assert stats["latest"] is None

    def test_calculate_stats_with_events(self):
        """Test stats calculation with events."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        events = [
            {"ts_utc": "2024-01-15T10:00:00"},
            {"ts_utc": "2024-01-16T12:00:00"},
            {"ts_utc": "2024-01-17T14:00:00"},
        ]
        stats = module._calculate_stats(events, "eu")

        assert stats["total_events"] == 3
        # European date format: DD.MM.YYYY HH:MM:SS
        assert stats["earliest"] == "15.01.2024 10:00:00"
        assert stats["latest"] == "17.01.2024 14:00:00"
        assert stats["span_days"] == 3

    def test_find_gaps(self):
        """Test gap detection algorithm."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        events = [
            {"ts_utc": "2024-01-10T10:00:00"},
            {"ts_utc": "2024-01-10T12:00:00"},
            {"ts_utc": "2024-01-12T12:00:00"},  # 48h gap
            {"ts_utc": "2024-01-15T12:00:00"},  # 72h gap
        ]
        t = {"hours": "hours", "days": "days", "weeks": "weeks"}

        gaps = module._find_gaps(events, min_gap_hours=24, date_format="eu", t=t)

        assert len(gaps) == 2
        # Sorted by duration descending
        assert gaps[0]["duration_hours"] == 72.0
        assert gaps[1]["duration_hours"] == 48.0

    def test_find_gaps_no_significant_gaps(self):
        """Test gap detection when no significant gaps exist."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        events = [
            {"ts_utc": "2024-01-10T10:00:00"},
            {"ts_utc": "2024-01-10T12:00:00"},
            {"ts_utc": "2024-01-10T14:00:00"},
        ]
        t = {"hours": "hours", "days": "days", "weeks": "weeks"}

        gaps = module._find_gaps(events, min_gap_hours=24, date_format="eu", t=t)
        assert len(gaps) == 0

    def test_daily_counts(self):
        """Test daily count calculation."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        events = [
            {"ts_utc": "2024-01-15T10:00:00"},
            {"ts_utc": "2024-01-15T12:00:00"},
            {"ts_utc": "2024-01-15T14:00:00"},
            {"ts_utc": "2024-01-16T10:00:00"},
        ]

        daily = module._calculate_daily_counts(events, max_days="60", date_format="eu")

        assert len(daily) == 2
        # European date format: DD.MM.YYYY
        assert daily[0]["date"] == "15.01.2024"
        assert daily[0]["count"] == 3
        assert daily[1]["date"] == "16.01.2024"
        assert daily[1]["count"] == 1

    def test_event_breakdown(self):
        """Test event type breakdown calculation."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        events = [
            {"kind": "browser_visit"},
            {"kind": "browser_visit"},
            {"kind": "download_started"},
            {"kind": "cookie_created"},
        ]

        breakdown = module._calculate_event_breakdown(events)

        assert "Browser Visit" in breakdown
        assert breakdown["Browser Visit"] == 2
        assert breakdown["Download Started"] == 1

    def test_format_duration(self):
        """Test duration formatting."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        t = {"hours": "hours", "days": "days", "weeks": "weeks"}

        assert module._format_duration(12.5, t) == "12.5 hours"
        assert module._format_duration(36, t) == "1.5 days"
        assert module._format_duration(168, t) == "1.0 weeks"

    def test_render_with_data(self, populated_db: sqlite3.Connection):
        """Test rendering with actual data."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        config = {
            "event_group": "all",
            "min_gap_hours": 24,
            "show_daily_breakdown": True,
            "max_days_shown": "60",
            "show_event_breakdown": True,
            "confidence_filter": "all",
        }

        html = module.render(populated_db, evidence_id=1, config=config)

        assert "Activity Overview" in html
        assert "Total Events" in html
        assert "Daily Activity" in html

    def test_render_empty_database(self, evidence_db: sqlite3.Connection):
        """Test rendering with empty database."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        config = {"event_group": "all", "min_gap_hours": 24}

        html = module.render(evidence_db, evidence_id=1, config=config)

        assert "No timeline events found" in html

    def test_render_with_gaps(self, db_with_gaps: sqlite3.Connection):
        """Test rendering shows detected gaps."""
        from reports.modules.activity_summary.module import ActivitySummaryModule

        module = ActivitySummaryModule()
        config = {
            "event_group": "all",
            "min_gap_hours": 24,
            "show_daily_breakdown": True,
            "max_days_shown": "60",
            "show_event_breakdown": True,
            "confidence_filter": "all",
        }

        html = module.render(db_with_gaps, evidence_id=1, config=config)

        assert "Inactivity Gaps" in html


# =============================================================================
# URL Activity Timeline Module Tests
# =============================================================================


class TestUrlActivityTimelineModule:
    """Tests for the UrlActivityTimelineModule."""

    def test_module_metadata(self):
        """Test module metadata is correctly defined."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule

        module = UrlActivityTimelineModule()
        meta = module.metadata

        assert meta.module_id == "url_activity_timeline"
        assert meta.name == "URL Activity Timeline"
        assert meta.category == "Timeline"
        assert meta.icon == "🌐"

    def test_filter_fields(self):
        """Test filter fields are correctly defined."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule

        module = UrlActivityTimelineModule()
        fields = module.get_filter_fields()

        assert len(fields) >= 7
        field_keys = [f.key for f in fields]
        assert "tag_filter" in field_keys
        assert "match_filter" in field_keys
        assert "domain_filter" in field_keys
        assert "min_occurrences" in field_keys
        assert "show_urls" in field_keys
        assert "sort_by" in field_keys

    def test_extract_domain(self):
        """Test domain extraction from URLs."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule

        module = UrlActivityTimelineModule()

        assert module._extract_domain("https://example.com/page") == "example.com"
        assert module._extract_domain("http://sub.domain.org:8080/path") == "sub.domain.org:8080"
        assert module._extract_domain("invalid-url") == "invalid-url"

    def test_aggregate_by_domain(self):
        """Test domain aggregation."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule

        module = UrlActivityTimelineModule()
        events = [
            {"url": "https://example.com/page1", "ts_utc": "2024-01-15T10:00:00"},
            {"url": "https://example.com/page2", "ts_utc": "2024-01-15T14:00:00"},
            {"url": "https://test.org/index", "ts_utc": "2024-01-16T09:00:00"},
        ]

        domains = module._aggregate_by_domain(events, min_occurrences=1, date_format="eu")

        assert len(domains) == 2
        example_domain = next(d for d in domains if d["domain"] == "example.com")
        assert example_domain["count"] == 2
        # European date format: DD.MM.YYYY HH:MM:SS
        assert "15.01.2024" in example_domain["first_seen"]
        assert "15.01.2024" in example_domain["last_seen"]

    def test_aggregate_by_domain_min_occurrences(self):
        """Test domain aggregation with minimum occurrence filter."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule

        module = UrlActivityTimelineModule()
        events = [
            {"url": "https://example.com/page1", "ts_utc": "2024-01-15T10:00:00"},
            {"url": "https://example.com/page2", "ts_utc": "2024-01-15T14:00:00"},
            {"url": "https://test.org/index", "ts_utc": "2024-01-16T09:00:00"},
        ]

        domains = module._aggregate_by_domain(events, min_occurrences=2, date_format="eu")

        assert len(domains) == 1
        assert domains[0]["domain"] == "example.com"

    def test_sort_domains(self):
        """Test domain sorting."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule

        module = UrlActivityTimelineModule()
        domains = [
            {"domain": "z-site.com", "count": 5, "first_seen": "2024-01-10", "last_seen": "2024-01-15"},
            {"domain": "a-site.com", "count": 10, "first_seen": "2024-01-05", "last_seen": "2024-01-20"},
        ]

        # Sort by count descending
        sorted_domains = module._sort_domains(domains, "count_desc")
        assert sorted_domains[0]["domain"] == "a-site.com"

        # Sort by domain name
        sorted_domains = module._sort_domains(domains, "domain_asc")
        assert sorted_domains[0]["domain"] == "a-site.com"

    def test_build_filter_description(self):
        """Test filter description generation."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule

        module = UrlActivityTimelineModule()

        desc = module._build_filter_description(
            tag_filter="any_tag",
            match_filter="all",
            domain_filter="casino",
            min_occurrences=5,
        )

        assert "tagged URLs" in desc
        assert "domain contains: casino" in desc
        assert "≥5 occurrences" in desc

    def test_render_with_data(self, populated_db: sqlite3.Connection):
        """Test rendering with actual data."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule

        module = UrlActivityTimelineModule()
        config = {
            "tag_filter": "all",
            "match_filter": "all",
            "domain_filter": "",
            "min_occurrences": 1,
            "show_urls": True,
            "urls_per_domain": "5",
            "sort_by": "count_desc",
            "max_domains": "50",
        }

        html = module.render(populated_db, evidence_id=1, config=config)

        assert "URL Activity Summary" in html
        assert "Domain Activity" in html

    def test_render_with_domain_filter(self, populated_db: sqlite3.Connection):
        """Test rendering with domain filter."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule

        module = UrlActivityTimelineModule()
        config = {
            "tag_filter": "all",
            "match_filter": "all",
            "domain_filter": "casino",
            "min_occurrences": 1,
            "show_urls": False,
            "sort_by": "count_desc",
            "max_domains": "50",
        }

        html = module.render(populated_db, evidence_id=1, config=config)

        assert "casino-site.com" in html or "domain contains: casino" in html

    def test_render_empty_database(self, evidence_db: sqlite3.Connection):
        """Test rendering with empty database."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule

        module = UrlActivityTimelineModule()
        config = {
            "tag_filter": "all",
            "match_filter": "all",
            "domain_filter": "",
            "min_occurrences": 1,
        }

        html = module.render(evidence_db, evidence_id=1, config=config)

        assert "No URL events found" in html

    def test_add_url_details(self):
        """Test URL detail addition to domains."""
        from reports.modules.url_activity_timeline.module import UrlActivityTimelineModule
        from collections import defaultdict

        module = UrlActivityTimelineModule()
        domains = [{
            "domain": "example.com",
            "urls": defaultdict(int, {
                "https://example.com/page1": 5,
                "https://example.com/page2": 3,
                "https://example.com/page3": 1,
            }),
        }]
        events = []  # Not used in current implementation

        module._add_url_details(domains, events, urls_per_domain="2")

        assert len(domains[0]["url_details"]) == 2
        assert domains[0]["url_details"][0]["url"] == "https://example.com/page1"
        assert domains[0]["hidden_url_count"] == 1


# =============================================================================
# Module Registration Tests
# =============================================================================


class TestModuleRegistration:
    """Tests for module auto-discovery and registration."""

    def test_activity_summary_registered(self):
        """Test activity_summary module is auto-discovered."""
        from reports.modules.registry import ModuleRegistry

        # Force re-discovery
        registry = ModuleRegistry()
        registry.reload()

        assert registry.is_registered("activity_summary")
        module = registry.get_module("activity_summary")
        assert module is not None
        assert module.metadata.name == "Activity Summary"

    def test_url_activity_timeline_registered(self):
        """Test url_activity_timeline module is auto-discovered."""
        from reports.modules.registry import ModuleRegistry

        registry = ModuleRegistry()
        registry.reload()

        assert registry.is_registered("url_activity_timeline")
        module = registry.get_module("url_activity_timeline")
        assert module is not None
        assert module.metadata.name == "URL Activity Timeline"

    def test_both_in_timeline_category(self):
        """Test both modules are in the Timeline category."""
        from reports.modules.registry import ModuleRegistry

        registry = ModuleRegistry()
        registry.reload()

        by_category = registry.list_modules_by_category()

        assert "Timeline" in by_category
        timeline_modules = by_category["Timeline"]
        module_ids = [m.module_id for m in timeline_modules]

        assert "activity_summary" in module_ids
        assert "url_activity_timeline" in module_ids
