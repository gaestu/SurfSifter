"""
Tests for timeline engine: event mapping, fusion, and querying.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
import tempfile
from types import SimpleNamespace
import pytest

# Import modules directly to avoid circular import issues
import core.database as db_module
from core.database import DatabaseManager
from app.features.timeline.engine import (
    _parse_timestamp,
    _format_note,
    _unix_to_datetime,
    TimelineEvent,
    build_timeline,
    persist_timeline,
    coalesce_events,
    TIMELINE_MAPPERS,
    map_browser_history_to_events,
    map_urls_to_events,
    map_images_to_events,
    map_os_indicators_to_events,
    map_cookies_to_events,
    map_bookmarks_to_events,
    map_browser_downloads_to_events,
    map_session_tabs_to_events,
    map_autofill_to_events,
    map_credentials_to_events,
    map_media_playback_to_events,
    map_hsts_to_events,
    map_jump_list_to_events,
)
from app.features.timeline.config import DEFAULT_TIMELINE_CONFIG, TimelineConfig, load_timeline_config

timelines_module = SimpleNamespace(
    load_timeline_config=load_timeline_config,
    TimelineConfig=TimelineConfig,
    TimelineEvent=TimelineEvent,
    build_timeline=build_timeline,
    persist_timeline=persist_timeline,
    coalesce_events=coalesce_events,
    TIMELINE_MAPPERS=TIMELINE_MAPPERS,
    map_browser_history_to_events=map_browser_history_to_events,
    map_urls_to_events=map_urls_to_events,
    map_images_to_events=map_images_to_events,
    map_os_indicators_to_events=map_os_indicators_to_events,
    map_cookies_to_events=map_cookies_to_events,
    map_bookmarks_to_events=map_bookmarks_to_events,
    map_browser_downloads_to_events=map_browser_downloads_to_events,
    map_session_tabs_to_events=map_session_tabs_to_events,
    map_autofill_to_events=map_autofill_to_events,
    map_credentials_to_events=map_credentials_to_events,
    map_media_playback_to_events=map_media_playback_to_events,
    map_hsts_to_events=map_hsts_to_events,
    map_jump_list_to_events=map_jump_list_to_events,
)


@pytest.fixture
def temp_case_db():
    """Create a temporary case database for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        case_path = Path(tmpdir)
        case_db_path = case_path / "test_surfsifter.sqlite"
        manager = DatabaseManager(case_path, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()
        evidence_conn = manager.get_evidence_conn(evidence_id=1, label="EV-001")

        # Create a case and evidence
        with case_conn:
            case_conn.execute(
                """
                INSERT INTO cases(case_id, title, investigator, created_at_utc)
                VALUES ('TEST-001', 'Timeline Test', 'Agent', ?)
                """,
                (datetime.now(tz=timezone.utc).isoformat(),)
            )
            case_conn.execute(
                """
                INSERT INTO evidences(case_id, label, source_path, added_at_utc)
                VALUES (1, 'EV-001', '/test/image.e01', ?)
                """,
                (datetime.now(tz=timezone.utc).isoformat(),)
            )

        yield evidence_conn, 1  # evidence_conn, evidence_id = 1


@pytest.fixture
def timeline_config():
    """Load timeline configuration (hardcoded since )."""
    return timelines_module.load_timeline_config()


def test_load_timeline_config(timeline_config):
    """Test that timeline config loads correctly."""
    assert timeline_config is not None
    assert "browser_history" in timeline_config.sources
    assert "urls" in timeline_config.sources
    assert "images" in timeline_config.sources
    assert "os_indicators" in timeline_config.sources

    assert timeline_config.confidence_weights["high"] == 1.0
    assert timeline_config.confidence_weights["medium"] == 0.7
    assert timeline_config.confidence_weights["low"] == 0.4

    assert timeline_config.cluster_window_seconds == 300
    assert timeline_config.min_confidence == 0.3


def test_default_config_fallback():
    """Test that config is returned even with invalid path (always returns hardcoded config)."""
    fake_rules_dir = Path("/nonexistent")
    config = timelines_module.load_timeline_config(fake_rules_dir)

    assert config is not None
    assert "browser_history" in config.sources
    assert config.confidence_weights["high"] == 1.0


def test_parse_timestamp():
    """Test timestamp parsing utility."""
    # ISO-8601 with timezone
    ts1 = _parse_timestamp("2025-01-15T14:30:00+00:00")
    assert ts1 is not None
    assert ts1.tzinfo is not None

    # ISO-8601 with Z
    ts2 = _parse_timestamp("2025-01-15T14:30:00Z")
    assert ts2 is not None

    # Without timezone (should assume UTC)
    ts3 = _parse_timestamp("2025-01-15T14:30:00")
    assert ts3 is not None
    assert ts3.tzinfo == timezone.utc

    # Invalid
    ts_invalid = _parse_timestamp("not-a-date")
    assert ts_invalid is None

    # None
    ts_none = _parse_timestamp(None)
    assert ts_none is None


def test_format_note():
    """Test note template formatting."""
    template = "Browser: {browser}, URL: {url}"
    context = {"browser": "Chrome", "url": "https://example.com"}

    note = _format_note(template, context)
    assert "Chrome" in note
    assert "https://example.com" in note

    # Missing key should fallback gracefully
    incomplete_context = {"browser": "Firefox"}
    note2 = _format_note(template, incomplete_context)
    assert note2 is not None  # Should not crash


def test_map_browser_history_to_events(temp_case_db, timeline_config):
    """Test mapping browser_history to timeline events."""
    conn, evidence_id = temp_case_db

    # Insert test browser history
    now = datetime.now(tz=timezone.utc)
    with conn:
        conn.execute(
            """
            INSERT INTO browser_history(evidence_id, url, title, ts_utc, browser, profile)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "https://example.com", "Example Site", now.isoformat(), "Chrome", "Default")
        )
        conn.execute(
            """
            INSERT INTO browser_history(evidence_id, url, title, ts_utc, browser, profile)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "https://test.com", "Test Site", (now + timedelta(minutes=5)).isoformat(), "Firefox", "Profile1")
        )

    events = timelines_module.map_browser_history_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 2
    assert events[0].kind == "browser_visit"
    assert events[0].ref_table == "browser_history"
    assert events[0].confidence == "high"
    assert "Chrome" in events[0].note or "Example" in events[0].note
    assert events[0].provenance == "browser:Chrome"


def test_map_urls_to_events(temp_case_db, timeline_config):
    """Test mapping urls to timeline events."""
    conn, evidence_id = temp_case_db

    now = datetime.now(tz=timezone.utc)
    with conn:
        conn.execute(
            """
            INSERT INTO urls(evidence_id, url, domain, scheme, discovered_by, first_seen_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "https://gambling.com", "gambling.com", "https", "bulk_extractor", now.isoformat())
        )

    events = timelines_module.map_urls_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 1
    assert events[0].kind == "url_discovered"
    assert events[0].ref_table == "urls"
    assert events[0].confidence == "medium"
    assert events[0].provenance == "discovered_by:bulk_extractor"


def test_map_images_to_events(temp_case_db, timeline_config):
    """Test mapping images to timeline events."""
    conn, evidence_id = temp_case_db

    now = datetime.now(tz=timezone.utc)
    with conn:
        # images table now uses first_discovered_by instead of discovered_by
        conn.execute(
            """
            INSERT INTO images(evidence_id, rel_path, filename, first_discovered_by, ts_utc, md5, sha256)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "carved/img001.jpg", "img001.jpg", "foremost", now.isoformat(), "abc123", "def456")
        )

    events = timelines_module.map_images_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 1
    assert events[0].kind == "image_extracted"
    assert events[0].ref_table == "images"
    assert events[0].confidence == "medium"
    assert "img001.jpg" in events[0].note


def test_map_os_indicators_to_events(temp_case_db, timeline_config):
    """Test mapping os_indicators to timeline events."""
    conn, evidence_id = temp_case_db

    now = datetime.now(tz=timezone.utc)
    with conn:
        conn.execute(
            """
            INSERT INTO os_indicators(evidence_id, type, name, value, confidence, detected_at_utc, provenance)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "registry", "ComputerName", "TEST-PC", "high", now.isoformat(), "registry:SYSTEM")
        )

    events = timelines_module.map_os_indicators_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 1
    assert events[0].kind == "os_artifact"
    assert events[0].ref_table == "os_indicators"
    # Should use indicator's own confidence
    assert events[0].confidence == "high"
    assert events[0].provenance == "registry:SYSTEM"


def test_build_timeline_integration(temp_case_db, timeline_config):
    """Test complete timeline building from multiple sources."""
    conn, evidence_id = temp_case_db

    base_time = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    # Insert data across all sources
    with conn:
        # Browser history
        conn.execute(
            """
            INSERT INTO browser_history(evidence_id, url, title, ts_utc, browser, profile)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "https://site1.com", "Site 1", base_time.isoformat(), "Chrome", "Default")
        )

        # URLs
        conn.execute(
            """
            INSERT INTO urls(evidence_id, url, domain, discovered_by, first_seen_utc)
            VALUES (?, ?, ?, ?, ?)
            """,
            (evidence_id, "https://site2.com", "site2.com", "bulk_extractor", (base_time + timedelta(minutes=10)).isoformat())
        )

        # Images - use first_discovered_by
        conn.execute(
            """
            INSERT INTO images(evidence_id, rel_path, filename, first_discovered_by, ts_utc, md5, sha256)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "img1.jpg", "img1.jpg", "cache_parser", (base_time + timedelta(minutes=20)).isoformat(), "md5", "sha256")
        )

        # OS indicators
        conn.execute(
            """
            INSERT INTO os_indicators(evidence_id, type, name, value, confidence, detected_at_utc, provenance)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "registry", "Test", "value", "low", (base_time + timedelta(minutes=30)).isoformat(), "test")
        )

    # Build timeline
    events = timelines_module.build_timeline(conn, evidence_id, timeline_config)

    # Should have 4 events, sorted by time
    assert len(events) == 4

    # Check deterministic ordering
    assert events[0].ref_table == "browser_history"
    assert events[1].ref_table == "urls"
    assert events[2].ref_table == "images"
    assert events[3].ref_table == "os_indicators"

    # Verify timestamps are ascending
    for i in range(len(events) - 1):
        assert events[i].ts_utc <= events[i + 1].ts_utc


def test_persist_timeline(temp_case_db, timeline_config):
    """Test persisting timeline events to database."""
    conn, evidence_id = temp_case_db

    base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    # Create some events manually
    events = [
        timelines_module.TimelineEvent(
            evidence_id=evidence_id,
            ts_utc=base_time,
            kind="test_event",
            ref_table="test_table",
            ref_id=1,
            confidence="high",
            note="Test event 1"
        ),
        timelines_module.TimelineEvent(
            evidence_id=evidence_id,
            ts_utc=base_time + timedelta(seconds=30),
            kind="test_event",
            ref_table="test_table",
            ref_id=2,
            confidence="medium",
            note="Test event 2"
        )
    ]

    # Persist
    count = timelines_module.persist_timeline(conn, events)
    assert count == 2

    # Verify in database
    rows = conn.execute(
        "SELECT * FROM timeline WHERE evidence_id = ? ORDER BY ts_utc",
        (evidence_id,)
    ).fetchall()

    assert len(rows) == 2
    assert rows[0]["kind"] == "test_event"
    assert rows[0]["confidence"] == "high"
    assert rows[1]["confidence"] == "medium"


def test_iter_timeline(temp_case_db):
    """Test timeline retrieval with filtering and paging."""
    conn, evidence_id = temp_case_db

    base_time = datetime(2025, 1, 15, 14, 0, 0, tzinfo=timezone.utc)

    # Insert multiple timeline events
    with conn:
        for i in range(10):
            conn.execute(
                """
                INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence, note)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evidence_id,
                    (base_time + timedelta(minutes=i)).isoformat(),
                    "browser_visit" if i % 2 == 0 else "url_discovered",
                    "browser_history" if i % 2 == 0 else "urls",
                    i,
                    "high" if i < 5 else "medium",
                    f"Event {i}"
                )
            )

    # Test basic retrieval
    events = db_module.iter_timeline(conn, evidence_id, page=1, page_size=5)
    assert len(events) == 5

    # Test paging
    events_page2 = db_module.iter_timeline(conn, evidence_id, page=2, page_size=5)
    assert len(events_page2) == 5
    assert events_page2[0]["note"] != events[0]["note"]

    # Test filtering by kind
    browser_events = db_module.iter_timeline(conn, evidence_id, filters={"kind": "browser_visit"})
    assert all(e["kind"] == "browser_visit" for e in browser_events)
    assert len(browser_events) == 5

    # Test filtering by confidence
    high_conf = db_module.iter_timeline(conn, evidence_id, filters={"confidence": "high"})
    assert all(e["confidence"] == "high" for e in high_conf)
    assert len(high_conf) == 5

    # Test date range filtering
    mid_time = (base_time + timedelta(minutes=5)).isoformat()
    recent_events = db_module.iter_timeline(conn, evidence_id, filters={"start_date": mid_time})
    assert len(recent_events) == 5  # Events 5-9


def test_get_timeline_stats(temp_case_db):
    """Test timeline statistics retrieval."""
    conn, evidence_id = temp_case_db

    base_time = datetime(2025, 1, 15, 16, 0, 0, tzinfo=timezone.utc)

    # Insert varied events
    with conn:
        conn.execute(
            """
            INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, base_time.isoformat(), "browser_visit", "browser_history", 1, "high", "Event 1")
        )
        conn.execute(
            """
            INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, (base_time + timedelta(minutes=5)).isoformat(), "browser_visit", "browser_history", 2, "high", "Event 2")
        )
        conn.execute(
            """
            INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, (base_time + timedelta(minutes=10)).isoformat(), "url_discovered", "urls", 1, "medium", "Event 3")
        )

    stats = db_module.get_timeline_stats(conn, evidence_id)

    assert stats["total_events"] == 3
    assert stats["by_kind"]["browser_visit"] == 2
    assert stats["by_kind"]["url_discovered"] == 1
    assert stats["by_confidence"]["high"] == 2
    assert stats["by_confidence"]["medium"] == 1
    assert stats["by_source"]["browser_history"] == 2
    assert stats["by_source"]["urls"] == 1
    assert stats["earliest"] is not None
    assert stats["latest"] is not None


def test_get_timeline_kinds(temp_case_db):
    """Test retrieval of distinct timeline kinds."""
    conn, evidence_id = temp_case_db

    base_time = datetime.now(tz=timezone.utc)

    with conn:
        conn.execute(
            "INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence) VALUES (?, ?, ?, ?, ?, ?)",
            (evidence_id, base_time.isoformat(), "browser_visit", "browser_history", 1, "high")
        )
        conn.execute(
            "INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence) VALUES (?, ?, ?, ?, ?, ?)",
            (evidence_id, base_time.isoformat(), "url_discovered", "urls", 1, "medium")
        )
        conn.execute(
            "INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence) VALUES (?, ?, ?, ?, ?, ?)",
            (evidence_id, base_time.isoformat(), "image_extracted", "images", 1, "medium")
        )

    kinds = db_module.get_timeline_kinds(conn, evidence_id)
    assert kinds == ["browser_visit", "image_extracted", "url_discovered"]  # Sorted


def test_get_timeline_confidences(temp_case_db):
    """Test retrieval of distinct confidence levels (sorted correctly)."""
    conn, evidence_id = temp_case_db

    base_time = datetime.now(tz=timezone.utc)

    with conn:
        conn.execute(
            "INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence) VALUES (?, ?, ?, ?, ?, ?)",
            (evidence_id, base_time.isoformat(), "test", "test", 1, "low")
        )
        conn.execute(
            "INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence) VALUES (?, ?, ?, ?, ?, ?)",
            (evidence_id, base_time.isoformat(), "test", "test", 2, "high")
        )
        conn.execute(
            "INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence) VALUES (?, ?, ?, ?, ?, ?)",
            (evidence_id, base_time.isoformat(), "test", "test", 3, "medium")
        )

    confidences = db_module.get_timeline_confidences(conn, evidence_id)
    # Should be sorted high > medium > low
    assert confidences == ["high", "medium", "low"]


def test_coalesce_events_determinism():
    """Test that event coalescing is deterministic."""
    base_time = datetime(2025, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

    # Create events with same timestamp but different refs (for determinism test)
    events = [
        timelines_module.TimelineEvent(1, base_time, "kind1", "table_b", 2, "high", "Event B"),
        timelines_module.TimelineEvent(1, base_time, "kind1", "table_a", 1, "high", "Event A"),
        timelines_module.TimelineEvent(1, base_time, "kind1", "table_c", 3, "high", "Event C"),
    ]

    # Coalesce multiple times
    sorted1 = timelines_module.coalesce_events(events)
    sorted2 = timelines_module.coalesce_events(events[::-1])  # Reverse input
    sorted3 = timelines_module.coalesce_events(events)

    # All should produce same order
    assert [e.ref_table for e in sorted1] == [e.ref_table for e in sorted2]
    assert [e.ref_table for e in sorted1] == [e.ref_table for e in sorted3]

    # Should be sorted by ref_table, then ref_id
    assert sorted1[0].ref_table == "table_a"
    assert sorted1[1].ref_table == "table_b"
    assert sorted1[2].ref_table == "table_c"


def test_empty_timeline(temp_case_db, timeline_config):
    """Test timeline building with no data."""
    conn, evidence_id = temp_case_db

    # Build timeline with no records
    events = timelines_module.build_timeline(conn, evidence_id, timeline_config)
    assert len(events) == 0

    # Stats should be empty
    stats = db_module.get_timeline_stats(conn, evidence_id)
    assert stats["total_events"] == 0
    assert stats["by_kind"] == {}


def test_timeline_with_null_timestamps(temp_case_db, timeline_config):
    """Test that records with NULL timestamps are skipped."""
    conn, evidence_id = temp_case_db

    with conn:
        # Insert browser history with NULL timestamp
        conn.execute(
            """
            INSERT INTO browser_history(evidence_id, url, title, ts_utc, browser, profile)
            VALUES (?, ?, ?, NULL, ?, ?)
            """,
            (evidence_id, "https://test.com", "Test", "Chrome", "Default")
        )

        # Insert one with valid timestamp
        conn.execute(
            """
            INSERT INTO browser_history(evidence_id, url, title, ts_utc, browser, profile)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "https://valid.com", "Valid", datetime.now(tz=timezone.utc).isoformat(), "Firefox", "Profile")
        )

    events = timelines_module.map_browser_history_to_events(conn, evidence_id, timeline_config)

    # Should only get the one with valid timestamp
    assert len(events) == 1
    assert "valid.com" in events[0].note or "Valid" in events[0].note


# =============================================================================
# Phase 5 Tests - Multi-mapping Config, New Mappers, Unix Conversion
# =============================================================================

def test_multi_mapping_config_parsing():
    """Test that list-of-mappings config schema is parsed correctly."""
    # Config is now hardcoded
    config = timelines_module.load_timeline_config()

    # cookies should have 2 mappings
    cookies_config = config.sources.get("cookies", {})
    mappings = cookies_config.get("mappings", [])
    assert isinstance(mappings, list)
    assert len(mappings) == 2

    # browser_downloads should have 2 mappings
    downloads_config = config.sources.get("browser_downloads", {})
    mappings = downloads_config.get("mappings", [])
    assert isinstance(mappings, list)
    assert len(mappings) == 2


def test_config_single_mapping_structure():
    """Test that single-mapping sources use list format consistently."""
    config = DEFAULT_TIMELINE_CONFIG

    # browser_history has single mapping but should be in list format
    bh_config = config.sources.get("browser_history", {})
    mappings = bh_config.get("mappings", [])
    assert isinstance(mappings, list)
    assert len(mappings) == 1
    assert mappings[0]["kind"] == "browser_visit"


def test_unix_to_datetime_conversion():
    """Test Unix timestamp to datetime conversion."""
    # Valid Unix timestamp (2025-01-15 10:00:00 UTC)
    unix_ts = 1736935200.0
    dt = _unix_to_datetime(unix_ts)
    assert dt is not None
    assert dt.year == 2025
    assert dt.tzinfo == timezone.utc

    # Zero should return None
    assert _unix_to_datetime(0) is None

    # Negative should return None
    assert _unix_to_datetime(-1000) is None

    # None should return None
    assert _unix_to_datetime(None) is None


def test_map_cookies_to_events(temp_case_db, timeline_config):
    """Test mapping cookies to timeline events (both creation and access)."""
    conn, evidence_id = temp_case_db

    now = datetime.now(tz=timezone.utc)
    access_time = now + timedelta(hours=2)

    with conn:
        conn.execute(
            """
            INSERT INTO cookies(evidence_id, browser, domain, name, path, creation_utc, last_access_utc, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "Chrome", "example.com", "session_id", "/", now.isoformat(), access_time.isoformat(), "run-001", "/path")
        )

    events = timelines_module.map_cookies_to_events(conn, evidence_id, timeline_config)

    # Should get 2 events: creation and access
    assert len(events) == 2

    kinds = {e.kind for e in events}
    assert "cookie_created" in kinds
    assert "cookie_accessed" in kinds

    # Check notes contain domain
    for event in events:
        assert "example.com" in event.note


def test_map_bookmarks_to_events(temp_case_db, timeline_config):
    """Test mapping bookmarks to timeline events."""
    conn, evidence_id = temp_case_db

    now = datetime.now(tz=timezone.utc)

    with conn:
        conn.execute(
            """
            INSERT INTO bookmarks(evidence_id, browser, url, title, folder_path, date_added_utc, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "Firefox", "https://docs.python.org", "Python Docs", "Bookmarks Bar", now.isoformat(), "run-001", "/path")
        )

    events = timelines_module.map_bookmarks_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 1
    assert events[0].kind == "bookmark_added"
    assert "Python Docs" in events[0].note
    assert events[0].confidence == "high"


def test_map_browser_downloads_to_events(temp_case_db, timeline_config):
    """Test mapping browser downloads to timeline events (start and end)."""
    conn, evidence_id = temp_case_db

    start_time = datetime.now(tz=timezone.utc)
    end_time = start_time + timedelta(minutes=5)

    with conn:
        conn.execute(
            """
            INSERT INTO browser_downloads(evidence_id, browser, url, filename, start_time_utc, end_time_utc, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "Chrome", "https://example.com/file.zip", "file.zip", start_time.isoformat(), end_time.isoformat(), "run-001", "/path")
        )

    events = timelines_module.map_browser_downloads_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 2
    kinds = {e.kind for e in events}
    assert "download_started" in kinds
    assert "download_completed" in kinds


def test_map_session_tabs_to_events(temp_case_db, timeline_config):
    """Test mapping session tabs to timeline events."""
    conn, evidence_id = temp_case_db

    now = datetime.now(tz=timezone.utc)

    with conn:
        conn.execute(
            """
            INSERT INTO session_tabs(evidence_id, browser, url, title, last_accessed_utc, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "Edge", "https://news.site.com", "Breaking News", now.isoformat(), "run-001", "/path")
        )

    events = timelines_module.map_session_tabs_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 1
    assert events[0].kind == "tab_accessed"
    assert "Breaking News" in events[0].note


def test_map_autofill_to_events(temp_case_db, timeline_config):
    """Test mapping autofill data to timeline events (created and used)."""
    conn, evidence_id = temp_case_db

    created_time = datetime.now(tz=timezone.utc)
    used_time = created_time + timedelta(days=7)

    with conn:
        conn.execute(
            """
            INSERT INTO autofill(evidence_id, browser, name, value, date_created_utc, date_last_used_utc, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "Chrome", "email", "user@example.com", created_time.isoformat(), used_time.isoformat(), "run-001", "/path")
        )

    events = timelines_module.map_autofill_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 2
    kinds = {e.kind for e in events}
    assert "autofill_created" in kinds
    assert "autofill_used" in kinds

    # Note should contain field name
    for event in events:
        assert "email" in event.note


def test_map_credentials_to_events(temp_case_db, timeline_config):
    """Test mapping credentials to timeline events (saved and used)."""
    conn, evidence_id = temp_case_db

    saved_time = datetime.now(tz=timezone.utc)
    used_time = saved_time + timedelta(days=3)

    with conn:
        conn.execute(
            """
            INSERT INTO credentials(evidence_id, browser, origin_url, username_value, date_created_utc, date_last_used_utc, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "Chrome", "https://accounts.google.com", "testuser", saved_time.isoformat(), used_time.isoformat(), "run-001", "/path")
        )

    events = timelines_module.map_credentials_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 2
    kinds = {e.kind for e in events}
    assert "credential_saved" in kinds
    assert "credential_used" in kinds
    assert events[0].confidence == "high"


def test_map_media_playback_to_events(temp_case_db, timeline_config):
    """Test mapping media playback to timeline events."""
    conn, evidence_id = temp_case_db

    now = datetime.now(tz=timezone.utc)

    with conn:
        conn.execute(
            """
            INSERT INTO media_playback(evidence_id, browser, url, origin, watch_time_seconds, last_played_utc, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "Chrome", "https://youtube.com/watch?v=abc", "youtube.com", 3600, now.isoformat(), "run-001", "/path")
        )

    events = timelines_module.map_media_playback_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 1
    assert events[0].kind == "media_played"
    assert "youtube.com" in events[0].note
    assert "3600" in events[0].note  # Watch time should be in note


def test_map_hsts_to_events(temp_case_db, timeline_config):
    """Test mapping HSTS entries with Unix timestamp conversion."""
    conn, evidence_id = temp_case_db

    # Unix timestamps (seconds since 1970)
    observed_ts = 1736935200.0  # 2025-01-15
    expiry_ts = 1768471200.0    # 2026-01-15

    with conn:
        conn.execute(
            """
            INSERT INTO hsts_entries(evidence_id, browser, hashed_host, decoded_host, sts_observed, expiry, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "Chrome", "BASE64HASH", "secure.example.com", observed_ts, expiry_ts, "run-001", "/path")
        )

    events = timelines_module.map_hsts_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 2
    kinds = {e.kind for e in events}
    assert "hsts_observed" in kinds
    assert "hsts_expiry" in kinds

    # Check that decoded_host is used (not hashed_host)
    for event in events:
        assert "secure.example.com" in event.note


def test_map_hsts_to_events_uses_hashed_host_fallback(temp_case_db, timeline_config):
    """Test that HSTS mapper falls back to hashed_host when decoded_host is NULL."""
    conn, evidence_id = temp_case_db

    with conn:
        conn.execute(
            """
            INSERT INTO hsts_entries(evidence_id, browser, hashed_host, decoded_host, sts_observed, expiry, run_id, source_path)
            VALUES (?, ?, ?, NULL, ?, NULL, ?, ?)
            """,
            (evidence_id, "Chrome", "BASE64HASH", 1736935200.0, "run-001", "/path")
        )

    events = timelines_module.map_hsts_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 1  # Only sts_observed (expiry is NULL)
    assert "BASE64HASH" in events[0].note


def test_map_jump_list_to_events(temp_case_db, timeline_config):
    """Test mapping jump list entries to timeline events."""
    conn, evidence_id = temp_case_db

    access_time = datetime.now(tz=timezone.utc)
    creation_time = access_time - timedelta(days=30)

    with conn:
        conn.execute(
            """
            INSERT INTO jump_list_entries(evidence_id, browser, url, target_path, appid, jumplist_path, lnk_access_time, lnk_creation_time, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "chrome", "https://visited.site.com", "C:\\path\\chrome.exe", "appid123", "/path/to/jumplist.automaticDestinations-ms", access_time.isoformat(), creation_time.isoformat(), "run-001", "/path")
        )

    events = timelines_module.map_jump_list_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 2
    kinds = {e.kind for e in events}
    assert "jumplist_accessed" in kinds
    assert "jumplist_created" in kinds


def test_timeline_mappers_registry():
    """Test that TIMELINE_MAPPERS registry contains all expected mappers."""
    expected = [
        "browser_history", "urls", "images", "os_indicators",
        "cookies", "bookmarks", "browser_downloads", "session_tabs",
        "autofill", "credentials", "media_playback",
        "hsts_entries", "jump_list_entries"
    ]

    for source in expected:
        assert source in timelines_module.TIMELINE_MAPPERS
        assert callable(timelines_module.TIMELINE_MAPPERS[source])


def test_build_timeline_with_progress_callback(temp_case_db, timeline_config):
    """Test that build_timeline invokes progress callback."""
    conn, evidence_id = temp_case_db

    # Insert minimal data
    now = datetime.now(tz=timezone.utc)
    with conn:
        conn.execute(
            """
            INSERT INTO browser_history(evidence_id, url, title, ts_utc, browser, profile)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "https://test.com", "Test", now.isoformat(), "Chrome", "Default")
        )

    progress_calls = []

    def progress_cb(pct, msg):
        progress_calls.append((pct, msg))

    events = timelines_module.build_timeline(conn, evidence_id, timeline_config, progress_cb=progress_cb)

    # Should have called progress at least once
    assert len(progress_calls) > 0
    # Should end at 1.0
    assert progress_calls[-1][0] == 1.0


def test_missing_table_graceful_skip(temp_case_db, timeline_config):
    """Test that mappers gracefully skip missing tables."""
    conn, evidence_id = temp_case_db

    # Drop the cookies table (simulate old schema)
    conn.execute("DROP TABLE IF EXISTS cookies")

    # Should not crash, just return empty
    events = timelines_module.map_cookies_to_events(conn, evidence_id, timeline_config)
    assert events == []


def test_timeline_event_has_datetime_ts_utc(temp_case_db, timeline_config):
    """Test that TimelineEvent.ts_utc is a datetime object, not a string."""
    conn, evidence_id = temp_case_db

    now = datetime.now(tz=timezone.utc)
    with conn:
        conn.execute(
            """
            INSERT INTO browser_history(evidence_id, url, title, ts_utc, browser, profile)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "https://test.com", "Test", now.isoformat(), "Chrome", "Default")
        )

    events = timelines_module.map_browser_history_to_events(conn, evidence_id, timeline_config)

    assert len(events) == 1
    assert isinstance(events[0].ts_utc, datetime)
    assert events[0].ts_utc.tzinfo is not None


def test_persist_timeline_clears_stale_data_on_empty_rebuild(temp_case_db):
    """Test that persist_timeline clears stale data even when rebuild yields 0 events."""
    conn, evidence_id = temp_case_db

    # Insert some existing timeline events manually
    base_time = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    with conn:
        conn.executemany(
            """
            INSERT INTO timeline(evidence_id, ts_utc, kind, ref_table, ref_id, confidence, note)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (evidence_id, base_time.isoformat(), "old_event", "test", 1, "high", "Old stale event 1"),
                (evidence_id, base_time.isoformat(), "old_event", "test", 2, "high", "Old stale event 2"),
            ]
        )

    # Verify stale data exists
    count_before = conn.execute(
        "SELECT COUNT(*) FROM timeline WHERE evidence_id = ?", (evidence_id,)
    ).fetchone()[0]
    assert count_before == 2

    # Rebuild with empty events list, passing evidence_id explicitly
    result = timelines_module.persist_timeline(conn, [], evidence_id=evidence_id)
    assert result == 0

    # Verify stale data is cleared
    count_after = conn.execute(
        "SELECT COUNT(*) FROM timeline WHERE evidence_id = ?", (evidence_id,)
    ).fetchone()[0]
    assert count_after == 0


def test_mapper_honors_config_kind_and_note_template(temp_case_db):
    """Test that mappers read kind and note_template from config mappings."""
    conn, evidence_id = temp_case_db

    # Create custom config with different kind and note_template
    custom_config = timelines_module.TimelineConfig(
        sources={
            "cookies": {
                "confidence": "high",  # Also test custom confidence
                "mappings": [
                    {
                        "timestamp_field": "creation_utc",
                        "kind": "custom_cookie_kind",
                        "note_template": "Custom: {domain} via {browser}"
                    }
                ]
            }
        },
        confidence_weights={"high": 1.0, "medium": 0.7, "low": 0.4},
        cluster_window_seconds=300,
        min_confidence=0.3
    )

    now = datetime.now(tz=timezone.utc)
    with conn:
        conn.execute(
            """
            INSERT INTO cookies(evidence_id, browser, domain, name, path, creation_utc, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "Chrome", "example.com", "session_id", "/", now.isoformat(), "run-001", "/path")
        )

    events = timelines_module.map_cookies_to_events(conn, evidence_id, custom_config)

    # Should only have 1 event (creation only, no access mapping in config)
    assert len(events) == 1

    # Should use custom kind from config
    assert events[0].kind == "custom_cookie_kind"

    # Should use custom note_template from config
    assert "Custom: example.com via Chrome" == events[0].note

    # Should use custom confidence from config
    assert events[0].confidence == "high"


def test_legacy_mapper_honors_multi_mapping_config(temp_case_db):
    """Test that legacy mappers (urls) properly iterate over multiple mappings."""
    conn, evidence_id = temp_case_db

    # Create config with two mappings for urls: first_seen and last_seen
    custom_config = timelines_module.TimelineConfig(
        sources={
            "urls": {
                "confidence": "medium",
                "mappings": [
                    {
                        "timestamp_field": "first_seen_utc",
                        "kind": "url_first_seen",
                        "note_template": "First: {url}"
                    },
                    {
                        "timestamp_field": "last_seen_utc",
                        "kind": "url_last_seen",
                        "note_template": "Last: {url}"
                    }
                ]
            }
        },
        confidence_weights={"high": 1.0, "medium": 0.7, "low": 0.4},
        cluster_window_seconds=300,
        min_confidence=0.3
    )

    first_seen = datetime.now(tz=timezone.utc) - timedelta(days=1)
    last_seen = datetime.now(tz=timezone.utc)

    with conn:
        conn.execute(
            """
            INSERT INTO urls(evidence_id, url, domain, discovered_by, first_seen_utc, last_seen_utc, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "https://example.com/page", "example.com", "browser_history", first_seen.isoformat(), last_seen.isoformat(), "run-001", "/path")
        )

    events = timelines_module.map_urls_to_events(conn, evidence_id, custom_config)

    # Should get 2 events: first_seen and last_seen
    assert len(events) == 2

    kinds = {e.kind for e in events}
    assert "url_first_seen" in kinds
    assert "url_last_seen" in kinds

    # Check note templates were applied
    notes = {e.note for e in events}
    assert "First: https://example.com/page" in notes
    assert "Last: https://example.com/page" in notes


def test_browser_history_multi_mapping_passthrough(temp_case_db):
    """Test browser_history mapper with single mapping honors config kind."""
    conn, evidence_id = temp_case_db

    # Custom config with different kind
    custom_config = timelines_module.TimelineConfig(
        sources={
            "browser_history": {
                "confidence": "high",
                "mappings": [
                    {
                        "timestamp_field": "ts_utc",
                        "kind": "custom_visit",
                        "note_template": "Visit: {url}"
                    }
                ]
            }
        },
        confidence_weights={"high": 1.0, "medium": 0.7, "low": 0.4},
        cluster_window_seconds=300,
        min_confidence=0.3
    )

    now = datetime.now(tz=timezone.utc)
    with conn:
        conn.execute(
            """
            INSERT INTO browser_history(evidence_id, url, title, ts_utc, browser, run_id, source_path)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (evidence_id, "https://python.org", "Python.org", now.isoformat(), "Chrome", "run-001", "/path")
        )

    events = timelines_module.map_browser_history_to_events(conn, evidence_id, custom_config)

    assert len(events) == 1
    assert events[0].kind == "custom_visit"  # Should use config kind, not default
    assert events[0].note == "Visit: https://python.org"  # Should use config template
