"""
Tests for process_log_service module.

Tests the helpers that read extraction/ingestion run status from process_log table.
"""

import sqlite3
import pytest
from pathlib import Path
from datetime import datetime, timezone

from core.process_log_service import (
    get_last_successful_extraction,
    get_last_successful_ingestion,
    get_extractor_run_status,
    format_timestamp_for_display,
)


@pytest.fixture
def temp_db(tmp_path):
    """Create a temporary database with process_log table."""
    db_path = tmp_path / "test_evidence.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE process_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            task TEXT NOT NULL,
            command TEXT,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT,
            exit_code INTEGER,
            stdout TEXT,
            stderr TEXT,
            run_id TEXT,
            extractor_name TEXT,
            extractor_version TEXT,
            records_extracted INTEGER,
            records_ingested INTEGER,
            warnings_json TEXT,
            log_file_path TEXT
        )
    """)
    conn.execute("CREATE INDEX idx_process_log_extractor_name ON process_log(extractor_name)")
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def populated_db(temp_db):
    """Populate database with test data."""
    conn = sqlite3.connect(temp_db)

    # Add some extraction runs for evidence_id=1
    rows = [
        # Successful extraction (older)
        (1, "extract:browser_history", None, "2025-01-10T10:00:00+00:00",
         "2025-01-10T10:05:00+00:00", 0, None, None, "run_001", "browser_history",
         None, 100, None, None, None),
        # Failed extraction (should be ignored)
        (1, "extract:browser_history", None, "2025-01-11T10:00:00+00:00",
         "2025-01-11T10:05:00+00:00", 1, None, "Error", "run_002", "browser_history",
         None, 0, None, None, None),
        # Successful extraction (newer - should be returned)
        (1, "extract:browser_history", None, "2025-01-12T10:00:00+00:00",
         "2025-01-12T10:15:00+00:00", 0, None, None, "run_003", "browser_history",
         None, 200, None, None, None),
        # Successful ingestion
        (1, "extract:browser_history:ingest", None, "2025-01-12T10:20:00+00:00",
         "2025-01-12T10:25:00+00:00", 0, None, None, "run_004", "browser_history:ingest",
         None, None, 500, None, None),
        # Another evidence (id=2) - should not be returned for evidence_id=1
        (2, "extract:browser_history", None, "2025-01-15T10:00:00+00:00",
         "2025-01-15T10:05:00+00:00", 0, None, None, "run_005", "browser_history",
         None, 300, None, None, None),
        # Different extractor
        (1, "extract:cookies", None, "2025-01-13T10:00:00+00:00",
         "2025-01-13T10:05:00+00:00", 0, None, None, "run_006", "cookies",
         None, 50, None, None, None),
    ]

    conn.executemany("""
        INSERT INTO process_log (
            evidence_id, task, command, started_at_utc, finished_at_utc,
            exit_code, stdout, stderr, run_id, extractor_name,
            extractor_version, records_extracted, records_ingested,
            warnings_json, log_file_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()
    return temp_db


class TestGetLastSuccessfulExtraction:
    """Tests for get_last_successful_extraction."""

    def test_returns_none_for_missing_db(self, tmp_path):
        """Returns None when database file doesn't exist."""
        db_path = tmp_path / "nonexistent.sqlite"
        result = get_last_successful_extraction(db_path, "browser_history", 1)
        assert result is None

    def test_returns_none_for_no_runs(self, temp_db):
        """Returns None when no runs exist."""
        result = get_last_successful_extraction(temp_db, "browser_history", 1)
        assert result is None

    def test_returns_last_successful_run(self, populated_db):
        """Returns the most recent successful extraction."""
        result = get_last_successful_extraction(populated_db, "browser_history", 1)

        assert result is not None
        assert result["run_id"] == "run_003"
        assert result["finished_at"] == "2025-01-12T10:15:00+00:00"

    def test_filters_by_evidence_id(self, populated_db):
        """Only returns runs for the specified evidence."""
        result = get_last_successful_extraction(populated_db, "browser_history", 2)

        assert result is not None
        assert result["run_id"] == "run_005"

    def test_ignores_failed_runs(self, populated_db):
        """Ignores runs with non-zero exit_code."""
        # Add a more recent failed run
        conn = sqlite3.connect(populated_db)
        conn.execute("""
            INSERT INTO process_log (
                evidence_id, task, started_at_utc, finished_at_utc,
                exit_code, run_id, extractor_name
            ) VALUES (1, 'extract:browser_history', '2025-01-20T10:00:00+00:00',
                      '2025-01-20T10:05:00+00:00', 1, 'run_failed', 'browser_history')
        """)
        conn.commit()
        conn.close()

        result = get_last_successful_extraction(populated_db, "browser_history", 1)

        # Should still return run_003, not the failed run
        assert result is not None
        assert result["run_id"] == "run_003"

    def test_filters_by_extractor_name(self, populated_db):
        """Returns only runs for the specified extractor."""
        result = get_last_successful_extraction(populated_db, "cookies", 1)

        assert result is not None
        assert result["run_id"] == "run_006"

    def test_returns_none_for_unknown_extractor(self, populated_db):
        """Returns None for extractor with no runs."""
        result = get_last_successful_extraction(populated_db, "unknown_extractor", 1)
        assert result is None


class TestGetLastSuccessfulIngestion:
    """Tests for get_last_successful_ingestion."""

    def test_returns_none_for_missing_db(self, tmp_path):
        """Returns None when database file doesn't exist."""
        db_path = tmp_path / "nonexistent.sqlite"
        result = get_last_successful_ingestion(db_path, "browser_history", 1)
        assert result is None

    def test_returns_none_for_no_runs(self, temp_db):
        """Returns None when no ingestion runs exist."""
        result = get_last_successful_ingestion(temp_db, "browser_history", 1)
        assert result is None

    def test_returns_last_successful_ingestion(self, populated_db):
        """Returns the most recent successful ingestion with record count."""
        result = get_last_successful_ingestion(populated_db, "browser_history", 1)

        assert result is not None
        assert result["run_id"] == "run_004"
        assert result["finished_at"] == "2025-01-12T10:25:00+00:00"
        assert result["records_ingested"] == 500

    def test_uses_ingest_suffix(self, populated_db):
        """Looks up extractor_name with ':ingest' suffix."""
        # Verify the ingestion run exists
        conn = sqlite3.connect(populated_db)
        cur = conn.execute(
            "SELECT extractor_name FROM process_log WHERE run_id = 'run_004'"
        )
        row = cur.fetchone()
        conn.close()

        assert row[0] == "browser_history:ingest"

        # And our function finds it
        result = get_last_successful_ingestion(populated_db, "browser_history", 1)
        assert result is not None


class TestGetExtractorRunStatus:
    """Tests for get_extractor_run_status."""

    def test_returns_combined_status(self, populated_db):
        """Returns both extraction and ingestion status."""
        status = get_extractor_run_status(populated_db, "browser_history", 1)

        assert "extraction" in status
        assert "ingestion" in status
        assert status["extraction"]["run_id"] == "run_003"
        assert status["ingestion"]["run_id"] == "run_004"

    def test_returns_none_for_missing_phases(self, populated_db):
        """Returns None for phases that haven't run."""
        status = get_extractor_run_status(populated_db, "cookies", 1)

        assert status["extraction"] is not None  # cookies extraction exists
        assert status["ingestion"] is None  # no cookies ingestion

    def test_handles_missing_db(self, tmp_path):
        """Handles missing database gracefully."""
        db_path = tmp_path / "nonexistent.sqlite"
        status = get_extractor_run_status(db_path, "browser_history", 1)

        assert status["extraction"] is None
        assert status["ingestion"] is None


class TestFallbackToStartedAt:
    """Tests for timestamp fallback when finished_at_utc is NULL."""

    def test_extraction_uses_started_at_when_finished_is_null(self, temp_db):
        """Falls back to started_at_utc when finished_at_utc is NULL."""
        conn = sqlite3.connect(temp_db)
        conn.execute("""
            INSERT INTO process_log (
                evidence_id, task, started_at_utc, finished_at_utc,
                exit_code, run_id, extractor_name
            ) VALUES (1, 'extract:test', '2025-01-15T10:00:00+00:00',
                      NULL, 0, 'run_unfinished', 'test')
        """)
        conn.commit()
        conn.close()

        result = get_last_successful_extraction(temp_db, "test", 1)

        assert result is not None
        assert result["started_at"] == "2025-01-15T10:00:00+00:00"
        # finished_at should be the fallback value (started_at)
        assert result["finished_at"] == "2025-01-15T10:00:00+00:00"

    def test_ingestion_uses_started_at_when_finished_is_null(self, temp_db):
        """Falls back to started_at_utc for ingestion as well."""
        conn = sqlite3.connect(temp_db)
        conn.execute("""
            INSERT INTO process_log (
                evidence_id, task, started_at_utc, finished_at_utc,
                exit_code, run_id, extractor_name, records_ingested
            ) VALUES (1, 'extract:test:ingest', '2025-01-15T10:00:00+00:00',
                      NULL, 0, 'run_ingest', 'test:ingest', 100)
        """)
        conn.commit()
        conn.close()

        result = get_last_successful_ingestion(temp_db, "test", 1)

        assert result is not None
        assert result["finished_at"] == "2025-01-15T10:00:00+00:00"
        assert result["records_ingested"] == 100


class TestFormatTimestampForDisplay:
    """Tests for format_timestamp_for_display."""

    def test_formats_iso_timestamp(self):
        """Formats ISO timestamp to YYYY-MM-DD HH:MM."""
        result = format_timestamp_for_display("2025-01-15T14:30:45.123456+00:00")
        assert result == "2025-01-15 14:30"

    def test_handles_z_suffix(self):
        """Handles timestamps with Z suffix."""
        result = format_timestamp_for_display("2025-01-15T14:30:45Z")
        assert result == "2025-01-15 14:30"

    def test_returns_na_for_none(self):
        """Returns N/A for None input."""
        result = format_timestamp_for_display(None)
        assert result == "N/A"

    def test_returns_na_for_empty_string(self):
        """Returns N/A for empty string."""
        result = format_timestamp_for_display("")
        assert result == "N/A"

    def test_handles_short_timestamp(self):
        """Handles timestamps shorter than 16 chars."""
        result = format_timestamp_for_display("2025-01-15")
        assert result == "2025-01-15"

