"""Tests for sync_process_log_counters helper."""
from __future__ import annotations

import json
import sqlite3

import pytest

from core.database.helpers.statistics import sync_process_log_counters


@pytest.fixture()
def evidence_conn():
    """In-memory SQLite with extractor_statistics and process_log tables."""
    conn = sqlite3.connect(":memory:")
    conn.execute("""
        CREATE TABLE extractor_statistics (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            extractor_name TEXT NOT NULL,
            run_id TEXT,
            started_at TEXT,
            finished_at TEXT,
            duration_seconds REAL,
            status TEXT,
            discovered TEXT,
            ingested TEXT,
            failed TEXT,
            skipped TEXT,
            UNIQUE(evidence_id, extractor_name)
        )
    """)
    conn.execute("""
        CREATE TABLE process_log (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER,
            run_id TEXT,
            extractor_name TEXT,
            task TEXT,
            records_extracted INTEGER DEFAULT 0,
            records_ingested INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    yield conn
    conn.close()


def _insert_stats(conn, evidence_id, extractor_name, run_id, discovered, ingested):
    conn.execute(
        "INSERT INTO extractor_statistics (evidence_id, extractor_name, run_id, discovered, ingested) "
        "VALUES (?, ?, ?, ?, ?)",
        (evidence_id, extractor_name, run_id, json.dumps(discovered), json.dumps(ingested)),
    )
    conn.commit()


def _insert_process_log(conn, evidence_id, run_id, extractor_name, task=""):
    conn.execute(
        "INSERT INTO process_log (evidence_id, run_id, extractor_name, task) VALUES (?, ?, ?, ?)",
        (evidence_id, run_id, extractor_name, task),
    )
    conn.commit()


def _get_process_log(conn, evidence_id):
    cur = conn.execute(
        "SELECT records_extracted, records_ingested FROM process_log WHERE evidence_id = ?",
        (evidence_id,),
    )
    return cur.fetchone()


class TestSyncProcessLogCounters:
    """Tests for sync_process_log_counters."""

    def test_sync_with_matching_data(self, evidence_conn):
        """process_log updated when extractor_statistics has matching data."""
        _insert_stats(evidence_conn, 1, "history", "run-1",
                      discovered={"files": 5}, ingested={"records": 42})
        _insert_process_log(evidence_conn, 1, "run-1", "history")

        result = sync_process_log_counters(evidence_conn, 1, "history")

        assert result is True
        row = _get_process_log(evidence_conn, 1)
        assert row == (5, 42)

    def test_no_statistics_returns_false(self, evidence_conn):
        """Returns False when no extractor_statistics row exists."""
        _insert_process_log(evidence_conn, 1, "run-1", "history")

        result = sync_process_log_counters(evidence_conn, 1, "history")

        assert result is False
        row = _get_process_log(evidence_conn, 1)
        assert row == (0, 0)

    def test_empty_json_sets_zero(self, evidence_conn):
        """Empty JSON dicts result in 0 counts."""
        _insert_stats(evidence_conn, 1, "cookies", "run-2",
                      discovered={}, ingested={})
        _insert_process_log(evidence_conn, 1, "run-2", "cookies")

        result = sync_process_log_counters(evidence_conn, 1, "cookies")

        assert result is True
        row = _get_process_log(evidence_conn, 1)
        assert row == (0, 0)

    def test_ingested_records_key(self, evidence_conn):
        """Uses 'records' key from ingested JSON when present."""
        _insert_stats(evidence_conn, 1, "history", "run-3",
                      discovered={"records": 10},
                      ingested={"records": 99, "urls": 50, "cookies": 49})
        _insert_process_log(evidence_conn, 1, "run-3", "history")

        result = sync_process_log_counters(evidence_conn, 1, "history")

        assert result is True
        row = _get_process_log(evidence_conn, 1)
        assert row == (10, 99)

    def test_multiple_count_keys_summed(self, evidence_conn):
        """Sums all values when no 'records' key exists."""
        _insert_stats(evidence_conn, 1, "bulk_extractor", "run-4",
                      discovered={"urls": 100, "emails": 50},
                      ingested={"urls": 80, "emails": 40})
        _insert_process_log(evidence_conn, 1, "run-4", "bulk_extractor")

        result = sync_process_log_counters(evidence_conn, 1, "bulk_extractor")

        assert result is True
        row = _get_process_log(evidence_conn, 1)
        assert row == (150, 120)

    def test_no_run_id_returns_false(self, evidence_conn):
        """Returns False when run_id is None in statistics."""
        evidence_conn.execute(
            "INSERT INTO extractor_statistics (evidence_id, extractor_name, run_id, discovered, ingested) "
            "VALUES (?, ?, ?, ?, ?)",
            (1, "history", None, "{}", "{}"),
        )
        evidence_conn.commit()
        _insert_process_log(evidence_conn, 1, "run-1", "history")

        result = sync_process_log_counters(evidence_conn, 1, "history")

        assert result is False

    def test_task_like_match(self, evidence_conn):
        """Matches process_log rows by task LIKE pattern."""
        _insert_stats(evidence_conn, 1, "history", "run-5",
                      discovered={"files": 3}, ingested={"records": 15})
        # process_log has extractor name in task, not extractor_name column
        evidence_conn.execute(
            "INSERT INTO process_log (evidence_id, run_id, extractor_name, task) VALUES (?, ?, ?, ?)",
            (1, "run-5", "", "Extract history artifacts"),
        )
        evidence_conn.commit()

        result = sync_process_log_counters(evidence_conn, 1, "history")

        assert result is True
        row = _get_process_log(evidence_conn, 1)
        assert row == (3, 15)

    def test_table_not_exists_returns_false(self):
        """Gracefully handles missing extractor_statistics table."""
        conn = sqlite3.connect(":memory:")
        conn.execute("""
            CREATE TABLE process_log (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                run_id TEXT,
                extractor_name TEXT,
                task TEXT,
                records_extracted INTEGER DEFAULT 0,
                records_ingested INTEGER DEFAULT 0
            )
        """)
        conn.commit()

        result = sync_process_log_counters(conn, 1, "history")

        assert result is False
        conn.close()

    def test_non_numeric_values_ignored(self, evidence_conn):
        """Non-numeric values in JSON are ignored when summing."""
        _insert_stats(evidence_conn, 1, "mixed", "run-6",
                      discovered={"urls": 10, "status": "ok"},
                      ingested={"urls": 5, "note": "partial"})
        _insert_process_log(evidence_conn, 1, "run-6", "mixed")

        result = sync_process_log_counters(evidence_conn, 1, "mixed")

        assert result is True
        row = _get_process_log(evidence_conn, 1)
        # Only numeric values summed: discovered=10, ingested=5
        assert row == (10, 5)
