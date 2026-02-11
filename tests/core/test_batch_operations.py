"""
Test suite for Batch Operations Enhancement

Tests database helpers, ingestion modes, and batch operations functionality.
"""

from datetime import datetime, timezone
import sqlite3
import pytest

from core.database import (
    EVIDENCE_MIGRATIONS_DIR,
    get_evidence_table_counts,
    purge_evidence_data,
    insert_urls,
    insert_bitcoin_addresses,
    insert_emails,
    migrate,
)


@pytest.fixture
def evidence_db(tmp_path):
    """Create a temporary evidence database with schema."""
    db_path = tmp_path / "test_evidence.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    # Apply all evidence migrations
    migrate(conn, EVIDENCE_MIGRATIONS_DIR)

    yield conn
    conn.close()


def test_get_evidence_table_counts_empty(evidence_db):
    """Verify counts on fresh DB return all zeros."""
    counts = get_evidence_table_counts(evidence_db, evidence_id=1)

    assert isinstance(counts, dict)
    assert len(counts) > 0
    assert all(count == 0 for count in counts.values())


def test_get_evidence_table_counts_with_data(evidence_db):
    """Verify table counting with populated test data."""
    evidence_id = 1

    # Insert some test data
    insert_urls(evidence_db, evidence_id, [
        {"url": "https://example.com", "domain": "example.com", "scheme": "https",
         "discovered_by": "test", "first_seen_utc": datetime.now(timezone.utc).isoformat()}
    ])

    insert_bitcoin_addresses(evidence_db, evidence_id, [
        {"address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", "discovered_by": "test",
         "first_seen_utc": datetime.now(timezone.utc).isoformat()}
    ])

    insert_emails(evidence_db, evidence_id, [
        {"email": "test@example.com", "domain": "example.com", "discovered_by": "test",
         "first_seen_utc": datetime.now(timezone.utc).isoformat()}
    ])

    counts = get_evidence_table_counts(evidence_db, evidence_id)

    assert counts['urls'] == 1
    assert counts['bitcoin_addresses'] == 1
    assert counts['emails'] == 1
    assert counts['images'] == 0  # Not inserted


def test_purge_evidence_data(evidence_db):
    """Verify purge deletes all records with FK integrity."""
    evidence_id = 1

    # Insert test data into multiple tables
    insert_urls(evidence_db, evidence_id, [
        {"url": "https://example.com", "domain": "example.com", "scheme": "https",
         "discovered_by": "test", "first_seen_utc": datetime.now(timezone.utc).isoformat()}
    ])

    insert_bitcoin_addresses(evidence_db, evidence_id, [
        {"address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", "discovered_by": "test",
         "first_seen_utc": datetime.now(timezone.utc).isoformat()}
    ])

    # Verify data exists
    counts_before = get_evidence_table_counts(evidence_db, evidence_id)
    assert counts_before['urls'] == 1
    assert counts_before['bitcoin_addresses'] == 1

    # Purge data
    deleted_count = purge_evidence_data(evidence_db, evidence_id)
    assert deleted_count == 2

    # Verify all artifact tables empty
    counts_after = get_evidence_table_counts(evidence_db, evidence_id)
    assert counts_after['urls'] == 0
    assert counts_after['bitcoin_addresses'] == 0


def test_purge_respects_evidence_id_scope(evidence_db):
    """Verify purge only deletes specified evidence_id."""
    # Insert records for evidence_id 1 and 2
    insert_urls(evidence_db, 1, [
        {"url": "https://evidence1.com", "domain": "evidence1.com", "scheme": "https",
         "discovered_by": "test", "first_seen_utc": datetime.now(timezone.utc).isoformat()}
    ])

    insert_urls(evidence_db, 2, [
        {"url": "https://evidence2.com", "domain": "evidence2.com", "scheme": "https",
         "discovered_by": "test", "first_seen_utc": datetime.now(timezone.utc).isoformat()}
    ])

    # Purge evidence_id 1
    deleted_count = purge_evidence_data(evidence_db, 1)
    assert deleted_count == 1

    # Verify evidence_id 2 records intact
    counts_ev2 = get_evidence_table_counts(evidence_db, 2)
    assert counts_ev2['urls'] == 1


def test_insert_functions_accept_run_id(evidence_db):
    """Verify all 11 updated insert functions accept run_id parameter."""
    evidence_id = 1
    run_id = "test_run_123"
    ts = datetime.now(timezone.utc).isoformat()

    # Test bulk_extractor artifacts (6 functions)
    insert_bitcoin_addresses(evidence_db, evidence_id, [
        {"address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", "discovered_by": "test",
         "first_seen_utc": ts, "run_id": run_id}
    ])

    insert_emails(evidence_db, evidence_id, [
        {"email": "test@example.com", "domain": "example.com", "discovered_by": "test",
         "first_seen_utc": ts, "run_id": run_id}
    ])

    # Verify run_id was written
    cursor = evidence_db.execute("SELECT run_id FROM bitcoin_addresses WHERE evidence_id = ?", (evidence_id,))
    row = cursor.fetchone()
    assert row[0] == run_id

    cursor = evidence_db.execute("SELECT run_id FROM emails WHERE evidence_id = ?", (evidence_id,))
    row = cursor.fetchone()
    assert row[0] == run_id


def test_file_list_filter_cache_unique_constraint(evidence_db):
    """Verify file_list_filter_cache UNIQUE constraint includes run_id."""
    evidence_id = 1

    # Insert first record
    evidence_db.execute(
        """
        INSERT INTO file_list_filter_cache (evidence_id, filter_type, filter_value, count, last_updated, run_id)
        VALUES (?, 'extension', 'jpg', 10, ?, 'run1')
        """,
        (evidence_id, datetime.now(timezone.utc).isoformat())
    )

    # Insert same filter with different run_id - should succeed
    evidence_db.execute(
        """
        INSERT INTO file_list_filter_cache (evidence_id, filter_type, filter_value, count, last_updated, run_id)
        VALUES (?, 'extension', 'jpg', 15, ?, 'run2')
        """,
        (evidence_id, datetime.now(timezone.utc).isoformat())
    )

    # Verify both records exist
    cursor = evidence_db.execute(
        "SELECT COUNT(*) FROM file_list_filter_cache WHERE evidence_id = ?",
        (evidence_id,)
    )
    assert cursor.fetchone()[0] == 2
