"""
Tests for Chromium TransportSecurity (HSTS) extractor.

Covers:
- HSTS schema conflict resolution (INSERT OR REPLACE for re-runs)
- Within-file deduplication of hashed_host entries
- insert_hsts_entries with unique constraint handling
"""

import sqlite3
from typing import Any, Dict, List

import pytest

from core.database.helpers.hsts import (
    insert_hsts_entries,
    get_hsts_entries,
    delete_hsts_by_run,
)
from core.database.schema import TABLE_SCHEMAS


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def hsts_db():
    """In-memory SQLite database with hsts_entries table and unique index."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE hsts_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            browser TEXT NOT NULL,
            profile TEXT,
            hashed_host TEXT NOT NULL,
            decoded_host TEXT,
            decode_method TEXT,
            sts_observed REAL,
            expiry REAL,
            include_subdomains INTEGER DEFAULT 0,
            mode TEXT,
            source_path TEXT NOT NULL,
            run_id TEXT NOT NULL,
            discovered_by TEXT DEFAULT 'transport_security',
            partition_index INTEGER,
            fs_type TEXT,
            logical_path TEXT,
            forensic_path TEXT,
            tags TEXT,
            notes TEXT,
            created_at_utc TEXT
        )
    """)
    conn.execute("""
        CREATE UNIQUE INDEX idx_hsts_entries_unique
            ON hsts_entries(evidence_id, hashed_host, source_path)
    """)
    conn.commit()
    return conn


def _make_hsts_record(
    hashed_host: str,
    run_id: str = "run1",
    browser: str = "edge",
    source_path: str = "/p3/edge/TransportSecurity",
    **overrides,
) -> Dict[str, Any]:
    """Create a minimal HSTS record dict."""
    record = {
        "browser": browser,
        "profile": "Default",
        "hashed_host": hashed_host,
        "sts_observed": 1700000000.0,
        "expiry": 1731536000.0,
        "mode": "force-https",
        "include_subdomains": 1,
        "decoded_host": None,
        "decode_method": None,
        "run_id": run_id,
        "source_path": source_path,
        "discovered_by": "transport_security:1.0:run1",
        "partition_index": 3,
        "fs_type": "NTFS",
        "logical_path": source_path,
        "forensic_path": None,
    }
    record.update(overrides)
    return record


# =============================================================================
# Schema Configuration Tests
# =============================================================================

class TestHstsSchemaConfig:
    """Verify schema is configured for REPLACE conflict resolution."""

    def test_conflict_action_is_replace(self):
        """HSTS schema uses REPLACE to handle re-run conflicts."""
        from core.database.schema.base import ConflictAction
        schema = TABLE_SCHEMAS["hsts_entries"]
        assert schema.conflict_action == ConflictAction.REPLACE


# =============================================================================
# Insert / UNIQUE Constraint Tests
# =============================================================================

class TestHstsInsertReplace:
    """Test that HSTS inserts use REPLACE for duplicate (evidence_id, hashed_host, source_path)."""

    def test_insert_basic(self, hsts_db):
        """Basic insertion works."""
        records = [
            _make_hsts_record("hash_abc"),
            _make_hsts_record("hash_def"),
        ]
        count = insert_hsts_entries(hsts_db, evidence_id=1, entries=records)
        assert count == 2

        rows = get_hsts_entries(hsts_db, evidence_id=1)
        assert len(rows) == 2

    def test_rerun_replaces_entries(self, hsts_db):
        """Re-running with different run_id replaces existing entries."""
        # First run
        records_v1 = [
            _make_hsts_record("hash_abc", run_id="run_old", expiry=1700000000.0),
            _make_hsts_record("hash_def", run_id="run_old", expiry=1700000000.0),
        ]
        insert_hsts_entries(hsts_db, evidence_id=1, entries=records_v1)

        # Second run (same hashed_hosts, different run_id) — should NOT error
        records_v2 = [
            _make_hsts_record("hash_abc", run_id="run_new", expiry=1800000000.0),
            _make_hsts_record("hash_def", run_id="run_new", expiry=1800000000.0),
        ]
        count = insert_hsts_entries(hsts_db, evidence_id=1, entries=records_v2)
        assert count == 2

        # Only 2 rows total (replaced, not appended)
        rows = get_hsts_entries(hsts_db, evidence_id=1)
        assert len(rows) == 2

        # Data should reflect the new run
        for row in rows:
            assert row["run_id"] == "run_new"
            assert row["expiry"] == 1800000000.0

    def test_duplicate_hashed_host_in_same_file(self, hsts_db):
        """Duplicate hashed_host within one file is handled by REPLACE."""
        records = [
            _make_hsts_record("hash_abc", expiry=1700000000.0),
            _make_hsts_record("hash_abc", expiry=1800000000.0),  # duplicate
        ]
        # Should NOT raise IntegrityError
        count = insert_hsts_entries(hsts_db, evidence_id=1, entries=records)
        # REPLACE: last one wins
        assert count >= 1

        rows = get_hsts_entries(hsts_db, evidence_id=1)
        assert len(rows) == 1
        assert rows[0]["expiry"] == 1800000000.0

    def test_different_source_paths_no_conflict(self, hsts_db):
        """Same hashed_host from different source files is allowed."""
        records = [
            _make_hsts_record("hash_abc", source_path="/edge/TransportSecurity"),
            _make_hsts_record("hash_abc", source_path="/chrome/TransportSecurity"),
        ]
        count = insert_hsts_entries(hsts_db, evidence_id=1, entries=records)
        assert count == 2

        rows = get_hsts_entries(hsts_db, evidence_id=1)
        assert len(rows) == 2

    def test_different_evidence_ids_no_conflict(self, hsts_db):
        """Same hashed_host in different evidences is allowed."""
        records = [_make_hsts_record("hash_abc")]
        insert_hsts_entries(hsts_db, evidence_id=1, entries=records)
        insert_hsts_entries(hsts_db, evidence_id=2, entries=records)

        assert len(get_hsts_entries(hsts_db, evidence_id=1)) == 1
        assert len(get_hsts_entries(hsts_db, evidence_id=2)) == 1
