"""
Tests for browser_cache_inventory migration and helper functions.

Validates:
- Migration 0002 creates browser_cache_inventory table correctly
- insert_browser_inventory() helper function
- update_inventory_ingestion_status() helper function
- Table schema and indexes
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from core.database import insert_browser_inventory, update_inventory_ingestion_status
from core.database import DatabaseManager, EVIDENCE_MIGRATIONS_DIR


class TestBrowserInventoryMigration:
    """Test migration 0001 creates browser_cache_inventory table."""

    def test_migration_file_exists(self):
        """Migration 0001 file should exist and contain browser_cache_inventory."""
        migration_path = EVIDENCE_MIGRATIONS_DIR / "0001_evidence_schema.sql"
        assert migration_path.exists()

        # Verify SQL content
        sql = migration_path.read_text()
        assert "CREATE TABLE IF NOT EXISTS browser_cache_inventory" in sql
        assert "browser TEXT NOT NULL" in sql
        assert "artifact_type TEXT NOT NULL" in sql
        assert "run_id TEXT NOT NULL" in sql
        assert "extraction_status TEXT NOT NULL" in sql
        assert "ingestion_status TEXT" in sql

    def test_migration_creates_table(self, tmp_path):
        """Migration should create browser_cache_inventory table."""
        # Create evidence database with migrations
        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test_surfsifter.sqlite"

        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        conn = db_manager.get_evidence_conn(1, "test_evidence")

        # Check table exists
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='browser_cache_inventory'
        """)
        result = cursor.fetchone()

        assert result is not None
        assert result[0] == "browser_cache_inventory"

        db_manager.close_all()

    def test_migration_creates_indexes(self, tmp_path):
        """Migration should create all required indexes."""
        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test_surfsifter.sqlite"

        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        conn = db_manager.get_evidence_conn(1, "test_evidence")

        # Check indexes exist
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='index' AND tbl_name='browser_cache_inventory'
        """)
        indexes = {row[0] for row in cursor.fetchall()}

        expected_indexes = {
            "idx_cache_inventory_evidence",
            "idx_cache_inventory_run_id",
            "idx_cache_inventory_browser",
            "idx_cache_inventory_type",
            "idx_cache_inventory_status",
        }

        assert expected_indexes.issubset(indexes)

        db_manager.close_all()

    def test_table_schema(self, tmp_path):
        """Table should have all required columns."""
        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test_surfsifter.sqlite"

        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        conn = db_manager.get_evidence_conn(1, "test_evidence")

        # Get table info
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(browser_cache_inventory)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}  # name: type

        # Check required columns exist
        required_columns = {
            "id": "INTEGER",
            "evidence_id": "INTEGER",
            "browser": "TEXT",
            "artifact_type": "TEXT",
            "profile": "TEXT",
            "partition_index": "INTEGER",
            "fs_type": "TEXT",
            "logical_path": "TEXT",
            "forensic_path": "TEXT",
            "run_id": "TEXT",
            "extracted_path": "TEXT",
            "extraction_status": "TEXT",
            "extraction_timestamp_utc": "TEXT",
            "extraction_tool": "TEXT",
            "extraction_notes": "TEXT",
            "ingestion_status": "TEXT",
            "ingestion_timestamp_utc": "TEXT",
            "urls_parsed": "INTEGER",
            "records_parsed": "INTEGER",
            "ingestion_notes": "TEXT",
            "file_size_bytes": "INTEGER",
            "file_md5": "TEXT",
            "file_sha256": "TEXT",
            "created_at_utc": "TEXT",
            "updated_at_utc": "TEXT",
        }

        for col_name, col_type in required_columns.items():
            assert col_name in columns, f"Column {col_name} missing"
            # SQLite may normalize types, so just check it exists

        db_manager.close_all()


class TestBrowserInventoryHelpers:
    """Test insert_browser_inventory and update_inventory_ingestion_status helpers."""

    @pytest.fixture
    def evidence_conn(self, tmp_path):
        """Create evidence database with migrations applied."""
        case_folder = tmp_path / "case"
        case_folder.mkdir()
        case_db_path = case_folder / "test_surfsifter.sqlite"

        db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        conn = db_manager.get_evidence_conn(1, "test_evidence")

        yield conn

        db_manager.close_all()

    def test_insert_browser_inventory_minimal(self, evidence_conn):
        """insert_browser_inventory should work with minimal args."""
        inventory_id = insert_browser_inventory(
            conn=evidence_conn,
            evidence_id=1,
            browser="chrome",
            artifact_type="history",
            run_id="20241116_143000_abc123de",
            extracted_path="20241116_143000_abc123de/chrome_default/History",
            extraction_status="ok",
            extraction_timestamp_utc=datetime.now(timezone.utc).isoformat(),
            logical_path="C:\\Users\\Alice\\AppData\\Local\\Google\\Chrome\\User Data\\Default\\History",
        )

        assert inventory_id > 0

        # Verify inserted
        cursor = evidence_conn.cursor()
        cursor.execute("SELECT * FROM browser_cache_inventory WHERE id = ?", (inventory_id,))
        row = cursor.fetchone()

        assert row is not None
        assert row["browser"] == "chrome"
        assert row["artifact_type"] == "history"
        assert row["run_id"] == "20241116_143000_abc123de"
        assert row["extraction_status"] == "ok"
        assert row["ingestion_status"] == "pending"  # Default value

    def test_insert_browser_inventory_with_optionals(self, evidence_conn):
        """insert_browser_inventory should accept optional kwargs."""
        inventory_id = insert_browser_inventory(
            conn=evidence_conn,
            evidence_id=1,
            browser="firefox",
            artifact_type="cache_firefox",
            run_id="20241116_143000_xyz789ab",
            extracted_path="20241116_143000_xyz789ab/firefox_default/1F2E3D4C",
            extraction_status="ok",
            extraction_timestamp_utc=datetime.now(timezone.utc).isoformat(),
            logical_path="C:\\Users\\Bob\\AppData\\Roaming\\Mozilla\\Firefox\\Profiles\\abc123.default\\cache2\\entries\\1F2E3D4C",
            profile="abc123.default",
            partition_index=1,
            fs_type="ntfs",
            forensic_path="/dev/sda1",
            extraction_tool="pytsk3:0.4.0",
            extraction_notes="Extracted from E01 partition 1",
            file_size_bytes=45678,
            file_md5="abc123def456",
            file_sha256="sha256hash123",
        )

        assert inventory_id > 0

        # Verify all fields
        cursor = evidence_conn.cursor()
        cursor.execute("SELECT * FROM browser_cache_inventory WHERE id = ?", (inventory_id,))
        row = cursor.fetchone()

        assert row["profile"] == "abc123.default"
        assert row["partition_index"] == 1
        assert row["fs_type"] == "ntfs"
        assert row["forensic_path"] == "/dev/sda1"
        assert row["extraction_tool"] == "pytsk3:0.4.0"
        assert row["extraction_notes"] == "Extracted from E01 partition 1"
        assert row["file_size_bytes"] == 45678
        assert row["file_md5"] == "abc123def456"
        assert row["file_sha256"] == "sha256hash123"

    def test_update_inventory_ingestion_status_success(self, evidence_conn):
        """update_inventory_ingestion_status should update status and counts."""
        # Insert inventory record
        inventory_id = insert_browser_inventory(
            conn=evidence_conn,
            evidence_id=1,
            browser="chrome",
            artifact_type="history",
            run_id="20241116_143000_test123",
            extracted_path="test/path",
            extraction_status="ok",
            extraction_timestamp_utc=datetime.now(timezone.utc).isoformat(),
            logical_path="C:\\test\\path",
        )

        # Update ingestion status
        update_inventory_ingestion_status(
            conn=evidence_conn,
            inventory_id=inventory_id,
            status="ok",
            urls_parsed=123,
            records_parsed=456,
            notes="Ingestion successful",
        )

        evidence_conn.commit()

        # Verify update
        cursor = evidence_conn.cursor()
        cursor.execute("SELECT * FROM browser_cache_inventory WHERE id = ?", (inventory_id,))
        row = cursor.fetchone()

        assert row["ingestion_status"] == "ok"
        assert row["urls_parsed"] == 123
        assert row["records_parsed"] == 456
        assert row["ingestion_notes"] == "Ingestion successful"
        assert row["ingestion_timestamp_utc"] is not None
        assert row["updated_at_utc"] is not None

    def test_update_inventory_ingestion_status_failure(self, evidence_conn):
        """update_inventory_ingestion_status should handle failure status."""
        # Insert inventory record
        inventory_id = insert_browser_inventory(
            conn=evidence_conn,
            evidence_id=1,
            browser="firefox",
            artifact_type="cache_firefox",
            run_id="20241116_143000_fail123",
            extracted_path="test/fail",
            extraction_status="ok",
            extraction_timestamp_utc=datetime.now(timezone.utc).isoformat(),
            logical_path="C:\\test\\fail",
        )

        # Update with failure
        update_inventory_ingestion_status(
            conn=evidence_conn,
            inventory_id=inventory_id,
            status="failed",
            urls_parsed=0,
            records_parsed=0,
            notes="Parse error: corrupted database",
        )

        evidence_conn.commit()

        # Verify failure recorded
        cursor = evidence_conn.cursor()
        cursor.execute("SELECT * FROM browser_cache_inventory WHERE id = ?", (inventory_id,))
        row = cursor.fetchone()

        assert row["ingestion_status"] == "failed"
        assert row["urls_parsed"] == 0
        assert row["records_parsed"] == 0
        assert "corrupted database" in row["ingestion_notes"]

    def test_multiple_artifacts_same_run(self, evidence_conn):
        """Should support multiple artifacts from same extraction run."""
        run_id = "20241116_143000_multi123"

        # Insert multiple artifacts
        id1 = insert_browser_inventory(
            conn=evidence_conn,
            evidence_id=1,
            browser="chrome",
            artifact_type="history",
            run_id=run_id,
            extracted_path="run/History",
            extraction_status="ok",
            extraction_timestamp_utc=datetime.now(timezone.utc).isoformat(),
            logical_path="C:\\Chrome\\History",
        )

        id2 = insert_browser_inventory(
            conn=evidence_conn,
            evidence_id=1,
            browser="chrome",
            artifact_type="cache_simple",
            run_id=run_id,
            extracted_path="run/cache_data_0",
            extraction_status="ok",
            extraction_timestamp_utc=datetime.now(timezone.utc).isoformat(),
            logical_path="C:\\Chrome\\Cache\\cache_data_0",
        )

        evidence_conn.commit()

        # Query by run_id
        cursor = evidence_conn.cursor()
        cursor.execute("SELECT * FROM browser_cache_inventory WHERE run_id = ?", (run_id,))
        rows = cursor.fetchall()

        assert len(rows) == 2
        assert {row["artifact_type"] for row in rows} == {"history", "cache_simple"}

    def test_query_by_status(self, evidence_conn):
        """Should support querying by ingestion_status (index)."""
        # Insert some records
        for i in range(3):
            inventory_id = insert_browser_inventory(
                conn=evidence_conn,
                evidence_id=1,
                browser="chrome",
                artifact_type="history",
                run_id=f"run_{i}",
                extracted_path=f"path_{i}",
                extraction_status="ok",
                extraction_timestamp_utc=datetime.now(timezone.utc).isoformat(),
                logical_path=f"C:\\path_{i}",
            )

            # Update first two to "ok", leave third as "pending"
            if i < 2:
                update_inventory_ingestion_status(
                    conn=evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                )

        evidence_conn.commit()

        # Query pending
        cursor = evidence_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM browser_cache_inventory WHERE ingestion_status = 'pending'")
        pending_count = cursor.fetchone()[0]

        assert pending_count == 1

        # Query ok
        cursor.execute("SELECT COUNT(*) FROM browser_cache_inventory WHERE ingestion_status = 'ok'")
        ok_count = cursor.fetchone()[0]

        assert ok_count == 2
