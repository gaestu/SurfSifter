"""
Test file list database schema (migration 0003).
"""
import sqlite3
from pathlib import Path

import pytest

from core.database import DatabaseManager


def test_file_list_tables_created(tmp_path):
    """Test that file_list tables are created by migration 0003."""
    case_folder = tmp_path / "test_case"
    case_folder.mkdir()
    case_db_path = case_folder / "TEST-001_surfsifter.sqlite"

    # Create case DB first
    manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    case_conn = manager.get_case_conn()

    # Insert case and evidence
    case_conn.execute(
        "INSERT INTO cases (case_id, title, investigator, created_at_utc) VALUES ('TEST-001', 'Test', 'Tester', '2025-11-05T10:00:00Z')"
    )
    case_conn.execute(
        "INSERT INTO evidences (case_id, label, source_path, added_at_utc) VALUES (1, 'EV-001', '/test.e01', '2025-11-05T10:00:00Z')"
    )
    case_conn.commit()
    evidence_id = case_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Get evidence connection (triggers migration 0003)
    evidence_conn = manager.get_evidence_conn(evidence_id, label="EV-001")

    # Verify file_list table exists with correct schema
    cursor = evidence_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='file_list'")
    assert cursor.fetchone() is not None, "file_list table should exist"

    # Verify columns
    cursor = evidence_conn.execute("PRAGMA table_info(file_list)")
    columns = {row[1] for row in cursor.fetchall()}
    expected_columns = {
        'id', 'evidence_id', 'file_path', 'file_name', 'extension', 'size_bytes',
        'created_ts', 'modified_ts', 'accessed_ts', 'md5_hash', 'sha1_hash',
        'sha256_hash', 'file_type', 'deleted', 'metadata', 'import_source',
        'import_timestamp'
    }
    assert expected_columns.issubset(columns), f"Missing columns: {expected_columns - columns}"

    # Verify indexes
    cursor = evidence_conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='file_list'")
    indexes = {row[0] for row in cursor.fetchall()}
    expected_indexes = {
        'idx_file_list_evidence', 'idx_file_list_name', 'idx_file_list_extension',
        'idx_file_list_path', 'idx_file_list_md5', 'idx_file_list_sha1',
        'idx_file_list_sha256'
    }
    assert expected_indexes.issubset(indexes), f"Missing indexes: {expected_indexes - indexes}"

    # Verify tags table exists
    cursor = evidence_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tags'")
    assert cursor.fetchone() is not None, "tags table should exist"

    # Verify tag_associations table exists
    cursor = evidence_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tag_associations'")
    assert cursor.fetchone() is not None, "tag_associations table should exist"

    # Verify file_list_matches table exists
    cursor = evidence_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='file_list_matches'")
    assert cursor.fetchone() is not None, "file_list_matches table should exist"

    evidence_conn.close()
    case_conn.close()


def test_file_list_insert_and_query(tmp_path):
    """Test inserting and querying file list entries."""
    case_folder = tmp_path / "test_case"
    case_folder.mkdir()
    case_db_path = case_folder / "TEST-001_surfsifter.sqlite"

    manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    case_conn = manager.get_case_conn()

    # Insert case and evidence
    case_conn.execute(
        "INSERT INTO cases (case_id, title, investigator, created_at_utc) VALUES ('TEST-001', 'Test Case', 'Tester', '2025-11-05T10:00:00Z')"
    )
    case_conn.commit()
    case_id = case_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    case_conn.execute(
        "INSERT INTO evidences (case_id, label, source_path, added_at_utc) VALUES (?, 'EV-001', '/test.e01', '2025-11-05T10:00:00Z')",
        (case_id,)
    )
    case_conn.commit()
    evidence_id = case_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Get evidence connection
    evidence_conn = manager.get_evidence_conn(evidence_id, label="EV-001")

    # Insert file list entry
    evidence_conn.execute("""
        INSERT INTO file_list (
            evidence_id, file_path, file_name, extension, size_bytes,
            modified_ts, md5_hash, import_source, import_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        evidence_id,
        'C:\\Windows\\System32\\deepfreeze.exe',
        'deepfreeze.exe',
        'exe',
        1024000,
        '2025-01-01 12:00:00',
        'd41d8cd98f00b204e9800998ecf8427e',
        'ftk',
        '2025-11-05 10:00:00'
    ))
    evidence_conn.commit()

    # Query back
    cursor = evidence_conn.execute("SELECT file_name, extension, size_bytes FROM file_list WHERE evidence_id = ?", (evidence_id,))
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == 'deepfreeze.exe'
    assert row[1] == 'exe'
    assert row[2] == 1024000

    evidence_conn.close()
    case_conn.close()


def test_file_list_tag_associations_foreign_key(tmp_path):
    """Test that tag_associations foreign key constraint works."""
    case_folder = tmp_path / "test_case"
    case_folder.mkdir()
    case_db_path = case_folder / "TEST-001_surfsifter.sqlite"

    manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    case_conn = manager.get_case_conn()

    # Insert case and evidence
    case_conn.execute(
        "INSERT INTO cases (case_id, title, investigator, created_at_utc) VALUES ('TEST-001', 'Test Case', 'Tester', '2025-11-05T10:00:00Z')"
    )
    case_conn.commit()
    case_id = case_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    case_conn.execute(
        "INSERT INTO evidences (case_id, label, source_path, added_at_utc) VALUES (?, 'EV-001', '/test.e01', '2025-11-05T10:00:00Z')",
        (case_id,)
    )
    case_conn.commit()
    evidence_id = case_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    evidence_conn = manager.get_evidence_conn(evidence_id, label="EV-001")

    # Insert file list entry
    evidence_conn.execute("""
        INSERT INTO file_list (
            evidence_id, file_path, file_name, import_source, import_timestamp
        ) VALUES (?, ?, ?, ?, ?)
    """, (evidence_id, 'C:\\test.exe', 'test.exe', 'ftk', '2025-11-05 10:00:00'))
    evidence_conn.commit()
    file_list_id = evidence_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Insert tag
    evidence_conn.execute("""
        INSERT INTO tags (evidence_id, name, name_normalized) VALUES (?, ?, ?)
    """, (evidence_id, 'Suspicious', 'suspicious'))
    tag_id = evidence_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Insert association
    evidence_conn.execute("""
        INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id)
        VALUES (?, ?, ?, ?)
    """, (tag_id, evidence_id, 'file_list', file_list_id))
    evidence_conn.commit()

    # Verify association exists
    cursor = evidence_conn.execute("SELECT count(*) FROM tag_associations WHERE artifact_id = ? AND artifact_type = 'file_list'", (file_list_id,))
    assert cursor.fetchone()[0] == 1

    # Delete tag (should cascade delete association)
    evidence_conn.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
    evidence_conn.commit()

    # Verify association was deleted (cascade)
    cursor = evidence_conn.execute("SELECT COUNT(*) FROM tag_associations WHERE artifact_id = ? AND artifact_type = 'file_list'", (file_list_id,))
    assert cursor.fetchone()[0] == 0, "Association should be cascade deleted"

    evidence_conn.close()
    case_conn.close()


def test_file_list_matches_foreign_key(tmp_path):
    """Test that file_list_matches foreign key constraint works."""
    case_folder = tmp_path / "test_case"
    case_folder.mkdir()
    case_db_path = case_folder / "TEST-001_surfsifter.sqlite"

    manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    case_conn = manager.get_case_conn()

    # Insert case and evidence
    case_conn.execute(
        "INSERT INTO cases (case_id, title, investigator, created_at_utc) VALUES ('TEST-001', 'Test Case', 'Tester', '2025-11-05T10:00:00Z')"
    )
    case_conn.commit()
    case_id = case_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    case_conn.execute(
        "INSERT INTO evidences (case_id, label, source_path, added_at_utc) VALUES (?, 'EV-001', '/test.e01', '2025-11-05T10:00:00Z')",
        (case_id,)
    )
    case_conn.commit()
    evidence_id = case_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    evidence_conn = manager.get_evidence_conn(evidence_id, label="EV-001")

    # Insert file list entry
    evidence_conn.execute("""
        INSERT INTO file_list (
            evidence_id, file_path, file_name, md5_hash, import_source, import_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (evidence_id, 'C:\\freeze.exe', 'freeze.exe', 'd41d8cd98f00b204e9800998ecf8427e', 'ftk', '2025-11-05 10:00:00'))
    evidence_conn.commit()
    file_list_id = evidence_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Insert match
    evidence_conn.execute("""
        INSERT INTO file_list_matches (
            evidence_id, file_list_id, reference_list_name, match_type, matched_value, matched_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (evidence_id, file_list_id, 'deepfreeze', 'hash', 'd41d8cd98f00b204e9800998ecf8427e', '2025-11-05 10:02:00'))
    evidence_conn.commit()

    # Verify match exists
    cursor = evidence_conn.execute("SELECT reference_list_name FROM file_list_matches WHERE file_list_id = ?", (file_list_id,))
    assert cursor.fetchone()[0] == 'deepfreeze'

    # Delete file_list entry (should cascade delete match)
    evidence_conn.execute("DELETE FROM file_list WHERE id = ?", (file_list_id,))
    evidence_conn.commit()

    # Verify match was deleted (cascade)
    cursor = evidence_conn.execute("SELECT COUNT(*) FROM file_list_matches WHERE file_list_id = ?", (file_list_id,))
    assert cursor.fetchone()[0] == 0, "Match should be cascade deleted"

    evidence_conn.close()
    case_conn.close()
