import sqlite3
import tempfile
from pathlib import Path
import pytest
from core.database import migrate
from core.database import EVIDENCE_MIGRATIONS_DIR


def test_unified_tagging_schema():
    """
    Test that the consolidated schema includes the unified tagging system.

    Note: This was previously test_migration_0005_unified_tagging which tested
    incremental migration from old tag tables. Since, migrations are
    consolidated into a single baseline with no backward compatibility.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "evidence_test.sqlite"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Apply consolidated schema
        migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

        evidence_id = 1

        # Verify tags table exists with correct schema
        conn.execute("""
            INSERT INTO tags (evidence_id, name, name_normalized, created_by)
            VALUES (?, 'Gambling', 'gambling', 'manual')
        """, (evidence_id,))

        conn.execute("""
            INSERT INTO tags (evidence_id, name, name_normalized, created_by)
            VALUES (?, 'Suspicious', 'suspicious', 'pattern_detection')
        """, (evidence_id,))

        # Insert URL and file_list for tagging
        conn.execute("""
            INSERT INTO urls (id, evidence_id, url, discovered_by)
            VALUES (1, ?, 'http://example.com', 'test')
        """, (evidence_id,))

        conn.execute("""
            INSERT INTO file_list (id, evidence_id, file_path, file_name, import_timestamp)
            VALUES (1, ?, '/path/to/file.txt', 'file.txt', datetime('now'))
        """, (evidence_id,))

        # Test tag associations
        gambling_tag_id = conn.execute(
            "SELECT id FROM tags WHERE name_normalized = 'gambling'"
        ).fetchone()['id']

        suspicious_tag_id = conn.execute(
            "SELECT id FROM tags WHERE name_normalized = 'suspicious'"
        ).fetchone()['id']

        conn.execute("""
            INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id, tagged_by)
            VALUES (?, ?, 'url', 1, 'manual')
        """, (gambling_tag_id, evidence_id))

        conn.execute("""
            INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id, tagged_by)
            VALUES (?, ?, 'file_list', 1, 'auto')
        """, (suspicious_tag_id, evidence_id))

        conn.commit()

        # Verify usage_count trigger works
        gambling_tag = conn.execute(
            "SELECT usage_count FROM tags WHERE id = ?", (gambling_tag_id,)
        ).fetchone()
        assert gambling_tag['usage_count'] == 1

        # Verify associations exist
        assocs = conn.execute("SELECT * FROM tag_associations").fetchall()
        assert len(assocs) == 2

        # Verify old tables (url_tags, file_list_tags) do NOT exist in new schema
        cursor = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name IN ('url_tags', 'file_list_tags')
        """)
        old_tables = cursor.fetchall()
        assert len(old_tables) == 0, "Old tag tables should not exist in consolidated schema"

        conn.close()
