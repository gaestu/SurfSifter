"""Test images table has unique SHA256 constraint in consolidated schema."""

import sqlite3
from pathlib import Path

from core.database import migrate
from core.database import EVIDENCE_MIGRATIONS_DIR


def test_images_sha256_unique_constraint(tmp_path: Path):
    """
    Test that the consolidated schema has unique SHA256 constraint.

    Note: This was previously test_migration_0007_deduplicates_images which tested
    incremental migration from a pre-0007 schema. Since, migrations are
    consolidated into a single baseline with no backward compatibility.
    images table no longer has discovered_by column, use first_discovered_by.
    """
    db_path = tmp_path / "evidence.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")

    # Apply consolidated schema
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

    # Insert first image
    conn.execute("""
        INSERT INTO images (evidence_id, rel_path, filename, sha256, first_discovered_by)
        VALUES (1, 'a.jpg', 'a.jpg', 'abc123', 'test')
    """)
    conn.commit()

    # Attempt to insert duplicate SHA256 for same evidence should fail
    try:
        conn.execute("""
            INSERT INTO images (evidence_id, rel_path, filename, sha256, first_discovered_by)
            VALUES (1, 'b.jpg', 'b.jpg', 'abc123', 'test')
        """)
        conn.commit()
        assert False, "Should have raised IntegrityError for duplicate SHA256"
    except sqlite3.IntegrityError:
        pass  # Expected behavior

    # Verify unique index exists
    index_row = conn.execute("""
        SELECT name FROM sqlite_master
        WHERE type='index' AND name='idx_images_evidence_sha256'
    """).fetchone()
    assert index_row is not None, "Unique index on images(evidence_id, sha256) should exist"

    # Verify only one image in table
    count = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    assert count == 1

    # Verify different evidence can have same SHA256 (constraint is per-evidence)
    conn.execute("""
        INSERT INTO images (evidence_id, rel_path, filename, sha256, first_discovered_by)
        VALUES (2, 'c.jpg', 'c.jpg', 'abc123', 'test')
    """)
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    assert count == 2, "Same SHA256 in different evidence should be allowed"

    conn.close()
