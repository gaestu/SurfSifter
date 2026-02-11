"""
Tests for File List tagging functionality using the unified tagging schema.
"""
import pytest
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime, timezone

from core.database import DatabaseManager


@pytest.fixture
def evidence_db(tmp_path):
    """Create evidence database with file_list and unified tagging tables."""
    case_path = tmp_path / "test_case"
    case_path.mkdir()

    # Create minimal case database
    case_db_path = case_path / "CASE-2025-001_surfsifter.sqlite"
    case_conn = sqlite3.connect(case_db_path)
    case_conn.execute("""
        CREATE TABLE IF NOT EXISTS evidences (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            source_path TEXT NOT NULL,
            evidence_slug TEXT NOT NULL
        )
    """)
    case_conn.execute(
        "INSERT INTO evidences (id, label, source_path, evidence_slug) VALUES (1, 'EV-001', '/test.E01', 'ev-001')"
    )
    case_conn.commit()
    case_conn.close()

    # Create evidence database via DatabaseManager (applies migrations)
    db_manager = DatabaseManager(case_path, case_db_path=case_db_path)
    evidence_conn = db_manager.get_evidence_conn(1, label="EV-001")

    # Insert test file list entries
    evidence_conn.execute("""
        INSERT INTO file_list (
            evidence_id, file_path, file_name, extension, size_bytes,
            modified_ts, import_source, import_timestamp
        ) VALUES
        (1, 'C:\\Windows\\System32\\freeze.exe', 'freeze.exe', 'exe', 1024, '2025-01-01T10:00:00Z', 'ftk', '2025-11-10T12:00:00Z'),
        (1, 'C:\\Temp\\casino.dll', 'casino.dll', 'dll', 2048, '2025-01-02T11:00:00Z', 'ftk', '2025-11-10T12:00:00Z'),
        (1, 'C:\\Program Files\\app.exe', 'app.exe', 'exe', 4096, '2025-01-03T12:00:00Z', 'ftk', '2025-11-10T12:00:00Z')
    """)
    evidence_conn.commit()

    return evidence_conn


def create_tag(conn, name):
    """Helper to create a tag."""
    cursor = conn.execute(
        "INSERT INTO tags (evidence_id, name, name_normalized, created_by) VALUES (1, ?, ?, 'test')",
        (name, name.lower())
    )
    return cursor.lastrowid


def tag_file(conn, file_id, tag_id):
    """Helper to tag a file."""
    conn.execute(
        """
        INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id, tagged_by, tagged_at_utc)
        VALUES (?, 1, 'file_list', ?, 'test', '2025-11-10T15:00:00Z')
        """,
        (tag_id, file_id)
    )


def test_tag_insertion(evidence_db):
    """Test inserting tags for file list entries."""
    # Create tag
    tag_id = create_tag(evidence_db, "DeepFreeze Evidence")

    # Tag first file
    tag_file(evidence_db, 1, tag_id)
    evidence_db.commit()

    # Verify tag was inserted
    cursor = evidence_db.execute(
        """
        SELECT t.name
        FROM tags t
        JOIN tag_associations ta ON t.id = ta.tag_id
        WHERE ta.artifact_type = 'file_list' AND ta.artifact_id = 1
        """
    )
    result = cursor.fetchone()
    assert result is not None
    assert result[0] == "DeepFreeze Evidence"


def test_duplicate_tag_prevention(evidence_db):
    """Test that duplicate tags (same file + same tag) are prevented."""
    tag_id = create_tag(evidence_db, "Evidence A")

    # Insert first tag
    tag_file(evidence_db, 1, tag_id)
    evidence_db.commit()

    # Try to insert duplicate tag (should fail due to unique constraint on tag_associations)
    with pytest.raises(sqlite3.IntegrityError):
        evidence_db.execute(
            """
            INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id, tagged_by, tagged_at_utc)
            VALUES (?, 1, 'file_list', ?, 'test', '2025-11-10T16:00:00Z')
            """,
            (tag_id, 1)
        )
        evidence_db.commit()


def test_multiple_tags_same_file(evidence_db):
    """Test that a file can have multiple different tags."""
    tag1_id = create_tag(evidence_db, "DeepFreeze")
    tag2_id = create_tag(evidence_db, "System Software")

    # Tag file with two different tags
    tag_file(evidence_db, 1, tag1_id)
    tag_file(evidence_db, 1, tag2_id)
    evidence_db.commit()

    # Verify both tags exist
    cursor = evidence_db.execute(
        """
        SELECT t.name
        FROM tags t
        JOIN tag_associations ta ON t.id = ta.tag_id
        WHERE ta.artifact_type = 'file_list' AND ta.artifact_id = 1
        ORDER BY t.name
        """
    )
    tags = [row[0] for row in cursor.fetchall()]
    assert tags == ["DeepFreeze", "System Software"]


def test_same_tag_multiple_files(evidence_db):
    """Test that the same tag can be applied to multiple files."""
    tag_id = create_tag(evidence_db, "Evidence A")

    # Tag two different files with same tag
    tag_file(evidence_db, 1, tag_id)
    tag_file(evidence_db, 2, tag_id)
    evidence_db.commit()

    # Verify tag appears for both files
    cursor = evidence_db.execute(
        """
        SELECT COUNT(*)
        FROM tag_associations ta
        JOIN tags t ON ta.tag_id = t.id
        WHERE t.name = 'Evidence A' AND ta.artifact_type = 'file_list'
        """
    )
    count = cursor.fetchone()[0]
    assert count == 2


def test_tag_removal(evidence_db):
    """Test removing tags from files."""
    tag1_id = create_tag(evidence_db, "Tag1")
    tag2_id = create_tag(evidence_db, "Tag2")

    # Insert tags
    tag_file(evidence_db, 1, tag1_id)
    tag_file(evidence_db, 2, tag2_id)
    evidence_db.commit()

    # Remove tag from first file
    evidence_db.execute(
        "DELETE FROM tag_associations WHERE artifact_type = 'file_list' AND artifact_id = 1"
    )
    evidence_db.commit()

    # Verify only second file has tags
    cursor = evidence_db.execute("SELECT COUNT(*) FROM tag_associations WHERE artifact_type = 'file_list'")
    count = cursor.fetchone()[0]
    assert count == 1

    cursor = evidence_db.execute(
        """
        SELECT ta.artifact_id
        FROM tag_associations ta
        JOIN tags t ON ta.tag_id = t.id
        WHERE t.name = 'Tag2' AND ta.artifact_type = 'file_list'
        """
    )
    result = cursor.fetchone()
    assert result[0] == 2


# def test_cascade_delete_tags_on_file_deletion(evidence_db):
#     """
#     Test that tags are automatically deleted when file is deleted (CASCADE).
#     NOTE: With unified tagging (tag_associations), there is no FK to file_list,
#     so cascade delete is not automatic at DB level unless triggers exist.
#     Skipping for now as this is likely application-level logic or requires triggers.
#     """
#     pass


def test_tag_query_with_file_info(evidence_db):
    """Test querying tags with associated file information."""
    tag1_id = create_tag(evidence_db, "DeepFreeze Evidence")
    tag2_id = create_tag(evidence_db, "Gambling Evidence")

    # Tag files
    tag_file(evidence_db, 1, tag1_id)  # freeze.exe
    tag_file(evidence_db, 2, tag2_id)  # casino.dll
    evidence_db.commit()

    # Query tags with file information
    cursor = evidence_db.execute(
        """
        SELECT fl.file_name, t.name
        FROM file_list fl
        JOIN tag_associations ta ON fl.id = ta.artifact_id
        JOIN tags t ON ta.tag_id = t.id
        WHERE fl.evidence_id = 1 AND ta.artifact_type = 'file_list'
        ORDER BY fl.file_name
        """
    )
    results = cursor.fetchall()

    assert len(results) == 2
    # Convert Row objects to tuples for comparison
    assert tuple(results[0]) == ("casino.dll", "Gambling Evidence")
    assert tuple(results[1]) == ("freeze.exe", "DeepFreeze Evidence")


def test_tag_grouping_for_reports(evidence_db):
    """Test grouping files by tag (report use case)."""
    tag_a_id = create_tag(evidence_db, "Evidence A")
    tag_b_id = create_tag(evidence_db, "Evidence B")

    # Tag files with overlapping tags
    tag_file(evidence_db, 1, tag_a_id)
    tag_file(evidence_db, 2, tag_a_id)
    tag_file(evidence_db, 3, tag_b_id)
    evidence_db.commit()

    # Group by tag and count files
    cursor = evidence_db.execute(
        """
        SELECT t.name, COUNT(*) as file_count
        FROM tag_associations ta
        JOIN tags t ON ta.tag_id = t.id
        WHERE ta.evidence_id = 1 AND ta.artifact_type = 'file_list'
        GROUP BY t.name
        ORDER BY t.name
        """
    )
    results = cursor.fetchall()

    assert len(results) == 2
    # Convert Row objects to tuples for comparison
    assert tuple(results[0]) == ("Evidence A", 2)
    assert tuple(results[1]) == ("Evidence B", 1)


def test_timestamp_format(evidence_db):
    """Test that timestamps are stored in ISO format."""
    tag_id = create_tag(evidence_db, "Test Tag")

    # Insert tag with known timestamp
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    evidence_db.execute(
        """
        INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id, tagged_by, tagged_at_utc)
        VALUES (?, 1, 'file_list', 1, 'test', ?)
        """,
        (tag_id, timestamp)
    )
    evidence_db.commit()

    # Verify timestamp format
    cursor = evidence_db.execute(
        """
        SELECT ta.tagged_at_utc
        FROM tag_associations ta
        JOIN tags t ON ta.tag_id = t.id
        WHERE t.name = 'Test Tag'
        """
    )
    result = cursor.fetchone()
    assert result is not None

    # Verify it ends with 'Z' (UTC indicator)
    assert result[0].endswith("Z")

    # Verify it can be parsed as ISO timestamp
    parsed = datetime.fromisoformat(result[0].replace("Z", "+00:00"))
    assert isinstance(parsed, datetime)
