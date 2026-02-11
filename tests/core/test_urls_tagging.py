"""
Tests for URL tagging functionality using the unified tagging schema.
"""
import pytest
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime

from core.database import DatabaseManager


@pytest.fixture
def evidence_db(tmp_path):
    """Create evidence database with urls and unified tagging tables."""
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

    # Insert test URL entries
    evidence_conn.execute("""
        INSERT INTO urls (
            evidence_id, url, domain, scheme, discovered_by, first_seen_utc
        ) VALUES
        (1, 'https://example.com/page1', 'example.com', 'https', 'history', '2025-01-01T10:00:00Z'),
        (1, 'http://gambling.com/game', 'gambling.com', 'http', 'history', '2025-01-02T11:00:00Z'),
        (1, 'https://google.com/search', 'google.com', 'https', 'history', '2025-01-03T12:00:00Z')
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


def tag_url(conn, url_id, tag_id):
    """Helper to tag a URL."""
    conn.execute(
        """
        INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id, tagged_by, tagged_at_utc)
        VALUES (?, 1, 'url', ?, 'test', '2025-11-10T15:00:00Z')
        """,
        (tag_id, url_id)
    )


def test_tag_insertion(evidence_db):
    """Test inserting tags for URLs."""
    # Create tag
    tag_id = create_tag(evidence_db, "Suspicious")

    # Tag first URL
    tag_url(evidence_db, 1, tag_id)
    evidence_db.commit()

    # Verify tag was inserted
    cursor = evidence_db.execute(
        """
        SELECT t.name
        FROM tags t
        JOIN tag_associations ta ON t.id = ta.tag_id
        WHERE ta.artifact_type = 'url' AND ta.artifact_id = 1
        """
    )
    result = cursor.fetchone()
    assert result is not None
    assert result[0] == "Suspicious"


def test_duplicate_tag_prevention(evidence_db):
    """Test that duplicate tags (same URL + same tag) are prevented."""
    tag_id = create_tag(evidence_db, "Evidence A")

    # Insert first tag
    tag_url(evidence_db, 1, tag_id)
    evidence_db.commit()

    # Try to insert duplicate tag (should fail due to unique constraint on tag_associations)
    with pytest.raises(sqlite3.IntegrityError):
        evidence_db.execute(
            """
            INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id, tagged_by, tagged_at_utc)
            VALUES (?, 1, 'url', ?, 'test', '2025-11-10T16:00:00Z')
            """,
            (tag_id, 1)
        )
        evidence_db.commit()


def test_multiple_tags_same_url(evidence_db):
    """Test that a URL can have multiple different tags."""
    tag1_id = create_tag(evidence_db, "Suspicious")
    tag2_id = create_tag(evidence_db, "Gambling")

    # Tag URL with two different tags
    tag_url(evidence_db, 1, tag1_id)
    tag_url(evidence_db, 1, tag2_id)
    evidence_db.commit()

    # Verify both tags exist
    cursor = evidence_db.execute(
        """
        SELECT t.name
        FROM tags t
        JOIN tag_associations ta ON t.id = ta.tag_id
        WHERE ta.artifact_type = 'url' AND ta.artifact_id = 1
        ORDER BY t.name
        """
    )
    tags = [row[0] for row in cursor.fetchall()]
    assert tags == ["Gambling", "Suspicious"]


def test_same_tag_multiple_urls(evidence_db):
    """Test that the same tag can be applied to multiple URLs."""
    tag_id = create_tag(evidence_db, "Evidence A")

    # Tag two different URLs with same tag
    tag_url(evidence_db, 1, tag_id)
    tag_url(evidence_db, 2, tag_id)
    evidence_db.commit()

    # Verify tag appears for both URLs
    cursor = evidence_db.execute(
        """
        SELECT COUNT(*)
        FROM tag_associations ta
        JOIN tags t ON ta.tag_id = t.id
        WHERE t.name = 'Evidence A' AND ta.artifact_type = 'url'
        """
    )
    count = cursor.fetchone()[0]
    assert count == 2


def test_tag_removal(evidence_db):
    """Test removing tags from URLs."""
    tag1_id = create_tag(evidence_db, "Tag1")
    tag2_id = create_tag(evidence_db, "Tag2")

    # Insert tags
    tag_url(evidence_db, 1, tag1_id)
    tag_url(evidence_db, 2, tag2_id)
    evidence_db.commit()

    # Remove tag from first URL
    evidence_db.execute(
        "DELETE FROM tag_associations WHERE artifact_type = 'url' AND artifact_id = 1"
    )
    evidence_db.commit()

    # Verify only second URL has tags
    cursor = evidence_db.execute("SELECT COUNT(*) FROM tag_associations WHERE artifact_type = 'url'")
    count = cursor.fetchone()[0]
    assert count == 1

    cursor = evidence_db.execute(
        """
        SELECT ta.artifact_id
        FROM tag_associations ta
        JOIN tags t ON ta.tag_id = t.id
        WHERE t.name = 'Tag2' AND ta.artifact_type = 'url'
        """
    )
    result = cursor.fetchone()
    assert result[0] == 2


def test_tag_query_with_url_info(evidence_db):
    """Test querying tags with associated URL information."""
    tag1_id = create_tag(evidence_db, "Safe")
    tag2_id = create_tag(evidence_db, "Gambling")

    # Tag URLs
    tag_url(evidence_db, 1, tag1_id)  # example.com
    tag_url(evidence_db, 2, tag2_id)  # gambling.com
    evidence_db.commit()

    # Query tags with URL information
    cursor = evidence_db.execute(
        """
        SELECT u.domain, t.name
        FROM urls u
        JOIN tag_associations ta ON u.id = ta.artifact_id
        JOIN tags t ON ta.tag_id = t.id
        WHERE u.evidence_id = 1 AND ta.artifact_type = 'url'
        ORDER BY u.domain
        """
    )
    results = cursor.fetchall()

    assert len(results) == 2
    # Convert Row objects to tuples for comparison
    assert tuple(results[0]) == ("example.com", "Safe")
    assert tuple(results[1]) == ("gambling.com", "Gambling")
