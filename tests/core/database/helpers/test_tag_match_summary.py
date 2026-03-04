"""
Tests for get_tag_artifact_summary() and get_match_summary() helpers.
"""
import sqlite3
from pathlib import Path

import pytest

from core.database import DatabaseManager
from core.database.helpers.tags import get_tag_artifact_summary, get_match_summary


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def evidence_db(tmp_path):
    """Create an evidence database via DatabaseManager (applies migrations)."""
    case_path = tmp_path / "test_case"
    case_path.mkdir()

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
        "INSERT INTO evidences (id, label, source_path, evidence_slug) "
        "VALUES (1, 'EV-001', '/test.E01', 'ev-001')"
    )
    case_conn.commit()
    case_conn.close()

    db_manager = DatabaseManager(case_path, case_db_path=case_db_path)
    conn = db_manager.get_evidence_conn(1, label="EV-001")
    return conn


EVIDENCE_ID = 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_tag(conn, name: str) -> int:
    """Insert a tag and return its id."""
    cursor = conn.execute(
        "INSERT INTO tags (evidence_id, name, name_normalized, created_by) "
        "VALUES (?, ?, ?, 'test')",
        (EVIDENCE_ID, name, name.lower()),
    )
    conn.commit()
    return cursor.lastrowid


def _tag_artifact(conn, tag_id: int, artifact_type: str, artifact_id: int) -> None:
    """Create a tag association."""
    conn.execute(
        "INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id, tagged_by) "
        "VALUES (?, ?, ?, ?, 'test')",
        (tag_id, EVIDENCE_ID, artifact_type, artifact_id),
    )
    conn.commit()


def _insert_url(conn, url_id: int, url: str = "https://example.com") -> None:
    conn.execute(
        "INSERT INTO urls (id, evidence_id, url, domain, discovered_by) "
        "VALUES (?, ?, ?, 'example.com', 'test')",
        (url_id, EVIDENCE_ID, url),
    )
    conn.commit()


def _insert_image(conn, image_id: int) -> None:
    conn.execute(
        "INSERT INTO images (id, evidence_id, rel_path, filename, md5, first_discovered_by) "
        "VALUES (?, ?, 'img/test.jpg', 'test.jpg', 'abc123', 'test')",
        (image_id, EVIDENCE_ID),
    )
    conn.commit()


def _insert_file_list_entry(conn, fid: int) -> None:
    conn.execute(
        "INSERT INTO file_list (id, evidence_id, file_path, file_name, extension, "
        "size_bytes, modified_ts, import_source, import_timestamp) "
        "VALUES (?, ?, '/path/file.txt', 'file.txt', 'txt', 100, "
        "'2025-01-01T00:00:00Z', 'ftk', '2025-01-01T00:00:00Z')",
        (fid, EVIDENCE_ID),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests – get_tag_artifact_summary
# ---------------------------------------------------------------------------

class TestGetTagArtifactSummary:

    def test_empty_returns_empty_list(self, evidence_db):
        """No tags → empty result."""
        result = get_tag_artifact_summary(evidence_db, EVIDENCE_ID)
        assert result == []

    def test_single_tag_single_type(self, evidence_db):
        """One tag with two URL associations → one row."""
        _insert_url(evidence_db, 1, "https://a.com")
        _insert_url(evidence_db, 2, "https://b.com")
        tag_id = _create_tag(evidence_db, "Suspicious")
        _tag_artifact(evidence_db, tag_id, "url", 1)
        _tag_artifact(evidence_db, tag_id, "url", 2)

        result = get_tag_artifact_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 1
        assert result[0]["tag_name"] == "Suspicious"
        assert result[0]["artifact_type"] == "url"
        assert result[0]["count"] == 2

    def test_single_tag_multiple_types(self, evidence_db):
        """One tag spanning URLs and images."""
        _insert_url(evidence_db, 1)
        _insert_image(evidence_db, 1)
        tag_id = _create_tag(evidence_db, "Evidence")
        _tag_artifact(evidence_db, tag_id, "url", 1)
        _tag_artifact(evidence_db, tag_id, "image", 1)

        result = get_tag_artifact_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 2
        types = {r["artifact_type"] for r in result}
        assert types == {"url", "image"}
        for r in result:
            assert r["count"] == 1

    def test_multiple_tags(self, evidence_db):
        """Two tags, each with different associations."""
        _insert_url(evidence_db, 1)
        _insert_image(evidence_db, 1)
        _insert_image(evidence_db, 2)

        tag_a = _create_tag(evidence_db, "Alpha")
        tag_b = _create_tag(evidence_db, "Beta")

        _tag_artifact(evidence_db, tag_a, "url", 1)
        _tag_artifact(evidence_db, tag_b, "image", 1)
        _tag_artifact(evidence_db, tag_b, "image", 2)

        result = get_tag_artifact_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 2

        alpha_rows = [r for r in result if r["tag_name"] == "Alpha"]
        beta_rows = [r for r in result if r["tag_name"] == "Beta"]

        assert len(alpha_rows) == 1
        assert alpha_rows[0]["artifact_type"] == "url"
        assert alpha_rows[0]["count"] == 1

        assert len(beta_rows) == 1
        assert beta_rows[0]["artifact_type"] == "image"
        assert beta_rows[0]["count"] == 2

    def test_ordering_by_tag_name(self, evidence_db):
        """Results should be ordered by tag name (case-insensitive)."""
        _insert_url(evidence_db, 1)
        tag_z = _create_tag(evidence_db, "Zebra")
        tag_a = _create_tag(evidence_db, "apple")
        _tag_artifact(evidence_db, tag_z, "url", 1)
        _tag_artifact(evidence_db, tag_a, "url", 1)

        result = get_tag_artifact_summary(evidence_db, EVIDENCE_ID)
        names = [r["tag_name"] for r in result]
        assert names == ["apple", "Zebra"]

    def test_different_evidence_ids_isolated(self, evidence_db):
        """Tags for evidence_id=2 should not appear in summary for evidence_id=1."""
        _insert_url(evidence_db, 1)
        tag_id = _create_tag(evidence_db, "Isolated")
        _tag_artifact(evidence_db, tag_id, "url", 1)

        # Should find data for our evidence
        result = get_tag_artifact_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 1

        # Should not find data for nonexistent evidence
        result_other = get_tag_artifact_summary(evidence_db, 9999)
        assert result_other == []


# ---------------------------------------------------------------------------
# Tests – get_match_summary
# ---------------------------------------------------------------------------

class TestGetMatchSummary:

    def test_empty_returns_empty_list(self, evidence_db):
        """No matches → empty result."""
        result = get_match_summary(evidence_db, EVIDENCE_ID)
        assert result == []

    def test_url_matches_only(self, evidence_db):
        """URL matches grouped by list name."""
        _insert_url(evidence_db, 1, "https://bad.com")
        _insert_url(evidence_db, 2, "https://evil.com")

        evidence_db.execute(
            "INSERT INTO url_matches (evidence_id, url_id, list_name, match_type, matched_pattern) "
            "VALUES (?, ?, 'blocklist', 'exact', 'bad.com')",
            (EVIDENCE_ID, 1),
        )
        evidence_db.execute(
            "INSERT INTO url_matches (evidence_id, url_id, list_name, match_type, matched_pattern) "
            "VALUES (?, ?, 'blocklist', 'exact', 'evil.com')",
            (EVIDENCE_ID, 2),
        )
        evidence_db.commit()

        result = get_match_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 1
        assert result[0]["list_name"] == "blocklist"
        assert result[0]["url_count"] == 2
        assert result[0]["image_count"] == 0
        assert result[0]["file_count"] == 0

    def test_hash_matches_only(self, evidence_db):
        """Image hash matches."""
        _insert_image(evidence_db, 1)

        evidence_db.execute(
            "INSERT INTO hash_matches (evidence_id, image_id, db_name, db_md5, hash_sha256, list_name) "
            "VALUES (?, ?, 'known_bad', 'md5here', 'sha256here', 'csam_list')",
            (EVIDENCE_ID, 1),
        )
        evidence_db.commit()

        result = get_match_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 1
        assert result[0]["list_name"] == "csam_list"
        assert result[0]["url_count"] == 0
        assert result[0]["image_count"] == 1
        assert result[0]["file_count"] == 0

    def test_file_list_matches_only(self, evidence_db):
        """File list matches use reference_list_name column."""
        _insert_file_list_entry(evidence_db, 1)

        evidence_db.execute(
            "INSERT INTO file_list_matches (evidence_id, file_list_id, reference_list_name, "
            "match_type, matched_value, matched_at) "
            "VALUES (?, ?, 'deepfreeze', 'path', 'freeze.exe', '2025-01-01T00:00:00Z')",
            (EVIDENCE_ID, 1),
        )
        evidence_db.commit()

        result = get_match_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 1
        assert result[0]["list_name"] == "deepfreeze"
        assert result[0]["url_count"] == 0
        assert result[0]["image_count"] == 0
        assert result[0]["file_count"] == 1

    def test_mixed_matches_same_list(self, evidence_db):
        """URL and file matches sharing the same list name merge into one row."""
        _insert_url(evidence_db, 1, "https://bad.com")
        _insert_file_list_entry(evidence_db, 1)

        evidence_db.execute(
            "INSERT INTO url_matches (evidence_id, url_id, list_name, match_type, matched_pattern) "
            "VALUES (?, ?, 'shared_list', 'exact', 'bad.com')",
            (EVIDENCE_ID, 1),
        )
        evidence_db.execute(
            "INSERT INTO file_list_matches (evidence_id, file_list_id, reference_list_name, "
            "match_type, matched_value, matched_at) "
            "VALUES (?, ?, 'shared_list', 'path', 'file.txt', '2025-01-01T00:00:00Z')",
            (EVIDENCE_ID, 1),
        )
        evidence_db.commit()

        result = get_match_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 1
        assert result[0]["list_name"] == "shared_list"
        assert result[0]["url_count"] == 1
        assert result[0]["file_count"] == 1

    def test_multiple_lists_sorted(self, evidence_db):
        """Multiple reference lists are returned sorted by name."""
        _insert_url(evidence_db, 1, "https://x.com")
        _insert_url(evidence_db, 2, "https://y.com")

        evidence_db.execute(
            "INSERT INTO url_matches (evidence_id, url_id, list_name, match_type, matched_pattern) "
            "VALUES (?, ?, 'zebra_list', 'exact', 'x.com')",
            (EVIDENCE_ID, 1),
        )
        evidence_db.execute(
            "INSERT INTO url_matches (evidence_id, url_id, list_name, match_type, matched_pattern) "
            "VALUES (?, ?, 'alpha_list', 'exact', 'y.com')",
            (EVIDENCE_ID, 2),
        )
        evidence_db.commit()

        result = get_match_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 2
        names = [r["list_name"] for r in result]
        assert names == ["alpha_list", "zebra_list"]

    def test_distinct_counting(self, evidence_db):
        """Duplicate url_id entries for the same list should be counted once."""
        _insert_url(evidence_db, 1, "https://dup.com")

        # Two match rows for the same URL in the same list (different patterns)
        evidence_db.execute(
            "INSERT INTO url_matches (evidence_id, url_id, list_name, match_type, matched_pattern) "
            "VALUES (?, ?, 'dup_list', 'exact', 'dup.com')",
            (EVIDENCE_ID, 1),
        )
        evidence_db.execute(
            "INSERT INTO url_matches (evidence_id, url_id, list_name, match_type, matched_pattern) "
            "VALUES (?, ?, 'dup_list', 'wildcard', '*.dup.com')",
            (EVIDENCE_ID, 1),
        )
        evidence_db.commit()

        result = get_match_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 1
        assert result[0]["url_count"] == 1  # DISTINCT url_id

    def test_different_evidence_ids_isolated(self, evidence_db):
        """Matches for a different evidence_id should not appear."""
        _insert_url(evidence_db, 1, "https://bad.com")
        evidence_db.execute(
            "INSERT INTO url_matches (evidence_id, url_id, list_name, match_type, matched_pattern) "
            "VALUES (?, ?, 'test_list', 'exact', 'bad.com')",
            (EVIDENCE_ID, 1),
        )
        evidence_db.commit()

        result = get_match_summary(evidence_db, EVIDENCE_ID)
        assert len(result) == 1

        result_other = get_match_summary(evidence_db, 9999)
        assert result_other == []
