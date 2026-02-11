"""
Tests for evidence database existence guards.

These tests verify that read-only methods return sensible defaults
when the evidence database doesn't exist yet, preventing auto-creation
and errors in background workers.
"""

from pathlib import Path

import pytest

from core.database import init_db
from core.database import DatabaseManager
from app.data.case_data import CaseDataAccess, EvidenceCounts


def create_case_db(tmp_path: Path) -> Path:
    """Create a case database with minimal setup."""
    case_db_path = tmp_path / "test_surfsifter.sqlite"
    init_db(tmp_path, case_db_path)

    # Insert a case and evidence record (but no evidence DB)
    import sqlite3
    conn = sqlite3.connect(case_db_path)
    conn.execute(
        "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
        ("CASE-1", "Test Case", "2025-01-01T00:00:00Z"),
    )
    conn.execute(
        "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
        (1, "Test Evidence", "/dev/null", "2025-01-01T00:00:00Z"),
    )
    conn.commit()
    conn.close()

    return case_db_path


class TestDatabaseManagerEvidenceDbExists:
    """Tests for DatabaseManager.evidence_db_exists()."""

    def test_evidence_db_exists_returns_false_for_missing(self, tmp_path: Path):
        """evidence_db_exists() returns False when DB not created."""
        case_db_path = create_case_db(tmp_path)
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)

        # Evidence DB should not exist yet (we only created case DB)
        assert manager.evidence_db_exists(1, "Test Evidence") is False

        manager.close_all()

    def test_evidence_db_exists_returns_true_after_creation(self, tmp_path: Path):
        """evidence_db_exists() returns True after DB created."""
        case_db_path = create_case_db(tmp_path)
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)

        # Create evidence DB by getting a connection
        conn = manager.get_evidence_conn(1, "Test Evidence")
        conn.execute("SELECT 1")  # Force creation

        # Now it should exist
        assert manager.evidence_db_exists(1, "Test Evidence") is True

        manager.close_all()

    def test_evidence_db_exists_does_not_create_db(self, tmp_path: Path):
        """evidence_db_exists() check doesn't create the database."""
        case_db_path = create_case_db(tmp_path)
        manager = DatabaseManager(tmp_path, case_db_path=case_db_path)

        # Check existence multiple times
        for _ in range(3):
            assert manager.evidence_db_exists(1, "Test Evidence") is False

        # DB should still not exist
        db_path = manager.evidence_db_path(1, "Test Evidence", create_dirs=False)
        assert not db_path.exists()

        manager.close_all()


class TestCaseDataAccessGuards:
    """Tests for CaseDataAccess read-only method guards."""

    def test_get_evidence_counts_returns_empty_before_extraction(self, tmp_path: Path):
        """get_evidence_counts() returns empty counts when evidence DB missing."""
        case_db_path = create_case_db(tmp_path)

        with CaseDataAccess(tmp_path, case_db_path) as cda:
            counts = cda.get_evidence_counts(1)

            assert isinstance(counts, EvidenceCounts)
            assert counts.urls == 0
            assert counts.images == 0
            assert counts.indicators == 0
            assert counts.last_run_utc is None

    def test_list_tags_returns_empty_before_extraction(self, tmp_path: Path):
        """list_tags() returns [] when evidence DB missing."""
        case_db_path = create_case_db(tmp_path)

        with CaseDataAccess(tmp_path, case_db_path) as cda:
            tags = cda.list_tags(1)
            assert tags == []

    def test_list_url_domains_returns_empty_before_extraction(self, tmp_path: Path):
        """list_url_domains() returns [] when evidence DB missing."""
        case_db_path = create_case_db(tmp_path)

        with CaseDataAccess(tmp_path, case_db_path) as cda:
            domains = cda.list_url_domains(1)
            assert domains == []

    def test_list_url_match_lists_returns_empty_before_extraction(self, tmp_path: Path):
        """list_url_match_lists() returns [] when evidence DB missing."""
        case_db_path = create_case_db(tmp_path)

        with CaseDataAccess(tmp_path, case_db_path) as cda:
            lists = cda.list_url_match_lists(1)
            assert lists == []

    def test_list_downloadable_urls_returns_empty_before_extraction(self, tmp_path: Path):
        """list_downloadable_urls() returns [] when evidence DB missing."""
        case_db_path = create_case_db(tmp_path)

        with CaseDataAccess(tmp_path, case_db_path) as cda:
            urls = cda.list_downloadable_urls(1)
            assert urls == []

    def test_count_downloadable_urls_returns_zero_before_extraction(self, tmp_path: Path):
        """count_downloadable_urls() returns 0 when evidence DB missing."""
        case_db_path = create_case_db(tmp_path)

        with CaseDataAccess(tmp_path, case_db_path) as cda:
            count = cda.count_downloadable_urls(1)
            assert count == 0

    def test_list_downloads_returns_empty_before_extraction(self, tmp_path: Path):
        """list_downloads() returns [] when evidence DB missing."""
        case_db_path = create_case_db(tmp_path)

        with CaseDataAccess(tmp_path, case_db_path) as cda:
            downloads = cda.list_downloads(1)
            assert downloads == []

    def test_count_downloads_returns_zero_before_extraction(self, tmp_path: Path):
        """count_downloads() returns 0 when evidence DB missing."""
        case_db_path = create_case_db(tmp_path)

        with CaseDataAccess(tmp_path, case_db_path) as cda:
            count = cda.count_downloads(1)
            assert count == 0

    def test_get_download_stats_returns_empty_before_extraction(self, tmp_path: Path):
        """get_download_stats() returns {} when evidence DB missing."""
        case_db_path = create_case_db(tmp_path)

        with CaseDataAccess(tmp_path, case_db_path) as cda:
            stats = cda.get_download_stats(1)
            assert stats == {}


class TestGuardPreventsDbCreation:
    """Tests verifying guards prevent database auto-creation."""

    def test_no_database_created_when_guards_active(self, tmp_path: Path):
        """Verify guard prevents database auto-creation when calling read-only methods."""
        case_db_path = create_case_db(tmp_path)

        with CaseDataAccess(tmp_path, case_db_path) as cda:
            # Call all guarded methods
            cda.get_evidence_counts(1)
            cda.list_tags(1)
            cda.list_url_domains(1)
            cda.list_url_match_lists(1)
            cda.list_downloadable_urls(1)
            cda.count_downloadable_urls(1)
            cda.list_downloads(1)
            cda.count_downloads(1)
            cda.get_download_stats(1)

        # Evidence DB should NOT have been created
        expected_db_path = tmp_path / "evidences" / "test-evidence" / "evidence_test-evidence.sqlite"
        assert not expected_db_path.exists(), f"Evidence DB was created unexpectedly: {expected_db_path}"
