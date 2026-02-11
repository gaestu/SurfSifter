"""Test that UrlsTableModel handles initialization order correctly."""
import tempfile
from pathlib import Path

from app.data.case_data import CaseDataAccess
from app.features.urls.models import UrlsTableModel
from core.database import DatabaseManager


def test_urls_model_requires_case_data_before_evidence():
    """Test that setting evidence_id before case_data results in empty model.

    This test documents the initialization order dependency that was causing
    the URLs tab to remain empty.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        case_folder = Path(tmpdir) / "case"

        # Setup database with test data
        case_db_path = case_folder / "test_surfsifter.sqlite"
        manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()
        with case_conn:
            cur = case_conn.execute(
                "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                ("TEST001", "Test Case", "2025-11-01T00:00:00Z")
            )
            case_db_id = cur.lastrowid

            cur = case_conn.execute(
                "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                (case_db_id, "Test Evidence", "/test/path", "2025-11-01T00:00:00Z")
            )
            evidence_id = cur.lastrowid

        # Insert 3 test URLs into evidence DB
        evidence_conn = manager.get_evidence_conn(evidence_id, "Test Evidence")
        with evidence_conn:
            for i in range(3):
                evidence_conn.execute(
                    """INSERT INTO urls(evidence_id, url, domain, scheme, discovered_by)
                       VALUES (?, ?, ?, ?, ?)""",
                    (evidence_id, f"http://example{i}.com", f"example{i}.com", "http", "test")
                )
        evidence_conn.close()

        case_data = CaseDataAccess(case_folder)

        # Test 1: WRONG ORDER - evidence before case_data
        model_wrong = UrlsTableModel()
        model_wrong.set_evidence(evidence_id)  # This calls reload() with case_data=None
        model_wrong.set_case_data(case_data)   # This doesn't call reload()

        assert model_wrong.rowCount() == 0, "Model should be empty with wrong initialization order"

        # Test 2: CORRECT ORDER - case_data before evidence
        model_correct = UrlsTableModel()
        model_correct.set_case_data(case_data)  # Set case_data first
        model_correct.set_evidence(evidence_id)  # This calls reload() with case_data available

        assert model_correct.rowCount() == 3, "Model should have 3 rows with correct initialization order"


def test_urls_model_set_case_data_clears_evidence_id():
    """Test that set_case_data() clears evidence_id, preventing reload() fix.

    This documents why wrong initialization order CANNOT be fixed by calling
    reload() afterwards - set_case_data() resets evidence_id to None.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        case_folder = Path(tmpdir) / "case"

        # Setup database
        case_db_path = case_folder / "test_surfsifter.sqlite"
        manager = DatabaseManager(case_folder, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()
        with case_conn:
            cur = case_conn.execute(
                "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                ("TEST001", "Test Case", "2025-11-01T00:00:00Z")
            )
            case_db_id = cur.lastrowid

            cur = case_conn.execute(
                "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                (case_db_id, "Test Evidence", "/test/path", "2025-11-01T00:00:00Z")
            )
            evidence_id = cur.lastrowid

        # Insert URL into evidence DB
        evidence_conn = manager.get_evidence_conn(evidence_id, "Test Evidence")
        with evidence_conn:
            evidence_conn.execute(
                """INSERT INTO urls(evidence_id, url, domain, scheme, discovered_by)
                   VALUES (?, ?, ?, ?, ?)""",
                (evidence_id, "http://example.com", "example.com", "http", "test")
            )
        evidence_conn.close()

        case_data = CaseDataAccess(case_folder)

        # Initialize in wrong order
        model = UrlsTableModel()
        model.set_evidence(evidence_id)  # Sets evidence_id = 1
        assert model.evidence_id == evidence_id, "Evidence ID should be set"

        model.set_case_data(case_data)  # Clears evidence_id to None!

        assert model.evidence_id is None, "set_case_data() clears evidence_id"
        assert model.rowCount() == 0, "Model is empty because evidence_id was cleared"

        # Even calling reload() cannot fix it because evidence_id is None
        model.reload()
        assert model.rowCount() == 0, "reload() still returns empty because evidence_id is None"

        # The ONLY fix is to set evidence_id again
        model.set_evidence(evidence_id)
        assert model.rowCount() == 1, "Setting evidence_id again loads the data"