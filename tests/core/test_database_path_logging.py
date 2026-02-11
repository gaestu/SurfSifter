"""Test database path logging in CaseDataAccess."""
import tempfile
from pathlib import Path
from unittest.mock import patch

from app.data.case_data import CaseDataAccess


def test_case_data_access_logs_default_database_path():
    """Test that CaseDataAccess logs when using default database."""
    import logging

    with tempfile.TemporaryDirectory() as tmpdir:
        case_folder = Path(tmpdir)
        db_path = case_folder / "test_surfsifter.sqlite"

        # Create empty database
        db_path.touch()

        # Capture log messages from _base module (where BaseDataAccess logs)
        with patch.object(logging.getLogger("app.data._base"), "info") as mock_info:
            # Create CaseDataAccess without db_path (uses default)
            case_data = CaseDataAccess(case_folder)

            # Verify logging was called at least once
            assert mock_info.call_count >= 1

            # Check that one of the log messages mentions default database
            all_calls = [str(call) for call in mock_info.call_args_list]
            assert any("default database" in call.lower() for call in all_calls)


def test_case_data_access_logs_specified_database_path():
    """Test that CaseDataAccess logs when using specified database."""
    import logging

    with tempfile.TemporaryDirectory() as tmpdir:
        case_folder = Path(tmpdir)
        custom_db_path = case_folder / "CUSTOM-001_surfsifter.sqlite"

        # Create empty database
        custom_db_path.touch()

        # Capture log messages from _base module (where BaseDataAccess logs)
        with patch.object(logging.getLogger("app.data._base"), "info") as mock_info:
            # Create CaseDataAccess with custom db_path
            case_data = CaseDataAccess(case_folder, custom_db_path)

            # Verify logging was called at least once
            assert mock_info.call_count >= 1

            # Check that one of the log messages mentions specified database
            all_calls = [str(call) for call in mock_info.call_args_list]
            assert any("specified database" in call.lower() for call in all_calls)


def test_iter_urls_logs_query_results(tmp_path):
    """Test that iter_urls logs the query details."""
    import logging
    from core.database import DatabaseManager

    case_folder = tmp_path / "case"

    # Initialize database with schema using DatabaseManager
    case_db_path = case_folder / "test_surfsifter.sqlite"
    manager = DatabaseManager(case_folder, case_db_path=case_db_path)

    # Insert test data into case DB
    case_conn = manager.get_case_conn()
    with case_conn:
        cur = case_conn.execute(
            "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("TEST001", "Test Case", "2025-11-01T00:00:00Z")
        )
        case_db_id = cur.lastrowid

        case_conn.execute(
            "INSERT INTO evidences(id, case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?, ?)",
            (1, case_db_id, "Test Evidence", "/test/path", "2025-11-01T00:00:00Z")
        )

    # Insert test URLs into evidence DB
    evidence_conn = manager.get_evidence_conn(1, "Test Evidence")
    with evidence_conn:
        evidence_conn.execute(
            """INSERT INTO urls(evidence_id, url, domain, scheme, discovered_by)
               VALUES (?, ?, ?, ?, ?)""",
            (1, "http://example.com", "example.com", "http", "bulk_extractor")
        )
    evidence_conn.close()

    # Capture log messages
    # iter_urls moved to app.data._urls module
    with patch.object(logging.getLogger("app.data._urls"), "info") as mock_info:
        # Create CaseDataAccess and query URLs
        case_data = CaseDataAccess(case_folder)

        # Clear the __init__ logging call
        mock_info.reset_mock()

        # Query URLs
        results = case_data.iter_urls(evidence_id=1, limit=10)

        # Verify we got results
        assert len(results) == 1
        assert results[0]['url'] == "http://example.com"

        # Verify logging was called
        mock_info.assert_called_once()
        call_args = mock_info.call_args[0]
        assert "iter_urls" in call_args[0]
        assert "evidence_id=1" in call_args[0] or 1 in call_args
        assert "1 rows" in call_args[0] or 1 in call_args  # 1 row returned
