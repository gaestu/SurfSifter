"""
Tests for the case validation framework (src/core/validation.py).

Tests cover:
- Quick validation (automatic on case open)
- Full validation (on-demand)
- Individual validation checks
- Edge cases (missing files, corrupted data, schema mismatches)
"""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from core.validation import (
    QuickValidator,
    FullValidator,
    ValidationStatus,
    ValidationResult,
    WorkspaceStructureCheck,
    DatabaseSchemaCheck,
    EvidenceFilesCheck,
    ProcessLogConsistencyCheck,
    ToolDependencyCheck,
    validate_case_quick,
    validate_case_full,
)
from core.database import init_db
from core.database import DatabaseManager


@pytest.fixture
def valid_case_folder(tmp_path):
    """Create a valid case folder with proper structure."""
    case_folder = tmp_path / "test_case"
    case_folder.mkdir()

    # Create test_surfsifter.sqlite with migrations
    case_db_path = case_folder / "test_surfsifter.sqlite"
    init_db(case_folder, db_path=case_db_path)

    # Create evidences directory
    evidences_dir = case_folder / "evidences"
    evidences_dir.mkdir()

    return case_folder


@pytest.fixture
def case_with_evidence(valid_case_folder, tmp_path):
    """Create a case with one evidence item (mocked E01 file)."""
    # First, we need a case in the cases table
    conn = sqlite3.connect(valid_case_folder / "test_surfsifter.sqlite")
    conn.row_factory = sqlite3.Row

    # Insert a case first
    conn.execute(
        "INSERT INTO cases (case_id, title, investigator, created_at_utc) VALUES (?, ?, ?, datetime('now'))",
        ("TEST-CASE-001", "Test Case", "Test Investigator"),
    )
    case_id = conn.execute("SELECT id FROM cases WHERE case_id = ?", ("TEST-CASE-001",)).fetchone()["id"]

    # Create a mock E01 file
    mock_e01 = tmp_path / "test.E01"
    mock_e01.write_text("mock e01 data")

    # Add evidence to database
    conn.execute(
        "INSERT INTO evidences (case_id, label, source_path, size, added_at_utc) VALUES (?, ?, ?, ?, datetime('now'))",
        (case_id, "TEST-EV-001", str(mock_e01), 1024),
    )
    conn.commit()
    conn.close()

    return valid_case_folder, mock_e01


@pytest.fixture
def case_with_extraction(case_with_evidence):
    """Create a case with completed extraction (process log)."""
    case_folder, mock_e01 = case_with_evidence

    # Create evidence database
    case_db_path = case_folder / "test_surfsifter.sqlite"
    db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    evidence_conn = db_manager.get_evidence_conn(1, label="TEST-EV-001")

    # Insert process log entry (successful)
    evidence_conn.execute(
        """
        INSERT INTO process_log (evidence_id, task, command, started_at_utc, finished_at_utc, exit_code)
        VALUES (?, ?, ?, datetime('now'), datetime('now'), ?)
        """,
        (1, "bulk_extractor", "/usr/bin/bulk_extractor", 0),
    )

    evidence_conn.commit()
    evidence_conn.close()

    return case_folder


# ============================================================================
# Test: Quick Validation
# ============================================================================


def test_quick_validation_valid_case(valid_case_folder):
    """Test quick validation on a valid case."""
    report = validate_case_quick(valid_case_folder)

    # Note: Valid case without evidence will get WARNING (no evidence added yet)
    assert report.overall_status in (ValidationStatus.PASS, ValidationStatus.WARNING)
    assert report.error_count == 0
    assert report.duration_seconds < 3.0  # Should be fast
    assert len(report.results) == 3  # workspace, schema, evidence files


def test_quick_validation_missing_sqlite(tmp_path):
    """Test quick validation when *_surfsifter.sqlite is missing."""
    case_folder = tmp_path / "incomplete_case"
    case_folder.mkdir()
    (case_folder / "evidences").mkdir()

    report = validate_case_quick(case_folder)

    assert report.overall_status == ValidationStatus.ERROR
    assert report.error_count >= 1

    # Find the database schema check result
    db_check = next((r for r in report.results if "Database" in r.check_name), None)
    assert db_check is not None
    assert db_check.status == ValidationStatus.ERROR


def test_quick_validation_missing_evidences_dir(tmp_path):
    """Test quick validation when evidences directory is missing."""
    case_folder = tmp_path / "incomplete_case"
    case_folder.mkdir()
    case_db_path = case_folder / "test_surfsifter.sqlite"
    init_db(case_folder, db_path=case_db_path)  # Create test_surfsifter.sqlite but no evidences dir

    report = validate_case_quick(case_folder)

    assert report.overall_status == ValidationStatus.ERROR

    # Find workspace structure check
    ws_check = next((r for r in report.results if "Workspace" in r.check_name), None)
    assert ws_check is not None
    assert ws_check.status == ValidationStatus.ERROR
    assert "evidences" in ws_check.message.lower()


def test_quick_validation_duration(valid_case_folder):
    """Test that quick validation completes in under 3 seconds."""
    report = validate_case_quick(valid_case_folder)

    assert report.duration_seconds < 3.0
    assert report.duration_seconds > 0


# ============================================================================
# Test: Full Validation
# ============================================================================


def test_full_validation_valid_case(case_with_extraction):
    """Test full validation on a case with completed extraction."""
    report = validate_case_full(case_with_extraction)

    assert report.overall_status in (ValidationStatus.PASS, ValidationStatus.WARNING)
    assert report.duration_seconds < 10.0  # Should be reasonably fast
    assert len(report.results) == 5  # All checks (workspace, schema, evidence, process_log, tools)


def test_full_validation_failed_tool_execution(case_with_evidence):
    """Test full validation detects failed tool executions."""
    case_folder, _ = case_with_evidence

    # Add failed process log entry
    case_db_path = case_folder / "test_surfsifter.sqlite"
    db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    evidence_conn = db_manager.get_evidence_conn(1, label="TEST-EV-001")

    evidence_conn.execute(
        """
        INSERT INTO process_log (evidence_id, task, command, started_at_utc, finished_at_utc, exit_code)
        VALUES (?, ?, ?, datetime('now'), datetime('now'), ?)
        """,
        (1, "bulk_extractor", "/usr/bin/bulk_extractor", 1),  # exit_code=1 (failure)
    )
    evidence_conn.commit()
    evidence_conn.close()

    report = validate_case_full(case_folder)

    # Find process log check
    log_check = next((r for r in report.results if "Process Log" in r.check_name), None)
    assert log_check is not None
    assert log_check.status == ValidationStatus.WARNING
    assert "failed tool execution" in log_check.message.lower()


def test_full_validation_duration(valid_case_folder):
    """Test that full validation completes in under 10 seconds."""
    report = validate_case_full(valid_case_folder)

    assert report.duration_seconds < 10.0
    assert report.duration_seconds > 0


# ============================================================================
# Test: Individual Checks
# ============================================================================


def test_workspace_structure_check_valid(valid_case_folder):
    """Test workspace structure check on valid case."""
    check = WorkspaceStructureCheck(valid_case_folder)
    result = check.run()

    assert result.status == ValidationStatus.PASS
    assert "present" in result.message.lower()


def test_workspace_structure_check_missing_sqlite(tmp_path):
    """Test workspace structure check detects missing *_surfsifter.sqlite."""
    case_folder = tmp_path / "incomplete"
    case_folder.mkdir()
    (case_folder / "evidences").mkdir()

    check = WorkspaceStructureCheck(case_folder)
    result = check.run()

    assert result.status == ValidationStatus.ERROR
    assert "surfsifter" in result.message


def test_database_schema_check_valid(valid_case_folder):
    """Test database schema check on valid case."""
    check = DatabaseSchemaCheck(valid_case_folder)
    result = check.run()

    assert result.status == ValidationStatus.PASS
    assert result.details is not None
    assert "latest_version" in result.details


def test_database_schema_check_missing_db(tmp_path):
    """Test database schema check when database is missing."""
    case_folder = tmp_path / "no_db"
    case_folder.mkdir()

    check = DatabaseSchemaCheck(case_folder)
    result = check.run()

    assert result.status == ValidationStatus.ERROR
    assert "not found" in result.message.lower()


def test_database_schema_check_no_migrations(tmp_path):
    """Test database schema check when no migrations are applied."""
    case_folder = tmp_path / "no_migrations"
    case_folder.mkdir()

    # Create empty database without migrations
    case_db_path = case_folder / "test_surfsifter.sqlite"
    conn = sqlite3.connect(case_db_path)
    conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY, applied_at_utc TEXT);")
    conn.commit()
    conn.close()

    check = DatabaseSchemaCheck(case_folder)
    result = check.run()

    assert result.status == ValidationStatus.WARNING
    assert "no migrations" in result.message.lower()


def test_evidence_files_check_accessible(case_with_evidence):
    """Test evidence files check when files are accessible."""
    case_folder, _ = case_with_evidence

    check = EvidenceFilesCheck(case_folder)
    result = check.run()

    assert result.status == ValidationStatus.PASS
    assert "accessible" in result.message.lower()


def test_evidence_files_check_missing_file(case_with_evidence):
    """Test evidence files check detects missing E01 file."""
    case_folder, mock_e01 = case_with_evidence

    # Delete the mock E01 file
    mock_e01.unlink()

    check = EvidenceFilesCheck(case_folder)
    result = check.run()

    assert result.status == ValidationStatus.ERROR
    assert "inaccessible" in result.message.lower()


def test_evidence_files_check_no_evidence(valid_case_folder):
    """Test evidence files check when no evidence is added yet."""
    check = EvidenceFilesCheck(valid_case_folder)
    result = check.run()

    assert result.status == ValidationStatus.WARNING
    assert "no evidence" in result.message.lower()


def test_process_log_consistency_check_success(case_with_extraction):
    """Test process log consistency check with successful executions."""
    check = ProcessLogConsistencyCheck(case_with_extraction)
    result = check.run()

    assert result.status == ValidationStatus.PASS
    assert "successfully" in result.message.lower()


def test_process_log_consistency_check_failures(case_with_evidence):
    """Test process log consistency check detects failures."""
    case_folder, _ = case_with_evidence

    # Add failed process log entry
    case_db_path = case_folder / "test_surfsifter.sqlite"
    db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    evidence_conn = db_manager.get_evidence_conn(1, label="TEST-EV-001")

    evidence_conn.execute(
        """
        INSERT INTO process_log (evidence_id, task, command, started_at_utc, finished_at_utc, exit_code)
        VALUES (?, ?, ?, datetime('now'), datetime('now'), ?)
        """,
        (1, "test_tool", "/usr/bin/test", 127),  # exit_code=127 (command not found)
    )
    evidence_conn.commit()
    evidence_conn.close()

    check = ProcessLogConsistencyCheck(case_folder)
    result = check.run()

    assert result.status == ValidationStatus.WARNING
    assert "failed tool execution" in result.message.lower()


@patch("core.validation.ToolRegistry")
def test_tool_dependency_check_all_available(mock_registry, valid_case_folder):
    """Test tool dependency check when all tools are available."""
    mock_registry.return_value.discover_all_tools.return_value = {
        "bulk_extractor": Mock(status="found"),
        "exiftool": Mock(status="found"),
    }

    check = ToolDependencyCheck(valid_case_folder)
    result = check.run()

    assert result.status == ValidationStatus.PASS
    assert "available" in result.message.lower()


@patch("core.validation.ToolRegistry")
def test_tool_dependency_check_missing_tools(mock_registry, valid_case_folder):
    """Test tool dependency check when tools are missing."""
    mock_registry.return_value.discover_all_tools.return_value = {
        "bulk_extractor": Mock(status="missing"),
        "exiftool": Mock(status="found"),
    }

    check = ToolDependencyCheck(valid_case_folder)
    result = check.run()

    assert result.status == ValidationStatus.WARNING
    assert "not available" in result.message.lower()
    assert "bulk_extractor" in result.message


# ============================================================================
# Test: ValidationReport
# ============================================================================


def test_validation_report_overall_status_pass(valid_case_folder):
    """Test ValidationReport overall status when all checks pass."""
    validator = QuickValidator(valid_case_folder)
    report = validator.validate()

    # Note: QuickValidator may return WARNING if no evidence added yet
    # This is expected behavior - empty case should warn
    assert report.overall_status in (ValidationStatus.PASS, ValidationStatus.WARNING)
    assert report.error_count == 0


def test_validation_report_overall_status_warning(case_with_evidence):
    """Test ValidationReport overall status with warnings."""
    case_folder, _ = case_with_evidence

    # Full validation should have warnings (no manifests, no tools, etc.)
    validator = FullValidator(case_folder)
    report = validator.validate()

    # Should have at least some warnings
    assert report.warning_count >= 1


def test_validation_report_overall_status_error(tmp_path):
    """Test ValidationReport overall status with errors."""
    case_folder = tmp_path / "broken_case"
    case_folder.mkdir()

    validator = QuickValidator(case_folder)
    report = validator.validate()

    assert report.overall_status == ValidationStatus.ERROR
    assert report.error_count >= 1


def test_validation_report_to_text(valid_case_folder):
    """Test ValidationReport text generation."""
    report = validate_case_quick(valid_case_folder)
    text = report.to_text()

    assert "CASE VALIDATION REPORT" in text
    assert str(valid_case_folder) in text
    assert "Overall Status" in text
    assert "✓" in text or "✗" in text or "⚠" in text


def test_validation_report_counts(case_with_evidence):
    """Test ValidationReport count properties."""
    case_folder, _ = case_with_evidence

    report = validate_case_full(case_folder)

    # Counts should sum to total results
    total = report.pass_count + report.warning_count + report.error_count
    assert total == len(report.results)
    assert total == 5  # Full validator has 5 checks (workspace, schema, evidence, process_log, tools)


# ============================================================================
# Test: Edge Cases
# ============================================================================


def test_validation_corrupted_database(tmp_path):
    """Test validation handles corrupted database gracefully."""
    case_folder = tmp_path / "corrupted"
    case_folder.mkdir()
    (case_folder / "evidences").mkdir()

    # Create corrupted database file
    case_db_path = case_folder / "test_surfsifter.sqlite"
    case_db_path.write_bytes(b"not a sqlite database")

    check = DatabaseSchemaCheck(case_folder)
    result = check.run()

    assert result.status == ValidationStatus.ERROR
    assert "error" in result.message.lower()


def test_validation_empty_case(valid_case_folder):
    """Test validation on completely empty case (no evidence added)."""
    report = validate_case_full(valid_case_folder)

    # Should pass basic structure checks
    assert report.overall_status in (ValidationStatus.PASS, ValidationStatus.WARNING)

    # Evidence files check should warn about no evidence
    evidence_check = next((r for r in report.results if "Evidence Files" in r.check_name), None)
    assert evidence_check is not None
    assert evidence_check.status == ValidationStatus.WARNING


def test_quick_validator_exception_handling(tmp_path):
    """Test QuickValidator handles exceptions in individual checks."""
    case_folder = tmp_path / "test"
    case_folder.mkdir()

    # Create a check that will raise an exception
    class BrokenCheck(WorkspaceStructureCheck):
        def run(self):
            raise RuntimeError("Intentional test error")

    validator = QuickValidator(case_folder)
    validator.checks = [BrokenCheck(case_folder)]

    report = validator.validate()

    # Should capture the exception and continue
    assert len(report.results) == 1
    assert report.results[0].status == ValidationStatus.ERROR
    assert "exception" in report.results[0].message.lower()


def test_full_validator_exception_handling(tmp_path):
    """Test FullValidator handles exceptions in individual checks."""
    case_folder = tmp_path / "test"
    case_folder.mkdir()

    class BrokenCheck(WorkspaceStructureCheck):
        def run(self):
            raise RuntimeError("Intentional test error")

    validator = FullValidator(case_folder)
    validator.checks = [BrokenCheck(case_folder)]

    report = validator.validate()

    assert len(report.results) == 1
    assert report.results[0].status == ValidationStatus.ERROR
    assert "exception" in report.results[0].message.lower()
