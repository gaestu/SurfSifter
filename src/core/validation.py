"""
Case validation framework for verifying case integrity and completeness.

This module provides automated validation of forensic cases, ensuring that:
- Evidence files are accessible
- Database schemas are compatible
- Workspace structure is valid
- Process logs are consistent
- Tool dependencies are available

Supports both quick validation (automatic on case open, <3s) and full validation
(on-demand, <10s) for comprehensive integrity checking.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import List, Optional, Dict, Any

from .database import init_db, DatabaseManager
from .evidence_fs import find_ewf_segments
from .logging import get_logger
from .tool_registry import ToolRegistry

LOGGER = get_logger("core.validation")


class ValidationStatus(Enum):
    """Validation result status levels."""
    PASS = "pass"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    check_name: str
    status: ValidationStatus
    message: str
    remediation: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

    def is_pass(self) -> bool:
        return self.status == ValidationStatus.PASS

    def is_warning(self) -> bool:
        return self.status == ValidationStatus.WARNING

    def is_error(self) -> bool:
        return self.status == ValidationStatus.ERROR


@dataclass
class ValidationReport:
    """Complete validation report with all check results."""
    case_folder: Path
    timestamp: datetime
    results: List[ValidationResult]
    duration_seconds: float

    @property
    def overall_status(self) -> ValidationStatus:
        """Return the worst status from all checks."""
        if any(r.is_error() for r in self.results):
            return ValidationStatus.ERROR
        if any(r.is_warning() for r in self.results):
            return ValidationStatus.WARNING
        return ValidationStatus.PASS

    @property
    def error_count(self) -> int:
        return sum(1 for r in self.results if r.is_error())

    @property
    def warning_count(self) -> int:
        return sum(1 for r in self.results if r.is_warning())

    @property
    def pass_count(self) -> int:
        return sum(1 for r in self.results if r.is_pass())

    def to_text(self) -> str:
        """Generate a text report suitable for display or file export."""
        lines = [
            "=" * 80,
            f"CASE VALIDATION REPORT",
            "=" * 80,
            f"Case Folder: {self.case_folder}",
            f"Validation Time: {self.timestamp.isoformat()}",
            f"Duration: {self.duration_seconds:.2f} seconds",
            "",
            f"Overall Status: {self.overall_status.value.upper()}",
            f"  ✓ Pass: {self.pass_count}",
            f"  ⚠ Warning: {self.warning_count}",
            f"  ✗ Error: {self.error_count}",
            "",
            "=" * 80,
            "CHECK RESULTS",
            "=" * 80,
            "",
        ]

        for result in self.results:
            status_symbol = {
                ValidationStatus.PASS: "✓",
                ValidationStatus.WARNING: "⚠",
                ValidationStatus.ERROR: "✗",
            }[result.status]

            lines.append(f"{status_symbol} {result.check_name}: {result.status.value.upper()}")
            lines.append(f"  {result.message}")
            if result.remediation:
                lines.append(f"  → Remediation: {result.remediation}")
            if result.details:
                for key, value in result.details.items():
                    lines.append(f"  • {key}: {value}")
            lines.append("")

        return "\n".join(lines)


class ValidationCheck:
    """Base class for individual validation checks."""

    def __init__(self, case_folder: Path):
        self.case_folder = case_folder

    def run(self) -> ValidationResult:
        """Execute the validation check and return the result."""
        raise NotImplementedError


class WorkspaceStructureCheck(ValidationCheck):
    """Verify that the case workspace has the expected directory structure."""

    def run(self) -> ValidationResult:
        from core.database import find_case_database

        required_dirs = ["evidences"]
        missing_dirs = [d for d in required_dirs if not (self.case_folder / d).is_dir()]

        # Check for case database
        case_db = find_case_database(self.case_folder)
        missing_items = []

        if case_db is None:
            missing_items.append("case database (*_surfsifter.sqlite or *_browser.sqlite)")

        missing_items.extend(missing_dirs)

        if missing_items:
            return ValidationResult(
                check_name="Workspace Structure",
                status=ValidationStatus.ERROR,
                message=f"Missing {len(missing_items)} required files/directories: {', '.join(missing_items)}",
                remediation="Ensure case folder was created by this application.",
                details={"missing_items": missing_items},
            )

        return ValidationResult(
            check_name="Workspace Structure",
            status=ValidationStatus.PASS,
            message="All required files and directories present.",
        )


class DatabaseSchemaCheck(ValidationCheck):
    """Verify that the database schema version is compatible."""

    def run(self) -> ValidationResult:
        from core.database import find_case_database

        case_db_path = find_case_database(self.case_folder)

        if not case_db_path or not case_db_path.exists():
            return ValidationResult(
                check_name="Database Schema",
                status=ValidationStatus.ERROR,
                message="Case database not found.",
                remediation="Case database is missing. Case may be corrupted.",
            )

        try:
            conn = sqlite3.connect(case_db_path)
            conn.row_factory = sqlite3.Row

            # Check if schema_version table exists
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version';"
            )
            if not cursor.fetchone():
                return ValidationResult(
                    check_name="Database Schema",
                    status=ValidationStatus.ERROR,
                    message="schema_version table not found.",
                    remediation="Database may be from an incompatible version. Try re-creating the case.",
                )

            # Get applied schema versions
            cursor = conn.execute("SELECT version, applied_at_utc FROM schema_version ORDER BY version;")
            versions = cursor.fetchall()
            conn.close()

            if not versions:
                return ValidationResult(
                    check_name="Database Schema",
                    status=ValidationStatus.WARNING,
                    message="No migrations applied. Database may be uninitialized.",
                    remediation="Try re-opening the case to trigger migrations.",
                )

            latest_version = versions[-1]["version"]
            applied_count = len(versions)

            return ValidationResult(
                check_name="Database Schema",
                status=ValidationStatus.PASS,
                message=f"Schema version {latest_version} applied ({applied_count} migrations).",
                details={
                    "latest_version": latest_version,
                    "migration_count": applied_count,
                },
            )

        except sqlite3.DatabaseError as exc:
            return ValidationResult(
                check_name="Database Schema",
                status=ValidationStatus.ERROR,
                message=f"Database error: {exc}",
                remediation="Database may be corrupted. Try restoring from backup.",
            )


class EvidenceFilesCheck(ValidationCheck):
    """Verify that all evidence files (E01 segments) are accessible."""

    def run(self) -> ValidationResult:
        from core.database import find_case_database

        case_db_path = find_case_database(self.case_folder)

        if not case_db_path or not case_db_path.exists():
            return ValidationResult(
                check_name="Evidence Files",
                status=ValidationStatus.ERROR,
                message="Cannot check evidence files: case database not found.",
            )

        try:
            conn = sqlite3.connect(case_db_path)
            conn.row_factory = sqlite3.Row

            cursor = conn.execute("SELECT id, label, source_path FROM evidences;")
            evidences = cursor.fetchall()
            conn.close()

            if not evidences:
                return ValidationResult(
                    check_name="Evidence Files",
                    status=ValidationStatus.WARNING,
                    message="No evidence items found in case.",
                    remediation="Add evidence files using 'Add Evidence' button.",
                )

            missing_files = []
            accessible_count = 0

            for evidence in evidences:
                source_path = Path(evidence["source_path"])

                # Check if source path exists
                if not source_path.exists():
                    missing_files.append(f"{evidence['label']} ({source_path})")
                    continue

                # If it's an E01 file, check all segments
                if source_path.suffix.lower() in (".e01", ".e0x"):
                    try:
                        segments = find_ewf_segments(source_path)
                        missing_segments = [str(seg) for seg in segments if not seg.exists()]
                        if missing_segments:
                            missing_files.append(f"{evidence['label']} - missing segments: {', '.join(missing_segments)}")
                        else:
                            accessible_count += 1
                    except Exception as exc:
                        missing_files.append(f"{evidence['label']} - error checking segments: {exc}")
                else:
                    accessible_count += 1

            if missing_files:
                return ValidationResult(
                    check_name="Evidence Files",
                    status=ValidationStatus.ERROR,
                    message=f"{len(missing_files)} evidence file(s) inaccessible.",
                    remediation="Ensure E01 files and all segments are present at their original paths.",
                    details={"missing_files": missing_files},
                )

            return ValidationResult(
                check_name="Evidence Files",
                status=ValidationStatus.PASS,
                message=f"All {accessible_count} evidence file(s) accessible.",
                details={"evidence_count": accessible_count},
            )

        except sqlite3.DatabaseError as exc:
            return ValidationResult(
                check_name="Evidence Files",
                status=ValidationStatus.ERROR,
                message=f"Database error: {exc}",
            )


class ProcessLogConsistencyCheck(ValidationCheck):
    """Verify that process logs show successful tool executions."""

    def run(self) -> ValidationResult:
        try:
            from .database import find_case_database
            db_path = find_case_database(self.case_folder)
            if db_path is None:
                return ValidationResult(
                    check_name="Process Log Consistency",
                    status=ValidationStatus.ERROR,
                    message="Cannot find case database.",
                )
            db_manager = DatabaseManager(self.case_folder, case_db_path=db_path)
            case_conn = db_manager.get_case_conn()

            # Get all evidences
            cursor = case_conn.execute("SELECT id, label FROM evidences;")
            evidences = cursor.fetchall()

            if not evidences:
                return ValidationResult(
                    check_name="Process Log Consistency",
                    status=ValidationStatus.PASS,
                    message="No evidences to check (case is empty).",
                )

            failed_tools = []
            total_logs = 0
            failed_count = 0

            for evidence in evidences:
                evidence_id = evidence["id"]
                evidence_label = evidence["label"]

                try:
                    evidence_conn = db_manager.get_evidence_conn(evidence_id, label=evidence_label)

                    # Check if process_log table exists
                    cursor = evidence_conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name='process_log';"
                    )
                    if not cursor.fetchone():
                        continue

                    # Check for failed tool executions (exit_code != 0)
                    cursor = evidence_conn.execute(
                        "SELECT task, exit_code, started_at_utc FROM process_log WHERE exit_code IS NOT NULL AND exit_code != 0;"
                    )
                    failures = cursor.fetchall()

                    total_logs += 1

                    for failure in failures:
                        failed_count += 1
                        failed_tools.append(
                            f"{evidence_label}: {failure['task']} (exit code {failure['exit_code']}) at {failure['started_at_utc']}"
                        )

                except Exception as exc:
                    failed_tools.append(f"{evidence_label}: error reading logs: {exc}")

            case_conn.close()

            if failed_tools:
                return ValidationResult(
                    check_name="Process Log Consistency",
                    status=ValidationStatus.WARNING,
                    message=f"Found {failed_count} failed tool execution(s).",
                    remediation="Review logs to determine if failures are expected or indicate issues.",
                    details={"failures": failed_tools[:10]},  # Limit to first 10
                )

            if total_logs == 0:
                return ValidationResult(
                    check_name="Process Log Consistency",
                    status=ValidationStatus.PASS,
                    message="No extraction runs recorded yet.",
                )

            return ValidationResult(
                check_name="Process Log Consistency",
                status=ValidationStatus.PASS,
                message="All tool executions completed successfully.",
            )

        except Exception as exc:
            return ValidationResult(
                check_name="Process Log Consistency",
                status=ValidationStatus.ERROR,
                message=f"Error checking process logs: {exc}",
            )


class ToolDependencyCheck(ValidationCheck):
    """Verify that required external tools are available."""

    def run(self) -> ValidationResult:
        try:
            tool_registry = ToolRegistry()
            all_tools = tool_registry.discover_all_tools()
            missing_tools = []
            available_tools = []

            for name, info in all_tools.items():
                if info.status == "found":
                    available_tools.append(name)
                else:
                    missing_tools.append(name)

            if missing_tools:
                return ValidationResult(
                    check_name="Tool Dependencies",
                    status=ValidationStatus.WARNING,
                    message=f"{len(missing_tools)} optional tool(s) not available: {', '.join(missing_tools)}",
                    remediation="Install missing tools or set custom paths in Preferences > Tools.",
                    details={
                        "missing": missing_tools,
                        "available": available_tools,
                    },
                )

            return ValidationResult(
                check_name="Tool Dependencies",
                status=ValidationStatus.PASS,
                message=f"All {len(available_tools)} tool(s) available.",
                details={"available": available_tools},
            )

        except Exception as exc:
            return ValidationResult(
                check_name="Tool Dependencies",
                status=ValidationStatus.WARNING,
                message=f"Error checking tool dependencies: {exc}",
            )


class QuickValidator:
    """
    Fast validation for automatic checks on case open (<3 seconds).

    Runs essential checks only:
    - Workspace structure
    - Database schema
    - Evidence files
    """

    def __init__(self, case_folder: Path):
        self.case_folder = case_folder
        self.checks = [
            WorkspaceStructureCheck(case_folder),
            DatabaseSchemaCheck(case_folder),
            EvidenceFilesCheck(case_folder),
        ]

    def validate(self) -> ValidationReport:
        """Run quick validation checks."""
        start_time = datetime.now(timezone.utc)
        results = []

        for check in self.checks:
            try:
                result = check.run()
                results.append(result)
            except Exception as exc:
                LOGGER.exception("Quick validation check failed: %s", check.__class__.__name__)
                results.append(
                    ValidationResult(
                        check_name=check.__class__.__name__.replace("Check", ""),
                        status=ValidationStatus.ERROR,
                        message=f"Check failed with exception: {exc}",
                    )
                )

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        return ValidationReport(
            case_folder=self.case_folder,
            timestamp=start_time,
            results=results,
            duration_seconds=duration,
        )


class FullValidator:
    """
    Comprehensive validation for on-demand checks (<10 seconds).

    Runs all validation checks:
    - All quick checks
    - Process log consistency
    - Tool dependencies
    """

    def __init__(self, case_folder: Path):
        self.case_folder = case_folder
        self.checks = [
            WorkspaceStructureCheck(case_folder),
            DatabaseSchemaCheck(case_folder),
            EvidenceFilesCheck(case_folder),
            ProcessLogConsistencyCheck(case_folder),
            ToolDependencyCheck(case_folder),
        ]

    def validate(self) -> ValidationReport:
        """Run full validation checks."""
        start_time = datetime.now(timezone.utc)
        results = []

        for check in self.checks:
            try:
                result = check.run()
                results.append(result)
                LOGGER.debug("Validation check '%s': %s", result.check_name, result.status.value)
            except Exception as exc:
                LOGGER.exception("Full validation check failed: %s", check.__class__.__name__)
                results.append(
                    ValidationResult(
                        check_name=check.__class__.__name__.replace("Check", ""),
                        status=ValidationStatus.ERROR,
                        message=f"Check failed with exception: {exc}",
                    )
                )

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        LOGGER.info(
            "Full validation complete: %d pass, %d warning, %d error (%.2fs)",
            sum(1 for r in results if r.is_pass()),
            sum(1 for r in results if r.is_warning()),
            sum(1 for r in results if r.is_error()),
            duration,
        )

        return ValidationReport(
            case_folder=self.case_folder,
            timestamp=start_time,
            results=results,
            duration_seconds=duration,
        )


def validate_case_quick(case_folder: Path) -> ValidationReport:
    """
    Run quick validation on a case (automatic on case open).

    Args:
        case_folder: Path to the case directory

    Returns:
        ValidationReport with essential checks (<3 seconds)
    """
    validator = QuickValidator(case_folder)
    return validator.validate()


def validate_case_full(case_folder: Path) -> ValidationReport:
    """
    Run full validation on a case (on-demand).

    Args:
        case_folder: Path to the case directory

    Returns:
        ValidationReport with all checks (<10 seconds)
    """
    validator = FullValidator(case_folder)
    return validator.validate()
