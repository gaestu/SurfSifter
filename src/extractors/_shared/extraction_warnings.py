"""
Extraction warnings utilities for extractors.

This module provides utilities for collecting and reporting unknown schemas,
parse errors, and other discovery findings during extraction. These warnings
help investigators understand what data formats were encountered but not fully
parsed, enabling continuous improvement of extractors.

Usage:
    from extractors._shared.extraction_warnings import (
        ExtractionWarningCollector,
        discover_unknown_tables,
        discover_unknown_columns,
        track_unknown_values,
    )

    def run_ingestion(self, ...):
        collector = ExtractionWarningCollector(
            extractor_name="chromium_autofill",
            run_id=run_id,
            evidence_id=evidence_id,
        )

        try:
            # ... extraction logic ...
            unknowns = discover_unknown_tables(conn, KNOWN_TABLES, PATTERNS)
            for unknown in unknowns:
                collector.add_unknown_table(unknown["name"], unknown["columns"], source_file)
        finally:
            collector.flush_to_database(evidence_conn)

Initial implementation
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3


# =============================================================================
# Warning Type Constants (mirror from helpers for convenience)
# =============================================================================

WARNING_TYPE_UNKNOWN_TABLE = "unknown_table"
WARNING_TYPE_UNKNOWN_COLUMN = "unknown_column"
WARNING_TYPE_UNKNOWN_TOKEN_TYPE = "unknown_token_type"
WARNING_TYPE_UNKNOWN_ENUM_VALUE = "unknown_enum_value"
WARNING_TYPE_SCHEMA_MISMATCH = "schema_mismatch"
WARNING_TYPE_EMPTY_EXPECTED = "empty_expected"
WARNING_TYPE_JSON_PARSE_ERROR = "json_parse_error"
WARNING_TYPE_JSON_UNKNOWN_KEY = "json_unknown_key"
WARNING_TYPE_JSON_SCHEMA_VERSION = "json_schema_version"
WARNING_TYPE_JSON_TYPE_MISMATCH = "json_type_mismatch"
WARNING_TYPE_LEVELDB_PARSE_ERROR = "leveldb_parse_error"
WARNING_TYPE_LEVELDB_UNKNOWN_PREFIX = "leveldb_unknown_prefix"
WARNING_TYPE_LEVELDB_CORRUPT_RECORD = "leveldb_corrupt_record"
WARNING_TYPE_BINARY_FORMAT_ERROR = "binary_format_error"
WARNING_TYPE_COMPRESSION_ERROR = "compression_error"
WARNING_TYPE_ENCODING_ERROR = "encoding_error"
WARNING_TYPE_FILE_CORRUPT = "file_corrupt"
WARNING_TYPE_VERSION_UNSUPPORTED = "version_unsupported"
WARNING_TYPE_PLIST_PARSE_ERROR = "plist_parse_error"
WARNING_TYPE_PLIST_UNKNOWN_KEY = "plist_unknown_key"
WARNING_TYPE_REGISTRY_PARSE_ERROR = "registry_parse_error"
WARNING_TYPE_REGISTRY_UNKNOWN_TYPE = "registry_unknown_type"
WARNING_TYPE_REGISTRY_CORRUPT_KEY = "registry_corrupt_key"

# Category Constants
CATEGORY_DATABASE = "database"
CATEGORY_JSON = "json"
CATEGORY_LEVELDB = "leveldb"
CATEGORY_BINARY = "binary"
CATEGORY_PLIST = "plist"
CATEGORY_REGISTRY = "registry"

# Severity Constants
SEVERITY_INFO = "info"
SEVERITY_WARNING = "warning"
SEVERITY_ERROR = "error"


# =============================================================================
# Warning Data Class
# =============================================================================

@dataclass
class ExtractionWarning:
    """A single extraction warning record."""

    warning_type: str
    item_name: str
    severity: str = SEVERITY_WARNING
    category: Optional[str] = None
    artifact_type: Optional[str] = None
    source_file: Optional[str] = None
    item_value: Optional[str] = None
    context_json: Optional[Dict[str, Any]] = None

    def to_dict(self, run_id: str, extractor_name: str) -> Dict[str, Any]:
        """Convert to dict for database insertion."""
        return {
            "run_id": run_id,
            "extractor_name": extractor_name,
            "warning_type": self.warning_type,
            "severity": self.severity,
            "category": self.category,
            "artifact_type": self.artifact_type,
            "source_file": self.source_file,
            "item_name": self.item_name,
            "item_value": self.item_value,
            "context_json": self.context_json,
        }


# =============================================================================
# Warning Collector Class
# =============================================================================

@dataclass
class ExtractionWarningCollector:
    """
    Collects extraction warnings for batch insert.

    Use this class to accumulate warnings during extraction, then flush
    them all to the database at the end. This is more efficient than
    inserting one at a time and ensures warnings are saved even if
    extraction fails partway through.

    Example:
        collector = ExtractionWarningCollector(
            extractor_name="chromium_autofill",
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # During extraction...
        collector.add_unknown_table("new_table", ["col1", "col2"], "Web Data")

        # At the end (in finally block)
        collector.flush_to_database(evidence_conn)
    """

    extractor_name: str
    run_id: str
    evidence_id: int
    _warnings: List[ExtractionWarning] = field(default_factory=list)

    def add_warning(
        self,
        warning_type: str,
        item_name: str,
        *,
        severity: str = SEVERITY_WARNING,
        category: Optional[str] = None,
        artifact_type: Optional[str] = None,
        source_file: Optional[str] = None,
        item_value: Optional[str] = None,
        context_json: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Add a warning to the collection.

        Args:
            warning_type: Type of warning (use WARNING_TYPE_* constants)
            item_name: Name of the unknown/problematic item
            severity: info/warning/error (default: warning)
            category: Category (use CATEGORY_* constants)
            artifact_type: Artifact type (e.g., "autofill", "history")
            source_file: Source file path within evidence
            item_value: Value or additional details
            context_json: Additional context as dict
        """
        self._warnings.append(ExtractionWarning(
            warning_type=warning_type,
            item_name=item_name,
            severity=severity,
            category=category,
            artifact_type=artifact_type,
            source_file=source_file,
            item_value=item_value,
            context_json=context_json,
        ))

    def add_unknown_table(
        self,
        table_name: str,
        columns: List[str],
        source_file: str,
        *,
        artifact_type: Optional[str] = None,
    ) -> None:
        """
        Convenience method for unknown table warnings.

        Args:
            table_name: Name of the unknown table
            columns: List of column names in the table
            source_file: Source database file path
            artifact_type: Artifact type being extracted
        """
        self.add_warning(
            warning_type=WARNING_TYPE_UNKNOWN_TABLE,
            item_name=table_name,
            severity=SEVERITY_WARNING,
            category=CATEGORY_DATABASE,
            artifact_type=artifact_type,
            source_file=source_file,
            context_json={"columns": columns},
        )

    def add_unknown_column(
        self,
        table_name: str,
        column_name: str,
        column_type: str,
        source_file: str,
        *,
        artifact_type: Optional[str] = None,
    ) -> None:
        """
        Convenience method for unknown column warnings.

        Args:
            table_name: Name of the table containing the unknown column
            column_name: Name of the unknown column
            column_type: SQLite type of the column
            source_file: Source database file path
            artifact_type: Artifact type being extracted
        """
        self.add_warning(
            warning_type=WARNING_TYPE_UNKNOWN_COLUMN,
            item_name=column_name,
            item_value=column_type,
            severity=SEVERITY_INFO,
            category=CATEGORY_DATABASE,
            artifact_type=artifact_type,
            source_file=source_file,
            context_json={"table": table_name},
        )

    def add_unknown_token_type(
        self,
        token_type: int,
        source_file: str,
        *,
        artifact_type: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> None:
        """
        Convenience method for unknown token type warnings.

        Args:
            token_type: Integer token type code
            source_file: Source database file path
            artifact_type: Artifact type being extracted
            table_name: Name of the token table
        """
        context = {}
        if table_name:
            context["table"] = table_name

        self.add_warning(
            warning_type=WARNING_TYPE_UNKNOWN_TOKEN_TYPE,
            item_name="TOKEN_TYPE",
            item_value=str(token_type),
            severity=SEVERITY_INFO,
            category=CATEGORY_DATABASE,
            artifact_type=artifact_type,
            source_file=source_file,
            context_json=context if context else None,
        )

    def add_unknown_enum_value(
        self,
        enum_name: str,
        value: Any,
        source_file: str,
        *,
        artifact_type: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Convenience method for unknown enum/constant value warnings.

        Args:
            enum_name: Name of the enum field (e.g., "visit_type")
            value: The unknown value
            source_file: Source file path
            artifact_type: Artifact type being extracted
            context: Additional context
        """
        self.add_warning(
            warning_type=WARNING_TYPE_UNKNOWN_ENUM_VALUE,
            item_name=enum_name,
            item_value=str(value),
            severity=SEVERITY_INFO,
            category=CATEGORY_DATABASE,
            artifact_type=artifact_type,
            source_file=source_file,
            context_json=context,
        )

    def add_json_parse_error(
        self,
        filename: str,
        error: str,
        *,
        artifact_type: Optional[str] = None,
    ) -> None:
        """
        Convenience method for JSON parse error warnings.

        Args:
            filename: Name/path of the JSON file
            error: Error message
            artifact_type: Artifact type being extracted
        """
        self.add_warning(
            warning_type=WARNING_TYPE_JSON_PARSE_ERROR,
            item_name=filename,
            item_value=error,
            severity=SEVERITY_ERROR,
            category=CATEGORY_JSON,
            artifact_type=artifact_type,
            source_file=filename,
        )

    def add_json_unknown_key(
        self,
        key_path: str,
        source_file: str,
        *,
        artifact_type: Optional[str] = None,
        sample_value: Optional[str] = None,
    ) -> None:
        """
        Convenience method for unknown JSON key warnings.

        Args:
            key_path: Dot-separated path to the unknown key
            source_file: Source file path
            artifact_type: Artifact type being extracted
            sample_value: Sample value at the key (truncated)
        """
        self.add_warning(
            warning_type=WARNING_TYPE_JSON_UNKNOWN_KEY,
            item_name=key_path,
            item_value=sample_value[:100] if sample_value else None,
            severity=SEVERITY_WARNING,
            category=CATEGORY_JSON,
            artifact_type=artifact_type,
            source_file=source_file,
        )

    def add_binary_format_error(
        self,
        filename: str,
        error: str,
        *,
        artifact_type: Optional[str] = None,
        format_type: Optional[str] = None,
    ) -> None:
        """
        Convenience method for binary format parse error warnings.

        Args:
            filename: Name/path of the binary file
            error: Error message
            artifact_type: Artifact type being extracted
            format_type: Expected format (e.g., "jsonlz4", "ese")
        """
        context = {}
        if format_type:
            context["format"] = format_type

        self.add_warning(
            warning_type=WARNING_TYPE_BINARY_FORMAT_ERROR,
            item_name=filename,
            item_value=error,
            severity=SEVERITY_ERROR,
            category=CATEGORY_BINARY,
            artifact_type=artifact_type,
            source_file=filename,
            context_json=context if context else None,
        )

    def add_file_corrupt(
        self,
        filename: str,
        error: str,
        *,
        artifact_type: Optional[str] = None,
    ) -> None:
        """
        Convenience method for corrupt file warnings.

        Args:
            filename: Name/path of the corrupt file
            error: Error description
            artifact_type: Artifact type being extracted
        """
        self.add_warning(
            warning_type=WARNING_TYPE_FILE_CORRUPT,
            item_name=filename,
            item_value=error,
            severity=SEVERITY_ERROR,
            category=CATEGORY_BINARY,
            artifact_type=artifact_type,
            source_file=filename,
        )

    @property
    def warning_count(self) -> int:
        """Number of warnings collected."""
        return len(self._warnings)

    @property
    def has_errors(self) -> bool:
        """True if any error-severity warnings collected."""
        return any(w.severity == SEVERITY_ERROR for w in self._warnings)

    @property
    def has_warnings(self) -> bool:
        """True if any warnings collected (any severity)."""
        return len(self._warnings) > 0

    def get_counts_by_severity(self) -> Dict[str, int]:
        """Get warning counts by severity level."""
        counts = {SEVERITY_INFO: 0, SEVERITY_WARNING: 0, SEVERITY_ERROR: 0}
        for w in self._warnings:
            counts[w.severity] = counts.get(w.severity, 0) + 1
        return counts

    def flush_to_database(self, conn: "sqlite3.Connection") -> int:
        """
        Insert all collected warnings to database.

        Args:
            conn: Evidence database connection

        Returns:
            Number of warnings inserted
        """
        if not self._warnings:
            return 0

        # Import here to avoid circular imports
        from core.database.helpers.extraction_warnings import insert_extraction_warnings

        warning_dicts = [
            w.to_dict(self.run_id, self.extractor_name)
            for w in self._warnings
        ]

        count = insert_extraction_warnings(conn, self.evidence_id, warning_dicts)

        # Clear after flush
        self._warnings.clear()

        return count

    def clear(self) -> None:
        """Clear all collected warnings without saving."""
        self._warnings.clear()


# =============================================================================
# Database Discovery Utilities
# =============================================================================

def discover_unknown_tables(
    conn: "sqlite3.Connection",
    known_tables: Set[str],
    patterns: Optional[List[str]] = None,
    *,
    exclude_system: bool = True,
) -> List[Dict[str, Any]]:
    """
    Discover tables in a SQLite database that we don't recognize.

    Args:
        conn: SQLite connection
        known_tables: Set of table names we know and parse
        patterns: Optional list of patterns that suggest relevant tables
                  (e.g., ["autofill", "address"] for autofill-related tables)
        exclude_system: Exclude sqlite_* and android_* system tables

    Returns:
        List of dicts: [{"name": str, "columns": [str, ...]}]
    """
    # Get all user tables
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    all_tables = {row[0] for row in cursor.fetchall()}

    # Filter system tables
    if exclude_system:
        all_tables = {
            t for t in all_tables
            if not t.startswith("sqlite_") and not t.startswith("android_")
        }

    # Find unknown tables
    unknown_tables = all_tables - known_tables

    # If patterns provided, filter to likely-relevant tables
    if patterns:
        pattern_re = re.compile("|".join(patterns), re.IGNORECASE)
        unknown_tables = {t for t in unknown_tables if pattern_re.search(t)}

    # Get column info for each unknown table
    results = []
    for table_name in sorted(unknown_tables):
        try:
            cursor = conn.execute(f"PRAGMA table_info('{table_name}')")
            columns = [row[1] for row in cursor.fetchall()]
            results.append({
                "name": table_name,
                "columns": columns,
            })
        except Exception:
            # Skip tables we can't inspect
            pass

    return results


def discover_unknown_columns(
    conn: "sqlite3.Connection",
    table_name: str,
    known_columns: Set[str],
) -> List[Dict[str, Any]]:
    """
    Discover columns in a table that we don't recognize.

    Args:
        conn: SQLite connection
        table_name: Table to inspect
        known_columns: Set of column names we know and parse

    Returns:
        List of dicts: [{"name": str, "type": str}]
    """
    try:
        cursor = conn.execute(f"PRAGMA table_info('{table_name}')")
        all_columns = {(row[1], row[2]) for row in cursor.fetchall()}
    except Exception:
        return []

    # Find unknown columns
    results = []
    for col_name, col_type in all_columns:
        if col_name not in known_columns:
            results.append({
                "name": col_name,
                "type": col_type or "UNKNOWN",
            })

    return sorted(results, key=lambda x: x["name"])


def track_unknown_values(
    known_mapping: Dict[int, str],
    found_values: Set[int],
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    value_name: str = "value",
    source_file: str = "",
    artifact_type: Optional[str] = None,
) -> Set[int]:
    """
    Find values that don't exist in a known mapping and optionally report warnings.

    Useful for detecting unknown enum/token type values.

    Args:
        known_mapping: Dict mapping known values to names (e.g., TOKEN_TYPES)
        found_values: Set of values encountered during parsing
        warning_collector: Optional collector to record warnings for unknown values
        value_name: Name of the value type for warning messages (e.g., "sameSite", "visit_type")
        source_file: Source file path for warning context
        artifact_type: Artifact type for warning categorization

    Returns:
        Set of unknown values

    Example:
        # Simple usage (just get unknown values)
        unknown = track_unknown_values(KNOWN_TYPES, found_types)

        # With warning collection
        unknown = track_unknown_values(
            KNOWN_TYPES, found_types,
            warning_collector=collector,
            value_name="permission_type",
            source_file="/path/to/file.sqlite",
            artifact_type="permissions",
        )
    """
    unknown_values = found_values - set(known_mapping.keys())

    # If a warning collector is provided, add warnings for each unknown value
    if warning_collector and unknown_values:
        for value in unknown_values:
            warning_collector.add_warning(
                warning_type="unknown_enum_value",
                category="database",
                severity="info",
                artifact_type=artifact_type,
                source_file=source_file,
                item_name=value_name,
                item_value=str(value),
            )

    return unknown_values


# =============================================================================
# JSON Discovery Utilities
# =============================================================================

def discover_unknown_json_keys(
    data: Dict[str, Any],
    known_keys: Set[str],
    *,
    path: str = "",
    max_depth: int = 3,
    _current_depth: int = 0,
) -> List[Dict[str, Any]]:
    """
    Discover unknown keys in a JSON structure.

    Only inspects the top-level keys and optionally recurses into dicts.
    Does not recurse into lists.

    Args:
        data: JSON data as dict
        known_keys: Set of known dot-separated key paths (e.g., {"sync.enabled", "version"})
        path: Current path prefix (for recursion)
        max_depth: Maximum depth to recurse
        _current_depth: Internal recursion tracker

    Returns:
        List of dicts: [{"path": str, "type": str, "sample": str}]
    """
    if not isinstance(data, dict) or _current_depth > max_depth:
        return []

    results = []

    for key, value in data.items():
        full_path = f"{path}.{key}" if path else key

        if full_path not in known_keys:
            # Get sample value (truncated)
            if isinstance(value, (dict, list)):
                sample = f"<{type(value).__name__}>"
            else:
                sample = str(value)[:50]

            results.append({
                "path": full_path,
                "type": type(value).__name__,
                "sample": sample,
            })

        # Recurse into dicts
        if isinstance(value, dict):
            results.extend(discover_unknown_json_keys(
                value,
                known_keys,
                path=full_path,
                max_depth=max_depth,
                _current_depth=_current_depth + 1,
            ))

    return results


# =============================================================================
# Convenience Functions (for simple cases)
# =============================================================================

def warn_unknown_table(
    collector: ExtractionWarningCollector,
    table_name: str,
    columns: List[str],
    source_file: str,
    artifact_type: Optional[str] = None,
) -> None:
    """
    Add an unknown table warning to a collector.

    Convenience wrapper for simple cases.
    """
    collector.add_unknown_table(table_name, columns, source_file, artifact_type=artifact_type)


def warn_json_parse_error(
    collector: ExtractionWarningCollector,
    filename: str,
    error: str,
    artifact_type: Optional[str] = None,
) -> None:
    """
    Add a JSON parse error warning to a collector.

    Convenience wrapper for simple cases.
    """
    collector.add_json_parse_error(filename, error, artifact_type=artifact_type)


def warn_unknown_token_type(
    collector: ExtractionWarningCollector,
    token_type: int,
    source_file: str,
    artifact_type: Optional[str] = None,
) -> None:
    """
    Add an unknown token type warning to a collector.

    Convenience wrapper for simple cases.
    """
    collector.add_unknown_token_type(token_type, source_file, artifact_type=artifact_type)


# =============================================================================
# Ingestion Validation Utilities
# =============================================================================

@dataclass
class IngestionValidationResult:
    """Result of ingestion validation check."""

    is_valid: bool
    expected_count: int
    actual_count: int
    table_name: str
    details: Optional[str] = None

    def __str__(self) -> str:
        if self.is_valid:
            return f"✓ {self.table_name}: {self.actual_count} records ingested"
        return f"✗ {self.table_name}: expected {self.expected_count}, got {self.actual_count}"


def validate_ingestion_count(
    conn: "sqlite3.Connection",
    table_name: str,
    run_id: str,
    expected_count: int,
    *,
    tolerance_percent: float = 0.0,
) -> IngestionValidationResult:
    """
    Validate that ingestion inserted the expected number of records.

    This is useful for detecting silent failures where parsing succeeds
    but database insertion fails or is skipped.

    Args:
        conn: SQLite connection to evidence database
        table_name: Table to check (e.g., "browser_history", "urls")
        run_id: Run ID to filter by
        expected_count: Number of records expected to be inserted
        tolerance_percent: Allow this percentage difference (0.0 = exact match)

    Returns:
        IngestionValidationResult with validation outcome

    Example:
        # After parsing 100 records and inserting
        result = validate_ingestion_count(
            conn, "browser_history", run_id,
            expected_count=100, tolerance_percent=5.0
        )
        if not result.is_valid:
            callbacks.on_log(f"Validation warning: {result}", "warning")
    """
    try:
        cursor = conn.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE run_id = ?",
            (run_id,)
        )
        actual_count = cursor.fetchone()[0]
    except Exception as e:
        return IngestionValidationResult(
            is_valid=False,
            expected_count=expected_count,
            actual_count=0,
            table_name=table_name,
            details=f"Query failed: {e}",
        )

    # Check if counts match within tolerance
    if expected_count == 0:
        is_valid = actual_count == 0
    else:
        diff_percent = abs(actual_count - expected_count) / expected_count * 100
        is_valid = diff_percent <= tolerance_percent

    return IngestionValidationResult(
        is_valid=is_valid,
        expected_count=expected_count,
        actual_count=actual_count,
        table_name=table_name,
    )


def validate_ingestion_batch(
    conn: "sqlite3.Connection",
    run_id: str,
    expectations: Dict[str, int],
    *,
    tolerance_percent: float = 0.0,
) -> List[IngestionValidationResult]:
    """
    Validate multiple table ingestion counts at once.

    Args:
        conn: SQLite connection to evidence database
        run_id: Run ID to filter by
        expectations: Dict mapping table_name -> expected_count
        tolerance_percent: Allow this percentage difference (0.0 = exact match)

    Returns:
        List of IngestionValidationResult for each table

    Example:
        results = validate_ingestion_batch(
            conn, run_id,
            {"browser_history": 100, "urls": 50, "cookies": 25}
        )
        failures = [r for r in results if not r.is_valid]
        if failures:
            for f in failures:
                callbacks.on_log(f"Validation warning: {f}", "warning")
    """
    return [
        validate_ingestion_count(
            conn, table_name, run_id, expected_count,
            tolerance_percent=tolerance_percent,
        )
        for table_name, expected_count in expectations.items()
    ]


# =============================================================================
# Safe Parse/Insert Wrapper
# =============================================================================

@dataclass
class ParseInsertResult:
    """Result of a safe parse and insert operation."""

    success: bool
    records_parsed: int
    records_inserted: int
    error: Optional[str] = None
    exception: Optional[Exception] = None

    @property
    def has_data_loss(self) -> bool:
        """Check if there was potential data loss (parsed > inserted)."""
        return self.records_parsed > self.records_inserted


def safe_parse_and_insert(
    parse_func,
    insert_func,
    *,
    source_description: str = "source",
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    artifact_type: Optional[str] = None,
    source_file: Optional[str] = None,
) -> ParseInsertResult:
    """
    Safely execute parse and insert functions with error handling.

    This wrapper catches exceptions in both parse and insert phases,
    logs them appropriately, and returns a result object indicating
    success/failure and any data loss.

    Args:
        parse_func: Callable that parses data, returns list of records
        insert_func: Callable that takes records list, returns count inserted
        source_description: Description for error messages
        warning_collector: Optional collector for warnings
        artifact_type: Artifact type for warnings
        source_file: Source file for warnings

    Returns:
        ParseInsertResult with success status and counts

    Example:
        result = safe_parse_and_insert(
            parse_func=lambda: parse_cookies(conn, browser, file_entry),
            insert_func=lambda records: insert_cookies(evidence_conn, evidence_id, records),
            source_description=f"{browser} cookies from {file_path}",
            warning_collector=collector,
            artifact_type="cookies",
            source_file=file_path,
        )

        if not result.success:
            callbacks.on_error(f"Failed to process {source_description}", result.error)
        elif result.has_data_loss:
            callbacks.on_log(
                f"Possible data loss: parsed {result.records_parsed} but inserted {result.records_inserted}",
                "warning"
            )
    """
    records_parsed = 0
    records_inserted = 0

    # Phase 1: Parse
    try:
        records = parse_func()
        records_parsed = len(records) if records else 0
    except Exception as e:
        error_msg = f"Parse failed for {source_description}: {e}"
        if warning_collector:
            warning_collector.add_warning(
                warning_type=WARNING_TYPE_FILE_CORRUPT,
                item_name=source_file or source_description,
                severity=SEVERITY_ERROR,
                artifact_type=artifact_type,
                source_file=source_file,
                item_value=str(e),
            )
        return ParseInsertResult(
            success=False,
            records_parsed=0,
            records_inserted=0,
            error=error_msg,
            exception=e,
        )

    # If no records, return early (success with 0 records)
    if not records:
        return ParseInsertResult(
            success=True,
            records_parsed=0,
            records_inserted=0,
        )

    # Phase 2: Insert
    try:
        records_inserted = insert_func(records)
    except Exception as e:
        error_msg = f"Insert failed for {source_description}: {e}"
        if warning_collector:
            warning_collector.add_warning(
                warning_type=WARNING_TYPE_FILE_CORRUPT,
                item_name=source_file or source_description,
                severity=SEVERITY_ERROR,
                artifact_type=artifact_type,
                source_file=source_file,
                item_value=f"Insert failed after parsing {records_parsed} records: {e}",
            )
        return ParseInsertResult(
            success=False,
            records_parsed=records_parsed,
            records_inserted=0,
            error=error_msg,
            exception=e,
        )

    return ParseInsertResult(
        success=True,
        records_parsed=records_parsed,
        records_inserted=records_inserted,
    )
