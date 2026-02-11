"""
Database helper functions for extraction warnings.

Extraction warnings track unknown schemas, parse errors, and other discovery
findings during extraction. This enables forensic visibility into what data
formats the extractors encounter but don't fully understand.

Initial implementation
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3


# Warning type constants
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

# Category constants
CATEGORY_DATABASE = "database"
CATEGORY_JSON = "json"
CATEGORY_LEVELDB = "leveldb"
CATEGORY_BINARY = "binary"
CATEGORY_PLIST = "plist"
CATEGORY_REGISTRY = "registry"

# Severity constants
SEVERITY_INFO = "info"
SEVERITY_WARNING = "warning"
SEVERITY_ERROR = "error"


def insert_extraction_warning(
    conn: "sqlite3.Connection",
    evidence_id: int,
    run_id: str,
    extractor_name: str,
    warning_type: str,
    item_name: str,
    *,
    severity: str = SEVERITY_WARNING,
    category: Optional[str] = None,
    artifact_type: Optional[str] = None,
    source_file: Optional[str] = None,
    item_value: Optional[str] = None,
    context_json: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Insert a single extraction warning.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        run_id: Extraction run ID
        extractor_name: Name of the extractor
        warning_type: Type of warning (use WARNING_TYPE_* constants)
        item_name: Name of the unknown/problematic item
        severity: info/warning/error (default: warning)
        category: Category (use CATEGORY_* constants)
        artifact_type: Artifact type (e.g., "autofill", "history")
        source_file: Source file path within evidence
        item_value: Value or additional details
        context_json: Additional context as dict (will be JSON serialized)

    Returns:
        Row ID of inserted warning
    """
    context_str = json.dumps(context_json) if context_json else None

    cursor = conn.execute(
        """
        INSERT INTO extraction_warnings (
            evidence_id, run_id, extractor_name, warning_type, severity,
            category, artifact_type, source_file, item_name, item_value,
            context_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            evidence_id, run_id, extractor_name, warning_type, severity,
            category, artifact_type, source_file, item_name, item_value,
            context_str,
        ),
    )
    conn.commit()
    return cursor.lastrowid


def insert_extraction_warnings(
    conn: "sqlite3.Connection",
    evidence_id: int,
    warnings: List[Dict[str, Any]],
) -> int:
    """
    Insert multiple extraction warnings in a batch.

    Each warning dict should have:
        - run_id: str
        - extractor_name: str
        - warning_type: str
        - item_name: str
        - severity: str (optional, default 'warning')
        - category: str (optional)
        - artifact_type: str (optional)
        - source_file: str (optional)
        - item_value: str (optional)
        - context_json: dict (optional)

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        warnings: List of warning dicts

    Returns:
        Number of warnings inserted
    """
    if not warnings:
        return 0

    rows = []
    for w in warnings:
        context_str = json.dumps(w.get("context_json")) if w.get("context_json") else None
        rows.append((
            evidence_id,
            w["run_id"],
            w["extractor_name"],
            w["warning_type"],
            w.get("severity", SEVERITY_WARNING),
            w.get("category"),
            w.get("artifact_type"),
            w.get("source_file"),
            w["item_name"],
            w.get("item_value"),
            context_str,
        ))

    conn.executemany(
        """
        INSERT INTO extraction_warnings (
            evidence_id, run_id, extractor_name, warning_type, severity,
            category, artifact_type, source_file, item_name, item_value,
            context_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def get_extraction_warnings(
    conn: "sqlite3.Connection",
    evidence_id: int,
    *,
    extractor_name: Optional[str] = None,
    category: Optional[str] = None,
    warning_type: Optional[str] = None,
    severity: Optional[str] = None,
    run_id: Optional[str] = None,
    limit: Optional[int] = None,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    """
    Get extraction warnings with optional filters.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        extractor_name: Filter by extractor name
        category: Filter by category
        warning_type: Filter by warning type
        severity: Filter by severity
        run_id: Filter by run ID
        limit: Maximum number of results
        offset: Offset for pagination

    Returns:
        List of warning dicts
    """
    query = """
        SELECT * FROM extraction_warnings
        WHERE evidence_id = ?
    """
    params: List[Any] = [evidence_id]

    if extractor_name:
        query += " AND extractor_name = ?"
        params.append(extractor_name)

    if category:
        query += " AND category = ?"
        params.append(category)

    if warning_type:
        query += " AND warning_type = ?"
        params.append(warning_type)

    if severity:
        query += " AND severity = ?"
        params.append(severity)

    if run_id:
        query += " AND run_id = ?"
        params.append(run_id)

    query += " ORDER BY created_at_utc DESC, id DESC"

    if limit:
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

    cursor = conn.execute(query, params)
    columns = [desc[0] for desc in cursor.description]

    results = []
    for row in cursor.fetchall():
        record = dict(zip(columns, row))
        # Parse context_json if present
        if record.get("context_json"):
            try:
                record["context_json"] = json.loads(record["context_json"])
            except json.JSONDecodeError:
                pass  # Keep as string if invalid JSON
        results.append(record)

    return results


def get_extraction_warnings_count(
    conn: "sqlite3.Connection",
    evidence_id: int,
    *,
    extractor_name: Optional[str] = None,
    category: Optional[str] = None,
    warning_type: Optional[str] = None,
    severity: Optional[str] = None,
    run_id: Optional[str] = None,
) -> int:
    """
    Get count of extraction warnings with optional filters.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        extractor_name: Filter by extractor name
        category: Filter by category
        warning_type: Filter by warning type
        severity: Filter by severity
        run_id: Filter by run ID

    Returns:
        Number of matching warnings
    """
    query = """
        SELECT COUNT(*) FROM extraction_warnings
        WHERE evidence_id = ?
    """
    params: List[Any] = [evidence_id]

    if extractor_name:
        query += " AND extractor_name = ?"
        params.append(extractor_name)

    if category:
        query += " AND category = ?"
        params.append(category)

    if warning_type:
        query += " AND warning_type = ?"
        params.append(warning_type)

    if severity:
        query += " AND severity = ?"
        params.append(severity)

    if run_id:
        query += " AND run_id = ?"
        params.append(run_id)

    cursor = conn.execute(query, params)
    return cursor.fetchone()[0]


def get_extraction_warnings_summary(
    conn: "sqlite3.Connection",
    evidence_id: int,
    *,
    extractor_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get summary statistics for extraction warnings.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        extractor_name: Optional filter by extractor

    Returns:
        Dict with counts by severity and category:
        {
            "total": int,
            "by_severity": {"info": int, "warning": int, "error": int},
            "by_category": {"database": int, "json": int, ...},
            "by_extractor": {"extractor_name": int, ...},
        }
    """
    # Base query parts
    base_where = "WHERE evidence_id = ?"
    params: List[Any] = [evidence_id]

    if extractor_name:
        base_where += " AND extractor_name = ?"
        params.append(extractor_name)

    # Total count
    total = conn.execute(
        f"SELECT COUNT(*) FROM extraction_warnings {base_where}",
        params,
    ).fetchone()[0]

    # By severity
    by_severity = {"info": 0, "warning": 0, "error": 0}
    for row in conn.execute(
        f"""
        SELECT severity, COUNT(*) as cnt
        FROM extraction_warnings {base_where}
        GROUP BY severity
        """,
        params,
    ):
        by_severity[row[0]] = row[1]

    # By category
    by_category: Dict[str, int] = {}
    for row in conn.execute(
        f"""
        SELECT category, COUNT(*) as cnt
        FROM extraction_warnings {base_where}
        GROUP BY category
        """,
        params,
    ):
        if row[0]:  # Skip NULL category
            by_category[row[0]] = row[1]

    # By extractor (only if not already filtered)
    by_extractor: Dict[str, int] = {}
    if not extractor_name:
        for row in conn.execute(
            f"""
            SELECT extractor_name, COUNT(*) as cnt
            FROM extraction_warnings {base_where}
            GROUP BY extractor_name
            """,
            params,
        ):
            by_extractor[row[0]] = row[1]

    return {
        "total": total,
        "by_severity": by_severity,
        "by_category": by_category,
        "by_extractor": by_extractor,
    }


def get_extraction_warnings_by_run(
    conn: "sqlite3.Connection",
    evidence_id: int,
    extractor_name: str,
    run_id: str,
) -> List[Dict[str, Any]]:
    """
    Get all warnings for a specific extractor run.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        extractor_name: Extractor name
        run_id: Run ID

    Returns:
        List of warning dicts
    """
    return get_extraction_warnings(
        conn,
        evidence_id,
        extractor_name=extractor_name,
        run_id=run_id,
    )


def delete_extraction_warnings_by_run(
    conn: "sqlite3.Connection",
    evidence_id: int,
    extractor_name: str,
    run_id: str,
) -> int:
    """
    Delete all warnings for a specific extractor run.

    Called when an extractor is re-run to clear old warnings.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        extractor_name: Extractor name
        run_id: Run ID

    Returns:
        Number of warnings deleted
    """
    cursor = conn.execute(
        """
        DELETE FROM extraction_warnings
        WHERE evidence_id = ? AND extractor_name = ? AND run_id = ?
        """,
        (evidence_id, extractor_name, run_id),
    )
    conn.commit()
    return cursor.rowcount


def delete_extraction_warnings_by_extractor(
    conn: "sqlite3.Connection",
    evidence_id: int,
    extractor_name: str,
) -> int:
    """
    Delete all warnings for an extractor (all runs).

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        extractor_name: Extractor name

    Returns:
        Number of warnings deleted
    """
    cursor = conn.execute(
        """
        DELETE FROM extraction_warnings
        WHERE evidence_id = ? AND extractor_name = ?
        """,
        (evidence_id, extractor_name),
    )
    conn.commit()
    return cursor.rowcount


def get_distinct_warning_extractors(
    conn: "sqlite3.Connection",
    evidence_id: int,
) -> List[str]:
    """
    Get list of extractors that have warnings.

    Args:
        conn: Database connection
        evidence_id: Evidence ID

    Returns:
        List of extractor names
    """
    cursor = conn.execute(
        """
        SELECT DISTINCT extractor_name FROM extraction_warnings
        WHERE evidence_id = ?
        ORDER BY extractor_name
        """,
        (evidence_id,),
    )
    return [row[0] for row in cursor.fetchall()]


def get_warning_count_for_extractor(
    conn: "sqlite3.Connection",
    evidence_id: int,
    extractor_name: str,
    run_id: Optional[str] = None,
) -> Dict[str, int]:
    """
    Get warning counts by severity for an extractor.

    Useful for displaying warning badge on extractor run status.

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        extractor_name: Extractor name
        run_id: Optional run ID filter

    Returns:
        Dict with counts: {"total": int, "info": int, "warning": int, "error": int}
    """
    query = """
        SELECT severity, COUNT(*) as cnt
        FROM extraction_warnings
        WHERE evidence_id = ? AND extractor_name = ?
    """
    params: List[Any] = [evidence_id, extractor_name]

    if run_id:
        query += " AND run_id = ?"
        params.append(run_id)

    query += " GROUP BY severity"

    counts = {"total": 0, "info": 0, "warning": 0, "error": 0}
    for row in conn.execute(query, params):
        counts[row[0]] = row[1]
        counts["total"] += row[1]

    return counts
