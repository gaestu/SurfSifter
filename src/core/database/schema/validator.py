"""
Database schema validation.

Validates TableSchema definitions against the baseline migration schema.
Ensures Python schema definitions match the actual SQL schema.

Extracted from db_schema_validator.py during database refactor.
Updated to apply all migrations, not just baseline.
"""
from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Dict, List, Optional

from .base import TableSchema

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations_evidence"


def _normalize_type(sql_type: Optional[str]) -> str:
    """Normalize SQL type for comparison."""
    if not sql_type:
        return ""
    return " ".join(sql_type.upper().split())


def _apply_all_migrations(conn: sqlite3.Connection) -> None:
    """Apply all migrations to in-memory database."""
    # Create schema_version table first (some migrations reference it)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    for migration_file in migration_files:
        sql = migration_file.read_text(encoding="utf-8")
        conn.executescript(sql)


def _apply_ensure_column_functions(conn: sqlite3.Connection) -> None:
    """Apply column-adding functions that run after migrations.

    These functions handle ALTER TABLE ADD COLUMN operations that can't be
    done directly in migrations due to SQLite's lack of IF NOT EXISTS support
    for ADD COLUMN. They're called by upgrade_evidence_database() after
    migrations are applied.

    We import them here to avoid circular imports at module level.
    """
    from ..manager import (
        _ensure_file_list_partition_columns,
        _ensure_cookies_origin_attributes_columns,
        _ensure_extensions_preferences_columns,
        _ensure_jump_list_working_directory_column,
        _ensure_browser_history_forensic_columns,
        _ensure_autofill_enhancement_columns,
    )

    _ensure_file_list_partition_columns(conn)
    _ensure_cookies_origin_attributes_columns(conn)
    _ensure_extensions_preferences_columns(conn)
    _ensure_jump_list_working_directory_column(conn)
    _ensure_browser_history_forensic_columns(conn)
    _ensure_autofill_enhancement_columns(conn)


def _baseline_connection() -> sqlite3.Connection:
    """Create in-memory database with all migrations applied."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _apply_all_migrations(conn)
    _apply_ensure_column_functions(conn)
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if table exists in database."""
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table_name,),
    ).fetchone()
    return row is not None


def _fetch_columns(conn: sqlite3.Connection, table_name: str) -> Dict[str, Dict[str, object]]:
    """Fetch column information for a table."""
    cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
    columns = {}
    for row in cursor.fetchall():
        columns[row["name"]] = {
            "type": row["type"],
            "notnull": bool(row["notnull"] or row["pk"]),
        }
    return columns


def validate_schema(schema: TableSchema, conn: Optional[sqlite3.Connection] = None) -> List[str]:
    """
    Validate a TableSchema against the consolidated baseline schema.

    Args:
        schema: TableSchema definition to validate
        conn: Optional database connection (creates in-memory if not provided)

    Returns:
        List of validation errors (empty if valid)
    """
    owns_conn = conn is None
    if conn is None:
        conn = _baseline_connection()

    try:
        errors: List[str] = []

        if not _table_exists(conn, schema.name):
            errors.append(f"Table '{schema.name}' not found in baseline schema")
            return errors

        db_columns = _fetch_columns(conn, schema.name)
        schema_columns = {col.name: col for col in schema.columns}

        extra_db_columns = set(db_columns.keys()) - set(schema_columns.keys())
        for col in sorted(extra_db_columns):
            errors.append(f"Column '{col}' in DB but not in TableSchema for {schema.name}")

        extra_schema_columns = set(schema_columns.keys()) - set(db_columns.keys())
        for col in sorted(extra_schema_columns):
            errors.append(f"Column '{col}' in TableSchema but not in DB for {schema.name}")

        for col_name, col_def in schema_columns.items():
            if col_name not in db_columns:
                continue
            db_info = db_columns[col_name]
            schema_type = _normalize_type(col_def.sql_type)
            db_type = _normalize_type(db_info["type"])
            if schema_type != db_type:
                errors.append(
                    f"Column '{col_name}' type mismatch on {schema.name}: "
                    f"schema={schema_type} db={db_type}"
                )

            db_notnull = bool(db_info["notnull"])
            if col_def.nullable and db_notnull:
                errors.append(
                    f"Column '{col_name}' marked nullable but DB has NOT NULL on {schema.name}"
                )
            if not col_def.nullable and not db_notnull:
                errors.append(
                    f"Column '{col_name}' marked NOT NULL but DB allows NULL on {schema.name}"
                )

        return errors
    finally:
        if owns_conn:
            conn.close()


def validate_all_schemas() -> Dict[str, List[str]]:
    """
    Validate all registered schemas against the baseline schema.

    Returns:
        Dictionary mapping table names to their validation errors.
        Empty dict means all schemas are valid.
    """
    from .definitions import TABLE_SCHEMAS

    conn = _baseline_connection()
    try:
        results: Dict[str, List[str]] = {}
        for schema in TABLE_SCHEMAS.values():
            errors = validate_schema(schema, conn)
            if errors:
                results[schema.name] = errors
        return results
    finally:
        conn.close()
