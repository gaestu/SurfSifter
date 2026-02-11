"""
Generic database CRUD helpers.

This module provides generic operations for TableSchema-based tables:
- insert_rows: Batch insert
- insert_row: Single row insert
- get_rows: Generic SELECT with filtering/pagination
- delete_by_run: Run-based deletion for re-ingestion
- get_distinct_values: DISTINCT column values
- get_count: Row count with optional filters

Extracted from db_helpers.py during database refactor.
"""
from __future__ import annotations

from contextlib import contextmanager
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from ..schema import ConflictAction, FilterOp, FilterSpec, OrderColumn, TableSchema


@contextmanager
def _dict_cursor(conn: sqlite3.Connection):
    """Context manager for dict-returning cursor without mutating connection."""
    old_factory = conn.row_factory
    try:
        conn.row_factory = sqlite3.Row
        yield conn.cursor()
    finally:
        conn.row_factory = old_factory


def _validate_column(
    schema: TableSchema,
    column: str,
    allowed: List[str],
    context: str,
) -> None:
    """Validate column name against whitelist to prevent SQL injection."""
    if column not in allowed:
        raise ValueError(
            f"Column '{column}' not allowed for {context} on table '{schema.name}'. "
            f"Allowed: {allowed}"
        )


def _build_order_clause(
    schema: TableSchema,
    order_by: Optional[Union[str, List[OrderColumn]]] = None,
    order_dir: str = "DESC",
) -> str:
    """
    Build ORDER BY clause supporting single-column (legacy) and multi-column specs.
    """
    if order_by is None:
        if schema.default_order:
            order_by = schema.default_order
        else:
            return ""

    if isinstance(order_by, str):
        _validate_column(schema, order_by, schema.sortable_columns, "ORDER BY")
        if order_dir.upper() not in ("ASC", "DESC"):
            raise ValueError(f"Invalid order_dir: {order_dir}")
        return f" ORDER BY {order_by} {order_dir.upper()}"

    parts = []
    for spec in order_by:
        _validate_column(schema, spec.name, schema.sortable_columns, "ORDER BY")
        if spec.direction.upper() not in ("ASC", "DESC"):
            raise ValueError(f"Invalid direction for {spec.name}: {spec.direction}")
        parts.append(f"{spec.name} {spec.direction.upper()}")

    return f" ORDER BY {', '.join(parts)}" if parts else ""


def insert_rows(
    conn: sqlite3.Connection,
    schema: TableSchema,
    evidence_id: int,
    records: Iterable[Dict[str, Any]],
) -> int:
    """Generic batch insert for any artifact table."""
    columns = [c.name for c in schema.columns if c.name != "id"]
    placeholders = ", ".join("?" * len(columns))
    column_names = ", ".join(columns)

    if schema.conflict_action == ConflictAction.FAIL:
        insert_sql = f"INSERT INTO {schema.name}"
    else:
        insert_sql = f"INSERT OR {schema.conflict_action} INTO {schema.name}"

    rows = []
    for record in records:
        if schema.pre_insert_hook:
            record = schema.pre_insert_hook(record)
        row: List[Any] = []
        for col in schema.columns:
            if col.name == "id":
                continue
            if col.name == "evidence_id":
                row.append(evidence_id)
            else:
                key = col.dict_key or col.name
                row.append(record.get(key, col.default))
        rows.append(tuple(row))

    if not rows:
        return 0

    before = conn.total_changes
    with conn:
        conn.executemany(
            f"{insert_sql} ({column_names}) VALUES ({placeholders})",
            rows,
        )
    return conn.total_changes - before


def insert_row(
    conn: sqlite3.Connection,
    schema: TableSchema,
    evidence_id: int,
    record: Dict[str, Any],
    *,
    commit: bool = False,
) -> int:
    """Generic single-row insert with optional commit."""
    columns = [c.name for c in schema.columns if c.name != "id"]
    placeholders = ", ".join("?" * len(columns))
    column_names = ", ".join(columns)

    if schema.conflict_action == ConflictAction.FAIL:
        insert_sql = f"INSERT INTO {schema.name}"
    else:
        insert_sql = f"INSERT OR {schema.conflict_action} INTO {schema.name}"

    if schema.pre_insert_hook:
        record = schema.pre_insert_hook(record)

    row: List[Any] = []
    for col in schema.columns:
        if col.name == "id":
            continue
        if col.name == "evidence_id":
            row.append(evidence_id)
        else:
            key = col.dict_key or col.name
            row.append(record.get(key, col.default))

    if commit:
        with conn:
            cursor = conn.execute(
                f"{insert_sql} ({column_names}) VALUES ({placeholders})",
                row,
            )
    else:
        cursor = conn.execute(
            f"{insert_sql} ({column_names}) VALUES ({placeholders})",
            row,
        )
    return cursor.lastrowid


def get_rows(
    conn: sqlite3.Connection,
    schema: TableSchema,
    evidence_id: int,
    *,
    filters: Optional[FilterSpec] = None,
    limit: Optional[int] = None,
    offset: int = 0,
    order_by: Optional[Union[str, List[OrderColumn]]] = None,
    order_dir: str = "DESC",
    include_excluded: bool = False,
) -> List[Dict[str, Any]]:
    """Generic SELECT for any artifact table."""
    if include_excluded:
        columns = [c.name for c in schema.columns]
    else:
        columns = [c.name for c in schema.columns if not c.exclude_from_select]

    sql = f"SELECT {', '.join(columns)} FROM {schema.name} WHERE evidence_id = ?"
    params: List[Any] = [evidence_id]

    if filters:
        allowed_filters = {fc.name: fc.ops for fc in schema.filterable_columns}
        for col, (op, val) in filters.items():
            if col not in allowed_filters:
                raise ValueError(f"Column '{col}' not filterable on table '{schema.name}'")
            if op not in allowed_filters[col]:
                raise ValueError(f"Operator '{op}' not allowed for column '{col}'")

            if op == FilterOp.IN:
                placeholders = ", ".join("?" * len(val))
                sql += f" AND {col} IN ({placeholders})"
                params.extend(val)
            else:
                sql += f" AND {col} {op} ?"
                params.append(val)

    sql += _build_order_clause(schema, order_by, order_dir)

    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

    with _dict_cursor(conn) as cursor:
        cursor.execute(sql, params)
        rows = [dict(row) for row in cursor.fetchall()]

    if schema.post_fetch_hook:
        rows = [schema.post_fetch_hook(row) for row in rows]

    return rows


def delete_by_run(
    conn: sqlite3.Connection,
    schema: TableSchema,
    evidence_id: int,
    run_id: str,
) -> int:
    """Delete rows by run_id for re-ingestion support."""
    if not schema.supports_run_delete:
        raise ValueError(f"Table {schema.name} does not support run-based deletion")

    before = conn.total_changes
    with conn:
        conn.execute(
            f"DELETE FROM {schema.name} WHERE evidence_id = ? AND run_id = ?",
            (evidence_id, run_id),
        )
    return conn.total_changes - before


def get_distinct_values(
    conn: sqlite3.Connection,
    schema: TableSchema,
    evidence_id: int,
    column: str,
) -> List[str]:
    """Get distinct values for a column."""
    all_columns = [c.name for c in schema.columns]
    _validate_column(schema, column, all_columns, "DISTINCT")

    cursor = conn.execute(
        f"SELECT DISTINCT {column} FROM {schema.name} WHERE evidence_id = ? AND {column} IS NOT NULL ORDER BY {column}",
        (evidence_id,),
    )
    return [row[0] for row in cursor.fetchall()]


def get_count(
    conn: sqlite3.Connection,
    schema: TableSchema,
    evidence_id: int,
    filters: Optional[FilterSpec] = None,
) -> int:
    """Get row count for a table with optional filters."""
    sql = f"SELECT COUNT(*) FROM {schema.name} WHERE evidence_id = ?"
    params: List[Any] = [evidence_id]

    if filters:
        allowed_filters = {fc.name: fc.ops for fc in schema.filterable_columns}
        for col, (op, val) in filters.items():
            if col not in allowed_filters:
                raise ValueError(f"Column '{col}' not filterable on table '{schema.name}'")
            if op not in allowed_filters[col]:
                raise ValueError(f"Operator '{op}' not allowed for column '{col}'")
            sql += f" AND {col} {op} ?"
            params.append(val)

    cursor = conn.execute(sql, params)
    return cursor.fetchone()[0]
