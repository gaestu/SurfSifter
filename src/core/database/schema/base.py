"""
Database schema type definitions.

This module contains the foundational types for defining table schemas:
- ConflictAction: SQLite INSERT conflict resolution
- FilterOp: Query filter operations
- Column: Column definition
- FilterColumn: Filterable column specification
- OrderColumn: ORDER BY clause column
- TableSchema: Complete table definition

Extracted from db_schema.py during database refactor.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Callable, Dict, List, Optional


class ConflictAction(StrEnum):
    """SQLite INSERT conflict resolution."""

    FAIL = ""  # Plain INSERT INTO (default)
    IGNORE = "IGNORE"
    REPLACE = "REPLACE"
    ABORT = "ABORT"


class FilterOp(StrEnum):
    """Supported filter operations for get_* queries."""

    EQ = "="
    LIKE = "LIKE"
    GTE = ">="
    LTE = "<="
    IN = "IN"


@dataclass
class Column:
    """Column definition for table schema."""

    name: str
    sql_type: str = "TEXT"
    nullable: bool = True
    default: Any = None
    dict_key: Optional[str] = None
    exclude_from_select: bool = False


@dataclass
class FilterColumn:
    """Filterable column with operator support."""

    name: str
    ops: List[FilterOp] = field(default_factory=lambda: [FilterOp.EQ])


@dataclass
class OrderColumn:
    """Single column in ORDER BY clause."""

    name: str
    direction: str = "DESC"


@dataclass
class TableSchema:
    """Complete table definition."""

    name: str
    columns: List[Column]
    conflict_action: ConflictAction = ConflictAction.FAIL
    sortable_columns: List[str] = field(default_factory=list)
    default_order: List[OrderColumn] = field(default_factory=list)
    filterable_columns: List[FilterColumn] = field(default_factory=list)
    supports_run_delete: bool = True
    pre_insert_hook: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    post_fetch_hook: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None


# Type alias for filter specifications
FilterSpec = Dict[str, tuple[FilterOp, Any]]
