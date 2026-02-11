"""Tests for generic CRUD helpers in db_helpers.py."""

import sqlite3
from typing import Any, Dict

import pytest

from core.database import (
    FilterSpec,
    delete_by_run,
    get_count,
    get_distinct_values,
    get_rows,
    insert_row,
    insert_rows,
)
from core.database import (
    Column,
    ConflictAction,
    FilterColumn,
    FilterOp,
    OrderColumn,
    TableSchema,
)


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mem_db() -> sqlite3.Connection:
    """In-memory SQLite database with test table."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE test_items (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            value TEXT,
            score INTEGER DEFAULT 0,
            run_id TEXT,
            created_at TEXT
        )
        """
    )
    conn.commit()
    return conn


@pytest.fixture
def test_schema() -> TableSchema:
    """Basic test schema matching mem_db table."""
    return TableSchema(
        name="test_items",
        columns=[
            Column("id", "INTEGER", nullable=False),
            Column("evidence_id", "INTEGER", nullable=False),
            Column("name", "TEXT", nullable=False),
            Column("value", "TEXT"),
            Column("score", "INTEGER", default=0),
            Column("run_id", "TEXT"),
            Column("created_at", "TEXT"),
        ],
        conflict_action=ConflictAction.FAIL,
        sortable_columns=["id", "name", "score", "created_at"],
        default_order=[OrderColumn("score", "DESC")],
        filterable_columns=[
            FilterColumn("name", [FilterOp.EQ, FilterOp.LIKE]),
            FilterColumn("score", [FilterOp.EQ, FilterOp.GTE, FilterOp.LTE]),
            FilterColumn("run_id", [FilterOp.EQ]),
        ],
        supports_run_delete=True,
    )


@pytest.fixture
def schema_no_run_delete(test_schema: TableSchema) -> TableSchema:
    """Schema that doesn't support run-based deletion."""
    return TableSchema(
        name=test_schema.name,
        columns=test_schema.columns,
        supports_run_delete=False,
    )


# ---------------------------------------------------------------------------
# insert_rows tests
# ---------------------------------------------------------------------------


def test_insert_rows_basic(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Insert multiple rows in batch."""
    records = [
        {"name": "item1", "value": "val1", "score": 10},
        {"name": "item2", "value": "val2", "score": 20},
    ]
    count = insert_rows(mem_db, test_schema, evidence_id=1, records=records)
    assert count == 2

    rows = mem_db.execute("SELECT name, score FROM test_items ORDER BY name").fetchall()
    assert len(rows) == 2
    assert rows[0]["name"] == "item1"
    assert rows[1]["score"] == 20


def test_insert_rows_empty(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Empty records list returns 0."""
    count = insert_rows(mem_db, test_schema, evidence_id=1, records=[])
    assert count == 0


def test_insert_rows_with_defaults(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Missing fields use column defaults."""
    records = [{"name": "item_default"}]  # score defaults to 0
    insert_rows(mem_db, test_schema, evidence_id=1, records=records)

    row = mem_db.execute("SELECT score FROM test_items WHERE name = ?", ("item_default",)).fetchone()
    assert row["score"] == 0


def test_insert_rows_with_run_id(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Run ID stored for re-ingestion support."""
    records = [{"name": "tracked", "run_id": "run_abc123"}]
    insert_rows(mem_db, test_schema, evidence_id=1, records=records)

    row = mem_db.execute("SELECT run_id FROM test_items WHERE name = ?", ("tracked",)).fetchone()
    assert row["run_id"] == "run_abc123"


# ---------------------------------------------------------------------------
# insert_row tests
# ---------------------------------------------------------------------------


def test_insert_row_returns_lastrowid(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Single insert returns the new row ID."""
    row_id = insert_row(mem_db, test_schema, evidence_id=1, record={"name": "single"})
    assert row_id == 1

    row_id2 = insert_row(mem_db, test_schema, evidence_id=1, record={"name": "second"})
    assert row_id2 == 2


# ---------------------------------------------------------------------------
# get_rows tests
# ---------------------------------------------------------------------------


def test_get_rows_basic(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Retrieve all rows for an evidence."""
    insert_rows(mem_db, test_schema, 1, [{"name": "a"}, {"name": "b"}])
    insert_rows(mem_db, test_schema, 2, [{"name": "other_evidence"}])

    rows = get_rows(mem_db, test_schema, evidence_id=1)
    assert len(rows) == 2
    names = {r["name"] for r in rows}
    assert names == {"a", "b"}


def test_get_rows_filter_eq(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Filter by equality."""
    insert_rows(mem_db, test_schema, 1, [
        {"name": "target", "score": 100},
        {"name": "other", "score": 50},
    ])

    filters: FilterSpec = {"name": (FilterOp.EQ, "target")}
    rows = get_rows(mem_db, test_schema, evidence_id=1, filters=filters)
    assert len(rows) == 1
    assert rows[0]["name"] == "target"


def test_get_rows_filter_like(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Filter with LIKE pattern."""
    insert_rows(mem_db, test_schema, 1, [
        {"name": "gambling_site"},
        {"name": "news_site"},
        {"name": "gambling_app"},
    ])

    filters: FilterSpec = {"name": (FilterOp.LIKE, "%gambling%")}
    rows = get_rows(mem_db, test_schema, evidence_id=1, filters=filters)
    assert len(rows) == 2


def test_get_rows_filter_gte(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Filter with >= operator."""
    insert_rows(mem_db, test_schema, 1, [
        {"name": "low", "score": 10},
        {"name": "mid", "score": 50},
        {"name": "high", "score": 100},
    ])

    filters: FilterSpec = {"score": (FilterOp.GTE, 50)}
    rows = get_rows(mem_db, test_schema, evidence_id=1, filters=filters)
    assert len(rows) == 2
    scores = {r["score"] for r in rows}
    assert scores == {50, 100}


def test_get_rows_filter_invalid_column(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Filtering by non-filterable column raises ValueError."""
    filters: FilterSpec = {"value": (FilterOp.EQ, "anything")}  # 'value' not in filterable_columns

    with pytest.raises(ValueError, match="not filterable"):
        get_rows(mem_db, test_schema, evidence_id=1, filters=filters)


def test_get_rows_filter_invalid_operator(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Using disallowed operator raises ValueError."""
    # 'name' only allows EQ and LIKE, not GTE
    filters: FilterSpec = {"name": (FilterOp.GTE, "abc")}

    with pytest.raises(ValueError, match="not allowed"):
        get_rows(mem_db, test_schema, evidence_id=1, filters=filters)


def test_get_rows_limit_zero(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """limit=0 returns empty list, not unbounded."""
    insert_rows(mem_db, test_schema, 1, [{"name": "a"}, {"name": "b"}])

    rows = get_rows(mem_db, test_schema, evidence_id=1, limit=0)
    assert rows == []


def test_get_rows_limit_and_offset(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Pagination with limit and offset."""
    insert_rows(mem_db, test_schema, 1, [
        {"name": f"item{i}", "score": i} for i in range(10)
    ])

    # Default order is score DESC
    page1 = get_rows(mem_db, test_schema, evidence_id=1, limit=3, offset=0)
    assert len(page1) == 3
    assert page1[0]["score"] == 9  # highest first

    page2 = get_rows(mem_db, test_schema, evidence_id=1, limit=3, offset=3)
    assert len(page2) == 3
    assert page2[0]["score"] == 6


def test_get_rows_order_by_single(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Single-column ORDER BY."""
    insert_rows(mem_db, test_schema, 1, [
        {"name": "z_last"},
        {"name": "a_first"},
        {"name": "m_middle"},
    ])

    rows = get_rows(mem_db, test_schema, evidence_id=1, order_by="name", order_dir="ASC")
    names = [r["name"] for r in rows]
    assert names == ["a_first", "m_middle", "z_last"]


def test_get_rows_order_by_multi(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Multi-column ORDER BY with mixed directions."""
    insert_rows(mem_db, test_schema, 1, [
        {"name": "a", "score": 10},
        {"name": "b", "score": 10},
        {"name": "c", "score": 20},
    ])

    order = [OrderColumn("score", "DESC"), OrderColumn("name", "ASC")]
    rows = get_rows(mem_db, test_schema, evidence_id=1, order_by=order)

    # score DESC first (20, then 10s), name ASC within same score
    assert rows[0]["name"] == "c"  # score 20
    assert rows[1]["name"] == "a"  # score 10, name 'a' before 'b'
    assert rows[2]["name"] == "b"  # score 10


def test_get_rows_order_by_invalid_column(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """ORDER BY non-sortable column raises ValueError."""
    with pytest.raises(ValueError, match="not allowed for ORDER BY"):
        get_rows(mem_db, test_schema, evidence_id=1, order_by="value")  # not in sortable_columns


# ---------------------------------------------------------------------------
# delete_by_run tests
# ---------------------------------------------------------------------------


def test_delete_by_run_basic(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Delete rows by run_id."""
    insert_rows(mem_db, test_schema, 1, [
        {"name": "keep", "run_id": "run_old"},
        {"name": "delete1", "run_id": "run_new"},
        {"name": "delete2", "run_id": "run_new"},
    ])

    deleted = delete_by_run(mem_db, test_schema, evidence_id=1, run_id="run_new")
    assert deleted == 2

    remaining = mem_db.execute("SELECT name FROM test_items").fetchall()
    assert len(remaining) == 1
    assert remaining[0]["name"] == "keep"


def test_delete_by_run_respects_evidence_id(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Delete only affects specified evidence_id."""
    insert_rows(mem_db, test_schema, 1, [{"name": "ev1", "run_id": "shared_run"}])
    insert_rows(mem_db, test_schema, 2, [{"name": "ev2", "run_id": "shared_run"}])

    deleted = delete_by_run(mem_db, test_schema, evidence_id=1, run_id="shared_run")
    assert deleted == 1

    # Evidence 2's row still exists
    row = mem_db.execute("SELECT name FROM test_items WHERE evidence_id = 2").fetchone()
    assert row["name"] == "ev2"


def test_delete_by_run_not_supported(
    mem_db: sqlite3.Connection, schema_no_run_delete: TableSchema
) -> None:
    """Tables without run delete support raise ValueError."""
    with pytest.raises(ValueError, match="does not support run-based deletion"):
        delete_by_run(mem_db, schema_no_run_delete, evidence_id=1, run_id="any")


# ---------------------------------------------------------------------------
# get_distinct_values tests
# ---------------------------------------------------------------------------


def test_get_distinct_values_basic(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Get distinct values for a column."""
    insert_rows(mem_db, test_schema, 1, [
        {"name": "chrome"},
        {"name": "firefox"},
        {"name": "chrome"},  # duplicate
        {"name": "edge"},
    ])

    values = get_distinct_values(mem_db, test_schema, evidence_id=1, column="name")
    assert sorted(values) == ["chrome", "edge", "firefox"]


def test_get_distinct_values_excludes_null(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """NULL values excluded from distinct list."""
    insert_rows(mem_db, test_schema, 1, [
        {"name": "has_value", "value": "x"},
        {"name": "no_value", "value": None},
    ])

    values = get_distinct_values(mem_db, test_schema, evidence_id=1, column="value")
    assert values == ["x"]


def test_get_distinct_values_invalid_column(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Invalid column raises ValueError."""
    with pytest.raises(ValueError, match="not allowed for DISTINCT"):
        get_distinct_values(mem_db, test_schema, evidence_id=1, column="nonexistent")


# ---------------------------------------------------------------------------
# get_count tests
# ---------------------------------------------------------------------------


def test_get_count_basic(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Count rows for evidence."""
    insert_rows(mem_db, test_schema, 1, [{"name": "a"}, {"name": "b"}])
    insert_rows(mem_db, test_schema, 2, [{"name": "other"}])

    count = get_count(mem_db, test_schema, evidence_id=1)
    assert count == 2


def test_get_count_with_filter(mem_db: sqlite3.Connection, test_schema: TableSchema) -> None:
    """Count with filter applied."""
    insert_rows(mem_db, test_schema, 1, [
        {"name": "high", "score": 100},
        {"name": "low", "score": 10},
    ])

    filters: FilterSpec = {"score": (FilterOp.GTE, 50)}
    count = get_count(mem_db, test_schema, evidence_id=1, filters=filters)
    assert count == 1


# ---------------------------------------------------------------------------
# Pre-insert hook tests
# ---------------------------------------------------------------------------


def test_pre_insert_hook_transforms_record(mem_db: sqlite3.Connection) -> None:
    """Pre-insert hook modifies records before insert."""
    def uppercase_name(record: Dict[str, Any]) -> Dict[str, Any]:
        updated = dict(record)
        if "name" in updated:
            updated["name"] = updated["name"].upper()
        return updated

    schema_with_hook = TableSchema(
        name="test_items",
        columns=[
            Column("id", "INTEGER", nullable=False),
            Column("evidence_id", "INTEGER", nullable=False),
            Column("name", "TEXT", nullable=False),
            Column("value", "TEXT"),
            Column("score", "INTEGER", default=0),
            Column("run_id", "TEXT"),
            Column("created_at", "TEXT"),
        ],
        pre_insert_hook=uppercase_name,
    )

    insert_rows(mem_db, schema_with_hook, 1, [{"name": "lowercase"}])

    row = mem_db.execute("SELECT name FROM test_items").fetchone()
    assert row["name"] == "LOWERCASE"


# ---------------------------------------------------------------------------
# Conflict action tests
# ---------------------------------------------------------------------------


def test_conflict_action_ignore(mem_db: sqlite3.Connection) -> None:
    """OR IGNORE skips duplicates without error."""
    # Create table with unique constraint
    mem_db.execute("DROP TABLE test_items")
    mem_db.execute(
        """
        CREATE TABLE test_items (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            name TEXT NOT NULL UNIQUE,
            value TEXT,
            score INTEGER DEFAULT 0,
            run_id TEXT,
            created_at TEXT
        )
        """
    )

    schema_ignore = TableSchema(
        name="test_items",
        columns=[
            Column("id", "INTEGER", nullable=False),
            Column("evidence_id", "INTEGER", nullable=False),
            Column("name", "TEXT", nullable=False),
            Column("value", "TEXT"),
            Column("score", "INTEGER", default=0),
            Column("run_id", "TEXT"),
            Column("created_at", "TEXT"),
        ],
        conflict_action=ConflictAction.IGNORE,
    )

    # Insert first
    insert_rows(mem_db, schema_ignore, 1, [{"name": "unique_name", "score": 10}])
    # Insert duplicate - should be ignored
    insert_rows(mem_db, schema_ignore, 1, [{"name": "unique_name", "score": 99}])

    row = mem_db.execute("SELECT score FROM test_items WHERE name = ?", ("unique_name",)).fetchone()
    assert row["score"] == 10  # original value preserved


def test_conflict_action_replace(mem_db: sqlite3.Connection) -> None:
    """OR REPLACE updates on conflict."""
    mem_db.execute("DROP TABLE test_items")
    mem_db.execute(
        """
        CREATE TABLE test_items (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            name TEXT NOT NULL UNIQUE,
            value TEXT,
            score INTEGER DEFAULT 0,
            run_id TEXT,
            created_at TEXT
        )
        """
    )

    schema_replace = TableSchema(
        name="test_items",
        columns=[
            Column("id", "INTEGER", nullable=False),
            Column("evidence_id", "INTEGER", nullable=False),
            Column("name", "TEXT", nullable=False),
            Column("value", "TEXT"),
            Column("score", "INTEGER", default=0),
            Column("run_id", "TEXT"),
            Column("created_at", "TEXT"),
        ],
        conflict_action=ConflictAction.REPLACE,
    )

    insert_rows(mem_db, schema_replace, 1, [{"name": "replaceable", "score": 10}])
    insert_rows(mem_db, schema_replace, 1, [{"name": "replaceable", "score": 99}])

    row = mem_db.execute("SELECT score FROM test_items WHERE name = ?", ("replaceable",)).fetchone()
    assert row["score"] == 99  # replaced with new value
