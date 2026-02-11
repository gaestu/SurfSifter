"""Tests for autofill IBAN helpers and migration."""

from pathlib import Path
import sqlite3

import pytest

from core.database import (
    insert_autofill_iban,
    insert_autofill_ibans,
    get_autofill_ibans,
    get_distinct_autofill_iban_browsers,
    delete_autofill_ibans_by_run,
)


@pytest.fixture
def evidence_db() -> sqlite3.Connection:
    """In-memory evidence DB with autofill_ibans table from consolidated schema."""
    from core.database.connection import migrate

    migrations_dir = (
        Path(__file__).parent.parent.parent
        / "src"
        / "core"
        / "database"
        / "migrations_evidence"
    )
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    migrate(conn, migrations_dir)
    return conn


def test_insert_autofill_iban_single(evidence_db: sqlite3.Connection) -> None:
    """Insert one IBAN row and verify key fields."""
    insert_autofill_iban(
        evidence_db,
        1,
        "chrome",
        "local_ibans",
        nickname="Personal Account",
        prefix="DE89",
        suffix="3000",
        use_count=4,
        run_id="run_1",
        source_path="/Users/test/Web Data",
    )

    row = evidence_db.execute(
        "SELECT browser, source_table, nickname, prefix, suffix, use_count "
        "FROM autofill_ibans WHERE evidence_id = 1"
    ).fetchone()
    assert row is not None
    assert row["browser"] == "chrome"
    assert row["source_table"] == "local_ibans"
    assert row["nickname"] == "Personal Account"
    assert row["prefix"] == "DE89"
    assert row["suffix"] == "3000"
    assert row["use_count"] == 4


def test_insert_autofill_ibans_batch_and_filters(evidence_db: sqlite3.Connection) -> None:
    """Batch insert and verify browser/source/nickname filtering."""
    records = [
        {
            "browser": "chrome",
            "source_table": "local_ibans",
            "nickname": "Personal",
            "run_id": "run_a",
            "source_path": "/test/Web Data",
        },
        {
            "browser": "edge",
            "source_table": "masked_ibans",
            "nickname": "Work",
            "run_id": "run_a",
            "source_path": "/test/Web Data",
        },
        {
            "browser": "chrome",
            "source_table": "masked_ibans",
            "nickname": "Savings",
            "run_id": "run_b",
            "source_path": "/test/Web Data",
        },
    ]

    inserted = insert_autofill_ibans(evidence_db, 1, records)
    assert inserted == 3

    all_rows = get_autofill_ibans(evidence_db, 1)
    assert len(all_rows) == 3

    chrome_rows = get_autofill_ibans(evidence_db, 1, browser="chrome")
    assert len(chrome_rows) == 2
    assert all(r["browser"] == "chrome" for r in chrome_rows)

    masked_rows = get_autofill_ibans(evidence_db, 1, source_table="masked_ibans")
    assert len(masked_rows) == 2
    assert all(r["source_table"] == "masked_ibans" for r in masked_rows)

    nickname_rows = get_autofill_ibans(evidence_db, 1, nickname="Pers")
    assert len(nickname_rows) == 1
    assert nickname_rows[0]["nickname"] == "Personal"


def test_get_distinct_autofill_iban_browsers(evidence_db: sqlite3.Connection) -> None:
    """Distinct browser helper returns sorted unique names."""
    insert_autofill_ibans(
        evidence_db,
        1,
        [
            {"browser": "chrome", "source_table": "local_ibans", "run_id": "run_1", "source_path": "/test"},
            {"browser": "edge", "source_table": "masked_ibans", "run_id": "run_1", "source_path": "/test"},
            {"browser": "chrome", "source_table": "masked_ibans", "run_id": "run_2", "source_path": "/test"},
        ],
    )

    browsers = get_distinct_autofill_iban_browsers(evidence_db, 1)
    assert set(browsers) == {"chrome", "edge"}


def test_delete_autofill_ibans_by_run(evidence_db: sqlite3.Connection) -> None:
    """Delete by run_id removes only matching rows."""
    insert_autofill_ibans(
        evidence_db,
        1,
        [
            {"browser": "chrome", "source_table": "local_ibans", "nickname": "A", "run_id": "run_a", "source_path": "/test"},
            {"browser": "chrome", "source_table": "masked_ibans", "nickname": "B", "run_id": "run_a", "source_path": "/test"},
            {"browser": "edge", "source_table": "masked_ibans", "nickname": "C", "run_id": "run_b", "source_path": "/test"},
        ],
    )

    deleted = delete_autofill_ibans_by_run(evidence_db, 1, "run_a")
    assert deleted == 2

    remaining = get_autofill_ibans(evidence_db, 1)
    assert len(remaining) == 1
    assert remaining[0]["nickname"] == "C"
