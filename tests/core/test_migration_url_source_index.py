"""Evidence migration 0005: single-source URL browsing index (issue #67)."""
import shutil
import sqlite3
from pathlib import Path

from core.database import EVIDENCE_MIGRATIONS_DIR, migrate
from core.database.helpers import deduplicate_urls

INDEX = "idx_urls_evidence_source"


def _index_columns(conn: sqlite3.Connection):
    # (column name, descending, collation) for the key columns
    return [(row[2], row[3], row[4]) for row in conn.execute(f"PRAGMA index_xinfo({INDEX})") if row[5]]


def _plan(conn: sqlite3.Connection, sql: str, params=()) -> str:
    return " | ".join(row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql, params))


def _migrate_before_0005(conn: sqlite3.Connection, tmp_path: Path) -> None:
    previous = tmp_path / "previous_migrations"
    previous.mkdir()
    for path in sorted(EVIDENCE_MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name.split("_", 1)[0]) < 5:
            shutil.copy(path, previous / path.name)
    migrate(conn, migrations_dir=previous)


def _insert_urls(conn: sqlite3.Connection) -> None:
    # Source "carved" has no timestamps; "history" alternates; every URL twice.
    with conn:
        conn.executemany(
            "INSERT INTO urls(evidence_id, url, domain, discovered_by, first_seen_utc) VALUES (1, ?, 'ex.com', ?, ?)",
            [
                (f"https://ex.com/{i % 10}", source, None if source == "carved" or i % 2 else f"2024-01-{1 + i % 9:02d}")
                for i in range(20)
                for source in ("carved", "history")
            ],
        )


def test_fresh_evidence_db_has_source_time_index(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "evidence.sqlite")
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

    assert _index_columns(conn) == [
        ("evidence_id", 0, "BINARY"),
        ("discovered_by", 0, "BINARY"),
        ("first_seen_utc", 1, "BINARY"),
    ]
    conn.close()


def test_upgrade_replaces_index_and_keeps_rows(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "evidence.sqlite")
    _migrate_before_0005(conn, tmp_path)
    assert [c[0] for c in _index_columns(conn)] == ["evidence_id", "discovered_by"]
    _insert_urls(conn)
    two_sources = (
        "SELECT id FROM urls WHERE evidence_id = 1 AND discovered_by IN ('carved', 'history') "
        "ORDER BY first_seen_utc DESC, id ASC"
    )
    before = conn.execute(two_sources).fetchall()

    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

    versions = [row[0] for row in conn.execute("SELECT version FROM schema_version ORDER BY version")]
    assert versions == list(range(1, len(versions) + 1)) and 5 in versions
    assert len(_index_columns(conn)) == 3
    assert conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", (INDEX,)
    ).fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0] == 40
    assert conn.execute(two_sources).fetchall() == before

    # Single-source first pages of both phases and the count are index seeks.
    for sql in (
        "SELECT id FROM urls WHERE evidence_id = 1 AND discovered_by IN ('carved') "
        "AND first_seen_utc IS NOT NULL ORDER BY first_seen_utc DESC, id ASC LIMIT 501",
        "SELECT id FROM urls WHERE evidence_id = 1 AND discovered_by IN ('carved') "
        "AND first_seen_utc IS NULL ORDER BY id ASC LIMIT 501",
        "SELECT COUNT(*) FROM urls WHERE evidence_id = 1 AND discovered_by IN ('carved')",
    ):
        plan = _plan(conn, sql)
        assert f"INDEX {INDEX}" in plan and "TEMP B-TREE" not in plan, plan
    assert conn.execute(
        "SELECT COUNT(*) FROM urls WHERE evidence_id = 1 AND discovered_by = 'carved' AND first_seen_utc IS NOT NULL"
    ).fetchone()[0] == 0

    # Re-running is a no-op, also when the 0005 script itself runs again.
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
    (script,) = EVIDENCE_MIGRATIONS_DIR.glob("0005_*.sql")
    conn.executescript(script.read_text())
    assert len(_index_columns(conn)) == 3
    assert conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0] == 40
    conn.close()


def test_deduplication_reindexes_replaced_index(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "evidence.sqlite")
    conn.row_factory = sqlite3.Row
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
    _insert_urls(conn)

    result = deduplicate_urls(
        conn, 1, ["carved", "history"], unique_by_first_seen=False, unique_by_last_seen=False
    )

    assert result["total_before"] == 40 and result["total_after"] == 10
    assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    conn.close()
