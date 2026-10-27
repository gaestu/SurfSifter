"""Evidence migration 0004: exact-domain URL browsing index (issue #61)."""
import shutil
import sqlite3
from pathlib import Path

from core.database import EVIDENCE_MIGRATIONS_DIR, migrate

INDEX = "idx_urls_evidence_domain_nocase_first_seen"


def _index_columns(conn: sqlite3.Connection):
    # (column name, descending, collation) for the key columns
    return [(row[2], row[3], row[4]) for row in conn.execute(f"PRAGMA index_xinfo({INDEX})") if row[5]]


def test_fresh_evidence_db_has_domain_time_index(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "evidence.sqlite")
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

    assert _index_columns(conn) == [
        ("evidence_id", 0, "BINARY"),
        ("domain", 0, "NOCASE"),
        ("first_seen_utc", 1, "BINARY"),
    ]
    conn.close()


def test_upgrade_adds_index_and_keeps_rows(tmp_path: Path) -> None:
    previous = tmp_path / "previous_migrations"
    previous.mkdir()
    for path in sorted(EVIDENCE_MIGRATIONS_DIR.glob("*.sql")):
        if int(path.name.split("_", 1)[0]) < 4:
            shutil.copy(path, previous / path.name)

    conn = sqlite3.connect(tmp_path / "evidence.sqlite")
    migrate(conn, migrations_dir=previous)
    assert not conn.execute("SELECT 1 FROM sqlite_master WHERE name = ?", (INDEX,)).fetchone()
    with conn:
        conn.executemany(
            "INSERT INTO urls(evidence_id, url, domain, discovered_by, first_seen_utc) VALUES (1, ?, ?, 'test', ?)",
            [(f"https://{d}/{i}", d, None if i % 2 else f"2024-01-0{1 + i % 9}") for i, d in enumerate(["Ex.com", "ex.com", "other.com"] * 5)],
        )

    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

    versions = [row[0] for row in conn.execute("SELECT version FROM schema_version ORDER BY version")]
    assert versions == list(range(1, len(versions) + 1)) and 4 in versions
    assert len(_index_columns(conn)) == 3
    assert conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0] == 15
    plan = " ".join(
        row[3]
        for row in conn.execute(
            "EXPLAIN QUERY PLAN SELECT COUNT(*) FROM urls WHERE evidence_id = 1 AND domain = 'EX.COM' COLLATE NOCASE"
        )
    )
    assert INDEX in plan
    assert conn.execute(
        "SELECT COUNT(*) FROM urls WHERE evidence_id = 1 AND domain = 'EX.COM' COLLATE NOCASE"
    ).fetchone()[0] == 10

    # Re-running is a no-op.
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
    conn.close()
