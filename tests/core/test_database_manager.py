from pathlib import Path

from core.database import (
    DatabaseManager,
    ensure_case_structure,
    slugify_label,
)


def test_slugify_label_produces_stable_slugs() -> None:
    assert slugify_label("Primary Evidence #1", 7) == "primary-evidence-1"
    assert slugify_label("   123_Main Drive   ", 2) == "ev-123-main-drive"
    # Label is now required, None should raise ValueError
    try:
        slugify_label(None, 3)
        assert False, "Expected ValueError for None label"
    except ValueError as e:
        assert "Evidence label is required" in str(e)


def test_case_connection_creation(tmp_path: Path) -> None:
    case_db_path = tmp_path / "test_surfsifter.sqlite"
    manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
    conn1 = manager.get_case_conn()
    conn2 = manager.get_case_conn()
    assert manager.case_db_path.exists()
    assert conn1.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone() is not None
    assert conn2.execute("SELECT name FROM sqlite_master LIMIT 1").fetchone() is not None
    # Connection caching returns same connection within a thread
    assert conn1 is conn2
    # No need to close individually since they're the same connection
    manager.close_all()


def test_evidence_connection_defaults_to_case_db(tmp_path: Path) -> None:
    case_db_path = tmp_path / "test_surfsifter.sqlite"
    manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
    case_conn = manager.get_case_conn()
    evidence_conn = manager.get_evidence_conn(1, label="Disk A")
    assert evidence_conn.execute("SELECT name FROM sqlite_master LIMIT 1") is not None
    expected_path = tmp_path / "evidences" / "disk-a" / "evidence_disk-a.sqlite"
    # Directories are created even if the DB is not yet used
    assert expected_path.parent.exists()
    assert manager.evidence_db_path(1, "Disk A", create_dirs=False) == expected_path
    case_conn.close()
    evidence_conn.close()
    manager.close_all()


def test_ensure_case_structure_creates_directories(tmp_path: Path) -> None:
    evidences_dir = ensure_case_structure(tmp_path)
    assert evidences_dir == tmp_path.resolve() / "evidences"
    assert evidences_dir.exists()


def test_evidence_db_writes_directly(tmp_path: Path) -> None:
    """Test that evidence DB can be written to directly without replication."""
    case_db_path = tmp_path / "test_surfsifter.sqlite"
    manager = DatabaseManager(tmp_path, case_db_path=case_db_path, enable_split=True)
    case_conn = manager.get_case_conn()
    case_conn.execute(
        "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
        ("CASE-1", "Case", "2025-01-01T00:00:00Z"),
    )
    case_conn.execute(
        "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
        (1, "Evidence One", "/dev/null", "2025-01-01T00:00:00Z"),
    )
    case_conn.commit()

    # Write directly to evidence DB
    evidence_conn = manager.get_evidence_conn(1, "Evidence One")
    evidence_conn.execute(
        "INSERT INTO urls(evidence_id, url, discovered_by) VALUES (?, ?, ?)",
        (1, "http://example.com", "test"),
    )
    evidence_conn.commit()

    # Verify data is in evidence DB
    rows = evidence_conn.execute("SELECT url FROM urls WHERE evidence_id = 1").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "http://example.com"

    # Verify evidence DB has expected schema
    tables = evidence_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_names = {row[0] for row in tables}
    expected_tables = {
        'schema_version', 'urls', 'browser_history', 'images', 'hash_matches',
        'os_indicators', 'timeline', 'platform_detections', 'process_log'
    }
    assert expected_tables.issubset(table_names), f"Missing tables: {expected_tables - table_names}"

    evidence_conn.close()
    case_conn.close()


def test_get_evidence_conn_split_disabled_does_not_create_evidence_dirs(tmp_path: Path) -> None:
    case_db_path = tmp_path / "test_surfsifter.sqlite"
    manager = DatabaseManager(tmp_path, case_db_path=case_db_path, enable_split=False)

    case_conn = manager.get_case_conn()
    evidence_conn = manager.get_evidence_conn(7, "Disk A")

    assert evidence_conn is case_conn
    assert not (tmp_path / "evidences" / "disk-a").exists()

    manager.close_all()
