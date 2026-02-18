"""Tests for embedded Chromium root discovery."""

import sqlite3

import pytest

from extractors.browser.chromium._embedded_discovery import (
    discover_artifacts_with_embedded_roots,
    discover_embedded_roots,
    get_embedded_root_paths,
)


@pytest.fixture
def evidence_db(tmp_path):
    db_path = tmp_path / "embedded.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE file_list (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            file_path TEXT,
            file_name TEXT,
            extension TEXT,
            size_bytes INTEGER,
            inode INTEGER,
            deleted INTEGER DEFAULT 0,
            partition_index INTEGER
        )
        """
    )
    conn.commit()
    yield conn
    conn.close()


def _insert_rows(conn: sqlite3.Connection, rows):
    conn.executemany(
        """
        INSERT INTO file_list
        (evidence_id, file_path, file_name, extension, size_bytes, inode, deleted, partition_index)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def test_discover_embedded_roots_positive_and_partitioned(evidence_db):
    evidence_id = 1
    rows = [
        # Embedded root (partition 2): at least two distinct signals.
        (evidence_id, "ProgramData/SomeApp/User Data/Default/Network/Cookies", "Cookies", "", 1, 101, 0, 2),
        (evidence_id, "ProgramData/SomeApp/User Data/Default/History", "History", "", 1, 102, 0, 2),
        (evidence_id, "ProgramData/SomeApp/User Data/Default/Local Storage/leveldb/000003.log", "000003.log", ".log", 1, 103, 0, 2),
        # Different embedded root on another partition.
        (evidence_id, "Users/Bob/AppData/Roaming/FooBrowser/User Data/Default/History", "History", "", 1, 201, 0, 3),
        (evidence_id, "Users/Bob/AppData/Roaming/FooBrowser/User Data/Default/Cookies", "Cookies", "", 1, 202, 0, 3),
    ]
    _insert_rows(evidence_db, rows)

    roots = discover_embedded_roots(evidence_db, evidence_id)

    assert len(roots) == 2
    assert any(root.root_path == "ProgramData/SomeApp/User Data" and root.partition_index == 2 for root in roots)
    assert any(root.root_path == "Users/Bob/AppData/Roaming/FooBrowser/User Data" and root.partition_index == 3 for root in roots)


def test_rejects_single_signal_and_known_browser_roots(evidence_db):
    evidence_id = 1
    rows = [
        # Single-signal app candidate (should be rejected).
        (evidence_id, "ProgramData/SingleSignal/User Data/Default/History", "History", "", 1, 301, 0, 2),
        # Known Chrome root (should be excluded).
        (evidence_id, "Users/Alice/AppData/Local/Google/Chrome/User Data/Default/History", "History", "", 1, 401, 0, 2),
        (evidence_id, "Users/Alice/AppData/Local/Google/Chrome/User Data/Default/Cookies", "Cookies", "", 1, 402, 0, 2),
    ]
    _insert_rows(evidence_db, rows)

    roots = discover_embedded_roots(evidence_db, evidence_id)
    assert roots == []


def test_discovery_scopes_embedded_patterns_per_partition(evidence_db):
    evidence_id = 1
    rows = [
        # Partition 2 qualifies as embedded root.
        (evidence_id, "ProgramData/ScopedApp/User Data/Default/History", "History", "", 1, 501, 0, 2),
        (evidence_id, "ProgramData/ScopedApp/User Data/Default/Cookies", "Cookies", "", 1, 502, 0, 2),
        # Same-looking path on partition 0 but only one signal.
        (evidence_id, "ProgramData/ScopedApp/User Data/Default/Cookies", "Cookies", "", 1, 601, 0, 0),
    ]
    _insert_rows(evidence_db, rows)

    result, roots = discover_artifacts_with_embedded_roots(
        evidence_db,
        evidence_id,
        artifact="cookies",
        filename_patterns=["Cookies"],
        path_patterns=["%Google%Chrome%"],  # intentionally excludes embedded path
    )

    assert roots
    assert get_embedded_root_paths(roots, partition_index=2) == ["ProgramData/ScopedApp/User Data"]
    assert result.matches_by_partition.get(2)
    assert result.matches_by_partition.get(0) is None
