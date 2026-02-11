import sqlite3
from pathlib import Path

from app.services.workers import HashLookupTask
from tests.fixtures.helpers import prepare_case_with_data


def test_hash_lookup_persists_matches(tmp_path: Path) -> None:
    case_data, evidence_id = prepare_case_with_data(tmp_path)
    case_db = tmp_path / "test_surfsifter.sqlite"

    # Get image_id from evidence DB
    evidence_db_path = case_data.db_manager.evidence_db_path(evidence_id, "EVID")
    with sqlite3.connect(evidence_db_path) as evidence_conn:
        image_id = evidence_conn.execute("SELECT id FROM images LIMIT 1").fetchone()[0]

    hash_db = tmp_path / "hash.sqlite"
    with sqlite3.connect(hash_db) as hash_conn:
        hash_conn.execute("CREATE TABLE images(md5 TEXT PRIMARY KEY, note TEXT)")
        hash_conn.execute("INSERT INTO images(md5, note) VALUES (?, ?)", ("deadbeef", "Known bad actor"))

    task = HashLookupTask(case_db, hash_db, evidence_id, [image_id], db_manager=case_data.db_manager)
    matches = task.run_task()

    assert matches == [(image_id, "deadbeef", "Known bad actor")]

    # Verify hash_matches are in evidence DB
    with sqlite3.connect(evidence_db_path) as evidence_conn:
        rows = evidence_conn.execute(
            "SELECT image_id, db_md5 FROM hash_matches WHERE evidence_id = ?",
            (evidence_id,)
        ).fetchall()
    assert rows and rows[0][0] == image_id and rows[0][1] == "deadbeef"

    persisted = case_data.list_hash_matches(evidence_id, [image_id])
    assert persisted and persisted[0]["db_md5"] == "deadbeef"
