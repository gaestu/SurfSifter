from __future__ import annotations

import sqlite3
from hashlib import sha256
from pathlib import Path

from tests.fixtures.helpers import prepare_case_with_data


def test_hash_check_worker_matches_text_hashlist(tmp_path: Path, monkeypatch) -> None:
    """HashCheckWorker preserves text-list matching and writes hash_matches rows."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))

    case_data, evidence_id = prepare_case_with_data(tmp_path)

    hashlists_dir = tmp_path / "home" / ".config" / "surfsifter" / "reference_lists" / "hashlists"
    hashlists_dir.mkdir(parents=True)
    hashlist_path = hashlists_dir / "known_images.txt"
    hashlist_content = "# test list\ndeadbeef\n"
    hashlist_path.write_text(hashlist_content, encoding="utf-8")

    from app.services.matching_workers import HashCheckWorker

    worker = HashCheckWorker(case_data.db_manager, evidence_id, ["known_images"])
    worker.run()

    evidence_db_path = case_data.db_manager.evidence_db_path(evidence_id, "EVID")
    with sqlite3.connect(evidence_db_path) as evidence_conn:
        row = evidence_conn.execute(
            """
            SELECT db_name, db_md5, matched_at_utc, list_name, list_version, hash_sha256, run_id
            FROM hash_matches
            WHERE evidence_id = ?
            """,
            (evidence_id,),
        ).fetchone()
        process_row = evidence_conn.execute(
            """
            SELECT id, task, command, exit_code, stdout, stderr
            FROM process_log
            WHERE evidence_id = ? AND task = 'hash_check'
            """,
            (evidence_id,),
        ).fetchone()

    expected_version = sha256(hashlist_content.encode("utf-8")).hexdigest()
    expected_run_id = f"hash_check:{process_row[0]}"
    assert row == (
        "known_images",
        "deadbeef",
        None,
        "known_images",
        expected_version,
        "feedface",
        expected_run_id,
    )
    assert process_row[1:] == (
        "hash_check",
        'lists=["known_images"] images=1',
        0,
        "matches=1",
        "",
    )


def test_hash_check_worker_logs_missing_text_hashlist(tmp_path: Path, monkeypatch) -> None:
    """Missing selected hash lists are logged as partial failures."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))

    case_data, evidence_id = prepare_case_with_data(tmp_path)

    from app.services.matching_workers import HashCheckWorker

    worker = HashCheckWorker(case_data.db_manager, evidence_id, ["missing_images"])
    worker.run()

    evidence_db_path = case_data.db_manager.evidence_db_path(evidence_id, "EVID")
    with sqlite3.connect(evidence_db_path) as evidence_conn:
        process_row = evidence_conn.execute(
            """
            SELECT task, exit_code, stdout, stderr
            FROM process_log
            WHERE evidence_id = ? AND task = 'hash_check'
            """,
            (evidence_id,),
        ).fetchone()

    assert process_row == (
        "hash_check",
        1,
        "matches=0",
        '{"error": "hash list not found", "list": "missing_images"}',
    )


def test_hash_check_worker_finalizes_process_log_when_cancelled(tmp_path: Path, monkeypatch) -> None:
    """Cooperative cancellation records a failed hash_check run instead of hanging."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))

    case_data, evidence_id = prepare_case_with_data(tmp_path)

    hashlists_dir = tmp_path / "home" / ".config" / "surfsifter" / "reference_lists" / "hashlists"
    hashlists_dir.mkdir(parents=True)
    (hashlists_dir / "known_images.txt").write_text("deadbeef\n", encoding="utf-8")

    from app.services import matching_workers
    from app.services.matching_workers import HashCheckWorker

    worker = HashCheckWorker(case_data.db_manager, evidence_id, ["known_images"])

    def cancel() -> None:
        raise matching_workers._HashCheckCancelled()

    monkeypatch.setattr(worker, "_raise_if_interrupted", cancel)
    worker.run()

    evidence_db_path = case_data.db_manager.evidence_db_path(evidence_id, "EVID")
    with sqlite3.connect(evidence_db_path) as evidence_conn:
        process_row = evidence_conn.execute(
            """
            SELECT task, exit_code, stdout, stderr
            FROM process_log
            WHERE evidence_id = ? AND task = 'hash_check'
            """,
            (evidence_id,),
        ).fetchone()
        match_count = evidence_conn.execute(
            "SELECT COUNT(*) FROM hash_matches WHERE evidence_id = ?",
            (evidence_id,),
        ).fetchone()[0]

    assert process_row == (
        "hash_check",
        1,
        "matches=0",
        '{"error": "cancelled"}',
    )
    assert match_count == 0
