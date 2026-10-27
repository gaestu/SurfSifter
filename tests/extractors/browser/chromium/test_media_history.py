"""Tests for Chromium Media History extractor parsing."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from extractors.browser.chromium.media_history import MediaHistoryExtractor


def _create_media_history_db(
    db_path: Path,
    *,
    include_origin_table: bool,
) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        if include_origin_table:
            conn.execute(
                """
                CREATE TABLE origin (
                    id INTEGER PRIMARY KEY,
                    origin TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO origin(id, origin) VALUES (?, ?)",
                (7, "https://video.example"),
            )

        conn.execute(
            """
            CREATE TABLE playback (
                id INTEGER PRIMARY KEY,
                origin_id INTEGER,
                url TEXT NOT NULL,
                watch_time_s REAL,
                has_video INTEGER,
                has_audio INTEGER,
                last_updated_time_s INTEGER
            )
            """
        )
        conn.execute(
            """
            INSERT INTO playback(
                id, origin_id, url, watch_time_s, has_video, has_audio, last_updated_time_s
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, 7, "https://video.example/watch?v=1", 42.5, 1, 1, 0),
        )
        conn.commit()
    finally:
        conn.close()


def _file_entry() -> dict[str, Any]:
    logical_path = "/Users/alice/AppData/Local/Google/Chrome/User Data/Default/Media History"
    return {
        "browser": "chrome",
        "profile": "Default",
        "logical_path": logical_path,
        "partition_index": 3,
        "fs_type": "NTFS",
        "forensic_path": f"p3:{logical_path}",
    }


def test_parse_playback_table_reads_joined_origin(evidence_db, case_context, tmp_path: Path) -> None:
    db_path = tmp_path / "Media History"
    _create_media_history_db(db_path, include_origin_table=True)

    extractor = MediaHistoryExtractor()
    counts = extractor._parse_media_history(
        db_path,
        _file_entry(),
        "run-media",
        case_context.evidence_id,
        evidence_db,
        callbacks=None,
    )

    assert counts["playback"] == 1

    row = evidence_db.execute(
        """
        SELECT browser, profile, url, origin, watch_time_seconds, has_video, has_audio,
               run_id, source_path, partition_index, fs_type, logical_path, forensic_path
        FROM media_playback
        WHERE evidence_id = ?
        """,
        (case_context.evidence_id,),
    ).fetchone()

    assert dict(row) == {
        "browser": "chrome",
        "profile": "Default",
        "url": "https://video.example/watch?v=1",
        "origin": "https://video.example",
        "watch_time_seconds": 42.5,
        "has_video": 1,
        "has_audio": 1,
        "run_id": "run-media",
        "source_path": "/Users/alice/AppData/Local/Google/Chrome/User Data/Default/Media History",
        "partition_index": 3,
        "fs_type": "NTFS",
        "logical_path": "/Users/alice/AppData/Local/Google/Chrome/User Data/Default/Media History",
        "forensic_path": "p3:/Users/alice/AppData/Local/Google/Chrome/User Data/Default/Media History",
    }


def test_parse_playback_table_without_origin_table_defaults_origin(
    evidence_db,
    case_context,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "Media History"
    _create_media_history_db(db_path, include_origin_table=False)

    extractor = MediaHistoryExtractor()
    counts = extractor._parse_media_history(
        db_path,
        _file_entry(),
        "run-media-no-origin",
        case_context.evidence_id,
        evidence_db,
        callbacks=None,
    )

    assert counts["playback"] == 1
    row = evidence_db.execute(
        "SELECT origin FROM media_playback WHERE evidence_id = ?",
        (case_context.evidence_id,),
    ).fetchone()
    assert row["origin"] == ""
