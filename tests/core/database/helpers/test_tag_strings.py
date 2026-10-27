"""
Tests for the Tags-column helpers get_artifact_tags_str() and
get_tag_strings_for_artifacts(): only tags of the evidence count, the
definition of the "Untagged" filters (issues #72, #73).
"""
from __future__ import annotations

import sqlite3

import pytest

from core.database import DatabaseManager
from core.database.helpers.tags import get_artifact_tags_str, get_tag_strings_for_artifacts

EVIDENCE_ID = 1
OTHER_EVIDENCE_ID = 2


@pytest.fixture
def evidence_db(tmp_path):
    case_path = tmp_path / "test_case"
    case_path.mkdir()
    case_db_path = case_path / "CASE-2025-001_surfsifter.sqlite"
    case_conn = sqlite3.connect(case_db_path)
    case_conn.execute(
        "CREATE TABLE IF NOT EXISTS evidences (id INTEGER PRIMARY KEY, label TEXT NOT NULL, "
        "source_path TEXT NOT NULL, evidence_slug TEXT NOT NULL)"
    )
    case_conn.execute(
        "INSERT INTO evidences (id, label, source_path, evidence_slug) VALUES (1, 'EV-001', '/test.E01', 'ev-001')"
    )
    case_conn.commit()
    case_conn.close()

    db_manager = DatabaseManager(case_path, case_db_path=case_db_path)
    conn = db_manager.get_evidence_conn(1, label="EV-001")
    conn.row_factory = sqlite3.Row
    yield conn
    db_manager.close_all()


def _tag(conn, name: str, evidence_id: int = EVIDENCE_ID) -> int:
    return conn.execute(
        "INSERT INTO tags (evidence_id, name, name_normalized) VALUES (?, ?, ?)",
        (evidence_id, name, name.lower()),
    ).lastrowid


def _associate(conn, tag_id: int, artifact_id: int, evidence_id: int = EVIDENCE_ID) -> None:
    conn.execute(
        "INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, artifact_id) VALUES (?, ?, 'file_list', ?)",
        (tag_id, evidence_id, artifact_id),
    )


def test_tags_of_another_evidence_do_not_count(evidence_db):
    conn = evidence_db
    own = _tag(conn, "Relevant")
    foreign = _tag(conn, "Foreign", OTHER_EVIDENCE_ID)
    _associate(conn, own, 1)
    _associate(conn, foreign, 1)  # this evidence's association, other evidence's tag
    _associate(conn, foreign, 2)
    _associate(conn, own, 3, OTHER_EVIDENCE_ID)  # association of the other evidence
    conn.commit()

    assert get_artifact_tags_str(conn, EVIDENCE_ID, "file_list", 1) == "Relevant"
    assert get_artifact_tags_str(conn, EVIDENCE_ID, "file_list", 2) == ""
    assert get_artifact_tags_str(conn, EVIDENCE_ID, "file_list", 3) == ""
    assert get_tag_strings_for_artifacts(conn, EVIDENCE_ID, "file_list", [1, 2, 3]) == {1: "Relevant"}


def test_several_tags_are_joined(evidence_db):
    conn = evidence_db
    for name in ("A", "B"):
        _associate(conn, _tag(conn, name), 5)
    conn.commit()

    assert sorted(get_artifact_tags_str(conn, EVIDENCE_ID, "file_list", 5).split(", ")) == ["A", "B"]
    assert sorted(get_tag_strings_for_artifacts(conn, EVIDENCE_ID, "file_list", [5])[5].split(", ")) == ["A", "B"]
