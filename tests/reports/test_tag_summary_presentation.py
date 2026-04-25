"""Tests for the Tag Summary presentation/composer layer."""
from __future__ import annotations

import sqlite3

import pytest

from core.database import DatabaseManager
from reports.tag_summary_presentation import (
    ARTIFACT_PRESENTATION,
    REFERENCE_PRESENTATION,
    compose_tag_summary,
)


EVIDENCE_ID = 1


@pytest.fixture
def evidence_db(tmp_path):
    case_path = tmp_path / "case"
    case_path.mkdir()
    case_db_path = case_path / "CASE-1_surfsifter.sqlite"
    case_conn = sqlite3.connect(case_db_path)
    case_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS evidences (
            id INTEGER PRIMARY KEY,
            label TEXT NOT NULL,
            source_path TEXT NOT NULL,
            evidence_slug TEXT NOT NULL
        )
        """
    )
    case_conn.execute(
        "INSERT INTO evidences (id, label, source_path, evidence_slug) "
        "VALUES (1, 'EV-001', '/test.E01', 'ev-001')"
    )
    case_conn.commit()
    case_conn.close()
    db_manager = DatabaseManager(case_path, case_db_path=case_db_path)
    return db_manager.get_evidence_conn(EVIDENCE_ID, label="EV-001")


def test_presentation_covers_every_canonical_artifact_type():
    """Every spec key in the helper must have a presentation entry, so
    the writer never silently drops sections."""
    from core.database.helpers.tag_export import ARTIFACT_EXPORT_SPECS

    missing = set(ARTIFACT_EXPORT_SPECS) - set(ARTIFACT_PRESENTATION)
    assert not missing, f"Missing presentation entries: {sorted(missing)}"


def test_presentation_headers_match_helper_column_count():
    """Each presentation header tuple length must match the helper's
    column count for that artifact type."""
    from core.database.helpers.tag_export import ARTIFACT_EXPORT_SPECS

    for canonical, spec in ARTIFACT_EXPORT_SPECS.items():
        _label, headers = ARTIFACT_PRESENTATION[canonical]
        assert len(headers) == len(spec["columns"]), (
            f"{canonical}: header count {len(headers)} != "
            f"column count {len(spec['columns'])}"
        )


def test_compose_attaches_label_and_headers(evidence_db):
    evidence_db.execute(
        "INSERT INTO urls (id, evidence_id, url, domain, discovered_by, "
        "last_seen_utc, occurrence_count) VALUES "
        "(1, ?, 'https://a.example', 'a.example', 'test', "
        "'2024-01-01T00:00:00Z', 1)",
        (EVIDENCE_ID,),
    )
    cursor = evidence_db.execute(
        "INSERT INTO tags (evidence_id, name, name_normalized, created_by) "
        "VALUES (?, 'T1', 't1', 'test')",
        (EVIDENCE_ID,),
    )
    tag_id = cursor.lastrowid
    evidence_db.execute(
        "INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, "
        "artifact_id, tagged_by) VALUES (?, ?, 'url', 1, 'test')",
        (tag_id, EVIDENCE_ID),
    )
    # And a NULL-name hash match → composer must still produce a bucket.
    evidence_db.execute(
        "INSERT INTO images (id, evidence_id, rel_path, filename, md5, "
        "first_discovered_by) VALUES (1, ?, 'img/x.jpg', 'x.jpg', 'aaa', 'test')",
        (EVIDENCE_ID,),
    )
    evidence_db.execute(
        "INSERT INTO hash_matches (evidence_id, image_id, db_name, db_md5, "
        "list_name) VALUES (?, 1, 'projectvic', 'aaa', NULL)",
        (EVIDENCE_ID,),
    )
    evidence_db.commit()

    data = compose_tag_summary(
        evidence_db,
        EVIDENCE_ID,
        evidence_label="EV-001",
        exported_at_iso="2025-01-01T00:00:00+00:00",
        top_n=5,
    )

    assert len(data.tags) == 1
    section = data.tags[0]["sections"][0]
    assert section["label"] == "URLs"
    assert section["headers"] == ARTIFACT_PRESENTATION["url"][1]

    assert len(data.reference_list_matches) == 1
    bucket = data.reference_list_matches[0]
    assert bucket["headers"] == REFERENCE_PRESENTATION["image"]
    # NULL list_name surfaces as a non-empty string sentinel.
    assert isinstance(bucket["list_name"], str) and bucket["list_name"]
