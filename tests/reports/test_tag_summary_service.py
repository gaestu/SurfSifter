"""
Tests for the reports.tag_summary_service orchestration layer.

These cover the boundary the app shim sits on:
  - path containment (refuses outside-case-folder destinations)
  - audit-after-write ordering with rollback on audit failure
"""
from __future__ import annotations

import base64
import sqlite3
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

from core.database import DatabaseManager
from core.database.manager import slugify_label
from reports.tag_summary_service import (
    TagSummaryExportError,
    export_tag_summary,
)


EVIDENCE_ID = 1
EXPORTED_AT = "2025-01-01T00:00:00+00:00"
PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADUlEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"
)


@pytest.fixture
def case(tmp_path):
    case_path = tmp_path / "case"
    case_path.mkdir()
    case_db = case_path / "CASE-1_surfsifter.sqlite"
    case_conn = sqlite3.connect(case_db)
    case_conn.execute(
        "CREATE TABLE IF NOT EXISTS evidences ("
        "id INTEGER PRIMARY KEY, label TEXT NOT NULL, "
        "source_path TEXT NOT NULL, evidence_slug TEXT NOT NULL)"
    )
    case_conn.execute(
        "INSERT INTO evidences VALUES (1, 'EV', '/x.E01', 'ev')"
    )
    case_conn.commit()
    case_conn.close()
    db_manager = DatabaseManager(case_path, case_db_path=case_db)
    conn = db_manager.get_evidence_conn(1, label="EV")
    return case_path, conn


def _process_log_count(conn) -> int:
    row = conn.execute("SELECT COUNT(*) AS c FROM process_log").fetchone()
    return int(row["c"])


def test_refuses_path_outside_case_folder(case, tmp_path):
    case_folder, conn = case
    outside = tmp_path / "elsewhere" / "report.xlsx"
    with pytest.raises(TagSummaryExportError) as exc:
        export_tag_summary(
            conn=conn,
            evidence_id=EVIDENCE_ID,
            evidence_label="EV",
            case_folder=case_folder,
            output_path=outside,
            exported_at_iso=EXPORTED_AT,
        )
    assert exc.value.stage == "path"
    assert not outside.exists()
    assert _process_log_count(conn) == 0


def test_writes_workbook_then_audit_for_empty_evidence(case):
    case_folder, conn = case
    out = case_folder / "reports" / "tag_summary.xlsx"
    result = export_tag_summary(
        conn=conn,
        evidence_id=EVIDENCE_ID,
        evidence_label="EV",
        case_folder=case_folder,
        output_path=out,
        exported_at_iso=EXPORTED_AT,
    )
    assert result.output_path == out.resolve()
    assert out.exists()
    assert out.stat().st_size > 0
    # Exactly one audit row, recording the actual outcome (after the
    # workbook was on disk).
    assert _process_log_count(conn) == 1


def test_audit_finished_at_can_differ_from_visible_export_label(case):
    case_folder, conn = case
    out = case_folder / "reports" / "tag_summary.xlsx"
    export_tag_summary(
        conn=conn,
        evidence_id=EVIDENCE_ID,
        evidence_label="EV",
        case_folder=case_folder,
        output_path=out,
        exported_at_iso="Recorded in process_log",
        finished_at_iso=EXPORTED_AT,
    )

    row = conn.execute(
        "SELECT finished_at_utc FROM process_log WHERE task = ?",
        ("reports.tag_summary_export",),
    ).fetchone()
    assert row["finished_at_utc"] == EXPORTED_AT


def test_refuses_to_overwrite_existing_export(case):
    case_folder, conn = case
    out = case_folder / "reports" / "tag_summary.xlsx"
    out.parent.mkdir(parents=True)
    out.write_text("existing", encoding="utf-8")

    with pytest.raises(TagSummaryExportError) as exc:
        export_tag_summary(
            conn=conn,
            evidence_id=EVIDENCE_ID,
            evidence_label="EV",
            case_folder=case_folder,
            output_path=out,
            exported_at_iso=EXPORTED_AT,
        )

    assert exc.value.stage == "path"
    assert out.read_text(encoding="utf-8") == "existing"
    assert _process_log_count(conn) == 0


def test_audit_failure_rolls_back_workbook(case):
    """If the process_log insert fails after the workbook is written,
    the workbook must be removed so we never leave an exported artifact
    on disk without a provenance record.
    """
    case_folder, conn = case
    out = case_folder / "reports" / "tag_summary.xlsx"

    target = "reports.tag_summary_service.insert_process_log"
    with patch(target, side_effect=RuntimeError("boom")):
        with pytest.raises(TagSummaryExportError) as exc:
            export_tag_summary(
                conn=conn,
                evidence_id=EVIDENCE_ID,
                evidence_label="EV",
                case_folder=case_folder,
                output_path=out,
                exported_at_iso=EXPORTED_AT,
            )
    assert exc.value.stage == "audit"
    # Workbook removed.
    assert not out.exists()
    # No audit row written.
    assert _process_log_count(conn) == 0


def test_audit_failure_rolls_back_with_contained_unlink(case):
    case_folder, conn = case
    out = case_folder / "reports" / "tag_summary.xlsx"

    target = "reports.tag_summary_service.unlink_no_follow"
    with patch(target) as unlink_mock:
        with patch("reports.tag_summary_service.insert_process_log", side_effect=RuntimeError("boom")):
            with pytest.raises(TagSummaryExportError):
                export_tag_summary(
                    conn=conn,
                    evidence_id=EVIDENCE_ID,
                    evidence_label="EV",
                    case_folder=case_folder,
                    output_path=out,
                    exported_at_iso=EXPORTED_AT,
                )

    unlink_mock.assert_called_once_with(out.resolve(), containment_root=case_folder.resolve(), missing_ok=True)


def test_export_embeds_tagged_and_reference_image_thumbnails(case):
    case_folder, conn = case
    rel_path = "img/example.jpg"
    evidence_slug = slugify_label("EV", EVIDENCE_ID)
    image_path = (
        case_folder
        / "evidences"
        / evidence_slug
        / "filesystem_images"
        / "extracted"
        / rel_path
    )
    image_path.parent.mkdir(parents=True, exist_ok=True)
    image_path.write_bytes(PNG_1X1)

    conn.execute(
        "INSERT INTO images (id, evidence_id, rel_path, filename, md5, "
        "first_discovered_by) VALUES (1, ?, ?, 'example.jpg', 'abc', "
        "'filesystem_images')",
        (EVIDENCE_ID, rel_path),
    )
    tag_cursor = conn.execute(
        "INSERT INTO tags (evidence_id, name, name_normalized, created_by) "
        "VALUES (?, 'Review', 'review', 'test')",
        (EVIDENCE_ID,),
    )
    conn.execute(
        "INSERT INTO tag_associations (tag_id, evidence_id, artifact_type, "
        "artifact_id, tagged_by) VALUES (?, ?, 'image', 1, 'test')",
        (tag_cursor.lastrowid, EVIDENCE_ID),
    )
    conn.execute(
        "INSERT INTO hash_matches (evidence_id, image_id, db_name, db_md5, "
        "list_name) VALUES (?, 1, 'known', 'abc', 'known')",
        (EVIDENCE_ID,),
    )
    conn.commit()

    out = case_folder / "reports" / "tag_summary_thumbs.xlsx"
    export_tag_summary(
        conn=conn,
        evidence_id=EVIDENCE_ID,
        evidence_label="EV",
        case_folder=case_folder,
        output_path=out,
        exported_at_iso=EXPORTED_AT,
    )

    with zipfile.ZipFile(out) as zf:
        media = sorted(name for name in zf.namelist() if name.startswith("xl/media/"))
        assert media == ["xl/media/image1.jpg", "xl/media/image2.jpg"]
        assert zf.read(media[0]).startswith(b"\xff\xd8")
        assert zf.read(media[1]).startswith(b"\xff\xd8")
