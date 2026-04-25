"""
Tests for the reports.tag_summary_service orchestration layer.

These cover the boundary the app shim sits on:
  - path containment (refuses outside-case-folder destinations)
  - audit-after-write ordering with rollback on audit failure
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from core.database import DatabaseManager
from reports.tag_summary_service import (
    TagSummaryExportError,
    export_tag_summary,
)


EVIDENCE_ID = 1
EXPORTED_AT = "2025-01-01T00:00:00+00:00"


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
