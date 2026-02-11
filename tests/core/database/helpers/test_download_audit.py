"""Tests for download audit helper functions."""

from core.database.helpers.download_audit import (
    get_download_audit,
    get_download_audit_count,
    get_download_audit_summary,
    insert_download_audit,
)


def test_insert_and_list_download_audit(case_context):
    """Insert final rows and verify listing/count/summary."""
    conn = case_context.manager.get_evidence_conn(
        case_context.evidence_id,
        case_context.evidence_label,
    )
    try:
        insert_download_audit(
            conn,
            case_context.evidence_id,
            "https://example.com/file1.jpg",
            "GET",
            "success",
            status_code=200,
            attempts=1,
            duration_s=0.2,
            bytes_written=1024,
            content_type="image/jpeg",
            caller_info="download_tab",
            ts_utc="2026-02-06T10:00:00+00:00",
        )
        insert_download_audit(
            conn,
            case_context.evidence_id,
            "https://example.com/file2.bin",
            "GET",
            "blocked",
            blocked=True,
            reason="Blocked content-type application/octet-stream",
            status_code=200,
            attempts=1,
            duration_s=0.01,
            bytes_written=0,
            content_type="application/octet-stream",
            caller_info="download_tab",
            ts_utc="2026-02-06T10:01:00+00:00",
        )
        conn.commit()

        rows = get_download_audit(conn, case_context.evidence_id)
        assert len(rows) == 2
        assert rows[0]["outcome"] == "blocked"
        assert rows[1]["outcome"] == "success"

        total = get_download_audit_count(conn, case_context.evidence_id)
        assert total == 2

        summary = get_download_audit_summary(conn, case_context.evidence_id)
        assert summary["total"] == 2
        assert summary["by_outcome"]["success"] == 1
        assert summary["by_outcome"]["blocked"] == 1
    finally:
        conn.close()


def test_download_audit_filters(case_context):
    """Verify outcome and text filtering."""
    conn = case_context.manager.get_evidence_conn(
        case_context.evidence_id,
        case_context.evidence_label,
    )
    try:
        insert_download_audit(
            conn,
            case_context.evidence_id,
            "https://a.test/one",
            "GET",
            "failed",
            reason="HTTP 404",
            status_code=404,
            caller_info="download_tab",
        )
        insert_download_audit(
            conn,
            case_context.evidence_id,
            "https://b.test/two",
            "GET",
            "error",
            reason="Connection reset by peer",
            caller_info="download_tab",
        )
        conn.commit()

        failed_rows = get_download_audit(
            conn,
            case_context.evidence_id,
            outcome="failed",
        )
        assert len(failed_rows) == 1
        assert failed_rows[0]["status_code"] == 404

        search_rows = get_download_audit(
            conn,
            case_context.evidence_id,
            search_text="reset",
        )
        assert len(search_rows) == 1
        assert search_rows[0]["outcome"] == "error"
    finally:
        conn.close()
