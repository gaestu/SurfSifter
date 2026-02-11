from pathlib import Path

import pytest

from app.config.settings import NetworkSettings
from app.services.net_download import DownloadRequest, DownloadResult
from app.services.workers import DownloadTask, DownloadTaskConfig


def test_sanitize_filename_roundtrip():
    from app.services.net_download import sanitize_filename

    assert sanitize_filename("abcDEF-_.jpg") == "abcDEF-_.jpg"
    assert sanitize_filename("bad/name?.png") == "bad_name_.png"
    assert sanitize_filename("") == "download"


def test_download_requests_build(tmp_path: Path):
    base = tmp_path / "downloads"
    req = DownloadRequest(item_id=1, url="http://example.com/a", dest_path=base / "domain" / "file", domain="domain")
    assert req.dest_path.parent == base / "domain"


@pytest.mark.parametrize(
    "allowed, content, expected",
    [
        (["image/"], "image/jpeg", True),
        (["image/"], "application/json", False),
        (["video/"], "video/mp4", True),
    ],
)
def test_content_type_matches(allowed, content, expected):
    assert any(content.startswith(prefix) for prefix in allowed) is expected


def test_download_task_writes_download_audit_rows(case_context, monkeypatch):
    """DownloadTask should persist one final download_audit row per URL."""

    def fake_download_items(requests_list, **kwargs):  # noqa: ANN001
        return [
            DownloadResult(
                item_id=1,
                url=requests_list[0].url,
                dest_path=requests_list[0].dest_path,
                ok=True,
                status_code=200,
                bytes_written=1200,
                sha256="a" * 64,
                md5="b" * 32,
                error=None,
                duration_s=0.21,
                attempts=1,
                content_type="image/jpeg",
            ),
            DownloadResult(
                item_id=2,
                url=requests_list[1].url,
                dest_path=None,
                ok=False,
                status_code=200,
                bytes_written=0,
                sha256=None,
                md5=None,
                error="Blocked content-type application/octet-stream",
                duration_s=0.01,
                attempts=1,
                content_type="application/octet-stream",
            ),
            DownloadResult(
                item_id=3,
                url=requests_list[2].url,
                dest_path=None,
                ok=False,
                status_code=404,
                bytes_written=0,
                sha256=None,
                md5=None,
                error="HTTP 404",
                duration_s=0.03,
                attempts=1,
                content_type="text/html",
            ),
            DownloadResult(
                item_id=4,
                url=requests_list[3].url,
                dest_path=None,
                ok=False,
                status_code=None,
                bytes_written=0,
                sha256=None,
                md5=None,
                error="cancelled",
                duration_s=0.0,
                attempts=0,
                content_type=None,
            ),
            DownloadResult(
                item_id=5,
                url=requests_list[4].url,
                dest_path=None,
                ok=False,
                status_code=None,
                bytes_written=0,
                sha256=None,
                md5=None,
                error="Connection reset by peer",
                duration_s=0.15,
                attempts=2,
                content_type=None,
            ),
        ]

    monkeypatch.setattr("app.services.workers.download_items", fake_download_items)

    items = [
        {"url": "https://a.test/1", "domain": "a.test", "filename": "1.jpg"},
        {"url": "https://a.test/2", "domain": "a.test", "filename": "2.bin"},
        {"url": "https://a.test/3", "domain": "a.test", "filename": "3.txt"},
        {"url": "https://a.test/4", "domain": "a.test", "filename": "4.txt"},
        {"url": "https://a.test/5", "domain": "a.test", "filename": "5.txt"},
    ]

    task = DownloadTask(
        DownloadTaskConfig(
            case_root=case_context.case_dir,
            case_db_path=case_context.case_db_path,
            evidence_id=case_context.evidence_id,
            items=items,
            network=NetworkSettings(concurrency=1, retries=0, timeout_s=5, max_bytes=1024 * 1024),
            db_manager=case_context.manager,
            evidence_label=case_context.evidence_label,
            caller_info="download_tab",
        )
    )
    task.run_task()

    conn = case_context.manager.get_evidence_conn(
        case_context.evidence_id,
        case_context.evidence_label,
    )
    try:
        rows = conn.execute(
            """
            SELECT url, outcome, blocked, status_code
            FROM download_audit
            WHERE evidence_id = ?
            ORDER BY id ASC
            """,
            (case_context.evidence_id,),
        ).fetchall()
        assert len(rows) == 5
        assert rows[0]["outcome"] == "success"
        assert rows[1]["outcome"] == "blocked"
        assert rows[1]["blocked"] == 1
        assert rows[2]["outcome"] == "failed"
        assert rows[2]["status_code"] == 404
        assert rows[3]["outcome"] == "cancelled"
        assert rows[4]["outcome"] == "error"
    finally:
        conn.close()
