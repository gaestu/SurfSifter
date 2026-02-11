import os
import sqlite3
from pathlib import Path

from app.features.urls.models import UrlsTableModel
from tests.fixtures.helpers import prepare_case_with_data
from core.database import insert_urls

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

def test_url_exports_are_deterministic(tmp_path: Path) -> None:
    case_data, evidence_id = prepare_case_with_data(tmp_path)

    # URLs are in evidence DB
    evidence_conn = case_data.db_manager.get_evidence_conn(evidence_id, "EVID")
    with evidence_conn:
        evidence_conn.execute(
            "UPDATE urls SET first_seen_utc = ? WHERE evidence_id = ?",
            ("2024-01-02T10:00:00", evidence_id),
        )
        insert_urls(
            evidence_conn,
            evidence_id,
            [
                {
                    "url": "https://b.example.com",
                    "domain": "b.example.com",
                    "scheme": "https",
                    "discovered_by": "regex",
                    "first_seen_utc": "2024-01-01T08:00:00",
                    "last_seen_utc": "2024-01-01T09:00:00",
                    "source_path": "History",
                },
                {
                    "url": "https://a.example.com",
                    "domain": "a.example.com",
                    "scheme": "https",
                    "discovered_by": "regex",
                    "first_seen_utc": "2024-01-03T08:00:00",
                    "last_seen_utc": "2024-01-03T09:00:00",
                    "source_path": "History",
                },
            ],
        )
    evidence_conn.close()

    model = UrlsTableModel(case_data)
    model.page_size = 1
    model.set_evidence(evidence_id)

    out1 = tmp_path / "export_one.csv"
    out2 = tmp_path / "export_two.csv"
    model.export_to_csv(out1)
    model.export_to_csv(out2)

    data_one = out1.read_bytes()
    data_two = out2.read_bytes()

    assert data_one == data_two
    assert b"\r\n" in data_one

    lines = [line for line in data_one.decode("utf-8").split("\r\n") if line]
    assert "https://b.example.com" in lines[1]
    assert "https://a.example.com" in lines[-1]
