from __future__ import annotations

from pathlib import Path

from PIL import Image

from app.data.case_data import CaseDataAccess
from core.database import (
    insert_browser_history,
    insert_images,
    insert_os_indicators,
    insert_urls,
)
from core.database import DatabaseManager


def prepare_case_with_data(tmp_path: Path) -> tuple[CaseDataAccess, int]:
    # Use test_surfsifter.sqlite naming convention for test cases
    case_db_path = tmp_path / "test_surfsifter.sqlite"
    manager = DatabaseManager(tmp_path, case_db_path=case_db_path)
    case_conn = manager.get_case_conn()
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE-1", "Case 1", "2024-01-01T00:00:00"),
        )
        cur = case_conn.execute(
            "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "EVID", "path", "2024-01-01T00:00:00"),
        )
    evidence_id = int(cur.lastrowid)
    evidence_conn = manager.get_evidence_conn(evidence_id, "EVID")
    insert_urls(
        evidence_conn,
        evidence_id,
        [
            {
                "url": "https://example.com",
                "domain": "example.com",
                "discovered_by": "regex",
                "scheme": "https",
                "source_path": "History",
            }
        ],
    )
    image_rel = "images/file.jpg"
    image_abs = tmp_path / image_rel
    image_abs.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (64, 64), color=(200, 10, 10)).save(image_abs)
    insert_images(
        evidence_conn,
        evidence_id,
        [
            {
                "rel_path": image_rel,
                "filename": "file.jpg",
                "discovered_by": "carver",
                "md5": "deadbeef",
                "sha256": "feedface",
            }
        ],
    )
    insert_browser_history(
        evidence_conn,
        evidence_id,
        [
            {
                "url": "https://example.com",
                "browser": "test",
            }
        ],
    )
    insert_os_indicators(
        evidence_conn,
        evidence_id,
        [
            {
                "type": "registry",
                "name": "ComputerName",
                "value": "TEST-PC",
                "path": "ControlSet001/Control/ComputerName",
                "hive": "SYSTEM",
                "confidence": "high",
                "provenance": "registry",
            }
        ],
    )
    evidence_conn.close()
    case_conn.close()
    return CaseDataAccess(tmp_path, db_manager=manager), evidence_id
