import os
from pathlib import Path

import pytest

from core.database import init_db
from core.database import DatabaseManager


@pytest.fixture(scope="session")
def e01_path() -> Path:
    """Provide the E01 path for tests that need real evidence."""
    env_path = os.environ.get("E01_PATH")
    path = Path(env_path) if env_path else Path("images/hackcase/4Dell Latitude CPi.E01")
    if not path.exists():
        pytest.skip(f"E01 evidence not found at {path}")
    return path


@pytest.fixture()
def case_folder(tmp_path: Path) -> Path:
    """Create a minimal case folder with case/evidence databases."""
    case_dir = tmp_path / "case_workspace"
    case_dir.mkdir(parents=True, exist_ok=True)

    case_db_path = case_dir / "CASE_TEST_surfsifter.sqlite"
    case_conn = init_db(case_dir, db_path=case_db_path)

    with case_conn:
        case_conn.execute(
            "INSERT INTO cases (case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE_TEST", "Test Case", "2025-01-01T00:00:00Z"),
        )
        case_conn.execute(
            """
            INSERT INTO evidences (case_id, label, source_path, added_at_utc)
            VALUES (?, ?, ?, ?)
            """,
            (1, "Test Evidence", "/tmp/test.E01", "2025-01-01T00:00:00Z"),
        )

    manager = DatabaseManager(case_dir, case_db_path=case_db_path, enable_split=True)
    evidence_conn = manager.get_evidence_conn(1, label="Test Evidence")
    evidence_conn.close()
    manager.close_all()
    case_conn.close()

    return case_dir
