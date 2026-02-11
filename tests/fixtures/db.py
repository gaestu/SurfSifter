"""Database and case fixtures for tests."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import count
from pathlib import Path
from typing import Callable

import pytest

from core.database import DatabaseManager


@dataclass
class CaseContext:
    manager: DatabaseManager
    case_dir: Path
    case_conn: object
    evidence_id: int
    evidence_label: str
    case_db_path: Path


@pytest.fixture
def case_factory(tmp_path: Path) -> Callable[..., CaseContext]:
    """Create a case folder + DB with a single evidence row."""
    counter = count(1)

    def _create(
        case_id: str = "CASE-1",
        title: str = "Test Case",
        investigator: str = "Tester",
        created_at: str | None = None,
        evidence_label: str = "EVID",
        source_path: str = "/test/path",
        added_at: str | None = None,
    ) -> CaseContext:
        idx = next(counter)
        case_dir = tmp_path / f"case_{idx}"
        case_dir.mkdir(parents=True, exist_ok=True)

        case_db_path = case_dir / "test_surfsifter.sqlite"
        manager = DatabaseManager(case_dir, case_db_path=case_db_path)
        case_conn = manager.get_case_conn()

        created_at = created_at or datetime.now(timezone.utc).isoformat()
        added_at = added_at or created_at

        with case_conn:
            case_conn.execute(
                "INSERT INTO cases(case_id, title, investigator, created_at_utc) VALUES (?, ?, ?, ?)",
                (case_id, title, investigator, created_at),
            )
            cur = case_conn.execute(
                "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                (1, evidence_label, source_path, added_at),
            )

        evidence_id = int(cur.lastrowid)
        return CaseContext(
            manager=manager,
            case_dir=case_dir,
            case_conn=case_conn,
            evidence_id=evidence_id,
            evidence_label=evidence_label,
            case_db_path=case_db_path,
        )

    return _create


@pytest.fixture
def case_context(case_factory: Callable[..., CaseContext]) -> CaseContext:
    """Default case context with one evidence row."""
    ctx = case_factory()
    try:
        yield ctx
    finally:
        ctx.case_conn.close()


@pytest.fixture
def db_manager(case_context: CaseContext) -> DatabaseManager:
    """Database manager for a default case context."""
    return case_context.manager


@pytest.fixture
def evidence_db(case_context: CaseContext):
    """Evidence DB connection for a default case context."""
    conn = case_context.manager.get_evidence_conn(
        case_context.evidence_id,
        case_context.evidence_label,
    )
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def case_dir(case_context: CaseContext) -> Path:
    """Case directory for a default case context."""
    return case_context.case_dir
