"""Deterministic synthetic File List fixtures for File List tests and benchmarks.

Builds a case workspace with one evidence whose ``file_list`` table is
populated with reproducible rows (no randomness, no wall-clock values).
Shared by fast tests (small row counts) and the opt-in File List query
benchmark (1M rows, issue #73).

Row ``i`` lies in one of five folders (``FOLDERS``, ``i % 5``), so 20% of the
paths contain ``WinSxS`` and 40% contain ``Windows``; every path contains
``:\\``. Its name is ``f{i:07d}.{ext}`` with ``EXTENSIONS[i % 6]`` (no
extension for ``""``); every 9th row is deleted.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from core.database import DatabaseManager

FOLDERS = (
    "C:\\Windows\\WinSxS\\amd64_pkg{n}",
    "C:\\Windows\\System32",
    "C:\\Users\\user{n}\\AppData\\Local\\Temp",
    "C:\\Program Files\\App{n}",
    "D:\\Data\\Projects\\p{n}",
)
EXTENSIONS = ("dll", "exe", "txt", "jpg", "docx", "")
TAG_NAMES = ("Relevant", "Review")
MATCH_LISTS = ("list0", "list1", "list2")
FIXED_TS = "2025-01-01T00:00:00Z"


@dataclass
class FileListFixtureCase:
    case_folder: Path
    case_db_path: Path
    evidence_id: int
    label: str


def build_file_list_fixture_case(
    case_folder: Path,
    *,
    rows: int,
    tag_every: int = 0,
    untag_every: int = 0,
    match_every: int = 0,
    batch_size: int = 50_000,
    analyze: bool = True,
) -> FileListFixtureCase:
    """Create a case with one evidence of synthetic file list rows.

    Args:
        case_folder: Case workspace folder (created if missing).
        rows: File list rows to insert.
        tag_every: Every Nth row is tagged (``TAG_NAMES`` alternating).
            0 disables tagging.
        untag_every: If set, every row but each Nth one is tagged instead
            ("mostly tagged"); overrides ``tag_every``.
        match_every: Every Nth row gets a ``file_list_matches`` row.
            0 disables matches.
        batch_size: Rows per ``executemany`` batch.
        analyze: Run ANALYZE at the end.
    """
    case_folder.mkdir(parents=True, exist_ok=True)
    case_db_path = case_folder / "filelist_surfsifter.sqlite"
    manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    label = "File List Evidence"
    try:
        case_conn = manager.get_case_conn()
        with case_conn:
            case_id = case_conn.execute(
                "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
                ("FL001", "File List Fixture", FIXED_TS),
            ).lastrowid
            evidence_id = case_conn.execute(
                "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
                (case_id, label, "/fixture/evidence1.E01", FIXED_TS),
            ).lastrowid
        conn = manager.get_evidence_conn(evidence_id, label)
        populate_file_list(
            conn,
            evidence_id,
            rows=rows,
            tag_every=tag_every,
            untag_every=untag_every,
            match_every=match_every,
            batch_size=batch_size,
        )
        if analyze:
            conn.execute("ANALYZE")
    finally:
        manager.close_all()
    return FileListFixtureCase(case_folder, case_db_path, evidence_id, label)


def populate_file_list(
    conn,
    evidence_id: int,
    *,
    rows: int,
    tag_every: int = 0,
    untag_every: int = 0,
    match_every: int = 0,
    batch_size: int = 50_000,
) -> None:
    conn.execute("PRAGMA synchronous = OFF")
    with conn:
        tag_ids = []
        if tag_every or untag_every:
            for name in TAG_NAMES:
                tag_ids.append(
                    conn.execute(
                        "INSERT INTO tags(evidence_id, name, name_normalized, created_at_utc) VALUES (?, ?, ?, ?)",
                        (evidence_id, name, name.lower(), FIXED_TS),
                    ).lastrowid
                )

        start = 0
        while start < rows:
            stop = min(start + batch_size, rows)
            batch = []
            for i in range(start, stop):
                folder = FOLDERS[i % len(FOLDERS)].format(n=i % 50)
                ext = EXTENSIONS[i % len(EXTENSIONS)]
                name = f"f{i:07d}.{ext}" if ext else f"f{i:07d}"
                batch.append(
                    (
                        evidence_id,
                        f"{folder}\\{name}",
                        name,
                        ext or None,
                        (i * 7919) % 200_000_000,
                        f"2024-{1 + i % 12:02d}-{1 + i % 28:02d}T{i % 24:02d}:{i % 60:02d}:00Z",
                        1 if i % 9 == 0 else 0,
                        FIXED_TS,
                        1 + i % 2,
                    )
                )
            conn.executemany(
                "INSERT INTO file_list(evidence_id, file_path, file_name, extension, size_bytes, "
                "modified_ts, deleted, import_timestamp, partition_index) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                batch,
            )
            start = stop

        # ids are assigned sequentially by AUTOINCREMENT; resolve the range.
        min_id, max_id = conn.execute(
            "SELECT MIN(id), MAX(id) FROM file_list WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()
        if min_id is None:
            return

        def tagged(offset: int) -> bool:
            if untag_every:
                return offset % untag_every != 0
            return bool(tag_every) and offset % tag_every == 0

        if tag_ids:
            conn.executemany(
                "INSERT INTO tag_associations(tag_id, evidence_id, artifact_type, artifact_id, tagged_at_utc) "
                "VALUES (?, ?, 'file_list', ?, ?)",
                (
                    (tag_ids[file_id % len(tag_ids)], evidence_id, file_id, FIXED_TS)
                    for file_id in range(min_id, max_id + 1)
                    if tagged(file_id - min_id)
                ),
            )
        if match_every:
            conn.executemany(
                "INSERT INTO file_list_matches(evidence_id, file_list_id, reference_list_name, match_type, "
                "matched_value, matched_at) VALUES (?, ?, ?, 'path', ?, ?)",
                (
                    (evidence_id, file_id, MATCH_LISTS[(file_id // match_every) % len(MATCH_LISTS)], "x", FIXED_TS)
                    for file_id in range(min_id, max_id + 1)
                    if (file_id - min_id) % match_every == 0
                ),
            )
    conn.execute("PRAGMA synchronous = NORMAL")
