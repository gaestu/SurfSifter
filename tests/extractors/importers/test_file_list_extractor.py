"""
Tests for file list extractor worker.
"""
import sqlite3
from pathlib import Path

import pytest

from core.database import DatabaseManager
from extractors.system.file_list.worker import FileListExtractor


@pytest.fixture
def evidence_db(tmp_path):
    """Create temporary evidence database with migrations applied."""
    case_folder = tmp_path / "test_case"
    case_folder.mkdir()
    case_db_path = case_folder / "TEST-001_surfsifter.sqlite"

    # Create case DB and evidence
    manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    case_conn = manager.get_case_conn()

    case_conn.execute(
        "INSERT INTO cases (case_id, title, investigator, created_at_utc) VALUES ('TEST-001', 'Test', 'Tester', '2025-11-05T10:00:00Z')"
    )
    case_conn.execute(
        "INSERT INTO evidences (case_id, label, source_path, added_at_utc) VALUES (1, 'EV-001', '/test.e01', '2025-11-05T10:00:00Z')"
    )
    case_conn.commit()
    evidence_id = case_conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # Get evidence connection (triggers migrations)
    evidence_conn = manager.get_evidence_conn(evidence_id, label="EV-001")

    return evidence_conn, evidence_id


def test_extractor_real_ftk_file(evidence_db, tmp_path):
    """Test extracting real FTK file (14k entries)."""
    # Use the real FTK file from tests/files directory
    ftk_file = Path(__file__).resolve().parents[2] / "files" / "direcotry_listing.csv"
    if not ftk_file.exists():
        pytest.skip("Real FTK file not available")

    conn, evidence_id = evidence_db

    # Run extractor
    extractor = FileListExtractor(
        evidence_conn=conn,
        evidence_id=evidence_id,
        csv_path=ftk_file,
        import_source="auto"
    )

    progress_calls = []

    def progress_callback(current, total):
        progress_calls.append((current, total))

    stats = extractor.run(progress_callback=progress_callback)

    # Verify statistics (file has 14456 lines, 1 header = 14455 data rows)
    # Real data may have duplicates or malformed rows, so check for >98% success
    assert stats["total_rows"] == 14455
    assert stats["inserted_rows"] >= 14200  # Allow ~200 duplicates/errors
    assert stats["inserted_rows"] <= 14455
    assert stats["import_source"] == "ftk"
    assert "duration_seconds" in stats
    assert "import_timestamp" in stats

    # Verify progress callbacks
    assert len(progress_calls) > 0
    assert progress_calls[-1] == (14455, 14455)  # Final callback

    # Verify database contents
    cursor = conn.execute(
        "SELECT COUNT(*) FROM file_list WHERE evidence_id = ?", (evidence_id,)
    )
    count = cursor.fetchone()[0]
    assert count >= 14200 and count <= 14455

    # Verify specific entry (boot.ini from Phase 2 tests)
    cursor = conn.execute(
        """
        SELECT file_path, file_name, extension, size_bytes, deleted
        FROM file_list
        WHERE file_name = 'boot.ini'
    """
    )
    row = cursor.fetchone()
    assert row is not None
    assert row[1] == "boot.ini"
    assert row[2] == ".ini"
    assert row[3] == 194
    assert row[4] == 0  # Not deleted


def test_extractor_batched_writes(evidence_db, tmp_path):
    """Test that batched writes work correctly."""
    # Create CSV with 2500 rows (3 batches: 1000, 1000, 500)
    csv_path = tmp_path / "large_file.csv"
    with open(csv_path, "w") as f:
        f.write("Filename\tFull Path\tSize (bytes)\tCreated\n")
        for i in range(2500):
            f.write(f"file_{i}.txt\tC:\\test\\file_{i}.txt\t{i * 100}\t2024-01-01 10:00:00\n")

    conn, evidence_id = evidence_db

    extractor = FileListExtractor(
        evidence_conn=conn,
        evidence_id=evidence_id,
        csv_path=csv_path,
        import_source="ftk"
    )

    stats = extractor.run()

    # Verify all rows inserted
    assert stats["total_rows"] == 2500
    assert stats["inserted_rows"] == 2500

    cursor = conn.execute("SELECT COUNT(*) FROM file_list")
    assert cursor.fetchone()[0] == 2500


def test_extractor_duplicate_handling(evidence_db, tmp_path):
    """Test that duplicate file paths are skipped."""
    csv_path = tmp_path / "with_duplicates.csv"
    with open(csv_path, "w") as f:
        f.write("Filename\tFull Path\tSize (bytes)\tCreated\n")
        f.write("file1.txt\tC:\\test\\file1.txt\t100\t2024-01-01 10:00:00\n")
        f.write("file2.txt\tC:\\test\\file2.txt\t200\t2024-01-01 10:00:00\n")
        f.write("file1.txt\tC:\\test\\file1.txt\t999\t2024-01-01 10:00:00\n")  # Duplicate path

    conn, evidence_id = evidence_db

    extractor = FileListExtractor(
        evidence_conn=conn,
        evidence_id=evidence_id,
        csv_path=csv_path,
        import_source="ftk"
    )

    stats = extractor.run()

    # Verify only unique paths inserted
    assert stats["total_rows"] == 3
    assert stats["inserted_rows"] == 2
    assert stats["skipped_rows"] == 1

    # Verify first file kept (size=100, not 999)
    cursor = conn.execute(
        "SELECT size_bytes FROM file_list WHERE file_name = 'file1.txt'"
    )
    assert cursor.fetchone()[0] == 100


def test_extractor_statistics(evidence_db, tmp_path):
    """Test get_import_statistics method."""
    csv_path = tmp_path / "stats_test.csv"
    with open(csv_path, "w") as f:
        f.write("Filename\tFull Path\tSize (bytes)\tIs Deleted\n")
        f.write("file1.txt\tC:\\test\\file1.txt\t100\tNo\n")
        f.write("file2.exe\tC:\\test\\file2.exe\t2000\tNo\n")
        f.write("file3.txt\tC:\\test\\file3.txt\t300\tYes\n")
        f.write("file4.dll\tC:\\test\\file4.dll\t500\tNo\n")

    conn, evidence_id = evidence_db

    extractor = FileListExtractor(
        evidence_conn=conn,
        evidence_id=evidence_id,
        csv_path=csv_path,
        import_source="ftk"
    )

    extractor.run()

    # Get statistics
    stats = extractor.get_import_statistics()

    assert stats["total_files"] == 4
    assert stats["unique_extensions"] == 3  # .txt, .exe, .dll
    assert stats["total_size_bytes"] == 2900  # 100+2000+300+500
    assert stats["avg_size_bytes"] == 725.0
    assert stats["max_size_bytes"] == 2000
    assert stats["deleted_count"] == 1


def test_extractor_top_extensions(evidence_db, tmp_path):
    """Test get_top_extensions method."""
    csv_path = tmp_path / "extensions_test.csv"
    with open(csv_path, "w") as f:
        f.write("Filename\tFull Path\tSize (bytes)\tCreated\n")
        # 5 .txt, 3 .exe, 2 .dll
        for i in range(5):
            f.write(f"file{i}.txt\tC:\\test\\file{i}.txt\t100\t2024-01-01 10:00:00\n")
        for i in range(3):
            f.write(f"prog{i}.exe\tC:\\test\\prog{i}.exe\t1000\t2024-01-01 10:00:00\n")
        for i in range(2):
            f.write(f"lib{i}.dll\tC:\\test\\lib{i}.dll\t500\t2024-01-01 10:00:00\n")

    conn, evidence_id = evidence_db

    extractor = FileListExtractor(
        evidence_conn=conn,
        evidence_id=evidence_id,
        csv_path=csv_path,
        import_source="ftk"
    )

    extractor.run()

    # Get top extensions
    top_exts = extractor.get_top_extensions(limit=10)

    assert len(top_exts) == 3
    assert top_exts[0] == {"extension": ".txt", "count": 5}
    assert top_exts[1] == {"extension": ".exe", "count": 3}
    assert top_exts[2] == {"extension": ".dll", "count": 2}


def test_extractor_file_not_found():
    """Test error handling for missing file."""
    with pytest.raises(FileNotFoundError):
        FileListExtractor(
            evidence_conn=None,
            evidence_id=1,
            csv_path=Path("/nonexistent/file.csv"),
            import_source="ftk"
        )


def test_extractor_progress_reporting(evidence_db, tmp_path):
    """Test progress callback invocation."""
    csv_path = tmp_path / "progress_test.csv"
    with open(csv_path, "w") as f:
        f.write("Filename\tFull Path\tSize (bytes)\tCreated\n")
        for i in range(2500):  # 3 batches
            f.write(f"file_{i}.txt\tC:\\test\\file_{i}.txt\t{i}\t2024-01-01 10:00:00\n")

    conn, evidence_id = evidence_db

    extractor = FileListExtractor(
        evidence_conn=conn,
        evidence_id=evidence_id,
        csv_path=csv_path,
        import_source="ftk"
    )

    progress_calls = []

    def track_progress(current, total):
        progress_calls.append((current, total))

    extractor.run(progress_callback=track_progress)

    # Verify we got callbacks for each batch
    assert len(progress_calls) == 3  # 3 batches (1000, 1000, 500)
    assert progress_calls[0] == (1000, 2500)
    assert progress_calls[1] == (2000, 2500)
    assert progress_calls[2] == (2500, 2500)


def test_extractor_auto_detection(evidence_db, tmp_path):
    """Test automatic parser detection."""
    # FTK format (tab-delimited with 4+ matching columns)
    ftk_path = tmp_path / "ftk_format.csv"
    with open(ftk_path, "w") as f:
        f.write("Filename\tFull Path\tSize (bytes)\tCreated\n")
        f.write("file1.txt\tC:\\test\\file1.txt\t100\t2024-01-01 10:00:00\n")

    conn, evidence_id = evidence_db

    # Auto-detect FTK
    extractor = FileListExtractor(
        evidence_conn=conn,
        evidence_id=evidence_id,
        csv_path=ftk_path,
        import_source="auto"
    )

    stats = extractor.run()
    assert stats["import_source"] == "ftk"
    assert stats["inserted_rows"] == 1
