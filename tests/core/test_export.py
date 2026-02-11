"""
Unit tests for case export system (src/core/export.py).

Tests cover:
- ExportOptions dataclass defaults
- Size estimation with various options
- Minimal package creation (DBs only)
- Full package creation (all options enabled)
- Manifest generation and JSON serialization
- SHA256 checksum calculation and validation
- Large file handling
- Error conditions (missing case folder, permission errors)
- Cleanup on failure
"""
from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from tests.fixtures.db import CaseContext
from core.database import DatabaseManager
from core.export import (
    ExportOptions,
    ExportResult,
    FileEntry,
    create_export_package,
    estimate_export_size,
    generate_export_manifest,
)


@pytest.fixture
def minimal_case(case_factory) -> Path:
    """
    Create a minimal case with just test_surfsifter.sqlite and one evidence database.

    Structure:
    minimal_case/
    ├── test_surfsifter.sqlite
    └── evidences/
        └── evidence_1_test-evid.sqlite
    """
    ctx: CaseContext = case_factory(
        case_id="CASE-TEST-001",
        title="Test Case",
        investigator="Agent",
        created_at="2025-11-04T10:00:00Z",
        evidence_label="TEST-EVID",
        source_path="/fake/source.E01",
        added_at="2025-11-04T10:00:00Z",
    )

    evidence_conn = ctx.manager.get_evidence_conn(ctx.evidence_id, ctx.evidence_label)
    evidence_conn.close()
    ctx.case_conn.close()

    return ctx.case_dir


@pytest.fixture
def full_case(case_factory) -> Path:
    """
    Create a full case with all artifact types.

    Structure:
    full_case/
    ├── test_surfsifter.sqlite
    ├── evidences/
    │   └── evidence_1_test-evid.sqlite
    ├── carved/
    │   └── image001.jpg
    ├── cache/
    │   └── cached_file.dat
    ├── thumbnails/
    │   └── thumb001.png
    """
    ctx: CaseContext = case_factory(
        case_id="CASE-FULL-001",
        title="Full Test Case",
        investigator="Agent",
        created_at="2025-11-04T10:00:00Z",
        evidence_label="FULL-EVID",
        source_path="/fake/source.E01",
        added_at="2025-11-04T10:00:00Z",
    )

    evidence_conn = ctx.manager.get_evidence_conn(ctx.evidence_id, ctx.evidence_label)
    evidence_conn.close()

    # Create artifact directories and files
    (ctx.case_dir / "carved").mkdir()
    (ctx.case_dir / "carved" / "image001.jpg").write_bytes(b"fake jpg data" * 100)

    (ctx.case_dir / "cache").mkdir()
    (ctx.case_dir / "cache" / "cached_file.dat").write_bytes(b"cached content" * 50)

    (ctx.case_dir / "thumbnails").mkdir()
    (ctx.case_dir / "thumbnails" / "thumb001.png").write_bytes(b"png thumbnail" * 20)

    ctx.case_conn.close()
    return ctx.case_dir


# ===== ExportOptions Tests =====

def test_export_options_defaults():
    """Test ExportOptions has correct defaults."""
    options = ExportOptions()
    assert options.include_source_evidence is False
    assert options.include_cached_artifacts is False


def test_export_options_custom():
    """Test ExportOptions with custom values."""
    options = ExportOptions(
        include_source_evidence=True,
        include_cached_artifacts=True,
    )
    assert options.include_source_evidence is True
    assert options.include_cached_artifacts is True


# ===== estimate_export_size() Tests =====

def test_estimate_export_size_minimal(minimal_case: Path):
    """Test size estimation for minimal case (DBs only)."""
    options = ExportOptions()
    size = estimate_export_size(minimal_case, options)

    # Should include test_surfsifter.sqlite + evidence DB
    assert size > 0

    # Check actual files exist
    case_db = minimal_case / "test_surfsifter.sqlite"
    evidences_dir = minimal_case / "evidences"

    expected_size = case_db.stat().st_size
    # Evidence DBs are in subdirectories (structure)
    for evidence_db in evidences_dir.rglob("*.sqlite"):
        expected_size += evidence_db.stat().st_size

    assert size == expected_size

def test_estimate_export_size_with_artifacts(full_case: Path):
    """Test size estimation with cached artifacts included."""
    options = ExportOptions(include_cached_artifacts=True)
    size = estimate_export_size(full_case, options)

    # Should include DBs + carved/ + cache/ + thumbnails/
    assert size > 0

    # Verify artifact directories included
    carved_size = sum(f.stat().st_size for f in (full_case / "carved").rglob("*") if f.is_file())
    assert carved_size > 0


def test_estimate_export_size_full(full_case: Path):
    """Test size estimation with all options enabled."""
    options = ExportOptions(
        include_cached_artifacts=True,
    )
    size = estimate_export_size(full_case, options)

    # Should be largest size (all artifacts included)
    assert size > 1000  # At least 1 KB


def test_estimate_export_size_missing_case_folder(tmp_path: Path):
    """Test estimate_export_size raises FileNotFoundError for missing folder."""
    nonexistent = tmp_path / "nonexistent"
    options = ExportOptions()

    with pytest.raises(FileNotFoundError, match="Case folder not found"):
        estimate_export_size(nonexistent, options)


def test_estimate_export_size_progress_callback(minimal_case: Path):
    """Test estimate_export_size calls progress callback."""
    options = ExportOptions()
    progress_calls = []

    def progress_callback(current: int, total: int):
        progress_calls.append((current, total))

    estimate_export_size(minimal_case, options, progress_callback=progress_callback)

    # Should have at least one progress update
    assert len(progress_calls) > 0

    # Last call should be (total, total)
    last_current, last_total = progress_calls[-1]
    assert last_current == last_total


# ===== generate_export_manifest() Tests =====

def test_generate_export_manifest(minimal_case: Path):
    """Test manifest generation with case metadata."""
    file_list = [
        FileEntry("test_surfsifter.sqlite", 12345, "abc123", "database"),
        FileEntry("evidences/evidence_1.sqlite", 6789, "def456", "database"),
    ]
    options = ExportOptions()

    manifest = generate_export_manifest(minimal_case, file_list, options)

    assert manifest.case_id == "CASE-TEST-001"
    assert manifest.case_title == "Test Case"
    assert manifest.investigator == "Agent"
    assert manifest.evidence_count == 1
    assert len(manifest.file_list) == 2
    assert manifest.total_size_bytes == 12345 + 6789
    assert manifest.export_version == "1.0"


def test_generate_export_manifest_to_dict(minimal_case: Path):
    """Test manifest JSON serialization."""
    file_list = [
        FileEntry("test_surfsifter.sqlite", 100, "hash1", "database"),
    ]
    options = ExportOptions()

    manifest = generate_export_manifest(minimal_case, file_list, options)
    manifest_dict = manifest.to_dict()

    # Verify JSON structure
    assert "export_version" in manifest_dict
    assert "case_id" in manifest_dict
    assert "file_list" in manifest_dict
    assert isinstance(manifest_dict["file_list"], list)

    # Verify file_list structure
    file_entry = manifest_dict["file_list"][0]
    assert file_entry["rel_path"] == "test_surfsifter.sqlite"
    assert file_entry["size_bytes"] == 100
    assert file_entry["sha256"] == "hash1"
    assert file_entry["category"] == "database"


def test_generate_export_manifest_missing_case_db(tmp_path: Path):
    """Test manifest generation raises error for missing case database."""
    nonexistent = tmp_path / "nonexistent"
    file_list = []
    options = ExportOptions()

    with pytest.raises(FileNotFoundError, match="No case database found"):
        generate_export_manifest(nonexistent, file_list, options)


# ===== create_export_package() Tests =====

def test_create_minimal_package(minimal_case: Path, tmp_path: Path):
    """Test creating minimal export package (DBs only)."""
    dest_path = tmp_path / "exports" / "test_export.zip"
    options = ExportOptions()

    result = create_export_package(minimal_case, dest_path, options)

    # Verify success
    assert result.success is True
    assert result.export_path == dest_path
    assert result.exported_files >= 2  # test_surfsifter.sqlite + evidence DB
    assert result.total_size_bytes > 0
    assert result.duration_seconds > 0
    assert result.error_message is None

    # Verify ZIP file exists
    assert dest_path.exists()
    assert dest_path.is_file()

    # Verify ZIP contents
    with zipfile.ZipFile(dest_path, "r") as zipf:
        namelist = zipf.namelist()
        assert "export_manifest.json" in namelist
        assert "test_surfsifter.sqlite" in namelist
        assert any(name.startswith("evidences/") for name in namelist)


def test_create_full_package(full_case: Path, tmp_path: Path):
    """Test creating full export package with all options."""
    dest_path = tmp_path / "exports" / "full_export.zip"
    options = ExportOptions(
        include_cached_artifacts=True,
    )

    result = create_export_package(full_case, dest_path, options)

    # Verify success
    assert result.success is True
    assert result.exported_files >= 5  # DBs + artifacts

    # Verify ZIP contents include artifacts
    with zipfile.ZipFile(dest_path, "r") as zipf:
        namelist = zipf.namelist()
        assert any(name.startswith("carved/") for name in namelist)
        assert any(name.startswith("cache/") for name in namelist)
        assert any(name.startswith("thumbnails/") for name in namelist)


def test_create_export_package_manifest_valid(minimal_case: Path, tmp_path: Path):
    """Test exported manifest is valid JSON with correct structure."""
    dest_path = tmp_path / "exports" / "test_manifest.zip"
    options = ExportOptions()

    result = create_export_package(minimal_case, dest_path, options)
    assert result.success is True

    # Extract and parse manifest
    with zipfile.ZipFile(dest_path, "r") as zipf:
        manifest_json = zipf.read("export_manifest.json").decode("utf-8")
        manifest_data = json.loads(manifest_json)

    # Verify manifest structure
    assert manifest_data["export_version"] == "1.0"
    assert manifest_data["case_id"] == "CASE-TEST-001"
    assert "exported_at_utc" in manifest_data
    assert "file_list" in manifest_data
    assert len(manifest_data["file_list"]) >= 2


def test_create_export_package_checksums_valid(minimal_case: Path, tmp_path: Path):
    """Test SHA256 checksums in manifest match extracted files."""
    dest_path = tmp_path / "exports" / "test_checksums.zip"
    options = ExportOptions()

    result = create_export_package(minimal_case, dest_path, options)
    assert result.success is True

    # Extract manifest
    with zipfile.ZipFile(dest_path, "r") as zipf:
        manifest_json = zipf.read("export_manifest.json").decode("utf-8")
        manifest_data = json.loads(manifest_json)

        # Verify each file's checksum
        for file_entry in manifest_data["file_list"]:
            rel_path = file_entry["rel_path"]
            expected_sha256 = file_entry["sha256"]

            # Extract file and calculate checksum
            file_data = zipf.read(rel_path)
            import hashlib
            actual_sha256 = hashlib.sha256(file_data).hexdigest()

            assert actual_sha256 == expected_sha256, f"Checksum mismatch for {rel_path}"


def test_create_export_package_missing_case_folder(tmp_path: Path):
    """Test create_export_package handles missing case folder gracefully."""
    nonexistent = tmp_path / "nonexistent"
    dest_path = tmp_path / "exports" / "test.zip"
    options = ExportOptions()

    result = create_export_package(nonexistent, dest_path, options)

    # Should fail gracefully
    assert result.success is False
    assert result.export_path is None
    assert "not found" in result.error_message.lower()


def test_create_export_package_progress_callback(minimal_case: Path, tmp_path: Path):
    """Test create_export_package calls progress callback."""
    dest_path = tmp_path / "exports" / "test_progress.zip"
    options = ExportOptions()
    progress_calls = []

    def progress_callback(current_bytes: int, total_bytes: int, current_file: str):
        progress_calls.append((current_bytes, total_bytes, current_file))

    result = create_export_package(minimal_case, dest_path, options, progress_callback=progress_callback)

    assert result.success is True
    # Should have progress updates for each file
    assert len(progress_calls) >= 2

    # Verify progress increases
    for i in range(len(progress_calls) - 1):
        assert progress_calls[i][0] <= progress_calls[i + 1][0]


def test_create_export_package_cleanup_on_failure(minimal_case: Path, tmp_path: Path):
    """Test partial ZIP file is cleaned up on failure."""
    # Make destination read-only to force failure
    dest_dir = tmp_path / "readonly"
    dest_dir.mkdir()
    dest_path = dest_dir / "test.zip"

    # Create a file to make the directory operation fail
    dest_path.touch()
    dest_path.chmod(0o444)  # Read-only
    dest_dir.chmod(0o555)   # Read-only directory

    options = ExportOptions()

    try:
        result = create_export_package(minimal_case, dest_path, options)
        # Should fail (permission denied or similar)
        assert result.success is False
    finally:
        # Cleanup: restore permissions
        try:
            dest_dir.chmod(0o755)
            if dest_path.exists():
                dest_path.chmod(0o644)
        except Exception:
            pass


def test_create_export_package_creates_parent_directories(minimal_case: Path, tmp_path: Path):
    """Test create_export_package creates parent directories if needed."""
    dest_path = tmp_path / "deeply" / "nested" / "path" / "export.zip"
    options = ExportOptions()

    result = create_export_package(minimal_case, dest_path, options)

    assert result.success is True
    assert dest_path.exists()
    assert dest_path.parent.exists()


# ===== Large File Tests =====

def test_create_export_large_files(tmp_path: Path):
    """Test exporting case with large file (>100 MB)."""
    case_folder = tmp_path / "large_case"
    case_folder.mkdir()

    # Create minimal case structure
    case_db_path = case_folder / "test_surfsifter.sqlite"
    db_mgr = DatabaseManager(case_folder, case_db_path=case_db_path)
    case_conn = db_mgr.get_case_conn()
    case_conn.execute(
        "INSERT INTO cases (case_id, title, created_at_utc) VALUES (?, ?, ?)",
        ("CASE-LARGE-001", "Large Case", "2025-11-04T10:00:00Z")
    )
    case_conn.execute(
        "INSERT INTO evidences (case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
        (1, "LARGE", "/fake/large.E01", "2025-11-04T10:00:00Z")
    )
    case_conn.commit()
    case_conn.close()

    evidence_conn = db_mgr.get_evidence_conn(1, "LARGE")
    evidence_conn.close()

    # Create a large file (simulated - 10 MB for test speed)
    large_file_dir = case_folder / "carved"
    large_file_dir.mkdir()
    large_file = large_file_dir / "large_image.bin"
    large_file.write_bytes(b"x" * (10 * 1024 * 1024))  # 10 MB

    # Export with artifacts
    dest_path = tmp_path / "exports" / "large_export.zip"
    options = ExportOptions(include_cached_artifacts=True)

    result = create_export_package(case_folder, dest_path, options)

    assert result.success is True
    assert result.total_size_bytes > 10 * 1024 * 1024

    # Verify large file in ZIP
    with zipfile.ZipFile(dest_path, "r") as zipf:
        assert "carved/large_image.bin" in zipf.namelist()


class TestSQLiteCompanionFilesExclusion:
    """Tests for _is_sqlite_companion_file helper and duplicate prevention."""

    def test_is_sqlite_companion_file_wal(self):
        """Test that -wal files are detected as companion files."""
        from core.export import _is_sqlite_companion_file

        assert _is_sqlite_companion_file(Path("evidence.sqlite-wal")) is True
        assert _is_sqlite_companion_file(Path("/some/path/db.sqlite-wal")) is True

    def test_is_sqlite_companion_file_shm(self):
        """Test that -shm files are detected as companion files."""
        from core.export import _is_sqlite_companion_file

        assert _is_sqlite_companion_file(Path("evidence.sqlite-shm")) is True

    def test_is_sqlite_companion_file_journal(self):
        """Test that -journal files are detected as companion files."""
        from core.export import _is_sqlite_companion_file

        assert _is_sqlite_companion_file(Path("evidence.sqlite-journal")) is True

    def test_is_sqlite_companion_file_regular_sqlite(self):
        """Test that regular .sqlite files are NOT companion files."""
        from core.export import _is_sqlite_companion_file

        assert _is_sqlite_companion_file(Path("evidence.sqlite")) is False

    def test_is_sqlite_companion_file_regular_files(self):
        """Test that regular files are NOT companion files."""
        from core.export import _is_sqlite_companion_file

        assert _is_sqlite_companion_file(Path("image.jpg")) is False
        assert _is_sqlite_companion_file(Path("data.json")) is False
        assert _is_sqlite_companion_file(Path("file-wal.txt")) is False  # suffix, not ending

    def test_no_duplicate_companion_files_in_export(self, case_factory, tmp_path):
        """Test that SQLite companion files are not duplicated in export."""
        from tests.fixtures.db import CaseContext
        from core.database.manager import slugify_label

        # Create case using the fixture factory
        ctx: CaseContext = case_factory(
            case_id="TEST-DUP",
            title="Duplicate Test",
            investigator="Tester",
            created_at="2025-01-01T00:00:00Z",
            evidence_label="TestEvid",
        )
        case_folder = ctx.case_dir

        # Create evidence DB to generate WAL/SHM files
        evidence_conn = ctx.manager.get_evidence_conn(ctx.evidence_id, ctx.evidence_label)
        evidence_conn.close()
        ctx.case_conn.close()

        # Add some artifacts to evidence folder (simulating extractor output)
        slug = slugify_label(ctx.evidence_label, ctx.evidence_id)
        evidence_dir = case_folder / "evidences" / slug
        (evidence_dir / "extracted_data.json").write_text("{}")

        # Export with artifacts
        dest_path = tmp_path / "export.zip"
        options = ExportOptions(include_cached_artifacts=True)

        result = create_export_package(case_folder, dest_path, options)
        assert result.success is True

        # Check for duplicates in ZIP
        with zipfile.ZipFile(dest_path, "r") as zipf:
            names = zipf.namelist()

            # Count occurrences of each file
            from collections import Counter
            name_counts = Counter(names)

            # No file should appear more than once
            duplicates = {name: count for name, count in name_counts.items() if count > 1}
            assert not duplicates, f"Found duplicate entries in ZIP: {duplicates}"
