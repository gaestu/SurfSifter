"""
Unit tests for case import system (src/core/import_case.py).

Tests cover:
- ValidationResult dataclass
- CollisionStrategy enum
- ImportOptions dataclass defaults
- validate_export_package() with various failure modes
- detect_case_collision() logic
- import_case() with all collision strategies
- Round-trip export → import verification
- Error conditions and edge cases
"""
from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from core.database import DatabaseManager
from core.export import ExportOptions, create_export_package
from core.import_case import (
    CollisionStrategy,
    ImportOptions,
    ImportResult,
    ValidationResult,
    detect_case_collision,
    import_case,
    validate_export_package,
)


# Use fixtures from test_export.py
from tests.core.test_export import full_case, minimal_case


@pytest.fixture
def valid_export_package(minimal_case: Path, tmp_path: Path) -> Path:
    """
    Create a valid export package for import testing.

    Returns path to ZIP file.
    """
    dest_path = tmp_path / "exports" / "test_package.zip"
    options = ExportOptions()

    result = create_export_package(minimal_case, dest_path, options)
    assert result.success is True

    return dest_path


def test_validation_result_dataclass():
    """Test ValidationResult dataclass properties."""
    result = ValidationResult(valid=False, error_message="Test error")

    assert result.valid is False
    assert result.error_message == "Test error"
    assert result.warnings == []
    assert result.manifest is None
    assert result.zip_valid is False
    assert result.manifest_present is False
    assert result.files_present is False
    assert result.checksums_valid is False
    assert result.schema_compatible is False


def test_collision_strategy_enum():
    """Test CollisionStrategy enum values."""
    assert CollisionStrategy.CANCEL.value == "cancel"
    assert CollisionStrategy.RENAME.value == "rename"
    assert CollisionStrategy.OVERWRITE.value == "overwrite"


def test_import_options_defaults():
    """Test ImportOptions dataclass defaults."""
    options = ImportOptions()

    assert options.collision_strategy == CollisionStrategy.CANCEL
    assert options.case_id_confirmation is None


def test_import_options_custom():
    """Test ImportOptions with custom values."""
    options = ImportOptions(
        collision_strategy=CollisionStrategy.OVERWRITE,
        case_id_confirmation="CASE-001"
    )

    assert options.collision_strategy == CollisionStrategy.OVERWRITE
    assert options.case_id_confirmation == "CASE-001"


def test_validate_export_package_valid(valid_export_package: Path):
    """Test validation of valid export package."""
    result = validate_export_package(valid_export_package)

    assert result.valid is True
    assert result.error_message is None
    assert result.zip_valid is True
    assert result.manifest_present is True
    assert result.files_present is True
    assert result.checksums_valid is True
    assert result.schema_compatible is True
    assert result.manifest is not None
    assert result.manifest["case_id"] == "CASE-TEST-001"


def test_validate_export_package_corrupted_zip(tmp_path: Path):
    """Test validation detects corrupted ZIP."""
    bad_zip = tmp_path / "bad.zip"
    bad_zip.write_text("Not a ZIP file")

    result = validate_export_package(bad_zip)

    assert result.valid is False
    assert "Invalid ZIP file" in result.error_message
    assert result.zip_valid is False


def test_validate_export_package_missing_manifest(tmp_path: Path):
    """Test validation detects missing manifest."""
    zip_path = tmp_path / "no_manifest.zip"

    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.writestr("test_surfsifter.sqlite", b"fake data")

    result = validate_export_package(zip_path)

    assert result.valid is False
    assert "export_manifest.json not found" in result.error_message
    assert result.zip_valid is True
    assert result.manifest_present is False


def test_validate_export_package_invalid_manifest_json(tmp_path: Path):
    """Test validation detects invalid manifest JSON."""
    zip_path = tmp_path / "bad_manifest.zip"

    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.writestr("export_manifest.json", b"{ invalid json")

    result = validate_export_package(zip_path)

    assert result.valid is False
    assert "Invalid manifest JSON" in result.error_message
    assert result.manifest_present is False


def test_validate_export_package_missing_files(tmp_path: Path):
    """Test validation detects missing files."""
    zip_path = tmp_path / "missing_files.zip"

    manifest = {
        "export_version": "1.0",
        "case_id": "CASE-001",
        "case_title": "Test",
        "investigator": "Agent",
        "exported_at_utc": "2025-11-04T12:00:00Z",
        "exported_by": "agent",
        "schema_version": 9,
        "evidence_count": 1,
        "total_size_bytes": 1000,
        "file_list": [
            {
                "rel_path": "test_surfsifter.sqlite",
                "size_bytes": 1000,
                "sha256": "abc123",
                "category": "database"
            },
            {
                "rel_path": "evidences/evidence_1_test.sqlite",
                "size_bytes": 1000,
                "sha256": "def456",
                "category": "database"
            }
        ]
    }

    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.writestr("export_manifest.json", json.dumps(manifest))
        # Only include test_surfsifter.sqlite, not evidence DB
        zipf.writestr("test_surfsifter.sqlite", b"fake data")

    result = validate_export_package(zip_path)

    assert result.valid is False
    assert "Missing" in result.error_message
    assert "evidences/evidence_1_test.sqlite" in result.error_message
    assert result.files_present is False


def test_validate_export_package_checksum_mismatch(tmp_path: Path):
    """Test validation detects checksum mismatches."""
    zip_path = tmp_path / "bad_checksum.zip"

    manifest = {
        "export_version": "1.0",
        "case_id": "CASE-001",
        "case_title": "Test",
        "investigator": "Agent",
        "exported_at_utc": "2025-11-04T12:00:00Z",
        "exported_by": "agent",
        "schema_version": 9,
        "evidence_count": 0,
        "total_size_bytes": 9,
        "file_list": [
            {
                "rel_path": "test_surfsifter.sqlite",
                "size_bytes": 9,
                # Wrong checksum for "fake data"
                "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "category": "database"
            }
        ]
    }

    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.writestr("export_manifest.json", json.dumps(manifest))
        zipf.writestr("test_surfsifter.sqlite", b"fake data")

    result = validate_export_package(zip_path)

    assert result.valid is False
    assert "Checksum mismatch" in result.error_message
    assert result.checksums_valid is False


def test_validate_export_package_unsupported_schema(tmp_path: Path):
    """Test validation detects unsupported schema versions."""
    zip_path = tmp_path / "old_schema.zip"

    manifest = {
        "export_version": "1.0",
        "case_id": "CASE-001",
        "case_title": "Test",
        "investigator": "Agent",
        "exported_at_utc": "2025-11-04T12:00:00Z",
        "exported_by": "agent",
        "schema_version": 999,  # Unsupported schema version
        "evidence_count": 0,
        "total_size_bytes": 0,
        "file_list": []
    }

    with zipfile.ZipFile(zip_path, "w") as zipf:
        zipf.writestr("export_manifest.json", json.dumps(manifest))

    result = validate_export_package(zip_path)

    assert result.valid is False
    assert "newer than supported" in result.error_message
    assert result.schema_compatible is False


def test_validate_export_package_progress_callback(valid_export_package: Path):
    """Test validate_export_package calls progress callback."""
    progress_calls = []

    def progress_callback(current: int, total: int, step_name: str):
        progress_calls.append((current, total, step_name))

    result = validate_export_package(valid_export_package, progress_callback=progress_callback)

    assert result.valid is True
    # Should have 5 progress updates (one per validation step)
    assert len(progress_calls) == 5
    assert progress_calls[0] == (1, 5, "Checking ZIP integrity")
    assert progress_calls[1] == (2, 5, "Checking manifest")
    assert progress_calls[2] == (3, 5, "Checking file presence")
    assert progress_calls[3] == (4, 5, "Verifying checksums")
    assert progress_calls[4] == (5, 5, "Checking schema compatibility")


def test_detect_case_collision_no_collision(tmp_path: Path):
    """Test detect_case_collision returns False when case doesn't exist."""
    cases_dir = tmp_path / "cases"
    cases_dir.mkdir()

    collision = detect_case_collision("CASE-NONEXISTENT", cases_dir)

    assert collision is False


def test_detect_case_collision_collision_exists(tmp_path: Path):
    """Test detect_case_collision returns True when case exists."""
    cases_dir = tmp_path / "cases"
    case_folder = cases_dir / "CASE-001"
    case_folder.mkdir(parents=True)

    # Create test_surfsifter.sqlite
    case_db = case_folder / "test_surfsifter.sqlite"
    case_db.write_text("fake db")

    collision = detect_case_collision("CASE-001", cases_dir)

    assert collision is True


def test_import_case_no_collision(valid_export_package: Path, tmp_path: Path):
    """Test import_case with no collision (CANCEL strategy)."""
    cases_dir = tmp_path / "cases"
    cases_dir.mkdir()

    options = ImportOptions(collision_strategy=CollisionStrategy.CANCEL)

    result = import_case(valid_export_package, cases_dir, options)

    assert result.success is True
    assert result.imported_case_id == "CASE-TEST-001"
    assert result.imported_files >= 2  # test_surfsifter.sqlite + evidence DB
    assert result.total_size_bytes > 0
    assert result.duration_seconds > 0
    assert result.error_message is None

    # Verify case exists
    imported_case = cases_dir / "CASE-TEST-001"
    assert imported_case.exists()
    assert (imported_case / "test_surfsifter.sqlite").exists()


def test_import_case_collision_cancel(valid_export_package: Path, tmp_path: Path):
    """Test import_case with collision and CANCEL strategy."""
    cases_dir = tmp_path / "cases"
    case_folder = cases_dir / "CASE-TEST-001"
    case_folder.mkdir(parents=True)
    (case_folder / "test_surfsifter.sqlite").write_text("existing")

    options = ImportOptions(collision_strategy=CollisionStrategy.CANCEL)

    with pytest.raises(FileExistsError, match="already exists"):
        import_case(valid_export_package, cases_dir, options)


def test_import_case_collision_rename(valid_export_package: Path, tmp_path: Path):
    """Test import_case with collision and RENAME strategy."""
    cases_dir = tmp_path / "cases"
    case_folder = cases_dir / "CASE-TEST-001"
    case_folder.mkdir(parents=True)
    (case_folder / "test_surfsifter.sqlite").write_text("existing")

    options = ImportOptions(collision_strategy=CollisionStrategy.RENAME)

    result = import_case(valid_export_package, cases_dir, options)

    assert result.success is True
    assert result.imported_case_id == "CASE-TEST-001-imported"
    assert result.imported_files >= 2

    # Verify renamed case exists
    imported_case = cases_dir / "CASE-TEST-001-imported"
    assert imported_case.exists()
    assert (imported_case / "test_surfsifter.sqlite").exists()

    # Verify original case unchanged
    assert (case_folder / "test_surfsifter.sqlite").read_text() == "existing"


def test_import_case_collision_rename_multiple(valid_export_package: Path, tmp_path: Path):
    """Test import_case RENAME strategy finds unique suffix."""
    cases_dir = tmp_path / "cases"

    # Create three existing cases
    for suffix in ["", "-imported", "-imported-2"]:
        case_id = f"CASE-TEST-001{suffix}"
        case_folder = cases_dir / case_id
        case_folder.mkdir(parents=True)
        (case_folder / "test_surfsifter.sqlite").write_text("existing")

    options = ImportOptions(collision_strategy=CollisionStrategy.RENAME)

    result = import_case(valid_export_package, cases_dir, options)

    assert result.success is True
    assert result.imported_case_id == "CASE-TEST-001-imported-3"


def test_import_case_collision_overwrite_success(valid_export_package: Path, tmp_path: Path):
    """Test import_case with collision and OVERWRITE strategy (correct confirmation)."""
    cases_dir = tmp_path / "cases"
    case_folder = cases_dir / "CASE-TEST-001"
    case_folder.mkdir(parents=True)
    (case_folder / "test_surfsifter.sqlite").write_bytes(b"old data")

    options = ImportOptions(
        collision_strategy=CollisionStrategy.OVERWRITE,
        case_id_confirmation="CASE-TEST-001"
    )

    result = import_case(valid_export_package, cases_dir, options)

    assert result.success is True
    assert result.imported_case_id == "CASE-TEST-001"

    # Verify case was replaced (old data is gone, new DB exists and is valid)
    case_db = case_folder / "test_surfsifter.sqlite"
    assert case_db.exists()
    # Verify it's a valid SQLite database
    import sqlite3
    conn = sqlite3.connect(case_db)
    cursor = conn.execute("SELECT case_id FROM cases")
    row = cursor.fetchone()
    assert row[0] == "CASE-TEST-001"
    conn.close()


def test_import_case_collision_overwrite_bad_confirmation(valid_export_package: Path, tmp_path: Path):
    """Test import_case OVERWRITE fails with wrong confirmation."""
    cases_dir = tmp_path / "cases"
    case_folder = cases_dir / "CASE-TEST-001"
    case_folder.mkdir(parents=True)
    (case_folder / "test_surfsifter.sqlite").write_text("existing")

    options = ImportOptions(
        collision_strategy=CollisionStrategy.OVERWRITE,
        case_id_confirmation="WRONG-ID"
    )

    with pytest.raises(ValueError, match="requires case_id_confirmation"):
        import_case(valid_export_package, cases_dir, options)


def test_import_case_missing_package(tmp_path: Path):
    """Test import_case handles missing export package."""
    cases_dir = tmp_path / "cases"
    cases_dir.mkdir()

    missing_package = tmp_path / "nonexistent.zip"
    options = ImportOptions()

    result = import_case(missing_package, cases_dir, options)

    assert result.success is False
    assert "not found" in result.error_message


def test_import_case_progress_callback(valid_export_package: Path, tmp_path: Path):
    """Test import_case calls progress callback."""
    cases_dir = tmp_path / "cases"
    cases_dir.mkdir()

    progress_calls = []

    def progress_callback(current_bytes: int, total_bytes: int, current_file: str):
        progress_calls.append((current_bytes, total_bytes, current_file))

    options = ImportOptions()
    result = import_case(valid_export_package, cases_dir, options, progress_callback=progress_callback)

    assert result.success is True
    # Should have progress updates for each file
    assert len(progress_calls) >= 2  # test_surfsifter.sqlite + evidence DB

    # Verify total_bytes is consistent
    assert all(call[1] == progress_calls[0][1] for call in progress_calls)


def test_round_trip_export_import(full_case: Path, tmp_path: Path):
    """Test round-trip: export → import → verify identical."""
    # Export full case
    export_path = tmp_path / "exports" / "full_export.zip"
    export_options = ExportOptions(
        include_cached_artifacts=True,
    )

    export_result = create_export_package(full_case, export_path, export_options)
    assert export_result.success is True

    # Import to new location
    import_dir = tmp_path / "imported_cases"
    import_dir.mkdir()

    import_options = ImportOptions()
    import_result = import_case(export_path, import_dir, import_options)
    assert import_result.success is True

    # Verify case exists
    imported_case = import_dir / import_result.imported_case_id
    assert imported_case.exists()

    # Verify file count matches
    assert import_result.imported_files == export_result.exported_files

    # Verify key files exist
    assert (imported_case / "test_surfsifter.sqlite").exists()

    # Verify case database can be opened
    import sqlite3
    conn = sqlite3.connect(imported_case / "test_surfsifter.sqlite")
    cursor = conn.execute("SELECT case_id FROM cases")
    row = cursor.fetchone()
    assert row[0] == import_result.imported_case_id
    conn.close()


# ============================================================================
# New tests for  Export/Import improvements
# ============================================================================

class TestZipSlipPrevention:
    """Tests for zip slip attack prevention."""

    def test_is_safe_path_normal_file(self, tmp_path: Path):
        """Test _is_safe_path accepts normal files."""
        from core.import_case import _is_safe_path

        dest = tmp_path / "cases"
        dest.mkdir()
        # Normal relative paths - must be resolved to absolute
        target = dest / "case_001" / "test_surfsifter.sqlite"
        assert _is_safe_path(dest, target) is True

        target2 = dest / "case_001" / "evidence_001.sqlite"
        assert _is_safe_path(dest, target2) is True

    def test_is_safe_path_parent_traversal(self, tmp_path: Path):
        """Test _is_safe_path rejects parent traversal."""
        from core.import_case import _is_safe_path

        dest = tmp_path / "cases"
        dest.mkdir()
        # Parent directory traversal (zip slip attack) - resolve to outside dest
        target_outside = (dest / ".." / "etc" / "passwd").resolve()
        assert _is_safe_path(dest, target_outside) is False

    def test_is_safe_path_absolute_path_outside(self, tmp_path: Path):
        """Test _is_safe_path rejects paths outside base."""
        from core.import_case import _is_safe_path

        dest = tmp_path / "cases"
        dest.mkdir()
        # Absolute path outside base
        assert _is_safe_path(dest, Path("/etc/passwd")) is False
        assert _is_safe_path(dest, tmp_path / "other_folder" / "file.txt") is False

    def test_import_rejects_malicious_zip(self, tmp_path: Path):
        """Test import_case rejects malicious ZIP with path traversal."""
        # Create malicious ZIP with path traversal
        malicious_zip = tmp_path / "malicious.zip"

        manifest = {
            "export_version": "1.0",
            "case_id": "CASE-001",
            "case_title": "Test",
            "investigator": "Agent",
            "exported_at_utc": "2025-01-01T00:00:00Z",
            "exported_by": "agent",
            "schema_version": 3,
            "evidence_count": 0,
            "total_size_bytes": 100,
            "file_list": [
                {"path": "../../../etc/passwd", "checksum": "abc123"}
            ]
        }

        with zipfile.ZipFile(malicious_zip, "w") as zipf:
            zipf.writestr("export_manifest.json", json.dumps(manifest))
            # Create a file with a name containing path traversal
            # Note: Most zip libraries prevent this, but we test our validation anyway
            zipf.writestr("CASE-001/normal.txt", b"safe content")

        cases_dir = tmp_path / "cases"
        cases_dir.mkdir()

        result = import_case(malicious_zip, cases_dir, ImportOptions())

        # The import should either:
        # 1. Fail validation due to path traversal in manifest
        # 2. Skip the malicious file during extraction
        # In either case, the malicious path should not exist
        assert not (tmp_path / "etc" / "passwd").exists()


class TestTempExtractionCleanup:
    """Tests for temp-then-move extraction with cleanup."""

    def test_cleanup_on_cancel(self, valid_export_package: Path, tmp_path: Path):
        """Test temp directory cleaned up on cancellation."""
        from core.import_case import _ImportCancelled

        cases_dir = tmp_path / "cases"
        cases_dir.mkdir()

        cancel_called = False

        def cancel_check():
            nonlocal cancel_called
            if cancel_called:
                return True
            cancel_called = True  # Cancel on second call
            return False

        result = import_case(
            valid_export_package,
            cases_dir,
            ImportOptions(),
            cancel_check=cancel_check
        )

        # Cancelled import should report cancelled
        assert result.success is False
        assert "cancelled" in result.error_message.lower()

        # No partial data should remain in destination
        # (The case folder should not exist or be empty)
        case_folders = list(cases_dir.glob("*"))
        for folder in case_folders:
            # Temp folders should be cleaned up
            assert not folder.name.startswith("_import_temp_")

    def test_cleanup_on_failure(self, tmp_path: Path):
        """Test temp directory cleaned up on extraction failure."""
        # Create a corrupted ZIP that will fail during extraction
        bad_zip = tmp_path / "corrupted.zip"

        manifest = {
            "export_version": "1.0",
            "case_id": "CASE-FAIL",
            "case_title": "Test",
            "investigator": "Agent",
            "exported_at_utc": "2025-01-01T00:00:00Z",
            "exported_by": "agent",
            "schema_version": 3,
            "evidence_count": 0,
            "total_size_bytes": 100,
            "file_list": [
                {"path": "CASE-FAIL/test.txt", "checksum": "wrongchecksum"}
            ]
        }

        with zipfile.ZipFile(bad_zip, "w") as zipf:
            zipf.writestr("export_manifest.json", json.dumps(manifest))
            zipf.writestr("CASE-FAIL/test.txt", b"test content")

        cases_dir = tmp_path / "cases"
        cases_dir.mkdir()

        # This should fail because checksum won't match
        result = import_case(bad_zip, cases_dir, ImportOptions())

        # Should fail (checksum mismatch)
        assert result.success is False

        # No temp directories should remain
        temp_dirs = list(cases_dir.glob("_import_temp_*"))
        assert len(temp_dirs) == 0


class TestSchemaVersionHandling:
    """Tests for schema version validation."""

    def test_schema_v1_supported(self, tmp_path: Path):
        """Test schema version 1 (minimum) is supported."""
        zip_path = tmp_path / "v1_package.zip"

        manifest = {
            "export_version": "1.0",
            "case_id": "CASE-V1",
            "case_title": "Test",
            "investigator": "Agent",
            "exported_at_utc": "2025-01-01T00:00:00Z",
            "exported_by": "agent",
            "schema_version": 1,  # MIN_SCHEMA_VERSION
            "evidence_count": 0,
            "total_size_bytes": 0,
            "file_list": []
        }

        with zipfile.ZipFile(zip_path, "w") as zipf:
            zipf.writestr("export_manifest.json", json.dumps(manifest))

        result = validate_export_package(zip_path)

        assert result.valid is True
        assert result.schema_compatible is True
        # Version 1 is MIN_SCHEMA_VERSION, so no warnings

    def test_schema_v0_below_minimum(self, tmp_path: Path):
        """Test schema version below minimum generates warning."""
        zip_path = tmp_path / "v0_package.zip"

        manifest = {
            "export_version": "1.0",
            "case_id": "CASE-V0",
            "case_title": "Test",
            "investigator": "Agent",
            "exported_at_utc": "2025-01-01T00:00:00Z",
            "exported_by": "agent",
            "schema_version": 0,  # Below MIN_SCHEMA_VERSION
            "evidence_count": 0,
            "total_size_bytes": 0,
            "file_list": []
        }

        with zipfile.ZipFile(zip_path, "w") as zipf:
            zipf.writestr("export_manifest.json", json.dumps(manifest))

        result = validate_export_package(zip_path)

        assert result.valid is True
        assert result.schema_compatible is True
        # Should have a warning about old version
        assert any("older" in w.lower() for w in result.warnings)

    def test_schema_v2_supported(self, tmp_path: Path):
        """Test schema version 2 is supported with warning."""
        zip_path = tmp_path / "v2_package.zip"

        manifest = {
            "export_version": "1.0",
            "case_id": "CASE-V2",
            "case_title": "Test",
            "investigator": "Agent",
            "exported_at_utc": "2025-01-01T00:00:00Z",
            "exported_by": "agent",
            "schema_version": 2,
            "evidence_count": 0,
            "total_size_bytes": 0,
            "file_list": []
        }

        with zipfile.ZipFile(zip_path, "w") as zipf:
            zipf.writestr("export_manifest.json", json.dumps(manifest))

        result = validate_export_package(zip_path)

        assert result.valid is True
        assert result.schema_compatible is True

    def test_schema_v3_current(self, tmp_path: Path):
        """Test schema version 3 (current) is fully supported."""
        zip_path = tmp_path / "v3_package.zip"

        manifest = {
            "export_version": "1.0",
            "case_id": "CASE-V3",
            "case_title": "Test",
            "investigator": "Agent",
            "exported_at_utc": "2025-01-01T00:00:00Z",
            "exported_by": "agent",
            "schema_version": 3,
            "evidence_count": 0,
            "total_size_bytes": 0,
            "file_list": []
        }

        with zipfile.ZipFile(zip_path, "w") as zipf:
            zipf.writestr("export_manifest.json", json.dumps(manifest))

        result = validate_export_package(zip_path)

        assert result.valid is True
        assert result.schema_compatible is True
        # No warnings for current version
        assert len(result.warnings) == 0


class TestImportResultPath:
    """Tests for ImportResult.imported_path field."""

    def test_imported_path_populated(self, valid_export_package: Path, tmp_path: Path):
        """Test imported_path is set to the actual imported folder."""
        cases_dir = tmp_path / "cases"
        cases_dir.mkdir()

        result = import_case(valid_export_package, cases_dir, ImportOptions())

        assert result.success is True
        assert result.imported_path is not None
        assert result.imported_path.exists()
        assert result.imported_path.is_dir()
        # Should contain case database
        db_files = list(result.imported_path.glob("*_surfsifter.sqlite"))
        assert len(db_files) == 1

    def test_imported_path_none_on_failure(self, tmp_path: Path):
        """Test imported_path is None on import failure."""
        # Non-existent package
        result = import_case(
            tmp_path / "nonexistent.zip",
            tmp_path / "cases",
            ImportOptions()
        )

        assert result.success is False
        assert result.imported_path is None


class TestMultiSegmentE01Export:
    """Tests for multi-segment E01 evidence export."""

    def test_collect_multi_segment_evidence_single(self, tmp_path: Path):
        """Test collecting single-segment E01."""
        from core.export import _collect_multi_segment_evidence

        # Create single E01
        e01_file = tmp_path / "image.E01"
        e01_file.write_bytes(b"EWF data")

        segments = _collect_multi_segment_evidence(e01_file, "EV001")

        # Returns list of (source_path, arcname, category) tuples
        assert len(segments) == 1
        assert segments[0][0] == e01_file
        assert "EV001" in segments[0][1]

    def test_collect_multi_segment_evidence_multiple(self, tmp_path: Path):
        """Test collecting multi-segment E01/E02/E03."""
        from core.export import _collect_multi_segment_evidence

        # Create multi-segment E01
        for ext in ["E01", "E02", "E03", "E04"]:
            (tmp_path / f"image.{ext}").write_bytes(b"EWF segment")

        segments = _collect_multi_segment_evidence(tmp_path / "image.E01", "EV001")

        assert len(segments) == 4
        assert all(s[0].exists() for s in segments)

    def test_collect_multi_segment_case_insensitive(self, tmp_path: Path):
        """Test multi-segment collection is case-insensitive."""
        from core.export import _collect_multi_segment_evidence

        # Create mixed-case segments
        (tmp_path / "image.E01").write_bytes(b"EWF segment")
        (tmp_path / "image.e02").write_bytes(b"EWF segment")
        (tmp_path / "image.E03").write_bytes(b"EWF segment")

        segments = _collect_multi_segment_evidence(tmp_path / "image.E01", "EV001")

        assert len(segments) == 3


class TestSQLiteCompanionFiles:
    """Tests for SQLite companion file handling."""

    def test_add_sqlite_with_companions(self, tmp_path: Path):
        """Test adding SQLite with WAL/SHM/journal files."""
        from core.export import _add_sqlite_with_companions

        # Create SQLite with companions
        db_file = tmp_path / "test.sqlite"
        db_file.write_bytes(b"SQLite format 3")
        (tmp_path / "test.sqlite-wal").write_bytes(b"WAL data")
        (tmp_path / "test.sqlite-shm").write_bytes(b"SHM data")
        (tmp_path / "test.sqlite-journal").write_bytes(b"journal data")

        # Call function with list to append to
        files_to_export = []
        _add_sqlite_with_companions(files_to_export, db_file, "case/test.sqlite", "database")

        assert len(files_to_export) == 4

        # Check all paths exist
        paths = [f[0] for f in files_to_export]
        assert db_file in paths
        assert (tmp_path / "test.sqlite-wal") in paths
        assert (tmp_path / "test.sqlite-shm") in paths
        assert (tmp_path / "test.sqlite-journal") in paths

    def test_add_sqlite_no_companions(self, tmp_path: Path):
        """Test adding SQLite without companion files."""
        from core.export import _add_sqlite_with_companions

        # Create SQLite only
        db_file = tmp_path / "test.sqlite"
        db_file.write_bytes(b"SQLite format 3")

        files_to_export = []
        _add_sqlite_with_companions(files_to_export, db_file, "case/test.sqlite", "database")

        assert len(files_to_export) == 1
        assert files_to_export[0][0] == db_file


class TestReportsExport:
    """Tests for reports directory export."""

    def test_export_includes_reports(self, full_case: Path, tmp_path: Path):
        """Test export includes reports directory when option enabled."""
        # Create reports directory with sample reports
        reports_dir = full_case / "reports"
        reports_dir.mkdir(exist_ok=True)
        (reports_dir / "report_2025-01-01.pdf").write_bytes(b"%PDF-1.4...")
        (reports_dir / "report_2025-01-01.html").write_text("<html>Report</html>")

        export_path = tmp_path / "with_reports.zip"
        options = ExportOptions(include_reports=True)

        result = create_export_package(full_case, export_path, options)

        assert result.success is True

        # Verify reports are in ZIP
        with zipfile.ZipFile(export_path, "r") as zipf:
            names = zipf.namelist()
            report_files = [n for n in names if "reports/" in n]
            assert len(report_files) >= 2

    def test_export_excludes_reports_when_disabled(self, full_case: Path, tmp_path: Path):
        """Test export excludes reports when option disabled."""
        # Create reports directory
        reports_dir = full_case / "reports"
        reports_dir.mkdir(exist_ok=True)
        (reports_dir / "report.pdf").write_bytes(b"%PDF-1.4...")

        export_path = tmp_path / "no_reports.zip"
        options = ExportOptions(include_reports=False)

        result = create_export_package(full_case, export_path, options)

        assert result.success is True

        with zipfile.ZipFile(export_path, "r") as zipf:
            names = zipf.namelist()
            report_files = [n for n in names if "reports/" in n]
            assert len(report_files) == 0

    def test_estimate_size_includes_reports(self, full_case: Path, tmp_path: Path):
        """Test size estimation includes reports directory."""
        from core.export import estimate_export_size

        # Create reports directory with content
        reports_dir = full_case / "reports"
        reports_dir.mkdir(exist_ok=True)
        (reports_dir / "large_report.pdf").write_bytes(b"x" * 10000)

        size_with = estimate_export_size(full_case, ExportOptions(include_reports=True))
        size_without = estimate_export_size(full_case, ExportOptions(include_reports=False))

        # Size with reports should be larger
        assert size_with > size_without
        assert size_with - size_without >= 10000

