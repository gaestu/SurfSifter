"""
Unit tests for Image Carving Extractor.

Tests cover:
- Metadata and configuration
- Tool discovery and validation
- Extraction workflow
- Ingestion workflow
- Error handling
- Forensic integrity
"""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
import pytest

from extractors._shared.carving import ImageCarvingExtractor
from extractors.callbacks import ExtractorCallbacks
from extractors.base import ExtractorMetadata
from extractors._shared.carving.worker import CarvingRunResult


# ========== Fixtures ==========

@pytest.fixture
def extractor():
    """Create ImageCarvingExtractor instance."""
    return ImageCarvingExtractor()


@pytest.fixture
def mock_callbacks():
    """Create mock ExtractorCallbacks."""
    return Mock(spec=ExtractorCallbacks)


@pytest.fixture
def temp_workspace(tmp_path):
    """Create temporary workspace with evidence structure."""
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    # Create evidence database
    evidence_db = workspace / "evidence_test.sqlite"
    conn = sqlite3.connect(evidence_db)

    # Create images table (matching current schema in 0001_evidence_schema.sql)
    # discovered_by/run_id/cache_key removed, first_discovered_by/at added
    conn.execute("""
        CREATE TABLE images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            rel_path TEXT NOT NULL,
            filename TEXT NOT NULL,
            md5 TEXT,
            sha256 TEXT UNIQUE,
            phash TEXT,
            phash_prefix INTEGER,
            exif_json TEXT,
            ts_utc TEXT,
            tags TEXT,
            notes TEXT,
            size_bytes INTEGER,
            first_discovered_by TEXT NOT NULL,
            first_discovered_at TEXT
        )
    """)

    # Create image_discoveries table (multi-source provenance,  cache columns)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS image_discoveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            image_id INTEGER NOT NULL REFERENCES images(id) ON DELETE CASCADE,
            discovered_by TEXT NOT NULL,
            extractor_version TEXT,
            run_id TEXT NOT NULL,
            discovered_at TEXT DEFAULT (datetime('now')),
            fs_path TEXT,
            fs_mtime_epoch REAL,
            fs_mtime TEXT,
            fs_atime_epoch REAL,
            fs_atime TEXT,
            fs_crtime_epoch REAL,
            fs_crtime TEXT,
            fs_ctime_epoch REAL,
            fs_ctime TEXT,
            fs_inode INTEGER,
            carved_offset_bytes INTEGER,
            carved_block_size INTEGER,
            carved_tool_output TEXT,
            cache_url TEXT,
            cache_key TEXT,
            cache_filename TEXT,
            cache_response_time TEXT,
            source_metadata_json TEXT
        )
    """)
    conn.commit()
    conn.close()

    return workspace, evidence_db


@pytest.fixture
def mock_evidence_fs():
    """Create mock evidence filesystem."""
    fs = Mock()
    fs.segments = [Path("/fake/evidence.E01")]
    return fs


def _make_run_result(carved_files):
    """Helper to construct a CarvingRunResult for tests."""
    return CarvingRunResult(
        carved_files=carved_files,
        stdout="",
        stderr="",
        returncode=0,
        command=["foremost", "-o", "/tmp/out", "/fake/evidence.E01"],
        input_source="/fake/evidence.E01",
        input_type="ewf",
        audit_path=None,
    )


# ========== Metadata Tests ==========

def test_metadata_structure(extractor):
    """Test metadata has required fields."""
    metadata = extractor.metadata

    assert isinstance(metadata, ExtractorMetadata)
    assert metadata.name == "image_carving"
    assert metadata.display_name == "Image Carving"
    assert metadata.version
    assert "." in metadata.version
    assert metadata.description
    assert "foremost" in metadata.requires_tools or "scalpel" in metadata.requires_tools


def test_metadata_tool_discovery(extractor):
    """Test metadata includes tool availability status."""
    metadata = extractor.metadata

    assert metadata.requires_tools is not None
    assert len(metadata.requires_tools) > 0

    # Check tool availability is populated
    # (actual availability depends on system, just check structure)
    assert isinstance(metadata.requires_tools, list)


def test_default_file_types(extractor):
    """Test default file types configuration."""
    assert hasattr(extractor, "DEFAULT_FILE_TYPES")
    assert isinstance(extractor.DEFAULT_FILE_TYPES, dict)
    assert "jpg" in extractor.DEFAULT_FILE_TYPES
    assert "png" in extractor.DEFAULT_FILE_TYPES
    assert "gif" in extractor.DEFAULT_FILE_TYPES


# ========== Tool Discovery Tests ==========

@patch("extractors._shared.carving.extractor.discover_tools")
def test_can_run_with_foremost(mock_discover, extractor, mock_evidence_fs):
    """Test can_run_extraction when foremost is available."""
    mock_discover.return_value = {
        "foremost": Mock(available=True, path=Path("/usr/bin/foremost"), version="1.5.7"),
        "scalpel": Mock(available=False, version=None)
    }

    can_run, reason = extractor.can_run_extraction(mock_evidence_fs)

    assert can_run is True
    assert reason == "" or reason is None


@patch("extractors._shared.carving.extractor.discover_tools")
def test_can_run_with_scalpel(mock_discover, extractor, mock_evidence_fs):
    """Test can_run_extraction when only scalpel is available."""
    mock_discover.return_value = {
        "foremost": Mock(available=False, version=None),
        "scalpel": Mock(available=True, path=Path("/usr/bin/scalpel"), version="2.0")
    }

    can_run, reason = extractor.can_run_extraction(mock_evidence_fs)

    assert can_run is True
    assert reason == "" or reason is None


@patch("extractors._shared.carving.extractor.discover_tools")
def test_cannot_run_without_tools(mock_discover, extractor, mock_evidence_fs):
    """Test can_run_extraction when no tools available."""
    mock_discover.return_value = {
        "foremost": Mock(available=False, version=None),
        "scalpel": Mock(available=False, version=None)
    }

    can_run, reason = extractor.can_run_extraction(mock_evidence_fs)

    assert can_run is False
    assert reason is not None
    assert "foremost" in reason.lower() or "scalpel" in reason.lower()


# ========== Extraction Workflow Tests ==========

@patch("extractors._shared.carving.worker.run_carving_extraction")
@patch("extractors._shared.carving.extractor.discover_tools")
def test_run_extraction_success(mock_discover, mock_carving, extractor, temp_workspace, mock_evidence_fs, mock_callbacks):
    """Test successful extraction workflow."""
    workspace, _ = temp_workspace
    output_dir = workspace / "output"
    output_dir.mkdir()

    # Mock tools
    mock_discover.return_value = {
        "foremost": Mock(available=True, path=Path("/usr/bin/foremost"), version="1.5.7"),
        "scalpel": Mock(available=False, version=None)
    }

    # Mock successful carving
    carved_dir = output_dir / "carved" / "jpg"
    carved_dir.mkdir(parents=True)
    img1 = carved_dir / "image1.jpg"
    img2 = carved_dir / "image2.jpg"
    img1.write_text("fake image")
    img2.write_text("fake image")

    mock_carving.return_value = _make_run_result([img1, img2])

    success = extractor.run_extraction(
        evidence_fs=mock_evidence_fs,
        output_dir=output_dir,
        config={},
        callbacks=mock_callbacks
    )

    assert success is True
    mock_callbacks.on_step.assert_called()


@patch("extractors._shared.carving.worker.run_carving_extraction")
@patch("extractors._shared.carving.extractor.discover_tools")
def test_run_extraction_failure(mock_discover, mock_carving, extractor, temp_workspace, mock_evidence_fs, mock_callbacks):
    """Test extraction workflow handles failures."""
    workspace, _ = temp_workspace
    output_dir = workspace / "output"
    output_dir.mkdir()

    # Mock tools
    mock_discover.return_value = {
        "foremost": Mock(available=True, path=Path("/usr/bin/foremost"), version="1.5.7"),
        "scalpel": Mock(available=False, version=None)
    }

    # Mock failed carving
    mock_carving.side_effect = Exception("Tool failed")

    success = extractor.run_extraction(
        evidence_fs=mock_evidence_fs,
        output_dir=output_dir,
        config={},
        callbacks=mock_callbacks
    )

    assert success is False
    mock_callbacks.on_error.assert_called()


@patch("extractors._shared.carving.worker.run_carving_extraction")
@patch("extractors._shared.carving.extractor.discover_tools")
def test_run_extraction_creates_manifest(mock_discover, mock_carving, extractor, temp_workspace, mock_evidence_fs, mock_callbacks):
    """Test extraction creates forensic manifest."""
    workspace, _ = temp_workspace
    output_dir = workspace / "output"
    output_dir.mkdir()

    # Mock tools
    mock_discover.return_value = {
        "foremost": Mock(available=True, path=Path("/usr/bin/foremost"), version="1.5.7"),
        "scalpel": Mock(available=False, version=None)
    }

    # Create fake carved files
    carved_dir = output_dir / "carved" / "jpg"
    carved_dir.mkdir(parents=True)
    img = carved_dir / "test.jpg"
    img.write_text("fake")

    mock_carving.return_value = _make_run_result([img])

    extractor.run_extraction(
        evidence_fs=mock_evidence_fs,
        output_dir=output_dir,
        config={},
        callbacks=mock_callbacks
    )

    # Check manifest exists
    manifest = output_dir / "manifest.json"
    assert manifest.exists()

    import json
    with open(manifest) as f:
        data = json.load(f)

    assert "run_id" in data
    assert data.get("tool", {}).get("name") == "foremost"
    assert "carved_files" in data
    assert len(data["carved_files"]) == 1
    file_entry = data["carved_files"][0]
    assert file_entry["md5"]
    assert file_entry["sha256"]
    assert data["stats"]["carved_total"] == 1


# ========== Ingestion Workflow Tests ==========

@patch("extractors._shared.carving.ingestion.ParallelImageProcessor")
def test_run_ingestion_success(mock_processor_class, extractor, temp_workspace, mock_callbacks):
    """Test successful ingestion workflow."""
    workspace, evidence_db = temp_workspace
    output_dir = workspace / "output"
    output_dir.mkdir()

    # Create fake carved files
    carved_dir = output_dir / "carved" / "jpg"
    carved_dir.mkdir(parents=True)
    (carved_dir / "image1.jpg").write_text("fake image")
    (carved_dir / "image2.jpg").write_text("fake image")

    # Create manifest
    import json
    manifest = output_dir / "manifest.json"
    manifest.write_text(json.dumps({
        "run_id": "test_run_001",
        "tool": "foremost",
        "file_count": 2,
        "files": []
    }))

    # Mock image processor
    mock_processor = Mock()
    mock_processor_class.return_value = mock_processor

    # Mock processing results
    from extractors._shared.carving.processor import ImageProcessResult
    mock_processor.process_images.return_value = [
        ImageProcessResult(
            path=carved_dir / "image1.jpg",
            rel_path="carved/jpg/image1.jpg",
            filename="image1.jpg",
            md5="abc123",
            sha256="def456",
            phash="1234567890abcdef",
            exif_json="{}",
        ),
        ImageProcessResult(
            path=carved_dir / "image2.jpg",
            rel_path="carved/jpg/image2.jpg",
            filename="image2.jpg",
            md5="xyz789",
            sha256="uvw012",
            phash=None,  # Corrupted image
            exif_json="{}",
        ),
    ]

    conn = sqlite3.connect(evidence_db)
    success = extractor.run_ingestion(
        output_dir=output_dir,
        evidence_conn=conn,
        evidence_id=1,
        config={},
        callbacks=mock_callbacks
    )
    conn.close()

    assert success is True
    mock_callbacks.on_step.assert_called()

    # Verify database insert
    conn = sqlite3.connect(evidence_db)
    cursor = conn.execute("SELECT COUNT(*) FROM images")
    count = cursor.fetchone()[0]
    conn.close()

    assert count == 2


def test_run_ingestion_no_files(extractor, temp_workspace, mock_callbacks):
    """Test ingestion handles empty carved directory."""
    workspace, evidence_db = temp_workspace
    output_dir = workspace / "output"
    output_dir.mkdir()

    # Create manifest
    import json
    manifest = output_dir / "manifest.json"
    manifest.write_text(json.dumps({
        "run_id": "test_run_001",
        "tool": "foremost",
        "file_count": 0,
        "files": []
    }))

    conn = sqlite3.connect(evidence_db)
    success = extractor.run_ingestion(
        output_dir=output_dir,
        evidence_conn=conn,
        evidence_id=1,
        config={},
        callbacks=mock_callbacks
    )
    conn.close()

    # Should succeed but insert nothing
    assert success is True


# ========== Error Handling Tests ==========

def test_extraction_validates_output_dir(extractor, mock_evidence_fs, mock_callbacks, tmp_path):
    """Test extraction validates output directory exists."""
    # Use a real path that can be created
    output_dir = tmp_path / "output"

    with patch("extractors._shared.carving.extractor.discover_tools") as mock_discover:
         mock_discover.return_value = {"foremost": Mock(available=True, path=Path("/usr/bin/foremost"), version="1.5.7")}
         with patch("extractors._shared.carving.worker.run_carving_extraction") as mock_worker:
             mock_worker.return_value = _make_run_result([])
             extractor.run_extraction(
                evidence_fs=mock_evidence_fs,
                output_dir=output_dir,
                config={},
                callbacks=mock_callbacks
            )

    assert output_dir.exists()


def test_ingestion_validates_manifest(extractor, temp_workspace, mock_callbacks):
    """Test ingestion handles missing manifest."""
    workspace, evidence_db = temp_workspace
    output_dir = workspace / "output"
    output_dir.mkdir()

    # No manifest file

    conn = sqlite3.connect(evidence_db)
    success = extractor.run_ingestion(
        output_dir=output_dir,
        evidence_conn=conn,
        evidence_id=1,
        config={},
        callbacks=mock_callbacks
    )
    conn.close()

    # Should fail gracefully
    assert success is False
    mock_callbacks.on_error.assert_called()


@patch("extractors._shared.carving.worker.run_carving_extraction")
@patch("extractors._shared.carving.extractor.discover_tools")
def test_extraction_handles_exceptions(mock_discover, mock_carving, extractor, temp_workspace, mock_evidence_fs, mock_callbacks):
    """Test extraction handles unexpected exceptions."""
    workspace, _ = temp_workspace
    output_dir = workspace / "output"
    output_dir.mkdir()

    mock_discover.return_value = {"foremost": Mock(available=True, path=Path("/usr/bin/foremost"), version="1.5.7")}

    # Simulate exception
    mock_carving.side_effect = RuntimeError("Unexpected error")

    success = extractor.run_extraction(
        evidence_fs=mock_evidence_fs,
        output_dir=output_dir,
        config={},
        callbacks=mock_callbacks
    )

    assert success is False
    mock_callbacks.on_error.assert_called()


# ========== Forensic Integrity Tests ==========

@patch("extractors._shared.carving.worker.run_carving_extraction")
@patch("extractors._shared.carving.extractor.discover_tools")
def test_extraction_deterministic_run_id(mock_discover, mock_carving, extractor, temp_workspace, mock_evidence_fs, mock_callbacks):
    """Test extraction generates consistent run IDs."""
    workspace, _ = temp_workspace
    output_dir1 = workspace / "run1"
    output_dir2 = workspace / "run2"
    output_dir1.mkdir()
    output_dir2.mkdir()

    mock_discover.return_value = {"foremost": Mock(available=True, path=Path("/usr/bin/foremost"), version="1.5.7")}
    mock_carving.return_value = _make_run_result([])

    # Run twice
    extractor.run_extraction(mock_evidence_fs, output_dir1, {}, mock_callbacks)
    extractor.run_extraction(mock_evidence_fs, output_dir2, {}, mock_callbacks)

    # Check run_ids are different (timestamp-based)
    import json
    with open(output_dir1 / "manifest.json") as f:
        run_id1 = json.load(f)["run_id"]
    with open(output_dir2 / "manifest.json") as f:
        run_id2 = json.load(f)["run_id"]

    assert run_id1 != run_id2  # Different runs should have different IDs
    assert run_id1.startswith("20")  # Timestamp format YYYYMMDD_HHMM_xxxx


@patch("extractors._shared.carving.worker.run_carving_extraction")
@patch("extractors._shared.carving.extractor.discover_tools")
def test_config_file_types_override(mock_discover, mock_carving, extractor, temp_workspace, mock_evidence_fs, mock_callbacks):
    """Test configuration can override default file types."""
    workspace, _ = temp_workspace
    output_dir = workspace / "output"
    output_dir.mkdir()

    mock_discover.return_value = {"foremost": Mock(available=True, path=Path("/usr/bin/foremost"), version="1.5.7")}
    mock_carving.return_value = _make_run_result([])

    config = {"file_types": ["jpg", "png"]}  # Only JPEG and PNG

    extractor.run_extraction(
        evidence_fs=mock_evidence_fs,
        output_dir=output_dir,
        config=config,
        callbacks=mock_callbacks
    )

    # Verify config was passed through
    call_args = mock_carving.call_args
    assert call_args is not None
    assert call_args.kwargs["file_types"] == ["jpg", "png"]


# ========== Integration Tests ==========

@patch("extractors._shared.carving.worker.run_carving_extraction")
@patch("extractors._shared.carving.extractor.discover_tools")
def test_full_workflow_e2e(mock_discover, mock_carving, extractor, temp_workspace, mock_evidence_fs, mock_callbacks):
    """Test complete extraction → ingestion workflow."""
    workspace, evidence_db = temp_workspace
    output_dir = workspace / "output"
    output_dir.mkdir()

    mock_discover.return_value = {"foremost": Mock(available=True, path=Path("/usr/bin/foremost"), version="1.5.7")}

    # Create fake carved files
    carved_dir = output_dir / "carved" / "jpg"
    carved_dir.mkdir(parents=True)
    img1 = carved_dir / "image1.jpg"
    img2 = carved_dir / "image2.jpg"
    img1.write_text("fake image 1")
    img2.write_text("fake image 2")

    mock_carving.return_value = _make_run_result([img1, img2])

    # Run extraction
    success = extractor.run_extraction(
        evidence_fs=mock_evidence_fs,
        output_dir=output_dir,
        config={},
        callbacks=mock_callbacks
    )
    assert success is True

    with patch("extractors._shared.carving.ingestion.ParallelImageProcessor") as mock_proc:
        from extractors._shared.carving.processor import ImageProcessResult
        mock_instance = Mock()
        mock_proc.return_value = mock_instance
        mock_instance.process_images.return_value = [
            ImageProcessResult(
                path=carved_dir / "image1.jpg",
                rel_path="carved/jpg/image1.jpg",
                filename="image1.jpg",
                md5="hash1",
                sha256="sha1",
                phash="phash1",
                exif_json="{}",
            ),
            ImageProcessResult(
                path=carved_dir / "image2.jpg",
                rel_path="carved/jpg/image2.jpg",
                filename="image2.jpg",
                md5="hash2",
                sha256="sha2",
                phash="phash2",
                exif_json="{}",
            ),
        ]

        # Run ingestion
        conn = sqlite3.connect(evidence_db)
        success = extractor.run_ingestion(
            output_dir=output_dir,
            evidence_conn=conn,
            evidence_id=1,
            config={},
            callbacks=mock_callbacks
        )
        conn.close()
        assert success is True

    # Verify final state - check image_discoveries table for provenance
    conn = sqlite3.connect(evidence_db)
    cursor = conn.execute("SELECT COUNT(*) FROM images")
    image_count = cursor.fetchone()[0]
    cursor = conn.execute("SELECT COUNT(*) FROM image_discoveries WHERE discovered_by = 'image_carving'")
    discovery_count = cursor.fetchone()[0]
    conn.close()

    assert image_count == 2
    assert discovery_count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
