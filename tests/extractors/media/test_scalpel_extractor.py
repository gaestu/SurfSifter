"""Unit tests for ScalpelExtractor."""

import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from extractors.media.scalpel import ScalpelExtractor
from extractors.callbacks import ExtractorCallbacks
from extractors._shared.carving.worker import CarvingRunResult


@pytest.fixture
def extractor():
    return ScalpelExtractor()


@pytest.fixture
def mock_callbacks():
    return Mock(spec=ExtractorCallbacks)


def _run_result(carved_files, input_source="/fake/evidence.E01", input_type="ewf"):
    return CarvingRunResult(
        carved_files=carved_files,
        stdout="",
        stderr="",
        returncode=0,
        command=["scalpel", "-o", "/tmp/out", input_source],
        input_source=input_source,
        input_type=input_type,
        audit_path=None,
    )


def test_metadata(extractor):
    meta = extractor.metadata
    assert meta.name == "scalpel"
    assert "scalpel" in meta.requires_tools
    assert meta.can_extract is True
    assert meta.can_ingest is True


@patch("extractors.media.scalpel.extractor.discover_tools")
def test_can_run_extraction_requires_tool(mock_discover, extractor):
    mock_discover.return_value = {"scalpel": Mock(available=False)}
    can_run, reason = extractor.can_run_extraction(Mock())
    assert can_run is False
    assert "Scalpel" in reason


@patch("extractors.media.scalpel.extractor.run_carving_extraction")
@patch("extractors.media.scalpel.extractor.discover_tools")
def test_run_extraction_creates_manifest(mock_discover, mock_run, extractor, tmp_path, mock_callbacks):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    carved_dir = output_dir / "carved"
    carved_dir.mkdir()
    img = carved_dir / "img.jpg"
    img.write_text("fake")

    mock_discover.return_value = {
        "scalpel": Mock(available=True, path=Path("/usr/bin/scalpel"), version="2.0")
    }
    mock_run.return_value = _run_result([img])

    success = extractor.run_extraction(
        evidence_fs=Mock(),
        output_dir=output_dir,
        config={},
        callbacks=mock_callbacks,
    )
    assert success is True
    manifest = output_dir / "manifest.json"
    assert manifest.exists()
    data = manifest.read_text()
    assert "scalpel" in data


@patch("extractors.media.scalpel.extractor.run_image_ingestion")
@patch("extractors.media.scalpel.extractor.validate_image_carving_manifest")
def test_run_ingestion_uses_discovered_by(mock_validate, mock_ingest, extractor, tmp_path, mock_callbacks):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    manifest = output_dir / "manifest.json"
    manifest.write_text(
        '{"schema_version":"1.0.0","run_id":"r1","tool":{"name":"scalpel"},'
        '"started_at":"2024-01-01T00:00:00Z","completed_at":"2024-01-01T00:00:00Z",'
        '"input":{"source":"/e01","source_type":"ewf","evidence_id":1,"context":{}},'
        '"output":{"root":".","carved_dir":".","manifest_path":""},'
        '"stats":{"carved_total":0},"carved_files":[]}'
    )
    mock_ingest.return_value = {"inserted": 0, "errors": 0, "total": 0}

    success = extractor.run_ingestion(
        output_dir=output_dir,
        evidence_conn=Mock(),
        evidence_id=1,
        config={},
        callbacks=mock_callbacks,
    )
    assert success is True
    mock_ingest.assert_called_once()
    assert mock_ingest.call_args.kwargs["discovered_by"] == "scalpel"
