"""
Tests for  Task 2: URL Discovery Extractor Integration

Tests the URL discovery extractor end-to-end:
- Rule filtering for url_discovery works
- bulk_extractor is called when url_discovery is selected
- bulk_extractor is skipped when url_discovery is not selected
- Regex URL scan works with url_discovery
"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from core.extraction_orchestrator import run_extraction_pipeline
from tests.fixtures.db import CaseContext

pytestmark = pytest.mark.integration


@patch("core.extraction_orchestrator.ExtractorRegistry")
def test_bulk_extractor_called_when_url_discovery_selected(mock_registry_cls, case_factory):
    """Test that bulk_extractor is called when url_discovery is selected."""
    # Setup mock registry and extractor
    mock_registry = mock_registry_cls.return_value
    mock_extractor = MagicMock()
    mock_extractor.metadata.name = "bulk_extractor"
    mock_extractor.metadata.can_extract = True
    mock_extractor.metadata.can_ingest = True
    mock_extractor.can_run_extraction.return_value = (True, "OK")
    mock_extractor.can_run_ingestion.return_value = (True, "OK")
    mock_registry.get_all.return_value = [mock_extractor]

    ctx: CaseContext = case_factory(
        case_id="TEST-001",
        title="Test Case",
        investigator="Tester",
        created_at="2024-01-01T00:00:00+00:00",
        evidence_label="EV-001",
        source_path="/path/to/evidence",
        added_at="2024-01-01T01:00:00+00:00",
    )

    evidence_conn = ctx.manager.get_evidence_conn(ctx.evidence_id, ctx.evidence_label)

    fs = MagicMock()
    fs.source_path = Path("/path/to/evidence")

    run_extraction_pipeline(
        fs,
        case_conn=ctx.case_conn,
        evidence_conn=evidence_conn,
        evidence_id=ctx.evidence_id,
        case_root=ctx.case_dir,
        selected_extractors=["url_discovery"],
    )

    assert mock_extractor.run_extraction.called, "bulk_extractor should be called when url_discovery is selected"
    evidence_conn.close()
    ctx.case_conn.close()


@patch("core.extraction_orchestrator.ExtractorRegistry")
def test_bulk_extractor_not_called_when_url_discovery_not_selected(mock_registry_cls, case_factory):
    """Test that bulk_extractor is NOT called when url_discovery is NOT selected."""
    # Setup mock registry and extractor
    mock_registry = mock_registry_cls.return_value
    mock_extractor = MagicMock()
    mock_extractor.metadata.name = "bulk_extractor"
    mock_extractor.metadata.can_extract = True
    mock_extractor.metadata.can_ingest = True
    mock_registry.get_all.return_value = [mock_extractor]

    ctx: CaseContext = case_factory(
        case_id="TEST-001",
        title="Test Case",
        investigator="Tester",
        created_at="2024-01-01T00:00:00+00:00",
        evidence_label="EV-001",
        source_path="/path/to/evidence",
        added_at="2024-01-01T01:00:00+00:00",
    )

    evidence_conn = ctx.manager.get_evidence_conn(ctx.evidence_id, ctx.evidence_label)

    fs = MagicMock()
    fs.source_path = Path("/path/to/evidence")

    run_extraction_pipeline(
        fs,
        case_conn=ctx.case_conn,
        evidence_conn=evidence_conn,
        evidence_id=ctx.evidence_id,
        case_root=ctx.case_dir,
        selected_extractors=["browser_history"],
    )

    assert not mock_extractor.run_extraction.called, "bulk_extractor should NOT be called when url_discovery is not selected"
    evidence_conn.close()
    ctx.case_conn.close()


@patch("core.extraction_orchestrator.ExtractorRegistry")
def test_bulk_extractor_called_when_no_filter(mock_registry_cls, case_factory):
    """Test that bulk_extractor is called when selected_extractors is None."""
    import tempfile
    from core.evidence_fs import MountedFS
    from core.database import DatabaseManager

    # Setup mock registry and extractor
    mock_registry = mock_registry_cls.return_value
    mock_extractor = MagicMock()
    mock_extractor.metadata.name = "bulk_extractor"
    mock_extractor.metadata.can_extract = True
    mock_extractor.metadata.can_ingest = True
    mock_extractor.can_run_extraction.return_value = (True, "OK")
    mock_extractor.can_run_ingestion.return_value = (True, "OK")
    mock_registry.get_all.return_value = [mock_extractor]

    ctx: CaseContext = case_factory(
        case_id="TEST-001",
        title="Test Case",
        investigator="Tester",
        created_at="2024-01-01T00:00:00+00:00",
        evidence_label="EV-001",
        source_path="/path/to/evidence",
        added_at="2024-01-01T01:00:00+00:00",
    )

    evidence_conn = ctx.manager.get_evidence_conn(ctx.evidence_id, ctx.evidence_label)

    fs = MagicMock()
    fs.source_path = Path("/path/to/evidence")

    run_extraction_pipeline(
        fs,
        case_conn=ctx.case_conn,
        evidence_conn=evidence_conn,
        evidence_id=ctx.evidence_id,
        case_root=ctx.case_dir,
        selected_extractors=None,
    )

    assert mock_extractor.run_extraction.called, "bulk_extractor should be called when no filter is applied"
    evidence_conn.close()
    ctx.case_conn.close()


@patch("core.extraction_orchestrator.ExtractorRegistry")
def test_bulk_extractor_not_called_when_filter_is_empty(mock_registry_cls, case_factory):
    """Test that empty selected_extractors means run nothing, not run all."""
    mock_registry = mock_registry_cls.return_value
    mock_extractor = MagicMock()
    mock_extractor.metadata.name = "bulk_extractor"
    mock_extractor.metadata.can_extract = True
    mock_extractor.metadata.can_ingest = True
    mock_registry.get_all.return_value = [mock_extractor]

    ctx: CaseContext = case_factory(
        case_id="TEST-001",
        title="Test Case",
        investigator="Tester",
        created_at="2024-01-01T00:00:00+00:00",
        evidence_label="EV-001",
        source_path="/path/to/evidence",
        added_at="2024-01-01T01:00:00+00:00",
    )

    evidence_conn = ctx.manager.get_evidence_conn(ctx.evidence_id, ctx.evidence_label)

    fs = MagicMock()
    fs.source_path = Path("/path/to/evidence")

    run_extraction_pipeline(
        fs,
        case_conn=ctx.case_conn,
        evidence_conn=evidence_conn,
        evidence_id=ctx.evidence_id,
        case_root=ctx.case_dir,
        selected_extractors=[],
    )

    assert not mock_extractor.run_extraction.called, "No extractor should run for empty filter"
    evidence_conn.close()
    ctx.case_conn.close()


@patch("core.extraction_orchestrator.ExtractorRegistry")
def test_extraction_failure_is_not_silent(mock_registry_cls, case_factory):
    """Pipeline should raise when an extractor fails, instead of swallowing errors."""
    mock_registry = mock_registry_cls.return_value
    mock_extractor = MagicMock()
    mock_extractor.metadata.name = "bulk_extractor"
    mock_extractor.metadata.can_extract = True
    mock_extractor.metadata.can_ingest = False
    mock_extractor.can_run_extraction.return_value = (True, "OK")
    mock_extractor.run_extraction.side_effect = RuntimeError("boom")
    mock_registry.get_all.return_value = [mock_extractor]

    ctx: CaseContext = case_factory(
        case_id="TEST-001",
        title="Test Case",
        investigator="Tester",
        created_at="2024-01-01T00:00:00+00:00",
        evidence_label="EV-001",
        source_path="/path/to/evidence",
        added_at="2024-01-01T01:00:00+00:00",
    )

    evidence_conn = ctx.manager.get_evidence_conn(ctx.evidence_id, ctx.evidence_label)
    fs = MagicMock()
    fs.source_path = Path("/path/to/evidence")

    with pytest.raises(RuntimeError, match="failure"):
        run_extraction_pipeline(
            fs,
            case_conn=ctx.case_conn,
            evidence_conn=evidence_conn,
            evidence_id=ctx.evidence_id,
            case_root=ctx.case_dir,
            selected_extractors=["bulk_extractor"],
        )

    evidence_conn.close()
    ctx.case_conn.close()
