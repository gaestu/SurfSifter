"""
Tests for IE WebCache downstream orchestration.

Verifies:
- BaseExtractor.downstream_extractors defaults to empty list
- IEWebCacheExtractor.downstream_extractors lists expected ingest-only extractors
- Orchestrator triggers downstream ingestion after extraction succeeds
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List, Optional
from unittest.mock import MagicMock, patch, call

import pytest

from extractors.base import BaseExtractor, ExtractorMetadata
from extractors.callbacks import ExtractorCallbacks


# ---------------------------------------------------------------------------
# Stub extractors for testing
# ---------------------------------------------------------------------------

class StubExtractor(BaseExtractor):
    """Minimal concrete extractor with default downstream_extractors."""

    @property
    def metadata(self):
        return ExtractorMetadata(
            name="stub",
            display_name="Stub",
            description="test",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs):
        return True, ""

    def can_run_ingestion(self, output_dir):
        return True, ""

    def get_config_widget(self, parent):
        return None

    def get_status_widget(self, parent, output_dir, evidence_conn, evidence_id):
        return None

    def get_output_dir(self, case_root, evidence_label, config=None):
        return case_root / "evidences" / evidence_label / "stub"

    def run_extraction(self, evidence_fs, output_dir, config, callbacks):
        return True

    def run_ingestion(self, output_dir, evidence_conn, evidence_id, config, callbacks):
        return {}


class ExtractOnlyExtractor(BaseExtractor):
    """Extract-only extractor with downstream dependencies."""

    @property
    def metadata(self):
        return ExtractorMetadata(
            name="parent_ext",
            display_name="Parent",
            description="extract only",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=False,
        )

    @property
    def downstream_extractors(self) -> List[str]:
        return ["child_a", "child_b"]

    def can_run_extraction(self, evidence_fs):
        return True, ""

    def can_run_ingestion(self, output_dir):
        return False, "extract-only"

    def get_config_widget(self, parent):
        return None

    def get_status_widget(self, parent, output_dir, evidence_conn, evidence_id):
        return None

    def get_output_dir(self, case_root, evidence_label, config=None):
        return case_root / "evidences" / evidence_label / "parent_ext"

    def run_extraction(self, evidence_fs, output_dir, config, callbacks):
        return True

    def run_ingestion(self, output_dir, evidence_conn, evidence_id, config, callbacks):
        return {}


class IngestOnlyExtractor(BaseExtractor):
    """Ingest-only downstream extractor."""

    def __init__(self, name: str):
        self._name = name

    @property
    def metadata(self):
        return ExtractorMetadata(
            name=self._name,
            display_name=self._name,
            description="ingest only",
            category="browser",
            requires_tools=[],
            can_extract=False,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs):
        return False, "ingest-only"

    def can_run_ingestion(self, output_dir):
        return True, ""

    def get_config_widget(self, parent):
        return None

    def get_status_widget(self, parent, output_dir, evidence_conn, evidence_id):
        return None

    def get_output_dir(self, case_root, evidence_label, config=None):
        return case_root / "evidences" / evidence_label / self._name

    def run_extraction(self, evidence_fs, output_dir, config, callbacks):
        return False

    def run_ingestion(self, output_dir, evidence_conn, evidence_id, config, callbacks):
        return {"records": 42}


# ---------------------------------------------------------------------------
# Tests: downstream_extractors property
# ---------------------------------------------------------------------------

class TestDownstreamExtractorsProperty:
    """Test the downstream_extractors property on BaseExtractor subclasses."""

    def test_default_is_empty_list(self):
        ext = StubExtractor()
        assert ext.downstream_extractors == []

    def test_override_returns_names(self):
        ext = ExtractOnlyExtractor()
        assert ext.downstream_extractors == ["child_a", "child_b"]


class TestIEWebCacheDownstream:
    """Test IEWebCacheExtractor.downstream_extractors."""

    def test_webcache_downstream_list(self):
        from extractors.browser.ie_legacy.webcache.extractor import IEWebCacheExtractor

        ext = IEWebCacheExtractor()
        expected = ["ie_history", "ie_cookies", "ie_downloads", "ie_cache_metadata"]
        assert ext.downstream_extractors == expected

    def test_webcache_is_extract_only(self):
        from extractors.browser.ie_legacy.webcache.extractor import IEWebCacheExtractor

        ext = IEWebCacheExtractor()
        assert ext.metadata.can_extract is True
        assert ext.metadata.can_ingest is False


# ---------------------------------------------------------------------------
# Tests: Orchestrator downstream triggering logic
# ---------------------------------------------------------------------------

class TestOrchestratorDownstreamTriggering:
    """Test that run_extraction_pipeline triggers downstream ingestion."""

    def test_downstream_ingestion_triggered(self, tmp_path):
        """Downstream ingesters run after extract-only parent succeeds."""
        from core.extraction_orchestrator import (
            run_extraction_pipeline,
            BridgeCallbacks,
            ExtractorFailure,
        )

        parent = ExtractOnlyExtractor()
        child_a = IngestOnlyExtractor("child_a")
        child_b = IngestOnlyExtractor("child_b")

        # Spy on ingestion
        child_a.run_ingestion = MagicMock(return_value={"records": 10})
        child_b.run_ingestion = MagicMock(return_value={"records": 20})

        # Mock registry
        mock_registry = MagicMock()
        mock_registry.get_all.return_value = [parent]
        mock_registry.get.side_effect = lambda name: {
            "child_a": child_a,
            "child_b": child_b,
        }.get(name)

        # Mock DB connections
        case_conn = MagicMock()
        case_conn.execute.return_value.fetchone.return_value = {"label": "test-evidence"}
        case_conn.row_factory = None

        evidence_conn = MagicMock()

        # Create case structure
        case_root = tmp_path / "case"
        case_root.mkdir()

        with patch("core.extraction_orchestrator.ExtractorRegistry", return_value=mock_registry):
            with patch("core.extraction_orchestrator.slugify_label", return_value="test-evidence"):
                summary = run_extraction_pipeline(
                    fs=MagicMock(),
                    case_conn=case_conn,
                    evidence_conn=evidence_conn,
                    evidence_id=1,
                    case_root=case_root,
                    selected_extractors=["parent_ext"],
                )

        # Both downstream ingesters should have been called
        child_a.run_ingestion.assert_called_once()
        child_b.run_ingestion.assert_called_once()

        # They should receive the parent's output dir
        parent_output = case_root / "evidences" / "test-evidence" / "parent_ext"
        assert child_a.run_ingestion.call_args[0][0] == parent_output
        assert child_b.run_ingestion.call_args[0][0] == parent_output

    def test_downstream_not_triggered_when_no_downstream(self, tmp_path):
        """Extractor with empty downstream_extractors triggers nothing extra."""
        from core.extraction_orchestrator import run_extraction_pipeline

        ext = StubExtractor()
        ext.run_ingestion = MagicMock(return_value={})

        mock_registry = MagicMock()
        mock_registry.get_all.return_value = [ext]
        mock_registry.get.return_value = None

        case_conn = MagicMock()
        case_conn.execute.return_value.fetchone.return_value = {"label": "test"}
        case_conn.row_factory = None

        case_root = tmp_path / "case"
        case_root.mkdir()

        with patch("core.extraction_orchestrator.ExtractorRegistry", return_value=mock_registry):
            with patch("core.extraction_orchestrator.slugify_label", return_value="test"):
                run_extraction_pipeline(
                    fs=MagicMock(),
                    case_conn=case_conn,
                    evidence_conn=MagicMock(),
                    evidence_id=1,
                    case_root=case_root,
                    selected_extractors=["stub"],
                )

        # Registry.get should not be called for downstream lookup
        mock_registry.get.assert_not_called()

    def test_downstream_missing_extractor_logged(self, tmp_path):
        """Missing downstream extractor is logged but doesn't fail pipeline."""
        from core.extraction_orchestrator import run_extraction_pipeline

        parent = ExtractOnlyExtractor()

        mock_registry = MagicMock()
        mock_registry.get_all.return_value = [parent]
        mock_registry.get.return_value = None  # downstream not found

        case_conn = MagicMock()
        case_conn.execute.return_value.fetchone.return_value = {"label": "test"}
        case_conn.row_factory = None

        case_root = tmp_path / "case"
        case_root.mkdir()

        with patch("core.extraction_orchestrator.ExtractorRegistry", return_value=mock_registry):
            with patch("core.extraction_orchestrator.slugify_label", return_value="test"):
                summary = run_extraction_pipeline(
                    fs=MagicMock(),
                    case_conn=case_conn,
                    evidence_conn=MagicMock(),
                    evidence_id=1,
                    case_root=case_root,
                    selected_extractors=["parent_ext"],
                )

        # Pipeline should succeed (no failures from missing downstream)
        assert len(summary.failed_extractors) == 0

    def test_downstream_ingestion_failure_recorded(self, tmp_path):
        """Failed downstream ingestion is recorded in failed_extractors."""
        from core.extraction_orchestrator import run_extraction_pipeline

        parent = ExtractOnlyExtractor()
        child_a = IngestOnlyExtractor("child_a")
        child_a.run_ingestion = MagicMock(side_effect=RuntimeError("parse error"))

        mock_registry = MagicMock()
        mock_registry.get_all.return_value = [parent]
        mock_registry.get.side_effect = lambda name: {
            "child_a": child_a,
            "child_b": None,  # child_b not found
        }.get(name)

        case_conn = MagicMock()
        case_conn.execute.return_value.fetchone.return_value = {"label": "test"}
        case_conn.row_factory = None

        case_root = tmp_path / "case"
        case_root.mkdir()

        with patch("core.extraction_orchestrator.ExtractorRegistry", return_value=mock_registry):
            with patch("core.extraction_orchestrator.slugify_label", return_value="test"):
                with pytest.raises(RuntimeError, match="failure"):
                    run_extraction_pipeline(
                        fs=MagicMock(),
                        case_conn=case_conn,
                        evidence_conn=MagicMock(),
                        evidence_id=1,
                        case_root=case_root,
                        selected_extractors=["parent_ext"],
                    )

    def test_downstream_cancelled(self, tmp_path):
        """Downstream ingestion respects cancellation."""
        from core.extraction_orchestrator import run_extraction_pipeline

        parent = ExtractOnlyExtractor()
        child_a = IngestOnlyExtractor("child_a")
        child_b = IngestOnlyExtractor("child_b")
        child_a.run_ingestion = MagicMock(return_value={})
        child_b.run_ingestion = MagicMock(return_value={})

        mock_registry = MagicMock()
        mock_registry.get_all.return_value = [parent]
        mock_registry.get.side_effect = lambda name: {
            "child_a": child_a,
            "child_b": child_b,
        }.get(name)

        case_conn = MagicMock()
        case_conn.execute.return_value.fetchone.return_value = {"label": "test"}
        case_conn.row_factory = None

        case_root = tmp_path / "case"
        case_root.mkdir()

        # Cancel after extraction completes but before downstream runs
        call_count = 0

        def cancel_after_extraction():
            nonlocal call_count
            call_count += 1
            # First call is in the main loop, second is in downstream loop
            return call_count > 1

        with patch("core.extraction_orchestrator.ExtractorRegistry", return_value=mock_registry):
            with patch("core.extraction_orchestrator.slugify_label", return_value="test"):
                run_extraction_pipeline(
                    fs=MagicMock(),
                    case_conn=case_conn,
                    evidence_conn=MagicMock(),
                    evidence_id=1,
                    case_root=case_root,
                    selected_extractors=["parent_ext"],
                    cancellation_check=cancel_after_extraction,
                )

        # At least one child should not have run due to cancellation
        total_calls = child_a.run_ingestion.call_count + child_b.run_ingestion.call_count
        assert total_calls < 2
