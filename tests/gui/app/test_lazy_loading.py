"""
Tests for lazy loading and background case loading.

Phase 2 + Phase 3 implementation tests.
"""
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QTabWidget


class TestLazyLoadingUrlsTab:
    """Tests for UrlsTab lazy loading functionality."""

    def test_set_evidence_deferred_does_not_load_immediately(self, qtbot):
        """Verify that set_evidence with defer_load=True doesn't load data immediately."""
        from app.features.urls import UrlsTab

        tab = UrlsTab()
        qtbot.addWidget(tab)

        # Mock the model to track calls
        tab.model = MagicMock()
        tab.group_model = MagicMock()

        # Set evidence with deferred loading
        tab.set_case_data(MagicMock(), defer_load=True)
        tab.set_evidence(1, defer_load=True)

        # Model should NOT have set_evidence called yet
        tab.model.set_evidence.assert_not_called()
        tab.group_model.set_evidence.assert_not_called()

        # But the tab should track that loading is pending
        assert tab._load_pending is True
        assert tab._data_loaded is False

    def test_set_evidence_immediate_loads_immediately(self, qtbot):
        """Verify that set_evidence with defer_load=False loads data immediately."""
        from app.features.urls import UrlsTab

        tab = UrlsTab()
        qtbot.addWidget(tab)

        # Create proper mock for model that returns expected values
        mock_model = MagicMock()
        mock_model.total_count.return_value = 0
        mock_model.page = 0
        mock_model.page_size = 100
        tab.model = mock_model

        mock_group_model = MagicMock()
        tab.group_model = mock_group_model

        # Avoid starting background workers during unit test
        tab._populate_filters = MagicMock()

        # Set evidence without deferred loading
        tab.set_evidence(1, defer_load=False)

        # Model should have set_evidence called
        mock_model.set_evidence.assert_called_once_with(1)


class TestLazyLoadingImagesTab:
    """Tests for ImagesTab lazy loading functionality."""

    def test_set_evidence_deferred_does_not_load_immediately(self, qtbot):
        """Verify that set_evidence with defer_load=True doesn't load data immediately."""
        from app.features.images import ImagesTab

        tab = ImagesTab()
        qtbot.addWidget(tab)

        # Mock the model
        tab.model = MagicMock()
        tab.cluster_model = MagicMock()

        # Set evidence with deferred loading
        tab.set_case_data(MagicMock(), defer_load=True)
        tab.set_evidence(1, defer_load=True)

        # Model should NOT have set_evidence called yet
        tab.model.set_evidence.assert_not_called()

        # But the tab should track that loading is pending
        assert tab._load_pending is True
        assert tab._data_loaded is False


class TestTabChangeTrigger:
    """Tests for tab change triggering lazy loading."""

    def test_on_evidence_subtab_changed_triggers_deferred_load(self, qtbot):
        """Verify that switching tabs triggers deferred loading for pending tabs."""
        from app.features.urls import UrlsTab

        # Create a tab widget simulating evidence tabs
        tab_widget = QTabWidget()
        qtbot.addWidget(tab_widget)

        # Create a tab with pending deferred load
        urls_tab = UrlsTab()
        urls_tab._load_pending = True
        urls_tab._data_loaded = False
        urls_tab._perform_deferred_load = MagicMock()

        tab_widget.addTab(urls_tab, "URLs")

        # Simulate the tab change handler logic from MainWindow
        def on_subtab_changed(index):
            widget = tab_widget.widget(index)
            if widget and hasattr(widget, '_perform_deferred_load') and hasattr(widget, '_load_pending'):
                if widget._load_pending and not getattr(widget, '_data_loaded', False):
                    widget._perform_deferred_load()

        # Trigger tab change
        on_subtab_changed(0)

        # Should have triggered deferred load
        urls_tab._perform_deferred_load.assert_called_once()

    def test_on_evidence_subtab_changed_does_not_retrigger_loaded_tab(self, qtbot):
        """Verify that already-loaded tabs don't retrigger loading."""
        from app.features.urls import UrlsTab

        tab_widget = QTabWidget()
        qtbot.addWidget(tab_widget)

        # Create a tab that was already loaded
        urls_tab = UrlsTab()
        urls_tab._load_pending = False
        urls_tab._data_loaded = True
        urls_tab._perform_deferred_load = MagicMock()

        tab_widget.addTab(urls_tab, "URLs")

        def on_subtab_changed(index):
            widget = tab_widget.widget(index)
            if widget and hasattr(widget, '_perform_deferred_load') and hasattr(widget, '_load_pending'):
                if widget._load_pending and not getattr(widget, '_data_loaded', False):
                    widget._perform_deferred_load()

        # Trigger tab change
        on_subtab_changed(0)

        # Should NOT have triggered deferred load since already loaded
        urls_tab._perform_deferred_load.assert_not_called()


class TestCaseLoadTask:
    """Tests for CaseLoadTask background worker."""

    def test_case_load_task_config_creation(self):
        """Verify CaseLoadTaskConfig can be created."""
        from app.services.workers import CaseLoadTaskConfig

        config = CaseLoadTaskConfig(
            case_path=Path("/tmp/test_case"),
            db_path=None
        )

        assert config.case_path == Path("/tmp/test_case")
        assert config.db_path is None

    def test_case_load_result_creation(self):
        """Verify CaseLoadResult can be created."""
        from app.services.workers import CaseLoadResult

        result = CaseLoadResult(
            case_path=Path("/tmp/test_case"),
            db_path=Path("/tmp/test_case/case.sqlite"),
            db_manager=None,
            case_metadata={"id": 1, "title": "Test"},
            evidences=[{"id": 1, "label": "E01"}],
            error=None
        )

        assert result.case_path == Path("/tmp/test_case")
        assert result.error is None
        assert len(result.evidences) == 1

    def test_case_load_result_with_error(self):
        """Verify CaseLoadResult can represent errors."""
        from app.services.workers import CaseLoadResult

        result = CaseLoadResult(
            case_path=Path("/tmp/test_case"),
            db_path=Path(),
            db_manager=None,
            case_metadata={},
            evidences=[],
            error="Database not found"
        )

        assert result.error == "Database not found"
        assert result.case_metadata == {}
