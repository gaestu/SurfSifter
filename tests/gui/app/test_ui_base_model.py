"""
Tests for base artifact table model.
"""

import pytest
import sqlite3
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

from PySide6.QtCore import QModelIndex, Qt

from app.common.qt_models.base_model import BaseArtifactTableModel
from core.database import DatabaseManager


class ConcreteTestModel(BaseArtifactTableModel):
    """Concrete implementation for testing."""

    COLUMNS = ["name", "value", "browser", "tags"]
    HEADERS = ["Name", "Value", "Browser", "Tags"]
    ARTIFACT_TYPE = "test_artifact"

    def _fetch_data(self, conn: sqlite3.Connection, **filters) -> List[Dict[str, Any]]:
        """Return mock data for testing."""
        data = [
            {"id": 1, "name": "item1", "value": "val1", "browser": "chrome"},
            {"id": 2, "name": "item2", "value": "val2", "browser": "firefox"},
            {"id": 3, "name": "item3", "value": "val3", "browser": "chrome"},
        ]

        # Apply browser filter
        browser = filters.get("browser")
        if browser:
            data = [d for d in data if d["browser"] == browser]

        return data


class TestBaseArtifactTableModel:
    """Tests for BaseArtifactTableModel."""

    @pytest.fixture
    def mock_db_manager(self, tmp_path):
        """Create mock database manager."""
        manager = MagicMock(spec=DatabaseManager)
        manager.evidence_db_path.return_value = tmp_path / "evidence_test.sqlite"
        return manager

    @pytest.fixture
    def model(self, mock_db_manager):
        """Create test model."""
        return ConcreteTestModel(
            db_manager=mock_db_manager,
            evidence_id=1,
            evidence_label="test-evidence",
            case_data=None,
            parent=None
        )

    def test_columns_and_headers_length_match(self, model):
        """COLUMNS and HEADERS must have same length."""
        assert len(model.COLUMNS) == len(model.HEADERS)

    def test_artifact_type_set(self, model):
        """ARTIFACT_TYPE should be defined."""
        assert model.ARTIFACT_TYPE == "test_artifact"

    def test_rowCount_empty(self, model):
        """Empty model has 0 rows."""
        assert model.rowCount() == 0

    def test_columnCount(self, model):
        """Column count matches HEADERS."""
        assert model.columnCount() == len(model.HEADERS)

    def test_headerData(self, model):
        """Headers return correct values."""
        assert model.headerData(0, Qt.Horizontal, Qt.DisplayRole) == "Name"
        assert model.headerData(1, Qt.Horizontal, Qt.DisplayRole) == "Value"
        assert model.headerData(2, Qt.Horizontal, Qt.DisplayRole) == "Browser"

    def test_load_populates_rows(self, model, mock_db_manager, tmp_path):
        """load() populates rows from _fetch_data."""
        # Create minimal database
        db_path = tmp_path / "evidence_test.sqlite"
        mock_db_manager.evidence_db_path.return_value = db_path

        with sqlite3.connect(db_path) as conn:
            # Just need schema_version for init
            conn.execute("CREATE TABLE schema_version (version INTEGER)")

        model.load()

        assert model.rowCount() == 3

    def test_load_with_filter(self, model, mock_db_manager, tmp_path):
        """load() passes filters to _fetch_data."""
        db_path = tmp_path / "evidence_test.sqlite"
        mock_db_manager.evidence_db_path.return_value = db_path

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE schema_version (version INTEGER)")

        model.load(browser="chrome")

        assert model.rowCount() == 2  # Only chrome items

    def test_data_display_role(self, model, mock_db_manager, tmp_path):
        """data() returns values for DisplayRole."""
        db_path = tmp_path / "evidence_test.sqlite"
        mock_db_manager.evidence_db_path.return_value = db_path

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE schema_version (version INTEGER)")

        model.load()

        # Test first row
        index = model.index(0, 0)  # Name column
        assert model.data(index, Qt.DisplayRole) == "item1"

        # Browser column capitalizes
        index = model.index(0, 2)  # Browser column
        assert model.data(index, Qt.DisplayRole) == "Chrome"

    def test_get_row_data(self, model, mock_db_manager, tmp_path):
        """get_row_data returns full row dict."""
        db_path = tmp_path / "evidence_test.sqlite"
        mock_db_manager.evidence_db_path.return_value = db_path

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE schema_version (version INTEGER)")

        model.load()

        index = model.index(0, 0)
        row_data = model.get_row_data(index)

        assert row_data["id"] == 1
        assert row_data["name"] == "item1"

    def test_get_row_id(self, model, mock_db_manager, tmp_path):
        """get_row_id returns ID for index."""
        db_path = tmp_path / "evidence_test.sqlite"
        mock_db_manager.evidence_db_path.return_value = db_path

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE schema_version (version INTEGER)")

        model.load()

        index = model.index(1, 0)
        assert model.get_row_id(index) == 2

    def test_get_selected_ids(self, model, mock_db_manager, tmp_path):
        """get_selected_ids returns unique IDs."""
        db_path = tmp_path / "evidence_test.sqlite"
        mock_db_manager.evidence_db_path.return_value = db_path

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE schema_version (version INTEGER)")

        model.load()

        # Select multiple columns from same rows
        indexes = [
            model.index(0, 0),
            model.index(0, 1),  # Same row
            model.index(2, 0),
        ]

        ids = model.get_selected_ids(indexes)
        assert ids == [1, 3]  # Unique IDs only

    def test_invalid_index_returns_none(self, model):
        """data() returns None for invalid index."""
        invalid_index = model.index(999, 0)
        assert model.data(invalid_index, Qt.DisplayRole) is None

    def test_refresh_reloads_with_same_filters(self, model, mock_db_manager, tmp_path):
        """refresh() reloads data with current filters."""
        db_path = tmp_path / "evidence_test.sqlite"
        mock_db_manager.evidence_db_path.return_value = db_path

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE schema_version (version INTEGER)")

        model.load(browser="firefox")
        assert model.rowCount() == 1

        model.refresh()
        assert model.rowCount() == 1  # Still filtered


class TestBaseModelTagIntegration:
    """Test tag integration with case_data."""

    @pytest.fixture
    def mock_case_data(self):
        """Create mock case data with tag lookup."""
        case_data = MagicMock()
        case_data.get_tag_strings_for_artifacts.return_value = {
            1: "important, reviewed",
            2: "suspicious",
        }
        return case_data

    @pytest.fixture
    def model_with_tags(self, tmp_path, mock_case_data):
        """Create model with tag support."""
        manager = MagicMock(spec=DatabaseManager)
        db_path = tmp_path / "evidence_test.sqlite"
        manager.evidence_db_path.return_value = db_path

        with sqlite3.connect(db_path) as conn:
            conn.execute("CREATE TABLE schema_version (version INTEGER)")

        return ConcreteTestModel(
            db_manager=manager,
            evidence_id=1,
            evidence_label="test-evidence",
            case_data=mock_case_data,
            parent=None
        )

    def test_tags_column_shows_tag_string(self, model_with_tags, mock_case_data):
        """Tags column displays tag strings from case_data."""
        model_with_tags.load()

        # Tag column (index 3)
        index = model_with_tags.index(0, 3)
        assert model_with_tags.data(index, Qt.DisplayRole) == "important, reviewed"

        index = model_with_tags.index(1, 3)
        assert model_with_tags.data(index, Qt.DisplayRole) == "suspicious"

        # No tags for row 3
        index = model_with_tags.index(2, 3)
        assert model_with_tags.data(index, Qt.DisplayRole) == ""
