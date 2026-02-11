"""
Tests for BrowserHistoryTableModel.

Tests the Qt model for displaying browser_history table data.
"""
import json
import pytest
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

from PySide6.QtCore import Qt, QModelIndex

from app.features.browser_inventory.history.model import BrowserHistoryTableModel


class TestBrowserHistoryModelInit:
    """Test model initialization."""

    def test_model_creation(self, tmp_path, qtbot):
        """Test model can be created."""
        db_manager = MagicMock()
        model = BrowserHistoryTableModel(
            db_manager=db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        assert model is not None
        assert model.evidence_id == 1

    def test_model_columns(self, tmp_path, qtbot):
        """Test model has correct columns."""
        db_manager = MagicMock()
        model = BrowserHistoryTableModel(
            db_manager=db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        # Check column count (11 columns as of  with forensic columns)
        assert model.columnCount() == 11
        # Check headers
        assert model.HEADERS[0] == "URL"
        assert model.HEADERS[1] == "Title"
        assert model.HEADERS[2] == "Visit Time"
        assert model.HEADERS[7] == "Type"
        assert model.HEADERS[8] == "Duration"
        assert model.HEADERS[9] == "Hidden"
        assert model.HEADERS[10] == "Tags"


class TestBrowserHistoryModelLoad:
    """Test model data loading."""

    @pytest.fixture
    def evidence_db(self, tmp_path):
        """Create evidence database with browser_history table."""
        db_path = tmp_path / "test_evidence.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE browser_history (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                url TEXT,
                title TEXT,
                visit_count INTEGER DEFAULT 0,
                typed_count INTEGER DEFAULT 0,
                ts_utc TEXT,
                browser TEXT,
                profile TEXT,
                source_path TEXT,
                run_id TEXT,
                notes TEXT,
                last_visit_time_utc TEXT,
                discovered_by TEXT,
                tags TEXT,
                partition_index INTEGER,
                fs_type TEXT,
                logical_path TEXT,
                forensic_path TEXT,
                -- Forensic columns
                transition_type INTEGER,
                from_visit INTEGER,
                visit_duration_ms INTEGER,
                hidden INTEGER DEFAULT 0,
                chromium_visit_id INTEGER,
                chromium_url_id INTEGER
            )
        """)
        # Insert test data (transition_type: 0=link, 1=typed, 2=auto_bookmark, 3=auto_subframe, etc.)
        conn.execute("""
            INSERT INTO browser_history
            (evidence_id, url, title, visit_count, typed_count, ts_utc, browser, profile, transition_type, notes)
            VALUES (1, 'https://example.com', 'Example', 5, 2, '2025-01-15T10:30:00', 'firefox', 'default', 1,
                    '{"frecency": 1000}')
        """)
        conn.execute("""
            INSERT INTO browser_history
            (evidence_id, url, title, visit_count, typed_count, ts_utc, browser, profile, transition_type, notes)
            VALUES (1, 'https://test.com', 'Test Site', 3, 0, '2025-01-15T11:00:00', 'firefox', 'default', 0,
                    '{}')
        """)
        conn.execute("""
            INSERT INTO browser_history
            (evidence_id, url, title, visit_count, typed_count, ts_utc, browser, profile, transition_type, notes)
            VALUES (1, 'https://chrome.com', 'Chrome Site', 1, 1, '2025-01-15T12:00:00', 'chrome', 'Profile 1', 1,
                    '{}')
        """)
        conn.commit()
        conn.close()
        return db_path

    def test_load_data(self, evidence_db, qtbot):
        """Test loading data from database."""
        db_manager = MagicMock()
        db_manager.evidence_db_path.return_value = str(evidence_db)

        model = BrowserHistoryTableModel(
            db_manager=db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        model.load()

        # Should have 3 rows
        assert model.rowCount() == 3

    def test_load_with_browser_filter(self, evidence_db, qtbot):
        """Test loading data with browser filter."""
        db_manager = MagicMock()
        db_manager.evidence_db_path.return_value = str(evidence_db)

        model = BrowserHistoryTableModel(
            db_manager=db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        model.load(browser_filter="firefox")

        # Should have 2 Firefox rows
        assert model.rowCount() == 2

    def test_load_with_visit_type_filter(self, evidence_db, qtbot):
        """Test loading data with visit type filter."""
        db_manager = MagicMock()
        db_manager.evidence_db_path.return_value = str(evidence_db)

        model = BrowserHistoryTableModel(
            db_manager=db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        model.load(visit_type_filter="typed")

        # Should have 2 typed rows (1 Firefox + 1 Chrome)
        assert model.rowCount() == 2


class TestBrowserHistoryModelData:
    """Test model data retrieval."""

    @pytest.fixture
    def loaded_model(self, tmp_path, qtbot):
        """Create and load a model with test data."""
        db_path = tmp_path / "test_evidence.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE browser_history (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                url TEXT,
                title TEXT,
                visit_count INTEGER DEFAULT 0,
                typed_count INTEGER DEFAULT 0,
                ts_utc TEXT,
                browser TEXT,
                profile TEXT,
                source_path TEXT,
                run_id TEXT,
                notes TEXT,
                last_visit_time_utc TEXT,
                discovered_by TEXT,
                tags TEXT,
                partition_index INTEGER,
                fs_type TEXT,
                logical_path TEXT,
                forensic_path TEXT,
                -- Forensic columns
                transition_type INTEGER,
                from_visit INTEGER,
                visit_duration_ms INTEGER,
                hidden INTEGER DEFAULT 0,
                chromium_visit_id INTEGER,
                chromium_url_id INTEGER
            )
        """)
        conn.execute("""
            INSERT INTO browser_history
            (id, evidence_id, url, title, visit_count, typed_count, ts_utc, browser, profile, transition_type, notes)
            VALUES (1, 1, 'https://example.com/very/long/path/that/should/be/truncated/in/display',
                    'Example Site', 5, 2, '2025-01-15T10:30:00', 'firefox', 'default', 1,
                    '{"frecency": 1000, "hidden": false}')
        """)
        conn.commit()
        conn.close()

        db_manager = MagicMock()
        db_manager.evidence_db_path.return_value = str(db_path)

        model = BrowserHistoryTableModel(
            db_manager=db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        model.load()
        return model

    def test_data_display_role(self, loaded_model, qtbot):
        """Test data retrieval with DisplayRole."""
        # URL column (truncated)
        index = loaded_model.index(0, BrowserHistoryTableModel.COL_URL)
        url = loaded_model.data(index, Qt.DisplayRole)
        assert "example.com" in url

        # Title column
        index = loaded_model.index(0, BrowserHistoryTableModel.COL_TITLE)
        title = loaded_model.data(index, Qt.DisplayRole)
        assert title == "Example Site"

        # Browser column
        index = loaded_model.index(0, BrowserHistoryTableModel.COL_BROWSER)
        browser = loaded_model.data(index, Qt.DisplayRole)
        assert browser == "firefox"

        # Visit type column
        index = loaded_model.index(0, BrowserHistoryTableModel.COL_VISIT_TYPE)
        visit_type = loaded_model.data(index, Qt.DisplayRole)
        assert visit_type == "typed"

    def test_data_tooltip_role(self, loaded_model, qtbot):
        """Test tooltip data for visit type."""
        index = loaded_model.index(0, BrowserHistoryTableModel.COL_VISIT_TYPE)
        tooltip = loaded_model.data(index, Qt.ToolTipRole)
        # Tooltip now shows transition code and type from transition_type column
        assert tooltip is not None
        assert "Type: typed" in tooltip
        assert "Transition code: 1" in tooltip

    def test_get_row_data(self, loaded_model, qtbot):
        """Test get_row_data returns full record."""
        index = loaded_model.index(0, 0)
        row_data = loaded_model.get_row_data(index)

        assert row_data is not None
        assert row_data["url"] == "https://example.com/very/long/path/that/should/be/truncated/in/display"
        assert row_data["browser"] == "firefox"
        assert row_data["visit_count"] == 5

    def test_header_data(self, loaded_model, qtbot):
        """Test header labels."""
        assert loaded_model.headerData(0, Qt.Horizontal, Qt.DisplayRole) == "URL"
        assert loaded_model.headerData(1, Qt.Horizontal, Qt.DisplayRole) == "Title"
        assert loaded_model.headerData(7, Qt.Horizontal, Qt.DisplayRole) == "Type"


class TestBrowserHistoryModelFilters:
    """Test filter dropdown helpers."""

    @pytest.fixture
    def multi_browser_db(self, tmp_path):
        """Create database with multiple browsers."""
        db_path = tmp_path / "test_evidence.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE browser_history (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                url TEXT,
                title TEXT,
                visit_count INTEGER DEFAULT 0,
                typed_count INTEGER DEFAULT 0,
                ts_utc TEXT,
                browser TEXT,
                profile TEXT,
                source_path TEXT,
                run_id TEXT,
                notes TEXT,
                last_visit_time_utc TEXT,
                discovered_by TEXT,
                tags TEXT,
                partition_index INTEGER,
                fs_type TEXT,
                logical_path TEXT,
                forensic_path TEXT,
                -- Forensic columns
                transition_type INTEGER,
                from_visit INTEGER,
                visit_duration_ms INTEGER,
                hidden INTEGER DEFAULT 0,
                chromium_visit_id INTEGER,
                chromium_url_id INTEGER
            )
        """)
        # Multiple browsers and profiles
        conn.execute("""
            INSERT INTO browser_history (evidence_id, url, browser, profile)
            VALUES (1, 'https://test1.com', 'firefox', 'default')
        """)
        conn.execute("""
            INSERT INTO browser_history (evidence_id, url, browser, profile)
            VALUES (1, 'https://test2.com', 'firefox', 'work')
        """)
        conn.execute("""
            INSERT INTO browser_history (evidence_id, url, browser, profile)
            VALUES (1, 'https://test3.com', 'chrome', 'Profile 1')
        """)
        conn.execute("""
            INSERT INTO browser_history (evidence_id, url, browser, profile)
            VALUES (1, 'https://test4.com', 'chrome', 'Profile 2')
        """)
        conn.commit()
        conn.close()
        return db_path

    def test_get_browsers(self, multi_browser_db, qtbot):
        """Test getting distinct browsers."""
        db_manager = MagicMock()
        db_manager.evidence_db_path.return_value = str(multi_browser_db)

        model = BrowserHistoryTableModel(
            db_manager=db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )

        browsers = model.get_browsers()
        assert set(browsers) == {"chrome", "firefox"}

    def test_get_profiles(self, multi_browser_db, qtbot):
        """Test getting distinct profiles."""
        db_manager = MagicMock()
        db_manager.evidence_db_path.return_value = str(multi_browser_db)

        model = BrowserHistoryTableModel(
            db_manager=db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )

        profiles = model.get_profiles()
        assert len(profiles) == 4  # default, work, Profile 1, Profile 2


class TestBrowserHistoryModelStats:
    """Test statistics retrieval."""

    @pytest.fixture
    def stats_db(self, tmp_path):
        """Create database with typed entries for stats."""
        db_path = tmp_path / "test_evidence.sqlite"
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE browser_history (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                url TEXT,
                title TEXT,
                visit_count INTEGER DEFAULT 0,
                typed_count INTEGER DEFAULT 0,
                ts_utc TEXT,
                browser TEXT,
                profile TEXT,
                source_path TEXT,
                run_id TEXT,
                notes TEXT,
                last_visit_time_utc TEXT,
                discovered_by TEXT,
                tags TEXT,
                partition_index INTEGER,
                fs_type TEXT,
                logical_path TEXT,
                forensic_path TEXT,
                -- Forensic columns
                transition_type INTEGER,
                from_visit INTEGER,
                visit_duration_ms INTEGER,
                hidden INTEGER DEFAULT 0,
                chromium_visit_id INTEGER,
                chromium_url_id INTEGER
            )
        """)
        # Mix of typed and non-typed
        conn.execute("""
            INSERT INTO browser_history (evidence_id, url, typed_count, ts_utc)
            VALUES (1, 'https://typed1.com', 1, '2025-01-15T10:00:00')
        """)
        conn.execute("""
            INSERT INTO browser_history (evidence_id, url, typed_count, ts_utc)
            VALUES (1, 'https://typed2.com', 3, '2025-01-15T11:00:00')
        """)
        conn.execute("""
            INSERT INTO browser_history (evidence_id, url, typed_count, ts_utc)
            VALUES (1, 'https://link.com', 0, '2025-01-15T12:00:00')
        """)
        conn.commit()
        conn.close()
        return db_path

    def test_get_stats(self, stats_db, qtbot):
        """Test getting statistics."""
        db_manager = MagicMock()
        db_manager.evidence_db_path.return_value = str(stats_db)

        model = BrowserHistoryTableModel(
            db_manager=db_manager,
            evidence_id=1,
            evidence_label="test_evidence",
        )
        model.load()

        stats = model.get_stats()
        assert stats["total_visits"] == 3
        assert stats["unique_urls"] == 3
        assert stats["typed_count"] == 2  # 2 entries with typed_count > 0
