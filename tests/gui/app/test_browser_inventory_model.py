"""
Tests for BrowserInventoryModel.

Tests database interactions, filtering, and data loading for the browser
inventory table.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from PySide6.QtCore import Qt

from app.features.browser_inventory.inventory.model import BrowserInventoryModel
from core.database import insert_browser_inventory
from core.database import DatabaseManager


@pytest.fixture
def test_case(tmp_path):
    """Create test case with evidence database."""
    case_folder = tmp_path / "case"
    case_folder.mkdir()
    case_db_path = case_folder / "test_surfsifter.sqlite"

    # Create case database
    db_manager = DatabaseManager(case_folder, case_db_path=case_db_path)
    case_conn = db_manager.get_case_conn()

    # Insert case and evidence
    with case_conn:
        case_conn.execute(
            "INSERT INTO cases(case_id, title, created_at_utc) VALUES (?, ?, ?)",
            ("CASE-001", "Test Case", "2024-01-01T00:00:00"),
        )

        # Insert evidence
        cursor = case_conn.execute(
            "INSERT INTO evidences(case_id, label, source_path, added_at_utc) VALUES (?, ?, ?, ?)",
            (1, "test_evidence", "/path/to/evidence.E01", "2024-01-01T00:00:00"),
        )

    evidence_id = int(cursor.lastrowid)

    # Get evidence connection (this applies migrations including browser_cache_inventory)
    evidence_conn = db_manager.get_evidence_conn(evidence_id, "test_evidence")

    # Also get the path for the model to use
    evidence_db_path = db_manager.evidence_db_path(evidence_id, "test_evidence")

    return {
        "case_folder": case_folder,
        "case_db_path": case_db_path,
        "db_manager": db_manager,
        "evidence_id": evidence_id,
        "evidence_conn": evidence_conn,  # For insert_browser_inventory(conn=)
        "evidence_db_path": evidence_db_path,  # For BrowserInventoryModel()
    }


class TestBrowserInventoryModel:
    """Tests for BrowserInventoryModel class."""

    def test_model_initialization(self, test_case):
        """Model should initialize with empty data."""
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        assert model.rowCount() == 0
        assert model.columnCount() == 8

    def test_load_single_inventory_entry(self, test_case):
        """Model should load single inventory entry."""
        # Insert inventory entry
        inventory_id = insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="Default_History",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\AppData\\Local\\Google\\Chrome\\User Data\\Default\\History",
            profile="Default",
            partition_index=2,
            fs_type="ntfs",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Create model
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        assert model.rowCount() == 1

        # Check data
        browser_idx = model.index(0, model.COL_BROWSER)
        assert model.data(browser_idx, Qt.DisplayRole) == "chrome"

        type_idx = model.index(0, model.COL_TYPE)
        assert model.data(type_idx, Qt.DisplayRole) == "history"

        profile_idx = model.index(0, model.COL_PROFILE)
        assert model.data(profile_idx, Qt.DisplayRole) == "Default"

    def test_load_multiple_inventory_entries(self, test_case):
        """Model should load multiple inventory entries."""
        # Insert Chrome history
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="Default_History",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\History",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Insert Firefox history
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="firefox",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="places.sqlite",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Firefox\\places.sqlite",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Insert Chrome cache
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="cache_simple",
            run_id="20241201T120000_abc123",
            extracted_path="f_000001",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\Cache\\f_000001",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Create model
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        assert model.rowCount() == 3

    def test_filter_by_browser(self, test_case):
        """Model should filter by browser."""
        # Insert Chrome entry
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="Default_History",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\History",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Insert Firefox entry
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="firefox",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="places.sqlite",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Firefox\\places.sqlite",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Create model
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        # Initially shows both
        assert model.rowCount() == 2

        # Filter by chrome
        model.set_filters(browser="chrome")
        assert model.rowCount() == 1

        browser_idx = model.index(0, model.COL_BROWSER)
        assert model.data(browser_idx, Qt.DisplayRole) == "chrome"

    def test_filter_by_artifact_type(self, test_case):
        """Model should filter by artifact type."""
        # Insert history
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="Default_History",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\History",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Insert cache
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="cache_simple",
            run_id="20241201T120000_abc123",
            extracted_path="f_000001",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\Cache\\f_000001",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Create model
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        # Initially shows both
        assert model.rowCount() == 2

        # Filter by history
        model.set_filters(artifact_type="history")
        assert model.rowCount() == 1

        type_idx = model.index(0, model.COL_TYPE)
        assert model.data(type_idx, Qt.DisplayRole) == "history"

    def test_filter_by_status(self, test_case):
        """Model should filter by ingestion status."""
        # Insert OK entry
        inventory_id_1 = insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="Default_History",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\History",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Update ingestion status to ok
        with sqlite3.connect(test_case["evidence_db_path"]) as conn:
            conn.execute(
                "UPDATE browser_cache_inventory SET ingestion_status = ? WHERE id = ?",
                ("ok", inventory_id_1)
            )

        # Insert pending entry
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="firefox",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="places.sqlite",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Firefox\\places.sqlite",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Create model
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        # Initially shows both
        assert model.rowCount() == 2

        # Filter by ok status
        model.set_filters(status="ok")
        assert model.rowCount() == 1

        browser_idx = model.index(0, model.COL_BROWSER)
        assert model.data(browser_idx, Qt.DisplayRole) == "chrome"

    def test_get_row_data(self, test_case):
        """Model should return full row data."""
        # Insert entry
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="Default_History",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\History",
            profile="Default",
            partition_index=2,
            fs_type="ntfs",
            file_size_bytes=524288,
            file_md5="abc123",
            file_sha256="def456",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Create model
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        # Get row data
        index = model.index(0, 0)
        row_data = model.get_row_data(index)

        assert row_data["browser"] == "chrome"
        assert row_data["artifact_type"] == "history"
        assert row_data["profile"] == "Default"
        assert row_data["logical_path"] == "C:\\Users\\test\\Chrome\\History"
        assert row_data["extraction_status"] == "ok"
        assert row_data["partition_index"] == 2
        assert row_data["fs_type"] == "ntfs"
        assert row_data["file_size_bytes"] == 524288
        assert row_data["file_md5"] == "abc123"
        assert row_data["file_sha256"] == "def456"

    def test_status_icons(self, test_case):
        """Model should display status icons."""
        # Insert entry with OK status
        inventory_id = insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="Default_History",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\History",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Create model
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        # Check extraction status display includes icon
        extraction_idx = model.index(0, model.COL_EXTRACTION_STATUS)
        extraction_text = model.data(extraction_idx, Qt.DisplayRole)
        assert "✓" in extraction_text
        assert "ok" in extraction_text

        # Check ingestion status (pending by default)
        ingestion_idx = model.index(0, model.COL_INGESTION_STATUS)
        ingestion_text = model.data(ingestion_idx, Qt.DisplayRole)
        assert "⊙" in ingestion_text or "pending" in ingestion_text

    def test_refresh(self, test_case):
        """Model should refresh data from database."""
        # Create model with no data
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        assert model.rowCount() == 0

        # Insert entry directly to database
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="Default_History",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\History",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Refresh model
        model.refresh()

        assert model.rowCount() == 1

    def test_get_available_browsers(self, test_case):
        """Model should return list of unique browsers."""
        # Insert Chrome entry
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="Default_History",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\History",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Insert Firefox entry
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="firefox",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="places.sqlite",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Firefox\\places.sqlite",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Insert another Chrome entry (different type)
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="cache_simple",
            run_id="20241201T120000_abc123",
            extracted_path="f_000001",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\Cache\\f_000001",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Create model
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        browsers = model.get_available_browsers()

        # Should return unique browsers only
        assert len(browsers) == 2
        assert "chrome" in browsers
        assert "firefox" in browsers

    def test_get_available_types(self, test_case):
        """Model should return list of unique artifact types."""
        # Insert history
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="history",
            run_id="20241201T120000_abc123",
            extracted_path="Default_History",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\History",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Insert cache
        insert_browser_inventory(conn=
            test_case["evidence_conn"],
            evidence_id=test_case["evidence_id"],
            browser="chrome",
            artifact_type="cache_simple",
            run_id="20241201T120000_abc123",
            extracted_path="f_000001",
            extraction_status="ok",
            extraction_timestamp_utc="2024-12-01T12:00:00Z",
            logical_path="C:\\Users\\test\\Chrome\\Cache\\f_000001",
        )
        test_case["evidence_conn"].commit()
        test_case["evidence_conn"].commit()

        # Create model
        model = BrowserInventoryModel(
            str(test_case["case_folder"]),
            test_case["evidence_id"],
            test_case["case_db_path"],
        )

        types = model.get_available_types()

        assert len(types) == 2
        assert "history" in types
        assert "cache_simple" in types
