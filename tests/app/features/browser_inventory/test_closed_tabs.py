"""Tests for Closed Tabs model and container registration."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from app.features.browser_inventory.sessions.closed_tabs_model import (
    ClosedTabsTableModel,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_evidence_db(db_path: Path, evidence_id: int = 1) -> Path:
    """Create a minimal evidence DB with closed_tabs table and seed data."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS closed_tabs (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            browser TEXT NOT NULL,
            profile TEXT,
            url TEXT NOT NULL,
            title TEXT,
            closed_at_utc TEXT,
            original_window_id INTEGER,
            original_tab_index INTEGER,
            run_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            discovered_by TEXT,
            partition_index INTEGER,
            fs_type TEXT,
            logical_path TEXT,
            forensic_path TEXT,
            tags TEXT,
            notes TEXT,
            created_at_utc TEXT
        )
    """)
    conn.execute("""
        INSERT INTO closed_tabs
            (evidence_id, browser, profile, url, title, closed_at_utc,
             original_window_id, original_tab_index, run_id, source_path)
        VALUES (?, 'safari', 'Yans', 'http://grandx.org/', 'grandx.org',
                '2023-10-04T19:30:33+00:00', 0, 0, 'run1', '/path/to/file')
    """, (evidence_id,))
    conn.execute("""
        INSERT INTO closed_tabs
            (evidence_id, browser, profile, url, title, closed_at_utc,
             original_window_id, original_tab_index, run_id, source_path)
        VALUES (?, 'safari', 'Yans', 'topsites://', 'Topsites',
                '2023-10-04T19:30:33+00:00', 0, 1, 'run1', '/path/to/file')
    """, (evidence_id,))
    conn.execute("""
        INSERT INTO closed_tabs
            (evidence_id, browser, profile, url, title, closed_at_utc,
             run_id, source_path)
        VALUES (?, 'chrome', 'Default', 'https://example.com/', 'Example',
                '2024-01-01T00:00:00+00:00', 'run2', '/path/to/chrome')
    """, (evidence_id,))
    conn.commit()
    conn.close()
    return db_path


def _make_db_manager(db_path: Path) -> MagicMock:
    """Create a mock db_manager that returns the given db_path."""
    mgr = MagicMock()
    mgr.evidence_db_path.return_value = str(db_path)
    return mgr


# ---------------------------------------------------------------------------
# Model Tests
# ---------------------------------------------------------------------------

class TestClosedTabsTableModel:
    """Tests for ClosedTabsTableModel."""

    def test_load_returns_all_rows(self, tmp_path):
        db_path = _create_evidence_db(tmp_path / "evidence.sqlite")
        mgr = _make_db_manager(db_path)
        model = ClosedTabsTableModel(mgr, evidence_id=1, evidence_label="ev1")

        model.load()
        assert model.rowCount() == 3

    def test_load_with_browser_filter(self, tmp_path):
        db_path = _create_evidence_db(tmp_path / "evidence.sqlite")
        mgr = _make_db_manager(db_path)
        model = ClosedTabsTableModel(mgr, evidence_id=1, evidence_label="ev1")

        model.load(browser_filter="safari")
        assert model.rowCount() == 2

        model.load(browser_filter="chrome")
        assert model.rowCount() == 1

    def test_column_count(self, tmp_path):
        db_path = _create_evidence_db(tmp_path / "evidence.sqlite")
        mgr = _make_db_manager(db_path)
        model = ClosedTabsTableModel(mgr, evidence_id=1, evidence_label="ev1")
        model.load()

        assert model.columnCount() == len(ClosedTabsTableModel.HEADERS)

    def test_display_data(self, tmp_path):
        from PySide6.QtCore import Qt

        db_path = _create_evidence_db(tmp_path / "evidence.sqlite")
        mgr = _make_db_manager(db_path)
        model = ClosedTabsTableModel(mgr, evidence_id=1, evidence_label="ev1")
        model.load(browser_filter="safari")

        # First row: http://grandx.org/
        idx = model.index(0, ClosedTabsTableModel.COL_URL)
        assert "grandx.org" in model.data(idx, Qt.DisplayRole)

        idx = model.index(0, ClosedTabsTableModel.COL_BROWSER)
        assert model.data(idx, Qt.DisplayRole) == "Safari"

        idx = model.index(0, ClosedTabsTableModel.COL_CLOSED_AT)
        assert "2023-10-04" in model.data(idx, Qt.DisplayRole)

    def test_get_available_browsers(self, tmp_path):
        db_path = _create_evidence_db(tmp_path / "evidence.sqlite")
        mgr = _make_db_manager(db_path)
        model = ClosedTabsTableModel(mgr, evidence_id=1, evidence_label="ev1")

        browsers = model.get_available_browsers()
        assert set(browsers) == {"safari", "chrome"}

    def test_get_row_data(self, tmp_path):
        db_path = _create_evidence_db(tmp_path / "evidence.sqlite")
        mgr = _make_db_manager(db_path)
        model = ClosedTabsTableModel(mgr, evidence_id=1, evidence_label="ev1")
        model.load()

        idx = model.index(0, 0)
        row = model.get_row_data(idx)
        assert row.get("url") is not None

    def test_header_data(self, tmp_path):
        from PySide6.QtCore import Qt

        db_path = _create_evidence_db(tmp_path / "evidence.sqlite")
        mgr = _make_db_manager(db_path)
        model = ClosedTabsTableModel(mgr, evidence_id=1, evidence_label="ev1")

        assert model.headerData(0, Qt.Horizontal) == "URL"
        assert model.headerData(4, Qt.Horizontal) == "Closed At"

    def test_empty_db(self, tmp_path):
        """Model handles empty closed_tabs table gracefully."""
        db_path = tmp_path / "empty.sqlite"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS closed_tabs (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER NOT NULL,
                browser TEXT NOT NULL,
                profile TEXT,
                url TEXT NOT NULL,
                title TEXT,
                closed_at_utc TEXT,
                original_window_id INTEGER,
                original_tab_index INTEGER,
                run_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                discovered_by TEXT,
                partition_index INTEGER,
                fs_type TEXT,
                logical_path TEXT,
                forensic_path TEXT,
                tags TEXT,
                notes TEXT,
                created_at_utc TEXT
            )
        """)
        conn.commit()
        conn.close()

        mgr = _make_db_manager(db_path)
        model = ClosedTabsTableModel(mgr, evidence_id=1, evidence_label="ev1")
        model.load()
        assert model.rowCount() == 0


# ---------------------------------------------------------------------------
# Container Registration Tests
# ---------------------------------------------------------------------------

class TestSessionsContainerClosedTabs:
    """Verify Closed Tabs subtab is registered in the sessions container."""

    def test_container_imports_closed_tabs(self):
        """SessionsContainer should import ClosedTabsSubtab."""
        import inspect
        from app.features.browser_inventory.sessions.container import SessionsContainer

        source = inspect.getsource(SessionsContainer)
        assert "ClosedTabsSubtab" in source
        assert "Closed Tabs" in source

    def test_container_has_three_subtabs(self):
        """Container should have Open Tabs, Closed Tabs, and Form Data."""
        import inspect
        from app.features.browser_inventory.sessions.container import SessionsContainer

        source = inspect.getsource(SessionsContainer._setup_ui)
        assert source.count("addTab") == 3
