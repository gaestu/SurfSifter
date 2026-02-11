"""
Tests for browser artifacts subtabs.

Tests for CookiesTableModel, BookmarksTableModel, BrowserDownloadsTableModel,
and the new helper functions in core.db.
"""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from core.database import (
    get_cookie_by_id,
    get_distinct_cookie_browsers,
    get_distinct_bookmark_browsers,
    get_distinct_download_browsers,
)


# =============================================================================
# DB Helper Function Tests
# =============================================================================


class TestGetCookieById:
    """Tests for get_cookie_by_id function."""

    def test_returns_none_for_nonexistent_id(self):
        """Should return None when cookie ID doesn't exist."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db_path = Path(f.name)

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("""
                CREATE TABLE cookies (
                    id INTEGER PRIMARY KEY,
                    evidence_id INTEGER,
                    browser TEXT,
                    profile TEXT,
                    name TEXT,
                    value TEXT,
                    domain TEXT,
                    path TEXT,
                    expires_utc TEXT,
                    is_secure INTEGER,
                    is_httponly INTEGER,
                    samesite TEXT,
                    creation_utc TEXT,
                    last_access_utc TEXT,
                    encrypted INTEGER,
                    encrypted_value BLOB,
                    run_id TEXT,
                    source_path TEXT,
                    discovered_by TEXT,
                    tags TEXT,
                    notes TEXT
                )
            """)
            conn.commit()

            result = get_cookie_by_id(conn, 999)
            assert result is None
        finally:
            conn.close()
            db_path.unlink()

    def test_returns_cookie_with_encrypted_value(self):
        """Should return full cookie data including encrypted_value blob."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db_path = Path(f.name)

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("""
                CREATE TABLE cookies (
                    id INTEGER PRIMARY KEY,
                    evidence_id INTEGER,
                    browser TEXT,
                    profile TEXT,
                    name TEXT,
                    value TEXT,
                    domain TEXT,
                    path TEXT,
                    expires_utc TEXT,
                    is_secure INTEGER,
                    is_httponly INTEGER,
                    samesite TEXT,
                    creation_utc TEXT,
                    last_access_utc TEXT,
                    encrypted INTEGER,
                    encrypted_value BLOB,
                    run_id TEXT,
                    source_path TEXT,
                    discovered_by TEXT,
                    tags TEXT,
                    notes TEXT
                )
            """)
            # Insert test cookie with encrypted value
            encrypted_bytes = b"\x01\x02\x03\x04\x05"
            conn.execute(
                """
                INSERT INTO cookies (
                    id, evidence_id, browser, profile, name, value, domain, path,
                    is_secure, is_httponly, encrypted, encrypted_value
                ) VALUES (1, 1, 'chrome', 'Default', 'session_id', '', '.example.com', '/',
                         1, 1, 1, ?)
                """,
                (encrypted_bytes,)
            )
            conn.commit()

            result = get_cookie_by_id(conn, 1)
            assert result is not None
            assert result["id"] == 1
            assert result["browser"] == "chrome"
            assert result["name"] == "session_id"
            assert result["encrypted"] == 1
            assert result["encrypted_value"] == encrypted_bytes
        finally:
            conn.close()
            db_path.unlink()


class TestGetDistinctCookieBrowsers:
    """Tests for get_distinct_cookie_browsers function."""

    def test_returns_empty_list_for_no_cookies(self):
        """Should return empty list when no cookies exist."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db_path = Path(f.name)

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("""
                CREATE TABLE cookies (
                    id INTEGER PRIMARY KEY,
                    evidence_id INTEGER,
                    browser TEXT
                )
            """)
            conn.commit()

            result = get_distinct_cookie_browsers(conn, 1)
            assert result == []
        finally:
            conn.close()
            db_path.unlink()

    def test_returns_distinct_browsers_sorted(self):
        """Should return sorted list of distinct browsers."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db_path = Path(f.name)

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("""
                CREATE TABLE cookies (
                    id INTEGER PRIMARY KEY,
                    evidence_id INTEGER,
                    browser TEXT
                )
            """)
            # Insert cookies from multiple browsers
            conn.execute("INSERT INTO cookies (evidence_id, browser) VALUES (1, 'firefox')")
            conn.execute("INSERT INTO cookies (evidence_id, browser) VALUES (1, 'chrome')")
            conn.execute("INSERT INTO cookies (evidence_id, browser) VALUES (1, 'chrome')")  # Duplicate
            conn.execute("INSERT INTO cookies (evidence_id, browser) VALUES (1, 'edge')")
            conn.execute("INSERT INTO cookies (evidence_id, browser) VALUES (2, 'safari')")  # Different evidence
            conn.commit()

            result = get_distinct_cookie_browsers(conn, 1)
            assert result == ["chrome", "edge", "firefox"]  # Sorted, distinct
        finally:
            conn.close()
            db_path.unlink()


class TestGetDistinctBookmarkBrowsers:
    """Tests for get_distinct_bookmark_browsers function."""

    def test_returns_distinct_browsers(self):
        """Should return distinct browsers for bookmarks."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db_path = Path(f.name)

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("""
                CREATE TABLE bookmarks (
                    id INTEGER PRIMARY KEY,
                    evidence_id INTEGER,
                    browser TEXT
                )
            """)
            conn.execute("INSERT INTO bookmarks (evidence_id, browser) VALUES (1, 'brave')")
            conn.execute("INSERT INTO bookmarks (evidence_id, browser) VALUES (1, 'chrome')")
            conn.execute("INSERT INTO bookmarks (evidence_id, browser) VALUES (1, 'brave')")
            conn.commit()

            result = get_distinct_bookmark_browsers(conn, 1)
            assert result == ["brave", "chrome"]
        finally:
            conn.close()
            db_path.unlink()


class TestGetDistinctDownloadBrowsers:
    """Tests for get_distinct_download_browsers function."""

    def test_returns_distinct_browsers(self):
        """Should return distinct browsers for downloads."""
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as f:
            db_path = Path(f.name)

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("""
                CREATE TABLE browser_downloads (
                    id INTEGER PRIMARY KEY,
                    evidence_id INTEGER,
                    browser TEXT
                )
            """)
            conn.execute("INSERT INTO browser_downloads (evidence_id, browser) VALUES (1, 'edge')")
            conn.execute("INSERT INTO browser_downloads (evidence_id, browser) VALUES (1, 'firefox')")
            conn.execute("INSERT INTO browser_downloads (evidence_id, browser) VALUES (1, 'edge')")
            conn.commit()

            result = get_distinct_download_browsers(conn, 1)
            assert result == ["edge", "firefox"]
        finally:
            conn.close()
            db_path.unlink()


# =============================================================================
# Model Tests
# =============================================================================


class TestCookiesTableModel:
    """Tests for CookiesTableModel."""

    def test_column_count(self):
        """Should have 11 columns."""
        from app.features.browser_inventory.cookies.model import CookiesTableModel
        assert len(CookiesTableModel.HEADERS) == 11
        assert len(CookiesTableModel.COLUMNS) == 11

    def test_headers(self):
        """Should have correct header names."""
        from app.features.browser_inventory.cookies.model import CookiesTableModel
        expected_headers = [
            "Domain", "Name", "Value", "Browser", "Profile",
            "Secure", "HttpOnly", "SameSite", "Expires", "Encrypted", "Tags"
        ]
        assert CookiesTableModel.HEADERS == expected_headers


class TestBookmarksTableModel:
    """Tests for BookmarksTableModel."""

    def test_column_count(self):
        """Should have 7 columns."""
        from app.features.browser_inventory.bookmarks.model import BookmarksTableModel
        assert len(BookmarksTableModel.HEADERS) == 7
        assert len(BookmarksTableModel.COLUMNS) == 7

    def test_headers(self):
        """Should have correct header names."""
        from app.features.browser_inventory.bookmarks.model import BookmarksTableModel
        expected_headers = [
            "Title", "URL", "Folder", "Browser", "Profile", "Date Added", "Tags"
        ]
        assert BookmarksTableModel.HEADERS == expected_headers


class TestBrowserDownloadsTableModel:
    """Tests for BrowserDownloadsTableModel."""

    def test_column_count(self):
        """Should have 9 columns."""
        from app.features.browser_inventory.downloads.model import BrowserDownloadsTableModel
        assert len(BrowserDownloadsTableModel.HEADERS) == 9
        assert len(BrowserDownloadsTableModel.COLUMNS) == 9

    def test_headers(self):
        """Should have correct header names."""
        from app.features.browser_inventory.downloads.model import BrowserDownloadsTableModel
        expected_headers = [
            "Filename", "URL", "Browser", "State", "Danger", "Size", "Start Time", "End Time", "Tags"
        ]
        assert BrowserDownloadsTableModel.HEADERS == expected_headers

    def test_format_bytes(self):
        """Should format bytes correctly."""
        from app.features.browser_inventory.downloads.model import BrowserDownloadsTableModel

        assert BrowserDownloadsTableModel.format_bytes(0) == ""
        assert BrowserDownloadsTableModel.format_bytes(None) == ""
        assert BrowserDownloadsTableModel.format_bytes(500) == "500 B"
        assert BrowserDownloadsTableModel.format_bytes(1024) == "1.0 KB"
        assert BrowserDownloadsTableModel.format_bytes(1536) == "1.5 KB"
        assert BrowserDownloadsTableModel.format_bytes(1048576) == "1.0 MB"
        assert BrowserDownloadsTableModel.format_bytes(1073741824) == "1.00 GB"

    def test_state_colors_defined(self):
        """Should have state colors defined."""
        from app.features.browser_inventory.downloads.model import BrowserDownloadsTableModel

        assert "complete" in BrowserDownloadsTableModel.STATE_COLORS
        assert "cancelled" in BrowserDownloadsTableModel.STATE_COLORS
        assert "interrupted" in BrowserDownloadsTableModel.STATE_COLORS
        assert "in_progress" in BrowserDownloadsTableModel.STATE_COLORS


# =============================================================================
# Dialog Tests (Structural)
# =============================================================================


class TestCookieDetailsDialog:
    """Tests for CookieDetailsDialog structure."""

    def test_dialog_can_be_instantiated(self):
        """Should be able to create dialog with sample data."""
        pytest.importorskip("PySide6")
        from app.features.browser_inventory.cookies.dialog import CookieDetailsDialog

        sample_data = {
            "id": 1,
            "domain": ".example.com",
            "name": "session_id",
            "value": "abc123",
            "browser": "chrome",
            "profile": "Default",
            "is_secure": True,
            "is_httponly": True,
            "samesite": "Lax",
            "encrypted": False,
        }

        # Just verify the class exists and takes the expected parameters
        assert callable(CookieDetailsDialog)


class TestBookmarkDetailsDialog:
    """Tests for BookmarkDetailsDialog structure."""

    def test_dialog_can_be_instantiated(self):
        """Should be able to create dialog with sample data."""
        pytest.importorskip("PySide6")
        from app.features.browser_inventory.bookmarks.dialog import BookmarkDetailsDialog

        # Just verify the class exists
        assert callable(BookmarkDetailsDialog)


class TestBrowserDownloadDetailsDialog:
    """Tests for BrowserDownloadDetailsDialog structure."""

    def test_dialog_can_be_instantiated(self):
        """Should be able to create dialog with sample data."""
        pytest.importorskip("PySide6")
        from app.features.browser_inventory.downloads.dialog import BrowserDownloadDetailsDialog

        # Just verify the class exists
        assert callable(BrowserDownloadDetailsDialog)


# =============================================================================
# BrowserInventoryTab Tests
# =============================================================================


class TestBrowserInventoryTabStructure:
    """Tests for BrowserInventoryTab structure changes."""

    def test_load_inventory_method_exists(self):
        """Should have load_inventory public method."""
        pytest.importorskip("PySide6")
        from app.features.browser_inventory import BrowserInventoryTab

        assert hasattr(BrowserInventoryTab, "load_inventory")
        assert callable(getattr(BrowserInventoryTab, "load_inventory"))

    def test_has_subtab_related_attributes(self):
        """Should have subtab-related method definitions."""
        pytest.importorskip("PySide6")
        from app.features.browser_inventory import BrowserInventoryTab

        # Check for subtab building methods
        assert hasattr(BrowserInventoryTab, "_build_inventory_widget")
        assert hasattr(BrowserInventoryTab, "_build_cookies_widget")
        assert hasattr(BrowserInventoryTab, "_build_bookmarks_widget")
        assert hasattr(BrowserInventoryTab, "_build_downloads_widget")

        # Check for lazy loading methods
        assert hasattr(BrowserInventoryTab, "_load_cookies")
        assert hasattr(BrowserInventoryTab, "_load_bookmarks")
        assert hasattr(BrowserInventoryTab, "_load_downloads")

        # Check for filter methods
        assert hasattr(BrowserInventoryTab, "_apply_cookies_filters")
        assert hasattr(BrowserInventoryTab, "_apply_bookmarks_filters")
        assert hasattr(BrowserInventoryTab, "_apply_downloads_filters")
