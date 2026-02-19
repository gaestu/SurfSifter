"""Tests for browser downloads report module."""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from reports.locales import get_translations
from reports.modules.browser_downloads import BrowserDownloadsModule


@pytest.fixture
def module() -> BrowserDownloadsModule:
    """Create module instance."""
    return BrowserDownloadsModule()


@pytest.fixture
def test_db() -> Generator[sqlite3.Connection, None, None]:
    """Create in-memory DB with browser_downloads sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript(
        """
        CREATE TABLE browser_downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            browser TEXT,
            url TEXT,
            target_path TEXT,
            filename TEXT,
            start_time_utc TEXT,
            end_time_utc TEXT,
            total_bytes INTEGER,
            received_bytes INTEGER,
            state TEXT
        );

        CREATE TABLE tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        );

        CREATE TABLE tag_associations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            artifact_type TEXT NOT NULL,
            artifact_id INTEGER NOT NULL
        );
        """
    )

    conn.execute(
        """
        INSERT INTO browser_downloads
            (id, evidence_id, browser, url, target_path, filename, start_time_utc, end_time_utc, total_bytes, received_bytes, state)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            1,
            "chrome",
            "https://example.com/files/report.pdf",
            "/Users/test/Downloads/report.pdf",
            "report.pdf",
            "2024-01-15T11:00:00",
            "2024-01-15T11:05:00",
            4096,
            4096,
            "complete",
        ),
    )
    conn.execute(
        """
        INSERT INTO browser_downloads
            (id, evidence_id, browser, url, target_path, filename, start_time_utc, end_time_utc, total_bytes, received_bytes, state)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            2,
            1,
            "firefox",
            "https://downloads.example.org/path/to/a/very/long/archive-name-with-many-segments.zip",
            "C:\\Users\\test\\Downloads\\archive-name-with-many-segments.zip",
            None,
            "2024-01-14T10:00:00",
            None,
            None,
            1024,
            "in_progress",
        ),
    )
    conn.execute(
        """
        INSERT INTO browser_downloads
            (id, evidence_id, browser, url, target_path, filename, start_time_utc, end_time_utc, total_bytes, received_bytes, state)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            3,
            2,
            "edge",
            "https://example.net/other.bin",
            "/tmp/other.bin",
            "other.bin",
            "2024-01-13T09:00:00",
            "2024-01-13T09:05:00",
            12,
            12,
            "complete",
        ),
    )

    conn.execute("INSERT INTO tags (id, name) VALUES (1, 'important')")
    conn.execute("INSERT INTO tags (id, name) VALUES (2, 'review')")
    conn.execute(
        """
        INSERT INTO tag_associations (evidence_id, tag_id, artifact_type, artifact_id)
        VALUES (1, 1, 'browser_download', 1)
        """
    )

    conn.commit()
    yield conn
    conn.close()


def test_metadata(module: BrowserDownloadsModule) -> None:
    """Module metadata is correct."""
    meta = module.metadata
    assert meta.module_id == "browser_downloads"
    assert meta.name == "Browser Downloads"


def test_dynamic_tag_options(
    module: BrowserDownloadsModule, test_db: sqlite3.Connection
) -> None:
    """Tag filter options include tags used on browser downloads."""
    options = module.get_dynamic_options("tag_filter", test_db)
    assert options is not None
    option_values = {value for value, _ in options}
    assert "important" in option_values


def test_render_all_downloads(
    module: BrowserDownloadsModule, test_db: sqlite3.Connection
) -> None:
    """Rendering with default tag filter includes evidence rows."""
    html = module.render(
        test_db,
        1,
        {
            "tag_filter": "all",
            "show_browser": True,
            "show_state": True,
            "show_size": True,
            "show_end_time": True,
            "sort_by": "start_time_desc",
        },
    )

    assert "report.pdf" in html
    assert "archive-name-with-many-segments.zip" in html
    assert "Complete" in html
    assert "In Progress" in html
    assert "other.bin" not in html


def test_render_tag_filtered(
    module: BrowserDownloadsModule, test_db: sqlite3.Connection
) -> None:
    """Specific tag filter only shows matching browser downloads."""
    html = module.render(
        test_db,
        1,
        {
            "tag_filter": "important",
            "show_browser": True,
            "show_state": True,
            "show_size": True,
            "show_end_time": True,
            "sort_by": "start_time_desc",
        },
    )

    assert "report.pdf" in html
    assert "archive-name-with-many-segments.zip" not in html


def test_render_shorten_urls_class(
    module: BrowserDownloadsModule, test_db: sqlite3.Connection
) -> None:
    """Shorten URL toggle adds the CSS class used for ellipsis mode."""
    html = module.render(
        test_db,
        1,
        {
            "tag_filter": "all",
            "shorten_urls": True,
            "sort_by": "start_time_desc",
        },
    )

    assert 'class="module-browser-downloads shorten-urls"' in html


def test_render_uses_german_translations(
    module: BrowserDownloadsModule, test_db: sqlite3.Connection
) -> None:
    """Rendering should use German labels when locale/translations are set."""
    html = module.render(
        test_db,
        1,
        {
            "tag_filter": "all",
            "_locale": "de",
            "_translations": get_translations("de"),
            "sort_by": "start_time_desc",
        },
    )

    assert "Dateiname" in html
    assert "Startzeit" in html
    assert "Status" in html
