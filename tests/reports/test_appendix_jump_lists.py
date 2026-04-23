"""Tests for appendix jump lists module."""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from reports.appendix import AppendixJumpListsModule, AppendixRegistry


@pytest.fixture
def module() -> AppendixJumpListsModule:
    return AppendixJumpListsModule()


@pytest.fixture
def test_db() -> Generator[sqlite3.Connection, None, None]:
    """In-memory DB with jump list sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript(
        """
        CREATE TABLE jump_list_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            appid TEXT NOT NULL,
            browser TEXT,
            jumplist_path TEXT NOT NULL,
            entry_id TEXT,
            target_path TEXT,
            url TEXT,
            title TEXT,
            lnk_creation_time TEXT,
            lnk_access_time TEXT,
            access_count INTEGER,
            pin_status TEXT
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

    rows = [
        (1, 1, "appid-chrome", "Chrome", "/jl/chrome.lnk", "e1",
         "C:/Users/u/Documents/a.txt", "https://example.com/a",
         "A", "2024-01-01T10:00:00", "2024-01-05T10:00:00", 3, ""),
        (2, 1, "appid-chrome", "Chrome", "/jl/chrome.lnk", "e2",
         "C:/Users/u/Documents/b.txt", "https://example.com/b",
         "B", "2024-01-02T10:00:00", "2024-01-06T10:00:00", 1, ""),
        (3, 1, "appid-word", "", "/jl/word.lnk", "e1",
         "C:/Users/u/Documents/c.docx", "", "C",
         "2024-01-03T10:00:00", "2024-01-07T10:00:00", 5, "Pinned"),
    ]
    for r in rows:
        conn.execute(
            """
            INSERT INTO jump_list_entries
                (id, evidence_id, appid, browser, jumplist_path, entry_id,
                 target_path, url, title, lnk_creation_time, lnk_access_time,
                 access_count, pin_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            r,
        )

    conn.execute("INSERT INTO tags (id, name) VALUES (1, 'relevant')")
    conn.execute(
        "INSERT INTO tag_associations (evidence_id, tag_id, artifact_type, artifact_id) "
        "VALUES (1, 1, 'jump_list', 1)"
    )
    conn.commit()
    yield conn
    conn.close()


class TestMetadata:
    def test_module_id(self, module: AppendixJumpListsModule) -> None:
        assert module.metadata.module_id == "appendix_jump_lists"

    def test_category(self, module: AppendixJumpListsModule) -> None:
        assert module.metadata.category == "Appendix"

    def test_default_title(self, module: AppendixJumpListsModule) -> None:
        assert module.get_default_title() == "Jump Lists"


class TestFilterFields:
    def test_has_tag_filter(self, module: AppendixJumpListsModule) -> None:
        keys = {f.key for f in module.get_filter_fields()}
        assert {"tag_filter", "group_by_application", "show_target_path"} <= keys

    def test_dynamic_tag_options(
        self, module: AppendixJumpListsModule, test_db: sqlite3.Connection
    ) -> None:
        opts = module.get_dynamic_options("tag_filter", test_db)
        assert opts == [("relevant", "relevant")]


class TestRender:
    def test_renders_all_entries_grouped(
        self, module: AppendixJumpListsModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(test_db, evidence_id=1, config={})
        # Grouped by application (Chrome + appid-word fallback)
        assert "Chrome" in html
        assert "appid-word" in html
        assert "C:/Users/u/Documents/a.txt" in html
        assert "C:/Users/u/Documents/c.docx" in html

    def test_filters_by_tag(
        self, module: AppendixJumpListsModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"tag_filter": ["relevant"]},
        )
        assert "C:/Users/u/Documents/a.txt" in html
        assert "C:/Users/u/Documents/b.txt" not in html
        assert "C:/Users/u/Documents/c.docx" not in html

    def test_empty_when_no_match(
        self, module: AppendixJumpListsModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"tag_filter": ["nonexistent"]},
        )
        assert "No jump list entries" in html

    def test_ungrouped_shows_application_column(
        self, module: AppendixJumpListsModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"group_by_application": False},
        )
        assert "Application" in html

    def test_evidence_isolation(
        self, module: AppendixJumpListsModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(test_db, evidence_id=999, config={})
        assert "No jump list entries" in html


class TestRegistry:
    def test_module_is_discovered(self) -> None:
        # Reset singleton to force re-discovery
        AppendixRegistry._instance = None
        registry = AppendixRegistry()
        module_ids = {m.module_id for m in registry.list_modules()}
        assert "appendix_jump_lists" in module_ids
