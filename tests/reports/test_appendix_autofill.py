"""Tests for appendix autofill module."""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from reports.appendix import AppendixAutofillModule


@pytest.fixture
def module() -> AppendixAutofillModule:
    return AppendixAutofillModule()


@pytest.fixture
def test_db() -> Generator[sqlite3.Connection, None, None]:
    """In-memory DB with autofill sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript(
        """
        CREATE TABLE autofill (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            browser TEXT NOT NULL,
            profile TEXT,
            name TEXT NOT NULL,
            value TEXT,
            date_created_utc TEXT,
            date_last_used_utc TEXT,
            count INTEGER DEFAULT 1
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
        (1, 1, "chrome", "Default", "email", "alice@example.com",
         "2024-01-01T10:00:00", "2024-02-01T10:00:00", 5),
        (2, 1, "chrome", "Default", "address", "1 Main St",
         "2024-01-02T10:00:00", "2024-02-02T10:00:00", 2),
        (3, 1, "firefox", None, "email", "bob@example.com",
         "2024-01-03T10:00:00", "2024-02-03T10:00:00", 1),
        # entry from a different evidence id (must NOT leak)
        (4, 2, "chrome", None, "email", "leak@example.com",
         "2024-01-04T10:00:00", "2024-02-04T10:00:00", 9),
    ]
    for r in rows:
        conn.execute(
            """
            INSERT INTO autofill
                (id, evidence_id, browser, profile, name, value,
                 date_created_utc, date_last_used_utc, count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            r,
        )

    conn.execute("INSERT INTO tags (id, name) VALUES (1, 'pii')")
    conn.execute(
        "INSERT INTO tag_associations (evidence_id, tag_id, artifact_type, artifact_id) "
        "VALUES (1, 1, 'autofill', 1)"
    )
    conn.commit()
    yield conn
    conn.close()


class TestMetadata:
    def test_module_id(self, module: AppendixAutofillModule) -> None:
        assert module.metadata.module_id == "appendix_autofill"

    def test_category(self, module: AppendixAutofillModule) -> None:
        assert module.metadata.category == "Appendix"

    def test_default_title(self, module: AppendixAutofillModule) -> None:
        assert module.get_default_title() == "Autofill"


class TestFilterFields:
    def test_has_expected_fields(self, module: AppendixAutofillModule) -> None:
        keys = {f.key for f in module.get_filter_fields()}
        assert {
            "tag_filter",
            "browser_filter",
            "field_filter",
            "group_by_browser",
            "show_browser",
            "show_value",
            "show_count",
            "show_first_used",
            "show_last_used",
            "hide_placeholders",
            "sort_by",
        } <= keys

    def test_dynamic_tag_options(
        self, module: AppendixAutofillModule, test_db: sqlite3.Connection
    ) -> None:
        opts = module.get_dynamic_options("tag_filter", test_db)
        assert opts == [("pii", "pii")]

    def test_dynamic_browser_options(
        self, module: AppendixAutofillModule, test_db: sqlite3.Connection
    ) -> None:
        opts = module.get_dynamic_options("browser_filter", test_db)
        assert opts is not None
        values = [v for v, _ in opts]
        assert values[0] == AppendixAutofillModule.ALL_BROWSERS
        assert "chrome" in values
        assert "firefox" in values


class TestRender:
    def test_renders_all_entries_grouped(
        self, module: AppendixAutofillModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(test_db, evidence_id=1, config={})
        assert "Chrome" in html
        assert "Firefox" in html
        assert "alice@example.com" in html
        assert "bob@example.com" in html
        assert "1 Main St" in html

    def test_filters_by_tag(
        self, module: AppendixAutofillModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"tag_filter": ["pii"]},
        )
        assert "alice@example.com" in html
        assert "bob@example.com" not in html
        assert "1 Main St" not in html

    def test_filters_by_browser(
        self, module: AppendixAutofillModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"browser_filter": "firefox"},
        )
        assert "bob@example.com" in html
        assert "alice@example.com" not in html

    def test_filters_by_field_name(
        self, module: AppendixAutofillModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"field_filter": "addr"},
        )
        assert "1 Main St" in html
        assert "alice@example.com" not in html

    def test_empty_when_no_match(
        self, module: AppendixAutofillModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"tag_filter": ["nonexistent"]},
        )
        assert "No autofill" in html

    def test_ungrouped_shows_browser_column(
        self, module: AppendixAutofillModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"group_by_browser": False, "show_browser": True},
        )
        assert "Browser" in html

    def test_evidence_isolation(
        self, module: AppendixAutofillModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(test_db, evidence_id=1, config={})
        assert "leak@example.com" not in html
