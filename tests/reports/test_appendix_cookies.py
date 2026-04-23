"""Tests for appendix cookies module."""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from reports.appendix import AppendixCookiesModule, AppendixRegistry


@pytest.fixture
def module() -> AppendixCookiesModule:
    return AppendixCookiesModule()


@pytest.fixture
def test_db() -> Generator[sqlite3.Connection, None, None]:
    """In-memory DB with cookie sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript(
        """
        CREATE TABLE cookies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            browser TEXT NOT NULL,
            profile TEXT,
            name TEXT NOT NULL,
            value TEXT,
            domain TEXT NOT NULL,
            path TEXT,
            expires_utc TEXT,
            is_secure INTEGER,
            is_httponly INTEGER,
            samesite TEXT,
            creation_utc TEXT,
            last_access_utc TEXT
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
        (1, 1, "chrome", None, "sid", "abc", "example.com", "/",
         "2025-01-01T00:00:00", 1, 1, "Lax",
         "2024-01-01T10:00:00", "2024-01-05T10:00:00"),
        (2, 1, "chrome", None, "pref", "v=1", "example.com", "/",
         "2025-01-01T00:00:00", 0, 0, None,
         "2024-01-02T10:00:00", "2024-01-06T10:00:00"),
        (3, 1, "firefox", None, "auth", "tok", "other.org", "/",
         "2025-01-01T00:00:00", 1, 1, "Strict",
         "2024-01-03T10:00:00", "2024-01-07T10:00:00"),
    ]
    for r in rows:
        conn.execute(
            """
            INSERT INTO cookies
                (id, evidence_id, browser, profile, name, value, domain, path,
                 expires_utc, is_secure, is_httponly, samesite,
                 creation_utc, last_access_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            r,
        )

    conn.execute("INSERT INTO tags (id, name) VALUES (1, 'relevant')")
    conn.execute(
        "INSERT INTO tag_associations (evidence_id, tag_id, artifact_type, artifact_id) "
        "VALUES (1, 1, 'cookie', 1)"
    )
    conn.commit()
    yield conn
    conn.close()


class TestMetadata:
    def test_module_id(self, module: AppendixCookiesModule) -> None:
        assert module.metadata.module_id == "appendix_cookies"

    def test_category(self, module: AppendixCookiesModule) -> None:
        assert module.metadata.category == "Appendix"

    def test_default_title(self, module: AppendixCookiesModule) -> None:
        assert module.get_default_title() == "Cookies"


class TestFilterFields:
    def test_has_expected_fields(self, module: AppendixCookiesModule) -> None:
        keys = {f.key for f in module.get_filter_fields()}
        assert {
            "tag_filter",
            "browser_filter",
            "group_by_domain",
            "show_browser",
            "show_expires",
            "show_last_access",
            "show_flags",
        } <= keys

    def test_dynamic_tag_options(
        self, module: AppendixCookiesModule, test_db: sqlite3.Connection
    ) -> None:
        opts = module.get_dynamic_options("tag_filter", test_db)
        assert opts == [("relevant", "relevant")]

    def test_dynamic_browser_options(
        self, module: AppendixCookiesModule, test_db: sqlite3.Connection
    ) -> None:
        opts = module.get_dynamic_options("browser_filter", test_db)
        assert opts is not None
        values = [v for v, _ in opts]
        assert values[0] == AppendixCookiesModule.ALL_BROWSERS
        assert "chrome" in values
        assert "firefox" in values


class TestRender:
    def test_renders_all_entries_grouped(
        self, module: AppendixCookiesModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(test_db, evidence_id=1, config={})
        # Grouped by domain
        assert "example.com" in html
        assert "other.org" in html
        assert "sid" in html
        assert "auth" in html

    def test_filters_by_tag(
        self, module: AppendixCookiesModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"tag_filter": ["relevant"]},
        )
        assert "sid" in html
        assert "pref" not in html
        assert "auth" not in html

    def test_filters_by_browser(
        self, module: AppendixCookiesModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"browser_filter": "firefox"},
        )
        assert "auth" in html
        assert "sid" not in html
        assert "pref" not in html

    def test_empty_when_no_match(
        self, module: AppendixCookiesModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"tag_filter": ["nonexistent"]},
        )
        assert "No cookies" in html

    def test_ungrouped_shows_domain_column(
        self, module: AppendixCookiesModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"group_by_domain": False},
        )
        assert "Domain" in html

    def test_flags_column(
        self, module: AppendixCookiesModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"show_flags": True, "group_by_domain": False},
        )
        assert "Secure" in html
        assert "HttpOnly" in html
        assert "Lax" in html or "Strict" in html

    def test_evidence_isolation(
        self, module: AppendixCookiesModule, test_db: sqlite3.Connection
    ) -> None:
        html = module.render(test_db, evidence_id=999, config={})
        assert "No cookies" in html


class TestRegistry:
    def test_module_is_discovered(self) -> None:
        AppendixRegistry._instance = None
        registry = AppendixRegistry()
        module_ids = {m.module_id for m in registry.list_modules()}
        assert "appendix_cookies" in module_ids
