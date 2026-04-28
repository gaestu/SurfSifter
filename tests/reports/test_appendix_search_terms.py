"""Tests for appendix search terms module."""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from reports.appendix import AppendixSearchTermsModule


@pytest.fixture
def module() -> AppendixSearchTermsModule:
    return AppendixSearchTermsModule()


@pytest.fixture
def test_db() -> Generator[sqlite3.Connection, None, None]:
    """In-memory DB with browser_search_terms sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript(
        """
        CREATE TABLE browser_search_terms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            term TEXT NOT NULL,
            normalized_term TEXT,
            url TEXT,
            browser TEXT,
            profile TEXT,
            search_engine TEXT,
            search_time_utc TEXT
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
        (1, 1, "forensics tooling", "forensics tooling",
         "https://www.google.com/search?q=forensics+tooling",
         "chrome", "Default", "Google", "2024-02-01T10:00:00"),
        (2, 1, "ewf format", "ewf format",
         "https://www.bing.com/search?q=ewf+format",
         "firefox", None, "Bing", "2024-02-02T11:00:00"),
        (3, 1, "kitten pictures", "kitten pictures",
         "https://duckduckgo.com/?q=kitten+pictures",
         "firefox", None, "DuckDuckGo", "2024-02-03T12:00:00"),
        # different evidence id (must NOT leak)
        (4, 2, "leak query", "leak query",
         "https://www.google.com/search?q=leak",
         "chrome", None, "Google", "2024-02-04T10:00:00"),
    ]
    for r in rows:
        conn.execute(
            """
            INSERT INTO browser_search_terms
                (id, evidence_id, term, normalized_term, url, browser,
                 profile, search_engine, search_time_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            r,
        )

    conn.execute("INSERT INTO tags (id, name) VALUES (1, 'relevant')")
    conn.execute(
        "INSERT INTO tag_associations "
        "(evidence_id, tag_id, artifact_type, artifact_id) "
        "VALUES (1, 1, 'browser_search_term', 1)"
    )
    conn.commit()
    yield conn
    conn.close()


class TestMetadata:
    def test_module_id(self, module: AppendixSearchTermsModule) -> None:
        assert module.metadata.module_id == "appendix_search_terms"

    def test_category(self, module: AppendixSearchTermsModule) -> None:
        assert module.metadata.category == "Appendix"

    def test_default_title(self, module: AppendixSearchTermsModule) -> None:
        assert module.get_default_title() == "Search Terms"


class TestFilterFields:
    def test_has_expected_fields(
        self, module: AppendixSearchTermsModule
    ) -> None:
        keys = {f.key for f in module.get_filter_fields()}
        assert {
            "tag_filter",
            "browser_filter",
            "engine_filter",
            "filter_mode",
            "show_url",
            "show_browser",
            "show_engine",
            "show_timestamp",
            "show_profile",
            "show_count",
        } <= keys

    def test_dynamic_tag_options(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        opts = module.get_dynamic_options("tag_filter", test_db)
        assert opts == [("relevant", "relevant")]

    def test_dynamic_browser_options(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        opts = module.get_dynamic_options("browser_filter", test_db)
        assert opts is not None
        values = [v for v, _ in opts]
        assert "chrome" in values
        assert "firefox" in values

    def test_dynamic_engine_options(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        opts = module.get_dynamic_options("engine_filter", test_db)
        assert opts is not None
        values = [v for v, _ in opts]
        assert {"Google", "Bing", "DuckDuckGo"} <= set(values)


class TestRender:
    def test_renders_all_entries(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(test_db, evidence_id=1, config={})
        assert "forensics tooling" in html
        assert "ewf format" in html
        assert "kitten pictures" in html
        # Default: browser/engine/timestamp shown, url/profile hidden
        assert "Google" in html
        assert "Chrome" in html

    def test_filters_by_tag(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"tag_filter": ["relevant"]},
        )
        assert "forensics tooling" in html
        assert "ewf format" not in html
        assert "kitten pictures" not in html

    def test_filters_by_browser(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"browser_filter": ["firefox"]},
        )
        assert "ewf format" in html
        assert "kitten pictures" in html
        assert "forensics tooling" not in html

    def test_filters_by_engine(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"engine_filter": ["Bing"]},
        )
        assert "ewf format" in html
        assert "forensics tooling" not in html

    def test_filter_mode_and(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        # tag 'relevant' => row 1 (chrome). AND with browser=firefox => empty.
        html = module.render(
            test_db,
            evidence_id=1,
            config={
                "tag_filter": ["relevant"],
                "browser_filter": ["firefox"],
                "filter_mode": "and",
            },
        )
        assert "forensics tooling" not in html
        assert "ewf format" not in html

        # AND with browser=chrome => row 1 only
        html = module.render(
            test_db,
            evidence_id=1,
            config={
                "tag_filter": ["relevant"],
                "browser_filter": ["chrome"],
                "filter_mode": "and",
            },
        )
        assert "forensics tooling" in html
        assert "ewf format" not in html

    def test_filter_mode_or(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        # tag 'relevant' (row 1) OR engine=Bing (row 2) => rows 1+2
        html = module.render(
            test_db,
            evidence_id=1,
            config={
                "tag_filter": ["relevant"],
                "engine_filter": ["Bing"],
                "filter_mode": "or",
            },
        )
        assert "forensics tooling" in html
        assert "ewf format" in html
        assert "kitten pictures" not in html

    def test_empty_when_no_match(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"tag_filter": ["nonexistent"]},
        )
        assert "No search terms" in html

    def test_show_url_column(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"show_url": True},
        )
        assert "google.com/search" in html

    def test_evidence_isolation(
        self,
        module: AppendixSearchTermsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(test_db, evidence_id=1, config={})
        assert "leak query" not in html
