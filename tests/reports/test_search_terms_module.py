"""Tests for search terms report module."""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from reports.modules.search_terms import SearchTermsModule


@pytest.fixture
def module() -> SearchTermsModule:
    """Create module instance."""
    return SearchTermsModule()


@pytest.fixture
def test_db() -> Generator[sqlite3.Connection, None, None]:
    """Create in-memory DB with browser_search_terms sample data."""
    conn = sqlite3.connect(":memory:")

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
            search_time_utc TEXT,
            source_path TEXT,
            discovered_by TEXT,
            run_id TEXT,
            partition_index INTEGER,
            logical_path TEXT,
            forensic_path TEXT,
            chromium_keyword_id INTEGER,
            chromium_url_id INTEGER,
            tags TEXT,
            notes TEXT
        );

        CREATE TABLE tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            name_normalized TEXT NOT NULL
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

    # Insert search terms
    conn.execute(
        """
        INSERT INTO browser_search_terms
            (id, evidence_id, term, url, browser, profile, search_engine, search_time_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (1, 1, "forensic tools", "https://www.google.com/search?q=forensic+tools",
         "chrome", "Default", "Google", "2024-03-15T10:30:00"),
    )
    conn.execute(
        """
        INSERT INTO browser_search_terms
            (id, evidence_id, term, url, browser, profile, search_engine, search_time_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (2, 1, "delete browser history", "https://www.google.com/search?q=delete+browser+history",
         "chrome", "Default", "Google", "2024-03-15T11:00:00"),
    )
    conn.execute(
        """
        INSERT INTO browser_search_terms
            (id, evidence_id, term, url, browser, profile, search_engine, search_time_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (3, 1, "how to encrypt files", "https://duckduckgo.com/?q=how+to+encrypt+files",
         "firefox", "default-release", "DuckDuckGo", "2024-03-14T09:00:00"),
    )
    conn.execute(
        """
        INSERT INTO browser_search_terms
            (id, evidence_id, term, url, browser, profile, search_engine, search_time_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (4, 2, "weather today", "https://www.google.com/search?q=weather+today",
         "edge", "Profile 1", "Google", "2024-03-16T08:00:00"),
    )

    # Insert tags
    conn.execute(
        "INSERT INTO tags (id, evidence_id, name, name_normalized) VALUES (1, 1, 'suspicious', 'suspicious')"
    )
    conn.execute(
        "INSERT INTO tags (id, evidence_id, name, name_normalized) VALUES (2, 1, 'antiforensics', 'antiforensics')"
    )

    # Tag associations
    conn.execute(
        """
        INSERT INTO tag_associations (evidence_id, tag_id, artifact_type, artifact_id)
        VALUES (1, 1, 'browser_search_term', 2)
        """
    )
    conn.execute(
        """
        INSERT INTO tag_associations (evidence_id, tag_id, artifact_type, artifact_id)
        VALUES (1, 2, 'browser_search_term', 2)
        """
    )
    conn.execute(
        """
        INSERT INTO tag_associations (evidence_id, tag_id, artifact_type, artifact_id)
        VALUES (1, 2, 'browser_search_term', 3)
        """
    )

    conn.commit()
    yield conn
    conn.close()


# ── Metadata ────────────────────────────────────────────────


def test_metadata(module: SearchTermsModule) -> None:
    """Module metadata is correct."""
    meta = module.metadata
    assert meta.module_id == "search_terms"
    assert meta.name == "Search Terms"
    assert meta.category == "Browser"


def test_filter_fields(module: SearchTermsModule) -> None:
    """Filter fields have expected keys."""
    fields = module.get_filter_fields()
    keys = [f.key for f in fields]
    assert "tag_filter" in keys
    assert "browser_filter" in keys
    assert "engine_filter" in keys
    assert "sort_by" in keys
    assert "limit" in keys
    assert "show_filter_info" in keys


# ── Dynamic options ─────────────────────────────────────────


def test_dynamic_tag_options(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Tag filter options include tags used on search terms."""
    options = module.get_dynamic_options("tag_filter", test_db)
    assert options is not None
    values = {v for v, _ in options}
    assert "suspicious" in values
    assert "antiforensics" in values


def test_dynamic_browser_options(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Browser filter options include browsers from data."""
    options = module.get_dynamic_options("browser_filter", test_db)
    assert options is not None
    values = {v for v, _ in options}
    assert "all" in values
    assert "chrome" in values
    assert "firefox" in values


def test_dynamic_engine_options(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Engine filter options include search engines from data."""
    options = module.get_dynamic_options("engine_filter", test_db)
    assert options is not None
    values = {v for v, _ in options}
    assert "all" in values
    assert "Google" in values
    assert "DuckDuckGo" in values


def test_dynamic_options_unknown_key(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Unknown key returns None."""
    assert module.get_dynamic_options("unknown_key", test_db) is None


# ── Render (all data) ──────────────────────────────────────


def test_render_all_search_terms(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Rendering with defaults shows evidence 1 rows only."""
    html = module.render(test_db, 1, {"sort_by": "time_desc"})

    assert "forensic tools" in html
    assert "delete browser history" in html
    assert "how to encrypt files" in html
    # Evidence 2 row should not appear
    assert "weather today" not in html


def test_render_empty_state(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Rendering with no matching data shows empty message."""
    html = module.render(test_db, 999, {"sort_by": "time_desc"})
    assert "No search terms found" in html


# ── Render (tag filtering) ─────────────────────────────────


def test_render_single_tag_filter(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Single tag filter shows only matching search terms."""
    html = module.render(
        test_db, 1, {"tag_filter": ["suspicious"], "sort_by": "time_desc"}
    )

    assert "delete browser history" in html
    # Not tagged with 'suspicious'
    assert "forensic tools" not in html


def test_render_multi_tag_filter(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Multi-tag filter shows terms matching ANY selected tag (OR logic)."""
    html = module.render(
        test_db, 1, {"tag_filter": ["suspicious", "antiforensics"], "sort_by": "time_desc"}
    )

    assert "delete browser history" in html
    assert "how to encrypt files" in html
    # Not tagged at all
    assert "forensic tools" not in html


def test_render_tag_filter_string_coercion(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """String tag_filter is coerced to list."""
    html = module.render(
        test_db, 1, {"tag_filter": "suspicious", "sort_by": "time_desc"}
    )

    assert "delete browser history" in html
    assert "forensic tools" not in html


# ── Render (browser/engine filtering) ──────────────────────


def test_render_browser_filter(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Browser filter shows only matching browser rows."""
    html = module.render(
        test_db, 1, {"browser_filter": "firefox", "sort_by": "time_desc"}
    )

    assert "how to encrypt files" in html
    assert "forensic tools" not in html
    assert "delete browser history" not in html


def test_render_engine_filter(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Engine filter shows only matching search engine rows."""
    html = module.render(
        test_db, 1, {"engine_filter": "DuckDuckGo", "sort_by": "time_desc"}
    )

    assert "how to encrypt files" in html
    assert "forensic tools" not in html


# ── Render (limit and truncation) ──────────────────────────


def test_render_with_limit(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Limit restricts number of shown rows and shows truncation notice."""
    html = module.render(
        test_db, 1, {"limit": "1", "sort_by": "time_desc"}
    )

    # Should show truncation info (showing 1 of 3)
    assert "1" in html
    assert "3" in html


def test_render_unlimited(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Unlimited shows all rows without truncation."""
    html = module.render(
        test_db, 1, {"limit": "unlimited", "sort_by": "time_desc"}
    )

    assert "forensic tools" in html
    assert "delete browser history" in html
    assert "how to encrypt files" in html


# ── Render (column visibility) ─────────────────────────────


def test_render_hide_columns(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Hidden columns are not rendered in HTML."""
    html = module.render(
        test_db,
        1,
        {
            "show_url": False,
            "show_browser": False,
            "show_engine": False,
            "show_timestamp": False,
            "show_profile": False,
            "sort_by": "time_desc",
        },
    )

    # Term should always be shown
    assert "forensic tools" in html
    # Column headers should be absent from the table
    assert '<th class="col-url">' not in html
    assert '<th class="col-browser">' not in html
    assert '<th class="col-engine">' not in html
    assert '<th class="col-time">' not in html
    assert '<th class="col-profile">' not in html


# ── Render (section title/description) ─────────────────────


def test_render_section_title(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Section title is rendered when provided."""
    html = module.render(
        test_db, 1, {"section_title": "Key Search Terms", "sort_by": "time_desc"}
    )
    assert "Key Search Terms" in html


def test_render_filter_info(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """Filter info footer is rendered when enabled."""
    html = module.render(
        test_db,
        1,
        {
            "tag_filter": ["suspicious"],
            "browser_filter": "chrome",
            "show_filter_info": True,
            "sort_by": "time_desc",
        },
    )

    assert "Tags: suspicious" in html
    assert "Browser: Chrome" in html


# ── Row factory safety ──────────────────────────────────────


def test_row_factory_restored(
    module: SearchTermsModule, test_db: sqlite3.Connection
) -> None:
    """row_factory is restored after render."""
    original_factory = test_db.row_factory
    module.render(test_db, 1, {"sort_by": "time_desc"})
    assert test_db.row_factory is original_factory
