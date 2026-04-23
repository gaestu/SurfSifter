"""Tests for appendix site permissions module."""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from reports.appendix import AppendixSitePermissionsModule


@pytest.fixture
def module() -> AppendixSitePermissionsModule:
    return AppendixSitePermissionsModule()


@pytest.fixture
def test_db() -> Generator[sqlite3.Connection, None, None]:
    """In-memory DB with site permission sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript(
        """
        CREATE TABLE site_permissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            browser TEXT NOT NULL,
            profile TEXT,
            origin TEXT NOT NULL,
            permission_type TEXT NOT NULL,
            permission_value TEXT NOT NULL,
            granted_at_utc TEXT,
            expires_at_utc TEXT
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
        (1, 1, "chrome", "Default", "https://example.com",
         "notifications", "ALLOW",
         "2024-01-01T10:00:00", None),
        (2, 1, "chrome", "Default", "https://maps.example.com",
         "geolocation", "ALLOW",
         "2024-01-02T10:00:00", None),
        (3, 1, "firefox", None, "https://evil.example",
         "notifications", "DENY",
         "2024-01-03T10:00:00", None),
        # entry from a different evidence id (must NOT leak)
        (4, 2, "chrome", None, "https://leak.example",
         "camera", "ALLOW",
         "2024-01-04T10:00:00", None),
    ]
    for r in rows:
        conn.execute(
            """
            INSERT INTO site_permissions
                (id, evidence_id, browser, profile, origin,
                 permission_type, permission_value,
                 granted_at_utc, expires_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            r,
        )

    conn.execute("INSERT INTO tags (id, name) VALUES (1, 'review')")
    conn.execute(
        "INSERT INTO tag_associations "
        "(evidence_id, tag_id, artifact_type, artifact_id) "
        "VALUES (1, 1, 'site_permission', 1)"
    )
    conn.commit()
    yield conn
    conn.close()


class TestMetadata:
    def test_module_id(self, module: AppendixSitePermissionsModule) -> None:
        assert module.metadata.module_id == "appendix_site_permissions"

    def test_category(self, module: AppendixSitePermissionsModule) -> None:
        assert module.metadata.category == "Appendix"

    def test_default_title(self, module: AppendixSitePermissionsModule) -> None:
        assert module.get_default_title() == "Site Permissions"


class TestFilterFields:
    def test_has_expected_fields(
        self, module: AppendixSitePermissionsModule
    ) -> None:
        keys = {f.key for f in module.get_filter_fields()}
        assert {
            "tag_filter",
            "browser_filter",
            "permission_type_filter",
            "permission_value_filter",
            "group_by",
            "show_origin",
            "show_type",
            "show_value",
            "show_browser",
            "show_profile",
            "show_granted_at",
            "show_expires_at",
            "sort_by",
        } <= keys

    def test_dynamic_tag_options(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        opts = module.get_dynamic_options("tag_filter", test_db)
        assert opts == [("review", "review")]

    def test_dynamic_browser_options(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        opts = module.get_dynamic_options("browser_filter", test_db)
        assert opts is not None
        values = [v for v, _ in opts]
        assert values[0] == AppendixSitePermissionsModule.ALL_BROWSERS
        assert "chrome" in values
        assert "firefox" in values

    def test_dynamic_permission_type_options(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        opts = module.get_dynamic_options("permission_type_filter", test_db)
        assert opts is not None
        values = [v for v, _ in opts]
        assert values[0] == AppendixSitePermissionsModule.ALL_TYPES
        assert "notifications" in values
        assert "geolocation" in values

    def test_dynamic_permission_value_options(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        opts = module.get_dynamic_options("permission_value_filter", test_db)
        assert opts is not None
        values = [v for v, _ in opts]
        assert values[0] == AppendixSitePermissionsModule.ALL_VALUES
        assert "ALLOW" in values
        assert "DENY" in values


class TestRender:
    def test_renders_all_entries_grouped_by_origin(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(test_db, evidence_id=1, config={})
        assert "https://example.com" in html
        assert "https://maps.example.com" in html
        assert "https://evil.example" in html
        assert "notifications" in html
        assert "geolocation" in html
        assert "ALLOW" in html
        assert "DENY" in html

    def test_filters_by_tag(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"tag_filter": ["review"]},
        )
        assert "https://example.com" in html
        assert "https://maps.example.com" not in html
        assert "https://evil.example" not in html

    def test_filters_by_browser(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"browser_filter": "firefox"},
        )
        assert "https://evil.example" in html
        assert "https://example.com" not in html

    def test_filters_by_permission_type(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"permission_type_filter": "geolocation"},
        )
        assert "https://maps.example.com" in html
        assert "https://example.com" not in html

    def test_filters_by_permission_value(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"permission_value_filter": "DENY"},
        )
        assert "https://evil.example" in html
        assert "https://example.com" not in html

    def test_empty_when_no_match(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"tag_filter": ["nonexistent"]},
        )
        assert "No site permissions" in html

    def test_no_grouping_renders_flat_table(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"group_by": AppendixSitePermissionsModule.GROUP_NONE},
        )
        # All origins still present
        assert "https://example.com" in html
        assert "https://evil.example" in html

    def test_group_by_browser(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(
            test_db,
            evidence_id=1,
            config={"group_by": AppendixSitePermissionsModule.GROUP_BROWSER},
        )
        assert "Chrome" in html
        assert "Firefox" in html

    def test_evidence_isolation(
        self,
        module: AppendixSitePermissionsModule,
        test_db: sqlite3.Connection,
    ) -> None:
        html = module.render(test_db, evidence_id=1, config={})
        assert "https://leak.example" not in html
