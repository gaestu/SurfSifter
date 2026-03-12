"""Tests for appendix download list module."""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from reports.appendix import AppendixDownloadListModule, AppendixRegistry
from reports.locales import get_translations


@pytest.fixture
def module() -> AppendixDownloadListModule:
    """Create module instance."""
    return AppendixDownloadListModule()


@pytest.fixture
def test_db() -> Generator[sqlite3.Connection, None, None]:
    """Create in-memory DB with downloads sample data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript(
        """
        CREATE TABLE downloads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evidence_id INTEGER NOT NULL,
            url TEXT,
            domain TEXT,
            filename TEXT,
            dest_path TEXT,
            md5 TEXT,
            sha256 TEXT,
            size_bytes INTEGER,
            completed_at_utc TEXT,
            width INTEGER,
            height INTEGER,
            file_type TEXT,
            status TEXT
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

    # Insert sample data
    for i in range(1, 6):
        conn.execute(
            """
            INSERT INTO downloads
                (id, evidence_id, url, domain, filename, dest_path, md5, sha256,
                 size_bytes, completed_at_utc, width, height, file_type, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                i,
                1,
                f"https://example.com/img/{i}.jpg",
                "example.com",
                f"image_{i}.jpg",
                f"downloads/image_{i}.jpg",
                f"{'a' * 31}{i:01x}",
                f"{'b' * 63}{i:01x}",
                1024 * i,
                f"2024-01-{i:02d}T10:00:00",
                640,
                480,
                "image",
                "completed",
            ),
        )

    # Add a tag for testing
    conn.execute("INSERT INTO tags (id, name) VALUES (1, 'relevant')")
    conn.execute(
        "INSERT INTO tag_associations (evidence_id, tag_id, artifact_type, artifact_id) "
        "VALUES (1, 1, 'download', 1)"
    )
    conn.commit()
    yield conn
    conn.close()


class TestAppendixDownloadListMetadata:
    """Test metadata and basic properties."""

    def test_module_id(self, module: AppendixDownloadListModule) -> None:
        assert module.metadata.module_id == "appendix_download_list"

    def test_category(self, module: AppendixDownloadListModule) -> None:
        assert module.metadata.category == "Appendix"

    def test_icon(self, module: AppendixDownloadListModule) -> None:
        assert module.metadata.icon == "📥"

    def test_default_title(self, module: AppendixDownloadListModule) -> None:
        assert module.get_default_title() == "Downloaded Images"


class TestAppendixDownloadListFilterFields:
    """Test filter fields."""

    def test_has_domain_filter(self, module: AppendixDownloadListModule) -> None:
        fields = {f.key: f for f in module.get_filter_fields()}
        assert "domain_filter" in fields
        assert fields["domain_filter"].default == "all"

    def test_has_tag_filter(self, module: AppendixDownloadListModule) -> None:
        fields = {f.key: f for f in module.get_filter_fields()}
        assert "tag_filter" in fields

    def test_has_include_url(self, module: AppendixDownloadListModule) -> None:
        fields = {f.key: f for f in module.get_filter_fields()}
        assert "include_url" in fields
        assert fields["include_url"].default is True

    def test_has_include_hash(self, module: AppendixDownloadListModule) -> None:
        fields = {f.key: f for f in module.get_filter_fields()}
        assert "include_hash" in fields
        assert fields["include_hash"].default is True

    def test_has_sort_by(self, module: AppendixDownloadListModule) -> None:
        fields = {f.key: f for f in module.get_filter_fields()}
        assert "sort_by" in fields


class TestAppendixDownloadListDynamicOptions:
    """Test dynamic option loading."""

    def test_domain_options(
        self,
        module: AppendixDownloadListModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Domain filter loads dynamically from DB."""
        options = module.get_dynamic_options("domain_filter", test_db)
        assert options is not None
        values = [opt[0] for opt in options]
        assert "all" in values
        assert "example.com" in values

    def test_tag_options(
        self,
        module: AppendixDownloadListModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Tag filter loads dynamically from DB."""
        options = module.get_dynamic_options("tag_filter", test_db)
        assert options is not None
        values = [opt[0] for opt in options]
        assert "all" in values
        assert "any_tag" in values
        assert "relevant" in values

    def test_unknown_key_returns_none(
        self,
        module: AppendixDownloadListModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Unknown key returns None."""
        assert module.get_dynamic_options("unknown_key", test_db) is None


class TestAppendixDownloadListRender:
    """Test rendering."""

    def test_render_all_images(
        self,
        module: AppendixDownloadListModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """All images are rendered (no limit in appendix)."""
        config = {"_translations": get_translations("en")}
        html = module.render(test_db, 1, config)
        assert "appendix-download-card" in html
        card_count = html.count('<div class="appendix-download-card">')
        assert card_count == 5

    def test_render_empty_db(
        self,
        module: AppendixDownloadListModule,
    ) -> None:
        """Renders empty message when no downloads exist."""
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            """
            CREATE TABLE downloads (
                id INTEGER PRIMARY KEY, evidence_id INTEGER, url TEXT,
                domain TEXT, filename TEXT, dest_path TEXT, md5 TEXT,
                sha256 TEXT, size_bytes INTEGER, completed_at_utc TEXT,
                width INTEGER, height INTEGER, file_type TEXT, status TEXT
            );
            """
        )
        config = {"_translations": get_translations("en")}
        html = module.render(conn, 1, config)
        assert "empty-message" in html
        assert "No downloaded images found." in html
        conn.close()

    def test_render_with_domain_filter(
        self,
        module: AppendixDownloadListModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Domain filter restricts results."""
        config = {
            "domain_filter": "example.com",
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        assert "appendix-download-card" in html

    def test_render_includes_url(
        self,
        module: AppendixDownloadListModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """URLs are shown when include_url is True."""
        config = {
            "include_url": True,
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        assert "appendix-download-url" in html
        assert "https://example.com/img/" in html

    def test_render_hides_url(
        self,
        module: AppendixDownloadListModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """URLs are hidden when include_url is False."""
        config = {
            "include_url": False,
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        # URL elements should not appear in rendered cards
        assert '<div class="appendix-download-url"' not in html

    def test_render_includes_hash(
        self,
        module: AppendixDownloadListModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Hashes are shown when include_hash is True."""
        config = {
            "include_hash": True,
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        assert "appendix-download-hash" in html

    def test_render_hides_hash(
        self,
        module: AppendixDownloadListModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Hashes are hidden when include_hash is False."""
        config = {
            "include_hash": False,
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        # Hash elements should not appear in rendered cards
        assert '<div class="appendix-download-hash"' not in html

    def test_render_german_locale(
        self,
        module: AppendixDownloadListModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """German translations are used."""
        config = {
            "_locale": "de",
            "_translations": get_translations("de"),
        }
        html = module.render(test_db, 1, config)
        assert "appendix-download-card" in html


class TestAppendixDownloadListRegistry:
    """Test registry discovery."""

    def test_registry_discovers_module(self) -> None:
        """AppendixRegistry discovers the download_list module."""
        registry = AppendixRegistry()
        modules = registry.list_modules()
        # list_modules() returns ModuleMetadata objects
        module_ids = [m.module_id for m in modules]
        assert "appendix_download_list" in module_ids

    def test_registry_get_module(self) -> None:
        """Can retrieve module by ID from registry."""
        registry = AppendixRegistry()
        mod = registry.get_module("appendix_download_list")
        assert mod is not None
        assert mod.metadata.module_id == "appendix_download_list"
