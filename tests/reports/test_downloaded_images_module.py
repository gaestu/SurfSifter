"""Tests for downloaded images report module enhancements (title, description, limit)."""

from __future__ import annotations

import sqlite3
from typing import Generator

import pytest

from reports.locales import get_translations
from reports.modules.downloaded_images import DownloadedImagesModule


@pytest.fixture
def module() -> DownloadedImagesModule:
    """Create module instance."""
    return DownloadedImagesModule()


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

    # Insert 12 test downloads
    for i in range(1, 13):
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
                f"https://example{i % 3}.com/img/{i}.jpg",
                f"example{i % 3}.com",
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
    conn.commit()
    yield conn
    conn.close()


class TestDownloadedImagesFilterFields:
    """Test the new filter fields."""

    def test_has_section_title_field(self, module: DownloadedImagesModule) -> None:
        """section_title field exists."""
        fields = {f.key: f for f in module.get_filter_fields()}
        assert "section_title" in fields
        assert fields["section_title"].filter_type.name == "TEXT"
        assert fields["section_title"].default == ""

    def test_has_section_description_field(self, module: DownloadedImagesModule) -> None:
        """section_description field exists."""
        fields = {f.key: f for f in module.get_filter_fields()}
        assert "section_description" in fields
        assert fields["section_description"].filter_type.name == "TEXT"
        assert fields["section_description"].default == ""

    def test_has_limit_field(self, module: DownloadedImagesModule) -> None:
        """limit field exists with correct options."""
        fields = {f.key: f for f in module.get_filter_fields()}
        assert "limit" in fields
        limit_field = fields["limit"]
        assert limit_field.default == "all"
        option_values = [opt[0] for opt in limit_field.options]
        assert "9" in option_values
        assert "15" in option_values
        assert "27" in option_values
        assert "39" in option_values
        assert "60" in option_values
        assert "all" in option_values

    def test_has_show_image_count_field(self, module: DownloadedImagesModule) -> None:
        """show_image_count checkbox field exists."""
        fields = {f.key: f for f in module.get_filter_fields()}
        assert "show_image_count" in fields
        assert fields["show_image_count"].filter_type.name == "CHECKBOX"
        assert fields["show_image_count"].default is True

    def test_existing_fields_preserved(self, module: DownloadedImagesModule) -> None:
        """Existing filter fields are still present."""
        field_keys = {f.key for f in module.get_filter_fields()}
        assert "domain_filter" in field_keys
        assert "tag_filter" in field_keys
        assert "sort_by" in field_keys
        assert "show_filter_info" in field_keys


class TestDownloadedImagesRender:
    """Test render with title, description, and limit."""

    def test_render_with_title(
        self,
        module: DownloadedImagesModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Title appears in rendered output."""
        config = {
            "section_title": "My Custom Title",
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        assert "My Custom Title" in html
        assert "module-title" in html

    def test_render_without_title(
        self,
        module: DownloadedImagesModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """No title element when section_title is empty."""
        config = {
            "section_title": "",
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        assert '<h3 class="module-title">' not in html

    def test_render_with_description(
        self,
        module: DownloadedImagesModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Description appears in rendered output."""
        config = {
            "section_description": "This is a custom description.",
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        assert "This is a custom description." in html
        assert "module-description" in html

    def test_render_without_description(
        self,
        module: DownloadedImagesModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """No description element when section_description is empty."""
        config = {
            "section_description": "",
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        assert '<p class="module-description">' not in html

    def test_render_limit_applied(
        self,
        module: DownloadedImagesModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Limit restricts the number of images shown."""
        config = {
            "limit": "9",
            "show_image_count": True,
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        # 12 total images, limited to 9
        assert "download-card" in html
        # Count actual card div elements (not CSS references)
        card_count = html.count('<div class="download-card">')
        assert card_count == 9

    def test_render_limit_all(
        self,
        module: DownloadedImagesModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """All images shown when limit is 'all'."""
        config = {
            "limit": "all",
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        card_count = html.count('<div class="download-card">')
        assert card_count == 12

    def test_render_shows_limit_note(
        self,
        module: DownloadedImagesModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Limit note is shown when limit < total and show_image_count is True."""
        config = {
            "limit": "9",
            "show_image_count": True,
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        assert "Showing 9 of 12 downloaded images" in html

    def test_render_no_limit_note_when_disabled(
        self,
        module: DownloadedImagesModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """Limit note is hidden when show_image_count is False."""
        config = {
            "limit": "9",
            "show_image_count": False,
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        assert '<p class="limit-note">' not in html

    def test_render_no_limit_note_when_all_shown(
        self,
        module: DownloadedImagesModule,
        test_db: sqlite3.Connection,
    ) -> None:
        """No limit note when all images are within the limit."""
        config = {
            "limit": "all",
            "show_image_count": True,
            "_translations": get_translations("en"),
        }
        html = module.render(test_db, 1, config)
        assert '<p class="limit-note">' not in html


class TestDownloadedImagesTranslations:
    """Test translation keys exist for both locales."""

    def test_en_translations(self) -> None:
        """English translations exist."""
        t = get_translations("en")
        assert "downloaded_images_default_title" in t
        assert "showing_x_of_y_downloaded" in t
        assert t["downloaded_images_default_title"] == "Downloaded Images"

    def test_de_translations(self) -> None:
        """German translations exist."""
        t = get_translations("de")
        assert "downloaded_images_default_title" in t
        assert "showing_x_of_y_downloaded" in t
        assert t["downloaded_images_default_title"] == "Heruntergeladene Bilder"

    def test_en_appendix_translations(self) -> None:
        """English appendix translations exist."""
        t = get_translations("en")
        assert "appendix_download_list_title" in t
        assert "appendix_download_list_empty" in t

    def test_de_appendix_translations(self) -> None:
        """German appendix translations exist."""
        t = get_translations("de")
        assert "appendix_download_list_title" in t
        assert "appendix_download_list_empty" in t
