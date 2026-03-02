"""Tests for Tagged File List Report Module."""

import sqlite3
import tempfile
from pathlib import Path

import pytest

from reports.modules.base import FilterType
from reports.modules.tagged_file_list.module import TaggedFileListModule


@pytest.fixture
def module():
    """Create a TaggedFileListModule instance."""
    return TaggedFileListModule()


@pytest.fixture
def test_db():
    """Create an in-memory test database with file_list schema."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE file_list (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            file_name TEXT,
            extension TEXT,
            size_bytes INTEGER,
            modified_ts TEXT,
            deleted INTEGER DEFAULT 0
        );

        CREATE TABLE tags (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );

        CREATE TABLE tag_associations (
            id INTEGER PRIMARY KEY,
            artifact_type TEXT NOT NULL,
            artifact_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL
        );

        CREATE TABLE file_list_matches (
            id INTEGER PRIMARY KEY,
            file_list_id INTEGER NOT NULL,
            reference_list_name TEXT NOT NULL
        );
    """)

    # Insert test data
    conn.executescript("""
        -- Files
        INSERT INTO file_list (id, evidence_id, file_path, file_name, modified_ts, deleted)
        VALUES
            (1, 1, '/home/user/doc.txt', 'doc.txt', '2024-01-15 10:30:00', 0),
            (2, 1, '/home/user/image.jpg', 'image.jpg', '2024-01-14 09:00:00', 0),
            (3, 1, '/home/user/deleted.log', 'deleted.log', '2024-01-10 08:00:00', 1),
            (4, 1, '/var/log/system.log', 'system.log', '2024-01-16 12:00:00', 0);

        -- Tags
        INSERT INTO tags (id, name) VALUES (1, 'important'), (2, 'review');

        -- Tag associations (file 1 and 2 have tags)
        INSERT INTO tag_associations (artifact_type, artifact_id, tag_id)
        VALUES
            ('file_list', 1, 1),
            ('file_list', 2, 2);

        -- Matches (file 1 and 4 match reference lists)
        INSERT INTO file_list_matches (file_list_id, reference_list_name)
        VALUES
            (1, 'browser_artifacts'),
            (4, 'system_cleaners');
    """)
    conn.commit()

    yield conn
    conn.close()


class TestModuleMetadata:
    """Tests for module metadata."""

    def test_metadata_module_id(self, module):
        """Test module ID is set correctly."""
        assert module.metadata.module_id == "tagged_file_list"

    def test_metadata_name(self, module):
        """Test module name is set."""
        assert module.metadata.name == "Tagged File List"

    def test_metadata_category(self, module):
        """Test module category is Files."""
        assert module.metadata.category == "Files"

    def test_metadata_icon(self, module):
        """Test module has an icon."""
        assert module.metadata.icon == "📁"


class TestFilterFields:
    """Tests for filter field definitions."""

    def test_has_eight_filters(self, module):
        """Test module defines 8 filter fields."""
        fields = module.get_filter_fields()
        assert len(fields) == 8

    def test_show_title_field(self, module):
        """Test show_title field properties."""
        fields = {f.key: f for f in module.get_filter_fields()}
        show_title = fields["show_title"]

        assert show_title.filter_type == FilterType.CHECKBOX
        assert show_title.default is True
        assert show_title.required is False

    def test_limit_field(self, module):
        """Test limit field properties."""
        fields = {f.key: f for f in module.get_filter_fields()}
        limit = fields["limit"]

        assert limit.filter_type == FilterType.DROPDOWN
        assert limit.default == "unlimited"
        assert limit.required is False
        # Check options: 5, 10, 25, 50, Unlimited
        option_values = [opt[0] for opt in limit.options]
        assert option_values == ["5", "10", "25", "50", "unlimited"]

    def test_custom_title_field(self, module):
        """Test custom_title field properties."""
        fields = {f.key: f for f in module.get_filter_fields()}
        custom_title = fields["custom_title"]

        assert custom_title.filter_type == FilterType.TEXT
        assert custom_title.required is False

    def test_tag_filter_field(self, module):
        """Test tag_filter field properties."""
        fields = {f.key: f for f in module.get_filter_fields()}
        tag_filter = fields["tag_filter"]

        assert tag_filter.filter_type == FilterType.DROPDOWN
        assert tag_filter.default == "all"
        assert tag_filter.required is False

    def test_match_filter_field(self, module):
        """Test match_filter field properties."""
        fields = {f.key: f for f in module.get_filter_fields()}
        match_filter = fields["match_filter"]

        assert match_filter.filter_type == FilterType.DROPDOWN
        assert match_filter.default == "all"

    def test_include_deleted_field(self, module):
        """Test include_deleted field properties."""
        fields = {f.key: f for f in module.get_filter_fields()}
        deleted = fields["include_deleted"]

        assert deleted.filter_type == FilterType.CHECKBOX
        assert deleted.default is False

    def test_sort_by_field(self, module):
        """Test sort_by field properties."""
        fields = {f.key: f for f in module.get_filter_fields()}
        sort_by = fields["sort_by"]

        assert sort_by.filter_type == FilterType.DROPDOWN
        assert sort_by.default == "modified_desc"
        assert len(sort_by.options) == 6  # 6 sort options

    def test_show_filter_info_field(self, module):
        """Test show_filter_info field properties."""
        fields = {f.key: f for f in module.get_filter_fields()}
        show_filter_info = fields["show_filter_info"]

        assert show_filter_info.filter_type == FilterType.CHECKBOX
        assert show_filter_info.default is False


class TestDynamicOptions:
    """Tests for dynamic option loading."""

    def test_tag_options_includes_base(self, module, test_db):
        """Test tag options include All and Any Tag."""
        options = module.get_dynamic_options("tag_filter", test_db)
        values = [v for v, _ in options]

        assert "all" in values
        assert "any_tag" in values

    def test_tag_options_loads_tags(self, module, test_db):
        """Test tag options loads tags from database."""
        options = module.get_dynamic_options("tag_filter", test_db)
        values = [v for v, _ in options]

        assert "important" in values
        assert "review" in values

    def test_match_options_includes_base(self, module, test_db):
        """Test match options include All and Any Match."""
        options = module.get_dynamic_options("match_filter", test_db)
        values = [v for v, _ in options]

        assert "all" in values
        assert "any_match" in values

    def test_match_options_loads_lists(self, module, test_db):
        """Test match options loads reference lists from database."""
        options = module.get_dynamic_options("match_filter", test_db)
        values = [v for v, _ in options]

        assert "browser_artifacts" in values
        assert "system_cleaners" in values

    def test_unknown_key_returns_none(self, module, test_db):
        """Test unknown key returns None."""
        result = module.get_dynamic_options("unknown_key", test_db)
        assert result is None


class TestRender:
    """Tests for render method."""

    def test_render_all_files(self, module, test_db):
        """Test rendering all files (default filters)."""
        config = {
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        # Should include non-deleted files
        assert "doc.txt" in html
        assert "image.jpg" in html
        assert "system.log" in html
        # Should not include deleted
        assert "deleted.log" not in html

    def test_render_with_deleted(self, module, test_db):
        """Test rendering includes deleted when flag set."""
        config = {
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": True,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        assert "deleted.log" in html
        assert "[deleted]" in html

    def test_render_filter_any_tag(self, module, test_db):
        """Test filtering by any tag."""
        config = {
            "tag_filter": "any_tag",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        # Only tagged files
        assert "doc.txt" in html
        assert "image.jpg" in html
        # Untagged files excluded
        assert "system.log" not in html

    def test_render_filter_specific_tag(self, module, test_db):
        """Test filtering by specific tag."""
        config = {
            "tag_filter": "important",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        # Only files with 'important' tag
        assert "doc.txt" in html
        # Files with other tags excluded
        assert "image.jpg" not in html

    def test_render_filter_any_match(self, module, test_db):
        """Test filtering by any match."""
        config = {
            "tag_filter": "all",
            "match_filter": "any_match",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        # Only files with matches
        assert "doc.txt" in html
        assert "system.log" in html
        # Files without matches excluded
        assert "image.jpg" not in html

    def test_render_filter_specific_match(self, module, test_db):
        """Test filtering by specific reference list."""
        config = {
            "tag_filter": "all",
            "match_filter": "browser_artifacts",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        # Only files matching browser_artifacts
        assert "doc.txt" in html
        # Other files excluded
        assert "system.log" not in html

    def test_render_empty_result(self, module, test_db):
        """Test rendering with no matching files."""
        config = {
            "tag_filter": "nonexistent_tag",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
            "show_filter_info": True,
        }

        html = module.render(test_db, 1, config)

        assert "No files found" in html
        assert "(0 files)" in html


class TestDateFormatting:
    """Tests for date formatting."""

    def test_format_european_date(self, module, test_db):
        """Test dates are formatted as DD.MM.YYYY HH:MM."""
        config = {
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        # Check European date format
        assert "15.01.2024 10:30" in html
        assert "14.01.2024 09:00" in html


class TestSorting:
    """Tests for sort order."""

    def test_sort_modified_desc(self, module, test_db):
        """Test sorting by modified date descending."""
        config = {
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        # system.log (Jan 16) should appear before doc.txt (Jan 15)
        pos_system = html.find("system.log")
        pos_doc = html.find("doc.txt")
        assert pos_system < pos_doc

    def test_sort_name_asc(self, module, test_db):
        """Test sorting by name ascending."""
        config = {
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "name_asc",
        }

        html = module.render(test_db, 1, config)

        # doc.txt should appear before image.jpg (alphabetical)
        pos_doc = html.find("doc.txt")
        pos_image = html.find("image.jpg")
        assert pos_doc < pos_image


class TestConfigSummary:
    """Tests for format_config_summary."""

    def test_summary_default_all(self, module):
        """Test summary for default config."""
        config = {
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
        }

        summary = module.format_config_summary(config)
        assert summary == "All files"

    def test_summary_any_tag(self, module):
        """Test summary with any tag filter."""
        config = {
            "tag_filter": "any_tag",
            "match_filter": "all",
            "include_deleted": False,
        }

        summary = module.format_config_summary(config)
        assert "Any Tag" in summary

    def test_summary_specific_tag(self, module):
        """Test summary with specific tag."""
        config = {
            "tag_filter": "important",
            "match_filter": "all",
            "include_deleted": False,
        }

        summary = module.format_config_summary(config)
        assert "Tag: important" in summary

    def test_summary_with_deleted(self, module):
        """Test summary includes +Deleted."""
        config = {
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": True,
        }

        summary = module.format_config_summary(config)
        assert "+Deleted" in summary

    def test_summary_multiple_filters(self, module):
        """Test summary with multiple filters."""
        config = {
            "tag_filter": "any_tag",
            "match_filter": "any_match",
            "include_deleted": True,
        }

        summary = module.format_config_summary(config)
        assert "Any Tag" in summary
        assert "Any Match" in summary
        assert "+Deleted" in summary

    def test_summary_with_custom_title(self, module):
        """Test summary includes custom title when set."""
        config = {
            "custom_title": "Sample Files",
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
        }

        summary = module.format_config_summary(config)
        assert "Title: Sample Files" in summary


class TestModuleDiscovery:
    """Tests for module auto-discovery."""

    def test_module_discovered(self):
        """Test module is discovered by registry."""
        from reports.modules import ModuleRegistry

        registry = ModuleRegistry()
        modules = registry.list_modules()
        module_ids = [m.module_id for m in modules]

        assert "tagged_file_list" in module_ids

    def test_module_in_files_category(self):
        """Test module appears in Files category."""
        from reports.modules import ModuleRegistry

        registry = ModuleRegistry()
        by_category = registry.list_modules_by_category()

        assert "Files" in by_category
        module_ids = [m.module_id for m in by_category["Files"]]
        assert "tagged_file_list" in module_ids


class TestTitleFeature:
    """Tests for optional title feature."""

    def test_title_shown_by_default(self, module, test_db):
        """Test title is shown when show_title=True (default)."""
        config = {
            "show_title": True,
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        assert "File List" in html
        assert 'class="module-title"' in html

    def test_title_hidden_when_disabled(self, module, test_db):
        """Test title is hidden when show_title=False."""
        config = {
            "show_title": False,
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        assert 'class="module-title"' not in html

    def test_title_localized_german(self, module, test_db):
        """Test title is localized to German."""
        from reports.locales import get_translations

        config = {
            "show_title": True,
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
            "_locale": "de",
            "_translations": get_translations("de"),
        }

        html = module.render(test_db, 1, config)

        assert "Dateiliste" in html

    def test_title_custom_title_overrides_default(self, module, test_db):
        """Test custom title is rendered when provided."""
        config = {
            "show_title": True,
            "custom_title": "Sample Files",
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        assert "Sample Files" in html
        assert "File List" not in html

    def test_title_custom_title_whitespace_uses_default(self, module, test_db):
        """Test whitespace-only custom title falls back to default title."""
        config = {
            "show_title": True,
            "custom_title": "   ",
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        assert "File List" in html

    def test_title_custom_title_none_uses_default(self, module, test_db):
        """Test None custom title falls back to default title."""
        config = {
            "show_title": True,
            "custom_title": None,
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
            "sort_by": "modified_desc",
        }

        html = module.render(test_db, 1, config)

        assert "File List" in html


class TestLimitFeature:
    """Tests for entry limit feature."""

    def test_limit_unlimited_shows_all(self, module, test_db):
        """Test unlimited shows all files."""
        config = {
            "limit": "unlimited",
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": True,
            "sort_by": "name_asc",
        }

        html = module.render(test_db, 1, config)

        # All files should be present
        assert "doc.txt" in html
        assert "image.jpg" in html
        assert "system.log" in html
        assert "deleted.log" in html
        # No truncation indicator element (CSS class exists in style block)
        assert '<p class="truncation-info">' not in html

    def test_limit_restricts_entries(self, module, test_db):
        """Test limit restricts number of shown files."""
        config = {
            "limit": "2",
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": True,
            "sort_by": "name_asc",
        }

        html = module.render(test_db, 1, config)

        # Should show truncation info element
        assert '<p class="truncation-info">' in html
        # "showing 2 of 4"
        assert "2" in html and "4" in html

    def test_limit_truncation_localized(self, module, test_db):
        """Test truncation indicator is localized."""
        from reports.locales import get_translations

        config = {
            "limit": "2",
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": True,
            "sort_by": "name_asc",
            "_locale": "de",
            "_translations": get_translations("de"),
        }

        html = module.render(test_db, 1, config)

        # German: "zeige X von Y"
        assert "zeige" in html or "von" in html
        assert "Dateien" in html

    def test_config_summary_with_limit(self, module):
        """Test config summary includes limit."""
        config = {
            "limit": "25",
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
        }

        summary = module.format_config_summary(config)
        assert "Limit: 25" in summary

    def test_config_summary_unlimited_not_shown(self, module):
        """Test config summary omits unlimited."""
        config = {
            "limit": "unlimited",
            "tag_filter": "all",
            "match_filter": "all",
            "include_deleted": False,
        }

        summary = module.format_config_summary(config)
        assert "Limit" not in summary
