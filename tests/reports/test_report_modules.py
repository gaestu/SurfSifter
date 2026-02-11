"""Tests for the report modules system."""

import json
import sqlite3
from typing import Any, Dict, Generator, List

import pytest

from reports.modules import (
    BaseReportModule,
    FilterField,
    FilterType,
    ModuleMetadata,
    ModuleRegistry,
)
from reports.database.module_helpers import (
    insert_section_module,
    update_section_module,
    delete_section_module,
    get_section_modules,
    get_section_module_by_id,
    reorder_section_module,
    delete_modules_by_section,
    get_modules_count_by_section,
)
from reports.database.helpers import insert_custom_section, _ensure_table


# --- Test Fixtures ---

@pytest.fixture
def db_conn() -> Generator[sqlite3.Connection, None, None]:
    """Create an in-memory SQLite database for testing."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    # Ensure sections table exists
    _ensure_table(conn)
    yield conn
    conn.close()


@pytest.fixture
def section_id(db_conn: sqlite3.Connection) -> int:
    """Create a test section and return its ID."""
    return insert_custom_section(db_conn, evidence_id=1, title="Test Section")


# --- Test Module Classes ---

class MockModule(BaseReportModule):
    """Mock module for testing."""

    @property
    def metadata(self) -> ModuleMetadata:
        return ModuleMetadata(
            module_id="mock_module",
            name="Mock Module",
            description="A mock module for testing",
            icon="🧪",
            category="Testing",
        )

    def get_filter_fields(self) -> List[FilterField]:
        return [
            FilterField(
                key="tag",
                label="Tag",
                filter_type=FilterType.TEXT,
                required=True,
            ),
            FilterField(
                key="limit",
                label="Limit",
                filter_type=FilterType.NUMBER,
                default=10,
            ),
        ]

    def render(
        self,
        db_conn: sqlite3.Connection,
        evidence_id: int,
        config: Dict[str, Any],
    ) -> str:
        tag = config.get("tag", "")
        limit = config.get("limit", 10)
        return f"<div>Tag: {tag}, Limit: {limit}</div>"


# --- Base Module Tests ---

class TestBaseReportModule:
    """Tests for BaseReportModule ABC."""

    def test_metadata_property(self) -> None:
        """Test metadata property returns ModuleMetadata."""
        module = MockModule()
        meta = module.metadata

        assert meta.module_id == "mock_module"
        assert meta.name == "Mock Module"
        assert meta.icon == "🧪"
        assert meta.category == "Testing"

    def test_get_filter_fields(self) -> None:
        """Test get_filter_fields returns list of FilterField."""
        module = MockModule()
        fields = module.get_filter_fields()

        assert len(fields) == 2
        assert fields[0].key == "tag"
        assert fields[0].required is True
        assert fields[1].key == "limit"
        assert fields[1].default == 10

    def test_render(self, db_conn: sqlite3.Connection) -> None:
        """Test render produces HTML output."""
        module = MockModule()
        html = module.render(db_conn, evidence_id=1, config={"tag": "test", "limit": 5})

        assert "Tag: test" in html
        assert "Limit: 5" in html

    def test_format_config_summary_with_values(self) -> None:
        """Test format_config_summary with config values."""
        module = MockModule()
        summary = module.format_config_summary({"tag": "important", "limit": 20})

        assert "Tag: important" in summary
        assert "Limit: 20" in summary

    def test_format_config_summary_empty(self) -> None:
        """Test format_config_summary with empty config."""
        module = MockModule()
        summary = module.format_config_summary({})

        assert summary == "No filters configured"

    def test_validate_config_valid(self) -> None:
        """Test validate_config with valid config."""
        module = MockModule()
        errors = module.validate_config({"tag": "test"})

        assert errors == []

    def test_validate_config_missing_required(self) -> None:
        """Test validate_config with missing required field."""
        module = MockModule()
        errors = module.validate_config({})

        assert len(errors) == 1
        assert "Tag is required" in errors[0]

    def test_get_default_config(self) -> None:
        """Test get_default_config returns defaults."""
        module = MockModule()
        defaults = module.get_default_config()

        assert defaults == {"limit": 10}


# --- Module Registry Tests ---

class TestModuleRegistry:
    """Tests for ModuleRegistry."""

    def test_singleton_pattern(self) -> None:
        """Test registry is singleton."""
        reg1 = ModuleRegistry()
        reg2 = ModuleRegistry()

        assert reg1 is reg2

    def test_manual_register(self) -> None:
        """Test manually registering a module."""
        registry = ModuleRegistry()
        registry.register(MockModule)

        assert registry.is_registered("mock_module")

    def test_get_module(self) -> None:
        """Test getting a registered module."""
        registry = ModuleRegistry()
        registry.register(MockModule)

        module = registry.get_module("mock_module")

        assert module is not None
        assert module.metadata.module_id == "mock_module"

    def test_get_module_not_found(self) -> None:
        """Test getting non-existent module returns None."""
        registry = ModuleRegistry()

        assert registry.get_module("nonexistent") is None

    def test_list_modules(self) -> None:
        """Test listing all module metadata."""
        registry = ModuleRegistry()
        registry.register(MockModule)

        modules = registry.list_modules()

        assert any(m.module_id == "mock_module" for m in modules)

    def test_get_all_module_ids(self) -> None:
        """Test getting all module IDs."""
        registry = ModuleRegistry()
        registry.register(MockModule)

        ids = registry.get_all_module_ids()

        assert "mock_module" in ids


# --- Section Module Database Tests ---

class TestSectionModuleDatabase:
    """Tests for section_modules database helpers."""

    def test_insert_module(self, db_conn: sqlite3.Connection, section_id: int) -> None:
        """Test inserting a module instance."""
        module_instance_id = insert_section_module(
            db_conn,
            section_id,
            "tagged_urls",
            {"tags": ["important"]},
        )

        assert module_instance_id > 0

        # Verify
        mod = get_section_module_by_id(db_conn, module_instance_id)
        assert mod is not None
        assert mod["module_id"] == "tagged_urls"
        assert mod["config"]["tags"] == ["important"]
        assert mod["sort_order"] == 0

    def test_insert_multiple_modules_auto_order(self, db_conn: sqlite3.Connection, section_id: int) -> None:
        """Test that sort_order is auto-incremented."""
        id1 = insert_section_module(db_conn, section_id, "mod1")
        id2 = insert_section_module(db_conn, section_id, "mod2")
        id3 = insert_section_module(db_conn, section_id, "mod3")

        m1 = get_section_module_by_id(db_conn, id1)
        m2 = get_section_module_by_id(db_conn, id2)
        m3 = get_section_module_by_id(db_conn, id3)

        assert m1["sort_order"] == 0
        assert m2["sort_order"] == 1
        assert m3["sort_order"] == 2

    def test_update_module_config(self, db_conn: sqlite3.Connection, section_id: int) -> None:
        """Test updating module configuration."""
        mod_id = insert_section_module(db_conn, section_id, "test", {"old": True})

        result = update_section_module(db_conn, mod_id, config={"new": True})

        assert result is True
        mod = get_section_module_by_id(db_conn, mod_id)
        assert mod["config"] == {"new": True}

    def test_delete_module(self, db_conn: sqlite3.Connection, section_id: int) -> None:
        """Test deleting a module instance."""
        mod_id = insert_section_module(db_conn, section_id, "test")

        result = delete_section_module(db_conn, mod_id)

        assert result is True
        assert get_section_module_by_id(db_conn, mod_id) is None

    def test_delete_nonexistent_module(self, db_conn: sqlite3.Connection) -> None:
        """Test deleting a module that doesn't exist."""
        result = delete_section_module(db_conn, 9999)
        assert result is False

    def test_get_section_modules_ordered(self, db_conn: sqlite3.Connection, section_id: int) -> None:
        """Test that get_section_modules returns sorted by sort_order."""
        insert_section_module(db_conn, section_id, "third", sort_order=2)
        insert_section_module(db_conn, section_id, "first", sort_order=0)
        insert_section_module(db_conn, section_id, "second", sort_order=1)

        modules = get_section_modules(db_conn, section_id)

        assert len(modules) == 3
        assert modules[0]["module_id"] == "first"
        assert modules[1]["module_id"] == "second"
        assert modules[2]["module_id"] == "third"

    def test_get_section_modules_empty(self, db_conn: sqlite3.Connection, section_id: int) -> None:
        """Test get_section_modules returns empty list for no modules."""
        modules = get_section_modules(db_conn, section_id)
        assert modules == []

    def test_reorder_module_move_up(self, db_conn: sqlite3.Connection, section_id: int) -> None:
        """Test moving a module up."""
        id1 = insert_section_module(db_conn, section_id, "mod1")
        id2 = insert_section_module(db_conn, section_id, "mod2")
        id3 = insert_section_module(db_conn, section_id, "mod3")

        # Move mod3 (order=2) to position 0
        result = reorder_section_module(db_conn, id3, 0)

        assert result is True

        modules = get_section_modules(db_conn, section_id)
        ids = [m["module_id"] for m in modules]
        assert ids == ["mod3", "mod1", "mod2"]

    def test_reorder_module_move_down(self, db_conn: sqlite3.Connection, section_id: int) -> None:
        """Test moving a module down."""
        id1 = insert_section_module(db_conn, section_id, "mod1")
        id2 = insert_section_module(db_conn, section_id, "mod2")
        id3 = insert_section_module(db_conn, section_id, "mod3")

        # Move mod1 (order=0) to position 2
        result = reorder_section_module(db_conn, id1, 2)

        assert result is True

        modules = get_section_modules(db_conn, section_id)
        ids = [m["module_id"] for m in modules]
        assert ids == ["mod2", "mod3", "mod1"]

    def test_delete_modules_by_section(self, db_conn: sqlite3.Connection, section_id: int) -> None:
        """Test deleting all modules for a section."""
        insert_section_module(db_conn, section_id, "mod1")
        insert_section_module(db_conn, section_id, "mod2")
        insert_section_module(db_conn, section_id, "mod3")

        count = delete_modules_by_section(db_conn, section_id)

        assert count == 3
        assert get_section_modules(db_conn, section_id) == []

    def test_get_modules_count_by_section(self, db_conn: sqlite3.Connection, section_id: int) -> None:
        """Test counting modules in a section."""
        assert get_modules_count_by_section(db_conn, section_id) == 0

        insert_section_module(db_conn, section_id, "mod1")
        insert_section_module(db_conn, section_id, "mod2")

        assert get_modules_count_by_section(db_conn, section_id) == 2


# --- Filter Types Tests ---

class TestFilterTypes:
    """Tests for FilterType enum and FilterField dataclass."""

    def test_filter_type_values(self) -> None:
        """Test FilterType enum has expected values."""
        assert FilterType.TAG_SELECT.value == "tag_select"
        assert FilterType.DATE_RANGE.value == "date_range"
        assert FilterType.TEXT.value == "text"
        assert FilterType.CHECKBOX.value == "checkbox"

    def test_filter_field_defaults(self) -> None:
        """Test FilterField default values."""
        field = FilterField(
            key="test",
            label="Test",
            filter_type=FilterType.TEXT,
        )

        assert field.required is False
        assert field.default is None
        assert field.options is None
        assert field.help_text is None

    def test_filter_field_with_options(self) -> None:
        """Test FilterField with options."""
        field = FilterField(
            key="browser",
            label="Browser",
            filter_type=FilterType.DROPDOWN,
            options=[
                ("chrome", "Chrome"),
                ("firefox", "Firefox"),
            ],
        )

        assert len(field.options) == 2
        assert field.options[0] == ("chrome", "Chrome")


# --- Module Metadata Tests ---

class TestModuleMetadata:
    """Tests for ModuleMetadata dataclass."""

    def test_metadata_defaults(self) -> None:
        """Test ModuleMetadata default values."""
        meta = ModuleMetadata(
            module_id="test",
            name="Test",
            description="A test module",
        )

        assert meta.icon == "📦"
        assert meta.category == "General"
        assert meta.version
        assert "." in meta.version

    def test_metadata_custom_values(self) -> None:
        """Test ModuleMetadata with custom values."""
        meta = ModuleMetadata(
            module_id="custom",
            name="Custom Module",
            description="Custom description",
            icon="🎯",
            category="Custom",
            version="2.0.0",
        )

        assert meta.icon == "🎯"
        assert meta.category == "Custom"
        assert meta.version == "2.0.0"


# --- Images Module Tests ---

class TestImagesModule:
    """Tests for the ImagesModule report module."""

    def test_images_module_registered(self) -> None:
        """Test that images module is auto-discovered by registry."""
        registry = ModuleRegistry()
        assert registry.is_registered("images")

    def test_images_module_metadata(self) -> None:
        """Test images module metadata."""
        registry = ModuleRegistry()
        module = registry.get_module("images")

        assert module is not None
        meta = module.metadata
        assert meta.module_id == "images"
        assert meta.name == "Images"
        assert meta.category == "Images"
        assert meta.icon == "🖼️"

    def test_images_module_filter_fields(self) -> None:
        """Test images module filter fields."""
        registry = ModuleRegistry()
        module = registry.get_module("images")

        assert module is not None
        fields = module.get_filter_fields()

        # Should have 8 filters: title, tag_filter, match_filter, include_filepath, include_url, sort_by, show_filter_info, limit
        assert len(fields) == 8

        field_keys = [f.key for f in fields]
        assert "title" in field_keys
        assert "tag_filter" in field_keys
        assert "match_filter" in field_keys
        assert "include_filepath" in field_keys
        assert "include_url" in field_keys
        assert "sort_by" in field_keys
        assert "show_filter_info" in field_keys
        assert "limit" in field_keys

    def test_images_module_default_config(self) -> None:
        """Test images module default configuration."""
        registry = ModuleRegistry()
        module = registry.get_module("images")

        assert module is not None
        defaults = module.get_default_config()

        assert defaults["tag_filter"] == "all"
        assert defaults["match_filter"] == "all"
        assert defaults["include_filepath"] is False
        assert defaults["sort_by"] == "date_desc"

    def test_images_module_render_empty(self, db_conn: sqlite3.Connection) -> None:
        """Test images module renders with empty database."""
        # Create required tables
        db_conn.execute("""
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                rel_path TEXT,
                filename TEXT,
                md5 TEXT,
                sha256 TEXT,
                ts_utc TEXT,
                exif_json TEXT,
                size_bytes INTEGER,
                first_discovered_by TEXT
            )
        """)
        db_conn.commit()

        registry = ModuleRegistry()
        module = registry.get_module("images")

        assert module is not None
        html = module.render(db_conn, evidence_id=1, config={})

        assert "No images found" in html
        # Filter info text not shown by default (show_filter_info=False)
        assert "<strong>Filter:</strong>" not in html

        # With show_filter_info=True, filter info should be shown
        html_with_info = module.render(db_conn, evidence_id=1, config={"show_filter_info": True})
        assert "0 images" in html_with_info
        assert "<strong>Filter:</strong>" in html_with_info

    def test_images_module_render_with_data(self, db_conn: sqlite3.Connection) -> None:
        """Test images module renders with image data."""
        # Create required tables
        db_conn.execute("""
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY,
                evidence_id INTEGER,
                rel_path TEXT,
                filename TEXT,
                md5 TEXT,
                sha256 TEXT,
                ts_utc TEXT,
                exif_json TEXT,
                size_bytes INTEGER,
                first_discovered_by TEXT
            )
        """)
        # Insert test image
        db_conn.execute("""
            INSERT INTO images (evidence_id, rel_path, filename, md5, ts_utc, size_bytes, first_discovered_by)
            VALUES (1, '/test/image.jpg', 'image.jpg', 'abc123def456', '2026-01-17 12:00:00', 1024, 'bulk_extractor')
        """)
        db_conn.commit()

        registry = ModuleRegistry()
        module = registry.get_module("images")

        assert module is not None
        html = module.render(db_conn, evidence_id=1, config={})

        # Filter info text not shown by default (show_filter_info=False)
        assert "<strong>Filter:</strong>" not in html
        assert "image.jpg" in html
        assert "abc123def456" in html

        # With show_filter_info=True, filter info should be shown
        html_with_info = module.render(db_conn, evidence_id=1, config={"show_filter_info": True})
        assert "1 images" in html_with_info
        assert "<strong>Filter:</strong>" in html_with_info

    def test_images_module_build_filter_description(self) -> None:
        """Test filter description generation."""
        registry = ModuleRegistry()
        module = registry.get_module("images")

        assert module is not None
        # Access private method for testing
        desc = module._build_filter_description("all", "all")
        assert "All tags" in desc
        assert "All matches" in desc

        desc = module._build_filter_description("any_tag", "any_match")
        assert "Any tagged" in desc
        assert "Any hash match" in desc

        desc = module._build_filter_description("important", "known_bad")
        assert "Tag: important" in desc
        assert "Match: known_bad" in desc

    def test_images_module_resolve_path_uses_canonical_slug(self, tmp_path) -> None:
        """Test path resolution uses canonical slugify_label function.

        Bug fix: Previously used incorrect inline slug logic that:
        - Removed hyphens (not alphanumeric)
        - Appended _evidence_id suffix
        This caused paths like ev4delllatitudecpi_1 instead of ev-4dell-latitude-cpi.
        """
        from core.database.manager import slugify_label

        # Create test directory structure
        case_folder = tmp_path / "case"
        case_folder.mkdir()

        # Test label that has spaces and numbers at start
        evidence_label = "4Dell Latitude CPi"
        evidence_id = 1

        # Expected slug from canonical function
        expected_slug = slugify_label(evidence_label, evidence_id)
        assert expected_slug == "ev-4dell-latitude-cpi"  # Verify canonical behavior

        # Create evidence directory with correct slug
        evidence_dir = case_folder / "evidences" / expected_slug / "bulk_extractor" / "jpeg"
        evidence_dir.mkdir(parents=True)

        # Create test image
        test_image = evidence_dir / "test.jpg"
        test_image.write_bytes(b"fake image data")

        # Test module path resolution
        registry = ModuleRegistry()
        module = registry.get_module("images")
        assert module is not None

        resolved = module._resolve_image_path(
            rel_path="jpeg/test.jpg",
            discovered_by="bulk_extractor",
            case_folder=case_folder,
            evidence_id=evidence_id,
            evidence_label=evidence_label,
        )

        assert resolved is not None
        assert resolved.exists()
        assert "ev-4dell-latitude-cpi" in str(resolved)
        # Ensure old buggy pattern NOT in path
        assert "ev4delllatitudecpi_1" not in str(resolved)

