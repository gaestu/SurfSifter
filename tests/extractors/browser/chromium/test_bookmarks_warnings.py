"""
Tests for Chromium Bookmarks schema warning support.

Tests cover:
- Unknown root-level JSON keys detection
- Unknown root folder detection
- Unknown bookmark node keys detection
- Unknown bookmark type detection
- Unknown meta_info keys detection
- Warning collector integration with parser
- JSON parse error handling

Initial tests for schema warning feature
"""

import json
import pytest
from unittest.mock import MagicMock

from extractors.browser.chromium.bookmarks._parser import (
    parse_bookmarks_json,
    get_bookmark_stats,
    ChromiumBookmark,
)
from extractors.browser.chromium.bookmarks._schemas import (
    KNOWN_ROOT_KEYS,
    KNOWN_ROOT_FOLDER_KEYS,
    KNOWN_BOOKMARK_NODE_KEYS,
    KNOWN_BOOKMARK_TYPES,
    KNOWN_META_INFO_KEYS,
)
from extractors._shared.extraction_warnings import ExtractionWarningCollector


# =============================================================================
# Test Schema Definitions
# =============================================================================

class TestSchemaDefinitions:
    """Test schema definition completeness."""

    def test_known_root_keys_not_empty(self):
        """Known root keys set is populated."""
        assert len(KNOWN_ROOT_KEYS) >= 3
        assert "roots" in KNOWN_ROOT_KEYS
        assert "version" in KNOWN_ROOT_KEYS
        assert "checksum" in KNOWN_ROOT_KEYS

    def test_known_root_folders_not_empty(self):
        """Known root folders set is populated."""
        assert len(KNOWN_ROOT_FOLDER_KEYS) >= 3
        assert "bookmark_bar" in KNOWN_ROOT_FOLDER_KEYS
        assert "other" in KNOWN_ROOT_FOLDER_KEYS
        assert "synced" in KNOWN_ROOT_FOLDER_KEYS

    def test_known_bookmark_types(self):
        """Known bookmark types include url and folder."""
        assert "url" in KNOWN_BOOKMARK_TYPES
        assert "folder" in KNOWN_BOOKMARK_TYPES

    def test_known_node_keys_include_core_fields(self):
        """Known node keys include essential fields."""
        assert "id" in KNOWN_BOOKMARK_NODE_KEYS
        assert "name" in KNOWN_BOOKMARK_NODE_KEYS
        assert "url" in KNOWN_BOOKMARK_NODE_KEYS
        assert "type" in KNOWN_BOOKMARK_NODE_KEYS
        assert "children" in KNOWN_BOOKMARK_NODE_KEYS
        assert "date_added" in KNOWN_BOOKMARK_NODE_KEYS
        assert "guid" in KNOWN_BOOKMARK_NODE_KEYS


# =============================================================================
# Test Warning Collector Integration
# =============================================================================

class TestWarningCollector:
    """Test warning collector integration with parser."""

    @pytest.fixture
    def warning_collector(self):
        """Create a warning collector for testing."""
        return ExtractionWarningCollector(
            extractor_name="chromium_bookmarks",
            run_id="test_run_001",
            evidence_id=1,
        )

    @pytest.fixture
    def minimal_bookmarks(self):
        """Minimal valid bookmarks JSON."""
        return {
            "checksum": "abc123",
            "roots": {
                "bookmark_bar": {
                    "children": [],
                    "date_added": "0",
                    "guid": "root-bar",
                    "id": "1",
                    "name": "Bookmarks bar",
                    "type": "folder",
                },
                "other": {
                    "children": [],
                    "date_added": "0",
                    "guid": "root-other",
                    "id": "2",
                    "name": "Other bookmarks",
                    "type": "folder",
                },
                "synced": {
                    "children": [],
                    "date_added": "0",
                    "guid": "root-synced",
                    "id": "3",
                    "name": "Mobile bookmarks",
                    "type": "folder",
                },
            },
            "version": 1,
        }

    def test_no_warnings_for_valid_bookmarks(self, warning_collector, minimal_bookmarks):
        """No warnings for standard bookmarks structure."""
        bookmarks = list(parse_bookmarks_json(
            minimal_bookmarks,
            warning_collector=warning_collector,
            source_file="Bookmarks",
        ))

        # Should have parsed the folders
        assert len(bookmarks) >= 3

        # No warnings for standard structure
        assert len(warning_collector._warnings) == 0

    def test_unknown_root_key_detected(self, warning_collector, minimal_bookmarks):
        """Unknown root-level keys are detected."""
        minimal_bookmarks["new_feature_flag"] = True
        minimal_bookmarks["experimental_data"] = {"key": "value"}

        list(parse_bookmarks_json(
            minimal_bookmarks,
            warning_collector=warning_collector,
            source_file="Bookmarks",
        ))

        # Should have 2 warnings for unknown root keys
        warnings = warning_collector._warnings
        root_warnings = [w for w in warnings if w.item_name.startswith("root.")]
        assert len(root_warnings) == 2

        # Check warning details
        feature_warning = next(w for w in root_warnings if "new_feature_flag" in w.item_name)
        assert feature_warning.warning_type == "json_unknown_key"
        assert feature_warning.category == "json"
        assert feature_warning.severity == "info"

    def test_unknown_root_folder_detected(self, warning_collector, minimal_bookmarks):
        """Unknown root folders are detected with warning severity."""
        minimal_bookmarks["roots"]["managed_bookmarks"] = {
            "children": [],
            "type": "folder",
            "name": "Managed Bookmarks",
        }

        list(parse_bookmarks_json(
            minimal_bookmarks,
            warning_collector=warning_collector,
            source_file="Bookmarks",
        ))

        # Should have warning for unknown root folder
        warnings = warning_collector._warnings
        folder_warnings = [w for w in warnings if "roots.managed_bookmarks" in w.item_name]
        assert len(folder_warnings) == 1

        # Unknown root folder should be warning severity (not info)
        assert folder_warnings[0].severity == "warning"

    def test_unknown_bookmark_node_key_detected(self, warning_collector, minimal_bookmarks):
        """Unknown keys in bookmark nodes are detected."""
        minimal_bookmarks["roots"]["bookmark_bar"]["children"] = [{
            "id": "100",
            "name": "Test Bookmark",
            "url": "https://example.com",
            "type": "url",
            "date_added": "13300000000000000",
            "guid": "test-guid",
            # Unknown keys
            "custom_annotation": "user note",
            "visit_count": 42,
        }]

        list(parse_bookmarks_json(
            minimal_bookmarks,
            warning_collector=warning_collector,
            source_file="Bookmarks",
        ))

        # Should have warnings for unknown node keys
        warnings = warning_collector._warnings
        node_warnings = [w for w in warnings if w.item_name.startswith("node.")]
        assert len(node_warnings) == 2

        # Check that both unknown keys were detected
        warning_names = {w.item_name for w in node_warnings}
        assert "node.custom_annotation" in warning_names
        assert "node.visit_count" in warning_names

    def test_unknown_bookmark_type_detected(self, warning_collector, minimal_bookmarks):
        """Unknown bookmark types are detected."""
        minimal_bookmarks["roots"]["bookmark_bar"]["children"] = [{
            "id": "100",
            "name": "Separator",
            "type": "separator",  # Not a standard Chromium type
            "date_added": "13300000000000000",
            "guid": "sep-guid",
        }]

        list(parse_bookmarks_json(
            minimal_bookmarks,
            warning_collector=warning_collector,
            source_file="Bookmarks",
        ))

        # Should have warning for unknown bookmark type
        warnings = warning_collector._warnings
        type_warnings = [w for w in warnings if w.item_name == "bookmark_type"]
        assert len(type_warnings) == 1
        assert type_warnings[0].warning_type == "unknown_enum_value"
        assert type_warnings[0].item_value == "separator"

    def test_unknown_meta_info_key_detected(self, warning_collector, minimal_bookmarks):
        """Unknown keys in meta_info are detected."""
        minimal_bookmarks["roots"]["bookmark_bar"]["children"] = [{
            "id": "100",
            "name": "Test Bookmark",
            "url": "https://example.com",
            "type": "url",
            "date_added": "13300000000000000",
            "guid": "test-guid",
            "meta_info": {
                "last_visited_desktop": "13300000000000000",  # Known
                "custom_tracking_id": "abc123",  # Unknown
            },
        }]

        list(parse_bookmarks_json(
            minimal_bookmarks,
            warning_collector=warning_collector,
            source_file="Bookmarks",
        ))

        # Should have warning for unknown meta_info key
        warnings = warning_collector._warnings
        meta_warnings = [w for w in warnings if w.item_name.startswith("meta_info.")]
        assert len(meta_warnings) == 1
        assert "custom_tracking_id" in meta_warnings[0].item_name


# =============================================================================
# Test Parser Without Warning Collector
# =============================================================================

class TestParserWithoutWarnings:
    """Test that parser works without warning collector (backward compat)."""

    def test_parse_without_collector(self):
        """Parser works without warning collector argument."""
        data = {
            "checksum": "abc",
            "roots": {
                "bookmark_bar": {
                    "children": [{
                        "id": "1",
                        "name": "Test",
                        "url": "https://test.com",
                        "type": "url",
                        "date_added": "0",
                        "guid": "test",
                        "unknown_key": "value",  # Should be ignored
                    }],
                    "date_added": "0",
                    "guid": "root",
                    "id": "0",
                    "name": "Bookmarks bar",
                    "type": "folder",
                },
            },
            "version": 1,
            "unknown_root_key": True,  # Should be ignored
        }

        # Should not raise exception
        bookmarks = list(parse_bookmarks_json(data))
        assert len(bookmarks) >= 2  # folder + url

    def test_parse_with_none_collector(self):
        """Parser handles None warning collector."""
        data = {
            "roots": {
                "bookmark_bar": {
                    "children": [],
                    "type": "folder",
                    "name": "Bookmarks bar",
                    "id": "0",
                },
            },
            "version": 1,
        }

        bookmarks = list(parse_bookmarks_json(
            data,
            warning_collector=None,
            source_file="Bookmarks",
        ))
        assert len(bookmarks) >= 1


# =============================================================================
# Test Statistics
# =============================================================================

class TestStatistics:
    """Test bookmark statistics function."""

    def test_get_bookmark_stats_counts_correctly(self):
        """Statistics function counts urls and folders."""
        data = {
            "roots": {
                "bookmark_bar": {
                    "children": [
                        {
                            "id": "1",
                            "name": "URL1",
                            "url": "https://url1.com",
                            "type": "url",
                        },
                        {
                            "id": "2",
                            "name": "Folder",
                            "type": "folder",
                            "children": [
                                {
                                    "id": "3",
                                    "name": "URL2",
                                    "url": "https://url2.com",
                                    "type": "url",
                                },
                            ],
                        },
                    ],
                    "type": "folder",
                    "name": "Bookmarks bar",
                    "id": "0",
                },
            },
            "version": 1,
        }

        stats = get_bookmark_stats(data)

        # Root folder + nested folder = 2 folders
        # 2 URL bookmarks
        assert stats["url_count"] == 2
        assert stats["folder_count"] == 2
        assert stats["bookmark_count"] == 4  # All nodes


# =============================================================================
# Test Edge Cases
# =============================================================================

class TestEdgeCases:
    """Test edge cases and error handling."""

    @pytest.fixture
    def warning_collector(self):
        """Create a warning collector for testing."""
        return ExtractionWarningCollector(
            extractor_name="chromium_bookmarks",
            run_id="test_run_001",
            evidence_id=1,
        )

    def test_empty_roots_no_warnings(self, warning_collector):
        """Empty roots should not cause warnings."""
        data = {"checksum": "abc", "roots": {}, "version": 1}

        bookmarks = list(parse_bookmarks_json(
            data,
            warning_collector=warning_collector,
            source_file="Bookmarks",
        ))

        assert bookmarks == []
        assert len(warning_collector._warnings) == 0

    def test_missing_roots_no_crash(self, warning_collector):
        """Missing roots key should not crash."""
        data = {"checksum": "abc", "version": 1}

        bookmarks = list(parse_bookmarks_json(
            data,
            warning_collector=warning_collector,
            source_file="Bookmarks",
        ))

        assert bookmarks == []

    def test_deeply_nested_folders(self, warning_collector):
        """Handle deeply nested folder structures."""
        data = {
            "roots": {
                "bookmark_bar": {
                    "type": "folder",
                    "name": "Bookmarks bar",
                    "id": "0",
                    "children": [{
                        "type": "folder",
                        "name": "Level 1",
                        "id": "1",
                        "children": [{
                            "type": "folder",
                            "name": "Level 2",
                            "id": "2",
                            "children": [{
                                "type": "url",
                                "name": "Deep Bookmark",
                                "url": "https://deep.com",
                                "id": "3",
                            }],
                        }],
                    }],
                },
            },
            "version": 1,
        }

        bookmarks = list(parse_bookmarks_json(
            data,
            warning_collector=warning_collector,
            source_file="Bookmarks",
        ))

        # Should find the deep bookmark
        deep_bm = next(b for b in bookmarks if b.name == "Deep Bookmark")
        assert "Level 1" in deep_bm.folder_path
        assert "Level 2" in deep_bm.folder_path

    def test_invalid_timestamp_no_crash(self, warning_collector):
        """Invalid timestamps should not crash."""
        data = {
            "roots": {
                "bookmark_bar": {
                    "type": "folder",
                    "name": "Bookmarks bar",
                    "id": "0",
                    "date_added": "not_a_number",
                    "children": [],
                },
            },
            "version": 1,
        }

        bookmarks = list(parse_bookmarks_json(
            data,
            warning_collector=warning_collector,
            source_file="Bookmarks",
        ))

        # Should still parse, just with None timestamp
        assert len(bookmarks) == 1
        assert bookmarks[0].date_added is None
        assert bookmarks[0].date_added_iso is None


# =============================================================================
# Test ChromiumBookmark Dataclass
# =============================================================================

class TestChromiumBookmarkDataclass:
    """Test ChromiumBookmark dataclass enhancements."""

    def test_date_last_used_field_exists(self):
        """ChromiumBookmark has date_last_used field."""
        bm = ChromiumBookmark(
            id="1",
            name="Test",
            url="https://test.com",
            date_added=None,
            date_added_iso=None,
            date_modified=None,
            date_modified_iso=None,
            bookmark_type="url",
            folder_path="Bookmarks Bar",
            guid="test-guid",
            date_last_used=None,
            date_last_used_iso=None,
        )
        assert hasattr(bm, "date_last_used")
        assert hasattr(bm, "date_last_used_iso")
