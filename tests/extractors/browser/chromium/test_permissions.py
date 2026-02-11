"""
Tests for ChromiumPermissionsExtractor

Tests the browser-specific Chromium permissions extractor:
- Permission type normalization
- Permission value mapping
- Preferences JSON parsing
- Multi-partition support
- Schema warning integration
- Profile extraction from paths
- Unique filename generation
"""
import pytest
import json
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from typing import Dict, Any

from extractors.browser.chromium.permissions import ChromiumPermissionsExtractor
from extractors.browser.chromium.permissions._schemas import (
    CHROMIUM_PERMISSION_TYPES,
    CHROMIUM_PERMISSION_VALUES,
    KNOWN_EXCEPTION_KEYS,
    KNOWN_SETTING_KEYS,
    KNOWN_SETTING_VALUES,
    get_permission_type_display,
    get_permission_value_display,
    is_known_permission_type,
    is_known_setting_value,
    is_known_setting_key,
)


@pytest.fixture
def extractor():
    """Create ChromiumPermissionsExtractor instance."""
    return ChromiumPermissionsExtractor()


@pytest.fixture
def mock_callbacks():
    """Create mock callbacks for extraction."""
    callbacks = Mock()
    callbacks.on_step = Mock()
    callbacks.on_progress = Mock()
    callbacks.on_log = Mock()
    callbacks.on_error = Mock()
    callbacks.is_cancelled = Mock(return_value=False)
    return callbacks


@pytest.fixture
def sample_preferences() -> Dict[str, Any]:
    """Sample Chromium Preferences JSON with permissions."""
    return {
        "profile": {
            "content_settings": {
                "exceptions": {
                    "notifications": {
                        "https://example.com,*": {
                            "setting": 1,
                            "last_modified": "13300000000000000"
                        },
                        "https://blocked.com,*": {
                            "setting": 2
                        }
                    },
                    "geolocation": {
                        "https://maps.google.com,*": {
                            "setting": 1
                        }
                    },
                    "media_stream_camera": {
                        "https://meet.google.com,*": {
                            "setting": 1
                        }
                    },
                }
            }
        }
    }


@pytest.fixture
def sample_preferences_with_unknowns() -> Dict[str, Any]:
    """Sample Preferences with unknown permission types and values."""
    return {
        "profile": {
            "content_settings": {
                "exceptions": {
                    "notifications": {
                        "https://example.com,*": {
                            "setting": 1,
                        }
                    },
                    # Unknown permission type
                    "future_permission_type": {
                        "https://example.com,*": {
                            "setting": 1,
                        }
                    },
                    "geolocation": {
                        "https://example.com,*": {
                            # Unknown setting value
                            "setting": 99,
                            # Unknown setting key
                            "future_key": "future_value",
                        }
                    },
                }
            }
        }
    }


class TestExtractorMetadata:
    """Test extractor metadata and configuration."""

    def test_metadata_name(self, extractor):
        assert extractor.metadata.name == "chromium_permissions"

    def test_metadata_category(self, extractor):
        assert extractor.metadata.category == "browser"

    def test_metadata_version(self, extractor):
        # Version should be a string like "2.0.0"
        assert isinstance(extractor.metadata.version, str)
        assert "." in extractor.metadata.version

    def test_supported_browsers(self, extractor):
        # Should support common Chromium browsers
        assert "chrome" in extractor.SUPPORTED_BROWSERS
        assert "edge" in extractor.SUPPORTED_BROWSERS
        assert "brave" in extractor.SUPPORTED_BROWSERS
        assert "opera" in extractor.SUPPORTED_BROWSERS

    def test_can_extract_and_ingest(self, extractor):
        assert extractor.metadata.can_extract is True
        assert extractor.metadata.can_ingest is True


class TestSchemaDefinitions:
    """Test schema definitions in _schemas.py."""

    def test_permission_types_mapping(self):
        """Test that common permission types are mapped."""
        assert CHROMIUM_PERMISSION_TYPES["notifications"] == "notifications"
        assert CHROMIUM_PERMISSION_TYPES["geolocation"] == "geolocation"
        assert CHROMIUM_PERMISSION_TYPES["media_stream_camera"] == "camera"
        assert CHROMIUM_PERMISSION_TYPES["media_stream_mic"] == "microphone"

    def test_permission_values_mapping(self):
        """Test permission value decoding."""
        assert CHROMIUM_PERMISSION_VALUES[0] == "default"
        assert CHROMIUM_PERMISSION_VALUES[1] == "allow"
        assert CHROMIUM_PERMISSION_VALUES[2] == "block"
        assert CHROMIUM_PERMISSION_VALUES[3] == "ask"
        assert CHROMIUM_PERMISSION_VALUES[4] == "session_only"

    def test_known_exception_keys_contains_common_types(self):
        """Test that known exception keys includes common types."""
        assert "notifications" in KNOWN_EXCEPTION_KEYS
        assert "geolocation" in KNOWN_EXCEPTION_KEYS
        assert "cookies" in KNOWN_EXCEPTION_KEYS

    def test_known_setting_keys_contains_common_keys(self):
        """Test that known setting keys includes common keys."""
        assert "setting" in KNOWN_SETTING_KEYS
        assert "last_modified" in KNOWN_SETTING_KEYS
        assert "expiration" in KNOWN_SETTING_KEYS

    def test_known_setting_values(self):
        """Test known setting value codes."""
        assert 0 in KNOWN_SETTING_VALUES
        assert 1 in KNOWN_SETTING_VALUES
        assert 2 in KNOWN_SETTING_VALUES
        assert 3 in KNOWN_SETTING_VALUES


class TestSchemaHelperFunctions:
    """Test helper functions in _schemas.py."""

    def test_get_permission_type_display_known(self):
        assert get_permission_type_display("notifications") == "notifications"
        assert get_permission_type_display("media_stream_camera") == "camera"

    def test_get_permission_type_display_unknown(self):
        # Unknown types should be returned as-is
        assert get_permission_type_display("unknown_type") == "unknown_type"

    def test_get_permission_value_display_known(self):
        assert get_permission_value_display(1) == "allow"
        assert get_permission_value_display(2) == "block"

    def test_get_permission_value_display_unknown(self):
        assert get_permission_value_display(99) == "unknown"

    def test_is_known_permission_type(self):
        assert is_known_permission_type("notifications") is True
        assert is_known_permission_type("unknown_type") is False

    def test_is_known_setting_value(self):
        assert is_known_setting_value(1) is True
        assert is_known_setting_value(99) is False

    def test_is_known_setting_key(self):
        assert is_known_setting_key("setting") is True
        assert is_known_setting_key("unknown_key") is False


class TestCoercePermissionSettingCode:
    """Test _coerce_permission_setting_code logic."""

    def test_integer_passthrough(self, extractor):
        assert extractor._coerce_permission_setting_code(1) == 1
        assert extractor._coerce_permission_setting_code(2) == 2
        assert extractor._coerce_permission_setting_code(0) == 0

    def test_float_conversion(self, extractor):
        assert extractor._coerce_permission_setting_code(1.0) == 1
        assert extractor._coerce_permission_setting_code(2.0) == 2

    def test_string_conversion(self, extractor):
        assert extractor._coerce_permission_setting_code("1") == 1
        assert extractor._coerce_permission_setting_code("2") == 2

    def test_string_name_conversion(self, extractor):
        assert extractor._coerce_permission_setting_code("allow") == 1
        assert extractor._coerce_permission_setting_code("block") == 2
        assert extractor._coerce_permission_setting_code("default") == 0

    def test_bool_conversion(self, extractor):
        assert extractor._coerce_permission_setting_code(True) == 1
        assert extractor._coerce_permission_setting_code(False) == 0

    def test_nested_dict_extraction(self, extractor):
        assert extractor._coerce_permission_setting_code({"setting": 2}) == 2
        assert extractor._coerce_permission_setting_code({"value": 1}) == 1

    def test_none_returns_none(self, extractor):
        assert extractor._coerce_permission_setting_code(None) is None

    def test_complex_dict_returns_none(self, extractor):
        # Dicts without known keys should return None
        assert extractor._coerce_permission_setting_code({"unknown": "data"}) is None

    def test_invalid_string_returns_none(self, extractor):
        assert extractor._coerce_permission_setting_code("invalid") is None


class TestProfileExtraction:
    """Test profile name extraction from paths."""

    def test_default_profile_windows(self, extractor):
        path = "Users/test/AppData/Local/Google/Chrome/User Data/Default/Preferences"
        assert extractor._extract_profile_from_path(path, "chrome") == "Default"

    def test_profile_1_windows(self, extractor):
        path = "Users/test/AppData/Local/Google/Chrome/User Data/Profile 1/Preferences"
        assert extractor._extract_profile_from_path(path, "chrome") == "Profile 1"

    def test_profile_with_space(self, extractor):
        path = "Users/test/AppData/Local/Google/Chrome/User Data/Profile 2/Preferences"
        assert extractor._extract_profile_from_path(path, "chrome") == "Profile 2"

    def test_opera_stable(self, extractor):
        path = "Users/test/AppData/Roaming/Opera Software/Opera Stable/Preferences"
        assert extractor._extract_profile_from_path(path, "opera") == "Opera Stable"

    def test_opera_gx_stable(self, extractor):
        path = "Users/test/AppData/Roaming/Opera Software/Opera GX Stable/Preferences"
        assert extractor._extract_profile_from_path(path, "opera") == "Opera GX Stable"

    def test_fallback_to_default(self, extractor):
        path = "some/random/path/Preferences"
        assert extractor._extract_profile_from_path(path, "chrome") == "Default"


class TestFilenameGeneration:
    """Test unique filename generation for multi-partition support."""

    def test_filename_includes_partition(self, extractor):
        """Filename should include partition index."""
        mock_fs = Mock()
        mock_fs.read_file = Mock(return_value=b'{}')

        file_info = {
            "logical_path": "/Users/test/Chrome/Default/Preferences",
            "browser": "chrome",
            "profile": "Default",
            "partition_index": 2,
        }

        with patch.object(Path, 'write_bytes'):
            result = extractor._extract_file(
                mock_fs,
                file_info,
                Path("/tmp/output"),
                Mock(),
            )

        # Filename should contain partition index
        assert "_p2_" in result["extracted_path"]
        assert result["partition_index"] == 2

    def test_filename_includes_hash(self, extractor):
        """Filename should include path hash for uniqueness."""
        mock_fs = Mock()
        mock_fs.read_file = Mock(return_value=b'{}')

        file_info = {
            "logical_path": "/Users/test/Chrome/Default/Preferences",
            "browser": "chrome",
            "profile": "Default",
            "partition_index": 0,
        }

        with patch.object(Path, 'write_bytes'):
            result = extractor._extract_file(
                mock_fs,
                file_info,
                Path("/tmp/output"),
                Mock(),
            )

        # Filename should contain hash (8 chars)
        filename = Path(result["extracted_path"]).name
        # Format: {browser}_{profile}_p{partition}_{hash}_preferences
        parts = filename.split("_")
        assert len(parts) >= 4
        # Hash should be 8 hex characters
        hash_part = parts[-2]
        assert len(hash_part) == 8


class TestCanRunChecks:
    """Test can_run_extraction and can_run_ingestion."""

    def test_can_run_extraction_no_fs(self, extractor):
        can_run, message = extractor.can_run_extraction(None)
        assert can_run is False
        assert "No evidence filesystem" in message

    def test_can_run_extraction_with_fs(self, extractor):
        mock_fs = Mock()
        can_run, message = extractor.can_run_extraction(mock_fs)
        assert can_run is True
        assert message == ""

    def test_can_run_ingestion_no_manifest(self, extractor, tmp_path):
        can_run, message = extractor.can_run_ingestion(tmp_path)
        assert can_run is False
        assert "No manifest.json" in message

    def test_can_run_ingestion_with_manifest(self, extractor, tmp_path):
        # Create manifest
        manifest = tmp_path / "manifest.json"
        manifest.write_text('{"run_id": "test"}')

        can_run, message = extractor.can_run_ingestion(tmp_path)
        assert can_run is True
        assert message == ""


class TestHasExistingOutput:
    """Test has_existing_output method."""

    def test_no_manifest(self, extractor, tmp_path):
        assert extractor.has_existing_output(tmp_path) is False

    def test_with_manifest(self, extractor, tmp_path):
        manifest = tmp_path / "manifest.json"
        manifest.write_text('{}')
        assert extractor.has_existing_output(tmp_path) is True


class TestGetOutputDir:
    """Test get_output_dir method."""

    def test_output_dir_structure(self, extractor, tmp_path):
        output_dir = extractor.get_output_dir(tmp_path, "test_evidence")
        expected = tmp_path / "evidences" / "test_evidence" / "chromium_permissions"
        assert output_dir == expected
