"""
Tests for FirefoxPermissionsExtractor

Tests the browser-specific Firefox permissions extractor:
- Metadata and registration
- Browser pattern matching (Firefox, Tor)
- SQLite permissions.sqlite parsing (moz_perms table)
"""
import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from extractors.browser.firefox.permissions import FirefoxPermissionsExtractor
from extractors.browser.firefox._patterns import FIREFOX_BROWSERS
from extractors.browser.firefox.permissions.extractor import (
    FIREFOX_PERMISSION_VALUES,
    FIREFOX_PERMISSION_TYPE_MAP,
)


# ===========================================================================
# Test Fixtures
# ===========================================================================

@pytest.fixture
def extractor():
    """Create FirefoxPermissionsExtractor instance."""
    return FirefoxPermissionsExtractor()


@pytest.fixture
def mock_evidence_fs():
    """Create mock EvidenceFS for extraction tests."""
    mock_fs = MagicMock()
    mock_fs.glob.return_value = []
    return mock_fs


# ===========================================================================
# Metadata Tests
# ===========================================================================

class TestFirefoxPermissionsMetadata:
    """Test extractor metadata properties."""

    def test_name(self, extractor):
        """Test extractor name."""
        assert extractor.metadata.name == "firefox_permissions"

    def test_display_name(self, extractor):
        """Test display name is descriptive."""
        assert "Firefox" in extractor.metadata.display_name
        assert "Permissions" in extractor.metadata.display_name

    def test_version(self, extractor):
        """Test version format."""
        assert extractor.metadata.version
        assert "." in extractor.metadata.version

    def test_category(self, extractor):
        """Test category is browser."""
        assert extractor.metadata.category == "browser"

    def test_can_extract(self, extractor):
        """Test can_extract is True."""
        assert extractor.metadata.can_extract is True

    def test_can_ingest(self, extractor):
        """Test can_ingest is True."""
        assert extractor.metadata.can_ingest is True


# ===========================================================================
# Browser Pattern Tests
# ===========================================================================

class TestFirefoxPermissionsPatterns:
    """Test browser-specific pattern matching."""

    def test_supported_browsers(self, extractor):
        """Test that Firefox browsers are supported."""
        supported = extractor.SUPPORTED_BROWSERS
        assert "firefox" in supported

    def test_no_chromium_support(self, extractor):
        """Test that Chromium browsers are NOT supported."""
        supported = extractor.SUPPORTED_BROWSERS
        assert "chrome" not in supported
        assert "edge" not in supported


# ===========================================================================
# Permission Type Mapping Tests
# ===========================================================================

class TestFirefoxPermissionTypeMappings:
    """Test Firefox permission type mappings."""

    def test_geo_maps_to_geolocation(self):
        """Test geo maps to geolocation."""
        assert FIREFOX_PERMISSION_TYPE_MAP.get("geo") == "geolocation"

    def test_desktop_notification_maps_to_notifications(self):
        """Test desktop-notification maps to notifications."""
        assert FIREFOX_PERMISSION_TYPE_MAP.get("desktop-notification") == "notifications"

    def test_camera_maps_to_camera(self):
        """Test camera maps to camera."""
        assert FIREFOX_PERMISSION_TYPE_MAP.get("camera") == "camera"

    def test_microphone_maps_to_microphone(self):
        """Test microphone maps to microphone."""
        assert FIREFOX_PERMISSION_TYPE_MAP.get("microphone") == "microphone"

    def test_cookie_maps_to_cookies(self):
        """Test cookie maps to cookies."""
        assert FIREFOX_PERMISSION_TYPE_MAP.get("cookie") == "cookies"


class TestFirefoxPermissionValueMappings:
    """Test Firefox permission value mappings."""

    def test_allow_value(self):
        """Test allow (1) value mapped."""
        assert FIREFOX_PERMISSION_VALUES.get(1) == "allow"

    def test_block_value(self):
        """Test block (2) value mapped."""
        assert FIREFOX_PERMISSION_VALUES.get(2) == "block"


# ===========================================================================
# Can Run Tests
# ===========================================================================

class TestFirefoxPermissionsCanRun:
    """Test can_run methods."""

    def test_can_run_extraction_requires_evidence_fs(self, extractor):
        """Test can_run_extraction requires evidence_fs."""
        can_run, message = extractor.can_run_extraction(None)
        assert can_run is False
        assert "No evidence filesystem" in message

    def test_can_run_extraction_with_evidence_fs(self, extractor, mock_evidence_fs):
        """Test can_run_extraction returns True with evidence."""
        can_run, message = extractor.can_run_extraction(mock_evidence_fs)
        assert can_run is True

    def test_can_run_ingestion_requires_manifest(self, extractor, tmp_path):
        """Test can_run_ingestion requires manifest.json."""
        can_run, message = extractor.can_run_ingestion(tmp_path)
        assert can_run is False
        assert "manifest" in message.lower()

    def test_can_run_ingestion_with_manifest(self, extractor, tmp_path):
        """Test can_run_ingestion returns True with manifest."""
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text('{"files": []}')

        can_run, message = extractor.can_run_ingestion(tmp_path)
        assert can_run is True


# ===========================================================================
# Extraction Tests
# ===========================================================================

class TestFirefoxPermissionsExtraction:
    """Test extraction functionality."""

    def test_has_existing_output_false(self, extractor, tmp_path):
        """Test has_existing_output returns False without manifest."""
        assert extractor.has_existing_output(tmp_path) is False

    def test_has_existing_output_true(self, extractor, tmp_path):
        """Test has_existing_output returns True with manifest."""
        (tmp_path / "manifest.json").write_text('{}')
        assert extractor.has_existing_output(tmp_path) is True

    def test_get_output_dir(self, extractor, tmp_path):
        """Test get_output_dir returns correct path."""
        output_dir = extractor.get_output_dir(tmp_path, "evidence_1")
        expected = tmp_path / "evidences" / "evidence_1" / "firefox_permissions"
        assert output_dir == expected

    def test_generate_run_id(self, extractor):
        """Test run ID generation."""
        run_id = extractor._generate_run_id()
        assert "_" in run_id  # timestamp_uuid format
        parts = run_id.split("_")
        assert len(parts) == 2
        # Timestamp part should be 15 chars (YYYYMMDDTHHmmss)
        assert len(parts[0]) == 15


# ===========================================================================
# Profile Extraction Tests
# ===========================================================================

class TestFirefoxPermissionsProfileExtraction:
    """Test profile name extraction from paths using shared extract_profile_from_path."""

    def test_extract_profile_from_windows_path(self):
        """Test extracting profile from Windows path."""
        from extractors.browser.firefox._patterns import extract_profile_from_path
        path = "Users/John/AppData/Roaming/Mozilla/Firefox/Profiles/abc123.default/permissions.sqlite"
        profile = extract_profile_from_path(path)
        assert profile == "abc123.default"

    def test_extract_profile_from_linux_path(self):
        """Test extracting profile from Linux path."""
        from extractors.browser.firefox._patterns import extract_profile_from_path
        path = "home/user/.mozilla/firefox/Profiles/xyz789.default-release/permissions.sqlite"
        profile = extract_profile_from_path(path)
        assert profile == "xyz789.default-release"

    def test_extract_profile_default_fallback(self):
        """Test default profile fallback."""
        from extractors.browser.firefox._patterns import extract_profile_from_path
        path = "some/unknown/path/permissions.sqlite"
        profile = extract_profile_from_path(path)
        assert profile == "Default"  # Shared function returns capitalized Default


# ===========================================================================
# Content-Prefs Tests
# ===========================================================================

class TestContentPrefsMappings:
    """Test content-prefs.sqlite value mappings."""

    def test_map_zoom_setting(self, extractor):
        """Test zoom setting is mapped correctly."""
        from extractors.browser.firefox.permissions._schemas import normalize_content_pref_type
        result = normalize_content_pref_type("browser.content.full-zoom")
        assert result == "zoom"

    def test_map_autoplay_setting(self, extractor):
        """Test autoplay setting is mapped correctly."""
        from extractors.browser.firefox.permissions._schemas import normalize_content_pref_type
        result = normalize_content_pref_type("media.autoplay.default")
        assert result == "autoplay"

    def test_map_unknown_setting_passthrough(self, extractor):
        """Test unknown settings pass through unchanged."""
        from extractors.browser.firefox.permissions._schemas import normalize_content_pref_type
        result = normalize_content_pref_type("some.custom.setting")
        assert result == "some.custom.setting"

    def test_normalize_zoom_value(self, extractor):
        """Test zoom value normalization."""
        from extractors.browser.firefox.permissions._parsers import _normalize_content_pref_value
        result = _normalize_content_pref_value("browser.zoom.siteSpecific", 1.5)
        assert result == "150%"

    def test_normalize_boolean_enabled(self, extractor):
        """Test boolean enabled value."""
        from extractors.browser.firefox.permissions._parsers import _normalize_content_pref_value
        result = _normalize_content_pref_value("some.setting", 1)
        assert result == "enabled"

    def test_normalize_boolean_disabled(self, extractor):
        """Test boolean disabled value."""
        from extractors.browser.firefox.permissions._parsers import _normalize_content_pref_value
        result = _normalize_content_pref_value("some.setting", 0)
        assert result == "disabled"

    def test_normalize_none_value(self, extractor):
        """Test None value normalization."""
        from extractors.browser.firefox.permissions._parsers import _normalize_content_pref_value
        result = _normalize_content_pref_value("some.setting", None)
        assert result == "unknown"


# ===========================================================================
# sqlite3.Row Compatibility Tests
# ===========================================================================

class TestSqlite3RowCompatibility:
    """Test that we don't use sqlite3.Row.get() (which doesn't exist)."""

    def test_sqlite_row_has_no_get_method(self):
        """Verify sqlite3.Row has no .get() method - documents why we avoid it."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (a TEXT)")
        cursor.execute("INSERT INTO test VALUES ('x')")
        cursor.execute("SELECT * FROM test")
        row = cursor.fetchone()

        # This is why we can't use row.get() in the extractor
        assert not hasattr(row, "get") or not callable(getattr(row, "get", None))

        # But these work:
        assert row["a"] == "x"
        assert "a" in row.keys()
        conn.close()


# ===========================================================================
# Discovery Pattern Tests
# ===========================================================================

class TestDiscoveryPatterns:
    """Test that discovery uses correct patterns from _patterns module."""

    def test_permissions_artifact_patterns_exist(self):
        """Test permissions artifact has patterns defined."""
        from extractors.browser.firefox._patterns import get_artifact_patterns

        patterns = get_artifact_patterns("firefox", "permissions")
        assert len(patterns) > 0

        # Should include both permissions.sqlite and content-prefs.sqlite
        pattern_str = " ".join(patterns)
        assert "permissions.sqlite" in pattern_str
        assert "content-prefs.sqlite" in pattern_str

    def test_firefox_browsers_have_profile_roots(self):
        """Test FIREFOX_BROWSERS dict has profile_roots (not patterns)."""
        from extractors.browser.firefox._patterns import FIREFOX_BROWSERS

        for browser_key, browser_info in FIREFOX_BROWSERS.items():
            # Should have profile_roots, NOT patterns
            assert "profile_roots" in browser_info, f"{browser_key} missing profile_roots"
            assert "patterns" not in browser_info, f"{browser_key} has old patterns structure"
