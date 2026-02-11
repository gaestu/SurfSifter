"""
Unit tests for shared browser patterns module.

Tests pattern structure, helper functions, and backward compatibility.
"""

import pytest

from extractors.browser_patterns import (
    BROWSER_PATTERNS,
    get_browser_paths,
    get_browsers_for_artifact,
    get_all_browsers,
    get_browser_display_name,
    get_browser_engine,
    get_legacy_browser_patterns,
    get_cache_patterns,
)


# =============================================================================
# Pattern Structure Tests
# =============================================================================

class TestBrowserPatternsStructure:
    """Test the structure of BROWSER_PATTERNS constant."""

    def test_all_browsers_present(self):
        """All expected browsers are defined."""
        expected = {"chrome", "edge", "firefox", "safari", "opera", "brave", "tor"}
        assert set(BROWSER_PATTERNS.keys()) == expected

    def test_browser_has_required_fields(self):
        """Each browser has required fields."""
        required_fields = {"display_name", "engine", "paths"}
        for browser, config in BROWSER_PATTERNS.items():
            assert required_fields <= set(config.keys()), f"{browser} missing fields"

    def test_paths_has_artifact_types(self):
        """Each browser's paths dict has core artifact type keys."""
        # Core artifact types that most browsers should have
        # Note: tor uses Firefox's places.sqlite but may not have all artifacts
        core_artifact_types = {"history", "cookies", "bookmarks", "cache", "downloads"}
        browsers_with_core_artifacts = ["chrome", "edge", "firefox", "safari", "opera", "brave"]
        for browser in browsers_with_core_artifacts:
            config = BROWSER_PATTERNS[browser]
            browser_artifacts = set(config["paths"].keys())
            assert core_artifact_types <= browser_artifacts, f"{browser} missing core artifact types"

    def test_chromium_browsers_have_history_patterns(self):
        """Chromium-based browsers have history patterns."""
        chromium_browsers = ["chrome", "edge", "opera", "brave"]
        for browser in chromium_browsers:
            assert len(BROWSER_PATTERNS[browser]["paths"]["history"]) > 0

    def test_firefox_has_history_patterns(self):
        """Firefox has history patterns."""
        assert len(BROWSER_PATTERNS["firefox"]["paths"]["history"]) > 0

    def test_safari_has_history_patterns(self):
        """Safari has history patterns."""
        assert len(BROWSER_PATTERNS["safari"]["paths"]["history"]) > 0

    def test_engine_types_correct(self):
        """Browser engines are correctly identified."""
        assert BROWSER_PATTERNS["chrome"]["engine"] == "chromium"
        assert BROWSER_PATTERNS["edge"]["engine"] == "chromium"
        assert BROWSER_PATTERNS["opera"]["engine"] == "chromium"
        assert BROWSER_PATTERNS["brave"]["engine"] == "chromium"
        assert BROWSER_PATTERNS["firefox"]["engine"] == "gecko"
        assert BROWSER_PATTERNS["safari"]["engine"] == "webkit"


# =============================================================================
# Windows Path Pattern Tests
# =============================================================================

class TestWindowsPatterns:
    """Test Windows-specific path patterns."""

    def test_chrome_windows_history_pattern(self):
        """Chrome Windows history patterns include Default and Profile *."""
        patterns = BROWSER_PATTERNS["chrome"]["paths"]["history"]
        windows_patterns = [p for p in patterns if "AppData" in p]
        assert len(windows_patterns) >= 2  # Default + Profile *

        # Check for explicit Default and Profile patterns
        has_default = any("Default/History" in p for p in windows_patterns)
        has_profile = any("Profile */History" in p for p in windows_patterns)
        assert has_default, "Chrome Windows missing Default pattern"
        assert has_profile, "Chrome Windows missing Profile * pattern"

    def test_edge_windows_history_pattern(self):
        """Edge Windows history patterns include Default and Profile *."""
        patterns = BROWSER_PATTERNS["edge"]["paths"]["history"]
        windows_patterns = [p for p in patterns if "AppData" in p]
        assert len(windows_patterns) >= 2

        has_default = any("Default/History" in p for p in windows_patterns)
        has_profile = any("Profile */History" in p for p in windows_patterns)
        assert has_default, "Edge Windows missing Default pattern"
        assert has_profile, "Edge Windows missing Profile * pattern"

    def test_opera_windows_history_pattern(self):
        """Opera Windows history patterns include Stable and GX variants."""
        patterns = BROWSER_PATTERNS["opera"]["paths"]["history"]
        windows_patterns = [p for p in patterns if "AppData" in p]
        assert len(windows_patterns) >= 2

        has_stable = any("Opera Stable" in p for p in windows_patterns)
        has_gx = any("Opera GX Stable" in p for p in windows_patterns)
        assert has_stable, "Opera Windows missing Stable pattern"
        assert has_gx, "Opera Windows missing GX pattern"

    def test_brave_windows_history_pattern(self):
        """Brave Windows history patterns exist."""
        patterns = BROWSER_PATTERNS["brave"]["paths"]["history"]
        windows_patterns = [p for p in patterns if "AppData" in p]
        assert len(windows_patterns) >= 2

        has_default = any("Default/History" in p for p in windows_patterns)
        has_profile = any("Profile */History" in p for p in windows_patterns)
        assert has_default, "Brave Windows missing Default pattern"
        assert has_profile, "Brave Windows missing Profile * pattern"

    def test_firefox_windows_history_pattern(self):
        """Firefox Windows history pattern points to places.sqlite."""
        patterns = BROWSER_PATTERNS["firefox"]["paths"]["history"]
        windows_patterns = [p for p in patterns if "AppData" in p]
        assert len(windows_patterns) >= 1
        assert all("places.sqlite" in p for p in windows_patterns)


# =============================================================================
# Helper Function Tests
# =============================================================================

class TestGetBrowserPaths:
    """Test get_browser_paths helper function."""

    def test_valid_browser_and_artifact(self):
        """Returns paths for valid browser/artifact combination."""
        paths = get_browser_paths("chrome", "history")
        assert len(paths) > 0
        assert all(isinstance(p, str) for p in paths)

    def test_unknown_browser(self):
        """Returns empty list for unknown browser."""
        paths = get_browser_paths("netscape", "history")
        assert paths == []

    def test_unknown_artifact(self):
        """Returns empty list for unknown artifact type."""
        paths = get_browser_paths("chrome", "passwords")
        assert paths == []

    def test_browser_without_artifact(self):
        """Returns empty list if browser doesn't support artifact."""
        # Safari doesn't have media_history patterns defined
        paths = get_browser_paths("safari", "media_history")
        assert paths == []


class TestGetBrowsersForArtifact:
    """Test get_browsers_for_artifact helper function."""

    def test_history_artifact(self):
        """Returns all browsers for history artifact."""
        browsers = get_browsers_for_artifact("history")
        assert len(browsers) == 6  # All browsers support history
        assert "chrome" in browsers
        assert "firefox" in browsers
        assert "opera" in browsers
        assert "brave" in browsers

    def test_cache_artifact(self):
        """Returns Chromium browsers for cache artifact."""
        browsers = get_browsers_for_artifact("cache")
        # Chromium browsers only (Firefox uses cache2 format)
        assert "chrome" in browsers
        assert "edge" in browsers
        assert "opera" in browsers
        assert "brave" in browsers
        assert "firefox" not in browsers  # Uses cache2 format

    def test_unknown_artifact(self):
        """Returns empty list for unknown artifact."""
        browsers = get_browsers_for_artifact("passwords")
        assert browsers == []


class TestGetAllBrowsers:
    """Test get_all_browsers helper function."""

    def test_returns_all_browser_keys(self):
        """Returns all browser keys."""
        browsers = get_all_browsers()
        assert len(browsers) == 7
        assert set(browsers) == {"chrome", "edge", "firefox", "safari", "opera", "brave", "tor"}


class TestGetBrowserDisplayName:
    """Test get_browser_display_name helper function."""

    def test_known_browsers(self):
        """Returns correct display names for known browsers."""
        assert get_browser_display_name("chrome") == "Google Chrome"
        assert get_browser_display_name("edge") == "Microsoft Edge"
        assert get_browser_display_name("firefox") == "Mozilla Firefox"
        assert get_browser_display_name("safari") == "Apple Safari"
        assert get_browser_display_name("opera") == "Opera"
        assert get_browser_display_name("brave") == "Brave"

    def test_unknown_browser(self):
        """Returns capitalized key for unknown browser."""
        assert get_browser_display_name("netscape") == "Netscape"


class TestGetBrowserEngine:
    """Test get_browser_engine helper function."""

    def test_chromium_browsers(self):
        """Chromium-based browsers return 'chromium'."""
        for browser in ["chrome", "edge", "opera", "brave"]:
            assert get_browser_engine(browser) == "chromium"

    def test_firefox(self):
        """Firefox returns 'gecko'."""
        assert get_browser_engine("firefox") == "gecko"

    def test_safari(self):
        """Safari returns 'webkit'."""
        assert get_browser_engine("safari") == "webkit"

    def test_unknown_browser(self):
        """Unknown browser returns 'unknown'."""
        assert get_browser_engine("netscape") == "unknown"


# =============================================================================
# Legacy Compatibility Tests
# =============================================================================

class TestGetLegacyBrowserPatterns:
    """Test get_legacy_browser_patterns for backward compatibility."""

    def test_returns_legacy_format(self):
        """Returns patterns in legacy format."""
        legacy = get_legacy_browser_patterns()

        assert "chrome" in legacy
        assert "display_name" in legacy["chrome"]
        assert "history" in legacy["chrome"]
        assert isinstance(legacy["chrome"]["history"], list)

    def test_all_browsers_included(self):
        """All browsers are in legacy patterns."""
        legacy = get_legacy_browser_patterns()
        assert len(legacy) == 7

    def test_compatible_with_extractor(self):
        """Legacy patterns work with BrowserHistoryExtractor.BROWSER_PATTERNS format."""
        legacy = get_legacy_browser_patterns()

        # Check each browser has expected structure
        for browser, config in legacy.items():
            assert "display_name" in config
            assert "history" in config
            assert isinstance(config["history"], list)


class TestGetCachePatterns:
    """Test get_cache_patterns helper function."""

    def test_returns_chromium_browsers(self):
        """Returns patterns for Chromium-based browsers."""
        patterns = get_cache_patterns()

        # Should include Chrome, Edge, Opera, Brave
        assert "chrome" in patterns
        assert "edge" in patterns
        assert "opera" in patterns
        assert "brave" in patterns

    def test_excludes_non_chromium_browsers(self):
        """Firefox and Safari are not included (different cache format)."""
        patterns = get_cache_patterns()

        # Firefox uses cache2 format (has separate cache_firefox extractor)
        assert "firefox" not in patterns
        # Safari has limited cache patterns (experimental)
        # Note: Safari cache IS included now for Phase 4 support

    def test_patterns_are_lists(self):
        """Each browser's patterns are lists of strings."""
        patterns = get_cache_patterns()

        for browser, path_list in patterns.items():
            assert isinstance(path_list, list)
            assert all(isinstance(p, str) for p in path_list)


# =============================================================================
# Edge-specific Tests (Regression for Edge extraction issue)
# =============================================================================

class TestEdgePatterns:
    """Regression tests for Edge browser pattern issues."""

    def test_edge_has_explicit_default_pattern(self):
        """Edge patterns include explicit 'Default' directory."""
        patterns = BROWSER_PATTERNS["edge"]["paths"]["history"]

        # Must have explicit Default pattern, not just wildcard
        has_explicit_default = any(
            "User Data/Default/History" in p
            for p in patterns
        )
        assert has_explicit_default, "Edge missing explicit Default pattern"

    def test_edge_has_profile_wildcard_pattern(self):
        """Edge patterns include 'Profile *' wildcard."""
        patterns = BROWSER_PATTERNS["edge"]["paths"]["history"]

        has_profile_wildcard = any(
            "Profile */History" in p or "Profile*/History" in p
            for p in patterns
        )
        assert has_profile_wildcard, "Edge missing Profile * pattern"

    def test_edge_cache_patterns(self):
        """Edge has cache patterns."""
        patterns = BROWSER_PATTERNS["edge"]["paths"]["cache"]
        assert len(patterns) > 0

    def test_edge_distinct_from_chrome(self):
        """Edge patterns are distinct from Chrome patterns."""
        chrome_patterns = set(BROWSER_PATTERNS["chrome"]["paths"]["history"])
        edge_patterns = set(BROWSER_PATTERNS["edge"]["paths"]["history"])

        # No overlap between Chrome and Edge patterns
        assert chrome_patterns.isdisjoint(edge_patterns)
