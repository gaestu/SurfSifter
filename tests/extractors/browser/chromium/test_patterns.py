"""
Tests for Chromium browser family file path patterns.

Tests the _patterns.py module which provides centralized path patterns for:
- Google Chrome (Stable, Beta, Dev, Canary)
- Chromium (open-source)
- Microsoft Edge (Stable, Beta, Dev, Canary)
- Brave (Stable, Beta, Nightly)
- Opera (Stable, GX) with flat profile structure
"""

import pytest

from extractors.browser.chromium._patterns import (
    CHROMIUM_BROWSERS,
    CHROMIUM_ARTIFACTS,
    PROFILE_PATTERNS,
    get_patterns,
    get_all_patterns,
    get_browser_display_name,
    get_all_browsers,
    get_artifact_patterns,
    get_stable_browsers,
    is_flat_profile_browser,
    get_patterns_for_root,
)


# =============================================================================
# Browser Configuration Tests
# =============================================================================

class TestChromiumBrowsersConfig:
    """Test CHROMIUM_BROWSERS configuration."""

    def test_stable_browsers_defined(self):
        """Core stable browsers are defined."""
        expected = {"chrome", "chromium", "edge", "brave", "opera", "opera_gx"}
        assert expected.issubset(set(CHROMIUM_BROWSERS.keys()))

    def test_beta_channels_defined(self):
        """Beta channel browsers are defined."""
        expected = {"chrome_beta", "edge_beta", "brave_beta"}
        assert expected.issubset(set(CHROMIUM_BROWSERS.keys()))

    def test_dev_channels_defined(self):
        """Dev channel browsers are defined."""
        expected = {"chrome_dev", "edge_dev"}
        assert expected.issubset(set(CHROMIUM_BROWSERS.keys()))

    def test_canary_channels_defined(self):
        """Canary channel browsers are defined."""
        expected = {"chrome_canary", "edge_canary", "brave_nightly"}
        assert expected.issubset(set(CHROMIUM_BROWSERS.keys()))

    def test_all_browsers_have_display_name(self):
        """All browsers have display_name."""
        for browser, config in CHROMIUM_BROWSERS.items():
            assert "display_name" in config, f"{browser} missing display_name"
            assert isinstance(config["display_name"], str)

    def test_all_browsers_have_profile_roots(self):
        """All browsers have profile_roots list."""
        for browser, config in CHROMIUM_BROWSERS.items():
            assert "profile_roots" in config, f"{browser} missing profile_roots"
            assert isinstance(config["profile_roots"], list)
            assert len(config["profile_roots"]) > 0, f"{browser} has empty profile_roots"

    def test_opera_has_flat_profile_flag(self):
        """Opera browsers have flat_profile=True."""
        assert CHROMIUM_BROWSERS["opera"].get("flat_profile") is True
        assert CHROMIUM_BROWSERS["opera_gx"].get("flat_profile") is True

    def test_chrome_no_flat_profile_flag(self):
        """Chrome does not have flat_profile flag."""
        assert CHROMIUM_BROWSERS["chrome"].get("flat_profile", False) is False


# =============================================================================
# Profile Patterns Tests
# =============================================================================

class TestProfilePatterns:
    """Test PROFILE_PATTERNS configuration."""

    def test_default_profile_included(self):
        """Default profile is in patterns."""
        assert "Default" in PROFILE_PATTERNS

    def test_numbered_profiles_included(self):
        """Profile * pattern is in patterns."""
        assert "Profile *" in PROFILE_PATTERNS

    def test_guest_profile_included(self):
        """Guest Profile is in patterns."""
        assert "Guest Profile" in PROFILE_PATTERNS

    def test_system_profile_included(self):
        """System Profile is in patterns."""
        assert "System Profile" in PROFILE_PATTERNS


# =============================================================================
# Artifact Configuration Tests
# =============================================================================

class TestChromiumArtifactsConfig:
    """Test CHROMIUM_ARTIFACTS configuration."""

    def test_core_artifacts_defined(self):
        """Core artifact types are defined."""
        expected = {"history", "cookies", "bookmarks", "downloads", "cache"}
        assert expected.issubset(set(CHROMIUM_ARTIFACTS.keys()))

    def test_phase1_artifacts_defined(self):
        """Phase 1 artifacts are defined."""
        expected = {"autofill", "sessions", "permissions", "media_history"}
        assert expected.issubset(set(CHROMIUM_ARTIFACTS.keys()))

    def test_phase2_artifacts_defined(self):
        """Phase 2 artifacts are defined."""
        expected = {"transport_security"}
        assert expected.issubset(set(CHROMIUM_ARTIFACTS.keys()))

    def test_phase3_artifacts_defined(self):
        """Phase 3 artifacts are defined."""
        expected = {"extensions", "local_storage", "session_storage", "indexeddb", "sync_data"}
        assert expected.issubset(set(CHROMIUM_ARTIFACTS.keys()))

    def test_phase4_artifacts_defined(self):
        """Phase 4 artifacts are defined."""
        expected = {"favicons", "top_sites"}
        assert expected.issubset(set(CHROMIUM_ARTIFACTS.keys()))

    def test_artifacts_are_relative_paths(self):
        """Artifact paths are relative (no Default/ prefix)."""
        for artifact, paths in CHROMIUM_ARTIFACTS.items():
            for path in paths:
                # Should NOT start with profile directory name
                assert not path.startswith("Default/"), f"{artifact}: {path} starts with Default/"
                assert not path.startswith("Profile "), f"{artifact}: {path} starts with Profile"


# =============================================================================
# get_patterns() Tests
# =============================================================================

class TestGetPatterns:
    """Test get_patterns() function."""

    def test_chrome_history_patterns(self):
        """Chrome history patterns include all profile types."""
        patterns = get_patterns("chrome", "history")

        # Should have patterns for Default, Profile *, Guest, System
        assert any("Default/History" in p for p in patterns)
        assert any("Profile */History" in p for p in patterns)
        assert any("Guest Profile/History" in p for p in patterns)
        assert any("System Profile/History" in p for p in patterns)

    def test_chrome_windows_path(self):
        """Chrome patterns include Windows paths."""
        patterns = get_patterns("chrome", "history")
        windows_patterns = [p for p in patterns if "AppData/Local" in p]
        assert len(windows_patterns) > 0

    def test_chrome_macos_path(self):
        """Chrome patterns include macOS paths."""
        patterns = get_patterns("chrome", "history")
        macos_patterns = [p for p in patterns if "Library/Application Support" in p]
        assert len(macos_patterns) > 0

    def test_chrome_linux_path(self):
        """Chrome patterns include Linux paths."""
        patterns = get_patterns("chrome", "history")
        linux_patterns = [p for p in patterns if ".config/google-chrome" in p]
        assert len(linux_patterns) > 0

    def test_opera_flat_profile(self):
        """Opera patterns do NOT include Default/ prefix (flat profile)."""
        patterns = get_patterns("opera", "history")

        # Should NOT have Default/ in Opera paths
        assert not any("Default/" in p for p in patterns)

        # Should have direct path to History
        assert any(p.endswith("/History") for p in patterns)
        assert any("Opera Stable/History" in p for p in patterns)

    def test_opera_gx_flat_profile(self):
        """Opera GX patterns do NOT include Default/ prefix."""
        patterns = get_patterns("opera_gx", "history")

        assert not any("Default/" in p for p in patterns)
        assert any("Opera GX Stable/History" in p for p in patterns)

    def test_chromium_patterns(self):
        """Chromium (OSS) browser patterns work."""
        patterns = get_patterns("chromium", "history")

        assert any("Chromium/User Data/Default/History" in p for p in patterns)
        assert any(".config/chromium/Default/History" in p for p in patterns)

    def test_chrome_beta_patterns(self):
        """Chrome Beta patterns use correct paths."""
        patterns = get_patterns("chrome_beta", "history")

        assert any("Chrome Beta/User Data" in p for p in patterns)
        assert any("google-chrome-beta" in p for p in patterns)

    def test_chrome_canary_patterns(self):
        """Chrome Canary patterns use SxS on Windows."""
        patterns = get_patterns("chrome_canary", "history")

        # Windows uses SxS (Side-by-Side)
        assert any("Chrome SxS/User Data" in p for p in patterns)
        # macOS uses Chrome Canary
        assert any("Chrome Canary" in p for p in patterns)

    def test_edge_canary_patterns(self):
        """Edge Canary patterns use SxS on Windows."""
        patterns = get_patterns("edge_canary", "history")

        assert any("Edge SxS/User Data" in p for p in patterns)

    def test_invalid_browser_raises(self):
        """Invalid browser raises ValueError."""
        with pytest.raises(ValueError, match="Unknown browser"):
            get_patterns("netscape", "history")

    def test_invalid_artifact_raises(self):
        """Invalid artifact raises ValueError."""
        with pytest.raises(ValueError, match="Unknown artifact"):
            get_patterns("chrome", "passwords")

    def test_cookies_network_paths(self):
        """Cookies patterns include Network/ subdirectory (Chrome 96+)."""
        patterns = get_patterns("chrome", "cookies")

        assert any("Default/Cookies" in p for p in patterns)
        assert any("Default/Network/Cookies" in p for p in patterns)


# =============================================================================
# get_all_patterns() Tests
# =============================================================================

class TestGetAllPatterns:
    """Test get_all_patterns() function."""

    def test_includes_all_browsers(self):
        """Includes patterns for all browsers."""
        patterns = get_all_patterns("history")

        # Should have Chrome patterns
        assert any("google-chrome" in p for p in patterns)
        # Should have Chromium patterns
        assert any("chromium" in p.lower() for p in patterns)
        # Should have Edge patterns
        assert any("microsoft-edge" in p for p in patterns)
        # Should have Opera patterns (flat profile)
        assert any("Opera Stable/History" in p for p in patterns)
        # Should have Brave patterns
        assert any("Brave-Browser" in p for p in patterns)

    def test_pattern_count_substantial(self):
        """Returns substantial number of patterns."""
        patterns = get_all_patterns("history")
        # Should have many patterns (browsers × OS × profiles)
        assert len(patterns) > 50


# =============================================================================
# Helper Function Tests
# =============================================================================

class TestHelperFunctions:
    """Test helper functions."""

    def test_get_browser_display_name_known(self):
        """Display name for known browsers."""
        assert get_browser_display_name("chrome") == "Google Chrome"
        assert get_browser_display_name("chrome_beta") == "Google Chrome Beta"
        assert get_browser_display_name("chromium") == "Chromium"
        assert get_browser_display_name("opera") == "Opera"
        assert get_browser_display_name("opera_gx") == "Opera GX"

    def test_get_browser_display_name_unknown(self):
        """Display name for unknown browser returns capitalized key."""
        assert get_browser_display_name("netscape") == "Netscape"

    def test_get_all_browsers_count(self):
        """get_all_browsers returns all browser keys."""
        browsers = get_all_browsers()
        # Should include all 14 browsers
        assert len(browsers) >= 14

    def test_get_stable_browsers(self):
        """get_stable_browsers returns only stable channels."""
        stable = get_stable_browsers()

        assert "chrome" in stable
        assert "chromium" in stable
        assert "opera" in stable

        # Should NOT include beta/dev/canary
        assert "chrome_beta" not in stable
        assert "chrome_canary" not in stable

    def test_is_flat_profile_browser_opera(self):
        """Opera is a flat profile browser."""
        assert is_flat_profile_browser("opera") is True
        assert is_flat_profile_browser("opera_gx") is True

    def test_is_flat_profile_browser_chrome(self):
        """Chrome is NOT a flat profile browser."""
        assert is_flat_profile_browser("chrome") is False
        assert is_flat_profile_browser("edge") is False

    def test_is_flat_profile_browser_unknown(self):
        """Unknown browser returns False."""
        assert is_flat_profile_browser("netscape") is False

    def test_get_artifact_patterns_alias(self):
        """get_artifact_patterns is alias for get_patterns."""
        patterns1 = get_patterns("chrome", "history")
        patterns2 = get_artifact_patterns("chrome", "history")
        assert patterns1 == patterns2

    def test_get_patterns_for_root_profiled(self):
        """Dynamic root pattern generation includes profile variants."""
        root = "ProgramData/SomeApp/User Data"
        patterns = get_patterns_for_root(root, "cookies")
        assert any(p.endswith("Default/Cookies") for p in patterns)
        assert any(p.endswith("Profile */Network/Cookies") for p in patterns)

    def test_get_patterns_for_root_flat(self):
        """Flat dynamic root generation skips profile directory variants."""
        root = "ProgramData/SomeApp/ProfileRoot"
        patterns = get_patterns_for_root(root, "history", flat_profile=True)
        assert patterns == [f"{root}/History"]

    def test_transport_security_artifact_has_network_variant(self):
        """TransportSecurity pattern includes the Network/ prefixed location."""
        assert "Network/TransportSecurity" in CHROMIUM_ARTIFACTS["transport_security"]
