"""Tests for Chromium shared path parsing helpers."""

from extractors.browser.chromium._parsers import (
    detect_browser_from_path,
    extract_profile_from_path,
)


class TestDetectBrowserFromPath:
    """Browser detection from artifact paths."""

    def test_detects_known_browser_channels(self):
        assert detect_browser_from_path("Users/Alice/AppData/Local/Google/Chrome Beta/User Data/Default/History") == "chrome_beta"
        assert detect_browser_from_path("home/alice/.config/google-chrome-unstable/Default/History") == "chrome_dev"
        assert detect_browser_from_path("Users/Alice/AppData/Local/Microsoft/Edge SxS/User Data/Default/History") == "edge_canary"
        assert detect_browser_from_path("home/alice/.config/BraveSoftware/Brave-Browser-Nightly/Default/History") == "brave_nightly"

    def test_detects_embedded_when_root_hint_is_provided(self):
        roots = ["ProgramData/SomeApp/User Data"]
        path = "ProgramData/SomeApp/User Data/Default/History"
        assert detect_browser_from_path(path, embedded_roots=roots) == "chromium_embedded"

    def test_returns_none_for_unrecognized_without_embedded_context(self):
        assert detect_browser_from_path("ProgramData/UnknownApp/State/history.db") is None


class TestExtractProfileFromPath:
    """Profile extraction across platform layouts."""

    def test_standard_user_data_profile(self):
        path = "Users/john/AppData/Local/Google/Chrome/User Data/Profile 2/History"
        assert extract_profile_from_path(path) == "Profile 2"

    def test_linux_profile_layout(self):
        path = "home/john/.config/google-chrome/Default/History"
        assert extract_profile_from_path(path) == "Default"

    def test_opera_profile_layout(self):
        path = "Users/john/AppData/Roaming/Opera Software/Opera Stable/History"
        assert extract_profile_from_path(path) == "Opera Stable"

    def test_flat_embedded_layout_defaults_profile(self):
        path = "ProgramData/SomeApp/User Data/History"
        assert extract_profile_from_path(path) == "Default"

    def test_unknown_non_artifact_path_returns_none(self):
        assert extract_profile_from_path("tmp/random/file.txt") is None
