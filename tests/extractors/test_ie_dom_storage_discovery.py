"""Tests for IE DOMStore pattern discovery and browser detection."""

import pytest

from extractors.browser.ie_legacy._patterns import (
    get_patterns,
    get_all_patterns,
    detect_browser_from_path,
)


class TestIEDomStoragePatterns:
    """Verify IE file-based DOMStore patterns are defined."""

    def test_ie_dom_storage_patterns_non_empty(self):
        patterns = get_patterns("ie", "dom_storage")
        assert len(patterns) > 0, "IE dom_storage patterns should not be empty"

    def test_ie_dom_storage_has_inetcache_path(self):
        patterns = get_patterns("ie", "dom_storage")
        assert any("INetCache" in p for p in patterns)

    def test_ie_dom_storage_has_internet_explorer_path(self):
        patterns = get_patterns("ie", "dom_storage")
        assert any("Internet Explorer" in p for p in patterns)

    def test_ie_old_windows_dom_storage_non_empty(self):
        patterns = get_patterns("ie_old_windows", "dom_storage")
        assert len(patterns) > 0, "ie_old_windows dom_storage patterns should not be empty"

    def test_ie_old_windows_dom_storage_has_windows_old_prefix(self):
        patterns = get_patterns("ie_old_windows", "dom_storage")
        for p in patterns:
            assert p.startswith("Windows.old/"), f"Expected Windows.old prefix: {p}"


class TestEdgeLegacyDomStoragePatterns:
    """Verify Edge Legacy DOMStore patterns are still present."""

    def test_edge_legacy_dom_storage_non_empty(self):
        patterns = get_patterns("edge_legacy", "dom_storage")
        assert len(patterns) > 0

    def test_edge_legacy_has_uwp_package_path(self):
        patterns = get_patterns("edge_legacy", "dom_storage")
        assert any("Microsoft.MicrosoftEdge_" in p for p in patterns)


class TestGetAllDomStoragePatterns:
    """Verify get_all_patterns returns combined IE + Edge Legacy patterns."""

    def test_get_all_returns_both_browsers(self):
        all_patterns = get_all_patterns("dom_storage")
        ie_patterns = get_patterns("ie", "dom_storage")
        edge_patterns = get_patterns("edge_legacy", "dom_storage")
        for p in ie_patterns:
            assert p in all_patterns, f"IE pattern missing from all: {p}"
        for p in edge_patterns:
            assert p in all_patterns, f"Edge pattern missing from all: {p}"

    def test_get_all_includes_old_windows(self):
        all_patterns = get_all_patterns("dom_storage")
        old_win = get_patterns("ie_old_windows", "dom_storage")
        for p in old_win:
            assert p in all_patterns, f"ie_old_windows pattern missing from all: {p}"

    def test_get_all_no_duplicates(self):
        all_patterns = get_all_patterns("dom_storage")
        assert len(all_patterns) == len(set(all_patterns)), "Duplicate patterns found"

    def test_get_all_count(self):
        """Combined patterns should be at least as many as each individual set."""
        all_patterns = get_all_patterns("dom_storage")
        ie_patterns = get_patterns("ie", "dom_storage")
        edge_patterns = get_patterns("edge_legacy", "dom_storage")
        assert len(all_patterns) >= len(ie_patterns) + len(edge_patterns)


class TestDetectBrowserFromDomStorePath:
    """Verify detect_browser_from_path identifies IE vs Edge Legacy DOMStore paths."""

    @pytest.mark.parametrize("path,expected", [
        (
            "Users/JohnDoe/AppData/Local/Microsoft/Windows/INetCache/IE/DOMStore/ABC123/example.com.xml",
            "ie",
        ),
        (
            "Users/JohnDoe/AppData/Local/Microsoft/Windows/INetCache/Low/DOMStore/data.xml",
            "ie",
        ),
        (
            "Users/JohnDoe/AppData/Local/Microsoft/Internet Explorer/DOMStore/data.xml",
            "ie",
        ),
        (
            "Users/JohnDoe/AppData/LocalLow/Microsoft/Internet Explorer/DOMStore/data.xml",
            "ie",
        ),
        (
            "Users/JohnDoe/AppData/Local/Packages/Microsoft.MicrosoftEdge_8wekyb/AC/MicrosoftEdge/User/Default/DOMStore/file.xml",
            "edge_legacy",
        ),
        (
            "Windows.old/Users/JohnDoe/AppData/Local/Microsoft/Windows/INetCache/IE/DOMStore/data.xml",
            "ie_old_windows",
        ),
        (
            "Windows.old/Users/JohnDoe/AppData/Local/Microsoft/Windows/INetCache/Low/DOMStore/data.xml",
            "ie_old_windows",
        ),
    ])
    def test_detect_browser(self, path, expected):
        assert detect_browser_from_path(path) == expected
