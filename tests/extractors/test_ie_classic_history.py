"""Tests for IE Classic History.IE5 patterns and extractor metadata."""

import pytest

from extractors.browser.ie_legacy._patterns import (
    get_patterns,
    get_all_patterns,
    detect_browser_from_path,
)
from extractors.browser.ie_legacy.classic_history.extractor import (
    IEClassicHistoryExtractor,
)


class TestHistoryIE5Patterns:
    """Verify History.IE5 patterns are defined for IE browsers."""

    def test_ie_history_patterns_non_empty(self):
        patterns = get_patterns("ie", "history")
        assert len(patterns) > 0, "IE history patterns should not be empty"

    def test_ie_history_has_container_dat(self):
        patterns = get_patterns("ie", "history")
        assert any("container.dat" in p for p in patterns)

    def test_ie_history_has_index_dat(self):
        patterns = get_patterns("ie", "history")
        assert any("index.dat" in p for p in patterns)

    def test_ie_history_has_history_ie5_path(self):
        patterns = get_patterns("ie", "history")
        assert any("History.IE5" in p for p in patterns)

    def test_ie_history_has_low_integrity_path(self):
        patterns = get_patterns("ie", "history")
        assert any("Low" in p and "History.IE5" in p for p in patterns)

    def test_ie_history_has_subdirectory_patterns(self):
        """History.IE5 subdirectories contain time-bucketed containers."""
        patterns = get_patterns("ie", "history")
        # Patterns with */container.dat or */index.dat under History.IE5
        subdir_patterns = [
            p for p in patterns
            if "History.IE5/*/" in p or "History.IE5\\*\\" in p
        ]
        assert len(subdir_patterns) > 0, "Should have subdirectory patterns"

    def test_ie_history_has_content_ie5_path(self):
        patterns = get_patterns("ie", "history")
        assert any("Content.IE5" in p for p in patterns)


class TestHistoryIE5OldWindowsPatterns:
    """Verify History.IE5 patterns exist for Windows.old."""

    def test_ie_old_windows_history_non_empty(self):
        patterns = get_patterns("ie_old_windows", "history")
        assert len(patterns) > 0, "ie_old_windows history patterns should not be empty"

    def test_ie_old_windows_has_windows_old_prefix(self):
        patterns = get_patterns("ie_old_windows", "history")
        for p in patterns:
            assert p.startswith("Windows.old/"), f"Expected Windows.old prefix: {p}"

    def test_ie_old_windows_has_history_ie5(self):
        patterns = get_patterns("ie_old_windows", "history")
        assert any("History.IE5" in p for p in patterns)


class TestGetAllHistoryPatterns:
    """Verify get_all_patterns returns combined IE + Edge Legacy + old Windows patterns."""

    def test_get_all_returns_ie_patterns(self):
        all_patterns = get_all_patterns("history")
        ie_patterns = get_patterns("ie", "history")
        for p in ie_patterns:
            assert p in all_patterns, f"IE pattern missing from all: {p}"

    def test_get_all_returns_old_windows_patterns(self):
        all_patterns = get_all_patterns("history")
        old_win = get_patterns("ie_old_windows", "history")
        for p in old_win:
            assert p in all_patterns, f"ie_old_windows pattern missing from all: {p}"

    def test_get_all_returns_edge_legacy_patterns(self):
        all_patterns = get_all_patterns("history")
        edge_patterns = get_patterns("edge_legacy", "history")
        for p in edge_patterns:
            assert p in all_patterns, f"Edge Legacy pattern missing from all: {p}"

    def test_get_all_no_duplicates(self):
        all_patterns = get_all_patterns("history")
        assert len(all_patterns) == len(set(all_patterns)), "Duplicate patterns found"


class TestDetectBrowserFromHistoryIE5Path:
    """Verify detect_browser_from_path identifies IE vs Edge for History.IE5 paths."""

    @pytest.mark.parametrize("path,expected", [
        (
            "Users/JohnDoe/AppData/Local/Microsoft/Windows/History/History.IE5/container.dat",
            "ie",
        ),
        (
            "Users/JohnDoe/AppData/Local/Microsoft/Windows/History/History.IE5/index.dat",
            "ie",
        ),
        (
            "Users/JohnDoe/AppData/Local/Microsoft/Windows/History/Low/History.IE5/container.dat",
            "ie",
        ),
        (
            "Users/JohnDoe/AppData/Local/Microsoft/Windows/History/History.IE5/MSHist012024/container.dat",
            "ie",
        ),
        (
            "Windows.old/Users/JohnDoe/AppData/Local/Microsoft/Windows/History/History.IE5/container.dat",
            "ie_old_windows",
        ),
        (
            "Users/JohnDoe/AppData/Local/Packages/Microsoft.MicrosoftEdge_8wekyb/AC/MicrosoftEdge/History/container.dat",
            "edge_legacy",
        ),
    ])
    def test_detect_browser(self, path, expected):
        assert detect_browser_from_path(path) == expected


class TestIEClassicHistoryExtractorMetadata:
    """Verify extractor metadata is correct."""

    def test_name(self):
        ext = IEClassicHistoryExtractor()
        assert ext.metadata.name == "ie_classic_history"

    def test_can_extract(self):
        ext = IEClassicHistoryExtractor()
        assert ext.metadata.can_extract is True

    def test_can_ingest(self):
        ext = IEClassicHistoryExtractor()
        assert ext.metadata.can_ingest is True

    def test_category(self):
        ext = IEClassicHistoryExtractor()
        assert ext.metadata.category == "browser"

    def test_no_required_tools(self):
        ext = IEClassicHistoryExtractor()
        assert ext.metadata.requires_tools == []

    def test_can_run_extraction_no_fs(self):
        ext = IEClassicHistoryExtractor()
        can_run, reason = ext.can_run_extraction(None)
        assert can_run is False

    def test_can_run_ingestion_no_manifest(self, tmp_path):
        ext = IEClassicHistoryExtractor()
        can_run, reason = ext.can_run_ingestion(tmp_path)
        assert can_run is False

    def test_can_run_ingestion_with_manifest(self, tmp_path):
        (tmp_path / "manifest.json").write_text("{}")
        ext = IEClassicHistoryExtractor()
        can_run, reason = ext.can_run_ingestion(tmp_path)
        assert can_run is True

    def test_output_dir(self, tmp_path):
        ext = IEClassicHistoryExtractor()
        out = ext.get_output_dir(tmp_path, "evidence_01")
        assert out == tmp_path / "evidences" / "evidence_01" / "ie_classic_history"


class TestURLExtractionFromBinary:
    """Test regex-based URL extraction from binary data."""

    def test_extract_ascii_urls(self):
        ext = IEClassicHistoryExtractor()
        data = b'\x00\x00http://www.example.com/page\x00\x00'
        urls = ext._extract_urls_from_binary(data)
        assert "http://www.example.com/page" in urls

    def test_extract_https_url(self):
        ext = IEClassicHistoryExtractor()
        data = b'\x00https://secure.example.com/login\x00'
        urls = ext._extract_urls_from_binary(data)
        assert "https://secure.example.com/login" in urls

    def test_deduplicates_urls(self):
        ext = IEClassicHistoryExtractor()
        data = b'http://www.example.com/\x00http://www.example.com/\x00'
        urls = ext._extract_urls_from_binary(data)
        assert urls.count("http://www.example.com/") == 1

    def test_rejects_invalid_tld(self):
        ext = IEClassicHistoryExtractor()
        # URL with numeric-only TLD should be rejected
        data = b'http://example.123\x00'
        urls = ext._extract_urls_from_binary(data)
        assert len(urls) == 0

    def test_empty_data(self):
        ext = IEClassicHistoryExtractor()
        urls = ext._extract_urls_from_binary(b'')
        assert urls == []

    def test_multiple_urls(self):
        ext = IEClassicHistoryExtractor()
        data = (
            b'\x00http://www.google.com/search?q=test\x00'
            b'\x00https://www.bing.com/\x00'
            b'padding bytes here'
            b'\x00http://example.org/path/to/page\x00'
        )
        urls = ext._extract_urls_from_binary(data)
        assert len(urls) == 3
