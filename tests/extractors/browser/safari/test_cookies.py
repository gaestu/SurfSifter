"""
Tests for Safari Cookies Extractor.

Tests cover:
- Cookie parsing (_parsers.py)
- SafariCookiesExtractor metadata and methods
- Registry discovery
"""

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from extractors.browser.safari._parsers import (
    parse_cookies,
    get_cookie_stats,
    SafariCookie,
)
from extractors.browser.safari.cookies import SafariCookiesExtractor


# =============================================================================
# Parser Tests
# =============================================================================

class TestSafariCookieParsers:
    """Test Safari cookie parsers."""

    def test_parse_cookies_no_library(self, tmp_path):
        """parse_cookies returns empty without binarycookies library."""
        # Create a dummy file
        cookie_path = tmp_path / "Cookies.binarycookies"
        cookie_path.write_bytes(b"\x00" * 100)

        # Mock import failure
        with patch.dict('sys.modules', {'binarycookies': None}):
            cookies = parse_cookies(cookie_path)
            # Should return empty list when library unavailable
            assert cookies == []

    def test_get_cookie_stats_with_data(self):
        """get_cookie_stats calculates correct statistics."""
        cookies = [
            SafariCookie(
                domain=".example.com",
                name="session",
                value="abc123",
                path="/",
                expires=None,
                expires_utc=None,
                creation_time=None,
                creation_time_utc=None,
                is_secure=True,
                is_httponly=True,
            ),
            SafariCookie(
                domain=".test.org",
                name="pref",
                value="xyz",
                path="/",
                expires=None,
                expires_utc=None,
                creation_time=None,
                creation_time_utc=None,
                is_secure=False,
                is_httponly=False,
            ),
        ]

        stats = get_cookie_stats(cookies)
        assert stats["total_cookies"] == 2
        assert stats["unique_domains"] == 2
        assert stats["secure_count"] == 1
        assert stats["httponly_count"] == 1

    def test_get_cookie_stats_empty(self):
        """get_cookie_stats handles empty list."""
        stats = get_cookie_stats([])
        assert stats["total_cookies"] == 0
        assert stats["unique_domains"] == 0

    def test_safari_cookie_not_encrypted(self):
        """Safari cookies are not encrypted (unlike Chromium)."""
        cookie = SafariCookie(
            domain=".example.com",
            name="test",
            value="value",
            path="/",
            expires=None,
            expires_utc=None,
            creation_time=None,
            creation_time_utc=None,
            is_secure=False,
            is_httponly=False,
        )
        assert cookie.is_encrypted is False


# =============================================================================
# Extractor Tests
# =============================================================================

class TestSafariCookiesExtractor:
    """Test SafariCookiesExtractor class."""

    def test_extractor_metadata(self):
        """Extractor has correct metadata."""
        extractor = SafariCookiesExtractor()
        meta = extractor.metadata

        assert meta.name == "safari_cookies"
        assert "Safari" in meta.display_name
        assert "Cookies" in meta.display_name
        assert meta.can_extract is True
        assert meta.can_ingest is True

    def test_can_run_extraction_with_fs(self):
        """can_run_extraction returns True with evidence filesystem."""
        extractor = SafariCookiesExtractor()
        mock_fs = MagicMock()

        can_run, msg = extractor.can_run_extraction(mock_fs)
        assert can_run is True

    def test_can_run_extraction_without_fs(self):
        """can_run_extraction returns False without evidence filesystem."""
        extractor = SafariCookiesExtractor()

        can_run, msg = extractor.can_run_extraction(None)
        assert can_run is False

    def test_get_output_dir(self, tmp_path):
        """get_output_dir returns correct path."""
        extractor = SafariCookiesExtractor()

        output = extractor.get_output_dir(tmp_path, "evidence1")
        assert "safari_cookies" in str(output)

    def test_run_extraction_creates_manifest(self, tmp_path):
        """run_extraction creates manifest file."""
        extractor = SafariCookiesExtractor()
        output_dir = tmp_path / "output"

        mock_fs = MagicMock()
        mock_fs.iter_paths = MagicMock(return_value=[])

        callbacks = MagicMock()
        config = {"evidence_id": 1, "evidence_label": "test"}

        result = extractor.run_extraction(mock_fs, output_dir, config, callbacks)

        assert result is True
        assert (output_dir / "manifest.json").exists()
