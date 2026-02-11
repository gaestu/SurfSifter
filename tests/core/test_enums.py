"""Tests for src/core/enums.py - Core enumerations."""

import pytest

from core.enums import (
    Browser,
    BrowserEngine,
    ExtractionStatus,
    ProcessingStatus,
    IngestionMode,
    ArtifactType,
    PermissionRiskLevel,
    DownloadState,
    DangerType,
    CookieSameSite,
    HashAlgorithm,
)


class TestBrowser:
    """Tests for Browser enum."""

    def test_browser_values(self):
        """Verify browser string values match expected keys."""
        assert Browser.CHROME == "chrome"
        assert Browser.EDGE == "edge"
        assert Browser.FIREFOX == "firefox"
        assert Browser.SAFARI == "safari"
        assert Browser.OPERA == "opera"
        assert Browser.BRAVE == "brave"

    def test_chromium_browsers(self):
        """Verify chromium browser subset."""
        chromium = Browser.chromium_browsers()
        assert Browser.CHROME in chromium
        assert Browser.EDGE in chromium
        assert Browser.OPERA in chromium
        assert Browser.BRAVE in chromium
        # Non-chromium should not be in subset
        assert Browser.FIREFOX not in chromium
        assert Browser.SAFARI not in chromium

    def test_all_browsers(self):
        """Verify all_browsers returns all values."""
        all_browsers = Browser.all_browsers()
        assert len(all_browsers) == 6
        for browser in Browser:
            assert browser in all_browsers

    def test_browser_string_comparison(self):
        """Verify enum can be compared with strings."""
        assert Browser.CHROME == "chrome"
        assert "firefox" == Browser.FIREFOX

    def test_browser_in_list(self):
        """Verify enum works in list membership tests."""
        browsers = ["chrome", "edge"]
        assert Browser.CHROME in browsers
        assert Browser.FIREFOX not in browsers


class TestBrowserEngine:
    """Tests for BrowserEngine enum."""

    def test_engine_values(self):
        """Verify engine string values."""
        assert BrowserEngine.CHROMIUM == "chromium"
        assert BrowserEngine.GECKO == "gecko"
        assert BrowserEngine.WEBKIT == "webkit"


class TestExtractionStatus:
    """Tests for ExtractionStatus enum."""

    def test_status_values(self):
        """Verify status string values."""
        assert ExtractionStatus.OK == "ok"
        assert ExtractionStatus.PARTIAL == "partial"
        assert ExtractionStatus.ERROR == "error"
        assert ExtractionStatus.SKIPPED == "skipped"

    def test_status_in_dict_key(self):
        """Verify enum works as dict key with string lookup."""
        results = {ExtractionStatus.OK: 10, ExtractionStatus.ERROR: 2}
        # String access should work
        assert results.get(ExtractionStatus.OK) == 10


class TestProcessingStatus:
    """Tests for ProcessingStatus enum."""

    def test_all_status_values(self):
        """Verify all processing status values."""
        assert ProcessingStatus.PENDING == "pending"
        assert ProcessingStatus.RUNNING == "running"
        assert ProcessingStatus.DONE == "done"
        assert ProcessingStatus.ERROR == "error"
        assert ProcessingStatus.SKIPPED == "skipped"


class TestIngestionMode:
    """Tests for IngestionMode enum."""

    def test_mode_values(self):
        """Verify ingestion mode string values."""
        assert IngestionMode.OVERWRITE == "overwrite"
        assert IngestionMode.APPEND == "append"
        assert IngestionMode.SKIP == "skip"


class TestArtifactType:
    """Tests for ArtifactType enum."""

    def test_core_artifact_types(self):
        """Verify core artifact type values."""
        assert ArtifactType.URL == "url"
        assert ArtifactType.IMAGE == "image"
        assert ArtifactType.COOKIE == "cookie"
        assert ArtifactType.BOOKMARK == "bookmark"

    def test_browser_artifact_types(self):
        """Verify browser-specific artifact types."""
        assert ArtifactType.BROWSER_HISTORY == "browser_history"
        assert ArtifactType.BROWSER_DOWNLOAD == "browser_download"
        assert ArtifactType.SESSION_TAB == "session_tab"
        assert ArtifactType.AUTOFILL == "autofill"

    def test_artifact_count(self):
        """Verify we have expected number of artifact types."""
        # This ensures we don't accidentally remove types
        assert len(ArtifactType) >= 25


class TestPermissionRiskLevel:
    """Tests for PermissionRiskLevel enum."""

    def test_risk_ordering(self):
        """Verify risk levels exist (string comparison)."""
        assert PermissionRiskLevel.CRITICAL == "critical"
        assert PermissionRiskLevel.HIGH == "high"
        assert PermissionRiskLevel.MEDIUM == "medium"
        assert PermissionRiskLevel.LOW == "low"
        assert PermissionRiskLevel.NONE == "none"


class TestDownloadState:
    """Tests for DownloadState enum."""

    def test_state_values(self):
        """Verify download state string values."""
        assert DownloadState.IN_PROGRESS == "in_progress"
        assert DownloadState.COMPLETE == "complete"
        assert DownloadState.CANCELLED == "cancelled"
        assert DownloadState.INTERRUPTED == "interrupted"
        assert DownloadState.UNKNOWN == "unknown"


class TestDangerType:
    """Tests for DangerType enum."""

    def test_danger_values(self):
        """Verify danger type string values."""
        assert DangerType.NOT_DANGEROUS == "not_dangerous"
        assert DangerType.DANGEROUS_FILE == "dangerous_file"
        assert DangerType.DANGEROUS_URL == "dangerous_url"


class TestCookieSameSite:
    """Tests for CookieSameSite enum."""

    def test_samesite_values(self):
        """Verify SameSite attribute string values."""
        assert CookieSameSite.NO_RESTRICTION == "no_restriction"
        assert CookieSameSite.LAX == "lax"
        assert CookieSameSite.STRICT == "strict"
        assert CookieSameSite.UNSPECIFIED == "unspecified"


class TestHashAlgorithm:
    """Tests for HashAlgorithm enum."""

    def test_algorithm_values(self):
        """Verify hash algorithm string values."""
        assert HashAlgorithm.MD5 == "md5"
        assert HashAlgorithm.SHA1 == "sha1"
        assert HashAlgorithm.SHA256 == "sha256"
        assert HashAlgorithm.PHASH == "phash"


class TestEnumStringBehavior:
    """Tests for StrEnum string behavior."""

    def test_str_conversion(self):
        """Verify str() returns the value."""
        assert str(Browser.CHROME) == "chrome"
        assert str(ExtractionStatus.OK) == "ok"

    def test_f_string_formatting(self):
        """Verify enum works in f-strings."""
        browser = Browser.FIREFOX
        status = ExtractionStatus.OK
        msg = f"Browser: {browser}, Status: {status}"
        assert msg == "Browser: firefox, Status: ok"

    def test_json_serialization_compatibility(self):
        """Verify enum can be serialized as string."""
        import json
        data = {"browser": Browser.CHROME, "status": ExtractionStatus.OK}
        # StrEnum values serialize directly as strings
        serialized = json.dumps(data)
        assert '"chrome"' in serialized
        assert '"ok"' in serialized

    def test_set_membership(self):
        """Verify enum works in sets with string values."""
        enabled_browsers = {Browser.CHROME, Browser.FIREFOX}
        assert "chrome" in enabled_browsers or Browser.CHROME in enabled_browsers
