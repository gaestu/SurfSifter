"""
Tests for sandbox browser functionality.

Initial implementation.
"""
from __future__ import annotations

import platform
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestSandboxAvailability:
    """Tests for sandbox method detection."""

    def test_detect_browser_finds_firefox(self):
        """Test browser detection with Firefox available."""
        from app.common.dialogs.sandbox_browser import detect_browser

        with patch("shutil.which") as mock_which:
            mock_which.side_effect = lambda x: "/usr/bin/firefox" if x == "firefox" else None
            result = detect_browser()
            assert result == "firefox"

    def test_detect_browser_finds_chrome(self):
        """Test browser detection with Chrome available."""
        from app.common.dialogs.sandbox_browser import detect_browser

        def which_side_effect(cmd):
            if cmd == "google-chrome":
                return "/usr/bin/google-chrome"
            return None

        with patch("shutil.which", side_effect=which_side_effect):
            result = detect_browser()
            assert result == "google-chrome"

    def test_detect_browser_returns_none_when_none_available(self):
        """Test browser detection returns None when no browser found."""
        from app.common.dialogs.sandbox_browser import detect_browser

        with patch("shutil.which", return_value=None):
            result = detect_browser()
            assert result is None

    def test_has_firejail_linux_with_firejail(self):
        """Test firejail detection on Linux with firejail installed."""
        from app.common.dialogs.sandbox_browser import has_firejail

        with patch("platform.system", return_value="Linux"):
            with patch("shutil.which", return_value="/usr/bin/firejail"):
                assert has_firejail() is True

    def test_has_firejail_linux_without_firejail(self):
        """Test firejail detection on Linux without firejail."""
        from app.common.dialogs.sandbox_browser import has_firejail

        with patch("platform.system", return_value="Linux"):
            with patch("shutil.which", return_value=None):
                assert has_firejail() is False

    def test_has_firejail_windows(self):
        """Test firejail detection on Windows (always False)."""
        from app.common.dialogs.sandbox_browser import has_firejail

        with patch("platform.system", return_value="Windows"):
            # Even if firejail is somehow in PATH, should be False on Windows
            with patch("shutil.which", return_value="/some/path/firejail"):
                assert has_firejail() is False

    def test_is_snap_wrapper_detects_snap_script(self, tmp_path):
        """Test snap wrapper detection with a fake snap wrapper script."""
        from app.common.dialogs.sandbox_browser import _is_snap_wrapper

        # Create a fake snap wrapper script
        snap_wrapper = tmp_path / "firefox"
        snap_wrapper.write_text(
            '#!/bin/sh\necho "Command requires the firefox snap to be installed"\n'
            'snap install firefox\n'
        )

        assert _is_snap_wrapper(str(snap_wrapper)) is True

    def test_is_snap_wrapper_returns_false_for_real_binary(self, tmp_path):
        """Test snap wrapper detection returns False for real binaries."""
        from app.common.dialogs.sandbox_browser import _is_snap_wrapper

        # Create a fake "real" binary (large file with no snap indicators)
        real_binary = tmp_path / "chrome"
        real_binary.write_bytes(b'\x7fELF' + b'\x00' * 20000)  # ELF header + padding

        assert _is_snap_wrapper(str(real_binary)) is False

    def test_is_snap_wrapper_returns_false_for_nonexistent(self):
        """Test snap wrapper detection returns False for nonexistent files."""
        from app.common.dialogs.sandbox_browser import _is_snap_wrapper

        assert _is_snap_wrapper("/nonexistent/path/browser") is False

    def test_detect_browser_skips_snap_for_firejail(self, tmp_path):
        """Test browser detection skips snap browsers when for_firejail=True."""
        from app.common.dialogs import sandbox_browser
        from app.common.dialogs.sandbox_browser import detect_browser

        # Mock: firefox is a snap, chromium is real
        def mock_which(cmd):
            if cmd == "firefox":
                return "/usr/bin/firefox"
            elif cmd == "chromium":
                return "/usr/bin/chromium"
            return None

        def mock_is_snap(path):
            return "firefox" in path

        with patch("shutil.which", side_effect=mock_which):
            with patch.object(sandbox_browser, "_is_snap_wrapper", side_effect=mock_is_snap):
                # Without firejail flag, should return firefox (first in list)
                assert detect_browser(for_firejail=False) == "firefox"
                # With firejail flag, should skip firefox and return chromium
                assert detect_browser(for_firejail=True) == "chromium"

    def test_get_sandbox_availability_all_available(self):
        """Test availability check with all methods available."""
        from app.common.dialogs import sandbox_browser
        from app.common.dialogs.sandbox_browser import get_sandbox_availability

        with patch.object(sandbox_browser, "HAS_WEBENGINE", True):
            with patch("platform.system", return_value="Linux"):
                with patch("shutil.which") as mock_which:
                    mock_which.side_effect = lambda x: f"/usr/bin/{x}" if x in ("firejail", "firefox") else None
                    # Mock snap wrapper detection to return False (not a snap)
                    with patch.object(sandbox_browser, "_is_snap_wrapper", return_value=False):
                        result = get_sandbox_availability()

                        assert result["webengine"] is True
                        assert result["firejail"] is True
                        assert result["firejail_compatible"] is True
                        assert result["external_browser"] == "firefox"
                        assert result["firejail_browser"] == "firefox"
                        assert result["recommended"] == "embedded"

    def test_get_sandbox_availability_only_firejail(self):
        """Test availability check with only firejail+browser."""
        from app.common.dialogs import sandbox_browser
        from app.common.dialogs.sandbox_browser import get_sandbox_availability

        with patch.object(sandbox_browser, "HAS_WEBENGINE", False):
            with patch("platform.system", return_value="Linux"):
                with patch("shutil.which") as mock_which:
                    mock_which.side_effect = lambda x: f"/usr/bin/{x}" if x in ("firejail", "firefox") else None
                    # Mock snap wrapper detection to return False (not a snap)
                    with patch.object(sandbox_browser, "_is_snap_wrapper", return_value=False):
                        result = get_sandbox_availability()

                        assert result["webengine"] is False
                        assert result["firejail"] is True
                        assert result["firejail_compatible"] is True
                        assert result["recommended"] == "firejail"

    def test_get_sandbox_availability_only_browser(self):
        """Test availability check with only external browser."""
        from app.common.dialogs import sandbox_browser
        from app.common.dialogs.sandbox_browser import get_sandbox_availability

        with patch.object(sandbox_browser, "HAS_WEBENGINE", False):
            with patch("platform.system", return_value="Linux"):
                with patch("shutil.which") as mock_which:
                    mock_which.side_effect = lambda x: "/usr/bin/firefox" if x == "firefox" else None

                    result = get_sandbox_availability()

                    assert result["webengine"] is False
                    assert result["firejail"] is False
                    assert result["external_browser"] == "firefox"
                    assert result["recommended"] == "external"

    def test_get_sandbox_availability_none(self):
        """Test availability check with nothing available."""
        from app.common.dialogs import sandbox_browser
        from app.common.dialogs.sandbox_browser import get_sandbox_availability

        with patch.object(sandbox_browser, "HAS_WEBENGINE", False):
            with patch("shutil.which", return_value=None):
                result = get_sandbox_availability()

                assert result["webengine"] is False
                assert result["firejail"] is False
                assert result["external_browser"] is None
                assert result["recommended"] == "none"


class TestSandboxSettings:
    """Tests for SandboxSettings dataclass."""

    def test_default_settings(self):
        """Test default sandbox settings."""
        from app.common.dialogs.sandbox_browser import SandboxSettings

        settings = SandboxSettings()

        assert settings.prefer_external is False
        assert settings.javascript_enabled is False
        assert settings.external_browser == ""
        assert settings.firejail_net_none is False
        assert settings.log_opens is True

    def test_custom_settings(self):
        """Test custom sandbox settings."""
        from app.common.dialogs.sandbox_browser import SandboxSettings

        settings = SandboxSettings(
            prefer_external=True,
            javascript_enabled=True,
            external_browser="brave-browser",
        )

        assert settings.prefer_external is True
        assert settings.javascript_enabled is True
        assert settings.external_browser == "brave-browser"


class TestExternalSandboxLauncher:
    """Tests for external browser sandbox launching."""

    def test_open_url_external_with_firejail(self):
        """Test launching URL with firejail on Linux."""
        from app.common.dialogs import sandbox_browser
        from app.common.dialogs.sandbox_browser import open_url_external_sandboxed, SandboxSettings

        with patch("platform.system", return_value="Linux"):
            with patch("shutil.which") as mock_which:
                mock_which.side_effect = lambda x: f"/usr/bin/{x}" if x in ("firejail", "google-chrome") else None
                # Mock snap wrapper detection to return False (not a snap)
                with patch.object(sandbox_browser, "_is_snap_wrapper", return_value=False):
                    with patch("subprocess.Popen") as mock_popen:
                        with patch("tempfile.mkdtemp", return_value="/tmp/forensic-sandbox-12345"):
                            settings = SandboxSettings()
                            result = open_url_external_sandboxed(
                                "https://example.com",
                                settings=settings,
                            )

                            assert result is True
                            mock_popen.assert_called_once()
                            cmd = mock_popen.call_args[0][0]
                            assert cmd[0] == "firejail"
                            assert "--noprofile" in cmd  # Uses noprofile to avoid restrictive defaults
                            assert "google-chrome" in cmd
                            assert "--incognito" in cmd  # Chrome uses incognito
                            assert "--user-data-dir=/tmp/forensic-sandbox-12345" in cmd
                            assert "https://example.com" in cmd

    def test_open_url_external_with_firejail_net_none(self):
        """Test launching URL with firejail --net=none option."""
        from app.common.dialogs import sandbox_browser
        from app.common.dialogs.sandbox_browser import open_url_external_sandboxed, SandboxSettings

        with patch("platform.system", return_value="Linux"):
            with patch("shutil.which") as mock_which:
                mock_which.side_effect = lambda x: f"/usr/bin/{x}" if x in ("firejail", "google-chrome") else None
                # Mock snap wrapper detection to return False (not a snap)
                with patch.object(sandbox_browser, "_is_snap_wrapper", return_value=False):
                    with patch("subprocess.Popen") as mock_popen:
                        with patch("tempfile.mkdtemp", return_value="/tmp/forensic-sandbox-12345"):
                            settings = SandboxSettings(firejail_net_none=True)
                            result = open_url_external_sandboxed(
                                "https://example.com",
                                settings=settings,
                            )

                            assert result is True
                            cmd = mock_popen.call_args[0][0]
                            assert "--net=none" in cmd

    def test_open_url_external_disposable_profile_firefox(self):
        """Test launching Firefox with disposable profile (no firejail)."""
        from app.common.dialogs.sandbox_browser import open_url_external_sandboxed, SandboxSettings

        with patch("platform.system", return_value="Windows"):  # No firejail on Windows
            with patch("shutil.which") as mock_which:
                mock_which.side_effect = lambda x: "/usr/bin/firefox" if x == "firefox" else None

                with patch("subprocess.Popen") as mock_popen:
                    with patch("tempfile.mkdtemp", return_value="/tmp/forensic-sandbox-12345"):
                        result = open_url_external_sandboxed("https://example.com")

                        assert result is True
                        cmd = mock_popen.call_args[0][0]
                        assert "firefox" in cmd[0]
                        assert "--private-window" in cmd
                        assert "--profile" in cmd
                        assert "/tmp/forensic-sandbox-12345" in cmd

    def test_open_url_external_disposable_profile_chrome(self):
        """Test launching Chrome with disposable profile (no firejail)."""
        from app.common.dialogs.sandbox_browser import open_url_external_sandboxed, SandboxSettings

        with patch("platform.system", return_value="Windows"):
            with patch("shutil.which") as mock_which:
                mock_which.side_effect = lambda x: "/usr/bin/google-chrome" if x == "google-chrome" else None

                with patch("subprocess.Popen") as mock_popen:
                    with patch("tempfile.mkdtemp", return_value="/tmp/forensic-sandbox-12345"):
                        result = open_url_external_sandboxed("https://example.com")

                        assert result is True
                        cmd = mock_popen.call_args[0][0]
                        assert "google-chrome" in cmd[0]
                        assert "--incognito" in cmd
                        assert "--user-data-dir=/tmp/forensic-sandbox-12345" in cmd

    def test_open_url_external_no_browser_fails(self):
        """Test external launch fails gracefully when no browser available."""
        from app.common.dialogs.sandbox_browser import open_url_external_sandboxed

        with patch("shutil.which", return_value=None):
            result = open_url_external_sandboxed("https://example.com")
            assert result is False

    def test_open_url_external_audit_callback(self):
        """Test audit callback is called on successful launch."""
        from app.common.dialogs import sandbox_browser
        from app.common.dialogs.sandbox_browser import open_url_external_sandboxed, SandboxSettings

        audit_calls = []

        def audit_callback(url, method, browser):
            audit_calls.append((url, method, browser))

        with patch("platform.system", return_value="Linux"):
            with patch("shutil.which") as mock_which:
                mock_which.side_effect = lambda x: f"/usr/bin/{x}" if x in ("firejail", "google-chrome") else None
                # Mock snap wrapper detection to return False (not a snap)
                with patch.object(sandbox_browser, "_is_snap_wrapper", return_value=False):
                    with patch("subprocess.Popen"):
                        with patch("tempfile.mkdtemp", return_value="/tmp/forensic-sandbox-12345"):
                            settings = SandboxSettings(log_opens=True)
                            open_url_external_sandboxed(
                                "https://example.com",
                                settings=settings,
                                audit_callback=audit_callback,
                            )

                            assert len(audit_calls) == 1
                            assert audit_calls[0][0] == "https://example.com"
                            assert audit_calls[0][1] == "firejail"
                            assert audit_calls[0][2] == "google-chrome"

    def test_open_url_external_audit_callback_disabled(self):
        """Test audit callback not called when log_opens is False."""
        from app.common.dialogs import sandbox_browser
        from app.common.dialogs.sandbox_browser import open_url_external_sandboxed, SandboxSettings

        audit_calls = []

        def audit_callback(url, method, browser):
            audit_calls.append((url, method, browser))

        with patch("platform.system", return_value="Linux"):
            with patch("shutil.which") as mock_which:
                mock_which.side_effect = lambda x: f"/usr/bin/{x}" if x in ("firejail", "google-chrome") else None
                # Mock snap wrapper detection to return False (not a snap)
                with patch.object(sandbox_browser, "_is_snap_wrapper", return_value=False):
                    with patch("subprocess.Popen"):
                        with patch("tempfile.mkdtemp", return_value="/tmp/forensic-sandbox-12345"):
                            settings = SandboxSettings(log_opens=False)
                            open_url_external_sandboxed(
                                "https://example.com",
                                settings=settings,
                                audit_callback=audit_callback,
                            )

                            assert len(audit_calls) == 0


class TestAppSettingsSandbox:
    """Tests for SandboxSettings integration with AppSettings."""

    def test_app_settings_includes_sandbox(self):
        """Test AppSettings includes sandbox settings."""
        from app.config.settings import AppSettings, SandboxSettings

        settings = AppSettings()

        assert hasattr(settings, "sandbox")
        assert isinstance(settings.sandbox, SandboxSettings)

    def test_app_settings_save_load_sandbox(self, tmp_path):
        """Test sandbox settings persist through save/load cycle."""
        from app.config.settings import AppSettings, SandboxSettings

        settings = AppSettings()
        settings.sandbox.prefer_external = True
        settings.sandbox.javascript_enabled = True
        settings.sandbox.external_browser = "brave-browser"

        settings_file = tmp_path / "settings.json"
        settings.save(settings_file)

        loaded = AppSettings.load(settings_file)

        assert loaded.sandbox.prefer_external is True
        assert loaded.sandbox.javascript_enabled is True
        assert loaded.sandbox.external_browser == "brave-browser"

    def test_app_settings_load_without_sandbox_uses_defaults(self, tmp_path):
        """Test loading old config without sandbox section uses defaults."""
        import json
        from app.config.settings import AppSettings

        # Simulate old config without sandbox
        old_config = {
            "general": {"thumbnail_size": 180},
            "tools": {},
            "network": {},
            "hash": {},
        }

        settings_file = tmp_path / "settings.json"
        settings_file.write_text(json.dumps(old_config))

        loaded = AppSettings.load(settings_file)

        # Should have default sandbox settings
        assert loaded.sandbox.prefer_external is False
        assert loaded.sandbox.javascript_enabled is False


class TestDialogsExports:
    """Tests for proper exports from dialogs package."""

    def test_sandbox_browser_exported(self):
        """Test sandbox browser components are exported."""
        from app.common.dialogs import (
            SandboxBrowserDialog,
            open_url_sandboxed,
            open_url_external_sandboxed,
            get_sandbox_availability,
            has_firejail,
            detect_browser,
        )

        # Just verify imports work
        assert callable(open_url_sandboxed)
        assert callable(open_url_external_sandboxed)
        assert callable(get_sandbox_availability)
        assert callable(has_firejail)
        assert callable(detect_browser)


# GUI tests for SandboxBrowserDialog would require pytest-qt and QtWebEngine
# Marked as gui tests for separate test run
@pytest.mark.gui_offscreen
class TestSandboxBrowserDialogGUI:
    """GUI tests for SandboxBrowserDialog."""

    def test_dialog_creation(self, qtbot):
        """Test SandboxBrowserDialog can be created."""
        pytest.importorskip("PySide6.QtWebEngineWidgets")

        from app.common.dialogs import SandboxBrowserDialog

        dialog = SandboxBrowserDialog("https://example.com")
        qtbot.addWidget(dialog)

        assert dialog.windowTitle().startswith("🔒 Sandbox")
        assert dialog.url == "https://example.com"

    def test_dialog_security_settings(self, qtbot):
        """Test dialog applies security settings."""
        pytest.importorskip("PySide6.QtWebEngineWidgets")

        from app.common.dialogs import SandboxBrowserDialog
        from app.common.dialogs.sandbox_browser import SandboxSettings
        from PySide6.QtWebEngineCore import QWebEngineSettings

        settings = SandboxSettings(javascript_enabled=False)
        dialog = SandboxBrowserDialog("https://example.com", settings=settings)
        qtbot.addWidget(dialog)

        # Check JavaScript is disabled
        js_enabled = dialog.page.settings().testAttribute(
            QWebEngineSettings.WebAttribute.JavascriptEnabled
        )
        assert js_enabled is False

    def test_dialog_javascript_toggle(self, qtbot):
        """Test JavaScript can be toggled."""
        pytest.importorskip("PySide6.QtWebEngineWidgets")

        from app.common.dialogs import SandboxBrowserDialog
        from PySide6.QtWebEngineCore import QWebEngineSettings

        dialog = SandboxBrowserDialog("https://example.com")
        qtbot.addWidget(dialog)

        # Initially disabled
        assert dialog.js_checkbox.isChecked() is False

        # Toggle on
        dialog.js_checkbox.setChecked(True)
        js_enabled = dialog.page.settings().testAttribute(
            QWebEngineSettings.WebAttribute.JavascriptEnabled
        )
        assert js_enabled is True
