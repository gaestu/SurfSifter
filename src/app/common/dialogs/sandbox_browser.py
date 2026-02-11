"""
Sandbox browser dialog for safely viewing URLs from evidence.

This module provides a safe way to open URLs found in forensic evidence:
1. QtWebEngine-based in-app viewer with restricted settings (all platforms)
2. Firejail external browser launcher (Linux, enhanced isolation)

Usage:
    from app.common.dialogs import SandboxBrowserDialog, open_url_sandboxed

    # In-app viewer
    dialog = SandboxBrowserDialog(url, parent)
    dialog.exec()

    # Auto-select best method
    open_url_sandboxed(url, parent, prefer_external=False)

Initial implementation.
"""
from __future__ import annotations

import logging
import platform
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List

from PySide6.QtCore import Qt, QUrl, Signal
from PySide6.QtWidgets import (
    QDialog,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QToolBar,
    QLineEdit,
    QMessageBox,
    QCheckBox,
    QWidget,
)

logger = logging.getLogger(__name__)

# Check for QtWebEngine availability
try:
    from PySide6.QtWebEngineWidgets import QWebEngineView
    from PySide6.QtWebEngineCore import (
        QWebEngineProfile,
        QWebEnginePage,
        QWebEngineSettings,
    )
    HAS_WEBENGINE = True
except ImportError:
    HAS_WEBENGINE = False
    logger.warning("QtWebEngine not available - embedded sandbox browser disabled")


@dataclass
class SandboxSettings:
    """Settings for sandbox browser behavior."""

    # Prefer external browser with Firejail (Linux only)
    prefer_external: bool = False

    # JavaScript enabled in embedded viewer (security vs functionality tradeoff)
    javascript_enabled: bool = False

    # External browser command (auto-detected if empty)
    external_browser: str = ""

    # Firejail options
    firejail_net_none: bool = False  # Block network (for offline analysis of cached content)

    # Audit logging
    log_opens: bool = True


def _is_snap_wrapper(browser_path: str) -> bool:
    """
    Check if a browser executable is a snap wrapper.

    On Ubuntu, /usr/bin/firefox may be a wrapper script that requires snap.
    Firejail doesn't work with snap applications.

    Args:
        browser_path: Path to browser executable

    Returns:
        True if the browser is a snap wrapper, False otherwise.
    """
    if not browser_path:
        return False

    try:
        path = Path(browser_path)
        if not path.exists():
            return False

        # Check if it's a small script (snap wrappers are tiny)
        if path.stat().st_size > 10000:
            return False  # Real binary, not a wrapper

        # Read the file and check for snap indicators
        try:
            content = path.read_text(encoding="utf-8", errors="ignore")
            snap_indicators = [
                "snap install",
                "requires the",
                "snap to be installed",
                "/snap/",
            ]
            return any(indicator in content for indicator in snap_indicators)
        except (PermissionError, IsADirectoryError):
            return False

    except Exception:
        return False


def detect_browser(for_firejail: bool = False) -> Optional[str]:
    """
    Detect available browser on the system.

    Args:
        for_firejail: If True, skip snap-wrapped browsers (incompatible with firejail)

    Returns:
        Browser executable name or None if not found.
    """
    # Preference order for firejail: non-snap browsers first
    # Chromium-based browsers are usually not snaps on Ubuntu
    if for_firejail:
        candidates = [
            "chromium",
            "chromium-browser",
            "google-chrome",
            "google-chrome-stable",
            "brave-browser",
            "firefox-esr",  # ESR is often a .deb, not snap
            "firefox",  # Check last, often a snap on Ubuntu
            "microsoft-edge",
        ]
    else:
        # General detection: Firefox first (better privacy)
        candidates = [
            "firefox",
            "firefox-esr",
            "chromium",
            "chromium-browser",
            "google-chrome",
            "google-chrome-stable",
            "brave-browser",
            "microsoft-edge",
        ]

    for browser in candidates:
        browser_path = shutil.which(browser)
        if browser_path:
            # For firejail, skip snap wrappers
            if for_firejail and _is_snap_wrapper(browser_path):
                logger.debug("Skipping snap-wrapped browser for firejail: %s", browser)
                continue
            return browser

    return None


def has_firejail() -> bool:
    """Check if firejail is available (Linux only)."""
    return platform.system() == "Linux" and shutil.which("firejail") is not None


def has_firejail_compatible_browser() -> bool:
    """Check if firejail is available AND a compatible (non-snap) browser exists."""
    if not has_firejail():
        return False
    return detect_browser(for_firejail=True) is not None


def open_url_external_sandboxed(
    url: str,
    settings: Optional[SandboxSettings] = None,
    audit_callback: Optional[callable] = None,
) -> bool:
    """
    Open URL in external browser with sandbox isolation.

    On Linux with Firejail: Uses firejail for process isolation.
    Otherwise: Uses disposable browser profile.

    Args:
        url: URL to open
        settings: Sandbox settings (uses defaults if None)
        audit_callback: Optional callback for audit logging (url, method, browser)

    Returns:
        True if browser was launched, False on error.
    """
    if settings is None:
        settings = SandboxSettings()

    # For firejail, use firejail-compatible browser detection (skip snap wrappers)
    use_firejail = has_firejail() and not settings.external_browser

    if use_firejail:
        browser = settings.external_browser or detect_browser(for_firejail=True)
    else:
        browser = settings.external_browser or detect_browser(for_firejail=False)

    if not browser:
        if use_firejail:
            logger.error("No firejail-compatible browser found (snap browsers don't work with firejail)")
        else:
            logger.error("No browser found for external sandbox")
        return False

    try:
        if use_firejail:
            # Build firejail command with temporary profile for browser isolation
            # We use a temp directory for the browser profile to ensure:
            # 1. No contamination of investigator's main browser profile
            # 2. Network access works (unlike --private which can break it)
            # 3. Session data is isolated and can be deleted after
            temp_profile = tempfile.mkdtemp(prefix="forensic-sandbox-")

            # Use --noprofile to avoid system firejail profiles that may block networking
            # This still provides namespace isolation but without restrictive defaults
            cmd: List[str] = ["firejail", "--noprofile"]

            if settings.firejail_net_none:
                cmd.append("--net=none")

            # Add browser with temporary profile
            if "firefox" in browser.lower():
                cmd.extend([
                    browser,
                    "--private-window",
                    "--profile", temp_profile,
                    "--no-remote",
                    url,
                ])
            else:
                # Chromium-based browsers
                cmd.extend([
                    browser,
                    "--incognito",
                    f"--user-data-dir={temp_profile}",
                    "--no-first-run",
                    "--no-default-browser-check",
                    url,
                ])
            method = "firejail"
        else:
            # Disposable profile fallback
            temp_profile = tempfile.mkdtemp(prefix="forensic-sandbox-")

            if "firefox" in browser.lower():
                cmd = [
                    browser,
                    "--private-window",
                    "--profile", temp_profile,
                    "--no-remote",
                    url,
                ]
            else:
                # Chromium-based browsers
                cmd = [
                    browser,
                    "--incognito",
                    f"--user-data-dir={temp_profile}",
                    "--no-first-run",
                    "--no-default-browser-check",
                    url,
                ]
            method = "disposable-profile"

        logger.info("Opening URL in sandbox: %s (method=%s, browser=%s)", url[:100], method, browser)
        subprocess.Popen(cmd, start_new_session=True)

        if audit_callback and settings.log_opens:
            audit_callback(url, method, browser)

        return True

    except Exception as e:
        logger.error("Failed to open URL in external sandbox: %s", e)
        return False


class SandboxBrowserDialog(QDialog):
    """
    In-app sandbox browser using QtWebEngine.

    Provides a restricted browsing environment for safely viewing
    URLs found in forensic evidence without contaminating the
    investigator's normal browser profile.

    Features:
    - Isolated profile (no cookies, history, etc.)
    - Optional JavaScript disable
    - No plugins, local storage disabled
    - Screenshot capability
    - Navigation controls
    """

    # Signal emitted when URL is opened (for audit logging)
    url_opened = Signal(str)  # url

    def __init__(
        self,
        url: str,
        parent: Optional[QWidget] = None,
        settings: Optional[SandboxSettings] = None,
        forensic_context=None,  # Optional[ForensicContext] - avoid circular import
    ):
        super().__init__(parent)

        if not HAS_WEBENGINE:
            raise RuntimeError("QtWebEngine not available")

        self.url = url
        self.settings = settings or SandboxSettings()
        self.forensic_context = forensic_context

        self.setWindowTitle(f"🔒 Sandbox Browser")
        self.setMinimumSize(900, 700)
        self.resize(1200, 800)

        self._setup_ui()
        self._setup_webview()

        # Navigate to URL
        self.webview.setUrl(QUrl(url))
        self.url_opened.emit(url)

    def _setup_ui(self) -> None:
        """Set up the dialog UI."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Warning banner
        warning_widget = QWidget()
        warning_widget.setStyleSheet(
            "background-color: #fff3cd; border-bottom: 1px solid #ffc107;"
        )
        warning_layout = QHBoxLayout(warning_widget)
        warning_layout.setContentsMargins(10, 5, 10, 5)

        warning_label = QLabel(
            "⚠️ <b>Sandbox Mode</b> - Isolated browsing session. "
            "No data persists after closing."
        )
        warning_layout.addWidget(warning_label)
        warning_layout.addStretch()

        self.js_checkbox = QCheckBox("JavaScript")
        self.js_checkbox.setChecked(self.settings.javascript_enabled)
        self.js_checkbox.setToolTip(
            "Enable JavaScript (may be needed for some sites, but increases risk)"
        )
        self.js_checkbox.toggled.connect(self._toggle_javascript)
        warning_layout.addWidget(self.js_checkbox)

        layout.addWidget(warning_widget)

        # Toolbar
        toolbar = QToolBar()
        toolbar.setMovable(False)

        self.back_btn = QPushButton("◀")
        self.back_btn.setToolTip("Go Back")
        self.back_btn.setFixedWidth(40)
        self.back_btn.clicked.connect(self._go_back)
        toolbar.addWidget(self.back_btn)

        self.forward_btn = QPushButton("▶")
        self.forward_btn.setToolTip("Go Forward")
        self.forward_btn.setFixedWidth(40)
        self.forward_btn.clicked.connect(self._go_forward)
        toolbar.addWidget(self.forward_btn)

        self.reload_btn = QPushButton("🔄")
        self.reload_btn.setToolTip("Reload")
        self.reload_btn.setFixedWidth(40)
        self.reload_btn.clicked.connect(self._reload)
        toolbar.addWidget(self.reload_btn)

        self.stop_btn = QPushButton("✕")
        self.stop_btn.setToolTip("Stop Loading")
        self.stop_btn.setFixedWidth(40)
        self.stop_btn.clicked.connect(self._stop)
        toolbar.addWidget(self.stop_btn)

        toolbar.addSeparator()

        self.url_bar = QLineEdit()
        self.url_bar.setReadOnly(True)
        self.url_bar.setText(self.url)
        toolbar.addWidget(self.url_bar)

        toolbar.addSeparator()

        self.screenshot_btn = QPushButton("📷 Screenshot")
        self.screenshot_btn.setToolTip("Save screenshot to file")
        self.screenshot_btn.clicked.connect(self._take_screenshot)
        toolbar.addWidget(self.screenshot_btn)

        self.external_btn = QPushButton("🚀 Open External")
        self.external_btn.setToolTip("Open in external browser with Firejail (Linux)")
        self.external_btn.clicked.connect(self._open_external)
        if not (has_firejail() or detect_browser()):
            self.external_btn.setEnabled(False)
            self.external_btn.setToolTip("No external browser available")
        toolbar.addWidget(self.external_btn)

        layout.addWidget(toolbar)

        # WebView placeholder (created in _setup_webview)
        self.webview_container = QVBoxLayout()
        layout.addLayout(self.webview_container, stretch=1)

        # Status bar
        self.status_label = QLabel("Ready")
        self.status_label.setStyleSheet(
            "padding: 5px; background-color: #f8f9fa; border-top: 1px solid #dee2e6;"
        )
        layout.addWidget(self.status_label)

    def _setup_webview(self) -> None:
        """Set up the QWebEngineView with security restrictions."""
        # Create isolated profile (anonymous, no persistence)
        self.profile = QWebEngineProfile(self)
        self.profile.setHttpCacheType(QWebEngineProfile.HttpCacheType.MemoryHttpCache)
        self.profile.setPersistentCookiesPolicy(
            QWebEngineProfile.PersistentCookiesPolicy.NoPersistentCookies
        )

        # Create page with isolated profile
        self.page = QWebEnginePage(self.profile, self)

        # Apply security settings
        settings = self.page.settings()
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.JavascriptEnabled,
            self.settings.javascript_enabled
        )
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.PluginsEnabled,
            False
        )
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.LocalStorageEnabled,
            False
        )
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.LocalContentCanAccessRemoteUrls,
            False
        )
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.AutoLoadImages,
            True  # Images are generally safe and useful
        )

        # Create webview
        self.webview = QWebEngineView()
        self.webview.setPage(self.page)

        # Connect signals
        self.webview.urlChanged.connect(self._on_url_changed)
        self.webview.loadStarted.connect(self._on_load_started)
        self.webview.loadProgress.connect(self._on_load_progress)
        self.webview.loadFinished.connect(self._on_load_finished)
        self.webview.titleChanged.connect(self._on_title_changed)

        self.webview_container.addWidget(self.webview)

    def _toggle_javascript(self, enabled: bool) -> None:
        """Toggle JavaScript on/off."""
        self.page.settings().setAttribute(
            QWebEngineSettings.WebAttribute.JavascriptEnabled,
            enabled
        )
        self.webview.reload()
        self.status_label.setText(
            f"JavaScript {'enabled' if enabled else 'disabled'} - reloading..."
        )

    def _go_back(self) -> None:
        self.webview.back()

    def _go_forward(self) -> None:
        self.webview.forward()

    def _reload(self) -> None:
        self.webview.reload()

    def _stop(self) -> None:
        self.webview.stop()

    def _on_url_changed(self, url: QUrl) -> None:
        self.url_bar.setText(url.toString())

    def _on_load_started(self) -> None:
        self.status_label.setText("Loading...")
        self.stop_btn.setEnabled(True)

    def _on_load_progress(self, progress: int) -> None:
        self.status_label.setText(f"Loading... {progress}%")

    def _on_load_finished(self, ok: bool) -> None:
        self.stop_btn.setEnabled(False)
        if ok:
            self.status_label.setText("Ready")
        else:
            self.status_label.setText("Failed to load page")

    def _on_title_changed(self, title: str) -> None:
        if title:
            self.setWindowTitle(f"🔒 Sandbox: {title[:50]}")

    def _take_screenshot(self) -> None:
        """Save screenshot of current page."""
        from PySide6.QtWidgets import QFileDialog

        # Grab the webview content
        pixmap = self.webview.grab()

        # If forensic context is available, use the screenshot dialog
        if self.forensic_context is not None:
            from .screenshot_dialog import ScreenshotCaptureDialog

            current_url = self.webview.url().toString()
            dialog = ScreenshotCaptureDialog(
                pixmap,
                current_url,
                self.forensic_context,
                parent=self,
            )
            if dialog.exec() == ScreenshotCaptureDialog.DialogCode.Accepted:
                self.status_label.setText("Screenshot saved to evidence folder")
                logger.info("Sandbox screenshot saved via dialog")
            return

        # Fallback: No forensic context - use legacy file dialog
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Screenshot",
            str(Path.home() / "sandbox_screenshot.png"),
            "PNG Images (*.png)",
        )
        if not path:
            return

        if pixmap.save(path):
            self.status_label.setText(f"Screenshot saved: {path}")
            logger.info("Sandbox screenshot saved: %s", path)
        else:
            self.status_label.setText("Failed to save screenshot")
            QMessageBox.warning(self, "Error", "Failed to save screenshot")

    def _open_external(self) -> None:
        """Open current URL in external sandboxed browser."""
        current_url = self.webview.url().toString()
        if open_url_external_sandboxed(current_url, self.settings):
            self.status_label.setText("Opened in external browser")
        else:
            QMessageBox.warning(
                self,
                "Error",
                "Failed to open external browser. Check if a browser is installed.",
            )

    def done(self, result: int) -> None:
        """
        Override done() to ensure proper cleanup order for WebEngine objects.

        Qt destroys children in reverse creation order, but WebEngine requires:
        webview -> page -> profile. We must clean up explicitly to avoid
        "Release of profile requested but WebEnginePage still not deleted" warning.
        """
        # Stop any pending loads
        if hasattr(self, 'webview'):
            self.webview.stop()
            # Disconnect signals to prevent callbacks during destruction
            try:
                self.webview.urlChanged.disconnect()
                self.webview.loadStarted.disconnect()
                self.webview.loadProgress.disconnect()
                self.webview.loadFinished.disconnect()
                self.webview.titleChanged.disconnect()
            except RuntimeError:
                pass  # Already disconnected

            # Clear the page from webview first
            self.webview.setPage(None)

        # Delete page before profile
        if hasattr(self, 'page'):
            self.page.deleteLater()
            self.page = None

        # Now safe to proceed with dialog closure (profile will be deleted last)
        super().done(result)


def open_url_sandboxed(
    url: str,
    parent: Optional[QWidget] = None,
    settings: Optional[SandboxSettings] = None,
    audit_callback: Optional[callable] = None,
) -> bool:
    """
    Open URL in sandbox - auto-selects best method.

    Decision logic:
    1. If settings.prefer_external and external browser available -> external
    2. If QtWebEngine available -> in-app dialog
    3. Otherwise -> external browser fallback

    Args:
        url: URL to open
        parent: Parent widget for dialog
        settings: Sandbox settings
        audit_callback: Callback for audit logging (url, method, browser_or_dialog)

    Returns:
        True if URL was opened, False on error.
    """
    if settings is None:
        settings = SandboxSettings()

    # Check if external is preferred and available
    use_external = settings.prefer_external and (has_firejail() or detect_browser())

    if use_external:
        return open_url_external_sandboxed(url, settings, audit_callback)

    # Try in-app QtWebEngine
    if HAS_WEBENGINE:
        try:
            dialog = SandboxBrowserDialog(url, parent, settings)
            if audit_callback and settings.log_opens:
                audit_callback(url, "embedded-webengine", "QtWebEngine")
            dialog.exec()
            return True
        except Exception as e:
            logger.error("Failed to open QtWebEngine dialog: %s", e)

    # Fallback to external
    if detect_browser():
        return open_url_external_sandboxed(url, settings, audit_callback)

    # No option available
    logger.error("No sandbox method available for URL: %s", url)
    if parent:
        QMessageBox.warning(
            parent,
            "Sandbox Unavailable",
            "No sandbox browser available.\n\n"
            "Options:\n"
            "• Install QtWebEngine (pip install PyQt6-WebEngine)\n"
            "• Install a browser (Firefox, Chrome, etc.)\n"
            "• On Linux: Install firejail for enhanced isolation",
        )
    return False


def get_sandbox_availability() -> dict:
    """
    Get information about available sandbox methods.

    Returns:
        Dictionary with availability info:
        {
            "webengine": bool,
            "firejail": bool,
            "firejail_compatible": bool,  # firejail + non-snap browser
            "external_browser": str or None,
            "firejail_browser": str or None,  # non-snap browser for firejail
            "recommended": str,
        }
    """
    browser = detect_browser(for_firejail=False)
    firejail_browser = detect_browser(for_firejail=True)
    firejail = has_firejail()
    firejail_compatible = firejail and firejail_browser is not None

    if HAS_WEBENGINE:
        recommended = "embedded"
    elif firejail_compatible:
        recommended = "firejail"
    elif browser:
        recommended = "external"
    else:
        recommended = "none"

    return {
        "webengine": HAS_WEBENGINE,
        "firejail": firejail,
        "firejail_compatible": firejail_compatible,
        "external_browser": browser,
        "firejail_browser": firejail_browser,
        "recommended": recommended,
    }
