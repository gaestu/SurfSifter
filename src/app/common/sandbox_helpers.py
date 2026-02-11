"""
Shared sandbox URL opening helpers for forensic URL viewing.

Provides reusable functions for opening URLs in sandboxed environments
from any tab that displays URLs from evidence.

Initial implementation - shared helpers extracted from URLs tab.
Added ForensicContext for screenshot integration.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from PySide6.QtWidgets import QMenu, QMessageBox, QWidget

logger = logging.getLogger(__name__)


@dataclass
class ForensicContext:
    """
    Context for forensic operations in sandbox browser.

    Provides the necessary information to save screenshots and other
    investigator-captured data within the correct evidence folder.

    Attributes:
        evidence_id: Database evidence ID
        evidence_label: Evidence label (used for folder naming)
        workspace_path: Path to case workspace folder
        db_conn: SQLite connection to evidence database
    """

    evidence_id: int
    evidence_label: str
    workspace_path: Path
    db_conn: sqlite3.Connection


def audit_sandbox_open(
    url: str,
    method: str,
    browser: str,
    evidence_id: Optional[int] = None,
) -> None:
    """
    Log sandbox URL open for audit trail.

    Args:
        url: URL being opened
        method: Method used (embedded-webengine, firejail)
        browser: Browser name
        evidence_id: Optional evidence ID for context
    """
    logger.info(
        "Sandbox URL open: url=%s, method=%s, browser=%s, evidence_id=%s",
        url[:100] if url else "",
        method,
        browser,
        evidence_id,
    )


def open_in_embedded_sandbox(
    url: str,
    parent: Optional[QWidget] = None,
    evidence_id: Optional[int] = None,
    forensic_context: Optional[ForensicContext] = None,
) -> bool:
    """
    Open URL in embedded QtWebEngine sandbox viewer.

    Args:
        url: URL to open
        parent: Parent widget for dialog
        evidence_id: Optional evidence ID for audit logging
        forensic_context: Optional forensic context for screenshot saving

    Returns:
        True if opened successfully, False otherwise
    """
    if not url:
        return False

    from app.common.dialogs import SandboxBrowserDialog, get_sandbox_availability

    availability = get_sandbox_availability()
    if not availability["webengine"]:
        QMessageBox.warning(
            parent,
            "Embedded Viewer Unavailable",
            "QtWebEngine is not available.\n\n"
            "It should be included with PySide6. Try reinstalling PySide6.",
        )
        return False

    try:
        audit_sandbox_open(url, "embedded-webengine", "QtWebEngine", evidence_id)
        dialog = SandboxBrowserDialog(url, parent=parent, forensic_context=forensic_context)
        dialog.exec()
        return True
    except Exception as e:
        logger.error("Failed to open embedded sandbox: %s", e)
        QMessageBox.critical(
            parent,
            "Error",
            f"Failed to open sandbox viewer:\n{e}",
        )
        return False


def open_with_firejail(
    url: str,
    parent: Optional[QWidget] = None,
    evidence_id: Optional[int] = None,
) -> bool:
    """
    Open URL in external browser with Firejail isolation (Linux only).

    Args:
        url: URL to open
        parent: Parent widget for dialogs
        evidence_id: Optional evidence ID for audit logging

    Returns:
        True if launched successfully, False otherwise
    """
    if not url:
        return False

    from app.common.dialogs import (
        open_url_external_sandboxed,
        has_firejail,
        detect_browser,
    )

    if not has_firejail():
        QMessageBox.warning(
            parent,
            "Firejail Unavailable",
            "Firejail is not installed or not available.\n\n"
            "On Ubuntu/Debian: sudo apt install firejail\n"
            "On Fedora: sudo dnf install firejail\n"
            "On Arch: sudo pacman -S firejail",
        )
        return False

    # Check for firejail-compatible browser (not snap)
    browser = detect_browser(for_firejail=True)
    if not browser:
        # Check if there's a snap browser
        any_browser = detect_browser(for_firejail=False)
        if any_browser:
            QMessageBox.warning(
                parent,
                "No Compatible Browser",
                "Your browser appears to be a snap package, which doesn't "
                "work with Firejail.\n\n"
                "Options:\n"
                "• Install Chromium or Chrome as a .deb package\n"
                "• Use the embedded Sandbox Viewer instead\n\n"
                "To install Chromium (non-snap):\n"
                "  sudo apt install chromium-browser\n\n"
                "Or install Google Chrome from:\n"
                "  https://www.google.com/chrome/",
            )
        else:
            QMessageBox.warning(
                parent,
                "No Browser Found",
                "No supported browser found for Firejail.\n\n"
                "Install Firefox, Chrome, Chromium, or Brave.",
            )
        return False

    def _audit_callback(url: str, method: str, browser: str) -> None:
        audit_sandbox_open(url, method, browser, evidence_id)

    success = open_url_external_sandboxed(url, audit_callback=_audit_callback)

    if not success:
        QMessageBox.warning(
            parent,
            "Launch Failed",
            "Failed to launch browser with Firejail.\n\n"
            "Check the logs for details.",
        )

    return success


def add_sandbox_url_actions(
    menu: QMenu,
    url: str,
    parent: QWidget,
    evidence_id: Optional[int] = None,
    *,
    evidence_label: Optional[str] = None,
    workspace_path: Optional[Path] = None,
    db_conn = None,
    case_data = None,  # CaseDataAccess - can extract db_conn from this
    add_separator_before: bool = False,
    add_separator_after: bool = False,
) -> None:
    """
    Add sandbox URL opening actions to a context menu.

    Adds two actions:
    - "🔒 Open in Sandbox Viewer" (QtWebEngine embedded browser)
    - "🛡️ Open with Firejail" (Linux only, external browser)

    Actions are automatically enabled/disabled based on availability.

    Args:
        menu: QMenu to add actions to
        url: URL to open when actions are triggered
        parent: Parent widget for dialogs
        evidence_id: Optional evidence ID for audit logging
        evidence_label: Optional evidence label for forensic context
        workspace_path: Optional workspace path for forensic context
        db_conn: Optional database connection for forensic context
        case_data: Optional CaseDataAccess - can extract db_conn from this
        add_separator_before: Add separator before sandbox actions
        add_separator_after: Add separator after sandbox actions

    Example:
        menu = QMenu(self)
        add_sandbox_url_actions(menu, url, self, self.evidence_id,
                                evidence_label=self.evidence_label,
                                workspace_path=self.case_folder,
                                case_data=self.case_data)
        menu.addSeparator()
        # ... add more actions
    """
    from PySide6.QtGui import QAction
    from app.common.dialogs import get_sandbox_availability, has_firejail

    if add_separator_before:
        menu.addSeparator()

    availability = get_sandbox_availability()

    # Build ForensicContext if all required info is provided
    forensic_context = None

    # Try to get db_conn from case_data if not provided directly
    conn = db_conn
    if conn is None and case_data is not None and evidence_id is not None and evidence_label:
        try:
            if hasattr(case_data, 'db_manager') and case_data.db_manager:
                conn = case_data.db_manager.get_evidence_conn(evidence_id, evidence_label)
        except Exception:
            pass  # Graceful fallback - no forensic context

    if evidence_id is not None and evidence_label and workspace_path and conn:
        forensic_context = ForensicContext(
            evidence_id=evidence_id,
            evidence_label=evidence_label,
            workspace_path=workspace_path,
            db_conn=conn,
        )

    # Embedded Sandbox Viewer (QtWebEngine)
    embedded_action = QAction("🔒 Open in Sandbox Viewer", parent)
    embedded_action.setToolTip("Open URL in embedded QtWebEngine sandbox browser")
    embedded_action.triggered.connect(
        lambda _checked=False, ctx=forensic_context: open_in_embedded_sandbox(
            url, parent, evidence_id, ctx
        )
    )
    embedded_action.setEnabled(bool(url) and availability["webengine"])
    menu.addAction(embedded_action)

    # Firejail (Linux only)
    firejail_action = QAction("🛡️ Open with Firejail", parent)
    firejail_action.setToolTip("Open URL in external browser with Firejail isolation (Linux)")
    firejail_action.triggered.connect(
        lambda _checked=False: open_with_firejail(url, parent, evidence_id)
    )
    firejail_available = availability.get("firejail_compatible", False)
    firejail_action.setEnabled(bool(url) and firejail_available)
    if not firejail_available:
        if has_firejail():
            firejail_action.setToolTip("No firejail-compatible browser (snap browsers don't work)")
        else:
            firejail_action.setToolTip("Firejail not available (Linux only)")
    menu.addAction(firejail_action)

    if add_separator_after:
        menu.addSeparator()
