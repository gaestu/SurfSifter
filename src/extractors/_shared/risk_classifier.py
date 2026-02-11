"""
Permission Risk Classifier for Browser Extensions

Classifies browser extension permissions into risk levels:
- critical: Full browser control, native messaging
- high: Tabs, history, cookies, downloads
- medium: Storage, notifications, active tab
- low: Basic permissions

Based on Mozilla's permission model and Chrome's permission warnings.
"""

from __future__ import annotations

from typing import List, Set, Optional


# Permission risk levels based on potential for abuse
PERMISSION_RISK_LEVELS = {
    "critical": [
        "<all_urls>",           # Access to ALL websites
        "webRequest",           # Intercept all network requests
        "webRequestBlocking",   # Block/modify network requests
        "nativeMessaging",      # Communicate with native apps
        "debugger",             # Debug other extensions/tabs
        "proxy",                # Control proxy settings
        "management",           # Control other extensions
        "privacy",              # Change privacy settings
        "browsingData",         # Delete browsing data
    ],
    "high": [
        "tabs",                 # Read tab URLs, titles
        "history",              # Access browsing history
        "cookies",              # Read/write cookies
        "downloads",            # Access downloads
        "bookmarks",            # Access bookmarks
        "topSites",             # Access most visited sites
        "sessions",             # Access session/tab history
        "webNavigation",        # Monitor navigation
        "clipboardRead",        # Read clipboard
        "clipboardWrite",       # Write to clipboard
        "geolocation",          # Access location
        "identity",             # Access user identity
        "declarativeNetRequest",# Block network requests
        "declarativeNetRequestWithHostAccess",
        "scripting",            # Inject scripts
    ],
    "medium": [
        "storage",              # Store extension data
        "unlimitedStorage",     # Large storage
        "notifications",        # Show notifications
        "alarms",               # Set alarms/timers
        "activeTab",            # Access current tab only
        "contextMenus",         # Add context menu items
        "background",           # Run in background
        "idle",                 # Detect user idle
        "power",                # Control power management
        "system.cpu",           # CPU info
        "system.memory",        # Memory info
        "system.storage",       # Storage info
        "tabCapture",           # Capture tab content
        "desktopCapture",       # Capture desktop
        "pageCapture",          # Save page as MHTML
        "fontSettings",         # Change font settings
    ],
    # Everything else is 'low'
}

# Host permissions that indicate high risk
HIGH_RISK_HOST_PATTERNS = [
    "<all_urls>",
    "*://*/*",
    "*://*.google.com/*",
    "*://*.facebook.com/*",
    "*://*.paypal.com/*",
    "*://*.amazon.com/*",
    "http://*/*",
    "https://*/*",
]


def calculate_risk_level(
    permissions: List[str],
    host_permissions: Optional[List[str]] = None
) -> str:
    """
    Calculate overall risk level from extension permissions.

    Args:
        permissions: List of API permissions
        host_permissions: List of host/URL patterns

    Returns:
        Risk level: 'critical', 'high', 'medium', or 'low'
    """
    if host_permissions is None:
        host_permissions = []

    all_perms = set(permissions + host_permissions)

    # Check for critical permissions
    for perm in all_perms:
        if perm in PERMISSION_RISK_LEVELS["critical"]:
            return "critical"
        # <all_urls> is critical
        if perm == "<all_urls>":
            return "critical"

    # Check for high-risk host patterns
    for host in host_permissions:
        if host in HIGH_RISK_HOST_PATTERNS:
            return "high"
        # Wildcards covering all sites
        if host.startswith("*://") and host.endswith("/*"):
            return "high"

    # Check for high permissions
    for perm in all_perms:
        if perm in PERMISSION_RISK_LEVELS["high"]:
            return "high"

    # Check for medium permissions
    for perm in all_perms:
        if perm in PERMISSION_RISK_LEVELS["medium"]:
            return "medium"

    return "low"


def get_permission_description(permission: str) -> str:
    """
    Get human-readable description of a permission.

    Args:
        permission: Permission string

    Returns:
        Human-readable description
    """
    descriptions = {
        "<all_urls>": "Access all websites",
        "webRequest": "Intercept network requests",
        "webRequestBlocking": "Block and modify network requests",
        "nativeMessaging": "Communicate with native applications",
        "debugger": "Debug other browser extensions",
        "proxy": "Control proxy settings",
        "management": "Manage other extensions",
        "privacy": "Change privacy settings",
        "browsingData": "Delete browsing data",
        "tabs": "Read tab URLs and titles",
        "history": "Access browsing history",
        "cookies": "Read and modify cookies",
        "downloads": "Access download files",
        "bookmarks": "Read and modify bookmarks",
        "topSites": "Access frequently visited sites",
        "sessions": "Access recently closed tabs",
        "webNavigation": "Monitor browser navigation",
        "clipboardRead": "Read clipboard contents",
        "clipboardWrite": "Modify clipboard contents",
        "geolocation": "Access your location",
        "identity": "Access user identity",
        "storage": "Store data locally",
        "unlimitedStorage": "Store unlimited data",
        "notifications": "Show notifications",
        "activeTab": "Access current tab",
        "contextMenus": "Add right-click menu items",
        "background": "Run in the background",
        "tabCapture": "Capture tab content",
        "desktopCapture": "Capture screen content",
    }

    return descriptions.get(permission, permission)


def get_risk_color(risk_level: str) -> str:
    """
    Get color code for risk level (for UI).

    Args:
        risk_level: 'critical', 'high', 'medium', or 'low'

    Returns:
        Color hex code
    """
    colors = {
        "critical": "#FF0000",  # Red
        "high": "#FF6600",      # Orange
        "medium": "#FFCC00",    # Yellow
        "low": "#00CC00",       # Green
    }
    return colors.get(risk_level, "#808080")  # Gray for unknown


def get_risk_emoji(risk_level: str) -> str:
    """
    Get emoji indicator for risk level (for UI).

    Args:
        risk_level: 'critical', 'high', 'medium', or 'low'

    Returns:
        Emoji string
    """
    emojis = {
        "critical": "🔴",
        "high": "🟠",
        "medium": "🟡",
        "low": "🟢",
    }
    return emojis.get(risk_level, "⚪")
