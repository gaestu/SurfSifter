"""
Chromium Extensions schema definitions and constants.

This module defines known tables, fields, and enums for Chromium extension
data formats. Used by the extractor for schema warning discovery.

Schema Sources:
- chromium/src/extensions/common/constants.h (location codes)
- chromium/src/extensions/browser/disable_reason.h (disable reasons)
- chromium/src/chrome/common/extensions/api/_manifest_features.json (manifest keys)

Extracted from extractor.py, added schema warning support
"""
from __future__ import annotations

from typing import Dict, List, Set


# =============================================================================
# Extension Path Patterns
# =============================================================================

# Pattern suffix to find extension manifests within Extensions directory
EXTENSION_MANIFEST_PATTERN = "/*/*/manifest.json"

# Path separator (use forward slash for evidence filesystem paths)
PATH_SEPARATOR = "/"


# =============================================================================
# Chromium Extension Installation Location Codes
# =============================================================================
# From chromium/src/extensions/common/constants.h

INSTALL_LOCATION_MAP: Dict[int, str] = {
    1: "internal",              # INTERNAL - Chrome built-in
    2: "external_pref",         # EXTERNAL_PREF - External preferences file
    3: "external_registry",     # EXTERNAL_REGISTRY - Windows registry
    4: "unpacked",              # UNPACKED - Developer mode (sideloaded)
    5: "component",             # COMPONENT - Chrome component
    6: "external_pref_download",    # External download pref
    7: "external_policy_download",  # Enterprise policy download
    8: "command_line",          # Command line argument
    9: "external_policy",       # Enterprise policy
    10: "external_component",   # External component
}

# Known install location values (for schema warning detection)
KNOWN_INSTALL_LOCATIONS: Set[int] = set(INSTALL_LOCATION_MAP.keys())


# =============================================================================
# Chromium Extension Disable Reason Bitmask
# =============================================================================
# From chromium/src/extensions/browser/disable_reason.h

DISABLE_REASONS: Dict[int, str] = {
    1: "user_action",               # User disabled in chrome://extensions
    2: "permissions_increase",      # Permissions increased after install
    4: "reload",                    # Extension is reloading
    8: "unsupported_requirement",   # Missing requirement
    16: "sideload_wipeout",         # Sideload wipeout policy
    32: "unknown_from_sync",        # Unknown extension from sync
    64: "not_verified",             # Extension not verified
    128: "greylist",                # On greylist
    256: "corrupted",               # Extension files corrupted
    512: "remote_install",          # Remote install blocked
    1024: "external_extension",     # External extension disabled
    2048: "update_required",        # Pending update required
    4096: "blocked_by_policy",      # Blocked by enterprise policy
    8192: "custodian_approval_required",  # Supervised user needs approval
    16384: "reinstall",             # Needs reinstall
}

# Known disable reason bits (for schema warning detection)
KNOWN_DISABLE_REASON_BITS: Set[int] = set(DISABLE_REASONS.keys())


# =============================================================================
# Extension State Values
# =============================================================================

EXTENSION_STATE_MAP: Dict[int, str] = {
    0: "disabled",
    1: "enabled",
    2: "external_extension_uninstalled",  # Rare state
}

KNOWN_EXTENSION_STATES: Set[int] = set(EXTENSION_STATE_MAP.keys())


# =============================================================================
# Known Manifest Keys (for schema warning detection)
# =============================================================================
# Based on Chrome extension manifest.json specification
# https://developer.chrome.com/docs/extensions/mv3/manifest/

KNOWN_MANIFEST_KEYS: Set[str] = {
    # Required keys
    "manifest_version",
    "name",
    "version",

    # Recommended keys
    "description",
    "icons",
    "default_locale",

    # Optional keys - Identity
    "author",
    "homepage_url",
    "short_name",
    "version_name",

    # Optional keys - Permissions
    "permissions",
    "optional_permissions",
    "host_permissions",
    "optional_host_permissions",

    # Optional keys - Background
    "background",  # V2: scripts[], V3: service_worker

    # Optional keys - Content
    "content_scripts",
    "content_security_policy",  # V2: string, V3: object
    "web_accessible_resources",

    # Optional keys - Browser Actions
    "action",           # V3 unified action
    "browser_action",   # V2 browser action
    "page_action",      # V2 page action

    # Optional keys - Features
    "commands",
    "devtools_page",
    "externally_connectable",
    "file_browser_handlers",
    "file_system_provider_capabilities",
    "incognito",
    "input_components",
    "key",
    "minimum_chrome_version",
    "nacl_modules",
    "natively_connectable",
    "oauth2",
    "offline_enabled",
    "omnibox",
    "options_page",
    "options_ui",
    "requirements",
    "sandbox",
    "side_panel",
    "storage",
    "tts_engine",
    "update_url",

    # Chrome-specific keys
    "chrome_settings_overrides",
    "chrome_url_overrides",

    # Edge-specific keys
    "edge_side_panel",

    # Deprecated but still seen
    "converted_from_user_script",
    "current_locale",
    "differential_fingerprint",
    "import",
    "app",
    "platforms",
    "plugins",

    # Internal Chrome keys (seen in extracted manifests)
    "__metadata",
}

# Top-level manifest keys we explicitly parse
PARSED_MANIFEST_KEYS: Set[str] = {
    "manifest_version",
    "name",
    "version",
    "description",
    "author",
    "homepage_url",
    "permissions",
    "optional_permissions",
    "host_permissions",
    "content_scripts",
    "background",
    "web_accessible_resources",
    "content_security_policy",
    "update_url",
}


# =============================================================================
# Known Preferences Fields (extensions.settings.{id})
# =============================================================================
# Fields found in Chromium Preferences JSON under extensions.settings

KNOWN_PREFERENCES_FIELDS: Set[str] = {
    # Core state
    "state",
    "disable_reasons",
    "location",
    "from_webstore",

    # Timestamps
    "install_time",
    "last_update_check",
    "first_install_time",

    # Permissions
    "granted_permissions",
    "active_permissions",
    "runtime_granted_permissions",
    "withheld_permissions",

    # Installation
    "path",
    "manifest",
    "creation_flags",
    "install_signature",
    "was_installed_by_default",
    "was_installed_by_oem",

    # Sync
    "ack_external",
    "ack_prompt_count",
    "acknowledged",
    "allowlist",
    "blocklist_state",
    "blocklist_text",

    # Enterprise
    "enterprise_policy_installed",
    "installed_by_custodian",

    # Features
    "content_settings",
    "events",
    "incognito_content_settings",
    "incognito_preferences",
    "preferences",
    "regular_only_preferences",

    # Other
    "blacklist",
    "lastpingday",
    "needs_sync",
    "newAllowFileAccess",
}

# Fields we actually parse from Preferences
PARSED_PREFERENCES_FIELDS: Set[str] = {
    "state",
    "disable_reasons",
    "location",
    "from_webstore",
    "install_time",
    "granted_permissions",
    "active_permissions",
}


# =============================================================================
# High-Risk Permissions (for enhanced risk classification)
# =============================================================================

# Permissions that can intercept/modify web traffic
WEB_REQUEST_PERMISSIONS: Set[str] = {
    "webRequest",
    "webRequestBlocking",
    "webRequestAuthProvider",
    "declarativeWebRequest",
    "declarativeNetRequest",
    "declarativeNetRequestWithHostAccess",
    "declarativeNetRequestFeedback",
}

# Permissions that access sensitive data
SENSITIVE_DATA_PERMISSIONS: Set[str] = {
    "cookies",
    "history",
    "bookmarks",
    "tabs",
    "topSites",
    "sessions",
    "browsingData",
    "downloads",
    "downloads.open",
    "clipboardRead",
    "clipboardWrite",
}

# Permissions that indicate system access
SYSTEM_PERMISSIONS: Set[str] = {
    "nativeMessaging",
    "management",
    "debugger",
    "processes",
    "system.cpu",
    "system.memory",
    "system.storage",
    "system.display",
    "fileSystem",
    "fileSystemProvider",
}


# =============================================================================
# Helper Functions
# =============================================================================

def decode_disable_reasons(bitmask: int) -> str:
    """
    Decode disable_reasons bitmask to human-readable string.

    Args:
        bitmask: Integer bitmask of disable reasons

    Returns:
        Comma-separated string of reason names, or empty string if no reasons
    """
    if not bitmask:
        return ""

    reasons = []
    unknown_bits = bitmask

    for bit, name in DISABLE_REASONS.items():
        if bitmask & bit:
            reasons.append(name)
            unknown_bits &= ~bit

    # If there are unknown bits, note them
    if unknown_bits:
        reasons.append(f"unknown_bits({unknown_bits})")

    return ", ".join(reasons) if reasons else f"unknown ({bitmask})"


def get_install_location_text(location: int) -> str:
    """
    Get human-readable text for install location code.

    Args:
        location: Install location integer code

    Returns:
        Human-readable location string
    """
    return INSTALL_LOCATION_MAP.get(location, f"unknown ({location})")


def get_unknown_disable_bits(bitmask: int) -> Set[int]:
    """
    Find unknown bits in a disable_reasons bitmask.

    Args:
        bitmask: Integer bitmask of disable reasons

    Returns:
        Set of unknown bit positions
    """
    if not bitmask:
        return set()

    unknown = set()
    # Check bits up to 32 (covers known range with room for new additions)
    for bit_pos in range(32):
        bit = 1 << bit_pos
        if (bitmask & bit) and bit not in KNOWN_DISABLE_REASON_BITS:
            unknown.add(bit)

    return unknown
