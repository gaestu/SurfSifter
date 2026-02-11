"""
Firefox Permissions schema definitions and type mappings.

This module defines known tables, columns, and permission type mappings
for Firefox permissions.sqlite and content-prefs.sqlite databases.
Used by extractors to detect unknown schema elements via extraction warnings.

Schema Sources:
- permissions.sqlite: moz_perms table (Firefox 11+ uses 'origin', legacy uses 'host')
- content-prefs.sqlite: groups, settings, prefs tables

References:
- Firefox source: toolkit/components/permissions/
- MDN: https://developer.mozilla.org/en-US/docs/Web/API/Permissions_API
"""

from __future__ import annotations

from typing import Dict, List, Set


# =============================================================================
# permissions.sqlite Schema
# =============================================================================

# Known tables in permissions.sqlite
KNOWN_PERMISSIONS_TABLES: Set[str] = {
    "moz_perms",           # Main permissions table
    "moz_hosts",           # Legacy (Firefox < 11)
    "sqlite_sequence",     # SQLite internal
}

# Patterns to filter unknown tables (for schema discovery)
PERMISSIONS_TABLE_PATTERNS: List[str] = [
    "perm",
    "host",
    "origin",
]

# Known columns in moz_perms table
# Modern schema (Firefox 11+)
KNOWN_MOZ_PERMS_COLUMNS: Set[str] = {
    "id",
    "origin",              # Modern: full origin URL
    "host",                # Legacy: hostname only
    "type",                # Permission type string
    "permission",          # Permission value (1=allow, 2=block, etc.)
    "expireType",          # 0=permanent, 1=expiring, 2=session
    "expireTime",          # PRTime (microseconds since Unix epoch)
    "modificationTime",    # PRTime when permission was set
}


# =============================================================================
# content-prefs.sqlite Schema
# =============================================================================

# Known tables in content-prefs.sqlite
KNOWN_CONTENT_PREFS_TABLES: Set[str] = {
    "groups",              # Origin/host groups
    "settings",            # Setting name definitions
    "prefs",               # Actual preference values
    "sqlite_sequence",     # SQLite internal
}

# Patterns to filter unknown tables
CONTENT_PREFS_TABLE_PATTERNS: List[str] = [
    "pref",
    "group",
    "setting",
]

# Known columns in content-prefs tables
KNOWN_GROUPS_COLUMNS: Set[str] = {"id", "name"}
KNOWN_SETTINGS_COLUMNS: Set[str] = {"id", "name"}
KNOWN_PREFS_COLUMNS: Set[str] = {"id", "groupID", "settingID", "value", "timestamp"}


# =============================================================================
# Firefox Permission Value Mappings
# =============================================================================

# Permission values from Firefox source
# Source: nsIPermissionManager.idl
FIREFOX_PERMISSION_VALUES: Dict[int, str] = {
    0: "unknown",          # UNKNOWN_ACTION
    1: "allow",            # ALLOW_ACTION
    2: "block",            # DENY_ACTION
    3: "prompt",           # PROMPT_ACTION (deprecated in most permission types)
    8: "prompt",           # Alternative prompt value (some permission types)
    9: "allow_session",    # SESSION_ALLOW (cookies)
}


# =============================================================================
# Firefox Permission Type Mappings
# =============================================================================

# Maps Firefox permission type strings to normalized names
# Source: Various Firefox components that register permission types
FIREFOX_PERMISSION_TYPE_MAP: Dict[str, str] = {
    # Geolocation
    "geo": "geolocation",

    # Notifications
    "desktop-notification": "notifications",

    # Media
    "camera": "camera",
    "microphone": "microphone",
    "speaker-selection": "speaker_selection",
    "midi": "midi",
    "midi-sysex": "midi_sysex",

    # Storage
    "persistent-storage": "persistent_storage",
    "indexedDB": "indexeddb",
    "storage-access": "storage_access",

    # Cookies and tracking
    "cookie": "cookies",
    "3rdPartyStorage": "third_party_storage",
    "3rdPartyFrameStorage": "third_party_frame_storage",

    # UI
    "popup": "popup",
    "focus-tab-by-prompt": "focus_tab",
    "fullscreen": "fullscreen",
    "pointerLock": "pointer_lock",

    # Installation
    "install": "install",
    "xr": "webxr",

    # Autoplay
    "autoplay-media": "autoplay",
    "autoplay-media-audible": "autoplay_audible",
    "autoplay-media-inaudible": "autoplay_inaudible",

    # Security
    "https-only-load-insecure": "https_only_exception",
    "https-only-load-insecure-pbm": "https_only_exception_private",

    # Canvas
    "canvas": "canvas_extraction",

    # Screen capture
    "screen": "screen_capture",

    # Clipboard
    "clipboard-read": "clipboard_read",
    "clipboard-write": "clipboard_write",

    # Web Authentication
    "publickey-credentials-get": "webauthn",

    # File system
    "file-handle-write": "file_write",
}


# =============================================================================
# Content-Prefs Mappings
# =============================================================================

# Maps content-prefs setting names to permission-like types
CONTENT_PREF_TYPE_MAP: Dict[str, str] = {
    # Zoom
    "browser.content.full-zoom": "zoom",
    "browser.zoom.siteSpecific": "zoom",

    # Autoplay
    "media.autoplay.default": "autoplay",
    "media.autoplay.allow-muted": "autoplay_muted",
    "media.autoplay.ask-permission": "autoplay_ask",

    # Default permissions
    "permissions.default.camera": "camera_default",
    "permissions.default.microphone": "microphone_default",
    "permissions.default.geo": "geolocation_default",
    "permissions.default.desktop-notification": "notifications_default",

    # Downloads
    "browser.download.lastDir": "downloads_last_dir",
    "browser.download.folderList": "downloads_folder_list",
    "browser.download.useDownloadDir": "downloads_use_dir",

    # Privacy
    "network.http.referer.spoofSource": "referer_spoof",
    "privacy.trackingprotection.enabled": "tracking_protection",
    "privacy.donottrackheader.enabled": "do_not_track",

    # Security
    "security.mixed_content.block_active_content": "mixed_content_block",
    "security.OCSP.enabled": "ocsp_enabled",
}


# =============================================================================
# Expire Type Mappings
# =============================================================================

EXPIRE_TYPE_MAP: Dict[int, str] = {
    0: "permanent",        # EXPIRE_NEVER
    1: "expiring",         # EXPIRE_TIME - expires at expireTime
    2: "session",          # EXPIRE_SESSION - expires when browser closes
    3: "policy",           # EXPIRE_POLICY - set by enterprise policy
}


# =============================================================================
# Helper Functions
# =============================================================================

def get_permission_value_name(value: int) -> str:
    """Get human-readable name for permission value."""
    return FIREFOX_PERMISSION_VALUES.get(value, f"unknown_{value}")


def normalize_permission_type(raw_type: str) -> str:
    """Normalize Firefox permission type to standard name."""
    return FIREFOX_PERMISSION_TYPE_MAP.get(raw_type, raw_type)


def get_expire_type_name(expire_type: int) -> str:
    """Get human-readable name for expire type."""
    return EXPIRE_TYPE_MAP.get(expire_type, "permanent")


def normalize_content_pref_type(setting_name: str) -> str:
    """Map content-prefs setting name to permission-like type."""
    return CONTENT_PREF_TYPE_MAP.get(setting_name, setting_name)
