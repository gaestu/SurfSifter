"""
Firefox Extensions schema definitions and constants.

This module defines known tables, fields, and enums for Firefox extension
data formats. Used by the extractor for schema warning discovery.

Schema Sources:
- Firefox source: toolkit/mozapps/extensions/internal/XPIDatabase.jsm
- Firefox source: toolkit/mozapps/extensions/AddonManager.jsm
- MDN: https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions

Initial extraction from extractor.py
"""
from __future__ import annotations

from typing import Dict, Set


# =============================================================================
# Extension File Patterns
# =============================================================================

# Files to discover for extension metadata
EXTENSION_FILES = {"extensions.json", "addons.json"}

# XPI extension archive pattern (relative to profile/extensions/)
XPI_PATTERN = "*/extensions/*.xpi"
XPI_STAGED_PATTERN = "*/extensions/staged/*.xpi"


# =============================================================================
# Addon Types to Skip
# =============================================================================
# These are system/builtin addons that don't provide forensic value

SKIP_ADDON_TYPES: Set[str] = {
    "locale",              # Language packs
    "dictionary",          # Spell check dictionaries
    "extension-builtin",   # Firefox built-in extensions (system principal)
    "theme-builtin",       # Built-in themes
}

# Addon types of forensic interest
FORENSIC_ADDON_TYPES: Set[str] = {
    "extension",           # User-installed extensions
    "theme",               # User-installed themes
    "webextension",        # WebExtension format (modern)
    "legacy-extension",    # Legacy XUL extensions (older Firefox)
}


# =============================================================================
# Firefox Addon Signed States
# =============================================================================
# From toolkit/mozapps/extensions/internal/XPIDatabase.jsm
# AddonManager.signedState values

SIGNED_STATE_MAP: Dict[int, str] = {
    -2: "broken",              # SIGNEDSTATE_BROKEN - signature exists but invalid
    -1: "unknown",             # SIGNEDSTATE_UNKNOWN - not checked yet
    0: "missing",              # SIGNEDSTATE_MISSING - no signature
    1: "preliminary",          # SIGNEDSTATE_PRELIMINARY - preliminary review
    2: "signed",               # SIGNEDSTATE_SIGNED - fully signed (AMO or Mozilla)
    3: "system",               # SIGNEDSTATE_SYSTEM - system addon
    4: "privileged",           # SIGNEDSTATE_PRIVILEGED - privileged addon
}

KNOWN_SIGNED_STATES: Set[int] = set(SIGNED_STATE_MAP.keys())

# Signed states indicating security concerns (for risk assessment)
UNSIGNED_OR_BROKEN_STATES: Set[int] = {-2, -1, 0}


# =============================================================================
# Known extensions.json Keys (Top Level)
# =============================================================================
# Based on Firefox 115+ extensions.json structure

KNOWN_EXTENSIONS_JSON_ROOT_KEYS: Set[str] = {
    "schemaVersion",
    "addons",
}


# =============================================================================
# Known Addon Entry Keys (extensions.json addons[])
# =============================================================================
# Keys we expect in each addon entry within extensions.json

KNOWN_ADDON_KEYS: Set[str] = {
    # Identification
    "id",
    "syncGUID",
    "version",
    "type",
    "loader",

    # Display/metadata
    "name",
    "description",
    "creator",
    "developers",
    "translators",
    "contributors",
    "homepageURL",
    "supportURL",
    "updateURL",
    "optionsURL",
    "optionsType",
    "optionsBrowserStyle",
    "aboutURL",
    "icons",
    "iconURL",
    "icon64URL",

    # Localization
    "defaultLocale",
    "locales",

    # State
    "active",
    "visible",
    "userDisabled",
    "appDisabled",
    "softDisabled",
    "embedderDisabled",
    "blocklistState",
    "signedState",
    "signedDate",
    "seen",
    "pendingUninstall",

    # Timestamps
    "installDate",
    "updateDate",
    "applyBackgroundUpdates",

    # Source/location
    "location",
    "path",
    "rootURI",
    "sourceURI",

    # Permissions
    "permissions",
    "userPermissions",
    "optionalPermissions",
    "grantedPermissions",
    "sitePermissions",
    "siteOrigin",

    # Content scripts and background
    "startupData",
    "manifest",
    "manifestVersion",

    # Telemetry and sync
    "telemetryKey",
    "recommendationState",
    "syncOperations",

    # Compatibility
    "targetApplications",
    "targetPlatforms",
    "strictCompatibility",
    "multiprocessCompatible",
    "runInSafeMode",

    # Install info
    "installTelemetryInfo",
    "installOrigins",
    "foreignInstall",
    "hasBinaryComponents",
    "incognito",
    "hiddenInstall",

    # Internal Firefox fields
    "_repositoryAddon",
    "_installLocation",
    "dependencies",
    "hasEmbeddedWebExtension",
    "mpiEnabled",
    "isBuiltin",
    "isSystem",
    "isWebExtension",
}


# =============================================================================
# Known defaultLocale Keys
# =============================================================================

KNOWN_DEFAULT_LOCALE_KEYS: Set[str] = {
    "name",
    "description",
    "creator",
    "developers",
    "translators",
    "contributors",
    "homepageURL",
}


# =============================================================================
# Known addons.json Keys (AMO metadata)
# =============================================================================
# Additional fields from addons.json (AMO repository metadata)

KNOWN_ADDONS_JSON_ADDON_KEYS: Set[str] = {
    "id",
    "name",
    "version",
    "type",
    "creator",
    "developers",
    "description",
    "fullDescription",
    "icons",
    "screenshots",
    "sourceURI",
    "homepageURL",
    "supportURL",
    "reviewURL",
    "reviewCount",
    "totalDownloads",
    "weeklyDownloads",
    "dailyUsers",
    "averageDailyUsers",
    "repositoryStatus",
    "amoListingURL",
    "contributionURL",
}


# =============================================================================
# Profile Name Safe Character Limit
# =============================================================================

MAX_SAFE_PROFILE_LENGTH = 32


# =============================================================================
# Helper Functions
# =============================================================================

def get_signed_state_name(signed_state: int) -> str:
    """
    Convert signed state code to human-readable name.

    Args:
        signed_state: Firefox signedState integer code

    Returns:
        Human-readable state name or "unknown_{code}"
    """
    return SIGNED_STATE_MAP.get(signed_state, f"unknown_{signed_state}")


def is_unsigned_or_broken(signed_state: int) -> bool:
    """
    Check if signed state indicates a security concern.

    Args:
        signed_state: Firefox signedState integer code

    Returns:
        True if unsigned or broken signature
    """
    return signed_state in UNSIGNED_OR_BROKEN_STATES


def should_skip_addon_type(addon_type: str) -> bool:
    """
    Check if addon type should be skipped (system/builtin).

    Args:
        addon_type: Firefox addon type string

    Returns:
        True if addon should be skipped
    """
    if addon_type is None:
        return False
    return addon_type.lower() in SKIP_ADDON_TYPES
