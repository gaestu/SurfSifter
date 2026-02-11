"""
Firefox Sync Data schema definitions for extraction warnings.

This module defines the known JSON keys and structures in the signedInUser.json
file used by Firefox for sync account data. Used for schema warning detection.

signedInUser.json structure:
- accountData: Main account information object
  - email, uid, displayName, verified
  - sessionToken, keyFetchToken, unwrapBKey (auth tokens)
  - device: Registered device info
- version: Schema version
- profilePath: Local profile path (sometimes at root level)

References:
- Mozilla source: services/fxaccounts/
- Firefox Accounts: https://github.com/mozilla/fxa

Initial implementation with schema warning support
"""

from __future__ import annotations

from typing import Set

# =============================================================================
# Known Keys for Schema Warning Detection
# =============================================================================

# Top-level keys in signedInUser.json
KNOWN_ROOT_KEYS: Set[str] = {
    "accountData",
    "version",
    "profilePath",
}

# Keys expected in the accountData section
KNOWN_ACCOUNT_DATA_KEYS: Set[str] = {
    # Core account info
    "email",
    "uid",
    "displayName",
    "verified",
    "profilePath",

    # Authentication tokens
    "sessionToken",
    "keyFetchToken",
    "unwrapBKey",
    "kSync",  # Sync encryption key (derived)
    "kXCS",   # Key cross-check string
    "kExtSync",  # Extension sync key
    "kExtKbHash",  # Extension key hash

    # Session state
    "sessionTokenState",
    "keyFetchTokenState",

    # Device registration
    "device",

    # Profile metadata
    "locale",
    "ecosystem_anon_id",  # Telemetry ID (newer Firefox)

    # Migration/upgrade state
    "encryptedSendTabKeys",  # Send Tab encryption
    "scopedKeys",  # Per-scope encryption keys

    # OAuth fields (Firefox 91+)
    "oauthTokens",
    "scopedKeysValidationString",
}

# Keys expected in the accountData.device section
KNOWN_DEVICE_KEYS: Set[str] = {
    "id",
    "name",
    "type",
    "pushCallback",
    "pushPublicKey",
    "pushAuthKey",
    "pushEndpointExpired",
    "availableCommands",
    "lastCommandIndex",
    "capabilities",  # Newer Firefox versions
    "registrationVersion",  # Device registration version
}

# Known device types from Firefox Accounts
KNOWN_DEVICE_TYPES: Set[str] = {
    "desktop",
    "mobile",
    "tablet",
    "vr",  # VR headsets (Firefox Reality)
    "tv",  # TV devices
    "unknown",
}

# Known available commands (sync commands between devices)
KNOWN_AVAILABLE_COMMANDS: Set[str] = {
    "https://identity.mozilla.com/cmd/open-uri",  # Send Tab
    "https://identity.mozilla.com/cmd/close-tabs",  # Close remote tabs
    "https://identity.mozilla.com/cmd/ring",  # Find my device (mobile)
    "https://identity.mozilla.com/cmd/wipe-data",  # Remote wipe
}

# Synced types inferred from token presence
SYNCED_TYPE_INDICATORS: dict = {
    "sessionToken": "active_session",
    "keyFetchToken": "key_sync",
    "unwrapBKey": "encryption_enabled",
    "kSync": "sync_encryption_ready",
    "encryptedSendTabKeys": "send_tab_enabled",
}

# =============================================================================
# Artifact Type
# =============================================================================

ARTIFACT_TYPE = "sync_data"
