"""
Chromium Sync Data schema definitions for extraction warnings.

This module defines the known JSON keys and structures in the Preferences
file related to sync data. Used for schema warning detection.

Preferences JSON structure (sync-related sections):
- account_info: Array of Google account objects
- google.services: Google services configuration
- sync: Sync settings, device list, enabled types
- signin: Sign-in state and restrictions
- protection_request_schedule: Account protection settings

References:
- Chromium source: components/sync/
- Preferences: chrome/browser/prefs/
"""

from __future__ import annotations

from typing import Set

# =============================================================================
# Known Keys for Schema Warning Detection
# =============================================================================

# Keys expected in the account_info array items
KNOWN_ACCOUNT_INFO_KEYS: Set[str] = {
    "account_id",
    "email",
    "full_name",
    "given_name",
    "gaia",
    "hd",  # Hosted domain (for G Suite accounts)
    "is_child_account",
    "is_under_advanced_protection",
    "locale",
    "picture_url",
    "is_supervised_child",
    "last_downloaded_image_url_with_size",
}

# Keys expected in the google.services section
KNOWN_GOOGLE_SERVICES_KEYS: Set[str] = {
    "account_id",
    "username",
    "signin",
    "consented_to_sync",
    "last_account_id",
    "last_username",
    "default_account_id",
    "last_gaia_id",
    "last_signed_in_time",
}

# Keys expected in the sync section
KNOWN_SYNC_KEYS: Set[str] = {
    # Sync state
    "last_synced_time",
    "has_setup_completed",
    "keep_everything_synced",
    "requested_types",
    "passphrase_type",
    "encryption_bootstrap_token",
    "keystore_encryption_bootstrap_token",

    # Devices
    "devices",

    # Individual sync types (boolean flags)
    "apps",
    "autofill",
    "autofill_profiles",
    "autofill_wallet_credential",
    "autofill_wallet_metadata",
    "autofill_wallet_offer",
    "bookmarks",
    "dictionary",
    "extension_settings",
    "extensions",
    "history_delete_directives",
    "passwords",
    "payment_instruments",
    "preferences",
    "priority_preferences",
    "reading_list",
    "saved_tab_groups",
    "search_engines",
    "security_events",
    "send_tab_to_self",
    "sessions",
    "sharing_message",
    "tabs",
    "themes",
    "typed_urls",
    "user_consents",
    "user_events",
    "web_apps",
    "wifi_configurations",
    "workspace_desk",

    # Invalidations
    "invalidations",
    "invalidation_versions",
}

# Keys expected in device objects within sync.devices
KNOWN_DEVICE_KEYS: Set[str] = {
    "name",
    "type",
    "os",
    "chrome_version",
    "last_updated_timestamp",
    "signin_scoped_device_id",
    "send_tab_to_self_receiving_enabled",
    "sharing_info",
    "full_name",
    "model_name",
    "manufacturer_name",
    "last_updated",
    "feature_fields",
}

# Device types known from Chromium source
KNOWN_DEVICE_TYPES: Set[str] = {
    "desktop",
    "phone",
    "tablet",
    "chromeos",
    "linux",
    "mac",
    "windows",
    "android",
    "ios",
    "unknown",
    "win",
    "cros",
    "unset",
}

# =============================================================================
# Extended Schema Keys
# =============================================================================

# Keys expected in the signin section (top-level Preferences key)
KNOWN_SIGNIN_KEYS: Set[str] = {
    "allowed",
    "allowed_on_next_startup",
}

# Keys expected in protection_request_schedule section
KNOWN_PROTECTION_KEYS: Set[str] = {
    "backoff_duration",
    "backoff_entry",
    "last_refresh_time",
    "minimum_delay",
    "next_request_time",
}

# Keys expected in profile section (limited to forensically relevant)
KNOWN_PROFILE_KEYS: Set[str] = {
    "avatar_index",
    "content_settings",
    "created_by_version",
    "default_content_setting_values",
    "exit_type",
    "exited_cleanly",
    "info_cache",
    "last_time_obsolete_http_credentials_removed",
    "managed_user_id",
    "name",
    "password_manager_enabled",
    "using_default_avatar",
    "using_default_name",
    "using_gaia_avatar",
}

# =============================================================================
# Table Patterns for Discovery (not applicable for JSON, but for consistency)
# =============================================================================

# Preferences file artifact type
ARTIFACT_TYPE = "sync_data"