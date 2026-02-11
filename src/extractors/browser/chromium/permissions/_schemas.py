"""
Chromium Permissions schema definitions.

Known permission types, values, and keys for schema warning detection.
This module defines the expected schema elements for Chromium's Preferences
JSON file under profile.content_settings.exceptions.

Usage:
    from ._schemas import (
        CHROMIUM_PERMISSION_TYPES,
        CHROMIUM_PERMISSION_VALUES,
        KNOWN_EXCEPTION_KEYS,
        KNOWN_SETTING_KEYS,
    )

Initial implementation for schema warning support
"""

from __future__ import annotations

from typing import Dict, Set


# =============================================================================
# Permission Type Mapping
# =============================================================================
# Maps Chromium's internal permission type names to normalized display names.
# Keys are the names found in profile.content_settings.exceptions
# Values are our normalized names for display/reporting

CHROMIUM_PERMISSION_TYPES: Dict[str, str] = {
    # Core permissions (commonly seen)
    "notifications": "notifications",
    "geolocation": "geolocation",
    "media_stream_camera": "camera",
    "media_stream_mic": "microphone",
    "midi_sysex": "midi",
    "push_messaging": "push",

    # Device/hardware access
    "usb_guard": "usb",
    "usb_chooser_data": "usb_chooser_data",  #
    "serial_guard": "serial",
    "serial_chooser_data": "serial_chooser_data",  #
    "bluetooth_guard": "bluetooth",
    "bluetooth_chooser_data": "bluetooth_chooser_data",  #
    "bluetooth_scanning": "bluetooth_scanning",  #
    "hid_guard": "hid",
    "hid_chooser_data": "hid_chooser_data",  #
    "nfc_devices": "nfc_devices",  #
    "nfc": "nfc",  # Alias for nfc_devices

    # Background/sync
    "background_sync": "background_sync",
    "periodic_background_sync": "periodic_background_sync",

    # Sensors/detection
    "sensors": "sensors",
    "idle_detection": "idle_detection",
    "accessibility_events": "accessibility",

    # Clipboard/input
    "clipboard": "clipboard",
    "clipboard_read_write": "clipboard_read_write",
    "clipboard_sanitized_write": "clipboard_sanitized_write",

    # Payment/commerce
    "payment_handler": "payment",
    "secure_payment_confirmation": "secure_payment_confirmation",

    # File system
    "file_system_access_guard": "file_system",
    "file_system_read_guard": "file_system_read",
    "file_system_write_guard": "file_system_write",
    "native_file_system_read_guard": "file_system_read",  # Legacy name
    "native_file_system_write_guard": "file_system_write",  # Legacy name
    "file_handling": "file_handling",
    "file_system_last_picked_directory": "file_system_last_picked_directory",  #
    "file_system_access_chooser_data": "file_system_access_chooser_data",  #

    # Window/display
    "window_placement": "window_placement",
    "window_management": "window_management",
    "captured_surface_control": "captured_surface_control",
    "get_display_media_set_select_all_screens": "get_display_media_set_select_all_screens",  #

    # Fonts/media
    "local_fonts": "fonts",
    "protected_media_identifier": "drm",

    # Content settings (not exactly "permissions" but same format)
    "images": "images",
    "javascript": "javascript",
    "javascript_jit": "javascript_jit",  #
    "popups": "popups",
    "cookies": "cookies",
    "plugins": "plugins",
    "automatic_downloads": "downloads",
    "autoplay": "autoplay",
    "mixed_script": "mixed_content",
    "sound": "sound",
    "ads": "ads",

    # Protocol handlers
    "protocol_handler": "protocol_handler",
    "register_protocol_handler": "register_protocol_handler",

    # AR/VR
    "ar": "ar",
    "vr": "vr",

    # Web features
    "durable_storage": "durable_storage",
    "federated_identity_api": "federated_identity",
    "federated_identity_auto_reauthn": "federated_identity_auto_reauthn",
    "storage_access": "storage_access",
    "top_level_storage_access": "top_level_storage_access",

    # FedCM (Federated Credential Management)
    "fedcm_active_session": "fedcm_active_session",
    "fedcm_idp_signin": "fedcm_idp_signin",
    "fedcm_share": "fedcm_share",
    "webid_api": "webid_api",

    # Insecure content
    "insecure_private_network": "insecure_private_network",
    "legacy_cookie_access": "legacy_cookie_access",

    # Private network
    "private_network_guard": "private_network_guard",
    "private_network_chooser_data": "private_network_chooser_data",

    # Site engagement/metrics
    "site_engagement": "site_engagement",
    "media_engagement": "media_engagement",
    "notification_interactions": "notification_interactions",
    "notification_permission_review": "notification_permission_review",
    "important_site_info": "important_site_info",
    "app_banner": "app_banner",

    # Security/blocking
    "subresource_filter": "subresource_filter",
    "subresource_filter_data": "subresource_filter_data",
    "safe_browsing_url_check_data": "safe_browsing_url_check_data",
    "password_protection": "password_protection",
    "permission_autoblocking_data": "permission_autoblocking_data",
    "permission_autorevocation_data": "permission_autorevocation_data",
    "ssl_cert_decisions": "ssl_cert_decisions",
    "http_allowed": "http_allowed",
    "secure_network": "secure_network",
    "secure_network_sites": "secure_network_sites",

    # Tracking/privacy
    "trackers": "trackers",
    "trackers_data": "trackers_data",
    "tracking_org_relationships": "tracking_org_relationships",
    "tracking_org_exceptions": "tracking_org_exceptions",

    # Client hints
    "client_hints": "client_hints",
    "reduced_accept_language": "reduced_accept_language",

    # Auto-select/intent
    "auto_select_certificate": "auto_select_certificate",
    "intent_picker_auto_display": "intent_picker_auto_display",

    # Unused sites/permissions
    "unused_site_permissions": "unused_site_permissions",
    "clear_browsing_data_cookies_exceptions": "clear_browsing_data_cookies_exceptions",

    # Misc
    "formfill_metadata": "formfill_metadata",
    "camera_pan_tilt_zoom": "camera_pan_tilt_zoom",
    "token_binding": "token_binding",
    "sleeping_tabs": "sleeping_tabs",
    "installed_web_app_metadata": "installed_web_app_metadata",  # PWA metadata

    # Edge-specific
    "edge_ad_targeting": "edge_ad_targeting",
    "edge_ad_targeting_data": "edge_ad_targeting_data",
    "edge_sdsm": "edge_sdsm",
    "edge_u2f_api_request": "edge_u2f_api_request",
    "edge_user_agent_token": "edge_user_agent_token",

    # Deprecated/legacy
    "ppapi_broker": "ppapi_broker",
    "flash_data": "flash",
}

# Known permission type keys (for detecting unknowns)
KNOWN_EXCEPTION_KEYS: Set[str] = set(CHROMIUM_PERMISSION_TYPES.keys())


# =============================================================================
# Permission Value Mapping
# =============================================================================
# Maps Chromium's content setting codes to human-readable values.
# Reference: chrome/browser/content_settings/content_settings_utils.cc

CHROMIUM_PERMISSION_VALUES: Dict[int, str] = {
    0: "default",      # CONTENT_SETTING_DEFAULT
    1: "allow",        # CONTENT_SETTING_ALLOW
    2: "block",        # CONTENT_SETTING_BLOCK
    3: "ask",          # CONTENT_SETTING_ASK
    4: "session_only", # CONTENT_SETTING_SESSION_ONLY (cookies)
    5: "detect_important_content",  # CONTENT_SETTING_DETECT_IMPORTANT_CONTENT
}

# Known setting value codes
KNOWN_SETTING_VALUES: Set[int] = set(CHROMIUM_PERMISSION_VALUES.keys())


# =============================================================================
# Known Keys in Permission Settings
# =============================================================================
# Keys that can appear within each origin's settings dict

KNOWN_SETTING_KEYS: Set[str] = {
    # Core setting
    "setting",

    # Timestamps
    "last_modified",
    "last_visit",
    "expiration",

    # Model/metadata
    "model",
    "display_name",
    "lifetime",

    # Notification-specific
    "notification_permission_source",

    # Content-specific
    "per_resource",
    "content_setting",

    # Expiration
    "expiring_session_count",

    # Embedded data (can be complex nested structures)
    "embedder",
}


# =============================================================================
# Reverse Mappings (for lookup)
# =============================================================================

# Reverse mapping: normalized name -> Chromium internal name
NORMALIZED_TO_CHROMIUM_TYPE: Dict[str, str] = {
    v: k for k, v in CHROMIUM_PERMISSION_TYPES.items()
}

# Reverse mapping: string value -> code
STRING_TO_SETTING_CODE: Dict[str, int] = {
    v: k for k, v in CHROMIUM_PERMISSION_VALUES.items()
}


def get_permission_type_display(chromium_type: str) -> str:
    """
    Get normalized display name for a Chromium permission type.

    Args:
        chromium_type: Internal Chromium permission type name

    Returns:
        Normalized display name, or original if unknown
    """
    return CHROMIUM_PERMISSION_TYPES.get(chromium_type, chromium_type)


def get_permission_value_display(code: int) -> str:
    """
    Get human-readable name for a permission value code.

    Args:
        code: Chromium content setting code (0-5)

    Returns:
        Human-readable value name, or "unknown" if not recognized
    """
    return CHROMIUM_PERMISSION_VALUES.get(code, "unknown")


def is_known_permission_type(chromium_type: str) -> bool:
    """Check if a permission type is in our known list."""
    return chromium_type in KNOWN_EXCEPTION_KEYS


def is_known_setting_value(code: int) -> bool:
    """Check if a setting value code is known."""
    return code in KNOWN_SETTING_VALUES


def is_known_setting_key(key: str) -> bool:
    """Check if a key within settings dict is known."""
    return key in KNOWN_SETTING_KEYS
