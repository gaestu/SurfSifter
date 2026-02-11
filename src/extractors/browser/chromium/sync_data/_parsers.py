"""
Chromium Sync Data parsing utilities.

This module provides parsing functions for extracting sync account info,
device inventory, and sync settings from Chromium Preferences JSON.

Data Format:
- Chromium stores sync data in Preferences JSON file
- Sections: account_info, google.services, sync (contains device list)
- Extended sections: signin, protection_request_schedule
- Timestamps: Chrome internal format (microseconds since Windows epoch)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

from ._schemas import (
    KNOWN_ACCOUNT_INFO_KEYS,
    KNOWN_GOOGLE_SERVICES_KEYS,
    KNOWN_SYNC_KEYS,
    KNOWN_DEVICE_KEYS,
    KNOWN_SIGNIN_KEYS,
    KNOWN_PROTECTION_KEYS,
    KNOWN_PROFILE_KEYS,
)

__all__ = [
    "chrome_timestamp_to_iso",
    "parse_chromium_sync",
    "discover_unknown_sync_keys",
]

LOGGER = logging.getLogger(__name__)


def chrome_timestamp_to_iso(timestamp: int) -> Optional[str]:
    """Convert Chrome internal timestamp to ISO 8601.

    Chrome uses microseconds since Jan 1, 1601 (Windows FILETIME epoch).

    Args:
        timestamp: Chrome timestamp (microseconds since 1601-01-01)

    Returns:
        ISO 8601 formatted datetime string, or None if invalid
    """
    if not timestamp:
        return None
    try:
        # Convert from Chrome epoch to Unix epoch
        unix_seconds = timestamp / 1_000_000 - 11644473600
        if unix_seconds < 0:
            return None
        dt = datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, OSError, OverflowError):
        return None


def parse_chromium_sync(
    data: Dict[str, Any],
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    source_file: Optional[str] = None,
) -> Dict[str, Any]:
    """Parse Chromium browser sync data from Preferences JSON.

    Extracts:
    - Account info from account_info array
    - Google services fallback account data
    - Sync settings and enabled types
    - Device inventory from sync.devices
    - Sign-in state and restrictions
    - Account protection status

    Args:
        data: Parsed Preferences JSON dictionary
        warning_collector: Optional collector for schema warnings
        source_file: Source file path for warning context

    Returns:
        Dictionary with 'accounts', 'devices', and 'metadata' keys
    """
    result: Dict[str, Any] = {
        "accounts": [],
        "devices": [],
        "metadata": {
            "signin_allowed": None,
            "protection_info": None,
            "profile_name": None,
            "profile_exit_type": None,
        },
    }

    # Track unknown keys for warnings
    if warning_collector:
        discover_unknown_sync_keys(data, warning_collector, source_file)

    # Extract sign-in state (forensically relevant)
    signin_section = data.get("signin", {})
    if isinstance(signin_section, dict):
        result["metadata"]["signin_allowed"] = signin_section.get("allowed", True)

    # Extract protection_request_schedule info
    protection = data.get("protection_request_schedule", {})
    if isinstance(protection, dict) and protection:
        result["metadata"]["protection_info"] = {
            "last_refresh_time": protection.get("last_refresh_time"),
            "next_request_time": protection.get("next_request_time"),
        }

    # Extract profile metadata
    profile = data.get("profile", {})
    if isinstance(profile, dict):
        result["metadata"]["profile_name"] = profile.get("name")
        result["metadata"]["profile_exit_type"] = profile.get("exit_type")

    # Account info from account_info array
    account_info = data.get("account_info", [])
    if isinstance(account_info, list):
        for acc in account_info:
            account = {
                "account_id": acc.get("account_id", ""),
                "email": acc.get("email", ""),
                "display_name": acc.get("full_name", acc.get("given_name", "")),
                "gaia_id": acc.get("gaia", ""),
                "hosted_domain": acc.get("hd", ""),  # G Suite domain
                "is_child_account": acc.get("is_child_account", False),
                "is_under_advanced_protection": acc.get("is_under_advanced_protection", False),
                "profile_path": "",
                "last_sync_time": None,
                "sync_enabled": True,
                "synced_types": [],
                "raw_data": acc,
            }
            result["accounts"].append(account)

    # Google services section (fallback if account_info empty)
    google_services = data.get("google", {}).get("services", {})
    if google_services:
        account = {
            "account_id": google_services.get("account_id", ""),
            "email": google_services.get("username", ""),
            "display_name": "",
            "gaia_id": google_services.get("last_gaia_id", ""),
            "hosted_domain": "",
            "is_child_account": False,
            "is_under_advanced_protection": False,
            "profile_path": "",
            "last_sync_time": None,
            "sync_enabled": google_services.get("signin", {}).get("allowed", True),
            "synced_types": [],
            "raw_data": google_services,
        }
        # Only add if we have an email and it's not already in accounts
        if account["email"] and not any(
            a["email"] == account["email"] for a in result["accounts"]
        ):
            result["accounts"].append(account)

    # Sync settings and types
    sync_prefs = data.get("sync", {})
    if sync_prefs:
        synced_types = []
        for key, value in sync_prefs.items():
            if isinstance(value, bool) and value:
                synced_types.append(key)

        # Last sync time
        last_sync = sync_prefs.get("last_synced_time")
        last_sync_dt = chrome_timestamp_to_iso(int(last_sync)) if last_sync else None

        # Check if sync setup is completed
        has_setup_completed = sync_prefs.get("has_setup_completed", False)

        # Update accounts with sync info
        for account in result["accounts"]:
            account["synced_types"] = synced_types
            account["last_sync_time"] = last_sync_dt
            account["sync_enabled"] = account.get("sync_enabled", True) and has_setup_completed

    # Device info from sync section
    devices = data.get("sync", {}).get("devices", {})
    if isinstance(devices, dict):
        for device_id, device_data in devices.items():
            device = {
                "device_id": device_id,
                "device_name": device_data.get("name", "Unknown"),
                "device_type": device_data.get("type", "unknown"),
                "os_type": device_data.get("os", ""),
                "chrome_version": device_data.get("chrome_version", ""),
                "last_updated": None,
                "sync_account_id": result["accounts"][0]["account_id"] if result["accounts"] else "",
                "send_tab_to_self_enabled": device_data.get("send_tab_to_self_receiving_enabled", False),
                "raw_data": device_data,
            }

            last_updated = device_data.get("last_updated_timestamp")
            if last_updated:
                device["last_updated"] = chrome_timestamp_to_iso(int(last_updated))

            result["devices"].append(device)

    return result


def discover_unknown_sync_keys(
    data: Dict[str, Any],
    warning_collector: "ExtractionWarningCollector",
    source_file: Optional[str] = None,
) -> None:
    """Discover unknown keys in sync-related sections for schema warnings.

    Checks:
    - account_info array items
    - google.services section
    - sync section
    - sync.devices items
    - signin section
    - protection_request_schedule section
    - profile section (limited keys, )

    Args:
        data: Parsed Preferences JSON dictionary
        warning_collector: Collector to add warnings to
        source_file: Source file path for warning context
    """
    # Check account_info items
    account_info = data.get("account_info", [])
    if isinstance(account_info, list):
        for acc in account_info:
            if isinstance(acc, dict):
                unknown_keys = set(acc.keys()) - KNOWN_ACCOUNT_INFO_KEYS
                for key in unknown_keys:
                    warning_collector.add_warning(
                        warning_type="json_unknown_key",
                        category="json",
                        severity="info",
                        artifact_type="sync_data",
                        source_file=source_file,
                        item_name=f"account_info.{key}",
                        item_value=str(type(acc[key]).__name__),
                    )

    # Check google.services section
    google_services = data.get("google", {}).get("services", {})
    if isinstance(google_services, dict):
        unknown_keys = set(google_services.keys()) - KNOWN_GOOGLE_SERVICES_KEYS
        for key in unknown_keys:
            warning_collector.add_warning(
                warning_type="json_unknown_key",
                category="json",
                severity="info",
                artifact_type="sync_data",
                source_file=source_file,
                item_name=f"google.services.{key}",
                item_value=str(type(google_services[key]).__name__),
            )

    # Check sync section (top-level keys only)
    sync_prefs = data.get("sync", {})
    if isinstance(sync_prefs, dict):
        unknown_keys = set(sync_prefs.keys()) - KNOWN_SYNC_KEYS
        for key in unknown_keys:
            # Skip device IDs which are dynamic
            if key != "devices":
                warning_collector.add_warning(
                    warning_type="json_unknown_key",
                    category="json",
                    severity="info",
                    artifact_type="sync_data",
                    source_file=source_file,
                    item_name=f"sync.{key}",
                    item_value=str(type(sync_prefs[key]).__name__),
                )

    # Check device objects
    devices = data.get("sync", {}).get("devices", {})
    if isinstance(devices, dict):
        for device_id, device_data in devices.items():
            if isinstance(device_data, dict):
                unknown_keys = set(device_data.keys()) - KNOWN_DEVICE_KEYS
                for key in unknown_keys:
                    warning_collector.add_warning(
                        warning_type="json_unknown_key",
                        category="json",
                        severity="info",
                        artifact_type="sync_data",
                        source_file=source_file,
                        item_name=f"sync.devices.*.{key}",
                        item_value=str(type(device_data[key]).__name__),
                    )

    # Check signin section
    signin_section = data.get("signin", {})
    if isinstance(signin_section, dict):
        unknown_keys = set(signin_section.keys()) - KNOWN_SIGNIN_KEYS
        for key in unknown_keys:
            warning_collector.add_warning(
                warning_type="json_unknown_key",
                category="json",
                severity="info",
                artifact_type="sync_data",
                source_file=source_file,
                item_name=f"signin.{key}",
                item_value=str(type(signin_section[key]).__name__),
            )

    # Check protection_request_schedule section
    protection = data.get("protection_request_schedule", {})
    if isinstance(protection, dict):
        unknown_keys = set(protection.keys()) - KNOWN_PROTECTION_KEYS
        for key in unknown_keys:
            warning_collector.add_warning(
                warning_type="json_unknown_key",
                category="json",
                severity="info",
                artifact_type="sync_data",
                source_file=source_file,
                item_name=f"protection_request_schedule.{key}",
                item_value=str(type(protection[key]).__name__),
            )

    # Check profile section (limited forensically relevant keys, )
    profile = data.get("profile", {})
    if isinstance(profile, dict):
        # Only warn about keys we explicitly track - profile section has many keys
        # that aren't relevant to sync/account forensics
        for key in ["name", "exit_type", "exited_cleanly"]:
            if key not in profile and key in KNOWN_PROFILE_KEYS:
                # This is fine, not all keys are always present
                pass
