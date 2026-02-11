"""
Firefox Sync Data parsing utilities.

This module provides parsing functions for extracting sync account info and
device registration from Firefox signedInUser.json files.

Data Format:
- Firefox stores sync data in signedInUser.json (JSON format)
- Main section: accountData with email, uid, device info
- Tokens indicate sync features: sessionToken, keyFetchToken, etc.

Initial implementation with schema warning support
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

from ._schemas import (
    KNOWN_ROOT_KEYS,
    KNOWN_ACCOUNT_DATA_KEYS,
    KNOWN_DEVICE_KEYS,
    SYNCED_TYPE_INDICATORS,
    ARTIFACT_TYPE,
)

__all__ = [
    "parse_firefox_sync",
    "discover_unknown_sync_keys",
]

LOGGER = logging.getLogger(__name__)


def parse_firefox_sync(
    data: Dict[str, Any],
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    source_file: Optional[str] = None,
) -> Dict[str, Any]:
    """Parse Firefox sync data from signedInUser.json.

    Extracts:
    - Account info (email, uid, displayName, verified status)
    - Device registration (id, name, type, push settings)
    - Sync capabilities based on available tokens

    Firefox signedInUser.json contains:
    - accountData.email - User email (cleartext)
    - accountData.uid - Firefox Account UID
    - accountData.displayName - Display name if set
    - accountData.verified - Email verification status
    - accountData.profilePath - Local profile path
    - accountData.sessionToken - Active session indicator
    - accountData.keyFetchToken - Key sync indicator
    - accountData.unwrapBKey - Encryption key state
    - accountData.device.{id,name,type} - Device registration
    - accountData.device.pushEndpointExpired - Push notification state
    - accountData.device.availableCommands - Enabled sync commands

    Args:
        data: Parsed signedInUser.json dictionary
        warning_collector: Optional collector for schema warnings
        source_file: Source file path for warning context

    Returns:
        Dictionary with 'accounts' and 'devices' keys
    """
    result: Dict[str, Any] = {"accounts": [], "devices": []}

    # Track unknown keys for warnings
    if warning_collector:
        discover_unknown_sync_keys(data, warning_collector, source_file)

    # Firefox signedInUser.json structure
    account_data = data.get("accountData")

    # Handle None or missing accountData (user logged out)
    if not account_data:
        return result

    email = account_data.get("email", "")
    uid = account_data.get("uid", "")

    if email or uid:
        # Build list of synced/enabled types based on available tokens
        synced_types: List[str] = []
        for token_key, type_name in SYNCED_TYPE_INDICATORS.items():
            if account_data.get(token_key):
                synced_types.append(type_name)

        # Check device available commands for remote_commands capability
        device_info = account_data.get("device", {})
        if device_info and device_info.get("availableCommands"):
            synced_types.append("remote_commands")

        # Extract profile path if present
        profile_path = account_data.get("profilePath", "")
        if not profile_path:
            # Some versions store it at root level
            profile_path = data.get("profilePath", "")

        account = {
            "account_id": uid,
            "email": email,
            "display_name": account_data.get("displayName", ""),
            "gaia_id": "",  # Chrome-specific, not used in Firefox
            "profile_path": profile_path,
            "last_sync_time": None,  # Not directly available in signedInUser.json
            "sync_enabled": account_data.get("verified", False),
            "synced_types": synced_types,
            "raw_data": account_data,
        }

        result["accounts"].append(account)

    # Firefox device info with additional fields
    device_registration = account_data.get("device", {}) if account_data else {}
    if device_registration and isinstance(device_registration, dict):
        # Extract available commands as capabilities indicator
        available_commands = device_registration.get("availableCommands", {})
        capabilities = list(available_commands.keys()) if available_commands else []

        device = {
            "device_id": device_registration.get("id", ""),
            "device_name": device_registration.get("name", ""),
            "device_type": device_registration.get("type", "desktop"),
            "os_type": "",  # Not directly available in Firefox sync
            "browser_version": "",  # Not directly available
            "last_updated": None,
            "sync_account_id": uid if account_data else "",
            "push_expired": device_registration.get("pushEndpointExpired", False),
            "capabilities": capabilities,
            "raw_data": device_registration,
        }
        result["devices"].append(device)

    return result


def discover_unknown_sync_keys(
    data: Dict[str, Any],
    warning_collector: "ExtractionWarningCollector",
    source_file: Optional[str] = None,
) -> None:
    """Discover unknown keys in signedInUser.json for schema warnings.

    Checks:
    - Root-level keys
    - accountData section keys
    - accountData.device section keys

    Args:
        data: Parsed signedInUser.json dictionary
        warning_collector: Collector to add warnings to
        source_file: Source file path for warning context
    """
    # Check root-level keys
    if isinstance(data, dict):
        unknown_root = set(data.keys()) - KNOWN_ROOT_KEYS
        for key in unknown_root:
            warning_collector.add_warning(
                warning_type="json_unknown_key",
                category="json",
                severity="info",
                artifact_type=ARTIFACT_TYPE,
                source_file=source_file,
                item_name=f"root.{key}",
                item_value=str(type(data[key]).__name__),
            )

    # Check accountData section
    account_data = data.get("accountData", {})
    if isinstance(account_data, dict):
        unknown_keys = set(account_data.keys()) - KNOWN_ACCOUNT_DATA_KEYS
        for key in unknown_keys:
            warning_collector.add_warning(
                warning_type="json_unknown_key",
                category="json",
                severity="info",
                artifact_type=ARTIFACT_TYPE,
                source_file=source_file,
                item_name=f"accountData.{key}",
                item_value=str(type(account_data[key]).__name__),
            )

        # Check device section within accountData
        device_data = account_data.get("device", {})
        if isinstance(device_data, dict):
            unknown_device_keys = set(device_data.keys()) - KNOWN_DEVICE_KEYS
            for key in unknown_device_keys:
                warning_collector.add_warning(
                    warning_type="json_unknown_key",
                    category="json",
                    severity="info",
                    artifact_type=ARTIFACT_TYPE,
                    source_file=source_file,
                    item_name=f"accountData.device.{key}",
                    item_value=str(type(device_data[key]).__name__),
                )
