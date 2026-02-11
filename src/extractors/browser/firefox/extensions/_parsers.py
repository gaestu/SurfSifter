"""
Firefox Extensions JSON parsing utilities.

This module handles parsing of extensions.json and addons.json files
with schema warning support for unknown fields.

Initial extraction from extractor.py
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from core.logging import get_logger

from ._schemas import (
    KNOWN_ADDON_KEYS,
    KNOWN_ADDONS_JSON_ADDON_KEYS,
    SKIP_ADDON_TYPES,
    should_skip_addon_type,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector
    from ....callbacks import ExtractorCallbacks

LOGGER = get_logger("extractors.browser.firefox.extensions.parsers")


# =============================================================================
# Main Parsing Functions
# =============================================================================

def parse_extensions_json(
    file_path: Path,
    browser: str,
    profile: str,
    file_entry: Dict[str, Any],
    addons_metadata: Dict[str, Dict],
    callbacks: "ExtractorCallbacks",
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse extensions.json from disk and return list of extension info.

    Args:
        file_path: Path to extracted extensions.json file
        browser: Browser identifier (firefox, tor, etc.)
        profile: Profile name
        file_entry: File metadata dict from manifest
        addons_metadata: Pre-parsed addons.json metadata by extension ID
        callbacks: Extractor callbacks for logging
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of parsed extension info dicts
    """
    extensions = []

    if not file_path.exists():
        callbacks.on_log(f"extensions.json not found: {file_path}", "warning")
        return extensions

    source_file = file_entry.get("logical_path", str(file_path))

    try:
        data = json.loads(file_path.read_text(encoding='utf-8', errors='replace'))

        # Schema warning: Check for unknown root keys
        if warning_collector:
            from ._schemas import KNOWN_EXTENSIONS_JSON_ROOT_KEYS
            _check_unknown_keys(
                data,
                KNOWN_EXTENSIONS_JSON_ROOT_KEYS,
                warning_collector,
                source_file,
                "extensions.json root",
            )

        for addon in data.get("addons", []):
            # Schema warning: Check for unknown addon keys
            if warning_collector:
                _check_unknown_addon_keys(
                    addon, warning_collector, source_file
                )

            ext_info = parse_firefox_addon(
                addon, browser, profile, source_file,
                warning_collector=warning_collector,
            )
            if ext_info:
                # Merge supplementary metadata from addons.json if available
                ext_id = ext_info.get("extension_id")
                if ext_id and ext_id in addons_metadata:
                    ext_info = merge_addons_metadata(ext_info, addons_metadata[ext_id])

                # Add provenance from file entry
                ext_info["partition_index"] = file_entry.get("partition_index")
                ext_info["fs_type"] = file_entry.get("fs_type")
                ext_info["forensic_path"] = file_entry.get("forensic_path")

                extensions.append(ext_info)
                callbacks.on_log(f"Found extension: {ext_info['name']}", "info")

        LOGGER.debug("Parsed extensions.json: %d extensions", len(extensions))

    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse extensions.json: {e}"
        callbacks.on_log(error_msg, "warning")
        LOGGER.warning("JSON parse error at %s: %s", file_path, e)

        if warning_collector:
            warning_collector.add_json_parse_error(
                filename=source_file,
                error=str(e),
            )

    except Exception as e:
        callbacks.on_log(f"Failed to parse extensions.json: {e}", "warning")
        LOGGER.warning("Failed to parse extensions.json at %s: %s", file_path, e)

    return extensions


def parse_addons_json(
    file_path: Path,
    callbacks: "ExtractorCallbacks",
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    source_file: Optional[str] = None,
) -> Dict[str, Dict]:
    """
    Parse addons.json from disk and return metadata by extension ID.

    Args:
        file_path: Path to extracted addons.json file
        callbacks: Extractor callbacks for logging
        warning_collector: Optional warning collector for schema discovery
        source_file: Original source path for warnings

    Returns:
        Dict mapping extension ID to AMO metadata dict
    """
    addons_metadata = {}

    if not file_path.exists():
        callbacks.on_log(f"addons.json not found: {file_path}", "warning")
        return addons_metadata

    src_file = source_file or str(file_path)

    try:
        data = json.loads(file_path.read_text(encoding='utf-8', errors='replace'))

        for addon in data.get("addons", []):
            addon_id = addon.get("id")
            if addon_id:
                # Schema warning: Check for unknown addons.json addon keys
                if warning_collector:
                    _check_unknown_keys(
                        addon,
                        KNOWN_ADDONS_JSON_ADDON_KEYS,
                        warning_collector,
                        src_file,
                        f"addons.json addon {addon_id}",
                    )

                addons_metadata[addon_id] = extract_addons_json_metadata(addon)

        LOGGER.debug("Parsed addons.json: %d entries", len(addons_metadata))

    except json.JSONDecodeError as e:
        callbacks.on_log(f"Failed to parse addons.json: {e}", "warning")
        LOGGER.warning("JSON parse error at %s: %s", file_path, e)

        if warning_collector:
            warning_collector.add_json_parse_error(
                filename=src_file,
                error=str(e),
            )

    except Exception as e:
        callbacks.on_log(f"Failed to parse addons.json: {e}", "warning")
        LOGGER.warning("Failed to parse addons.json at %s: %s", file_path, e)

    return addons_metadata


def parse_firefox_addon(
    addon: Dict[str, Any],
    browser: str,
    profile: str,
    source_path: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Optional[Dict[str, Any]]:
    """
    Parse a Firefox addon entry from extensions.json.

    Args:
        addon: Addon dict from extensions.json
        browser: Browser identifier
        profile: Profile name
        source_path: Source file path for provenance
        warning_collector: Optional warning collector

    Returns:
        Parsed extension info dict or None if should be skipped
    """
    addon_id = addon.get("id")
    if not addon_id:
        return None

    # Skip system/builtin addons
    addon_type = addon.get("type")
    if should_skip_addon_type(addon_type):
        return None

    # Schema warning: Check for unknown signedState values
    signed_state = addon.get("signedState")
    if warning_collector and signed_state is not None:
        from ._schemas import KNOWN_SIGNED_STATES
        if signed_state not in KNOWN_SIGNED_STATES:
            warning_collector.add_warning(
                warning_type="unknown_enum_value",
                category="json",
                severity="info",
                artifact_type="extensions",
                source_file=source_path,
                item_name="signedState",
                item_value=str(signed_state),
            )

    ext_info = {
        "browser": browser,
        "profile": profile,
        "extension_id": addon_id,
        "version": addon.get("version"),
        "name": addon.get("defaultLocale", {}).get("name") or addon.get("name", "Unknown"),
        "description": addon.get("defaultLocale", {}).get("description") or addon.get("description"),
        "author": addon.get("defaultLocale", {}).get("creator"),
        "homepage_url": addon.get("homepageURL"),
        "enabled": 1 if addon.get("active") else 0,
        "permissions": addon.get("permissions", []),
        "host_permissions": [],
        "source_path": source_path,
        "logical_path": source_path,
        "signed_state": signed_state,
        "addon_type": addon_type,
    }

    # Convert install/update times (milliseconds since epoch)
    if addon.get("installDate"):
        ext_info["install_time_utc"] = ms_to_iso8601(addon["installDate"])
    if addon.get("updateDate"):
        ext_info["update_time_utc"] = ms_to_iso8601(addon["updateDate"])

    return ext_info


# =============================================================================
# Metadata Extraction Helpers
# =============================================================================

def extract_addons_json_metadata(addon: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract supplementary metadata from addons.json entry.

    addons.json contains AMO (addons.mozilla.org) metadata not in extensions.json:
    - sourceURI: Download/update URL
    - icons: Icon URLs (various sizes)
    - repositoryStatus: AMO verification status
    - amoListingURL: AMO listing page URL

    Args:
        addon: Addon dict from addons.json

    Returns:
        Dict with extracted AMO metadata
    """
    return {
        "source_uri": addon.get("sourceURI"),
        "icon_url": get_best_icon(addon.get("icons", {})),
        "amo_listing_url": addon.get("amoListingURL"),
        "repository_status": addon.get("repositoryStatus"),
        "average_daily_users": addon.get("averageDailyUsers"),
        "weekly_downloads": addon.get("weeklyDownloads"),
    }


def merge_addons_metadata(ext_info: Dict[str, Any], addons_meta: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge addons.json metadata into extension info.

    Args:
        ext_info: Extension info dict from extensions.json parsing
        addons_meta: AMO metadata dict from addons.json

    Returns:
        Merged extension info dict
    """
    # Only add fields that are missing or empty in ext_info
    if not ext_info.get("homepage_url") and addons_meta.get("amo_listing_url"):
        ext_info["homepage_url"] = addons_meta["amo_listing_url"]

    # Add AMO-specific fields
    ext_info["source_uri"] = addons_meta.get("source_uri")
    ext_info["icon_url"] = addons_meta.get("icon_url")
    ext_info["repository_status"] = addons_meta.get("repository_status")

    return ext_info


def get_best_icon(icons: Dict[str, str]) -> Optional[str]:
    """
    Get the best quality icon URL from icons dict.

    Args:
        icons: Dict mapping size to URL

    Returns:
        Best icon URL or None
    """
    if not icons:
        return None
    # Prefer larger icons: 128 > 64 > 48 > 32 > 16
    for size in ["128", "64", "48", "32", "16"]:
        if size in icons:
            return icons[size]
    # Fallback to any available icon
    return next(iter(icons.values()), None)


# =============================================================================
# Time Conversion Helpers
# =============================================================================

def ms_to_iso8601(ms: Optional[int]) -> Optional[str]:
    """
    Convert milliseconds since 1970 to ISO 8601.

    Args:
        ms: Milliseconds since epoch

    Returns:
        ISO 8601 timestamp string or None
    """
    if ms is None or ms == 0:
        return None
    try:
        # Guard against negative or extremely large values
        if ms < 0 or ms > 253402300799999:  # Year 9999
            return None
        unix_seconds = ms / 1000
        dt = datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, OSError, OverflowError):
        return None


# =============================================================================
# Schema Warning Helpers
# =============================================================================

def _check_unknown_keys(
    data: Dict[str, Any],
    known_keys: set,
    warning_collector: "ExtractionWarningCollector",
    source_file: str,
    context: str,
) -> None:
    """
    Check for unknown keys in a dict and add warnings.

    Args:
        data: Dict to check
        known_keys: Set of known/expected keys
        warning_collector: Warning collector
        source_file: Source file path
        context: Context description for warning
    """
    if not isinstance(data, dict):
        return

    unknown_keys = set(data.keys()) - known_keys
    for key in unknown_keys:
        warning_collector.add_warning(
            warning_type="json_unknown_key",
            category="json",
            severity="info",
            artifact_type="extensions",
            source_file=source_file,
            item_name=f"{context}.{key}",
            item_value=str(type(data[key]).__name__),
        )


def _check_unknown_addon_keys(
    addon: Dict[str, Any],
    warning_collector: "ExtractionWarningCollector",
    source_file: str,
) -> None:
    """
    Check for unknown keys in an addon entry (addon level only).

    Args:
        addon: Addon dict from extensions.json
        warning_collector: Warning collector
        source_file: Source file path
    """
    if not isinstance(addon, dict):
        return

    addon_id = addon.get("id", "unknown")
    unknown_keys = set(addon.keys()) - KNOWN_ADDON_KEYS

    for key in unknown_keys:
        warning_collector.add_warning(
            warning_type="json_unknown_key",
            category="json",
            severity="info",
            artifact_type="extensions",
            source_file=source_file,
            item_name=f"addon[{addon_id}].{key}",
            item_value=str(type(addon[key]).__name__),
        )
