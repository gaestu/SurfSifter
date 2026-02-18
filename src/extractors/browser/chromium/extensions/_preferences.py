"""
Chromium Extensions Preferences parsing functions.

This module handles parsing of Chromium Preferences JSON files to extract
extension runtime state (enabled/disabled, install_time, permissions, etc.).

Extracted from extractor.py, added schema warning support
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .._patterns import CHROMIUM_BROWSERS, get_artifact_patterns, get_patterns_for_root
from .._parsers import extract_profile_from_path
from ._schemas import (
    PATH_SEPARATOR,
    KNOWN_PREFERENCES_FIELDS,
    KNOWN_EXTENSION_STATES,
    KNOWN_INSTALL_LOCATIONS,
    KNOWN_DISABLE_REASON_BITS,
    get_unknown_disable_bits,
)
from ...._shared.timestamps import webkit_to_iso
from core.logging import get_logger

if TYPE_CHECKING:
    from ....callbacks import ExtractorCallbacks
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.browser.chromium.extensions.preferences")


def parse_all_preferences(
    evidence_fs,
    browsers: List[str],
    output_dir: Path,
    callbacks: "ExtractorCallbacks",
    *,
    embedded_roots: Optional[List[str]] = None,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Dict[str, Any]:
    """
    Parse Preferences files for all browsers to extract extension state.

    Args:
        evidence_fs: Evidence filesystem interface
        browsers: List of browser keys to search
        output_dir: Output directory for copying Preferences files
        callbacks: Extractor callbacks for progress/logging
        warning_collector: Optional warning collector for schema discovery

    Returns:
        Dict with:
        - extensions: Dict[key, ext_data] where key = browser:profile:extension_id
        - files_parsed: List of Preferences files successfully parsed
    """
    result = {
        "extensions": {},
        "files_parsed": [],
    }

    for browser_key in browsers:
        if browser_key not in CHROMIUM_BROWSERS:
            continue

        browser_result = parse_browser_preferences(
            evidence_fs,
            browser_key,
            output_dir,
            callbacks,
            embedded_roots=None,
            warning_collector=warning_collector,
        )
        result["extensions"].update(browser_result.get("extensions", {}))
        result["files_parsed"].extend(browser_result.get("files_parsed", []))

    if embedded_roots:
        embedded_result = parse_browser_preferences(
            evidence_fs,
            "chromium_embedded",
            output_dir,
            callbacks,
            embedded_roots=embedded_roots,
            warning_collector=warning_collector,
        )
        result["extensions"].update(embedded_result.get("extensions", {}))
        result["files_parsed"].extend(embedded_result.get("files_parsed", []))

    LOGGER.info(
        "Parsed %d Preferences files, found %d extension settings",
        len(result["files_parsed"]),
        len(result["extensions"]),
    )
    return result


def parse_browser_preferences(
    evidence_fs,
    browser: str,
    output_dir: Path,
    callbacks: "ExtractorCallbacks",
    *,
    embedded_roots: Optional[List[str]] = None,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Dict[str, Any]:
    """
    Parse Preferences files for a specific browser.

    Args:
        evidence_fs: Evidence filesystem interface
        browser: Browser key
        output_dir: Output directory for copying Preferences files
        callbacks: Extractor callbacks for progress/logging
        warning_collector: Optional warning collector for schema discovery

    Returns:
        Dict with extensions and files_parsed lists
    """
    result = {
        "extensions": {},
        "files_parsed": [],
    }

    patterns: List[str] = []
    if browser in CHROMIUM_BROWSERS:
        try:
            patterns.extend(get_artifact_patterns(browser, "permissions"))
        except ValueError:
            pass

    if embedded_roots:
        for root in embedded_roots:
            patterns.extend(get_patterns_for_root(root, "preferences", flat_profile=False))
            patterns.extend(get_patterns_for_root(root, "preferences", flat_profile=True))

    if not patterns:
        return result

    for pattern in patterns:
        # Only process Preferences files, not permissions.sqlite
        if "Preferences" not in pattern and not pattern.endswith("Preferences"):
            continue

        try:
            for path_str in evidence_fs.iter_paths(pattern):
                if callbacks.is_cancelled():
                    return result

                parsed = _parse_single_preferences_file(
                    evidence_fs,
                    path_str,
                    browser,
                    output_dir,
                    callbacks,
                    warning_collector=warning_collector,
                )

                if parsed:
                    result["extensions"].update(parsed.get("extensions", {}))
                    if parsed.get("file_info"):
                        result["files_parsed"].append(parsed["file_info"])

        except Exception as e:
            LOGGER.debug("Pattern %s failed: %s", pattern, e)

    return result


def _parse_single_preferences_file(
    evidence_fs,
    path_str: str,
    browser: str,
    output_dir: Path,
    callbacks: "ExtractorCallbacks",
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Optional[Dict[str, Any]]:
    """
    Parse a single Preferences file.

    Args:
        evidence_fs: Evidence filesystem interface
        path_str: Path to Preferences file in evidence
        browser: Browser key
        output_dir: Output directory for copying
        callbacks: Extractor callbacks
        warning_collector: Optional warning collector

    Returns:
        Dict with extensions and file_info, or None on error
    """
    result = {
        "extensions": {},
        "file_info": None,
    }

    try:
        content = evidence_fs.read_file(path_str)
        prefs = json.loads(content.decode('utf-8', errors='replace'))

        # Extract profile from path
        profile = _extract_profile_from_preferences_path(path_str)

        # Parse extensions.settings section
        ext_settings = prefs.get("extensions", {}).get("settings", {})

        for ext_id, settings in ext_settings.items():
            # Skip component/internal extensions (location 5)
            location = settings.get("location", 0)
            if location == 5:
                continue

            # Check for unknown preference fields
            if warning_collector:
                _check_unknown_preferences_fields(
                    settings,
                    ext_id,
                    path_str,
                    warning_collector,
                )

            key = f"{browser}:{profile}:{ext_id}"

            # Extract state (0=disabled, 1=enabled)
            state = settings.get("state", 1)
            enabled = 1 if state == 1 else 0

            # Check for unknown state values
            if warning_collector and state not in KNOWN_EXTENSION_STATES:
                warning_collector.add_warning(
                    warning_type="unknown_enum_value",
                    category="json",
                    severity="info",
                    artifact_type="extensions",
                    source_file=path_str,
                    item_name="state",
                    item_value=str(state),
                    context_json={"extension_id": ext_id},
                )

            # Check for unknown install location
            if warning_collector and location not in KNOWN_INSTALL_LOCATIONS:
                warning_collector.add_warning(
                    warning_type="unknown_enum_value",
                    category="json",
                    severity="info",
                    artifact_type="extensions",
                    source_file=path_str,
                    item_name="location",
                    item_value=str(location),
                    context_json={"extension_id": ext_id},
                )

            # Check for unknown disable reason bits
            disable_reasons = settings.get("disable_reasons", 0)
            if warning_collector and disable_reasons:
                unknown_bits = get_unknown_disable_bits(disable_reasons)
                for bit in unknown_bits:
                    warning_collector.add_warning(
                        warning_type="unknown_enum_value",
                        category="json",
                        severity="info",
                        artifact_type="extensions",
                        source_file=path_str,
                        item_name="disable_reasons_bit",
                        item_value=str(bit),
                        context_json={"extension_id": ext_id, "full_bitmask": disable_reasons},
                    )

            # Extract install_time (WebKit timestamp)
            install_time_raw = settings.get("install_time")
            install_time_utc = None
            if install_time_raw:
                try:
                    # Preferences stores as string
                    install_time_utc = webkit_to_iso(int(install_time_raw))
                except (ValueError, TypeError):
                    pass

            result["extensions"][key] = {
                "browser": browser,
                "profile": profile,
                "extension_id": ext_id,
                "enabled": enabled,
                "state": state,
                "disable_reasons": disable_reasons,
                "install_location": location,
                "from_webstore": 1 if settings.get("from_webstore", False) else 0,
                "install_time_utc": install_time_utc,
                "granted_permissions": settings.get("granted_permissions"),
                "active_permissions": settings.get("active_permissions"),
                "preferences_path": path_str,
            }

        # Copy Preferences file to output
        safe_profile = re.sub(r'[^a-zA-Z0-9_-]', '_', profile)[:32]
        pref_filename = f"{browser}_{safe_profile}_Preferences.json"
        pref_dest = output_dir / pref_filename
        pref_dest.write_bytes(content)

        result["file_info"] = {
            "source_path": path_str,
            "dest_path": str(pref_dest),
            "browser": browser,
            "profile": profile,
            "extension_count": len(ext_settings),
        }

        callbacks.on_log(
            f"Parsed {browser}/{profile} Preferences: {len(ext_settings)} extensions",
            "info",
        )

        return result

    except json.JSONDecodeError as e:
        LOGGER.debug("Failed to parse Preferences JSON at %s: %s", path_str, e)
        if warning_collector:
            warning_collector.add_json_parse_error(
                filename=path_str,
                error=str(e),
            )
        return None

    except Exception as e:
        LOGGER.debug("Failed to read Preferences at %s: %s", path_str, e)
        return None


def _extract_profile_from_preferences_path(path_str: str) -> str:
    """
    Extract profile name from a Preferences file path.

    Args:
        path_str: Full path to Preferences file

    Returns:
        Profile name (e.g., "Default", "Profile 1")
    """
    return extract_profile_from_path(path_str) or "Default"


def _check_unknown_preferences_fields(
    settings: Dict[str, Any],
    ext_id: str,
    source_file: str,
    warning_collector: "ExtractionWarningCollector",
) -> None:
    """
    Check extension settings for unknown fields and record as warnings.

    Args:
        settings: Extension settings dict from Preferences
        ext_id: Extension ID for context
        source_file: Source file path
        warning_collector: Warning collector instance
    """
    from extractors._shared.extraction_warnings import (
        CATEGORY_JSON,
        SEVERITY_INFO,
    )

    for key in settings.keys():
        if key not in KNOWN_PREFERENCES_FIELDS:
            warning_collector.add_warning(
                warning_type="json_unknown_key",
                category=CATEGORY_JSON,
                severity=SEVERITY_INFO,
                artifact_type="extensions",
                source_file=source_file,
                item_name=f"extensions.settings.{key}",
                item_value=str(type(settings[key]).__name__),
                context_json={"extension_id": ext_id},
            )


def merge_preferences_data(
    extensions: List[Dict[str, Any]],
    preferences_data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Merge Preferences data into extensions discovered from manifests.

    Preferences provides authoritative runtime state:
    - enabled/disabled state
    - install_time
    - disable_reasons
    - install_location
    - from_webstore
    - granted_permissions

    Args:
        extensions: List of extension dicts from manifest discovery
        preferences_data: Dict with extensions key from Preferences parsing

    Returns:
        List of extensions with merged Preferences data
    """
    pref_extensions = preferences_data.get("extensions", {})

    for ext in extensions:
        key = f"{ext['browser']}:{ext['profile']}:{ext['extension_id']}"

        if key in pref_extensions:
            pref_data = pref_extensions[key]

            # Override enabled state from Preferences (authoritative)
            ext["enabled"] = pref_data.get("enabled", ext.get("enabled", 1))

            # Add Preferences-only fields
            ext["disable_reasons"] = pref_data.get("disable_reasons", 0)
            ext["install_location"] = pref_data.get("install_location")
            ext["from_webstore"] = pref_data.get("from_webstore")
            ext["granted_permissions"] = pref_data.get("granted_permissions")

            # Use Preferences install_time if available (more accurate)
            if pref_data.get("install_time_utc"):
                ext["install_time_utc"] = pref_data["install_time_utc"]

            ext["preferences_path"] = pref_data.get("preferences_path")

            LOGGER.debug(
                "Merged Preferences data for %s: enabled=%d, location=%s",
                ext["extension_id"],
                ext["enabled"],
                ext.get("install_location"),
            )

    return extensions
