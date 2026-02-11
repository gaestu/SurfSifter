"""
Chromium Extensions discovery functions.

This module handles discovery of extension manifests and metadata from
evidence filesystems. Supports all Chromium-based browsers.

Extracted from extractor.py, added partition tracking and warning support
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from .._patterns import CHROMIUM_BROWSERS, get_artifact_patterns, is_flat_profile_browser
from ._schemas import (
    EXTENSION_MANIFEST_PATTERN,
    PATH_SEPARATOR,
    KNOWN_MANIFEST_KEYS,
    PARSED_MANIFEST_KEYS,
)
from core.logging import get_logger

if TYPE_CHECKING:
    from ....callbacks import ExtractorCallbacks
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.browser.chromium.extensions.discovery")


def discover_extensions(
    evidence_fs,
    browsers: List[str],
    callbacks: "ExtractorCallbacks",
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Discover all Chromium browser extensions from evidence.

    Args:
        evidence_fs: Evidence filesystem interface
        browsers: List of browser keys to search
        callbacks: Extractor callbacks for progress/logging
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of extension info dicts with metadata and forensic provenance
    """
    extensions = []

    for browser_key in browsers:
        if browser_key not in CHROMIUM_BROWSERS:
            continue
        extensions.extend(
            discover_browser_extensions(
                evidence_fs,
                browser_key,
                callbacks,
                warning_collector=warning_collector,
            )
        )

    return extensions


def discover_browser_extensions(
    evidence_fs,
    browser: str,
    callbacks: "ExtractorCallbacks",
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Discover extensions for a specific Chromium browser.

    Args:
        evidence_fs: Evidence filesystem interface
        browser: Browser key (chrome, edge, brave, opera, etc.)
        callbacks: Extractor callbacks for progress/logging
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of extension info dicts
    """
    extensions = []

    try:
        patterns = get_artifact_patterns(browser, "extensions")
    except ValueError:
        return extensions

    for pattern in patterns:
        # Add manifest.json suffix to extension directory patterns
        if not pattern.endswith("manifest.json"):
            pattern = f"{pattern}{EXTENSION_MANIFEST_PATTERN}"

        try:
            for path_str in evidence_fs.iter_paths(pattern):
                try:
                    ext_info = _parse_extension_manifest(
                        evidence_fs,
                        path_str,
                        browser,
                        warning_collector=warning_collector,
                    )
                    if ext_info:
                        extensions.append(ext_info)
                        callbacks.on_log(f"Found {browser} extension: {ext_info['name']}", "info")

                except Exception as e:
                    LOGGER.debug("Failed to parse extension at %s: %s", path_str, e)
                    # Record parse error as warning
                    if warning_collector:
                        warning_collector.add_json_parse_error(
                            filename=path_str,
                            error=str(e),
                        )

        except Exception as e:
            LOGGER.debug("Pattern %s failed: %s", pattern, e)

    return extensions


def _parse_extension_manifest(
    evidence_fs,
    path_str: str,
    browser: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Optional[Dict[str, Any]]:
    """
    Parse a single extension manifest.json file.

    Args:
        evidence_fs: Evidence filesystem interface
        path_str: Path to manifest.json in evidence
        browser: Browser key
        warning_collector: Optional warning collector for schema discovery

    Returns:
        Extension info dict or None if invalid
    """
    content = evidence_fs.read_file(path_str)
    manifest = json.loads(content.decode('utf-8', errors='replace'))

    # Extract info from path
    path_parts = path_str.split(PATH_SEPARATOR)
    extension_id = None
    version = None
    profile = "Default"
    partition_index = None

    # Find extension ID and version from path
    # Pattern: .../Extensions/{extension_id}/{version}/manifest.json
    for i, part in enumerate(path_parts):
        if part == "Extensions" and i + 2 < len(path_parts):
            extension_id = path_parts[i + 1]
            version = path_parts[i + 2]
        elif part == "User Data" and i + 1 < len(path_parts):
            profile = path_parts[i + 1]

    # Handle Opera-style flat profiles
    if is_flat_profile_browser(browser):
        profile = _extract_opera_profile(path_parts)

    # Try to extract partition index from path (if available)
    partition_index = _extract_partition_index(path_parts)

    if not extension_id:
        return None

    # Collect unknown manifest keys for schema warnings
    if warning_collector:
        _check_unknown_manifest_keys(manifest, path_str, warning_collector)

    # Build extension info dict
    ext_info = {
        "browser": browser,
        "profile": profile,
        "extension_id": extension_id,
        "version": version or manifest.get("version"),
        "name": manifest.get("name", "Unknown"),
        "description": manifest.get("description"),
        "author": manifest.get("author"),
        "homepage_url": manifest.get("homepage_url"),
        "manifest_version": manifest.get("manifest_version"),
        "permissions": manifest.get("permissions", []),
        "host_permissions": manifest.get("host_permissions", []),
        "optional_permissions": manifest.get("optional_permissions", []),
        "content_scripts": manifest.get("content_scripts"),
        "background": manifest.get("background"),  # V2: scripts[], V3: service_worker
        "web_accessible_resources": manifest.get("web_accessible_resources"),
        # New: Extract CSP and update_url for forensic analysis
        "content_security_policy": manifest.get("content_security_policy"),
        "update_url": manifest.get("update_url"),
        # Forensic provenance
        "source_path": path_str,
        "logical_path": path_str,
        "partition_index": partition_index,
        "enabled": 1,
    }

    # Manifest V2 uses "permissions" for hosts too - separate them
    if ext_info["manifest_version"] == 2:
        host_patterns = [p for p in ext_info["permissions"] if _is_host_pattern(p)]
        ext_info["host_permissions"].extend(host_patterns)

    return ext_info


def _extract_opera_profile(path_parts: List[str]) -> str:
    """
    Extract profile name from Opera-style flat profile path.

    Opera stores profiles directly without Default/ subdirectory.

    Args:
        path_parts: Path split by separator

    Returns:
        Profile name (usually "Default" for Opera)
    """
    for part in path_parts:
        if "Opera" in part or "opera" in part:
            return "Default"
    return "Default"


def _extract_partition_index(path_parts: List[str]) -> Optional[int]:
    """
    Extract partition index from evidence path if available.

    Looks for partition indicators in the path like "p0", "p1", "partition0", etc.

    Args:
        path_parts: Path split by separator

    Returns:
        Partition index or None if not found
    """
    import re

    for part in path_parts:
        # Match patterns like "p0", "p1", "partition0", "Partition 1"
        match = re.match(r'^(?:p|partition\s*)(\d+)$', part, re.IGNORECASE)
        if match:
            return int(match.group(1))

    return None


def _is_host_pattern(permission: str) -> bool:
    """
    Check if a permission string is a host pattern.

    Host patterns start with URL schemes or are special host permissions.

    Args:
        permission: Permission string to check

    Returns:
        True if permission is a host pattern
    """
    return (
        permission.startswith("http://") or
        permission.startswith("https://") or
        permission.startswith("*://") or
        permission.startswith("file://") or
        permission.startswith("ftp://") or
        permission == "<all_urls>"
    )


def _check_unknown_manifest_keys(
    manifest: Dict[str, Any],
    source_file: str,
    warning_collector: "ExtractionWarningCollector",
) -> None:
    """
    Check manifest for unknown keys and record as warnings.

    Args:
        manifest: Parsed manifest.json dict
        source_file: Source file path for warning context
        warning_collector: Warning collector instance
    """
    from extractors._shared.extraction_warnings import (
        CATEGORY_JSON,
        SEVERITY_INFO,
    )

    for key in manifest.keys():
        if key not in KNOWN_MANIFEST_KEYS:
            warning_collector.add_warning(
                warning_type="json_unknown_key",
                category=CATEGORY_JSON,
                severity=SEVERITY_INFO,
                artifact_type="extensions",
                source_file=source_file,
                item_name=key,
                item_value=str(type(manifest[key]).__name__),
                context_json={"manifest_version": manifest.get("manifest_version")},
            )


def extract_profile_from_path(path_str: str, browser: str = "") -> str:
    """
    Extract profile name from a Chromium path.

    Handles both standard Chromium profile structure (User Data/Profile *)
    and Opera-style flat profiles.

    Args:
        path_str: Full path string
        browser: Optional browser key for context

    Returns:
        Profile name (e.g., "Default", "Profile 1")
    """
    path_parts = path_str.split(PATH_SEPARATOR)

    # Look for "User Data" directory followed by profile
    for i, part in enumerate(path_parts):
        if part == "User Data" and i + 1 < len(path_parts):
            return path_parts[i + 1]

    # For Opera-style flat profiles
    for part in path_parts:
        if "Opera" in part or "opera" in part:
            return "Default"

    return "Default"
