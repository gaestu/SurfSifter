"""
Registry parser logic.

Handles parsing of offline registry hives using regipy and rule definitions.
"""

from __future__ import annotations

import json
import re
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

from core.logging import get_logger

LOGGER = get_logger("extractors.system.registry.parser")


# =============================================================================
# Timestamp Conversion Helpers
# =============================================================================

# Windows FILETIME epoch: January 1, 1601 UTC
# Unix epoch: January 1, 1970 UTC
# Difference in 100-nanosecond intervals
EPOCH_AS_FILETIME = 116444736000000000
HUNDREDS_OF_NS = 10000000


def filetime_to_datetime(filetime: Union[int, bytes]) -> Optional[datetime]:
    """
    Convert Windows FILETIME to datetime.

    FILETIME is a 64-bit value representing 100-nanosecond intervals
    since January 1, 1601 UTC.

    Args:
        filetime: Either an integer FILETIME value or 8 bytes (little-endian)

    Returns:
        datetime object in UTC, or None if conversion fails
    """
    try:
        if isinstance(filetime, bytes):
            if len(filetime) != 8:
                return None
            filetime = struct.unpack('<Q', filetime)[0]

        if filetime == 0:
            return None

        timestamp = (filetime - EPOCH_AS_FILETIME) / HUNDREDS_OF_NS
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def unix_timestamp_to_datetime(timestamp: int) -> Optional[datetime]:
    """
    Convert Unix timestamp to datetime.

    Args:
        timestamp: Unix timestamp (seconds since 1970-01-01)

    Returns:
        datetime object in UTC, or None if conversion fails
    """
    try:
        if timestamp == 0:
            return None
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


def systemtime_to_datetime(data: bytes) -> Optional[datetime]:
    """
    Convert Windows SYSTEMTIME structure to datetime.

    SYSTEMTIME is a 16-byte structure containing:
        WORD wYear           (2 bytes)
        WORD wMonth          (2 bytes)
        WORD wDayOfWeek      (2 bytes)  - ignored
        WORD wDay            (2 bytes)
        WORD wHour           (2 bytes)
        WORD wMinute         (2 bytes)
        WORD wSecond         (2 bytes)
        WORD wMilliseconds   (2 bytes)

    Used in Windows registry for DateLastConnected, DateCreated in
    NetworkList\\Profiles keys.

    Args:
        data: 16 bytes of SYSTEMTIME structure (little-endian)

    Returns:
        datetime object in UTC, or None if conversion fails
    """
    try:
        if not isinstance(data, bytes) or len(data) != 16:
            return None

        # Unpack 8 WORDs (unsigned 16-bit little-endian)
        year, month, day_of_week, day, hour, minute, second, ms = struct.unpack(
            '<8H', data
        )

        # Validate ranges
        if year < 1601 or year > 3000:
            return None
        if month < 1 or month > 12:
            return None
        if day < 1 or day > 31:
            return None
        if hour > 23 or minute > 59 or second > 59:
            return None

        return datetime(
            year=year,
            month=month,
            day=day,
            hour=hour,
            minute=minute,
            second=second,
            microsecond=ms * 1000,  # milliseconds to microseconds
            tzinfo=timezone.utc
        )
    except (ValueError, struct.error, OverflowError) as e:
        LOGGER.debug("SYSTEMTIME conversion failed: %s", e)
        return None


def format_datetime(dt: Optional[datetime], fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """
    Format datetime to string.

    Args:
        dt: datetime object or None
        fmt: strftime format string

    Returns:
        Formatted string or empty string if dt is None
    """
    if dt is None:
        return ""
    return dt.strftime(fmt)


@dataclass(slots=True)
class RegistryFinding:
    """Registry finding data structure."""
    detector_id: str
    name: str
    value: str
    confidence: str
    provenance: str
    hive: str
    path: str
    extra_json: str | None = None


def _get_key_robust(hive, path: str):
    """
    Robustly get a registry key, handling path separators and case sensitivity.

    Args:
        hive: RegistryHive object
        path: Path string (e.g. "Microsoft\\Windows\\CurrentVersion")

    Returns:
        RegistryKey object

    Raises:
        ValueError: If key not found
    """
    # Normalize path separators
    path = path.replace("/", "\\")

    # Try direct access first
    try:
        return hive.get_key(path)
    except Exception:
        pass

    # Try manual traversal (case-insensitive)
    parts = [p for p in path.split("\\") if p]
    current_key = hive.root

    for part in parts:
        found = False
        for subkey in current_key.iter_subkeys():
            if subkey.name.lower() == part.lower():
                current_key = subkey
                found = True
                break

        if not found:
            # Detailed error for debugging
            available = [k.name for k in current_key.iter_subkeys()]
            # Limit available list in error message
            avail_str = ", ".join(available[:10]) + ("..." if len(available) > 10 else "")
            raise ValueError(f"Key not found: {path} (failed at '{part}'). Available: {avail_str}")

    return current_key


def process_hive_file(
    hive_path: Path,
    target: Dict[str, Any],
) -> List[RegistryFinding]:
    """
    Process a local hive file against a target definition.

    Args:
        hive_path: Path to the local hive file
        target: Target definition from rules

    Returns:
        List of RegistryFinding objects
    """
    try:
        from regipy.registry import RegistryHive  # type: ignore
    except ImportError:
        LOGGER.error("regipy not installed")
        return []

    findings: List[RegistryFinding] = []

    try:
        hive = RegistryHive(str(hive_path))

        for action in target.get("actions", []):
            if action.get("type") != "registry_reader":
                continue

            # Process keys
            for key_def in action.get("keys", []):
                key_path_pattern = key_def.get("path")
                if not key_path_pattern:
                    continue

                # Handle wildcards in registry path
                # If path ends with *, we iterate subkeys
                if key_path_pattern.endswith("\\*"):
                    base_path = key_path_pattern[:-2]
                    try:
                        key = _get_key_robust(hive, base_path)
                        for subkey in key.iter_subkeys():
                            subkey_path = f"{base_path}\\{subkey.name}"
                            _process_registry_key(
                                subkey,
                                key_def,
                                target,
                                action,
                                str(hive_path),
                                findings,
                                subkey_path
                            )
                    except Exception as e:
                        LOGGER.debug("Failed to iterate subkeys of %s in hive %s: %s", base_path, hive_path, e)
                        pass
                else:
                    # Exact path
                    try:
                        key = _get_key_robust(hive, key_path_pattern)
                        _process_registry_key(
                            key,
                            key_def,
                            target,
                            action,
                            str(hive_path),
                            findings,
                            key_path_pattern
                        )
                    except Exception as e:
                        LOGGER.debug("Failed to get key %s in hive %s: %s", key_path_pattern, hive_path, e)
                        pass

    except Exception as e:
        LOGGER.warning("Error processing hive %s: %s", hive_path, e)

    return findings


def _process_registry_key(
    key,
    key_def: Dict[str, Any],
    target: Dict[str, Any],
    action: Dict[str, Any],
    hive_path: str,
    findings: List[RegistryFinding],
    key_path_str: str
):
    """Process a single registry key and extract values."""

    # Check if we need to extract specific values
    values_to_check = key_def.get("values", [])

    # Check if this is a software entry (Uninstall key) - extract full metadata
    if key_def.get("extract_software_entry"):
        _process_software_entry(key, key_def, target, action, hive_path, findings, key_path_str)
        return

    # Check if we need to extract all values (e.g. Run keys)
    if key_def.get("extract_all_values"):
        try:
            for val in key.iter_values():
                val_name = val.name
                val_content = val.value

                findings.append(RegistryFinding(
                    detector_id=target.get("name"),
                    name=key_def.get("indicator", target.get("name")),
                    value=str(val_content),
                    confidence=str(key_def.get("confidence", 1.0)),
                    provenance=action.get("provenance", "registry"),
                    hive=hive_path,
                    path=f"{key_path_str}\\{val_name}",
                    extra_json=json.dumps({
                        "type": key_def.get("indicator"),
                        "value_name": val_name,
                        "raw_value": val_content,
                        "key_last_modified": str(key.header.last_modified)
                    }, default=str)
                ))
        except Exception as e:
            LOGGER.warning("Failed to iterate values for key %s: %s", key_path_str, e)
        return

    # If no values specified, maybe we just want to flag the key existence?
    if not values_to_check and key_def.get("extract"):
        # Just flag the key
        findings.append(RegistryFinding(
            detector_id=target.get("name"),
            name=key_def.get("indicator", target.get("name")),
            value=key.name,
            confidence=str(key_def.get("confidence", 1.0)),
            provenance=action.get("provenance", "registry"),
            hive=hive_path,
            path=key_path_str,
            extra_json=json.dumps({"timestamp": str(key.header.last_modified)})
        ))
        return

    for val_def in values_to_check:
        val_name = val_def.get("name")
        val_regex = val_def.get("regex")

        registry_value = None
        try:
            registry_value = key.get_value(val_name)
        except Exception:
            continue

        if registry_value is None:
            continue

        value_content = registry_value
        display_value = value_content  # Human-readable display value
        extra_data = {}  # Additional metadata for extra_json

        # Regex check
        if val_regex:
            if not re.match(val_regex, str(value_content)):
                continue

        # Type conversion for timestamps and special types
        value_type = val_def.get("type")

        if value_type == "unix_timestamp":
            # Unix timestamp (seconds since 1970)
            try:
                if isinstance(value_content, int):
                    dt = unix_timestamp_to_datetime(value_content)
                    if dt:
                        display_value = format_datetime(dt)
                        extra_data["timestamp_utc"] = dt.isoformat()
            except Exception:
                pass

        elif value_type == "filetime":
            # Windows FILETIME (100-ns intervals since 1601)
            try:
                dt = filetime_to_datetime(value_content)
                if dt:
                    display_value = format_datetime(dt)
                    extra_data["timestamp_utc"] = dt.isoformat()
            except Exception:
                pass

        elif value_type == "filetime_bytes":
            # FILETIME stored as raw bytes (e.g., ShutdownTime)
            try:
                if isinstance(value_content, bytes) and len(value_content) == 8:
                    dt = filetime_to_datetime(value_content)
                    if dt:
                        display_value = format_datetime(dt)
                        extra_data["timestamp_utc"] = dt.isoformat()
            except Exception:
                pass

        elif value_type == "systemtime_bytes":
            # SYSTEMTIME stored as raw 16 bytes (e.g., DateLastConnected, DateCreated)
            try:
                if isinstance(value_content, bytes) and len(value_content) == 16:
                    dt = systemtime_to_datetime(value_content)
                    if dt:
                        display_value = format_datetime(dt)
                        extra_data["timestamp_utc"] = dt.isoformat()
                    else:
                        # Keep raw hex representation for debugging
                        display_value = value_content.hex()
            except Exception:
                pass

        elif value_type == "profile_path":
            # Extract username from profile path (e.g., C:\Users\HP -> HP)
            try:
                if isinstance(value_content, str):
                    # Normalize path and extract username
                    path_str = value_content.replace("/", "\\")
                    parts = path_str.rstrip("\\").split("\\")
                    username = parts[-1] if parts else value_content
                    display_value = username
                    extra_data["profile_path"] = value_content
                    extra_data["username"] = username
            except Exception:
                pass

        # Build extra_json with all metadata
        extra_json_data = {
            "type": val_def.get("indicator"),  # Semantic type
            "raw_value": value_content,
            "key_last_modified": str(key.header.last_modified),
            **extra_data,  # Include any additional type-specific data
        }

        # Create finding
        findings.append(RegistryFinding(
            detector_id=target.get("name"),
            name=val_def.get("indicator", target.get("name")),
            value=str(display_value),
            confidence=str(val_def.get("confidence", 1.0)),
            provenance=action.get("provenance", "registry"),
            hive=hive_path,
            path=f"{key_path_str}\\{val_name}",
            extra_json=json.dumps(extra_json_data, default=str)
        ))


# =============================================================================
# Software Entry Processing (Uninstall Keys)
# =============================================================================

# Registry values to extract for installed software
SOFTWARE_FIELDS = [
    "DisplayName",
    "Publisher",
    "DisplayVersion",
    "InstallDate",
    "InstallLocation",
    "InstallSource",
    "UninstallString",
    "EstimatedSize",
    "URLInfoAbout",
    "Comments",
]

# Forensically interesting software patterns (case-insensitive)
FORENSIC_SOFTWARE_PATTERNS = [
    "deep freeze",
    "faronics",
    "shadow defender",
    "reboot restore",
    "rollback rx",
    "time freeze",
    "toolwiz time freeze",
    "returnil",
    "steadystate",
    "ccleaner",
    "evidence eliminator",
    "privazer",
    "bleachbit",
]


def _process_software_entry(
    key,
    key_def: Dict[str, Any],
    target: Dict[str, Any],
    action: Dict[str, Any],
    hive_path: str,
    findings: List[RegistryFinding],
    key_path_str: str
) -> None:
    """
    Process a software Uninstall key and extract full metadata.

    Extracts DisplayName, Publisher, Version, InstallDate, InstallLocation, etc.
    and stores them as a single finding with correlated extra_json.

    Args:
        key: Registry key object (the specific software subkey)
        key_def: Key definition from rules
        target: Target definition
        action: Action configuration
        hive_path: Path to the hive file
        findings: List to append findings to
        key_path_str: Full registry path string
    """
    # Collect all software metadata from this key
    software_data: Dict[str, Any] = {
        "registry_key": key.name,  # The Uninstall subkey name (e.g., "Google Chrome")
        "key_last_modified": str(key.header.last_modified),
    }

    display_name = None

    try:
        for field_name in SOFTWARE_FIELDS:
            try:
                value = key.get_value(field_name)
                if value is not None:
                    # Convert to appropriate type
                    if field_name == "EstimatedSize":
                        # Size in KB
                        software_data["size_kb"] = value
                    elif field_name == "InstallDate":
                        # YYYYMMDD format - parse if valid
                        software_data["install_date"] = str(value)
                        if len(str(value)) == 8:
                            try:
                                # Format as YYYY-MM-DD for readability
                                date_str = str(value)
                                software_data["install_date_formatted"] = (
                                    f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                                )
                            except Exception:
                                pass
                    elif field_name == "DisplayName":
                        display_name = str(value)
                        software_data["name"] = display_name
                    elif field_name == "Publisher":
                        software_data["publisher"] = str(value)
                    elif field_name == "DisplayVersion":
                        software_data["version"] = str(value)
                    elif field_name == "InstallLocation":
                        software_data["install_location"] = str(value)
                    elif field_name == "InstallSource":
                        software_data["install_source"] = str(value)
                    elif field_name == "UninstallString":
                        software_data["uninstall_command"] = str(value)
                    elif field_name == "URLInfoAbout":
                        software_data["url"] = str(value)
                    elif field_name == "Comments":
                        software_data["comments"] = str(value)
            except Exception:
                continue
    except Exception as e:
        LOGGER.debug("Error reading software values from %s: %s", key_path_str, e)

    # Skip entries without DisplayName (likely orphan/system entries)
    if not display_name:
        return

    # Check for forensically interesting software
    is_forensic = False
    forensic_category = None
    display_name_lower = display_name.lower()
    publisher_lower = software_data.get("publisher", "").lower()

    for pattern in FORENSIC_SOFTWARE_PATTERNS:
        if pattern in display_name_lower or pattern in publisher_lower:
            is_forensic = True
            if "freeze" in pattern or "rollback" in pattern or "restore" in pattern:
                forensic_category = "system_restore"
            elif "cleaner" in pattern or "eliminator" in pattern or "privazer" in pattern or "bleachbit" in pattern:
                forensic_category = "anti_forensic"
            else:
                forensic_category = "forensic_interest"
            break

    if is_forensic:
        software_data["forensic_interest"] = True
        software_data["forensic_category"] = forensic_category

    # Determine if this is a 32-bit app on 64-bit Windows
    if "WOW6432Node" in key_path_str:
        software_data["architecture"] = "32-bit"

    # Create the finding
    findings.append(RegistryFinding(
        detector_id=target.get("name"),
        name=key_def.get("indicator", "system:installed_software"),
        value=display_name,
        confidence=str(key_def.get("confidence", 1.0)),
        provenance=action.get("provenance", "registry"),
        hive=hive_path,
        path=key_path_str,
        extra_json=json.dumps(software_data, default=str)
    ))

