"""
Firefox Permissions parsing functions.

Modular parsers for Firefox permissions.sqlite and content-prefs.sqlite files.
These parsers are used by the extractor during ingestion phase.

Features:
- permissions.sqlite moz_perms table parsing
- content-prefs.sqlite parsing (groups/settings/prefs)
- Schema warning support for unknown tables/columns
- Unknown permission type tracking
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

from core.logging import get_logger
from extractors._shared.timestamps import prtime_to_datetime

from ._schemas import (
    KNOWN_PERMISSIONS_TABLES,
    KNOWN_MOZ_PERMS_COLUMNS,
    KNOWN_CONTENT_PREFS_TABLES,
    KNOWN_GROUPS_COLUMNS,
    KNOWN_SETTINGS_COLUMNS,
    KNOWN_PREFS_COLUMNS,
    PERMISSIONS_TABLE_PATTERNS,
    CONTENT_PREFS_TABLE_PATTERNS,
    FIREFOX_PERMISSION_VALUES,
    FIREFOX_PERMISSION_TYPE_MAP,
    CONTENT_PREF_TYPE_MAP,
    EXPIRE_TYPE_MAP,
    get_permission_value_name,
    normalize_permission_type,
    get_expire_type_name,
    normalize_content_pref_type,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.browser.firefox.permissions._parsers")


# =============================================================================
# permissions.sqlite Parsing
# =============================================================================

def parse_permissions_file(
    file_path: Path,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Firefox permissions.sqlite and return permission records.

    Args:
        file_path: Path to extracted permissions.sqlite file
        file_entry: Manifest entry with browser, profile, logical_path, etc.
        run_id: Current extraction run ID
        discovered_by: Extractor identification string
        warning_collector: Optional collector for schema warnings

    Returns:
        List of permission record dicts ready for database insertion
    """
    if not file_path.exists():
        LOGGER.warning("Permissions file not found: %s", file_path)
        return []

    browser = file_entry.get("browser", "firefox")
    profile = file_entry.get("profile", "default")
    source_file = file_entry.get("logical_path", str(file_path))

    try:
        conn = sqlite3.connect(f"file:{file_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        LOGGER.error("Failed to open permissions.sqlite: %s", e)
        if warning_collector:
            warning_collector.add_file_corrupt(
                filename=str(file_path),
                error=str(e),
                artifact_type="permissions",
            )
        return []

    records = []
    found_permission_values: Set[int] = set()
    found_permission_types: Set[str] = set()

    try:
        cursor = conn.cursor()

        # Schema discovery: detect unknown tables
        if warning_collector:
            from extractors._shared.extraction_warnings import discover_unknown_tables
            unknown_tables = discover_unknown_tables(
                conn, KNOWN_PERMISSIONS_TABLES, PERMISSIONS_TABLE_PATTERNS
            )
            for table_info in unknown_tables:
                warning_collector.add_unknown_table(
                    table_name=table_info["name"],
                    columns=table_info["columns"],
                    source_file=source_file,
                    artifact_type="permissions",
                )

        # Check if moz_perms table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='moz_perms'")
        if not cursor.fetchone():
            LOGGER.info("No moz_perms table found in %s", file_path)
            return []

        # Detect schema version via column inspection
        cursor.execute("PRAGMA table_info(moz_perms)")
        columns = {row[1] for row in cursor.fetchall()}

        has_origin = "origin" in columns
        has_host = "host" in columns
        has_modification_time = "modificationTime" in columns

        # Schema discovery: detect unknown columns
        if warning_collector:
            unknown_columns = columns - KNOWN_MOZ_PERMS_COLUMNS
            for col in unknown_columns:
                warning_collector.add_unknown_column(
                    table_name="moz_perms",
                    column_name=col,
                    column_type="unknown",
                    source_file=source_file,
                    artifact_type="permissions",
                )

        # Query permissions
        cursor.execute("SELECT * FROM moz_perms")

        for row in cursor:
            # Handle origin/host (modern vs legacy schema)
            if has_origin:
                origin = row["origin"] or ""
            elif has_host:
                origin = row["host"] or ""
            else:
                origin = ""

            # Permission type
            perm_type = row["type"] if "type" in columns else "unknown"
            found_permission_types.add(perm_type)

            # Permission value
            perm_value = row["permission"] if "permission" in columns else 0
            found_permission_values.add(perm_value)

            permission_value_str = get_permission_value_name(perm_value)
            normalized_type = normalize_permission_type(perm_type)

            # Expiration
            expire_type = row["expireType"] if "expireType" in columns else 0
            expire_time = row["expireTime"] if "expireTime" in columns else 0

            expires_at_utc = None
            if expire_time and expire_time > 0:
                try:
                    dt = prtime_to_datetime(expire_time)
                    expires_at_utc = dt.isoformat() if dt else None
                except (ValueError, TypeError, OSError, OverflowError) as e:
                    LOGGER.debug("Failed to parse expire_time %s: %s", expire_time, e)

            # Modification time
            granted_at_utc = None
            if has_modification_time:
                mod_time = row["modificationTime"]
                if mod_time and mod_time > 0:
                    try:
                        dt = prtime_to_datetime(mod_time)
                        granted_at_utc = dt.isoformat() if dt else None
                    except (ValueError, TypeError, OSError, OverflowError) as e:
                        LOGGER.debug("Failed to parse modificationTime %s: %s", mod_time, e)

            record = {
                "browser": browser,
                "profile": profile,
                "origin": origin,
                "permission_type": normalized_type,
                "permission_value": permission_value_str,
                "raw_type": perm_type,
                "raw_value": perm_value,
                "granted_at_utc": granted_at_utc,
                "expires_at_utc": expires_at_utc,
                "expires_type": get_expire_type_name(expire_type),
                "run_id": run_id,
                "source_path": source_file,
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry.get("logical_path", ""),
                "forensic_path": file_entry.get("forensic_path"),
            }
            records.append(record)

        # Track unknown permission values
        if warning_collector:
            from extractors._shared.extraction_warnings import track_unknown_values
            track_unknown_values(
                warning_collector=warning_collector,
                known_mapping=FIREFOX_PERMISSION_VALUES,
                found_values=found_permission_values,
                value_name="permission_value",
                source_file=source_file,
                artifact_type="permissions",
            )
            track_unknown_values(
                warning_collector=warning_collector,
                known_mapping=FIREFOX_PERMISSION_TYPE_MAP,
                found_values=found_permission_types,
                value_name="permission_type",
                source_file=source_file,
                artifact_type="permissions",
            )

    except Exception as e:
        LOGGER.error("Error parsing permissions.sqlite: %s", e, exc_info=True)
        if warning_collector:
            warning_collector.add_file_corrupt(
                filename=str(file_path),
                error=str(e),
                artifact_type="permissions",
            )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return records


# =============================================================================
# content-prefs.sqlite Parsing
# =============================================================================

def parse_content_prefs_file(
    file_path: Path,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Firefox content-prefs.sqlite and return preference records.

    content-prefs.sqlite stores site-specific preferences like:
    - browser.zoom.siteSpecific (zoom level per site)
    - media.autoplay.default (autoplay policy)
    - permissions.default.* (default permission states)

    These are stored as permission records with appropriate type normalization.

    Args:
        file_path: Path to extracted content-prefs.sqlite file
        file_entry: Manifest entry with browser, profile, logical_path, etc.
        run_id: Current extraction run ID
        discovered_by: Extractor identification string
        warning_collector: Optional collector for schema warnings

    Returns:
        List of permission record dicts ready for database insertion
    """
    if not file_path.exists():
        LOGGER.warning("Content-prefs file not found: %s", file_path)
        return []

    browser = file_entry.get("browser", "firefox")
    profile = file_entry.get("profile", "default")
    source_file = file_entry.get("logical_path", str(file_path))

    try:
        conn = sqlite3.connect(f"file:{file_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except Exception as e:
        LOGGER.error("Failed to open content-prefs.sqlite: %s", e)
        if warning_collector:
            warning_collector.add_file_corrupt(
                filename=str(file_path),
                error=str(e),
                artifact_type="content_prefs",
            )
        return []

    records = []
    found_setting_names: Set[str] = set()

    try:
        cursor = conn.cursor()

        # Schema discovery: detect unknown tables
        if warning_collector:
            from extractors._shared.extraction_warnings import discover_unknown_tables
            unknown_tables = discover_unknown_tables(
                conn, KNOWN_CONTENT_PREFS_TABLES, CONTENT_PREFS_TABLE_PATTERNS
            )
            for table_info in unknown_tables:
                warning_collector.add_unknown_table(
                    table_name=table_info["name"],
                    columns=table_info["columns"],
                    source_file=source_file,
                    artifact_type="content_prefs",
                )

        # Check if prefs table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='prefs'")
        if not cursor.fetchone():
            LOGGER.info("No prefs table found in %s", file_path)
            return []

        # Query with joins to get origin and setting name
        query = """
            SELECT
                g.name AS origin,
                s.name AS setting_name,
                p.value AS setting_value,
                p.timestamp AS timestamp
            FROM prefs p
            LEFT JOIN groups g ON p.groupID = g.id
            LEFT JOIN settings s ON p.settingID = s.id
        """

        cursor.execute(query)

        for row in cursor:
            origin = row["origin"] or ""
            setting_name = row["setting_name"] or "unknown"
            setting_value = row["setting_value"]
            timestamp = row["timestamp"]

            found_setting_names.add(setting_name)

            # Map content-prefs to permission types
            permission_type = normalize_content_pref_type(setting_name)
            permission_value = _normalize_content_pref_value(setting_name, setting_value)

            # Parse timestamp (PRTime - microseconds since Unix epoch)
            granted_at_utc = None
            if timestamp and timestamp > 0:
                try:
                    dt = prtime_to_datetime(timestamp)
                    granted_at_utc = dt.isoformat() if dt else None
                except (ValueError, TypeError, OSError, OverflowError) as e:
                    LOGGER.debug("Failed to parse timestamp %s: %s", timestamp, e)

            record = {
                "browser": browser,
                "profile": profile,
                "origin": origin,
                "permission_type": permission_type,
                "permission_value": permission_value,
                "raw_type": setting_name,
                "raw_value": setting_value,
                "granted_at_utc": granted_at_utc,
                "expires_at_utc": None,  # content-prefs don't expire
                "expires_type": "permanent",
                "run_id": run_id,
                "source_path": source_file,
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry.get("logical_path", ""),
                "forensic_path": file_entry.get("forensic_path"),
            }
            records.append(record)

        # Track unknown setting names
        if warning_collector:
            from extractors._shared.extraction_warnings import track_unknown_values
            track_unknown_values(
                warning_collector=warning_collector,
                known_mapping=CONTENT_PREF_TYPE_MAP,
                found_values=found_setting_names,
                value_name="content_pref_setting",
                source_file=source_file,
                artifact_type="content_prefs",
            )

    except Exception as e:
        LOGGER.error("Error parsing content-prefs.sqlite: %s", e, exc_info=True)
        if warning_collector:
            warning_collector.add_file_corrupt(
                filename=str(file_path),
                error=str(e),
                artifact_type="content_prefs",
            )
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return records


def _normalize_content_pref_value(setting_name: str, value: Any) -> str:
    """
    Normalize content-prefs values to human-readable strings.

    Args:
        setting_name: The preference setting name
        value: The raw preference value

    Returns:
        Human-readable string representation
    """
    if value is None:
        return "unknown"

    # Zoom levels (stored as float multiplier, e.g., 1.1 = 110%)
    if "zoom" in setting_name.lower():
        try:
            zoom_pct = float(value) * 100
            return f"{zoom_pct:.0f}%"
        except (ValueError, TypeError):
            return str(value)

    # Boolean-like integer values
    if isinstance(value, (int, float)):
        int_value = int(value)
        if int_value == 0:
            return "disabled"
        elif int_value == 1:
            return "enabled"
        elif int_value == 2:
            return "allow"
        elif int_value == 3:
            return "block"

    return str(value)
