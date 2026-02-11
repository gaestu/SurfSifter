"""
Firefox Autofill Parser Modules.

This module contains parser functions for each artifact type extracted from
Firefox autofill databases and JSON files. Each parser:

1. Checks if the target table/structure exists
2. Discovers unknown columns/keys for schema warnings
3. Parses records with proper timestamp conversion
4. Returns parsed records ready for database insertion

All parsers accept an optional ExtractionWarningCollector to report:
- Unknown columns in known tables
- Unknown JSON keys in logins.json
- Parse errors

Initial implementation with schema warning support
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

from ...._shared.timestamps import prtime_to_datetime, unix_milliseconds_to_datetime
from ._schemas import (
    KNOWN_MOZ_FORMHISTORY_COLUMNS,
    KNOWN_MOZ_DELETED_FORMHISTORY_COLUMNS,
    KNOWN_MOZ_SOURCES_COLUMNS,
    KNOWN_MOZ_HISTORY_TO_SOURCES_COLUMNS,
    KNOWN_LOGINS_JSON_ROOT_KEYS,
    KNOWN_LOGINS_JSON_ENTRY_KEYS,
    KNOWN_MOZ_LOGINS_COLUMNS,
    KNOWN_FORMHISTORY_TABLES,
    KNOWN_SIGNONS_TABLES,
    FORMHISTORY_TABLE_PATTERNS,
    get_fieldname_column,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.firefox.autofill._parsers")


# =============================================================================
# Helper Functions
# =============================================================================

def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the database."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    """Get column names for a table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    return {row[1] for row in cursor.fetchall()}


def get_column_info(conn: sqlite3.Connection, table_name: str) -> Dict[str, str]:
    """Get column names and types for a table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    return {row[1]: row[2] for row in cursor.fetchall()}


def discover_and_warn_unknown_columns(
    conn: sqlite3.Connection,
    table_name: str,
    known_columns: Set[str],
    source_file: str,
    artifact_type: str,
    warning_collector: Optional["ExtractionWarningCollector"],
) -> Set[str]:
    """
    Get table columns and warn about any unknown ones.

    Returns the set of actual columns in the table.
    """
    column_info = get_column_info(conn, table_name)
    columns = set(column_info.keys())

    if warning_collector:
        unknown_columns = columns - known_columns
        for col_name in unknown_columns:
            warning_collector.add_unknown_column(
                table_name=table_name,
                column_name=col_name,
                column_type=column_info.get(col_name, "unknown"),
                source_file=source_file,
                artifact_type=artifact_type,
            )

    return columns


def discover_unknown_tables(
    conn: sqlite3.Connection,
    known_tables: Set[str],
    patterns: List[str],
    source_file: str,
    artifact_type: str,
    warning_collector: Optional["ExtractionWarningCollector"],
) -> None:
    """
    Discover and warn about unknown tables.

    Args:
        conn: SQLite connection
        known_tables: Set of known table names
        patterns: List of patterns that suggest relevant tables
        source_file: Source file path for warning context
        artifact_type: Artifact type for warning context
        warning_collector: Optional warning collector
    """
    if not warning_collector:
        return

    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    all_tables = {row[0] for row in cursor.fetchall()}

    # Filter to unknown tables
    unknown_tables = all_tables - known_tables

    # Filter to relevant tables (matching patterns)
    relevant_unknowns = set()
    for table in unknown_tables:
        table_lower = table.lower()
        # Skip SQLite system tables
        if table_lower.startswith("sqlite_"):
            continue
        # Check if table matches any pattern
        for pattern in patterns:
            if pattern.lower() in table_lower:
                relevant_unknowns.add(table)
                break

    # Add warnings for relevant unknown tables
    for table_name in relevant_unknowns:
        columns = list(get_table_columns(conn, table_name))
        warning_collector.add_unknown_table(
            table_name=table_name,
            columns=columns,
            source_file=source_file,
            artifact_type=artifact_type,
        )


# =============================================================================
# formhistory.sqlite Parsers
# =============================================================================

def _first_existing_column(columns: Set[str], candidates: List[str]) -> Optional[str]:
    """Return the first matching column name from candidates."""
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _load_formhistory_guid_lookup(conn: sqlite3.Connection) -> Dict[str, Dict[str, Any]]:
    """Build guid -> {formhistory_id, original_fieldname, original_value} map."""
    if not table_exists(conn, "moz_formhistory"):
        return {}

    columns = get_table_columns(conn, "moz_formhistory")
    fieldname_col = get_fieldname_column(columns)
    if not fieldname_col or "guid" not in columns:
        return {}

    select_cols = ["id", "guid", fieldname_col]
    if "value" in columns:
        select_cols.append("value")

    cursor = conn.cursor()
    cursor.execute(f"SELECT {', '.join(select_cols)} FROM moz_formhistory WHERE guid IS NOT NULL")

    guid_map: Dict[str, Dict[str, Any]] = {}
    for row in cursor:
        guid = row["guid"] if "guid" in row.keys() else None
        if not guid:
            continue
        guid_map[guid] = {
            "formhistory_id": row["id"] if "id" in row.keys() else None,
            "original_fieldname": row[fieldname_col] if fieldname_col in row.keys() else None,
            "original_value": row["value"] if "value" in row.keys() else None,
        }

    return guid_map


def _load_deleted_source_correlations(
    conn: sqlite3.Connection,
    source_file: str,
    warning_collector: Optional["ExtractionWarningCollector"],
) -> tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """
    Build source correlation maps for deleted form history.

    Returns:
        (sources_by_history_id, sources_by_guid)
    """
    if not table_exists(conn, "moz_sources") or not table_exists(conn, "moz_history_to_sources"):
        return {}, {}

    source_columns = discover_and_warn_unknown_columns(
        conn, "moz_sources", KNOWN_MOZ_SOURCES_COLUMNS,
        source_file, "deleted_form_history", warning_collector,
    )
    rel_columns = discover_and_warn_unknown_columns(
        conn, "moz_history_to_sources", KNOWN_MOZ_HISTORY_TO_SOURCES_COLUMNS,
        source_file, "deleted_form_history", warning_collector,
    )

    source_id_col = _first_existing_column(source_columns, ["id", "source_id"])
    rel_source_col = _first_existing_column(rel_columns, ["source_id", "source", "sourceId"])
    history_col = _first_existing_column(rel_columns, ["history_id", "formhistory_id", "form_history_id", "entry_id"])
    guid_col = _first_existing_column(rel_columns, ["guid", "form_guid"])
    if not source_id_col or not rel_source_col:
        return {}, {}

    source_name_col = _first_existing_column(source_columns, ["source_name", "name"])
    source_url_col = _first_existing_column(source_columns, ["source_url", "url", "origin"])
    source_field_col = _first_existing_column(source_columns, ["fieldname", "field_name"])
    source_value_col = _first_existing_column(source_columns, ["value", "source_value"])

    select_source_cols = [source_id_col]
    for col in [source_name_col, source_url_col, source_field_col, source_value_col, "guid"]:
        if col and col in source_columns and col not in select_source_cols:
            select_source_cols.append(col)

    source_cursor = conn.cursor()
    source_cursor.execute(f"SELECT {', '.join(select_source_cols)} FROM moz_sources")

    source_descriptions: Dict[str, str] = {}
    for row in source_cursor:
        source_id = row[source_id_col]
        if source_id is None:
            continue

        parts: List[str] = []
        if source_name_col and row[source_name_col]:
            parts.append(str(row[source_name_col]))
        if source_url_col and row[source_url_col]:
            parts.append(str(row[source_url_col]))
        if source_field_col and row[source_field_col]:
            parts.append(f"field={row[source_field_col]}")
        if source_value_col and row[source_value_col]:
            parts.append(f"value={row[source_value_col]}")
        if not parts and "guid" in row.keys() and row["guid"]:
            parts.append(f"guid={row['guid']}")

        source_descriptions[str(source_id)] = " | ".join(parts) if parts else f"source_id={source_id}"

    rel_select_cols = [rel_source_col]
    if history_col:
        rel_select_cols.append(history_col)
    if guid_col:
        rel_select_cols.append(guid_col)

    rel_cursor = conn.cursor()
    rel_cursor.execute(f"SELECT {', '.join(rel_select_cols)} FROM moz_history_to_sources")

    sources_by_history_id: Dict[str, List[str]] = {}
    sources_by_guid: Dict[str, List[str]] = {}
    for row in rel_cursor:
        source_id = row[rel_source_col]
        if source_id is None:
            continue

        source_desc = source_descriptions.get(str(source_id), f"source_id={source_id}")
        if history_col and row[history_col] is not None:
            history_key = str(row[history_col])
            sources_by_history_id.setdefault(history_key, []).append(source_desc)
        if guid_col and row[guid_col]:
            guid_key = str(row[guid_col])
            sources_by_guid.setdefault(guid_key, []).append(source_desc)

    for mapping in (sources_by_history_id, sources_by_guid):
        for key, values in list(mapping.items()):
            mapping[key] = list(dict.fromkeys(values))

    return sources_by_history_id, sources_by_guid


def parse_moz_formhistory(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Firefox moz_formhistory table.

    Args:
        conn: SQLite connection to formhistory.sqlite
        browser: Browser identifier (firefox, firefox_esr, tor)
        file_entry: File entry dict with metadata
        run_id: Current run ID
        discovered_by: Discovery attribution string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of autofill record dicts ready for database insertion
    """
    source_file = file_entry.get("logical_path", "formhistory.sqlite")

    if not table_exists(conn, "moz_formhistory"):
        return []

    # Discover unknown tables in this database
    discover_unknown_tables(
        conn, KNOWN_FORMHISTORY_TABLES, FORMHISTORY_TABLE_PATTERNS,
        source_file, "autofill", warning_collector,
    )

    # Get columns and warn about unknown ones
    columns = discover_and_warn_unknown_columns(
        conn, "moz_formhistory", KNOWN_MOZ_FORMHISTORY_COLUMNS,
        source_file, "autofill", warning_collector,
    )

    # Determine which fieldname column to use
    fieldname_col = get_fieldname_column(columns)
    if not fieldname_col:
        LOGGER.warning("moz_formhistory has neither 'fieldname' nor 'name' column")
        return []

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM moz_formhistory")

    records = []
    for row in cursor:
        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "name": row[fieldname_col] if fieldname_col in row.keys() else "",
            "value": row["value"] if "value" in row.keys() else None,
            "run_id": run_id,
            "source_path": source_file,
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": source_file,
            "forensic_path": file_entry.get("forensic_path"),
        }

        # Firefox uses PRTime (microseconds since 1970)
        if "firstUsed" in row.keys() and row["firstUsed"]:
            dt = prtime_to_datetime(row["firstUsed"])
            record["date_created_utc"] = dt.isoformat() if dt else None
        if "lastUsed" in row.keys() and row["lastUsed"]:
            dt = prtime_to_datetime(row["lastUsed"])
            record["date_last_used_utc"] = dt.isoformat() if dt else None
        if "timesUsed" in row.keys():
            record["count"] = row["timesUsed"]

        # Extract guid for correlation (Firefox 4+)
        if "guid" in row.keys() and row["guid"]:
            record["notes"] = f"guid:{row['guid']}"

        records.append(record)

    return records


def parse_moz_deleted_formhistory(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Firefox moz_deleted_formhistory table.

    Firefox stores deleted form history entries with their GUID and deletion
    timestamp. Useful for forensic analysis to understand deleted data.

    Table structure (Firefox 44+):
        id: INTEGER PRIMARY KEY
        timeDeleted: INTEGER (PRTime - microseconds since 1970-01-01)
        guid: TEXT

    Args:
        conn: SQLite connection to formhistory.sqlite
        browser: Browser identifier
        file_entry: File entry dict with metadata
        run_id: Current run ID
        discovered_by: Discovery attribution string
        warning_collector: Optional warning collector

    Returns:
        List of deleted form history record dicts
    """
    source_file = file_entry.get("logical_path", "formhistory.sqlite")

    if not table_exists(conn, "moz_deleted_formhistory"):
        LOGGER.debug("No moz_deleted_formhistory table found in %s", source_file)
        return []

    # Get columns and warn about unknown ones
    columns = discover_and_warn_unknown_columns(
        conn, "moz_deleted_formhistory", KNOWN_MOZ_DELETED_FORMHISTORY_COLUMNS,
        source_file, "deleted_form_history", warning_collector,
    )
    formhistory_guid_map = _load_formhistory_guid_lookup(conn)
    sources_by_history_id, sources_by_guid = _load_deleted_source_correlations(
        conn, source_file, warning_collector
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM moz_deleted_formhistory")

    records = []
    for row in cursor:
        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "guid": row["guid"] if "guid" in row.keys() else None,
            "run_id": run_id,
            "source_path": source_file,
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": source_file,
            "forensic_path": file_entry.get("forensic_path"),
        }

        # Firefox uses PRTime (microseconds since 1970)
        if "timeDeleted" in row.keys() and row["timeDeleted"]:
            dt = prtime_to_datetime(row["timeDeleted"])
            record["time_deleted_utc"] = dt.isoformat() if dt else None

        guid = record.get("guid")
        if guid:
            correlation = formhistory_guid_map.get(guid)
            notes_parts: List[str] = []

            if correlation:
                if correlation.get("original_fieldname"):
                    record["original_fieldname"] = correlation.get("original_fieldname")
                if correlation.get("original_value") is not None:
                    record["original_value"] = correlation.get("original_value")
                notes_parts.append("correlated:moz_formhistory")

                formhistory_id = correlation.get("formhistory_id")
                if formhistory_id is not None:
                    source_matches = sources_by_history_id.get(str(formhistory_id), [])
                    if source_matches:
                        notes_parts.append(f"sources:{'; '.join(source_matches[:5])}")

            if not notes_parts:
                guid_source_matches = sources_by_guid.get(guid, [])
                if guid_source_matches:
                    notes_parts.append("correlated:moz_history_to_sources")
                    notes_parts.append(f"sources:{'; '.join(guid_source_matches[:5])}")

            if notes_parts:
                record["notes"] = " | ".join(notes_parts)

        records.append(record)

    return records


# =============================================================================
# logins.json Parser
# =============================================================================

def parse_logins_json(
    json_path: Path,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Firefox logins.json file.

    Args:
        json_path: Path to logins.json file
        browser: Browser identifier
        file_entry: File entry dict with metadata
        run_id: Current run ID
        discovered_by: Discovery attribution string
        warning_collector: Optional warning collector

    Returns:
        List of credential record dicts ready for database insertion
    """
    source_file = file_entry.get("logical_path", str(json_path))

    try:
        data = json.loads(json_path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as e:
        LOGGER.error("Failed to parse logins.json: %s", e)
        if warning_collector:
            warning_collector.add_json_parse_error(
                filename=source_file,
                error=str(e),
                artifact_type="credentials",
            )
        return []
    except Exception as e:
        LOGGER.error("Failed to read logins.json: %s", e)
        if warning_collector:
            warning_collector.add_file_corrupt(
                filename=source_file,
                error=str(e),
                artifact_type="credentials",
            )
        return []

    # Warn about unknown root-level keys
    if warning_collector and isinstance(data, dict):
        unknown_root_keys = set(data.keys()) - KNOWN_LOGINS_JSON_ROOT_KEYS
        for key in unknown_root_keys:
            warning_collector.add_json_unknown_key(
                key_path=key,
                source_file=source_file,
                artifact_type="credentials",
                sample_value=str(type(data[key]).__name__),
            )

    logins = data.get("logins", [])

    records = []
    seen_entry_keys: Set[str] = set()

    for login in logins:
        # Track unknown entry keys (do once after seeing all logins)
        if isinstance(login, dict):
            seen_entry_keys.update(login.keys())

        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "origin_url": login.get("hostname", ""),
            "action_url": login.get("formSubmitURL"),
            "username_value": login.get("username"),
            "signon_realm": login.get("httpRealm"),
            "run_id": run_id,
            "source_path": source_file,
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": source_file,
            "forensic_path": file_entry.get("forensic_path"),
        }

        # Form field element names (forensic value: shows which form was used)
        if login.get("usernameField"):
            record["username_element"] = login["usernameField"]
        if login.get("passwordField"):
            record["password_element"] = login["passwordField"]

        # Store encrypted credentials for forensic record
        # Both username and password are encrypted in Firefox
        if login.get("encryptedPassword"):
            record["password_value_encrypted"] = login["encryptedPassword"].encode('utf-8')

        # Build notes with additional metadata
        notes_parts = []
        if login.get("guid"):
            notes_parts.append(f"guid:{login['guid']}")
        if login.get("encType") is not None:
            notes_parts.append(f"encType:{login['encType']}")
        if login.get("encryptedUsername"):
            enc_user = login["encryptedUsername"]
            notes_parts.append(f"encryptedUsername:{enc_user[:50]}..." if len(enc_user) > 50 else f"encryptedUsername:{enc_user}")
        # Sync metadata (Firefox 57+)
        if login.get("syncCounter") is not None:
            notes_parts.append(f"syncCounter:{login['syncCounter']}")
        if login.get("everSynced") is not None:
            notes_parts.append(f"everSynced:{login['everSynced']}")
        if notes_parts:
            record["notes"] = "; ".join(notes_parts)

        # Firefox timestamps in milliseconds
        if login.get("timeCreated"):
            dt = unix_milliseconds_to_datetime(login["timeCreated"])
            record["date_created_utc"] = dt.isoformat() if dt else None
        if login.get("timeLastUsed"):
            dt = unix_milliseconds_to_datetime(login["timeLastUsed"])
            record["date_last_used_utc"] = dt.isoformat() if dt else None
        if login.get("timePasswordChanged"):
            dt = unix_milliseconds_to_datetime(login["timePasswordChanged"])
            record["date_password_modified_utc"] = dt.isoformat() if dt else None
        if login.get("timesUsed"):
            record["times_used"] = login["timesUsed"]

        records.append(record)

    # Warn about unknown entry keys
    if warning_collector and seen_entry_keys:
        unknown_entry_keys = seen_entry_keys - KNOWN_LOGINS_JSON_ENTRY_KEYS
        for key in unknown_entry_keys:
            warning_collector.add_json_unknown_key(
                key_path=f"logins[].{key}",
                source_file=source_file,
                artifact_type="credentials",
            )

    return records


# =============================================================================
# signons.sqlite Parser (Legacy Firefox < 32)
# =============================================================================

def parse_moz_logins_signons(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse legacy Firefox signons.sqlite database.

    Used in Firefox < 32 (2014) before migration to logins.json.
    Contains moz_logins table with encrypted credentials.

    Args:
        conn: SQLite connection to signons.sqlite
        browser: Browser identifier
        file_entry: File entry dict with metadata
        run_id: Current run ID
        discovered_by: Discovery attribution string
        warning_collector: Optional warning collector

    Returns:
        List of credential record dicts
    """
    source_file = file_entry.get("logical_path", "signons.sqlite")

    if not table_exists(conn, "moz_logins"):
        LOGGER.warning("moz_logins table not found in signons.sqlite")
        return []

    # Discover unknown tables
    discover_unknown_tables(
        conn, KNOWN_SIGNONS_TABLES, ["moz_", "login", "password"],
        source_file, "credentials", warning_collector,
    )

    # Get columns and warn about unknown ones
    columns = discover_and_warn_unknown_columns(
        conn, "moz_logins", KNOWN_MOZ_LOGINS_COLUMNS,
        source_file, "credentials", warning_collector,
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM moz_logins")

    records = []
    for row in cursor:
        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "origin_url": row["hostname"] if "hostname" in columns else "",
            "signon_realm": row["httpRealm"] if "httpRealm" in columns else None,
            "action_url": row["formSubmitURL"] if "formSubmitURL" in columns else None,
            "run_id": run_id,
            "source_path": source_file,
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": source_file,
            "forensic_path": file_entry.get("forensic_path"),
        }

        # Form field element names
        if "usernameField" in columns and row["usernameField"]:
            record["username_element"] = row["usernameField"]
        if "passwordField" in columns and row["passwordField"]:
            record["password_element"] = row["passwordField"]

        # Store encrypted credentials for forensic record
        if "encryptedUsername" in columns and row["encryptedUsername"]:
            # Username is also encrypted in signons.sqlite
            record["username_value"] = None  # Can't extract plaintext
        if "encryptedPassword" in columns and row["encryptedPassword"]:
            record["password_value_encrypted"] = row["encryptedPassword"].encode('utf-8')

        # Build notes with additional metadata
        notes_parts = ["source:signons.sqlite (legacy)"]
        if "guid" in columns and row["guid"]:
            notes_parts.append(f"guid:{row['guid']}")
        if "encType" in columns and row["encType"] is not None:
            notes_parts.append(f"encType:{row['encType']}")
        if "encryptedUsername" in columns and row["encryptedUsername"]:
            enc_user = row["encryptedUsername"]
            notes_parts.append(f"encryptedUsername:{enc_user[:50]}..." if len(enc_user) > 50 else f"encryptedUsername:{enc_user}")
        record["notes"] = "; ".join(notes_parts)

        # Legacy Firefox uses PRTime (microseconds since 1970)
        if "timeCreated" in columns and row["timeCreated"]:
            dt = prtime_to_datetime(row["timeCreated"])
            record["date_created_utc"] = dt.isoformat() if dt else None
        if "timeLastUsed" in columns and row["timeLastUsed"]:
            dt = prtime_to_datetime(row["timeLastUsed"])
            record["date_last_used_utc"] = dt.isoformat() if dt else None
        if "timePasswordChanged" in columns and row["timePasswordChanged"]:
            dt = prtime_to_datetime(row["timePasswordChanged"])
            record["date_password_modified_utc"] = dt.isoformat() if dt else None
        if "timesUsed" in columns:
            record["times_used"] = row["timesUsed"]

        records.append(record)

    return records
