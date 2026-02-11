"""
Chromium Autofill Parser Modules

This module contains parser functions for each artifact type extracted from
Chromium Web Data and Login Data databases. Each parser:

1. Checks if the target table exists
2. Discovers unknown columns for schema warnings
3. Parses records with proper timestamp conversion
4. Returns parsed records and collected warnings

All parsers accept an optional ExtractionWarningCollector to report:
- Unknown columns in known tables
- Unknown enum/token values
- Parse errors

Initial implementation with schema warning support
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

from ...._shared.timestamps import webkit_to_datetime, unix_to_datetime
from ._schemas import (
    # Token type mapping
    TOKEN_TYPES,
    get_token_type_name,
    # Known column sets
    KNOWN_AUTOFILL_COLUMNS,
    KNOWN_AUTOFILL_PROFILES_COLUMNS,
    KNOWN_CREDIT_CARDS_COLUMNS,
    KNOWN_LOCAL_IBANS_COLUMNS,
    KNOWN_MASKED_IBANS_COLUMNS,
    KNOWN_LOGINS_COLUMNS,
    KNOWN_KEYWORDS_COLUMNS,
    KNOWN_TOKEN_TABLE_COLUMNS,
    KNOWN_EDGE_AUTOFILL_FIELD_VALUES_COLUMNS,
    KNOWN_EDGE_FIELD_CLIENT_INFO_COLUMNS,
    KNOWN_EDGE_AUTOFILL_BLOCK_LIST_COLUMNS,
    # Edge enum mappings
    EDGE_BLOCK_VALUE_TYPES,
    EDGE_ATTRIBUTE_FLAGS,
    get_block_value_type_name,
    get_attribute_flag_name,
    # Table lists
    ADDRESS_TOKEN_TABLES,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.chromium.autofill._parsers")


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
    columns = get_table_columns(conn, table_name)

    if warning_collector:
        # Import here to avoid circular imports
        from extractors._shared.extraction_warnings import discover_unknown_columns

        unknown = discover_unknown_columns(conn, table_name, known_columns)
        for col_info in unknown:
            warning_collector.add_unknown_column(
                table_name=table_name,
                column_name=col_info["name"],
                column_type=col_info["type"],
                source_file=source_file,
                artifact_type=artifact_type,
            )

    return columns


def track_and_warn_unknown_values(
    known_mapping: Dict[int, str],
    found_values: Set[int],
    value_name: str,
    source_file: str,
    artifact_type: str,
    warning_collector: Optional["ExtractionWarningCollector"],
    table_name: Optional[str] = None,
) -> None:
    """Track unknown enum values and add warnings."""
    if not warning_collector or not found_values:
        return

    from extractors._shared.extraction_warnings import track_unknown_values

    unknown = track_unknown_values(known_mapping, found_values)
    for value in unknown:
        if value_name == "TOKEN_TYPE":
            warning_collector.add_unknown_token_type(
                token_type=value,
                source_file=source_file,
                artifact_type=artifact_type,
                table_name=table_name,
            )
        else:
            warning_collector.add_unknown_enum_value(
                enum_name=value_name,
                value=value,
                source_file=source_file,
                artifact_type=artifact_type,
                context={"table": table_name} if table_name else None,
            )


# =============================================================================
# Autofill Table Parser
# =============================================================================

def parse_autofill_table(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Chromium autofill table (name-value pairs from web forms).

    Args:
        conn: SQLite connection to Web Data
        browser: Browser identifier
        file_entry: File metadata dict
        run_id: Extraction run ID
        discovered_by: Provenance string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of parsed autofill records
    """
    if not table_exists(conn, "autofill"):
        return []

    source_file = file_entry.get("logical_path", "")

    # Get columns and warn about unknowns
    columns = discover_and_warn_unknown_columns(
        conn, "autofill", KNOWN_AUTOFILL_COLUMNS,
        source_file, "autofill", warning_collector
    )

    # Build dynamic SELECT
    select_cols = ["name", "value"]
    if "date_created" in columns:
        select_cols.append("date_created")
    if "date_last_used" in columns:
        select_cols.append("date_last_used")
    if "count" in columns:
        select_cols.append("count")

    cursor = conn.cursor()
    cursor.execute(f"SELECT {', '.join(select_cols)} FROM autofill")

    records = []
    for row in cursor:
        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "name": row["name"],
            "value": row["value"],
            "run_id": run_id,
            "source_path": file_entry["logical_path"],
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry["logical_path"],
            "forensic_path": file_entry.get("forensic_path"),
        }

        if "date_created" in columns and row["date_created"]:
            dt = webkit_to_datetime(row["date_created"])
            record["date_created_utc"] = dt.isoformat() if dt else None
        if "date_last_used" in columns and row["date_last_used"]:
            dt = webkit_to_datetime(row["date_last_used"])
            record["date_last_used_utc"] = dt.isoformat() if dt else None
        if "count" in columns:
            record["count"] = row["count"]

        records.append(record)

    return records


# =============================================================================
# Autofill Profiles Table Parser
# =============================================================================

def parse_autofill_profiles_table(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Chromium autofill_profiles table (legacy address storage).

    Args:
        conn: SQLite connection to Web Data
        browser: Browser identifier
        file_entry: File metadata dict
        run_id: Extraction run ID
        discovered_by: Provenance string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of parsed profile records
    """
    if not table_exists(conn, "autofill_profiles"):
        return []

    source_file = file_entry.get("logical_path", "")

    # Get columns and warn about unknowns
    columns = discover_and_warn_unknown_columns(
        conn, "autofill_profiles", KNOWN_AUTOFILL_PROFILES_COLUMNS,
        source_file, "autofill_profiles", warning_collector
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM autofill_profiles")

    records = []
    for row in cursor:
        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "guid": row["guid"] if "guid" in columns else None,
            "run_id": run_id,
            "source_path": file_entry["logical_path"],
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry["logical_path"],
            "forensic_path": file_entry.get("forensic_path"),
        }

        # Copy known columns
        for col in ["full_name", "company_name", "street_address", "city",
                    "state", "zipcode", "country_code", "use_count"]:
            if col in columns:
                record[col] = row[col]

        # Convert timestamps
        if "date_modified" in columns and row["date_modified"]:
            dt = webkit_to_datetime(row["date_modified"])
            record["date_modified_utc"] = dt.isoformat() if dt else None
        if "use_date" in columns and row["use_date"]:
            dt = webkit_to_datetime(row["use_date"])
            record["use_date_utc"] = dt.isoformat() if dt else None

        records.append(record)

    return records


# =============================================================================
# Credit Cards Table Parser
# =============================================================================

def parse_credit_cards_table(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Chromium credit_cards table.

    Args:
        conn: SQLite connection to Web Data
        browser: Browser identifier
        file_entry: File metadata dict
        run_id: Extraction run ID
        discovered_by: Provenance string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of parsed credit card records
    """
    if not table_exists(conn, "credit_cards"):
        return []

    source_file = file_entry.get("logical_path", "")

    # Get columns and warn about unknowns
    columns = discover_and_warn_unknown_columns(
        conn, "credit_cards", KNOWN_CREDIT_CARDS_COLUMNS,
        source_file, "credit_cards", warning_collector
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM credit_cards")

    records = []
    for row in cursor:
        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "guid": row["guid"] if "guid" in columns else None,
            "run_id": run_id,
            "source_path": file_entry["logical_path"],
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry["logical_path"],
            "forensic_path": file_entry.get("forensic_path"),
        }

        # Copy known columns
        for col in ["name_on_card", "expiration_month", "expiration_year",
                    "card_number_encrypted", "nickname", "use_count"]:
            if col in columns:
                record[col] = row[col]

        # Convert timestamps
        if "date_modified" in columns and row["date_modified"]:
            dt = webkit_to_datetime(row["date_modified"])
            record["date_modified_utc"] = dt.isoformat() if dt else None
        if "use_date" in columns and row["use_date"]:
            dt = webkit_to_datetime(row["use_date"])
            record["use_date_utc"] = dt.isoformat() if dt else None

        records.append(record)

    return records


# =============================================================================
# IBAN Tables Parser
# =============================================================================

def parse_iban_table(
    conn: sqlite3.Connection,
    table_name: str,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Chromium IBAN table (local_ibans or masked_ibans).

    Args:
        conn: SQLite connection to Web Data
        table_name: IBAN table name
        browser: Browser identifier
        file_entry: File metadata dict
        run_id: Extraction run ID
        discovered_by: Provenance string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of parsed IBAN records
    """
    if not table_exists(conn, table_name):
        return []

    source_file = file_entry.get("logical_path", "")
    known_columns = (
        KNOWN_LOCAL_IBANS_COLUMNS if table_name == "local_ibans" else KNOWN_MASKED_IBANS_COLUMNS
    )

    columns = discover_and_warn_unknown_columns(
        conn, table_name, known_columns, source_file, "autofill_ibans", warning_collector
    )

    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")

    records = []
    for row in cursor:
        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "source_table": table_name,
            "run_id": run_id,
            "source_path": file_entry["logical_path"],
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry["logical_path"],
            "forensic_path": file_entry.get("forensic_path"),
        }

        # Common identity/display fields (present in one or both tables)
        for col in [
            "guid",
            "instrument_id",
            "nickname",
            "value",
            "prefix",
            "suffix",
            "length",
            "use_count",
        ]:
            if col in columns:
                record[col] = row[col]

        # Encrypted value (local_ibans)
        if "value_encrypted" in columns and row["value_encrypted"]:
            record["value_encrypted"] = row["value_encrypted"]

        # Timestamps (WebKit)
        if "use_date" in columns and row["use_date"]:
            dt = webkit_to_datetime(row["use_date"])
            record["use_date_utc"] = dt.isoformat() if dt else None
        if "date_modified" in columns and row["date_modified"]:
            dt = webkit_to_datetime(row["date_modified"])
            record["date_modified_utc"] = dt.isoformat() if dt else None

        records.append(record)

    if records:
        LOGGER.debug("Parsed %d IBAN records from %s", len(records), table_name)

    return records


def parse_iban_tables(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse all Chromium IBAN tables.

    Returns:
        Combined list of parsed IBAN records from local and masked tables.
    """
    all_records: List[Dict[str, Any]] = []
    for table_name in ("local_ibans", "masked_ibans"):
        all_records.extend(
            parse_iban_table(
                conn,
                table_name,
                browser,
                file_entry,
                run_id,
                discovered_by,
                warning_collector=warning_collector,
            )
        )
    return all_records


# =============================================================================
# Keywords (Search Engines) Table Parser
# =============================================================================

def parse_keywords_table(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Chromium keywords table (search engines).

    Args:
        conn: SQLite connection to Web Data
        browser: Browser identifier
        file_entry: File metadata dict
        run_id: Extraction run ID
        discovered_by: Provenance string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of parsed search engine records
    """
    if not table_exists(conn, "keywords"):
        return []

    source_file = file_entry.get("logical_path", "")

    # Get columns and warn about unknowns
    columns = discover_and_warn_unknown_columns(
        conn, "keywords", KNOWN_KEYWORDS_COLUMNS,
        source_file, "search_engines", warning_collector
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM keywords")

    records = []
    for row in cursor:
        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "short_name": row["short_name"] if "short_name" in columns else None,
            "keyword": row["keyword"] if "keyword" in columns else None,
            "url": row["url"] if "url" in columns else None,
            "favicon_url": row["favicon_url"] if "favicon_url" in columns else None,
            "suggest_url": row["suggest_url"] if "suggest_url" in columns else None,
            "prepopulate_id": row["prepopulate_id"] if "prepopulate_id" in columns else None,
            "run_id": run_id,
            "source_path": file_entry["logical_path"],
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry["logical_path"],
            "forensic_path": file_entry.get("forensic_path"),
        }

        # Usage statistics
        if "usage_count" in columns:
            record["usage_count"] = row["usage_count"]

        # Timestamps
        if "date_created" in columns and row["date_created"]:
            dt = webkit_to_datetime(row["date_created"])
            record["date_created_utc"] = dt.isoformat() if dt else None
        if "last_modified" in columns and row["last_modified"]:
            dt = webkit_to_datetime(row["last_modified"])
            record["last_modified_utc"] = dt.isoformat() if dt else None
        if "last_visited" in columns and row["last_visited"]:
            dt = webkit_to_datetime(row["last_visited"])
            record["last_visited_utc"] = dt.isoformat() if dt else None

        # Status flags
        if "is_active" in columns:
            record["is_active"] = row["is_active"]

        # Additional URL templates
        for col in ["new_tab_url", "image_url", "search_url_post_params"]:
            if col in columns:
                record[col] = row[col]
        if "suggestions_url_post_params" in columns:
            record["suggest_url_post_params"] = row["suggestions_url_post_params"]

        records.append(record)

    # Mark likely default based on usage (heuristic)
    if records:
        max_usage = max((r.get("usage_count") or 0) for r in records)
        for r in records:
            if (r.get("usage_count") or 0) == max_usage and max_usage > 0:
                r["is_default"] = 1

    return records


# =============================================================================
# Token Table Parser (Modern Chromium Addresses)
# =============================================================================

def _load_parent_table_metadata(
    conn: sqlite3.Connection,
    parent_table: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Load parent-table usage metadata keyed by guid.

    Returns:
        Dict[guid, {"parent_use_count", "parent_use_date_utc", "parent_date_modified_utc"}]
    """
    if not table_exists(conn, parent_table):
        return {}

    parent_columns = get_table_columns(conn, parent_table)
    if "guid" not in parent_columns:
        return {}

    select_cols = ["guid"]
    if "use_count" in parent_columns:
        select_cols.append("use_count")
    if "use_date" in parent_columns:
        select_cols.append("use_date")
    if "date_modified" in parent_columns:
        select_cols.append("date_modified")

    cursor = conn.cursor()
    cursor.execute(f"SELECT {', '.join(select_cols)} FROM {parent_table}")

    metadata: Dict[str, Dict[str, Any]] = {}
    for row in cursor:
        guid = row["guid"] if "guid" in row.keys() else None
        if not guid:
            continue

        parent_info: Dict[str, Any] = {}
        if "use_count" in row.keys():
            parent_info["parent_use_count"] = row["use_count"]
        if "use_date" in row.keys() and row["use_date"]:
            dt = webkit_to_datetime(row["use_date"])
            parent_info["parent_use_date_utc"] = dt.isoformat() if dt else None
        if "date_modified" in row.keys() and row["date_modified"]:
            dt = webkit_to_datetime(row["date_modified"])
            parent_info["parent_date_modified_utc"] = dt.isoformat() if dt else None

        metadata[guid] = parent_info

    return metadata


def parse_token_table(
    conn: sqlite3.Connection,
    parent_table: str,
    token_table: str,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse a token-based address table (e.g., address_type_tokens).

    Args:
        conn: SQLite connection to Web Data
        parent_table: Parent table name (for reference)
        token_table: Token table to parse
        browser: Browser identifier
        file_entry: File metadata dict
        run_id: Extraction run ID
        discovered_by: Provenance string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of parsed token records
    """
    if not table_exists(conn, token_table):
        return []

    source_file = file_entry.get("logical_path", "")

    # Get columns and warn about unknowns
    columns = discover_and_warn_unknown_columns(
        conn, token_table, KNOWN_TOKEN_TABLE_COLUMNS,
        source_file, "autofill_profile_tokens", warning_collector
    )
    parent_metadata = _load_parent_table_metadata(conn, parent_table)

    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {token_table}")

    records = []
    found_token_types: Set[int] = set()

    for row in cursor:
        guid = row["guid"] if "guid" in columns else None
        token_type = row["type"] if "type" in columns else 0
        token_value = row["value"] if "value" in columns else None

        if not guid:
            continue

        found_token_types.add(token_type)

        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "guid": guid,
            "token_type": token_type,
            "token_type_name": get_token_type_name(token_type),
            "token_value": token_value,
            "source_table": token_table,
            "parent_table": parent_table,
            "run_id": run_id,
            "source_path": file_entry["logical_path"],
            "discovered_by": discovered_by,
        }
        parent_info = parent_metadata.get(guid, {})
        record["parent_use_count"] = parent_info.get("parent_use_count")
        record["parent_use_date_utc"] = parent_info.get("parent_use_date_utc")
        record["parent_date_modified_utc"] = parent_info.get("parent_date_modified_utc")

        records.append(record)

    # Track unknown token types
    track_and_warn_unknown_values(
        TOKEN_TYPES, found_token_types, "TOKEN_TYPE",
        source_file, "autofill_profile_tokens", warning_collector,
        table_name=token_table,
    )

    if records:
        LOGGER.debug("Parsed %d tokens from %s", len(records), token_table)

    return records


def parse_all_token_tables(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse all token-based address tables for a browser.

    Iterates over ADDRESS_TOKEN_TABLES and parses each applicable one.

    Args:
        conn: SQLite connection to Web Data
        browser: Browser identifier
        file_entry: File metadata dict
        run_id: Extraction run ID
        discovered_by: Provenance string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        Combined list of token records from all tables
    """
    all_records = []

    for parent_table, token_table, browser_filter in ADDRESS_TOKEN_TABLES:
        # Skip if table is for a different browser
        if browser_filter and browser != browser_filter:
            continue

        records = parse_token_table(
            conn, parent_table, token_table,
            browser, file_entry, run_id, discovered_by,
            warning_collector=warning_collector,
        )
        all_records.extend(records)

    return all_records


# =============================================================================
# Login Data Parser
# =============================================================================

def parse_logins_table(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Chromium logins table (saved credentials).

    Also loads related security metadata from insecure_credentials,
    breached, and password_notes tables.

    Args:
        conn: SQLite connection to Login Data
        browser: Browser identifier
        file_entry: File metadata dict
        run_id: Extraction run ID
        discovered_by: Provenance string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of parsed credential records
    """
    if not table_exists(conn, "logins"):
        return []

    source_file = file_entry.get("logical_path", "")

    # Get columns and warn about unknowns
    columns = discover_and_warn_unknown_columns(
        conn, "logins", KNOWN_LOGINS_COLUMNS,
        source_file, "credentials", warning_collector
    )

    # Load security metadata from related tables
    insecure_urls = _load_insecure_credentials(conn)
    breached_urls = _load_breached_credentials(conn)
    password_notes = _load_password_notes(conn)

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM logins")

    records = []
    for row in cursor:
        origin_url = row["origin_url"] if "origin_url" in columns else ""
        signon_realm = row["signon_realm"] if "signon_realm" in columns else None

        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "origin_url": origin_url,
            "action_url": row["action_url"] if "action_url" in columns else None,
            "username_element": row["username_element"] if "username_element" in columns else None,
            "username_value": row["username_value"] if "username_value" in columns else None,
            "password_element": row["password_element"] if "password_element" in columns else None,
            "signon_realm": signon_realm,
            "run_id": run_id,
            "source_path": file_entry["logical_path"],
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry["logical_path"],
            "forensic_path": file_entry.get("forensic_path"),
        }

        # Encrypted password
        if "password_value" in columns and row["password_value"]:
            record["password_value_encrypted"] = row["password_value"]

        # Timestamps
        if "date_created" in columns and row["date_created"]:
            dt = webkit_to_datetime(row["date_created"])
            record["date_created_utc"] = dt.isoformat() if dt else None
        if "date_last_used" in columns and row["date_last_used"]:
            dt = webkit_to_datetime(row["date_last_used"])
            record["date_last_used_utc"] = dt.isoformat() if dt else None
        if "date_password_modified" in columns and row["date_password_modified"]:
            dt = webkit_to_datetime(row["date_password_modified"])
            record["date_password_modified_utc"] = dt.isoformat() if dt else None

        # Usage/metadata
        if "times_used" in columns:
            record["times_used"] = row["times_used"]
        if "blacklisted_by_user" in columns:
            record["blacklisted_by_user"] = row["blacklisted_by_user"]

        # Security metadata
        record["is_insecure"] = 1 if signon_realm in insecure_urls or origin_url in insecure_urls else 0
        record["is_breached"] = 1 if signon_realm in breached_urls or origin_url in breached_urls else 0
        if signon_realm in password_notes:
            record["password_notes"] = password_notes[signon_realm]
        elif origin_url in password_notes:
            record["password_notes"] = password_notes[origin_url]

        records.append(record)

    return records


def _load_insecure_credentials(conn: sqlite3.Connection) -> Set[str]:
    """Load URLs flagged as insecure from insecure_credentials table."""
    try:
        if not table_exists(conn, "insecure_credentials"):
            return set()

        cursor = conn.cursor()
        cursor.execute("SELECT signon_realm FROM insecure_credentials")
        return {row[0] for row in cursor.fetchall() if row[0]}
    except Exception as e:
        LOGGER.debug("Could not load insecure_credentials: %s", e)
        return set()


def _load_breached_credentials(conn: sqlite3.Connection) -> Set[str]:
    """Load URLs from breached table (known in data breaches)."""
    try:
        if not table_exists(conn, "breached"):
            return set()

        cursor = conn.cursor()
        cursor.execute("SELECT signon_realm FROM breached")
        return {row[0] for row in cursor.fetchall() if row[0]}
    except Exception as e:
        LOGGER.debug("Could not load breached: %s", e)
        return set()


def _load_password_notes(conn: sqlite3.Connection) -> Dict[str, str]:
    """Load password notes from password_notes table."""
    try:
        if not table_exists(conn, "password_notes"):
            return {}

        cursor = conn.cursor()
        cursor.execute("SELECT signon_realm, value FROM password_notes")
        return {row[0]: row[1] for row in cursor.fetchall() if row[0] and row[1]}
    except Exception as e:
        LOGGER.debug("Could not load password_notes: %s", e)
        return {}


# =============================================================================
# Edge-Specific Parsers
# =============================================================================

def parse_edge_autofill_field_values(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Edge-specific autofill_edge_field_values table.

    Edge uses this table instead of/alongside the standard autofill table.
    JOINs with autofill_edge_field_client_info for field name resolution.

    Args:
        conn: SQLite connection to Web Data
        browser: Browser identifier
        file_entry: File metadata dict
        run_id: Extraction run ID
        discovered_by: Provenance string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of parsed autofill records
    """
    if not table_exists(conn, "autofill_edge_field_values"):
        return []

    source_file = file_entry.get("logical_path", "")

    # Get columns and warn about unknowns for main table
    discover_and_warn_unknown_columns(
        conn, "autofill_edge_field_values", KNOWN_EDGE_AUTOFILL_FIELD_VALUES_COLUMNS,
        source_file, "autofill", warning_collector
    )

    # Check if field_client_info table exists (for field name resolution)
    has_client_info = table_exists(conn, "autofill_edge_field_client_info")

    if has_client_info:
        # Warn about unknown columns in client info table too
        discover_and_warn_unknown_columns(
            conn, "autofill_edge_field_client_info", KNOWN_EDGE_FIELD_CLIENT_INFO_COLUMNS,
            source_file, "autofill", warning_collector
        )

        query = """
            SELECT
                fv.field_id,
                fv.value,
                fv.count,
                fv.date_created,
                fv.date_last_used,
                ci.label,
                ci.domain_value
            FROM autofill_edge_field_values fv
            LEFT JOIN autofill_edge_field_client_info ci
                ON fv.field_id = ci.field_id
        """
        LOGGER.debug("Using JOIN with autofill_edge_field_client_info for field name resolution")
    else:
        query = "SELECT * FROM autofill_edge_field_values"
        LOGGER.debug("autofill_edge_field_client_info not found, using raw field_id")

    cursor = conn.cursor()
    cursor.execute(query)

    records = []
    for row in cursor:
        field_id = str(row["field_id"]) if row["field_id"] else None

        # Get human-readable label if available from JOIN
        if has_client_info:
            label = row["label"] if row["label"] else None
            domain = row["domain_value"] if row["domain_value"] else None
        else:
            label = None
            domain = None

        # Use label as name if available, otherwise fall back to field_id hash
        display_name = label or field_id or "unknown"

        # Build notes with source info and domain context
        notes_parts = ["source:autofill_edge_field_values"]
        if domain:
            notes_parts.append(f"domain:{domain}")
        if field_id and label:
            notes_parts.append(f"field_id_hash:{field_id}")

        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "name": display_name,
            "value": row["value"] if row["value"] is not None else None,
            "field_id_hash": field_id,
            "run_id": run_id,
            "source_path": file_entry["logical_path"],
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry["logical_path"],
            "forensic_path": file_entry.get("forensic_path"),
            "notes": "; ".join(notes_parts),
        }

        # Edge uses Unix timestamps (seconds), NOT WebKit timestamps
        if row["date_created"]:
            dt = unix_to_datetime(row["date_created"])
            record["date_created_utc"] = dt.isoformat() if dt else None
        if row["date_last_used"]:
            dt = unix_to_datetime(row["date_last_used"])
            record["date_last_used_utc"] = dt.isoformat() if dt else None
        if row["count"] is not None:
            record["count"] = row["count"]

        records.append(record)

    if records:
        resolved_count = sum(1 for r in records if r["name"] != r.get("field_id_hash"))
        LOGGER.debug(
            "Parsed %d Edge autofill_edge_field_values entries (%d with resolved field names)",
            len(records), resolved_count
        )

    return records


def parse_edge_autofill_block_list(
    conn: sqlite3.Connection,
    browser: str,
    file_entry: Dict[str, Any],
    run_id: str,
    discovered_by: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse Edge-specific autofill_edge_block_list table.

    This table contains sites/domains where the user has disabled autofill.

    Args:
        conn: SQLite connection to Web Data
        browser: Browser identifier
        file_entry: File metadata dict
        run_id: Extraction run ID
        discovered_by: Provenance string
        warning_collector: Optional warning collector for schema discovery

    Returns:
        List of parsed block list records
    """
    if not table_exists(conn, "autofill_edge_block_list"):
        return []

    source_file = file_entry.get("logical_path", "")

    # Get columns and warn about unknowns
    discover_and_warn_unknown_columns(
        conn, "autofill_edge_block_list", KNOWN_EDGE_AUTOFILL_BLOCK_LIST_COLUMNS,
        source_file, "autofill_block_list", warning_collector
    )

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM autofill_edge_block_list")

    records = []
    found_block_value_types: Set[int] = set()
    found_attribute_flags: Set[int] = set()

    for row in cursor:
        block_value_type = row["block_value_type"]
        attribute_flag = row["attribute_flag"]

        # Track enum values for unknown detection
        if block_value_type is not None:
            found_block_value_types.add(block_value_type)
        if attribute_flag is not None:
            found_attribute_flags.add(attribute_flag)

        # Parse date_created - Edge uses Unix timestamp string or integer
        date_created = row["date_created"]
        date_created_utc = None
        if date_created:
            try:
                if isinstance(date_created, str) and date_created.isdigit():
                    date_created = int(date_created)
                if isinstance(date_created, (int, float)):
                    dt = unix_to_datetime(date_created)
                    date_created_utc = dt.isoformat() if dt else None
            except (ValueError, TypeError):
                date_created_utc = str(date_created)

        # Parse date_modified - integer timestamp
        date_modified = row["date_modified"]
        date_modified_utc = None
        if date_modified and date_modified > 0:
            dt = unix_to_datetime(date_modified)
            date_modified_utc = dt.isoformat() if dt else None

        record = {
            "browser": browser,
            "profile": file_entry.get("profile"),
            "guid": row["guid"],
            "block_value": row["block_value"],
            "block_value_type": block_value_type,
            "block_value_type_name": get_block_value_type_name(block_value_type) if block_value_type is not None else None,
            "attribute_flag": attribute_flag,
            "attribute_flag_name": get_attribute_flag_name(attribute_flag) if attribute_flag is not None else None,
            "meta_data": row["meta_data"],
            "device_model": row["device_model"],
            "date_created_utc": date_created_utc,
            "date_modified_utc": date_modified_utc,
            "run_id": run_id,
            "source_path": file_entry["logical_path"],
            "discovered_by": discovered_by,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry["logical_path"],
            "forensic_path": file_entry.get("forensic_path"),
            "notes": "source:autofill_edge_block_list",
        }
        records.append(record)

    # Track unknown enum values
    track_and_warn_unknown_values(
        EDGE_BLOCK_VALUE_TYPES, found_block_value_types, "block_value_type",
        source_file, "autofill_block_list", warning_collector,
        table_name="autofill_edge_block_list",
    )
    track_and_warn_unknown_values(
        EDGE_ATTRIBUTE_FLAGS, found_attribute_flags, "attribute_flag",
        source_file, "autofill_block_list", warning_collector,
        table_name="autofill_edge_block_list",
    )

    if records:
        LOGGER.debug("Parsed %d Edge autofill_edge_block_list entries", len(records))

    return records
