"""
Firefox Autofill schema definitions and known data structures.

This module defines the known database schemas, JSON keys, and patterns
for Firefox autofill artifacts. Used by the extractor to:
1. Detect unknown tables/columns for schema warnings
2. Provide type mappings for enum fields

Schema Sources:
- formhistory.sqlite: moz_formhistory, moz_deleted_formhistory tables
- logins.json: Firefox 32+ credential storage (JSON)
- signons.sqlite: Legacy credential storage (Firefox < 32)
- key3.db/key4.db: NSS key stores (copied only, not parsed)

References:
- Firefox source: toolkit/components/satchel/FormHistoryRecord.idl
- Firefox source: toolkit/components/passwordmgr/LoginInfo.idl

Initial implementation with schema warning support
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set


# =============================================================================
# formhistory.sqlite Tables
# =============================================================================

# Tables we know about in formhistory.sqlite
KNOWN_FORMHISTORY_TABLES: Set[str] = {
    # Main form history table
    "moz_formhistory",
    # Deleted entries table (Firefox 44+)
    "moz_deleted_formhistory",
    # SQLite system tables (present in all SQLite databases)
    "sqlite_sequence",
    # Source attribution tables (Firefox 95+, )
    # These track where form data originated from
    "moz_sources",
    "moz_history_to_sources",
}

# Patterns to identify formhistory-related tables (for unknown table discovery)
FORMHISTORY_TABLE_PATTERNS: List[str] = [
    "formhistory",
    "form",
    "moz_",
]

# =============================================================================
# moz_formhistory Table Columns
# =============================================================================

# Known columns in moz_formhistory table
# Firefox has evolved this table - some columns are version-dependent
KNOWN_MOZ_FORMHISTORY_COLUMNS: Set[str] = {
    # Primary key
    "id",
    # Modern Firefox (v4+) uses "fieldname", older used "name"
    "fieldname",
    "name",  # Legacy
    # Value
    "value",
    # Timestamps (PRTime - microseconds since 1970)
    "firstUsed",
    "lastUsed",
    # Usage count
    "timesUsed",
    # GUID for sync (Firefox 4+)
    "guid",
}

# =============================================================================
# moz_deleted_formhistory Table Columns (Firefox 44+)
# =============================================================================

KNOWN_MOZ_DELETED_FORMHISTORY_COLUMNS: Set[str] = {
    "id",
    "timeDeleted",  # PRTime - microseconds since 1970
    "guid",
}

# Optional source-correlation tables (Firefox 95+, schema varies by version)
KNOWN_MOZ_SOURCES_COLUMNS: Set[str] = {
    "id",
    "guid",
    "source_id",
    "source_name",
    "source_url",
    "name",
    "url",
    "origin",
    "fieldname",
    "value",
    "type",
    "created_at",
    "updated_at",
}

KNOWN_MOZ_HISTORY_TO_SOURCES_COLUMNS: Set[str] = {
    "id",
    "guid",
    "form_guid",
    "history_id",
    "formhistory_id",
    "form_history_id",
    "entry_id",
    "source_id",
    "source",
    "sourceId",
    "created_at",
    "updated_at",
}

# =============================================================================
# logins.json Known Keys
# =============================================================================

# Top-level keys in logins.json
KNOWN_LOGINS_JSON_ROOT_KEYS: Set[str] = {
    "logins",
    "nextId",
    "potentiallyVulnerablePasswords",
    "dismissedBreachAlertsByLoginGUID",
    "version",
}

# Keys within each login entry
KNOWN_LOGINS_JSON_ENTRY_KEYS: Set[str] = {
    # Identity
    "id",
    "guid",
    # URLs
    "hostname",
    "formSubmitURL",
    "httpRealm",
    # Form field names
    "usernameField",
    "passwordField",
    # Credentials (encrypted)
    "encryptedUsername",
    "encryptedPassword",
    # Encryption type
    "encType",
    # Timestamps (milliseconds since epoch)
    "timeCreated",
    "timeLastUsed",
    "timePasswordChanged",
    # Usage count
    "timesUsed",
    # Sync fields (Firefox 57+)
    "syncCounter",
    "everSynced",
    # Origin (Firefox 70+)
    "origin",
}

# =============================================================================
# signons.sqlite Tables (Legacy, Firefox < 32)
# =============================================================================

KNOWN_SIGNONS_TABLES: Set[str] = {
    "moz_logins",
    "moz_disabledHosts",
    "sqlite_sequence",
}

# Known columns in moz_logins table (signons.sqlite)
KNOWN_MOZ_LOGINS_COLUMNS: Set[str] = {
    "id",
    "hostname",
    "httpRealm",
    "formSubmitURL",
    "usernameField",
    "passwordField",
    "encryptedUsername",
    "encryptedPassword",
    "guid",
    "encType",
    # PRTime timestamps
    "timeCreated",
    "timeLastUsed",
    "timePasswordChanged",
    "timesUsed",
}

# =============================================================================
# File Type Classification
# =============================================================================

# Map filename to file type identifier
AUTOFILL_FILE_TYPES: Dict[str, str] = {
    "formhistory.sqlite": "formhistory",
    "logins.json": "logins_json",
    "key4.db": "key4",
    "key3.db": "key3",
    "signons.sqlite": "signons",
}


def classify_autofill_file(path: str) -> str:
    """
    Classify autofill file type based on filename.

    Args:
        path: File path

    Returns:
        File type identifier (formhistory, logins_json, key4, key3, signons, unknown)
    """
    filename = path.split('/')[-1].lower()
    return AUTOFILL_FILE_TYPES.get(filename, "unknown")


# =============================================================================
# Helper Functions for Column Name Compatibility
# =============================================================================

def get_fieldname_column(columns: Set[str]) -> Optional[str]:
    """
    Get the correct fieldname column for the Firefox version.

    Modern Firefox uses 'fieldname', older versions used 'name'.

    Args:
        columns: Set of column names in the table

    Returns:
        The fieldname column to use, or None if neither exists
    """
    if "fieldname" in columns:
        return "fieldname"
    if "name" in columns:
        return "name"
    return None
