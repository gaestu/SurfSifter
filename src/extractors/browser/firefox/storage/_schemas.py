"""
Firefox Browser Storage schema definitions and known patterns.

This module defines known database schemas, column names, and patterns
for Firefox storage artifacts. Used with ExtractionWarningCollector to
detect unknown or new data formats that may contain forensic value.

Schema Sources:
- webappsstore.sqlite: Legacy Local Storage (Firefox < 67)
- storage/default/*/ls/data.sqlite: Modern Local Storage (Firefox 67+)
- storage/default/*/idb/*.sqlite: IndexedDB

Initial implementation with schema warnings support
"""

from __future__ import annotations

from typing import Dict, Set


# =============================================================================
# webappsstore.sqlite - Legacy Local Storage Schema
# =============================================================================

# Known tables in webappsstore.sqlite
KNOWN_WEBAPPSSTORE_TABLES: Set[str] = {
    "webappsstore2",
}

# Known columns in webappsstore2 table
KNOWN_WEBAPPSSTORE_COLUMNS: Set[str] = {
    "scope",
    "key",
    "value",
    "secure",
    "owner",
}

# Table name patterns for filtering relevant unknown tables
WEBAPPSSTORE_TABLE_PATTERNS = ["webapps", "storage"]


# =============================================================================
# Modern Local Storage (data.sqlite) - Firefox 67+
# =============================================================================

# Known tables in modern LocalStorage data.sqlite
KNOWN_MODERN_LS_TABLES: Set[str] = {
    "database",
    "data",
}

# Known columns in 'database' table
KNOWN_MODERN_LS_DATABASE_COLUMNS: Set[str] = {
    "origin",
    "last_vacuum_time",
    "last_analyze_time",
    "last_vacuum_size",
    "usage",
}

# Known columns in 'data' table
KNOWN_MODERN_LS_DATA_COLUMNS: Set[str] = {
    "key",
    "utf16_length",
    "conversion_type",
    "compression_type",
    "last_access_time",
    "value",
}

# Combined for quick lookup
KNOWN_MODERN_LS_COLUMNS: Set[str] = (
    KNOWN_MODERN_LS_DATABASE_COLUMNS | KNOWN_MODERN_LS_DATA_COLUMNS
)

# Table patterns for modern LS
MODERN_LS_TABLE_PATTERNS = ["data", "database"]


# =============================================================================
# IndexedDB Schema (idb/*.sqlite)
# =============================================================================

# Known tables in Firefox IndexedDB sqlite files
KNOWN_INDEXEDDB_TABLES: Set[str] = {
    "database",
    "object_store",
    "object_data",
    "index_data",
    "unique_index_data",
    "object_store_index",
    "autoincrement",
}

# Known columns in 'database' table
KNOWN_INDEXEDDB_DATABASE_COLUMNS: Set[str] = {
    "id",
    "name",
    "origin",
    "version",
    "last_modified",
}

# Known columns in 'object_store' table
KNOWN_INDEXEDDB_OBJECT_STORE_COLUMNS: Set[str] = {
    "id",
    "auto_increment",
    "name",
    "key_path",
}

# Known columns in 'object_data' table
KNOWN_INDEXEDDB_OBJECT_DATA_COLUMNS: Set[str] = {
    "object_store_id",
    "key",
    "index_data_values",
    "file_ids",
    "data",
}

# Known columns in 'index_data' table
KNOWN_INDEXEDDB_INDEX_DATA_COLUMNS: Set[str] = {
    "index_id",
    "value",
    "object_data_key",
    "object_store_id",
    "key",
}

# Combined for detection
ALL_KNOWN_INDEXEDDB_COLUMNS: Set[str] = (
    KNOWN_INDEXEDDB_DATABASE_COLUMNS |
    KNOWN_INDEXEDDB_OBJECT_STORE_COLUMNS |
    KNOWN_INDEXEDDB_OBJECT_DATA_COLUMNS |
    KNOWN_INDEXEDDB_INDEX_DATA_COLUMNS
)

# Table patterns for IndexedDB
INDEXEDDB_TABLE_PATTERNS = ["object", "index", "database", "store", "data"]


# =============================================================================
# Compression and Conversion Types (Modern LocalStorage)
# =============================================================================

# LSValue::CompressionType enum from Firefox source
COMPRESSION_TYPES: Dict[int, str] = {
    0: "UNCOMPRESSED",
    1: "SNAPPY",
}

# LSValue::ConversionType enum from Firefox source
CONVERSION_TYPES: Dict[int, str] = {
    0: "UTF16_UTF16",  # No conversion
    1: "UTF16_UTF8",   # Converted from UTF16 to UTF8
}


def get_compression_type_name(type_code: int) -> str:
    """Get human-readable compression type name."""
    return COMPRESSION_TYPES.get(type_code, f"UNKNOWN_{type_code}")


def get_conversion_type_name(type_code: int) -> str:
    """Get human-readable conversion type name."""
    return CONVERSION_TYPES.get(type_code, f"UNKNOWN_{type_code}")


# =============================================================================
# Value Type Classifications
# =============================================================================

KNOWN_VALUE_TYPES: Set[str] = {
    "string",
    "json",
    "number",
    "boolean",
    "empty",
    "null",
    "blob",
    "array",
    "object",
    "unknown",
}


# =============================================================================
# Storage Type Identifiers
# =============================================================================

STORAGE_TYPE_LOCAL = "local_storage"
STORAGE_TYPE_INDEXEDDB = "indexeddb"
STORAGE_FORMAT_LEGACY = "legacy_webappsstore"
STORAGE_FORMAT_MODERN = "modern_ls"

ALL_STORAGE_TYPES: Set[str] = {
    STORAGE_TYPE_LOCAL,
    STORAGE_TYPE_INDEXEDDB,
}


# =============================================================================
# Artifact Types for Warnings
# =============================================================================

ARTIFACT_TYPE_LOCAL_STORAGE = "local_storage"
ARTIFACT_TYPE_INDEXEDDB = "indexeddb"


# =============================================================================
# Helper Functions
# =============================================================================

def is_known_webappsstore_table(table_name: str) -> bool:
    """Check if a table name is a known webappsstore table."""
    return table_name in KNOWN_WEBAPPSSTORE_TABLES


def is_known_modern_ls_table(table_name: str) -> bool:
    """Check if a table name is a known modern LocalStorage table."""
    return table_name in KNOWN_MODERN_LS_TABLES


def is_known_indexeddb_table(table_name: str) -> bool:
    """Check if a table name is a known IndexedDB table."""
    return table_name in KNOWN_INDEXEDDB_TABLES


def get_known_columns_for_table(table_name: str, storage_format: str) -> Set[str]:
    """
    Get known columns for a table based on storage format.

    Args:
        table_name: Name of the table
        storage_format: One of 'legacy_webappsstore', 'modern_ls', 'indexeddb'

    Returns:
        Set of known column names for the table
    """
    if storage_format == "legacy_webappsstore":
        if table_name == "webappsstore2":
            return KNOWN_WEBAPPSSTORE_COLUMNS
    elif storage_format == "modern_ls":
        if table_name == "database":
            return KNOWN_MODERN_LS_DATABASE_COLUMNS
        elif table_name == "data":
            return KNOWN_MODERN_LS_DATA_COLUMNS
    elif storage_format == "indexeddb":
        if table_name == "database":
            return KNOWN_INDEXEDDB_DATABASE_COLUMNS
        elif table_name == "object_store":
            return KNOWN_INDEXEDDB_OBJECT_STORE_COLUMNS
        elif table_name == "object_data":
            return KNOWN_INDEXEDDB_OBJECT_DATA_COLUMNS
        elif table_name in ("index_data", "unique_index_data"):
            return KNOWN_INDEXEDDB_INDEX_DATA_COLUMNS

    return set()
