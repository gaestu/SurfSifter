"""
Safari Browser Storage schema definitions and known patterns.

Defines the expected table structures for Safari LocalStorage
and WebKit IndexedDB SQLite files, used by the schema-warning
system to flag unknown columns or tables.

Safari LocalStorage:
    Per-origin SQLite files (*.localstorage) with ``ItemTable``.

Safari (WebKit) IndexedDB:
    Per-database SQLite files with WebKit-specific tables:
    ``Records``, ``ObjectStoreInfo``, ``IndexInfo``, ``IndexRecords``,
    ``KeyGenerators``, ``DatabaseInfo``.
"""

from __future__ import annotations

from typing import Dict, Set


# ============================================================================
# Safari LocalStorage Schema (*.localstorage SQLite files)
# ============================================================================

# Known tables in Safari .localstorage files
KNOWN_LOCALSTORAGE_TABLES: Set[str] = {
    "ItemTable",
}

# Known columns in ItemTable
KNOWN_LOCALSTORAGE_COLUMNS: Set[str] = {
    "key",
    "value",
}

# Table name patterns for unknown-table discovery
LOCALSTORAGE_TABLE_PATTERNS = ["item", "storage"]


# ============================================================================
# Safari (WebKit) IndexedDB Schema
# ============================================================================

# Known tables in WebKit IndexedDB SQLite files
KNOWN_INDEXEDDB_TABLES: Set[str] = {
    "Records",
    "ObjectStoreInfo",
    "IndexInfo",
    "IndexRecords",
    "KeyGenerators",
    "DatabaseInfo",
    "BlobRecords",
    "BlobFiles",
}

# Known columns per table
KNOWN_INDEXEDDB_RECORDS_COLUMNS: Set[str] = {
    "objectStoreID",
    "key",
    "value",
    "recordPaddingSize",
}

KNOWN_INDEXEDDB_OBJECTSTOREINFO_COLUMNS: Set[str] = {
    "id",
    "name",
    "keyPath",
    "autoIncrement",
    "maxIndexID",
}

KNOWN_INDEXEDDB_INDEXINFO_COLUMNS: Set[str] = {
    "id",
    "name",
    "objectStoreID",
    "keyPath",
    "unique",
    "multiEntry",
}

KNOWN_INDEXEDDB_INDEXRECORDS_COLUMNS: Set[str] = {
    "indexID",
    "key",
    "value",
    "objectStoreID",
}

KNOWN_INDEXEDDB_KEYGENERATORS_COLUMNS: Set[str] = {
    "objectStoreID",
    "currentNumber",
}

KNOWN_INDEXEDDB_DATABASEINFO_COLUMNS: Set[str] = {
    "key",
    "value",
}

ALL_KNOWN_INDEXEDDB_COLUMNS: Set[str] = (
    KNOWN_INDEXEDDB_RECORDS_COLUMNS
    | KNOWN_INDEXEDDB_OBJECTSTOREINFO_COLUMNS
    | KNOWN_INDEXEDDB_INDEXINFO_COLUMNS
    | KNOWN_INDEXEDDB_INDEXRECORDS_COLUMNS
    | KNOWN_INDEXEDDB_KEYGENERATORS_COLUMNS
    | KNOWN_INDEXEDDB_DATABASEINFO_COLUMNS
)

# Table name patterns for unknown-table discovery
INDEXEDDB_TABLE_PATTERNS = [
    "Records",
    "ObjectStore",
    "Index",
    "KeyGenerator",
    "Database",
    "Blob",
]


# ============================================================================
# Helper functions
# ============================================================================

def get_known_columns_for_table(table_name: str) -> Set[str]:
    """
    Return the set of known columns for a given table name.

    Args:
        table_name: SQLite table name (case-sensitive for WebKit tables)

    Returns:
        Set of known column names, or empty set if table is unknown.
    """
    mapping: Dict[str, Set[str]] = {
        "ItemTable": KNOWN_LOCALSTORAGE_COLUMNS,
        "Records": KNOWN_INDEXEDDB_RECORDS_COLUMNS,
        "ObjectStoreInfo": KNOWN_INDEXEDDB_OBJECTSTOREINFO_COLUMNS,
        "IndexInfo": KNOWN_INDEXEDDB_INDEXINFO_COLUMNS,
        "IndexRecords": KNOWN_INDEXEDDB_INDEXRECORDS_COLUMNS,
        "KeyGenerators": KNOWN_INDEXEDDB_KEYGENERATORS_COLUMNS,
        "DatabaseInfo": KNOWN_INDEXEDDB_DATABASEINFO_COLUMNS,
    }
    return mapping.get(table_name, set())
