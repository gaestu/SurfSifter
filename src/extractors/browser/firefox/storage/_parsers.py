"""
Firefox Browser Storage Parser Modules.

This module contains parser functions for each storage type extracted from
Firefox browsers. Each parser:

1. Checks if target tables exist
2. Discovers unknown columns for schema warnings
3. Parses records with proper encoding/decompression
4. Returns parsed records with forensic metadata

All parsers accept an optional ExtractionWarningCollector to report:
- Unknown columns in known tables
- Unknown enum/token values
- Parse errors

Initial implementation with schema warning support
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from ._schemas import (
    KNOWN_WEBAPPSSTORE_TABLES,
    KNOWN_WEBAPPSSTORE_COLUMNS,
    WEBAPPSSTORE_TABLE_PATTERNS,
    KNOWN_MODERN_LS_TABLES,
    KNOWN_MODERN_LS_DATA_COLUMNS,
    MODERN_LS_TABLE_PATTERNS,
    KNOWN_INDEXEDDB_TABLES,
    KNOWN_INDEXEDDB_OBJECT_DATA_COLUMNS,
    KNOWN_INDEXEDDB_INDEX_DATA_COLUMNS,
    INDEXEDDB_TABLE_PATTERNS,
    COMPRESSION_TYPES,
    CONVERSION_TYPES,
    get_known_columns_for_table,
)

# Optional Snappy decompression for modern Firefox LocalStorage
try:
    import snappy
    HAS_SNAPPY = True
except ImportError:
    HAS_SNAPPY = False

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.firefox.storage._parsers")


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


def get_all_tables(conn: sqlite3.Connection) -> List[str]:
    """Get all table names in database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [row[0] for row in cursor.fetchall()]


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
) -> None:
    """Track unknown enum values and add warnings."""
    if not warning_collector or not found_values:
        return

    from extractors._shared.extraction_warnings import track_unknown_values

    unknown = track_unknown_values(known_mapping, found_values)
    for value in unknown:
        warning_collector.add_unknown_enum_value(
            enum_name=value_name,
            value=value,
            source_file=source_file,
            artifact_type=artifact_type,
        )


def classify_value_type(value: str) -> str:
    """Classify the type of a storage value."""
    if not value:
        return "empty"

    value = value.strip()

    if value.startswith("{") or value.startswith("["):
        try:
            json.loads(value)
            return "json"
        except Exception:
            pass

    if value.lower() in ("true", "false"):
        return "boolean"

    try:
        float(value)
        return "number"
    except ValueError:
        pass

    return "string"


def decode_firefox_scope(scope: str) -> str:
    """
    Decode Firefox webappsstore scope to origin.

    Scope format: moc.elpmaxe.:https:443
    Firefox stores domain parts individually reversed, then in reverse order
    moc = com reversed, elpmaxe = example reversed
    So "moc.elpmaxe." decodes to "example.com"
    """
    if not scope:
        return ""

    parts = scope.split(":")
    if len(parts) >= 2:
        reversed_host = parts[0]
        scheme = parts[1] if len(parts) > 1 else "https"
        port = parts[2] if len(parts) > 2 else ""

        # Decode: each part is individually reversed, then order is reversed
        host_parts = [p[::-1] for p in reversed_host.split(".") if p]
        host = ".".join(reversed(host_parts))

        origin = f"{scheme}://{host}"
        if port and port not in ("80", "443", ""):
            origin += f":{port}"
        return origin

    return scope


def decompress_modern_ls_value(
    raw_value: bytes | str | None,
    compression_type: int,
    conversion_type: int
) -> str:
    """
    Decompress and decode modern Firefox LocalStorage value.

    Args:
        raw_value: The raw value from SQLite (BLOB or TEXT)
        compression_type: 0=uncompressed, 1=snappy
        conversion_type: 0=UTF16, 1=UTF8

    Returns:
        Decoded string value
    """
    if raw_value is None:
        return ""

    # Handle string values (stored as TEXT for empty strings)
    if isinstance(raw_value, str):
        return raw_value

    # Handle BLOB values
    if not isinstance(raw_value, bytes):
        return str(raw_value)

    data = raw_value

    # Decompress if Snappy-compressed
    if compression_type == 1:
        if HAS_SNAPPY:
            try:
                data = snappy.decompress(data)
            except Exception as e:
                LOGGER.debug("Snappy decompression failed: %s", e)
                return f"[snappy-compressed:{len(raw_value)}bytes]"
        else:
            return f"[snappy-compressed:{len(raw_value)}bytes,snappy-not-installed]"

    # Decode based on conversion type
    if conversion_type == 1:
        # UTF8 encoding
        try:
            return data.decode('utf-8', errors='replace')
        except Exception:
            return data.decode('latin-1', errors='replace')
    else:
        # UTF16 encoding
        try:
            return data.decode('utf-16-le', errors='replace')
        except Exception:
            return data.decode('utf-8', errors='replace')


def firefox_timestamp_to_utc(timestamp: int) -> Optional[str]:
    """
    Convert Firefox microsecond timestamp to ISO 8601 UTC string.

    Firefox uses microseconds since Unix epoch for last_access_time.
    """
    if not timestamp or timestamp <= 0:
        return None

    try:
        # Firefox uses microseconds
        seconds = timestamp / 1_000_000
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        return dt.isoformat()
    except (OSError, OverflowError, ValueError):
        return None


# =============================================================================
# Legacy WebAppsStore Parser
# =============================================================================

def parse_webappsstore(
    sqlite_path: Path,
    loc: Dict,
    run_id: str,
    evidence_id: int,
    excerpt_size: int,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict]:
    """
    Parse Firefox webappsstore.sqlite (legacy Local Storage).

    Args:
        sqlite_path: Path to extracted SQLite file
        loc: Location metadata dict
        run_id: Extraction run ID
        evidence_id: Evidence ID
        excerpt_size: Max value excerpt size
        warning_collector: Optional warning collector

    Returns:
        List of parsed storage records
    """
    records = []
    browser = loc.get("browser", "firefox")
    profile = loc.get("profile")
    source_file = loc.get("logical_path", str(sqlite_path))
    partition_index = loc.get("partition_index")

    try:
        conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Schema warnings: discover unknown tables
        if warning_collector:
            from extractors._shared.extraction_warnings import discover_unknown_tables

            unknown_tables = discover_unknown_tables(
                conn, KNOWN_WEBAPPSSTORE_TABLES, WEBAPPSSTORE_TABLE_PATTERNS
            )
            for table_info in unknown_tables:
                warning_collector.add_unknown_table(
                    table_name=table_info["name"],
                    columns=table_info["columns"],
                    source_file=source_file,
                    artifact_type="local_storage",
                )

        if not table_exists(conn, "webappsstore2"):
            LOGGER.warning("webappsstore2 table not found: %s", sqlite_path)
            conn.close()
            return records

        # Discover unknown columns
        discover_and_warn_unknown_columns(
            conn, "webappsstore2", KNOWN_WEBAPPSSTORE_COLUMNS,
            source_file, "local_storage", warning_collector
        )

        cursor.execute("SELECT scope, key, value FROM webappsstore2")
        for row in cursor:
            scope = row["scope"] or ""
            origin = decode_firefox_scope(scope)
            value = row["value"] or ""

            records.append({
                "run_id": run_id,
                "browser": browser,
                "profile": profile,
                "origin": origin,
                "key": row["key"] or "",
                "value": value[:excerpt_size] if len(value) > excerpt_size else value,
                "value_type": classify_value_type(value),
                "value_size": len(value.encode('utf-8', errors='replace')),
                "source_path": source_file,
                "partition_index": partition_index,
                "last_access_utc": None,  # Legacy format doesn't have this
                "notes": None,
            })

        conn.close()
    except Exception as e:
        LOGGER.warning("Failed to parse webappsstore: %s", e)
        if warning_collector:
            warning_collector.add_file_corrupt(
                filename=str(sqlite_path),
                error=str(e),
                artifact_type="local_storage",
            )

    return records


# =============================================================================
# Modern LocalStorage Parser (Firefox 67+)
# =============================================================================

def parse_modern_localstorage(
    sqlite_path: Path,
    loc: Dict,
    run_id: str,
    evidence_id: int,
    excerpt_size: int,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict]:
    """
    Parse modern Firefox LocalStorage (data.sqlite).

    Firefox 67+ stores LocalStorage per-origin at:
    storage/default/{origin}/ls/data.sqlite

    Schema:
    - database: origin, usage, last_vacuum_time, last_analyze_time, last_vacuum_size
    - data: key, utf16_length, conversion_type, compression_type, last_access_time, value

    Args:
        sqlite_path: Path to extracted SQLite file
        loc: Location metadata dict
        run_id: Extraction run ID
        evidence_id: Evidence ID
        excerpt_size: Max value excerpt size
        warning_collector: Optional warning collector

    Returns:
        List of parsed storage records
    """
    records = []
    browser = loc.get("browser", "firefox")
    profile = loc.get("profile")
    origin = loc.get("origin", "")
    source_file = loc.get("logical_path", str(sqlite_path))
    partition_index = loc.get("partition_index")

    # Track found compression/conversion types for warnings
    found_compression_types: Set[int] = set()
    found_conversion_types: Set[int] = set()

    try:
        conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Schema warnings: discover unknown tables
        if warning_collector:
            from extractors._shared.extraction_warnings import discover_unknown_tables

            unknown_tables = discover_unknown_tables(
                conn, KNOWN_MODERN_LS_TABLES, MODERN_LS_TABLE_PATTERNS
            )
            for table_info in unknown_tables:
                warning_collector.add_unknown_table(
                    table_name=table_info["name"],
                    columns=table_info["columns"],
                    source_file=source_file,
                    artifact_type="local_storage",
                )

        # Try to get origin from database table if not already set
        if not origin:
            try:
                cursor.execute("SELECT origin FROM database LIMIT 1")
                db_row = cursor.fetchone()
                if db_row and db_row["origin"]:
                    origin = db_row["origin"]
            except Exception:
                pass

        # Check if data table exists
        if not table_exists(conn, "data"):
            LOGGER.warning("Modern LS file missing 'data' table: %s", sqlite_path)
            conn.close()
            return records

        # Discover unknown columns
        discover_and_warn_unknown_columns(
            conn, "data", KNOWN_MODERN_LS_DATA_COLUMNS,
            source_file, "local_storage", warning_collector
        )

        cursor.execute("""
            SELECT key, value, utf16_length, conversion_type, compression_type, last_access_time
            FROM data
        """)

        for row in cursor:
            key = row["key"] or ""
            raw_value = row["value"]
            utf16_length = row["utf16_length"] or 0
            conversion_type = row["conversion_type"] or 0
            compression_type = row["compression_type"] or 0
            last_access_time = row["last_access_time"] or 0

            # Track for warnings
            found_compression_types.add(compression_type)
            found_conversion_types.add(conversion_type)

            # Decompress value if needed
            value = decompress_modern_ls_value(raw_value, compression_type, conversion_type)

            # Convert timestamp
            last_access_utc = firefox_timestamp_to_utc(last_access_time)

            # Truncate for excerpt
            if len(value) > excerpt_size:
                value = value[:excerpt_size]

            records.append({
                "run_id": run_id,
                "browser": browser,
                "profile": profile,
                "origin": origin,
                "key": key,
                "value": value,
                "value_type": classify_value_type(value),
                "value_size": utf16_length * 2 if conversion_type == 1 else len(value.encode('utf-8', errors='replace')),
                "source_path": source_file,
                "partition_index": partition_index,
                "last_access_utc": last_access_utc,
                "notes": f"compression={compression_type}" if compression_type else None,
            })

        conn.close()

        # Warn about unknown compression/conversion types
        track_and_warn_unknown_values(
            COMPRESSION_TYPES, found_compression_types,
            "COMPRESSION_TYPE", source_file, "local_storage", warning_collector
        )
        track_and_warn_unknown_values(
            CONVERSION_TYPES, found_conversion_types,
            "CONVERSION_TYPE", source_file, "local_storage", warning_collector
        )

    except Exception as e:
        LOGGER.warning("Failed to parse modern LocalStorage: %s", e, exc_info=True)
        if warning_collector:
            warning_collector.add_file_corrupt(
                filename=str(sqlite_path),
                error=str(e),
                artifact_type="local_storage",
            )

    return records


# =============================================================================
# IndexedDB Parser
# =============================================================================

def parse_indexeddb_sqlite(
    sqlite_path: Path,
    loc: Dict,
    run_id: str,
    evidence_id: int,
    excerpt_size: int,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Tuple[Dict, List[Dict]]]:
    """
    Parse Firefox IndexedDB SQLite file with proper metadata tables.

    Firefox IndexedDB schema (idb/*.sqlite):
    - database: id, name, origin, version, last_modified, etc.
    - object_store: id, name, key_path, auto_increment
    - object_data: object_store_id, key, data
    - index_data: index_id, key, value, object_data_key (now parsed!)

    Args:
        sqlite_path: Path to extracted SQLite file
        loc: Location metadata dict
        run_id: Extraction run ID
        evidence_id: Evidence ID
        excerpt_size: Max value excerpt size
        warning_collector: Optional warning collector

    Returns:
        List of (db_record, entries_list) tuples
    """
    results = []
    browser = loc.get("browser", "firefox")
    profile = loc.get("profile")
    origin = loc.get("origin", "")
    source_file = loc.get("logical_path", str(sqlite_path))
    partition_index = loc.get("partition_index")

    try:
        conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get table list
        tables = get_all_tables(conn)

        # Schema warnings: discover unknown tables
        if warning_collector:
            from extractors._shared.extraction_warnings import discover_unknown_tables

            unknown_tables = discover_unknown_tables(
                conn, KNOWN_INDEXEDDB_TABLES, INDEXEDDB_TABLE_PATTERNS
            )
            for table_info in unknown_tables:
                warning_collector.add_unknown_table(
                    table_name=table_info["name"],
                    columns=table_info["columns"],
                    source_file=source_file,
                    artifact_type="indexeddb",
                )

        # Discover unknown columns in known tables
        if "object_data" in tables:
            discover_and_warn_unknown_columns(
                conn, "object_data", KNOWN_INDEXEDDB_OBJECT_DATA_COLUMNS,
                source_file, "indexeddb", warning_collector
            )
        if "index_data" in tables:
            discover_and_warn_unknown_columns(
                conn, "index_data", KNOWN_INDEXEDDB_INDEX_DATA_COLUMNS,
                source_file, "indexeddb", warning_collector
            )

        # Try to get real database metadata from 'database' table
        db_name = sqlite_path.stem
        db_version = None
        if "database" in tables:
            try:
                cursor.execute("SELECT name, version FROM database LIMIT 1")
                db_row = cursor.fetchone()
                if db_row:
                    db_name = db_row["name"] or db_name
                    db_version = db_row["version"]
            except Exception:
                pass

        # Get real object store names from 'object_store' table
        object_store_map = {}  # id -> name
        if "object_store" in tables:
            try:
                cursor.execute("SELECT id, name FROM object_store")
                for row in cursor:
                    object_store_map[row["id"]] = row["name"]
            except Exception:
                pass

        db_record = {
            "run_id": run_id,
            "browser": browser,
            "profile": profile,
            "origin": origin,
            "database_name": db_name,
            "version": db_version,
            "object_store_count": len(object_store_map) if object_store_map else len([t for t in tables if t.startswith("object_data")]),
            "source_path": source_file,
            "partition_index": partition_index,
        }

        entries = []

        # Parse object_data table (modern schema with object_store_id)
        if "object_data" in tables:
            try:
                cursor.execute("SELECT object_store_id, key, data FROM object_data")
                for row in cursor:
                    store_id = row["object_store_id"]
                    store_name = object_store_map.get(store_id, f"object_store_{store_id}")
                    key = row["key"]
                    data = row["data"]

                    # Data is typically BLOB, convert to string excerpt
                    if isinstance(data, bytes):
                        value_str = data[:excerpt_size].decode('utf-8', errors='replace')
                    else:
                        value_str = str(data) if data else ""

                    entry_record = {
                        "run_id": run_id,
                        "object_store": store_name,
                        "key": str(key) if key else "",
                        "value": value_str[:excerpt_size] if len(value_str) > excerpt_size else value_str,
                        "value_type": classify_value_type(value_str),
                        "value_size": len(value_str.encode('utf-8', errors='replace')),
                        "source": "object_data",
                    }
                    entries.append(entry_record)
            except Exception as e:
                LOGGER.debug("Failed to parse object_data table: %s", e)

        # Parse index_data table (new!)
        if "index_data" in tables:
            try:
                cursor.execute("SELECT index_id, key, value, object_data_key FROM index_data")
                for row in cursor:
                    index_id = row["index_id"]
                    key = row["key"]
                    value = row["value"]
                    object_data_key = row["object_data_key"]

                    # Convert value to string
                    if isinstance(value, bytes):
                        value_str = value[:excerpt_size].decode('utf-8', errors='replace')
                    else:
                        value_str = str(value) if value else ""

                    entry_record = {
                        "run_id": run_id,
                        "object_store": f"index_{index_id}",
                        "key": str(key) if key else "",
                        "value": value_str[:excerpt_size] if len(value_str) > excerpt_size else value_str,
                        "value_type": classify_value_type(value_str),
                        "value_size": len(value_str.encode('utf-8', errors='replace')),
                        "source": "index_data",
                        "object_data_key": str(object_data_key) if object_data_key else None,
                    }
                    entries.append(entry_record)
            except Exception as e:
                LOGGER.debug("Failed to parse index_data table: %s", e)

        # Fallback: Parse old-style object_data_* tables (if no modern tables)
        if not entries:
            for table in tables:
                if table.startswith("object_data"):
                    try:
                        cursor.execute(f"SELECT key, data FROM {table}")
                        for row in cursor:
                            key = row["key"]
                            data = row["data"]

                            if isinstance(data, bytes):
                                value_str = data[:excerpt_size].decode('utf-8', errors='replace')
                            else:
                                value_str = str(data) if data else ""

                            entry_record = {
                                "run_id": run_id,
                                "object_store": table,
                                "key": str(key) if key else "",
                                "value": value_str[:excerpt_size] if len(value_str) > excerpt_size else value_str,
                                "value_type": classify_value_type(value_str),
                                "value_size": len(value_str.encode('utf-8', errors='replace')),
                                "source": "legacy_table",
                            }
                            entries.append(entry_record)
                    except Exception as e:
                        LOGGER.debug("Failed to parse table %s: %s", table, e)

        results.append((db_record, entries))
        conn.close()

    except Exception as e:
        LOGGER.warning("Failed to parse IndexedDB: %s", e)
        if warning_collector:
            warning_collector.add_file_corrupt(
                filename=str(sqlite_path),
                error=str(e),
                artifact_type="indexeddb",
            )

    return results
