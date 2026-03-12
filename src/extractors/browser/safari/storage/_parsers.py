"""
Safari Browser Storage Parser Modules.

Parsers for Safari LocalStorage (.localstorage SQLite files) and
WebKit IndexedDB (per-origin SQLite files with WebKit-specific tables).

Each parser:
1. Opens the SQLite file in read-only mode
2. Discovers unknown tables/columns for schema warnings
3. Parses records with proper encoding
4. Returns standardized record dicts for database insertion

Safari LocalStorage:
    Per-origin SQLite files named ``{scheme}_{host}_{port}.localstorage``
    containing ``ItemTable (key TEXT, value BLOB)`` with UTF-16LE values.
    Identical format to pre-LevelDB Chromium LocalStorage.

Safari (WebKit) IndexedDB:
    Per-origin directories containing per-database SQLite files with
    WebKit-specific tables: ``Records``, ``ObjectStoreInfo``, ``IndexInfo``,
    ``IndexRecords``, ``KeyGenerators``, ``DatabaseInfo``.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from ._schemas import (
    KNOWN_LOCALSTORAGE_TABLES,
    KNOWN_LOCALSTORAGE_COLUMNS,
    LOCALSTORAGE_TABLE_PATTERNS,
    KNOWN_INDEXEDDB_TABLES,
    KNOWN_INDEXEDDB_RECORDS_COLUMNS,
    KNOWN_INDEXEDDB_OBJECTSTOREINFO_COLUMNS,
    KNOWN_INDEXEDDB_INDEXINFO_COLUMNS,
    KNOWN_INDEXEDDB_INDEXRECORDS_COLUMNS,
    INDEXEDDB_TABLE_PATTERNS,
    get_known_columns_for_table,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.safari.storage._parsers")


# =============================================================================
# Helper Functions
# =============================================================================


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the database."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cursor.fetchone() is not None


def _get_table_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    """Get column names for a table."""
    # Sanitize table name — PRAGMA doesn't support parameterised queries
    safe_name = table_name.replace('"', '""')
    cursor = conn.cursor()
    cursor.execute(f'PRAGMA table_info("{safe_name}")')
    return {row[1] for row in cursor.fetchall()}


def _get_all_tables(conn: sqlite3.Connection) -> List[str]:
    """Get all table names in database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [row[0] for row in cursor.fetchall()]


def _discover_and_warn_unknown_columns(
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
    columns = _get_table_columns(conn, table_name)
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


def _classify_value_type(value: str) -> str:
    """Classify the type of a storage value."""
    if not value:
        return "empty"
    value_stripped = value.strip()
    if value_stripped.startswith("{") or value_stripped.startswith("["):
        try:
            json.loads(value_stripped)
            return "json"
        except Exception:
            pass
    if value_stripped.lower() in ("true", "false"):
        return "boolean"
    try:
        float(value_stripped)
        return "number"
    except ValueError:
        pass
    return "string"


def parse_origin_from_localstorage_filename(filename: str) -> str:
    """
    Extract the origin URL from a Safari .localstorage filename.

    Safari uses the same naming convention as old Chromium LocalStorage:
        ``{scheme}_{host}_{port}.localstorage``

    Args:
        filename: Filename like ``https_example.com_0.localstorage``

    Returns:
        Origin URL like ``https://example.com``

    Examples:
        >>> parse_origin_from_localstorage_filename("https_example.net_0.localstorage")
        'https://example.net'
        >>> parse_origin_from_localstorage_filename("http_localhost_8080.localstorage")
        'http://localhost:8080'
    """
    stem = filename.replace(".localstorage", "")
    parts = stem.split("_")
    if len(parts) < 3:
        return stem  # Unparseable — return as-is
    scheme = parts[0]
    port = parts[-1]
    host = "_".join(parts[1:-1])
    if port == "0":
        return f"{scheme}://{host}"
    return f"{scheme}://{host}:{port}"


def parse_origin_from_indexeddb_path(path: str) -> str:
    """
    Extract origin from a WebKit IndexedDB directory path.

    IndexedDB paths contain the origin encoded in the directory name:
        ``.../v1/https_example.com_0/DatabaseName/...``

    The origin directory follows the same convention as LocalStorage filenames.

    Args:
        path: Full path to an IndexedDB SQLite file or directory.

    Returns:
        Origin URL or empty string if not determinable.
    """
    path_obj = Path(path)
    parts = path_obj.parts

    # Look for a "v1" or "v*" directory — the next directory is the origin
    for i, part in enumerate(parts):
        if part.startswith("v") and part[1:].isdigit():
            if i + 1 < len(parts):
                origin_dir = parts[i + 1]
                return _decode_origin_dir(origin_dir)

    # Fallback: look for IndexedDB marker and take next relevant dir
    for i, part in enumerate(parts):
        if part in ("___IndexedDB", "IndexedDB"):
            # Check subsequent parts for origin-like directories
            for j in range(i + 1, min(i + 4, len(parts))):
                candidate = parts[j]
                if "_" in candidate and not candidate.startswith("v"):
                    return _decode_origin_dir(candidate)

    return ""


def _decode_origin_dir(dirname: str) -> str:
    """
    Decode a WebKit origin directory name to an origin URL.

    WebKit encodes origins in directory names as:
        ``{scheme}_{host}_{port}``
    (same convention as .localstorage filenames).

    Args:
        dirname: Directory name like ``https_example.com_0``

    Returns:
        Origin URL like ``https://example.com``
    """
    parts = dirname.split("_")
    if len(parts) < 3:
        return dirname
    scheme = parts[0]
    port = parts[-1]
    host = "_".join(parts[1:-1])
    if port == "0":
        return f"{scheme}://{host}"
    return f"{scheme}://{host}:{port}"


# =============================================================================
# Safari LocalStorage Parser
# =============================================================================


def parse_safari_localstorage(
    sqlite_path: Path,
    loc: Dict[str, Any],
    run_id: str,
    evidence_id: int,
    excerpt_size: int = 4096,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse a Safari .localstorage SQLite file.

    Safari LocalStorage files contain a single ``ItemTable`` with
    ``key TEXT`` and ``value BLOB`` (UTF-16LE encoded) columns.

    Args:
        sqlite_path: Path to the .localstorage SQLite file.
        loc: Location metadata dict with browser, profile, source info.
        run_id: Extraction run identifier.
        evidence_id: Evidence ID.
        excerpt_size: Maximum value size before truncation.
        warning_collector: Optional schema warning collector.

    Returns:
        List of record dicts ready for ``insert_local_storages()``.
    """
    records: List[Dict[str, Any]] = []

    browser = loc.get("browser", "safari")
    profile = loc.get("profile")
    source_file = loc.get("logical_path", str(sqlite_path))
    partition_index = loc.get("partition_index")
    fs_type = loc.get("fs_type")

    # Derive origin from filename
    origin = loc.get("origin") or parse_origin_from_localstorage_filename(
        sqlite_path.name
    )

    try:
        conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Schema warning: discover unknown tables
        if warning_collector:
            from extractors._shared.extraction_warnings import discover_unknown_tables

            unknown_tables = discover_unknown_tables(
                conn, KNOWN_LOCALSTORAGE_TABLES, LOCALSTORAGE_TABLE_PATTERNS
            )
            for table_info in unknown_tables:
                warning_collector.add_unknown_table(
                    table_name=table_info["name"],
                    columns=table_info["columns"],
                    source_file=source_file,
                    artifact_type="local_storage",
                )

        if not _table_exists(conn, "ItemTable"):
            LOGGER.warning("ItemTable not found: %s", sqlite_path)
            conn.close()
            return records

        # Schema warning: discover unknown columns
        _discover_and_warn_unknown_columns(
            conn,
            "ItemTable",
            KNOWN_LOCALSTORAGE_COLUMNS,
            source_file,
            "local_storage",
            warning_collector,
        )

        cursor.execute("SELECT key, value FROM ItemTable")
        for row in cursor:
            key = row["key"] or ""
            value_blob = row["value"]

            # Decode UTF-16LE BLOB → string (with UTF-8 fallback)
            if isinstance(value_blob, bytes):
                try:
                    value_str = value_blob.decode("utf-16-le")
                except (UnicodeDecodeError, Exception):
                    value_str = value_blob.decode("utf-8", errors="replace")
            elif value_blob is not None:
                value_str = str(value_blob)
            else:
                value_str = ""

            value_size = len(
                value_str.encode("utf-8", errors="replace")
            )
            truncated = (
                value_str[:excerpt_size]
                if len(value_str) > excerpt_size
                else value_str
            )

            records.append(
                {
                    "run_id": run_id,
                    "browser": browser,
                    "profile": profile,
                    "origin": origin,
                    "key": key,
                    "value": truncated,
                    "value_type": _classify_value_type(truncated),
                    "value_size": value_size,
                    "source_path": source_file,
                    "partition_index": partition_index,
                    "fs_type": fs_type,
                    "logical_path": source_file,
                    "forensic_path": source_file,
                    "notes": None,
                }
            )

        conn.close()

    except Exception as e:
        LOGGER.warning("Failed to parse Safari LocalStorage %s: %s", sqlite_path, e)
        if warning_collector:
            warning_collector.add_file_corrupt(
                filename=str(sqlite_path),
                error=str(e),
                artifact_type="local_storage",
            )

    return records


# =============================================================================
# Safari (WebKit) IndexedDB Parser
# =============================================================================


def parse_safari_indexeddb(
    sqlite_path: Path,
    loc: Dict[str, Any],
    run_id: str,
    evidence_id: int,
    excerpt_size: int = 4096,
    *,
    include_index_records: bool = False,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
    """
    Parse a Safari/WebKit IndexedDB SQLite file.

    WebKit IndexedDB files use a distinct schema from Firefox:
    - ``ObjectStoreInfo``: object store metadata (id, name, keyPath, autoIncrement)
    - ``Records``: data records (objectStoreID, key, value BLOB)
    - ``IndexInfo``: index definitions
    - ``IndexRecords``: indexed entries (opt-in via *include_index_records*)
    - ``DatabaseInfo``: version and metadata (key-value pairs)
    - ``KeyGenerators``: auto-increment counters

    Args:
        sqlite_path: Path to the IndexedDB SQLite file.
        loc: Location metadata dict.
        run_id: Extraction run identifier.
        evidence_id: Evidence ID.
        excerpt_size: Maximum value size before truncation.
        include_index_records: If True, also parse ``IndexRecords`` table
            (index keys/references — not user-created data).  Defaults to
            False to avoid inflating entry counts.
        warning_collector: Optional schema warning collector.

    Returns:
        List of (db_record, entries_list) tuples ready for insertion.
    """
    results: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = []

    browser = loc.get("browser", "safari")
    profile = loc.get("profile")
    source_file = loc.get("logical_path", str(sqlite_path))
    partition_index = loc.get("partition_index")
    fs_type = loc.get("fs_type")

    # Derive origin from path
    origin = loc.get("origin") or parse_origin_from_indexeddb_path(
        str(sqlite_path)
    )

    try:
        conn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        tables = _get_all_tables(conn)

        # Schema warning: discover unknown tables
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

        # Schema warnings for known tables
        if "Records" in tables:
            _discover_and_warn_unknown_columns(
                conn,
                "Records",
                KNOWN_INDEXEDDB_RECORDS_COLUMNS,
                source_file,
                "indexeddb",
                warning_collector,
            )
        if "ObjectStoreInfo" in tables:
            _discover_and_warn_unknown_columns(
                conn,
                "ObjectStoreInfo",
                KNOWN_INDEXEDDB_OBJECTSTOREINFO_COLUMNS,
                source_file,
                "indexeddb",
                warning_collector,
            )
        if "IndexInfo" in tables:
            _discover_and_warn_unknown_columns(
                conn,
                "IndexInfo",
                KNOWN_INDEXEDDB_INDEXINFO_COLUMNS,
                source_file,
                "indexeddb",
                warning_collector,
            )
        if "IndexRecords" in tables:
            _discover_and_warn_unknown_columns(
                conn,
                "IndexRecords",
                KNOWN_INDEXEDDB_INDEXRECORDS_COLUMNS,
                source_file,
                "indexeddb",
                warning_collector,
            )

        # Parse database metadata from DatabaseInfo table
        db_name = sqlite_path.stem
        db_version = None

        if "DatabaseInfo" in tables:
            try:
                cursor.execute("SELECT key, value FROM DatabaseInfo")
                for row in cursor:
                    k = row["key"]
                    v = row["value"]
                    if k == "DatabaseName":
                        db_name = str(v) if v else db_name
                    elif k == "DatabaseVersion":
                        try:
                            db_version = int(v) if v else None
                        except (ValueError, TypeError):
                            db_version = None
            except Exception:
                pass

        # Build object store map from ObjectStoreInfo
        object_store_map: Dict[int, str] = {}
        if "ObjectStoreInfo" in tables:
            try:
                cursor.execute("SELECT id, name FROM ObjectStoreInfo")
                for row in cursor:
                    object_store_map[row["id"]] = row["name"] or f"store_{row['id']}"
            except Exception:
                pass

        # Build database record
        object_stores_json = (
            json.dumps(list(object_store_map.values()))
            if object_store_map
            else None
        )

        db_record: Dict[str, Any] = {
            "run_id": run_id,
            "browser": browser,
            "profile": profile,
            "origin": origin,
            "database_name": db_name,
            "database_version": db_version,
            "object_stores": object_stores_json,
            "total_entries": 0,
            "source_path": source_file,
            "partition_index": partition_index,
            "fs_type": fs_type,
            "logical_path": source_file,
            "forensic_path": source_file,
            "notes": None,
        }

        # Parse records from Records table
        entries: List[Dict[str, Any]] = []

        if "Records" in tables:
            try:
                cursor.execute("SELECT objectStoreID, key, value FROM Records")
                for row in cursor:
                    store_id = row["objectStoreID"]
                    store_name = object_store_map.get(
                        store_id, f"object_store_{store_id}"
                    )

                    key = row["key"]
                    data = row["value"]

                    # Decode BLOB value
                    if isinstance(data, bytes):
                        value_str = data.decode("utf-8", errors="replace")
                    elif data is not None:
                        value_str = str(data)
                    else:
                        value_str = ""

                    truncated = (
                        value_str[:excerpt_size]
                        if len(value_str) > excerpt_size
                        else value_str
                    )

                    entries.append(
                        {
                            "run_id": run_id,
                            "object_store": store_name,
                            "key": str(key) if key else "",
                            "value": truncated,
                            "value_type": _classify_value_type(truncated),
                            "value_size": len(
                                value_str.encode("utf-8", errors="replace")
                            ),
                            "notes": None,
                        }
                    )
            except Exception as e:
                LOGGER.debug("Failed to parse Records table: %s", e)

        # Optionally parse IndexRecords (index keys/references, not user data)
        if include_index_records and "IndexRecords" in tables:
            try:
                cursor.execute(
                    "SELECT indexID, key, value, objectStoreID FROM IndexRecords"
                )
                for row in cursor:
                    index_id = row["indexID"]
                    key = row["key"]
                    value = row["value"]

                    if isinstance(value, bytes):
                        value_str = value.decode("utf-8", errors="replace")
                    elif value is not None:
                        value_str = str(value)
                    else:
                        value_str = ""

                    truncated = (
                        value_str[:excerpt_size]
                        if len(value_str) > excerpt_size
                        else value_str
                    )

                    entries.append(
                        {
                            "run_id": run_id,
                            "object_store": f"index_{index_id}",
                            "key": str(key) if key else "",
                            "value": truncated,
                            "value_type": _classify_value_type(truncated),
                            "value_size": len(
                                value_str.encode("utf-8", errors="replace")
                            ),
                            "notes": "source=IndexRecords",
                        }
                    )
            except Exception as e:
                LOGGER.debug("Failed to parse IndexRecords table: %s", e)

        db_record["total_entries"] = len(entries)
        results.append((db_record, entries))

        conn.close()

    except Exception as e:
        LOGGER.warning("Failed to parse Safari IndexedDB %s: %s", sqlite_path, e)
        if warning_collector:
            warning_collector.add_file_corrupt(
                filename=str(sqlite_path),
                error=str(e),
                artifact_type="indexeddb",
            )

    return results
