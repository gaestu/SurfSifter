"""
ESE (Extensible Storage Engine) database reader wrapper.

Provides a unified interface for reading ESE databases (WebCacheV01.dat, etc.)
using available Python libraries:

1. libesedb-python (preferred) - C library bindings, fast and mature
2. dissect.esedb - Pure Python, good fallback
3. External esedbexport tool - Last resort fallback

ESE (aka JET Blue) is Microsoft's embedded database engine used by:
- Internet Explorer / Edge WebCache
- Windows Search index
- Active Directory (NTDS.dit)
- SRUM database
- Exchange databases

Usage:
    from extractors.browser.ie_legacy._ese_reader import (
        ESEReader,
        ESETable,
        ESERecord,
        ESE_AVAILABLE,
    )

    if not ESE_AVAILABLE:
        # Handle missing dependency
        pass

    # Open database
    with ESEReader("/path/to/WebCacheV01.dat") as db:
        # List tables
        for table_name in db.tables():
            print(f"Table: {table_name}")

        # Read table
        for record in db.read_table("Containers"):
            print(record["Name"], record["Url"])
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

from core.logging import get_logger


LOGGER = get_logger("extractors.browser.ie_legacy.ese_reader")


# ============================================================================
# Library Detection
# ============================================================================

ESE_LIBRARY: Optional[str] = None
ESE_AVAILABLE: bool = False

# Try libesedb-python first (C library, fastest)
try:
    import pyesedb  # libesedb-python uses this module name
    ESE_LIBRARY = "libesedb"
    ESE_AVAILABLE = True
    LOGGER.debug("ESE library available: libesedb-python (pyesedb)")
except ImportError:
    pass

# Try dissect.esedb as fallback
if not ESE_AVAILABLE:
    try:
        from dissect import esedb as dissect_esedb
        ESE_LIBRARY = "dissect"
        ESE_AVAILABLE = True
        LOGGER.debug("ESE library available: dissect.esedb")
    except ImportError:
        pass

if not ESE_AVAILABLE:
    LOGGER.info(
        "No ESE library available. Install libesedb-python or dissect.esedb "
        "to enable Internet Explorer/Legacy Edge extraction."
    )


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class ESEColumn:
    """ESE table column definition."""
    name: str
    type: str  # text, integer, binary, currency, datetime, guid, etc.
    index: int


@dataclass
class ESERecord:
    """
    A single record (row) from an ESE table.

    Acts as a dictionary-like object for field access.
    """
    table_name: str
    values: Dict[str, Any] = field(default_factory=dict)

    def __getitem__(self, key: str) -> Any:
        return self.values.get(key)

    def __contains__(self, key: str) -> bool:
        return key in self.values

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

    def keys(self) -> List[str]:
        return list(self.values.keys())

    def items(self):
        return self.values.items()


@dataclass
class ESETable:
    """ESE table metadata."""
    name: str
    columns: List[ESEColumn] = field(default_factory=list)
    record_count: int = 0


# ============================================================================
# ESE Reader (Abstract Interface)
# ============================================================================

class ESEReader:
    """
    Unified ESE database reader.

    Automatically uses the best available library (libesedb or dissect).
    Provides a consistent interface regardless of backend.

    Usage:
        with ESEReader("/path/to/WebCacheV01.dat") as db:
            for record in db.read_table("Containers"):
                print(record["Name"])
    """

    def __init__(self, path: Union[str, Path]):
        """
        Initialize ESE reader.

        Args:
            path: Path to ESE database file

        Raises:
            ImportError: If no ESE library is available
            FileNotFoundError: If database file doesn't exist
        """
        if not ESE_AVAILABLE:
            raise ImportError(
                "No ESE library available. Install libesedb-python or dissect.esedb: "
                "pip install libesedb-python"
            )

        self.path = Path(path)
        if not self.path.exists():
            raise FileNotFoundError(f"ESE database not found: {self.path}")

        self._db = None
        self._backend = ESE_LIBRARY

    def __enter__(self) -> "ESEReader":
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def open(self) -> None:
        """Open the ESE database."""
        if self._backend == "libesedb":
            self._db = pyesedb.file()
            self._db.open(str(self.path))
        elif self._backend == "dissect":
            self._db = dissect_esedb.EseDB(self.path)
        else:
            raise RuntimeError(f"Unknown ESE backend: {self._backend}")

        LOGGER.debug("Opened ESE database: %s (backend=%s)", self.path.name, self._backend)

    def close(self) -> None:
        """Close the ESE database."""
        if self._db is not None:
            if self._backend == "libesedb":
                self._db.close()
            # dissect.esedb doesn't need explicit close
            self._db = None

    def tables(self) -> List[str]:
        """
        Get list of table names in the database.

        Returns:
            List of table names
        """
        if self._db is None:
            raise RuntimeError("Database not open")

        if self._backend == "libesedb":
            return [
                self._db.get_table(i).name
                for i in range(self._db.number_of_tables)
            ]
        elif self._backend == "dissect":
            return list(self._db.tables())

        return []

    def get_table_info(self, table_name: str) -> Optional[ESETable]:
        """
        Get metadata for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            ESETable with column definitions, or None if table not found
        """
        if self._db is None:
            raise RuntimeError("Database not open")

        if self._backend == "libesedb":
            table = self._get_libesedb_table(table_name)
            if table is None:
                return None

            columns = []
            for i in range(table.number_of_columns):
                col = table.get_column(i)
                columns.append(ESEColumn(
                    name=col.name,
                    type=self._libesedb_column_type(col.type),
                    index=i,
                ))

            return ESETable(
                name=table_name,
                columns=columns,
                record_count=table.number_of_records,
            )

        elif self._backend == "dissect":
            try:
                table = self._db.table(table_name)
                columns = []
                for i, col in enumerate(table.columns):
                    columns.append(ESEColumn(
                        name=col.name,
                        type=str(col.type),
                        index=i,
                    ))
                return ESETable(
                    name=table_name,
                    columns=columns,
                    record_count=len(list(table.records())),  # May be slow
                )
            except KeyError:
                return None

        return None

    def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> Iterator[ESERecord]:
        """
        Read records from a table.

        Args:
            table_name: Name of the table to read
            columns: Optional list of column names to include (None = all)
            limit: Optional maximum number of records to return

        Yields:
            ESERecord objects with field values
        """
        if self._db is None:
            raise RuntimeError("Database not open")

        if self._backend == "libesedb":
            yield from self._read_table_libesedb(table_name, columns, limit)
        elif self._backend == "dissect":
            yield from self._read_table_dissect(table_name, columns, limit)

    # ========================================================================
    # libesedb Backend Implementation
    # ========================================================================

    def _get_libesedb_table(self, table_name: str):
        """Get table by name from libesedb."""
        for i in range(self._db.number_of_tables):
            table = self._db.get_table(i)
            if table.name == table_name:
                return table
        return None

    def _read_table_libesedb(
        self,
        table_name: str,
        columns: Optional[List[str]],
        limit: Optional[int],
    ) -> Iterator[ESERecord]:
        """Read table using libesedb backend."""
        table = self._get_libesedb_table(table_name)
        if table is None:
            LOGGER.warning("Table not found: %s", table_name)
            return

        # Build column name -> index mapping
        col_map = {}
        for i in range(table.number_of_columns):
            col = table.get_column(i)
            col_map[col.name] = (i, col)

        # Determine which columns to read
        if columns is None:
            read_columns = list(col_map.keys())
        else:
            read_columns = [c for c in columns if c in col_map]

        count = 0
        for i in range(table.number_of_records):
            if limit is not None and count >= limit:
                break

            record = table.get_record(i)
            values = {}

            for col_name in read_columns:
                col_idx, col_def = col_map[col_name]
                try:
                    value = self._get_libesedb_value(record, col_idx, col_def)
                    values[col_name] = value
                except Exception as e:
                    LOGGER.debug(
                        "Failed to read column %s in record %d: %s",
                        col_name, i, e
                    )
                    values[col_name] = None

            yield ESERecord(table_name=table_name, values=values)
            count += 1

    def _get_libesedb_value(self, record, col_idx: int, col_def) -> Any:
        """Extract value from libesedb record."""
        # Check for NULL
        if not record.is_long_value(col_idx) and record.get_value_data(col_idx) is None:
            return None

        col_type = col_def.type

        # Text types
        if col_type in (pyesedb.column_types.TEXT, pyesedb.column_types.LARGE_TEXT):
            return record.get_value_data_as_string(col_idx)

        # Integer types
        if col_type == pyesedb.column_types.INTEGER_8BIT_UNSIGNED:
            data = record.get_value_data(col_idx)
            return int.from_bytes(data, 'little', signed=False) if data else None

        if col_type == pyesedb.column_types.INTEGER_16BIT_SIGNED:
            data = record.get_value_data(col_idx)
            return int.from_bytes(data, 'little', signed=True) if data else None

        if col_type == pyesedb.column_types.INTEGER_16BIT_UNSIGNED:
            data = record.get_value_data(col_idx)
            return int.from_bytes(data, 'little', signed=False) if data else None

        if col_type == pyesedb.column_types.INTEGER_32BIT_SIGNED:
            return record.get_value_data_as_integer(col_idx)

        if col_type == pyesedb.column_types.INTEGER_32BIT_UNSIGNED:
            data = record.get_value_data(col_idx)
            return int.from_bytes(data, 'little', signed=False) if data else None

        if col_type == pyesedb.column_types.INTEGER_64BIT_SIGNED:
            # pyesedb doesn't have get_value_data_as_long_integer, read raw bytes
            data = record.get_value_data(col_idx)
            return int.from_bytes(data, 'little', signed=True) if data else None

        # Currency (64-bit, often used for timestamps)
        if col_type == pyesedb.column_types.CURRENCY:
            # Currency is a 64-bit signed integer (used for timestamps)
            data = record.get_value_data(col_idx)
            return int.from_bytes(data, 'little', signed=True) if data else None

        # Floating point
        if col_type == pyesedb.column_types.FLOAT_32BIT:
            return record.get_value_data_as_floating_point(col_idx)

        if col_type == pyesedb.column_types.DOUBLE_64BIT:
            return record.get_value_data_as_floating_point(col_idx)

        # Date/Time (OLE Automation date)
        if col_type == pyesedb.column_types.DATE_TIME:
            return record.get_value_data_as_floating_point(col_idx)

        # Binary
        if col_type in (pyesedb.column_types.BINARY_DATA, pyesedb.column_types.LARGE_BINARY_DATA):
            return record.get_value_data(col_idx)

        # GUID
        if col_type == pyesedb.column_types.GUID:
            data = record.get_value_data(col_idx)
            if data and len(data) == 16:
                import uuid
                return str(uuid.UUID(bytes_le=data))
            return None

        # Boolean
        if col_type == pyesedb.column_types.BOOLEAN:
            data = record.get_value_data(col_idx)
            return bool(data[0]) if data else None

        # Default: return raw bytes
        return record.get_value_data(col_idx)

    def _libesedb_column_type(self, type_code: int) -> str:
        """Convert libesedb column type code to string."""
        type_map = {
            pyesedb.column_types.NULL: "null",
            pyesedb.column_types.BOOLEAN: "boolean",
            pyesedb.column_types.INTEGER_8BIT_UNSIGNED: "uint8",
            pyesedb.column_types.INTEGER_16BIT_SIGNED: "int16",
            pyesedb.column_types.INTEGER_16BIT_UNSIGNED: "uint16",
            pyesedb.column_types.INTEGER_32BIT_SIGNED: "int32",
            pyesedb.column_types.INTEGER_32BIT_UNSIGNED: "uint32",
            pyesedb.column_types.CURRENCY: "currency",
            pyesedb.column_types.FLOAT_32BIT: "float32",
            pyesedb.column_types.DOUBLE_64BIT: "float64",
            pyesedb.column_types.DATE_TIME: "datetime",
            pyesedb.column_types.BINARY_DATA: "binary",
            pyesedb.column_types.TEXT: "text",
            pyesedb.column_types.LARGE_BINARY_DATA: "large_binary",
            pyesedb.column_types.LARGE_TEXT: "large_text",
            pyesedb.column_types.INTEGER_64BIT_SIGNED: "int64",
            pyesedb.column_types.GUID: "guid",
        }
        return type_map.get(type_code, f"unknown({type_code})")

    # ========================================================================
    # dissect.esedb Backend Implementation
    # ========================================================================

    def _read_table_dissect(
        self,
        table_name: str,
        columns: Optional[List[str]],
        limit: Optional[int],
    ) -> Iterator[ESERecord]:
        """Read table using dissect.esedb backend."""
        try:
            table = self._db.table(table_name)
        except KeyError:
            LOGGER.warning("Table not found: %s", table_name)
            return

        # Get column names
        all_columns = [col.name for col in table.columns]
        if columns is None:
            read_columns = all_columns
        else:
            read_columns = [c for c in columns if c in all_columns]

        count = 0
        for record in table.records():
            if limit is not None and count >= limit:
                break

            values = {}
            for col_name in read_columns:
                try:
                    values[col_name] = getattr(record, col_name, None)
                except Exception as e:
                    LOGGER.debug(
                        "Failed to read column %s: %s",
                        col_name, e
                    )
                    values[col_name] = None

            yield ESERecord(table_name=table_name, values=values)
            count += 1


# ============================================================================
# WebCache-Specific Helpers
# ============================================================================

class WebCacheReader(ESEReader):
    """
    Specialized reader for IE/Edge WebCacheV01.dat.

    Provides helper methods for common WebCache operations.
    """

    # Container name constants
    CONTAINER_HISTORY = "History"
    CONTAINER_COOKIES = "Cookies"
    CONTAINER_IEDOWNLOAD = "iedownload"
    CONTAINER_CONTENT = "Content"
    CONTAINER_DOMSTORE = "DOMStore"

    def get_containers(self) -> List[Dict[str, Any]]:
        """
        Get list of containers from the Containers table.

        Returns:
            List of container metadata dicts
        """
        containers = []
        for record in self.read_table("Containers"):
            containers.append({
                "container_id": record.get("ContainerId"),
                "name": record.get("Name"),
                "directory": record.get("Directory"),
                "secure_directories": record.get("SecureDirectories"),
                "partition_id": record.get("PartitionId"),
            })
        return containers

    def get_history_entries(self, limit: Optional[int] = None) -> Iterator[ESERecord]:
        """
        Get browsing history entries.

        Finds the History container and reads its entries.

        Yields:
            ESERecord objects with history data
        """
        # Find History container
        history_table = self._find_container_table(self.CONTAINER_HISTORY)
        if history_table:
            yield from self.read_table(history_table, limit=limit)

    def get_download_entries(self, limit: Optional[int] = None) -> Iterator[ESERecord]:
        """
        Get download history entries.

        Yields:
            ESERecord objects with download data
        """
        download_table = self._find_container_table(self.CONTAINER_IEDOWNLOAD)
        if download_table:
            yield from self.read_table(download_table, limit=limit)

    def get_cookie_entries(self, limit: Optional[int] = None) -> Iterator[ESERecord]:
        """
        Get cookie entries from all cookie containers.

        Yields:
            ESERecord objects with cookie data
        """
        # There may be multiple cookie containers (Cookies, CookiesLow, etc.)
        for container in self.get_containers():
            if container["name"] and "cookie" in container["name"].lower():
                table_name = f"Container_{container['container_id']}"
                if table_name in self.tables():
                    yield from self.read_table(table_name, limit=limit)

    def _find_container_table(self, container_name: str) -> Optional[str]:
        """
        Find the table name for a container by its name.

        Container data is stored in tables named Container_N where N
        is the ContainerId from the Containers table.

        Args:
            container_name: Name of the container (History, Cookies, etc.)

        Returns:
            Table name (Container_N) or None if not found
        """
        for container in self.get_containers():
            if container["name"] == container_name:
                table_name = f"Container_{container['container_id']}"
                if table_name in self.tables():
                    return table_name
        return None


def check_ese_available() -> tuple[bool, str]:
    """
    Check if ESE parsing is available.

    Returns:
        Tuple of (is_available, library_name_or_error_message)
    """
    if ESE_AVAILABLE:
        return True, ESE_LIBRARY
    else:
        return False, "No ESE library available. Install: pip install libesedb-python or pip install dissect.esedb"
