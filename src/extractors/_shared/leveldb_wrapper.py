"""
LevelDB Wrapper for Browser Forensics

Provides a unified interface for parsing Chromium LevelDB databases.
Wraps ccl_chromium_reader for robust error handling and consistent iteration.

Features:
- Local Storage iteration
- Session Storage iteration
- IndexedDB iteration
- Raw record iteration (for Sync Data, Extensions)
- Graceful corruption handling
- Deleted record flagging (when supported)

Usage:
    wrapper = LevelDBWrapper(db_path)
    for record in wrapper.iterate_local_storage():
        print(record['origin'], record['key'], record['value'])

Dependencies:
    - ccl_chromium_reader (optional, graceful degradation)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator, Dict, Any, Optional, Tuple, List
from dataclasses import dataclass

from core.logging import get_logger

LOGGER = get_logger("extractors.leveldb")

# Try to import ccl_chromium_reader
# API changed in v0.3.x - ccl_leveldb moved inside submodules
CCL_AVAILABLE = False
ccl_leveldb = None
LocalStoreDb = None
IndexedDb = None
SessionStoreDb = None

try:
    import ccl_chromium_reader

    # Try to import ccl_leveldb (for raw LevelDB access)
    try:
        from ccl_chromium_reader import ccl_leveldb
    except ImportError:
        # New API (v0.3.x) - ccl_leveldb is inside submodules
        from ccl_chromium_reader.ccl_chromium_localstorage import ccl_leveldb

    # Try to import LocalStoreDb (for Local Storage parsing)
    try:
        from ccl_chromium_reader import LocalStoreDb
    except ImportError:
        from ccl_chromium_reader.ccl_chromium_localstorage import LocalStoreDb

    # Try to import SessionStoreDb (for Session Storage parsing)
    try:
        from ccl_chromium_reader import SessionStoreDb
    except ImportError:
        try:
            from ccl_chromium_reader.ccl_chromium_sessionstorage import SessionStoreDb
        except ImportError:
            SessionStoreDb = None  # May not be available

    # Try to import IndexedDb (for IndexedDB parsing)
    try:
        from ccl_chromium_reader import IndexedDb
    except ImportError:
        try:
            from ccl_chromium_reader.ccl_chromium_indexeddb import WrappedIndexDB as IndexedDb
        except ImportError:
            IndexedDb = None  # May not be available

    CCL_AVAILABLE = True
except ImportError:
    LOGGER.warning("ccl_chromium_reader not installed - LevelDB parsing unavailable")


@dataclass
class LevelDBRecord:
    """Raw LevelDB record with metadata."""
    key: bytes
    value: bytes
    seq_number: int
    is_deleted: bool = False

    @property
    def key_str(self) -> str:
        """Decode key as UTF-8 with error replacement."""
        return self.key.decode('utf-8', errors='replace')

    @property
    def value_str(self) -> str:
        """Decode value as UTF-8 with error replacement."""
        return self.value.decode('utf-8', errors='replace')


@dataclass
class LocalStorageRecord:
    """Parsed Local Storage record."""
    origin: str
    key: str
    value: str
    seq_number: int
    is_deleted: bool = False


@dataclass
class IndexedDBRecord:
    """Parsed IndexedDB record."""
    origin: str
    database_name: str
    object_store: str
    key: Any
    value: Any
    value_type: str
    seq_number: int
    is_deleted: bool = False


class LevelDBWrapper:
    """
    Wrapper for Chromium LevelDB database parsing.

    Provides consistent iteration over different types of LevelDB storage:
    - Local Storage
    - Session Storage
    - IndexedDB
    - Raw records (for Sync Data, Extension Storage)

    Handles corruption gracefully with logging and partial recovery.
    """

    def __init__(
        self,
        db_path: Path,
        blob_path: Optional[Path] = None,
        include_deleted: bool = True
    ):
        """
        Initialize LevelDB wrapper.

        Args:
            db_path: Path to LevelDB directory (contains MANIFEST-*, *.ldb, etc.)
            blob_path: Optional path to blob storage (for IndexedDB)
            include_deleted: Whether to include deleted/historical records
        """
        self.db_path = Path(db_path)
        self.blob_path = Path(blob_path) if blob_path else None
        self.include_deleted = include_deleted
        self._db = None
        self._error_count = 0
        self._ccl_available = CCL_AVAILABLE

        if not CCL_AVAILABLE:
            LOGGER.warning("ccl_chromium_reader not installed - LevelDB parsing unavailable. Install with: pip install ccl-chromium-reader")

    def _open_db(self):
        """Open LevelDB database if not already open."""
        if self._db is None:
            try:
                self._db = ccl_leveldb.RawLevelDb(str(self.db_path))
            except Exception as e:
                LOGGER.error("Failed to open LevelDB at %s: %s", self.db_path, e)
                raise
        return self._db

    def close(self):
        """Close database connection."""
        if self._db is not None:
            try:
                self._db.close()
            except Exception:
                pass
            self._db = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close database."""
        self.close()
        return False

    def iterate_records_raw(self) -> Iterator[LevelDBRecord]:
        """
        Iterate over all raw LevelDB records.

        Yields raw key-value pairs for scanning (useful for Sync Data, Extensions).
        Handles invalid KeyState values (e.g., 114 in old CefSharp WAL files)
        by stopping iteration gracefully rather than crashing.

        Yields:
            LevelDBRecord with raw key, value, sequence number
        """
        db = self._open_db()
        iterator = db.iterate_records_raw()

        while True:
            # Fetch the next record from the generator.
            # ValueError can occur when ccl_leveldb encounters an invalid
            # KeyState enum value (e.g., 114 in old/corrupt WAL files).
            # After such an error the generator is exhausted, so we break.
            try:
                record = next(iterator)
            except StopIteration:
                break
            except ValueError as e:
                self._error_count += 1
                LOGGER.debug(
                    "Stopping raw LevelDB iteration — invalid record state "
                    "(remaining WAL records may be lost): %s",
                    e,
                )
                break
            except Exception as e:
                self._error_count += 1
                if self._error_count <= 10:
                    LOGGER.warning(
                        "Error iterating LevelDB records: %s", e,
                    )
                break  # Generator is likely exhausted after exception

            try:
                # Check if record is deleted
                is_deleted = getattr(record, 'state', None) == ccl_leveldb.KeyState.Deleted

                if is_deleted and not self.include_deleted:
                    continue

                yield LevelDBRecord(
                    key=record.user_key,
                    value=record.value,
                    seq_number=record.seq,
                    is_deleted=is_deleted,
                )
            except Exception as e:
                self._error_count += 1
                if self._error_count <= 10:
                    LOGGER.warning("Error reading LevelDB record: %s", e)
                elif self._error_count == 11:
                    LOGGER.warning("Suppressing further LevelDB read errors...")

    def iterate_local_storage(self) -> Iterator[LocalStorageRecord]:
        """
        Iterate over Local Storage records.

        Parses Chromium Local Storage LevelDB format:
        - Key: "_<origin>\x00<key>" (prefixed with underscore)
        - Value: UTF-16LE encoded string

        Yields:
            LocalStorageRecord with origin, key, value
        """
        try:
            # Try using ccl's native local storage parser first
            if LocalStoreDb is not None:
                yield from self._iterate_local_storage_native()
                return
        except Exception as e:
            LOGGER.debug("Native Local Storage parser failed, using raw: %s", e)

        # Fall back to raw parsing
        yield from self._iterate_local_storage_raw()

    def _iterate_local_storage_native(self) -> Iterator[LocalStorageRecord]:
        """Use ccl's native Local Storage parser."""
        # Note: ccl_chromium_reader v0.3+ requires Path object, not str
        ls_db = LocalStoreDb(self.db_path)

        for record in ls_db.iter_all_records():
            try:
                yield LocalStorageRecord(
                    origin=record.storage_key,
                    key=record.script_key,
                    value=record.value,
                    seq_number=getattr(record, 'seq', 0),
                    is_deleted=getattr(record, 'is_deleted', False),
                )
            except Exception as e:
                self._error_count += 1
                if self._error_count <= 10:
                    LOGGER.warning("Error parsing Local Storage record: %s", e)

    def _iterate_local_storage_raw(self) -> Iterator[LocalStorageRecord]:
        """Parse Local Storage from raw LevelDB records."""
        for record in self.iterate_records_raw():
            try:
                # Local Storage keys start with '_' followed by origin
                if not record.key.startswith(b'_'):
                    continue

                # Split key at null byte
                key_data = record.key[1:]  # Skip underscore
                if b'\x00' in key_data:
                    origin_bytes, ls_key_bytes = key_data.split(b'\x00', 1)
                    origin = origin_bytes.decode('utf-8', errors='replace')
                    ls_key = ls_key_bytes.decode('utf-8', errors='replace')
                else:
                    # Malformed key, skip
                    continue

                # Value is typically UTF-16LE encoded
                try:
                    value = record.value.decode('utf-16-le')
                except UnicodeDecodeError:
                    value = record.value.decode('utf-8', errors='replace')

                yield LocalStorageRecord(
                    origin=origin,
                    key=ls_key,
                    value=value,
                    seq_number=record.seq_number,
                    is_deleted=record.is_deleted,
                )

            except Exception as e:
                self._error_count += 1
                if self._error_count <= 10:
                    LOGGER.warning("Error parsing raw Local Storage record: %s", e)

    def iterate_session_storage(self) -> Iterator[LocalStorageRecord]:
        """
        Iterate over Session Storage records.

        Session Storage uses a different format than Local Storage.
        Uses ccl_chromium_reader's SessionStoreDb parser.

        Yields:
            LocalStorageRecord with origin, key, value (reusing type)
        """
        if SessionStoreDb is None:
            LOGGER.warning("SessionStoreDb not available for Session Storage parsing")
            return

        try:
            ss = SessionStoreDb(self.db_path)
            try:
                for rec in ss.iter_all_records():
                    try:
                        # rec.host is the origin (e.g., 'https://example.com/')
                        # rec.key is the storage key
                        # rec.value is the value (string)
                        origin = rec.host if hasattr(rec, 'host') else ""
                        key = str(rec.key) if hasattr(rec, 'key') else ""
                        value = str(rec.value) if hasattr(rec, 'value') else ""
                        is_deleted = getattr(rec, 'is_deleted', False)

                        yield LocalStorageRecord(
                            origin=origin,
                            key=key,
                            value=value,
                            seq_number=getattr(rec, 'leveldb_sequence_number', 0),
                            is_deleted=is_deleted,
                        )
                    except Exception as e:
                        self._error_count += 1
                        if self._error_count <= 10:
                            LOGGER.debug("Error reading session storage record: %s", e)
            finally:
                ss.close()
        except Exception as e:
            LOGGER.warning("Session storage parsing failed for %s: %s", self.db_path, e)

    def iterate_indexeddb(self) -> Iterator[IndexedDBRecord]:
        """
        Iterate over IndexedDB records.

        Uses ccl_chromium_reader's IndexedDB parser for V8 object deserialization.

        Yields:
            IndexedDBRecord with database_name, object_store, key, value
        """
        try:
            if IndexedDb is not None:
                yield from self._iterate_indexeddb_native()
                return
        except Exception as e:
            LOGGER.debug("Native IndexedDB parser failed: %s", e)

        # Raw parsing of IndexedDB is complex - log warning
        LOGGER.warning("IndexedDB native parser not available, raw parsing not implemented")

    def _iterate_indexeddb_native(self) -> Iterator[IndexedDBRecord]:
        """Use ccl's native IndexedDB parser."""
        blob_dir = self.blob_path or (self.db_path.parent / "blob_storage")

        try:
            idb = IndexedDb(
                str(self.db_path),
                str(blob_dir) if blob_dir.exists() else None
            )
        except Exception as e:
            LOGGER.error("Failed to open IndexedDB: %s", e)
            return

        for db_info in idb.database_ids:
            try:
                db_name = db_info.name
                origin = db_info.origin

                for obj_store_info in idb.get_object_store_names(db_info.dbid_no):
                    obj_store_name = obj_store_info.name

                    for record in idb.iterate_records(
                        db_info.dbid_no,
                        obj_store_info.store_id
                    ):
                        try:
                            value = record.value
                            value_type = self._classify_value_type(value)

                            yield IndexedDBRecord(
                                origin=origin,
                                database_name=db_name,
                                object_store=obj_store_name,
                                key=record.key,
                                value=value,
                                value_type=value_type,
                                seq_number=getattr(record, 'seq', 0),
                                is_deleted=getattr(record, 'is_deleted', False),
                            )
                        except Exception as e:
                            self._error_count += 1
                            if self._error_count <= 10:
                                LOGGER.warning("Error parsing IndexedDB record: %s", e)

            except Exception as e:
                LOGGER.warning("Error iterating database %s: %s", db_info, e)

    def _classify_value_type(self, value: Any) -> str:
        """Classify value type for storage."""
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, (int, float)):
            return "number"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, bytes):
            return "blob"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
            return "unknown"

    # Method aliases for backward compatibility
    iter_local_storage = iterate_local_storage
    iter_session_storage = iterate_session_storage
    iter_indexeddb = iterate_indexeddb

    def iterate_indexeddb_databases(self) -> Iterator[Dict[str, Any]]:
        """
        Iterate over IndexedDB databases, yielding database info with entries.

        This is the high-level API that returns database metadata along with
        all entries, suitable for ingestion.

        IndexedDB directories have structure:
        - IndexedDB/
          - https_example.com_0.indexeddb.leveldb/  (per-origin LevelDB)
          - https_example.com_0.indexeddb.blob/     (blob storage)

        Yields:
            Dict with keys: origin, name, version, object_store_count, entries
        """
        if IndexedDb is None:
            LOGGER.warning("IndexedDB native parser not available")
            return

        # Check if this is a top-level IndexedDB directory with subdirs
        # or a specific origin's leveldb directory
        if self.db_path.name.endswith('.indexeddb.leveldb'):
            # Single origin directory
            yield from self._parse_single_indexeddb(self.db_path)
        else:
            # Top-level IndexedDB directory - iterate subdirectories
            for subdir in self.db_path.iterdir():
                if subdir.is_dir() and subdir.name.endswith('.indexeddb.leveldb'):
                    yield from self._parse_single_indexeddb(subdir)

    def _parse_single_indexeddb(self, leveldb_path: Path) -> Iterator[Dict[str, Any]]:
        """Parse a single origin's IndexedDB LevelDB directory."""
        # Find corresponding blob storage
        blob_dir = leveldb_path.parent / leveldb_path.name.replace('.leveldb', '.blob')

        try:
            # Use WrappedIndexDB for cleaner high-level API
            idb = IndexedDb(
                str(leveldb_path),
                str(blob_dir) if blob_dir.exists() else None
            )
        except Exception as e:
            LOGGER.debug("Failed to open IndexedDB %s: %s", leveldb_path.name, e)
            return

        try:
            for db_info in idb.database_ids:
                try:
                    db_name = db_info.name
                    origin = db_info.origin

                    # Access the wrapped database object
                    wrapped_db = idb[db_info.dbid_no]

                    entries = []
                    object_store_count = 0

                    # Iterate object stores via the wrapped database
                    for obj_store in wrapped_db:
                        object_store_count += 1
                        obj_store_name = obj_store.name

                        try:
                            for record in obj_store.iterate_records():
                                try:
                                    # Extract key - may be an IdbKey object
                                    key = record.key
                                    if hasattr(key, 'raw_key'):
                                        key = key.raw_key
                                    elif hasattr(key, '__str__'):
                                        key = str(key)

                                    entries.append({
                                        "object_store": obj_store_name,
                                        "key": key,
                                        "value": record.value,
                                    })
                                except Exception as rec_err:
                                    self._error_count += 1
                                    if self._error_count <= 10:
                                        LOGGER.debug("Error reading IDB record: %s", rec_err)
                        except Exception as store_err:
                            LOGGER.debug("Error iterating object store %s: %s", obj_store_name, store_err)

                    yield {
                        "origin": origin,
                        "name": db_name,
                        "version": getattr(db_info, 'version', None),
                        "object_store_count": object_store_count,
                        "entries": entries,
                    }

                except Exception as e:
                    self._error_count += 1
                    if self._error_count <= 10:
                        LOGGER.warning("Error parsing IndexedDB database: %s", e)
        finally:
            # Clean up
            try:
                idb.close()
            except Exception:
                pass

    # Alias for backward compatibility
    iter_indexeddb_databases = iterate_indexeddb_databases

    def get_stats(self) -> Dict[str, Any]:
        """Get parsing statistics."""
        return {
            "db_path": str(self.db_path),
            "error_count": self._error_count,
            "ccl_available": CCL_AVAILABLE,
        }


def is_leveldb_available() -> bool:
    """Check if LevelDB parsing is available (ccl_chromium_reader installed)."""
    return CCL_AVAILABLE


def check_leveldb_directory(path: Path) -> bool:
    """
    Check if a path looks like a valid LevelDB directory.

    Args:
        path: Directory path to check

    Returns:
        True if directory contains LevelDB files
    """
    if not path.is_dir():
        return False

    # LevelDB directories typically have CURRENT or MANIFEST files
    has_current = (path / "CURRENT").exists()
    has_manifest = any(path.glob("MANIFEST-*"))
    has_ldb = any(path.glob("*.ldb")) or any(path.glob("*.log"))

    return has_current or has_manifest or has_ldb
