"""Base data access infrastructure for case repositories.

This module provides the foundation for all data access classes:
- Database connection management (case DB and evidence DBs)
- Thread-safe evidence context switching
- TTL-based filter caching with thread-safe operations
- Evidence existence guards

Extracted from case_data.py for modular repository pattern.
"""
from __future__ import annotations

import logging
import sqlite3
import time
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from core.database import DatabaseManager, find_case_database


class BaseDataAccess:
    """Base class providing database connection and caching infrastructure.

    Provides:
    - DatabaseManager setup and lifecycle management
    - Thread-local evidence ID tracking for multi-threaded access
    - TTL-based filter cache with thread-safe operations
    - Connection factories for case and evidence databases
    - Evidence label resolution and caching
    - Context manager for evidence-scoped connections

    Added in-memory filter cache for improved tab switching performance.
    Thread-safe evidence connection handling using thread-local storage.
    Added close() method to properly release database connections.
    Thread lock for cache operations (Bug #4 fix).
    Extracted to _base.py as foundation for domain repositories.
    """

    # Cache TTL in seconds (60s default - enough for typical session, not too long to show stale data)
    FILTER_CACHE_TTL = 60.0

    def __init__(
        self,
        case_folder: Path,
        db_path: Optional[Path] = None,
        *,
        db_manager: Optional[DatabaseManager] = None,
    ) -> None:
        """Initialize data access with database connections.

        Args:
            case_folder: Path to the case folder
            db_path: Optional explicit path to case database
            db_manager: Optional pre-configured DatabaseManager instance

        Raises:
            FileNotFoundError: If no case database found or db_path doesn't exist
        """
        logger = logging.getLogger(__name__)

        self.case_folder = case_folder
        self._owns_db_manager = False  # Track if we created the manager

        if db_manager is not None:
            self._db_manager = db_manager
            logger.info("BaseDataAccess: Using provided DatabaseManager (case DB: %s)",
                       db_manager.case_db_path)
        else:
            if db_path is not None:
                logger.info("BaseDataAccess: Using specified database: %s", db_path)
                self._db_manager = DatabaseManager(case_folder, case_db_path=db_path)
            else:
                logger.info("BaseDataAccess: Using default database under case folder")
                found_db = find_case_database(case_folder)
                if found_db is None:
                    raise FileNotFoundError(f"No case database found in {case_folder}")
                self._db_manager = DatabaseManager(case_folder, case_db_path=found_db)
            self._owns_db_manager = True  # We created the manager
            logger.info("BaseDataAccess: Created DatabaseManager (case DB: %s)",
                       self._db_manager.case_db_path)

        self.db_path = self._db_manager.case_db_path

        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        # Evidence label cache (evidence_id -> label string)
        self._evidence_label_cache: Dict[int, Optional[str]] = {}

        # Thread-local storage for current evidence ID
        # This prevents race conditions when multiple threads use the same instance
        self._thread_local = threading.local()

        # In-memory filter cache for performance
        # Keys are f"{cache_type}_{evidence_id}", values are (data, expiry_time)
        self._filter_cache: Dict[str, Tuple[Any, float]] = {}

        # Thread lock for cache operations (Bug #4 fix)
        self._filter_cache_lock = threading.Lock()

    # -------------------------------------------------------------------------
    # Lifecycle Management
    # -------------------------------------------------------------------------

    def close(self) -> None:
        """Close database connections for the current thread.

        Should be called when the instance is no longer needed, especially in
        worker threads, to prevent file descriptor leaks.

        Only closes connections if this instance owns the DatabaseManager (i.e.,
        it was not passed in via the db_manager parameter).

        Added for proper resource cleanup.
        """
        if self._owns_db_manager and self._db_manager is not None:
            self._db_manager.close_thread_connections()

    def __enter__(self) -> "BaseDataAccess":
        """Support context manager protocol."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close connections on context exit."""
        self.close()

    # -------------------------------------------------------------------------
    # Thread-Local Evidence ID
    # -------------------------------------------------------------------------

    @property
    def _current_evidence_id(self) -> Optional[int]:
        """Get current evidence ID for this thread."""
        return getattr(self._thread_local, 'evidence_id', None)

    @_current_evidence_id.setter
    def _current_evidence_id(self, value: Optional[int]) -> None:
        """Set current evidence ID for this thread."""
        self._thread_local.evidence_id = value

    # -------------------------------------------------------------------------
    # Filter Cache (Thread-Safe)
    # -------------------------------------------------------------------------

    def invalidate_filter_cache(self, evidence_id: Optional[int] = None) -> None:
        """Invalidate filter cache entries.

        Call this after extraction or data changes to ensure fresh data.

        Args:
            evidence_id: If provided, only invalidate caches for this evidence.
                        If None, invalidate all caches.

        Added thread lock for thread-safety (Bug #4 fix).
        """
        with self._filter_cache_lock:
            if evidence_id is None:
                self._filter_cache.clear()
            else:
                keys_to_remove = [k for k in self._filter_cache if k.endswith(f"_{evidence_id}")]
                for k in keys_to_remove:
                    del self._filter_cache[k]

    def _get_cached(self, cache_key: str) -> Optional[Any]:
        """Get value from cache if not expired.

        Args:
            cache_key: Cache key string

        Returns:
            Cached data if present and not expired, None otherwise

        Added thread lock for thread-safety (Bug #4 fix).
        """
        with self._filter_cache_lock:
            if cache_key in self._filter_cache:
                data, expiry = self._filter_cache[cache_key]
                if time.time() < expiry:
                    return data
                else:
                    del self._filter_cache[cache_key]
            return None

    def _set_cached(self, cache_key: str, data: Any) -> None:
        """Store value in cache with TTL.

        Args:
            cache_key: Cache key string
            data: Data to cache

        Added thread lock for thread-safety (Bug #4 fix).
        """
        with self._filter_cache_lock:
            self._filter_cache[cache_key] = (data, time.time() + self.FILTER_CACHE_TTL)

    # -------------------------------------------------------------------------
    # Database Manager Access
    # -------------------------------------------------------------------------

    @property
    def db_manager(self) -> Optional[DatabaseManager]:
        """Get the underlying DatabaseManager instance."""
        return self._db_manager

    # -------------------------------------------------------------------------
    # Connection Factories
    # -------------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        """Get a database connection based on current evidence context.

        If _current_evidence_id is set, returns evidence database connection.
        Otherwise, returns case database connection.

        Returns:
            SQLite connection with row_factory set to sqlite3.Row
        """
        if self._current_evidence_id is not None:
            label = self._get_evidence_label(self._current_evidence_id)
            conn = self._db_manager.get_evidence_conn(self._current_evidence_id, label)
        else:
            conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _connect_case(self) -> sqlite3.Connection:
        """Connect to case database only (not evidence database).

        Use this for tables that exist only in the case database
        (e.g., report_sections, cases, evidences).

        Returns:
            SQLite connection with row_factory set to sqlite3.Row
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # -------------------------------------------------------------------------
    # Evidence Label Resolution
    # -------------------------------------------------------------------------

    def _get_evidence_label(self, evidence_id: int) -> Optional[str]:
        """Get and cache evidence label by ID.

        Args:
            evidence_id: Evidence ID to look up

        Returns:
            Evidence label string or None if not found
        """
        if evidence_id not in self._evidence_label_cache:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT label FROM evidences WHERE id = ?",
                    (evidence_id,),
                ).fetchone()
                label = None
                if row is not None:
                    try:
                        label = row["label"]
                    except (IndexError, KeyError):
                        label = row[0] if row else None
                self._evidence_label_cache[evidence_id] = label
        return self._evidence_label_cache.get(evidence_id)

    def get_evidence_label(self, evidence_id: int) -> Optional[str]:
        """Get the label for an evidence by ID.

        Public wrapper around _get_evidence_label for external use.

        Args:
            evidence_id: Evidence ID

        Returns:
            Evidence label string or None if not found
        """
        return self._get_evidence_label(evidence_id)

    # -------------------------------------------------------------------------
    # Evidence Context Management
    # -------------------------------------------------------------------------

    @contextmanager
    def _use_evidence_conn(self, evidence_id: Optional[int]) -> Iterable[None]:
        """Context manager to temporarily set the current evidence ID.

        Use this to scope database operations to a specific evidence:

            with self._use_evidence_conn(evidence_id):
                conn = self._connect()  # Returns evidence DB connection
                # ... do work ...

        Args:
            evidence_id: Evidence ID to use, or None for case DB

        Yields:
            None (context manager pattern)
        """
        previous = self._current_evidence_id
        self._current_evidence_id = evidence_id
        try:
            yield
        finally:
            self._current_evidence_id = previous

    def _evidence_db_exists(self, evidence_id: int) -> bool:
        """Check if evidence database exists for this evidence_id.

        Must be called BEFORE any method that uses _use_evidence_conn
        or calls _connect() to prevent auto-creation of empty databases.

        Args:
            evidence_id: Evidence ID to check

        Returns:
            True if evidence database file exists, False otherwise
        """
        label = self._get_evidence_label(evidence_id)
        if not label:
            return False
        return self._db_manager.evidence_db_exists(evidence_id, label)

    # -------------------------------------------------------------------------
    # Query Utilities
    # -------------------------------------------------------------------------

    def _execute_with_retry(
        self,
        sql: str,
        params: Tuple[Any, ...],
        attempts: int = 3,
        *,
        evidence_id: Optional[int] = None,
    ) -> None:
        """Execute SQL with retry on database lock errors.

        Useful for writes that may encounter transient lock contention
        in multi-threaded scenarios.

        Args:
            sql: SQL statement to execute
            params: Query parameters
            attempts: Maximum retry attempts (default 3)
            evidence_id: Evidence ID for evidence-scoped execution

        Raises:
            sqlite3.OperationalError: If all retry attempts fail

        Moved from CaseDataAccess to BaseDataAccess.
        """
        delay = 0.05
        for attempt in range(attempts):
            try:
                with self._use_evidence_conn(evidence_id):
                    with self._connect() as conn:
                        conn.execute(sql, params)
                        conn.commit()
                return
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "locked" in message and attempt < attempts - 1:
                    time.sleep(delay)
                    delay *= 2
                    continue
                raise

    @staticmethod
    def _scalar(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...]) -> Any:
        """Execute a query and return the first column of the first row.

        Args:
            conn: SQLite connection
            sql: SQL query string
            params: Query parameters

        Returns:
            The scalar value, or 0 if no rows returned
        """
        cursor = conn.execute(sql, params)
        row = cursor.fetchone()
        return row[0] if row else 0
