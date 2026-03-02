"""
Database connection manager.

This module provides:
- DatabaseManager: Connection pooling for case/evidence databases
- slugify_label: Convert evidence labels to filesystem-friendly slugs
- ensure_case_structure: Create standard case folder structure
- find_case_database: Locate case database file

Extracted from db_manager.py during database refactor.
"""
from __future__ import annotations

import re
import sqlite3
import threading
from pathlib import Path
from typing import Dict, Optional

from .connection import init_db, migrate
from core.logging import get_logger

__all__ = [
    "DatabaseManager",
    "slugify_label",
    "ensure_case_structure",
    "find_case_database",
    "CASE_MIGRATIONS_DIR",
    "EVIDENCE_MIGRATIONS_DIR",
    "CASE_DB_SUFFIX",
    "LEGACY_CASE_DB_SUFFIX",
    "CASE_DB_GLOB",
    "LEGACY_CASE_DB_GLOB",
]

LOGGER = get_logger("core.database.manager")

# Case database filename suffixes
CASE_DB_SUFFIX = "_surfsifter.sqlite"
LEGACY_CASE_DB_SUFFIX = "_browser.sqlite"
CASE_DB_GLOB = "*_surfsifter.sqlite"
LEGACY_CASE_DB_GLOB = "*_browser.sqlite"


_SLUG_INVALID_RE = re.compile(r"[^a-z0-9-]+")
_SLUG_DUP_DASH_RE = re.compile(r"-+")


def slugify_label(label: Optional[str], evidence_id: int) -> str:
    """
    Convert an evidence label into a filesystem-friendly slug.

    Label is now always required (auto-derived from E01 filename).
    The fallback to ev-<id> is no longer used.

    Args:
        label: Evidence label (typically E01 base filename)
        evidence_id: Evidence ID (kept for backward compatibility, not used)

    Returns:
        Slugified label prefixed with 'ev-' if it doesn't start with a letter

    Example:
        >>> slugify_label("4Dell Latitude CPi", 1)
        'ev-4dell-latitude-cpi'
    """
    if not label:
        raise ValueError(f"Evidence label is required (evidence_id={evidence_id})")

    slug = label.strip().lower().replace("_", "-")
    slug = _SLUG_INVALID_RE.sub("-", slug)
    slug = _SLUG_DUP_DASH_RE.sub("-", slug).strip("-")

    if not slug:
        raise ValueError(f"Evidence label '{label}' resulted in empty slug (evidence_id={evidence_id})")

    # Prefix with 'ev-' if slug doesn't start with a letter
    if not slug[0].isalpha():
        slug = f"ev-{slug}"

    return slug


def ensure_case_structure(case_folder: Path) -> Path:
    """
    Ensure the standard folder structure exists inside a case directory.

    Returns the path to the ``evidences`` subfolder.
    """
    case_folder = case_folder.resolve()
    evidences_dir = case_folder / "evidences"
    evidences_dir.mkdir(parents=True, exist_ok=True)

    # Create logs directory for evidence-level audit logs
    logs_dir = case_folder / "logs"
    logs_dir.mkdir(exist_ok=True)

    return evidences_dir


def find_case_database(case_folder: Path) -> Optional[Path]:
    """
    Find the case database file in the case folder.

    Looks for files matching the *_surfsifter.sqlite naming convention first,
    then falls back to the legacy *_browser.sqlite pattern for backward
    compatibility with cases created before the SurfSifter rename.

    Args:
        case_folder: Path to case workspace

    Returns:
        Path to case database file, or None if not found

    Example:
        >>> db_path = find_case_database(Path("/cases/CASE-001"))
        >>> # Returns: /cases/CASE-001/CASE-001_surfsifter.sqlite
        >>> # Or (legacy): /cases/CASE-001/CASE-001_browser.sqlite
    """
    if not case_folder.exists():
        return None

    # Primary: look for *_surfsifter.sqlite
    dbs = list(case_folder.glob(CASE_DB_GLOB))
    if dbs:
        dbs.sort()
        return dbs[0]

    # Fallback: look for legacy *_browser.sqlite
    legacy_dbs = list(case_folder.glob(LEGACY_CASE_DB_GLOB))
    if legacy_dbs:
        legacy_dbs.sort()
        LOGGER.info("Found legacy case database: %s", legacy_dbs[0].name)
        return legacy_dbs[0]

    return None


CASE_MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
EVIDENCE_MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations_evidence"


class DatabaseManager:
    """
    Helper responsible for resolving and opening case/evidence databases.

    Added per-thread connection caching to prevent file descriptor leaks.
    Connections are cached per (thread_id, db_path) and reused within the same thread.
    Call close_all() when done to properly release resources.

    For now evidence connections fall back to the case database. The evidence-local
    split will be enabled in a later milestone once the migration is in place.

    Note:
        As of, case_db_path is required and must follow the naming convention:
        {case_number}_surfsifter.sqlite (e.g., CASE-2025-001_surfsifter.sqlite)
        Legacy _browser.sqlite naming is still supported for opening existing cases.
    """

    def __init__(
        self,
        case_folder: Path,
        *,
        enable_split: bool = True,
        case_db_path: Path,
    ) -> None:
        self.case_folder = case_folder.resolve()
        self.enable_split = enable_split
        self._case_db_path = case_db_path.resolve()
        # Thread-safe connection cache
        # Key: (thread_id, db_path_str), Value: sqlite3.Connection
        self._conn_cache: Dict[tuple, sqlite3.Connection] = {}
        self._cache_lock = threading.Lock()
        ensure_case_structure(self.case_folder)

    def __enter__(self) -> "DatabaseManager":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:  # noqa: ANN001
        self.close_all()

    @property
    def case_db_path(self) -> Path:
        """Absolute path to the case database."""
        return self._case_db_path

    def get_case_conn(self) -> sqlite3.Connection:
        """Return a connection to the case database, creating it if necessary."""
        return self._get_or_create_conn(self._case_db_path, init_case=True)

    def get_evidence_conn(
        self,
        evidence_id: int,
        label: Optional[str] = None,
    ) -> sqlite3.Connection:
        """
        Return a connection for the given evidence.

        Evidence-local databases are not enabled yet; the case connection is returned
        as a temporary measure to preserve existing behaviour.
        """
        if not self.enable_split:
            return self.get_case_conn()

        db_path = self.evidence_db_path(evidence_id, label)
        return self._get_or_create_conn(db_path, init_case=False, evidence_id=evidence_id)

    def evidence_db_path(
        self,
        evidence_id: int,
        label: Optional[str] = None,
        *,
        create_dirs: bool = True,
    ) -> Path:
        """
        Resolve the on-disk path for the evidence database.

        Directories are created by default so callers can immediately persist files.
        """
        slug = slugify_label(label, evidence_id)
        evidences_dir = ensure_case_structure(self.case_folder)
        evidence_dir = evidences_dir / slug
        if create_dirs:
            evidence_dir.mkdir(parents=True, exist_ok=True)
        return evidence_dir / f"evidence_{slug}.sqlite"

    def evidence_db_exists(self, evidence_id: int, label: str) -> bool:
        """
        Check if evidence database file exists (without creating it).

        IMPORTANT: This must be called BEFORE get_evidence_conn() to prevent
        auto-creation. The _get_or_create_conn() method always creates the
        database on first access.

        Args:
            evidence_id: Evidence ID
            label: Evidence label (required to compute the database path)

        Returns:
            True if the evidence database file exists, False otherwise
        """
        db_path = self.evidence_db_path(evidence_id, label, create_dirs=False)
        return db_path.exists()

    def close_all(self) -> None:
        """
        Close all cached SQLite connections.

        Properly closes all connections in the cache to prevent
        file descriptor leaks. Should be called when the DatabaseManager
        is no longer needed.

        Note: SQLite connections have thread affinity. If close_all() is called
        from a different thread than where the connection was created, a warning
        may be logged. This is expected behavior and the connection will still
        be removed from the cache.
        """
        with self._cache_lock:
            keys_to_remove = []
            for key, conn in list(self._conn_cache.items()):
                try:
                    conn.close()
                    keys_to_remove.append(key)
                    LOGGER.debug("Closed cached connection: %s", key[1])
                except sqlite3.ProgrammingError as e:
                    # SQLite thread affinity - connection was created in different thread
                    # This is expected when closing from a different thread
                    LOGGER.warning(
                        "Connection %s could not be closed due to thread affinity: %s",
                        key[1],
                        e,
                    )
                except Exception as e:
                    LOGGER.warning("Error closing connection %s: %s", key[1], e)
            for key in keys_to_remove:
                self._conn_cache.pop(key, None)

    def close_thread_connections(self) -> None:
        """
        Close cached connections for the current thread only.

        Should be called by worker threads when they're done to properly
        release resources. This is thread-safe and avoids cross-thread close issues.
        """
        thread_id = threading.get_ident()
        with self._cache_lock:
            keys_to_remove = [k for k in self._conn_cache if k[0] == thread_id]
            for key in keys_to_remove:
                conn = self._conn_cache.pop(key)
                try:
                    conn.close()
                    LOGGER.debug("Closed thread connection: %s", key[1])
                except Exception as e:
                    LOGGER.warning("Error closing thread connection %s: %s", key[1], e)

    # Internal helpers -------------------------------------------------

    def _get_or_create_conn(
        self,
        path: Path,
        *,
        init_case: bool,
        evidence_id: Optional[int] = None,
    ) -> sqlite3.Connection:
        """
        Get or create a cached connection for the given database path.

        Connections are now cached per (thread_id, db_path) to prevent
        creating excessive file descriptors while maintaining thread safety.
        """
        real_path = path.resolve()
        db_exists = real_path.exists()
        thread_id = threading.get_ident()
        cache_key = (thread_id, str(real_path))

        with self._cache_lock:
            # Check if we have a cached connection for this thread+path
            if cache_key in self._conn_cache:
                conn = self._conn_cache[cache_key]
                # Verify connection is still valid
                try:
                    conn.execute("SELECT 1")
                    return conn
                except sqlite3.Error:
                    # Connection is broken, remove from cache
                    LOGGER.debug("Removing stale connection from cache: %s", cache_key)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    del self._conn_cache[cache_key]

        # Create new connection (outside lock to avoid blocking other threads)
        if init_case:
            conn = init_db(self.case_folder, real_path)
        else:
            if not real_path.parent.exists():
                real_path.parent.mkdir(parents=True, exist_ok=True)

            conn = sqlite3.connect(real_path, check_same_thread=False)
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA busy_timeout = 10000;")  # Wait up to 10s for locks

            # Phase 1 performance optimizations for large datasets
            conn.execute("PRAGMA synchronous = NORMAL;")  # Faster writes, safe for forensic use
            conn.execute("PRAGMA cache_size = -64000;")  # 64MB cache (default is ~2MB)
            conn.execute("PRAGMA temp_store = MEMORY;")  # Use RAM for temp tables

            conn.row_factory = sqlite3.Row
            if db_exists:
                _assert_evidence_baseline(conn, real_path)
            migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)
            # Ensure  columns exist (handles v1.8.x upgrade path)
            _ensure_file_list_partition_columns(conn)
            # Ensure  columns exist (handles  upgrade path)
            _ensure_cookies_origin_attributes_columns(conn)
            # Ensure  columns exist (handles  upgrade path)
            _ensure_extensions_preferences_columns(conn)
            # Ensure  columns exist (handles pre- upgrade path)
            _ensure_jump_list_working_directory_column(conn)
            # Ensure  columns exist (handles pre- upgrade path)
            _ensure_browser_history_forensic_columns(conn)
            # Ensure  columns exist (handles pre- upgrade path)
            _ensure_autofill_enhancement_columns(conn)

        # Cache the connection
        with self._cache_lock:
            # Double-check another thread didn't create one while we were connecting
            if cache_key in self._conn_cache:
                # Another thread beat us, close our connection and use theirs
                try:
                    conn.close()
                except Exception:
                    pass
                return self._conn_cache[cache_key]
            self._conn_cache[cache_key] = conn
            LOGGER.debug("Cached new connection: %s (total: %d)", cache_key, len(self._conn_cache))

        return conn


def _ensure_file_list_partition_columns(conn: sqlite3.Connection) -> None:
    """
    Ensure file_list has partition_index and inode columns (upgrade).

    This handles the upgrade path from v1.8.x databases where the 0001 baseline
    didn't include these columns. SQLite doesn't support "ALTER TABLE ... ADD
    COLUMN IF NOT EXISTS", so we check via PRAGMA and add if missing.

    Also updates unique constraints to include partition_index, allowing the same
    file path to exist on different partitions (multi-partition EWF support).
    Handles both named indexes and table-level UNIQUE constraints (auto-indexes).

    Called after migrate() to ensure columns exist before any code uses them.
    """
    # Check if file_list table exists (guard against partially-created DBs)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "file_list" not in tables:
        LOGGER.debug("file_list table does not exist, skipping column upgrade")
        return

    # Check current columns in file_list
    columns = {row[1] for row in conn.execute("PRAGMA table_info(file_list)")}

    needs_commit = False

    if "partition_index" not in columns:
        conn.execute("ALTER TABLE file_list ADD COLUMN partition_index INTEGER DEFAULT -1")
        LOGGER.info("Added partition_index column to file_list (v1.8.x upgrade)")
        needs_commit = True

    if "inode" not in columns:
        conn.execute("ALTER TABLE file_list ADD COLUMN inode TEXT")
        LOGGER.info("Added inode column to file_list (v1.8.x upgrade)")
        needs_commit = True

    # Ensure partition index exists (idempotent)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_file_list_partition
        ON file_list(evidence_id, partition_index)
    """)

    # Always check and fix unique constraints (not just when columns_added)
    # This handles: 1) fresh upgrades, 2) DBs with columns but old index, 3) re-runs
    _ensure_file_list_unique_index(conn)

    if needs_commit:
        conn.commit()


def _ensure_file_list_unique_index(conn: sqlite3.Connection) -> None:
    """
    Ensure file_list unique constraint includes partition_index.

    Handles three cases:
    1. Named index 'idx_file_list_unique_path' without partition_index → drop and recreate
    2. Table-level UNIQUE constraint (auto-index) → recreate table
    3. Correct index already exists → no-op
    """
    # Get all indexes on file_list
    indexes = conn.execute("""
        SELECT name, sql FROM sqlite_master
        WHERE type='index' AND tbl_name='file_list'
    """).fetchall()

    # Check if we already have the correct unique index
    for name, sql in indexes:
        if name == "idx_file_list_unique_path" and sql:
            # Check if it includes partition_index
            if "partition_index" in sql.lower():
                LOGGER.debug("idx_file_list_unique_path already includes partition_index")
                return

    # Check for named index that needs updating
    index_names = {row[0] for row in indexes}
    if "idx_file_list_unique_path" in index_names:
        conn.execute("DROP INDEX idx_file_list_unique_path")
        try:
            conn.execute("""
                CREATE UNIQUE INDEX idx_file_list_unique_path
                ON file_list(evidence_id, COALESCE(partition_index, -1), file_path)
            """)
        except sqlite3.IntegrityError:
            _dedupe_file_list(conn)
            conn.execute("""
                CREATE UNIQUE INDEX idx_file_list_unique_path
                ON file_list(evidence_id, COALESCE(partition_index, -1), file_path)
            """)
        LOGGER.info("Updated idx_file_list_unique_path to include partition_index")
        conn.commit()
        return

    # Check for auto-indexes from table-level UNIQUE constraints
    # These have names like 'sqlite_autoindex_file_list_1'
    auto_indexes = [name for name in index_names if name.startswith("sqlite_autoindex_")]

    if auto_indexes:
        # Table has a UNIQUE constraint - need to recreate the table
        LOGGER.info("Detected table-level UNIQUE constraint, rebuilding file_list table")
        _rebuild_file_list_without_unique_constraint(conn)
        return

    # No conflicting unique constraint found, create the correct one
    try:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_file_list_unique_path
            ON file_list(evidence_id, COALESCE(partition_index, -1), file_path)
        """)
    except sqlite3.IntegrityError:
        _dedupe_file_list(conn)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_file_list_unique_path
            ON file_list(evidence_id, COALESCE(partition_index, -1), file_path)
        """)
    conn.commit()


def _dedupe_file_list(conn: sqlite3.Connection) -> None:
    """
    Remove duplicate file_list rows that violate the unique path constraint.
    """
    dupes = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT evidence_id, COALESCE(partition_index, -1) AS partition_key, file_path
            FROM file_list
            GROUP BY evidence_id, partition_key, file_path
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    if not dupes:
        return
    LOGGER.warning("Deduplicating %d file_list key(s) before recreating unique index", dupes)
    conn.execute("""
        DELETE FROM file_list
        WHERE id NOT IN (
            SELECT MIN(id) FROM file_list
            GROUP BY evidence_id, COALESCE(partition_index, -1), file_path
        )
    """)
    conn.commit()


def _rebuild_file_list_without_unique_constraint(conn: sqlite3.Connection) -> None:
    """
    Rebuild file_list table without table-level UNIQUE constraint.

    SQLite doesn't allow dropping constraints, so we must:
    1. Create a new table without the UNIQUE constraint
    2. Copy all data
    3. Drop old table
    4. Rename new table
    5. Create the correct unique index
    """
    # Get current column definitions (excluding constraints)
    columns_info = conn.execute("PRAGMA table_info(file_list)").fetchall()

    # Build column definitions for new table
    col_defs = []
    for col in columns_info:
        cid, name, type_, notnull, default_value, pk = col
        parts = [name, type_ if type_ else ""]
        if pk:
            parts.append("PRIMARY KEY")
        if notnull and not pk:
            parts.append("NOT NULL")
        if default_value is not None:
            parts.append(f"DEFAULT {default_value}")
        col_defs.append(" ".join(filter(None, parts)))

    col_names = [col[1] for col in columns_info]
    col_list = ", ".join(col_names)
    col_set = set(col_names)

    # Create new table without UNIQUE constraint
    create_sql = f"CREATE TABLE file_list_new ({', '.join(col_defs)})"
    conn.execute(create_sql)

    # Copy data
    conn.execute(f"INSERT INTO file_list_new ({col_list}) SELECT {col_list} FROM file_list")

    # Swap tables
    conn.execute("DROP TABLE file_list")
    conn.execute("ALTER TABLE file_list_new RENAME TO file_list")

    # Create correct indexes (only for columns that exist)
    conn.execute("""
        CREATE UNIQUE INDEX idx_file_list_unique_path
        ON file_list(evidence_id, COALESCE(partition_index, -1), file_path)
    """)

    if "partition_index" in col_set:
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_list_partition
            ON file_list(evidence_id, partition_index)
        """)

    if "extension" in col_set:
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_list_extension
            ON file_list(extension)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_list_evidence_extension
            ON file_list(evidence_id, extension)
        """)

    conn.commit()
    LOGGER.info("Rebuilt file_list table with partition-aware unique index")


def _ensure_extensions_preferences_columns(conn: sqlite3.Connection) -> None:
    """
    Ensure browser_extensions has Preferences-sourced columns (upgrade).

    This handles the upgrade path from  databases where the 0001 baseline
    didn't include these columns. SQLite doesn't support "ALTER TABLE ... ADD
    COLUMN IF NOT EXISTS", so we check via PRAGMA and add if missing.

    Called after migrate() to ensure columns exist before any code uses them.
    """
    # Check if browser_extensions table exists
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "browser_extensions" not in tables:
        LOGGER.debug("browser_extensions table does not exist, skipping column upgrade")
        return

    # Check current columns in browser_extensions
    columns = {row[1] for row in conn.execute("PRAGMA table_info(browser_extensions)")}

    needs_commit = False
    new_columns = [
        ("disable_reasons", "TEXT"),
        ("install_location", "INTEGER"),
        ("install_location_text", "TEXT"),
        ("from_webstore", "INTEGER"),
        ("granted_permissions", "TEXT"),
    ]

    for col_name, col_type in new_columns:
        if col_name not in columns:
            conn.execute(f"ALTER TABLE browser_extensions ADD COLUMN {col_name} {col_type}")
            LOGGER.info("Added %s column to browser_extensions (upgrade)", col_name)
            needs_commit = True

    if needs_commit:
        conn.commit()


def _ensure_browser_history_forensic_columns(conn: sqlite3.Connection) -> None:
    """
    Ensure browser_history has forensic columns (upgrade).

    Adds columns for Chromium visit metadata:
    - transition_type: Navigation type (link, typed, bookmark, etc.)
    - from_visit: Referrer visit ID for navigation chain reconstruction
    - visit_duration_ms: Time spent on page in milliseconds
    - hidden: Whether visit is hidden (subframes, errors)
    - chromium_visit_id: Original visit.id from Chromium database
    - chromium_url_id: Original urls.id from Chromium database

    Called after migrate() to ensure columns exist before any code uses them.
    """
    # Check if browser_history table exists
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "browser_history" not in tables:
        LOGGER.debug("browser_history table does not exist, skipping forensic column upgrade")
        return

    # Check current columns in browser_history
    columns = {row[1] for row in conn.execute("PRAGMA table_info(browser_history)")}

    needs_commit = False
    new_columns = [
        ("transition_type", "INTEGER"),
        ("from_visit", "INTEGER"),
        ("visit_duration_ms", "INTEGER"),
        ("hidden", "INTEGER DEFAULT 0"),
        ("chromium_visit_id", "INTEGER"),
        ("chromium_url_id", "INTEGER"),
    ]

    for col_name, col_type in new_columns:
        if col_name not in columns:
            conn.execute(f"ALTER TABLE browser_history ADD COLUMN {col_name} {col_type}")
            LOGGER.info("Added %s column to browser_history (upgrade)", col_name)
            needs_commit = True

    # Add indexes if they don't exist
    indexes = {row[1] for row in conn.execute("PRAGMA index_list(browser_history)")}
    if "idx_browser_history_transition" not in indexes:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_browser_history_transition ON browser_history(transition_type)")
        LOGGER.info("Added idx_browser_history_transition index")
        needs_commit = True
    if "idx_browser_history_from_visit" not in indexes:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_browser_history_from_visit ON browser_history(from_visit)")
        LOGGER.info("Added idx_browser_history_from_visit index")
        needs_commit = True

    if needs_commit:
        conn.commit()


def _ensure_autofill_enhancement_columns(conn: sqlite3.Connection) -> None:
    """
    Ensure autofill and credentials have  enhancement columns.

    Adds columns for:
    - autofill: field_id_hash (Edge autofill_edge_field_values), is_deleted (deletion tracking)
    - credentials: is_insecure, is_breached, password_notes (security metadata)

    Called after migrate() to ensure columns exist before any code uses them.
    """
    # Get existing tables
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}

    needs_commit = False

    # Check and add autofill columns
    if "autofill" in tables:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(autofill)")}
        autofill_columns = [
            ("field_id_hash", "TEXT"),  # Edge-specific field hash
            ("is_deleted", "INTEGER DEFAULT 0"),  # Deletion flag
        ]
        for col_name, col_type in autofill_columns:
            if col_name not in columns:
                conn.execute(f"ALTER TABLE autofill ADD COLUMN {col_name} {col_type}")
                LOGGER.info("Added %s column to autofill (upgrade)", col_name)
                needs_commit = True

    # Check and add credentials columns
    if "credentials" in tables:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(credentials)")}
        credentials_columns = [
            ("is_insecure", "INTEGER DEFAULT 0"),  # Insecure credentials flag
            ("is_breached", "INTEGER DEFAULT 0"),  # Breached credentials flag
            ("password_notes", "TEXT"),  # User password notes
        ]
        for col_name, col_type in credentials_columns:
            if col_name not in columns:
                conn.execute(f"ALTER TABLE credentials ADD COLUMN {col_name} {col_type}")
                LOGGER.info("Added %s column to credentials (upgrade)", col_name)
                needs_commit = True

    if needs_commit:
        conn.commit()


def _ensure_jump_list_working_directory_column(conn: sqlite3.Connection) -> None:
    """
    Ensure jump_list_entries has working_directory column (upgrade).

    The working_directory field is extracted from LNK files and shows where
    the application was launched from - forensically useful for understanding
    user activity context.

    Called after migrate() to ensure column exists before any code uses it.
    """
    # Check if jump_list_entries table exists
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "jump_list_entries" not in tables:
        LOGGER.debug("jump_list_entries table does not exist, skipping column upgrade")
        return

    # Check current columns in jump_list_entries
    columns = {row[1] for row in conn.execute("PRAGMA table_info(jump_list_entries)")}

    if "working_directory" not in columns:
        conn.execute("ALTER TABLE jump_list_entries ADD COLUMN working_directory TEXT")
        LOGGER.info("Added working_directory column to jump_list_entries (upgrade)")
        conn.commit()


def _ensure_cookies_origin_attributes_columns(conn: sqlite3.Connection) -> None:
    """
    Ensure cookies has Firefox originAttributes columns (upgrade).

    Firefox stores originAttributes in moz_cookies for:
    - Container tabs (userContextId)
    - Private browsing (privateBrowsingId)
    - First-Party Isolation (firstPartyDomain)
    - State Partitioning (partitionKey)

    Also adds samesite_raw for preserving original integer values.

    Called after migrate() to ensure columns exist before any code uses them.
    """
    # Check if cookies table exists
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    if "cookies" not in tables:
        LOGGER.debug("cookies table does not exist, skipping column upgrade")
        return

    # Check current columns in cookies
    columns = {row[1] for row in conn.execute("PRAGMA table_info(cookies)")}

    needs_commit = False
    new_columns = [
        ("origin_attributes", "TEXT"),
        ("user_context_id", "INTEGER"),
        ("private_browsing_id", "INTEGER"),
        ("first_party_domain", "TEXT"),
        ("partition_key", "TEXT"),
        ("samesite_raw", "INTEGER"),
    ]

    for col_name, col_type in new_columns:
        if col_name not in columns:
            conn.execute(f"ALTER TABLE cookies ADD COLUMN {col_name} {col_type}")
            LOGGER.info("Added %s column to cookies (upgrade)", col_name)
            needs_commit = True

    # Add indexes for container/privacy forensics if they don't exist
    indexes = {row[1] for row in conn.execute("PRAGMA index_list(cookies)")}
    if "idx_cookies_user_context" not in indexes and "user_context_id" in columns | {c[0] for c in new_columns}:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cookies_user_context ON cookies(evidence_id, user_context_id)")
        LOGGER.info("Added idx_cookies_user_context index")
        needs_commit = True
    if "idx_cookies_private_browsing" not in indexes and "private_browsing_id" in columns | {c[0] for c in new_columns}:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cookies_private_browsing ON cookies(evidence_id, private_browsing_id)")
        LOGGER.info("Added idx_cookies_private_browsing index")
        needs_commit = True

    if needs_commit:
        conn.commit()


def _assert_evidence_baseline(conn: sqlite3.Connection, db_path: Path) -> None:
    """Reject legacy evidence databases that predate the consolidated baseline."""
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table';"
    ).fetchall()
    if not tables:
        return

    table_names = {row[0] for row in tables}
    if "schema_version" not in table_names:
        raise RuntimeError(
            f"Evidence database {db_path} lacks schema_version; start a fresh case."
        )

    versions = conn.execute(
        "SELECT version FROM schema_version ORDER BY version;"
    ).fetchall()
    if not versions:
        raise RuntimeError(
            f"Evidence database {db_path} has no schema_version entries; start a fresh case."
        )

    # Valid version sequence is always contiguous from 1..N
    version_list = [int(row[0]) for row in versions]
    expected_sequence = list(range(1, len(version_list) + 1))
    if version_list != expected_sequence:
        raise RuntimeError(
            f"Evidence database {db_path} uses legacy migrations ({version_list}); "
            "start a fresh case."
        )

    # Ensure baseline-only tables are present (guards against old version-1 schemas).
    # Requires image_discoveries table for multi-source provenance
    required_tables = {"favicons", "browser_extensions", "image_discoveries"}
    if not required_tables.issubset(table_names):
        missing = sorted(required_tables - table_names)
        raise RuntimeError(
            f"Evidence database {db_path} is missing baseline tables {missing}; start a fresh case."
        )
