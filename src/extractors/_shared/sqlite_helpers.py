"""
Safe SQLite helpers for browser extractors.

Provides utilities for safely reading SQLite databases from evidence:
- Read-only connections to prevent modification
- Automatic copying to handle locked databases
- Error handling for corrupt/incomplete databases

Design Principle:
    Evidence databases should NEVER be modified. These helpers ensure
    all access is read-only and handles common forensic scenarios.
"""

from __future__ import annotations

import shutil
import sqlite3
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, List, Optional, Tuple, Union


class SQLiteReadError(Exception):
    """Raised when SQLite database cannot be read."""
    pass


@contextmanager
def safe_sqlite_connect(
    db_path: Union[str, Path],
    copy_first: bool = False,
    timeout: float = 5.0,
) -> Iterator[sqlite3.Connection]:
    """
    Safely connect to SQLite database in read-only mode.

    Args:
        db_path: Path to the SQLite database file
        copy_first: If True, copy database to temp location before opening
                   (useful for locked databases or evidence preservation)
        timeout: Connection timeout in seconds

    Yields:
        sqlite3.Connection in read-only mode

    Raises:
        SQLiteReadError: If database cannot be opened
        FileNotFoundError: If database file doesn't exist

    Example:
        with safe_sqlite_connect("/path/to/History") as conn:
            cursor = conn.execute("SELECT url FROM urls")
            for row in cursor:
                print(row[0])
    """
    db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    temp_copy: Optional[Path] = None
    conn: Optional[sqlite3.Connection] = None

    try:
        if copy_first:
            # Copy to temp location to avoid locks and preserve evidence
            temp_copy = copy_sqlite_for_reading(db_path)
            target_path = temp_copy
        else:
            target_path = db_path

        # Open in read-only mode using URI
        uri = f"file:{target_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=timeout)
        conn.row_factory = sqlite3.Row  # Enable column access by name

        yield conn

    except sqlite3.Error as e:
        raise SQLiteReadError(f"Failed to open database {db_path}: {e}") from e

    finally:
        if conn:
            conn.close()
        if temp_copy and temp_copy.exists():
            temp_copy.unlink()


def copy_sqlite_for_reading(
    db_path: Union[str, Path],
    include_wal: bool = True,
    dest_dir: Optional[Path] = None,
) -> Path:
    """
    Copy SQLite database and associated files for safe reading.

    Copies the main database file and optionally WAL/journal files
    to a temporary location. This is necessary when:
    - Database may be locked by another process
    - Evidence integrity must be preserved
    - WAL mode recovery is needed

    Args:
        db_path: Path to the SQLite database
        include_wal: If True, also copy -wal, -journal, -shm files
        dest_dir: Destination directory (default: system temp)

    Returns:
        Path to the copied database

    Note:
        Caller is responsible for cleaning up the copied files.
    """
    db_path = Path(db_path)

    if dest_dir is None:
        dest_dir = Path(tempfile.mkdtemp(prefix="sqlite_copy_"))
    else:
        dest_dir = Path(dest_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)

    # Copy main database
    dest_db = dest_dir / db_path.name
    shutil.copy2(db_path, dest_db)

    # Copy associated files if they exist
    if include_wal:
        for suffix in ["-wal", "-journal", "-shm"]:
            companion = db_path.parent / (db_path.name + suffix)
            if companion.exists():
                shutil.copy2(companion, dest_dir / companion.name)

    return dest_db


def safe_execute(
    conn: sqlite3.Connection,
    query: str,
    params: Tuple[Any, ...] = (),
    fetch_all: bool = True,
) -> Union[List[sqlite3.Row], sqlite3.Cursor]:
    """
    Safely execute a query with error handling.

    Args:
        conn: SQLite connection
        query: SQL query to execute
        params: Query parameters
        fetch_all: If True, return all rows; otherwise return cursor

    Returns:
        List of rows if fetch_all=True, else cursor

    Raises:
        SQLiteReadError: If query execution fails

    Example:
        rows = safe_execute(conn, "SELECT url FROM urls WHERE id = ?", (1,))
        for row in rows:
            print(row["url"])
    """
    try:
        cursor = conn.execute(query, params)
        if fetch_all:
            return cursor.fetchall()
        return cursor
    except sqlite3.Error as e:
        raise SQLiteReadError(f"Query execution failed: {e}") from e


def get_table_names(conn: sqlite3.Connection) -> List[str]:
    """
    Get list of table names in database.

    Args:
        conn: SQLite connection

    Returns:
        List of table names
    """
    rows = safe_execute(
        conn,
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    return [row["name"] for row in rows]


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    Check if a table exists in the database.

    Args:
        conn: SQLite connection
        table_name: Name of table to check

    Returns:
        True if table exists
    """
    rows = safe_execute(
        conn,
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return len(rows) > 0


def get_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    """
    Get row count for a table.

    Args:
        conn: SQLite connection
        table_name: Name of table

    Returns:
        Number of rows in table
    """
    # Use parameterized table name would be better but SQLite doesn't support it
    # We validate table exists first to prevent injection
    if not table_exists(conn, table_name):
        return 0

    rows = safe_execute(conn, f"SELECT COUNT(*) as cnt FROM [{table_name}]")
    return rows[0]["cnt"] if rows else 0
