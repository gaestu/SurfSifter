"""
Database connection and migration utilities.

This module provides:
- init_db: Open/create database with migrations
- migrate: Execute pending SQL migrations
- SQLite type adapters for JSON, Path

Extracted from db.py during database refactor.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Set

from core.logging import get_logger

LOGGER = get_logger("core.database.connection")

# =============================================================================
# SQLite Type Adapters
# =============================================================================

def _adapt_json_value(value: Any) -> str:
    """Convert dict/list to JSON string for SQLite storage."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


# Register adapters globally for sqlite3
# This allows insert helpers/extractors to pass JSON-like Python values safely.
# Prevents sqlite3.ProgrammingError: "type 'dict' is not supported"
sqlite3.register_adapter(dict, _adapt_json_value)
sqlite3.register_adapter(list, _adapt_json_value)
sqlite3.register_adapter(Path, lambda p: str(p))


# =============================================================================
# Database Initialization
# =============================================================================

def init_db(case_folder: Path, db_path: Path) -> sqlite3.Connection:
    """
    Open (or create) the SQLite database for a case and run migrations.

    Args:
        case_folder: The case folder directory
        db_path: Explicit path to the database file

    Returns:
        SQLite connection with migrations applied
    """
    case_folder.mkdir(parents=True, exist_ok=True)
    LOGGER.debug("Opening case database at %s", db_path)
    # Allow safe close from cleanup threads while DatabaseManager still keeps one
    # connection per thread/path cache entry.
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 10000;")  # Wait up to 10s for locks
    conn.row_factory = sqlite3.Row
    migrate(conn)
    return conn


# =============================================================================
# Migration System
# =============================================================================

def migrate(conn: sqlite3.Connection, migrations_dir: Optional[Path] = None) -> None:
    """Execute pending SQL migrations."""
    if migrations_dir is None:
        migrations_dir = Path(__file__).resolve().parent / "migrations"
    _ensure_schema_table(conn)
    applied_versions = _fetch_applied_versions(conn)

    for migration_path in sorted(migrations_dir.glob("*.sql")):
        version = _extract_version(migration_path.name)
        if version in applied_versions:
            continue
        LOGGER.info("Applying migration %s", migration_path.name)
        try:
            with conn:
                sql = migration_path.read_text(encoding="utf-8")
                conn.executescript(sql)
                conn.execute(
                    "INSERT INTO schema_version(version, applied_at_utc) VALUES (?, ?)",
                    (version, _utc_now()),
                )
        except sqlite3.DatabaseError as exc:
            LOGGER.exception("Migration %s failed", migration_path.name)
            raise RuntimeError(f"Failed to apply migration {migration_path}") from exc


def _ensure_schema_table(conn: sqlite3.Connection) -> None:
    """Create schema_version table if it doesn't exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            version INTEGER PRIMARY KEY,
            applied_at_utc TEXT NOT NULL
        );
        """
    )


def _fetch_applied_versions(conn: sqlite3.Connection) -> Set[int]:
    """Get set of already-applied migration versions."""
    rows = conn.execute("SELECT version FROM schema_version;").fetchall()
    return {int(row[0]) for row in rows}


def _extract_version(filename: str) -> int:
    """Extract version number from migration filename (e.g., '0001_foo.sql' -> 1)."""
    prefix = filename.split("_", 1)[0]
    return int(prefix)


def _utc_now() -> str:
    """Return current UTC timestamp in ISO 8601 format."""
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()

