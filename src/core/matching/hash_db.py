"""
Hash Database - SQLite-based hash list storage and lookup.

Provides efficient hash lookup via SQLite database instead of in-memory sets.
Useful for large hash lists where memory usage would be prohibitive.

Database schema supports:
- Multiple named hash lists with metadata
- MD5 and SHA256 hashes
- Version tracking via source file hash
- Per-entry notes
"""
from __future__ import annotations

import hashlib
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from core.logging import get_logger

__all__ = [
    "init_hash_db",
    "import_hash_list",
    "rebuild_hash_db",
    "lookup_hash",
    "list_hash_lists",
    "compute_file_hash",
    "parse_hash_line",
    "HASH_DB_SCHEMA",
    "MD5_PATTERN",
    "SHA256_PATTERN",
]

logger = get_logger(__name__)

# Regex patterns for hash detection
MD5_PATTERN = re.compile(r'^[a-fA-F0-9]{32}$')
SHA256_PATTERN = re.compile(r'^[a-fA-F0-9]{64}$')

# Hash DB Schema
HASH_DB_SCHEMA = """
-- Global hash database schema

CREATE TABLE IF NOT EXISTS hash_lists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,           -- e.g., "known_bad_images"
    description TEXT,
    category TEXT,                        -- "blacklist", "whitelist", "reference"
    list_type TEXT DEFAULT 'blacklist',   -- for future whitelist support
    source_file TEXT NOT NULL,            -- original .txt filename
    source_file_hash TEXT,                -- SHA256 of source file (for version tracking)
    entry_count INTEGER DEFAULT 0,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS hash_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    list_id INTEGER NOT NULL,
    hash_md5 TEXT,                        -- MD5 hash (32 hex chars)
    hash_sha256 TEXT,                     -- SHA256 hash (64 hex chars) - optional
    note TEXT,                            -- Per-entry note from source file
    FOREIGN KEY (list_id) REFERENCES hash_lists(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_hash_entries_md5 ON hash_entries(hash_md5);
CREATE INDEX IF NOT EXISTS idx_hash_entries_sha256 ON hash_entries(hash_sha256);
CREATE INDEX IF NOT EXISTS idx_hash_entries_list ON hash_entries(list_id);
"""


def init_hash_db(db_path: Path) -> sqlite3.Connection:
    """
    Initialize or open a hash database.

    Args:
        db_path: Path to SQLite database file

    Returns:
        Open connection to database
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(HASH_DB_SCHEMA)
    return conn


def compute_file_hash(file_path: Path) -> str:
    """Compute SHA256 hash of a file for version tracking."""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def parse_hash_line(line: str) -> Optional[Tuple[str, Optional[str], Optional[str]]]:
    """
    Parse a line from a hash list file.

    Supported formats:
    - d41d8cd98f00b204e9800998ecf8427e
    - d41d8cd98f00b204e9800998ecf8427e, note text
    - d41d8cd98f00b204e9800998ecf8427e  note text
    - # comment (ignored)

    Returns:
        Tuple of (hash_md5, hash_sha256, note) or None if invalid/comment
    """
    line = line.strip()

    # Skip empty lines and comments
    if not line or line.startswith('#'):
        return None

    # Split on comma or whitespace (for note)
    parts = re.split(r'[,\s]+', line, maxsplit=1)
    hash_value = parts[0].strip()
    note = parts[1].strip() if len(parts) > 1 else None

    # Detect hash type
    if MD5_PATTERN.match(hash_value):
        return (hash_value.lower(), None, note)
    elif SHA256_PATTERN.match(hash_value):
        return (None, hash_value.lower(), note)

    # Unknown format
    logger.debug("Skipping unrecognized line: %s", line[:50])
    return None


def import_hash_list(
    txt_path: Path,
    db_path: Path,
    *,
    list_name: Optional[str] = None,
    category: str = "blacklist",
    description: Optional[str] = None,
) -> int:
    """
    Import a text hash list into the SQLite hash database.

    Format: One hash per line, optionally followed by comma/space and note.
    Example:
        d41d8cd98f00b204e9800998ecf8427e
        098f6bcd4621d373cade4e832627b4f6, known malware

    Args:
        txt_path: Path to text file containing hashes
        db_path: Path to SQLite hash database
        list_name: Human-readable list name (default: filename without extension)
        category: List category (blacklist, whitelist, reference)
        description: Optional description

    Returns:
        Number of entries imported
    """
    if not txt_path.exists():
        raise FileNotFoundError(f"Hash list file not found: {txt_path}")

    # Generate list name from filename if not provided
    if list_name is None:
        list_name = txt_path.stem

    # Compute source file hash for version tracking
    source_hash = compute_file_hash(txt_path)

    conn = init_hash_db(db_path)
    now_utc = datetime.now(timezone.utc).isoformat()

    try:
        # Check if this list already exists
        existing = conn.execute(
            "SELECT id, source_file_hash FROM hash_lists WHERE name = ?",
            (list_name,)
        ).fetchone()

        if existing:
            if existing["source_file_hash"] == source_hash:
                logger.info("Hash list '%s' is up to date (same source hash)", list_name)
                return 0

            # Delete old entries for this list
            logger.info("Updating hash list '%s' (source changed)", list_name)
            conn.execute("DELETE FROM hash_entries WHERE list_id = ?", (existing["id"],))
            conn.execute(
                """
                UPDATE hash_lists
                SET source_file = ?, source_file_hash = ?, updated_at_utc = ?
                WHERE id = ?
                """,
                (txt_path.name, source_hash, now_utc, existing["id"])
            )
            list_id = existing["id"]
        else:
            # Create new list
            cursor = conn.execute(
                """
                INSERT INTO hash_lists
                (name, description, category, list_type, source_file, source_file_hash,
                 created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (list_name, description, category, category, txt_path.name, source_hash,
                 now_utc, now_utc)
            )
            list_id = cursor.lastrowid

        # Parse and insert entries
        entries_to_insert: List[Tuple[int, Optional[str], Optional[str], Optional[str]]] = []

        with open(txt_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                parsed = parse_hash_line(line)
                if parsed:
                    md5_hash, sha256_hash, note = parsed
                    entries_to_insert.append((list_id, md5_hash, sha256_hash, note))

        if entries_to_insert:
            conn.executemany(
                "INSERT INTO hash_entries (list_id, hash_md5, hash_sha256, note) VALUES (?, ?, ?, ?)",
                entries_to_insert
            )

        # Update entry count
        conn.execute(
            "UPDATE hash_lists SET entry_count = ? WHERE id = ?",
            (len(entries_to_insert), list_id)
        )

        conn.commit()
        logger.info(
            "Imported %d entries into hash list '%s' (version: %s)",
            len(entries_to_insert), list_name, source_hash[:12]
        )

        return len(entries_to_insert)

    finally:
        conn.close()


def rebuild_hash_db(hashlists_dir: Path, db_path: Path) -> int:
    """
    Rebuild the hash database from all .txt files in a directory.

    Args:
        hashlists_dir: Directory containing .txt hash list files
        db_path: Path to SQLite hash database

    Returns:
        Total number of entries imported
    """
    total = 0
    txt_files = list(hashlists_dir.glob("*.txt"))

    if not txt_files:
        logger.warning("No .txt files found in %s", hashlists_dir)
        return 0

    for txt_path in txt_files:
        try:
            count = import_hash_list(txt_path, db_path)
            total += count
        except Exception as e:
            logger.error("Failed to import %s: %s", txt_path.name, e)

    logger.info("Rebuilt hash database: %d total entries from %d files", total, len(txt_files))
    return total


def lookup_hash(
    db_path: Path,
    *,
    md5: Optional[str] = None,
    sha256: Optional[str] = None,
) -> List[dict]:
    """
    Look up a hash in the database.

    Args:
        db_path: Path to hash database
        md5: MD5 hash to look up
        sha256: SHA256 hash to look up

    Returns:
        List of match dicts with keys: hash_md5, hash_sha256, note, list_name, list_version
    """
    if not db_path.exists():
        return []

    if not md5 and not sha256:
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        conditions = []
        params = []

        if md5:
            conditions.append("he.hash_md5 = ?")
            params.append(md5.lower())
        if sha256:
            conditions.append("he.hash_sha256 = ?")
            params.append(sha256.lower())

        sql = f"""
            SELECT he.hash_md5, he.hash_sha256, he.note,
                   hl.name as list_name, hl.source_file_hash as list_version,
                   hl.category
            FROM hash_entries he
            JOIN hash_lists hl ON he.list_id = hl.id
            WHERE {' OR '.join(conditions)}
        """

        cursor = conn.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]

    finally:
        conn.close()


def list_hash_lists(db_path: Path) -> List[dict]:
    """
    List all hash lists in the database.

    Returns:
        List of list info dicts with keys: name, description, category, entry_count,
        source_file, source_file_hash, created_at_utc, updated_at_utc
    """
    if not db_path.exists():
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        cursor = conn.execute(
            """
            SELECT name, description, category, entry_count, source_file,
                   source_file_hash, created_at_utc, updated_at_utc
            FROM hash_lists
            ORDER BY name
            """
        )
        return [dict(row) for row in cursor.fetchall()]

    finally:
        conn.close()
