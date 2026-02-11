"""
Resilient SQLite Parser for Browser Carver

Best-effort SQLite parsing for potentially corrupted databases
recovered from unallocated space.

Recovery levels:
1. Standard sqlite3 open
2. sqlite3 with PRAGMA ignore_check_constraints
3. Raw string scanning for URLs
4. Raw page scanning (deep scan)
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Any, Optional

from core.logging import get_logger

LOGGER = get_logger("extractors.browser_carver.resilient_sqlite")


# URL pattern for raw scanning
URL_PATTERN = re.compile(
    rb'https?://[a-zA-Z0-9][a-zA-Z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+',
    re.IGNORECASE
)


def parse_sqlite_best_effort(filepath: Path) -> Dict[str, List[Dict[str, Any]]]:
    """
    Attempt to read SQLite with increasing levels of recovery.

    Args:
        filepath: Path to SQLite database

    Returns:
        Dictionary mapping table names to list of row dicts
    """
    # Level 1: Standard open
    result = _try_standard_open(filepath)
    if result:
        return result

    # Level 2: With pragma tweaks
    result = _try_pragma_open(filepath)
    if result:
        return result

    # Level 3: Raw URL scanning
    urls = scan_for_urls(filepath.read_bytes())
    if urls:
        return {"raw_urls": [{"url": u} for u in urls]}

    return {}


def _try_standard_open(filepath: Path) -> Optional[Dict[str, List[Dict[str, Any]]]]:
    """Try standard sqlite3 open."""
    try:
        conn = sqlite3.connect(f"file:{filepath}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Get tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]

        result = {}
        for table in tables:
            try:
                cursor.execute(f"SELECT * FROM {table} LIMIT 10000")
                rows = [dict(row) for row in cursor.fetchall()]
                result[table] = rows
            except sqlite3.DatabaseError:
                continue

        conn.close()
        return result if result else None

    except sqlite3.DatabaseError as e:
        LOGGER.debug("Standard open failed: %s", e)
        return None


def _try_pragma_open(filepath: Path) -> Optional[Dict[str, List[Dict[str, Any]]]]:
    """Try opening with pragma tweaks for corrupted DBs."""
    try:
        conn = sqlite3.connect(f"file:{filepath}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Try to ignore some constraints
        try:
            cursor.execute("PRAGMA ignore_check_constraints = ON")
        except Exception:
            pass

        try:
            cursor.execute("PRAGMA writable_schema = ON")
        except Exception:
            pass

        # Get tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]

        result = {}
        for table in tables:
            try:
                cursor.execute(f"SELECT * FROM {table} LIMIT 10000")
                rows = [dict(row) for row in cursor.fetchall()]
                result[table] = rows
            except sqlite3.DatabaseError:
                continue

        conn.close()
        return result if result else None

    except sqlite3.DatabaseError as e:
        LOGGER.debug("Pragma open failed: %s", e)
        return None


def scan_for_urls(data: bytes) -> List[str]:
    """
    Scan raw bytes for HTTP/HTTPS URL patterns.

    Args:
        data: Raw file bytes

    Returns:
        List of discovered URLs
    """
    urls = []

    for match in URL_PATTERN.finditer(data):
        try:
            url = match.group().decode('utf-8', errors='ignore')
            # Basic validation
            if len(url) > 10 and len(url) < 2048:
                # Filter out obvious noise
                if not _is_noise_url(url):
                    urls.append(url)
        except Exception:
            continue

    return urls


def _is_noise_url(url: str) -> bool:
    """Check if URL is likely noise/garbage."""
    # Too many repeating chars
    if len(set(url)) < 10:
        return True

    # Too many non-printable
    printable = sum(1 for c in url if c.isprintable())
    if printable / len(url) < 0.9:
        return True

    # Common browser internal URLs
    noise_prefixes = [
        "http://127.0.0.1",
        "http://localhost",
        "https://localhost",
        "http://0.0.0.0",
        "chrome://",
        "chrome-extension://",
        "moz-extension://",
        "edge://",
        "about:",
        "data:",
    ]

    for prefix in noise_prefixes:
        if url.lower().startswith(prefix):
            return True

    return False


def scan_for_timestamps(data: bytes) -> List[tuple[str, int]]:
    """
    Scan for URL + WebKit timestamp pairs.

    WebKit timestamps are microseconds since 1601-01-01.
    Look for URL followed by 8-byte integer in plausible range.

    Args:
        data: Raw file bytes

    Returns:
        List of (url, timestamp) tuples
    """
    results = []

    # This is a simplified heuristic - real implementation would
    # need to understand SQLite page structure

    urls = scan_for_urls(data)
    # For now just return URLs without timestamps
    return [(url, 0) for url in urls]
