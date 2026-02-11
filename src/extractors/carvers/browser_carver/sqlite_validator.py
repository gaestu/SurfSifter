"""
SQLite Validator for Browser Carver

Validates carved SQLite files to identify browser-related databases.
Checks table structure against known browser schemas.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional, Set

from core.logging import get_logger

LOGGER = get_logger("extractors.browser_carver.sqlite_validator")


# Known browser database schemas
BROWSER_SCHEMAS = {
    "history": {
        # Chromium History
        "chromium": {"urls", "visits"},
        # Firefox places.sqlite is handled separately
    },
    "cookies": {
        # Chromium Cookies
        "chromium": {"cookies"},
        # Firefox cookies.sqlite
        "firefox": {"moz_cookies"},
    },
    "webdata": {
        # Chromium Web Data
        "chromium": {"autofill", "autofill_profiles"},
    },
    "places": {
        # Firefox places.sqlite
        "firefox": {"moz_places", "moz_historyvisits"},
    },
    "logins": {
        # Chromium Login Data
        "chromium": {"logins"},
    },
}


def identify_browser_db(filepath: Path) -> Optional[str]:
    """
    Identify if a SQLite file is browser-related.

    Args:
        filepath: Path to SQLite file

    Returns:
        Database type string ('history', 'cookies', 'places', etc.)
        or None if not browser-related
    """
    if not filepath.exists():
        return None

    # Check SQLite header
    try:
        header = filepath.read_bytes()[:16]
        if not header.startswith(b"SQLite format 3"):
            return None
    except (IOError, OSError):
        return None

    # Try to open and check tables
    try:
        conn = sqlite3.connect(f"file:{filepath}?mode=ro", uri=True)
        cursor = conn.cursor()

        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0].lower() for row in cursor.fetchall()}

        conn.close()

        return _match_schema(tables)

    except sqlite3.DatabaseError as e:
        LOGGER.debug("SQLite error for %s: %s", filepath, e)
        return None
    except Exception as e:
        LOGGER.debug("Error checking %s: %s", filepath, e)
        return None


def _match_schema(tables: Set[str]) -> Optional[str]:
    """Match table set against known browser schemas."""

    # Check for Chromium History
    if "urls" in tables and "visits" in tables:
        return "history"

    # Check for Firefox places.sqlite
    if "moz_places" in tables and "moz_historyvisits" in tables:
        return "places"

    # Check for Chromium Cookies
    if "cookies" in tables:
        return "cookies"

    # Check for Firefox cookies.sqlite
    if "moz_cookies" in tables:
        return "cookies"

    # Check for Chromium Web Data
    if "autofill" in tables and "autofill_profiles" in tables:
        return "webdata"

    # Check for Chromium Login Data
    if "logins" in tables:
        return "logins"

    # Check for Firefox form history
    if "moz_formhistory" in tables:
        return "formhistory"

    return None


def get_table_row_count(filepath: Path, table: str) -> int:
    """Get row count for a table (best-effort)."""
    try:
        conn = sqlite3.connect(f"file:{filepath}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return 0
