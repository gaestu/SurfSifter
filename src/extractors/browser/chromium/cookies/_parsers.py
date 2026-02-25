"""
Chromium Cookie SQLite database parser.

Parses the Cookies database from Chromium-based browsers (Chrome, Edge, Brave, Opera).
All Chromium browsers use an identical schema, so one parser works for all.

Features:
- Full cookie metadata extraction
- Encrypted value detection (DPAPI on Windows, Keychain on macOS)
- SameSite attribute parsing with unknown value tracking
- Schema warning support for unknown columns
- WebKit timestamp conversion

Usage:
    from extractors.browser.chromium.cookies._parsers import (
        parse_cookies,
        get_cookie_stats,
        ChromiumCookie,
    )

    with safe_sqlite_connect(cookies_path) as conn:
        for cookie in parse_cookies(conn):
            print(f"{cookie.host_key}: {cookie.name}")
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterator, Optional, Set, TYPE_CHECKING

from core.logging import get_logger
from ...._shared.sqlite_helpers import safe_execute, table_exists
from ...._shared.timestamps import webkit_to_datetime, webkit_to_iso
from ._schemas import (
    KNOWN_COOKIES_COLUMNS,
    KNOWN_SAMESITE_VALUES,
    LEGACY_COLUMN_ALIASES,
    LEGACY_COLUMN_NAMES,
    SAMESITE_VALUES,
    get_samesite_name,
)

LOGGER = get_logger("extractors.chromium.cookies.parsers")

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector


# =============================================================================
# Column Name Resolution (Legacy Schema Support)
# =============================================================================

def _resolve_column_names(conn: sqlite3.Connection) -> Optional[Dict[str, str]]:
    """
    Resolve modern column names to whatever the actual database uses.

    Old Chromium (<67) and CefSharp/CEF embedded browsers used shorter
    column names that were later prefixed with ``is_`` in modern Chromium.
    This function checks which variant is present and returns a mapping
    from the *modern* name to the *actual* column name in the database.

    Returns:
        Dict mapping modern name → actual column name found in the DB,
        or ``None`` if the ``cookies`` table cannot be introspected.
    """
    try:
        cursor = conn.execute("PRAGMA table_info(cookies)")
        actual_columns = {row[1].lower() for row in cursor.fetchall()}
    except Exception:
        return None

    result: Dict[str, str] = {}
    for modern_name, legacy_alternatives in LEGACY_COLUMN_ALIASES.items():
        if modern_name in actual_columns:
            result[modern_name] = modern_name
        else:
            # Try legacy alternatives
            for legacy in legacy_alternatives:
                if legacy in actual_columns:
                    result[modern_name] = legacy
                    LOGGER.debug(
                        "Legacy column detected: %s → %s", modern_name, legacy,
                    )
                    break
            else:
                # Neither modern nor legacy found — use modern name and let
                # the query fail gracefully downstream
                result[modern_name] = modern_name

    return result


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ChromiumCookie:
    """A single Chromium cookie record with full forensic context."""

    # Core cookie data
    host_key: str
    name: str
    value: str
    path: str

    # Timestamps (converted from WebKit format)
    creation_utc: Optional[datetime]
    creation_utc_iso: Optional[str]
    expires_utc: Optional[datetime]
    expires_utc_iso: Optional[str]
    last_access_utc: Optional[datetime]
    last_access_utc_iso: Optional[str]

    # Security flags
    is_secure: bool
    is_httponly: bool
    samesite: str           # Human-readable string
    samesite_raw: int       # Original integer value for forensic record

    # Persistence
    is_persistent: bool
    has_expires: bool
    priority: int

    # Encryption
    encrypted_value: Optional[bytes]
    is_encrypted: bool


# =============================================================================
# Parsing Functions
# =============================================================================

def discover_and_warn_unknown_columns(
    conn: sqlite3.Connection,
    warning_collector: Optional["ExtractionWarningCollector"],
    source_file: str,
) -> None:
    """
    Discover unknown columns in the cookies table and add warnings.

    Args:
        conn: SQLite connection to Cookies database
        warning_collector: Optional collector for schema warnings
        source_file: Source file path for warning context
    """
    if warning_collector is None:
        return

    if not table_exists(conn, "cookies"):
        return

    try:
        # Get actual columns from database
        cursor = conn.execute("PRAGMA table_info(cookies)")
        actual_columns = {row[1] for row in cursor.fetchall()}

        # Find columns we don't know about
        unknown_columns = actual_columns - KNOWN_COOKIES_COLUMNS

        # Also check for columns we know about but don't parse
        # (these are informational, not warnings)
        from ._schemas import KNOWN_BUT_NOT_PARSED_COLUMNS
        truly_unknown = unknown_columns - KNOWN_BUT_NOT_PARSED_COLUMNS

        # Report truly unknown columns as warnings
        for col in truly_unknown:
            warning_collector.add_unknown_column(
                table_name="cookies",
                column_name=col,
                column_type="unknown",  # PRAGMA doesn't give type reliably
                source_file=source_file,
                artifact_type="cookies",
            )

        # Report known-but-not-parsed columns as info
        for col in (unknown_columns & KNOWN_BUT_NOT_PARSED_COLUMNS):
            warning_collector.add_warning(
                warning_type="unknown_column",
                item_name=col,
                severity="info",
                category="database",
                artifact_type="cookies",
                source_file=source_file,
                item_value="Known column not currently extracted",
            )

    except Exception:
        # Don't fail extraction if column discovery fails
        pass


def parse_cookies(
    conn: sqlite3.Connection,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    source_file: Optional[str] = None,
) -> Iterator[ChromiumCookie]:
    """
    Parse Chromium Cookies database.

    Args:
        conn: SQLite connection to Cookies database
        warning_collector: Optional collector for schema warnings
        source_file: Source file path for warning context

    Yields:
        ChromiumCookie records ordered by creation_utc DESC

    Note:
        Chromium cookies may have encrypted_value (DPAPI on Windows, Keychain on macOS).
        If encrypted_value is non-empty and value is empty, the cookie is encrypted.
    """
    if not table_exists(conn, "cookies"):
        return

    # Discover unknown columns for schema warnings
    if warning_collector and source_file:
        discover_and_warn_unknown_columns(conn, warning_collector, source_file)

    # Detect actual column names to handle legacy schemas (Chromium <67, CefSharp)
    column_map = _resolve_column_names(conn)
    if column_map is None:
        # Could not determine columns — table may be corrupt
        LOGGER.warning("Cannot determine cookies table columns for %s", source_file)
        return

    col_secure = column_map.get("is_secure", "is_secure")
    col_httponly = column_map.get("is_httponly", "is_httponly")
    col_persistent = column_map.get("is_persistent", "is_persistent")
    col_samesite = column_map.get("samesite", "samesite")

    # Track samesite values we encounter
    found_samesite_values: Set[int] = set()

    # Query all cookie fields — column names are resolved dynamically
    # to support legacy schemas (Chromium <67 used "secure" instead of
    # "is_secure", "httponly" instead of "is_httponly", etc.)
    query = f"""
        SELECT
            host_key,
            name,
            value,
            path,
            creation_utc,
            expires_utc,
            last_access_utc,
            {col_secure} as is_secure,
            {col_httponly} as is_httponly,
            COALESCE({col_samesite}, -1) as samesite,
            {col_persistent} as is_persistent,
            has_expires,
            COALESCE(priority, 1) as priority,
            encrypted_value
        FROM cookies
        ORDER BY creation_utc DESC
    """

    try:
        rows = safe_execute(conn, query)
    except Exception:
        LOGGER.warning("Failed to query cookies table for %s", source_file, exc_info=True)
        return

    for row in rows:
        # Track samesite value
        samesite_raw = row["samesite"]
        found_samesite_values.add(samesite_raw)

        # Detect if cookie is encrypted
        encrypted_value = row["encrypted_value"]
        is_encrypted = bool(encrypted_value and len(encrypted_value) > 0 and not row["value"])

        # Convert timestamps
        creation_dt = webkit_to_datetime(row["creation_utc"])
        creation_iso = webkit_to_iso(row["creation_utc"])
        expires_dt = webkit_to_datetime(row["expires_utc"]) if row["has_expires"] else None
        expires_iso = webkit_to_iso(row["expires_utc"]) if row["has_expires"] else None
        last_access_dt = webkit_to_datetime(row["last_access_utc"])
        last_access_iso = webkit_to_iso(row["last_access_utc"])

        yield ChromiumCookie(
            host_key=row["host_key"],
            name=row["name"],
            value=row["value"] or "",
            path=row["path"],
            creation_utc=creation_dt,
            creation_utc_iso=creation_iso,
            expires_utc=expires_dt,
            expires_utc_iso=expires_iso,
            last_access_utc=last_access_dt,
            last_access_utc_iso=last_access_iso,
            is_secure=bool(row["is_secure"]),
            is_httponly=bool(row["is_httponly"]),
            samesite=get_samesite_name(samesite_raw),
            samesite_raw=samesite_raw,
            is_persistent=bool(row["is_persistent"]),
            has_expires=bool(row["has_expires"]),
            priority=row["priority"],
            encrypted_value=encrypted_value if is_encrypted else None,
            is_encrypted=is_encrypted,
        )

    # Report unknown samesite values
    if warning_collector and source_file:
        unknown_samesite = found_samesite_values - KNOWN_SAMESITE_VALUES
        for value in unknown_samesite:
            warning_collector.add_unknown_enum_value(
                enum_name="samesite",
                enum_value=value,
                source_file=source_file,
                artifact_type="cookies",
            )


def get_cookie_stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    """
    Get quick statistics from Cookies database.

    Args:
        conn: SQLite connection to Cookies database

    Returns:
        Dict with cookie_count, encrypted_count, domain_count
    """
    stats: Dict[str, Any] = {
        "cookie_count": 0,
        "encrypted_count": 0,
        "domain_count": 0,
    }

    if not table_exists(conn, "cookies"):
        return stats

    try:
        rows = safe_execute(conn, "SELECT COUNT(*) as cnt FROM cookies")
        stats["cookie_count"] = rows[0]["cnt"] if rows else 0

        rows = safe_execute(conn, """
            SELECT COUNT(*) as cnt FROM cookies
            WHERE encrypted_value IS NOT NULL AND LENGTH(encrypted_value) > 0
        """)
        stats["encrypted_count"] = rows[0]["cnt"] if rows else 0

        rows = safe_execute(conn, "SELECT COUNT(DISTINCT host_key) as cnt FROM cookies")
        stats["domain_count"] = rows[0]["cnt"] if rows else 0
    except Exception:
        pass

    return stats


# =============================================================================
# Re-exports for backward compatibility
# =============================================================================
# These are re-exported in the parent _parsers.py module

__all__ = [
    "ChromiumCookie",
    "parse_cookies",
    "get_cookie_stats",
]
