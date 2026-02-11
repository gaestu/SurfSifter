"""
Firefox Favicons parser functions.

This module contains parser functions for extracting favicon data from
Firefox favicons.sqlite databases. Supports both modern (Firefox 55+)
and legacy (Firefox < 55) schemas.

All parsers accept an optional ExtractionWarningCollector to report:
- Unknown columns in known tables
- Unknown enum/token values
- Parse errors

Initial implementation with schema warning support
"""

from __future__ import annotations

import hashlib
import sqlite3
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from ._schemas import (
    KNOWN_FAVICONS_TABLES,
    FAVICONS_TABLE_PATTERNS,
    KNOWN_MOZ_ICONS_COLUMNS,
    KNOWN_MOZ_PAGES_W_ICONS_COLUMNS,
    KNOWN_MOZ_ICONS_TO_PAGES_COLUMNS,
    KNOWN_MOZ_FAVICONS_COLUMNS,
    ICON_ROOT_TYPES,
    get_known_columns_for_table,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.firefox.favicons._parsers")

# Maximum icon size to store (1MB)
MAX_ICON_SIZE_BYTES = 1 * 1024 * 1024


# =============================================================================
# Helper Functions
# =============================================================================

def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """Check if a table exists in the database."""
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cursor.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    """Get column names for a table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info('{table_name}')")
    return {row[1] for row in cursor.fetchall()}


def get_all_tables(conn: sqlite3.Connection) -> Set[str]:
    """Get all table names in the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row[0] for row in cursor.fetchall()}


def discover_and_warn_unknown_tables(
    conn: sqlite3.Connection,
    source_file: str,
    artifact_type: str,
    warning_collector: Optional["ExtractionWarningCollector"],
) -> None:
    """Discover unknown tables and add warnings."""
    if not warning_collector:
        return

    from extractors._shared.extraction_warnings import discover_unknown_tables

    unknown_tables = discover_unknown_tables(
        conn, KNOWN_FAVICONS_TABLES, FAVICONS_TABLE_PATTERNS
    )

    for table_info in unknown_tables:
        warning_collector.add_unknown_table(
            table_name=table_info["name"],
            columns=table_info["columns"],
            source_file=source_file,
            artifact_type=artifact_type,
        )


def discover_and_warn_unknown_columns(
    conn: sqlite3.Connection,
    table_name: str,
    known_columns: Set[str],
    source_file: str,
    artifact_type: str,
    warning_collector: Optional["ExtractionWarningCollector"],
) -> Set[str]:
    """
    Get table columns and warn about any unknown ones.

    Returns the set of actual columns in the table.
    """
    columns = get_table_columns(conn, table_name)

    if warning_collector:
        from extractors._shared.extraction_warnings import discover_unknown_columns

        unknown = discover_unknown_columns(conn, table_name, known_columns)
        for col_info in unknown:
            warning_collector.add_unknown_column(
                table_name=table_name,
                column_name=col_info["name"],
                column_type=col_info["type"],
                source_file=source_file,
                artifact_type=artifact_type,
            )

    return columns


def track_and_warn_unknown_root_values(
    found_values: Set[int],
    source_file: str,
    warning_collector: Optional["ExtractionWarningCollector"],
) -> None:
    """Track unknown root flag values and add warnings."""
    if not warning_collector or not found_values:
        return

    from extractors._shared.extraction_warnings import track_unknown_values

    unknown = track_unknown_values(ICON_ROOT_TYPES, found_values)
    for value in unknown:
        warning_collector.add_unknown_enum_value(
            enum_name="ICON_ROOT_TYPE",
            value=value,
            source_file=source_file,
            artifact_type="favicons",
            context={"table": "moz_icons", "column": "root"},
        )


# =============================================================================
# Modern Schema Parsers (Firefox 55+)
# =============================================================================

def parse_moz_icons(
    conn: sqlite3.Connection,
    source_file: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse moz_icons table (Firefox 55+).

    Returns list of icon records with:
    - id: Original icon ID (for mapping)
    - icon_url: URL of the icon
    - width: Icon width in pixels
    - root: 0=favicon, 1=touch_icon
    - expire_ms: Expiration timestamp
    - data: Binary icon data
    - icon_sha256: SHA256 hash of icon data
    - icon_md5: MD5 hash of icon data
    """
    if not table_exists(conn, "moz_icons"):
        return []

    # Check columns and warn about unknowns
    columns = discover_and_warn_unknown_columns(
        conn, "moz_icons", KNOWN_MOZ_ICONS_COLUMNS,
        source_file, "favicons", warning_collector
    )

    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row

    # Build dynamic SELECT based on available columns
    select_cols = ["id", "icon_url", "width", "data"]
    if "root" in columns:
        select_cols.append("root")
    if "expire_ms" in columns:
        select_cols.append("expire_ms")
    if "fixed_icon_url_hash" in columns:
        select_cols.append("fixed_icon_url_hash")

    cursor.execute(f"""
        SELECT {', '.join(select_cols)}
        FROM moz_icons
        WHERE data IS NOT NULL
    """)

    records = []
    found_root_values: Set[int] = set()

    for row in cursor:
        icon_data = row["data"]
        if icon_data is None or len(icon_data) > MAX_ICON_SIZE_BYTES:
            continue

        root_value = row["root"] if "root" in row.keys() else 0
        found_root_values.add(root_value)

        record = {
            "id": row["id"],
            "icon_url": row["icon_url"],
            "width": row["width"],
            "root": root_value,
            "icon_type": 2 if root_value == 1 else 1,  # 2=touch_icon if root
            "expire_ms": row["expire_ms"] if "expire_ms" in row.keys() else None,
            "data": icon_data,
            "icon_sha256": hashlib.sha256(icon_data).hexdigest(),
            "icon_md5": hashlib.md5(icon_data).hexdigest(),
        }
        records.append(record)

    # Warn about unknown root values
    track_and_warn_unknown_root_values(found_root_values, source_file, warning_collector)

    return records


def parse_page_mappings(
    conn: sqlite3.Connection,
    icon_ids: Set[int],
    source_file: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse page-to-icon mappings from moz_icons_to_pages and moz_pages_w_icons.

    Args:
        conn: SQLite connection
        icon_ids: Set of icon IDs to filter mappings for
        source_file: Source file path for warnings
        warning_collector: Optional warning collector

    Returns list of mapping records with:
    - icon_id: Reference to moz_icons.id
    - page_url: Page URL associated with this icon
    """
    if not icon_ids:
        return []

    if not table_exists(conn, "moz_icons_to_pages") or not table_exists(conn, "moz_pages_w_icons"):
        return []

    # Check columns and warn about unknowns
    discover_and_warn_unknown_columns(
        conn, "moz_icons_to_pages", KNOWN_MOZ_ICONS_TO_PAGES_COLUMNS,
        source_file, "favicons", warning_collector
    )
    discover_and_warn_unknown_columns(
        conn, "moz_pages_w_icons", KNOWN_MOZ_PAGES_W_ICONS_COLUMNS,
        source_file, "favicons", warning_collector
    )

    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row

    cursor.execute("""
        SELECT itp.icon_id, p.page_url
        FROM moz_icons_to_pages itp
        JOIN moz_pages_w_icons p ON itp.page_id = p.id
    """)

    mappings = []
    for row in cursor:
        icon_id = row["icon_id"]
        if icon_id in icon_ids:
            mappings.append({
                "icon_id": icon_id,
                "page_url": row["page_url"],
            })

    return mappings


# =============================================================================
# Legacy Schema Parser (Firefox < 55)
# =============================================================================

def parse_moz_favicons_legacy(
    conn: sqlite3.Connection,
    source_file: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict[str, Any]]:
    """
    Parse legacy moz_favicons table (Firefox < 55).

    Returns list of icon records with:
    - id: Original icon ID
    - icon_url: URL of the icon (from 'url' column)
    - mime_type: MIME type of the icon
    - expiration: Expiration timestamp
    - data: Binary icon data
    - icon_sha256: SHA256 hash of icon data
    - icon_md5: MD5 hash of icon data
    """
    if not table_exists(conn, "moz_favicons"):
        return []

    # Check columns and warn about unknowns
    columns = discover_and_warn_unknown_columns(
        conn, "moz_favicons", KNOWN_MOZ_FAVICONS_COLUMNS,
        source_file, "favicons", warning_collector
    )

    cursor = conn.cursor()
    cursor.row_factory = sqlite3.Row

    # Build dynamic SELECT based on available columns
    select_cols = ["id", "url", "data"]
    if "mime_type" in columns:
        select_cols.append("mime_type")
    if "expiration" in columns:
        select_cols.append("expiration")

    cursor.execute(f"""
        SELECT {', '.join(select_cols)}
        FROM moz_favicons
        WHERE data IS NOT NULL
    """)

    records = []
    for row in cursor:
        icon_data = row["data"]
        if icon_data is None or len(icon_data) > MAX_ICON_SIZE_BYTES:
            continue

        record = {
            "id": row["id"],
            "icon_url": row["url"],
            "mime_type": row["mime_type"] if "mime_type" in row.keys() else None,
            "expiration": row["expiration"] if "expiration" in row.keys() else None,
            "width": None,  # Legacy schema doesn't store dimensions
            "icon_type": 1,  # favicon (legacy didn't distinguish)
            "data": icon_data,
            "icon_sha256": hashlib.sha256(icon_data).hexdigest(),
            "icon_md5": hashlib.md5(icon_data).hexdigest(),
        }
        records.append(record)

    LOGGER.info("Parsed %d favicons from legacy moz_favicons table", len(records))
    return records


# =============================================================================
# High-Level Parser
# =============================================================================

def parse_favicons_database(
    conn: sqlite3.Connection,
    source_file: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bool]:
    """
    Parse a Firefox favicons.sqlite database.

    Automatically detects schema version and parses appropriately.

    Args:
        conn: SQLite connection to favicons.sqlite
        source_file: Source file path for warnings
        warning_collector: Optional warning collector

    Returns:
        Tuple of (icons, page_mappings, is_legacy)
        - icons: List of icon records
        - page_mappings: List of page-to-icon mapping records
        - is_legacy: True if legacy schema was used
    """
    # First, discover unknown tables
    discover_and_warn_unknown_tables(conn, source_file, "favicons", warning_collector)

    # Check which schema we have
    tables = get_all_tables(conn)

    # Try modern schema first (Firefox 55+)
    if "moz_icons" in tables:
        icons = parse_moz_icons(conn, source_file, warning_collector=warning_collector)

        if icons:
            icon_ids = {icon["id"] for icon in icons}
            mappings = parse_page_mappings(
                conn, icon_ids, source_file, warning_collector=warning_collector
            )
            return icons, mappings, False

    # Fall back to legacy schema
    if "moz_favicons" in tables:
        icons = parse_moz_favicons_legacy(conn, source_file, warning_collector=warning_collector)
        # Legacy schema doesn't have separate page mappings
        # (handled via places.sqlite moz_places.favicon_id)
        return icons, [], True

    LOGGER.warning("No recognized favicon tables found in %s", source_file)
    return [], [], False
