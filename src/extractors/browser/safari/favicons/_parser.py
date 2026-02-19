"""SQLite parser for Safari Favicons.db."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from core.logging import get_logger

from ._schemas import KNOWN_COLUMNS_BY_TABLE, KNOWN_TABLES, RELEVANT_TABLE_PATTERNS

LOGGER = get_logger("extractors.browser.safari.favicons.parser")


@dataclass
class SafariIconRecord:
    uuid: str
    icon_url: str
    timestamp: Optional[float]
    width: Optional[int]
    height: Optional[int]
    has_generated_representations: Optional[int]


def get_favicons_db_tables(db_path: Path) -> set[str]:
    """Return the table names present in Favicons.db."""
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            return {str(r[0]) for r in rows}
        finally:
            conn.close()
    except Exception:
        return set()


def get_favicons_db_columns(db_path: Path, table_name: str) -> set[str]:
    """Return column names for a table in Favicons.db."""
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            return {str(r[1]) for r in rows}
        finally:
            conn.close()
    except Exception:
        return set()


def parse_favicons_db(
    db_path: Path,
    *,
    warning_collector=None,
    source_file: Optional[str] = None,
) -> tuple[List[SafariIconRecord], Dict[str, List[str]]]:
    """
    Parse Safari Favicons.db into icon rows and UUID->page URL mappings.

    Returns:
        (icons, page_mappings_by_uuid)
    """
    icons: List[SafariIconRecord] = []
    mappings: Dict[str, List[str]] = {}
    source = source_file or str(db_path)

    if not db_path.exists():
        return icons, mappings

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        LOGGER.debug("Unable to open Favicons.db: %s", exc)
        return icons, mappings

    try:
        cursor = conn.cursor()
        tables = _get_tables(cursor)
        _collect_schema_warnings(cursor, tables, source, warning_collector)

        if "icon_info" not in tables:
            return icons, mappings

        icon_cols = _get_table_columns(cursor, "icon_info")
        if "uuid" not in icon_cols or "url" not in icon_cols:
            return icons, mappings

        has_page_table = "page_url" in tables
        page_cols = _get_table_columns(cursor, "page_url") if has_page_table else set()
        has_page_join = has_page_table and {"uuid", "url"}.issubset(page_cols)

        select_cols = [
            "ii.uuid AS uuid",
            "ii.url AS icon_url",
            "ii.timestamp AS timestamp" if "timestamp" in icon_cols else "NULL AS timestamp",
            "ii.width AS width" if "width" in icon_cols else "NULL AS width",
            "ii.height AS height" if "height" in icon_cols else "NULL AS height",
            (
                "ii.has_generated_representations AS has_generated_representations"
                if "has_generated_representations" in icon_cols
                else "NULL AS has_generated_representations"
            ),
        ]
        if has_page_join:
            select_cols.append("pu.url AS page_url")
        else:
            select_cols.append("NULL AS page_url")

        query = f"SELECT {', '.join(select_cols)} FROM icon_info ii"
        if has_page_join:
            query += " LEFT JOIN page_url pu ON pu.uuid = ii.uuid"

        seen_uuid: set[str] = set()
        for row in cursor.execute(query):
            uuid = str(row["uuid"] or "").strip()
            icon_url = str(row["icon_url"] or "").strip()
            page_url = str(row["page_url"] or "").strip()
            if not uuid or not icon_url:
                continue

            if uuid not in seen_uuid:
                seen_uuid.add(uuid)
                icons.append(
                    SafariIconRecord(
                        uuid=uuid,
                        icon_url=icon_url,
                        timestamp=row["timestamp"],
                        width=row["width"],
                        height=row["height"],
                        has_generated_representations=row["has_generated_representations"],
                    )
                )

            if page_url:
                existing = mappings.setdefault(uuid, [])
                if page_url not in existing:
                    existing.append(page_url)
    except sqlite3.Error as exc:
        LOGGER.debug("Favicons.db parse error for %s: %s", db_path, exc)
    finally:
        conn.close()

    return icons, mappings


def _get_tables(cursor: sqlite3.Cursor) -> set[str]:
    rows = cursor.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {str(row[0]) for row in rows}


def _get_table_columns(cursor: sqlite3.Cursor, table_name: str) -> set[str]:
    rows = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _collect_schema_warnings(
    cursor: sqlite3.Cursor,
    tables: set[str],
    source_file: str,
    warning_collector,
) -> None:
    if warning_collector is None:
        return

    unknown_tables = tables - KNOWN_TABLES
    for table_name in sorted(unknown_tables):
        table_lower = table_name.lower()
        if not any(pattern in table_lower for pattern in RELEVANT_TABLE_PATTERNS):
            continue
        columns = sorted(_get_table_columns(cursor, table_name))
        warning_collector.add_unknown_table(
            table_name=table_name,
            columns=columns,
            source_file=source_file,
            artifact_type="favicons",
        )

    for table_name, known_cols in KNOWN_COLUMNS_BY_TABLE.items():
        if table_name not in tables:
            continue
        actual_cols = _get_table_columns(cursor, table_name)
        for unknown_col in sorted(actual_cols - known_cols):
            warning_collector.add_unknown_column(
                table_name=table_name,
                column_name=unknown_col,
                column_type="",
                source_file=source_file,
                artifact_type="favicons",
            )
