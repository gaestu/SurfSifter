"""SQLite parser for Safari Cache.db artifacts."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set

from core.logging import get_logger
from .._parsers import cocoa_to_iso

LOGGER = get_logger("extractors.browser.safari.cache.parser")

# Regex for text-style timestamps stored in newer Safari Cache.db files
# e.g. '2023-10-04 19:29:21' or '2023-10-04T19:29:21'
_TEXT_TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%f",
)


@dataclass
class SafariCacheEntry:
    entry_id: int
    url: str
    timestamp_cocoa: Optional[float]
    timestamp_utc: Optional[str]
    version: int
    storage_policy: int
    partition: Optional[str]
    hash_value: int
    is_data_on_fs: bool
    inline_body_size: int
    inline_body: Optional[bytes]
    response_blob: Optional[bytes]
    request_blob: Optional[bytes]
    proto_props_blob: Optional[bytes]


def parse_cache_db(db_path: Path) -> List[SafariCacheEntry]:
    """Parse a Safari Cache.db file into normalized entries."""
    entries: List[SafariCacheEntry] = []
    if not db_path.exists():
        return entries

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        tables = get_cache_db_tables(db_path, conn=conn)
        if "cfurl_cache_response" not in tables:
            conn.close()
            return entries

        has_blob = "cfurl_cache_blob_data" in tables
        has_receiver = "cfurl_cache_receiver_data" in tables

        sql = [
            "SELECT",
            "  r.entry_ID AS entry_id,",
            "  r.request_key AS request_key,",
            "  r.time_stamp AS time_stamp,",
            "  r.version AS version,",
            "  r.storage_policy AS storage_policy,",
            "  r.partition AS partition,",
            "  r.hash_value AS hash_value,",
        ]
        if has_receiver:
            sql.extend([
                "  rd.isDataOnFS AS is_data_on_fs,",
                "  rd.receiver_data AS receiver_data,",
            ])
        else:
            sql.extend([
                "  NULL AS is_data_on_fs,",
                "  NULL AS receiver_data,",
            ])
        if has_blob:
            sql.extend([
                "  bd.response_object AS response_blob,",
                "  bd.request_object AS request_blob,",
                "  bd.proto_props AS proto_props_blob",
            ])
        else:
            sql.extend([
                "  NULL AS response_blob,",
                "  NULL AS request_blob,",
                "  NULL AS proto_props_blob",
            ])
        sql.extend(["FROM cfurl_cache_response r"])
        if has_receiver:
            sql.extend(["LEFT JOIN cfurl_cache_receiver_data rd ON rd.entry_ID = r.entry_ID"])
        if has_blob:
            sql.extend(["LEFT JOIN cfurl_cache_blob_data bd ON bd.entry_ID = r.entry_ID"])
        sql.extend(["ORDER BY r.entry_ID"])

        cursor.execute("\n".join(sql))
        for row in cursor.fetchall():
            raw_receiver = row["receiver_data"]
            is_data_on_fs = bool(row["is_data_on_fs"]) if row["is_data_on_fs"] is not None else False
            inline_body = None if is_data_on_fs else raw_receiver
            cocoa_ts, utc_str = _parse_timestamp(row["time_stamp"])
            entry = SafariCacheEntry(
                entry_id=int(row["entry_id"] or 0),
                url=str(row["request_key"] or ""),
                timestamp_cocoa=cocoa_ts,
                timestamp_utc=utc_str,
                version=int(row["version"] or 0),
                storage_policy=int(row["storage_policy"] or 0),
                partition=row["partition"],
                hash_value=int(row["hash_value"] or 0),
                is_data_on_fs=is_data_on_fs,
                inline_body_size=len(inline_body or b""),
                inline_body=inline_body,
                response_blob=row["response_blob"],
                request_blob=row["request_blob"],
                proto_props_blob=row["proto_props_blob"],
            )
            entries.append(entry)

        conn.close()
    except sqlite3.Error as exc:
        LOGGER.debug("Failed to parse Safari cache DB %s: %s", db_path, exc)

    return entries


def get_cache_db_tables(
    db_path: Path,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Set[str]:
    """Return table names from a Cache.db SQLite file."""
    own_conn = False
    if conn is None:
        conn = sqlite3.connect(str(db_path))
        own_conn = True
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        return {row[0] for row in rows if row[0]}
    finally:
        if own_conn:
            conn.close()


def get_cache_db_columns(
    db_path: Path,
    table_name: str,
    *,
    conn: Optional[sqlite3.Connection] = None,
) -> Set[str]:
    """Return column names for a specific table in Cache.db."""
    own_conn = False
    if conn is None:
        conn = sqlite3.connect(str(db_path))
        own_conn = True
    try:
        rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return {row[1] for row in rows if len(row) > 1 and row[1]}
    finally:
        if own_conn:
            conn.close()


def _parse_timestamp(value) -> tuple[Optional[float], Optional[str]]:
    """Parse time_stamp which can be a Cocoa float or a text date string.

    Newer Safari versions store text dates like '2023-10-04 19:29:21'
    while older versions use Cocoa epoch floats (seconds since 2001-01-01).

    Returns:
        (cocoa_float_or_none, iso_utc_string_or_none)
    """
    if value is None:
        return None, None

    # Try numeric Cocoa timestamp first
    cocoa = _coerce_float(value)
    if cocoa is not None:
        return cocoa, cocoa_to_iso(cocoa)

    # Try parsing as text date string
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None, None
        for fmt in _TEXT_TIMESTAMP_FORMATS:
            try:
                from datetime import datetime, timezone
                dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
                return None, dt.isoformat()
            except ValueError:
                continue
        # If it looks like an ISO-ish string, return it directly
        if len(text) >= 10 and text[4:5] == "-":
            return None, text

    return None, None


def _coerce_float(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
