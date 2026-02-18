from __future__ import annotations

import sqlite3
from pathlib import Path

from extractors.browser.safari.cache._parser import (
    get_cache_db_columns,
    get_cache_db_tables,
    parse_cache_db,
)


def _create_cache_db(db_path: Path, *, with_blob: bool = True, with_receiver: bool = True) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE cfurl_cache_response (
            entry_ID INTEGER PRIMARY KEY,
            version INTEGER,
            hash_value INTEGER,
            storage_policy INTEGER,
            request_key TEXT,
            time_stamp REAL,
            partition TEXT
        )
        """
    )
    if with_blob:
        conn.execute(
            """
            CREATE TABLE cfurl_cache_blob_data (
                entry_ID INTEGER,
                response_object BLOB,
                request_object BLOB,
                proto_props BLOB,
                user_info BLOB
            )
            """
        )
    if with_receiver:
        conn.execute(
            """
            CREATE TABLE cfurl_cache_receiver_data (
                entry_ID INTEGER,
                isDataOnFS INTEGER,
                receiver_data BLOB
            )
            """
        )

    conn.execute(
        """
        INSERT INTO cfurl_cache_response
        (entry_ID, version, hash_value, storage_policy, request_key, time_stamp, partition)
        VALUES (1, 0, 42, 0, 'https://example.com/image.png', 730000000.0, '')
        """
    )
    if with_blob:
        conn.execute(
            """
            INSERT INTO cfurl_cache_blob_data
            (entry_ID, response_object, request_object, proto_props, user_info)
            VALUES (1, ?, ?, NULL, NULL)
            """,
            (b"resp", b"req"),
        )
    if with_receiver:
        conn.execute(
            """
            INSERT INTO cfurl_cache_receiver_data
            (entry_ID, isDataOnFS, receiver_data)
            VALUES (1, 0, ?)
            """,
            (b"\x89PNG\r\n\x1a\ntest",),
        )
    conn.commit()
    conn.close()


def test_parse_cache_db_basic(tmp_path: Path) -> None:
    db_path = tmp_path / "Cache.db"
    _create_cache_db(db_path)

    entries = parse_cache_db(db_path)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.entry_id == 1
    assert entry.url == "https://example.com/image.png"
    assert entry.timestamp_cocoa == 730000000.0
    assert entry.timestamp_utc is not None
    assert entry.is_data_on_fs is False
    assert entry.inline_body_size > 0
    assert entry.response_blob == b"resp"
    assert entry.request_blob == b"req"


def test_parse_cache_db_missing_optional_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "Cache.db"
    _create_cache_db(db_path, with_blob=False, with_receiver=False)

    entries = parse_cache_db(db_path)
    assert len(entries) == 1
    entry = entries[0]
    assert entry.response_blob is None
    assert entry.request_blob is None
    assert entry.inline_body is None


def test_get_tables_and_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "Cache.db"
    _create_cache_db(db_path)

    tables = get_cache_db_tables(db_path)
    assert "cfurl_cache_response" in tables
    assert "cfurl_cache_blob_data" in tables
    assert "cfurl_cache_receiver_data" in tables

    response_columns = get_cache_db_columns(db_path, "cfurl_cache_response")
    assert "request_key" in response_columns
    assert "time_stamp" in response_columns
