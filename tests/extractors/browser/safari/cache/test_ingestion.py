from __future__ import annotations

import json
import plistlib
import sqlite3
from io import BytesIO
from pathlib import Path

from PIL import Image

from extractors.browser.safari.cache.ingestion import CacheIngestionHandler


class _Callbacks:
    def __init__(self) -> None:
        self.steps: list[str] = []
        self.logs: list[str] = []

    def on_step(self, step_name: str) -> None:
        self.steps.append(step_name)

    def on_log(self, message: str, level: str = "info") -> None:
        self.logs.append(f"{level}:{message}")

    def on_error(self, error: str, details: str = "") -> None:
        self.logs.append(f"error:{error}:{details}")

    def on_progress(self, current: int, total: int, message: str = "") -> None:
        return None

    def is_cancelled(self) -> bool:
        return False


def _png_bytes() -> bytes:
    buffer = BytesIO()
    Image.new("RGB", (10, 10), color=(200, 50, 50)).save(buffer, format="PNG")
    return buffer.getvalue()


def _create_cache_db(path: Path, *, add_unknown_table: bool = False) -> None:
    response_blob = plistlib.dumps(
        {
            "NSHTTPURLResponse": {
                "statusCode": 200,
                "allHeaderFields": {
                    "Content-Type": "image/png",
                    "X-Unexpected-Header": "value",
                },
            }
        },
        fmt=plistlib.FMT_BINARY,
    )
    request_blob = plistlib.dumps(
        {
            "NSURLRequest": {
                "HTTPMethod": "GET",
                "allHTTPHeaderFields": {"User-Agent": "Safari"},
            }
        },
        fmt=plistlib.FMT_BINARY,
    )

    conn = sqlite3.connect(str(path))
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
    conn.execute(
        """
        CREATE TABLE cfurl_cache_receiver_data (
            entry_ID INTEGER,
            isDataOnFS INTEGER,
            receiver_data BLOB
        )
        """
    )
    if add_unknown_table:
        conn.execute("CREATE TABLE cfurl_cache_future_table (id INTEGER)")

    conn.execute(
        """
        INSERT INTO cfurl_cache_response
        (entry_ID, version, hash_value, storage_policy, request_key, time_stamp, partition)
        VALUES (1, 0, 1, 0, 'https://example.com/image.png', 730000000.0, '')
        """
    )
    conn.execute(
        """
        INSERT INTO cfurl_cache_blob_data
        (entry_ID, response_object, request_object, proto_props, user_info)
        VALUES (1, ?, ?, NULL, NULL)
        """,
        (response_blob, request_blob),
    )
    conn.execute(
        """
        INSERT INTO cfurl_cache_receiver_data
        (entry_ID, isDataOnFS, receiver_data)
        VALUES (1, 0, ?)
        """,
        (_png_bytes(),),
    )
    conn.commit()
    conn.close()


def test_ingestion_inserts_urls_images_inventory_and_warnings(case_context, tmp_path: Path) -> None:
    run_id = "20260101T000000_abcd1234"
    output_dir = tmp_path / "safari_cache"
    run_dir = output_dir / run_id / "cache_group"
    run_dir.mkdir(parents=True)
    cache_db = run_dir / "Cache.db"
    _create_cache_db(cache_db, add_unknown_table=True)

    manifest = {
        "run_id": run_id,
        "status": "ok",
        "extraction_timestamp_utc": "2026-01-01T00:00:00+00:00",
        "files": [
            {
                "artifact_type": "cache_db",
                "local_path": str(cache_db),
                "source_path": "Users/alice/Library/Caches/com.apple.Safari/Cache.db",
                "user": "alice",
                "md5": "x",
                "sha256": "y",
                "size_bytes": cache_db.stat().st_size,
            }
        ],
    }
    (output_dir / run_id / "manifest.json").write_text(json.dumps(manifest))

    conn = case_context.manager.get_evidence_conn(case_context.evidence_id, case_context.evidence_label)
    try:
        handler = CacheIngestionHandler("safari_cache", "1.0.0")
        stats = handler.run(output_dir, conn, case_context.evidence_id, {}, _Callbacks())
        assert stats["entries_parsed"] == 1
        assert stats["urls_inserted"] >= 1
        assert stats["images_inserted"] >= 1

        url_count = conn.execute(
            "SELECT COUNT(*) FROM urls WHERE evidence_id = ? AND run_id = ?",
            (case_context.evidence_id, run_id),
        ).fetchone()[0]
        assert url_count >= 1

        inventory_count = conn.execute(
            "SELECT COUNT(*) FROM browser_cache_inventory WHERE evidence_id = ? AND run_id = ?",
            (case_context.evidence_id, run_id),
        ).fetchone()[0]
        assert inventory_count == 1

        warning_count = conn.execute(
            "SELECT COUNT(*) FROM extraction_warnings WHERE evidence_id = ? AND run_id = ?",
            (case_context.evidence_id, run_id),
        ).fetchone()[0]
        assert warning_count >= 1
    finally:
        conn.close()
