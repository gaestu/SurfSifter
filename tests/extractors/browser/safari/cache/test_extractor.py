from __future__ import annotations

import fnmatch
import sqlite3
from io import BytesIO
from pathlib import Path

from PIL import Image

from extractors.browser.safari.cache import SafariCacheExtractor
from extractors.extractor_registry import ExtractorRegistry


class _Callbacks:
    def on_step(self, step_name: str) -> None:
        return None

    def on_log(self, message: str, level: str = "info") -> None:
        return None

    def on_error(self, error: str, details: str = "") -> None:
        return None

    def on_progress(self, current: int, total: int, message: str = "") -> None:
        return None

    def is_cancelled(self) -> bool:
        return False


class _FakeEvidenceFS:
    def __init__(self, file_map: dict[str, bytes]):
        self.file_map = file_map
        self.fs_type = "HFS+"
        self.source_path = "/tmp/evidence.E01"

    def iter_paths(self, pattern: str):
        for path in self.file_map:
            if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(f"/{path}", pattern):
                yield path

    def read_file(self, path: str) -> bytes:
        if path in self.file_map:
            return self.file_map[path]
        alt = path.lstrip("/")
        if alt in self.file_map:
            return self.file_map[alt]
        raise FileNotFoundError(path)


def _png_bytes() -> bytes:
    buffer = BytesIO()
    Image.new("RGB", (8, 8), color=(30, 140, 60)).save(buffer, format="PNG")
    return buffer.getvalue()


def _cache_db_bytes(tmp_path: Path) -> bytes:
    db_path = tmp_path / "Cache.db"
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
        VALUES (1, 0, 1, 0, 'https://example.com/a.png', 730000000.0, '')
        """
    )
    conn.execute(
        """
        INSERT INTO cfurl_cache_receiver_data (entry_ID, isDataOnFS, receiver_data)
        VALUES (1, 0, ?)
        """,
        (_png_bytes(),),
    )
    conn.commit()
    conn.close()
    return db_path.read_bytes()


def test_extractor_metadata() -> None:
    extractor = SafariCacheExtractor()
    assert extractor.metadata.name == "safari_cache"
    assert extractor.metadata.can_extract is True
    assert extractor.metadata.can_ingest is True


def test_registry_discovers_safari_cache_extractor() -> None:
    registry = ExtractorRegistry()
    names = registry.list_names()
    assert "safari_cache" in names
    assert isinstance(registry.get("safari_cache"), SafariCacheExtractor)


def test_full_extraction_ingestion_flow(case_context, tmp_path: Path) -> None:
    cache_db = _cache_db_bytes(tmp_path)
    fs = _FakeEvidenceFS(
        {
            "Users/alice/Library/Caches/com.apple.Safari/Cache.db": cache_db,
        }
    )

    extractor = SafariCacheExtractor()
    output_dir = tmp_path / "out"
    conn = case_context.manager.get_evidence_conn(case_context.evidence_id, case_context.evidence_label)
    try:
        ok = extractor.run_extraction(
            fs,
            output_dir,
            {"evidence_id": case_context.evidence_id, "evidence_conn": conn},
            _Callbacks(),
        )
        assert ok is True

        stats = extractor.run_ingestion(
            output_dir,
            conn,
            case_context.evidence_id,
            {},
            _Callbacks(),
        )
        assert stats["entries_parsed"] >= 1
        assert stats["urls_inserted"] >= 1
    finally:
        conn.close()
