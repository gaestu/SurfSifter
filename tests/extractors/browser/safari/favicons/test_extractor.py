from __future__ import annotations

import fnmatch
import json
import sqlite3
from io import BytesIO
from pathlib import Path

from PIL import Image

from extractors.browser.safari.favicons import SafariFaviconsExtractor
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
        self.fs_type = "APFS"
        self.source_path = "/tmp/evidence.E01"
        self.partition_index = 0

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


def _png_bytes(size: tuple[int, int] = (64, 64)) -> bytes:
    buffer = BytesIO()
    Image.new("RGB", size, color=(210, 90, 10)).save(buffer, format="PNG")
    return buffer.getvalue()


def _favicons_db_bytes(tmp_path: Path) -> bytes:
    db_path = tmp_path / "Favicons.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE icon_info (
            uuid TEXT PRIMARY KEY,
            url TEXT,
            timestamp REAL,
            width INTEGER,
            height INTEGER,
            has_generated_representations INTEGER
        )
        """
    )
    conn.execute("CREATE TABLE page_url (uuid TEXT, url TEXT)")
    conn.commit()
    conn.close()
    return db_path.read_bytes()


def test_metadata_and_registry_discovery() -> None:
    extractor = SafariFaviconsExtractor()
    assert extractor.metadata.name == "safari_favicons"
    assert extractor.metadata.can_extract is True
    assert extractor.metadata.can_ingest is True

    registry = ExtractorRegistry()
    assert "safari_favicons" in registry.list_names()
    assert isinstance(registry.get("safari_favicons"), SafariFaviconsExtractor)


def test_extraction_copies_db_touch_template_and_profiles(tmp_path: Path) -> None:
    favicons_db = _favicons_db_bytes(tmp_path)
    fs = _FakeEvidenceFS(
        {
            "Users/alice/Library/Safari/Favicon Cache/Favicons.db": favicons_db,
            "Users/alice/Library/Safari/Profiles/ProfileA/Favicon Cache/Favicons.db": favicons_db,
            "Users/alice/Library/Safari/Touch Icons Cache/icon_a": _png_bytes((128, 128)),
            "Users/alice/Library/Safari/Template Icons/pin.svg": b"<svg width='64' height='64'></svg>",
        }
    )

    extractor = SafariFaviconsExtractor()
    output_dir = tmp_path / "out"
    ok = extractor.run_extraction(
        fs,
        output_dir,
        {"evidence_id": 1},
        _Callbacks(),
    )
    assert ok is True

    manifests = sorted(output_dir.glob("*/manifest.json"))
    assert manifests, "No extraction manifest generated"
    manifest = json.loads(manifests[-1].read_text())

    assert any(f["artifact_type"] == "favicons_db" for f in manifest["files"])
    assert any(f["artifact_type"] == "touch_icon_file" for f in manifest["files"])
    assert any(f["artifact_type"] == "template_icon_file" for f in manifest["files"])
    assert any("ProfileA" in f.get("profile", "") for f in manifest["files"])
