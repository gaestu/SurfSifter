from __future__ import annotations

import json
import sqlite3
from io import BytesIO
from pathlib import Path

from PIL import Image

from extractors.browser.safari.favicons.ingestion import FaviconsIngestionHandler


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


def _png_bytes(size: tuple[int, int] = (128, 128)) -> bytes:
    buf = BytesIO()
    Image.new("RGB", size, color=(20, 190, 100)).save(buf, format="PNG")
    return buf.getvalue()


def _create_favicons_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
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
    # Unknown table to verify schema warning collection.
    conn.execute("CREATE TABLE icon_future_table (id INTEGER)")
    conn.execute(
        """
        INSERT INTO icon_info(uuid, url, timestamp, width, height, has_generated_representations)
        VALUES ('abc123', 'https://example.com/favicon.ico', 730000000.0, 128, 128, 1)
        """
    )
    conn.execute(
        "INSERT INTO page_url(uuid, url) VALUES ('abc123', 'https://example.com/page')"
    )
    conn.commit()
    conn.close()


def test_ingestion_inserts_favicons_mappings_urls_images_and_warnings(case_context, tmp_path: Path) -> None:
    run_id = "20260101T000000_abcd1234"
    output_dir = tmp_path / "safari_favicons"
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True)

    db_path = run_dir / "favicons" / "alice" / "Favicons.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _create_favicons_db(db_path)

    icon_cache_file = run_dir / "favicons" / "alice" / "abc123"
    icon_cache_file.write_bytes(_png_bytes((128, 128)))

    touch_icon_file = run_dir / "touch_icons" / "alice" / "touch_1"
    touch_icon_file.parent.mkdir(parents=True, exist_ok=True)
    touch_icon_file.write_bytes(_png_bytes((180, 180)))

    template_icon_file = run_dir / "template_icons" / "alice" / "template.svg"
    template_icon_file.parent.mkdir(parents=True, exist_ok=True)
    template_icon_file.write_text("<svg width='64' height='64'></svg>")

    manifest = {
        "run_id": run_id,
        "status": "ok",
        "extraction_timestamp_utc": "2026-01-01T00:00:00+00:00",
        "files": [
            {
                "artifact_type": "favicons_db",
                "local_path": str(db_path),
                "source_path": "Users/alice/Library/Safari/Favicon Cache/Favicons.db",
                "profile": "alice",
                "user": "alice",
                "md5": "x",
                "sha256": "y",
                "size_bytes": db_path.stat().st_size,
            },
            {
                "artifact_type": "favicon_cache_file",
                "local_path": str(icon_cache_file),
                "source_path": "Users/alice/Library/Safari/Favicon Cache/abc123",
                "profile": "alice",
                "user": "alice",
                "md5": "x1",
                "sha256": "y1",
                "size_bytes": icon_cache_file.stat().st_size,
            },
            {
                "artifact_type": "touch_icon_file",
                "local_path": str(touch_icon_file),
                "source_path": "Users/alice/Library/Safari/Touch Icons Cache/touch_1",
                "profile": "alice",
                "user": "alice",
                "md5": "x2",
                "sha256": "y2",
                "size_bytes": touch_icon_file.stat().st_size,
            },
            {
                "artifact_type": "template_icon_file",
                "local_path": str(template_icon_file),
                "source_path": "Users/alice/Library/Safari/Template Icons/template.svg",
                "profile": "alice",
                "user": "alice",
                "md5": "x3",
                "sha256": "y3",
                "size_bytes": template_icon_file.stat().st_size,
            },
        ],
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest))

    conn = case_context.manager.get_evidence_conn(case_context.evidence_id, case_context.evidence_label)
    try:
        handler = FaviconsIngestionHandler("safari_favicons", "1.0.0")
        stats = handler.run(output_dir, conn, case_context.evidence_id, {}, _Callbacks())

        assert stats["favicons_inserted"] >= 3
        assert stats["mappings_inserted"] >= 1
        assert stats["urls_inserted"] >= 2
        assert stats["images_inserted"] >= 2
        assert stats["warnings"] >= 1

        favicon_count = conn.execute(
            "SELECT COUNT(*) FROM favicons WHERE evidence_id = ? AND run_id = ?",
            (case_context.evidence_id, run_id),
        ).fetchone()[0]
        assert favicon_count >= 3

        mapping_count = conn.execute(
            "SELECT COUNT(*) FROM favicon_mappings WHERE evidence_id = ? AND run_id = ?",
            (case_context.evidence_id, run_id),
        ).fetchone()[0]
        assert mapping_count >= 1

        url_count = conn.execute(
            "SELECT COUNT(*) FROM urls WHERE evidence_id = ? AND run_id = ?",
            (case_context.evidence_id, run_id),
        ).fetchone()[0]
        assert url_count >= 2

        warning_count = conn.execute(
            "SELECT COUNT(*) FROM extraction_warnings WHERE evidence_id = ? AND run_id = ?",
            (case_context.evidence_id, run_id),
        ).fetchone()[0]
        assert warning_count >= 1
    finally:
        conn.close()
