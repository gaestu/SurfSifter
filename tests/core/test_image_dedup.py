"""Deduplication tests for images insertion."""

import sqlite3
from pathlib import Path

from core.database import insert_images, migrate
from core.database import EVIDENCE_MIGRATIONS_DIR


def test_insert_images_deduplicates_sha256(tmp_path: Path):
    db_path = tmp_path / "evidence.sqlite"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    migrate(conn, migrations_dir=EVIDENCE_MIGRATIONS_DIR)

    record = {
        "rel_path": "carved/img1.jpg",
        "filename": "img1.jpg",
        "md5": "a" * 32,
        "sha256": "b" * 64,
        "phash": None,
        "exif_json": "{}",
        "discovered_by": "test",
        "ts_utc": None,
        "notes": None,
    }

    inserted_first = insert_images(conn, 1, [record])
    inserted_second = insert_images(conn, 1, [record])

    count = conn.execute("SELECT COUNT(*) FROM images").fetchone()[0]
    assert inserted_first == 1
    assert inserted_second == 0  # duplicate ignored
    assert count == 1
