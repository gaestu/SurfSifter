from __future__ import annotations

import sqlite3

import reports.modules.screenshots.module as screenshots_module
from reports.modules.downloaded_images import DownloadedImagesModule
from reports.modules.images import ImagesModule
from reports.modules.screenshots import ScreenshotsModule


def test_images_module_escapes_evidence_fields() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE images (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER,
            rel_path TEXT,
            filename TEXT,
            md5 TEXT,
            sha256 TEXT,
            ts_utc TEXT,
            exif_json TEXT,
            size_bytes INTEGER,
            first_discovered_by TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO images (id, evidence_id, rel_path, filename, md5, ts_utc, size_bytes, first_discovered_by)
        VALUES (1, 1, 'image.jpg', '<script>alert(1)</script>', 'abc123', '2024-01-01T00:00:00', 1024, 'filesystem_images')
        """
    )
    conn.commit()

    html = ImagesModule().render(conn, evidence_id=1, config={"include_filename": True})

    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html


def test_downloaded_images_module_escapes_evidence_fields() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE downloads (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            url TEXT,
            domain TEXT,
            filename TEXT,
            dest_path TEXT,
            md5 TEXT,
            sha256 TEXT,
            size_bytes INTEGER,
            completed_at_utc TEXT,
            width INTEGER,
            height INTEGER,
            file_type TEXT,
            status TEXT
        );
        CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
        CREATE TABLE tag_associations (
            id INTEGER PRIMARY KEY,
            evidence_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            artifact_type TEXT NOT NULL,
            artifact_id INTEGER NOT NULL
        );
        """
    )
    conn.execute(
        """
        INSERT INTO downloads
            (id, evidence_id, url, domain, filename, dest_path, md5, sha256, size_bytes, completed_at_utc, width, height, file_type, status)
        VALUES
            (1, 1, 'https://example.test/<script>alert(1)</script>', 'example.test', '<script>alert(1)</script>', 'example/image.jpg', 'abc', 'def', 1024, '2024-01-01T00:00:00', 64, 64, 'image', 'completed')
        """
    )
    conn.commit()

    html = DownloadedImagesModule().render(conn, evidence_id=1, config={})

    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html


def test_screenshots_module_escapes_evidence_fields(monkeypatch) -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    monkeypatch.setattr(
        screenshots_module,
        "get_screenshots",
        lambda *args, **kwargs: [
            {
                "id": 1,
                "title": "<script>alert(1)</script>",
                "caption": "<script>alert(2)</script>",
                "notes": "<script>alert(3)</script>",
                "captured_url": "https://example.test/<script>alert(4)</script>",
                "source": "manual",
                "captured_at_utc": "2024-01-01T00:00:00",
                "dest_path": "screenshots/example.jpg",
                "sequence_name": None,
            }
        ],
    )

    html = ScreenshotsModule().render(
        conn,
        evidence_id=1,
        config={"include_notes": True, "include_url": True},
    )

    assert "<script>alert(" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
    assert "&lt;script&gt;alert(2)&lt;/script&gt;" in html
    assert "&lt;script&gt;alert(3)&lt;/script&gt;" in html
    assert "&lt;script&gt;alert(4)&lt;/script&gt;" in html