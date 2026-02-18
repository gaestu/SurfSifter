from __future__ import annotations

import gzip
from io import BytesIO
from pathlib import Path

from PIL import Image

from extractors.browser.safari.cache._blob_parser import ResponseMetadata
from extractors.browser.safari.cache._image_carver import (
    carve_image_from_cache_entry,
    carve_orphan_images,
)
from extractors.browser.safari.cache._parser import SafariCacheEntry


def _png_bytes() -> bytes:
    buffer = BytesIO()
    Image.new("RGB", (12, 8), color=(20, 100, 180)).save(buffer, format="PNG")
    return buffer.getvalue()


def _response_meta(*, content_type: str = "image/png", content_encoding: str | None = None) -> ResponseMetadata:
    headers = {"Content-Type": content_type}
    if content_encoding:
        headers["Content-Encoding"] = content_encoding
    return ResponseMetadata(
        http_status=200,
        content_type=content_type,
        content_length=None,
        mime_type=content_type,
        text_encoding=None,
        server=None,
        cache_control=None,
        etag=None,
        last_modified=None,
        set_cookie=None,
        all_headers=headers,
        raw_deserialized=None,
    )


def test_carve_inline_image_png(tmp_path: Path) -> None:
    entry = SafariCacheEntry(
        entry_id=1,
        url="https://example.com/img.png",
        timestamp_cocoa=730000000.0,
        timestamp_utc=None,
        version=0,
        storage_policy=0,
        partition=None,
        hash_value=1,
        is_data_on_fs=False,
        inline_body_size=0,
        inline_body=_png_bytes(),
        response_blob=None,
        request_blob=None,
        proto_props_blob=None,
    )

    carved = carve_image_from_cache_entry(entry, _response_meta(), tmp_path, tmp_path / "fsCachedData")
    assert carved is not None
    assert carved.format == "png"
    assert carved.source_type == "inline"
    assert carved.width == 12
    assert carved.height == 8


def test_carve_inline_gzip_image(tmp_path: Path) -> None:
    entry = SafariCacheEntry(
        entry_id=2,
        url="https://example.com/img2.png",
        timestamp_cocoa=730000000.0,
        timestamp_utc=None,
        version=0,
        storage_policy=0,
        partition=None,
        hash_value=1,
        is_data_on_fs=False,
        inline_body_size=0,
        inline_body=gzip.compress(_png_bytes()),
        response_blob=None,
        request_blob=None,
        proto_props_blob=None,
    )

    carved = carve_image_from_cache_entry(
        entry,
        _response_meta(content_encoding="gzip"),
        tmp_path,
        tmp_path / "fsCachedData",
    )
    assert carved is not None
    assert carved.format == "png"


def test_orphan_fscached_detection(tmp_path: Path) -> None:
    fs_dir = tmp_path / "fsCachedData"
    fs_dir.mkdir()
    (fs_dir / "orphan_png").write_bytes(_png_bytes())
    (fs_dir / "not_image").write_text("hello")

    orphans = carve_orphan_images(fs_dir, known_entry_ids={1, 2}, run_dir=tmp_path)
    assert len(orphans) == 1
    assert orphans[0].source_type == "orphan"
