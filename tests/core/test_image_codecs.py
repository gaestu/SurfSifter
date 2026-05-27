from __future__ import annotations

import importlib
from io import BytesIO
from pathlib import Path

from PIL import Image

from core import image_codecs


def test_import_does_not_reset_pillow_pixel_limit() -> None:
    original_limit = Image.MAX_IMAGE_PIXELS
    sentinel_limit = 123_456_789
    Image.MAX_IMAGE_PIXELS = sentinel_limit
    try:
        importlib.reload(image_codecs)
        assert Image.MAX_IMAGE_PIXELS == sentinel_limit
    finally:
        Image.MAX_IMAGE_PIXELS = original_limit
        importlib.reload(image_codecs)


def test_thumbnail_to_jpeg_bytes_restores_pillow_pixel_limit(tmp_path: Path) -> None:
    image_path = tmp_path / "sample.jpg"
    Image.new("RGB", (32, 32), color=(32, 96, 160)).save(image_path, format="JPEG")

    original_limit = Image.MAX_IMAGE_PIXELS
    sentinel_limit = 123_456_789
    Image.MAX_IMAGE_PIXELS = sentinel_limit
    try:
        thumbnail = image_codecs.thumbnail_to_jpeg_bytes(image_path, max_pixels=1_000_000)
        assert thumbnail is not None
        assert Image.MAX_IMAGE_PIXELS == sentinel_limit
    finally:
        Image.MAX_IMAGE_PIXELS = original_limit


def test_thumbnail_to_jpeg_bytes_composites_transparency_on_white(tmp_path: Path) -> None:
    image_path = tmp_path / "transparent.png"
    Image.new("RGBA", (16, 16), color=(255, 0, 0, 0)).save(image_path, format="PNG")

    thumbnail = image_codecs.thumbnail_to_jpeg_bytes(image_path, size=(16, 16))

    assert thumbnail is not None
    with Image.open(BytesIO(thumbnail)) as img:
        r, g, b = img.convert("RGB").getpixel((8, 8))
    assert r > 240 and g > 240 and b > 240