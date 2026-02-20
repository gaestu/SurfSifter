from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest
from PIL import Image

from app.services import thumbnailer
from core import image_codecs


@pytest.fixture(autouse=True)
def _reset_thumbnailer_and_codec_state() -> None:
    thumbnailer._CACHE_INDEX.clear()
    image_codecs._HEIF_REGISTERED = False
    image_codecs._HEIF_INIT_DONE = False
    yield
    thumbnailer._CACHE_INDEX.clear()
    image_codecs._HEIF_REGISTERED = False
    image_codecs._HEIF_INIT_DONE = False


def _write_jpeg(path: Path) -> None:
    image = Image.new("RGB", (64, 64), color=(64, 128, 192))
    image.save(path, format="JPEG")


def _write_heif(path: Path, pillow_heif: Any) -> None:
    image = Image.new("RGB", (64, 64), color=(160, 40, 80))

    try:
        heif_file = pillow_heif.from_pillow(image)
        heif_file.save(str(path), quality=50)
        return
    except Exception:
        pass

    try:
        pillow_heif.register_heif_opener()
        image.save(path, format="HEIF")
        return
    except Exception as exc:
        pytest.skip(f"HEIF encoder unavailable in test environment: {exc}")


def test_ensure_thumbnail_jpeg_baseline(tmp_path: Path) -> None:
    image_path = tmp_path / "sample.jpg"
    cache_dir = tmp_path / "thumbs"
    _write_jpeg(image_path)

    thumbnail_path = thumbnailer.ensure_thumbnail(image_path, cache_dir, size=(48, 48))

    assert thumbnail_path is not None
    assert thumbnail_path.exists()
    assert thumbnail_path.suffix == ".jpg"
    assert thumbnail_path.stat().st_size >= 100


def test_ensure_thumbnail_calls_codec_bootstrap(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    image_path = tmp_path / "sample.jpg"
    cache_dir = tmp_path / "thumbs"
    _write_jpeg(image_path)
    calls = {"count": 0}

    def fake_bootstrap() -> bool:
        calls["count"] += 1
        return False

    monkeypatch.setattr(thumbnailer, "ensure_pillow_heif_registered", fake_bootstrap)

    thumbnail_path = thumbnailer.ensure_thumbnail(image_path, cache_dir, size=(48, 48))
    assert thumbnail_path is not None
    assert calls["count"] == 1


def test_ensure_thumbnail_reuses_valid_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    image_path = tmp_path / "sample.jpg"
    cache_dir = tmp_path / "thumbs"
    _write_jpeg(image_path)

    first = thumbnailer.ensure_thumbnail(image_path, cache_dir, size=(48, 48))
    assert first is not None
    assert first.exists()

    def fail_open(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("Image.open should not run for valid cached thumbnail")

    monkeypatch.setattr(thumbnailer.Image, "open", fail_open)

    second = thumbnailer.ensure_thumbnail(image_path, cache_dir, size=(48, 48))
    assert second == first


def test_ensure_thumbnail_decode_failure_cleans_invalid_cache(tmp_path: Path) -> None:
    image_path = tmp_path / "broken.heic"
    cache_dir = tmp_path / "thumbs"
    image_path.write_bytes(b"not-a-real-image")
    cache_dir.mkdir(parents=True, exist_ok=True)

    key = hashlib.md5(str(image_path).encode("utf-8")).hexdigest()
    stale_thumb = cache_dir / f"{key}.jpg"
    stale_thumb.write_bytes(b"bad")
    assert stale_thumb.exists()

    thumbnail_path = thumbnailer.ensure_thumbnail(image_path, cache_dir)

    assert thumbnail_path is None
    assert not stale_thumb.exists()
    assert not any(cache_dir.glob("*.jpg"))


@pytest.mark.parametrize("extension", [".heic", ".heif"])
def test_ensure_thumbnail_heif_success_when_decoder_available(tmp_path: Path, extension: str) -> None:
    pillow_heif = pytest.importorskip("pillow_heif")
    image_path = tmp_path / f"sample{extension}"
    cache_dir = tmp_path / "thumbs"
    _write_heif(image_path, pillow_heif)

    thumbnail_path = thumbnailer.ensure_thumbnail(image_path, cache_dir, size=(48, 48))

    assert thumbnail_path is not None
    assert thumbnail_path.exists()
    assert thumbnail_path.suffix == ".jpg"
    assert thumbnail_path.stat().st_size >= 100
