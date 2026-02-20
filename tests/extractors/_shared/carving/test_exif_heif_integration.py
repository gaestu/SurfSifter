from __future__ import annotations

from pathlib import Path

from PIL import Image

import extractors._shared.carving.exif as exif_mod


def _write_jpeg(path: Path) -> None:
    image = Image.new("RGB", (64, 64), color=(200, 80, 60))
    image.save(path, format="JPEG")


def test_exif_helpers_bootstrap_heif_codec(monkeypatch, tmp_path: Path) -> None:
    image_path = tmp_path / "sample.jpg"
    thumb_path = tmp_path / "thumb.jpg"
    _write_jpeg(image_path)

    calls = {"count": 0}

    def fake_bootstrap() -> bool:
        calls["count"] += 1
        return True

    monkeypatch.setattr(exif_mod, "ensure_pillow_heif_registered", fake_bootstrap)

    exif_data = exif_mod.extract_exif(image_path)
    out = exif_mod.generate_thumbnail(image_path, thumb_path)

    assert isinstance(exif_data, dict)
    assert out == thumb_path
    assert thumb_path.exists()
    assert calls["count"] == 2
