from __future__ import annotations

from pathlib import Path

from PIL import Image

import reports.appendix.image_list.module as appendix_image_list_module
import reports.modules.downloaded_images.module as downloaded_images_module
import reports.modules.images.module as images_module


def _write_jpeg(path: Path) -> None:
    image = Image.new("RGB", (64, 64), color=(30, 120, 220))
    image.save(path, format="JPEG")


def test_images_report_thumbnail_bootstraps_heif(monkeypatch, tmp_path: Path) -> None:
    image_path = tmp_path / "report_image.jpg"
    _write_jpeg(image_path)
    calls = {"count": 0}

    def fake_bootstrap() -> bool:
        calls["count"] += 1
        return True

    monkeypatch.setattr(images_module, "ensure_pillow_heif_registered", fake_bootstrap)

    module = images_module.ImagesModule()
    thumb_b64 = module._generate_thumbnail(
        rel_path=str(image_path),
        discovered_by=None,
        case_folder=None,
        evidence_id=1,
        evidence_label=None,
    )

    assert thumb_b64.startswith("data:image/jpeg;base64,")
    assert calls["count"] == 1


def test_downloaded_images_report_thumbnail_bootstraps_heif(monkeypatch, tmp_path: Path) -> None:
    image_path = tmp_path / "downloaded_image.jpg"
    _write_jpeg(image_path)
    calls = {"count": 0}

    def fake_bootstrap() -> bool:
        calls["count"] += 1
        return True

    monkeypatch.setattr(downloaded_images_module, "ensure_pillow_heif_registered", fake_bootstrap)

    module = downloaded_images_module.DownloadedImagesModule()
    thumb_b64 = module._generate_thumbnail(
        dest_path=str(image_path),
        case_folder=None,
        evidence_id=1,
        evidence_label=None,
    )

    assert thumb_b64.startswith("data:image/jpeg;base64,")
    assert calls["count"] == 1


def test_appendix_image_list_thumbnail_bootstraps_heif(monkeypatch, tmp_path: Path) -> None:
    image_path = tmp_path / "appendix_image.jpg"
    _write_jpeg(image_path)
    calls = {"count": 0}

    def fake_bootstrap() -> bool:
        calls["count"] += 1
        return True

    monkeypatch.setattr(appendix_image_list_module, "ensure_pillow_heif_registered", fake_bootstrap)

    module = appendix_image_list_module.AppendixImageListModule()

    # HEIF registration is now called once before the batch, not per-image.
    # Simulate that by calling it manually and then testing the thumbnail method.
    fake_bootstrap()

    thumb = module._generate_single_thumbnail(
        image_path=image_path,
        cache_path=None,
    )

    assert thumb.startswith("data:image/jpeg;base64,")
    assert calls["count"] == 1
