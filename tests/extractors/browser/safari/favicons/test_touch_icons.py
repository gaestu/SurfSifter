from __future__ import annotations

from io import BytesIO
from pathlib import Path

from PIL import Image

from extractors.browser.safari.favicons._schemas import ICON_TYPE_MASK_ICON, ICON_TYPE_TOUCH_ICON
from extractors.browser.safari.favicons._touch_icons import detect_image_extension, parse_icon_file


def _png_bytes(size: tuple[int, int] = (180, 180)) -> bytes:
    buf = BytesIO()
    Image.new("RGB", size, color=(30, 120, 220)).save(buf, format="PNG")
    return buf.getvalue()


def test_detect_image_extension_png() -> None:
    assert detect_image_extension(_png_bytes()) == "png"


def test_parse_touch_icon_png(tmp_path: Path) -> None:
    icon_path = tmp_path / "touch_icon.bin"
    icon_path.write_bytes(_png_bytes((120, 120)))

    parsed = parse_icon_file(icon_path, ICON_TYPE_TOUCH_ICON)
    assert parsed is not None
    assert parsed.icon_type == ICON_TYPE_TOUCH_ICON
    assert parsed.file_type == "png"
    assert parsed.width == 120
    assert parsed.height == 120
    assert parsed.sha256
    assert parsed.md5


def test_parse_template_icon_svg(tmp_path: Path) -> None:
    svg = """<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64"></svg>"""
    icon_path = tmp_path / "template.svg"
    icon_path.write_text(svg)

    parsed = parse_icon_file(icon_path, ICON_TYPE_MASK_ICON)
    assert parsed is not None
    assert parsed.icon_type == ICON_TYPE_MASK_ICON
    assert parsed.file_type == "svg"
    assert parsed.width == 64
    assert parsed.height == 64
    assert parsed.phash is None
