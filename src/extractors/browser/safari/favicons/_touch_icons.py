"""Helpers for parsing Safari Touch Icons and Template Icons cache files."""

from __future__ import annotations

import hashlib
import io
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from core.phash import compute_phash


@dataclass
class ParsedIconFile:
    icon_type: int
    path: Path
    file_type: str
    size_bytes: int
    md5: str
    sha256: str
    width: Optional[int]
    height: Optional[int]
    phash: Optional[str]
    icon_url: Optional[str]


def detect_image_extension(data: bytes) -> str:
    """Detect file type from magic bytes/content heuristics."""
    if len(data) < 4:
        return "bin"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if data[:3] == b"\xff\xd8\xff":
        return "jpg"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "gif"
    if data[:4] == b"RIFF" and len(data) >= 12 and data[8:12] == b"WEBP":
        return "webp"
    if data[:2] == b"BM":
        return "bmp"
    if data[:4] == b"\x00\x00\x01\x00":
        return "ico"
    start = data[:512].lstrip().lower()
    if start.startswith(b"<svg") or (start.startswith(b"<?xml") and b"<svg" in start):
        return "svg"
    return "bin"


def parse_icon_file(path: Path, icon_type: int, *, icon_url: Optional[str] = None) -> Optional[ParsedIconFile]:
    """Parse a cached icon file into normalized metadata."""
    if not path.exists() or not path.is_file():
        return None

    data = path.read_bytes()
    file_type = detect_image_extension(data)
    md5 = hashlib.md5(data).hexdigest()
    sha256 = hashlib.sha256(data).hexdigest()
    width: Optional[int] = None
    height: Optional[int] = None

    if file_type == "svg":
        width, height = _parse_svg_dimensions(data)
    elif file_type != "bin":
        width, height = _parse_raster_dimensions(data)

    phash: Optional[str] = None
    if file_type not in {"svg", "bin"}:
        try:
            phash = compute_phash(io.BytesIO(data))
        except Exception:
            phash = None

    return ParsedIconFile(
        icon_type=icon_type,
        path=path,
        file_type=file_type,
        size_bytes=len(data),
        md5=md5,
        sha256=sha256,
        width=width,
        height=height,
        phash=phash,
        icon_url=icon_url,
    )


def _parse_raster_dimensions(data: bytes) -> tuple[Optional[int], Optional[int]]:
    try:
        from PIL import Image

        with Image.open(io.BytesIO(data)) as img:
            width, height = img.size
            return int(width), int(height)
    except Exception:
        return None, None


def _parse_svg_dimensions(data: bytes) -> tuple[Optional[int], Optional[int]]:
    try:
        text = data.decode("utf-8", errors="ignore")
    except Exception:
        return None, None

    width = _parse_svg_length(_find_svg_attr(text, "width"))
    height = _parse_svg_length(_find_svg_attr(text, "height"))
    if width is not None and height is not None:
        return width, height

    # Fallback: derive dimensions from viewBox="minx miny width height"
    viewbox = _find_svg_attr(text, "viewBox")
    if viewbox:
        parts = re.split(r"[,\s]+", viewbox.strip())
        if len(parts) == 4:
            try:
                vb_width = float(parts[2])
                vb_height = float(parts[3])
                return int(vb_width), int(vb_height)
            except Exception:
                return width, height

    return width, height


def _find_svg_attr(text: str, name: str) -> Optional[str]:
    match = re.search(rf"\b{name}\s*=\s*['\"]([^'\"]+)['\"]", text, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def _parse_svg_length(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        match = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)", value)
        if not match:
            return None
        return int(float(match.group(1)))
    except Exception:
        return None
