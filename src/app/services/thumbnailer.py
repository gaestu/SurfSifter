from __future__ import annotations

import hashlib
from collections import OrderedDict
from pathlib import Path
from typing import Optional, Tuple

from PIL import Image

from core.image_codecs import ensure_pillow_heif_registered


MAX_CACHE_ITEMS = 200
_CACHE_INDEX: "OrderedDict[str, Path]" = OrderedDict()


# Minimum valid JPEG size (header + minimal data)
_MIN_VALID_THUMB_SIZE = 100  # bytes
MAX_SVG_FILE_SIZE_BYTES = 20 * 1024 * 1024  # 20 MiB


def ensure_thumbnail(image_path: Path, cache_dir: Path, size: Tuple[int, int] = (200, 200)) -> Optional[Path]:
    """
    Generate or retrieve a cached thumbnail for an image.

    Args:
        image_path: Path to the source image
        cache_dir: Directory to store cached thumbnails
        size: Target thumbnail size (width, height)

    Returns:
        Path to the thumbnail file, or None if generation failed.

    Returns None on failure instead of creating empty placeholder.
             Also validates existing thumbnails aren't empty/corrupted.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = hashlib.md5(str(image_path).encode("utf-8")).hexdigest()
    thumb_path = cache_dir / f"{key}.jpg"

    # Check for valid cached thumbnail
    if thumb_path.exists():
        try:
            stat = thumb_path.stat()
            # Validate: must be newer than source AND have actual content
            if stat.st_mtime >= image_path.stat().st_mtime and stat.st_size >= _MIN_VALID_THUMB_SIZE:
                _touch_cache(thumb_path)
                _prune_disk(cache_dir)
                return thumb_path
            # Cached thumbnail is invalid (empty/stale) - regenerate
            thumb_path.unlink(missing_ok=True)
        except OSError:
            # Source file missing or inaccessible
            pass

    try:
        generated = False
        if image_path.suffix.lower() == ".svg":
            generated = _render_svg_thumbnail(image_path, thumb_path, size)
        else:
            ensure_pillow_heif_registered()
            with Image.open(image_path) as img:
                img.thumbnail(size)
                img.convert("RGB").save(thumb_path, format="JPEG")
            generated = True

        if not generated:
            thumb_path.unlink(missing_ok=True)
            return None

        # Verify the generated thumbnail is valid
        if thumb_path.exists() and thumb_path.stat().st_size >= _MIN_VALID_THUMB_SIZE:
            _touch_cache(thumb_path)
            _prune_disk(cache_dir)
            return thumb_path
        # Generated file too small/empty - remove it
        thumb_path.unlink(missing_ok=True)
        return None
    except Exception:
        # Image processing failed (unsupported format, corrupted file, etc.)
        # Don't cache the failure - return None so placeholder icon is used
        thumb_path.unlink(missing_ok=True)
        return None


def _render_svg_thumbnail(image_path: Path, thumb_path: Path, size: Tuple[int, int]) -> bool:
    try:
        if image_path.stat().st_size > MAX_SVG_FILE_SIZE_BYTES:
            return False
    except OSError:
        return False

    try:
        from PySide6.QtCore import QRectF, QSize
        from PySide6.QtGui import QImage, QPainter
        from PySide6.QtSvg import QSvgRenderer
    except Exception:
        return False

    try:
        svg_data = image_path.read_bytes()
    except OSError:
        return False

    renderer = QSvgRenderer(svg_data)
    if not renderer.isValid():
        return False

    target_width = max(1, int(size[0]))
    target_height = max(1, int(size[1]))

    target = QImage(target_width, target_height, QImage.Format.Format_RGB32)
    target.fill(0xFFFFFFFF)

    painter = QPainter(target)
    if not painter.isActive():
        painter.end()
        return False

    painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
    painter.setRenderHint(QPainter.RenderHint.SmoothPixmapTransform, True)

    source_size: QSize = renderer.defaultSize()
    source_width = float(source_size.width())
    source_height = float(source_size.height())
    if source_width <= 0.0 or source_height <= 0.0:
        view_box = renderer.viewBoxF()
        source_width = view_box.width()
        source_height = view_box.height()
    if source_width <= 0.0 or source_height <= 0.0:
        source_width = float(target_width)
        source_height = float(target_height)

    scale = min(target_width / source_width, target_height / source_height)
    render_width = source_width * scale
    render_height = source_height * scale
    x = (target_width - render_width) / 2.0
    y = (target_height - render_height) / 2.0
    target_rect = QRectF(x, y, render_width, render_height)

    try:
        renderer.render(painter, target_rect)
    finally:
        painter.end()
    return target.save(str(thumb_path), "JPEG")


def _touch_cache(thumb_path: Path) -> None:
    key = str(thumb_path)
    if key in _CACHE_INDEX:
        _CACHE_INDEX.move_to_end(key)
    else:
        _CACHE_INDEX[key] = thumb_path
    while len(_CACHE_INDEX) > MAX_CACHE_ITEMS:
        _, stale_path = _CACHE_INDEX.popitem(last=False)
        try:
            stale_path.unlink(missing_ok=True)
        except OSError:
            pass


def _prune_disk(cache_dir: Path) -> None:
    entries = list(cache_dir.glob("*.jpg"))
    threshold = MAX_CACHE_ITEMS * 2
    if len(entries) <= threshold:
        return
    try:
        entries.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    except OSError:
        return
    for stale in entries[MAX_CACHE_ITEMS:]:
        try:
            stale.unlink()
        except OSError:
            pass
        _CACHE_INDEX.pop(str(stale), None)
