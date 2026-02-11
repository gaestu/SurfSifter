from __future__ import annotations

import hashlib
from collections import OrderedDict
from pathlib import Path
from typing import Optional, Tuple

from PIL import Image


MAX_CACHE_ITEMS = 200
_CACHE_INDEX: "OrderedDict[str, Path]" = OrderedDict()


# Minimum valid JPEG size (header + minimal data)
_MIN_VALID_THUMB_SIZE = 100  # bytes


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
        with Image.open(image_path) as img:
            img.thumbnail(size)
            img.convert("RGB").save(thumb_path, format="JPEG")
        # Verify the generated thumbnail is valid
        if thumb_path.exists() and thumb_path.stat().st_size >= _MIN_VALID_THUMB_SIZE:
            _touch_cache(thumb_path)
            _prune_disk(cache_dir)
            return thumb_path
        # Generated file too small/empty - remove it
        thumb_path.unlink(missing_ok=True)
        return None
    except Exception:
        # PIL failed to process the image (unsupported format, corrupted, etc.)
        # Don't cache the failure - return None so placeholder icon is used
        thumb_path.unlink(missing_ok=True)
        return None


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
