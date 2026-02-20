from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

from PIL import Image, UnidentifiedImageError
from PIL.Image import DecompressionBombError

from core.image_codecs import ensure_pillow_heif_registered
from core.logging import get_logger

LOGGER = get_logger("extractors._shared.carving.exif")


def extract_exif(path: Path) -> Dict[str, str]:
    """Extract a subset of EXIF metadata using Pillow."""
    metadata: Dict[str, str] = {}
    try:
        ensure_pillow_heif_registered()
        with Image.open(path) as img:
            info = img.getexif()
            for tag, value in info.items():
                metadata[str(tag)] = str(value)
    except (FileNotFoundError, UnidentifiedImageError, OSError, DecompressionBombError) as exc:
        LOGGER.debug("EXIF extraction failed for %s: %s", path, exc)
    return metadata


def generate_thumbnail(path: Path, out_path: Path, size: tuple[int, int] = (256, 256)) -> Optional[Path]:
    """Generate a thumbnail for the provided image."""
    try:
        ensure_pillow_heif_registered()
        with Image.open(path) as img:
            img.thumbnail(size)
            img.save(out_path)
            return out_path
    except (FileNotFoundError, UnidentifiedImageError, OSError, DecompressionBombError) as exc:
        LOGGER.debug("Thumbnail generation failed for %s: %s", path, exc)
        return None
