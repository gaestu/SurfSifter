from __future__ import annotations

from contextlib import nullcontext
from io import BytesIO
from pathlib import Path
from threading import Lock
from typing import Optional, Tuple

from core.logging import get_logger
from core.safe_io import open_file_no_follow


_LOGGER = get_logger("core.image_codecs")
_INIT_LOCK = Lock()
_HEIF_REGISTERED = False
_HEIF_INIT_DONE = False
DEFAULT_MAX_IMAGE_PIXELS = 175_000_000
THUMBNAIL_MAX_IMAGE_PIXELS = 50_000_000

try:
    from PIL import Image
    from PIL.Image import DecompressionBombError

    PIL_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised in dependency-light envs
    Image = None  # type: ignore[assignment]
    DecompressionBombError = Exception  # type: ignore[assignment]
    PIL_AVAILABLE = False


def ensure_pillow_heif_registered() -> bool:
    """Register pillow-heif opener once so Pillow can decode HEIC/HEIF."""
    global _HEIF_REGISTERED, _HEIF_INIT_DONE

    if _HEIF_INIT_DONE:
        return _HEIF_REGISTERED

    with _INIT_LOCK:
        if _HEIF_INIT_DONE:
            return _HEIF_REGISTERED

        try:
            from pillow_heif import register_heif_opener
        except ImportError:
            _LOGGER.debug("pillow-heif is not installed; HEIC/HEIF decoding unavailable")
            _HEIF_REGISTERED = False
            _HEIF_INIT_DONE = True
            return False
        except Exception as exc:  # pragma: no cover - defensive
            _LOGGER.warning("Failed to import pillow-heif: %s", exc)
            _HEIF_REGISTERED = False
            _HEIF_INIT_DONE = True
            return False

        try:
            register_heif_opener()
            _HEIF_REGISTERED = True
            _LOGGER.debug("Registered pillow-heif opener for HEIC/HEIF decoding")
        except Exception as exc:
            _LOGGER.warning("Failed to register pillow-heif opener: %s", exc)
            _HEIF_REGISTERED = False
        finally:
            _HEIF_INIT_DONE = True

        return _HEIF_REGISTERED


def thumbnail_to_jpeg_bytes(
    image_path: Path,
    *,
    size: Tuple[int, int] = (96, 96),
    quality: int = 85,
    max_pixels: int = THUMBNAIL_MAX_IMAGE_PIXELS,
    containment_root: Optional[Path] = None,
) -> Optional[bytes]:
    """Render an image thumbnail to deterministic JPEG bytes.

    The pixel cap mirrors the carving/phash safety convention and prevents
    hostile carved images from forcing oversized native decoder work.  The
    white canvas keeps the output dimensions stable for XLSX embedding.
    """
    if not PIL_AVAILABLE or Image is None:
        return None

    try:
        ensure_pillow_heif_registered()
        image_source = (
            open_file_no_follow(image_path, containment_root=containment_root)
            if containment_root is not None
            else nullcontext(Path(image_path))
        )
        with image_source as source:
            with Image.open(source) as img:
                return _thumbnail_image_to_jpeg_bytes(
                    img,
                    size=size,
                    quality=quality,
                    max_pixels=max_pixels,
                )
    except DecompressionBombError as exc:
        _LOGGER.warning("Skipping oversized image thumbnail %s: %s", image_path, exc)
        return None
    except Exception:
        _LOGGER.warning("Failed to generate image thumbnail for %s", image_path, exc_info=True)
        return None


def _thumbnail_image_to_jpeg_bytes(
    img,
    *,
    size: Tuple[int, int],
    quality: int,
    max_pixels: int,
) -> bytes:
    pixels = img.width * img.height
    if pixels > max_pixels:
        raise DecompressionBombError(
            f"Image pixels {pixels} exceeds limit of {max_pixels}"
        )
    img.draft("RGB", size)
    img.thumbnail(size, Image.Resampling.LANCZOS)
    if img.mode in {"RGBA", "LA"} or (img.mode == "P" and "transparency" in img.info):
        rgba = img.convert("RGBA")
        background = Image.new("RGBA", rgba.size, "white")
        background.alpha_composite(rgba)
        img = background.convert("RGB")
    elif img.mode != "RGB":
        img = img.convert("RGB")

    canvas = Image.new("RGB", size, "white")
    x = (size[0] - img.width) // 2
    y = (size[1] - img.height) // 2
    canvas.paste(img, (x, y))

    buf = BytesIO()
    canvas.save(buf, format="JPEG", quality=quality)
    return buf.getvalue()
