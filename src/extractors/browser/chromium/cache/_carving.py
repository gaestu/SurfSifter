"""
Image carving and hashing utilities for cache extraction.

Detects images in cached response bodies, computes hashes (MD5, SHA-256, pHash),
and saves carved images to disk.
"""

from __future__ import annotations

import hashlib
import io
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

from core.logging import get_logger
from ....image_signatures import detect_image_type, get_extension_for_format

if TYPE_CHECKING:
    from ._parser import CacheEntry
    from .blockfile import BlockfileCacheEntry

LOGGER = get_logger("extractors.cache_simple.carving")


def detect_image_format(data: bytes) -> Optional[str]:
    """
    Detect image format from magic bytes using unified signatures.

    Args:
        data: First bytes of the data to check

    Returns:
        Format name (e.g., 'jpeg', 'png', 'gif') or None if not an image
    """
    result = detect_image_type(data)
    if result:
        return result[0]  # Return format name only
    return None


def carve_and_hash_image(
    body: bytes,
    output_dir: Path,
    entry: "CacheEntry",
    run_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Detect if body is an image, compute hashes, save to disk.

    Args:
        body: Decompressed response body
        output_dir: Directory for carved images
        entry: Cache entry metadata
        run_id: Extraction run ID for provenance

    Returns:
        Image metadata dict with rel_path/filename, or None if not an image
    """
    if not body or len(body) < 8:
        return None

    fmt = detect_image_format(body)
    if not fmt:
        return None

    try:
        # Compute hashes
        md5 = hashlib.md5(body).hexdigest()
        sha256 = hashlib.sha256(body).hexdigest()

        # Compute pHash
        phash = None
        try:
            from PIL import Image
            import imagehash

            img = Image.open(io.BytesIO(body))
            phash = str(imagehash.phash(img))
        except Exception as e:
            LOGGER.debug("pHash computation failed for %s: %s", entry.url, e)

        # Save carved image under run_id directory to avoid cross-run collisions
        carved_dir = output_dir / run_id / "carved_images"
        carved_dir.mkdir(parents=True, exist_ok=True)

        ext = get_extension_for_format(fmt)
        filename = f"{sha256[:16]}{ext}"
        dest_path = carved_dir / filename

        # Avoid overwriting if same hash
        if not dest_path.exists():
            dest_path.write_bytes(body)

        # rel_path is relative to output_dir and includes filename
        rel_path = str(dest_path.relative_to(output_dir))

        return {
            'format': fmt,
            'md5': md5,
            'sha256': sha256,
            'phash': phash,
            'size_bytes': len(body),
            'rel_path': rel_path,
            'filename': filename,
            'cache_key': entry.url,
            'source_file': str(entry.file_path) if entry.file_path else None,
        }

    except Exception as e:
        LOGGER.debug("Image carving failed for %s: %s", entry.url, e)
        return None


def carve_blockfile_image(
    body: bytes,
    fmt: str,
    entry: "BlockfileCacheEntry",
    extraction_dir: Path,
    run_id: str,
) -> Optional[Dict[str, Any]]:
    """
    Carve and hash an image from blockfile cache body.

    Args:
        body: Decompressed body bytes
        fmt: Detected image format
        entry: Blockfile cache entry
        extraction_dir: Base extraction directory
        run_id: Run ID for output path

    Returns:
        Image metadata dict with rel_path/filename or None
    """
    try:
        # Compute hashes
        md5 = hashlib.md5(body).hexdigest()
        sha256 = hashlib.sha256(body).hexdigest()

        # Compute pHash
        phash = None
        try:
            from PIL import Image
            import imagehash

            img = Image.open(io.BytesIO(body))
            phash = str(imagehash.phash(img))
        except Exception as e:
            LOGGER.debug("pHash computation failed for blockfile entry: %s", e)

        # Save carved image under run_id directory
        carved_dir = extraction_dir / run_id / "carved_images"
        carved_dir.mkdir(parents=True, exist_ok=True)

        ext = get_extension_for_format(fmt)
        filename = f"{sha256[:16]}{ext}"
        dest_path = carved_dir / filename

        # Avoid overwriting if same hash
        if not dest_path.exists():
            dest_path.write_bytes(body)

        # rel_path is relative to extraction_dir and includes filename
        rel_path = str(dest_path.relative_to(extraction_dir))

        return {
            "format": fmt,
            "md5": md5,
            "sha256": sha256,
            "phash": phash,
            "size_bytes": len(body),
            "rel_path": rel_path,
            "filename": filename,
        }

    except Exception as e:
        LOGGER.debug("Image carving failed for blockfile entry: %s", e)
        return None


# Backward compatibility aliases
_detect_image_format = detect_image_format
_carve_and_hash_image = carve_and_hash_image
