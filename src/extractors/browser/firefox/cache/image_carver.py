"""
Firefox Cache2 Image Carver

Extracts and processes image content from Firefox cache2 response bodies.
Handles decompression (gzip/brotli/zstd), format detection via magic bytes,
and generates MD5, SHA-256, and perceptual hashes.

Uses unified image signature detection from image_signatures.py.
"""

from __future__ import annotations

import gzip
import hashlib
from io import BytesIO
from pathlib import Path
from typing import Dict, Optional, Tuple, Any

from core.logging import get_logger
from core.phash import compute_phash
from ....image_signatures import detect_image_type as unified_detect_image_type

LOGGER = get_logger("extractors.cache_firefox.image_carver")

# Try to import brotli for brotli-compressed content
try:
    import brotli
    BROTLI_AVAILABLE = True
except ImportError:
    BROTLI_AVAILABLE = False
    LOGGER.debug("brotli library not available; brotli-compressed content will not be decompressed")

# Try to import zstandard for zstd-compressed content (Firefox 90+)
try:
    import zstandard
    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False
    LOGGER.debug("zstandard library not available; zstd-compressed content will not be decompressed")


def extract_body(
    data: bytes,
    meta_offset: int,
    content_encoding: Optional[str] = None,
) -> bytes:
    """
    Extract and decompress response body from cache2 entry.

    Args:
        data: Full cache2 entry file content
        meta_offset: Offset where metadata begins (body ends)
        content_encoding: HTTP Content-Encoding header value

    Returns:
        Decompressed body bytes, or raw body if decompression fails/not needed
    """
    if meta_offset <= 0 or meta_offset > len(data):
        LOGGER.warning("Invalid meta_offset %d for data length %d", meta_offset, len(data))
        return b''

    body = data[0:meta_offset]

    if not body:
        return b''

    # Normalize encoding
    encoding = content_encoding.lower().strip() if content_encoding else ''

    # Try gzip decompression
    if 'gzip' in encoding:
        try:
            body = gzip.decompress(body)
            LOGGER.debug("Decompressed gzip body: %d -> %d bytes", meta_offset, len(body))
        except Exception as e:
            LOGGER.debug("Failed to decompress gzip body: %s", e)
            # Return raw body on failure - might be partially valid

    # Try brotli decompression
    elif 'br' in encoding:
        if BROTLI_AVAILABLE:
            try:
                body = brotli.decompress(body)
                LOGGER.debug("Decompressed brotli body: %d -> %d bytes", meta_offset, len(body))
            except Exception as e:
                LOGGER.debug("Failed to decompress brotli body: %s", e)
        else:
            LOGGER.debug("Brotli content detected but library not available")

    # Try zstd decompression (Firefox 90+)
    elif 'zstd' in encoding:
        if ZSTD_AVAILABLE:
            try:
                dctx = zstandard.ZstdDecompressor()
                body = dctx.decompress(body)
                LOGGER.debug("Decompressed zstd body: %d -> %d bytes", meta_offset, len(body))
            except Exception as e:
                LOGGER.debug("Failed to decompress zstd body: %s", e)
        else:
            LOGGER.debug("zstd content detected but zstandard library not available")

    # Try deflate decompression
    elif 'deflate' in encoding:
        import zlib
        try:
            body = zlib.decompress(body, -zlib.MAX_WBITS)
            LOGGER.debug("Decompressed deflate body: %d -> %d bytes", meta_offset, len(body))
        except Exception as e:
            LOGGER.debug("Failed to decompress deflate body: %s", e)

    return body


def detect_image_type(body: bytes) -> Optional[Tuple[str, str]]:
    """
    Detect image format using magic byte signatures.

    Delegates to unified image_signatures module for consistency.

    Args:
        body: Decompressed response body

    Returns:
        Tuple of (format_name, extension) or None if not an image
    """
    return unified_detect_image_type(body)


def compute_hashes(body: bytes) -> Tuple[str, str]:
    """
    Compute MD5 and SHA-256 hashes of body content.

    Args:
        body: Image body bytes

    Returns:
        Tuple of (md5_hex, sha256_hex)
    """
    md5 = hashlib.md5(body).hexdigest()
    sha256 = hashlib.sha256(body).hexdigest()
    return md5, sha256


def save_carved_image(
    body: bytes,
    run_dir: Path,
    cache_filename: str,
    image_type: Tuple[str, str],
) -> Dict[str, Any]:
    """
    Save carved image to disk and compute hashes.

    Args:
        body: Decompressed image body bytes
        run_dir: Extraction run directory
        cache_filename: Original cache entry filename (hash-based)
        image_type: Tuple of (format_name, extension) from detect_image_type

    Returns:
        Dict with carved image metadata:
        {
            "rel_path": str,      # Relative path from run_dir parent
            "filename": str,      # Saved filename
            "md5": str,           # MD5 hash
            "sha256": str,        # SHA-256 hash
            "phash": str | None,  # Perceptual hash (None if computation fails)
            "size_bytes": int,    # File size
            "format": str,        # Image format (jpeg, png, etc.)
        }
    """
    fmt, ext = image_type

    # Create carved_images directory
    images_dir = run_dir / "carved_images"
    images_dir.mkdir(exist_ok=True)

    # Save with cache filename + detected extension
    dest_path = images_dir / f"{cache_filename}{ext}"
    dest_path.write_bytes(body)

    # Compute hashes
    md5, sha256 = compute_hashes(body)

    # Compute perceptual hash
    try:
        phash = compute_phash(BytesIO(body))
    except Exception as e:
        LOGGER.debug("Failed to compute phash for %s: %s", cache_filename, e)
        phash = None

    # Calculate relative path from run_dir's parent (output_dir)
    # This ensures consistency with manifest paths
    try:
        rel_path = str(dest_path.relative_to(run_dir.parent))
    except ValueError:
        # Fallback if paths don't have expected relationship
        rel_path = str(dest_path)

    return {
        "rel_path": rel_path,
        "filename": dest_path.name,
        "md5": md5,
        "sha256": sha256,
        "phash": phash,
        "size_bytes": len(body),
        "format": fmt,
    }


def carve_image_from_cache_entry(
    data: bytes,
    meta_offset: int,
    content_encoding: Optional[str],
    content_type: Optional[str],
    run_dir: Path,
    cache_filename: str,
) -> Optional[Dict[str, Any]]:
    """
    High-level function to extract and save image from cache entry.

    Combines body extraction, decompression, format detection, and saving.

    Args:
        data: Full cache2 entry file content
        meta_offset: Offset where metadata begins
        content_encoding: HTTP Content-Encoding (gzip, br, etc.)
        content_type: HTTP Content-Type (image/jpeg, etc.)
        run_dir: Extraction run directory
        cache_filename: Cache entry filename

    Returns:
        Dict with carved image metadata, or None if not an image or failed
    """
    # Quick check: if content_type is known and not image, skip
    if content_type:
        ct_lower = content_type.lower()
        # Accept explicit image types
        is_image_content_type = (
            ct_lower.startswith('image/') or
            'svg' in ct_lower or
            ct_lower == 'application/octet-stream'  # May be image
        )
        # Skip known non-image types
        if not is_image_content_type and not ct_lower.startswith('application/octet-stream'):
            if any(x in ct_lower for x in ['text/', 'application/json', 'application/javascript']):
                LOGGER.debug("Skipping non-image content type: %s", content_type)
                return None

    # Extract and decompress body
    body = extract_body(data, meta_offset, content_encoding)
    if not body:
        LOGGER.debug("Empty body for cache entry %s", cache_filename)
        return None

    # Detect image type via magic bytes
    image_type = detect_image_type(body)
    if not image_type:
        LOGGER.debug("No image signature detected in %s", cache_filename)
        return None

    # Save carved image
    try:
        result = save_carved_image(body, run_dir, cache_filename, image_type)
        LOGGER.info(
            "Carved %s image from %s: %d bytes, SHA256: %s",
            result["format"],
            cache_filename,
            result["size_bytes"],
            result["sha256"][:16] + "..."
        )
        return result
    except Exception as e:
        LOGGER.warning("Failed to save carved image %s: %s", cache_filename, e)
        return None
