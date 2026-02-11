"""
Unified Image Signature Detection

Centralized image format detection via magic bytes for all extractors.
Consolidates signatures from cache_simple and cache_firefox to prevent drift.

Supported formats:
- JPEG (all variants: JFIF, EXIF, ICC, SPIFF, Adobe, raw)
- PNG
- GIF (87a, 89a)
- WebP (RIFF container)
- BMP
- ICO
- TIFF (little-endian and big-endian)
- SVG (XML-based, with size limit)
- AVIF (HEIF container with avif/avis brand)
- HEIC (HEIF container with heic/heix brand)
"""

from __future__ import annotations

from typing import Optional, Tuple

from core.logging import get_logger

LOGGER = get_logger("extractors.image_signatures")

# Default size limit for SVG detection (avoid parsing huge XML)
DEFAULT_SVG_SIZE_LIMIT = 256 * 1024  # 256KB


# Magic byte signatures for image detection
# Maps signature bytes to (format_name, extension)
IMAGE_SIGNATURES = {
    # JPEG: FFD8FF variants
    b'\xff\xd8\xff\xe0': ('jpeg', '.jpg'),  # JFIF
    b'\xff\xd8\xff\xe1': ('jpeg', '.jpg'),  # EXIF
    b'\xff\xd8\xff\xe2': ('jpeg', '.jpg'),  # ICC
    b'\xff\xd8\xff\xe8': ('jpeg', '.jpg'),  # SPIFF
    b'\xff\xd8\xff\xdb': ('jpeg', '.jpg'),  # Raw
    b'\xff\xd8\xff\xee': ('jpeg', '.jpg'),  # Adobe
    b'\xff\xd8\xff': ('jpeg', '.jpg'),      # Generic JPEG (3-byte prefix)

    # PNG
    b'\x89PNG\r\n\x1a\n': ('png', '.png'),

    # GIF
    b'GIF87a': ('gif', '.gif'),
    b'GIF89a': ('gif', '.gif'),

    # BMP
    b'BM': ('bmp', '.bmp'),

    # ICO (Windows icon)
    b'\x00\x00\x01\x00': ('ico', '.ico'),

    # TIFF (little-endian and big-endian)
    b'II*\x00': ('tiff', '.tif'),
    b'MM\x00*': ('tiff', '.tif'),
}


def detect_image_type(
    data: bytes,
    svg_size_limit: int = DEFAULT_SVG_SIZE_LIMIT,
) -> Optional[Tuple[str, str]]:
    """
    Detect image format using magic byte signatures.

    Args:
        data: Raw bytes to check (at least first 32 bytes recommended)
        svg_size_limit: Maximum data size to check for SVG (default 256KB)

    Returns:
        Tuple of (format_name, extension) or None if not an image

    Examples:
        >>> detect_image_type(b'\\xff\\xd8\\xff\\xe0...')
        ('jpeg', '.jpg')
        >>> detect_image_type(b'\\x89PNG\\r\\n\\x1a\\n...')
        ('png', '.png')
        >>> detect_image_type(b'not an image')
        None
    """
    if not data or len(data) < 2:
        return None

    # Check fixed signatures (longest match first for accuracy)
    # Sort by length descending to prefer longer matches
    for signature, format_info in sorted(
        IMAGE_SIGNATURES.items(),
        key=lambda x: len(x[0]),
        reverse=True
    ):
        if data.startswith(signature):
            return format_info

    # Special case: WebP (RIFF container)
    # Format: RIFF<4-byte size>WEBP
    if data[:4] == b'RIFF' and len(data) >= 12 and data[8:12] == b'WEBP':
        return ('webp', '.webp')

    # Special case: SVG (XML-based)
    # Only check if data is within size limit to avoid memory issues
    if len(data) <= svg_size_limit:
        head = data[:256].lstrip()
        head_lower = head.lower()

        # Check for <svg tag directly
        if b'<svg' in head_lower:
            return ('svg', '.svg')

        # Check for XML declaration followed by svg
        if head_lower.startswith(b'<?xml') and b'<svg' in data[:1024].lower():
            return ('svg', '.svg')

    # Special case: AVIF/HEIC (ISO BMFF container with ftyp box)
    # Format: <4-byte size><ftyp><brand>
    if len(data) >= 12:
        # ftyp box at offset 4, brand at offset 8
        if data[4:8] == b'ftyp':
            # Check brands in the first 32 bytes
            brands = data[8:32]
            if b'avif' in brands or b'avis' in brands:
                return ('avif', '.avif')
            if b'heic' in brands or b'heix' in brands or b'mif1' in brands:
                return ('heic', '.heic')

    return None


def get_extension_for_format(fmt: str) -> str:
    """
    Get file extension for a format name.

    Args:
        fmt: Format name (e.g., 'jpeg', 'png')

    Returns:
        Extension with leading dot (e.g., '.jpg', '.png')
    """
    format_extensions = {
        'jpeg': '.jpg',
        'png': '.png',
        'gif': '.gif',
        'webp': '.webp',
        'bmp': '.bmp',
        'ico': '.ico',
        'tiff': '.tif',
        'svg': '.svg',
        'avif': '.avif',
        'heic': '.heic',
    }
    return format_extensions.get(fmt, f'.{fmt}')


# Supported image extensions for file collection
SUPPORTED_IMAGE_EXTENSIONS = {
    '.jpg', '.jpeg', '.jpe', '.jfif', '.png', '.gif', '.bmp', '.tiff', '.tif',
    '.webp', '.avif', '.heic', '.heif', '.svg', '.ico'
}


def is_supported_image_extension(path_or_ext: str) -> bool:
    """
    Check if a file path or extension is a supported image format.

    Args:
        path_or_ext: File path or extension (with or without leading dot)

    Returns:
        True if supported image format
    """
    ext = path_or_ext.lower()
    if '.' in ext:
        ext = '.' + ext.rsplit('.', 1)[-1]
    elif not ext.startswith('.'):
        ext = '.' + ext
    return ext in SUPPORTED_IMAGE_EXTENSIONS
