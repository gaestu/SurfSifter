"""Perceptual hashing utilities for image similarity detection."""
from __future__ import annotations

from pathlib import Path
from typing import Optional, BinaryIO

try:
    import imagehash
    from PIL import Image
    IMAGEHASH_AVAILABLE = True
    # Set a reasonable pixel limit to prevent decompression bombs during phash
    # This is checked by Image.open() when loading the image data
    Image.MAX_IMAGE_PIXELS = 175_000_000
except ImportError:
    IMAGEHASH_AVAILABLE = False

from .image_codecs import ensure_pillow_heif_registered
from .logging import get_logger

LOGGER = get_logger("core.phash")


def compute_phash(image_path_or_stream: Path | BinaryIO) -> Optional[str]:
    """
    Compute perceptual hash (average hash) for an image.

    Args:
        image_path_or_stream: Path to image file or opened binary stream

    Returns:
        Hexadecimal string representation of the perceptual hash, or None if unavailable

    Notes:
        - Uses average hash (aHash) algorithm from imagehash library
        - Returns 16-character hex string (64-bit hash)
        - Gracefully returns None if imagehash not available or image unreadable
    """
    if not IMAGEHASH_AVAILABLE:
        LOGGER.debug("imagehash library not available; skipping phash computation")
        return None

    try:
        ensure_pillow_heif_registered()
        img = Image.open(image_path_or_stream)
        # Use average hash - good balance of speed and accuracy
        phash = imagehash.average_hash(img)
        return str(phash)
    except Exception as exc:
        LOGGER.debug("Failed to compute phash: %s", exc)
        return None


def compute_phash_prefix(phash: Optional[str]) -> Optional[int]:
    """
    Convert first 4 hex characters of phash to integer for SQL indexing.

    Args:
        phash: Perceptual hash as 16-character hex string

    Returns:
        Integer value of first 16 bits (0-65535), or None if invalid

    Notes:
        - Used for two-phase similarity search optimization
        - SQL can filter by prefix range before Python Hamming comparison
        - Reduces O(N) full scan to O(N/k) where k depends on threshold
    """
    if not phash or len(phash) < 4:
        return None
    try:
        return int(phash[:4], 16)
    except ValueError:
        return None


def hamming_distance(hash1: str, hash2: str) -> int:
    """
    Compute Hamming distance between two perceptual hashes.

    Args:
        hash1: First hash as hex string
        hash2: Second hash as hex string

    Returns:
        Number of differing bits (0-64 for 64-bit hashes)

    Notes:
        - Lower distance = more similar images
        - Typical similarity threshold: ≤10 bits
    """
    if not IMAGEHASH_AVAILABLE:
        return 64  # Maximum distance if library unavailable

    try:
        h1 = imagehash.hex_to_hash(hash1)
        h2 = imagehash.hex_to_hash(hash2)
        return h1 - h2  # imagehash overloads subtraction for Hamming distance
    except Exception as exc:
        LOGGER.warning("Failed to compute Hamming distance: %s", exc)
        return 64  # Return maximum distance on error
