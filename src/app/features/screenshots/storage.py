"""
Screenshot storage utilities.

Provides functions for saving and importing screenshots to the evidence workspace.

Initial implementation for investigator screenshot documentation.
"""
from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from PySide6.QtGui import QPixmap

from core.database.manager import slugify_label

logger = logging.getLogger(__name__)

__all__ = [
    "ScreenshotMetadata",
    "save_screenshot",
    "import_screenshot",
    "get_screenshots_dir",
]


@dataclass
class ScreenshotMetadata:
    """Metadata for a saved screenshot."""

    dest_path: str      # Relative path from evidence folder
    filename: str       # Just the filename
    width: int
    height: int
    size_bytes: int
    md5: str
    sha256: str


def get_screenshots_dir(workspace_path: Path, evidence_label: str, evidence_id: int) -> Path:
    """
    Get the screenshots directory for an evidence.

    Args:
        workspace_path: Path to case workspace
        evidence_label: Evidence label
        evidence_id: Evidence ID

    Returns:
        Path to screenshots directory (created if needed)
    """
    slug = slugify_label(evidence_label, evidence_id)
    screenshots_dir = workspace_path / "evidences" / slug / "screenshots"
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    return screenshots_dir


def _compute_hashes(data: bytes) -> tuple[str, str]:
    """Compute MD5 and SHA-256 hashes of data."""
    md5 = hashlib.md5(data).hexdigest()
    sha256 = hashlib.sha256(data).hexdigest()
    return md5, sha256


def save_screenshot(
    pixmap: QPixmap,
    workspace_path: Path,
    evidence_label: str,
    evidence_id: int,
    prefix: str = "screenshot",
) -> ScreenshotMetadata:
    """
    Save a QPixmap screenshot to the evidence workspace.

    The file is saved as PNG with a timestamp-based filename to ensure uniqueness.

    Args:
        pixmap: QPixmap to save
        workspace_path: Path to case workspace
        evidence_label: Evidence label
        evidence_id: Evidence ID
        prefix: Filename prefix (default: "screenshot")

    Returns:
        ScreenshotMetadata with file info and hashes

    Raises:
        ValueError: If pixmap is null or save fails
    """
    if pixmap.isNull():
        raise ValueError("Cannot save null pixmap")

    screenshots_dir = get_screenshots_dir(workspace_path, evidence_label, evidence_id)
    slug = slugify_label(evidence_label, evidence_id)

    # Generate unique filename with timestamp
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    filename = f"{prefix}_{timestamp}.png"
    full_path = screenshots_dir / filename

    # Handle unlikely collision
    counter = 1
    while full_path.exists():
        filename = f"{prefix}_{timestamp}_{counter}.png"
        full_path = screenshots_dir / filename
        counter += 1

    # Save the pixmap
    if not pixmap.save(str(full_path), "PNG"):
        raise ValueError(f"Failed to save screenshot to {full_path}")

    # Read back for hashing
    data = full_path.read_bytes()
    md5, sha256 = _compute_hashes(data)

    # Build relative path from evidence folder
    rel_path = f"screenshots/{filename}"

    logger.info("Saved screenshot: %s (%d bytes, %dx%d)",
                full_path, len(data), pixmap.width(), pixmap.height())

    return ScreenshotMetadata(
        dest_path=rel_path,
        filename=filename,
        width=pixmap.width(),
        height=pixmap.height(),
        size_bytes=len(data),
        md5=md5,
        sha256=sha256,
    )


def import_screenshot(
    source_path: Path,
    workspace_path: Path,
    evidence_label: str,
    evidence_id: int,
) -> ScreenshotMetadata:
    """
    Import an external image file as a screenshot.

    The file is copied to the evidence screenshots directory.
    Supports common image formats (PNG, JPEG, GIF, WebP, BMP).

    Args:
        source_path: Path to image file to import
        workspace_path: Path to case workspace
        evidence_label: Evidence label
        evidence_id: Evidence ID

    Returns:
        ScreenshotMetadata with file info and hashes

    Raises:
        FileNotFoundError: If source file doesn't exist
        ValueError: If file is not a valid image or import fails
    """
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    # Load image to validate and get dimensions
    pixmap = QPixmap(str(source_path))
    if pixmap.isNull():
        raise ValueError(f"Could not load image: {source_path}")

    screenshots_dir = get_screenshots_dir(workspace_path, evidence_label, evidence_id)
    slug = slugify_label(evidence_label, evidence_id)

    # Generate unique filename with timestamp, preserving extension
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    suffix = source_path.suffix.lower() or ".png"
    filename = f"import_{timestamp}{suffix}"
    full_path = screenshots_dir / filename

    # Handle unlikely collision
    counter = 1
    while full_path.exists():
        filename = f"import_{timestamp}_{counter}{suffix}"
        full_path = screenshots_dir / filename
        counter += 1

    # Copy the file
    import shutil
    shutil.copy2(source_path, full_path)

    # Read for hashing
    data = full_path.read_bytes()
    md5, sha256 = _compute_hashes(data)

    # Build relative path from evidence folder
    rel_path = f"screenshots/{filename}"

    logger.info("Imported screenshot: %s -> %s (%d bytes, %dx%d)",
                source_path, full_path, len(data), pixmap.width(), pixmap.height())

    return ScreenshotMetadata(
        dest_path=rel_path,
        filename=filename,
        width=pixmap.width(),
        height=pixmap.height(),
        size_bytes=len(data),
        md5=md5,
        sha256=sha256,
    )
