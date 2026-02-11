"""
Media extractors - Image carving and filesystem image analysis.

This module provides extractors for media/image artifacts:
- Filesystem Images: Extract images from evidence filesystem
- Foremost Carver: Foremost-based image carving
- Scalpel: Scalpel-based image carving

Note: _shared.carving/ and image_signatures.py remain at extractors/ root
due to extensive imports across the codebase.

Usage:
    from extractors.media import (
        FilesystemImagesExtractor,
        ForemostCarverExtractor,
        ScalpelExtractor,
    )
"""

from __future__ import annotations

# Import from nested locations within media/
from .filesystem_images import FilesystemImagesExtractor
from .foremost_carver import ForemostCarverExtractor
from .scalpel import ScalpelExtractor

__all__ = [
    "FilesystemImagesExtractor",
    "ForemostCarverExtractor",
    "ScalpelExtractor",
]
