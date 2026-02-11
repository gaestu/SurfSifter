"""
Filesystem Images Extractor

Extracts images directly from evidence filesystem with full path context,
preserving original MACB timestamps and inode information.

Key features:
- Stream-based hashing (no full-file memory buffering)
- Signature-based image detection (not extension-only)
- Full filesystem path preservation
- MACB timestamp preservation (mtime, atime, ctime, crtime)
- Order-independent enrichment via image_discoveries table
"""

from .extractor import FilesystemImagesExtractor

# Registry-compatible alias (follows {Group}{Extractor}Extractor convention)
MediaFilesystemImagesExtractor = FilesystemImagesExtractor

__all__ = ["FilesystemImagesExtractor", "MediaFilesystemImagesExtractor"]

__all__ = ["FilesystemImagesExtractor"]
