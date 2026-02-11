"""
Image Carving Extractor

Extracts and ingests carved images from unallocated space using foremost/scalpel
with perceptual hashing and EXIF metadata extraction.

Features:
- Forensic file carving (foremost/scalpel)
- Perceptual hash clustering (phash, dhash, average)
- EXIF metadata extraction
- Parallel processing for CPU-bound operations
- Order-independent enrichment via image_discoveries table
"""

from .extractor import ImageCarvingExtractor
from .enrichment import ingest_with_enrichment, parse_foremost_audit_with_bytes

__all__ = [
    'ImageCarvingExtractor',
    'ingest_with_enrichment',
    'parse_foremost_audit_with_bytes',
]
