"""
Safari Browser Storage Extractor.

Extracts LocalStorage and IndexedDB from Safari on macOS.

Module structure:
- extractor.py: Main SafariStorageExtractor class
- _discovery.py: Multi-partition storage file discovery
- _parsers.py: SQLite parsers for LocalStorage and IndexedDB
- _schemas.py: Known table/column definitions for schema warnings
"""

from .extractor import SafariStorageExtractor

__all__ = ["SafariStorageExtractor"]
