"""
Chromium Browser Storage Extractor

Extracts Local Storage, Session Storage, and IndexedDB from Chromium browsers.

Features:
- Multi-partition support via file_list discovery
- Schema warnings for unknown LevelDB patterns
- IndexedDB blob image extraction
- Deleted record recovery

Multi-partition support, schema warnings, file splitting
Initial implementation
"""

from .extractor import ChromiumStorageExtractor
from .widget import ChromiumStorageWidget

# Alias for backward compatibility
ChromiumBrowserStorageExtractor = ChromiumStorageExtractor

__all__ = [
    "ChromiumStorageExtractor",
    "ChromiumStorageWidget",
    "ChromiumBrowserStorageExtractor",
]