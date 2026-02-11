"""
Firefox Browser Storage Extractor

Extracts Local Storage and IndexedDB from Firefox browsers.

Architecture:
- extractor.py: Main extractor class
- _discovery.py: Multi-partition discovery using file_list
- _parsers.py: SQLite parsing with schema warnings
- _schemas.py: Known tables, columns, and enums
- widget.py: Configuration UI widget
- analyzer.py: Deep value analysis for forensic artifacts
"""

from .extractor import FirefoxStorageExtractor

# Alias for backward compatibility
FirefoxBrowserStorageExtractor = FirefoxStorageExtractor

__all__ = ["FirefoxStorageExtractor", "FirefoxBrowserStorageExtractor"]