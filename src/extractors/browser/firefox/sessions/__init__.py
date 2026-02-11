"""
Firefox Sessions Extractor

Extracts session/tab data from Firefox sessionstore.jsonlz4 files.

Features:
- Multi-partition support via file_list discovery
- Form data extraction from session entries
- Schema warning support for unknown JSON keys
- Collision-safe extracted file naming
"""

from .extractor import FirefoxSessionsExtractor

__all__ = ["FirefoxSessionsExtractor"]
