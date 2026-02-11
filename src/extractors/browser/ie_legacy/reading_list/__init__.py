"""
Edge Legacy Reading List Extractor.

Extracts reading list entries from Legacy Edge UWP package.
"""

from .extractor import EdgeReadingListExtractor

# Registry expects IeLegacyReadingListExtractor (family_artifact pattern)
IeLegacyReadingListExtractor = EdgeReadingListExtractor

__all__ = ["EdgeReadingListExtractor", "IeLegacyReadingListExtractor"]
