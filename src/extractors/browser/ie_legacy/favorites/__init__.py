"""
IE/Legacy Edge Favorites Extractor.

Parses bookmark data from .url shortcut files.
"""

from .extractor import IEFavoritesExtractor

# Registry expects IeLegacyFavoritesExtractor (family_artifact pattern)
IeLegacyFavoritesExtractor = IEFavoritesExtractor

__all__ = ["IEFavoritesExtractor", "IeLegacyFavoritesExtractor"]
