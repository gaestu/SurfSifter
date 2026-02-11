"""
IE/Legacy Edge Typed URLs Extractor.

Parses manually typed URLs from Windows Registry.
"""

from .extractor import IETypedURLsExtractor

# Registry expects IeLegacyTypedUrlsExtractor (family_artifact pattern)
IeLegacyTypedUrlsExtractor = IETypedURLsExtractor

__all__ = ["IETypedURLsExtractor", "IeLegacyTypedUrlsExtractor"]
