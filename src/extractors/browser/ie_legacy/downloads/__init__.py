"""
IE/Legacy Edge Downloads Extractor.

Parses download history from WebCacheV01.dat ESE database.
"""

from .extractor import IEDownloadsExtractor

# Registry expects IeLegacyDownloadsExtractor (family_artifact pattern)
IeLegacyDownloadsExtractor = IEDownloadsExtractor

__all__ = ["IEDownloadsExtractor", "IeLegacyDownloadsExtractor"]
