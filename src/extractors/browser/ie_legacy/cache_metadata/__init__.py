"""
IE/Legacy Edge Cache Metadata Extractor.

Extracts cache metadata from WebCache Content container,
providing cached URL listing without extracting actual cache files.
"""

from .extractor import IECacheMetadataExtractor

# Registry expects IeLegacyCacheMetadataExtractor (family_artifact pattern)
IeLegacyCacheMetadataExtractor = IECacheMetadataExtractor

__all__ = ["IECacheMetadataExtractor", "IeLegacyCacheMetadataExtractor"]
