"""Internet Explorer WebCache extractor."""

from .extractor import IEWebCacheExtractor

# Registry expects IeLegacyWebcacheExtractor (family_artifact pattern)
IeLegacyWebcacheExtractor = IEWebCacheExtractor

__all__ = ['IEWebCacheExtractor', 'IeLegacyWebcacheExtractor']
