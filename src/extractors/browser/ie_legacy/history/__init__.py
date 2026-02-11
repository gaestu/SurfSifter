"""Internet Explorer History extractor."""

from .extractor import IEHistoryExtractor

# Registry expects IeLegacyHistoryExtractor (family_artifact pattern)
IeLegacyHistoryExtractor = IEHistoryExtractor

__all__ = ['IEHistoryExtractor', 'IeLegacyHistoryExtractor']
