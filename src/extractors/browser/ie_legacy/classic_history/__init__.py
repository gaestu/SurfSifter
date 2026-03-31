"""IE Classic History.IE5 extractor."""

from .extractor import IEClassicHistoryExtractor

# Registry expects IeLegacyClassicHistoryExtractor (family_artifact pattern)
IeLegacyClassicHistoryExtractor = IEClassicHistoryExtractor

__all__ = ['IEClassicHistoryExtractor', 'IeLegacyClassicHistoryExtractor']
