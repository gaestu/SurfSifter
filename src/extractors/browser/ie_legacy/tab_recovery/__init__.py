"""IE/Legacy Edge Tab Recovery extractor."""

from .extractor import IETabRecoveryExtractor

# Registry expects IeLegacyTabRecoveryExtractor (family_artifact pattern)
IeLegacyTabRecoveryExtractor = IETabRecoveryExtractor

__all__ = ['IETabRecoveryExtractor', 'IeLegacyTabRecoveryExtractor']
