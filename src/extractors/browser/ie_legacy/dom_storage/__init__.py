"""
IE/Legacy Edge DOM Storage Extractor.

Extracts and parses DOM Storage (Web Storage API) data from
WebCache database and Legacy Edge file-based storage.
"""

from .extractor import IEDOMStorageExtractor

# Registry expects IeLegacyDomStorageExtractor (family_artifact pattern)
IeLegacyDomStorageExtractor = IEDOMStorageExtractor

__all__ = ["IEDOMStorageExtractor", "IeLegacyDomStorageExtractor"]
