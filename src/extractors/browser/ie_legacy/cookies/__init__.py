"""
IE/Legacy Edge Cookies Extractor.

Parses cookie data from WebCacheV01.dat ESE database.
"""

from .extractor import IECookiesExtractor

# Registry expects IeLegacyCookiesExtractor (family_artifact pattern)
IeLegacyCookiesExtractor = IECookiesExtractor

__all__ = ["IECookiesExtractor", "IeLegacyCookiesExtractor"]
