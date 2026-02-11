"""
IE/Legacy Edge File-based Cookies Extractor (INetCookies).

Parses .cookie and .txt files from INetCookies folders,
separate from WebCache database cookies.
"""

from .extractor import IEINetCookiesExtractor

# Registry expects IeLegacyInetcookiesExtractor (family_artifact pattern)
IeLegacyInetcookiesExtractor = IEINetCookiesExtractor

__all__ = ["IEINetCookiesExtractor", "IeLegacyInetcookiesExtractor"]
