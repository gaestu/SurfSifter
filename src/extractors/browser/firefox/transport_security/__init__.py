"""
Firefox Transport Security Extractor

Extracts HSTS entries from Firefox SiteSecurityServiceState.txt.
"""

from .extractor import FirefoxTransportSecurityExtractor

__all__ = ["FirefoxTransportSecurityExtractor"]
