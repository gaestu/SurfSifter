"""
Chromium Sync Data Extractor

Extracts sync account information and device inventory from Chromium browsers.

Features:
- Multi-partition support with file_list-based discovery
- Schema warning support for unknown JSON keys
- Preferences JSON parsing (account_info, google.services, sync sections)
"""

from .extractor import ChromiumSyncDataExtractor

__all__ = ["ChromiumSyncDataExtractor"]
