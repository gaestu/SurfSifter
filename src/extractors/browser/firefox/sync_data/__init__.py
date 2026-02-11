"""
Firefox Sync Data Extractor

Extracts sync account information from Firefox signedInUser.json.

Features:
- Multi-partition support via file_list discovery
- Schema warning support for unknown JSON keys
- Database helper integration (no raw SQL)
"""

from .extractor import FirefoxSyncDataExtractor
from ._parsers import parse_firefox_sync
from ._schemas import (
    KNOWN_ROOT_KEYS,
    KNOWN_ACCOUNT_DATA_KEYS,
    KNOWN_DEVICE_KEYS,
)

__all__ = [
    "FirefoxSyncDataExtractor",
    "parse_firefox_sync",
    "KNOWN_ROOT_KEYS",
    "KNOWN_ACCOUNT_DATA_KEYS",
    "KNOWN_DEVICE_KEYS",
]
