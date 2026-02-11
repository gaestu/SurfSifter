"""
Firefox Permissions Extractor

Extracts site permissions from Firefox's permissions.sqlite database.

Modules:
- extractor.py: Main extractor class with extraction/ingestion logic
- _schemas.py: Known tables, columns, and permission type mappings
- _parsers.py: SQLite parsing functions for permissions.sqlite and content-prefs.sqlite
"""

from .extractor import FirefoxPermissionsExtractor

# Re-export schema constants for tests and external use
from ._schemas import (
    FIREFOX_PERMISSION_VALUES,
    FIREFOX_PERMISSION_TYPE_MAP,
    KNOWN_PERMISSIONS_TABLES,
    KNOWN_MOZ_PERMS_COLUMNS,
)

__all__ = [
    "FirefoxPermissionsExtractor",
    "FIREFOX_PERMISSION_VALUES",
    "FIREFOX_PERMISSION_TYPE_MAP",
    "KNOWN_PERMISSIONS_TABLES",
    "KNOWN_MOZ_PERMS_COLUMNS",
]
