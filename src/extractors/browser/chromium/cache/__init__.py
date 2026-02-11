"""
Chromium Cache Extractor

Extracts and ingests Chrome/Edge/Opera/Brave HTTP cache with full forensic provenance.
Supports both modern simple cache format and legacy blockfile format.

This is the canonical location for Chromium cache parsing.
For backward compatibility, also available at:
- extractors.cache_simple (re-export)
- extractors.cache.CacheSimpleExtractor (re-export)
"""

from .extractor import CacheSimpleExtractor

# Also export as ChromiumCacheExtractor for new code
ChromiumCacheExtractor = CacheSimpleExtractor

__all__ = [
    "CacheSimpleExtractor",
    "ChromiumCacheExtractor",
]
