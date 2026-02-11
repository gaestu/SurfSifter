"""
Browser Carver Extractor

Deep scan unallocated space for browser artifacts (History, Cache, Cookies).
Critical for forensic recovery from Deep Freeze systems, secure deletions,
or reformatted drives.

Uses foremost/scalpel for carving, then validates and parses SQLite files
to recover browser data.

Fixed limit enforcement - now actively monitors and terminates carving
         if limits are exceeded. Fixed O(n²) performance in limit checking.
Added safety guardrails (size/count caps, auto-pruning of non-ingested files)
         to prevent disk exhaustion from LevelDB false positives.
"""

from .extractor import BrowserCarverExtractor

# Registry-compatible alias (follows {Group}{Extractor}Extractor convention)
CarversBrowserCarverExtractor = BrowserCarverExtractor

__all__ = ["BrowserCarverExtractor", "CarversBrowserCarverExtractor"]
