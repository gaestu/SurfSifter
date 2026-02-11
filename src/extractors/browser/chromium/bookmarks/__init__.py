"""
Chromium Bookmarks Extractor

Exports:
- ChromiumBookmarksExtractor: Main extractor class for registry discovery
- parse_bookmarks_json: Parser function for Bookmarks JSON files
- get_bookmark_stats: Quick statistics from parsed bookmarks
- ChromiumBookmark: Dataclass representing a single bookmark

Added schema warning support
"""
from .extractor import ChromiumBookmarksExtractor
from ._parser import parse_bookmarks_json, get_bookmark_stats, ChromiumBookmark

__all__ = [
    "ChromiumBookmarksExtractor",
    "parse_bookmarks_json",
    "get_bookmark_stats",
    "ChromiumBookmark",
]
