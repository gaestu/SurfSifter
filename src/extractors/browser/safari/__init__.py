"""
Safari Browser Family Extractors.

Safari is Apple's web browser, exclusive to macOS (and iOS).
Uses WebKit engine with Apple-specific data formats:
- Cocoa timestamps (seconds since 2001-01-01)
- Plist files (binary or XML)
- Binary cookies format

Note: Safari support is marked EXPERIMENTAL due to unique formats.

Exported Extractors:
- SafariHistoryExtractor: Browser history from History.db
- SafariCookiesExtractor: Cookies from Cookies.binarycookies
- SafariBookmarksExtractor: Bookmarks from Bookmarks.plist
- SafariDownloadsExtractor: Downloads from Downloads.plist
- SafariFaviconsExtractor: Favicons, touch icons, and template icon mappings
- SafariSessionsExtractor: Open windows/tabs and recently closed tabs
"""

from .history import SafariHistoryExtractor
from .cookies import SafariCookiesExtractor
from .bookmarks import SafariBookmarksExtractor
from .downloads import SafariDownloadsExtractor
from .favicons import SafariFaviconsExtractor
from .sessions import SafariSessionsExtractor
from .cache import SafariCacheExtractor

__all__ = [
    "SafariHistoryExtractor",
    "SafariCookiesExtractor",
    "SafariBookmarksExtractor",
    "SafariDownloadsExtractor",
    "SafariFaviconsExtractor",
    "SafariSessionsExtractor",
    "SafariCacheExtractor",
]
