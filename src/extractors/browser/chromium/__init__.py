"""
Chromium browser family extractors.

Covers: Chrome, Edge, Brave, Opera (all use Blink/V8 engine).

All Chromium browsers share:
- Same SQLite schema for History, Cookies, Bookmarks, etc.
- Same cache format (Simple Cache or legacy Blockfile)
- Same profile structure (User Data/Default, Profile 1, etc.)
- Same timestamp format (WebKit microseconds since 1601-01-01)

Extractors:
- ChromiumHistoryExtractor: Browser visit history
- ChromiumCookiesExtractor: Browser cookies (with encryption detection)
- ChromiumBookmarksExtractor: Bookmark JSON files
- ChromiumDownloadsExtractor: Download history from History database
- ChromiumCacheExtractor: HTTP cache (simple cache + blockfile formats)
- ChromiumMediaHistoryExtractor: Media playback history
- ChromiumAutofillExtractor: Autofill data, profiles, credentials, credit cards
- ChromiumPermissionsExtractor: Site permissions from Preferences JSON
- ChromiumFaviconsExtractor: Favicon data and page mappings
- ChromiumExtensionsExtractor: Browser extension metadata and permissions
- ChromiumSyncDataExtractor: Sync account info and device data
- ChromiumTransportSecurityExtractor: HSTS/preload entries
- ChromiumSessionsExtractor: Session/tab data from SNSS files
- ChromiumBrowserStorageExtractor: Local Storage, Session Storage, IndexedDB
- ChromiumSiteEngagementExtractor: Site/media engagement metrics from Preferences
"""

from .history import ChromiumHistoryExtractor
from .cookies import ChromiumCookiesExtractor
from .bookmarks import ChromiumBookmarksExtractor
from .downloads import ChromiumDownloadsExtractor
from .cache import CacheSimpleExtractor, ChromiumCacheExtractor
from .media_history import MediaHistoryExtractor, ChromiumMediaHistoryExtractor
from .autofill import ChromiumAutofillExtractor
from .permissions import ChromiumPermissionsExtractor
from .favicons import ChromiumFaviconsExtractor
from .extensions import ChromiumExtensionsExtractor
from .sync_data import ChromiumSyncDataExtractor
from .transport_security import ChromiumTransportSecurityExtractor
from .sessions import ChromiumSessionsExtractor
from .storage import ChromiumBrowserStorageExtractor
from .site_engagement import ChromiumSiteEngagementExtractor

__all__ = [
    "ChromiumHistoryExtractor",
    "ChromiumCookiesExtractor",
    "ChromiumBookmarksExtractor",
    "ChromiumDownloadsExtractor",
    "ChromiumCacheExtractor",
    "CacheSimpleExtractor",  # Legacy name
    "ChromiumMediaHistoryExtractor",
    "MediaHistoryExtractor",  # Legacy name
    "ChromiumAutofillExtractor",
    "ChromiumPermissionsExtractor",
    "ChromiumFaviconsExtractor",
    "ChromiumExtensionsExtractor",
    "ChromiumSyncDataExtractor",
    "ChromiumTransportSecurityExtractor",
    "ChromiumSessionsExtractor",
    "ChromiumBrowserStorageExtractor",
    "ChromiumSiteEngagementExtractor",
]
