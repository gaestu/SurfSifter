"""
Firefox browser family extractors.

Firefox uses the Gecko engine with SQLite-based artifact storage:
- places.sqlite: History (moz_historyvisits + moz_places), Bookmarks (moz_bookmarks + moz_places)
- cookies.sqlite: Cookies (moz_cookies)
- places.sqlite: Downloads (moz_annos annotations or legacy moz_downloads)
- cache2/: HTTP cache (body-first format with metadata at end)
- formhistory.sqlite: Form autofill data
- logins.json: Saved passwords (encrypted)

Also supports Tor Browser which is Firefox-based.

Exports:
    FirefoxHistoryExtractor: Extract visit-level browsing history
    FirefoxCookiesExtractor: Extract cookies (plaintext, not encrypted)
    FirefoxBookmarksExtractor: Extract bookmarks with folder hierarchy
    FirefoxDownloadsExtractor: Extract download history
    FirefoxCacheExtractor: Extract HTTP cache (cache2 format)
    FirefoxAutofillExtractor: Extract form history and saved logins
    FirefoxPermissionsExtractor: Site permissions from permissions.sqlite
    FirefoxFaviconsExtractor: Favicon data from favicons.sqlite
    FirefoxExtensionsExtractor: Browser extension metadata
    FirefoxSyncDataExtractor: Sync account info from signedInUser.json
    FirefoxTransportSecurityExtractor: HSTS entries from SiteSecurityServiceState.txt
    FirefoxSessionsExtractor: Session/tab data from sessionstore.jsonlz4
    FirefoxBrowserStorageExtractor: Local Storage and IndexedDB
    FirefoxTorStateExtractor: Tor Browser config/state files from Tor data directory
"""

from __future__ import annotations

from .history.extractor import FirefoxHistoryExtractor
from .cookies.extractor import FirefoxCookiesExtractor
from .bookmarks.extractor import FirefoxBookmarksExtractor
from .downloads.extractor import FirefoxDownloadsExtractor
from .cache import CacheFirefoxExtractor, FirefoxCacheExtractor
from .autofill import FirefoxAutofillExtractor
from .permissions import FirefoxPermissionsExtractor
from .favicons import FirefoxFaviconsExtractor
from .extensions import FirefoxExtensionsExtractor
from .sync_data import FirefoxSyncDataExtractor
from .transport_security import FirefoxTransportSecurityExtractor
from .sessions import FirefoxSessionsExtractor
from .storage import FirefoxBrowserStorageExtractor
from .tor_state import FirefoxTorStateExtractor

__all__ = [
    "FirefoxHistoryExtractor",
    "FirefoxCookiesExtractor",
    "FirefoxBookmarksExtractor",
    "FirefoxDownloadsExtractor",
    "FirefoxCacheExtractor",
    "CacheFirefoxExtractor",  # Legacy name
    "FirefoxAutofillExtractor",
    "FirefoxPermissionsExtractor",
    "FirefoxFaviconsExtractor",
    "FirefoxExtensionsExtractor",
    "FirefoxSyncDataExtractor",
    "FirefoxTransportSecurityExtractor",
    "FirefoxSessionsExtractor",
    "FirefoxBrowserStorageExtractor",
    "FirefoxTorStateExtractor",
]
