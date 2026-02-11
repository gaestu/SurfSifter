"""
Internet Explorer & Legacy Edge (EdgeHTML) extractors.

These browsers use different storage formats than Chromium/Firefox:
- ESE (Extensible Storage Engine) database for WebCache
- container.dat files for Legacy Edge
- .url files for favorites
- Registry for typed URLs and settings

Browser Coverage:
- Internet Explorer 10/11
- Legacy Edge (EdgeHTML/UWP) - Pre-Chromium Microsoft Edge (2015-2020)

Note: Chromium-based Edge (post-2020) is handled by the chromium module.

Main Database:
    WebCacheV01.dat - ESE database containing:
    - History (browsing history)
    - Cookies (cookie data)
    - iedownload (download history)
    - Content (cached content metadata)
    - DOMStore (DOM storage)

Modular Extractor Architecture:
    WebCache-based (IE/Edge shared database):
    - IEWebCacheExtractor: Extraction phase - copies raw WebCacheV01.dat from evidence
    - IEHistoryExtractor: Ingestion phase - parses History container
    - IECookiesExtractor: Ingestion phase - parses Cookies container(s) from WebCache
    - IEDownloadsExtractor: Ingestion phase - parses iedownload container
    - IECacheMetadataExtractor: Ingestion phase - parses Content containers
    - IEDOMStorageExtractor: Extract+Ingest - parses DOMStore from WebCache + Edge files

    File-based:
    - IEINetCookiesExtractor: Extract+Ingest - parses .cookie/.txt files from INetCookies
    - IEFavoritesExtractor: Extract+Ingest - parses .url shortcut files
    - IETypedURLsExtractor: Extract+Ingest - parses TypedURLs from Registry

    Legacy Edge specific:
    - LegacyEdgeContainerExtractor: Extract+Ingest - parses container.dat files
    - EdgeReadingListExtractor: Extract+Ingest - parses Reading List entries

Usage:
    from extractors.browser.ie_legacy import (
        IEWebCacheExtractor,
        IEHistoryExtractor,
        IECookiesExtractor,
        IEDownloadsExtractor,
        IECacheMetadataExtractor,
        IEDOMStorageExtractor,
        IEINetCookiesExtractor,
        IEFavoritesExtractor,
        IETypedURLsExtractor,
        IETabRecoveryExtractor,
        LegacyEdgeContainerExtractor,
        EdgeReadingListExtractor,
    )
"""

from .webcache.extractor import IEWebCacheExtractor
from .history.extractor import IEHistoryExtractor
from .cookies.extractor import IECookiesExtractor
from .downloads.extractor import IEDownloadsExtractor
from .favorites.extractor import IEFavoritesExtractor
from .typed_urls.extractor import IETypedURLsExtractor
from .edge_container.extractor import LegacyEdgeContainerExtractor
from .inetcookies.extractor import IEINetCookiesExtractor
from .dom_storage.extractor import IEDOMStorageExtractor
from .cache_metadata.extractor import IECacheMetadataExtractor
from .reading_list.extractor import EdgeReadingListExtractor
from .tab_recovery.extractor import IETabRecoveryExtractor

__all__ = [
    'IEWebCacheExtractor',
    'IEHistoryExtractor',
    'IECookiesExtractor',
    'IEDownloadsExtractor',
    'IECacheMetadataExtractor',
    'IEDOMStorageExtractor',
    'IEINetCookiesExtractor',
    'IEFavoritesExtractor',
    'IETypedURLsExtractor',
    'IETabRecoveryExtractor',
    'LegacyEdgeContainerExtractor',
    'EdgeReadingListExtractor',
]
