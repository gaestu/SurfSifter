"""Browser inventory feature - artifact browser with multiple subtabs.

Provides comprehensive browser artifact viewing: history, cookies, bookmarks,
downloads, autofill, credentials, sessions, permissions, media, extensions,
browser config, storage, tokens, and cache.
"""

from .tab import BrowserInventoryTab

__all__ = ["BrowserInventoryTab"]
