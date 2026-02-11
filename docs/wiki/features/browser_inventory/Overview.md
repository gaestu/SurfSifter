# Browser Inventory (Tab)

Browser/Cache Inventory tab - displays all discovered browser artifacts.

## Purpose
- Browser/Cache Inventory tab - displays all discovered browser artifacts.
- Shows extraction and ingestion status for browser history databases and cache files.
- Now includes subtabs for viewing parsed Cookies, Bookmarks, Browser Downloads, Autofill, Sessions, Permissions, and Media History (v0.70.0).

## When to use
- When reviewing parsed browser artifacts across many categories.
- When you need to tag, filter, or inspect detailed browser records.

## Data sources
- Evidence database browser tables (history, cookies, bookmarks, downloads, etc.).
- Inventory of discovered browser databases and cache files.

## Key controls
- Subtab-specific filters (domain, browser, origin, or search fields).
- Context menus for details, copying values, and tagging.
- Inventory status view for extraction/ingestion progress.

## Outputs
- Tags saved to the case database for selected artifacts.
- Detailed record dialogs for deeper inspection.

## Subtabs
- [[./inventory|Inventory]] - raw browser artifact files with extraction/ingestion status.
- [[./history|History]] - parsed browsing history.
- [[./cookies|Cookies]] - parsed cookies with domain/browser filters.
- [[./bookmarks|Bookmarks]] - saved bookmarks and folders.
- [[./downloads|Downloads]] - browser download history entries.
- [[./autofill|Autofill]] - container for form data, logins, and addresses.
- [[./restored-tabs|Restored Tabs]] - session restore tabs and windows.
- [[./permissions|Permissions]] - per-site permission settings.
- [[./media|Media]] - media playback history.
- [[./extensions|Extensions]] - installed extensions and risk metadata.
- [[./web-storage|Web Storage]] - container for storage views and tokens.
- [[./cache|Cache]] - browser cache entries.
- [[./form-data|Form Data]] - saved form field entries.
- [[./saved-logins|Saved Logins]] - stored website credentials.
- [[./addresses|Addresses]] - autofill profile fields.
- [[./search-engines|Search Engines]] - configured search providers.
- [[./deleted-history|Deleted History]] - deleted form history (Firefox).
- [[./block-list|Block List]] - sites with autofill disabled.
- [[./sites-overview|Sites Overview]] - per-site storage summaries.
- [[./storage-keys|Storage Keys]] - local/session storage keys.
- [[./indexeddb|IndexedDB]] - IndexedDB databases and records.
- [[./auth-tokens|Auth Tokens]] - tokens recovered from storage.

## Notes
- Shows extraction and ingestion status for browser history databases and cache files.
- Now includes subtabs for viewing parsed Cookies, Bookmarks, Browser Downloads, Autofill, Sessions, Permissions, and Media History (v0.70.0).
- Dialog classes moved to detail_dialogs.py in v1.12.0.
