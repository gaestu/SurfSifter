# Safari Family

Source: `src/extractors/browser/safari/`

## Overview
- Scope: Apple Safari on macOS only; support is marked EXPERIMENTAL.
- Extraction: Discovers Safari artifacts via family patterns and copies them into per-extractor output dirs with manifest.json.
- Ingestion: Parses SQLite/plist/binary cookies, converts Cocoa timestamps to UTC ISO where available, and inserts evidence DB rows (with URL cross-posting for history/bookmarks/downloads).

## Extractors
### SafariHistoryExtractor
- Purpose: Extract Safari visit-level browsing history from macOS evidence (EXPERIMENTAL).
- Extraction (source): History.db plus History.db-wal/-journal/-shm discovered under Safari profile roots.
- Extraction (behavior): Copies matched files into a per-run output directory and writes manifest.json with hashes/metadata.
- Ingestion (transform + store): Parses History.db (SQLite) joining history_items/history_visits and converts Cocoa timestamps to UTC ISO.
- Ingestion (transform + store): Inserts visit records into the evidence DB and cross-posts URLs to a unified urls table.
- Outputs: manifest.json plus copied History.db files (including WAL/journal/shm if present).
- Notes: WAL/journal/shm files are not parsed; user/profile inferred from source path or "Default".

### SafariCookiesExtractor
- Purpose: Extract Safari cookies from Cookies.binarycookies on macOS (EXPERIMENTAL).
- Extraction (source): Cookies.binarycookies under Safari cookies roots.
- Extraction (behavior): Copies matched files into a per-run output directory and writes manifest.json with hashes/metadata.
- Ingestion (transform + store): Parses binary cookies via the binarycookies library; converts expiry/creation timestamps to UTC ISO.
- Ingestion (transform + store): Inserts cookie records into the evidence DB and marks cookies as not encrypted.
- Outputs: manifest.json plus copied Cookies.binarycookies.
- Notes: If binarycookies is missing, parsing yields no cookies; user/profile inferred from source path or "Default".

### SafariBookmarksExtractor
- Purpose: Extract Safari bookmarks from Bookmarks.plist on macOS (EXPERIMENTAL).
- Extraction (source): Bookmarks.plist under Safari profile roots.
- Extraction (behavior): Copies matched files into a per-run output directory and writes manifest.json with hashes/metadata.
- Ingestion (transform + store): Parses plist hierarchy; only "leaf" bookmark entries are ingested and folder paths are built from the hierarchy.
- Ingestion (transform + store): Inserts bookmark records into the evidence DB and cross-posts non-javascript/data URLs to a unified urls table.
- Outputs: manifest.json plus copied Bookmarks.plist.
- Notes: Safari bookmark plist does not store dates (date_added fields remain None); user/profile inferred from source path or "Default".

### SafariDownloadsExtractor
- Purpose: Extract Safari download history from Downloads.plist on macOS (EXPERIMENTAL).
- Extraction (source): Downloads.plist under Safari profile roots.
- Extraction (behavior): Copies matched files into a per-run output directory and writes manifest.json with hashes/metadata.
- Ingestion (transform + store): Parses plist list or dict["DownloadHistory"]; extracts URL/target_path/bytes and sets state to "complete".
- Ingestion (transform + store): Inserts download records into the evidence DB and cross-posts URLs with first_seen_utc=None.
- Outputs: manifest.json plus copied Downloads.plist.
- Notes: Safari stores only completed downloads and no download timestamps; user/profile inferred from source path or "Default".

### SafariCacheExtractor
- Purpose: Extract Safari Cache.db and cached response bodies from multiple cache storage locations (EXPERIMENTAL).
- Extraction (source): Cache.db (plus WAL/journal/shm companions), fsCachedData/ blobs, WebKitCache/Version */Blobs/*, WebKitCache/Version */Records/*/*/*, WebKit/NetworkCache/, and WebKit/CacheStorage/ discovered under Safari cache roots.
- Extraction (behavior): Groups cache files by root directory; classifies files by type (cache_db, fscached_data, webkit_cache, cache_storage); generates group IDs via SHA1 hash of cache root for correlation.
- Ingestion (transform + store): Registers extracted files in browser_inventory with artifact type classification and group IDs.
- Outputs: manifest.json plus copied Cache.db files, fsCachedData blobs, and WebKit cache entries.
- Notes: Multi-partition support via file_list discovery with fallback to filesystem iteration; companion files (WAL, SHM, journal) automatically collected.

### SafariFaviconsExtractor
- Purpose: Extract Safari favicon database and icon cache files from three separate storage locations (EXPERIMENTAL).
- Extraction (source): Favicon Cache/Favicons.db (plus companions), Favicon Cache/* files, Touch Icons Cache/* files, and Template Icons/* files.
- Extraction (behavior): Organizes files into three groups (favicons, touch_icons, template_icons); handles path collisions by adding SHA1 suffix to filename; extracts profile metadata from path.
- Ingestion (transform + store): Registers extracted files in browser_inventory with artifact type classification per icon group.
- Outputs: manifest.json plus copied Favicons.db and icon cache files organized by group.
- Notes: Profile-aware extraction groups files by Safari profile (default or explicit profile_id); multi-partition support.

### SafariSessionsExtractor
- Purpose: Extract Safari session recovery artifacts — active sessions and recently closed tabs (EXPERIMENTAL).
- Extraction (source): LastSession.plist (active session windows/tabs) and RecentlyClosedTabs.plist (user-closed tab history) under Safari profile roots.
- Extraction (behavior): Copies matched plist files into per-run output directory with manifest.json.
- Ingestion (transform + store): Parses plist session data including SessionWindows, TabStates, and ClosedTabOrWindowPersistentStates; converts Cocoa timestamps to UTC ISO; inserts into session_windows, session_tabs, session_tab_histories, closed_tabs, and cross-posts URLs.
- Outputs: manifest.json plus copied session plist files; session tables and urls entries.
- Notes: Detects private windows (is_private flag); reconstructs full back/forward navigation history per tab via NSKeyedArchive decoding (Safari 13+); recently closed tabs are high-value evidence of user intent.

### SafariStorageExtractor
- Purpose: Extract Safari browser web storage — LocalStorage and IndexedDB with optional deep value analysis (EXPERIMENTAL).
- Extraction (source): LocalStorage/*.localstorage (legacy), WebsiteData LocalStorage paths (modern), Databases/___IndexedDB/v*/{origin}/*.sqlite (legacy IndexedDB), and WebsiteData/IndexedDB paths (modern); plus WAL/SHM companions.
- Extraction (behavior): Copies matched storage files with per-origin metadata into manifest.
- Ingestion (transform + store): Parses legacy/modern LocalStorage (UTF-16LE encoded values) and IndexedDB (WebKit SQLite with ObjectStore/Records tables); optional deep scanning via StorageValueAnalyzer for URLs, emails, JWT tokens, and user identifiers; inserts into local_storages, indexeddb_databases, indexeddb_entries, storage_tokens, storage_identifiers, urls, and emails.
- Outputs: manifest.json plus copied storage files; storage tables, token/identifier rows when analysis enabled.
- Notes: Deep value analysis is configurable (analyze_values flag); detects JWT, OAuth, and session tokens with risk scoring; extracts SSO IDs and account markers; records unknown table/column structures as schema warnings for forensic review.

### SafariTopSitesExtractor
- Purpose: Extract Safari TopSites.plist — frequently-visited sites and user-banned sites (EXPERIMENTAL).
- Extraction (source): TopSites.plist (binary or XML plist) under Safari profile roots.
- Extraction (behavior): Copies matched plist file into per-run output directory with manifest.json.
- Ingestion (transform + store): Parses TopSites array and BannedURLStrings list; assigns sequential rank per file; distinguishes built-in shortcuts from user-added sites; inserts into top_sites table and cross-posts URLs.
- Outputs: manifest.json plus copied TopSites.plist; top_sites and urls entries.
- Notes: Forensically significant — includes both frequently accessed sites and user-removed (banned) sites indicating user intent; distinguishes Apple default shortcuts from user customization.

## Patterns
- File/path patterns: Users/*/Library/Safari and Library/Safari (profile roots); Users/*/Library/Cookies and Library/Cookies (Cookies.binarycookies); Users/*/Library/Caches/com.apple.Safari and Library/Caches/com.apple.Safari (cache); Users/*/Library/WebKit (modern WebsiteData storage); Users/*/Library/Safari/Databases/___IndexedDB (IndexedDB); Users/*/Library/Caches/Metadata/Safari (persistent metadata that survives history clearing).
- Root types: profile, cookies, cache, metadata, websitedata — each artifact maps to a specific root type.
- Safari Technology Preview paths are also supported as alternative roots.
- Notes: Multi-partition discovery via file_list table (SleuthKit) with fallback to single-partition filesystem iteration; 27 artifact types defined across all root types.
