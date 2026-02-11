# Chromium Family

Source: `src/extractors/browser/chromium/`

## Overview
- Scope: Chromium family extractors for Chrome, Chromium, Edge, Brave, and Opera (stable plus beta/dev/canary/nightly/GX where listed in chromium/_patterns).
- Extraction: Uses chromium/_patterns path globs to locate profile artifacts; several extractors copy companion WAL/journal/shm files and write per-run manifests.
- Ingestion: Parses SQLite/JSON/SNSS/LevelDB artifacts, converts WebKit/Chrome timestamps to ISO 8601, and inserts into evidence DB via core.database helpers (plus cross-posts to urls/images where implemented).

## Extractors
### ChromiumHistoryExtractor
- Purpose: Extract per-visit browsing history from Chromium History databases.
- Extraction (source): History SQLite files matched via chromium/_patterns; copies WAL/journal/shm companions; supports multi-partition discovery via file_list when enabled.
- Ingestion (transform + store): Parses visits joined with urls via parse_history_visits, converts WebKit timestamps, inserts via insert_browser_history_rows and insert_browser_inventory, and cross-posts each visit to urls via insert_urls.
- Outputs: Extracted History DB copies, manifest.json, and extracted_files audit records.
- Special behavior: Stores transition/from_visit/visit_duration/hidden and Chromium visit/url IDs; per-visit URL events (no aggregation).
- Notes: Uses run_id-based provenance in discovered_by; browser selection via BrowserConfigWidget.

### ChromiumCookiesExtractor
- Purpose: Extract cookie records from Chromium Cookies databases.
- Extraction (source): Cookies and Network/Cookies SQLite files via chromium/_patterns; multi-partition file_list discovery when evidence_db_path is provided; copies WAL/journal/shm.
- Ingestion (transform + store): parse_cookies maps SameSite values, detects encrypted cookies, converts WebKit timestamps, inserts via insert_cookie_row (domain=host_key) with encrypted_value preserved.
- Outputs: Copied cookie DBs, manifest.json, and extracted_files audit records.
- Special behavior: Encrypted detection when encrypted_value present and value empty; counts encrypted cookies.
- Notes: Pure Python; no external tools required in metadata.

### ChromiumBookmarksExtractor
- Purpose: Extract bookmark URLs and folders from Chromium Bookmarks JSON.
- Extraction (source): Bookmarks files via chromium/_patterns; copied as JSON with hashes.
- Ingestion (transform + store): parse_bookmarks_json reconstructs folder paths and WebKit timestamps; inserts via insert_bookmark_row and cross-posts URL bookmarks to urls (skips javascript/data schemes).
- Outputs: Copied Bookmarks JSON files, manifest.json, and extracted_files audit records.
- Special behavior: Recursively parses folder hierarchy and records folder_path.
- Notes: Uses run_id provenance; no multi-partition file_list in this extractor.

### ChromiumDownloadsExtractor
- Purpose: Extract download history from Chromium History databases.
- Extraction (source): History SQLite files via chromium/_patterns "downloads" (maps to History); copies WAL/journal/shm.
- Ingestion (transform + store): parse_downloads maps state/danger_type and URL chains, converts WebKit timestamps, inserts via insert_browser_download_row, cross-posts URL chain entries to urls.
- Outputs: Copied History DBs, manifest.json, and extracted_files audit records.
- Special behavior: Uses last URL in chain as primary; deduplicates URL records per file.
- Notes: Downloads are stored in History DB (not a separate file).

### ChromiumCacheExtractor (CacheSimpleExtractor alias)
- Purpose: Extract HTTP cache entries (simple + blockfile) and carve images.
- Extraction (source): Cache/Cache_Data and Cache directories via chromium/_patterns; optional Service Worker CacheStorage via extractors.browser_patterns.get_browser_paths('cache_storage'); concurrent extraction with resume and hash mode options.
- Ingestion (transform + store): Parses simple cache headers/HTTP headers and blockfile entries, decompresses bodies (gzip/br/zstd/deflate), inserts URLs via insert_urls, images via insert_image_with_discovery, and registers inventory via insert_browser_inventory/update_inventory_ingestion_status.
- Outputs: Run-id subdirectory with manifest.json (and manifest.partial.json during resume), extracted cache files, carved_images output, and extracted_files audit records.
- Special behavior: Auto-detects cache format; uses index files to populate last_used_time for timeline correlation.
- Notes: Optional brotli/zstandard dependencies; hash mode can be deferred to ingestion.

### ChromiumMediaHistoryExtractor (MediaHistoryExtractor alias)
- Purpose: Extract media playback history from Chromium Media History databases.
- Extraction (source): Media History SQLite files via chromium/_patterns; copied with hashes.
- Ingestion (transform + store): Parses playback and playbackSession tables (with origin join when present), converts WebKit timestamps, inserts via insert_media_playback and insert_media_sessions, cross-posts media URLs to urls.
- Outputs: Copied Media History DBs, manifest.json, and extracted_files audit records.
- Special behavior: Clears prior run data via delete_media_by_run.
- Notes: Chromium-only (Firefox noted as no dedicated DB).

### ChromiumAutofillExtractor
- Purpose: Extract autofill entries, profiles, credentials, credit cards, and search engines.
- Extraction (source): Web Data and Login Data SQLite files via get_artifact_patterns("autofill"); copied with hashes.
- Ingestion (transform + store): Parses multiple tables (autofill, profiles, logins, keywords, credit_cards, Edge-specific tables, token tables), converts WebKit/Unix timestamps, inserts via insert_autofill_entries, insert_autofill_profiles, insert_credentials, insert_credit_cards, insert_search_engines, insert_autofill_profile_tokens, insert_autofill_block_list_entries.
- Outputs: Copied Web Data/Login Data DBs, manifest.json, and extracted_files audit records.
- Special behavior: Stores encrypted password_value/card data; loads insecure/breached/password_notes metadata when present; clears previous run data.
- Notes: Uses get_artifact_patterns from chromium/_patterns; supported browsers are CHROMIUM_BROWSERS keys.

### ChromiumPermissionsExtractor
- Purpose: Extract site permissions from Chromium Preferences files.
- Extraction (source): Preferences JSON via get_artifact_patterns("permissions") (filters filename "Preferences"); copied with hashes.
- Ingestion (transform + store): Parses profile.content_settings.exceptions, normalizes permission types/values, converts WebKit timestamps for expiration/last_modified, inserts via insert_permissions, and cross-posts origin URLs to urls.
- Outputs: Copied Preferences files, manifest.json, and extracted_files audit records.
- Special behavior: Skips non-permission entries by coercing setting values; records raw_type/raw_value and expires_type.
- Notes: Clears previous run data via delete_permissions_by_run.

### ChromiumFaviconsExtractor
- Purpose: Extract favicon icons and top sites data.
- Extraction (source): Favicons and Top Sites SQLite DBs via get_artifact_patterns("favicons"/"top_sites"); copies DBs and companion WAL/journal/shm.
- Ingestion (transform + store): Parses favicon_bitmaps/favicons/icon_mapping and top_sites/thumbnails, hashes icons, inserts via insert_favicon/insert_favicon_mappings/insert_top_sites, cross-posts favicon images to images table and URLs to urls.
- Outputs: Copied DBs, manifest.json, icon files under output_dir/icons/, and extracted_files audit records.
- Special behavior: Skips icons > 1MB; deduplicates via SHA256; computes pHash for images table.
- Notes: Supports legacy thumbnails table; uses WebKit timestamp conversion for favicon times.

### ChromiumExtensionsExtractor
- Purpose: Extract extension inventory and related script files.
- Extraction (source): Preferences (extensions.settings) and Extensions/{id}/{version}/manifest.json; copies manifest and referenced JS (background/content/service worker), hashes files.
- Ingestion (transform + store): Computes permission risk via calculate_risk_level, matches known extensions, and inserts via insert_extensions with runtime state from Preferences.
- Outputs: Per-extension directories with manifest/scripts, copied Preferences JSON, manifest.json, and extracted_files audit records.
- Special behavior: Merges Preferences state (enabled, install_time, disable_reasons, install_location); host_permissions inferred for MV2.
- Notes: Uses known_extensions reference list and risk classification helpers.

### ChromiumSyncDataExtractor
- Purpose: Extract sync account and device metadata from Preferences.
- Extraction (source): Preferences JSON via get_artifact_patterns("permissions"); copied with hashes and preview counts for accounts/devices.
- Ingestion (transform + store): Parses account_info/google.services/sync and sync.devices, converts Chrome timestamps, inserts into sync_data and synced_devices (with optional raw_data if include_raw).
- Outputs: Copied Preferences files, manifest.json, and extracted_files audit records.
- Special behavior: Updates synced_types and last_sync_time across accounts; clears prior run data from sync tables.
- Notes: Uses _chrome_timestamp_to_iso for Windows-epoch timestamps.

### ChromiumTransportSecurityExtractor
- Purpose: Extract HSTS entries from TransportSecurity files.
- Extraction (source): TransportSecurity JSON via get_artifact_patterns("transport_security"); copied with hashes.
- Ingestion (transform + store): Parses sts list with hashed_host, timestamps, and flags; inserts via insert_hsts_entries; cross-references hashed domains against urls/bookmarks to populate decoded_host.
- Outputs: Copied TransportSecurity files, manifest.json, and extracted_files audit records.
- Special behavior: hashed_host is SHA256+Base64; decoded_host populated via cross-reference with known domains.
- Notes: Clears previous run data from hsts_entries.

### ChromiumSessionsExtractor
- Purpose: Extract session/tab state from SNSS session files.
- Extraction (source): Session files via get_artifact_patterns("sessions") including legacy Current/Last files and Sessions/Session_* and Tabs_*; copied with hashes and timestamp suffixes preserved.
- Ingestion (transform + store): parse_snss_data builds windows/tabs/history, maps transition types, inserts via insert_session_windows/insert_session_tabs/insert_session_tab_histories, and cross-posts URL events to urls.
- Outputs: Copied SNSS files, manifest.json, and extracted_files audit records.
- Special behavior: Creates synthetic window if none parsed; preserves referrer, original_request_url, post data, and HTTP status in history records.
- Notes: Cross-posting skips about:/chrome:/chrome-extension:/javascript:/data schemes.

### ChromiumBrowserStorageExtractor
- Purpose: Extract Local Storage, Session Storage, and IndexedDB from Chromium profiles.
- Extraction (source): Storage directories via get_artifact_patterns("local_storage"/"session_storage"/"indexeddb"); copies directories with file counts/sizes to output_dir.
- Ingestion (transform + store): Requires ccl_chromium_reader; parses LevelDB-based storage, inserts via insert_local_storages/insert_session_storages/insert_indexeddb_database/insert_indexeddb_entries, extracts IndexedDB blob images via insert_image_with_discovery, and inserts storage origins to urls.
- Outputs: Copied storage directories, manifest.json, optional indexeddb_images directory, and extracted_files audit records.
- Special behavior: Configurable excerpt_size/include_deleted/extract_images; parses and normalizes origin URLs (including IndexedDB origin format).
- Notes: Ingestion aborts with error if LevelDB dependency is missing.

## Patterns
- File/path patterns: CHROMIUM_BROWSERS lists Windows/macOS/Linux profile roots for Chrome/Chromium/Edge/Brave/Opera (including beta/dev/canary/nightly/GX), with PROFILE_PATTERNS of Default/Profile */Guest/System Profile; CHROMIUM_ARTIFACTS includes History, Cookies (+ Network/Cookies), Bookmarks, Web Data/Login Data, Sessions (legacy + Sessions/Session_* and Tabs_*), Media History, Favicons/Top Sites, Sync Data, TransportSecurity, Cache/Cache_Data and Cache, and storage directories.
- Notes: Opera uses flat profile structure (no Default/Profile * prefix). CacheStorage patterns are not in CHROMIUM_ARTIFACTS and are discovered via extractors.browser_patterns.get_browser_paths('cache_storage') when enabled.
