# Firefox Family

Source: `src/extractors/browser/firefox/`

## Overview
- Scope: Firefox family extractors for Firefox, Firefox ESR (label-only), and Tor Browser using firefox/_patterns.
- Extraction: Uses firefox/_patterns glob paths for profile/cache roots; most extractors copy SQLite/JSON/LZ4 artifacts (plus WAL/journal/shm when present) and write per-run manifests.
- Ingestion: Parses Firefox artifacts (places.sqlite, cookies.sqlite, sessionstore.jsonlz4, etc.), converts PRTime/epoch timestamps, and inserts into evidence DB tables via core.database helpers (plus urls/images integrations where implemented).

## Extractors
### FirefoxHistoryExtractor
- Purpose: Extract per-visit browsing history from Firefox-based profiles.
- Extraction (source): places.sqlite via firefox/_patterns ("history") with WAL/journal/shm companions; supports file_list discovery and multi-partition.
- Ingestion (transform + store): parse_history_visits joins moz_historyvisits+moz_places and moz_inputhistory, converts PRTime to ISO, inserts via insert_browser_history_rows and insert_browser_inventory, and aggregates unique URLs into urls via insert_urls.
- Outputs: Copied DBs + manifest.json + extracted_files audit records; browser_inventory entries; urls table entries.
- Special behavior: Builds per-visit notes (visit_type, frecency, hidden, typed_input, from_visit) and aggregates first/last seen timestamps by URL.
- Notes: Browser selection via BrowserConfigWidget; supports Firefox/ESR/Tor.

### FirefoxCookiesExtractor
- Purpose: Extract cookie records from Firefox-based profiles.
- Extraction (source): cookies.sqlite via firefox/_patterns ("cookies") with WAL/journal/shm companions; supports file_list discovery and multi-partition.
- Ingestion (transform + store): parse_cookies handles moz_cookies or legacy cookies table, maps SameSite values, parses originAttributes (container/private/FPI), converts PRTime/Unix times, inserts via insert_cookie_row and insert_browser_inventory.
- Outputs: Copied DBs + manifest.json + extracted_files audit records; browser_inventory entries.
- Special behavior: Marks cookies as unencrypted (Firefox cookies are plaintext); captures originAttributes fields (user_context_id, private_browsing_id, first_party_domain, partition_key).
- Notes: Browser selection via BrowserConfigWidget; supports Firefox/ESR/Tor.

### FirefoxBookmarksExtractor
- Purpose: Extract bookmarks and bookmark backups.
- Extraction (source): places.sqlite and bookmarkbackups/*.jsonlz4 via firefox/_patterns; copies WAL/journal/shm for SQLite files and records backup metadata.
- Ingestion (transform + store): parse_bookmarks builds folder paths from moz_bookmarks/moz_places; parse_bookmark_backup decompresses mozLz4 JSON backups; inserts via insert_bookmark_row and cross-posts URLs to urls.
- Outputs: Copied DBs/backup files + manifest.json + extracted_files audit records; urls table entries.
- Special behavior: Tracks source_type (live vs backup) and backup_date in provenance; skips javascript/data URLs for urls table.
- Notes: Requires lz4 module to parse jsonlz4 backups; supports Firefox/ESR/Tor.

### FirefoxDownloadsExtractor
- Purpose: Extract download history from Firefox-based profiles.
- Extraction (source): places.sqlite via firefox/_patterns ("downloads") with WAL/journal/shm companions; supports file_list discovery and multi-partition.
- Ingestion (transform + store): parse_downloads reads moz_annos (modern) or moz_downloads (legacy), maps state/danger_type and timestamps, inserts via insert_browser_download_row and insert_browser_inventory, and cross-posts download URLs to urls.
- Outputs: Copied DBs + manifest.json + extracted_files audit records; browser_inventory entries; urls table entries.
- Special behavior: Adds notes for deleted downloads and includes referrer/mime_type when available.
- Notes: Downloads are stored in places.sqlite (same file as history).

### FirefoxCacheExtractor (CacheFirefoxExtractor alias)
- Purpose: Extract Firefox cache2 HTTP cache entries and carve images.
- Extraction (source): cache2/entries/* via firefox/_patterns ("cache"); uses file_list discovery and icat/concurrent/sequential strategies; writes manifest.part-*.jsonl and manifest.json.
- Ingestion (transform + store): CacheIngestionHandler parses cache2 entries (URL, headers, metadata), inserts urls via insert_urls, registers browser_inventory, and carves images from response bodies (gzip/brotli/zstd/deflate) via insert_image_with_discovery; can compute deferred hashes.
- Outputs: Run-id subdirectory with extracted cache files, manifest.json (and part files), carved_images/ output, extracted_files audit records, urls/images/inventory entries.
- Special behavior: Hash mode selectable (extraction/ingestion/disabled) and skips supporting files (index/doomed/trash) during ingestion.
- Notes: Uses MultiPartitionWidget; optional brotli/zstandard dependencies; alias FirefoxCacheExtractor for backward compatibility.

### FirefoxAutofillExtractor
- Purpose: Extract form history and saved credentials from Firefox-based profiles.
- Extraction (source): formhistory.sqlite, logins.json, key4.db/key3.db, and signons.sqlite via firefox/_patterns ("autofill"); files copied to output_dir.
- Ingestion (transform + store): Parses moz_formhistory and moz_deleted_formhistory, logins.json, and legacy moz_logins in signons.sqlite; converts PRTime/epoch timestamps; inserts via insert_autofill_entries, insert_credentials, and insert_deleted_form_history_entries (clears prior run data).
- Outputs: Copied files + manifest.json + extracted_files audit records; autofill/credentials/deleted_form_history rows.
- Special behavior: Stores encrypted credentials as-is (no decryption); includes NSS key DBs for optional offline decryption.
- Notes: Firefox has no autofill profiles/credit cards; supports Firefox/ESR/Tor.

### FirefoxPermissionsExtractor
- Purpose: Extract site permissions and site-specific preferences.
- Extraction (source): permissions.sqlite and content-prefs.sqlite via firefox/_patterns ("permissions"); files copied to output_dir.
- Ingestion (transform + store): Parses moz_perms with schema detection (origin vs host), normalizes permission types/values, converts PRTime timestamps; parses content-prefs groups/settings/prefs; inserts via insert_permissions and insert_browser_inventory (clears prior run data).
- Outputs: Copied DBs + manifest.json + extracted_files audit records; site_permissions rows.
- Special behavior: Maps permission types via FIREFOX_PERMISSION_TYPE_MAP and supports legacy permission schema.
- Notes: Browser selection via BrowserConfigWidget; supports Firefox/ESR/Tor.

### FirefoxFaviconsExtractor
- Purpose: Extract favicon icons and page-to-icon mappings.
- Extraction (source): favicons.sqlite via firefox/_patterns ("favicons") with journal/WAL/SHM companions; copied to output_dir.
- Ingestion (transform + store): Parses moz_icons/moz_pages_w_icons/moz_icons_to_pages and legacy moz_favicons; hashes and deduplicates icons; inserts via insert_favicon and insert_favicon_mappings; cross-posts icon/page URLs to urls; inserts large icons (>=64px) to images with pHash.
- Outputs: Copied DBs + manifest.json + extracted_files audit records; favicon image files under output_dir/favicons/ and thumbnails/; urls/images entries.
- Special behavior: Skips icons >1MB; ignores fixed_icon_url_hash; legacy schema support for Firefox <55.
- Notes: Image insertion uses compute_phash; favicon files saved by SHA256-based pathing.

### FirefoxExtensionsExtractor
- Purpose: Extract extension inventory with risk classification.
- Extraction (source): extensions.json and addons.json via firefox/_patterns ("extensions"); copied to output_dir for ELT workflow.
- Ingestion (transform + store): Parses addons.json (supplementary AMO metadata) and extensions.json (primary list), merges by extension ID, computes permission risk via calculate_risk_level and matches known extensions, inserts via insert_extensions (clears prior run data).
- Outputs: Copied JSON files + manifest.json + extracted_files audit records; browser_extensions rows.
- Special behavior: Skips system/builtin addons, tracks signing state, and adds risk factors for high/critical permissions.
- Notes: Uses known_extensions reference list; supports Firefox/ESR/Tor.

### FirefoxSyncDataExtractor
- Purpose: Extract Firefox Sync account and device metadata.
- Extraction (source): signedInUser.json via firefox/_patterns ("sync_data"); copied to output_dir with preview counts.
- Ingestion (transform + store): Parses accountData (email/uid/displayName/verified/profilePath/device), derives synced_types, inserts into sync_data and synced_devices tables via SQL, and registers browser_inventory (clears prior run data).
- Outputs: Copied JSON + manifest.json + extracted_files audit records; sync_data/synced_devices rows; browser_inventory entries.
- Special behavior: include_raw config controls raw_data JSON storage; handles missing accountData (logged out) gracefully.
- Notes: last_sync_time not available in signedInUser.json.

### FirefoxTransportSecurityExtractor
- Purpose: Extract HSTS entries with cleartext domains.
- Extraction (source): SiteSecurityServiceState.txt via firefox/_patterns ("transport_security"); copied to output_dir.
- Ingestion (transform + store): Parses tab-separated entries, converts last_access (days since epoch) and expiry_ms, inserts URLs via insert_urls and HSTS entries via insert_hsts_entries (cleartext decoded_host), and registers browser_inventory (clears prior run data).
- Outputs: Copied text files + manifest.json + extracted_files audit records; urls and hsts_entries rows; browser_inventory entries.
- Special behavior: Uses last_access as last_seen for URL records and stores cleartext domains (hashed_host placeholder).
- Notes: Supports Firefox/ESR/Tor; high forensic value due to cleartext hosts.

### FirefoxSessionsExtractor
- Purpose: Extract session windows/tabs, navigation history, and closed tabs.
- Extraction (source): sessionstore.jsonlz4 and sessionstore-backups/*.jsonlz4/*.baklz4 plus legacy sessionstore.js via firefox/_patterns ("sessions"); copied to output_dir.
- Ingestion (transform + store): Decompresses mozLz4 or parses legacy JSON, builds window/tab records and per-tab navigation history, captures closed tabs, inserts via insert_session_windows/insert_session_tabs/insert_session_tab_history/insert_closed_tabs, and cross-posts URLs to urls.
- Outputs: Copied session files + manifest.json + extracted_files audit records; session tables and urls entries.
- Special behavior: Requires lz4 for jsonlz4; handles legacy sessionstore.js prefix stripping; resolves tab_id for history via window_id/tab_index.
- Notes: Supports Firefox/ESR/Tor.

### FirefoxBrowserStorageExtractor (FirefoxStorageExtractor alias)
- Purpose: Extract Local Storage and IndexedDB content with optional value analysis.
- Extraction (source): webappsstore.sqlite, storage/default/*/ls/data.sqlite, and storage/default/*/idb/*.sqlite via firefox/_patterns ("local_storage"/"indexeddb"); copied with per-origin metadata into manifest.
- Ingestion (transform + store): Parses legacy/modern LocalStorage and IndexedDB metadata/entries, inserts via insert_local_storages and insert_indexeddb_database/insert_indexeddb_entries, and analyzes values with StorageValueAnalyzer to insert urls/emails/storage_tokens/storage_identifiers (clears prior run data).
- Outputs: Copied storage files + manifest.json + extracted_files audit records; local_storage/indexeddb rows; urls/emails/tokens/identifiers rows when analysis enabled.
- Special behavior: Optional Snappy decompression for modern LocalStorage; analysis is configurable (urls/emails/tokens/identifiers) with excerpt sizing.
- Notes: Alias FirefoxBrowserStorageExtractor for backward compatibility.

### FirefoxTorStateExtractor
- Purpose: Collect Tor Browser state/config files for forensic review.
- Extraction (source): TorBrowser/Data/Tor artifacts (torrc, state, cached-*, control_auth_cookie, geoip, pt_state/*, keys/*) via TOR_DATA_ROOTS/TOR_ARTIFACT_PATHS; copied to output_dir.
- Ingestion (transform + store): Registers each file in browser_inventory; parses torrc to count settings and stores parsed summary in inventory notes; other files are inventory-only.
- Outputs: Copied files + manifest.json + extracted_files audit records; browser_inventory entries with extraction_notes/notes.
- Special behavior: File_type classification (torrc/state/cached/pt_state/keys/etc.) drives ingestion notes.
- Notes: Tor-only (SUPPORTED_BROWSERS = ["tor"]).

## Patterns
- File/path patterns: FIREFOX_BROWSERS defines profile_roots/cache_roots for firefox/firefox_esr/tor, and FIREFOX_ARTIFACTS lists relative paths for places.sqlite, cookies.sqlite, formhistory.sqlite/logins.json/key*.db, sessionstore*.jsonlz4, permissions.sqlite/content-prefs.sqlite, favicons.sqlite, extensions.json/addons.json, signedInUser.json, SiteSecurityServiceState.txt, cache2/entries/*, and storage/default paths.
