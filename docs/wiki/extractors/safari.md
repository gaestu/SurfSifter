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

## Patterns
- File/path patterns: Users/*/Library/Safari and Library/Safari (profile roots); Users/*/Library/Cookies and Library/Cookies (Cookies.binarycookies); Users/*/Library/Caches/com.apple.Safari and Library/Caches/com.apple.Safari (cache).
- Notes: Unknown/TBD.
