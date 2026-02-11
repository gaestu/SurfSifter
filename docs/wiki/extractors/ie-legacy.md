# IE Legacy Family

Source: `src/extractors/browser/ie_legacy/`

## Overview
- Scope: Internet Explorer 10/11 and Legacy Edge (EdgeHTML/UWP) artifacts, including WebCache ESE, container.dat, favorites, cookies, typed URLs, DOMStore, recovery, and reading list.
- Extraction: Uses ie_legacy/_patterns globs and targeted file_list discovery to copy WebCacheV01.dat plus ESE logs and file-based artifacts into per-extractor output dirs with manifests.
- Ingestion: Parses ESE databases, registry hives, and file-based formats (INI, cookie text, XML/JSON, binary recovery) and inserts into browser_history, downloads, cookies, bookmarks, local_storage, and urls tables.

## Extractors
### IEWebCacheExtractor
- Purpose: Copy IE/Edge WebCacheV01.dat ESE databases and required log/journal files.
- Extraction (source): WebCacheV01.dat matches from ie_legacy/_patterns ("webcache") plus same-directory logs (V01.log/chk/tmp, V01*.log, V01res*.jrs, WebCacheV01.jfm).
- Ingestion (transform + store): None; artifact-specific WebCache ingestors handle parsing.
- Outputs: Copied WebCache DBs and log files, manifest.json, and extracted_files audit records.
- Special behavior: Multi-partition discovery via file_list (filename WebCacheV01.dat, path %WebCache%); per-user filenames and hashes.
- Notes: ESE library checked for status/version only; can_extract=True, can_ingest=False.

### IEHistoryExtractor
- Purpose: Ingest browsing history from WebCache History containers.
- Extraction (source): None; reads WebCache manifest/output from IEWebCacheExtractor.
- Ingestion (transform + store): WebCacheReader parses all "History" containers; URL cleanup for "Visited: user@..." and "user@scheme://"; FILETIME to ISO; inserts browser_history and aggregated urls.
- Outputs: Database rows only (browser_history, urls); no files emitted.
- Special behavior: Accepts any URL scheme; sanitizes title fields that look binary; handles multiple History containers per DB.
- Notes: Requires ESE parsing library; registers browser_inventory and updates ingestion status.

### IECookiesExtractor
- Purpose: Ingest cookie metadata from WebCache cookie containers (URL references only).
- Extraction (source): None; reads WebCache manifest/output from IEWebCacheExtractor.
- Ingestion (transform + store): Parses containers with "cookie" in name; FILETIME to ISO; flags to secure/httponly; inserts cookies with empty value and note "webcache_metadata:url_reference_only".
- Outputs: Cookies table rows only; no files emitted.
- Special behavior: Cookie name derived from URL (Cookie:user@domain/path) for uniqueness.
- Notes: Requires ESE parsing library; actual cookie values come from IEINetCookiesExtractor.

### IEDownloadsExtractor
- Purpose: Ingest download history from WebCache iedownload container.
- Extraction (source): None; reads WebCache manifest/output from IEWebCacheExtractor.
- Ingestion (transform + store): Parses iedownload container; http/https only; filename from field or URL; Content-Length/MIME from ResponseHeaders; inserts browser_downloads and urls.
- Outputs: Database rows only (browser_downloads, urls); no files emitted.
- Special behavior: start_time uses ModifiedTime, end_time uses AccessedTime; state marked complete when AccessedTime present.
- Notes: Requires ESE parsing library; registers browser_inventory and updates ingestion status.

### IECacheMetadataExtractor
- Purpose: Ingest cache URL metadata from WebCache Content containers.
- Extraction (source): None; reads WebCache manifest/output from IEWebCacheExtractor.
- Ingestion (transform + store): Parses Content*/cache containers; extracts URL, timestamps, FileSize, and ResponseHeaders content-type; inserts urls with notes (protected_mode/size/dir/type).
- Outputs: urls rows only (cache metadata); no cache files extracted.
- Special behavior: Marks "protected_mode" when container name includes "low".
- Notes: Requires ESE parsing library; does not extract cached file content.

### IEDOMStorageExtractor
- Purpose: Extract and ingest DOM Storage from IE WebCache DOMStore and Edge Legacy DOMStore files.
- Extraction (source): Edge Legacy DOMStore files via ie_legacy/_patterns ("dom_storage"); copied per partition/user into ie_dom_storage output.
- Ingestion (transform + store): Parses extracted Edge DOMStore (XML or key=value) into local_storage; also parses WebCache DOMStore containers from ie_webcache manifest when ESE available.
- Outputs: Copied DOMStore files, manifest.json, extracted_files audit; local_storage rows.
- Special behavior: Skips WebCache DOMStore when ESE missing; WebCache values decoded from Data bytes or hex fallback.
- Notes: Depends on IEWebCacheExtractor output for IE DOMStore; ESE optional.

### IEINetCookiesExtractor
- Purpose: Extract and ingest file-based cookies (.cookie/.txt) outside WebCache.
- Extraction (source): INetCookies/Cookies patterns from ie_legacy/_patterns ("inetcookies"), including Low integrity paths; copied per partition/user with hashes.
- Ingestion (transform + store): Parses .cookie (Windows format) and .txt (Netscape) cookies; FILETIME/Unix timestamps to ISO; inserts cookies with values.
- Outputs: Copied cookie files, manifest.json, extracted_files audit; cookies rows.
- Special behavior: Flags low-integrity via path and stores note; decoding attempts utf-8/utf-16/latin-1.
- Notes: Complements WebCache cookie metadata; no external tools required.

### IEFavoritesExtractor
- Purpose: Extract bookmarks from IE/Edge Favorites .url files.
- Extraction (source): Favorites .url patterns; non-recursive scan plus targeted recursive walk of Favorites dirs; copies files and records crtime/mtime.
- Ingestion (transform + store): Parses INI-style .url with multi-encoding fallback; inserts bookmarks and cross-posts to urls (skips javascript/data).
- Outputs: Copied .url files, manifest.json, extracted_files audit; bookmarks and urls rows.
- Special behavior: folder_path derived from Favorites subfolders; date_added from original crtime/mtime.
- Notes: Pure Python; no external tools required.

### IETypedURLsExtractor
- Purpose: Extract manually typed URLs from NTUSER.DAT registry hives.
- Extraction (source): NTUSER.DAT files in user profiles; copied per partition/user to ie_typed_urls output.
- Ingestion (transform + store): regipy reads TypedURLs and TypedURLsTime keys; correlates urlN with per-URL FILETIME when present; inserts urls with notes.
- Outputs: Copied NTUSER.DAT, manifest.json, extracted_files audit; urls rows.
- Special behavior: Falls back to TypedURLs key last-write time when per-URL time missing.
- Notes: Requires regipy; registers browser_inventory and updates ingestion status.

### IETabRecoveryExtractor
- Purpose: Extract session recovery .dat files and recover open-tab URLs.
- Extraction (source): IE Recovery/Active, Last Active, Immersive, and Edge Legacy Recovery paths; copies .dat files per partition/user.
- Ingestion (transform + store): Extracts URLs from UTF-16 and ASCII patterns; inserts browser_history rows (tabs) and urls; optional FILETIME heuristic for session time.
- Outputs: Copied recovery .dat, manifest.json, extracted_files audit; history and urls rows.
- Special behavior: recovery_type derived from path (active/last_active/immersive/inprivate); URL validation checks scheme and TLD.
- Notes: Timestamps are heuristic and may be missing.

### LegacyEdgeContainerExtractor
- Purpose: Extract and ingest Legacy Edge UWP container.dat ESE databases.
- Extraction (source): container.dat in Microsoft.MicrosoftEdge_* UWP paths (including Windows.old); skips zero-size files and copies readable files with hashes.
- Ingestion (transform + store): ESEReader parses tables with URL/cookie/storage columns; inserts browser_history, urls, cookies (metadata only), and local_storage; cache containers are skipped.
- Outputs: Copied container.dat files, manifest.json, extracted_files audit; database rows as above.
- Special behavior: container_type inferred from path (history/cookies/cache/domstore); unreadable sparse files marked failed with notes.
- Notes: Requires ESE parsing library; cookies stored with note "container_dat_metadata:url_reference_only".

### EdgeReadingListExtractor
- Purpose: Extract Legacy Edge Reading List entries as bookmarks.
- Extraction (source): ReadingList and related feeds patterns under Edge UWP paths; copied per partition/user.
- Ingestion (transform + store): Attempts JSON, XML, then plain-text URL extraction; inserts bookmarks (folder_path "Reading List") and cross-posts urls.
- Outputs: Copied Reading List files, manifest.json, extracted_files audit; bookmarks and urls rows.
- Special behavior: Timestamp parsing accepts ISO, Unix seconds, milliseconds, and WebKit-style microseconds; skips javascript/data URLs for urls table.
- Notes: Legacy Edge only; no external tools required.

## Patterns
- File/path patterns: WebCacheV01.dat (+ V01*.log/jrs/jfm), Legacy Edge container.dat, Favorites *.url, INetCookies *.cookie/*.txt, NTUSER.DAT, DOMStore, ReadingList, and Recovery/*.dat in user, system, and Windows.old paths.
- Notes: ie_legacy/_patterns defines per-browser globs and helpers (get_patterns/get_all_patterns) plus detect_browser_from_path for ie/ie_system/ie_old_windows/edge_legacy classification.
