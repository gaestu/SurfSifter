-- Consolidated evidence database schema (v0.3.0-beta — February 2026)
-- This is the COMPLETE schema for per-evidence artifact storage.
-- Case metadata lives in the case database ({case_number}_surfsifter.sqlite).
--
-- Schema version: 1 (consolidated baseline)
-- All migrations consolidated into this single file for public release.

-- ============================================================================
-- URLs: Discovered URLs (bulk_extractor, browser history, etc.)
-- ============================================================================

CREATE TABLE IF NOT EXISTS urls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    domain TEXT,
    scheme TEXT,
    discovered_by TEXT NOT NULL,
    first_seen_utc TEXT,
    last_seen_utc TEXT,
    source_path TEXT,
    tags TEXT,
    notes TEXT,
    context TEXT,
    run_id TEXT,
    cache_key TEXT,
    cache_filename TEXT,
    response_code INTEGER,
    content_type TEXT,
    file_extension TEXT,
    file_type TEXT,
    occurrence_count INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_urls_cache_key ON urls(cache_key);
CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain);
CREATE INDEX IF NOT EXISTS idx_urls_evidence ON urls(evidence_id);
CREATE INDEX IF NOT EXISTS idx_urls_evidence_domain ON urls(evidence_id, domain);
CREATE INDEX IF NOT EXISTS idx_urls_evidence_first_seen ON urls(evidence_id, first_seen_utc DESC);
CREATE INDEX IF NOT EXISTS idx_urls_evidence_last_seen ON urls(evidence_id, last_seen_utc DESC);
CREATE INDEX IF NOT EXISTS idx_urls_evidence_occurrence ON urls(evidence_id, occurrence_count DESC);
CREATE INDEX IF NOT EXISTS idx_urls_evidence_source ON urls(evidence_id, discovered_by);
CREATE INDEX IF NOT EXISTS idx_urls_file_extension ON urls(evidence_id, file_extension);
CREATE INDEX IF NOT EXISTS idx_urls_file_type ON urls(evidence_id, file_type);
CREATE INDEX IF NOT EXISTS idx_urls_file_type_domain ON urls(evidence_id, file_type, domain);
CREATE INDEX IF NOT EXISTS idx_urls_run_id ON urls(run_id);
CREATE INDEX IF NOT EXISTS idx_urls_url ON urls(url);

-- ============================================================================
-- URL Analysis Tables
-- ============================================================================


-- URL reference list matches
CREATE TABLE IF NOT EXISTS url_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    url_id INTEGER NOT NULL,
    list_name TEXT NOT NULL,
    match_type TEXT NOT NULL,
    matched_pattern TEXT,
    created_at_utc TEXT,
    run_id TEXT,
    FOREIGN KEY (url_id) REFERENCES urls(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_url_matches_evidence_list ON url_matches(evidence_id, list_name);
CREATE INDEX IF NOT EXISTS idx_url_matches_evidence_url ON url_matches(evidence_id, url_id);
CREATE INDEX IF NOT EXISTS idx_url_matches_list ON url_matches(list_name);
CREATE INDEX IF NOT EXISTS idx_url_matches_run_id ON url_matches(run_id);
CREATE INDEX IF NOT EXISTS idx_url_matches_url_id ON url_matches(url_id);

-- ============================================================================
-- bulk_extractor Artifact Tables
-- ============================================================================


-- Bitcoin addresses
CREATE TABLE IF NOT EXISTS bitcoin_addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    address TEXT NOT NULL,
    discovered_by TEXT NOT NULL,
    first_seen_utc TEXT,
    last_seen_utc TEXT,
    source_path TEXT,
    tags TEXT,
    notes TEXT,
    context TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_bitcoin_address ON bitcoin_addresses(address);
CREATE INDEX IF NOT EXISTS idx_bitcoin_evidence ON bitcoin_addresses(evidence_id);
CREATE INDEX IF NOT EXISTS idx_bitcoin_run_id ON bitcoin_addresses(run_id);


-- Ethereum addresses
CREATE TABLE IF NOT EXISTS ethereum_addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    address TEXT NOT NULL,
    discovered_by TEXT NOT NULL,
    first_seen_utc TEXT,
    last_seen_utc TEXT,
    source_path TEXT,
    tags TEXT,
    notes TEXT,
    context TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_ethereum_address ON ethereum_addresses(address);
CREATE INDEX IF NOT EXISTS idx_ethereum_evidence ON ethereum_addresses(evidence_id);
CREATE INDEX IF NOT EXISTS idx_ethereum_run_id ON ethereum_addresses(run_id);


-- Email addresses
CREATE TABLE IF NOT EXISTS emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    email TEXT NOT NULL,
    domain TEXT,
    discovered_by TEXT NOT NULL,
    first_seen_utc TEXT,
    last_seen_utc TEXT,
    source_path TEXT,
    tags TEXT,
    notes TEXT,
    context TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_emails_domain ON emails(domain);
CREATE INDEX IF NOT EXISTS idx_emails_email ON emails(email);
CREATE INDEX IF NOT EXISTS idx_emails_evidence ON emails(evidence_id);
CREATE INDEX IF NOT EXISTS idx_emails_run_id ON emails(run_id);


-- Domain names
CREATE TABLE IF NOT EXISTS domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    domain TEXT NOT NULL,
    discovered_by TEXT NOT NULL,
    first_seen_utc TEXT,
    last_seen_utc TEXT,
    source_path TEXT,
    tags TEXT,
    notes TEXT,
    context TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_domains_domain ON domains(domain);
CREATE INDEX IF NOT EXISTS idx_domains_evidence ON domains(evidence_id);
CREATE INDEX IF NOT EXISTS idx_domains_run_id ON domains(run_id);


-- IP addresses
CREATE TABLE IF NOT EXISTS ip_addresses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    ip_address TEXT NOT NULL,
    ip_version TEXT,
    discovered_by TEXT NOT NULL,
    first_seen_utc TEXT,
    last_seen_utc TEXT,
    source_path TEXT,
    tags TEXT,
    notes TEXT,
    context TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_ips_address ON ip_addresses(ip_address);
CREATE INDEX IF NOT EXISTS idx_ips_evidence ON ip_addresses(evidence_id);
CREATE INDEX IF NOT EXISTS idx_ips_run_id ON ip_addresses(run_id);


-- Phone numbers
CREATE TABLE IF NOT EXISTS telephone_numbers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    phone_number TEXT NOT NULL,
    country_code TEXT,
    discovered_by TEXT NOT NULL,
    first_seen_utc TEXT,
    last_seen_utc TEXT,
    source_path TEXT,
    tags TEXT,
    notes TEXT,
    context TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_phones_evidence ON telephone_numbers(evidence_id);
CREATE INDEX IF NOT EXISTS idx_phones_number ON telephone_numbers(phone_number);
CREATE INDEX IF NOT EXISTS idx_phones_run_id ON telephone_numbers(run_id);

-- ============================================================================
-- Browser Artifacts
-- ============================================================================


-- Browser history
CREATE TABLE IF NOT EXISTS browser_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    title TEXT,
    ts_utc TEXT,
    browser TEXT,
    profile TEXT,
    source_path TEXT,
    visit_count INTEGER,
    typed_count INTEGER,
    last_visit_time_utc TEXT,
    discovered_by TEXT,
    tags TEXT,
    notes TEXT,
    run_id TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    transition_type INTEGER,
    from_visit INTEGER,
    visit_duration_ms INTEGER,
    hidden INTEGER DEFAULT 0,
    chromium_visit_id INTEGER,
    chromium_url_id INTEGER,
    transition_type_name TEXT
);

CREATE INDEX IF NOT EXISTS idx_browser_history_discovered_by ON browser_history(discovered_by);
CREATE INDEX IF NOT EXISTS idx_browser_history_evidence ON browser_history(evidence_id);
CREATE INDEX IF NOT EXISTS idx_browser_history_from_visit ON browser_history(from_visit);
CREATE INDEX IF NOT EXISTS idx_browser_history_run_id ON browser_history(run_id);
CREATE INDEX IF NOT EXISTS idx_browser_history_transition ON browser_history(transition_type);
CREATE INDEX IF NOT EXISTS idx_browser_history_ts ON browser_history(ts_utc);


-- Browser/Cache Inventory
CREATE TABLE IF NOT EXISTS browser_cache_inventory (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    artifact_type TEXT NOT NULL,
    profile TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT NOT NULL,
    forensic_path TEXT,
    run_id TEXT NOT NULL,
    extracted_path TEXT NOT NULL,
    extraction_status TEXT NOT NULL,
    extraction_timestamp_utc TEXT,
    extraction_tool TEXT,
    extraction_notes TEXT,
    ingestion_status TEXT,
    ingestion_timestamp_utc TEXT,
    urls_parsed INTEGER DEFAULT 0,
    records_parsed INTEGER DEFAULT 0,
    ingestion_notes TEXT,
    file_size_bytes INTEGER,
    file_md5 TEXT,
    file_sha256 TEXT,
    created_at_utc TEXT DEFAULT (datetime('now')),
    updated_at_utc TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_cache_inventory_browser ON browser_cache_inventory(browser);
CREATE INDEX IF NOT EXISTS idx_cache_inventory_evidence ON browser_cache_inventory(evidence_id);
CREATE INDEX IF NOT EXISTS idx_cache_inventory_run_id ON browser_cache_inventory(run_id);
CREATE INDEX IF NOT EXISTS idx_cache_inventory_status ON browser_cache_inventory(ingestion_status);
CREATE INDEX IF NOT EXISTS idx_cache_inventory_type ON browser_cache_inventory(artifact_type);


-- Cookies table
CREATE TABLE IF NOT EXISTS cookies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    name TEXT NOT NULL,
    value TEXT,
    domain TEXT NOT NULL,
    path TEXT,
    expires_utc TEXT,
    is_secure INTEGER,
    is_httponly INTEGER,
    samesite TEXT,
    samesite_raw INTEGER,
    creation_utc TEXT,
    last_access_utc TEXT,
    encrypted INTEGER DEFAULT 0,
    encrypted_value BLOB,
    origin_attributes TEXT,
    user_context_id INTEGER,
    private_browsing_id INTEGER,
    first_party_domain TEXT,
    partition_key TEXT,
    run_id TEXT,
    source_path TEXT,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_cookies_browser ON cookies(browser);
CREATE INDEX IF NOT EXISTS idx_cookies_domain ON cookies(domain);
CREATE INDEX IF NOT EXISTS idx_cookies_evidence ON cookies(evidence_id);
CREATE INDEX IF NOT EXISTS idx_cookies_evidence_browser ON cookies(evidence_id, browser);
CREATE INDEX IF NOT EXISTS idx_cookies_evidence_domain ON cookies(evidence_id, domain);
CREATE INDEX IF NOT EXISTS idx_cookies_name ON cookies(name);
CREATE INDEX IF NOT EXISTS idx_cookies_private_browsing ON cookies(evidence_id, private_browsing_id);
CREATE INDEX IF NOT EXISTS idx_cookies_run_id ON cookies(run_id);
CREATE INDEX IF NOT EXISTS idx_cookies_user_context ON cookies(evidence_id, user_context_id);


-- Bookmarks table
CREATE TABLE IF NOT EXISTS bookmarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    url TEXT NOT NULL,
    title TEXT,
    folder_path TEXT,
    bookmark_type TEXT,
    guid TEXT,
    date_added_utc TEXT,
    date_modified_utc TEXT,
    run_id TEXT,
    source_path TEXT,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_bookmarks_browser ON bookmarks(browser);
CREATE INDEX IF NOT EXISTS idx_bookmarks_evidence ON bookmarks(evidence_id);
CREATE INDEX IF NOT EXISTS idx_bookmarks_evidence_browser ON bookmarks(evidence_id, browser);
CREATE INDEX IF NOT EXISTS idx_bookmarks_evidence_folder ON bookmarks(evidence_id, folder_path);
CREATE INDEX IF NOT EXISTS idx_bookmarks_folder ON bookmarks(folder_path);
CREATE INDEX IF NOT EXISTS idx_bookmarks_run_id ON bookmarks(run_id);
CREATE INDEX IF NOT EXISTS idx_bookmarks_url ON bookmarks(url);


-- Browser downloads (distinct from investigator downloads)
CREATE TABLE IF NOT EXISTS browser_downloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    url TEXT NOT NULL,
    target_path TEXT,
    filename TEXT,
    start_time_utc TEXT,
    end_time_utc TEXT,
    total_bytes INTEGER,
    received_bytes INTEGER,
    mime_type TEXT,
    referrer TEXT,
    state TEXT,
    danger_type TEXT,
    opened INTEGER,
    run_id TEXT,
    source_path TEXT,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_browser_downloads_browser ON browser_downloads(browser);
CREATE INDEX IF NOT EXISTS idx_browser_downloads_evidence_danger ON browser_downloads(evidence_id, danger_type);
CREATE INDEX IF NOT EXISTS idx_browser_downloads_evidence_id ON browser_downloads(evidence_id);
CREATE INDEX IF NOT EXISTS idx_browser_downloads_evidence_opened ON browser_downloads(evidence_id, opened);
CREATE INDEX IF NOT EXISTS idx_browser_downloads_evidence_state ON browser_downloads(evidence_id, state);
CREATE INDEX IF NOT EXISTS idx_browser_downloads_filename ON browser_downloads(filename);
CREATE INDEX IF NOT EXISTS idx_browser_downloads_run_id ON browser_downloads(run_id);
CREATE INDEX IF NOT EXISTS idx_browser_downloads_start_time ON browser_downloads(start_time_utc);
CREATE INDEX IF NOT EXISTS idx_browser_downloads_url ON browser_downloads(url);

-- ============================================================================
-- Browser Search Terms
-- ============================================================================


-- Keyword search extraction from Chromium omnibox/URL bar
CREATE TABLE IF NOT EXISTS browser_search_terms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    term TEXT NOT NULL,
    normalized_term TEXT,
    url TEXT,
    browser TEXT,
    profile TEXT,
    search_engine TEXT,
    search_time_utc TEXT,
    source_path TEXT,
    discovered_by TEXT,
    run_id TEXT,
    partition_index INTEGER,
    logical_path TEXT,
    forensic_path TEXT,
    chromium_keyword_id INTEGER,
    chromium_url_id INTEGER,
    tags TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_browser_search_terms_browser 
    ON browser_search_terms(browser);
CREATE INDEX IF NOT EXISTS idx_browser_search_terms_evidence 
    ON browser_search_terms(evidence_id);
CREATE INDEX IF NOT EXISTS idx_browser_search_terms_normalized 
    ON browser_search_terms(normalized_term);
CREATE INDEX IF NOT EXISTS idx_browser_search_terms_run_id 
    ON browser_search_terms(run_id);
CREATE INDEX IF NOT EXISTS idx_browser_search_terms_term 
    ON browser_search_terms(term);
CREATE INDEX IF NOT EXISTS idx_browser_search_terms_time 
    ON browser_search_terms(search_time_utc);


-- Browser search engine configuration
CREATE TABLE IF NOT EXISTS search_engines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    short_name TEXT,
    keyword TEXT,
    url TEXT,
    favicon_url TEXT,
    suggest_url TEXT,
    prepopulate_id INTEGER,
    usage_count INTEGER DEFAULT 0,
    date_created_utc TEXT,
    last_modified_utc TEXT,
    last_visited_utc TEXT,
    is_default INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    new_tab_url TEXT,
    image_url TEXT,
    search_url_post_params TEXT,
    suggest_url_post_params TEXT,
    token_mappings TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_search_engines_browser ON search_engines(browser);
CREATE INDEX IF NOT EXISTS idx_search_engines_default ON search_engines(evidence_id, is_default);
CREATE INDEX IF NOT EXISTS idx_search_engines_evidence ON search_engines(evidence_id);
CREATE INDEX IF NOT EXISTS idx_search_engines_keyword ON search_engines(keyword);
CREATE INDEX IF NOT EXISTS idx_search_engines_run_id ON search_engines(run_id);

-- ============================================================================
-- Image Artifacts
-- ============================================================================


-- Carved/cached images metadata (one row per unique image)
CREATE TABLE IF NOT EXISTS images (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    rel_path TEXT NOT NULL,
    filename TEXT NOT NULL,
    md5 TEXT,
    sha256 TEXT,
    phash TEXT,
    phash_prefix INTEGER,
    exif_json TEXT,
    ts_utc TEXT,
    tags TEXT,
    notes TEXT,
    size_bytes INTEGER,
    first_discovered_by TEXT NOT NULL,
    first_discovered_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_images_evidence ON images(evidence_id);
CREATE INDEX IF NOT EXISTS idx_images_evidence_phash_prefix ON images(evidence_id, phash_prefix);
CREATE UNIQUE INDEX IF NOT EXISTS idx_images_evidence_sha256
    ON images(evidence_id, sha256)
    WHERE sha256 IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_images_evidence_size ON images(evidence_id, size_bytes);
CREATE INDEX IF NOT EXISTS idx_images_first_discovered ON images(first_discovered_by);
CREATE INDEX IF NOT EXISTS idx_images_md5 ON images(md5);
CREATE INDEX IF NOT EXISTS idx_images_sha256 ON images(sha256);
CREATE INDEX IF NOT EXISTS idx_images_size ON images(size_bytes);


-- Per-source provenance (one row per discovery location)
CREATE TABLE IF NOT EXISTS image_discoveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    image_id INTEGER NOT NULL,
    discovered_by TEXT NOT NULL,
    extractor_version TEXT,
    run_id TEXT NOT NULL,
    discovered_at TEXT DEFAULT (datetime('now')),
    fs_path TEXT,
    fs_mtime_epoch REAL,
    fs_mtime TEXT,
    fs_atime_epoch REAL,
    fs_atime TEXT,
    fs_crtime_epoch REAL,
    fs_crtime TEXT,
    fs_ctime_epoch REAL,
    fs_ctime TEXT,
    fs_inode INTEGER,
    carved_offset_bytes INTEGER,
    carved_block_size INTEGER,
    carved_tool_output TEXT,
    cache_url TEXT,
    cache_key TEXT,
    cache_filename TEXT,
    cache_response_time TEXT,
    source_metadata_json TEXT,
    FOREIGN KEY (image_id) REFERENCES images(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_image_disc_evidence ON image_discoveries(evidence_id);
CREATE INDEX IF NOT EXISTS idx_image_disc_fs_path ON image_discoveries(fs_path);
CREATE INDEX IF NOT EXISTS idx_image_disc_image ON image_discoveries(image_id);
CREATE INDEX IF NOT EXISTS idx_image_disc_offset ON image_discoveries(carved_offset_bytes);
CREATE INDEX IF NOT EXISTS idx_image_disc_run ON image_discoveries(run_id);
CREATE INDEX IF NOT EXISTS idx_image_disc_source ON image_discoveries(discovered_by);
CREATE UNIQUE INDEX IF NOT EXISTS idx_image_disc_unique
    ON image_discoveries(
        evidence_id, image_id, discovered_by, run_id,
        COALESCE(fs_path, ''),
        COALESCE(carved_offset_bytes, -1),
        COALESCE(carved_tool_output, ''),
        COALESCE(cache_key, ''),
        COALESCE(cache_filename, '')
    );

-- Convenience view for aggregated image sources
CREATE VIEW IF NOT EXISTS v_image_sources AS
SELECT 
    evidence_id,
    image_id,
    (SELECT discovered_by FROM image_discoveries d2 
     WHERE d2.evidence_id = id.evidence_id AND d2.image_id = id.image_id 
     ORDER BY discovered_at LIMIT 1) AS first_discovered_by,
    GROUP_CONCAT(DISTINCT discovered_by) AS sources,
    COUNT(DISTINCT discovered_by) AS source_count,
    COUNT(*) AS total_discoveries,
    MAX(CASE WHEN discovered_by = 'filesystem_images' THEN fs_path END) AS fs_path,
    MAX(CASE WHEN discovered_by IN ('cache_simple', 'cache_blockfile', 'cache_firefox', 'browser_storage_indexeddb', 'safari') THEN 1 ELSE 0 END) AS has_browser_source,
    GROUP_CONCAT(DISTINCT CASE WHEN discovered_by IN ('cache_simple', 'cache_blockfile', 'cache_firefox', 'browser_storage_indexeddb', 'safari') THEN discovered_by END) AS browser_sources
FROM image_discoveries id
GROUP BY evidence_id, image_id;


-- Hash matches: External hash database matches
CREATE TABLE IF NOT EXISTS hash_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    image_id INTEGER NOT NULL,
    db_name TEXT NOT NULL,
    db_md5 TEXT NOT NULL,
    matched_at_utc TEXT,
    list_name TEXT,
    list_version TEXT,
    note TEXT,
    hash_sha256 TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_hash_matches_evidence ON hash_matches(evidence_id);
CREATE INDEX IF NOT EXISTS idx_hash_matches_image ON hash_matches(image_id);
CREATE INDEX IF NOT EXISTS idx_hash_matches_run_id ON hash_matches(run_id);

-- ============================================================================
-- OS Artifacts
-- ============================================================================


-- OS indicators: Registry findings & OS artifacts
CREATE TABLE IF NOT EXISTS os_indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    type TEXT NOT NULL,
    name TEXT NOT NULL,
    value TEXT,
    path TEXT,
    hive TEXT,
    confidence TEXT,
    detected_at_utc TEXT,
    provenance TEXT,
    extra_json TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_os_indicators_evidence ON os_indicators(evidence_id);
CREATE INDEX IF NOT EXISTS idx_os_indicators_run_id ON os_indicators(run_id);
CREATE INDEX IF NOT EXISTS idx_os_indicators_type ON os_indicators(type);


-- Platform detections
CREATE TABLE IF NOT EXISTS platform_detections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    platform_id TEXT NOT NULL,
    platform_name TEXT NOT NULL,
    category TEXT NOT NULL,
    confidence TEXT NOT NULL,
    score INTEGER NOT NULL,
    matched_patterns_json TEXT NOT NULL,
    source_url TEXT,
    source_file TEXT,
    detected_at_utc TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_platform_detections_category ON platform_detections(evidence_id, category);
CREATE INDEX IF NOT EXISTS idx_platform_detections_confidence ON platform_detections(evidence_id, confidence);
CREATE INDEX IF NOT EXISTS idx_platform_detections_evidence ON platform_detections(evidence_id);
CREATE INDEX IF NOT EXISTS idx_platform_detections_run_id ON platform_detections(run_id);

-- ============================================================================
-- File List
-- ============================================================================


-- File listing from SleuthKit enumeration or CSV import
CREATE TABLE IF NOT EXISTS file_list (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    extension TEXT,
    size_bytes INTEGER,
    created_ts TEXT,
    modified_ts TEXT,
    accessed_ts TEXT,
    md5_hash TEXT,
    sha1_hash TEXT,
    sha256_hash TEXT,
    file_type TEXT,
    deleted BOOLEAN DEFAULT 0,
    metadata TEXT,
    import_source TEXT,
    import_timestamp TEXT NOT NULL,
    partition_index INTEGER DEFAULT -1,
    inode TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_file_list_evidence ON file_list(evidence_id);
CREATE INDEX IF NOT EXISTS idx_file_list_evidence_extension ON file_list(evidence_id, extension);
CREATE INDEX IF NOT EXISTS idx_file_list_extension ON file_list(extension);
CREATE INDEX IF NOT EXISTS idx_file_list_md5 ON file_list(md5_hash);
CREATE INDEX IF NOT EXISTS idx_file_list_name ON file_list(file_name);
CREATE INDEX IF NOT EXISTS idx_file_list_partition ON file_list(evidence_id, partition_index);
CREATE INDEX IF NOT EXISTS idx_file_list_path ON file_list(file_path);
CREATE INDEX IF NOT EXISTS idx_file_list_run_id ON file_list(run_id);
CREATE INDEX IF NOT EXISTS idx_file_list_sha1 ON file_list(sha1_hash);
CREATE INDEX IF NOT EXISTS idx_file_list_sha256 ON file_list(sha256_hash);
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_list_unique_path ON file_list(evidence_id, COALESCE(partition_index, -1), file_path);


-- File list matches: Reference list matches
CREATE TABLE IF NOT EXISTS file_list_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    file_list_id INTEGER NOT NULL,
    reference_list_name TEXT NOT NULL,
    match_type TEXT NOT NULL,
    matched_value TEXT NOT NULL,
    matched_at TEXT NOT NULL,
    run_id TEXT,
    FOREIGN KEY (file_list_id) REFERENCES file_list(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_file_list_matches_evidence ON file_list_matches(evidence_id);
CREATE INDEX IF NOT EXISTS idx_file_list_matches_file ON file_list_matches(file_list_id);
CREATE INDEX IF NOT EXISTS idx_file_list_matches_reflist ON file_list_matches(reference_list_name);
CREATE INDEX IF NOT EXISTS idx_file_list_matches_run_id ON file_list_matches(run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_file_list_matches_unique ON file_list_matches(file_list_id, reference_list_name);


-- File list filter cache: Pre-computed filter values
CREATE TABLE IF NOT EXISTS file_list_filter_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    filter_type TEXT NOT NULL,
    filter_value TEXT NOT NULL,
    count INTEGER DEFAULT 0,
    last_updated TEXT NOT NULL,
    run_id TEXT,
    UNIQUE(evidence_id, filter_type, filter_value, run_id)
);

CREATE INDEX IF NOT EXISTS idx_filter_cache_lookup ON file_list_filter_cache(evidence_id, filter_type);
CREATE INDEX IF NOT EXISTS idx_filter_cache_run_id ON file_list_filter_cache(run_id);
CREATE INDEX IF NOT EXISTS idx_filter_cache_updated ON file_list_filter_cache(last_updated);

-- ============================================================================
-- Unified Tagging System
-- ============================================================================


-- Central tag registry
CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    name_normalized TEXT NOT NULL,
    created_at_utc TEXT,
    created_by TEXT NOT NULL DEFAULT 'manual',
    usage_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE(evidence_id, name_normalized)
);

CREATE INDEX IF NOT EXISTS idx_tags_evidence ON tags(evidence_id);
CREATE INDEX IF NOT EXISTS idx_tags_normalized ON tags(name_normalized);


-- Tag associations
CREATE TABLE IF NOT EXISTS tag_associations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag_id INTEGER NOT NULL,
    evidence_id INTEGER NOT NULL,
    artifact_type TEXT NOT NULL,
    artifact_id INTEGER NOT NULL,
    tagged_at_utc TEXT,
    tagged_by TEXT NOT NULL DEFAULT 'manual',
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE,
    UNIQUE(tag_id, artifact_type, artifact_id)
);

CREATE INDEX IF NOT EXISTS idx_tag_assoc_artifact ON tag_associations(artifact_type, artifact_id);
CREATE INDEX IF NOT EXISTS idx_tag_assoc_artifact_evidence ON tag_associations(evidence_id, artifact_type, artifact_id);
CREATE INDEX IF NOT EXISTS idx_tag_assoc_evidence ON tag_associations(evidence_id);
CREATE INDEX IF NOT EXISTS idx_tag_assoc_tag ON tag_associations(tag_id);

-- Triggers for usage_count
CREATE TRIGGER IF NOT EXISTS trg_tags_usage_count_delete
AFTER DELETE ON tag_associations
BEGIN
    UPDATE tags SET usage_count = usage_count - 1 WHERE id = OLD.tag_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_tags_usage_count_insert
AFTER INSERT ON tag_associations
BEGIN
    UPDATE tags SET usage_count = usage_count + 1 WHERE id = NEW.tag_id;
END;

-- ============================================================================
-- Report Configuration
-- ============================================================================


-- Report configuration key-value store
CREATE TABLE IF NOT EXISTS report_config (
    evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_json TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_report_config_updated ON report_config(updated_at_utc);


-- Report sections (module-based workflow)
CREATE TABLE IF NOT EXISTS report_sections_v2 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    statement TEXT NOT NULL DEFAULT '',
    order_index INTEGER NOT NULL DEFAULT 0,
    is_collapsed INTEGER NOT NULL DEFAULT 0,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_report_sections_v2_evidence_order
    ON report_sections_v2 (evidence_id, order_index, id);


-- Report section modules
CREATE TABLE IF NOT EXISTS report_section_modules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    section_id INTEGER NOT NULL,
    module_id TEXT NOT NULL,
    config_json TEXT NOT NULL DEFAULT '{}',
    order_index INTEGER NOT NULL DEFAULT 0,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY (section_id) REFERENCES report_sections_v2(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_report_section_modules_section
    ON report_section_modules (section_id, order_index);

-- ============================================================================
-- Downloads: Investigator-acquired files
-- ============================================================================


-- Investigator downloads (distinct from browser_downloads)
CREATE TABLE IF NOT EXISTS downloads (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    url_id INTEGER,
    url TEXT NOT NULL,
    domain TEXT,
    file_type TEXT NOT NULL,
    file_extension TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    dest_path TEXT,
    filename TEXT,
    size_bytes INTEGER,
    md5 TEXT,
    sha256 TEXT,
    content_type TEXT,
    phash TEXT,
    exif_json TEXT,
    width INTEGER,
    height INTEGER,
    queued_at_utc TEXT NOT NULL,
    started_at_utc TEXT,
    completed_at_utc TEXT,
    response_code INTEGER,
    error_message TEXT,
    attempts INTEGER DEFAULT 0,
    duration_seconds REAL,
    notes TEXT,
    run_id TEXT,
    FOREIGN KEY (url_id) REFERENCES urls(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_downloads_dedup ON downloads(evidence_id, dest_path)
    WHERE dest_path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_downloads_evidence ON downloads(evidence_id);
CREATE INDEX IF NOT EXISTS idx_downloads_file_type ON downloads(file_type);
CREATE INDEX IF NOT EXISTS idx_downloads_md5 ON downloads(md5);
CREATE INDEX IF NOT EXISTS idx_downloads_phash ON downloads(phash);
CREATE INDEX IF NOT EXISTS idx_downloads_run_id ON downloads(run_id);
CREATE INDEX IF NOT EXISTS idx_downloads_sha256 ON downloads(sha256);
CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads(status);
CREATE INDEX IF NOT EXISTS idx_downloads_url_id ON downloads(url_id);


-- Download audit trail
CREATE TABLE IF NOT EXISTS download_audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    ts_utc TEXT NOT NULL,
    url TEXT NOT NULL,
    method TEXT NOT NULL,
    outcome TEXT NOT NULL,
    blocked INTEGER NOT NULL DEFAULT 0,
    reason TEXT,
    status_code INTEGER,
    attempts INTEGER,
    duration_s REAL,
    bytes_written INTEGER,
    content_type TEXT,
    caller_info TEXT,
    created_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_download_audit_evidence_ts
    ON download_audit(evidence_id, ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_download_audit_outcome
    ON download_audit(outcome);
CREATE INDEX IF NOT EXISTS idx_download_audit_url
    ON download_audit(url);

-- ============================================================================
-- Metadata & Audit Tables
-- ============================================================================


-- Timeline: Fused evidence timeline
CREATE TABLE IF NOT EXISTS timeline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    ts_utc TEXT NOT NULL,
    kind TEXT NOT NULL,
    ref_table TEXT NOT NULL,
    ref_id INTEGER NOT NULL,
    confidence TEXT,
    note TEXT,
    run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_timeline_evidence ON timeline(evidence_id);
CREATE INDEX IF NOT EXISTS idx_timeline_run_id ON timeline(run_id);
CREATE INDEX IF NOT EXISTS idx_timeline_ts ON timeline(ts_utc);


-- Process log: Task execution audit trail
CREATE TABLE IF NOT EXISTS process_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    task TEXT NOT NULL,
    command TEXT,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    exit_code INTEGER,
    stdout TEXT,
    stderr TEXT,
    run_id TEXT,
    extractor_name TEXT,
    extractor_version TEXT,
    records_extracted INTEGER,
    records_ingested INTEGER,
    warnings_json TEXT,
    log_file_path TEXT
);

CREATE INDEX IF NOT EXISTS idx_process_log_evidence_task ON process_log(evidence_id, task);
CREATE INDEX IF NOT EXISTS idx_process_log_extractor_name ON process_log(extractor_name);
CREATE INDEX IF NOT EXISTS idx_process_log_run_id ON process_log(run_id);


-- Extractor statistics: Per-extractor run metrics
CREATE TABLE IF NOT EXISTS extractor_statistics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    extractor_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    duration_seconds REAL,
    status TEXT NOT NULL DEFAULT 'running',
    discovered TEXT DEFAULT '{}',
    ingested TEXT DEFAULT '{}',
    failed TEXT DEFAULT '{}',
    skipped TEXT DEFAULT '{}',
    UNIQUE(evidence_id, extractor_name)
);

CREATE INDEX IF NOT EXISTS idx_extractor_statistics_evidence 
    ON extractor_statistics(evidence_id);


-- Extraction warnings: Unknown schemas, parse errors
CREATE TABLE IF NOT EXISTS extraction_warnings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    extractor_name TEXT NOT NULL,
    warning_type TEXT NOT NULL,
    severity TEXT DEFAULT 'info',
    category TEXT,
    artifact_type TEXT,
    source_file TEXT,
    item_name TEXT NOT NULL,
    item_value TEXT,
    context_json TEXT,
    created_at_utc TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_extraction_warnings_category
    ON extraction_warnings(category);
CREATE INDEX IF NOT EXISTS idx_extraction_warnings_evidence 
    ON extraction_warnings(evidence_id);
CREATE INDEX IF NOT EXISTS idx_extraction_warnings_extractor 
    ON extraction_warnings(extractor_name);
CREATE INDEX IF NOT EXISTS idx_extraction_warnings_run_id
    ON extraction_warnings(run_id);
CREATE INDEX IF NOT EXISTS idx_extraction_warnings_type 
    ON extraction_warnings(warning_type);


-- Extracted files: Universal audit log for all extracted files
CREATE TABLE IF NOT EXISTS extracted_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    extractor_name TEXT NOT NULL,
    extractor_version TEXT,
    run_id TEXT NOT NULL,
    extracted_at_utc TEXT NOT NULL DEFAULT (datetime('now')),
    source_path TEXT,
    source_inode TEXT,
    partition_index INTEGER,
    source_offset_bytes INTEGER,
    source_block_size INTEGER,
    dest_rel_path TEXT NOT NULL,
    dest_filename TEXT NOT NULL,
    size_bytes INTEGER,
    file_type TEXT,
    mime_type TEXT,
    md5 TEXT,
    sha256 TEXT,
    status TEXT NOT NULL DEFAULT 'ok',
    error_message TEXT,
    metadata_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_extracted_files_ev_extractor 
    ON extracted_files(evidence_id, extractor_name);
CREATE INDEX IF NOT EXISTS idx_extracted_files_ev_partition 
    ON extracted_files(evidence_id, partition_index);
CREATE INDEX IF NOT EXISTS idx_extracted_files_ev_run 
    ON extracted_files(evidence_id, run_id);
CREATE INDEX IF NOT EXISTS idx_extracted_files_ev_status 
    ON extracted_files(evidence_id, status);
CREATE INDEX IF NOT EXISTS idx_extracted_files_sha256 ON extracted_files(sha256);
CREATE INDEX IF NOT EXISTS idx_extracted_files_source_path ON extracted_files(source_path);

-- ============================================================================
-- Autofill Tables
-- ============================================================================


-- Basic autofill entries (name-value pairs from forms)
CREATE TABLE IF NOT EXISTS autofill (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    name TEXT NOT NULL,
    value TEXT,
    date_created_utc TEXT,
    date_last_used_utc TEXT,
    count INTEGER DEFAULT 1,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT,
    field_id_hash TEXT,
    is_deleted INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_autofill_browser ON autofill(browser);
CREATE INDEX IF NOT EXISTS idx_autofill_evidence ON autofill(evidence_id);
CREATE INDEX IF NOT EXISTS idx_autofill_name ON autofill(name);
CREATE INDEX IF NOT EXISTS idx_autofill_run_id ON autofill(run_id);


-- Autofill address profiles
CREATE TABLE IF NOT EXISTS autofill_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    guid TEXT,
    full_name TEXT,
    company_name TEXT,
    street_address TEXT,
    city TEXT,
    state TEXT,
    zipcode TEXT,
    country_code TEXT,
    phone TEXT,
    email TEXT,
    date_modified_utc TEXT,
    use_count INTEGER,
    use_date_utc TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_autofill_profiles_email ON autofill_profiles(email);
CREATE INDEX IF NOT EXISTS idx_autofill_profiles_evidence ON autofill_profiles(evidence_id);
CREATE INDEX IF NOT EXISTS idx_autofill_profiles_phone ON autofill_profiles(phone);
CREATE INDEX IF NOT EXISTS idx_autofill_profiles_run_id ON autofill_profiles(run_id);


-- Autofill profile tokens (Chromium 100+ contact info)
CREATE TABLE IF NOT EXISTS autofill_profile_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    guid TEXT NOT NULL,
    token_type INTEGER NOT NULL,
    token_type_name TEXT,
    token_value TEXT,
    source_table TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    created_at_utc TEXT,
    parent_table TEXT,
    parent_use_count INTEGER,
    parent_use_date_utc TEXT,
    parent_date_modified_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_autofill_profile_tokens_evidence ON autofill_profile_tokens(evidence_id);
CREATE INDEX IF NOT EXISTS idx_autofill_profile_tokens_guid ON autofill_profile_tokens(guid);
CREATE INDEX IF NOT EXISTS idx_autofill_profile_tokens_parent_table
ON autofill_profile_tokens(parent_table);
CREATE INDEX IF NOT EXISTS idx_autofill_profile_tokens_run_id ON autofill_profile_tokens(run_id);
CREATE INDEX IF NOT EXISTS idx_autofill_profile_tokens_type ON autofill_profile_tokens(token_type);


-- Autofill block list (Edge-specific)
CREATE TABLE IF NOT EXISTS autofill_block_list (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    guid TEXT,
    block_value TEXT,
    block_value_type INTEGER,
    attribute_flag INTEGER,
    meta_data TEXT,
    device_model TEXT,
    date_created_utc TEXT,
    date_modified_utc TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_autofill_block_list_block_value ON autofill_block_list(block_value);
CREATE INDEX IF NOT EXISTS idx_autofill_block_list_browser ON autofill_block_list(browser);
CREATE INDEX IF NOT EXISTS idx_autofill_block_list_device ON autofill_block_list(device_model);
CREATE INDEX IF NOT EXISTS idx_autofill_block_list_evidence ON autofill_block_list(evidence_id);
CREATE INDEX IF NOT EXISTS idx_autofill_block_list_run_id ON autofill_block_list(run_id);


-- Autofill IBANs (Chromium/Edge)
CREATE TABLE IF NOT EXISTS autofill_ibans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    source_table TEXT NOT NULL,
    guid TEXT,
    instrument_id INTEGER,
    nickname TEXT,
    value TEXT,
    value_encrypted BLOB,
    prefix TEXT,
    suffix TEXT,
    length INTEGER,
    use_count INTEGER,
    use_date_utc TEXT,
    date_modified_utc TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_autofill_ibans_browser ON autofill_ibans(browser);
CREATE INDEX IF NOT EXISTS idx_autofill_ibans_evidence ON autofill_ibans(evidence_id);
CREATE INDEX IF NOT EXISTS idx_autofill_ibans_guid ON autofill_ibans(guid);
CREATE INDEX IF NOT EXISTS idx_autofill_ibans_instrument_id ON autofill_ibans(instrument_id);
CREATE INDEX IF NOT EXISTS idx_autofill_ibans_run_id ON autofill_ibans(run_id);
CREATE INDEX IF NOT EXISTS idx_autofill_ibans_source_table ON autofill_ibans(source_table);


-- Deleted form history (Firefox moz_deleted_formhistory)
CREATE TABLE IF NOT EXISTS deleted_form_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    guid TEXT,
    time_deleted_utc TEXT,
    original_fieldname TEXT,
    original_value TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_deleted_form_history_evidence ON deleted_form_history(evidence_id);
CREATE INDEX IF NOT EXISTS idx_deleted_form_history_guid ON deleted_form_history(guid);
CREATE INDEX IF NOT EXISTS idx_deleted_form_history_run_id ON deleted_form_history(run_id);
CREATE INDEX IF NOT EXISTS idx_deleted_form_history_time ON deleted_form_history(time_deleted_utc);

-- ============================================================================
-- Credentials & Payment
-- ============================================================================


-- Saved credentials (Login Data)
CREATE TABLE IF NOT EXISTS credentials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    origin_url TEXT NOT NULL,
    action_url TEXT,
    username_element TEXT,
    username_value TEXT,
    password_element TEXT,
    password_value_encrypted BLOB,
    signon_realm TEXT,
    date_created_utc TEXT,
    date_last_used_utc TEXT,
    date_password_modified_utc TEXT,
    times_used INTEGER,
    blacklisted_by_user INTEGER DEFAULT 0,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT,
    is_insecure INTEGER DEFAULT 0,
    is_breached INTEGER DEFAULT 0,
    password_notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_credentials_evidence ON credentials(evidence_id);
CREATE INDEX IF NOT EXISTS idx_credentials_origin ON credentials(origin_url);
CREATE INDEX IF NOT EXISTS idx_credentials_run_id ON credentials(run_id);
CREATE INDEX IF NOT EXISTS idx_credentials_signon_realm ON credentials(signon_realm);
CREATE INDEX IF NOT EXISTS idx_credentials_username ON credentials(username_value);


-- Saved credit cards
CREATE TABLE IF NOT EXISTS credit_cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    guid TEXT,
    name_on_card TEXT,
    expiration_month INTEGER,
    expiration_year INTEGER,
    card_number_encrypted BLOB,
    card_number_last_four TEXT,
    billing_address_id TEXT,
    date_modified_utc TEXT,
    use_count INTEGER,
    use_date_utc TEXT,
    nickname TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_credit_cards_evidence ON credit_cards(evidence_id);
CREATE INDEX IF NOT EXISTS idx_credit_cards_name ON credit_cards(name_on_card);
CREATE INDEX IF NOT EXISTS idx_credit_cards_run_id ON credit_cards(run_id);

-- ============================================================================
-- Session Restore Tables
-- ============================================================================


-- Browser windows at seizure time
CREATE TABLE IF NOT EXISTS session_windows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    window_id INTEGER,
    selected_tab_index INTEGER,
    window_type TEXT,
    bounds_x INTEGER,
    bounds_y INTEGER,
    bounds_width INTEGER,
    bounds_height INTEGER,
    show_state TEXT,
    session_type TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_session_windows_evidence ON session_windows(evidence_id);
CREATE INDEX IF NOT EXISTS idx_session_windows_run_id ON session_windows(run_id);
CREATE INDEX IF NOT EXISTS idx_session_windows_session_type ON session_windows(session_type);


-- Open tabs with URLs and titles
CREATE TABLE IF NOT EXISTS session_tabs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    window_id INTEGER,
    tab_index INTEGER,
    url TEXT NOT NULL,
    title TEXT,
    pinned INTEGER DEFAULT 0,
    group_id INTEGER,
    last_accessed_utc TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_session_tabs_evidence ON session_tabs(evidence_id);
CREATE INDEX IF NOT EXISTS idx_session_tabs_run_id ON session_tabs(run_id);
CREATE INDEX IF NOT EXISTS idx_session_tabs_url ON session_tabs(url);
CREATE INDEX IF NOT EXISTS idx_session_tabs_window ON session_tabs(window_id);


-- Navigation history within tabs
CREATE TABLE IF NOT EXISTS session_tab_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    tab_id INTEGER,
    nav_index INTEGER,
    url TEXT NOT NULL,
    title TEXT,
    transition_type TEXT,
    timestamp_utc TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT,
    referrer_url TEXT,
    original_request_url TEXT,
    has_post_data INTEGER DEFAULT 0,
    http_status_code INTEGER
);

CREATE INDEX IF NOT EXISTS idx_session_tab_history_evidence ON session_tab_history(evidence_id);
CREATE INDEX IF NOT EXISTS idx_session_tab_history_run_id ON session_tab_history(run_id);
CREATE INDEX IF NOT EXISTS idx_session_tab_history_tab ON session_tab_history(tab_id);
CREATE INDEX IF NOT EXISTS idx_session_tab_history_url ON session_tab_history(url);


-- Recently closed tabs
CREATE TABLE IF NOT EXISTS closed_tabs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    url TEXT NOT NULL,
    title TEXT,
    closed_at_utc TEXT,
    original_window_id INTEGER,
    original_tab_index INTEGER,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_closed_tabs_closed_at ON closed_tabs(closed_at_utc);
CREATE INDEX IF NOT EXISTS idx_closed_tabs_evidence ON closed_tabs(evidence_id);
CREATE INDEX IF NOT EXISTS idx_closed_tabs_run_id ON closed_tabs(run_id);
CREATE INDEX IF NOT EXISTS idx_closed_tabs_url ON closed_tabs(url);


-- Firefox session restore form data
CREATE TABLE IF NOT EXISTS session_form_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    url TEXT,
    field_name TEXT NOT NULL,
    field_value TEXT,
    field_type TEXT,
    xpath TEXT,
    window_id INTEGER,
    tab_id INTEGER,
    nav_index INTEGER,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_session_form_data_evidence ON session_form_data(evidence_id);
CREATE INDEX IF NOT EXISTS idx_session_form_data_field_name ON session_form_data(field_name);
CREATE INDEX IF NOT EXISTS idx_session_form_data_run_id ON session_form_data(run_id);
CREATE INDEX IF NOT EXISTS idx_session_form_data_url ON session_form_data(url);

-- ============================================================================
-- Site Permissions
-- ============================================================================

CREATE TABLE IF NOT EXISTS site_permissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    origin TEXT NOT NULL,
    permission_type TEXT NOT NULL,
    permission_value TEXT NOT NULL,
    raw_type TEXT,
    raw_value INTEGER,
    granted_at_utc TEXT,
    expires_at_utc TEXT,
    expires_type TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_site_permissions_evidence ON site_permissions(evidence_id);
CREATE INDEX IF NOT EXISTS idx_site_permissions_origin ON site_permissions(origin);
CREATE INDEX IF NOT EXISTS idx_site_permissions_run_id ON site_permissions(run_id);
CREATE INDEX IF NOT EXISTS idx_site_permissions_type ON site_permissions(permission_type);
CREATE INDEX IF NOT EXISTS idx_site_permissions_value ON site_permissions(permission_value);

-- ============================================================================
-- Site Engagement
-- ============================================================================


-- Chromium site/media engagement data from Preferences
CREATE TABLE IF NOT EXISTS site_engagement (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT NOT NULL,
    origin TEXT NOT NULL,
    engagement_type TEXT NOT NULL,
    raw_score REAL,
    points_added_today REAL,
    last_engagement_time_utc TEXT,
    last_shortcut_launch_time_utc TEXT,
    has_high_score INTEGER,
    media_playbacks INTEGER,
    visits INTEGER,
    last_media_playback_time_utc TEXT,
    last_modified_webkit INTEGER,
    expiration TEXT,
    model INTEGER,
    run_id TEXT NOT NULL,
    source_path TEXT,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    created_at_utc TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_site_engagement_browser ON site_engagement(browser);
CREATE INDEX IF NOT EXISTS idx_site_engagement_evidence ON site_engagement(evidence_id);
CREATE INDEX IF NOT EXISTS idx_site_engagement_origin ON site_engagement(origin);
CREATE INDEX IF NOT EXISTS idx_site_engagement_run ON site_engagement(run_id);
CREATE INDEX IF NOT EXISTS idx_site_engagement_score ON site_engagement(raw_score);
CREATE INDEX IF NOT EXISTS idx_site_engagement_type ON site_engagement(engagement_type);

-- ============================================================================
-- Media History Tables (Chromium 86+)
-- ============================================================================


-- Playback records
CREATE TABLE IF NOT EXISTS media_playback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    url TEXT NOT NULL,
    origin TEXT,
    watch_time_seconds REAL DEFAULT 0,
    has_video INTEGER DEFAULT 0,
    has_audio INTEGER DEFAULT 0,
    last_played_utc TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_media_playback_evidence ON media_playback(evidence_id);
CREATE INDEX IF NOT EXISTS idx_media_playback_origin ON media_playback(origin);
CREATE INDEX IF NOT EXISTS idx_media_playback_run_id ON media_playback(run_id);
CREATE INDEX IF NOT EXISTS idx_media_playback_url ON media_playback(url);


-- Detailed playback sessions
CREATE TABLE IF NOT EXISTS media_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    url TEXT NOT NULL,
    origin TEXT,
    title TEXT,
    artist TEXT,
    album TEXT,
    source_title TEXT,
    duration_ms INTEGER DEFAULT 0,
    position_ms INTEGER DEFAULT 0,
    completion_percent REAL,
    last_played_utc TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_media_sessions_evidence ON media_sessions(evidence_id);
CREATE INDEX IF NOT EXISTS idx_media_sessions_run_id ON media_sessions(run_id);
CREATE INDEX IF NOT EXISTS idx_media_sessions_title ON media_sessions(title);
CREATE INDEX IF NOT EXISTS idx_media_sessions_url ON media_sessions(url);


-- Media origins: Sites that played media
CREATE TABLE IF NOT EXISTS media_origins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    origin TEXT NOT NULL,
    origin_id_source INTEGER,
    last_updated_utc TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    created_at_utc TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_media_origins_browser ON media_origins(evidence_id, browser);
CREATE INDEX IF NOT EXISTS idx_media_origins_evidence ON media_origins(evidence_id);
CREATE INDEX IF NOT EXISTS idx_media_origins_origin ON media_origins(origin);
CREATE INDEX IF NOT EXISTS idx_media_origins_run_id ON media_origins(run_id);

-- ============================================================================
-- HSTS/Transport Security (Chromium hashed entries)
-- ============================================================================

CREATE TABLE IF NOT EXISTS hsts_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    hashed_host TEXT NOT NULL,
    sts_observed REAL,
    expiry REAL,
    mode TEXT,
    include_subdomains INTEGER DEFAULT 0,
    decoded_host TEXT,
    decode_method TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_hsts_entries_browser ON hsts_entries(browser);
CREATE INDEX IF NOT EXISTS idx_hsts_entries_decoded_host ON hsts_entries(decoded_host);
CREATE INDEX IF NOT EXISTS idx_hsts_entries_evidence ON hsts_entries(evidence_id);
CREATE INDEX IF NOT EXISTS idx_hsts_entries_hashed_host ON hsts_entries(hashed_host);
CREATE INDEX IF NOT EXISTS idx_hsts_entries_run_id ON hsts_entries(run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_hsts_entries_unique
    ON hsts_entries(evidence_id, hashed_host, source_path);

-- ============================================================================
-- Windows Jump List Entries
-- ============================================================================

CREATE TABLE IF NOT EXISTS jump_list_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    appid TEXT NOT NULL,
    browser TEXT,
    jumplist_path TEXT NOT NULL,
    entry_id TEXT,
    target_path TEXT,
    arguments TEXT,
    working_directory TEXT,
    url TEXT,
    title TEXT,
    lnk_creation_time TEXT,
    lnk_modification_time TEXT,
    lnk_access_time TEXT,
    access_count INTEGER,
    pin_status TEXT,
    run_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    discovered_by TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_jump_list_entries_appid ON jump_list_entries(appid);
CREATE INDEX IF NOT EXISTS idx_jump_list_entries_browser ON jump_list_entries(browser);
CREATE INDEX IF NOT EXISTS idx_jump_list_entries_evidence ON jump_list_entries(evidence_id);
CREATE INDEX IF NOT EXISTS idx_jump_list_entries_run_id ON jump_list_entries(run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jump_list_entries_unique
    ON jump_list_entries(evidence_id, jumplist_path, entry_id);
CREATE INDEX IF NOT EXISTS idx_jump_list_entries_url ON jump_list_entries(url);

-- ============================================================================
-- Browser Extensions
-- ============================================================================


-- Installed browser extensions/add-ons with risk classification
CREATE TABLE IF NOT EXISTS browser_extensions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    extension_id TEXT NOT NULL,
    name TEXT NOT NULL,
    version TEXT,
    description TEXT,
    author TEXT,
    homepage_url TEXT,
    manifest_version INTEGER,
    permissions TEXT,
    host_permissions TEXT,
    content_scripts TEXT,
    install_time TEXT,
    update_time TEXT,
    enabled INTEGER DEFAULT 1,
    risk_score INTEGER DEFAULT 0,
    risk_factors TEXT,
    known_category TEXT,
    disable_reasons INTEGER DEFAULT 0,
    install_location INTEGER,
    install_location_text TEXT,
    from_webstore INTEGER,
    granted_permissions TEXT,
    source_path TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_browser_extensions_browser ON browser_extensions(browser);
CREATE INDEX IF NOT EXISTS idx_browser_extensions_category ON browser_extensions(known_category);
CREATE INDEX IF NOT EXISTS idx_browser_extensions_evidence ON browser_extensions(evidence_id);
CREATE INDEX IF NOT EXISTS idx_browser_extensions_id ON browser_extensions(extension_id);
CREATE INDEX IF NOT EXISTS idx_browser_extensions_risk ON browser_extensions(risk_score);
CREATE INDEX IF NOT EXISTS idx_browser_extensions_run ON browser_extensions(run_id);

-- ============================================================================
-- Web Storage Tables
-- ============================================================================


-- Browser Local Storage key-value pairs
CREATE TABLE IF NOT EXISTS local_storage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    origin TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    value_type TEXT,
    value_size INTEGER,
    source_path TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_local_storage_browser ON local_storage(browser);
CREATE INDEX IF NOT EXISTS idx_local_storage_evidence ON local_storage(evidence_id);
CREATE INDEX IF NOT EXISTS idx_local_storage_key ON local_storage(key);
CREATE INDEX IF NOT EXISTS idx_local_storage_origin ON local_storage(origin);
CREATE INDEX IF NOT EXISTS idx_local_storage_run ON local_storage(run_id);


-- Browser Session Storage key-value pairs
CREATE TABLE IF NOT EXISTS session_storage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    origin TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    value_type TEXT,
    value_size INTEGER,
    source_path TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_session_storage_browser ON session_storage(browser);
CREATE INDEX IF NOT EXISTS idx_session_storage_evidence ON session_storage(evidence_id);
CREATE INDEX IF NOT EXISTS idx_session_storage_key ON session_storage(key);
CREATE INDEX IF NOT EXISTS idx_session_storage_origin ON session_storage(origin);
CREATE INDEX IF NOT EXISTS idx_session_storage_run ON session_storage(run_id);


-- Unified tagging of web storage origins
CREATE TABLE IF NOT EXISTS stored_sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    origin TEXT NOT NULL,
    local_storage_count INTEGER DEFAULT 0,
    session_storage_count INTEGER DEFAULT 0,
    indexeddb_count INTEGER DEFAULT 0,
    total_keys INTEGER DEFAULT 0,
    browsers TEXT,
    first_seen_utc TEXT,
    last_updated_utc TEXT,
    tags TEXT,
    notes TEXT,
    created_at_utc TEXT DEFAULT (datetime('now')),
    UNIQUE(evidence_id, origin)
);

CREATE INDEX IF NOT EXISTS idx_stored_sites_evidence ON stored_sites(evidence_id);
CREATE INDEX IF NOT EXISTS idx_stored_sites_origin ON stored_sites(origin);

-- ============================================================================
-- Storage Analysis (Token/Identifier Discovery)
-- ============================================================================


-- Storage tokens (OAuth, JWT, session tokens)
CREATE TABLE IF NOT EXISTS storage_tokens (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    origin TEXT NOT NULL,
    storage_type TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    token_type TEXT NOT NULL,
    token_value TEXT NOT NULL,
    token_hash TEXT,
    issuer TEXT,
    subject TEXT,
    audience TEXT,
    associated_email TEXT,
    associated_user_id TEXT,
    issued_at_utc TEXT,
    expires_at_utc TEXT,
    last_used_utc TEXT,
    risk_level TEXT DEFAULT 'medium',
    is_expired INTEGER DEFAULT 0,
    source_path TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_storage_tokens_email ON storage_tokens(associated_email);
CREATE INDEX IF NOT EXISTS idx_storage_tokens_evidence ON storage_tokens(evidence_id);
CREATE INDEX IF NOT EXISTS idx_storage_tokens_expires ON storage_tokens(expires_at_utc);
CREATE INDEX IF NOT EXISTS idx_storage_tokens_hash ON storage_tokens(token_hash);
CREATE INDEX IF NOT EXISTS idx_storage_tokens_origin ON storage_tokens(origin);
CREATE INDEX IF NOT EXISTS idx_storage_tokens_run_id ON storage_tokens(run_id);
CREATE INDEX IF NOT EXISTS idx_storage_tokens_token_type ON storage_tokens(token_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_tokens_unique 
    ON storage_tokens(evidence_id, origin, storage_key, token_hash);


-- Storage identifiers (tracking IDs, user IDs, device IDs)
CREATE TABLE IF NOT EXISTS storage_identifiers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    origin TEXT NOT NULL,
    storage_type TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    identifier_type TEXT NOT NULL,
    identifier_name TEXT,
    identifier_value TEXT NOT NULL,
    first_seen_utc TEXT,
    last_seen_utc TEXT,
    source_path TEXT,
    partition_index INTEGER,
    fs_type TEXT,
    created_at_utc TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_storage_identifiers_evidence ON storage_identifiers(evidence_id);
CREATE INDEX IF NOT EXISTS idx_storage_identifiers_origin ON storage_identifiers(origin);
CREATE INDEX IF NOT EXISTS idx_storage_identifiers_run_id ON storage_identifiers(run_id);
CREATE INDEX IF NOT EXISTS idx_storage_identifiers_type ON storage_identifiers(identifier_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_storage_identifiers_unique
    ON storage_identifiers(evidence_id, origin, storage_key, identifier_type, identifier_value);
CREATE INDEX IF NOT EXISTS idx_storage_identifiers_value ON storage_identifiers(identifier_value);

-- ============================================================================
-- IndexedDB Tables
-- ============================================================================


-- IndexedDB database metadata
CREATE TABLE IF NOT EXISTS indexeddb_databases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    origin TEXT NOT NULL,
    database_name TEXT NOT NULL,
    database_version INTEGER,
    object_stores TEXT,
    total_entries INTEGER DEFAULT 0,
    source_path TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_indexeddb_db_browser ON indexeddb_databases(browser);
CREATE INDEX IF NOT EXISTS idx_indexeddb_db_evidence ON indexeddb_databases(evidence_id);
CREATE INDEX IF NOT EXISTS idx_indexeddb_db_origin ON indexeddb_databases(origin);
CREATE INDEX IF NOT EXISTS idx_indexeddb_db_run ON indexeddb_databases(run_id);


-- IndexedDB object store entries
CREATE TABLE IF NOT EXISTS indexeddb_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    database_id INTEGER,
    object_store TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT,
    value_type TEXT,
    value_size INTEGER,
    notes TEXT,
    created_at_utc TEXT,
    FOREIGN KEY (database_id) REFERENCES indexeddb_databases(id)
);

CREATE INDEX IF NOT EXISTS idx_indexeddb_entries_db ON indexeddb_entries(database_id);
CREATE INDEX IF NOT EXISTS idx_indexeddb_entries_evidence ON indexeddb_entries(evidence_id);
CREATE INDEX IF NOT EXISTS idx_indexeddb_entries_run ON indexeddb_entries(run_id);
CREATE INDEX IF NOT EXISTS idx_indexeddb_entries_store ON indexeddb_entries(object_store);

-- ============================================================================
-- Sync Data Tables
-- ============================================================================


-- Browser sync account information
CREATE TABLE IF NOT EXISTS sync_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    account_id TEXT,
    email TEXT,
    display_name TEXT,
    gaia_id TEXT,
    profile_path TEXT,
    last_sync_time TEXT,
    sync_enabled INTEGER DEFAULT 1,
    synced_types TEXT,
    raw_data TEXT,
    source_path TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_data_browser ON sync_data(browser);
CREATE INDEX IF NOT EXISTS idx_sync_data_email ON sync_data(email);
CREATE INDEX IF NOT EXISTS idx_sync_data_evidence ON sync_data(evidence_id);
CREATE INDEX IF NOT EXISTS idx_sync_data_run ON sync_data(run_id);


-- Linked devices from browser sync
CREATE TABLE IF NOT EXISTS synced_devices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    device_id TEXT,
    device_name TEXT,
    device_type TEXT,
    os_type TEXT,
    chrome_version TEXT,
    last_updated TEXT,
    sync_account_id TEXT,
    raw_data TEXT,
    source_path TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_synced_devices_browser ON synced_devices(browser);
CREATE INDEX IF NOT EXISTS idx_synced_devices_evidence ON synced_devices(evidence_id);
CREATE INDEX IF NOT EXISTS idx_synced_devices_name ON synced_devices(device_name);
CREATE INDEX IF NOT EXISTS idx_synced_devices_run ON synced_devices(run_id);
CREATE INDEX IF NOT EXISTS idx_synced_devices_type ON synced_devices(device_type);

-- ============================================================================
-- Favicon Tables
-- ============================================================================


-- Icon storage with deduplication via hash
CREATE TABLE IF NOT EXISTS favicons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    icon_url TEXT NOT NULL,
    icon_type INTEGER,
    width INTEGER,
    height INTEGER,
    icon_data BLOB,
    icon_md5 TEXT,
    icon_sha256 TEXT,
    last_updated_utc TEXT,
    last_requested_utc TEXT,
    source_path TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_favicons_browser ON favicons(browser);
CREATE UNIQUE INDEX IF NOT EXISTS idx_favicons_dedup
    ON favicons(evidence_id, browser, icon_sha256)
    WHERE icon_sha256 IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_favicons_evidence ON favicons(evidence_id);
CREATE INDEX IF NOT EXISTS idx_favicons_evidence_browser ON favicons(evidence_id, browser);
CREATE INDEX IF NOT EXISTS idx_favicons_icon_url ON favicons(icon_url);
CREATE INDEX IF NOT EXISTS idx_favicons_md5 ON favicons(icon_md5);
CREATE INDEX IF NOT EXISTS idx_favicons_run ON favicons(run_id);
CREATE INDEX IF NOT EXISTS idx_favicons_sha256 ON favicons(icon_sha256);


-- Page URL to icon mapping
CREATE TABLE IF NOT EXISTS favicon_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    favicon_id INTEGER NOT NULL,
    page_url TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    created_at_utc TEXT,
    FOREIGN KEY (favicon_id) REFERENCES favicons(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_favicon_mappings_browser ON favicon_mappings(browser);
CREATE INDEX IF NOT EXISTS idx_favicon_mappings_evidence ON favicon_mappings(evidence_id);
CREATE INDEX IF NOT EXISTS idx_favicon_mappings_favicon ON favicon_mappings(favicon_id);
CREATE INDEX IF NOT EXISTS idx_favicon_mappings_page_url ON favicon_mappings(page_url);
CREATE INDEX IF NOT EXISTS idx_favicon_mappings_run ON favicon_mappings(run_id);

-- ============================================================================
-- Top Sites
-- ============================================================================


-- Most visited sites with URL rank
CREATE TABLE IF NOT EXISTS top_sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    url TEXT NOT NULL,
    title TEXT,
    url_rank INTEGER,
    thumbnail_data BLOB,
    thumbnail_width INTEGER,
    thumbnail_height INTEGER,
    source_path TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_top_sites_browser ON top_sites(browser);
CREATE INDEX IF NOT EXISTS idx_top_sites_evidence ON top_sites(evidence_id);
CREATE INDEX IF NOT EXISTS idx_top_sites_evidence_browser ON top_sites(evidence_id, browser);
CREATE INDEX IF NOT EXISTS idx_top_sites_rank ON top_sites(url_rank);
CREATE INDEX IF NOT EXISTS idx_top_sites_run ON top_sites(run_id);
CREATE INDEX IF NOT EXISTS idx_top_sites_url ON top_sites(url);

-- ============================================================================
-- Browser Configuration
-- ============================================================================


-- Generic key-value store for browser configuration files
CREATE TABLE IF NOT EXISTS browser_config (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    browser TEXT NOT NULL,
    profile TEXT,
    config_type TEXT NOT NULL,
    config_key TEXT NOT NULL,
    config_value TEXT,
    value_count INTEGER DEFAULT 1,
    source_path TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_browser_config_browser ON browser_config(browser);
CREATE INDEX IF NOT EXISTS idx_browser_config_evidence ON browser_config(evidence_id);
CREATE INDEX IF NOT EXISTS idx_browser_config_key ON browser_config(config_key);
CREATE INDEX IF NOT EXISTS idx_browser_config_run ON browser_config(run_id);
CREATE INDEX IF NOT EXISTS idx_browser_config_type ON browser_config(config_type);


-- Tor state file data
CREATE TABLE IF NOT EXISTS tor_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    run_id TEXT NOT NULL,
    profile TEXT,
    state_key TEXT NOT NULL,
    state_value TEXT,
    timestamp_utc TEXT,
    source_path TEXT NOT NULL,
    partition_index INTEGER,
    fs_type TEXT,
    logical_path TEXT,
    forensic_path TEXT,
    notes TEXT,
    created_at_utc TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_tor_state_evidence ON tor_state(evidence_id);
CREATE INDEX IF NOT EXISTS idx_tor_state_key ON tor_state(state_key);
CREATE INDEX IF NOT EXISTS idx_tor_state_run ON tor_state(run_id);

-- ============================================================================
-- Firefox Cache Index
-- ============================================================================


-- Metadata-only records from Firefox cache2/index binary file
CREATE TABLE IF NOT EXISTS firefox_cache_index (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    evidence_id TEXT NOT NULL,
    partition_index INTEGER DEFAULT 0,
    source_path TEXT NOT NULL,
    entry_hash TEXT NOT NULL,
    frecency INTEGER,
    origin_attrs_hash INTEGER,
    on_start_time INTEGER,
    on_stop_time INTEGER,
    content_type INTEGER,
    content_type_name TEXT,
    file_size_kb INTEGER,
    raw_flags INTEGER,
    is_initialized BOOLEAN DEFAULT 0,
    is_anonymous BOOLEAN DEFAULT 0,
    is_removed BOOLEAN DEFAULT 0,
    is_pinned BOOLEAN DEFAULT 0,
    has_alt_data BOOLEAN DEFAULT 0,
    index_version INTEGER,
    index_timestamp INTEGER,
    index_dirty BOOLEAN DEFAULT 0,
    has_entry_file BOOLEAN DEFAULT 0,
    entry_source TEXT,
    url TEXT,
    browser TEXT DEFAULT 'firefox',
    profile_path TEXT,
    os_user TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_firefox_cache_index_content_type
    ON firefox_cache_index(content_type);
CREATE INDEX IF NOT EXISTS idx_firefox_cache_index_hash
    ON firefox_cache_index(entry_hash);
CREATE INDEX IF NOT EXISTS idx_firefox_cache_index_removed
    ON firefox_cache_index(is_removed);
CREATE INDEX IF NOT EXISTS idx_firefox_cache_index_run
    ON firefox_cache_index(run_id);
CREATE INDEX IF NOT EXISTS idx_firefox_cache_index_url
    ON firefox_cache_index(url);

-- ============================================================================
-- Screenshots
-- ============================================================================


-- Investigator-captured documentation images
CREATE TABLE IF NOT EXISTS screenshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_id INTEGER NOT NULL,
    captured_url TEXT,
    dest_path TEXT NOT NULL,
    filename TEXT NOT NULL,
    size_bytes INTEGER,
    width INTEGER,
    height INTEGER,
    md5 TEXT,
    sha256 TEXT,
    title TEXT,
    caption TEXT,
    notes TEXT,
    sequence_name TEXT,
    sequence_order INTEGER DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'sandbox',
    captured_at_utc TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT
);

CREATE INDEX IF NOT EXISTS idx_screenshots_captured ON screenshots(captured_at_utc);
CREATE INDEX IF NOT EXISTS idx_screenshots_evidence ON screenshots(evidence_id);
CREATE INDEX IF NOT EXISTS idx_screenshots_sequence ON screenshots(evidence_id, sequence_name, sequence_order);

