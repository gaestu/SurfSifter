"""
Database helper functions package.

This package provides domain-specific CRUD operations for all artifact tables.

Exports:
- Generic CRUD: insert_rows, insert_row, get_rows, delete_by_run, get_distinct_values, get_count
- Statistics: upsert_extractor_statistics, get_extractor_statistics_*
- Browser History: insert_browser_history*, get_browser_history*, delete_browser_history_by_run
- Cookies: insert_cookie*, get_cookies, get_cookie_*, delete_cookies_by_run
- Bookmarks: insert_bookmark*, get_bookmarks, delete_bookmarks_by_run
- Browser Downloads: insert_browser_download*, get_browser_downloads, delete_browser_downloads_by_run
- Images: insert_image*, get_image*, delete_images_by_run
- URLs: insert_url*, get_urls, delete_urls_by_run
- Timeline: insert_timeline_*, get_timeline_*, delete_timeline_by_run
- Process Log: insert_process_log, get_process_logs
- Artifacts (bulk_extractor): insert_bitcoins, insert_emails, insert_ips, etc.
- Autofill: insert_autofill_*, get_autofill_*, credentials, credit cards
- Sessions: insert_session_*, get_session_*, closed_tabs
- Permissions: insert_permission*, get_permissions
- Media: insert_media_*, get_media_*
- HSTS: insert_hsts_*, get_hsts_*
- Jump Lists: insert_jump_list_*, get_jump_list_*
- Extensions: insert_extension*, get_extensions
- Storage: local_storage, session_storage, indexeddb, tokens, identifiers
- Sync Data: insert_sync_data*, get_sync_data*
- Favicons: insert_favicon*, get_favicons*, favicon_mappings, top_sites
- Browser Inventory: get_browser_inventory
- Batch Operations: get_evidence_table_counts, purge_evidence_data
- OS Indicators: insert_os_indicator*, get_os_indicators*, platform_detections
- Hash Matches: insert_hash_match*, get_hash_matches*, url_matches
- Report Sections v2: list_report_sections_v2, create/update/delete sections and modules

Reorganized from monolithic db.py into domain-specific modules.
Added report_sections module for v2 report sections CRUD.
"""
from .generic import (
    delete_by_run,
    get_count,
    get_distinct_values,
    get_rows,
    insert_row,
    insert_rows,
)
from .statistics import (
    delete_extractor_statistics_by_evidence,
    delete_extractor_statistics_by_run,
    get_extractor_statistics_by_evidence,
    get_extractor_statistics_by_name,
    upsert_extractor_statistics,
    sync_process_log_from_statistics,
)

# Browser History
from .browser_history import (
    insert_browser_history,
    insert_browser_history_row,
    insert_browser_history_rows,
    get_browser_history,
    get_browser_history_by_id,
    get_distinct_history_browsers,
    get_distinct_history_profiles,
    get_browser_history_stats,
    delete_browser_history_by_run,
)

# Browser Search Terms
from .browser_search_terms import (
    insert_search_term,
    insert_search_terms,
    get_search_terms,
    get_search_term_by_id,
    get_search_terms_stats,
    delete_search_terms_by_run,
)

# Cookies
from .cookies import (
    insert_cookie_row,
    insert_cookies,
    get_cookies,
    get_cookie_domains,
    get_cookie_by_id,
    get_distinct_cookie_browsers,
    delete_cookies_by_run,
)

# Bookmarks
from .bookmarks import (
    insert_bookmark_row,
    insert_bookmarks,
    get_bookmarks,
    get_bookmark_folders,
    delete_bookmarks_by_run,
    get_distinct_bookmark_browsers,
)

# Browser Downloads
from .browser_downloads import (
    insert_browser_download_row,
    insert_browser_downloads,
    get_browser_downloads,
    get_browser_download_stats,
    delete_browser_downloads_by_run,
    get_distinct_download_browsers,
)

# Investigator Downloads
from .downloads import (
    insert_download,
    update_download_status,
    update_download_image_metadata,
    get_download,
    get_download_by_path,
    get_downloads,
    get_download_count,
    get_download_stats,
    get_download_domains,
    get_url_download_status,
    find_url_by_filename_domain,
)

# Download Audit
from .download_audit import (
    insert_download_audit,
    get_download_audit,
    get_download_audit_count,
    get_download_audit_summary,
)

# Images
from .images import (
    insert_image,
    insert_images,
    get_images,
    get_image_by_id,
    get_image_by_md5,
    get_image_by_sha256,
    get_image_stats,
    delete_images_by_run,
    update_image_tags,
    update_image_notes,
    insert_image_discovery,
    get_image_discoveries,
    insert_image_with_discovery,
    get_image_sources,
    get_image_fs_path,
    delete_discoveries_by_run,
)

# URLs
from .urls import (
    insert_url_row,
    insert_urls,
    get_urls,
    get_url_by_id,
    get_url_stats,
    delete_urls_by_run,
    insert_url_groups,
    get_url_groups,
    # Deduplication
    analyze_url_duplicates,
    deduplicate_urls,
)

# URL Tags
from .url_tags import (
    insert_url_tags,
    get_url_tags,
)

# Tags (generic)
from .tags import (
    get_all_tags,
    get_tag_by_name,
    get_tag_by_id,
    insert_tag,
    get_or_create_tag,
    update_tag_name,
    delete_tag,
    insert_tag_association,
    delete_tag_association,
    get_artifact_tags,
    get_artifact_tags_str,
    get_tag_strings_for_artifacts,
    get_artifacts_by_tag_id,
    merge_tag_associations,
    query_artifacts_by_tags,
    query_all_tagged_artifacts,
)

# Timeline
from .timeline import (
    insert_timeline_event,
    insert_timeline_events,
    get_timeline_events,
    get_timeline_stats,
    delete_timeline_by_run,
    clear_timeline,
    iter_timeline,
    get_timeline_kinds,
    get_timeline_confidences,
)

# Process Log
from .process_log import (
    insert_process_log,
    get_process_logs,
    create_process_log,
    finalize_process_log,
)

# Artifacts (bulk_extractor output)
from .artifacts import (
    insert_bitcoins,
    insert_bitcoin_addresses,
    get_bitcoins,
    get_bitcoin_addresses,
    delete_bitcoins_by_run,
    insert_ethereums,
    insert_ethereum_addresses,
    get_ethereums,
    get_ethereum_addresses,
    delete_ethereums_by_run,
    insert_emails,
    get_emails,
    delete_emails_by_run,
    insert_domains,
    get_domains,
    delete_domains_by_run,
    insert_ips,
    insert_ip_addresses,
    get_ips,
    get_ip_addresses,
    delete_ips_by_run,
    insert_phones,
    insert_telephone_numbers,
    get_phones,
    get_telephone_numbers,
    delete_phones_by_run,
)

# Autofill
from .autofill import (
    insert_autofill_entry,
    insert_autofill_entries,
    insert_autofill,
    get_autofill_entries,
    get_autofill,
    delete_autofill_by_run,
    insert_autofill_profile,
    insert_autofill_profiles,
    get_autofill_profiles,
    delete_autofill_profiles_by_run,
    insert_credential,
    insert_credentials,
    get_credentials,
    delete_credentials_by_run,
    insert_credit_card,
    insert_credit_cards,
    get_credit_cards,
    delete_credit_cards_by_run,
)

# Autofill IBANs (Chromium/Edge)
from .autofill_ibans import (
    insert_autofill_iban,
    insert_autofill_ibans,
    get_autofill_ibans,
    get_distinct_autofill_iban_browsers,
    delete_autofill_ibans_by_run,
)

# Autofill Profile Tokens (Chromium 100+)
from .autofill_profile_tokens import (
    CHROMIUM_TOKEN_TYPES,
    get_token_type_name,
    insert_autofill_profile_token,
    insert_autofill_profile_tokens,
    get_autofill_profile_tokens,
    delete_autofill_profile_tokens_by_run,
)

# Autofill Block List (Edge)
from .autofill_block_list import (
    BLOCK_TYPE_URL,
    BLOCK_TYPE_DOMAIN,
    BLOCK_TYPE_FIELD_SPECIFIC,
    BLOCK_TYPE_PATTERN,
    BLOCK_TYPE_NAMES,
    get_block_type_name,
    insert_autofill_block_list_entry,
    insert_autofill_block_list_entries,
    get_autofill_block_list,
    delete_autofill_block_list_by_run,
)

# Deleted Form History (Firefox)
from .deleted_form_history import (
    insert_deleted_form_history,
    insert_deleted_form_history_entries,
    get_deleted_form_history,
    get_distinct_deleted_form_history_browsers,
    delete_deleted_form_history_by_run,
)

# Search Engines
from .search_engines import (
    insert_search_engine,
    insert_search_engines,
    get_search_engines,
    delete_search_engines_by_run,
)

# Sessions
from .sessions import (
    insert_session_window,
    insert_session_windows,
    get_session_windows,
    get_session_window_by_id,
    delete_session_windows_by_run,
    insert_session_tab,
    insert_session_tabs,
    get_session_tabs,
    delete_session_tabs_by_run,
    insert_session_tab_history,
    insert_session_tab_histories,
    get_session_tab_history,
    delete_session_tab_history_by_run,
    insert_closed_tab,
    insert_closed_tabs,
    get_closed_tabs,
    delete_closed_tabs_by_run,
    insert_session_form_data,
    insert_session_form_datas,
    get_session_form_data,
    delete_session_form_data_by_run,
    delete_sessions_by_run,
)

# Permissions
from .permissions import (
    insert_permission,
    insert_permissions,
    insert_site_permissions,
    get_permissions,
    get_site_permissions,
    get_distinct_permission_types,
    delete_permissions_by_run,
)

# Media
from .media import (
    insert_media_playback,
    insert_media_playbacks,
    get_media_playback,
    delete_media_playback_by_run,
    insert_media_session,
    insert_media_sessions,
    get_media_sessions,
    delete_media_sessions_by_run,
    # Media origins
    insert_media_origin,
    insert_media_origins,
    get_media_origins,
    delete_media_origins_by_run,
    # Stats
    get_media_stats,
    delete_media_by_run,
)

# HSTS
from .hsts import (
    insert_hsts_entry,
    insert_hsts_entries,
    get_hsts_entries,
    get_hsts_stats,
    delete_hsts_by_run,
)

# Jump Lists
from .jump_lists import (
    insert_jump_list_entry,
    insert_jump_list_entries,
    get_jump_list_entries,
    get_jump_list_stats,
    delete_jump_list_by_run,
    delete_jump_lists_by_run,
)

# Extensions
from .extensions import (
    insert_extension,
    insert_extensions,
    insert_browser_extension_row,
    insert_browser_extensions,
    get_extensions,
    get_browser_extensions,
    get_extension_stats,
    get_browser_extension_stats,
    delete_extensions_by_run,
    delete_browser_extensions_by_run,
)

# Storage
from .storage import (
    insert_local_storage,
    insert_local_storages,
    get_local_storage,
    get_local_storage_origins,
    delete_local_storage_by_run,
    insert_session_storage,
    insert_session_storages,
    get_session_storage,
    delete_session_storage_by_run,
    insert_indexeddb_database,
    insert_indexeddb_databases,
    get_indexeddb_databases,
    delete_indexeddb_databases_by_run,
    insert_indexeddb_entry,
    insert_indexeddb_entries,
    get_indexeddb_entries,
    delete_indexeddb_entries_by_run,
    insert_storage_token,
    insert_storage_tokens,
    get_storage_tokens,
    get_storage_token_stats,
    delete_storage_tokens_by_run,
    insert_storage_identifier,
    insert_storage_identifiers,
    get_storage_identifiers,
    get_storage_identifier_stats,
    delete_storage_identifiers_by_run,
    get_stored_sites_summary,
)

# Site Engagement
from .site_engagement import (
    insert_site_engagement,
    insert_site_engagements,
    get_site_engagements,
    get_site_engagement_stats,
    delete_site_engagement_by_run,
    delete_site_engagement_by_evidence,
    get_top_engaged_sites,
)

# Stored Sites (materialized view for tagging)
from .stored_sites import (
    insert_stored_site,
    upsert_stored_site,
    get_stored_sites,
    get_stored_site_by_id,
    get_stored_site_by_origin,
    refresh_stored_sites,
    delete_stored_sites_by_evidence,
    get_stored_sites_for_report,
)

# Sync Data
from .sync_data import (
    insert_sync_data,
    insert_sync_datas,
    insert_sync_data_row,
    get_sync_data,
    delete_sync_data_by_run,
    insert_synced_device,
    insert_synced_devices,
    insert_synced_device_row,
    get_synced_devices,
    delete_synced_devices_by_run,
    get_sync_stats,
)

# Browser Config
from .browser_config import (
    insert_browser_config,
    insert_browser_configs,
    get_browser_configs,
    get_browser_config_keys,
    delete_browser_config_by_run,
    insert_tor_state,
    insert_tor_states,
    get_tor_states,
    delete_tor_state_by_run,
)

# Firefox Cache Index
from .firefox_cache_index import (
    insert_firefox_cache_index_entry,
    insert_firefox_cache_index_entries,
    get_firefox_cache_index_entries,
    get_firefox_cache_index_count,
    get_firefox_cache_index_stats,
    delete_firefox_cache_index_by_run,
)

# Favicons
from .favicons import (
    insert_favicon,
    insert_favicons,
    get_favicons,
    get_favicon_by_hash,
    get_favicon_by_id,
    delete_favicons_by_run,
    insert_favicon_mapping,
    insert_favicon_mappings,
    get_favicon_mappings,
    delete_favicon_mappings_by_run,
    insert_top_site,
    insert_top_sites,
    get_top_sites,
    get_top_sites_stats,
    delete_top_sites_by_run,
    get_favicon_stats,
)

# Browser Inventory
from .browser_inventory import (
    insert_browser_inventory,
    update_inventory_ingestion_status,
    get_browser_inventory,
)

# Batch Operations
from .batch import (
    get_evidence_table_counts,
    purge_evidence_data,
    PURGEABLE_TABLES,
)

# OS Indicators
from .os_indicators import (
    insert_os_indicator,
    insert_os_indicators,
    get_os_indicators,
    delete_os_indicators_by_run,
    insert_platform_detection,
    insert_platform_detections,
    get_platform_detections,
    delete_platform_detections_by_run,
)

# Hash Matches
from .hash_matches import (
    insert_hash_match,
    insert_hash_matches,
    get_hash_matches,
    delete_hash_matches_by_run,
    insert_url_match,
    insert_url_matches,
    get_url_matches,
    delete_url_matches_by_run,
)

# Extracted Files
from .extracted_files import (
    insert_extracted_file,
    insert_extracted_files,
    insert_extracted_files_batch,
    get_extracted_files,
    get_extracted_file_by_id,
    get_extracted_file_by_sha256,
    get_extraction_stats,
    get_distinct_extractors,
    get_distinct_run_ids,
    delete_extracted_files_by_run,
    delete_extracted_files_by_extractor,
)

# Screenshots
from .screenshots import (
    insert_screenshot,
    update_screenshot,
    delete_screenshot,
    get_screenshot,
    get_screenshots,
    get_screenshot_count,
    get_sequences,
    reorder_sequence,
    get_screenshot_stats,
)

# Extraction Warnings
from .extraction_warnings import (
    # Constants
    WARNING_TYPE_UNKNOWN_TABLE,
    WARNING_TYPE_UNKNOWN_COLUMN,
    WARNING_TYPE_UNKNOWN_TOKEN_TYPE,
    WARNING_TYPE_UNKNOWN_ENUM_VALUE,
    WARNING_TYPE_SCHEMA_MISMATCH,
    WARNING_TYPE_JSON_PARSE_ERROR,
    WARNING_TYPE_JSON_UNKNOWN_KEY,
    WARNING_TYPE_FILE_CORRUPT,
    CATEGORY_DATABASE,
    CATEGORY_JSON,
    CATEGORY_LEVELDB,
    CATEGORY_BINARY,
    CATEGORY_PLIST,
    CATEGORY_REGISTRY,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    SEVERITY_ERROR,
    # CRUD functions
    insert_extraction_warning,
    insert_extraction_warnings,
    get_extraction_warnings,
    get_extraction_warnings_summary,
    delete_extraction_warnings_by_run,
    delete_extraction_warnings_by_extractor,
    get_warning_count_for_extractor,
)

__all__ = [
    # Generic CRUD
    "delete_by_run",
    "get_count",
    "get_distinct_values",
    "get_rows",
    "insert_row",
    "insert_rows",
    # Statistics
    "delete_extractor_statistics_by_evidence",
    "delete_extractor_statistics_by_run",
    "get_extractor_statistics_by_evidence",
    "get_extractor_statistics_by_name",
    "upsert_extractor_statistics",
    "sync_process_log_from_statistics",
    # Browser History
    "insert_browser_history",
    "insert_browser_history_row",
    "insert_browser_history_rows",
    "get_browser_history",
    "get_browser_history_by_id",
    "get_distinct_history_browsers",
    "get_distinct_history_profiles",
    "get_browser_history_stats",
    "delete_browser_history_by_run",
    # Browser Search Terms
    "insert_search_term",
    "insert_search_terms",
    "get_search_terms",
    "get_search_term_by_id",
    "get_search_terms_stats",
    "delete_search_terms_by_run",
    # Cookies
    "insert_cookie_row",
    "insert_cookies",
    "get_cookies",
    "get_cookie_domains",
    "get_cookie_by_id",
    "get_distinct_cookie_browsers",
    "delete_cookies_by_run",
    # Bookmarks
    "insert_bookmark_row",
    "insert_bookmarks",
    "get_bookmarks",
    "get_bookmark_folders",
    "delete_bookmarks_by_run",
    "get_distinct_bookmark_browsers",
    # Browser Downloads
    "insert_browser_download_row",
    "insert_browser_downloads",
    "get_browser_downloads",
    "get_browser_download_stats",
    "delete_browser_downloads_by_run",
    "get_distinct_download_browsers",
    # Investigator Downloads
    "insert_download",
    "update_download_status",
    "update_download_image_metadata",
    "get_download",
    "get_download_by_path",
    "get_downloads",
    "get_download_count",
    "get_download_stats",
    "get_download_domains",
    "get_url_download_status",
    "find_url_by_filename_domain",
    # Download Audit
    "insert_download_audit",
    "get_download_audit",
    "get_download_audit_count",
    "get_download_audit_summary",
    # Images
    "insert_image",
    "insert_images",
    "get_images",
    "get_image_by_id",
    "get_image_by_md5",
    "get_image_by_sha256",
    "get_image_stats",
    "delete_images_by_run",
    "update_image_tags",
    "update_image_notes",
    "insert_image_discovery",
    "get_image_discoveries",
    "insert_image_with_discovery",
    "get_image_sources",
    "get_image_fs_path",
    "delete_discoveries_by_run",
    # URLs
    "insert_url_row",
    "insert_urls",
    "get_urls",
    "get_url_by_id",
    "get_url_stats",
    "delete_urls_by_run",
    "insert_url_groups",
    "get_url_groups",
    # Deduplication
    "analyze_url_duplicates",
    "deduplicate_urls",
    # URL Tags
    "insert_url_tags",
    "get_url_tags",
    # Tags (generic)
    "get_all_tags",
    "get_tag_by_name",
    "get_tag_by_id",
    "insert_tag",
    "get_or_create_tag",
    "update_tag_name",
    "delete_tag",
    "insert_tag_association",
    "delete_tag_association",
    "get_artifact_tags",
    "get_artifact_tags_str",
    "get_tag_strings_for_artifacts",
    "get_artifacts_by_tag_id",
    "merge_tag_associations",
    "query_artifacts_by_tags",
    "query_all_tagged_artifacts",
    # Timeline
    "insert_timeline_event",
    "insert_timeline_events",
    "get_timeline_events",
    "get_timeline_stats",
    "delete_timeline_by_run",
    "clear_timeline",
    "iter_timeline",
    "get_timeline_kinds",
    "get_timeline_confidences",
    # Process Log
    "insert_process_log",
    "get_process_logs",
    "create_process_log",
    "finalize_process_log",
    # Artifacts (bulk_extractor)
    "insert_bitcoins",
    "insert_bitcoin_addresses",
    "get_bitcoins",
    "get_bitcoin_addresses",
    "delete_bitcoins_by_run",
    "insert_ethereums",
    "insert_ethereum_addresses",
    "get_ethereums",
    "get_ethereum_addresses",
    "delete_ethereums_by_run",
    "insert_emails",
    "get_emails",
    "delete_emails_by_run",
    "insert_domains",
    "get_domains",
    "delete_domains_by_run",
    "insert_ips",
    "insert_ip_addresses",
    "get_ips",
    "get_ip_addresses",
    "delete_ips_by_run",
    "insert_phones",
    "insert_telephone_numbers",
    "get_phones",
    "get_telephone_numbers",
    "delete_phones_by_run",
    # Autofill
    "insert_autofill_entry",
    "insert_autofill_entries",
    "insert_autofill",
    "get_autofill_entries",
    "get_autofill",
    "delete_autofill_by_run",
    "insert_autofill_profile",
    "insert_autofill_profiles",
    "get_autofill_profiles",
    "delete_autofill_profiles_by_run",
    "insert_credential",
    "insert_credentials",
    "get_credentials",
    "delete_credentials_by_run",
    "insert_credit_card",
    "insert_credit_cards",
    "get_credit_cards",
    "delete_credit_cards_by_run",
    # Autofill IBANs (Chromium/Edge)
    "insert_autofill_iban",
    "insert_autofill_ibans",
    "get_autofill_ibans",
    "get_distinct_autofill_iban_browsers",
    "delete_autofill_ibans_by_run",
    # Autofill Profile Tokens (Chromium 100+)
    "CHROMIUM_TOKEN_TYPES",
    "get_token_type_name",
    "insert_autofill_profile_token",
    "insert_autofill_profile_tokens",
    "get_autofill_profile_tokens",
    "delete_autofill_profile_tokens_by_run",
    # Autofill Block List (Edge)
    "BLOCK_TYPE_URL",
    "BLOCK_TYPE_DOMAIN",
    "BLOCK_TYPE_FIELD_SPECIFIC",
    "BLOCK_TYPE_PATTERN",
    "BLOCK_TYPE_NAMES",
    "get_block_type_name",
    "insert_autofill_block_list_entry",
    "insert_autofill_block_list_entries",
    "get_autofill_block_list",
    "delete_autofill_block_list_by_run",
    # Deleted Form History (Firefox)
    "insert_deleted_form_history",
    "insert_deleted_form_history_entries",
    "get_deleted_form_history",
    "get_distinct_deleted_form_history_browsers",
    "delete_deleted_form_history_by_run",
    # Search Engines
    "insert_search_engine",
    "insert_search_engines",
    "get_search_engines",
    "delete_search_engines_by_run",
    # Sessions
    "insert_session_window",
    "insert_session_windows",
    "get_session_windows",
    "delete_session_windows_by_run",
    "insert_session_tab",
    "insert_session_tabs",
    "get_session_tabs",
    "delete_session_tabs_by_run",
    "insert_session_tab_history",
    "insert_session_tab_histories",
    "get_session_tab_history",
    "delete_session_tab_history_by_run",
    "insert_closed_tab",
    "insert_closed_tabs",
    "get_closed_tabs",
    "delete_closed_tabs_by_run",
    "insert_session_form_data",
    "insert_session_form_datas",
    "get_session_form_data",
    "delete_session_form_data_by_run",
    "delete_sessions_by_run",
    # Permissions
    "insert_permission",
    "insert_permissions",
    "insert_site_permissions",
    "get_permissions",
    "get_site_permissions",
    "get_distinct_permission_types",
    "delete_permissions_by_run",
    # Media
    "insert_media_playback",
    "insert_media_playbacks",
    "get_media_playback",
    "delete_media_playback_by_run",
    "insert_media_session",
    "insert_media_sessions",
    "get_media_sessions",
    "delete_media_sessions_by_run",
    # Media origins
    "insert_media_origin",
    "insert_media_origins",
    "get_media_origins",
    "delete_media_origins_by_run",
    # Media stats
    "get_media_stats",
    "delete_media_by_run",
    # HSTS
    "insert_hsts_entry",
    "insert_hsts_entries",
    "get_hsts_entries",
    "get_hsts_stats",
    "delete_hsts_by_run",
    # Jump Lists
    "insert_jump_list_entry",
    "insert_jump_list_entries",
    "get_jump_list_entries",
    "get_jump_list_stats",
    "delete_jump_list_by_run",
    "delete_jump_lists_by_run",
    # Extensions
    "insert_extension",
    "insert_extensions",
    "insert_browser_extension_row",
    "insert_browser_extensions",
    "get_extensions",
    "get_browser_extensions",
    "get_extension_stats",
    "get_browser_extension_stats",
    "delete_extensions_by_run",
    "delete_browser_extensions_by_run",
    # Storage
    "insert_local_storage",
    "insert_local_storages",
    "get_local_storage",
    "get_local_storage_origins",
    "delete_local_storage_by_run",
    "insert_session_storage",
    "insert_session_storages",
    "get_session_storage",
    "delete_session_storage_by_run",
    "insert_indexeddb_database",
    "insert_indexeddb_databases",
    "get_indexeddb_databases",
    "delete_indexeddb_databases_by_run",
    "insert_indexeddb_entry",
    "insert_indexeddb_entries",
    "get_indexeddb_entries",
    "delete_indexeddb_entries_by_run",
    "insert_storage_token",
    "insert_storage_tokens",
    "get_storage_tokens",
    "get_storage_token_stats",
    "delete_storage_tokens_by_run",
    "insert_storage_identifier",
    "insert_storage_identifiers",
    "get_storage_identifiers",
    "get_storage_identifier_stats",
    "delete_storage_identifiers_by_run",
    "get_stored_sites_summary",
    # Site Engagement
    "insert_site_engagement",
    "insert_site_engagements",
    "get_site_engagements",
    "get_site_engagement_stats",
    "delete_site_engagement_by_run",
    "delete_site_engagement_by_evidence",
    "get_top_engaged_sites",
    # Stored Sites (materialized view for tagging)
    "insert_stored_site",
    "upsert_stored_site",
    "get_stored_sites",
    "get_stored_site_by_id",
    "get_stored_site_by_origin",
    "refresh_stored_sites",
    "delete_stored_sites_by_evidence",
    "get_stored_sites_for_report",
    # Sync Data
    "insert_sync_data",
    "insert_sync_datas",
    "insert_sync_data_row",
    "get_sync_data",
    "delete_sync_data_by_run",
    "insert_synced_device",
    "insert_synced_devices",
    "insert_synced_device_row",
    "get_synced_devices",
    "delete_synced_devices_by_run",
    "get_sync_stats",
    # Favicons
    "insert_favicon",
    "insert_favicons",
    "get_favicons",
    "get_favicon_by_hash",
    "get_favicon_by_id",
    "delete_favicons_by_run",
    "insert_favicon_mapping",
    "insert_favicon_mappings",
    "get_favicon_mappings",
    "delete_favicon_mappings_by_run",
    "insert_top_site",
    "insert_top_sites",
    "get_top_sites",
    "get_top_sites_stats",
    "delete_top_sites_by_run",
    "get_favicon_stats",
    # Browser Inventory
    "insert_browser_inventory",
    "update_inventory_ingestion_status",
    "get_browser_inventory",
    # Batch Operations
    "get_evidence_table_counts",
    "purge_evidence_data",
    "PURGEABLE_TABLES",
    # OS Indicators
    "insert_os_indicator",
    "insert_os_indicators",
    "get_os_indicators",
    "delete_os_indicators_by_run",
    "insert_platform_detection",
    "insert_platform_detections",
    "get_platform_detections",
    "delete_platform_detections_by_run",
    # Hash Matches
    "insert_hash_match",
    "insert_hash_matches",
    "get_hash_matches",
    "delete_hash_matches_by_run",
    "insert_url_match",
    "insert_url_matches",
    "get_url_matches",
    "delete_url_matches_by_run",
    # Extracted Files
    "insert_extracted_file",
    "insert_extracted_files",
    "insert_extracted_files_batch",
    "get_extracted_files",
    "get_extracted_file_by_id",
    "get_extracted_file_by_sha256",
    "get_extraction_stats",
    "get_distinct_extractors",
    "get_distinct_run_ids",
    "delete_extracted_files_by_run",
    "delete_extracted_files_by_extractor",
    # Screenshots
    "insert_screenshot",
    "update_screenshot",
    "delete_screenshot",
    "get_screenshot",
    "get_screenshots",
    "get_screenshot_count",
    "get_sequences",
    "reorder_sequence",
    "get_screenshot_stats",
    # Extraction Warnings
    "WARNING_TYPE_UNKNOWN_TABLE",
    "WARNING_TYPE_UNKNOWN_COLUMN",
    "WARNING_TYPE_UNKNOWN_TOKEN_TYPE",
    "WARNING_TYPE_UNKNOWN_ENUM_VALUE",
    "WARNING_TYPE_SCHEMA_MISMATCH",
    "WARNING_TYPE_JSON_PARSE_ERROR",
    "WARNING_TYPE_JSON_UNKNOWN_KEY",
    "WARNING_TYPE_FILE_CORRUPT",
    "CATEGORY_DATABASE",
    "CATEGORY_JSON",
    "CATEGORY_LEVELDB",
    "CATEGORY_BINARY",
    "CATEGORY_PLIST",
    "CATEGORY_REGISTRY",
    "SEVERITY_INFO",
    "SEVERITY_WARNING",
    "SEVERITY_ERROR",
    "insert_extraction_warning",
    "insert_extraction_warnings",
    "get_extraction_warnings",
    "get_extraction_warnings_summary",
    "delete_extraction_warnings_by_run",
    "delete_extraction_warnings_by_extractor",
    "get_warning_count_for_extractor",
    # Browser Config
    "insert_browser_config",
    "insert_browser_configs",
    "get_browser_configs",
    "get_browser_config_keys",
    "delete_browser_config_by_run",
    "insert_tor_state",
    "insert_tor_states",
    "get_tor_states",
    "delete_tor_state_by_run",
    # Firefox Cache Index
    "insert_firefox_cache_index_entry",
    "insert_firefox_cache_index_entries",
    "get_firefox_cache_index_entries",
    "get_firefox_cache_index_count",
    "get_firefox_cache_index_stats",
    "delete_firefox_cache_index_by_run",
]
