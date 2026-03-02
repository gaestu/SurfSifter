"""
Table schema definitions for all artifact tables.

This module contains all TableSchema instances for database tables.
Pre-insert hooks for data transformation are defined here.

Extracted from db_schema.py during database refactor.
"""
from __future__ import annotations

from typing import Any, Dict, List

from .base import (
    Column,
    ConflictAction,
    FilterColumn,
    FilterOp,
    OrderColumn,
    TableSchema,
)

# Re-export base types for backward compatibility
__all__ = [
    # Base types (re-exported)
    "Column",
    "ConflictAction",
    "FilterColumn",
    "FilterOp",
    "OrderColumn",
    "TableSchema",
    # Schema instances
    "TABLE_SCHEMAS",
    "COOKIES_SCHEMA",
    "BOOKMARKS_SCHEMA",
    "BROWSER_DOWNLOADS_SCHEMA",
    "URLS_SCHEMA",
    "BROWSER_HISTORY_SCHEMA",
    "IMAGES_SCHEMA",
    "IMAGE_DISCOVERIES_SCHEMA",
    "URL_MATCHES_SCHEMA",
    "TAGS_SCHEMA",
    "TAG_ASSOCIATIONS_SCHEMA",
    "FAVICONS_SCHEMA",
    "FAVICON_MAPPINGS_SCHEMA",
    "TOP_SITES_SCHEMA",
    "BITCOIN_ADDRESSES_SCHEMA",
    "ETHEREUM_ADDRESSES_SCHEMA",
    "EMAILS_SCHEMA",
    "DOMAINS_SCHEMA",
    "IP_ADDRESSES_SCHEMA",
    "TELEPHONE_NUMBERS_SCHEMA",
    "AUTOFILL_SCHEMA",
    "AUTOFILL_PROFILES_SCHEMA",
    "AUTOFILL_IBANS_SCHEMA",
    "CREDENTIALS_SCHEMA",
    "CREDIT_CARDS_SCHEMA",
    "SEARCH_ENGINES_SCHEMA",
    "DELETED_FORM_HISTORY_SCHEMA",
    "AUTOFILL_BLOCK_LIST_SCHEMA",
    "AUTOFILL_PROFILE_TOKENS_SCHEMA",
    "SESSION_WINDOWS_SCHEMA",
    "SESSION_TABS_SCHEMA",
    "SESSION_TAB_HISTORY_SCHEMA",
    "CLOSED_TABS_SCHEMA",
    "SESSION_FORM_DATA_SCHEMA",
    "SITE_PERMISSIONS_SCHEMA",
    "MEDIA_PLAYBACK_SCHEMA",
    "MEDIA_SESSIONS_SCHEMA",
    "HSTS_ENTRIES_SCHEMA",
    "JUMP_LIST_ENTRIES_SCHEMA",
    "BROWSER_EXTENSIONS_SCHEMA",
    "LOCAL_STORAGE_SCHEMA",
    "SESSION_STORAGE_SCHEMA",
    "INDEXEDDB_DATABASES_SCHEMA",
    "INDEXEDDB_ENTRIES_SCHEMA",
    "SYNC_DATA_SCHEMA",
    "SYNCED_DEVICES_SCHEMA",
    "OS_INDICATORS_SCHEMA",
    "PLATFORM_DETECTIONS_SCHEMA",
    "TIMELINE_SCHEMA",
    "STORAGE_TOKENS_SCHEMA",
    "STORAGE_IDENTIFIERS_SCHEMA",
    "EXTRACTED_FILES_SCHEMA",
    "SCREENSHOTS_SCHEMA",
]


# =============================================================================
# Pre-insert hooks
# =============================================================================


def _images_pre_insert(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compute phash_prefix on insert and handle discovered_by → first_discovered_by transition.

    Also maps 'discovered_by' to 'first_discovered_by' for backward compatibility
    with callers still using the old key.
    """
    from core.phash import compute_phash_prefix
    from datetime import datetime, timezone

    updated = dict(record)
    updated["phash_prefix"] = compute_phash_prefix(updated.get("phash"))

    # Map discovered_by to first_discovered_by for backward compat
    if "first_discovered_by" not in updated:
        updated["first_discovered_by"] = updated.get("discovered_by", "unknown")
    if "first_discovered_at" not in updated:
        updated["first_discovered_at"] = datetime.now(timezone.utc).isoformat()

    return updated


def _platform_detections_pre_insert(record: Dict[str, Any]) -> Dict[str, Any]:
    import json

    updated = dict(record)
    if "matched_patterns_json" in updated:
        value = updated["matched_patterns_json"]
        if value is not None and not isinstance(value, str):
            updated["matched_patterns_json"] = json.dumps(value)
            return updated
        if value is not None:
            return updated

    matched = updated.get("matched_patterns", {})
    updated["matched_patterns_json"] = json.dumps(matched)
    return updated


def _coerce_truthy(record: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
    updated = dict(record)
    for key in keys:
        updated[key] = 1 if updated.get(key) else 0
    return updated


def _hsts_pre_insert(record: Dict[str, Any]) -> Dict[str, Any]:
    return _coerce_truthy(record, ["include_subdomains"])


def _extensions_pre_insert(record: Dict[str, Any]) -> Dict[str, Any]:
    return _coerce_truthy(record, ["enabled"])


def _sync_pre_insert(record: Dict[str, Any]) -> Dict[str, Any]:
    return _coerce_truthy(record, ["sync_enabled"])


def _browser_history_pre_insert(record: Dict[str, Any]) -> Dict[str, Any]:
    updated = dict(record)
    if updated.get("ts_utc") is None and updated.get("visit_time_utc") is not None:
        updated["ts_utc"] = updated.get("visit_time_utc")
    return updated


def _tags_pre_insert(record: Dict[str, Any]) -> Dict[str, Any]:
    updated = dict(record)
    name = updated.get("name")
    if name and not updated.get("name_normalized"):
        updated["name_normalized"] = name.lower()
    return updated


COOKIES_SCHEMA = TableSchema(
    name="cookies",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("name", "TEXT", nullable=False),
        Column("value", "TEXT"),
        Column("domain", "TEXT", nullable=False),
        Column("path", "TEXT"),
        Column("expires_utc", "TEXT"),
        Column("is_secure", "INTEGER"),
        Column("is_httponly", "INTEGER"),
        Column("samesite", "TEXT"),
        Column("samesite_raw", "INTEGER"),  # Preserve original Firefox sameSite value
        Column("creation_utc", "TEXT"),
        Column("last_access_utc", "TEXT"),
        Column("encrypted", "INTEGER", default=0),
        Column("encrypted_value", "BLOB", exclude_from_select=True),
        # Firefox originAttributes: Container tabs, private browsing, FPI, state partitioning
        Column("origin_attributes", "TEXT"),  # Raw originAttributes string
        Column("user_context_id", "INTEGER"),  # Container tab ID (0=default, 1+=containers)
        Column("private_browsing_id", "INTEGER"),  # 0=normal, 1=private browsing
        Column("first_party_domain", "TEXT"),  # First-Party Isolation domain
        Column("partition_key", "TEXT"),  # State Partitioning key
        Column("run_id", "TEXT"),
        Column("source_path", "TEXT"),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["id", "browser", "domain", "name", "creation_utc", "last_access_utc"],
    default_order=[OrderColumn("domain", "ASC"), OrderColumn("name", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("domain", [FilterOp.EQ, FilterOp.LIKE]),
        FilterColumn("run_id", [FilterOp.EQ]),
        FilterColumn("user_context_id", [FilterOp.EQ]),  # Filter by container
        FilterColumn("private_browsing_id", [FilterOp.EQ]),  # Filter by private mode
    ],
    supports_run_delete=True,
)


BOOKMARKS_SCHEMA = TableSchema(
    name="bookmarks",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("url", "TEXT", nullable=False),
        Column("title", "TEXT"),
        Column("folder_path", "TEXT"),
        Column("bookmark_type", "TEXT", default="url"),
        Column("guid", "TEXT"),
        Column("date_added_utc", "TEXT"),
        Column("date_modified_utc", "TEXT"),
        Column("run_id", "TEXT"),
        Column("source_path", "TEXT"),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["id", "browser", "folder_path", "title", "date_added_utc"],
    default_order=[OrderColumn("folder_path", "ASC"), OrderColumn("title", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("folder_path", [FilterOp.EQ, FilterOp.LIKE]),
        FilterColumn("run_id", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


BROWSER_DOWNLOADS_SCHEMA = TableSchema(
    name="browser_downloads",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("url", "TEXT", nullable=False),
        Column("target_path", "TEXT"),
        Column("filename", "TEXT"),
        Column("start_time_utc", "TEXT"),
        Column("end_time_utc", "TEXT"),
        Column("total_bytes", "INTEGER"),
        Column("received_bytes", "INTEGER"),
        Column("mime_type", "TEXT"),
        Column("referrer", "TEXT"),
        Column("state", "TEXT"),
        Column("danger_type", "TEXT"),
        Column("opened", "INTEGER"),
        Column("run_id", "TEXT"),
        Column("source_path", "TEXT"),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["id", "browser", "filename", "start_time_utc", "end_time_utc", "state"],
    default_order=[OrderColumn("start_time_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("state", [FilterOp.EQ]),
        FilterColumn("filename", [FilterOp.LIKE]),
        FilterColumn("run_id", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


URLS_SCHEMA = TableSchema(
    name="urls",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("url", "TEXT", nullable=False),
        Column("domain", "TEXT"),
        Column("scheme", "TEXT"),
        Column("discovered_by", "TEXT", nullable=False),
        Column("first_seen_utc", "TEXT"),
        Column("last_seen_utc", "TEXT"),
        Column("source_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("context", "TEXT"),
        Column("run_id", "TEXT"),
        Column("cache_key", "TEXT"),
        Column("cache_filename", "TEXT"),
        Column("response_code", "INTEGER"),
        Column("content_type", "TEXT"),
        # Added in 0008_url_file_extension.sql
        Column("file_extension", "TEXT"),
        Column("file_type", "TEXT"),
        # Added in 0016_urls_occurrence_count.sql for deduplication tracking
        Column("occurrence_count", "INTEGER"),
    ],
    # Changed from IGNORE to FAIL to preserve all URL events with timestamps
    # for timeline reconstruction. Deduplication removed in 0015_urls_allow_duplicates.sql
    conflict_action=ConflictAction.FAIL,
)


BROWSER_HISTORY_SCHEMA = TableSchema(
    name="browser_history",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("url", "TEXT", nullable=False),
        Column("title", "TEXT"),
        Column("ts_utc", "TEXT"),
        Column("browser", "TEXT"),
        Column("profile", "TEXT"),
        Column("source_path", "TEXT"),
        Column("visit_count", "INTEGER"),
        Column("typed_count", "INTEGER"),
        Column("last_visit_time_utc", "TEXT"),
        Column("discovered_by", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("run_id", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        # Forensic visit metadata
        Column("transition_type", "INTEGER"),
        Column("transition_type_name", "TEXT"),  # Human-readable transition
        Column("from_visit", "INTEGER"),
        Column("visit_duration_ms", "INTEGER"),
        Column("hidden", "INTEGER"),
        Column("chromium_visit_id", "INTEGER"),
        Column("chromium_url_id", "INTEGER"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["id", "ts_utc", "browser", "profile", "url", "visit_count", "typed_count", "transition_type", "visit_duration_ms"],
    default_order=[OrderColumn("ts_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("profile", [FilterOp.EQ]),
        FilterColumn("url", [FilterOp.LIKE]),
        FilterColumn("run_id", [FilterOp.EQ]),
        FilterColumn("transition_type", [FilterOp.EQ]),
        FilterColumn("transition_type_name", [FilterOp.LIKE]),  #
        FilterColumn("hidden", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
    pre_insert_hook=_browser_history_pre_insert,
)


IMAGES_SCHEMA = TableSchema(
    name="images",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("rel_path", "TEXT", nullable=False),
        Column("filename", "TEXT", nullable=False),
        Column("md5", "TEXT"),
        Column("sha256", "TEXT"),
        Column("phash", "TEXT"),
        Column("phash_prefix", "INTEGER"),
        Column("exif_json", "TEXT"),
        Column("ts_utc", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("size_bytes", "INTEGER"),
        # Multi-source tracking - provenance now in image_discoveries
        Column("first_discovered_by", "TEXT", nullable=False),
        Column("first_discovered_at", "TEXT"),
    ],
    conflict_action=ConflictAction.IGNORE,
    supports_run_delete=False,  # Images cleaned via image_discoveries
    pre_insert_hook=_images_pre_insert,
)


IMAGE_DISCOVERIES_SCHEMA = TableSchema(
    name="image_discoveries",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("image_id", "INTEGER", nullable=False),
        Column("discovered_by", "TEXT", nullable=False),
        Column("extractor_version", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("discovered_at", "TEXT"),
        # Filesystem context
        Column("fs_path", "TEXT"),
        Column("fs_mtime_epoch", "REAL"),
        Column("fs_mtime", "TEXT"),
        Column("fs_atime_epoch", "REAL"),
        Column("fs_atime", "TEXT"),
        Column("fs_crtime_epoch", "REAL"),
        Column("fs_crtime", "TEXT"),
        Column("fs_ctime_epoch", "REAL"),
        Column("fs_ctime", "TEXT"),
        Column("fs_inode", "INTEGER"),
        # Carving context
        Column("carved_offset_bytes", "INTEGER"),
        Column("carved_block_size", "INTEGER"),
        Column("carved_tool_output", "TEXT"),
        # Cache context
        Column("cache_url", "TEXT"),
        Column("cache_key", "TEXT"),
        Column("cache_filename", "TEXT"),
        Column("cache_response_time", "TEXT"),
        Column("source_metadata_json", "TEXT"),
    ],
    conflict_action=ConflictAction.IGNORE,  # D2: UNIQUE allows multiple paths/offsets
    supports_run_delete=True,
    sortable_columns=["discovered_at", "discovered_by"],
    default_order=[OrderColumn("discovered_at", "DESC")],
    filterable_columns=[
        FilterColumn("discovered_by", [FilterOp.EQ, FilterOp.IN]),
    ],
)


URL_MATCHES_SCHEMA = TableSchema(
    name="url_matches",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("url_id", "INTEGER", nullable=False),
        Column("list_name", "TEXT", nullable=False),
        Column("match_type", "TEXT", nullable=False),
        Column("matched_pattern", "TEXT"),
        Column("created_at_utc", "TEXT"),
        Column("run_id", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["created_at_utc", "list_name"],
    default_order=[OrderColumn("created_at_utc", "DESC")],
    filterable_columns=[
        FilterColumn("list_name", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


TAGS_SCHEMA = TableSchema(
    name="tags",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("name", "TEXT", nullable=False),
        Column("name_normalized", "TEXT", nullable=False),
        Column("created_at_utc", "TEXT"),
        Column("created_by", "TEXT", nullable=False, default="manual"),
        Column("usage_count", "INTEGER", nullable=False, default=0),
    ],
    conflict_action=ConflictAction.IGNORE,
    supports_run_delete=False,
    pre_insert_hook=_tags_pre_insert,
)


TAG_ASSOCIATIONS_SCHEMA = TableSchema(
    name="tag_associations",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("tag_id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("artifact_type", "TEXT", nullable=False),
        Column("artifact_id", "INTEGER", nullable=False),
        Column("tagged_at_utc", "TEXT"),
        Column("tagged_by", "TEXT", nullable=False, default="manual"),
    ],
    conflict_action=ConflictAction.IGNORE,
    supports_run_delete=False,
)


FAVICONS_SCHEMA = TableSchema(
    name="favicons",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("icon_url", "TEXT", nullable=False),
        Column("icon_type", "INTEGER"),
        Column("width", "INTEGER"),
        Column("height", "INTEGER"),
        Column("icon_data", "BLOB", exclude_from_select=True),
        Column("icon_md5", "TEXT"),
        Column("icon_sha256", "TEXT"),
        Column("last_updated_utc", "TEXT"),
        Column("last_requested_utc", "TEXT"),
        Column("source_path", "TEXT", nullable=False),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.IGNORE,
    sortable_columns=["browser", "icon_url"],
    default_order=[OrderColumn("browser", "ASC"), OrderColumn("icon_url", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


FAVICON_MAPPINGS_SCHEMA = TableSchema(
    name="favicon_mappings",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("favicon_id", "INTEGER", nullable=False),
        Column("page_url", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    supports_run_delete=True,
)


TOP_SITES_SCHEMA = TableSchema(
    name="top_sites",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("url", "TEXT", nullable=False),
        Column("title", "TEXT"),
        Column("url_rank", "INTEGER"),
        Column("thumbnail_data", "BLOB", exclude_from_select=True),
        Column("thumbnail_width", "INTEGER", exclude_from_select=True),
        Column("thumbnail_height", "INTEGER", exclude_from_select=True),
        Column("source_path", "TEXT", nullable=False),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["url_rank", "browser", "url"],
    default_order=[OrderColumn("url_rank", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


BITCOIN_ADDRESSES_SCHEMA = TableSchema(
    name="bitcoin_addresses",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("address", "TEXT", nullable=False),
        Column("discovered_by", "TEXT", nullable=False),
        Column("first_seen_utc", "TEXT"),
        Column("last_seen_utc", "TEXT"),
        Column("source_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("context", "TEXT"),
        Column("run_id", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["first_seen_utc"],
    default_order=[OrderColumn("first_seen_utc", "DESC")],
    supports_run_delete=True,
)


ETHEREUM_ADDRESSES_SCHEMA = TableSchema(
    name="ethereum_addresses",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("address", "TEXT", nullable=False),
        Column("discovered_by", "TEXT", nullable=False),
        Column("first_seen_utc", "TEXT"),
        Column("last_seen_utc", "TEXT"),
        Column("source_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("context", "TEXT"),
        Column("run_id", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["first_seen_utc"],
    default_order=[OrderColumn("first_seen_utc", "DESC")],
    supports_run_delete=True,
)


EMAILS_SCHEMA = TableSchema(
    name="emails",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("email", "TEXT", nullable=False),
        Column("domain", "TEXT"),
        Column("discovered_by", "TEXT", nullable=False),
        Column("first_seen_utc", "TEXT"),
        Column("last_seen_utc", "TEXT"),
        Column("source_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("context", "TEXT"),
        Column("run_id", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["first_seen_utc"],
    default_order=[OrderColumn("first_seen_utc", "DESC")],
    supports_run_delete=True,
)


DOMAINS_SCHEMA = TableSchema(
    name="domains",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("domain", "TEXT", nullable=False),
        Column("discovered_by", "TEXT", nullable=False),
        Column("first_seen_utc", "TEXT"),
        Column("last_seen_utc", "TEXT"),
        Column("source_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("context", "TEXT"),
        Column("run_id", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["first_seen_utc"],
    default_order=[OrderColumn("first_seen_utc", "DESC")],
    supports_run_delete=True,
)


IP_ADDRESSES_SCHEMA = TableSchema(
    name="ip_addresses",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("ip_address", "TEXT", nullable=False),
        Column("ip_version", "TEXT"),
        Column("discovered_by", "TEXT", nullable=False),
        Column("first_seen_utc", "TEXT"),
        Column("last_seen_utc", "TEXT"),
        Column("source_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("context", "TEXT"),
        Column("run_id", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["first_seen_utc"],
    default_order=[OrderColumn("first_seen_utc", "DESC")],
    supports_run_delete=True,
)


TELEPHONE_NUMBERS_SCHEMA = TableSchema(
    name="telephone_numbers",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("phone_number", "TEXT", nullable=False),
        Column("country_code", "TEXT"),
        Column("discovered_by", "TEXT", nullable=False),
        Column("first_seen_utc", "TEXT"),
        Column("last_seen_utc", "TEXT"),
        Column("source_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("context", "TEXT"),
        Column("run_id", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["first_seen_utc"],
    default_order=[OrderColumn("first_seen_utc", "DESC")],
    supports_run_delete=True,
)


AUTOFILL_SCHEMA = TableSchema(
    name="autofill",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("name", "TEXT", nullable=False),
        Column("value", "TEXT"),
        Column("date_created_utc", "TEXT"),
        Column("date_last_used_utc", "TEXT"),
        Column("count", "INTEGER", default=1),
        # Edge-specific field_id hash (autofill_edge_field_values)
        Column("field_id_hash", "TEXT"),
        # Flag for deleted entries shown alongside active ones
        Column("is_deleted", "INTEGER", default=0),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["date_last_used_utc", "name"],
    default_order=[OrderColumn("date_last_used_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("name", [FilterOp.LIKE]),
        FilterColumn("is_deleted", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


AUTOFILL_PROFILES_SCHEMA = TableSchema(
    name="autofill_profiles",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("guid", "TEXT"),
        Column("full_name", "TEXT"),
        Column("company_name", "TEXT"),
        Column("street_address", "TEXT"),
        Column("city", "TEXT"),
        Column("state", "TEXT"),
        Column("zipcode", "TEXT"),
        Column("country_code", "TEXT"),
        Column("phone", "TEXT"),
        Column("email", "TEXT"),
        Column("date_modified_utc", "TEXT"),
        Column("use_count", "INTEGER"),
        Column("use_date_utc", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["date_modified_utc"],
    default_order=[OrderColumn("date_modified_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


CREDENTIALS_SCHEMA = TableSchema(
    name="credentials",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("origin_url", "TEXT", nullable=False),
        Column("action_url", "TEXT"),
        Column("username_element", "TEXT"),
        Column("username_value", "TEXT"),
        Column("password_element", "TEXT"),
        Column("password_value_encrypted", "BLOB", exclude_from_select=True),
        Column("signon_realm", "TEXT"),
        Column("date_created_utc", "TEXT"),
        Column("date_last_used_utc", "TEXT"),
        Column("date_password_modified_utc", "TEXT"),
        Column("times_used", "INTEGER"),
        Column("blacklisted_by_user", "INTEGER", default=0),
        # Security metadata from Chromium insecure_credentials/breached tables
        Column("is_insecure", "INTEGER", default=0),
        Column("is_breached", "INTEGER", default=0),
        Column("password_notes", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["date_last_used_utc", "origin_url"],
    default_order=[OrderColumn("date_last_used_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("origin_url", [FilterOp.LIKE]),
        FilterColumn("is_insecure", [FilterOp.EQ]),
        FilterColumn("is_breached", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


CREDIT_CARDS_SCHEMA = TableSchema(
    name="credit_cards",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("guid", "TEXT"),
        Column("name_on_card", "TEXT"),
        Column("expiration_month", "INTEGER"),
        Column("expiration_year", "INTEGER"),
        Column("card_number_encrypted", "BLOB", exclude_from_select=True),
        Column("card_number_last_four", "TEXT"),
        Column("billing_address_id", "TEXT"),
        Column("date_modified_utc", "TEXT"),
        Column("use_count", "INTEGER"),
        Column("use_date_utc", "TEXT"),
        Column("nickname", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["date_modified_utc", "name_on_card"],
    default_order=[OrderColumn("date_modified_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


# IBANs from Chromium/Edge Web Data (local_ibans, masked_ibans)
AUTOFILL_IBANS_SCHEMA = TableSchema(
    name="autofill_ibans",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("source_table", "TEXT", nullable=False),
        Column("guid", "TEXT"),
        Column("instrument_id", "INTEGER"),
        Column("nickname", "TEXT"),
        Column("value", "TEXT"),
        Column("value_encrypted", "BLOB", exclude_from_select=True),
        Column("prefix", "TEXT"),
        Column("suffix", "TEXT"),
        Column("length", "INTEGER"),
        Column("use_count", "INTEGER"),
        Column("use_date_utc", "TEXT"),
        Column("date_modified_utc", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["nickname", "use_date_utc", "date_modified_utc", "source_table"],
    default_order=[OrderColumn("use_date_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("source_table", [FilterOp.EQ]),
        FilterColumn("nickname", [FilterOp.LIKE]),
    ],
    supports_run_delete=True,
)


# Search engines from Chromium keywords table
SEARCH_ENGINES_SCHEMA = TableSchema(
    name="search_engines",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("short_name", "TEXT"),
        Column("keyword", "TEXT"),
        Column("url", "TEXT"),
        Column("favicon_url", "TEXT"),
        Column("suggest_url", "TEXT"),
        Column("prepopulate_id", "INTEGER"),
        Column("usage_count", "INTEGER", default=0),
        Column("date_created_utc", "TEXT"),
        Column("last_modified_utc", "TEXT"),
        Column("last_visited_utc", "TEXT"),
        Column("is_default", "INTEGER", default=0),
        Column("is_active", "INTEGER", default=1),
        Column("new_tab_url", "TEXT"),
        Column("image_url", "TEXT"),
        Column("search_url_post_params", "TEXT"),
        Column("suggest_url_post_params", "TEXT"),
        Column("token_mappings", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["keyword", "short_name", "usage_count", "is_default"],
    default_order=[OrderColumn("is_default", "DESC"), OrderColumn("usage_count", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("keyword", [FilterOp.LIKE]),
        FilterColumn("is_default", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


# Deleted form history from Firefox moz_deleted_formhistory
DELETED_FORM_HISTORY_SCHEMA = TableSchema(
    name="deleted_form_history",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("guid", "TEXT"),
        Column("time_deleted_utc", "TEXT"),
        Column("original_fieldname", "TEXT"),
        Column("original_value", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["time_deleted_utc", "guid"],
    default_order=[OrderColumn("time_deleted_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("guid", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


# Autofill block list from Edge autofill_edge_block_list
AUTOFILL_BLOCK_LIST_SCHEMA = TableSchema(
    name="autofill_block_list",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("guid", "TEXT"),
        Column("block_value", "TEXT"),
        Column("block_value_type", "INTEGER"),
        Column("attribute_flag", "INTEGER"),
        Column("meta_data", "TEXT"),
        Column("device_model", "TEXT"),
        Column("date_created_utc", "TEXT"),
        Column("date_modified_utc", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["block_value", "date_created_utc", "block_value_type"],
    default_order=[OrderColumn("date_created_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("block_value", [FilterOp.LIKE]),
        FilterColumn("block_value_type", [FilterOp.EQ]),
        FilterColumn("device_model", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


# Token-based contact info from Chromium 100+
AUTOFILL_PROFILE_TOKENS_SCHEMA = TableSchema(
    name="autofill_profile_tokens",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("guid", "TEXT", nullable=False),
        Column("token_type", "INTEGER", nullable=False),
        Column("token_type_name", "TEXT"),
        Column("token_value", "TEXT"),
        Column("source_table", "TEXT"),
        Column("parent_table", "TEXT"),
        Column("parent_use_count", "INTEGER"),
        Column("parent_use_date_utc", "TEXT"),
        Column("parent_date_modified_utc", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["guid", "token_type"],
    default_order=[OrderColumn("guid", "ASC"), OrderColumn("token_type", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("guid", [FilterOp.EQ]),
        FilterColumn("token_type", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


SESSION_WINDOWS_SCHEMA = TableSchema(
    name="session_windows",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("window_id", "INTEGER"),
        Column("selected_tab_index", "INTEGER"),
        Column("window_type", "TEXT"),
        Column("bounds_x", "INTEGER"),
        Column("bounds_y", "INTEGER"),
        Column("bounds_width", "INTEGER"),
        Column("bounds_height", "INTEGER"),
        Column("show_state", "TEXT"),
        Column("session_type", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("window_id", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


SESSION_TABS_SCHEMA = TableSchema(
    name="session_tabs",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("window_id", "INTEGER"),
        Column("tab_index", "INTEGER"),
        Column("url", "TEXT", nullable=False),
        Column("title", "TEXT"),
        Column("pinned", "INTEGER", default=0),
        Column("group_id", "INTEGER"),
        Column("last_accessed_utc", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["window_id", "tab_index", "last_accessed_utc"],
    default_order=[OrderColumn("window_id", "ASC"), OrderColumn("tab_index", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


SESSION_TAB_HISTORY_SCHEMA = TableSchema(
    name="session_tab_history",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("tab_id", "INTEGER"),
        Column("nav_index", "INTEGER"),
        Column("url", "TEXT", nullable=False),
        Column("title", "TEXT"),
        Column("transition_type", "TEXT"),
        Column("timestamp_utc", "TEXT"),
        # Forensic metadata
        Column("referrer_url", "TEXT"),
        Column("original_request_url", "TEXT"),
        Column("has_post_data", "INTEGER"),
        Column("http_status_code", "INTEGER"),
        # Provenance
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    filterable_columns=[
        FilterColumn("tab_id", [FilterOp.EQ]),
        FilterColumn("browser", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


CLOSED_TABS_SCHEMA = TableSchema(
    name="closed_tabs",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("url", "TEXT", nullable=False),
        Column("title", "TEXT"),
        Column("closed_at_utc", "TEXT"),
        Column("original_window_id", "INTEGER"),
        Column("original_tab_index", "INTEGER"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["closed_at_utc", "url"],
    default_order=[OrderColumn("closed_at_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


SESSION_FORM_DATA_SCHEMA = TableSchema(
    name="session_form_data",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("url", "TEXT"),
        Column("field_name", "TEXT", nullable=False),
        Column("field_value", "TEXT"),
        Column("field_type", "TEXT"),
        Column("xpath", "TEXT"),
        Column("window_id", "INTEGER"),
        Column("tab_id", "INTEGER"),
        Column("nav_index", "INTEGER"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["url", "field_name"],
    default_order=[OrderColumn("url", "ASC"), OrderColumn("field_name", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("url", [FilterOp.EQ, FilterOp.LIKE]),
        FilterColumn("field_name", [FilterOp.EQ, FilterOp.LIKE]),
    ],
    supports_run_delete=True,
)


SITE_PERMISSIONS_SCHEMA = TableSchema(
    name="site_permissions",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("origin", "TEXT", nullable=False),
        Column("permission_type", "TEXT", nullable=False),
        Column("permission_value", "TEXT", nullable=False),
        Column("raw_type", "TEXT"),
        Column("raw_value", "INTEGER"),
        Column("granted_at_utc", "TEXT"),
        Column("expires_at_utc", "TEXT"),
        Column("expires_type", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["origin", "permission_type"],
    default_order=[OrderColumn("origin", "ASC"), OrderColumn("permission_type", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("permission_type", [FilterOp.EQ]),
        FilterColumn("permission_value", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


MEDIA_PLAYBACK_SCHEMA = TableSchema(
    name="media_playback",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("url", "TEXT", nullable=False),
        Column("origin", "TEXT"),
        Column("watch_time_seconds", "REAL", default=0),
        Column("has_video", "INTEGER", default=0),
        Column("has_audio", "INTEGER", default=0),
        Column("last_played_utc", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["last_played_utc"],
    default_order=[OrderColumn("last_played_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("has_video", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


MEDIA_SESSIONS_SCHEMA = TableSchema(
    name="media_sessions",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("url", "TEXT", nullable=False),
        Column("origin", "TEXT"),
        Column("title", "TEXT"),
        Column("artist", "TEXT"),
        Column("album", "TEXT"),
        Column("source_title", "TEXT"),
        Column("duration_ms", "INTEGER", default=0),
        Column("position_ms", "INTEGER", default=0),
        Column("completion_percent", "REAL"),
        Column("last_played_utc", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("source_path", "TEXT", nullable=False),
        Column("discovered_by", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["last_played_utc"],
    default_order=[OrderColumn("last_played_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


HSTS_ENTRIES_SCHEMA = TableSchema(
    name="hsts_entries",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("hashed_host", "TEXT", nullable=False),
        Column("decoded_host", "TEXT"),
        Column("decode_method", "TEXT"),
        Column("sts_observed", "REAL"),
        Column("expiry", "REAL"),
        Column("include_subdomains", "INTEGER", default=0),
        Column("mode", "TEXT"),
        Column("source_path", "TEXT", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("discovered_by", "TEXT", default="transport_security"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.REPLACE,  # UNIQUE on (evidence_id, hashed_host, source_path); REPLACE for re-runs
    sortable_columns=["created_at_utc"],
    default_order=[OrderColumn("created_at_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("decoded_host", [FilterOp.LIKE]),
    ],
    supports_run_delete=True,
    pre_insert_hook=_hsts_pre_insert,
)


JUMP_LIST_ENTRIES_SCHEMA = TableSchema(
    name="jump_list_entries",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("appid", "TEXT", nullable=False),
        Column("browser", "TEXT"),
        Column("jumplist_path", "TEXT", nullable=False),
        Column("entry_id", "TEXT"),
        Column("target_path", "TEXT"),
        Column("arguments", "TEXT"),
        Column("working_directory", "TEXT"),
        Column("url", "TEXT"),
        Column("title", "TEXT"),
        Column("lnk_creation_time", "TEXT"),
        Column("lnk_modification_time", "TEXT"),
        Column("lnk_access_time", "TEXT"),
        Column("access_count", "INTEGER"),
        Column("pin_status", "TEXT"),
        Column("source_path", "TEXT", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("discovered_by", "TEXT", default="jump_lists"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("tags", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["lnk_access_time", "target_path", "url"],
    default_order=[OrderColumn("lnk_access_time", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("url", [FilterOp.LIKE]),
        FilterColumn("target_path", [FilterOp.LIKE]),
        FilterColumn("pin_status", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


BROWSER_EXTENSIONS_SCHEMA = TableSchema(
    name="browser_extensions",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("extension_id", "TEXT", nullable=False),
        Column("name", "TEXT", nullable=False),
        Column("version", "TEXT"),
        Column("description", "TEXT"),
        Column("author", "TEXT"),
        Column("homepage_url", "TEXT"),
        Column("manifest_version", "INTEGER"),
        Column("permissions", "TEXT"),
        Column("host_permissions", "TEXT"),
        Column("content_scripts", "TEXT"),
        Column("install_time", "TEXT"),
        Column("update_time", "TEXT"),
        Column("enabled", "INTEGER", default=0),
        Column("risk_score", "INTEGER", default=0),
        Column("risk_factors", "TEXT"),
        Column("known_category", "TEXT"),
        # New columns from Preferences parsing
        Column("disable_reasons", "INTEGER", default=0),
        Column("install_location", "INTEGER"),
        Column("install_location_text", "TEXT"),
        Column("from_webstore", "INTEGER"),
        Column("granted_permissions", "TEXT"),
        # Forensic provenance
        Column("source_path", "TEXT", nullable=False),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["risk_score", "name"],
    default_order=[OrderColumn("risk_score", "DESC"), OrderColumn("name", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("known_category", [FilterOp.EQ]),
        FilterColumn("risk_score", [FilterOp.GTE]),
    ],
    supports_run_delete=True,
    pre_insert_hook=_extensions_pre_insert,
)


LOCAL_STORAGE_SCHEMA = TableSchema(
    name="local_storage",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("origin", "TEXT", nullable=False),
        Column("key", "TEXT", nullable=False),
        Column("value", "TEXT"),
        Column("value_type", "TEXT"),
        Column("value_size", "INTEGER"),
        Column("source_path", "TEXT", nullable=False),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["origin", "key"],
    default_order=[OrderColumn("origin", "ASC"), OrderColumn("key", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("origin", [FilterOp.LIKE]),
        FilterColumn("key", [FilterOp.LIKE]),
    ],
    supports_run_delete=True,
)


SESSION_STORAGE_SCHEMA = TableSchema(
    name="session_storage",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("origin", "TEXT", nullable=False),
        Column("key", "TEXT", nullable=False),
        Column("value", "TEXT"),
        Column("value_type", "TEXT"),
        Column("value_size", "INTEGER"),
        Column("source_path", "TEXT", nullable=False),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["origin", "key"],
    default_order=[OrderColumn("origin", "ASC"), OrderColumn("key", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("origin", [FilterOp.LIKE]),
        FilterColumn("key", [FilterOp.LIKE]),
    ],
    supports_run_delete=True,
)


INDEXEDDB_DATABASES_SCHEMA = TableSchema(
    name="indexeddb_databases",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("origin", "TEXT", nullable=False),
        Column("database_name", "TEXT", nullable=False),
        Column("database_version", "INTEGER"),
        Column("object_stores", "TEXT"),
        Column("total_entries", "INTEGER", default=0),
        Column("source_path", "TEXT", nullable=False),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["origin", "database_name"],
    default_order=[OrderColumn("origin", "ASC"), OrderColumn("database_name", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("origin", [FilterOp.LIKE]),
    ],
    supports_run_delete=True,
)


INDEXEDDB_ENTRIES_SCHEMA = TableSchema(
    name="indexeddb_entries",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("database_id", "INTEGER"),
        Column("object_store", "TEXT", nullable=False),
        Column("key", "TEXT", nullable=False),
        Column("value", "TEXT"),
        Column("value_type", "TEXT"),
        Column("value_size", "INTEGER"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["database_id", "object_store", "key"],
    default_order=[
        OrderColumn("database_id", "ASC"),
        OrderColumn("object_store", "ASC"),
        OrderColumn("key", "ASC"),
    ],
    filterable_columns=[
        FilterColumn("database_id", [FilterOp.EQ]),
        FilterColumn("object_store", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


SYNC_DATA_SCHEMA = TableSchema(
    name="sync_data",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("account_id", "TEXT"),
        Column("email", "TEXT"),
        Column("display_name", "TEXT"),
        Column("gaia_id", "TEXT"),
        Column("profile_path", "TEXT"),
        Column("last_sync_time", "TEXT"),
        Column("sync_enabled", "INTEGER", default=0),
        Column("synced_types", "TEXT"),
        Column("raw_data", "TEXT"),
        Column("source_path", "TEXT", nullable=False),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["email", "browser"],
    default_order=[OrderColumn("email", "ASC"), OrderColumn("browser", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
    pre_insert_hook=_sync_pre_insert,
)


SYNCED_DEVICES_SCHEMA = TableSchema(
    name="synced_devices",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("device_id", "TEXT"),
        Column("device_name", "TEXT"),
        Column("device_type", "TEXT"),
        Column("os_type", "TEXT"),
        Column("chrome_version", "TEXT"),
        Column("last_updated", "TEXT"),
        Column("sync_account_id", "TEXT"),
        Column("raw_data", "TEXT"),
        Column("source_path", "TEXT", nullable=False),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["device_name"],
    default_order=[OrderColumn("device_name", "ASC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("sync_account_id", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


OS_INDICATORS_SCHEMA = TableSchema(
    name="os_indicators",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("type", "TEXT", nullable=False),
        Column("name", "TEXT", nullable=False),
        Column("value", "TEXT"),
        Column("path", "TEXT"),
        Column("hive", "TEXT"),
        Column("confidence", "TEXT"),
        Column("detected_at_utc", "TEXT"),
        Column("provenance", "TEXT"),
        Column("extra_json", "TEXT"),
        Column("run_id", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    supports_run_delete=True,
)


PLATFORM_DETECTIONS_SCHEMA = TableSchema(
    name="platform_detections",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("platform_id", "TEXT", nullable=False),
        Column("platform_name", "TEXT", nullable=False),
        Column("category", "TEXT", nullable=False),
        Column("confidence", "TEXT", nullable=False),
        Column("score", "INTEGER", nullable=False),
        Column("matched_patterns_json", "TEXT", nullable=False),
        Column("source_url", "TEXT"),
        Column("source_file", "TEXT"),
        Column("detected_at_utc", "TEXT"),
        Column("run_id", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    supports_run_delete=True,
    pre_insert_hook=_platform_detections_pre_insert,
)


TIMELINE_SCHEMA = TableSchema(
    name="timeline",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("ts_utc", "TEXT", nullable=False),
        Column("kind", "TEXT", nullable=False),
        Column("ref_table", "TEXT", nullable=False),
        Column("ref_id", "INTEGER", nullable=False),
        Column("confidence", "TEXT"),
        Column("note", "TEXT"),
        Column("run_id", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["ts_utc", "kind"],
    default_order=[OrderColumn("ts_utc", "DESC")],
    supports_run_delete=True,
)


# =============================================================================
# Storage Analysis Schemas (- Firefox Storage Deep Analysis)
# =============================================================================

STORAGE_TOKENS_SCHEMA = TableSchema(
    name="storage_tokens",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("origin", "TEXT", nullable=False),
        Column("storage_type", "TEXT", nullable=False),
        Column("storage_key", "TEXT", nullable=False),
        Column("token_type", "TEXT", nullable=False),
        Column("token_value", "TEXT", nullable=False),
        Column("token_hash", "TEXT"),
        Column("issuer", "TEXT"),
        Column("subject", "TEXT"),
        Column("audience", "TEXT"),
        Column("associated_email", "TEXT"),
        Column("associated_user_id", "TEXT"),
        Column("issued_at_utc", "TEXT"),
        Column("expires_at_utc", "TEXT"),
        Column("last_used_utc", "TEXT"),
        Column("risk_level", "TEXT", default="medium"),
        Column("is_expired", "INTEGER", default=0),
        Column("source_path", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("logical_path", "TEXT"),
        Column("forensic_path", "TEXT"),
        Column("notes", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.IGNORE,  # Ignore duplicates by token_hash
    sortable_columns=["expires_at_utc", "origin", "token_type"],
    default_order=[OrderColumn("expires_at_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("origin", [FilterOp.LIKE]),
        FilterColumn("token_type", [FilterOp.EQ]),
        FilterColumn("risk_level", [FilterOp.EQ]),
        FilterColumn("is_expired", [FilterOp.EQ]),
        FilterColumn("run_id", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)

STORAGE_IDENTIFIERS_SCHEMA = TableSchema(
    name="storage_identifiers",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("run_id", "TEXT", nullable=False),
        Column("browser", "TEXT", nullable=False),
        Column("profile", "TEXT"),
        Column("origin", "TEXT", nullable=False),
        Column("storage_type", "TEXT", nullable=False),
        Column("storage_key", "TEXT", nullable=False),
        Column("identifier_type", "TEXT", nullable=False),
        Column("identifier_name", "TEXT"),
        Column("identifier_value", "TEXT", nullable=False),
        Column("first_seen_utc", "TEXT"),
        Column("last_seen_utc", "TEXT"),
        Column("source_path", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("fs_type", "TEXT"),
        Column("created_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.IGNORE,  # Ignore duplicates
    sortable_columns=["origin", "identifier_type", "first_seen_utc"],
    default_order=[OrderColumn("first_seen_utc", "DESC")],
    filterable_columns=[
        FilterColumn("browser", [FilterOp.EQ]),
        FilterColumn("origin", [FilterOp.LIKE]),
        FilterColumn("identifier_type", [FilterOp.EQ]),
    ],
    supports_run_delete=True,
)


EXTRACTED_FILES_SCHEMA = TableSchema(
    name="extracted_files",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        Column("extractor_name", "TEXT", nullable=False),
        Column("extractor_version", "TEXT"),
        Column("run_id", "TEXT", nullable=False),
        Column("extracted_at_utc", "TEXT", nullable=False),
        Column("source_path", "TEXT"),
        Column("source_inode", "TEXT"),
        Column("partition_index", "INTEGER"),
        Column("source_offset_bytes", "INTEGER"),
        Column("source_block_size", "INTEGER"),
        Column("dest_rel_path", "TEXT", nullable=False),
        Column("dest_filename", "TEXT", nullable=False),
        Column("size_bytes", "INTEGER"),
        Column("file_type", "TEXT"),
        Column("mime_type", "TEXT"),
        Column("md5", "TEXT"),
        Column("sha256", "TEXT"),
        Column("status", "TEXT", nullable=False, default="ok"),
        Column("error_message", "TEXT"),
        Column("metadata_json", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,  # Audit table - no ON CONFLICT
    sortable_columns=["id", "extractor_name", "extracted_at_utc", "size_bytes", "status", "dest_filename"],
    default_order=[OrderColumn("id", "DESC")],
    filterable_columns=[
        FilterColumn("extractor_name", [FilterOp.EQ, FilterOp.IN]),
        FilterColumn("run_id", [FilterOp.EQ]),
        FilterColumn("status", [FilterOp.EQ, FilterOp.IN]),
        FilterColumn("partition_index", [FilterOp.EQ]),
        FilterColumn("file_type", [FilterOp.EQ, FilterOp.IN]),
        FilterColumn("sha256", [FilterOp.EQ]),
        FilterColumn("source_path", [FilterOp.LIKE]),
        FilterColumn("dest_filename", [FilterOp.LIKE]),
    ],
    supports_run_delete=True,
)


# =============================================================================
# Screenshots - Investigator-captured documentation images
# =============================================================================

SCREENSHOTS_SCHEMA = TableSchema(
    name="screenshots",
    columns=[
        Column("id", "INTEGER", nullable=False),
        Column("evidence_id", "INTEGER", nullable=False),
        # URL context (text only, no FK)
        Column("captured_url", "TEXT"),
        # File storage
        Column("dest_path", "TEXT", nullable=False),
        Column("filename", "TEXT", nullable=False),
        Column("size_bytes", "INTEGER"),
        Column("width", "INTEGER"),
        Column("height", "INTEGER"),
        Column("md5", "TEXT"),
        Column("sha256", "TEXT"),
        # Investigator annotations
        Column("title", "TEXT"),
        Column("caption", "TEXT"),
        Column("notes", "TEXT"),
        # Sequence grouping
        Column("sequence_name", "TEXT"),
        Column("sequence_order", "INTEGER", default=0),
        # Source tracking
        Column("source", "TEXT", nullable=False, default="sandbox"),
        # Timestamps
        Column("captured_at_utc", "TEXT", nullable=False),
        Column("created_at_utc", "TEXT", nullable=False),
        Column("updated_at_utc", "TEXT"),
    ],
    conflict_action=ConflictAction.FAIL,
    sortable_columns=["id", "title", "filename", "captured_at_utc", "created_at_utc", "sequence_name", "sequence_order"],
    default_order=[OrderColumn("captured_at_utc", "DESC")],
    filterable_columns=[
        FilterColumn("sequence_name", [FilterOp.EQ]),
        FilterColumn("source", [FilterOp.EQ]),
        FilterColumn("captured_url", [FilterOp.LIKE]),
    ],
    supports_run_delete=False,  # Screenshots are manually managed, not run-based
)


TABLE_SCHEMAS: Dict[str, TableSchema] = {
    "cookies": COOKIES_SCHEMA,
    "bookmarks": BOOKMARKS_SCHEMA,
    "browser_downloads": BROWSER_DOWNLOADS_SCHEMA,
    "urls": URLS_SCHEMA,
    "browser_history": BROWSER_HISTORY_SCHEMA,
    "images": IMAGES_SCHEMA,
    "image_discoveries": IMAGE_DISCOVERIES_SCHEMA,
    "url_matches": URL_MATCHES_SCHEMA,
    "tags": TAGS_SCHEMA,
    "tag_associations": TAG_ASSOCIATIONS_SCHEMA,
    "favicons": FAVICONS_SCHEMA,
    "favicon_mappings": FAVICON_MAPPINGS_SCHEMA,
    "top_sites": TOP_SITES_SCHEMA,
    "bitcoin_addresses": BITCOIN_ADDRESSES_SCHEMA,
    "ethereum_addresses": ETHEREUM_ADDRESSES_SCHEMA,
    "emails": EMAILS_SCHEMA,
    "domains": DOMAINS_SCHEMA,
    "ip_addresses": IP_ADDRESSES_SCHEMA,
    "telephone_numbers": TELEPHONE_NUMBERS_SCHEMA,
    "autofill": AUTOFILL_SCHEMA,
    "autofill_profiles": AUTOFILL_PROFILES_SCHEMA,
    "credentials": CREDENTIALS_SCHEMA,
    "credit_cards": CREDIT_CARDS_SCHEMA,
    "autofill_ibans": AUTOFILL_IBANS_SCHEMA,
    "search_engines": SEARCH_ENGINES_SCHEMA,
    "deleted_form_history": DELETED_FORM_HISTORY_SCHEMA,
    "autofill_block_list": AUTOFILL_BLOCK_LIST_SCHEMA,
    "autofill_profile_tokens": AUTOFILL_PROFILE_TOKENS_SCHEMA,
    "session_windows": SESSION_WINDOWS_SCHEMA,
    "session_tabs": SESSION_TABS_SCHEMA,
    "session_tab_history": SESSION_TAB_HISTORY_SCHEMA,
    "closed_tabs": CLOSED_TABS_SCHEMA,
    "session_form_data": SESSION_FORM_DATA_SCHEMA,
    "site_permissions": SITE_PERMISSIONS_SCHEMA,
    "media_playback": MEDIA_PLAYBACK_SCHEMA,
    "media_sessions": MEDIA_SESSIONS_SCHEMA,
    "hsts_entries": HSTS_ENTRIES_SCHEMA,
    "jump_list_entries": JUMP_LIST_ENTRIES_SCHEMA,
    "browser_extensions": BROWSER_EXTENSIONS_SCHEMA,
    "local_storage": LOCAL_STORAGE_SCHEMA,
    "session_storage": SESSION_STORAGE_SCHEMA,
    "indexeddb_databases": INDEXEDDB_DATABASES_SCHEMA,
    "indexeddb_entries": INDEXEDDB_ENTRIES_SCHEMA,
    "sync_data": SYNC_DATA_SCHEMA,
    "synced_devices": SYNCED_DEVICES_SCHEMA,
    "os_indicators": OS_INDICATORS_SCHEMA,
    "platform_detections": PLATFORM_DETECTIONS_SCHEMA,
    "timeline": TIMELINE_SCHEMA,
    "storage_tokens": STORAGE_TOKENS_SCHEMA,
    "storage_identifiers": STORAGE_IDENTIFIERS_SCHEMA,
    "extracted_files": EXTRACTED_FILES_SCHEMA,
    "screenshots": SCREENSHOTS_SCHEMA,
}
