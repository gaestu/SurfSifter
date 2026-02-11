"""
Chromium Downloads schema definitions for schema warning support.

This module defines known tables, columns, and enum values for the Chromium
downloads tables in the History database. Used by the extractor to detect
unknown schemas that may contain forensically valuable data we're not capturing.

Schema Evolution:
- Chromium <80: Basic downloads table
- Chromium 80+: downloads_url_chains for redirect tracking
- Chromium 100+: downloads_slices for partial download segments

References:
- Chromium source: components/download/database/download_db_conversions.cc
- History DB: chrome/browser/history/history_database.cc
"""

from __future__ import annotations

from typing import Dict, Set, List


# =============================================================================
# Known Tables
# =============================================================================
# Tables we expect in a Chromium History database related to downloads.
# Used to detect if new download-related tables have been added.

KNOWN_DOWNLOADS_TABLES: Set[str] = {
    "downloads",
    "downloads_url_chains",
    "downloads_slices",  # Partial download segments (modern Chromium)
    "meta",  # Schema version tracking (shared with history)
}

# Patterns to filter relevant unknown tables (download-related)
DOWNLOADS_TABLE_PATTERNS: List[str] = [
    "download",
    "chain",
    "slice",
]


# =============================================================================
# Known Columns (downloads table)
# =============================================================================
# Columns we currently parse from the downloads table.
# Unknown columns will be reported as warnings.

KNOWN_DOWNLOADS_COLUMNS: Set[str] = {
    # Core identification
    "id",
    "guid",  # GUID for cross-referencing

    # File paths
    "target_path",
    "current_path",  # Temp path during download

    # Timestamps (WebKit format)
    "start_time",
    "end_time",
    "last_access_time",

    # Size tracking
    "received_bytes",
    "total_bytes",

    # State and danger
    "state",
    "danger_type",
    "interrupt_reason",

    # User interaction
    "opened",

    # URL context
    "referrer",
    "tab_url",
    "tab_referrer_url",
    "site_url",

    # MIME types
    "mime_type",
    "original_mime_type",

    # Hash
    "hash",

    # Additional metadata
    "http_method",
    "by_ext_id",
    "by_ext_name",
    "etag",
    "last_modified",
    "transient",
}

# Columns in downloads_url_chains table
KNOWN_URL_CHAINS_COLUMNS: Set[str] = {
    "id",
    "chain_index",
    "url",
}

# Columns in downloads_slices table
KNOWN_SLICES_COLUMNS: Set[str] = {
    "download_id",
    "offset",
    "received_bytes",
    "finished",
}


# =============================================================================
# Download State Mapping
# =============================================================================
# Maps Chromium download state codes to human-readable strings.
# Unknown state codes will be reported as warnings.

DOWNLOAD_STATE_MAP: Dict[int, str] = {
    0: "in_progress",
    1: "complete",
    2: "cancelled",
    3: "interrupted",
    4: "interrupted_network",  # Some Chromium versions
}


# =============================================================================
# Danger Type Mapping
# =============================================================================
# Maps Chromium danger type codes to human-readable strings.
# Reference: components/download/public/common/download_danger_type.h

DANGER_TYPE_MAP: Dict[int, str] = {
    0: "not_dangerous",
    1: "dangerous_file",
    2: "dangerous_url",
    3: "dangerous_content",
    4: "maybe_dangerous_content",
    5: "uncommon_content",
    6: "user_validated",
    7: "dangerous_host",
    8: "potentially_unwanted",
    9: "allowlisted_by_policy",
    10: "async_scanning",
    11: "blocked_password_protected",
    12: "blocked_too_large",
    13: "sensitive_content_warning",
    14: "sensitive_content_block",
    15: "deep_scanned_safe",
    16: "deep_scanned_opened_dangerous",
    17: "prompt_for_scanning",
    18: "blocked_unsupported_file_type",
    # Reserve space for future additions
}


# =============================================================================
# Interrupt Reason Mapping
# =============================================================================
# Maps interrupt reason codes to human-readable strings.
# Reference: components/download/public/common/download_interrupt_reasons.h

INTERRUPT_REASON_MAP: Dict[int, str] = {
    0: "none",
    # File errors (1-19)
    1: "file_failed",
    2: "file_access_denied",
    3: "file_no_space",
    5: "file_name_too_long",
    6: "file_too_large",
    7: "file_virus_infected",
    10: "file_transient_error",
    11: "file_blocked",
    12: "file_security_check_failed",
    13: "file_too_short",
    14: "file_hash_mismatch",
    15: "file_same_as_source",
    # Network errors (20-39)
    20: "network_failed",
    21: "network_timeout",
    22: "network_disconnected",
    23: "network_server_down",
    24: "network_invalid_request",
    # Server errors (30-39)
    30: "server_failed",
    31: "server_no_range",
    32: "server_bad_content",
    33: "server_unauthorized",
    34: "server_cert_problem",
    35: "server_forbidden",
    36: "server_unreachable",
    37: "server_content_length_mismatch",
    38: "server_cross_origin_redirect",
    # User actions (40-49)
    40: "user_canceled",
    41: "user_shutdown",
    # Crash (50)
    50: "crash",
}
