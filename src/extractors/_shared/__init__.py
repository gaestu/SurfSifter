"""
Shared utilities for browser extractors.

This package provides common functionality used across multiple extractors:
- timestamps: Browser timestamp format conversions (WebKit, PRTime, Unix, Cocoa)
- sqlite_helpers: Safe read-only SQLite access
- path_utils: Glob/profile path helpers

Design Principle:
    Extractors are self-contained modules, independent from src/core/.
    These utilities are specifically for extractors to maintain modularity.
"""

from .timestamps import (
    webkit_to_datetime,
    webkit_to_iso,
    prtime_to_datetime,
    prtime_to_iso,
    unix_to_datetime,
    unix_to_iso,
    cocoa_to_datetime,
    cocoa_to_iso,
    WEBKIT_EPOCH_DIFF,
    COCOA_EPOCH_DIFF,
)

from .sqlite_helpers import (
    safe_sqlite_connect,
    safe_execute,
    copy_sqlite_for_reading,
    SQLiteReadError,
    get_table_names,
    table_exists,
    get_row_count,
)

from .path_utils import (
    expand_windows_env_vars,
    glob_pattern_to_regex,
    find_matching_paths,
    enumerate_browser_profiles,
    normalize_evidence_path,
    extract_username_from_path,
    WINDOWS_ENV_DEFAULTS,
)

from .appid_loader import (
    load_browser_appids,
    is_browser_appid,
    get_app_name,
    get_browser_name,
    get_browser_appids_for_browser,
    get_category_for_appid,
    get_category_display_name,
    is_forensically_interesting,
    get_all_browser_appids,
    get_forensic_categories,
    CATEGORY_DISPLAY_NAMES,
    BROWSER_DISPLAY_NAMES,
    APP_DISPLAY_NAMES,
)

__all__ = [
    # Timestamps
    "webkit_to_datetime",
    "webkit_to_iso",
    "prtime_to_datetime",
    "prtime_to_iso",
    "unix_to_datetime",
    "unix_to_iso",
    "cocoa_to_datetime",
    "cocoa_to_iso",
    "WEBKIT_EPOCH_DIFF",
    "COCOA_EPOCH_DIFF",
    # SQLite helpers
    "safe_sqlite_connect",
    "safe_execute",
    "copy_sqlite_for_reading",
    "SQLiteReadError",
    "get_table_names",
    "table_exists",
    "get_row_count",
    # Path utils
    "expand_windows_env_vars",
    "glob_pattern_to_regex",
    "find_matching_paths",
    "enumerate_browser_profiles",
    "normalize_evidence_path",
    "extract_username_from_path",
    "WINDOWS_ENV_DEFAULTS",
    # AppID registry
    "load_browser_appids",
    "is_browser_appid",
    "get_app_name",
    "get_browser_name",
    "get_browser_appids_for_browser",
    "get_category_for_appid",
    "get_category_display_name",
    "is_forensically_interesting",
    "get_all_browser_appids",
    "get_forensic_categories",
    "CATEGORY_DISPLAY_NAMES",
    "BROWSER_DISPLAY_NAMES",
    "APP_DISPLAY_NAMES",
]
