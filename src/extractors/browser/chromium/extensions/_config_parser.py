"""
Chrome Preferences configuration parser.

Extracts forensically useful configuration fields from Chrome
Preferences JSON and structures them for the browser_config table.

Fields extracted:
- profile.name: Profile display name
- default_search_provider_data: Default search engine
- homepage / homepage_is_newtabpage: Homepage configuration
- session.startup_urls: Startup page URLs
- browser.show_home_button: Home button visibility
- safebrowsing: Safe browsing settings
- download.default_directory: Download path
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from core.logging import get_logger

LOGGER = get_logger("extractors.browser.chromium.extensions.config_parser")


# Config keys to extract from Preferences JSON.
# Each tuple: (json_path, config_key, config_type)
# json_path uses dot notation for nested access.
CONFIG_KEYS = [
    ("profile.name", "profile_name", "profile"),
    ("profile.exit_type", "exit_type", "profile"),
    ("default_search_provider_data.short_name", "default_search_engine", "search"),
    ("default_search_provider_data.keyword", "default_search_keyword", "search"),
    ("homepage", "homepage_url", "startup"),
    ("homepage_is_newtabpage", "homepage_is_newtab", "startup"),
    ("session.restore_on_startup", "restore_on_startup", "startup"),
    ("browser.show_home_button", "show_home_button", "startup"),
    ("download.default_directory", "download_directory", "download"),
    ("safebrowsing.enabled", "safe_browsing_enabled", "security"),
    ("safebrowsing.enhanced", "safe_browsing_enhanced", "security"),
    ("signin.allowed", "signin_allowed", "account"),
]

# Startup URLs are a list, handled separately
STARTUP_URLS_PATH = "session.startup_urls"


def _get_nested(data: dict, path: str) -> Any:
    """Get a value from nested dict using dot-notation path."""
    parts = path.split(".")
    current = data
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def parse_preferences_config(
    preferences_data: dict,
    browser: str,
    profile: str,
    source_path: str,
    run_id: str,
    *,
    partition_index: Optional[int] = None,
    fs_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Extract browser config records from Chrome Preferences JSON.

    Args:
        preferences_data: Parsed Preferences JSON dict
        browser: Browser name (e.g., "chrome", "edge")
        profile: Profile name (e.g., "Default", "Profile 1")
        source_path: Evidence path to the Preferences file
        run_id: Extraction run ID
        partition_index: Optional partition index
        fs_type: Optional filesystem type

    Returns:
        List of config record dicts ready for insert_browser_configs()
    """
    records = []

    for json_path, config_key, config_type in CONFIG_KEYS:
        value = _get_nested(preferences_data, json_path)
        if value is not None:
            # Convert booleans and numbers to strings for storage
            str_value = str(value) if not isinstance(value, str) else value
            records.append({
                "run_id": run_id,
                "browser": browser,
                "profile": profile,
                "config_type": config_type,
                "config_key": config_key,
                "config_value": str_value,
                "value_count": 1,
                "source_path": source_path,
                "partition_index": partition_index,
                "fs_type": fs_type,
            })

    # Handle startup URLs (list)
    startup_urls = _get_nested(preferences_data, STARTUP_URLS_PATH)
    if isinstance(startup_urls, list) and startup_urls:
        records.append({
            "run_id": run_id,
            "browser": browser,
            "profile": profile,
            "config_type": "startup",
            "config_key": "startup_urls",
            "config_value": json.dumps(startup_urls),
            "value_count": len(startup_urls),
            "source_path": source_path,
            "partition_index": partition_index,
            "fs_type": fs_type,
        })

    return records
