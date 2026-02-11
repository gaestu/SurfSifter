"""
AppID Registry for Jump Lists

Wrapper module providing backward-compatible access to the centralized
AppID registry in extractors._shared.appid_loader.

Maps Windows Jump List Application IDs (AppIDs) to application names.
AppIDs are the 16-character hex prefix of .automaticDestinations-ms filenames.

Example filename: 5d696d521de238c3.automaticDestinations-ms
                 ^^^^^^^^^^^^^^^^^
                      AppID (Chrome in this case)

The actual registry data is now stored in:
    extractors/_shared/appids.json

This module provides backward-compatible API for existing code.
For new code, consider importing directly from extractors._shared.appid_loader.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Set, Optional

# Import from centralized registry
from extractors._shared.appid_loader import (
    load_browser_appids as _load_browser_appids,
    is_browser_appid as _is_browser_appid,
    get_app_name as _get_app_name,
    get_browser_name as _get_browser_name,
    get_browser_appids_for_browser as _get_browser_appids_for_browser,
    get_category_for_appid,
    get_category_display_name,
    is_forensically_interesting,
    get_all_browser_appids,
)

LOGGER = logging.getLogger(__name__)


def load_browser_appids(config_path: Optional[Path] = None) -> Dict[str, str]:
    """
    Load browser AppID mappings.

    Now uses centralized registry from extractors/_shared/appids.json.
    The config_path parameter is kept for backward compatibility but ignored.

    Args:
        config_path: Deprecated. External config files are no longer supported.

    Returns:
        Dictionary mapping AppID (lowercase) to browser name.
    """
    if config_path:
        LOGGER.debug(
            "config_path parameter is deprecated, using centralized registry"
        )
    return _load_browser_appids()


def is_browser_jumplist(appid: str, appid_map: Optional[Dict[str, str]] = None) -> tuple[bool, str]:
    """
    Check if a Jump List AppID belongs to a browser.

    Args:
        appid: Jump List AppID (e.g., "5d696d521de238c3")
        appid_map: Optional pre-loaded AppID map. If None, uses centralized registry.

    Returns:
        Tuple of (is_browser, browser_name). browser_name is empty string if not a browser.
    """
    if not appid:
        return False, ""

    # If custom map provided, use it (backward compat)
    if appid_map is not None:
        appid_lower = appid.lower()
        if appid_lower in appid_map:
            return True, appid_map[appid_lower]
        return False, ""

    # Use centralized registry
    return _is_browser_appid(appid)


def get_app_name(appid: str) -> str:
    """
    Get application name for any AppID (browser or other known app).

    Now uses comprehensive centralized registry with 700+ applications.

    Args:
        appid: 16-character hex AppID.

    Returns:
        Application name, or "Unknown (AppID:XXXXXXXX)" if not found.
    """
    return _get_app_name(appid)


def get_browser_for_appid(appid: str, appid_map: Optional[Dict[str, str]] = None) -> Optional[str]:
    """
    Get browser name for an AppID.

    Args:
        appid: 16-character hex AppID.
        appid_map: Optional pre-loaded AppID map (deprecated).

    Returns:
        Browser name or None if not recognized.
    """
    if appid_map is not None:
        return appid_map.get(appid.lower())
    return _get_browser_name(appid)


def get_browser_appids_for_browser(browser_name: str) -> Set[str]:
    """
    Get all known AppIDs for a specific browser.

    Args:
        browser_name: Browser name (case-insensitive).

    Returns:
        Set of AppIDs (lowercase) for that browser.
    """
    return _get_browser_appids_for_browser(browser_name)


# Re-export new functions for enhanced functionality
__all__ = [
    "load_browser_appids",
    "is_browser_jumplist",
    "get_app_name",
    "get_browser_for_appid",
    "get_browser_appids_for_browser",
    # New functions from centralized registry
    "get_category_for_appid",
    "get_category_display_name",
    "is_forensically_interesting",
    "get_all_browser_appids",
]
