"""Compatibility shim for the AppID registry loader.

The implementation lives in :mod:`core.appid_loader` so app/report modules can
resolve Jump List AppIDs without importing the extractor package.
"""
from __future__ import annotations

from core.appid_loader import (
    APP_DISPLAY_NAMES,
    BROWSER_DISPLAY_NAMES,
    CATEGORY_DISPLAY_NAMES,
    get_all_browser_appids,
    get_app_name,
    get_browser_appids_for_browser,
    get_browser_name,
    get_category_for_appid,
    get_category_display_name,
    get_forensic_categories,
    is_browser_appid,
    is_forensically_interesting,
    load_browser_appids,
)

__all__ = [
    "APP_DISPLAY_NAMES",
    "BROWSER_DISPLAY_NAMES",
    "CATEGORY_DISPLAY_NAMES",
    "get_all_browser_appids",
    "get_app_name",
    "get_browser_appids_for_browser",
    "get_browser_name",
    "get_category_for_appid",
    "get_category_display_name",
    "get_forensic_categories",
    "is_browser_appid",
    "is_forensically_interesting",
    "load_browser_appids",
]