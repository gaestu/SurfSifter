"""
System Jump Lists Extractor

Windows Jump Lists extraction with OLE parsing and LNK analysis.

Jump Lists contain "Recent" and "Frequent" items from taskbar applications.
For browsers, this includes visited URLs that may survive history clearing.
For other apps, this shows recently opened files - high forensic value.

Supported formats:
    - AutomaticDestinations-ms: OLE compound files with recent/frequent items
    - CustomDestinations-ms: Concatenated LNK files with pinned items

Components:
    - SystemJumpListsExtractor: Main extractor with statistics integration
    - appid_registry: Browser AppID mapping (centralized 700+ app registry)
    - ole_parser: OLE compound + concatenated LNK parsers
    - lnk_parser: LNK shortcut parser for target paths and URLs

The AppID registry is centralized at:
    extractors/_shared/appids.json (700+ applications)
    extractors/_shared/appid_loader.py (loader module)
"""

from .extractor import SystemJumpListsExtractor
from .appid_registry import (
    load_browser_appids,
    is_browser_jumplist,
    get_browser_for_appid,
    get_app_name,
    get_browser_appids_for_browser,
    get_category_for_appid,
    get_category_display_name,
    is_forensically_interesting,
    get_all_browser_appids,
)
from .ole_parser import parse_jumplist_ole, parse_jumplist_custom, parse_jumplist_file
from .lnk_parser import parse_lnk_data, parse_lnk_stream, extract_url_from_lnk

__all__ = [
    "SystemJumpListsExtractor",
    "load_browser_appids",
    "is_browser_jumplist",
    "get_browser_for_appid",
    "get_app_name",
    "get_browser_appids_for_browser",
    "get_category_for_appid",
    "get_category_display_name",
    "is_forensically_interesting",
    "get_all_browser_appids",
    "parse_jumplist_ole",
    "parse_jumplist_custom",
    "parse_jumplist_file",
    "parse_lnk_data",
    "parse_lnk_stream",
    "extract_url_from_lnk",
]
