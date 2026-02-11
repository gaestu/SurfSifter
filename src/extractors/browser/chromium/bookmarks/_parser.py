"""
Chromium Bookmarks JSON parser.

Parses the Bookmarks JSON file from Chromium-based browsers (Chrome, Edge,
Brave, Opera) with schema discovery support for unknown keys detection.

The parser handles:
- Recursive folder tree traversal
- WebKit timestamp conversion (microseconds since 1601-01-01)
- Folder path reconstruction
- Unknown key detection via warning collector

Usage:
    from extractors.browser.chromium.bookmarks._parser import (
        parse_bookmarks_json,
        get_bookmark_stats,
        ChromiumBookmark,
    )

    with open(bookmarks_path) as f:
        data = json.load(f)

    for bookmark in parse_bookmarks_json(data, warning_collector=collector, source_file="Bookmarks"):
        print(bookmark.name, bookmark.url)

Moved from _parsers.py with schema warning support
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterator, Optional, Set, TYPE_CHECKING

from extractors._shared.timestamps import webkit_to_datetime, webkit_to_iso
from ._schemas import (
    KNOWN_ROOT_KEYS,
    KNOWN_ROOT_FOLDER_KEYS,
    KNOWN_BOOKMARK_NODE_KEYS,
    KNOWN_BOOKMARK_TYPES,
    KNOWN_META_INFO_KEYS,
    KNOWN_ROOT_FOLDERS,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ChromiumBookmark:
    """A single Chromium bookmark record."""
    id: str
    name: str
    url: Optional[str]  # None for folders
    date_added: Optional[datetime]
    date_added_iso: Optional[str]
    date_modified: Optional[datetime]
    date_modified_iso: Optional[str]
    bookmark_type: str  # "url" or "folder"
    folder_path: str  # e.g., "Bookmarks Bar/Tech/Dev"
    guid: Optional[str]
    date_last_used: Optional[datetime] = None
    date_last_used_iso: Optional[str] = None


# =============================================================================
# Main Parser Functions
# =============================================================================

def parse_bookmarks_json(
    data: Dict[str, Any],
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    source_file: Optional[str] = None,
) -> Iterator[ChromiumBookmark]:
    """
    Parse Chromium Bookmarks JSON file.

    Args:
        data: Parsed JSON dict from Bookmarks file
        warning_collector: Optional collector for schema warnings
        source_file: Source file path for warning context

    Yields:
        ChromiumBookmark records (both URLs and folders)

    Note:
        Chromium stores bookmarks as JSON with nested folder structure.
        Root folders: bookmark_bar, other, synced (mobile)
    """
    # Discover unknown root-level keys
    if warning_collector:
        _discover_root_level_unknowns(data, warning_collector, source_file)

    roots = data.get("roots", {})

    # Discover unknown root folders
    if warning_collector:
        _discover_unknown_root_folders(roots, warning_collector, source_file)

    # Process each known root folder
    for root_key, display_name in KNOWN_ROOT_FOLDERS.items():
        root_node = roots.get(root_key)
        if root_node:
            yield from _parse_bookmark_node(
                root_node,
                display_name,
                warning_collector=warning_collector,
                source_file=source_file,
            )


def get_bookmark_stats(data: Dict[str, Any]) -> Dict[str, int]:
    """
    Get quick statistics from parsed Bookmarks JSON.

    Args:
        data: Parsed JSON dict from Bookmarks file

    Returns:
        Dict with bookmark_count, folder_count, url_count
    """
    stats = {"bookmark_count": 0, "folder_count": 0, "url_count": 0}

    for bookmark in parse_bookmarks_json(data):
        stats["bookmark_count"] += 1
        if bookmark.bookmark_type == "folder":
            stats["folder_count"] += 1
        elif bookmark.bookmark_type == "url":
            stats["url_count"] += 1

    return stats


# =============================================================================
# Internal Parser Helpers
# =============================================================================

def _parse_bookmark_node(
    node: Dict[str, Any],
    folder_path: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    source_file: Optional[str] = None,
) -> Iterator[ChromiumBookmark]:
    """
    Recursively parse a bookmark node and its children.

    Args:
        node: Bookmark node dict
        folder_path: Current folder path
        warning_collector: Optional collector for schema warnings
        source_file: Source file path for warning context

    Yields:
        ChromiumBookmark records
    """
    # Discover unknown keys in this node
    if warning_collector:
        _discover_unknown_node_keys(node, warning_collector, source_file, folder_path)

    node_type = node.get("type", "")

    # Check for unknown bookmark type
    if warning_collector and node_type and node_type not in KNOWN_BOOKMARK_TYPES:
        warning_collector.add_warning(
            warning_type="unknown_enum_value",
            category="json",
            severity="warning",
            artifact_type="bookmarks",
            source_file=source_file,
            item_name="bookmark_type",
            item_value=node_type,
            context_json={"folder_path": folder_path, "node_id": node.get("id")},
        )

    # Convert timestamps (WebKit format stored as string in JSON)
    date_added, date_added_iso = _parse_webkit_timestamp(node.get("date_added"))
    date_modified, date_modified_iso = _parse_webkit_timestamp(node.get("date_modified"))
    date_last_used, date_last_used_iso = _parse_webkit_timestamp(node.get("date_last_used"))

    # Yield the node itself
    yield ChromiumBookmark(
        id=node.get("id", ""),
        name=node.get("name", ""),
        url=node.get("url"),
        date_added=date_added,
        date_added_iso=date_added_iso,
        date_modified=date_modified,
        date_modified_iso=date_modified_iso,
        bookmark_type=node_type,
        folder_path=folder_path,
        guid=node.get("guid"),
        date_last_used=date_last_used,
        date_last_used_iso=date_last_used_iso,
    )

    # Recursively process children
    if node_type == "folder" and "children" in node:
        child_path = f"{folder_path}/{node.get('name', '')}" if folder_path else node.get("name", "")
        for child in node.get("children", []):
            yield from _parse_bookmark_node(
                child,
                child_path,
                warning_collector=warning_collector,
                source_file=source_file,
            )


def _parse_webkit_timestamp(value: Any) -> tuple[Optional[datetime], Optional[str]]:
    """
    Parse a WebKit timestamp value from JSON.

    Args:
        value: Timestamp value (may be string or int, or None)

    Returns:
        Tuple of (datetime, iso_string) or (None, None) if invalid
    """
    if value is None:
        return None, None

    try:
        ts = int(value)
        if ts <= 0:
            return None, None
        dt = webkit_to_datetime(ts)
        iso = webkit_to_iso(ts)
        return dt, iso
    except (ValueError, TypeError, OverflowError):
        return None, None


# =============================================================================
# Schema Discovery Helpers
# =============================================================================

def _discover_root_level_unknowns(
    data: Dict[str, Any],
    warning_collector: "ExtractionWarningCollector",
    source_file: Optional[str],
) -> None:
    """Discover unknown keys at the root level of Bookmarks JSON."""
    unknown_keys = set(data.keys()) - KNOWN_ROOT_KEYS

    for key in unknown_keys:
        value = data[key]
        warning_collector.add_warning(
            warning_type="json_unknown_key",
            category="json",
            severity="info",
            artifact_type="bookmarks",
            source_file=source_file,
            item_name=f"root.{key}",
            item_value=_describe_value(value),
            context_json={"key_path": key},
        )


def _discover_unknown_root_folders(
    roots: Dict[str, Any],
    warning_collector: "ExtractionWarningCollector",
    source_file: Optional[str],
) -> None:
    """Discover unknown root folders in the 'roots' object."""
    unknown_folders = set(roots.keys()) - KNOWN_ROOT_FOLDER_KEYS

    for folder_name in unknown_folders:
        folder_data = roots[folder_name]
        warning_collector.add_warning(
            warning_type="json_unknown_key",
            category="json",
            severity="warning",  # Unknown root folder is more significant
            artifact_type="bookmarks",
            source_file=source_file,
            item_name=f"roots.{folder_name}",
            item_value=_describe_folder(folder_data),
            context_json={"folder_name": folder_name},
        )


def _discover_unknown_node_keys(
    node: Dict[str, Any],
    warning_collector: "ExtractionWarningCollector",
    source_file: Optional[str],
    folder_path: str,
) -> None:
    """Discover unknown keys in a bookmark node."""
    unknown_keys = set(node.keys()) - KNOWN_BOOKMARK_NODE_KEYS

    for key in unknown_keys:
        value = node[key]
        warning_collector.add_warning(
            warning_type="json_unknown_key",
            category="json",
            severity="info",
            artifact_type="bookmarks",
            source_file=source_file,
            item_name=f"node.{key}",
            item_value=_describe_value(value),
            context_json={
                "folder_path": folder_path,
                "node_id": node.get("id"),
                "node_name": node.get("name"),
            },
        )

    # Also check meta_info if present
    if "meta_info" in node and isinstance(node["meta_info"], dict):
        _discover_unknown_meta_info_keys(
            node["meta_info"],
            warning_collector,
            source_file,
            folder_path,
            node.get("id"),
        )


def _discover_unknown_meta_info_keys(
    meta_info: Dict[str, Any],
    warning_collector: "ExtractionWarningCollector",
    source_file: Optional[str],
    folder_path: str,
    node_id: Optional[str],
) -> None:
    """Discover unknown keys in a bookmark's meta_info object."""
    unknown_keys = set(meta_info.keys()) - KNOWN_META_INFO_KEYS

    for key in unknown_keys:
        value = meta_info[key]
        warning_collector.add_warning(
            warning_type="json_unknown_key",
            category="json",
            severity="info",
            artifact_type="bookmarks",
            source_file=source_file,
            item_name=f"meta_info.{key}",
            item_value=_describe_value(value),
            context_json={
                "folder_path": folder_path,
                "node_id": node_id,
            },
        )


def _describe_value(value: Any) -> str:
    """Create a brief description of a value for logging."""
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return str(value).lower()
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        if len(value) <= 50:
            return f'"{value}"'
        return f'"{value[:47]}..." (len={len(value)})'
    elif isinstance(value, list):
        return f"array[{len(value)}]"
    elif isinstance(value, dict):
        return f"object{{{len(value)} keys}}"
    else:
        return type(value).__name__


def _describe_folder(folder_data: Any) -> str:
    """Create a brief description of a folder node."""
    if not isinstance(folder_data, dict):
        return _describe_value(folder_data)

    children = folder_data.get("children", [])
    child_count = len(children) if isinstance(children, list) else 0
    folder_type = folder_data.get("type", "unknown")

    return f"folder(type={folder_type}, children={child_count})"
