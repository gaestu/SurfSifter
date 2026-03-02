"""
LNK Shell Link Parser for Jump Lists

Parses Windows Shell Link (LNK) data embedded in Jump Lists.
Extracts target paths, arguments (may contain URLs), and timestamps.
"""

from __future__ import annotations

import io
import logging
import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional

LOGGER = logging.getLogger(__name__)


# URL pattern for extraction
URL_PATTERN = re.compile(r'https?://[^\s"<>\']+', re.IGNORECASE)


def parse_lnk_data(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Parse LNK shell link data.

    Args:
        data: Raw LNK file bytes

    Returns:
        Dict with target, arguments, timestamps, etc.
    """
    try:
        # Try LnkParse3 library first
        return _parse_with_lnkparse3(data)
    except ImportError:
        LOGGER.debug("LnkParse3 not available, using fallback parser")
        return _parse_fallback(data)
    except Exception as e:
        LOGGER.debug("LnkParse3 failed: %s, using fallback", e)
        return _parse_fallback(data)


def parse_lnk_stream(stream_data: bytes) -> Dict[str, Any]:
    """Alias for parse_lnk_data for backward compatibility."""
    result = parse_lnk_data(stream_data)
    return result or {}


def _parse_with_lnkparse3(data: bytes) -> Optional[Dict[str, Any]]:
    """Parse LNK using LnkParse3 library.

    LnkParse3 uses get_json() to return parsed data as a dictionary,
    not individual getter methods.
    """
    import LnkParse3

    with io.BytesIO(data) as f:
        lnk = LnkParse3.lnk_file(f)
        parsed = lnk.get_json()

        # Extract header timestamps
        header = parsed.get("header", {})

        # Extract target path from link_info
        link_info = parsed.get("link_info", {})
        target_path = link_info.get("local_base_path")

        # If no local path, try to reconstruct from target items
        if not target_path:
            target_items = parsed.get("target", {}).get("items", [])
            if target_items:
                # Build path from target item chain
                path_parts = []
                for item in target_items:
                    if item.get("class") == "Volume Item":
                        path_parts.append(item.get("data", "").rstrip("\\"))
                    elif item.get("class") == "File entry":
                        path_parts.append(item.get("primary_name", ""))
                if path_parts:
                    target_path = "\\".join(path_parts)

        # Extract string data (arguments, working dir, etc.)
        string_data = parsed.get("data", {})
        arguments = string_data.get("command_line_arguments")
        working_directory = string_data.get("working_directory")
        icon_location = string_data.get("icon_location")

        # Also check for URLs in extra data or property stores
        url = None
        extra = parsed.get("extra", {})

        # Some LNK files store URLs in property stores
        for block_name, block_data in extra.items():
            if isinstance(block_data, dict):
                prop_store = block_data.get("property_store", [])
                for prop in prop_store:
                    if isinstance(prop, dict):
                        for val in prop.get("serialized_property_values", []):
                            value = val.get("value")
                            if isinstance(value, str) and value.startswith(("http://", "https://")):
                                url = value
                                break

        result = {
            "target_path": target_path,
            "arguments": arguments,
            "working_directory": working_directory,
            "icon_location": icon_location,
            "creation_time": _format_header_time(header.get("creation_time")),
            "modification_time": _format_header_time(header.get("modified_time")),
            "access_time": _format_header_time(header.get("accessed_time")),
            "url": url,  # Pre-extracted URL if found in property store
        }

        return result


def _format_header_time(time_val) -> Optional[str]:
    """Format timestamp from LnkParse3 header to ISO 8601."""
    if time_val is None:
        return None
    try:
        if isinstance(time_val, datetime):
            return time_val.isoformat()
        return str(time_val)
    except Exception:
        return None


def _parse_fallback(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Fallback LNK parser when LnkParse3 is not available.

    Extracts basic information using simple heuristics.
    """
    if len(data) < 76:
        return None

    # Check LNK header magic
    if data[:4] != b'\x4c\x00\x00\x00':
        return None

    result = {
        "target_path": None,
        "arguments": None,
        "creation_time": None,
        "modification_time": None,
        "access_time": None,
    }

    # Try to find strings (UTF-16 LE encoded paths/arguments)
    try:
        # Look for URL patterns in arguments/paths
        text = data.decode('utf-16-le', errors='ignore')

        # Find URLs
        urls = URL_PATTERN.findall(text)
        if urls:
            result["arguments"] = urls[0]

        # Try to find paths
        if ':\\' in text or '/' in text:
            # Simple path extraction
            for part in text.split('\x00'):
                part = part.strip()
                if part and (':\\' in part or part.startswith('/')):
                    if not result["target_path"]:
                        result["target_path"] = part[:260]  # Max path length
    except Exception:
        pass

    # Try to extract timestamps from header
    try:
        import struct

        # FILETIME offsets in LNK header
        # Creation: offset 28 (8 bytes)
        # Access: offset 36 (8 bytes)
        # Write: offset 44 (8 bytes)

        creation = struct.unpack_from('<Q', data, 28)[0]
        access = struct.unpack_from('<Q', data, 36)[0]
        write = struct.unpack_from('<Q', data, 44)[0]

        result["creation_time"] = _filetime_to_iso(creation)
        result["access_time"] = _filetime_to_iso(access)
        result["modification_time"] = _filetime_to_iso(write)
    except Exception:
        pass

    return result


def _filetime_to_iso(filetime: int) -> Optional[str]:
    """Convert Windows FILETIME to ISO 8601."""
    if filetime == 0:
        return None

    try:
        # FILETIME is 100-nanosecond intervals since 1601-01-01
        EPOCH_DIFF = 116444736000000000

        if filetime < EPOCH_DIFF:
            return None

        unix_time = (filetime - EPOCH_DIFF) / 10000000
        dt = datetime.fromtimestamp(unix_time, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, OSError, OverflowError):
        return None


def extract_url_from_lnk(lnk_data: Dict[str, Any]) -> Optional[str]:
    """
    Extract URL from parsed LNK data.

    URLs in browser Jump Lists typically appear in:
    1. Pre-extracted from property store during parsing
    2. DestList path field (e.g., "microsoft-edge:?...&url=https%3A//..." )
    3. Arguments field: "-url http://example.com" or just "http://example.com"
    4. Target path (rare): when target IS the URL

    Args:
        lnk_data: Parsed LNK dict with target_path, arguments, url,
                  destlist_path, etc.

    Returns:
        Extracted URL or None
    """
    # Check pre-extracted URL first (from property store)
    url = lnk_data.get("url")
    if url:
        return url

    # Check DestList path — browser jump lists store URLs here
    # e.g., "microsoft-edge:?source=windowsfeeds&...&url=https%3A%2F%2Fwww.msn.com%2F..."
    # or direct "http://example.com" paths
    destlist_path = lnk_data.get("destlist_path") or ""
    if destlist_path:
        extracted = _extract_url_from_destlist_path(destlist_path)
        if extracted:
            return extracted

    # Check arguments (most common location for browser shortcuts)
    arguments = lnk_data.get("arguments", "") or ""
    urls = URL_PATTERN.findall(arguments)
    if urls:
        return urls[0]

    # Check target path
    target = lnk_data.get("target_path", "") or ""
    if target.startswith(("http://", "https://")):
        return target

    # Check for URL patterns in target
    urls = URL_PATTERN.findall(target)
    if urls:
        return urls[0]

    return None


def _extract_url_from_destlist_path(path: str) -> Optional[str]:
    """
    Extract URL from a DestList path string.

    Browser jump lists store URLs in the DestList path in various formats:
    - Direct URL: "http://example.com/page"
    - Edge protocol: "microsoft-edge:?source=...&url=https%3A%2F%2Fexample.com"
    - Chrome protocol: similar patterns with chrome: prefix
    - URL-encoded query params

    Args:
        path: The DestList path string

    Returns:
        Extracted and decoded URL, or None
    """
    from urllib.parse import unquote, parse_qs, urlparse

    if not path:
        return None

    # Direct URL
    if path.startswith(("http://", "https://")):
        return path

    # Check for URL in query parameters (common for Edge/Chrome protocol URIs)
    # e.g., "microsoft-edge:?source=...&url=https%3A%2F%2Fwww.msn.com%2F..."
    if "url=" in path.lower():
        try:
            # Find the url= parameter — may be in a protocol URI like microsoft-edge:?...
            # Strip the protocol prefix to parse as a proper URL
            query_part = path
            if "?" in query_part:
                query_part = query_part.split("?", 1)[1]
            params = parse_qs(query_part)
            # Try 'url' key (case-insensitive)
            for key in params:
                if key.lower() == "url":
                    url_val = params[key][0]
                    # Decode if needed
                    decoded = unquote(url_val)
                    if decoded.startswith(("http://", "https://")):
                        return decoded
        except Exception:
            pass

    # Try regex on the decoded string
    decoded_path = unquote(path)
    urls = URL_PATTERN.findall(decoded_path)
    if urls:
        return urls[0]

    return None
