"""
Multi-partition discovery for Firefox Browser Storage Extractor.

Provides discovery of Local Storage and IndexedDB files across ALL partitions
using file_list SQL queries for fast, comprehensive discovery.

Features:
- Fast SQL-based discovery (seconds vs minutes for filesystem walk)
- Multi-partition support with partition_index preservation
- Fallback to filesystem walk if file_list is empty
- Path hash to prevent overwrites when same browser exists in multiple locations

Initial implementation with multi-partition support
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, TYPE_CHECKING

from .._patterns import (
    FIREFOX_BROWSERS,
    get_artifact_patterns,
    extract_profile_from_path as patterns_extract_profile,
    detect_browser_from_path as patterns_detect_browser,
)

if TYPE_CHECKING:
    from extractors.callbacks import ExtractorCallbacks

LOGGER = logging.getLogger(__name__)


def extract_profile_from_path(path: str) -> str:
    """
    Extract Firefox profile name from a path.

    Delegates to _patterns.py implementation.
    """
    return patterns_extract_profile(path)


def detect_browser_from_path(path: str) -> str:
    """
    Detect which Firefox-family browser from a file path.

    Delegates to _patterns.py implementation.
    """
    return patterns_detect_browser(path)


def discover_storage_multi_partition(
    evidence_conn,
    evidence_id: int,
    evidence_fs,
    browsers: List[str],
    config: Dict[str, Any],
    callbacks: "ExtractorCallbacks",
) -> Dict[int, List[Dict]]:
    """
    Discover Firefox storage files across ALL partitions.

    Uses file_list SQL queries for fast multi-partition discovery,
    with fallback to filesystem walk if file_list is empty.

    Args:
        evidence_conn: Evidence database connection
        evidence_id: Evidence ID
        evidence_fs: Evidence filesystem (for fallback)
        browsers: List of browser keys to search
        config: Extraction config
        callbacks: Extractor callbacks

    Returns:
        Dict mapping partition_index -> list of storage location dicts
    """
    from extractors._shared.file_list_discovery import (
        discover_from_file_list,
        FileListDiscoveryResult,
    )

    files_by_partition: Dict[int, List[Dict]] = {}

    # Storage types to search for
    storage_configs = []
    if config.get("local_storage", True):
        storage_configs.append("local_storage")
    if config.get("indexeddb", True):
        storage_configs.append("indexeddb")

    for storage_type in storage_configs:
        # Build patterns using _patterns.py
        combined_patterns = _build_storage_path_patterns(browsers, storage_type)

        if not combined_patterns:
            continue

        # Query file_list for SQLite files matching storage patterns
        filename_patterns = ["*.sqlite", "*.sqlite-wal", "*.sqlite-shm"]
        if storage_type == "local_storage":
            filename_patterns.extend(["webappsstore.sqlite", "data.sqlite"])

        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=filename_patterns,
            path_patterns=combined_patterns,
        )

        if result.is_empty:
            callbacks.on_log(f"No {storage_type} found in file_list", "debug")
            continue

        # Group and deduplicate by storage file
        for partition_idx, matches in result.matches_by_partition.items():
            seen_files: Set[str] = set()

            for match in matches:
                file_path = match.file_path

                # Skip WAL/SHM files, we'll read the main sqlite file
                if file_path.endswith("-wal") or file_path.endswith("-shm"):
                    continue

                if file_path in seen_files:
                    continue
                seen_files.add(file_path)

                # Detect browser from path
                browser = detect_browser_from_path(file_path)
                if browser and browser not in browsers:
                    continue  # Skip browsers not in selection

                profile = extract_profile_from_path(file_path)
                display_name = FIREFOX_BROWSERS.get(browser, {}).get("display_name", browser) if browser else "Firefox"

                # Determine storage format based on path
                storage_format = _detect_storage_format(file_path, storage_type)

                # Extract origin from modern storage paths
                origin = _extract_origin_from_storage_path(file_path) if storage_format == "modern_ls" else None

                loc = {
                    "logical_path": file_path,
                    "browser": browser or "firefox",
                    "profile": profile,
                    "origin": origin,
                    "storage_type": storage_type,
                    "storage_format": storage_format,
                    "display_name": display_name,
                    "partition_index": partition_idx,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                }

                if partition_idx not in files_by_partition:
                    files_by_partition[partition_idx] = []
                files_by_partition[partition_idx].append(loc)

                callbacks.on_log(
                    f"Found {browser or 'firefox'} {storage_type} ({storage_format}) on partition {partition_idx}: {file_path}",
                    "info"
                )

    # If no results from file_list, fall back to filesystem walk
    if not files_by_partition:
        callbacks.on_log(
            "No storage found in file_list, falling back to filesystem scan",
            "warning"
        )
        partition_index = getattr(evidence_fs, 'partition_index', 0)
        locations = _discover_storage_filesystem(evidence_fs, browsers, config, callbacks)
        if locations:
            for loc in locations:
                loc["partition_index"] = partition_index
            files_by_partition[partition_index] = locations

    # Log summary
    total_locations = sum(len(locs) for locs in files_by_partition.values())
    if len(files_by_partition) > 1:
        callbacks.on_log(
            f"Found {total_locations} storage locations across {len(files_by_partition)} partitions",
            "info"
        )

    return files_by_partition


def _build_storage_path_patterns(browsers: List[str], storage_type: str) -> List[str]:
    """
    Build SQL LIKE patterns for browser storage paths using _patterns.py.

    Args:
        browsers: List of browser keys to build patterns for
        storage_type: Storage type key ("local_storage" or "indexeddb")

    Returns:
        List of SQL LIKE patterns for file_list queries
    """
    patterns = set()

    for browser in browsers:
        if browser not in FIREFOX_BROWSERS:
            continue

        try:
            # Use the canonical patterns from _patterns.py
            artifact_patterns = get_artifact_patterns(browser, storage_type)
            for pattern in artifact_patterns:
                # Convert glob pattern to SQL LIKE pattern:
                # - Replace * with %
                # - Wrap with % for partial matching
                sql_pattern = pattern.replace("*", "%")
                patterns.add(f"%{sql_pattern}%")
        except ValueError:
            # Artifact type not defined for this browser
            LOGGER.debug("No %s patterns defined for %s", storage_type, browser)
            continue

    return list(patterns) if patterns else []


def _detect_storage_format(path: str, storage_type: str) -> str:
    """
    Detect the storage format from the file path.

    Args:
        path: File path
        storage_type: "local_storage" or "indexeddb"

    Returns:
        Storage format identifier
    """
    if storage_type == "indexeddb":
        return "indexeddb"

    # Local storage format detection
    if "/ls/" in path or "\\ls\\" in path:
        return "modern_ls"
    elif "webappsstore" in path.lower():
        return "legacy_webappsstore"
    else:
        # Try to detect from directory structure
        return "modern_ls" if "storage/default" in path else "legacy_webappsstore"


def _extract_origin_from_storage_path(path: str) -> Optional[str]:
    """
    Extract and decode origin from Firefox storage path.

    Firefox encodes origins in directory names as:
    - https+++example.com -> https://example.com
    - https+++example.com+443 -> https://example.com:443

    Path formats:
    - storage/default/{origin_encoded}/ls/data.sqlite (Local Storage)
    - storage/default/{origin_encoded}/idb/{db}.sqlite (IndexedDB)
    """
    # Match storage/default/{origin}/ls/ or storage/default/{origin}/idb/
    match = re.search(r"storage/default/([^/]+)/(ls|idb)/", path)
    if match:
        encoded = match.group(1)
        return _decode_firefox_origin_dir(encoded)
    return None


def _decode_firefox_origin_dir(encoded: str) -> str:
    """
    Decode Firefox origin directory encoding.

    Firefox encodes origins in directory names:
    - https+++example.com -> https://example.com
    - https+++example.com+443 -> https://example.com:443
    - http+++example.com+8080 -> http://example.com:8080
    - file+++ -> file://
    - moz-extension+++{uuid} -> moz-extension://{uuid}

    Origin attributes (if present) are appended with ^ separator:
    - https+++example.com^userContextId=1 -> container/contextual identity
    - https+++example.com^privateBrowsingId=1 -> private browsing
    """
    if not encoded:
        return ""

    # Split off origin attributes (^userContextId=1, etc.)
    if "^" in encoded:
        encoded, _ = encoded.split("^", 1)

    # Decode the origin: +++ represents ://
    origin = encoded.replace("+++", "://")

    # Handle port (last + followed by digits is port separator)
    if "://" in origin:
        scheme, rest = origin.split("://", 1)
        # Find last + that's followed by digits (port)
        port_match = re.search(r"\+(\d+)$", rest)
        if port_match:
            host = rest[:port_match.start()]
            port = port_match.group(1)
            origin = f"{scheme}://{host}:{port}"
        else:
            origin = f"{scheme}://{rest}"

    return origin


def _discover_storage_filesystem(
    evidence_fs,
    browsers: List[str],
    config: Dict[str, Any],
    callbacks: "ExtractorCallbacks"
) -> List[Dict]:
    """
    Fallback: discover storage via filesystem walk (single partition).

    Used when file_list is empty.
    """
    locations = []

    for browser_key in browsers:
        if browser_key not in FIREFOX_BROWSERS:
            continue

        display_name = FIREFOX_BROWSERS[browser_key]["display_name"]

        # Local Storage
        if config.get("local_storage", True):
            patterns = get_artifact_patterns(browser_key, "local_storage")
            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        if not path_str.endswith(".sqlite"):
                            continue

                        profile = extract_profile_from_path(path_str)
                        storage_format = _detect_storage_format(path_str, "local_storage")
                        origin = _extract_origin_from_storage_path(path_str) if storage_format == "modern_ls" else None

                        locations.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "profile": profile,
                            "origin": origin,
                            "storage_type": "local_storage",
                            "storage_format": storage_format,
                            "display_name": display_name,
                        })
                        callbacks.on_log(f"Found {browser_key} Local Storage: {path_str}", "info")
                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        # IndexedDB
        if config.get("indexeddb", True):
            patterns = get_artifact_patterns(browser_key, "indexeddb")
            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        if not path_str.endswith(".sqlite"):
                            continue

                        profile = extract_profile_from_path(path_str)
                        origin = _extract_origin_from_storage_path(path_str)

                        locations.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "profile": profile,
                            "origin": origin,
                            "storage_type": "indexeddb",
                            "storage_format": "indexeddb",
                            "display_name": display_name,
                        })
                        callbacks.on_log(f"Found {browser_key} IndexedDB: {path_str}", "info")
                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

    return locations


def extract_storage_file(
    evidence_fs,
    loc: Dict,
    output_dir: Path,
    callbacks: "ExtractorCallbacks"
) -> Dict:
    """
    Copy storage SQLite file from evidence to workspace.

    Includes partition_index AND path hash in output path to prevent overwrites.

    Args:
        evidence_fs: Evidence filesystem handle
        loc: Location dict with logical_path, browser, profile, partition_index
        output_dir: Output directory
        callbacks: Extractor callbacks

    Returns:
        Dict with copy status and metadata
    """
    try:
        source_path = loc["logical_path"]
        browser = loc["browser"]
        profile = loc["profile"]
        storage_type = loc["storage_type"]
        storage_format = loc.get("storage_format", "")
        partition_index = loc.get("partition_index", 0)

        safe_profile = profile.replace(' ', '_').replace('/', '_').replace('.', '_')

        # Include partition_index AND path hash in dest name to prevent overwrites
        path_hash = hashlib.md5(source_path.encode()).hexdigest()[:8]

        if storage_type == "local_storage":
            if storage_format == "modern_ls":
                origin_safe = loc.get("origin", "unknown")
                origin_safe = origin_safe.replace("://", "_").replace(".", "_").replace("/", "_").replace(":", "_")
                dest_name = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_ls_{origin_safe}.sqlite"
            else:
                dest_name = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_webappsstore.sqlite"
        else:
            origin_safe = loc.get("origin", "unknown")
            origin_safe = origin_safe.replace("://", "_").replace(".", "_").replace("/", "_").replace(":", "_")
            dest_name = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_indexeddb_{origin_safe}.sqlite"

        dest_path = output_dir / dest_name

        callbacks.on_log(f"Copying {source_path} to {dest_name}", "info")

        file_content = evidence_fs.read_file(source_path)
        dest_path.write_bytes(file_content)

        md5 = hashlib.md5(file_content).hexdigest()
        sha256 = hashlib.sha256(file_content).hexdigest()
        size = len(file_content)

        return {
            "copy_status": "ok",
            "browser": browser,
            "profile": profile,
            "storage_type": storage_type,
            "storage_format": storage_format,
            "origin": loc.get("origin"),
            "logical_path": source_path,
            "extracted_path": str(dest_path),
            "file_size_bytes": size,
            "md5": md5,
            "sha256": sha256,
            "partition_index": partition_index,
        }

    except Exception as e:
        callbacks.on_log(f"Failed to extract {loc['logical_path']}: {e}", "error")
        return {
            "copy_status": "error",
            "browser": loc.get("browser"),
            "profile": loc.get("profile"),
            "storage_type": loc.get("storage_type"),
            "storage_format": loc.get("storage_format"),
            "logical_path": loc.get("logical_path"),
            "partition_index": loc.get("partition_index", 0),
            "error_message": str(e),
        }
