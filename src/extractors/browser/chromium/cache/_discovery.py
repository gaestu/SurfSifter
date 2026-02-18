"""
Cache directory discovery utilities.

Scans evidence filesystems to find Chromium browser cache directories
(both disk cache and Service Worker CacheStorage).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from core.logging import get_logger
from .._patterns import (
    CHROMIUM_BROWSERS,
    get_patterns,
    get_patterns_for_root,
    get_browser_display_name,
)
from .._parsers import extract_profile_from_path as _extract_profile_from_path_shared

if TYPE_CHECKING:
    from ....callbacks import ExtractorCallbacks

LOGGER = get_logger("extractors.cache_simple.discovery")


def discover_cache_directories(
    evidence_fs,
    browsers: List[str],
    callbacks: "ExtractorCallbacks",
    include_cache_storage: bool = False,
    include_disk_cache: bool = True,
    embedded_roots: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Scan evidence for Chromium browser cache directories.

    Uses chromium/_patterns for comprehensive browser coverage including
    beta/dev/canary channels and open-source Chromium.

    Args:
        evidence_fs: Evidence filesystem
        browsers: List of browser keys to scan (from CHROMIUM_BROWSERS)
        callbacks: Progress callbacks
        include_cache_storage: If True, also scan Service Worker CacheStorage directories
        include_disk_cache: If True, scan standard disk cache (default)

    Returns:
        List of dicts: [
            {
                "path": "/Users/john/Library/Caches/Google/Chrome/Default/Cache/Cache_Data",
                "browser": "chrome",
                "profile": "Default",
                "partition_index": 2,
                "fs_type": "apfs",
                "cache_type": "disk_cache" or "cache_storage",
                "files": [
                    {"path": "...", "filename": "f_000001", "size": 16384},
                    ...
                ]
            },
            ...
        ]
    """
    cache_directories = []

    for browser in browsers:
        if browser not in CHROMIUM_BROWSERS:
            callbacks.on_log(f"Unknown browser: {browser}", "warning")
            continue

        display_name = get_browser_display_name(browser)
        callbacks.on_log(f"Scanning for {display_name} cache directories")

        # Scan disk cache patterns (only if enabled)
        if include_disk_cache:
            # Get cache patterns from chromium/_patterns module
            cache_patterns = get_patterns(browser, "cache")
            for pattern in cache_patterns:
                dirs = scan_cache_pattern(
                    evidence_fs, pattern, browser, "disk_cache", callbacks
                )
                cache_directories.extend(dirs)

        if include_disk_cache and embedded_roots:
            for root in embedded_roots:
                for pattern in get_patterns_for_root(root, "cache", flat_profile=False):
                    dirs = scan_cache_pattern(
                        evidence_fs, pattern, "chromium_embedded", "disk_cache", callbacks
                    )
                    cache_directories.extend(dirs)
                for pattern in get_patterns_for_root(root, "cache", flat_profile=True):
                    dirs = scan_cache_pattern(
                        evidence_fs, pattern, "chromium_embedded", "disk_cache", callbacks
                    )
                    cache_directories.extend(dirs)

        # Scan CacheStorage if enabled - use legacy browser_patterns for now
        # TODO: Add cache_storage to chromium/_patterns.CHROMIUM_ARTIFACTS
        if include_cache_storage:
            try:
                from extractors.browser_patterns import get_browser_paths
                cache_storage_patterns = get_browser_paths(browser, 'cache_storage')
                if cache_storage_patterns:
                    callbacks.on_log(f"Scanning {display_name} Service Worker CacheStorage")
                    dirs = discover_cache_storage_directories(
                        evidence_fs, browser, cache_storage_patterns, callbacks
                    )
                    cache_directories.extend(dirs)
            except Exception as e:
                LOGGER.debug("CacheStorage discovery failed for %s: %s", browser, e)

    return cache_directories


def scan_cache_pattern(
    evidence_fs,
    pattern: str,
    browser: str,
    cache_type: str,
    callbacks: "ExtractorCallbacks",
) -> List[Dict[str, Any]]:
    """
    Scan a single cache pattern and return matching directories with files.

    The pattern (e.g., "Cache/Cache_Data") identifies cache directories.
    We then enumerate files inside those directories to get actual cache entries.

    Args:
        evidence_fs: Evidence filesystem
        pattern: Glob pattern to match cache directory
        browser: Browser name
        cache_type: "disk_cache" or "cache_storage"
        callbacks: Progress callbacks

    Returns:
        List of cache directory info dicts with their files
    """
    cache_directories = []

    try:
        # Step 1: Find cache directories matching the pattern
        # The pattern points to directories (e.g., "Cache/Cache_Data"), not files
        dir_matches = list(evidence_fs.iter_paths(pattern))
        callbacks.on_log(f"Pattern '{pattern}': {len(dir_matches)} cache directories found")

        if not dir_matches:
            return cache_directories

        # Step 2: For each cache directory, enumerate the files inside
        # Use walk_directory or iter_paths with wildcard to get contents
        all_files = []
        for cache_dir_path in dir_matches:
            # Enumerate files inside this cache directory
            # Try walk_directory first, fall back to iter_paths with /*
            try:
                dir_files = list(evidence_fs.walk_directory(cache_dir_path))
                LOGGER.debug("walk_directory(%s) found %d files", cache_dir_path, len(dir_files))
            except Exception as e:
                # Fallback: use glob pattern to enumerate files
                LOGGER.debug("walk_directory failed for %s, trying glob: %s", cache_dir_path, e)
                file_pattern = f"{cache_dir_path}/*"
                dir_files = list(evidence_fs.iter_paths(file_pattern))

            all_files.extend(dir_files)

        callbacks.on_log(f"Pattern '{pattern}': {len(all_files)} cache files found")
        matches = all_files

        # Group files by cache directory
        dirs_map = {}
        for file_path in matches:
            path_obj = Path(file_path)

            # Extract profile name
            # e.g., "Users/john/AppData/Local/Google/Chrome/User Data/Default/Cache/Cache_Data/f_000001"
            parts = path_obj.parts

            # Find profile (directory before Cache)
            profile = "Default"
            try:
                if "User Data" in parts:
                    # Chrome/Edge/Brave: User Data/<Profile>/Cache
                    user_data_idx = parts.index("User Data")
                    profile = parts[user_data_idx + 1]
                elif browser == "opera":
                    # Opera: Opera Software/<variant>/Cache
                    if "Opera Stable" in file_path:
                        profile = "Opera Stable"
                    elif "Opera GX Stable" in file_path:
                        profile = "Opera GX Stable"
                    else:
                        profile = "Default"
                elif browser == "chrome" and ".config" in parts:
                    # Linux: ~/.config/google-chrome/<Profile>/Cache
                    config_idx = parts.index(".config")
                    profile = parts[config_idx + 2]
                elif browser == "brave" and ".config" in parts:
                    # Linux: ~/.config/BraveSoftware/Brave-Browser/<Profile>/Cache
                    if "Brave-Browser" in parts:
                        brave_idx = parts.index("Brave-Browser")
                        profile = parts[brave_idx + 1]
            except (ValueError, IndexError):
                pass

            # Cache directory is parent of file
            cache_dir = str(path_obj.parent)

            if cache_dir not in dirs_map:
                dirs_map[cache_dir] = {
                    "path": cache_dir,
                    "browser": browser,
                    "profile": profile,
                    "cache_type": cache_type,
                    "files": [],
                }

            dirs_map[cache_dir]["files"].append({
                "path": file_path,
                "filename": path_obj.name,
            })

        # Add discovered directories to results
        for cache_dir_info in dirs_map.values():
            cache_type_label = "CacheStorage" if cache_type == "cache_storage" else "cache"
            callbacks.on_log(
                f"Found {browser} {cache_type_label}: {cache_dir_info['profile']} "
                f"({len(cache_dir_info['files'])} files)"
            )
            cache_directories.append(cache_dir_info)

    except Exception as e:
        error_msg = f"Error scanning pattern '{pattern}': {e}"
        LOGGER.warning(error_msg, exc_info=True)
        callbacks.on_log(error_msg)

    return cache_directories


def discover_cache_storage_directories(
    evidence_fs,
    browser: str,
    patterns: List[str],
    callbacks: "ExtractorCallbacks",
) -> List[Dict[str, Any]]:
    """
    Two-step CacheStorage discovery: find origin dirs, then scan for cache files.

    CacheStorage structure:
        Service Worker/CacheStorage/{origin_hash}/{cache_id}/
            - index
            - index-dir/the-real-index
            - [0-9a-f]*_0, [0-9a-f]*_1, [0-9a-f]*_s (entry files)
            - f_* (external data files)

    Step 1: Glob CacheStorage/* to find origin directories
    Step 2: For each origin, glob for actual cache files

    Args:
        evidence_fs: Evidence filesystem
        browser: Browser name
        patterns: CacheStorage patterns (e.g., "Users/*/AppData/.../CacheStorage/*")
        callbacks: Progress callbacks

    Returns:
        List of cache directory info dicts with files
    """
    cache_directories = []

    # Cache file patterns to look for inside each origin/cache directory
    # Includes both simple cache format and legacy blockfile format
    cache_file_patterns = [
        "[0-9a-f]*_0",  # Simple cache entry files (metadata)
        "[0-9a-f]*_1",  # Simple cache entry files (data)
        "[0-9a-f]*_s",  # Sparse entry files
        "f_*",           # External data files (both formats)
        "index",         # Index file (both formats)
        "data_*",        # Legacy blockfile data files
    ]

    for pattern in patterns:
        try:
            # Step 1: Find origin directories (CacheStorage/* matches)
            origin_matches = list(evidence_fs.iter_paths(pattern))

            if not origin_matches:
                continue

            # Identify unique origin directories
            # Find the LAST occurrence of CacheStorage in path to handle nested layouts
            # (e.g., macOS variants or nested partition roots)
            origin_dirs = set()
            for match in origin_matches:
                path_obj = Path(match)
                parts = list(path_obj.parts)

                # Find last occurrence of CacheStorage (handles nested paths)
                cs_indices = [i for i, p in enumerate(parts) if p == "CacheStorage"]
                if not cs_indices:
                    continue

                cs_idx = cs_indices[-1]  # Use last occurrence
                if len(parts) > cs_idx + 1:
                    # Build path to origin directory (up to CacheStorage/{origin_hash})
                    origin_path = "/".join(parts[:cs_idx + 2])
                    origin_dirs.add(origin_path)

            callbacks.on_log(f"Found {len(origin_dirs)} CacheStorage origins for {browser}")

            # Step 2: For each origin, scan for cache directories with actual files
            # CacheStorage structure: {origin}/{cache_id}/{cache_files}
            # Files are at fixed depth - no need for recursive ** patterns
            for origin_dir in origin_dirs:
                # Scan for cache files inside this origin
                dirs_map = {}

                for file_pattern in cache_file_patterns:
                    # Fixed depth: {origin}/*/{file_pattern} covers {origin}/{cache_id}/{file}
                    # Also check {origin}/*/*/{file_pattern} for index-dir/the-real-index
                    search_patterns = [
                        f"{origin_dir}/*/{file_pattern}",
                        f"{origin_dir}/*/*/{file_pattern}",
                    ]

                    for search_pattern in search_patterns:
                        try:
                            matches = list(evidence_fs.iter_paths(search_pattern))
                            for file_path in matches:
                                path_obj = Path(file_path)
                                cache_dir = str(path_obj.parent)

                                if cache_dir not in dirs_map:
                                    # Extract profile from path
                                    profile = extract_profile_from_path(
                                        file_path, browser
                                    )
                                    dirs_map[cache_dir] = {
                                        "path": cache_dir,
                                        "browser": browser,
                                        "profile": profile,
                                        "cache_type": "cache_storage",
                                        "files": [],
                                    }

                                # Avoid duplicate files
                                existing_paths = {f["path"] for f in dirs_map[cache_dir]["files"]}
                                if file_path not in existing_paths:
                                    dirs_map[cache_dir]["files"].append({
                                        "path": file_path,
                                        "filename": path_obj.name,
                                    })
                        except Exception as e:
                            LOGGER.debug("Error searching %s: %s", search_pattern, e)

                # Add discovered directories with files
                for cache_dir_info in dirs_map.values():
                    if cache_dir_info["files"]:  # Only add if has files
                        callbacks.on_log(
                            f"Found {browser} CacheStorage: {cache_dir_info['profile']} "
                            f"({len(cache_dir_info['files'])} files)"
                        )
                        cache_directories.append(cache_dir_info)

        except Exception as e:
            error_msg = f"Error discovering CacheStorage for pattern '{pattern}': {e}"
            LOGGER.warning(error_msg, exc_info=True)
            callbacks.on_log(error_msg)

    return cache_directories


def extract_profile_from_path(file_path: str, browser: str) -> str:
    """Extract profile name from cache file path using shared Chromium parser."""
    return _extract_profile_from_path_shared(file_path) or "Default"
