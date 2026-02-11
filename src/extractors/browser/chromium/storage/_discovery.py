"""
Multi-partition discovery for Chromium Browser Storage Extractor.

Provides discovery of Local Storage, Session Storage, and IndexedDB
directories across ALL partitions using file_list SQL queries.

Initial implementation with multi-partition support
Added path hash to prevent overwrites, use _patterns.py for discovery
"""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, TYPE_CHECKING

from .._patterns import CHROMIUM_BROWSERS, get_artifact_patterns, get_patterns

if TYPE_CHECKING:
    from extractors.callbacks import ExtractorCallbacks

LOGGER = logging.getLogger(__name__)


def extract_profile_from_path(path: str) -> str:
    """
    Extract browser profile name from file path.

    Handles both standard Chrome/Edge ("User Data/Default/") and
    Opera flat profiles.
    """
    parts = path.replace("\\", "/").split("/")

    try:
        idx = parts.index("User Data")
        return parts[idx + 1] if idx + 1 < len(parts) else "Default"
    except (ValueError, IndexError):
        pass

    for part in parts:
        if part.startswith("Profile "):
            return part
        if part.lower() == "default":
            return "Default"

    return "Default"


def detect_browser_from_path(path: str) -> Optional[str]:
    """Detect browser type from file path."""
    path_lower = path.lower()

    # Check for specific browser paths
    if "google" in path_lower and "chrome" in path_lower:
        if "canary" in path_lower or "sxs" in path_lower:
            return "chrome_canary"
        if "beta" in path_lower:
            return "chrome_beta"
        if "dev" in path_lower or "unstable" in path_lower:
            return "chrome_dev"
        return "chrome"

    if "microsoft" in path_lower and "edge" in path_lower:
        if "canary" in path_lower or "sxs" in path_lower:
            return "edge_canary"
        if "beta" in path_lower:
            return "edge_beta"
        if "dev" in path_lower:
            return "edge_dev"
        return "edge"

    if "bravesoftware" in path_lower or "brave-browser" in path_lower:
        if "nightly" in path_lower:
            return "brave_nightly"
        if "beta" in path_lower:
            return "brave_beta"
        return "brave"

    if "opera" in path_lower:
        if "opera gx" in path_lower or "operagx" in path_lower:
            return "opera_gx"
        return "opera"

    if "chromium" in path_lower:
        return "chromium"

    return None


def discover_storage_multi_partition(
    evidence_conn,
    evidence_id: int,
    evidence_fs,
    browsers: List[str],
    config: Dict[str, Any],
    callbacks: "ExtractorCallbacks",
) -> Dict[int, List[Dict]]:
    """
    Discover storage directories across ALL partitions.

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
    if config.get("session_storage", True):
        storage_configs.append("session_storage")
    if config.get("indexeddb", True):
        storage_configs.append("indexeddb")

    for storage_type in storage_configs:
        # Build patterns using _patterns.py
        combined_patterns = _build_storage_path_patterns(browsers, storage_type)

        if not combined_patterns:
            continue

        # Query file_list for directories matching storage patterns
        # We look for files INSIDE the storage directories (e.g., MANIFEST, .ldb files)
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["MANIFEST-*", "*.ldb", "*.log", "CURRENT", "LOCK"],
            path_patterns=combined_patterns if combined_patterns else None,
        )

        if result.is_empty:
            callbacks.on_log(f"No {storage_type} found in file_list", "debug")
            continue

        # Group by storage directory (parent of the matched files)
        for partition_idx, matches in result.matches_by_partition.items():
            seen_dirs: Set[str] = set()

            for match in matches:
                # Get parent directory (the LevelDB directory)
                dir_path = str(Path(match.file_path).parent)

                if dir_path in seen_dirs:
                    continue
                seen_dirs.add(dir_path)

                # Detect browser from path
                browser = detect_browser_from_path(dir_path)
                if browser and browser not in browsers:
                    continue  # Skip browsers not in selection

                profile = extract_profile_from_path(dir_path)
                display_name = CHROMIUM_BROWSERS.get(browser, {}).get("display_name", browser) if browser else "Chromium"

                loc = {
                    "logical_path": dir_path,
                    "browser": browser or "chromium",
                    "profile": profile,
                    "storage_type": storage_type,
                    "display_name": display_name,
                    "partition_index": partition_idx,
                    "inode": match.inode,
                }

                if partition_idx not in files_by_partition:
                    files_by_partition[partition_idx] = []
                files_by_partition[partition_idx].append(loc)

                callbacks.on_log(
                    f"Found {browser or 'chromium'} {storage_type} on partition {partition_idx}: {dir_path}",
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
            # Add partition_index to all locations
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
        storage_type: Storage type key ("local_storage", "session_storage", "indexeddb")

    Returns:
        List of SQL LIKE patterns for file_list queries
    """
    patterns = set()

    for browser in browsers:
        if browser not in CHROMIUM_BROWSERS:
            continue

        try:
            # Use the canonical patterns from _patterns.py
            artifact_patterns = get_patterns(browser, storage_type)
            for pattern in artifact_patterns:
                # Convert glob pattern to SQL LIKE pattern:
                # - Replace * with %
                # - Wrap with % for partial matching
                sql_pattern = pattern.replace("*", "%")
                # Ensure we match subdirectories too
                patterns.add(f"%{sql_pattern}%")
        except ValueError:
            # Artifact type not defined for this browser
            LOGGER.debug("No %s patterns defined for %s", storage_type, browser)
            continue

    return list(patterns) if patterns else ["%"]


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
        if browser_key not in CHROMIUM_BROWSERS:
            continue

        display_name = CHROMIUM_BROWSERS[browser_key]["display_name"]

        # Local Storage
        if config.get("local_storage", True):
            patterns = get_artifact_patterns(browser_key, "local_storage")
            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        try:
                            stat_info = evidence_fs.stat(path_str)
                            if stat_info.is_dir:
                                profile = extract_profile_from_path(path_str)
                                locations.append({
                                    "logical_path": path_str,
                                    "browser": browser_key,
                                    "profile": profile,
                                    "storage_type": "local_storage",
                                    "display_name": display_name,
                                })
                                callbacks.on_log(f"Found {browser_key} Local Storage: {path_str}", "info")
                        except FileNotFoundError:
                            pass
                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        # Session Storage
        if config.get("session_storage", True):
            patterns = get_artifact_patterns(browser_key, "session_storage")
            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        try:
                            stat_info = evidence_fs.stat(path_str)
                            if stat_info.is_dir:
                                profile = extract_profile_from_path(path_str)
                                locations.append({
                                    "logical_path": path_str,
                                    "browser": browser_key,
                                    "profile": profile,
                                    "storage_type": "session_storage",
                                    "display_name": display_name,
                                })
                                callbacks.on_log(f"Found {browser_key} Session Storage: {path_str}", "info")
                        except FileNotFoundError:
                            pass
                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        # IndexedDB
        if config.get("indexeddb", True):
            patterns = get_artifact_patterns(browser_key, "indexeddb")
            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        try:
                            stat_info = evidence_fs.stat(path_str)
                            if stat_info.is_dir:
                                profile = extract_profile_from_path(path_str)
                                locations.append({
                                    "logical_path": path_str,
                                    "browser": browser_key,
                                    "profile": profile,
                                    "storage_type": "indexeddb",
                                    "display_name": display_name,
                                })
                                callbacks.on_log(f"Found {browser_key} IndexedDB: {path_str}", "info")
                        except FileNotFoundError:
                            pass
                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

    return locations


def extract_storage_directory(
    evidence_fs,
    loc: Dict,
    output_dir: Path,
    run_id: str,
    callbacks: "ExtractorCallbacks"
) -> Dict:
    """
    Copy storage directory from evidence to workspace.

    Includes partition_index in output path to prevent overwrites.

    Args:
        evidence_fs: Evidence filesystem handle
        loc: Location dict with logical_path, browser, profile, partition_index
        output_dir: Output directory
        run_id: Run ID
        callbacks: Extractor callbacks

    Returns:
        Dict with copy status and metadata
    """
    try:
        source_path = loc["logical_path"]
        browser = loc["browser"]
        profile = loc["profile"]
        storage_type = loc["storage_type"]
        partition_index = loc.get("partition_index", 0)

        safe_profile = profile.replace(' ', '_').replace('/', '_')
        # Include partition_index AND path hash in dest name to prevent overwrites
        # (handles case where same browser/profile exists in different user folders)
        path_hash = hashlib.md5(source_path.encode()).hexdigest()[:8]
        dest_name = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_{storage_type}"
        dest_path = output_dir / dest_name

        callbacks.on_log(f"Copying {source_path} to {dest_name}", "info")

        # Copy directory recursively
        normalized_source = source_path.strip("/")
        dest_path.mkdir(parents=True, exist_ok=True)
        file_count = 0
        total_size = 0

        # Collect all file paths
        callbacks.on_log(f"Scanning {storage_type} directory...", "info")
        file_paths = list(evidence_fs.walk_directory(normalized_source))
        total_files = len(file_paths)

        if total_files == 0:
            callbacks.on_log(f"No files found in {storage_type}", "warning")
            return {
                "copy_status": "ok",
                "browser": browser,
                "profile": profile,
                "storage_type": storage_type,
                "logical_path": source_path,
                "extracted_path": str(dest_name),
                "partition_index": partition_index,
                "file_count": 0,
                "total_size": 0,
            }

        callbacks.on_log(f"Found {total_files} files to copy", "info")

        for idx, file_path in enumerate(file_paths):
            if callbacks.is_cancelled():
                callbacks.on_log("Extraction cancelled", "warning")
                return {
                    "copy_status": "cancelled",
                    "browser": browser,
                    "profile": profile,
                    "storage_type": storage_type,
                    "logical_path": source_path,
                    "extracted_path": str(dest_name),
                    "partition_index": partition_index,
                    "file_count": file_count,
                    "total_size": total_size,
                }

            if idx % 10 == 0:
                pct = int((idx / total_files) * 100)
                callbacks.on_log(f"Copying file {idx + 1}/{total_files} ({pct}%)", "debug")

            try:
                normalized_file = file_path.strip("/")

                # Calculate relative path from source directory
                if normalized_file.startswith(normalized_source + "/"):
                    rel_path = normalized_file[len(normalized_source) + 1:]
                else:
                    rel_path = Path(normalized_file).name

                dest_file = dest_path / rel_path
                dest_file.parent.mkdir(parents=True, exist_ok=True)

                # Use streaming for memory efficiency
                file_size = 0
                try:
                    with open(dest_file, "wb") as out_f:
                        for chunk in evidence_fs.open_for_stream(file_path):
                            out_f.write(chunk)
                            file_size += len(chunk)
                except AttributeError:
                    # Fallback if open_for_stream not available
                    content = evidence_fs.read_file(file_path)
                    dest_file.write_bytes(content)
                    file_size = len(content)

                file_count += 1
                total_size += file_size
            except Exception as e:
                LOGGER.debug("Failed to copy %s: %s", file_path, e)

        callbacks.on_log(f"Copied {file_count} files ({total_size:,} bytes)", "info")

        return {
            "copy_status": "ok",
            "browser": browser,
            "profile": profile,
            "storage_type": storage_type,
            "logical_path": source_path,
            "extracted_path": str(dest_name),
            "partition_index": partition_index,
            "file_count": file_count,
            "total_size": total_size,
        }

    except Exception as e:
        callbacks.on_log(f"Failed to extract {loc['logical_path']}: {e}", "error")
        return {
            "copy_status": "error",
            "browser": loc.get("browser"),
            "profile": loc.get("profile"),
            "storage_type": loc.get("storage_type"),
            "logical_path": loc.get("logical_path"),
            "partition_index": loc.get("partition_index", 0),
            "error_message": str(e),
        }
