"""
Safari storage discovery for LocalStorage and IndexedDB.

Extends the shared Safari discovery infrastructure to find storage files
across multiple partitions. Handles both legacy paths
(``~/Library/Safari/LocalStorage/``) and modern WebsiteData paths
(``~/Library/WebKit/com.apple.Safari/WebsiteData/``).

Provides file-level extraction helpers (copy from evidence to workspace)
mirroring the Firefox storage ``extract_storage_file()`` pattern.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .._discovery import (
    discover_safari_files,
    discover_safari_files_fallback,
)

LOGGER = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────
# Discovery
# ─────────────────────────────────────────────────────────────────────


def discover_storage_files(
    evidence_conn: Any,
    evidence_id: int,
    evidence_fs: Any,
    config: Dict[str, Any],
    callbacks: Any = None,
) -> Dict[int, List[Dict[str, Any]]]:
    """
    Discover Safari storage files across all partitions.

    Searches for both LocalStorage and IndexedDB artifacts using the
    shared Safari discovery infrastructure with ``file_list`` multi-partition
    queries, falling back to filesystem walk.

    Args:
        evidence_conn: Evidence database connection.
        evidence_id: Evidence ID.
        evidence_fs: Evidence filesystem.
        config: Extraction config (``local_storage``, ``indexeddb`` booleans).
        callbacks: Optional ExtractorCallbacks.

    Returns:
        Dict mapping partition_index → list of file-info dicts enriched
        with ``storage_type`` and ``storage_format`` fields.
    """
    include_ls = config.get("local_storage", True)
    include_idb = config.get("indexeddb", True)

    # Build artifact names to search for
    artifact_names: List[str] = []
    if include_ls:
        artifact_names.extend(["local_storage", "local_storage_websitedata"])
    if include_idb:
        artifact_names.extend(["indexeddb", "indexeddb_websitedata"])

    if not artifact_names:
        return {}

    # Multi-partition discovery via file_list
    files_by_partition = discover_safari_files(
        evidence_conn,
        evidence_id,
        artifact_names=artifact_names,
        callbacks=callbacks,
    )

    # Fallback to filesystem iteration
    if not files_by_partition:
        files_by_partition = discover_safari_files_fallback(
            evidence_fs,
            artifact_names=artifact_names,
            callbacks=callbacks,
        )

    if not files_by_partition:
        return {}

    # Enrich file info with storage type classification
    enriched: Dict[int, List[Dict[str, Any]]] = {}

    for partition_idx, files_list in files_by_partition.items():
        enriched_files: List[Dict[str, Any]] = []
        for file_info in files_list:
            classified = _classify_storage_file(file_info)
            if classified is not None:
                enriched_files.append(classified)

        if enriched_files:
            enriched[partition_idx] = enriched_files

    return enriched


def _classify_storage_file(
    file_info: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Classify a discovered file as local_storage or indexeddb.

    Returns a *copy* of the file_info dict enriched with ``storage_type``
    and ``storage_format``, or None if the file is not relevant
    (e.g., WAL/SHM files, non-storage files).  The original dict is
    never mutated.
    """
    path = file_info.get("logical_path", "")
    filename = file_info.get("file_name", "")
    # Shallow copy to avoid mutating the caller's dict
    file_info = dict(file_info)
    path_lower = path.lower()

    # Skip WAL/SHM/journal files — we only parse main SQLite files
    if filename.endswith(("-wal", "-shm", "-journal")):
        return None

    # Classify by path
    if "localstorage" in path_lower or filename.endswith(".localstorage"):
        file_info["storage_type"] = "local_storage"
        file_info["storage_format"] = "safari_localstorage"
        # Extract origin from filename
        if filename.endswith(".localstorage"):
            from ._parsers import parse_origin_from_localstorage_filename
            file_info["origin"] = parse_origin_from_localstorage_filename(filename)
        return file_info

    if "indexeddb" in path_lower or "___indexeddb" in path_lower:
        if filename.endswith(".sqlite"):
            file_info["storage_type"] = "indexeddb"
            file_info["storage_format"] = "webkit_indexeddb"
            # Extract origin from path
            from ._parsers import parse_origin_from_indexeddb_path
            file_info["origin"] = parse_origin_from_indexeddb_path(path)
            return file_info
        # Skip non-SQLite files in IndexedDB directories
        return None

    # Fallback: .sqlite in a storage/database path is likely IndexedDB
    if filename.endswith(".sqlite") and ("storage" in path_lower or "database" in path_lower):
        file_info["storage_type"] = "indexeddb"
        file_info["storage_format"] = "webkit_indexeddb"
        from ._parsers import parse_origin_from_indexeddb_path
        file_info["origin"] = parse_origin_from_indexeddb_path(path)
        return file_info

    return None


# ─────────────────────────────────────────────────────────────────────
# File Extraction (copy from evidence to workspace)
# ─────────────────────────────────────────────────────────────────────


def extract_storage_file(
    evidence_fs: Any,
    loc: Dict[str, Any],
    output_dir: Path,
    callbacks: Any = None,
) -> Dict[str, Any]:
    """
    Extract (copy) a single storage file from evidence to workspace.

    Creates a safe filename incorporating the partition index and a path
    hash for deduplication. Computes MD5 and SHA256 hashes.

    Args:
        evidence_fs: Evidence filesystem to read from.
        loc: File location dict (from discovery).
        output_dir: Target directory for extracted files.
        callbacks: Optional ExtractorCallbacks.

    Returns:
        Enriched location dict with ``extracted_path``, ``md5``, ``sha256``,
        ``copy_status`` fields.
    """
    result = dict(loc)
    path_str = loc["logical_path"]

    try:
        content = evidence_fs.read_file(path_str)

        # Build safe filename with path hash for uniqueness
        original_name = Path(path_str).name
        path_hash = hashlib.sha256(path_str.encode()).hexdigest()[:8]
        partition_idx = loc.get("partition_index", 0)
        user = loc.get("user", "unknown")
        storage_type = loc.get("storage_type", "storage")

        safe_name = f"safari_{user}_p{partition_idx}_{path_hash}_{original_name}"
        dest_path = output_dir / safe_name
        dest_path.write_bytes(content)

        md5 = hashlib.md5(content).hexdigest()
        sha256 = hashlib.sha256(content).hexdigest()

        result["extracted_path"] = str(dest_path)
        result["md5"] = md5
        result["sha256"] = sha256
        result["size_bytes"] = len(content)
        result["copy_status"] = "ok"

        if callbacks:
            callbacks.on_log(f"Extracted: {path_str}", "debug")

    except Exception as e:
        LOGGER.warning("Failed to extract storage file %s: %s", path_str, e)
        result["copy_status"] = "error"
        result["error_message"] = str(e)
        if callbacks:
            callbacks.on_log(f"Failed to extract: {path_str} — {e}", "warning")

    return result
