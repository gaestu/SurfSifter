"""Firefox cache2 doomed/trash entry recovery.

Discovers cache entries in non-standard locations (``doomed/``, ``trash/``)
and correlates them with records from the binary cache index.

The ``doomed/`` directory contains entries marked for deletion but not yet
physically removed.  The ``trash/`` directory (with numbered sub-directories)
holds recently deleted files.  Both are forensically valuable because they
may contain browsing artifacts the user attempted to clear.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple

from ._index import CacheIndexEntry

LOGGER = logging.getLogger(__name__)


def discover_all_cache_entries(
    cache_root: Path,
) -> Dict[str, List[Path]]:
    """Discover all cache2 entry files, including doomed and trash.

    Args:
        cache_root: Path to the ``cache2/`` directory.

    Returns:
        Dict mapping source type to entry file paths::

            {
                "entries": [Path, ...],
                "doomed":  [Path, ...],
                "trash":   [Path, ...],
            }
    """
    result: Dict[str, List[Path]] = {"entries": [], "doomed": [], "trash": []}

    # Active entries
    entries_dir = cache_root / "entries"
    if entries_dir.exists():
        result["entries"] = [
            p for p in entries_dir.iterdir() if p.is_file()
        ]

    # Doomed entries (marked for deletion)
    doomed_dir = cache_root / "doomed"
    if doomed_dir.exists():
        result["doomed"] = [
            p for p in doomed_dir.iterdir() if p.is_file()
        ]
        if result["doomed"]:
            LOGGER.info("Found %d doomed cache entries", len(result["doomed"]))

    # Trash entries (recently deleted — trash has numbered sub-directories)
    trash_dir = cache_root / "trash"
    if trash_dir.exists():
        for subdir in trash_dir.iterdir():
            if subdir.is_dir():
                result["trash"].extend(
                    p for p in subdir.iterdir() if p.is_file()
                )
        if result["trash"]:
            LOGGER.info("Found %d trash cache entries", len(result["trash"]))

    return result


def correlate_index_with_files(
    index_entries: List[CacheIndexEntry],
    discovered_files: Dict[str, List[Path]],
) -> List[Dict[str, Any]]:
    """Correlate index entries with discovered entry files.

    For each index record, determines whether a corresponding entry file
    exists and, if so, in which directory (``entries``, ``doomed``, or
    ``trash``).

    Args:
        index_entries: Parsed ``CacheIndexEntry`` objects from the index.
        discovered_files: Output of :func:`discover_all_cache_entries`.

    Returns:
        List of enriched dicts — one per index entry — with the keys from
        ``CacheIndexEntry`` plus ``has_file``, ``file_source``, and
        ``file_path``.
    """
    # Build hash → (source, path) lookup
    file_lookup: Dict[str, Tuple[str, Path]] = {}
    for source, paths in discovered_files.items():
        for path in paths:
            # Entry filenames are the hex hash (upper-case)
            file_hash = path.name.upper()
            file_lookup[file_hash] = (source, path)

    results: List[Dict[str, Any]] = []
    for entry in index_entries:
        file_info = file_lookup.get(entry.hash)

        results.append({
            "hash": entry.hash,
            "frecency": entry.frecency,
            "origin_attrs_hash": entry.origin_attrs_hash,
            "on_start_time": entry.on_start_time,
            "on_stop_time": entry.on_stop_time,
            "content_type": entry.content_type,
            "content_type_name": entry.content_type_name,
            "file_size_kb": entry.file_size_kb,
            "raw_flags": entry.flags,
            "is_initialized": entry.is_initialized,
            "is_anonymous": entry.is_anonymous,
            "is_removed": entry.is_removed,
            "is_pinned": entry.is_pinned,
            "has_alt_data": entry.has_alt_data,
            "has_file": file_info is not None,
            "file_source": file_info[0] if file_info else None,
            "file_path": file_info[1] if file_info else None,
        })

    # Report statistics
    total = len(results)
    with_files = sum(1 for r in results if r["has_file"])
    removed = sum(1 for r in results if r["is_removed"])

    LOGGER.info(
        "Cache index correlation: %d entries, %d with files, %d marked removed",
        total,
        with_files,
        removed,
    )

    return results
