"""
Multi-partition file list discovery for Safari extractors.

Provides shared utilities so Safari extractors can discover artifact files
across ALL partitions in an E01 image via the pre-populated ``file_list``
table, falling back to single-partition filesystem iteration when the
file list is unavailable.

Usage::

    from .._discovery import discover_safari_files, discover_safari_files_fallback

    # Multi-partition: queries file_list table
    files_by_partition = discover_safari_files(
        evidence_conn, evidence_id,
        artifact_names=["history"],
        callbacks=callbacks,
    )

    # Fallback: single-partition filesystem walk
    if not files_by_partition:
        files_by_partition = discover_safari_files_fallback(
            evidence_fs, artifact_names=["history"], callbacks=callbacks,
        )
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..._shared.file_list_discovery import (
    check_file_list_available,
    discover_from_file_list,
    glob_to_sql_like,
)
from ._patterns import (
    SAFARI_ARTIFACTS,
    SAFARI_BROWSERS,
    extract_user_from_path,
    get_patterns,
)

__all__ = [
    "discover_safari_files",
    "discover_safari_files_fallback",
    "get_safari_filename_patterns",
    "get_safari_path_patterns",
]

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pattern helpers
# ---------------------------------------------------------------------------


def get_safari_filename_patterns(artifact_names: List[str]) -> List[str]:
    """
    Extract unique filename patterns for the given Safari artifact types.

    For compound patterns that include directory separators
    (e.g. ``"Favicon Cache/*"``), the filename/glob part after the last
    ``/`` is returned.

    Returns:
        De-duplicated list of filename patterns suitable for
        :func:`discover_from_file_list`'s *filename_patterns* parameter.
    """
    filenames: List[str] = []
    seen: set[str] = set()

    for artifact_name in artifact_names:
        if artifact_name not in SAFARI_ARTIFACTS:
            continue
        for pattern in SAFARI_ARTIFACTS[artifact_name]["patterns"]:
            # Extract filename portion from path patterns
            if "/" in pattern:
                filename = pattern.rsplit("/", 1)[-1]
            else:
                filename = pattern
            if filename not in seen:
                seen.add(filename)
                filenames.append(filename)

    return filenames


def get_safari_path_patterns(artifact_names: List[str]) -> List[str]:
    """
    Build SQL LIKE path patterns from Safari root directories.

    Uses the root directories defined in ``_patterns.SAFARI_BROWSERS``
    (selecting the correct root type per artifact) and converts them to
    SQL LIKE format with ``%`` wrappers so they match anywhere in
    ``file_list.file_path``.

    Only *non-absolute* roots are used; the leading ``%`` already covers
    paths with or without leading ``/``.

    Returns:
        Sorted, de-duplicated list of SQL LIKE patterns.
    """
    patterns: set[str] = set()

    for artifact_name in artifact_names:
        if artifact_name not in SAFARI_ARTIFACTS:
            continue

        root_type = SAFARI_ARTIFACTS[artifact_name]["root_type"]
        browser = SAFARI_BROWSERS["safari"]
        root_key = f"{root_type}_roots"
        roots = browser.get(root_key, browser["profile_roots"])

        for root in roots:
            # Skip absolute variants — the leading % already handles both
            if root.startswith("/"):
                continue

            like = glob_to_sql_like(root)
            patterns.add(f"%{like}%")

    return sorted(patterns)


# ---------------------------------------------------------------------------
# Discovery functions
# ---------------------------------------------------------------------------


def discover_safari_files(
    evidence_conn: Any,
    evidence_id: int,
    artifact_names: List[str],
    callbacks: Any = None,
) -> Dict[int, List[Dict[str, Any]]]:
    """
    Discover Safari artifact files across all partitions via ``file_list``.

    Args:
        evidence_conn: Evidence database connection (may be *None*).
        evidence_id: Evidence ID.
        artifact_names: Safari artifact keys as defined in
            ``_patterns.SAFARI_ARTIFACTS`` (e.g. ``["history"]``,
            ``["sessions", "recently_closed_tabs"]``).
        callbacks: Optional :class:`ExtractorCallbacks` for logging.

    Returns:
        Dict mapping ``partition_index`` → list of file-info dicts.
        Each dict contains: ``logical_path``, ``file_name``, ``browser``,
        ``user``, ``display_name``, ``partition_index``, ``inode``,
        ``size_bytes``.
        Returns empty dict when ``file_list`` is unavailable or has no
        matches.
    """
    if evidence_conn is None:
        return {}

    available, count = check_file_list_available(evidence_conn, evidence_id)
    if not available:
        if callbacks:
            callbacks.on_log(
                "File list not available, using filesystem discovery", "info"
            )
        return {}

    filename_patterns = get_safari_filename_patterns(artifact_names)
    path_patterns = get_safari_path_patterns(artifact_names)

    if not filename_patterns and not path_patterns:
        LOGGER.debug("No patterns generated for artifacts: %s", artifact_names)
        return {}

    result = discover_from_file_list(
        evidence_conn,
        evidence_id,
        filename_patterns=filename_patterns or None,
        path_patterns=path_patterns or None,
    )

    if result.is_empty:
        if callbacks:
            callbacks.on_log(
                "File list query returned no Safari matches", "debug"
            )
        return {}

    if callbacks:
        callbacks.on_log(
            f"File list discovery: {result.get_partition_summary()}", "info"
        )

    # Convert FileListMatch objects to extractor-compatible dicts
    files_by_partition: Dict[int, List[Dict[str, Any]]] = {}

    for partition_idx, matches in result.matches_by_partition.items():
        files_list: List[Dict[str, Any]] = []
        for match in matches:
            user = extract_user_from_path(match.file_path)
            files_list.append(
                {
                    "logical_path": match.file_path,
                    "file_name": match.file_name,
                    "browser": "safari",
                    "user": user or "Default",
                    "display_name": "Apple Safari",
                    "partition_index": partition_idx,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                }
            )
        if files_list:
            files_by_partition[partition_idx] = files_list

    return files_by_partition


def discover_safari_files_fallback(
    evidence_fs: Any,
    artifact_names: List[str],
    callbacks: Any = None,
) -> Dict[int, List[Dict[str, Any]]]:
    """
    Fallback discovery using filesystem iteration (single partition).

    Used when ``file_list`` is unavailable or returns no matches.
    Returns results in the same dict format as :func:`discover_safari_files`.
    """
    discovered: List[str] = []
    seen_paths: set[str] = set()

    for artifact_name in artifact_names:
        try:
            patterns = get_patterns(artifact_name)
        except ValueError:
            continue

        for pattern in patterns:
            try:
                for path_str in evidence_fs.iter_paths(pattern):
                    if path_str not in seen_paths:
                        seen_paths.add(path_str)
                        discovered.append(path_str)
            except Exception as exc:
                LOGGER.debug("Pattern %s failed: %s", pattern, exc)

    if not discovered:
        return {}

    partition_index = getattr(evidence_fs, "partition_index", 0)
    files_list: List[Dict[str, Any]] = []
    for path_str in discovered:
        user = extract_user_from_path(path_str)
        file_name = Path(path_str).name
        files_list.append(
            {
                "logical_path": path_str,
                "file_name": file_name,
                "browser": "safari",
                "user": user or "Default",
                "display_name": "Apple Safari",
                "partition_index": partition_index,
                "inode": None,
                "size_bytes": None,
            }
        )

    return {partition_index: files_list}
