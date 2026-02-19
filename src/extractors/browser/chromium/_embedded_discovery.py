"""
Embedded Chromium root discovery and artifact lookup helpers.

This module discovers Chromium-like roots from file_list signals and provides
helpers to run artifact discovery with embedded-root scoped patterns.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

from ..._shared.file_list_discovery import (
    FileListDiscoveryResult,
    FileListMatch,
    discover_from_file_list,
    glob_to_sql_like,
)
from ._patterns import CHROMIUM_BROWSERS, get_patterns_for_root


_SIGNAL_PATH_PATTERNS = [
    "%/Network/Cookies",
    "%/Cookies",
    "%/History",
    "%/Web Data",
    "%/Preferences",
    "%/Cache/Cache_Data/index",
    "%/Cache/index",
    "%/Local Storage/leveldb/%",
    "%/Session Storage/%",
]

_PROFILE_MARKERS = {"default", "guest profile", "system profile"}
_ARTIFACT_SUFFIXES = {
    "cookies": ["/network/cookies", "/cookies"],
    "history": ["/history"],
    "web_data": ["/web data"],
    "preferences": ["/preferences"],
    "cache": ["/cache/cache_data/index", "/cache/index"],
}

# Artifacts where patterns point to directories containing files
# (e.g., LevelDB databases, extension folders) rather than single files
_DIRECTORY_ARTIFACTS = frozenset({
    "local_storage",
    "session_storage",
    "indexeddb",
    "cache",
    "extensions",  # Extensions/ directory contains extension subdirectories
    "sync_data",   # Sync Data/ directory contains LevelDB subdirectory
})


@dataclass(frozen=True)
class EmbeddedRoot:
    """Embedded Chromium root candidate accepted by multi-signal detection."""

    root_path: str
    partition_index: Optional[int]
    signals: List[str]
    signal_count: int


def _normalize_path(path: str) -> str:
    normalized = str(path).replace("\\", "/")
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    return normalized.rstrip("/")


def _match_known_browser_root(path: str) -> bool:
    normalized = _normalize_path(path).lstrip("/").lower()

    known_roots: List[str] = []
    for info in CHROMIUM_BROWSERS.values():
        known_roots.extend(info.get("profile_roots", []))
        known_roots.extend(info.get("cache_roots", []))

    for pattern in known_roots:
        normalized_pattern = _normalize_path(pattern).lstrip("/").lower()
        if (
            fnmatch.fnmatch(normalized, normalized_pattern)
            or fnmatch.fnmatch(normalized, f"{normalized_pattern}/*")
        ):
            return True
    return False


def _detect_signal(file_path: str, file_name: str) -> Optional[str]:
    lower_path = _normalize_path(file_path).lower()
    lower_name = (file_name or "").lower()

    if any(lower_path.endswith(suffix) for suffix in _ARTIFACT_SUFFIXES["cookies"]):
        return "cookies"
    if any(lower_path.endswith(suffix) for suffix in _ARTIFACT_SUFFIXES["history"]):
        return "history"
    if any(lower_path.endswith(suffix) for suffix in _ARTIFACT_SUFFIXES["web_data"]):
        return "web_data"
    if any(lower_path.endswith(suffix) for suffix in _ARTIFACT_SUFFIXES["preferences"]):
        return "preferences"
    if any(lower_path.endswith(suffix) for suffix in _ARTIFACT_SUFFIXES["cache"]):
        return "cache"
    if "/local storage/leveldb/" in lower_path:
        return "local_storage"
    if "/session storage/" in lower_path:
        return "session_storage"

    if lower_name == "cookies":
        return "cookies"
    if lower_name == "history":
        return "history"
    if lower_name == "web data":
        return "web_data"
    if lower_name == "preferences":
        return "preferences"
    if lower_name == "index" and ("/cache/" in lower_path or "/leveldb/" in lower_path):
        return "cache"

    return None


def _extract_profile_root(file_path: str, signal: str) -> Optional[str]:
    normalized = _normalize_path(file_path)
    lower_path = normalized.lower()

    if signal in _ARTIFACT_SUFFIXES:
        for suffix in _ARTIFACT_SUFFIXES[signal]:
            if lower_path.endswith(suffix):
                return normalized[: -len(suffix)].rstrip("/")

    if signal == "local_storage":
        marker = "/local storage/leveldb/"
        idx = lower_path.find(marker)
        if idx >= 0:
            return normalized[:idx].rstrip("/")

    if signal == "session_storage":
        marker = "/session storage/"
        idx = lower_path.find(marker)
        if idx >= 0:
            return normalized[:idx].rstrip("/")

    return None


def _derive_embedded_root(profile_root: str) -> str:
    parts = _normalize_path(profile_root).split("/")
    if not parts:
        return _normalize_path(profile_root)

    last_part = parts[-1]
    last_lower = last_part.lower()
    if last_lower in _PROFILE_MARKERS or last_lower.startswith("profile "):
        return "/".join(parts[:-1]).rstrip("/")
    return "/".join(parts).rstrip("/")


def _dedupe_patterns(patterns: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    ordered: List[str] = []
    for pattern in patterns:
        if pattern not in seen:
            seen.add(pattern)
            ordered.append(pattern)
    return ordered


def discover_embedded_roots(evidence_conn, evidence_id: int) -> List[EmbeddedRoot]:
    """
    Discover embedded Chromium roots from file_list using multi-signal matching.

    Acceptance heuristic:
    - Group candidates by (partition_index, derived root path)
    - Require at least two distinct signal types for acceptance
    - Exclude candidates that already match known Chromium browser roots
    """
    result = discover_from_file_list(
        evidence_conn,
        evidence_id,
        path_patterns=_SIGNAL_PATH_PATTERNS,
        exclude_deleted=True,
    )

    signals_by_root: Dict[Tuple[Optional[int], str], Set[str]] = {}

    for match in result.get_all_matches():
        signal = _detect_signal(match.file_path, match.file_name)
        if signal is None:
            continue

        profile_root = _extract_profile_root(match.file_path, signal)
        if not profile_root:
            continue

        root_path = _derive_embedded_root(profile_root)
        if not root_path or _match_known_browser_root(root_path):
            continue

        key = (match.partition_index, root_path)
        signals_by_root.setdefault(key, set()).add(signal)

    embedded_roots: List[EmbeddedRoot] = []
    for (partition_index, root_path), signals in signals_by_root.items():
        if len(signals) < 2:
            continue
        signal_list = sorted(signals)
        embedded_roots.append(
            EmbeddedRoot(
                root_path=root_path,
                partition_index=partition_index,
                signals=signal_list,
                signal_count=len(signal_list),
            )
        )

    embedded_roots.sort(key=lambda item: (item.partition_index is None, item.partition_index, item.root_path))
    return embedded_roots


def get_embedded_root_paths(
    embedded_roots: List[EmbeddedRoot],
    partition_index: Optional[int] = None,
) -> List[str]:
    """Return discovered embedded root paths, optionally filtered by partition."""
    roots = []
    for root in embedded_roots:
        if partition_index is not None and root.partition_index != partition_index:
            continue
        roots.append(root.root_path)
    return roots


def _merge_discovery_results(
    base: FileListDiscoveryResult,
    extra: FileListDiscoveryResult,
) -> FileListDiscoveryResult:
    merged: Dict[int, List[FileListMatch]] = {}
    seen: Dict[int, Set[Tuple[str, str, Optional[int]]]] = {}

    for result in (base, extra):
        for partition_idx, matches in result.matches_by_partition.items():
            merged.setdefault(partition_idx, [])
            seen.setdefault(partition_idx, set())

            for match in matches:
                key = (match.file_path, match.file_name, match.inode)
                if key in seen[partition_idx]:
                    continue
                seen[partition_idx].add(key)
                merged[partition_idx].append(match)

    total_matches = sum(len(matches) for matches in merged.values())
    return FileListDiscoveryResult(
        matches_by_partition=merged,
        total_matches=total_matches,
        partitions_with_matches=sorted(merged.keys()),
        query_info="merged_embedded_discovery",
    )


def discover_artifacts_with_embedded_roots(
    evidence_conn,
    evidence_id: int,
    *,
    artifact: str,
    filename_patterns: Optional[List[str]] = None,
    path_patterns: Optional[List[str]] = None,
) -> Tuple[FileListDiscoveryResult, List[EmbeddedRoot]]:
    """
    Discover artifact files via file_list, including embedded Chromium roots.

    Returns:
        Tuple of (merged FileListDiscoveryResult, discovered embedded roots)
    """
    # For directory-based artifacts, ensure path patterns end with wildcard
    # so they match files inside the directory
    normalized_path_patterns = path_patterns
    if path_patterns and artifact in _DIRECTORY_ARTIFACTS:
        normalized_path_patterns = []
        for p in path_patterns:
            if not p.endswith('%'):
                p = p.rstrip('/') + '/%'
            normalized_path_patterns.append(p)

    base_result = discover_from_file_list(
        evidence_conn,
        evidence_id,
        filename_patterns=filename_patterns,
        path_patterns=normalized_path_patterns,
        exclude_deleted=True,
    )

    embedded_roots = discover_embedded_roots(evidence_conn, evidence_id)
    merged_result = base_result

    for root in embedded_roots:
        partition_filter = {root.partition_index} if root.partition_index is not None else None

        embedded_patterns: List[str] = []
        for flat_profile in (False, True):
            for pattern in get_patterns_for_root(root.root_path, artifact, flat_profile=flat_profile):
                sql_pattern = glob_to_sql_like(pattern)
                # For directory-based artifacts (storage, cache), ensure pattern
                # ends with wildcard to match files inside directories
                if artifact in _DIRECTORY_ARTIFACTS and not sql_pattern.endswith('%'):
                    sql_pattern = sql_pattern.rstrip('/') + '/%'
                embedded_patterns.append(sql_pattern)

        embedded_patterns = _dedupe_patterns(embedded_patterns)
        if not embedded_patterns:
            continue

        embedded_result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=filename_patterns,
            path_patterns=embedded_patterns,
            partition_filter=partition_filter,
            exclude_deleted=True,
        )
        merged_result = _merge_discovery_results(merged_result, embedded_result)

    return merged_result, embedded_roots
