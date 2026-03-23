"""
App Extension discovery via file_list table.

Discovers Safari App Extension (.appex) bundles across all partitions
by querying the file_list for Info.plist and manifest.json files within
.appex bundle paths.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from extractors._shared.file_list_discovery import (
    check_file_list_available,
    discover_from_file_list,
)
from .._patterns import extract_user_from_path

LOGGER = logging.getLogger(__name__)


def discover_app_extensions(
    evidence_conn,
    evidence_id: int,
    callbacks=None,
) -> Dict[int, List[Dict[str, Any]]]:
    """Discover Safari App Extension .appex bundles via file_list table.

    Queries for Info.plist and manifest.json files in .appex bundle paths.
    Groups results by .appex bundle path.

    Args:
        evidence_conn: Evidence database connection (may be None).
        evidence_id: Evidence ID.
        callbacks: Optional ExtractorCallbacks for logging.

    Returns:
        Dict mapping partition_index -> list of bundle dicts with keys:
        - bundle_path: path to .appex bundle
        - info_plist_path: path to Contents/Info.plist
        - manifest_json_path: optional path to manifest.json
        - partition_index: partition index
        - user: extracted username
    """
    if evidence_conn is None:
        return {}

    available, _count = check_file_list_available(evidence_conn, evidence_id)
    if not available:
        if callbacks:
            callbacks.on_log(
                "File list not available for app extension discovery", "info"
            )
        return {}

    result = discover_from_file_list(
        evidence_conn,
        evidence_id,
        filename_patterns=["Info.plist", "manifest.json"],
        path_patterns=[
            "%Applications%.appex/Contents%",
            "%AppExtensions%",
            "%Safari/Extensions%",
        ],
    )

    if result.is_empty:
        if callbacks:
            callbacks.on_log(
                "No .appex bundles found in file_list", "debug"
            )
        return {}

    if callbacks:
        callbacks.on_log(
            f"App extension discovery: {result.get_partition_summary()}", "info"
        )

    # Group by .appex bundle path
    bundles_by_partition: Dict[int, Dict[str, Dict[str, Any]]] = {}

    for partition_idx, matches in result.matches_by_partition.items():
        bundles: Dict[str, Dict[str, Any]] = {}

        for match in matches:
            path = match.file_path
            # Find the .appex boundary in the path
            bundle_path = _extract_appex_bundle_path(path)
            if not bundle_path:
                continue

            if bundle_path not in bundles:
                user = extract_user_from_path(path)
                bundles[bundle_path] = {
                    "bundle_path": bundle_path,
                    "info_plist_path": None,
                    "manifest_json_path": None,
                    "partition_index": partition_idx,
                    "user": user or "Default",
                }

            if match.file_name == "Info.plist":
                bundles[bundle_path]["info_plist_path"] = path
            elif match.file_name == "manifest.json":
                bundles[bundle_path]["manifest_json_path"] = path

        if bundles:
            if partition_idx not in bundles_by_partition:
                bundles_by_partition[partition_idx] = {}
            bundles_by_partition[partition_idx].update(bundles)

    # Filter to only include bundles with Info.plist and flatten
    result_by_partition: Dict[int, List[Dict[str, Any]]] = {}
    for partition_idx, bundles in bundles_by_partition.items():
        valid = [b for b in bundles.values() if b["info_plist_path"] is not None]
        if valid:
            result_by_partition[partition_idx] = valid

    return result_by_partition


def _extract_appex_bundle_path(path: str) -> Optional[str]:
    """Extract the .appex bundle root path from a file path.

    E.g. '/Applications/Foo.app/Contents/PlugIns/Bar.appex/Contents/Info.plist'
         -> '/Applications/Foo.app/Contents/PlugIns/Bar.appex'
    """
    lower = path.lower()
    idx = lower.find(".appex/")
    if idx < 0:
        # Check if path ends with .appex
        if lower.endswith(".appex"):
            return path
        return None
    return path[: idx + len(".appex")]
