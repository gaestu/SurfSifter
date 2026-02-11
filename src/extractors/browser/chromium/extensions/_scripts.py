"""
Chromium Extensions script extraction functions.

This module handles extraction of JavaScript files from extension directories,
including background scripts, service workers, and content scripts.

Extracted from extractor.py
"""
from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ._schemas import PATH_SEPARATOR
from core.logging import get_logger

if TYPE_CHECKING:
    from ....callbacks import ExtractorCallbacks

LOGGER = get_logger("extractors.browser.chromium.extensions.scripts")


def extract_extension_scripts(
    evidence_fs,
    ext_info: Dict[str, Any],
    ext_output_dir: Path,
    callbacks: "ExtractorCallbacks",
) -> List[Dict[str, Any]]:
    """
    Extract JavaScript files referenced in extension manifest.

    Extracts:
    - background.service_worker (Manifest V3)
    - background.scripts[] (Manifest V2)
    - content_scripts[].js[] (all versions)
    - web_accessible_resources/*.js

    Args:
        evidence_fs: Evidence filesystem interface
        ext_info: Extension info dict with manifest data
        ext_output_dir: Output directory for this extension
        callbacks: Extractor callbacks for progress/logging

    Returns:
        List of extracted script metadata dicts
    """
    extracted = []

    # Get extension base directory from manifest path
    source_path = ext_info["source_path"]
    ext_base_dir = PATH_SEPARATOR.join(source_path.split(PATH_SEPARATOR)[:-1])

    # Collect all script paths from manifest
    script_paths = _collect_script_paths(ext_info)

    # Extract each script file
    for script_rel_path in sorted(script_paths):
        if callbacks.is_cancelled():
            break

        script_info = _extract_single_script(
            evidence_fs,
            ext_base_dir,
            script_rel_path,
            ext_output_dir,
            ext_info,
        )

        if script_info:
            extracted.append(script_info)

    if extracted:
        callbacks.on_log(
            f"Extracted {len(extracted)} script(s) for {ext_info.get('name', 'unknown')}",
            "info",
        )

    return extracted


def _collect_script_paths(ext_info: Dict[str, Any]) -> set:
    """
    Collect all script paths from extension manifest.

    Args:
        ext_info: Extension info dict with manifest data

    Returns:
        Set of relative script paths
    """
    script_paths = set()

    # Manifest V3: background.service_worker
    background = ext_info.get("background") or {}
    if isinstance(background, dict):
        service_worker = background.get("service_worker")
        if service_worker:
            script_paths.add(service_worker)

        # Manifest V2: background.scripts[]
        bg_scripts = background.get("scripts", [])
        if isinstance(bg_scripts, list):
            script_paths.update(bg_scripts)

    # Content scripts (all versions)
    content_scripts = ext_info.get("content_scripts") or []
    if isinstance(content_scripts, list):
        for cs in content_scripts:
            if isinstance(cs, dict):
                js_files = cs.get("js", [])
                if isinstance(js_files, list):
                    script_paths.update(js_files)

    # Web accessible resources (V2 and V3 formats)
    war = ext_info.get("web_accessible_resources") or []
    if isinstance(war, list):
        for resource in war:
            if isinstance(resource, dict):
                # V3 format: {"resources": [...], "matches": [...]}
                resources = resource.get("resources", [])
                for r in resources:
                    if isinstance(r, str) and r.endswith('.js'):
                        script_paths.add(r)
            elif isinstance(resource, str) and resource.endswith('.js'):
                # V2 format: simple string list
                script_paths.add(resource)

    return script_paths


def _extract_single_script(
    evidence_fs,
    ext_base_dir: str,
    script_rel_path: str,
    ext_output_dir: Path,
    ext_info: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Extract a single script file from evidence.

    Args:
        evidence_fs: Evidence filesystem interface
        ext_base_dir: Base directory of extension in evidence
        script_rel_path: Relative path to script within extension
        ext_output_dir: Output directory for this extension
        ext_info: Extension info dict (for script classification)

    Returns:
        Script metadata dict or None if extraction failed
    """
    try:
        # Build full path in evidence
        script_full_path = f"{ext_base_dir}{PATH_SEPARATOR}{script_rel_path}"

        # Read script content
        script_content = evidence_fs.read_file(script_full_path)

        # Create safe local path preserving directory structure
        # Replace invalid filename characters
        safe_rel_path = re.sub(r'[<>:"|?*]', '_', script_rel_path)
        script_dest = ext_output_dir / safe_rel_path
        script_dest.parent.mkdir(parents=True, exist_ok=True)
        script_dest.write_bytes(script_content)

        # Calculate hashes
        script_md5 = hashlib.md5(script_content).hexdigest()
        script_sha256 = hashlib.sha256(script_content).hexdigest()

        script_info = {
            "relative_path": script_rel_path,
            "local_path": str(script_dest),
            "source_path": script_full_path,
            "size_bytes": len(script_content),
            "md5": script_md5,
            "sha256": script_sha256,
            "type": classify_script_type(script_rel_path, ext_info),
        }

        LOGGER.debug("Extracted script: %s (%d bytes)", script_rel_path, len(script_content))

        return script_info

    except FileNotFoundError:
        LOGGER.debug("Script not found: %s", script_rel_path)
        return None
    except Exception as e:
        LOGGER.debug("Failed to extract script %s: %s", script_rel_path, e)
        return None


def classify_script_type(script_path: str, ext_info: Dict[str, Any]) -> str:
    """
    Classify a script by its role in the extension.

    Args:
        script_path: Relative path to script
        ext_info: Extension info dict with manifest data

    Returns:
        Script type: "service_worker", "background_script", "content_script", or "resource"
    """
    background = ext_info.get("background") or {}

    # Check if it's the service worker
    if isinstance(background, dict):
        if script_path == background.get("service_worker"):
            return "service_worker"
        if script_path in (background.get("scripts") or []):
            return "background_script"

    # Check if it's a content script
    content_scripts = ext_info.get("content_scripts") or []
    for cs in content_scripts:
        if isinstance(cs, dict) and script_path in (cs.get("js") or []):
            return "content_script"

    return "resource"


def extract_extension_manifest(
    evidence_fs,
    ext_info: Dict[str, Any],
    output_dir: Path,
) -> Dict[str, Any]:
    """
    Copy extension manifest.json to workspace with hash calculation.

    Args:
        evidence_fs: Evidence filesystem interface
        ext_info: Extension info dict
        output_dir: Base output directory

    Returns:
        Updated ext_info dict with file_path, md5, sha256, file_size_bytes
    """
    source_path = ext_info["source_path"]
    browser = ext_info["browser"]
    ext_id = ext_info["extension_id"]
    version = ext_info.get("version", "unknown")
    profile = ext_info.get("profile", "Default")

    # Create safe filename prefix - include profile to avoid collisions
    safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', ext_id)[:32]
    safe_profile = re.sub(r'[^a-zA-Z0-9_-]', '_', profile)[:16]

    # Create extension subdirectory for all files
    ext_output_dir = output_dir / f"{browser}_{safe_profile}_{safe_id}_{version}"
    ext_output_dir.mkdir(parents=True, exist_ok=True)

    # Copy manifest.json
    manifest_dest = ext_output_dir / "manifest.json"

    content = evidence_fs.read_file(source_path)
    manifest_dest.write_bytes(content)

    # Calculate hashes for manifest
    md5 = hashlib.md5(content).hexdigest()
    sha256 = hashlib.sha256(content).hexdigest()

    ext_info["file_path"] = str(manifest_dest)
    ext_info["md5"] = md5
    ext_info["sha256"] = sha256
    ext_info["file_size_bytes"] = len(content)
    ext_info["_output_dir"] = ext_output_dir  # For script extraction

    return ext_info
