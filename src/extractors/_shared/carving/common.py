"""
Shared utilities for image carving extractors.

This module provides common functionality for:
- Image verification
- File summarization
- Manifest generation
- Path handling
- Run ID generation

Using these helpers allows extractors to remain independent modules while avoiding
code duplication.
"""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from PIL import Image

from core.hashing import hash_file
from core.image_codecs import ensure_pillow_heif_registered
from core.manifest import validate_image_carving_manifest, ManifestValidationError
from core.logging import get_logger

LOGGER = get_logger("extractors._shared.carving.common")


def generate_run_id() -> str:
    """
    Generate a unique run ID for extraction jobs.

    Format: YYYYMMDD_HHMM_UUID (e.g., 20240101_1200_a1b2c3d4)
    Includes UUID to prevent collisions if multiple runs occur simultaneously.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    unique_id = str(uuid.uuid4())[:8]
    return f"{timestamp}_{unique_id}"


def safe_rel_path(path: Path, base: Path) -> str:
    """
    Return path relative to base when possible, otherwise return absolute path.
    Safe for cross-platform path handling.
    """
    try:
        return path.relative_to(base).as_posix()
    except ValueError:
        return path.as_posix()


def verify_image(path: Path) -> Tuple[Optional[bool], Optional[str]]:
    """
    Verify image readability with Pillow (lightweight sanity check).

    Returns:
        Tuple[Optional[bool], Optional[str]]: (is_valid, error_message)
        - (True, None): Valid image
        - (False, error): Invalid image
        - (None, error): File missing
    """
    if not path.exists():
        return None, "file missing"
    try:
        ensure_pillow_heif_registered()
        with Image.open(path) as img:
            img.verify()
        return True, None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def get_evidence_context(evidence_fs) -> Dict[str, Any]:
    """
    Extract forensic context from evidence filesystem.

    Handles both PyEwfTskFS (image_handle) and MountedFS (root) attributes.
    """
    context: Dict[str, Any] = {}

    # Try to get E01 metadata
    if hasattr(evidence_fs, "image_handle"):
        try:
            context["image_type"] = "e01"
            # Add more E01 metadata if available
        except Exception:
            pass

    # Get mount point / root
    if hasattr(evidence_fs, "root"):
        context["mount_point"] = str(evidence_fs.root)
    elif hasattr(evidence_fs, "mount_point"):
        mount_point = getattr(evidence_fs, "mount_point", None)
        if isinstance(mount_point, (str, Path)):
            context["mount_point"] = str(mount_point)

    # Get EWF paths
    if hasattr(evidence_fs, "ewf_paths"):
        ewf_paths = getattr(evidence_fs, "ewf_paths", None)
        if isinstance(ewf_paths, (list, tuple)):
            context["ewf_paths"] = [str(p) for p in ewf_paths]

    return context


def summarize_carved_files(
    carved_files: List[Path],
    output_dir: Path,
    audit_index: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Build manifest entries and aggregate stats for carved files.

    Args:
        carved_files: List of paths to carved files
        output_dir: Root output directory (for relative paths)
        audit_index: Optional dictionary mapping filenames to audit info (offsets)

    Returns:
        Tuple[List[Dict], Dict]: (file_entries, stats_summary)
    """
    audit_index = audit_index or {}

    stats = {
        "carved_total": len(carved_files),
        "zero_byte": 0,
        "failed_validation": 0,
        "by_type": {},
    }
    entries: List[Dict[str, Any]] = []

    for carved_path in carved_files:
        rel_path = safe_rel_path(carved_path, output_dir)
        size = carved_path.stat().st_size if carved_path.exists() else 0
        file_type = carved_path.suffix.lstrip(".").lower() or None
        stats["by_type"][file_type or "unknown"] = stats["by_type"].get(file_type or "unknown", 0) + 1

        file_warnings: List[str] = []
        file_errors: List[str] = []
        validated: Dict[str, Any] = {}

        if size == 0:
            validated["zero_byte"] = True
            file_warnings.append("zero-byte file")
            stats["zero_byte"] += 1

        try:
            md5 = hash_file(carved_path, alg="md5")
            sha256 = hash_file(carved_path, alg="sha256")
        except Exception as exc:
            md5 = None
            sha256 = None
            file_errors.append(f"hash_error: {exc}")

        pillow_ok, pillow_err = verify_image(carved_path)
        if pillow_ok is not None:
            validated["pillow_ok"] = pillow_ok
        if pillow_ok is False:
            stats["failed_validation"] += 1
            file_warnings.append("image verification failed")
            if pillow_err:
                file_errors.append(pillow_err)

        audit_entry = audit_index.get(carved_path.name)
        audit_offset = audit_entry.get("offset") if audit_entry else None

        file_entry: Dict[str, Any] = {
            "rel_path": rel_path,
            "size": size,
            "md5": md5,
            "sha256": sha256,
            "file_type": file_type,
            "offset": audit_offset,
            "warnings": file_warnings,
            "errors": file_errors,
            "validated": validated if validated else None,
        }
        # Clean up None values
        if file_entry["validated"] is None:
            del file_entry["validated"]

        entries.append(file_entry)

    return entries, stats


def create_manifest(
    extractor_name: str,
    tool_name: str,
    tool_version: Optional[str],
    tool_path: Optional[Path],
    command: List[str],
    run_id: str,
    start_time: datetime,
    end_time: datetime,
    input_info: Dict[str, Any],
    output_dir: Path,
    file_types: Dict[str, Any],
    carved_files: List[Path],
    returncode: int,
    stdout: str,
    stderr: str,
    audit_entries: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Construct and validate a forensic manifest dictionary.

    Returns:
        Dict: The valid manifest data

    Raises:
        ManifestValidationError: If validation fails
    """
    # Summarize files
    audit_index = {entry["name"]: entry for entry in audit_entries} if audit_entries else {}
    carved_file_entries, stats = summarize_carved_files(carved_files, output_dir, audit_index)

    warnings: List[str] = []
    if returncode != 0:
        warnings.append(f"{tool_name} exited with code {returncode}")

    manifest_data: Dict[str, Any] = {
        "schema_version": "1.0.0",
        "run_id": run_id,
        "extractor": extractor_name,
        "tool": {
            "name": tool_name,
            "version": tool_version,
            "path": str(tool_path) if tool_path else None,
            "arguments": [str(part) for part in command],
        },
        "started_at": start_time.isoformat(),
        "completed_at": end_time.isoformat(),
        "input": input_info,
        "output": {
            "root": str(output_dir),
            "carved_dir": str(output_dir / "carved"),
            "manifest_path": str(output_dir / "manifest.json"),
        },
        "file_types": file_types,
        "stats": stats,
        "warnings": warnings,
        "notes": [],
        "process": {
            "command": [str(part) for part in command],
            "returncode": returncode,
            "stdout": stdout,
            "stderr": stderr,
        },
        "carved_files": carved_file_entries,
    }

    if audit_entries:
        manifest_data["audit"] = {"tool": tool_name, "entries": audit_entries}

    # Validate
    validate_image_carving_manifest(manifest_data)

    return manifest_data
