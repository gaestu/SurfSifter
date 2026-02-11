"""
Manifest I/O utilities for cache extraction.

Handles reading/writing partial and final manifests for crash recovery
and resume capability.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.logging import get_logger

LOGGER = get_logger("extractors.cache_simple.manifest")

# Extractor version (keep in sync with extractor.py)
EXTRACTOR_VERSION = "0.68.1"


def write_partial_manifest(
    run_dir: Path,
    run_id: str,
    evidence_id: int,
    manifest_files: List[Dict],
    stats: Dict[str, Any],
    hash_mode: str,
) -> None:
    """
    Write incremental partial manifest for crash recovery.

    Uses atomic write (temp file + rename) to prevent corruption.

    Args:
        run_dir: Directory for this extraction run
        run_id: Unique run identifier
        evidence_id: Evidence database ID
        manifest_files: List of file entries already extracted
        stats: Current extraction statistics
        hash_mode: Hash computation mode ("extraction", "ingestion", "disabled")
    """
    partial_manifest = {
        "version": "1.0",
        "extractor": "cache_simple",
        "extractor_version": EXTRACTOR_VERSION,
        "run_id": run_id,
        "evidence_id": evidence_id,
        "hash_mode": hash_mode,
        "status": "in_progress",
        "files": manifest_files,
        "statistics": stats,
        "partial_timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }

    temp_path = run_dir / "manifest.partial.json.tmp"
    temp_path.write_text(json.dumps(partial_manifest, indent=2))
    temp_path.rename(run_dir / "manifest.partial.json")


def write_final_manifest(
    run_output: Path,
    run_id: str,
    evidence_id: int,
    manifest_files: List[Dict],
    stats: Dict[str, Any],
    hash_mode: str,
    status: str,
    notes: List[str],
    extraction_tool: str,
    e01_context: Dict[str, Any],
    config: Dict[str, Any],
) -> None:
    """
    Write final manifest.json with all extraction metadata.

    Args:
        run_output: Output directory for this run
        run_id: Unique run identifier
        evidence_id: Evidence database ID
        manifest_files: Complete list of extracted file entries
        stats: Final extraction statistics
        hash_mode: Hash computation mode
        status: Final status ("ok", "partial", "error", "skipped")
        notes: List of notes/warnings
        extraction_tool: Tool version string
        e01_context: E01 image context (path, partition, fs_type)
        config: Extraction configuration
    """
    manifest = {
        "version": "1.0",
        "extractor": "cache_simple",
        "extractor_version": EXTRACTOR_VERSION,
        "run_id": run_id,
        "evidence_id": evidence_id,
        "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "extraction_tool": extraction_tool,
        "hash_mode": hash_mode,
        "config": config,
        "e01_context": e01_context,
        "files": manifest_files,
        "statistics": stats,
        "status": status,
        "notes": notes,
    }

    manifest_path = run_output / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))


def find_incomplete_extraction(output_dir: Path) -> Optional[Dict[str, Any]]:
    """
    Find an incomplete extraction (has partial manifest but no final manifest).

    Scans run directories in reverse order (newest first) looking for
    partial manifests without corresponding final manifests.

    Args:
        output_dir: Base output directory containing run subdirectories

    Returns:
        Dict with run_id, run_dir, files, stats, hash_mode, evidence_id
        or None if no incomplete extraction found
    """
    if not output_dir.exists():
        return None

    for run_dir in sorted(output_dir.iterdir(), reverse=True):
        if not run_dir.is_dir():
            continue

        partial_path = run_dir / "manifest.partial.json"
        final_path = run_dir / "manifest.json"

        if partial_path.exists() and not final_path.exists():
            try:
                data = json.loads(partial_path.read_text())
                return {
                    "run_id": data.get("run_id"),
                    "run_dir": run_dir,
                    "files": data.get("files", []),
                    "stats": data.get("statistics", {}),
                    "hash_mode": data.get("hash_mode", "extraction"),
                    "evidence_id": data.get("evidence_id", 0),
                }
            except Exception as e:
                LOGGER.warning("Failed to load partial manifest %s: %s", partial_path, e)

    return None


def load_manifest(manifest_path: Path) -> Dict[str, Any]:
    """
    Load and parse a manifest file.

    Args:
        manifest_path: Path to manifest.json

    Returns:
        Parsed manifest data

    Raises:
        FileNotFoundError: If manifest doesn't exist
        json.JSONDecodeError: If manifest is invalid JSON
    """
    return json.loads(manifest_path.read_text())


def get_latest_manifest(output_dir: Path) -> Optional[Path]:
    """
    Get the path to the latest manifest.json in output directory.

    Args:
        output_dir: Base output directory containing run subdirectories

    Returns:
        Path to latest manifest.json or None if none found
    """
    manifests = sorted(output_dir.glob("*/manifest.json"))
    return manifests[-1] if manifests else None
