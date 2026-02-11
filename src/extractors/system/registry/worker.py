"""
Registry extraction worker logic.

Exports registry hives from evidence to the case workspace.
"""

from __future__ import annotations

import fnmatch
import json
import re
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional

from core.logging import get_logger
from ...callbacks import ExtractorCallbacks

LOGGER = get_logger("extractors.system.registry.worker")


@dataclass(slots=True)
class RegistryFinding:
    """Registry finding data structure."""
    detector_id: str
    name: str
    value: str
    confidence: str
    provenance: str
    hive: str
    path: str
    extra_json: str | None = None


# Standard registry hive patterns to export
# NOTE: Using case-insensitive wildcards for Windows compatibility
# NOTE: Avoid starting with **/ to enable targeted search optimization in EvidenceFS
STANDARD_HIVE_PATTERNS = [
    # Machine hives (case-insensitive patterns)
    "[Ww][Ii][Nn][Dd][Oo][Ww][Ss]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SYSTEM",
    "[Ww][Ii][Nn][Dd][Oo][Ww][Ss]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SOFTWARE",
    "[Ww][Ii][Nn][Dd][Oo][Ww][Ss]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SAM",
    "[Ww][Ii][Nn][Dd][Oo][Ww][Ss]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SECURITY",
    # Legacy Windows (WINNT)
    "[Ww][Ii][Nn][Nn][Tt]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SYSTEM",
    "[Ww][Ii][Nn][Nn][Tt]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SOFTWARE",
    "[Ww][Ii][Nn][Nn][Tt]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SAM",
    "[Ww][Ii][Nn][Nn][Tt]/[Ss][Yy][Ss][Tt][Ee][Mm]32/[Cc][Oo][Nn][Ff][Ii][Gg]/SECURITY",
    # User hives (modern Windows)
    "[Uu][Ss][Ee][Rr][Ss]/*/NTUSER.DAT",
    "[Uu][Ss][Ee][Rr][Ss]/*/[Aa][Pp][Pp][Dd][Aa][Tt][Aa]/[Ll][Oo][Cc][Aa][Ll]/[Mm][Ii][Cc][Rr][Oo][Ss][Oo][Ff][Tt]/[Ww][Ii][Nn][Dd][Oo][Ww][Ss]/UsrClass.dat",
    # User hives (legacy Windows)
    "[Dd][Oo][Cc][Uu][Mm][Ee][Nn][Tt][Ss] and [Ss][Ee][Tt][Tt][Ii][Nn][Gg][Ss]/*/NTUSER.DAT",
    "[Dd][Oo][Cc][Uu][Mm][Ee][Nn][Tt][Ss] and [Ss][Ee][Tt][Tt][Ii][Nn][Gg][Ss]/*/[Ll][Oo][Cc][Aa][Ll] [Ss][Ee][Tt][Tt][Ii][Nn][Gg][Ss]/[Aa][Pp][Pp][Ll][Ii][Cc][Aa][Tt][Ii][Oo][Nn] [Dd][Aa][Tt][Aa]/[Mm][Ii][Cc][Rr][Oo][Ss][Oo][Ff][Tt]/[Ww][Ii][Nn][Dd][Oo][Ww][Ss]/UsrClass.dat",
]


def run_registry_extraction(
    evidence_fs,
    output_dir: Path,
    config: Dict[str, Any],
    callbacks: ExtractorCallbacks,
) -> Dict[str, Any]:
    """
    Export registry hives from evidence.

    Args:
        evidence_fs: Evidence filesystem
        output_dir: Directory to write manifest
        config: Configuration (ignored for extraction scope)
        callbacks: Progress callbacks

    Returns:
        Dict with run_id, extracted_hives count, hives_scanned
    """
    # Generate run ID
    run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}_{uuid.uuid4().hex[:8]}"

    callbacks.on_step("Scanning for registry hives")

    # Find all relevant hives
    found_hives = []
    for i, pattern in enumerate(STANDARD_HIVE_PATTERNS, 1):
        try:
            callbacks.on_log(f"Scanning pattern {i}/{len(STANDARD_HIVE_PATTERNS)}: {pattern}", "info")
            LOGGER.info("Scanning for pattern: %s", pattern)
            paths = list(evidence_fs.iter_paths(pattern))
            if paths:
                callbacks.on_log(f"  Found {len(paths)} matches", "info")
                LOGGER.info("  Found %d matches for pattern %s", len(paths), pattern)
            found_hives.extend(paths)
        except Exception as e:
            LOGGER.warning("Error scanning for pattern %s: %s", pattern, e)
            callbacks.on_log(f"  Error: {e}", "error")

    # Deduplicate paths
    found_hives = sorted(list(set(found_hives)))

    if not found_hives:
        callbacks.on_log("No registry hives found in evidence", "warning")
        LOGGER.warning("No registry hives found in evidence")
    else:
        msg = f"Found {len(found_hives)} unique registry hives in evidence"
        callbacks.on_log(msg, "info")
        LOGGER.info(msg)

    # Prepare output directory
    hives_dir = output_dir / "hives"
    hives_dir.mkdir(parents=True, exist_ok=True)

    extracted_hives_info = []

    callbacks.on_step(f"Exporting {len(found_hives)} hives")

    for i, hive_path in enumerate(found_hives):
        try:
            # Determine logical hive type
            filename = Path(hive_path).name.upper()
            logical_hive = "UNKNOWN"
            if filename == "SYSTEM":
                logical_hive = "SYSTEM"
            elif filename == "SOFTWARE":
                logical_hive = "SOFTWARE"
            elif filename == "SAM":
                logical_hive = "SAM"
            elif filename == "SECURITY":
                logical_hive = "SECURITY"
            elif filename == "NTUSER.DAT":
                logical_hive = "NTUSER"
            elif filename == "USRCLASS.DAT":
                logical_hive = "USRCLASS"

            # Create unique local filename
            # e.g. SYSTEM_0.hive, NTUSER_1.hive
            local_filename = f"{logical_hive}_{i}.hive"
            local_path = hives_dir / local_filename

            callbacks.on_log(f"Exporting {hive_path} -> {local_filename}", "info")

            # Copy file
            file_size = 0
            with evidence_fs.open_for_read(hive_path) as src, open(local_path, "wb") as dst:
                while True:
                    chunk = src.read(8192)
                    if not chunk:
                        break
                    dst.write(chunk)
                    file_size += len(chunk)

            extracted_hives_info.append({
                "original_path": hive_path,
                "local_path": f"hives/{local_filename}",
                "filename": Path(hive_path).name,
                "logical_hive": logical_hive,
                "size": file_size
            })

            LOGGER.info("Exported hive %s (%d bytes)", hive_path, file_size)

        except Exception as e:
            LOGGER.error("Failed to export hive %s: %s", hive_path, e)
            callbacks.on_error(f"Failed to export {Path(hive_path).name}: {e}")

    # Write manifest
    callbacks.on_log(f"Writing manifest with {len(extracted_hives_info)} exported hives", "info")
    manifest = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "extractor": "registry",
        "version": "0.50.0",
        "hives_scanned": found_hives,
        "extracted_hives": extracted_hives_info
    }

    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    return {
        "run_id": run_id,
        "extracted_hives": len(extracted_hives_info),
        "hives_scanned": found_hives
    }


