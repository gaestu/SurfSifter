"""
Registry ingestion logic.

Performs analysis of local registry hives using rules and loads findings into the database.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timezone

from core.logging import get_logger
from ...callbacks import ExtractorCallbacks
from .parser import process_hive_file
from .rules_util import load_registry_rules

LOGGER = get_logger("extractors.system.registry.ingestion")


def save_ingestion_summary(output_dir: Path, summary: Dict[str, Any]) -> None:
    """Save ingestion summary to JSON file."""
    try:
        summary_path = output_dir / "ingestion_registry.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
    except Exception as e:
        LOGGER.error("Failed to save ingestion summary: %s", e)


def run_registry_ingestion(
    manifest_data: Dict[str, Any],
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
    callbacks: ExtractorCallbacks,
    output_dir: Path = None,
    config: Dict[str, Any] = None,
) -> Dict[str, int]:
    """
    Analyze local registry hives and insert findings into database.

    Args:
        manifest_data: Manifest JSON data containing list of exported hives
        evidence_conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        callbacks: Progress callbacks
        output_dir: Directory containing the manifest and hives
        config: Ingestion configuration (optional rules_path)

    Returns:
        Dict with inserted count and errors count
    """
    extracted_hives = manifest_data.get("extracted_hives", [])
    config = config or {}
    purge_existing = bool(config.get("purge_existing"))

    summary = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "hives_processed": 0,
        "hives_total": len(extracted_hives),
        "rules_matched": 0,
        "findings_count": 0,
        "purged_existing": 0,
        "errors": [],
        "processed_hives": []
    }

    if purge_existing:
        try:
            from core.database import purge_evidence_data
            purged = purge_evidence_data(evidence_conn, evidence_id, tables=["os_indicators"])
            summary["purged_existing"] = purged
            callbacks.on_log(f"Purged {purged} existing registry indicators", "info")
            LOGGER.info("Purged %d os_indicators before registry ingestion", purged)
        except Exception as e:
            LOGGER.warning("Failed to purge existing os_indicators: %s", e)
            callbacks.on_log(f"Failed to purge existing indicators: {e}", "warning")

    if not extracted_hives:
        callbacks.on_log("No extracted hives to process", "info")
        if output_dir:
            save_ingestion_summary(output_dir, summary)
        return {"inserted": 0, "errors": 0}

    if not output_dir:
        callbacks.on_error("Output directory required for ingestion")
        return {"inserted": 0, "errors": 1}

    callbacks.on_step(f"Processing {len(extracted_hives)} registry hives")

    # Load rules from Python module
    compiled_rules = load_registry_rules()

    # All targets are registry targets (no filtering needed)
    registry_targets = compiled_rules.targets

    callbacks.on_log(f"Loaded {len(registry_targets)} registry analysis targets", "info")
    LOGGER.info("Loaded %d registry analysis targets", len(registry_targets))

    all_findings = []

    # Process each hive against all targets
    for hive_info in extracted_hives:
        local_path = hive_info.get("local_path")
        if not local_path:
            continue

        hive_full_path = output_dir / local_path
        if not hive_full_path.exists():
            LOGGER.warning("Hive file not found: %s", hive_full_path)
            summary["errors"].append(f"Hive file not found: {local_path}")
            continue

        callbacks.on_step(f"Analyzing {hive_info.get('filename', 'hive')}")
        summary["hives_processed"] += 1

        hive_summary = {
            "filename": hive_info.get("filename"),
            "original_path": hive_info.get("original_path"),
            "matched_rules": [],
            "findings": 0
        }

        # We need to match the hive to the target based on the original path
        # But since we don't have the original path structure locally, we rely on the filename or just try all targets?
        # The parser logic in worker.py was iterating targets and finding paths.
        # Here we have the file, we need to know which target applies.
        # A simple heuristic: check if the original path matches the target pattern.

        original_path = hive_info.get("original_path", "")
        # Normalize path separators for matching
        original_path_normalized = original_path.replace("\\", "/")

        for target in registry_targets:
            # Check if this hive matches any path in the target
            matches_target = False
            for path_pattern in target.get("paths", []):
                # Use pathlib-style glob matching (supports **)
                # Case-insensitive comparison for Windows compatibility
                from pathlib import PurePosixPath

                pattern_lower = path_pattern.lower()
                path_lower = original_path_normalized.lower()

                # Handle ** prefix (recursive glob)
                # pathlib.PurePosixPath.match() doesn't support ** prefix, so we strip it
                if pattern_lower.startswith("**/"):
                    pattern_lower = pattern_lower[3:]  # Remove "**/

                # Now use PurePosixPath.match() which supports * wildcards
                if PurePosixPath(path_lower).match(pattern_lower):
                    matches_target = True
                    LOGGER.debug("Path %s matched pattern %s", original_path, path_pattern)
                    break

            if matches_target:
                callbacks.on_log(f"Hive {hive_info.get('filename')} matches target '{target.get('name')}'", "info")
                try:
                    findings = process_hive_file(hive_full_path, target)
                    if findings:
                        all_findings.extend(findings)
                        summary["rules_matched"] += 1
                        hive_summary["matched_rules"].append(target.get("name"))
                        hive_summary["findings"] += len(findings)
                        callbacks.on_log(f"  -> Found {len(findings)} indicators for '{target.get('name')}'", "info")
                    else:
                        callbacks.on_log(f"  -> No indicators found for '{target.get('name')}'", "info")

                except Exception as e:
                    LOGGER.error("Error processing hive %s with target %s: %s",
                               hive_info.get("filename"), target.get("name"), e)
                    callbacks.on_log(f"Error processing {hive_info.get('filename')}: {e}", "error")
                    summary["errors"].append(f"Error processing {hive_info.get('filename')}: {str(e)}")

        summary["processed_hives"].append(hive_summary)

    inserted = 0
    errors = 0

    if all_findings:
        callbacks.on_log(f"Total findings to insert: {len(all_findings)}", "info")
    else:
        callbacks.on_log("No findings generated from analysis", "warning")

    # Use run_id for idempotent re-ingestion when available
    run_id = config.get("run_id") if config else None
    if not run_id:
        run_id = manifest_data.get("run_id")

    if run_id:
        try:
            from core.database import delete_os_indicators_by_run
            deleted = delete_os_indicators_by_run(evidence_conn, evidence_id, run_id)
            if deleted:
                callbacks.on_log(f"Cleaned up {deleted} previous registry indicators", "info")
                LOGGER.info("Deleted %d os_indicators for run_id=%s", deleted, run_id)
        except Exception as e:
            LOGGER.warning("Failed to delete prior os_indicators for run_id=%s: %s", run_id, e)

    # Prepare batch insert
    records = []
    for finding in all_findings:
        try:
            # Use semantic type from finding name (e.g. system:os_version)
            # Fallback to "registry" if not semantic
            indicator_type = finding.name if ":" in finding.name else "registry"

            record = {
                "type": indicator_type,
                "name": finding.name,
                "value": finding.value,
                "path": finding.path,
                "hive": finding.hive,
                "confidence": finding.confidence,
                "provenance": finding.provenance,
                "extra_json": finding.extra_json,
                "run_id": run_id,
            }
            records.append(record)
        except Exception as e:
            LOGGER.warning("Error preparing finding for insert: %s", e)
            errors += 1

    # Batch insert
    if records:
        try:
            from core.database import insert_os_indicators
            insert_os_indicators(evidence_conn, evidence_id, records)
            inserted = len(records)
            callbacks.on_step(f"Inserted {inserted} findings")
        except Exception as e:
            LOGGER.exception("Failed to insert registry findings: %s", e)
            callbacks.on_error(f"Database insert failed: {e}")
            errors += len(records)

    summary["findings_count"] = inserted
    save_ingestion_summary(output_dir, summary)

    return {"inserted": inserted, "errors": errors}
