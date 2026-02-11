"""
Shared utilities for recording extracted files to the audit table.

This module provides helper functions to record extracted files to the
`extracted_files` audit table introduced in.

Usage pattern in extractors:

    from extractors._shared.extracted_files_audit import record_browser_files

    # After writing manifest.json in run_extraction():
    record_browser_files(
        evidence_conn=evidence_conn,
        evidence_id=evidence_id,
        run_id=run_id,
        extractor_name=self.metadata.name,
        extractor_version=self.metadata.version,
        manifest_data=manifest_data,
        callbacks=callbacks,
    )
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.logging import get_logger
from core.database.helpers.extracted_files import (
    insert_extracted_files_batch,
    delete_extracted_files_by_run,
)

LOGGER = get_logger("extractors._shared.extracted_files_audit")


def record_browser_files(
    evidence_conn,
    evidence_id: int,
    run_id: str,
    extractor_name: str,
    extractor_version: str,
    manifest_data: Dict[str, Any],
    callbacks,
    *,
    files_key: str = "files",
) -> int:
    """
    Record browser extractor files to extracted_files audit table.

    This handles the common pattern for browser extractors that copy
    SQLite databases and JSON files from evidence to the case folder.

    Args:
        evidence_conn: Evidence database connection (can be None)
        evidence_id: Evidence ID
        run_id: Unique run ID for this extraction
        extractor_name: Name of the extractor (e.g., "chromium_history")
        extractor_version: Version string for the extractor
        manifest_data: Manifest data dict with files list
        callbacks: ExtractorCallbacks for logging
        files_key: Key in manifest_data containing files list (default: "files")

    Returns:
        Number of records inserted (0 if no connection or no files)
    """
    files = manifest_data.get(files_key, [])
    if not files:
        return 0

    if evidence_conn is None:
        callbacks.on_log(
            "Skipping extracted_files audit (no database connection)",
            level="debug"
        )
        return 0

    try:
        # Clean up previous run records if re-extracting
        deleted = delete_extracted_files_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            callbacks.on_log(
                f"Cleaned up {deleted} previous extracted_files records",
                level="debug"
            )

        # Convert manifest files to extracted_files format
        extracted_records = []
        for file_info in files:
            # Browser extractors use various field names
            source_path = (
                file_info.get("logical_path") or
                file_info.get("source_path") or
                file_info.get("path") or
                ""
            )
            # Handle None values explicitly (key exists but value is None)
            extracted_path = file_info.get("extracted_path") or ""
            local_filename = (
                file_info.get("local_filename") or
                (extracted_path.split("/")[-1] if extracted_path else "") or
                ""
            )

            # Build relative path from extracted_path or local_filename
            if extracted_path:
                rel_path = Path(extracted_path).name
            else:
                rel_path = local_filename

            record = {
                "dest_rel_path": rel_path,
                "dest_filename": local_filename or Path(source_path).name,
                "source_path": source_path,
                "source_inode": str(file_info.get("inode")) if file_info.get("inode") else None,
                "partition_index": file_info.get("partition_index"),
                "source_offset_bytes": None,  # Not available for filesystem files
                "source_block_size": None,
                "size_bytes": file_info.get("size_bytes") or file_info.get("file_size_bytes"),
                "file_type": file_info.get("artifact_type") or "SQLite",
                "md5": file_info.get("md5"),
                "sha256": file_info.get("sha256"),
                "status": file_info.get("copy_status", "ok"),
                "error_message": file_info.get("error"),
                "metadata_json": _build_browser_metadata_json(file_info),
            }
            extracted_records.append(record)

        # Batch insert
        count = insert_extracted_files_batch(
            evidence_conn,
            evidence_id,
            extractor_name,
            run_id,
            extracted_records,
            extractor_version=extractor_version,
        )
        evidence_conn.commit()

        callbacks.on_log(
            f"Recorded {count:,} files to extracted_files audit table",
            level="debug"
        )
        LOGGER.debug(
            "Recorded %d extracted files to audit table (extractor=%s, run_id=%s)",
            count, extractor_name, run_id
        )
        return count

    except Exception as exc:
        callbacks.on_log(
            f"Failed to record extracted files: {exc}",
            level="warning"
        )
        LOGGER.warning("Failed to record extracted files: %s", exc, exc_info=True)
        return 0


def record_carved_files(
    evidence_conn,
    evidence_id: int,
    run_id: str,
    extractor_name: str,
    extractor_version: str,
    manifest_data: Dict[str, Any],
    callbacks,
    *,
    files_key: str = "carved_files",
    block_size: int = 512,
) -> int:
    """
    Record carved files to extracted_files audit table.

    This handles carving tools like foremost, scalpel that produce
    files with byte offsets but no source paths.

    Args:
        evidence_conn: Evidence database connection (can be None)
        evidence_id: Evidence ID
        run_id: Unique run ID for this extraction
        extractor_name: Name of the extractor (e.g., "foremost_carver")
        extractor_version: Version string for the extractor
        manifest_data: Manifest data dict with carved_files list
        callbacks: ExtractorCallbacks for logging
        files_key: Key in manifest_data containing files list (default: "carved_files")
        block_size: Block size for offset calculation (default: 512)

    Returns:
        Number of records inserted (0 if no connection or no files)
    """
    files = manifest_data.get(files_key, [])
    if not files:
        return 0

    if evidence_conn is None:
        callbacks.on_log(
            "Skipping extracted_files audit (no database connection)",
            level="debug"
        )
        return 0

    try:
        # Clean up previous run records if re-extracting
        deleted = delete_extracted_files_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            callbacks.on_log(
                f"Cleaned up {deleted} previous extracted_files records",
                level="debug"
            )

        # Convert carved_files to extracted_files format
        extracted_records = []
        for file_info in files:
            rel_path = file_info.get("rel_path", "")
            filename = Path(rel_path).name if rel_path else ""

            record = {
                "dest_rel_path": rel_path,
                "dest_filename": filename,
                "source_path": None,  # Carved files don't have source path
                "source_inode": None,
                "partition_index": None,
                "source_offset_bytes": file_info.get("offset"),
                "source_block_size": block_size,
                "size_bytes": file_info.get("size"),
                "file_type": file_info.get("file_type"),
                "md5": file_info.get("md5"),
                "sha256": file_info.get("sha256"),
                "status": "ok" if not file_info.get("errors") else "error",
                "error_message": "; ".join(file_info.get("errors", [])) or None,
                "metadata_json": json.dumps({
                    "warnings": file_info.get("warnings"),
                    "validated": file_info.get("validated"),
                }) if file_info.get("warnings") or file_info.get("validated") else None,
            }
            extracted_records.append(record)

        # Batch insert
        count = insert_extracted_files_batch(
            evidence_conn,
            evidence_id,
            extractor_name,
            run_id,
            extracted_records,
            extractor_version=extractor_version,
        )
        evidence_conn.commit()

        callbacks.on_log(
            f"Recorded {count:,} carved files to extracted_files audit table",
            level="debug"
        )
        LOGGER.debug(
            "Recorded %d carved files to audit table (extractor=%s, run_id=%s)",
            count, extractor_name, run_id
        )
        return count

    except Exception as exc:
        callbacks.on_log(
            f"Failed to record extracted files: {exc}",
            level="warning"
        )
        LOGGER.warning("Failed to record extracted files: %s", exc, exc_info=True)
        return 0


def _build_browser_metadata_json(file_info: Dict[str, Any]) -> Optional[str]:
    """Build metadata JSON for browser files."""
    metadata = {}

    # Browser context
    if file_info.get("browser"):
        metadata["browser"] = file_info["browser"]
    if file_info.get("profile"):
        metadata["profile"] = file_info["profile"]

    # Companion files
    if file_info.get("companion_files"):
        metadata["companion_files"] = file_info["companion_files"]

    return json.dumps(metadata) if metadata else None
