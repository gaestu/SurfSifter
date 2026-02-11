"""
Image Carving Enrichment Helpers

Shared enrichment logic for image carving extractors.
Supports order-independent enrichment via image_discoveries table.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, Optional, Tuple

from core.database import insert_image_with_discovery
from core.logging import get_logger

LOGGER = get_logger("extractors._shared.carving.enrichment")


def ingest_with_enrichment(
    conn: sqlite3.Connection,
    evidence_id: int,
    image_data: Dict[str, Any],
    discovered_by: str,
    run_id: str,
    extractor_version: Optional[str] = None,
    carved_offset_bytes: Optional[int] = None,
    carved_block_size: Optional[int] = None,
    carved_tool_output: Optional[str] = None,
) -> Tuple[int, bool]:
    """
    Insert or enrich image with carving discovery.

    Uses the centralized insert_image_with_discovery() function to:
    1. Check if image SHA256 exists
    2. If new: insert image + discovery record
    3. If exists: add discovery record only (enrichment)

    Args:
        conn: Database connection
        evidence_id: Evidence ID
        image_data: Image record dict (must include sha256)
        discovered_by: Extractor name (e.g., "foremost_carver")
        run_id: Extraction run identifier
        extractor_version: Optional extractor version string
        carved_offset_bytes: Byte offset in evidence
        carved_block_size: Block size used for offset calculation
        carved_tool_output: Path in carver output directory

    Returns:
        (image_id, was_new) - was_new=False means enriched existing record
    """
    discovery_data = {
        "discovered_by": discovered_by,
        "run_id": run_id,
        "extractor_version": extractor_version,
        "carved_offset_bytes": carved_offset_bytes,
        "carved_block_size": carved_block_size,
        "carved_tool_output": carved_tool_output,
    }
    return insert_image_with_discovery(conn, evidence_id, image_data, discovery_data)


def parse_foremost_audit_with_bytes(
    audit_path,
    default_block_size: int = 512,
) -> dict:
    """
    Parse foremost audit.txt with block-to-byte offset conversion.

    Foremost stores offsets in BLOCKS (default bs=512), not bytes.
    This function extracts the per-entry block size from the name field
    (e.g., "jpg(bs=512)") and converts to byte offsets.

    Args:
        audit_path: Path to audit.txt
        default_block_size: Default block size if not specified (512)

    Returns:
        Dict mapping filename -> {carved_offset_bytes, carved_block_size}
    """
    import re
    from pathlib import Path

    audit_path = Path(audit_path)
    if not audit_path.exists():
        LOGGER.warning("Audit file not found: %s", audit_path)
        return {}

    # Pattern for parsing audit lines
    # Format: <num>:	<name>.jpg	<size>	<offset>
    # Example: 0:	00001524.jpg      39 KB          1524
    line_pattern = re.compile(r"(\d+):\s+(\S+)\s+(\d+(?:\.\d+)?)\s*([KMG]?B?)\s+(\d+)")

    # Pattern for extracting block size from name
    bs_pattern = re.compile(r"\(bs=(\d+)\)")

    entries = {}

    try:
        content = audit_path.read_text(encoding="utf-8", errors="ignore")
        current_block_size = default_block_size

        for line in content.splitlines():
            line = line.strip()
            if not line or line.startswith("Foremost") or line.startswith("Audit"):
                continue

            # Check for block size declaration in section header
            # Format: jpg(bs=512): or just jpg:
            if ":" in line and not line[0].isdigit():
                bs_match = bs_pattern.search(line)
                if bs_match:
                    current_block_size = int(bs_match.group(1))
                continue

            # Parse entry line
            match = line_pattern.match(line)
            if match:
                num, filename, size_val, size_unit, block_offset = match.groups()

                # Convert block offset to byte offset
                byte_offset = int(block_offset) * current_block_size

                if filename in entries:
                    LOGGER.debug("Duplicate audit entry for %s; overwriting", filename)

                entries[filename] = {
                    "carved_offset_bytes": byte_offset,
                    "carved_block_size": current_block_size,
                }

    except Exception as e:
        LOGGER.warning("Error parsing audit file %s: %s", audit_path, e)

    LOGGER.debug("Parsed %d entries from foremost audit", len(entries))
    return entries
