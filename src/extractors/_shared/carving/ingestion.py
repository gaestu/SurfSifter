"""
Image Carving Ingestion - Database Import Logic

Processes carved images with perceptual hashing and EXIF extraction.
Supports order-independent enrichment via image_discoveries table.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, Any, List, Optional

from extractors.callbacks import ExtractorCallbacks
from core.database import insert_images, delete_discoveries_by_run
from core.logging import get_logger
from .processor import ParallelImageProcessor, ImageProcessResult
from .enrichment import ingest_with_enrichment
from core.config import ParallelConfig

LOGGER = get_logger("extractors._shared.carving.ingestion")
LARGE_IMAGE_THRESHOLD = 20000  # Fallback to sequential when the carved set is huge


def run_image_ingestion(
    output_dir: Path,
    evidence_conn: sqlite3.Connection,
    evidence_id: int,
    manifest_data: Dict[str, Any],
    callbacks: ExtractorCallbacks,
    discovered_by: str = "image_carving",
    extractor_version: Optional[str] = None,
    offset_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, int]:
    """
    Ingest carved images into database with perceptual hashing and enrichment support.

    Workflow:
        1. Collect carved image files
        2. Process in parallel (phash, EXIF, thumbnails)
        3. Insert to images table with enrichment (handles duplicates)
        4. Return statistics

    Args:
        output_dir: Directory containing carved images
        evidence_conn: SQLite connection to evidence database
        evidence_id: Evidence ID
        manifest_data: Manifest metadata from extraction
        callbacks: Progress callbacks
        discovered_by: Extractor name for provenance
        extractor_version: Extractor version string
        offset_map: Optional dict mapping filename -> {carved_offset_bytes, carved_block_size}

    Returns:
        Dict with statistics: {"inserted": int, "enriched": int, "errors": int, "total": int}
    """
    run_id = manifest_data.get("run_id")
    if not run_id:
        callbacks.on_error("Manifest missing run_id - required for ingestion")
        return {"inserted": 0, "enriched": 0, "errors": 0, "total": 0}

    callbacks.on_step("Collecting carved images")

    # Collect all carved files
    carved_dir = output_dir / "carved"
    if not carved_dir.exists():
        LOGGER.warning("No carved directory found at %s", carved_dir)
        return {"inserted": 0, "enriched": 0, "errors": 0, "total": 0}

    image_files = _collect_image_files(carved_dir)
    total_files = len(image_files)

    if total_files == 0:
        LOGGER.info("No carved images to ingest")
        return {"inserted": 0, "enriched": 0, "errors": 0, "total": 0}

    callbacks.on_step(f"Processing {total_files} images")
    LOGGER.info("Processing %d carved images", total_files)

    # Clean up previous run if re-ingesting
    deleted = delete_discoveries_by_run(evidence_conn, evidence_id, run_id)
    if deleted > 0:
        callbacks.on_log(f"Cleaned up {deleted} previous discovery records")
        LOGGER.info("Deleted %d previous discovery records for run_id=%s", deleted, run_id)

    # Process images in parallel (CPU-bound: phash, EXIF, thumbnails)
    parallel_cfg = ParallelConfig.from_environment()
    enable_parallel = parallel_cfg.enable_parallel
    if enable_parallel and total_files >= LARGE_IMAGE_THRESHOLD:
        enable_parallel = False
        LOGGER.warning(
            "Large carved image set detected (%d files); running sequentially to avoid worker pool stalls",
            total_files,
        )
        callbacks.on_log(
            f"Large carved image set detected ({total_files} files); running sequentially to avoid worker pool stalls",
            level="warning",
        )

    processor = ParallelImageProcessor(
        max_workers=parallel_cfg.max_workers,
        enable_parallel=enable_parallel,
    )

    try:
        results = processor.process_images(image_files, output_dir)
    except Exception as exc:
        LOGGER.warning("Parallel image ingestion failed (%s); retrying sequentially", exc)
        callbacks.on_log("Parallel ingestion failed; retrying sequentially", level="warning")
        processor = ParallelImageProcessor(enable_parallel=False)
        results = processor.process_images(image_files, output_dir)

    # Insert to database with enrichment support
    inserted = 0
    enriched = 0
    error_count = 0

    callbacks.on_step(f"Inserting {len(results)} images to database")

    for result in results:
        if result.error is not None:
            error_count += 1
            LOGGER.warning("Skipping failed image %s: %s", result.path, result.error)
            continue

        try:
            # Build image data from processing result
            # NOTE: Do not set run_id on image record - provenance tracked via discoveries
            record = result.to_db_record(discovered_by)

            # Get carved tool output path (relative path in carved directory)
            carved_tool_output = str(result.path.relative_to(output_dir))

            # Get offset info if available
            filename = result.path.name
            offset_info = offset_map.get(filename, {}) if offset_map else {}

            # Insert with enrichment
            image_id, was_inserted = ingest_with_enrichment(
                conn=evidence_conn,
                evidence_id=evidence_id,
                image_data=record,
                discovered_by=discovered_by,
                run_id=run_id,
                extractor_version=extractor_version,
                carved_offset_bytes=offset_info.get("carved_offset_bytes"),
                carved_block_size=offset_info.get("carved_block_size"),
                carved_tool_output=carved_tool_output,
            )

            if was_inserted:
                inserted += 1
            else:
                enriched += 1

        except Exception as e:
            error_count += 1
            LOGGER.warning("Error ingesting %s: %s", result.path, e)

    evidence_conn.commit()

    # Count how many have phash
    phash_count = sum(1 for r in results if r.error is None and r.phash)
    LOGGER.info(
        "Ingestion complete: %d inserted, %d enriched, %d errors, %d with phash",
        inserted, enriched, error_count, phash_count
    )

    return {
        "inserted": inserted,
        "enriched": enriched,
        "skipped_duplicates": enriched,  # For backward compatibility
        "errors": error_count,
        "total": total_files,
    }


def _collect_image_files(carved_dir: Path) -> List[Path]:
    """
    Collect all image files from carved directory.

    Args:
        carved_dir: Directory containing carved files

    Returns:
        Sorted list of image file paths
    """
    # Expanded extension list to include modern image formats
    image_extensions = {
        ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif",
        ".webp", ".avif", ".heic", ".heif", ".svg", ".ico"
    }
    files: List[Path] = []

    for path in carved_dir.rglob("*"):
        if path.is_file() and path.suffix.lower() in image_extensions:
            files.append(path)

    return sorted(files)  # Deterministic ordering
