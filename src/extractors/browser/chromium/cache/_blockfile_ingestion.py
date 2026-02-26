"""
Blockfile cache ingestion utilities.

Handles parsing and ingestion of legacy Chromium blockfile cache format.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING
from urllib.parse import urlparse

from core.logging import get_logger
from core.database import (
    insert_image_with_discovery,
    insert_urls,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)

from .blockfile import (
    detect_blockfile_cache,
    is_cache_url,
    parse_blockfile_cache,
    read_stream_data,
    scan_data1_orphan_entries,
)
from ._parser import parse_http_headers
from ._decompression import decompress_body
from ._carving import detect_image_format, carve_blockfile_image

if TYPE_CHECKING:
    from ....callbacks import ExtractorCallbacks
    from ...._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.cache_simple.blockfile_ingestion")


def find_blockfile_directories(
    files: List[Dict[str, Any]],
    output_dir: Path,
) -> List[Dict[str, Any]]:
    """
    Find directories containing blockfile cache (data_0/1/2/3 + index).

    Scans extracted files to identify directories that contain the
    characteristic blockfile cache structure.

    Args:
        files: List of file entries from manifest
        output_dir: Base extraction directory

    Returns:
        List of dicts with path, browser, profile, files keys
    """
    # Group files by parent directory
    dir_files: Dict[str, List[Dict[str, Any]]] = {}

    for file_entry in files:
        extracted_path = file_entry.get("extracted_path", "")
        if Path(extracted_path).is_absolute():
            file_path = Path(extracted_path)
        else:
            file_path = output_dir / extracted_path

        parent_dir = str(file_path.parent)
        if parent_dir not in dir_files:
            dir_files[parent_dir] = []
        dir_files[parent_dir].append(file_entry)

    blockfile_dirs = []

    for dir_path_str, dir_entries in dir_files.items():
        dir_path = Path(dir_path_str)

        filenames = {Path(e.get("extracted_path", "")).name for e in dir_entries}

        # Must have data_1 (entry metadata blocks)
        if "data_1" not in filenames:
            continue

        # Must have index file
        if "index" not in filenames:
            continue

        index_path = dir_path / "index"
        if not index_path.exists():
            continue

        # Verify it's actually a blockfile cache
        if not detect_blockfile_cache(dir_path):
            continue

        # Extract browser/profile from file entries
        browser = "chrome"
        profile = "Default"

        for entry in dir_entries:
            if entry.get("browser"):
                browser = entry["browser"]
            if entry.get("profile"):
                profile = entry["profile"]
            break

        blockfile_dirs.append({
            "path": dir_path,
            "browser": browser,
            "profile": profile,
            "files": [e.get("extracted_path", "") for e in dir_entries],
        })

        LOGGER.info("Found blockfile cache: %s (%s/%s)", dir_path, browser, profile)

    return blockfile_dirs


def ingest_blockfile_directory(
    evidence_conn,
    evidence_id: int,
    run_id: str,
    cache_dir: Path,
    browser: str,
    profile: str,
    extraction_dir: Path,
    callbacks: "ExtractorCallbacks",
    extractor_version: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    manifest_data: Optional[Dict[str, Any]] = None,
    file_entries: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, int]:
    """
    Parse and ingest a blockfile cache directory.

    Args:
        evidence_conn: Database connection
        evidence_id: Evidence ID
        run_id: Extraction run ID
        cache_dir: Path to blockfile cache directory
        browser: Browser name
        profile: Profile name
        extraction_dir: Base extraction directory
        callbacks: Progress callbacks
        extractor_version: Extractor version string
        warning_collector: Optional warning collector
        manifest_data: Optional manifest data for inventory registration
        file_entries: Optional file entries for inventory registration

    Returns:
        Dict with urls, images, records, entries, inventory_entries counts
    """
    stats = {
        "urls": 0,
        "images": 0,
        "records": 0,
        "entries": 0,
        "inventory_entries": 0,
    }

    # Register inventory entries for blockfile files.
    # Track which inventory ID corresponds to the index file — only that
    # entry carries the directory-level URL/record totals.
    inventory_ids = []
    index_inventory_id: Optional[int] = None
    if manifest_data and file_entries:
        extraction_timestamp = manifest_data.get(
            "extraction_timestamp_utc",
            datetime.now(timezone.utc).isoformat()
        )
        for file_entry in file_entries:
            try:
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=browser,
                    artifact_type="cache_blockfile",
                    profile=profile,
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    logical_path=file_entry.get("logical_path", ""),
                    forensic_path=file_entry.get("forensic_path"),
                    run_id=run_id,
                    extracted_path=file_entry.get("extracted_path", ""),
                    extraction_status=manifest_data.get("status", "ok"),
                    extraction_timestamp_utc=extraction_timestamp,
                    extraction_tool=manifest_data.get("extraction_tool"),
                    file_size_bytes=file_entry.get("size_bytes"),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                )
                inventory_ids.append(inventory_id)
                stats["inventory_entries"] += 1
                # Remember the index file's inventory row
                ep = file_entry.get("extracted_path", "")
                if Path(ep).name == "index" and index_inventory_id is None:
                    index_inventory_id = inventory_id
            except Exception as e:
                LOGGER.warning(
                    "Failed to register inventory for %s: %s",
                    file_entry.get("extracted_path", "unknown"),
                    e,
                )

    try:
        # Parse blockfile cache
        entries = parse_blockfile_cache(cache_dir, warning_collector=warning_collector)
        stats["entries"] = len(entries)

        if not entries:
            # Index parsed but yielded no valid entries.  Try scanning
            # data_1 blocks directly — orphaned EntryStore structures may
            # still be intact even when the index hash table is
            # cleared/corrupted.
            entries = scan_data1_orphan_entries(
                cache_dir, warning_collector=warning_collector,
            )
            stats["entries"] = len(entries)

            if entries:
                callbacks.on_log(
                    f"Recovered {len(entries)} orphaned entries from data_1 block scan"
                )
            else:
                # No structured entries at all — fall back to blind image
                # carving on external f_* files as a last resort.
                orphan_images = _carve_orphan_data_files(
                    evidence_conn=evidence_conn,
                    evidence_id=evidence_id,
                    cache_dir=cache_dir,
                    extraction_dir=extraction_dir,
                    run_id=run_id,
                    extractor_version=extractor_version,
                    warning_collector=warning_collector,
                )
                stats["images"] += orphan_images

                status = "no_entries" if orphan_images == 0 else "orphan_carved"
                notes = (
                    "Blockfile cache parsed but no entries found"
                    + (f"; carved {orphan_images} images from orphan files"
                       if orphan_images else "")
                )
                for inv_id in inventory_ids:
                    update_inventory_ingestion_status(
                        evidence_conn, inv_id,
                        status=status,
                        notes=notes,
                    )
                return stats

        callbacks.on_log(f"Parsed {len(entries)} entries from blockfile cache")

        discovered_by = f"cache_blockfile:{extractor_version}:{run_id}"

        # Derive forensic provenance from file entries (directory-level)
        base_forensic_path: Optional[str] = None
        base_logical_path: Optional[str] = None
        if file_entries:
            for fe in file_entries:
                if fe.get("forensic_path") and not base_forensic_path:
                    base_forensic_path = str(Path(fe["forensic_path"]).parent)
                if fe.get("logical_path") and not base_logical_path:
                    base_logical_path = str(Path(fe["logical_path"]).parent)
                if base_forensic_path and base_logical_path:
                    break

        # Process each entry
        for entry in entries:
            try:
                _process_blockfile_entry(
                    evidence_conn=evidence_conn,
                    evidence_id=evidence_id,
                    entry=entry,
                    cache_dir=cache_dir,
                    extraction_dir=extraction_dir,
                    run_id=run_id,
                    extractor_version=extractor_version,
                    discovered_by=discovered_by,
                    warning_collector=warning_collector,
                    stats=stats,
                    base_forensic_path=base_forensic_path,
                    base_logical_path=base_logical_path,
                )
            except Exception as e:
                LOGGER.warning("Failed to process blockfile entry %s: %s", entry.url[:50], e)

        # Update inventory status.
        # The index file entry carries the directory-level totals;
        # subsidiary files (data_*, f_*) get zero counts so that SUM()
        # across all inventory rows reflects the true total.
        summary_note = (
            f"Blockfile cache: {stats['entries']} entries, "
            f"{stats['urls']} URLs, {stats['images']} images"
        )
        for inv_id in inventory_ids:
            if inv_id == index_inventory_id:
                update_inventory_ingestion_status(
                    evidence_conn, inv_id,
                    status="ok",
                    urls_parsed=stats["urls"],
                    records_parsed=stats["records"],
                    notes=summary_note,
                )
            else:
                update_inventory_ingestion_status(
                    evidence_conn, inv_id,
                    status="ok",
                    urls_parsed=0,
                    records_parsed=0,
                    notes=summary_note,
                )

    except Exception as e:
        LOGGER.error("Failed to parse blockfile cache %s: %s", cache_dir, e, exc_info=True)
        for inv_id in inventory_ids:
            update_inventory_ingestion_status(
                evidence_conn, inv_id,
                status="failed",
                notes=f"Blockfile parsing failed: {e}",
            )

    return stats


def _process_blockfile_entry(
    evidence_conn,
    evidence_id: int,
    entry,
    cache_dir: Path,
    extraction_dir: Path,
    run_id: str,
    extractor_version: str,
    discovered_by: str,
    warning_collector: Optional["ExtractionWarningCollector"],
    stats: Dict[str, int],
    base_forensic_path: Optional[str] = None,
    base_logical_path: Optional[str] = None,
) -> None:
    """Process a single blockfile cache entry."""
    timestamp = entry.last_used_time or entry.creation_time
    if timestamp:
        timestamp_str = timestamp.isoformat()
    else:
        timestamp_str = datetime.now(timezone.utc).isoformat()

    parsed_url = urlparse(entry.url)

    # Parse HTTP headers from stream 0
    http_info = {"response_code": None, "content_type": None, "content_encoding": None}
    if entry.data_sizes[0] > 0:
        stream0_data = read_stream_data(cache_dir, entry, 0)
        if stream0_data:
            http_info = parse_http_headers(stream0_data)

    # Build source_path from forensic provenance (prefer forensic > logical > workstation)
    if base_forensic_path:
        source_path = f"{base_forensic_path}/{entry.source_file}"
    elif base_logical_path:
        source_path = f"{base_logical_path}/{entry.source_file}"
    else:
        source_path = str(cache_dir / entry.source_file)

    # Insert URL record only for entries that carry actual URLs.
    # Opaque cache keys (SHA-256 hashes, GPU shader hashes) should not
    # pollute the urls table.
    if is_cache_url(entry.url):
        url_record = {
            "url": entry.url,
            "domain": parsed_url.netloc,
            "scheme": parsed_url.scheme,
            "discovered_by": discovered_by,
            "first_seen_utc": timestamp_str,
            "last_seen_utc": timestamp_str,
            "source_path": source_path,
            "notes": f"Blockfile cache entry (offset {entry.block_offset})",
            "context": None,
            "run_id": run_id,
            "cache_key": entry.raw_cache_key or entry.url,
            "cache_filename": entry.source_file,
            "response_code": http_info.get("response_code"),
            "content_type": http_info.get("content_type"),
            "tags": json.dumps({
                "cache_backend": "blockfile",
                "entry_hash": entry.entry_hash,
                "entry_state": entry.state,
                "stream0_size": entry.data_sizes[0],
                "stream1_size": entry.data_sizes[1],
                "content_encoding": http_info.get("content_encoding"),
                "creation_time": entry.creation_time.isoformat() if entry.creation_time else None,
                "last_used_time": entry.last_used_time.isoformat() if entry.last_used_time else None,
                "raw_cache_key": entry.raw_cache_key,
                "forensic_path": f"{base_forensic_path}/{entry.source_file}" if base_forensic_path else None,
                "logical_path": f"{base_logical_path}/{entry.source_file}" if base_logical_path else None,
            }),
        }

        insert_urls(evidence_conn, evidence_id, [url_record])
        stats["urls"] += 1
        stats["records"] += 1
    else:
        LOGGER.debug("Skipping non-URL blockfile cache key: %s", entry.url[:80])

    # Process body stream for images
    if entry.data_sizes[1] > 0:
        stream1_data = read_stream_data(cache_dir, entry, 1)
        if stream1_data:
            content_encoding = http_info.get("content_encoding")
            body = decompress_body(
                stream1_data,
                content_encoding,
                warning_collector=warning_collector,
                source_file=str(cache_dir / entry.source_file),
            )

            if body and len(body) >= 8:
                fmt = detect_image_format(body)
                if fmt:
                    image_info = carve_blockfile_image(
                        body=body,
                        fmt=fmt,
                        entry=entry,
                        extraction_dir=extraction_dir,
                        run_id=run_id,
                    )

                    if image_info:
                        _insert_blockfile_image(
                            evidence_conn=evidence_conn,
                            evidence_id=evidence_id,
                            image_info=image_info,
                            entry=entry,
                            http_info=http_info,
                            body=body,
                            run_id=run_id,
                            extractor_version=extractor_version,
                        )
                        stats["images"] += 1
                        stats["records"] += 1


def _insert_blockfile_image(
    evidence_conn,
    evidence_id: int,
    image_info: Dict[str, Any],
    entry,
    http_info: Dict[str, Any],
    body: bytes,
    run_id: str,
    extractor_version: str,
) -> None:
    """Insert a carved blockfile image into the database."""
    ts_utc = None
    if entry.last_used_time:
        ts_utc = entry.last_used_time.isoformat()
    elif entry.creation_time:
        ts_utc = entry.creation_time.isoformat()

    image_data = {
        "rel_path": image_info["rel_path"],
        "filename": image_info["filename"],
        "sha256": image_info["sha256"],
        "md5": image_info["md5"],
        "phash": image_info["phash"],
        "size_bytes": image_info["size_bytes"],
        "ts_utc": ts_utc,
    }

    source_metadata = {
        "cache_backend": "blockfile",
        "response_code": http_info.get("response_code"),
        "content_type": http_info.get("content_type"),
        "content_encoding": http_info.get("content_encoding"),
        "stream0_size": entry.data_sizes[0],
        "stream1_size": entry.data_sizes[1],
        "entry_hash": entry.entry_hash,
        "entry_state": entry.state,
        "http_body_size_bytes": len(body) if body else 0,
        "creation_time": entry.creation_time.isoformat() if entry.creation_time else None,
        "raw_headers_text": http_info.get("raw_headers_text"),
        "headers": http_info.get("headers", {}),
        "body_storage_path": image_info["rel_path"],
    }

    discovery_data = {
        "discovered_by": "cache_blockfile",
        "run_id": run_id,
        "extractor_version": extractor_version,
        "cache_url": entry.url,
        "cache_key": entry.raw_cache_key or entry.url,
        "cache_filename": image_info["filename"],
        "cache_response_time": ts_utc,
        "source_metadata_json": json.dumps(source_metadata),
    }

    try:
        insert_image_with_discovery(
            evidence_conn, evidence_id, image_data, discovery_data
        )
    except Exception as e:
        if "UNIQUE constraint" not in str(e):
            LOGGER.warning("Failed to insert image: %s", e)


# ---------------------------------------------------------------------------
# Orphan data-file carving
# ---------------------------------------------------------------------------

def _carve_orphan_data_files(
    evidence_conn,
    evidence_id: int,
    cache_dir: Path,
    extraction_dir: Path,
    run_id: str,
    extractor_version: str,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> int:
    """
    Scan orphan blockfile data files for images when the index has no entries.

    When a blockfile cache index has been cleared but external data files
    (``f_XXXXXX``) still exist on disk, this function reads each file and
    attempts to detect and carve images.  This is a best-effort recovery
    mechanism for damaged or partially-cleared caches.

    Only external files (``f_*``) are scanned because their content is raw
    cached data.  Block files (``data_2``, ``data_3``) are skipped since
    locating individual entries within them requires the index metadata
    that is no longer available.

    Args:
        evidence_conn: Database connection.
        evidence_id: Evidence ID.
        cache_dir: Path to blockfile cache directory.
        extraction_dir: Base extraction directory.
        run_id: Current extraction run ID.
        extractor_version: Extractor version string.
        warning_collector: Optional extraction-warning collector.

    Returns:
        Number of images successfully carved and inserted.
    """
    f_files = sorted(cache_dir.glob("f_*"))
    if not f_files:
        return 0

    LOGGER.info(
        "Attempting orphan image carving on %d external files in %s",
        len(f_files), cache_dir,
    )

    images_carved = 0
    discovered_by = f"cache_blockfile_orphan:{extractor_version}:{run_id}"

    for f_path in f_files:
        if not f_path.is_file():
            continue
        try:
            body = f_path.read_bytes()
            if len(body) < 8:
                continue

            # Try decompression for gzip-wrapped content (common for HTML/JS
            # but occasionally wraps images).
            actual_body = body
            if body[:2] == b'\x1f\x8b':  # gzip magic
                try:
                    actual_body = decompress_body(
                        body, "gzip",
                        warning_collector=warning_collector,
                        source_file=str(f_path),
                    )
                    if actual_body is None or len(actual_body) < 8:
                        continue
                except Exception:
                    actual_body = body  # keep raw bytes if decompression fails

            fmt = detect_image_format(actual_body)
            if not fmt:
                continue

            image_info = carve_blockfile_image(
                body=actual_body,
                fmt=fmt,
                entry=None,  # No entry metadata available
                extraction_dir=extraction_dir,
                run_id=run_id,
            )
            if not image_info:
                continue

            image_data = {
                "rel_path": image_info["rel_path"],
                "filename": image_info["filename"],
                "sha256": image_info["sha256"],
                "md5": image_info["md5"],
                "phash": image_info["phash"],
                "size_bytes": image_info["size_bytes"],
                "ts_utc": None,
            }

            source_metadata = {
                "cache_backend": "blockfile_orphan",
                "orphan_file": f_path.name,
                "orphan_file_size": f_path.stat().st_size,
                "image_format": fmt,
                "body_storage_path": image_info["rel_path"],
            }

            discovery_data = {
                "discovered_by": discovered_by,
                "run_id": run_id,
                "extractor_version": extractor_version,
                "cache_url": None,
                "cache_key": f"orphan:{f_path.name}",
                "cache_filename": image_info["filename"],
                "cache_response_time": None,
                "source_metadata_json": json.dumps(source_metadata),
            }

            insert_image_with_discovery(
                evidence_conn, evidence_id, image_data, discovery_data
            )
            images_carved += 1

        except Exception as e:
            if "UNIQUE constraint" not in str(e):
                LOGGER.debug("Orphan carving failed for %s: %s", f_path.name, e)

    if images_carved:
        LOGGER.info(
            "Carved %d images from %d orphan files in %s",
            images_carved, len(f_files), cache_dir,
        )

    return images_carved
