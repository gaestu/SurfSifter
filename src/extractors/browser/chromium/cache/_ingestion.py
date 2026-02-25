"""
Cache file ingestion utilities.

Parses extracted cache files and ingests URLs/images into the database.
"""

from __future__ import annotations

import hashlib
import json
import re
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

from ._parser import (
    parse_cache_entry,
    read_stream,
    parse_http_headers,
)
from ._decompression import decompress_body
from ._carving import carve_and_hash_image, detect_image_format
from ._index import IndexEntry, parse_index_file
from ._workers import CHUNK_SIZE

if TYPE_CHECKING:
    from ....callbacks import ExtractorCallbacks
    from ...._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.cache_simple.ingestion")


def compute_deferred_hashes(
    manifest_data: Dict[str, Any],
    manifest_path: Path,
    output_dir: Path,
    callbacks: "ExtractorCallbacks",
) -> int:
    """
    Compute MD5+SHA-256 hashes for files that were extracted without hashing.

    Args:
        manifest_data: Loaded manifest dict (will be modified in place)
        manifest_path: Path to manifest file (will be updated)
        output_dir: Base extraction output directory
        callbacks: Progress callbacks

    Returns:
        Number of hashes computed
    """
    files_to_hash = manifest_data.get("files", [])
    hashes_computed = 0

    for idx, file_entry in enumerate(files_to_hash):
        if file_entry.get("sha256"):
            continue

        extracted_rel_path = file_entry["extracted_path"]
        if Path(extracted_rel_path).is_absolute():
            extracted_path = Path(extracted_rel_path)
        else:
            extracted_path = output_dir / extracted_rel_path

        if extracted_path.exists():
            md5 = hashlib.md5()
            sha256 = hashlib.sha256()
            with open(extracted_path, "rb") as f:
                for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                    md5.update(chunk)
                    sha256.update(chunk)
            file_entry["md5"] = md5.hexdigest()
            file_entry["sha256"] = sha256.hexdigest()
            hashes_computed += 1

        callbacks.on_progress(idx + 1, len(files_to_hash))

    if hashes_computed > 0:
        manifest_data["hash_mode"] = "extraction"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))
        callbacks.on_log(f"Computed {hashes_computed} deferred MD5+SHA-256 hashes")

    return hashes_computed


def register_inventory_entry(
    evidence_conn,
    evidence_id: int,
    run_id: str,
    manifest: Dict[str, Any],
    file_entry: Dict[str, Any],
) -> int:
    """
    Insert row into browser_cache_inventory table.

    Args:
        evidence_conn: Database connection
        evidence_id: Evidence ID
        run_id: Extraction run ID
        manifest: Manifest data with extraction metadata
        file_entry: File entry from manifest

    Returns:
        Inventory entry ID
    """
    return insert_browser_inventory(
        evidence_conn,
        evidence_id=evidence_id,
        browser=file_entry["browser"],
        artifact_type=file_entry["artifact_type"],
        profile=file_entry.get("profile"),
        partition_index=file_entry.get("partition_index"),
        fs_type=file_entry.get("fs_type"),
        logical_path=file_entry["logical_path"],
        forensic_path=file_entry.get("forensic_path"),
        run_id=run_id,
        extracted_path=file_entry["extracted_path"],
        extraction_status=manifest.get("status", "ok"),
        extraction_timestamp_utc=manifest["extraction_timestamp_utc"],
        extraction_tool=manifest.get("extraction_tool"),
        file_size_bytes=file_entry.get("size_bytes"),
        file_md5=file_entry.get("md5"),
        file_sha256=file_entry.get("sha256"),
    )


def parse_and_ingest_cache_file(
    evidence_conn,
    evidence_id: int,
    run_id: str,
    file_entry: Dict[str, Any],
    extraction_dir: Path,
    callbacks: "ExtractorCallbacks",
    extractor_version: str,
    index_lookup: Optional[Dict[int, IndexEntry]] = None,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> Dict[str, int]:
    """
    Parse a single cache file and insert URLs/images into database.

    Args:
        evidence_conn: Database connection
        evidence_id: Evidence ID
        run_id: Extraction run ID
        file_entry: File entry from manifest
        extraction_dir: Base extraction directory
        callbacks: Progress callbacks
        extractor_version: Version string for discovered_by
        index_lookup: Optional index entry lookup table
        warning_collector: Optional warning collector

    Returns:
        Dict with urls, images, records counts
    """
    extracted_path = file_entry["extracted_path"]
    if Path(extracted_path).is_absolute():
        cache_file_path = Path(extracted_path)
    else:
        cache_file_path = extraction_dir / extracted_path

    stats = {"urls": 0, "images": 0, "records": 0}

    filename = cache_file_path.name
    file_type = file_entry.get("file_type", "unknown")
    entry_hash_str = file_entry.get("entry_hash")

    # Skip non-entry files
    if filename in ('index', 'the-real-index') or file_type == "index":
        return stats

    if filename.startswith('f_') or file_type == "block":
        LOGGER.debug("Skipping block file: %s", filename)
        return stats

    if re.match(r'^data_[0-3]$', filename) or file_type == "data_block":
        LOGGER.debug("Skipping blockfile data file: %s", filename)
        return stats

    if not filename.endswith('_0') and file_type not in ("entry", "sparse"):
        LOGGER.debug("Skipping non-primary entry file: %s", filename)
        return stats

    try:
        entry = parse_cache_entry(cache_file_path, warning_collector=warning_collector)

        if not entry:
            LOGGER.debug("Could not parse cache entry: %s", cache_file_path)
            return stats

        # Read and parse HTTP headers from stream 0
        stream0_bytes = read_stream(cache_file_path, entry.stream0_offset, entry.stream0_size)
        http_info = parse_http_headers(stream0_bytes)
        entry.http_info = http_info

        # Look up index entry for timestamps
        last_used_time = None
        index_entry_size = None
        if index_lookup and entry_hash_str:
            try:
                entry_hash_int = int(entry_hash_str, 16)
                index_entry = index_lookup.get(entry_hash_int)
                if index_entry:
                    last_used_time = index_entry.last_used_time
                    index_entry_size = index_entry.entry_size
            except ValueError:
                pass

        discovered_by = f"cache_simple:{extractor_version}:{run_id}"
        parsed_url = urlparse(entry.url)

        if last_used_time:
            timestamp_str = last_used_time.isoformat()
        else:
            timestamp_str = datetime.now(timezone.utc).isoformat()

        # Build source_path from forensic provenance (prefer forensic > logical > workstation)
        source_path = (
            file_entry.get("forensic_path")
            or file_entry.get("logical_path")
            or str(cache_file_path)
        )

        # Insert URL record
        url_record = {
            "url": entry.url,
            "domain": parsed_url.netloc,
            "scheme": parsed_url.scheme,
            "discovered_by": discovered_by,
            "first_seen_utc": timestamp_str,
            "last_seen_utc": timestamp_str,
            "source_path": source_path,
            "notes": None,
            "context": None,
            "run_id": run_id,
            "cache_key": entry.url,
            "cache_filename": filename,
            "response_code": http_info.get("response_code"),
            "content_type": http_info.get("content_type"),
            "tags": json.dumps({
                "stream0_size": entry.stream0_size,
                "stream1_size": entry.stream1_size,
                "content_encoding": http_info.get("content_encoding"),
                "entry_version": entry.version,
                "last_used_time": timestamp_str if last_used_time else None,
                "index_entry_size": index_entry_size,
                "forensic_path": file_entry.get("forensic_path"),
                "logical_path": file_entry.get("logical_path"),
            }),
        }

        insert_urls(evidence_conn, evidence_id, [url_record])
        stats["urls"] += 1
        stats["records"] += 1

        # Process body stream for images
        if entry.stream1_size > 0:
            stream1_bytes = read_stream(cache_file_path, entry.stream1_offset, entry.stream1_size)

            if stream1_bytes:
                content_encoding = http_info.get("content_encoding")
                body = decompress_body(
                    stream1_bytes,
                    content_encoding,
                    warning_collector=warning_collector,
                    source_file=str(cache_file_path),
                )

                image_info = carve_and_hash_image(
                    body, extraction_dir, entry, run_id
                )

                if image_info:
                    _insert_carved_image(
                        evidence_conn=evidence_conn,
                        evidence_id=evidence_id,
                        image_info=image_info,
                        entry=entry,
                        http_info=http_info,
                        body=body,
                        last_used_time=last_used_time,
                        filename=filename,
                        run_id=run_id,
                        extractor_version=extractor_version,
                        discovered_by="cache_simple",
                    )
                    stats["images"] += 1
                    stats["records"] += 1

    except Exception as e:
        LOGGER.warning("Failed to parse %s: %s", cache_file_path, e)
        stats["notes"] = str(e)

    return stats


def _insert_carved_image(
    evidence_conn,
    evidence_id: int,
    image_info: Dict[str, Any],
    entry,
    http_info: Dict[str, Any],
    body: bytes,
    last_used_time,
    filename: str,
    run_id: str,
    extractor_version: str,
    discovered_by: str,
) -> None:
    """Insert a carved image into the database."""
    image_data = {
        "rel_path": image_info["rel_path"],
        "filename": image_info["filename"],
        "sha256": image_info["sha256"],
        "md5": image_info["md5"],
        "phash": image_info["phash"],
        "size_bytes": image_info["size_bytes"],
        "ts_utc": last_used_time.isoformat() if last_used_time else None,
    }

    source_metadata = {
        "response_code": http_info.get("response_code"),
        "content_type": http_info.get("content_type"),
        "content_encoding": http_info.get("content_encoding"),
        "stream0_size": entry.stream0_size,
        "stream1_size": entry.stream1_size,
        "entry_version": entry.version,
        "http_body_size_bytes": len(body) if body else 0,
        "raw_headers_text": http_info.get("raw_headers_text"),
        "headers": http_info.get("headers", {}),
        "body_storage_path": image_info["rel_path"],
    }

    discovery_data = {
        "discovered_by": discovered_by,
        "run_id": run_id,
        "extractor_version": extractor_version,
        "cache_url": entry.url,
        "cache_key": entry.url,
        "cache_filename": filename,
        "cache_response_time": last_used_time.isoformat() if last_used_time else None,
        "source_metadata_json": json.dumps(source_metadata),
    }

    try:
        insert_image_with_discovery(
            evidence_conn, evidence_id, image_data, discovery_data
        )
    except Exception as e:
        if "UNIQUE constraint" not in str(e):
            LOGGER.warning("Failed to insert image: %s", e)


def build_index_lookup(
    files: List[Dict[str, Any]],
    output_dir: Path,
    callbacks: "ExtractorCallbacks",
) -> Dict[int, IndexEntry]:
    """
    Build a lookup table from cache index files.

    Parses all index files and builds a hash->entry lookup table
    for timestamp correlation.

    Args:
        files: List of file entries from manifest
        output_dir: Base extraction directory
        callbacks: Progress callbacks

    Returns:
        Dict mapping entry_hash (int) to IndexEntry
    """
    index_lookup: Dict[int, IndexEntry] = {}

    index_files = [
        f for f in files
        if f.get("file_type") == "index" or
           Path(f.get("extracted_path", "")).name in ("index", "the-real-index")
    ]

    for file_entry in index_files:
        extracted_path = file_entry["extracted_path"]
        if Path(extracted_path).is_absolute():
            index_path = Path(extracted_path)
        else:
            index_path = output_dir / extracted_path

        if not index_path.exists():
            LOGGER.debug("Index file not found: %s", index_path)
            continue

        try:
            metadata, entries = parse_index_file(index_path)

            if metadata:
                LOGGER.debug(
                    "Parsed index %s: version=%d, %d entries, cache_size=%d bytes",
                    index_path.name,
                    metadata.version,
                    len(entries),
                    metadata.cache_size,
                )

            for entry in entries:
                existing = index_lookup.get(entry.entry_hash)
                if existing is None or entry.last_used_time > existing.last_used_time:
                    index_lookup[entry.entry_hash] = entry

        except Exception as e:
            LOGGER.warning("Failed to parse index file %s: %s", index_path, e)

    return index_lookup
