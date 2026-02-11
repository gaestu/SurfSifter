"""
LevelDB parsing methods for Chromium Browser Storage Extractor.

Contains parsing logic for Local Storage, Session Storage, and IndexedDB,
with extraction warning support for unknown schemas and parse errors.

Extracted from extractor.py with schema warning integration
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, TYPE_CHECKING

from core.logging import get_logger
from ....image_signatures import detect_image_type, get_extension_for_format

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

from ._schemas import (
    KNOWN_LOCAL_STORAGE_PREFIXES,
    KNOWN_SESSION_STORAGE_PREFIXES,
    ARTIFACT_TYPE_LOCAL_STORAGE,
    ARTIFACT_TYPE_SESSION_STORAGE,
    ARTIFACT_TYPE_INDEXEDDB,
    ARTIFACT_TYPE_INDEXEDDB_BLOB,
    extract_unknown_prefix,
    is_interesting_indexeddb_origin,
)

LOGGER = get_logger("extractors.browser.chromium.storage.parsers")


def classify_value_type(value: str) -> str:
    """Classify the type of a storage value."""
    if not value:
        return "empty"

    value = value.strip()

    if value.startswith("{") or value.startswith("["):
        try:
            json.loads(value)
            return "json"
        except Exception:
            pass

    if value.lower() in ("true", "false"):
        return "boolean"

    try:
        float(value)
        return "number"
    except ValueError:
        pass

    return "string"


def format_kv_record(
    browser: str,
    profile: str,
    origin: str,
    key: str,
    value: str,
    storage_type: str,
    loc: Dict,
    run_id: str,
    excerpt_size: int
) -> Dict:
    """Format a key-value record for database insertion."""
    value_str = str(value) if value else ""
    value_bytes = value_str.encode('utf-8', errors='replace')

    return {
        "run_id": run_id,
        "browser": browser,
        "profile": profile,
        "origin": origin,
        "key": key,
        "value": value_str[:excerpt_size] if len(value_str) > excerpt_size else value_str,
        "value_type": classify_value_type(value_str),
        "value_size": len(value_bytes),
        "source_path": loc.get("logical_path"),
        "partition_index": loc.get("partition_index"),
        "fs_type": loc.get("fs_type"),
        "logical_path": loc.get("logical_path"),
        "forensic_path": loc.get("forensic_path"),
        "notes": None,
    }


def parse_leveldb_storage(
    path: Path,
    loc: Dict,
    run_id: str,
    evidence_id: int,
    storage_type: str,
    excerpt_size: int,
    include_deleted: bool,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[Dict]:
    """
    Parse LevelDB storage directory.

    Args:
        path: Path to extracted LevelDB directory
        loc: Location dict with browser, profile, partition_index etc.
        run_id: Extraction run ID
        evidence_id: Evidence ID
        storage_type: "local_storage" or "session_storage"
        excerpt_size: Max size for value excerpt
        include_deleted: Include deleted/historical records
        warning_collector: Optional collector for extraction warnings

    Returns:
        List of record dicts for database insertion
    """
    from extractors._shared.leveldb_wrapper import LevelDBWrapper, is_leveldb_available

    records = []
    browser = loc.get("browser", "unknown")
    profile = loc.get("profile")
    source_file = loc.get("logical_path", str(path))
    unknown_prefixes_seen: set = set()

    if not is_leveldb_available():
        LOGGER.warning("ccl_chromium_reader not available for LevelDB parsing")
        if warning_collector:
            from extractors._shared.extraction_warnings import (
                WARNING_TYPE_LEVELDB_PARSE_ERROR,
                CATEGORY_LEVELDB,
                SEVERITY_ERROR,
            )
            warning_collector.add_warning(
                warning_type=WARNING_TYPE_LEVELDB_PARSE_ERROR,
                item_name="ccl_chromium_reader",
                severity=SEVERITY_ERROR,
                category=CATEGORY_LEVELDB,
                artifact_type=ARTIFACT_TYPE_LOCAL_STORAGE if storage_type == "local_storage" else ARTIFACT_TYPE_SESSION_STORAGE,
                source_file=source_file,
                item_value="Library not installed - LevelDB parsing unavailable",
            )
        return records

    try:
        wrapper = LevelDBWrapper(path, include_deleted=include_deleted)
        try:
            if storage_type == "local_storage":
                for rec in wrapper.iter_local_storage():
                    records.append(format_kv_record(
                        browser, profile, rec.origin, rec.key, rec.value,
                        storage_type, loc, run_id, excerpt_size
                    ))
            else:
                for rec in wrapper.iter_session_storage():
                    records.append(format_kv_record(
                        browser, profile, rec.origin, rec.key, rec.value,
                        storage_type, loc, run_id, excerpt_size
                    ))

            # Check for unknown prefixes in raw records if warning collector provided
            if warning_collector:
                _check_unknown_prefixes(
                    wrapper, storage_type, source_file,
                    unknown_prefixes_seen, warning_collector
                )

        finally:
            wrapper.close()
    except Exception as e:
        LOGGER.warning("LevelDB parsing failed for %s: %s", path, e)
        if warning_collector:
            from extractors._shared.extraction_warnings import (
                WARNING_TYPE_LEVELDB_PARSE_ERROR,
                CATEGORY_LEVELDB,
                SEVERITY_ERROR,
            )
            warning_collector.add_warning(
                warning_type=WARNING_TYPE_LEVELDB_PARSE_ERROR,
                item_name=str(path.name),
                severity=SEVERITY_ERROR,
                category=CATEGORY_LEVELDB,
                artifact_type=ARTIFACT_TYPE_LOCAL_STORAGE if storage_type == "local_storage" else ARTIFACT_TYPE_SESSION_STORAGE,
                source_file=source_file,
                item_value=str(e),
            )

    return records


def _check_unknown_prefixes(
    wrapper,
    storage_type: str,
    source_file: str,
    seen_prefixes: set,
    warning_collector: "ExtractionWarningCollector",
) -> None:
    """Check for unknown LevelDB key prefixes and report them."""
    from extractors._shared.extraction_warnings import (
        WARNING_TYPE_LEVELDB_UNKNOWN_PREFIX,
        CATEGORY_LEVELDB,
        SEVERITY_INFO,
    )

    known_prefixes = (
        KNOWN_LOCAL_STORAGE_PREFIXES
        if storage_type == "local_storage"
        else KNOWN_SESSION_STORAGE_PREFIXES
    )
    artifact_type = (
        ARTIFACT_TYPE_LOCAL_STORAGE
        if storage_type == "local_storage"
        else ARTIFACT_TYPE_SESSION_STORAGE
    )

    try:
        # Sample first 100 raw records to check for unknown prefixes
        count = 0
        for record in wrapper.iterate_records_raw():
            if count >= 100:
                break
            count += 1

            unknown_prefix = extract_unknown_prefix(record.key, known_prefixes)
            if unknown_prefix and unknown_prefix not in seen_prefixes:
                seen_prefixes.add(unknown_prefix)
                warning_collector.add_warning(
                    warning_type=WARNING_TYPE_LEVELDB_UNKNOWN_PREFIX,
                    item_name=unknown_prefix,
                    severity=SEVERITY_INFO,
                    category=CATEGORY_LEVELDB,
                    artifact_type=artifact_type,
                    source_file=source_file,
                    item_value=f"Unknown key prefix in {storage_type}",
                )
    except Exception as e:
        LOGGER.debug("Error checking unknown prefixes: %s", e)


def parse_indexeddb_storage(
    path: Path,
    loc: Dict,
    run_id: str,
    evidence_id: int,
    excerpt_size: int,
    include_deleted: bool,
    extract_images: bool,
    images_dir: Path,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
) -> List[tuple]:
    """
    Parse IndexedDB storage directory.

    Args:
        path: Path to extracted IndexedDB directory
        loc: Location dict with browser, profile, partition_index etc.
        run_id: Extraction run ID
        evidence_id: Evidence ID
        excerpt_size: Max size for value excerpt
        include_deleted: Include deleted/historical records
        extract_images: Extract images from blob values
        images_dir: Directory to save extracted images
        warning_collector: Optional collector for extraction warnings

    Returns:
        List of tuples: (db_record, entries, extracted_images)
    """
    from extractors._shared.leveldb_wrapper import LevelDBWrapper, is_leveldb_available

    results = []
    browser = loc.get("browser", "unknown")
    profile = loc.get("profile")
    source_file = loc.get("logical_path", str(path))

    if not is_leveldb_available():
        LOGGER.warning("ccl_chromium_reader not available for IndexedDB parsing")
        if warning_collector:
            from extractors._shared.extraction_warnings import (
                WARNING_TYPE_LEVELDB_PARSE_ERROR,
                CATEGORY_LEVELDB,
                SEVERITY_ERROR,
            )
            warning_collector.add_warning(
                warning_type=WARNING_TYPE_LEVELDB_PARSE_ERROR,
                item_name="ccl_chromium_reader",
                severity=SEVERITY_ERROR,
                category=CATEGORY_LEVELDB,
                artifact_type=ARTIFACT_TYPE_INDEXEDDB,
                source_file=source_file,
                item_value="Library not installed - IndexedDB parsing unavailable",
            )
        return results

    try:
        wrapper = LevelDBWrapper(path, include_deleted=include_deleted)
        try:
            for db_info in wrapper.iter_indexeddb_databases():
                origin = db_info.get("origin", "")
                db_name = db_info.get("name", "")

                db_record = {
                    "run_id": run_id,
                    "browser": browser,
                    "profile": profile,
                    "origin": origin,
                    "database_name": db_name,
                    "version": db_info.get("version"),
                    "object_store_count": db_info.get("object_store_count", 0),
                    "source_path": loc.get("logical_path"),
                    "partition_index": loc.get("partition_index"),
                    "fs_type": loc.get("fs_type"),
                    "logical_path": loc.get("logical_path"),
                    "forensic_path": loc.get("forensic_path"),
                }

                entries = []
                extracted_images = []

                # Log interesting origins
                if warning_collector and is_interesting_indexeddb_origin(origin):
                    from extractors._shared.extraction_warnings import (
                        SEVERITY_INFO,
                        CATEGORY_LEVELDB,
                    )
                    warning_collector.add_warning(
                        warning_type="interesting_origin",
                        item_name=origin,
                        severity=SEVERITY_INFO,
                        category=CATEGORY_LEVELDB,
                        artifact_type=ARTIFACT_TYPE_INDEXEDDB,
                        source_file=source_file,
                        item_value=f"Database: {db_name}",
                        context_json={"database_name": db_name, "object_store_count": db_info.get("object_store_count", 0)},
                    )

                for entry in db_info.get("entries", []):
                    value = entry.get("value", "")
                    value_str = str(value) if value else ""

                    entry_record = {
                        "run_id": run_id,
                        "object_store": entry.get("object_store", ""),
                        "key": str(entry.get("key", "")),
                        "value": value_str[:excerpt_size] if len(value_str) > excerpt_size else value_str,
                        "value_type": classify_value_type(value_str),
                        "value_size": len(value_str.encode('utf-8', errors='replace')),
                    }
                    entries.append(entry_record)

                    # Check for image blobs
                    if extract_images and isinstance(value, bytes):
                        img_result = extract_image_from_blob(
                            value, origin, entry.get("key", ""),
                            run_id, evidence_id, images_dir,
                            warning_collector=warning_collector,
                            source_file=source_file,
                        )
                        if img_result:
                            extracted_images.append(img_result)

                results.append((db_record, entries, extracted_images))
        finally:
            wrapper.close()
    except Exception as e:
        LOGGER.warning("IndexedDB parsing failed for %s: %s", path, e)
        if warning_collector:
            from extractors._shared.extraction_warnings import (
                WARNING_TYPE_LEVELDB_PARSE_ERROR,
                CATEGORY_LEVELDB,
                SEVERITY_ERROR,
            )
            warning_collector.add_warning(
                warning_type=WARNING_TYPE_LEVELDB_PARSE_ERROR,
                item_name=str(path.name),
                severity=SEVERITY_ERROR,
                category=CATEGORY_LEVELDB,
                artifact_type=ARTIFACT_TYPE_INDEXEDDB,
                source_file=source_file,
                item_value=str(e),
            )

    return results


def extract_image_from_blob(
    blob_data: bytes,
    origin: str,
    key: str,
    run_id: str,
    evidence_id: int,
    images_dir: Path,
    *,
    warning_collector: Optional["ExtractionWarningCollector"] = None,
    source_file: Optional[str] = None,
) -> Optional[tuple]:
    """
    Extract image from IndexedDB blob data.

    Args:
        blob_data: Raw blob bytes
        origin: IndexedDB origin
        key: Record key
        run_id: Extraction run ID
        evidence_id: Evidence ID
        images_dir: Directory to save extracted images
        warning_collector: Optional collector for extraction warnings
        source_file: Source file for warning context

    Returns:
        Tuple of (image_data, discovery_data) or None
    """
    if len(blob_data) < 8:
        return None

    image_type = detect_image_type(blob_data)
    if not image_type:
        return None

    ext = get_extension_for_format(image_type) or ".bin"

    # Calculate hashes
    md5 = hashlib.md5(blob_data).hexdigest()
    sha256 = hashlib.sha256(blob_data).hexdigest()

    # Save image
    filename = f"{sha256[:16]}{ext}"
    dest_path = images_dir / filename

    try:
        dest_path.write_bytes(blob_data)
    except Exception as e:
        LOGGER.warning("Failed to save image blob: %s", e)
        if warning_collector:
            from extractors._shared.extraction_warnings import (
                WARNING_TYPE_BINARY_FORMAT_ERROR,
                CATEGORY_BINARY,
                SEVERITY_WARNING,
            )
            warning_collector.add_warning(
                warning_type=WARNING_TYPE_BINARY_FORMAT_ERROR,
                item_name=filename,
                severity=SEVERITY_WARNING,
                category=CATEGORY_BINARY,
                artifact_type=ARTIFACT_TYPE_INDEXEDDB_BLOB,
                source_file=source_file,
                item_value=f"Failed to save: {e}",
            )
        return None

    # Build image record
    image_data = {
        "md5": md5,
        "sha256": sha256,
        "size_bytes": len(blob_data),
        "format": image_type,
        "extracted_path": str(dest_path),
    }

    discovery_data = {
        "discovered_by": "chromium_browser_storage",
        "discovery_path": f"indexeddb:{origin}:{key}",
        "run_id": run_id,
    }

    return (image_data, discovery_data)
