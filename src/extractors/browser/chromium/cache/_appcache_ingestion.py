"""
Application Cache (AppCache) ingestion utilities.

Handles parsing and ingestion of the Chromium Application Cache format,
which consists of:
- A SQLite ``Index`` database containing Groups, Caches, and Entries tables
- A standard blockfile cache in ``Cache/`` where keys are numeric response_ids

The SQLite Index maps ``response_id -> url``, allowing us to resolve the
opaque numeric keys produced by the blockfile parser into real URLs.

Application Cache was part of the HTML5 offline spec and was removed in
Chrome 93 (2021).  It remains forensically relevant for older browsers and
embedded Chromium applications.
"""

from __future__ import annotations

import json
import sqlite3
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

LOGGER = get_logger("extractors.cache_simple.appcache_ingestion")

# Chromium WebKit epoch: 1601-01-01 00:00:00 UTC
_WEBKIT_EPOCH = datetime(1601, 1, 1, tzinfo=timezone.utc)


def _webkit_to_datetime(microseconds: int) -> Optional[datetime]:
    """Convert a Chromium/WebKit timestamp (microseconds since 1601-01-01) to datetime."""
    if not microseconds or microseconds <= 0:
        return None
    try:
        from datetime import timedelta
        dt = _WEBKIT_EPOCH + timedelta(microseconds=microseconds)
        # Reject obviously invalid dates (before 1970 or after 2100)
        if dt.year < 1970 or dt.year > 2100:
            return None
        return dt
    except (OverflowError, ValueError, OSError):
        return None


def find_appcache_directories(
    files: List[Dict[str, Any]],
    output_dir: Path,
) -> List[Dict[str, Any]]:
    """
    Find directories containing Application Cache (Index + Cache/).

    An Application Cache directory is identified by:
    - An ``Index`` SQLite file (the AppCache database)
    - A ``Cache/`` subdirectory containing blockfile data

    The typical layout is::

        Application Cache/
            Index
            Index-journal
            Cache/
                index
                data_0, data_1, data_2, data_3
                f_000001, f_000002, ...

    Args:
        files: List of file entries from manifest
        output_dir: Base extraction directory

    Returns:
        List of dicts with keys: path (to Cache/ dir), index_path (to
        Index SQLite), browser, profile, files (extracted paths).
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

    appcache_dirs: List[Dict[str, Any]] = []

    # Check each directory for the AppCache pattern:
    # We look for dirs containing blockfile data whose *parent* has an Index file.
    # The layout is: "Application Cache/Index" + "Application Cache/Cache/{blockfile}"
    for dir_path_str, dir_entries in dir_files.items():
        dir_path = Path(dir_path_str)

        filenames = {Path(e.get("extracted_path", "")).name for e in dir_entries}

        # Must have blockfile data_1 + index (this is the Cache/ subdir)
        if "data_1" not in filenames or "index" not in filenames:
            continue

        # Verify the blockfile index exists
        blockfile_index = dir_path / "index"
        if not blockfile_index.exists():
            continue

        # Check that the parent directory has an "Index" SQLite file
        # (the Application Cache SQLite database)
        appcache_root = dir_path.parent
        sqlite_index = appcache_root / "Index"
        if not sqlite_index.exists():
            continue

        # Quick check: is the "Index" file a SQLite database with AppCache tables?
        if not _is_appcache_index(sqlite_index):
            continue

        # Verify blockfile format
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

        # Collect all file paths from *both* the Cache/ dir and the parent
        # (for Index and Index-journal)
        all_file_paths = [e.get("extracted_path", "") for e in dir_entries]

        # Also include files from the parent Application Cache directory
        parent_dir_str = str(appcache_root)
        if parent_dir_str in dir_files:
            all_file_paths.extend(
                e.get("extracted_path", "") for e in dir_files[parent_dir_str]
            )

        appcache_dirs.append({
            "path": dir_path,               # Path to Cache/ blockfile dir
            "index_path": sqlite_index,      # Path to SQLite Index
            "browser": browser,
            "profile": profile,
            "files": all_file_paths,
        })

        LOGGER.info(
            "Found Application Cache: %s (Index: %s, browser=%s, profile=%s)",
            dir_path, sqlite_index, browser, profile,
        )

    return appcache_dirs


def _is_appcache_index(path: Path) -> bool:
    """Check if a file is a valid Application Cache SQLite Index."""
    try:
        # Quick SQLite magic check
        with open(path, "rb") as f:
            header = f.read(16)
        if not header.startswith(b"SQLite format 3"):
            return False

        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            # AppCache Index must have these three tables
            return {"Groups", "Caches", "Entries"}.issubset(tables)
        finally:
            conn.close()
    except Exception:
        return False


def _read_appcache_index(index_path: Path) -> Dict[str, Any]:
    """
    Read the Application Cache SQLite Index and build lookup structures.

    Returns:
        Dict with:
        - response_id_to_url: {response_id: url}
        - response_id_to_entry: {response_id: entry_dict}
        - groups: list of group dicts (manifest_url, origin, timestamps)
        - total_entries: count
    """
    conn = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        # Read Groups
        groups = []
        try:
            for row in conn.execute("SELECT * FROM Groups"):
                groups.append({
                    "group_id": row["group_id"],
                    "origin": row["origin"],
                    "manifest_url": row["manifest_url"],
                    "creation_time": _webkit_to_datetime(row["creation_time"]),
                    "last_access_time": _webkit_to_datetime(row["last_access_time"]),
                })
        except Exception as e:
            LOGGER.warning("Failed to read Groups table: %s", e)

        # Build cache_id -> group lookup
        cache_to_group: Dict[int, Dict] = {}
        try:
            for row in conn.execute(
                "SELECT c.cache_id, c.group_id, g.origin, g.manifest_url "
                "FROM Caches c JOIN Groups g ON c.group_id = g.group_id"
            ):
                cache_to_group[row["cache_id"]] = {
                    "group_id": row["group_id"],
                    "origin": row["origin"],
                    "manifest_url": row["manifest_url"],
                }
        except Exception as e:
            LOGGER.warning("Failed to read Caches table: %s", e)

        # Read Entries and build response_id -> url mapping
        response_id_to_url: Dict[int, str] = {}
        response_id_to_entry: Dict[int, Dict[str, Any]] = {}
        try:
            for row in conn.execute("SELECT * FROM Entries"):
                cache_id = row["cache_id"]
                url = row["url"]
                response_id = row["response_id"]
                response_size = row["response_size"]
                flags = row["flags"]

                group_info = cache_to_group.get(cache_id, {})

                response_id_to_url[response_id] = url
                response_id_to_entry[response_id] = {
                    "url": url,
                    "cache_id": cache_id,
                    "response_id": response_id,
                    "response_size": response_size,
                    "flags": flags,
                    "origin": group_info.get("origin"),
                    "manifest_url": group_info.get("manifest_url"),
                }
        except Exception as e:
            LOGGER.warning("Failed to read Entries table: %s", e)

        return {
            "response_id_to_url": response_id_to_url,
            "response_id_to_entry": response_id_to_entry,
            "groups": groups,
            "total_entries": len(response_id_to_url),
        }
    finally:
        conn.close()


def ingest_appcache_directory(
    evidence_conn,
    evidence_id: int,
    run_id: str,
    cache_dir: Path,
    index_path: Path,
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
    Parse and ingest an Application Cache directory.

    Workflow:
    1. Read the SQLite Index to build response_id → url mapping
    2. Parse the blockfile cache in Cache/ (entries have numeric response_id keys)
    3. Resolve each numeric key to its real URL via the SQLite mapping
    4. Insert resolved URLs and images into the evidence database

    Args:
        evidence_conn: Database connection
        evidence_id: Evidence ID
        run_id: Extraction run ID
        cache_dir: Path to Application Cache/Cache/ blockfile directory
        index_path: Path to Application Cache/Index SQLite database
        browser: Browser name
        profile: Profile name
        extraction_dir: Base extraction directory
        callbacks: Progress callbacks
        extractor_version: Extractor version string
        warning_collector: Optional warning collector
        manifest_data: Optional manifest data for inventory registration
        file_entries: Optional file entries for inventory registration

    Returns:
        Dict with urls, images, records, entries, inventory_entries, groups counts
    """
    stats = {
        "urls": 0,
        "images": 0,
        "records": 0,
        "entries": 0,
        "inventory_entries": 0,
        "groups": 0,
    }

    # Register inventory entries
    inventory_ids: List[int] = []
    index_inventory_id: Optional[int] = None
    if manifest_data and file_entries:
        extraction_timestamp = manifest_data.get(
            "extraction_timestamp_utc",
            datetime.now(timezone.utc).isoformat(),
        )
        for file_entry in file_entries:
            try:
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=browser,
                    artifact_type="cache_appcache",
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

                # Remember the SQLite Index inventory row for summary stats
                ep = file_entry.get("extracted_path", "")
                if Path(ep).name == "Index" and index_inventory_id is None:
                    index_inventory_id = inventory_id
            except Exception as e:
                LOGGER.warning(
                    "Failed to register inventory for %s: %s",
                    file_entry.get("extracted_path", "unknown"),
                    e,
                )

    try:
        # Step 1: Read the SQLite Index
        callbacks.on_log(f"Reading Application Cache Index: {index_path}")
        index_data = _read_appcache_index(index_path)
        response_id_to_url = index_data["response_id_to_url"]
        response_id_to_entry = index_data["response_id_to_entry"]
        stats["groups"] = len(index_data["groups"])

        if not response_id_to_url:
            LOGGER.warning("Application Cache Index is empty: %s", index_path)
            for inv_id in inventory_ids:
                update_inventory_ingestion_status(
                    evidence_conn, inv_id,
                    status="no_entries",
                    notes="Application Cache Index contains no entries",
                )
            return stats

        callbacks.on_log(
            f"Application Cache Index: {index_data['total_entries']} entries, "
            f"{stats['groups']} groups"
        )

        # Step 2: Parse blockfile cache
        entries = parse_blockfile_cache(cache_dir, warning_collector=warning_collector)
        if not entries:
            entries = scan_data1_orphan_entries(
                cache_dir, warning_collector=warning_collector,
            )
            if entries:
                callbacks.on_log(
                    f"Recovered {len(entries)} orphaned entries from AppCache data_1 scan"
                )

        stats["entries"] = len(entries)

        # Step 3: Also insert URLs from the SQLite Index that have NO
        # blockfile entry.  The Index is authoritative — every Entries row
        # represents a cached resource regardless of whether the blockfile
        # parser recovered the data.
        blockfile_response_ids = set()
        if entries:
            for entry in entries:
                try:
                    rid = int(entry.url)
                    blockfile_response_ids.add(rid)
                except (ValueError, TypeError):
                    pass

        # Insert URLs for ALL Index entries (resolved from SQLite)
        discovered_by = f"cache_appcache:{extractor_version}:{run_id}"

        # Derive forensic provenance
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

        # First: insert all URLs from the SQLite Index (authoritative source)
        for response_id, entry_info in response_id_to_entry.items():
            url = entry_info["url"]
            if not is_cache_url(url):
                continue

            parsed_url = urlparse(url)

            # Determine timestamp from the group
            group_time = None
            for group in index_data["groups"]:
                if group.get("group_id") == entry_info.get("cache_id"):
                    group_time = group.get("last_access_time") or group.get("creation_time")
                    break

            timestamp_str = group_time.isoformat() if group_time else datetime.now(timezone.utc).isoformat()

            has_body = response_id in blockfile_response_ids
            source_path = str(index_path) if not base_forensic_path else base_forensic_path

            url_record = {
                "url": url,
                "domain": parsed_url.netloc,
                "scheme": parsed_url.scheme,
                "discovered_by": discovered_by,
                "first_seen_utc": timestamp_str,
                "last_seen_utc": timestamp_str,
                "source_path": source_path,
                "notes": (
                    f"Application Cache entry (response_id={response_id}, "
                    f"size={entry_info.get('response_size', 0)})"
                ),
                "context": entry_info.get("manifest_url"),
                "run_id": run_id,
                "cache_key": str(response_id),
                "cache_filename": f"response_id_{response_id}",
                "response_code": None,
                "content_type": None,
                "tags": json.dumps({
                    "cache_backend": "appcache",
                    "response_id": response_id,
                    "response_size": entry_info.get("response_size"),
                    "flags": entry_info.get("flags"),
                    "origin": entry_info.get("origin"),
                    "manifest_url": entry_info.get("manifest_url"),
                    "body_in_blockfile": has_body,
                    "forensic_path": base_forensic_path,
                    "logical_path": base_logical_path,
                }),
            }

            try:
                insert_urls(evidence_conn, evidence_id, [url_record])
                stats["urls"] += 1
                stats["records"] += 1
            except Exception as e:
                if "UNIQUE constraint" not in str(e):
                    LOGGER.warning("Failed to insert AppCache URL %s: %s", url[:80], e)

        # Step 4: Process blockfile entries for body data (images, HTTP headers)
        if entries:
            callbacks.on_log(
                f"Processing {len(entries)} blockfile entries from Application Cache"
            )

            for entry in entries:
                try:
                    _process_appcache_blockfile_entry(
                        evidence_conn=evidence_conn,
                        evidence_id=evidence_id,
                        entry=entry,
                        cache_dir=cache_dir,
                        extraction_dir=extraction_dir,
                        run_id=run_id,
                        extractor_version=extractor_version,
                        discovered_by=discovered_by,
                        response_id_to_url=response_id_to_url,
                        response_id_to_entry=response_id_to_entry,
                        warning_collector=warning_collector,
                        stats=stats,
                        base_forensic_path=base_forensic_path,
                        base_logical_path=base_logical_path,
                    )
                except Exception as e:
                    LOGGER.warning(
                        "Failed to process AppCache blockfile entry %s: %s",
                        entry.url[:50], e,
                    )

        # Update inventory status
        summary_note = (
            f"Application Cache: {stats['groups']} groups, "
            f"{index_data['total_entries']} index entries, "
            f"{stats['entries']} blockfile entries, "
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
        LOGGER.error(
            "Failed to parse Application Cache %s: %s",
            cache_dir, e, exc_info=True,
        )
        for inv_id in inventory_ids:
            update_inventory_ingestion_status(
                evidence_conn, inv_id,
                status="failed",
                notes=f"Application Cache parsing failed: {e}",
            )

    return stats


def _process_appcache_blockfile_entry(
    evidence_conn,
    evidence_id: int,
    entry,
    cache_dir: Path,
    extraction_dir: Path,
    run_id: str,
    extractor_version: str,
    discovered_by: str,
    response_id_to_url: Dict[int, str],
    response_id_to_entry: Dict[int, Dict[str, Any]],
    warning_collector: Optional["ExtractionWarningCollector"],
    stats: Dict[str, int],
    base_forensic_path: Optional[str] = None,
    base_logical_path: Optional[str] = None,
) -> None:
    """
    Process a single blockfile entry from an Application Cache.

    The blockfile key is a numeric response_id.  We resolve it to a URL
    via the SQLite Index lookup, then extract HTTP headers and images
    from the body stream.
    """
    # Resolve numeric key to response_id
    try:
        response_id = int(entry.url)
    except (ValueError, TypeError):
        LOGGER.debug(
            "AppCache blockfile entry has non-numeric key: %s", entry.url[:50]
        )
        return

    # Look up the real URL from the SQLite Index
    real_url = response_id_to_url.get(response_id)
    if not real_url:
        LOGGER.debug(
            "AppCache response_id %d not found in SQLite Index", response_id
        )
        return

    entry_info = response_id_to_entry.get(response_id, {})

    # Parse HTTP headers from stream 0
    http_info = {"response_code": None, "content_type": None, "content_encoding": None}
    if entry.data_sizes[0] > 0:
        stream0_data = read_stream_data(cache_dir, entry, 0)
        if stream0_data:
            http_info = parse_http_headers(stream0_data)

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
                        _insert_appcache_image(
                            evidence_conn=evidence_conn,
                            evidence_id=evidence_id,
                            image_info=image_info,
                            entry=entry,
                            http_info=http_info,
                            body=body,
                            run_id=run_id,
                            extractor_version=extractor_version,
                            real_url=real_url,
                            entry_info=entry_info,
                        )
                        stats["images"] += 1
                        stats["records"] += 1


def _insert_appcache_image(
    evidence_conn,
    evidence_id: int,
    image_info: Dict[str, Any],
    entry,
    http_info: Dict[str, Any],
    body: bytes,
    run_id: str,
    extractor_version: str,
    real_url: str,
    entry_info: Dict[str, Any],
) -> None:
    """Insert a carved Application Cache image into the database."""
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
        "cache_backend": "appcache",
        "response_code": http_info.get("response_code"),
        "content_type": http_info.get("content_type"),
        "content_encoding": http_info.get("content_encoding"),
        "stream0_size": entry.data_sizes[0],
        "stream1_size": entry.data_sizes[1],
        "entry_hash": entry.entry_hash,
        "entry_state": entry.state,
        "http_body_size_bytes": len(body) if body else 0,
        "creation_time": entry.creation_time.isoformat() if entry.creation_time else None,
        "response_id": entry_info.get("response_id"),
        "response_size": entry_info.get("response_size"),
        "manifest_url": entry_info.get("manifest_url"),
        "origin": entry_info.get("origin"),
        "raw_headers_text": http_info.get("raw_headers_text"),
        "headers": http_info.get("headers", {}),
        "body_storage_path": image_info["rel_path"],
    }

    discovery_data = {
        "discovered_by": "cache_appcache",
        "run_id": run_id,
        "extractor_version": extractor_version,
        "cache_url": real_url,
        "cache_key": str(entry_info.get("response_id", entry.url)),
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
            LOGGER.warning("Failed to insert AppCache image: %s", e)
