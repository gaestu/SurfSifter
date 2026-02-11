"""
Firefox Cache Ingestion

Handles ingestion of extracted Firefox cache2 files into the evidence database.
Parses cache entries, extracts URLs, carves images, and registers inventory.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from core.logging import get_logger
from core.database import (
    insert_urls,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    insert_image_with_discovery,
    delete_urls_by_run,
    insert_firefox_cache_index_entries,
    delete_firefox_cache_index_by_run,
)
from core.statistics_collector import StatisticsCollector
from ...._shared.extraction_warnings import (
    ExtractionWarningCollector,
    CATEGORY_BINARY,
    SEVERITY_INFO,
    SEVERITY_WARNING,
)
from ....callbacks import ExtractorCallbacks

from .parser import parse_cache2_entry
from .image_carver import carve_image_from_cache_entry
from .strategies import CHUNK_SIZE
from ._index import parse_cache_index, parse_journal, CacheIndex
from ._recovery import discover_all_cache_entries, correlate_index_with_files
from ._schemas import (
    KNOWN_ELEMENT_KEYS,
    ELEMENT_KEY_PATTERNS,
    KNOWN_HTTP_HEADERS,
    HTTP_HEADER_PATTERNS,
    KNOWN_REQUEST_METHODS,
    KNOWN_CACHE2_VERSIONS,
)

LOGGER = get_logger("extractors.cache_firefox.ingestion")


class CacheIngestionHandler:
    """
    Handles ingestion of Firefox cache2 extraction results.

    Workflow:
    1. Load manifest.json from extraction run
    2. For each cache entry:
       - Parse cache2 format (URL, timestamps, metadata)
       - Carve images from response body
       - Insert URLs into database
       - Register in browser_cache_inventory
    """

    def __init__(
        self,
        extractor_name: str,
        extractor_version: str,
    ):
        """
        Initialize ingestion handler.

        Args:
            extractor_name: Extractor name for provenance
            extractor_version: Extractor version string
        """
        self.extractor_name = extractor_name
        self.extractor_version = extractor_version

    def run(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, Any]:
        """
        Ingest extracted Firefox cache2 files into evidence database.

        Args:
            output_dir: Directory containing extraction runs
            evidence_conn: SQLite connection to evidence database
            evidence_id: Database evidence ID
            config: Configuration dict from config widget
            callbacks: Progress and logging callbacks

        Returns:
            Ingestion statistics dict
        """
        callbacks.on_step("Initializing Firefox cache2 ingestion")
        callbacks.on_log("Starting Firefox cache2 ingestion")

        evidence_label = config.get("evidence_label", "")

        stats = {
            "entries_parsed": 0,
            "urls_inserted": 0,
            "images_inserted": 0,
            "supporting_files_skipped": 0,
            "failed_extractions_skipped": 0,
            "inventory_entries": 0,
            "index_entries": 0,
            "doomed_entries": 0,
            "trash_entries": 0,
            "errors": [],
        }

        # Find latest manifest
        manifest_path = self._find_latest_manifest(output_dir)
        if not manifest_path:
            error_msg = "No extraction manifests found"
            callbacks.on_log(error_msg)
            stats["errors"].append(error_msg)
            return stats

        manifest = json.loads(manifest_path.read_text())
        run_id = manifest.get("run_id", "")
        run_dir = manifest_path.parent

        # Track ingestion with StatisticsCollector
        stats_collector = StatisticsCollector.instance()
        if stats_collector:
            stats_collector.continue_run(
                evidence_id, evidence_label, self.extractor_name, run_id
            )

        # Check manifest status
        status = manifest.get("status", "ok")
        if status != "ok":
            error_msg = f"Extraction status is '{status}', not 'ok'. Re-run extraction."
            callbacks.on_log(error_msg, level="error")
            stats["errors"].append(error_msg)
            if stats_collector:
                stats_collector.report_ingested(evidence_id, self.extractor_name, records=0, urls=0)
                stats_collector.finish_run(evidence_id, self.extractor_name, status="error")
            return stats

        callbacks.on_log(f"Ingesting run_id: {run_id}")

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.extractor_name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Clear previous run data for idempotent re-ingestion
        self._clear_previous_run(evidence_conn, evidence_id, run_id, callbacks)

        # Handle deferred hash computation (uses run_dir, not output_dir)
        manifest = self._compute_deferred_hashes(
            manifest, manifest_path, run_dir, callbacks
        )

        try:
            # Process files
            stats = self._process_files(
                manifest=manifest,
                output_dir=output_dir,
                run_dir=run_dir,
                run_id=run_id,
                evidence_conn=evidence_conn,
                evidence_id=evidence_id,
                callbacks=callbacks,
                stats=stats,
                warning_collector=warning_collector,
            )

            # Process cache index file and doomed/trash entries
            index_stats = self._process_cache_index(
                manifest=manifest,
                run_dir=run_dir,
                run_id=run_id,
                evidence_conn=evidence_conn,
                evidence_id=evidence_id,
                callbacks=callbacks,
                warning_collector=warning_collector,
            )
            stats["index_entries"] = index_stats.get("index_entries_inserted", 0)
            stats["doomed_entries"] = index_stats.get("doomed_count", 0)
            stats["trash_entries"] = index_stats.get("trash_count", 0)

            # Flush collected warnings to database before commit
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)
                stats["schema_warnings"] = warning_count

            evidence_conn.commit()

            callbacks.on_step("Firefox cache2 ingestion complete")
            index_note = ""
            if stats.get("index_entries", 0) > 0:
                index_note = f", {stats['index_entries']} index records"
            callbacks.on_log(
                f"Ingestion complete: {stats['entries_parsed']} entries, "
                f"{stats['urls_inserted']} URLs, {stats['images_inserted']} images"
                f"{index_note}, "
                f"{len(stats['errors'])} errors"
            )

        except Exception as e:
            evidence_conn.rollback()
            error_msg = f"Ingestion failed: {e}"
            LOGGER.error(error_msg, exc_info=True)
            callbacks.on_log(error_msg)
            stats["errors"].append(error_msg)

            if stats_collector:
                stats_collector.report_ingested(
                    evidence_id, self.extractor_name,
                    records=stats.get("urls_inserted", 0) + stats.get("images_inserted", 0),
                    urls=stats.get("urls_inserted", 0),
                    images=stats.get("images_inserted", 0)
                )
                stats_collector.finish_run(evidence_id, self.extractor_name, status="error")
            raise

        # Report success stats
        if stats_collector:
            stats_collector.report_ingested(
                evidence_id, self.extractor_name,
                records=stats.get("urls_inserted", 0) + stats.get("images_inserted", 0),
                urls=stats.get("urls_inserted", 0),
                images=stats.get("images_inserted", 0)
            )
            stats_collector.finish_run(evidence_id, self.extractor_name, status="success")

        return stats

    def _find_latest_manifest(self, output_dir: Path) -> Optional[Path]:
        """Find latest manifest.json in extraction runs."""
        manifests = sorted(output_dir.glob("*/manifest.json"))
        return manifests[-1] if manifests else None

    def _clear_previous_run(
        self,
        evidence_conn,
        evidence_id: int,
        run_id: str,
        callbacks: ExtractorCallbacks,
    ) -> None:
        """
        Clear data from a previous run for idempotent re-ingestion.

        Args:
            evidence_conn: Database connection
            evidence_id: Evidence ID
            run_id: Run ID to clear
            callbacks: Progress callbacks
        """
        deleted = 0

        # Delete URLs from previous run
        deleted += delete_urls_by_run(evidence_conn, evidence_id, run_id)

        # Delete firefox_cache_index entries from previous run
        deleted += delete_firefox_cache_index_by_run(evidence_conn, evidence_id, run_id)

        # Delete inventory entries from previous run
        cursor = evidence_conn.execute(
            "DELETE FROM browser_cache_inventory WHERE evidence_id = ? AND run_id = ?",
            (evidence_id, run_id),
        )
        deleted += cursor.rowcount

        if deleted > 0:
            LOGGER.info("Cleared %d records from previous run %s", deleted, run_id)
            callbacks.on_log(f"Cleared {deleted} records from previous run")

    def _compute_deferred_hashes(
        self,
        manifest: Dict[str, Any],
        manifest_path: Path,
        run_dir: Path,
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, Any]:
        """
        Compute MD5/SHA256 hashes if deferred to ingestion time.

        Args:
            manifest: Manifest dict
            manifest_path: Path to manifest file
            run_dir: Run-specific directory (where files are actually located)
            callbacks: Progress callbacks

        Returns:
            Updated manifest dict
        """
        hash_mode = manifest.get("hash_mode", "extraction")
        if hash_mode != "ingestion":
            return manifest

        callbacks.on_step("Computing deferred hashes")
        files_to_hash = manifest.get("files", [])
        hashes_computed = 0

        for idx, file_entry in enumerate(files_to_hash):
            # Skip failed entries or entries without extracted_path
            if not file_entry.get("success", True):
                continue
            if not file_entry.get("extracted_path"):
                continue
            if file_entry.get("sha256") and file_entry.get("md5"):
                continue

            # Use run_dir since extracted_path is just the filename
            extracted_path = run_dir / file_entry["extracted_path"]
            if extracted_path.exists():
                md5_hash = hashlib.md5()
                sha256_hash = hashlib.sha256()

                with open(extracted_path, "rb") as f:
                    for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                        md5_hash.update(chunk)
                        sha256_hash.update(chunk)

                file_entry["md5"] = md5_hash.hexdigest()
                file_entry["sha256"] = sha256_hash.hexdigest()
                hashes_computed += 1

            callbacks.on_progress(idx + 1, len(files_to_hash))

        if hashes_computed > 0:
            manifest["hash_mode"] = "extraction"
            manifest_path.write_text(json.dumps(manifest, indent=2))
            callbacks.on_log(f"Computed {hashes_computed} deferred MD5+SHA-256 hashes")

        return manifest

    def _process_files(
        self,
        manifest: Dict[str, Any],
        output_dir: Path,
        run_dir: Path,
        run_id: str,
        evidence_conn,
        evidence_id: int,
        callbacks: ExtractorCallbacks,
        stats: Dict[str, Any],
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, Any]:
        """
        Process extracted files from manifest.

        Args:
            manifest: Manifest dict with files list
            output_dir: Base output directory
            run_dir: Run-specific directory
            run_id: Run identifier
            evidence_conn: Database connection
            evidence_id: Evidence ID
            callbacks: Progress callbacks
            stats: Statistics dict to update
            warning_collector: Optional warning collector for schema discovery

        Returns:
            Updated statistics dict
        """
        url_batch: List[Dict[str, Any]] = []
        inventory_batch: List[Dict[str, Any]] = []

        files = manifest.get("files", [])
        total_files = len(files)

        for idx, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                callbacks.on_log("Ingestion cancelled by user")
                break

            try:
                result = self._process_single_file(
                    file_entry=file_entry,
                    output_dir=output_dir,
                    run_dir=run_dir,
                    run_id=run_id,
                    evidence_conn=evidence_conn,
                    evidence_id=evidence_id,
                    manifest=manifest,
                    stats=stats,
                    warning_collector=warning_collector,
                )

                if result.get("url_record"):
                    url_batch.append(result["url_record"])

                if result.get("inventory_entry"):
                    inventory_batch.append(result["inventory_entry"])
                    stats["inventory_entries"] += 1

                if result.get("parsed"):
                    stats["entries_parsed"] += 1

                if result.get("skipped_supporting"):
                    stats["supporting_files_skipped"] += 1

                if result.get("skipped_failed"):
                    stats["failed_extractions_skipped"] += 1

            except Exception as e:
                error_msg = f"Failed to parse {file_entry.get('source_path', 'unknown')}: {e}"
                LOGGER.warning(error_msg, exc_info=True)
                callbacks.on_log(error_msg)
                stats["errors"].append(error_msg)

            callbacks.on_progress(idx + 1, total_files)

        # Batch insert URLs
        if url_batch:
            insert_urls(evidence_conn, evidence_id, url_batch)
            stats["urls_inserted"] = len(url_batch)
            callbacks.on_log(f"Inserted {len(url_batch)} URLs")

        # Report image count
        if stats.get("images_inserted", 0) > 0:
            callbacks.on_log(f"Inserted {stats['images_inserted']} carved images")

        # Update inventory status
        for inv_entry in inventory_batch:
            update_inventory_ingestion_status(
                evidence_conn,
                inv_entry["id"],
                status="ok",
                urls_parsed=1 if inv_entry["has_url"] else 0,
                records_parsed=1,
                notes=None,
            )

        if stats["inventory_entries"] > 0:
            callbacks.on_log(f"Registered {stats['inventory_entries']} cache entries in inventory")

        return stats

    def _process_single_file(
        self,
        file_entry: Dict[str, Any],
        output_dir: Path,
        run_dir: Path,
        run_id: str,
        evidence_conn,
        evidence_id: int,
        manifest: Dict[str, Any],
        stats: Dict[str, Any],
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, Any]:
        """
        Process a single extracted cache file.

        Args:
            file_entry: File entry from manifest
            output_dir: Base output directory
            run_dir: Run-specific directory
            run_id: Run identifier
            evidence_conn: Database connection
            evidence_id: Evidence ID
            manifest: Full manifest dict
            stats: Statistics dict to update

        Returns:
            Dict with processing results
        """
        result = {
            "parsed": False,
            "skipped_supporting": False,
            "skipped_failed": False,
            "url_record": None,
            "inventory_entry": None,
        }

        # Skip failed extractions (success=False in manifest)
        if not file_entry.get("success", True):
            result["skipped_failed"] = True
            LOGGER.debug("Skipping failed extraction: %s", file_entry.get("source_path", "unknown"))
            return result

        # Skip entries without extracted_path (failed extractions)
        if not file_entry.get("extracted_path"):
            result["skipped_failed"] = True
            LOGGER.debug("Skipping entry without extracted_path: %s", file_entry.get("source_path", "unknown"))
            return result

        # Skip supporting files (index, doomed, trash)
        artifact_type = file_entry.get("artifact_type", "cache_firefox")
        if artifact_type != "cache_firefox":
            result["skipped_supporting"] = True
            return result

        # Use run_dir (not output_dir) since extracted_path is just the filename
        extracted_path = run_dir / file_entry["extracted_path"]
        cache_filename = file_entry.get(
            "cache_filename",
            Path(file_entry.get("source_path", "")).name
        )

        if not extracted_path.exists():
            raise FileNotFoundError(f"Extracted file missing: {extracted_path}")

        # Parse cache2 entry
        parse_result = parse_cache2_entry(extracted_path, file_entry)

        # Collect schema warnings for unknown elements and headers
        if warning_collector:
            self._collect_schema_warnings(
                parse_result=parse_result,
                source_file=file_entry.get("source_path", cache_filename),
                warning_collector=warning_collector,
            )

        # Build URL record if URL found
        if parse_result.url:
            result["url_record"] = self._build_url_record(
                parse_result=parse_result,
                file_entry=file_entry,
                run_id=run_id,
                cache_filename=cache_filename,
            )

        # Carve images from response body
        if parse_result.is_image and parse_result.body_size > 0:
            carved = self._carve_and_insert_image(
                extracted_path=extracted_path,
                parse_result=parse_result,
                run_dir=run_dir,
                run_id=run_id,
                cache_filename=cache_filename,
                evidence_conn=evidence_conn,
                evidence_id=evidence_id,
            )
            if carved:
                stats["images_inserted"] = stats.get("images_inserted", 0) + 1

        # Register in inventory
        inventory_id = insert_browser_inventory(
            evidence_conn,
            evidence_id=evidence_id,
            browser="firefox",
            artifact_type="cache_firefox",
            run_id=run_id,
            extracted_path=file_entry.get("extracted_path", ""),
            extraction_status="ok",
            extraction_timestamp_utc=manifest.get("extraction_timestamp"),
            logical_path=file_entry.get("logical_path", file_entry.get("source_path", "")),
            profile=file_entry.get("profile", "unknown"),
            partition_index=file_entry.get("partition_index"),
            fs_type=file_entry.get("fs_type"),
            forensic_path=file_entry.get("forensic_path"),
            extraction_tool=f"cache_firefox:{self.extractor_version}",
            extraction_notes=f"URL: {parse_result.url[:100]}" if parse_result.url else None,
            file_size_bytes=file_entry.get("size_bytes"),
            file_md5=file_entry.get("md5"),
            file_sha256=file_entry.get("sha256"),
        )

        result["inventory_entry"] = {
            "id": inventory_id,
            "has_url": bool(parse_result.url),
            "is_image": parse_result.is_image,
        }

        result["parsed"] = True
        return result

    def _build_url_record(
        self,
        parse_result,
        file_entry: Dict[str, Any],
        run_id: str,
        cache_filename: str,
    ) -> Dict[str, Any]:
        """Build URL record for database insertion."""
        parsed_url = urlparse(parse_result.url)
        meta = parse_result.metadata

        # Derive timestamps
        first_seen = None
        last_seen = None
        if meta.get("last_fetched"):
            last_seen = meta["last_fetched"]
            first_seen = meta.get("last_modified") or last_seen

        # Build tags from metadata
        tags_parts = []
        if meta.get("fetch_count"):
            tags_parts.append(f"fetch_count:{meta['fetch_count']}")
        if meta.get("frecency"):
            tags_parts.append(f"frecency:{meta['frecency']}")
        if meta.get("expiration"):
            tags_parts.append(f"expiration:{meta['expiration']}")
        cache_tags = ",".join(tags_parts) if tags_parts else None

        return {
            "url": parse_result.url,
            "domain": parsed_url.netloc or None,
            "scheme": parsed_url.scheme or None,
            "discovered_by": f"cache_firefox:{self.extractor_version}:{run_id}",
            "first_seen_utc": first_seen,
            "last_seen_utc": last_seen,
            "source_path": file_entry.get("logical_path", file_entry.get("source_path")),
            "notes": None,
            "context": None,
            "run_id": run_id,
            "cache_key": parse_result.cache_key,
            "cache_filename": cache_filename,
            "response_code": parse_result.response_code,
            "content_type": parse_result.content_type,
            "tags": cache_tags,
        }

    def _carve_and_insert_image(
        self,
        extracted_path: Path,
        parse_result,
        run_dir: Path,
        run_id: str,
        cache_filename: str,
        evidence_conn,
        evidence_id: int,
    ) -> bool:
        """
        Carve image from cache entry and insert into database.

        Returns:
            True if image was successfully carved and inserted
        """
        data = extracted_path.read_bytes()

        carved_result = carve_image_from_cache_entry(
            data=data,
            meta_offset=parse_result.body_size,
            content_encoding=parse_result.content_encoding,
            content_type=parse_result.content_type,
            run_dir=run_dir,
            cache_filename=cache_filename,
        )

        if not carved_result:
            return False

        meta = parse_result.metadata
        image_ts = meta.get("last_fetched") or meta.get("last_modified")

        image_data = {
            "rel_path": carved_result["rel_path"],
            "filename": carved_result["filename"],
            "md5": carved_result["md5"],
            "sha256": carved_result["sha256"],
            "phash": carved_result["phash"],
            "ts_utc": image_ts,
            "notes": f"Carved from Firefox cache2 entry {cache_filename}",
            "size_bytes": carved_result["size_bytes"],
        }

        # Source metadata for forensic context
        elements = parse_result.elements
        source_metadata = {
            "response_code": parse_result.response_code,
            "content_type": parse_result.content_type,
            "content_encoding": parse_result.content_encoding,
            "body_size": parse_result.body_size,
            "fetch_count": meta.get("fetch_count"),
            "frecency": meta.get("frecency"),
            "last_fetched": meta.get("last_fetched"),
            "last_modified": meta.get("last_modified"),
            "response_head": elements.get("response-head"),
            "request_method": elements.get("request-method"),
            "body_storage_path": carved_result["rel_path"],
        }

        discovery_data = {
            "discovered_by": "cache_firefox",
            "run_id": run_id,
            "extractor_version": self.extractor_version,
            "cache_url": parse_result.url,
            "cache_key": parse_result.cache_key,
            "cache_filename": cache_filename,
            "cache_response_time": meta.get("last_fetched"),
            "source_metadata_json": json.dumps(source_metadata),
        }

        try:
            insert_image_with_discovery(
                evidence_conn, evidence_id, image_data, discovery_data
            )
            return True
        except Exception as img_err:
            if "UNIQUE constraint" not in str(img_err):
                LOGGER.warning("Failed to insert carved image: %s", img_err)
            return False

    def _collect_schema_warnings(
        self,
        parse_result,
        source_file: str,
        warning_collector: ExtractionWarningCollector,
    ) -> None:
        """
        Collect schema warnings for unknown cache2 elements and HTTP headers.

        Args:
            parse_result: Parsed cache2 entry result
            source_file: Source file path for context
            warning_collector: Warning collector instance
        """
        # Check for unknown element keys
        if parse_result.elements:
            found_keys = set(parse_result.elements.keys())
            unknown_keys = found_keys - KNOWN_ELEMENT_KEYS

            for key in unknown_keys:
                # Filter to relevant keys using patterns
                if any(pattern in key.lower() for pattern in ELEMENT_KEY_PATTERNS):
                    warning_collector.add_warning(
                        warning_type="unknown_element_key",
                        category=CATEGORY_BINARY,
                        severity=SEVERITY_INFO,
                        artifact_type="cache_firefox",
                        source_file=source_file,
                        item_name=key,
                        item_value=parse_result.elements.get(key, "")[:200],  # Truncate long values
                    )

        # Check for unknown HTTP headers in response-head
        response_head = parse_result.elements.get("response-head", "")
        if response_head:
            self._check_unknown_http_headers(
                response_head=response_head,
                source_file=source_file,
                warning_collector=warning_collector,
            )

        # Check for unknown request methods
        request_method = parse_result.elements.get("request-method", "")
        if request_method and request_method.upper() not in KNOWN_REQUEST_METHODS:
            warning_collector.add_warning(
                warning_type="unknown_request_method",
                category=CATEGORY_BINARY,
                severity=SEVERITY_INFO,
                artifact_type="cache_firefox",
                source_file=source_file,
                item_name="request-method",
                item_value=request_method,
            )

        # Check for unknown cache2 version (from metadata)
        cache_version = parse_result.metadata.get("version")
        if cache_version is not None and cache_version not in KNOWN_CACHE2_VERSIONS:
            warning_collector.add_warning(
                warning_type="unknown_cache_version",
                category=CATEGORY_BINARY,
                severity=SEVERITY_WARNING,
                artifact_type="cache_firefox",
                source_file=source_file,
                item_name="cache2_version",
                item_value=str(cache_version),
            )

    def _check_unknown_http_headers(
        self,
        response_head: str,
        source_file: str,
        warning_collector: ExtractionWarningCollector,
    ) -> None:
        """
        Check for unknown HTTP headers in response-head element.

        Args:
            response_head: Raw response-head string
            source_file: Source file path for context
            warning_collector: Warning collector instance
        """
        lines = response_head.split('\r\n')
        if len(lines) <= 1:
            lines = response_head.split('\n')

        # Skip status line (first line)
        for line in lines[1:]:
            if ':' not in line:
                continue

            header_name, _, header_value = line.partition(':')
            header_name_lower = header_name.strip().lower()

            # Skip empty headers
            if not header_name_lower:
                continue

            # Check if header is unknown
            if header_name_lower not in KNOWN_HTTP_HEADERS:
                # Filter to relevant headers using patterns
                if any(pattern in header_name_lower for pattern in HTTP_HEADER_PATTERNS):
                    warning_collector.add_warning(
                        warning_type="unknown_http_header",
                        category=CATEGORY_BINARY,
                        severity=SEVERITY_INFO,
                        artifact_type="cache_firefox",
                        source_file=source_file,
                        item_name=header_name.strip(),
                        item_value=header_value.strip()[:200],  # Truncate long values
                    )

    # -----------------------------------------------------------------
    # Cache Index + Doomed/Trash Processing
    # -----------------------------------------------------------------
    def _process_cache_index(
        self,
        manifest: Dict[str, Any],
        run_dir: Path,
        run_id: str,
        evidence_conn,
        evidence_id: int,
        callbacks: ExtractorCallbacks,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, Any]:
        """Parse the cache2 index and correlate with extracted entry files.

        Looks for extracted index files in the manifest (``artifact_type``
        of ``cache_index``, ``cache_doomed``, or ``cache_trash``).  When
        an index file is found it is parsed, correlated with discovered
        entry files, and the resulting metadata records are inserted into
        the ``firefox_cache_index`` table.

        Args:
            manifest: Manifest dict.
            run_dir: Run-specific directory with extracted files.
            run_id: Current run identifier.
            evidence_conn: Evidence database connection.
            evidence_id: Evidence ID.
            callbacks: Progress/log callbacks.
            warning_collector: Optional warning collector.

        Returns:
            Statistics dict with index/doomed/trash counts.
        """
        stats: Dict[str, Any] = {
            "index_entries_inserted": 0,
            "journal_entries": 0,
            "doomed_count": 0,
            "trash_count": 0,
        }

        # Collect supporting file entries from the manifest
        index_files: List[Dict[str, Any]] = []
        journal_files: List[Dict[str, Any]] = []
        doomed_files: List[Dict[str, Any]] = []
        trash_files: List[Dict[str, Any]] = []

        for file_entry in manifest.get("files", []):
            if not file_entry.get("success", True):
                continue
            if not file_entry.get("extracted_path"):
                continue

            artifact_type = file_entry.get("artifact_type", "cache_firefox")
            if artifact_type == "cache_index":
                index_files.append(file_entry)
            elif artifact_type == "cache_journal":
                journal_files.append(file_entry)
            elif artifact_type == "cache_doomed":
                doomed_files.append(file_entry)
            elif artifact_type == "cache_trash":
                trash_files.append(file_entry)

        stats["doomed_count"] = len(doomed_files)
        stats["trash_count"] = len(trash_files)

        if not index_files and not journal_files:
            LOGGER.debug("No cache index or journal files found in manifest")
            return stats

        callbacks.on_step("Processing Firefox cache index")

        # Build a lookup of extracted entry hashes → source type
        # by scanning the manifest for regular + doomed + trash entries
        entry_file_lookup: Dict[str, str] = {}  # hash -> source
        for fe in manifest.get("files", []):
            if not fe.get("success", True) or not fe.get("extracted_path"):
                continue
            at = fe.get("artifact_type", "cache_firefox")
            if at == "cache_firefox":
                entry_file_lookup[Path(fe.get("source_path", "")).name.upper()] = "entries"
            elif at == "cache_doomed":
                entry_file_lookup[Path(fe.get("source_path", "")).name.upper()] = "doomed"
            elif at == "cache_trash":
                entry_file_lookup[Path(fe.get("source_path", "")).name.upper()] = "trash"

        # Process each index file
        all_db_rows: List[Dict[str, Any]] = []

        for idx_entry in index_files:
            idx_path = run_dir / idx_entry["extracted_path"]
            if not idx_path.exists():
                LOGGER.warning("Index file missing: %s", idx_path)
                continue

            index_result, parse_warnings = parse_cache_index(
                idx_path, warning_collector=warning_collector,
            )
            if index_result is None:
                LOGGER.warning(
                    "Failed to parse index %s: %s",
                    idx_path, "; ".join(parse_warnings),
                )
                continue

            callbacks.on_log(
                f"Parsed cache index: {len(index_result.entries)} entries "
                f"(v0x{index_result.version:X}, "
                f"{'dirty' if index_result.is_dirty else 'clean'})"
            )

            source_path = idx_entry.get("source_path", str(idx_path))
            profile_path = idx_entry.get("profile")
            partition_index = idx_entry.get("partition_index", 0)

            for entry in index_result.entries:
                file_source = entry_file_lookup.get(entry.hash)
                # Try to find URL from the already-parsed cache entries
                url = self._find_url_for_hash(
                    entry.hash, manifest, run_dir,
                )
                all_db_rows.append({
                    "run_id": run_id,
                    "partition_index": partition_index,
                    "source_path": source_path,
                    "entry_hash": entry.hash,
                    "frecency": entry.frecency,
                    "origin_attrs_hash": entry.origin_attrs_hash,
                    "on_start_time": entry.on_start_time,
                    "on_stop_time": entry.on_stop_time,
                    "content_type": entry.content_type,
                    "content_type_name": entry.content_type_name,
                    "file_size_kb": entry.file_size_kb,
                    "raw_flags": entry.flags,
                    "is_initialized": entry.is_initialized,
                    "is_anonymous": entry.is_anonymous,
                    "is_removed": entry.is_removed,
                    "is_pinned": entry.is_pinned,
                    "has_alt_data": entry.has_alt_data,
                    "index_version": index_result.version,
                    "index_timestamp": index_result.timestamp,
                    "index_dirty": index_result.is_dirty,
                    "has_entry_file": file_source is not None,
                    "entry_source": file_source,
                    "url": url,
                    "browser": "firefox",
                    "profile_path": profile_path,
                })

        # Process journal files
        for jnl_entry in journal_files:
            jnl_path = run_dir / jnl_entry["extracted_path"]
            if not jnl_path.exists():
                continue

            journal_entries, jnl_warnings = parse_journal(
                jnl_path, warning_collector=warning_collector,
            )

            if journal_entries:
                callbacks.on_log(
                    f"Parsed cache journal: {len(journal_entries)} entries"
                )
                source_path = jnl_entry.get("source_path", str(jnl_path))
                profile_path = jnl_entry.get("profile")
                partition_index = jnl_entry.get("partition_index", 0)

                for entry in journal_entries:
                    file_source = entry_file_lookup.get(entry.hash)
                    all_db_rows.append({
                        "run_id": run_id,
                        "partition_index": partition_index,
                        "source_path": source_path,
                        "entry_hash": entry.hash,
                        "frecency": entry.frecency,
                        "origin_attrs_hash": entry.origin_attrs_hash,
                        "on_start_time": entry.on_start_time,
                        "on_stop_time": entry.on_stop_time,
                        "content_type": entry.content_type,
                        "content_type_name": entry.content_type_name,
                        "file_size_kb": entry.file_size_kb,
                        "raw_flags": entry.flags,
                        "is_initialized": entry.is_initialized,
                        "is_anonymous": entry.is_anonymous,
                        "is_removed": entry.is_removed,
                        "is_pinned": entry.is_pinned,
                        "has_alt_data": entry.has_alt_data,
                        "index_version": None,  # Journal has no header
                        "index_timestamp": None,
                        "index_dirty": None,
                        "has_entry_file": file_source is not None,
                        "entry_source": "journal" if file_source is None else file_source,
                        "url": None,
                        "browser": "firefox",
                        "profile_path": profile_path,
                    })

                stats["journal_entries"] = len(journal_entries)

        # Batch insert all index records
        if all_db_rows:
            inserted = insert_firefox_cache_index_entries(
                evidence_conn, evidence_id, all_db_rows,
            )
            stats["index_entries_inserted"] = inserted
            callbacks.on_log(
                f"Inserted {inserted} cache index entries "
                f"({sum(1 for r in all_db_rows if r['is_removed'])} removed, "
                f"{sum(1 for r in all_db_rows if not r['has_entry_file'])} metadata-only)"
            )

        return stats

    def _find_url_for_hash(
        self,
        entry_hash: str,
        manifest: Dict[str, Any],
        run_dir: Path,
    ) -> Optional[str]:
        """Look up the URL for a cache entry by hash.

        Checks the manifest for a matching cache entry file and, if found,
        parses it to extract the URL.  This is best-effort — returns
        ``None`` if the entry file doesn't exist or can't be parsed.

        Args:
            entry_hash: Uppercase hex SHA1 hash of the entry.
            manifest: Manifest dict with files list.
            run_dir: Run directory containing extracted files.

        Returns:
            URL string or ``None``.
        """
        for fe in manifest.get("files", []):
            if not fe.get("success", True) or not fe.get("extracted_path"):
                continue
            at = fe.get("artifact_type", "cache_firefox")
            if at not in ("cache_firefox", "cache_doomed", "cache_trash"):
                continue
            # Match by filename (the hash)
            source_name = Path(fe.get("source_path", "")).name.upper()
            if source_name != entry_hash:
                continue

            extracted_path = run_dir / fe["extracted_path"]
            if not extracted_path.exists():
                return None
            try:
                result = parse_cache2_entry(extracted_path, fe)
                return result.url
            except Exception:
                return None

        return None
