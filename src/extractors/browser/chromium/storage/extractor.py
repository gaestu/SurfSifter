"""
Chromium Browser Storage Extractor

Extracts Local Storage, Session Storage, and IndexedDB from Chromium browsers.

Features:
- Local Storage (LevelDB-based)
- Session Storage (LevelDB-based)
- IndexedDB (LevelDB with blob storage)
- IndexedDB blob image extraction
- Configurable value excerpt size
- Deleted record recovery via LevelDB
- StatisticsCollector integration for run tracking
- Multi-partition support via file_list discovery
- Schema warnings for unknown LevelDB patterns

Data Sources:
- Local Storage: {Profile}/Local Storage/leveldb/
- Session Storage: {Profile}/Session Storage/
- IndexedDB: {Profile}/IndexedDB/

Dependencies:
- ccl_chromium_reader (optional, for LevelDB parsing)

Forensic Value:
- Web application data persistence
- User preferences and settings
- Cached authentication tokens
- Stored form data

Path hash to prevent overwrites, use _patterns.py, URL cleanup, flush warnings on early returns
Multi-partition support, schema warnings, file splitting
Initial implementation
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, TYPE_CHECKING

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ...._shared.file_list_discovery import (
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from .._patterns import CHROMIUM_BROWSERS
from .widget import ChromiumStorageWidget
from ._discovery import (
    discover_storage_multi_partition,
    extract_storage_directory,
    extract_profile_from_path,
)
from ._parsers import (
    parse_leveldb_storage,
    parse_indexeddb_storage,
)

from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.browser.chromium.storage")


class ChromiumStorageExtractor(BaseExtractor):
    """
    Extract browser web storage from Chromium browsers.

    Uses ccl_chromium_reader for LevelDB parsing when available.
    Supports Chrome, Edge, Brave, Opera with multi-partition discovery.
    """

    SUPPORTED_BROWSERS = list(CHROMIUM_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="chromium_browser_storage",
            display_name="Chromium Browser Storage",
            description="Extract Local Storage, Session Storage, IndexedDB from Chrome/Edge/Opera/Brave",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted"
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run."""
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output has existing extraction."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget."""
        return ChromiumStorageWidget(parent, supported_browsers=self.SUPPORTED_BROWSERS)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            storage_count = len(data.get("storage_locations", []))
            partitions = set(
                loc.get("partition_index", 0)
                for loc in data.get("storage_locations", [])
            )
            partition_info = f" across {len(partitions)} partition(s)" if len(partitions) > 1 else ""
            status_text = (
                f"Chromium Storage\n"
                f"Locations found: {storage_count}{partition_info}\n"
                f"Run ID: {data.get('run_id', 'N/A')}"
            )
        else:
            status_text = "Chromium Storage\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory."""
        return case_root / "evidences" / evidence_label / "chromium_browser_storage"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Chromium storage directories from evidence."""
        callbacks.on_step("Initializing Chromium storage extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Chromium storage extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "e01_context": self._get_e01_context(evidence_fs),
            "storage_locations": [],
            "partitions_scanned": [],
            "config": {
                "local_storage": config.get("local_storage", True),
                "session_storage": config.get("session_storage", True),
                "indexeddb": config.get("indexeddb", True),
                "excerpt_size": config.get("excerpt_size", 4096),
                "include_deleted": config.get("include_deleted", True),
                "extract_images": config.get("extract_images", True),
            },
            "status": "ok",
            "notes": [],
        }

        callbacks.on_step("Discovering Chromium storage (multi-partition)")

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        browsers_to_search = [b for b in browsers_to_search if b in self.SUPPORTED_BROWSERS]

        # Multi-partition discovery
        storage_by_partition = discover_storage_multi_partition(
            evidence_conn,
            evidence_id,
            evidence_fs,
            browsers_to_search,
            config,
            callbacks
        )

        # Flatten for counting and manifest
        all_locations = []
        for partition_idx, locations in storage_by_partition.items():
            all_locations.extend(locations)

        manifest_data["partitions_scanned"] = list(storage_by_partition.keys())

        # Report discovered locations
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(all_locations))

        callbacks.on_log(f"Found {len(all_locations)} storage location(s) across {len(storage_by_partition)} partition(s)")

        if not all_locations:
            LOGGER.info("No Chromium storage found")
            manifest_data["notes"].append("No Chromium storage locations found")
        else:
            callbacks.on_progress(0, len(all_locations), "Copying storage data")

            # Get EWF paths for multi-partition extraction
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)

            location_idx = 0
            for partition_idx, locations in storage_by_partition.items():
                # Open partition filesystem for this set of locations
                try:
                    if ewf_paths and len(storage_by_partition) > 1:
                        # Multi-partition: open specific partition
                        with open_partition_for_extraction(ewf_paths, partition_idx) as partition_fs:
                            for loc in locations:
                                if callbacks.is_cancelled():
                                    manifest_data["status"] = "cancelled"
                                    manifest_data["notes"].append("Extraction cancelled by user")
                                    break

                                location_idx += 1
                                callbacks.on_progress(
                                    location_idx, len(all_locations),
                                    f"Copying {loc['browser']} {loc['storage_type']} (partition {partition_idx})"
                                )

                                try:
                                    extracted = extract_storage_directory(
                                        partition_fs, loc, output_dir, run_id, callbacks
                                    )
                                    manifest_data["storage_locations"].append(extracted)
                                except Exception as e:
                                    error_msg = f"Failed to extract {loc['storage_type']} from {loc['browser']}: {e}"
                                    LOGGER.error(error_msg, exc_info=True)
                                    manifest_data["notes"].append(error_msg)
                                    if stats:
                                        stats.report_failed(evidence_id, self.metadata.name, files=1)
                    else:
                        # Single partition or fallback: use provided evidence_fs
                        for loc in locations:
                            if callbacks.is_cancelled():
                                manifest_data["status"] = "cancelled"
                                manifest_data["notes"].append("Extraction cancelled by user")
                                break

                            location_idx += 1
                            callbacks.on_progress(
                                location_idx, len(all_locations),
                                f"Copying {loc['browser']} {loc['storage_type']}"
                            )

                            try:
                                extracted = extract_storage_directory(
                                    evidence_fs, loc, output_dir, run_id, callbacks
                                )
                                manifest_data["storage_locations"].append(extracted)
                            except Exception as e:
                                error_msg = f"Failed to extract {loc['storage_type']} from {loc['browser']}: {e}"
                                LOGGER.error(error_msg, exc_info=True)
                                manifest_data["notes"].append(error_msg)
                                if stats:
                                    stats.report_failed(evidence_id, self.metadata.name, files=1)

                except Exception as e:
                    error_msg = f"Failed to open partition {partition_idx}: {e}"
                    LOGGER.error(error_msg, exc_info=True)
                    manifest_data["notes"].append(error_msg)

                if callbacks.is_cancelled():
                    break

        # Finish statistics
        if stats:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_step("Writing manifest")
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        # Record extracted files to audit table
        from extractors._shared.extracted_files_audit import record_browser_files
        record_browser_files(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            run_id=run_id,
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
            manifest_data=manifest_data,
            callbacks=callbacks,
            files_key="storage_locations",
        )

        LOGGER.info(
            "Chromium storage extraction complete: %d locations across %d partitions, status=%s",
            len(manifest_data["storage_locations"]),
            len(manifest_data["partitions_scanned"]),
            manifest_data["status"],
        )

        return manifest_data["status"] != "error"

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """Parse extracted storage and ingest into database."""
        from extractors._shared.leveldb_wrapper import is_leveldb_available
        from extractors._shared.extraction_warnings import ExtractionWarningCollector
        from core.database import (
            insert_local_storages, delete_local_storage_by_run,
            insert_session_storages, delete_session_storage_by_run,
            insert_indexeddb_database, insert_indexeddb_entries,
            delete_indexeddb_entries_by_run, insert_image_with_discovery,
            insert_urls, delete_urls_by_run, delete_extraction_warnings_by_run,
        )

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"local_storage": 0, "session_storage": 0, "indexeddb": 0, "images": 0, "urls": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        storage_locations = manifest_data.get("storage_locations", [])
        manifest_config = manifest_data.get("config", {})

        # Create warning collector
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Continue statistics tracking
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Check if LevelDB parsing is available
        if not is_leveldb_available():
            error_msg = (
                "ccl-chromium-reader is not installed. "
                "Install with: poetry install --extras leveldb\n"
                "Or: pip install git+https://github.com/cclgroupltd/ccl_chromium_reader.git"
            )
            callbacks.on_error("Missing dependency", error_msg)
            LOGGER.error("Ingestion failed: %s", error_msg)
            # Flush any collected warnings before early return
            try:
                warning_count = warning_collector.flush_to_database(evidence_conn)
                if warning_count > 0:
                    callbacks.on_log(f"Recorded {warning_count} extraction warnings", "info")
            except Exception as e:
                LOGGER.warning("Failed to flush extraction warnings: %s", e)
            if stats:
                stats.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0,
                    local_storage=0,
                    session_storage=0,
                    indexeddb=0,
                )
                stats.finish_run(evidence_id, self.metadata.name, status="error")
            return {"local_storage": 0, "session_storage": 0, "indexeddb": 0, "images": 0, "urls": 0}

        if not storage_locations:
            callbacks.on_log("No storage to ingest", "warning")
            # Flush any collected warnings before early return
            try:
                warning_count = warning_collector.flush_to_database(evidence_conn)
                if warning_count > 0:
                    callbacks.on_log(f"Recorded {warning_count} extraction warnings", "info")
            except Exception as e:
                LOGGER.warning("Failed to flush extraction warnings: %s", e)
            if stats:
                stats.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0,
                    local_storage=0,
                    session_storage=0,
                    indexeddb=0,
                )
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"local_storage": 0, "session_storage": 0, "indexeddb": 0, "images": 0, "urls": 0}

        # Clear previous data for this run
        delete_local_storage_by_run(evidence_conn, evidence_id, run_id)
        delete_session_storage_by_run(evidence_conn, evidence_id, run_id)
        delete_indexeddb_entries_by_run(evidence_conn, evidence_id, run_id)
        delete_urls_by_run(evidence_conn, evidence_id, run_id)
        delete_extraction_warnings_by_run(evidence_conn, evidence_id, self.metadata.name, run_id)

        excerpt_size = manifest_config.get("excerpt_size", 4096)
        include_deleted = manifest_config.get("include_deleted", True)
        extract_images = manifest_config.get("extract_images", True)

        # Setup image output directory
        images_dir = output_dir / run_id / "indexeddb_images"
        if extract_images:
            images_dir.mkdir(parents=True, exist_ok=True)

        local_count = 0
        session_count = 0
        indexeddb_count = 0
        image_count = 0

        # Collect ALL URLs (no deduplication - forensic completeness)
        url_records: List[Dict[str, Any]] = []

        callbacks.on_progress(0, len(storage_locations), "Parsing storage data")

        for i, loc in enumerate(storage_locations):
            if callbacks.is_cancelled():
                break

            storage_type = loc.get("storage_type", "")
            browser = loc.get("browser", "unknown")
            partition_idx = loc.get("partition_index", 0)
            callbacks.on_progress(
                i + 1, len(storage_locations),
                f"Parsing {browser} {storage_type} (p{partition_idx})"
            )

            try:
                extracted_path = output_dir / loc.get("extracted_path", "")

                if not extracted_path.exists():
                    continue

                if storage_type == "local_storage":
                    records = parse_leveldb_storage(
                        extracted_path, loc, run_id, evidence_id,
                        "local_storage", excerpt_size, include_deleted,
                        warning_collector=warning_collector,
                    )
                    if records:
                        local_count += insert_local_storages(evidence_conn, evidence_id, records)
                        # Collect ALL origins as URLs (no deduplication)
                        for rec in records:
                            origin = rec.get("origin", "")
                            if origin:
                                url_records.append(self._build_url_record(
                                    origin, browser, "local_storage", loc, run_id
                                ))

                elif storage_type == "session_storage":
                    records = parse_leveldb_storage(
                        extracted_path, loc, run_id, evidence_id,
                        "session_storage", excerpt_size, include_deleted,
                        warning_collector=warning_collector,
                    )
                    if records:
                        session_count += insert_session_storages(evidence_conn, evidence_id, records)
                        # Collect ALL origins as URLs (no deduplication)
                        for rec in records:
                            origin = rec.get("origin", "")
                            if origin:
                                url_records.append(self._build_url_record(
                                    origin, browser, "session_storage", loc, run_id
                                ))

                elif storage_type == "indexeddb":
                    db_records = parse_indexeddb_storage(
                        extracted_path, loc, run_id, evidence_id,
                        excerpt_size, include_deleted, extract_images, images_dir,
                        warning_collector=warning_collector,
                    )
                    for db_record, entries, extracted_images in db_records:
                        try:
                            db_id = insert_indexeddb_database(evidence_conn, evidence_id, db_record)
                            if entries:
                                for entry in entries:
                                    entry["database_id"] = db_id
                                insert_indexeddb_entries(evidence_conn, evidence_id, entries)
                                indexeddb_count += len(entries)

                            # Collect origin as URL (no deduplication)
                            origin = db_record.get("origin", "")
                            if origin:
                                url = self._indexeddb_origin_to_url(origin)
                                if url:
                                    url_records.append(self._build_url_record(
                                        url, browser, "indexeddb", loc, run_id,
                                        context=f"IndexedDB: {db_record.get('database_name', '')}"
                                    ))

                            # Insert images (with deduplication via UNIQUE constraint)
                            for img_tuple in extracted_images:
                                try:
                                    image_data, discovery_data = img_tuple
                                    insert_image_with_discovery(
                                        evidence_conn, evidence_id, image_data, discovery_data
                                    )
                                    image_count += 1
                                except Exception as img_err:
                                    if "UNIQUE constraint" not in str(img_err):
                                        LOGGER.warning("Failed to insert image: %s", img_err)
                        except Exception as e:
                            LOGGER.error("Failed to insert IndexedDB: %s", e)

            except Exception as e:
                LOGGER.error("Failed to parse storage %s: %s", loc.get("extracted_path"), e)
                callbacks.on_log(f"Failed to parse {storage_type}: {e}", "error")

        # Insert ALL collected URLs (no deduplication)
        url_count = 0
        if url_records:
            try:
                url_count = insert_urls(evidence_conn, evidence_id, url_records, run_id=run_id)
                callbacks.on_log(f"Inserted {url_count} URLs from storage origins", "info")
            except Exception as e:
                LOGGER.warning("Failed to insert URLs: %s", e)

        # Flush extraction warnings
        try:
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                callbacks.on_log(f"Recorded {warning_count} extraction warnings", "info")
        except Exception as e:
            LOGGER.warning("Failed to flush extraction warnings: %s", e)

        evidence_conn.commit()

        # Report ingested counts and finish
        total_records = local_count + session_count + indexeddb_count
        if stats:
            stats.report_ingested(
                evidence_id, self.metadata.name,
                records=total_records,
                local_storage=local_count,
                session_storage=session_count,
                indexeddb=indexeddb_count,
                urls=url_count,
            )
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        if image_count > 0:
            callbacks.on_log(f"Extracted {image_count} images from IndexedDB blobs", "info")

        return {
            "local_storage": local_count,
            "session_storage": session_count,
            "indexeddb": indexeddb_count,
            "images": image_count,
            "urls": url_count,
        }

    # ─────────────────────────────────────────────────────────────────
    # Helper Methods
    # ─────────────────────────────────────────────────────────────────

    def _generate_run_id(self) -> str:
        """Generate run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"storage_chromium_{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context from evidence filesystem."""
        try:
            source_path = evidence_fs.source_path if hasattr(evidence_fs, 'source_path') else None
            if source_path is not None and not isinstance(source_path, (str, Path)):
                source_path = None

            fs_type = getattr(evidence_fs, 'fs_type', "unknown")
            if not isinstance(fs_type, str):
                fs_type = "unknown"

            return {
                "image_path": str(source_path) if source_path else None,
                "fs_type": fs_type,
            }
        except Exception:
            return {"image_path": None, "fs_type": "unknown"}

    def _build_url_record(
        self,
        url: str,
        browser: str,
        storage_type: str,
        loc: Dict,
        run_id: str,
        context: str = None
    ) -> Dict[str, Any]:
        """Build a URL record from a storage origin."""
        from urllib.parse import urlparse

        try:
            parsed = urlparse(url)
            domain = parsed.netloc or parsed.path
            scheme = parsed.scheme or "https"
        except Exception:
            domain = url
            scheme = "https"

        return {
            "url": url,
            "domain": domain,
            "scheme": scheme,
            "discovered_by": "chromium_browser_storage",
            "source_path": loc.get("logical_path", ""),
            "partition_index": loc.get("partition_index"),
            "run_id": run_id,
            "context": context or f"{browser} {storage_type}",
            "notes": f"Origin from {browser.title()} {storage_type.replace('_', ' ').title()} (partition {loc.get('partition_index', 0)})",
        }

    def _indexeddb_origin_to_url(self, origin_str: str) -> Optional[str]:
        """Convert IndexedDB origin format to a URL.

        IndexedDB origins have format like:
        - https_example.com_0@1  -> https://example.com
        - http_localhost_8080@2  -> http://localhost:8080
        """
        if not origin_str:
            return None

        # Remove the @N suffix (IndexedDB database version marker)
        if "@" in origin_str:
            origin_str = origin_str.rsplit("@", 1)[0]

        # Split by underscore: scheme_domain_port
        parts = origin_str.split("_")
        if len(parts) < 2:
            return None

        scheme = parts[0]
        # Rejoin middle parts (domain might have underscores)
        rest = "_".join(parts[1:])

        # Check if last underscore-separated part is a port
        last_underscore_idx = rest.rfind("_")
        if last_underscore_idx > 0:
            potential_port = rest[last_underscore_idx + 1:]
            if potential_port.isdigit() and potential_port != "0":
                domain = rest[:last_underscore_idx]
                port = potential_port
                return f"{scheme}://{domain}:{port}"
            elif potential_port == "0":
                # Port 0 means default port (omit from URL)
                domain = rest[:last_underscore_idx]
                return f"{scheme}://{domain}"

        # No port found, whole rest is domain
        return f"{scheme}://{rest}"
