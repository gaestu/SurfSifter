"""
Legacy Edge Container Extractor.

Extracts and parses container.dat files from Legacy Edge (EdgeHTML/UWP) paths.

Legacy Edge (2015-2020) stores some browser data in container.dat files
within the UWP package directories:
    Users/*/AppData/Local/Packages/Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/

These container.dat files are ESE databases similar to WebCacheV01.dat but
contain Edge-specific data like:
- History container.dat — browsing history
- Additional metadata containers

Note: Legacy Edge also uses the shared WebCacheV01.dat for most data,
but has these additional container.dat files for Edge-specific features.

This extractor:
1. Discovers container.dat files in Edge UWP paths
2. Copies them to workspace
3. Parses using ESE reader (same as WebCache)
4. Inserts into browser_history and urls tables

Dependencies:
- libesedb-python or dissect.esedb (optional, for ESE parsing)
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse

from PySide6.QtWidgets import QWidget, QLabel, QVBoxLayout

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    get_ewf_paths_from_evidence_fs,
    open_partition_for_extraction,
)
from .._patterns import (
    extract_user_from_path,
)
from .._ese_reader import (
    check_ese_available,
    ESEReader,
)
from .._timestamps import filetime_to_iso
from core.logging import get_logger
from core.database import (
    insert_browser_history_rows,
    insert_urls,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    insert_cookie_row,
    insert_local_storages,
)


LOGGER = get_logger("extractors.browser.ie_legacy.edge_container")


class LegacyEdgeContainerExtractor(BaseExtractor):
    """
    Extract and parse Legacy Edge container.dat files.

    Legacy Edge (EdgeHTML/UWP, 2015-2020) uses container.dat files
    in the UWP package paths for some browser data. These are ESE
    databases with a similar structure to WebCacheV01.dat.

    Workflow:
    1. Discover container.dat files in Microsoft.MicrosoftEdge_* paths
    2. Copy to workspace
    3. Parse ESE containers
    4. Insert into browser_history and urls tables
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="edge_legacy_container",
            display_name="Legacy Edge Container",
            description="Extract Legacy Edge (EdgeHTML) container.dat files",
            category="browser",
            requires_tools=["libesedb-python"],
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted"
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run (manifest + ESE library)."""
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"

        available, _ = check_ese_available()
        if not available:
            return False, "No ESE parsing library installed (libesedb-python or dissect.esedb)"

        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget."""
        return MultiPartitionWidget(parent, default_scan_all=True)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction/ingestion state."""
        widget = QWidget(parent)
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)

        # ESE library status
        available, lib_name = check_ese_available()

        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"Legacy Edge Container\nFiles: {file_count}"
        else:
            status_text = "Legacy Edge Container\nNo extraction yet"

        if not available:
            status_text += "\n⚠️ ESE library not installed"
        else:
            status_text += f"\n✓ {lib_name}"

        layout.addWidget(QLabel(status_text, widget))
        return widget

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "edge_legacy_container"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract container.dat files from Legacy Edge UWP paths.
        """
        callbacks.on_step("Initializing Legacy Edge container extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting Legacy Edge container extraction (run_id=%s)", run_id)

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        # Start statistics tracking
        collector = self._get_statistics_collector()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Initialize manifest
        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "multi_partition_extraction": True,
            "partitions_scanned": [],
            "partitions_with_artifacts": [],
            "files": [],
            "status": "ok",
            "notes": [],
        }

        # Scan for container.dat files
        callbacks.on_step("Scanning for Legacy Edge container.dat files")

        if evidence_conn is None:
            error_msg = (
                "file_list discovery requires evidence_conn; cannot run Legacy Edge "
                "container extraction without file_list data"
            )
            LOGGER.error(error_msg)
            callbacks.on_error(error_msg, "")
            manifest_data["status"] = "error"
            manifest_data["notes"].append(error_msg)
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="error")
            callbacks.on_step("Writing manifest")
            (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))
            return False

        available, count = check_file_list_available(evidence_conn, evidence_id)
        if not available:
            error_msg = (
                "file_list is empty/unavailable for this evidence; cannot run Legacy Edge "
                "container extraction without file_list data. Run file_list extraction first."
            )
            LOGGER.error(error_msg)
            callbacks.on_error(error_msg, "")
            manifest_data["status"] = "error"
            manifest_data["notes"].append(error_msg)
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="error")
            callbacks.on_step("Writing manifest")
            (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))
            return False

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")
        files_by_partition, skipped_zero_size = self._discover_files_multi_partition(
            evidence_conn, evidence_id, callbacks
        )

        # Flatten for counting
        all_files = []
        for files_list in files_by_partition.values():
            all_files.extend(files_list)

        # Update manifest with partition info
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = sorted(
            p for p, files in files_by_partition.items() if files
        )

        # Report discovered files
        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=len(all_files))

        callbacks.on_log(f"Found {len(all_files)} container.dat file(s)")

        if not all_files:
            LOGGER.info("No Legacy Edge container.dat files found")
            if skipped_zero_size:
                manifest_data["notes"].append(
                    f"{skipped_zero_size} container.dat file(s) found but all are zero-size (empty/sparse)"
                )
            else:
                manifest_data["notes"].append("No container.dat files found in Edge UWP paths")
        else:
            import time
            callbacks.on_progress(0, len(all_files), "Extracting container files")
            LOGGER.info("Starting extraction of %d container.dat files", len(all_files))

            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            LOGGER.debug("EWF paths for extraction: %s", ewf_paths)
            file_index = 0
            extraction_start = time.monotonic()

            for partition_index in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_index]
                current_partition = getattr(evidence_fs, 'partition_index', 0)

                LOGGER.info(
                    "Opening partition %d for extraction (%d files), current=%d",
                    partition_index, len(partition_files), current_partition
                )
                partition_start = time.monotonic()

                fs_ctx = (
                    open_partition_for_extraction(evidence_fs, None)
                    if (partition_index == current_partition or ewf_paths is None)
                    else open_partition_for_extraction(ewf_paths, partition_index)
                )

                try:
                    LOGGER.debug("Entering partition context manager")
                    with fs_ctx as fs_to_use:
                        open_elapsed = time.monotonic() - partition_start
                        LOGGER.info(
                            "Partition %d opened in %.2fs, fs_to_use=%s",
                            partition_index, open_elapsed, type(fs_to_use).__name__ if fs_to_use else None
                        )

                        if fs_to_use is None:
                            callbacks.on_log(f"Failed to open partition {partition_index}", "warning")
                            continue

                        for file_info in partition_files:
                            if callbacks.is_cancelled():
                                manifest_data["status"] = "cancelled"
                                break

                            file_index += 1
                            callbacks.on_progress(
                                file_index, len(all_files),
                                f"Copying {file_info.get('container_type', 'container')}.dat"
                            )

                            # _extract_file handles read errors gracefully
                            file_start = time.monotonic()
                            result = self._extract_file(
                                fs_to_use, file_info, output_dir, callbacks
                            )
                            file_elapsed = time.monotonic() - file_start

                            LOGGER.debug(
                                "Extracted file %d/%d in %.2fs: %s (status=%s)",
                                file_index, len(all_files), file_elapsed,
                                file_info.get('logical_path', '')[-60:],
                                result.get('copy_status', 'unknown')
                            )
                            manifest_data["files"].append(result)

                except Exception as e:
                    LOGGER.error("Failed to open partition %d: %s", partition_index, e, exc_info=True)

            total_elapsed = time.monotonic() - extraction_start
            LOGGER.info(
                "Extraction phase complete in %.2fs: %d files processed",
                total_elapsed, file_index
            )

        # Count successful vs failed extractions
        successful_files = [f for f in manifest_data["files"] if f.get("copy_status") == "ok"]
        failed_files = [f for f in manifest_data["files"] if f.get("copy_status") == "failed"]

        if failed_files:
            manifest_data["notes"].append(
                f"{len(failed_files)} file(s) found but unreadable (sparse/deleted)"
            )

        if manifest_data["status"] != "cancelled":
            if failed_files and not successful_files:
                manifest_data["status"] = "error"
            elif failed_files and successful_files:
                manifest_data["status"] = "partial"

        # Finish statistics
        if collector:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        # Write manifest
        callbacks.on_step("Writing manifest")
        (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))

        # Record extracted files to audit table
        from extractors._shared.extracted_files_audit import record_browser_files
        record_browser_files(
            evidence_conn=config.get("evidence_conn"),
            evidence_id=evidence_id,
            run_id=run_id,
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
            manifest_data=manifest_data,
            callbacks=callbacks,
        )

        LOGGER.info(
            "Legacy Edge container extraction complete: %d files extracted, %d unreadable, status=%s",
            len(successful_files),
            len(failed_files),
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
        """
        Parse extracted container.dat files and ingest into database.
        """
        empty_counts = {"history": 0, "urls": 0, "cookies": 0, "storage": 0}
        callbacks.on_step("Reading Legacy Edge container manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return empty_counts

        # Check ESE availability
        available, lib_name = check_ese_available()
        if not available:
            callbacks.on_error("ESE library not installed", "Install libesedb-python or dissect.esedb")
            return empty_counts

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())
        evidence_label = config.get("evidence_label", "")
        files = manifest_data.get("files", [])

        # Continue statistics tracking
        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"history": 0, "urls": 0, "cookies": 0, "storage": 0}

        # Filter to only successfully extracted files
        extractable_files = [f for f in files if f.get("copy_status") == "ok"]
        skipped_files = len(files) - len(extractable_files)

        if skipped_files:
            callbacks.on_log(
                f"Skipping {skipped_files} unreadable file(s) from extraction phase",
                "info"
            )

        # Edge case: all files failed extraction - nothing to ingest
        if not extractable_files:
            callbacks.on_log("No extractable files to ingest (all failed extraction)", "warning")
            if collector:
                collector.report_failed(evidence_id, self.metadata.name, files=skipped_files)
                collector.finish_run(evidence_id, self.metadata.name, status="error")
            return {"history": 0, "urls": 0, "cookies": 0, "storage": 0}

        total_history = 0
        total_urls = 0
        total_cookies = 0
        total_storage = 0
        failed_files = 0

        callbacks.on_progress(0, len(extractable_files), "Parsing container files")

        for i, file_entry in enumerate(extractable_files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(extractable_files),
                f"Parsing {file_entry.get('container_type', 'container')}.dat"
            )

            inventory_id = None
            try:
                db_path = Path(file_entry["extracted_path"])
                if not db_path.is_absolute():
                    db_path = output_dir / db_path

                if not db_path.exists():
                    callbacks.on_log(f"File not found: {db_path}", "warning")
                    failed_files += 1
                    continue

                # Register in browser inventory
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser="edge_legacy",
                    artifact_type=file_entry.get("container_type", "history"),
                    run_id=run_id,
                    extracted_path=str(db_path),
                    extraction_status="ok",
                    extraction_timestamp_utc=manifest_data.get("extraction_timestamp_utc"),
                    logical_path=file_entry.get("logical_path", ""),
                    profile=file_entry.get("user"),
                    partition_index=file_entry.get("partition_index"),
                )

                # Parse container based on type
                container_type = file_entry.get("container_type", "history")
                records_parsed = 0
                urls_parsed = 0
                ingestion_status = "ok"
                ingestion_notes = None

                if container_type == "history":
                    history_count, url_count = self._parse_history_container(
                        db_path, file_entry, run_id, evidence_conn, evidence_id
                    )
                    total_history += history_count
                    total_urls += url_count
                    records_parsed = history_count
                    urls_parsed = url_count

                elif container_type == "cookies":
                    cookie_count = self._parse_cookies_container(
                        db_path, file_entry, run_id, evidence_conn, evidence_id
                    )
                    total_cookies += cookie_count
                    records_parsed = cookie_count

                elif container_type == "dom_storage":
                    storage_count = self._parse_dom_storage_container(
                        db_path, file_entry, run_id, evidence_conn, evidence_id
                    )
                    total_storage += storage_count
                    records_parsed = storage_count

                elif container_type == "cache":
                    # Cache containers contain binary cache data, not parseable records
                    # Log for awareness but don't treat as error
                    LOGGER.debug("Skipping cache container (binary data): %s", db_path)
                    ingestion_status = "skipped"
                    ingestion_notes = "cache_container_binary"

                else:
                    LOGGER.info("Unknown container type '%s', skipping parse", container_type)
                    ingestion_status = "skipped"
                    ingestion_notes = "unsupported_container_type"

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status=ingestion_status,
                    urls_parsed=urls_parsed,
                    records_parsed=records_parsed,
                    notes=ingestion_notes,
                )

            except Exception as e:
                error_msg = f"Failed to parse {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")
                if inventory_id:
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="failed",
                        notes=str(e),
                    )
                failed_files += 1

        evidence_conn.commit()

        # Report final statistics
        total_records = total_history + total_urls + total_cookies + total_storage
        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=total_records,
            )
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(
            f"Ingested {total_history} history, {total_urls} URLs, "
            f"{total_cookies} cookies, {total_storage} storage entries",
            "info"
        )

        return {
            "history": total_history,
            "urls": total_urls,
            "cookies": total_cookies,
            "storage": total_storage,
        }

    # =========================================================================
    # Private Helper Methods
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate run ID: {timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_statistics_collector(self):
        """Get StatisticsCollector instance (may be None in tests)."""
        try:
            from core.statistics_collector import StatisticsCollector
            return StatisticsCollector.get_instance()
        except Exception:
            return None

    def _discover_files_multi_partition(
        self,
        evidence_conn,
        evidence_id: int,
        callbacks: ExtractorCallbacks,
    ) -> tuple[Dict[int, List[Dict]], int]:
        """Discover container.dat files across ALL partitions using file_list."""
        import time

        LOGGER.info("Starting file_list SQL query for container.dat files")

        query_start = time.monotonic()
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["container.dat"],
            path_patterns=["%Microsoft.MicrosoftEdge_%"],
        )
        query_elapsed = time.monotonic() - query_start
        LOGGER.info(
            "file_list query completed in %.2fs: %d total matches, is_empty=%s",
            query_elapsed, result.total_matches, result.is_empty
        )
        callbacks.on_log(f"SQL query completed in {query_elapsed:.2f}s ({result.total_matches} matches)", "info")

        if result.is_empty:
            # file_list is available but found no matches - trust it!
            # DO NOT fall back to slow iter_paths() with ** patterns
            # If file_list is populated, the SQL query is authoritative
            LOGGER.info(
                "file_list returned no matches for Legacy Edge containers. "
                "This is expected if no Legacy Edge was used on this system. "
                "Skipping slow filesystem fallback."
            )
            callbacks.on_log(
                "No Legacy Edge container.dat files found in file_list (no fallback needed)",
                "info"
            )
            return {}, 0

        files_by_partition: Dict[int, List[Dict]] = {}
        skipped_zero_size = 0

        LOGGER.info("Processing %d partition(s) with matches", len(result.matches_by_partition))
        for partition_index, matches in result.matches_by_partition.items():
            LOGGER.debug("Processing partition %d with %d raw matches", partition_index, len(matches))
            files_list = []
            for match in matches:
                if match.file_name.lower() != "container.dat":
                    continue

                user = extract_user_from_path(match.file_path)
                container_type = self._detect_container_type(match.file_path)

                # Skip zero-size files early - they have no data blocks to read
                # This is common for Windows.old files and sparse/deleted files
                if match.size_bytes is not None and match.size_bytes == 0:
                    LOGGER.debug(
                        "Skipping zero-size file: %s (partition %d)",
                        match.file_path, partition_index
                    )
                    skipped_zero_size += 1
                    continue

                files_list.append({
                    "logical_path": match.file_path,
                    "user": user,
                    "container_type": container_type,
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

            if files_list:
                files_by_partition[partition_index] = files_list
                LOGGER.info(
                    "Partition %d: found %d container.dat file(s)",
                    partition_index, len(files_list)
                )

        total_files = sum(len(f) for f in files_by_partition.values())
        if skipped_zero_size:
            callbacks.on_log(
                f"Skipped {skipped_zero_size} zero-size file(s) (empty/sparse)",
                "info"
            )
            LOGGER.info(
                "Skipped %d zero-size container.dat file(s) during discovery",
                skipped_zero_size
            )
        LOGGER.info(
            "Discovery complete: %d container.dat file(s) across %d partition(s)",
            total_files, len(files_by_partition)
        )
        return files_by_partition, skipped_zero_size

    def _detect_container_type(self, file_path: str) -> str:
        """Detect container type from path."""
        path_lower = file_path.lower()

        if "/history/" in path_lower or "\\history\\" in path_lower:
            return "history"
        elif "/cookies/" in path_lower or "\\cookies\\" in path_lower:
            return "cookies"
        elif "/cache/" in path_lower or "\\cache\\" in path_lower:
            return "cache"
        elif "/domstore/" in path_lower or "\\domstore\\" in path_lower:
            return "dom_storage"
        else:
            return "unknown"

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Copy container.dat from evidence to workspace."""
        source_path = file_info["logical_path"]
        user = file_info.get("user", "unknown")
        container_type = file_info.get("container_type", "unknown")
        partition_index = file_info.get("partition_index", 0)

        # Create output filename with partition prefix to avoid collisions
        safe_user = user.replace(" ", "_").replace("/", "_").replace("\\", "_")
        filename = f"p{partition_index}_{safe_user}_{container_type}_container.dat"
        dest_path = output_dir / filename

        # Try to read the file - may fail for sparse/deleted files (common in Windows.old)
        try:
            file_content = evidence_fs.read_file(source_path)
        except OSError as e:
            # Common for Windows.old files - sparse/no data blocks allocated
            error_reason = str(e)
            if "Invalid file offset" in error_reason:
                error_reason = "sparse_or_deleted_file"

            LOGGER.warning(
                "Cannot read %s (likely sparse/deleted): %s",
                source_path, e
            )
            callbacks.on_log(
                f"Skipped unreadable file: {source_path} ({error_reason})",
                "warning"
            )

            return {
                "copy_status": "failed",
                "error": error_reason,
                "extracted_path": None,
                "logical_path": source_path,
                "user": user,
                "container_type": container_type,
                "partition_index": partition_index,
                "notes": "File exists in filesystem but has no readable data blocks (common in Windows.old)",
            }

        dest_path.write_bytes(file_content)

        # Calculate hashes
        md5 = hashlib.md5(file_content).hexdigest()
        sha256 = hashlib.sha256(file_content).hexdigest()

        return {
            "copy_status": "ok",
            "size_bytes": len(file_content),
            "md5": md5,
            "sha256": sha256,
            "extracted_path": str(dest_path),
            "logical_path": source_path,
            "user": user,
            "container_type": container_type,
            "partition_index": partition_index,
        }

    def _parse_history_container(
        self,
        db_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_conn,
        evidence_id: int
    ) -> tuple[int, int]:
        """
        Parse a history container.dat ESE database.

        Returns (history_count, url_count).
        """
        history_records = []
        url_records = []

        try:
            # Use context manager to ensure proper resource cleanup
            with ESEReader(str(db_path)) as reader:
                # Container.dat has a similar structure to WebCache
                # Look for tables with URL data
                table_names = reader.tables()

                for table_name in table_names:
                    # Skip system tables
                    if table_name.startswith("MSys"):
                        continue

                    try:
                        # Get table metadata to check for URL columns
                        table_info = reader.get_table_info(table_name)
                        if table_info is None:
                            continue

                        # Check for URL-like columns
                        column_names = [col.name for col in table_info.columns]
                        has_url = any("url" in col.lower() for col in column_names)

                        if not has_url:
                            continue

                        # Read records from table
                        for record in reader.read_table(table_name):
                            url = record.get("Url") or record.get("URL") or record.get("url")
                            if not url:
                                continue

                            # Extract timestamps
                            accessed_time = record.get("AccessedTime")
                            accessed_iso = filetime_to_iso(accessed_time) if accessed_time else None

                            # Extract domain and scheme from URL
                            domain = None
                            scheme = None
                            try:
                                parsed = urlparse(url)
                                scheme = parsed.scheme or None
                                domain = parsed.netloc or None
                            except Exception:
                                pass

                            # Build history record
                            history_records.append({
                                "browser": "edge_legacy",
                                "profile": file_entry.get("user", "unknown"),
                                "url": url,
                                "title": record.get("Title", ""),
                                "visit_count": record.get("AccessCount", 1),
                                "last_visit_time_utc": accessed_iso,
                                "visit_time_utc": accessed_iso,  # Maps to ts_utc via pre_insert_hook
                                "source_path": file_entry.get("logical_path", ""),
                                "discovered_by": f"{self.metadata.name}:{self.metadata.version}:{run_id}",
                                "run_id": run_id,
                                "partition_index": file_entry.get("partition_index", 0),
                            })

                            # Build URL record with domain/scheme
                            url_records.append({
                                "url": url,
                                "domain": domain,
                                "scheme": scheme,
                                "source_path": file_entry.get("logical_path", ""),
                                "discovered_by": f"{self.metadata.name}:{self.metadata.version}:{run_id}",
                                "run_id": run_id,
                                "first_seen_utc": accessed_iso,
                                "last_seen_utc": accessed_iso,
                            })

                    except Exception as e:
                        LOGGER.warning("Error parsing table %s: %s", table_name, e)
                        continue

        except Exception as e:
            LOGGER.warning("Failed to parse container.dat %s: %s", db_path, e)
            return 0, 0

        # Batch insert records
        if history_records:
            insert_browser_history_rows(evidence_conn, evidence_id, history_records)

        if url_records:
            insert_urls(evidence_conn, evidence_id, url_records)

        return len(history_records), len(url_records)

    def _parse_cookies_container(
        self,
        db_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_conn,
        evidence_id: int
    ) -> int:
        """
        Parse a cookies container.dat ESE database.

        Returns number of cookies parsed.
        """
        cookie_count = 0
        user = file_entry.get("user", "unknown")
        source_path = file_entry.get("logical_path", "")

        try:
            with ESEReader(str(db_path)) as reader:
                table_names = reader.tables()

                for table_name in table_names:
                    if table_name.startswith("MSys"):
                        continue

                    try:
                        table_info = reader.get_table_info(table_name)
                        if table_info is None:
                            continue

                        # Check for cookie-related columns
                        column_names = [col.name for col in table_info.columns]
                        has_cookie_data = any(
                            col.lower() in ("url", "cookiedata", "expirytime", "flags")
                            for col in column_names
                        )

                        if not has_cookie_data:
                            continue

                        for record in reader.read_table(table_name):
                            url = record.get("Url") or record.get("URL")
                            if not url:
                                continue

                            # Extract domain from URL
                            domain = None
                            try:
                                parsed = urlparse(url)
                                domain = parsed.netloc or None
                            except Exception:
                                pass

                            # Extract timestamps
                            accessed_time = record.get("AccessedTime")
                            expiry_time = record.get("ExpiryTime")
                            modified_time = record.get("ModifiedTime")

                            accessed_iso = filetime_to_iso(accessed_time) if accessed_time else None
                            expiry_iso = filetime_to_iso(expiry_time) if expiry_time else None

                            # Parse flags for secure/httponly
                            flags = record.get("Flags") or 0
                            is_secure = bool(flags & 0x2000)  # INTERNET_COOKIE_IS_SECURE
                            is_httponly = bool(flags & 0x2)   # INTERNET_COOKIE_HTTPONLY

                            # Edge container.dat stores cookie URL metadata,
                            # not individual cookie name/value pairs.
                            # Similar to WebCache cookies - this is metadata only.
                            cookie_name = self._extract_cookie_name_from_url(url)

                            # Insert cookie metadata record
                            insert_cookie_row(
                                evidence_conn,
                                evidence_id=evidence_id,
                                browser="edge_legacy",
                                name=cookie_name,
                                domain=domain or "",
                                profile=user,
                                value="",  # Content in external files, not ESE
                                path="/",
                                is_secure=1 if is_secure else 0,
                                is_httponly=1 if is_httponly else 0,
                                creation_utc=accessed_iso,
                                expires_utc=expiry_iso,
                                last_access_utc=accessed_iso,
                                source_path=source_path,
                                discovered_by=f"{self.metadata.name}:{self.metadata.version}:{run_id}",
                                run_id=run_id,
                                notes="container_dat_metadata:url_reference_only",
                            )
                            cookie_count += 1

                    except Exception as e:
                        LOGGER.warning("Error parsing cookies table %s: %s", table_name, e)
                        continue

        except Exception as e:
            LOGGER.warning("Failed to parse cookies container %s: %s", db_path, e)

        return cookie_count

    def _extract_cookie_name_from_url(self, url: str) -> str:
        """
        Extract a meaningful cookie identifier from an Edge container URL.

        Container.dat stores cookie metadata indexed by URL.
        We derive a unique identifier from the URL for database uniqueness.

        Args:
            url: The URL from the container cookie data

        Returns:
            A descriptive identifier for the cookie metadata entry
        """
        if not url:
            return "unknown"

        # Handle Cookie: prefix format
        if url.lower().startswith("cookie:"):
            cookie_part = url[7:]
            if "@" in cookie_part:
                domain_path = cookie_part.split("@", 1)[1]
                return f"edge_container:{domain_path[:50]}" if len(domain_path) > 50 else f"edge_container:{domain_path}"
            return f"edge_container:{cookie_part[:50]}" if len(cookie_part) > 50 else f"edge_container:{cookie_part}"

        # For regular URLs, use domain + path for uniqueness
        try:
            parsed = urlparse(url)
            identifier = f"{parsed.netloc}{parsed.path}"
            return f"edge_container:{identifier[:50]}" if len(identifier) > 50 else f"edge_container:{identifier}"
        except Exception:
            return f"edge_container:{url[:50]}" if len(url) > 50 else f"edge_container:{url}"

    def _parse_dom_storage_container(
        self,
        db_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_conn,
        evidence_id: int
    ) -> int:
        """
        Parse a DOM storage container.dat ESE database.

        Returns number of storage entries parsed.
        """
        storage_records = []
        user = file_entry.get("user", "unknown")
        source_path = file_entry.get("logical_path", "")

        try:
            with ESEReader(str(db_path)) as reader:
                table_names = reader.tables()

                for table_name in table_names:
                    if table_name.startswith("MSys"):
                        continue

                    try:
                        table_info = reader.get_table_info(table_name)
                        if table_info is None:
                            continue

                        # Check for storage-related columns
                        column_names = [col.name for col in table_info.columns]
                        has_storage = any(
                            col.lower() in ("key", "value", "url", "itemname")
                            for col in column_names
                        )

                        if not has_storage:
                            continue

                        for record in reader.read_table(table_name):
                            # Extract URL/origin
                            url = record.get("Url") or record.get("URL") or ""

                            # Extract key/value
                            key = record.get("Key") or record.get("ItemName") or ""
                            value = record.get("Value") or ""

                            if not key and not url:
                                continue

                            # Extract domain from URL
                            origin = None
                            try:
                                parsed = urlparse(url)
                                if parsed.scheme and parsed.netloc:
                                    origin = f"{parsed.scheme}://{parsed.netloc}"
                            except Exception:
                                pass

                            # Build storage record
                            storage_records.append({
                                "browser": "edge_legacy",
                                "profile": user,
                                "origin": origin or url,
                                "key": str(key),
                                "value": str(value) if value else None,
                                "source_path": source_path,
                                "discovered_by": f"{self.metadata.name}:{self.metadata.version}:{run_id}",
                                "run_id": run_id,
                            })

                    except Exception as e:
                        LOGGER.warning("Error parsing DOM storage table %s: %s", table_name, e)
                        continue

        except Exception as e:
            LOGGER.warning("Failed to parse DOM storage container %s: %s", db_path, e)

        # Batch insert storage records
        if storage_records:
            insert_local_storages(evidence_conn, evidence_id, storage_records)

        return len(storage_records)
