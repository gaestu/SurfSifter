"""
Firefox Downloads Extractor

Extracts browser download history from all Firefox-based browsers.
Uses shared patterns and parsers from the firefox family module.

Features:
- Modern annotation-based downloads (Firefox v26+)
- Legacy moz_downloads table (Firefox < v26)
- PRTime timestamp conversion to ISO 8601
- StatisticsCollector integration
- WAL/journal file copying for SQLite recovery
- Forensic provenance (run_id, source_path, partition context)
- Multi-partition extraction support
- Schema warning support for unknown tables/values

Added multi-partition support, schema warnings, delete-by-run cleanup
Initial implementation
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import BrowserConfigWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    get_ewf_paths_from_evidence_fs,
)
from ...._shared.extraction_warnings import ExtractionWarningCollector
from .._patterns import (
    FIREFOX_BROWSERS,
    get_patterns,
    get_browser_display_name,
    get_all_browsers,
    extract_profile_from_path,
    detect_browser_from_path,
)
from ._parsers import (
    parse_downloads,
    get_download_stats,
)
from core.logging import get_logger
from core.database import (
    insert_browser_download_row,
    insert_browser_inventory,
    insert_urls,
    update_inventory_ingestion_status,
)
from core.database.helpers.browser_downloads import delete_browser_downloads_by_run


LOGGER = get_logger("extractors.browser.firefox.downloads")


class FirefoxDownloadsExtractor(BaseExtractor):
    """
    Extract browser downloads from Firefox-based browsers.

    Supports: Firefox, Firefox ESR, Tor Browser

    Dual-phase workflow:
    - Extraction: Scans filesystem, copies places.sqlite files to workspace
    - Ingestion: Parses SQLite databases, inserts with forensic fields

    Features:
    - Modern annotation-based downloads (moz_annos, Firefox v26+)
    - Legacy moz_downloads table (Firefox < v26)
    - PRTime timestamp conversion to ISO 8601
    - StatisticsCollector integration for run tracking
    - WAL/journal file copying for SQLite recovery
    - Browser selection config widget
    - Multi-partition extraction support
    - Schema warning support for unknown tables/values

    Note: Firefox stores downloads in places.sqlite (same file as history).
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="firefox_downloads",
            display_name="Firefox Downloads",
            description="Extract browser downloads from Firefox, Firefox ESR, Tor Browser",
            category="browser",
            requires_tools=[],  # Pure Python, no external tools
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted"
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run (manifest exists)."""
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """
        Return configuration widget (browser selection + multi-partition).

        Uses BrowserConfigWidget filtered to Firefox browsers with multi-partition option.
        """
        return BrowserConfigWidget(
            parent,
            supported_browsers=get_all_browsers(),
            default_scan_all_partitions=True,
        )

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction/ingestion state."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"Firefox Downloads\nFiles: {file_count}\nRun: {data.get('run_id', 'N/A')[:20]}"
        else:
            status_text = "Firefox Downloads\nNo extraction yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "firefox_downloads"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Firefox downloads databases from evidence.

        Workflow:
            1. Generate run_id
            2. Scan evidence for Firefox places.sqlite files (multi-partition if enabled)
            3. Copy matching files to output_dir/
            4. Calculate hashes, collect E01 context
            5. Write manifest.json

        Multi-partition support:
            When scan_all_partitions=True (default), uses file_list discovery to
            find places.sqlite files across ALL partitions, not just the main partition.
            This captures browser artifacts from dual-boot systems, portable apps,
            and old OS installations.
        """
        callbacks.on_step("Initializing Firefox downloads extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting Firefox downloads extraction (run_id=%s)", run_id)

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")
        scan_all_partitions = config.get("scan_all_partitions", True)

        # Start statistics tracking
        collector = self._get_statistics_collector()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Initialize manifest
        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "multi_partition_extraction": scan_all_partitions,
            "partitions_scanned": [],
            "partitions_with_artifacts": [],
            "files": [],
            "status": "ok",
            "notes": [],
        }

        # Determine which browsers to search
        browsers = config.get("browsers") or config.get("selected_browsers") or get_all_browsers()

        # Scan for downloads files (places.sqlite) - use multi-partition if enabled
        callbacks.on_step("Scanning for Firefox downloads databases")

        files_by_partition: Dict[int, List[Dict]] = {}

        if scan_all_partitions and evidence_conn is not None:
            # Multi-partition discovery via file_list
            files_by_partition = self._discover_files_multi_partition(
                evidence_fs, evidence_conn, evidence_id, browsers, callbacks
            )
        else:
            # Single partition fallback
            if scan_all_partitions and evidence_conn is None:
                callbacks.on_log(
                    "Multi-partition scan requested but no evidence_conn provided, using single partition",
                    "warning"
                )
            downloads_files = self._discover_files(evidence_fs, browsers, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if downloads_files:
                files_by_partition[partition_index] = downloads_files

        # Flatten for counting
        all_downloads_files = []
        for files_list in files_by_partition.values():
            all_downloads_files.extend(files_list)

        # Update manifest with partition info
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = sorted(
            p for p, files in files_by_partition.items() if files
        )

        # Report discovered files
        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=len(all_downloads_files))

        callbacks.on_log(f"Found {len(all_downloads_files)} places.sqlite database(s) across {len(files_by_partition)} partition(s)")

        if not all_downloads_files:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Firefox downloads files found")
            LOGGER.info("No downloads files found")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="success")
        else:
            callbacks.on_progress(0, len(all_downloads_files), "Extracting downloads databases")

            # Get EWF paths for opening other partitions
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            file_index = 0

            # Process each partition
            for partition_index in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_index]

                # Determine which filesystem to use
                current_partition = getattr(evidence_fs, 'partition_index', 0)

                if partition_index == current_partition or ewf_paths is None:
                    # Use existing filesystem handle
                    fs_to_use = evidence_fs
                    need_close = False
                else:
                    # Open partition-specific filesystem
                    try:
                        from core.evidence_fs import open_ewf_partition
                        fs_to_use = open_ewf_partition(ewf_paths, partition_index=partition_index)
                        need_close = True
                        callbacks.on_log(f"Opened partition {partition_index} for extraction", "info")
                    except Exception as e:
                        callbacks.on_log(
                            f"Failed to open partition {partition_index}: {e}",
                            "error"
                        )
                        manifest_data["notes"].append(f"Failed to open partition {partition_index}: {e}")
                        continue

                try:
                    for file_info in partition_files:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            manifest_data["notes"].append("Extraction cancelled by user")
                            if collector:
                                collector.finish_run(evidence_id, self.metadata.name, status="cancelled")
                            break

                        try:
                            callbacks.on_progress(
                                file_index + 1, len(all_downloads_files),
                                f"Copying {file_info['browser']} downloads (partition {partition_index})"
                            )

                            extracted = self._extract_file(
                                fs_to_use, file_info, output_dir, run_id, callbacks
                            )
                            extracted["partition_index"] = partition_index
                            manifest_data["files"].append(extracted)
                            file_index += 1

                        except Exception as e:
                            error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            manifest_data["notes"].append(error_msg)
                            manifest_data["status"] = "partial"
                            if collector:
                                collector.report_failed(evidence_id, self.metadata.name, files=1)
                            file_index += 1

                    if callbacks.is_cancelled():
                        break

                finally:
                    # Close partition handle if we opened it
                    if need_close and fs_to_use is not None:
                        try:
                            close_method = getattr(fs_to_use, 'close', None)
                            if close_method and callable(close_method):
                                close_method()
                        except Exception as e:
                            LOGGER.debug("Error closing partition %d handle: %s", partition_index, e)

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

        # Finish statistics tracking
        if collector and manifest_data["status"] not in ("cancelled",):
            status = "success" if manifest_data["status"] == "ok" else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        LOGGER.info(
            "Firefox downloads extraction complete: %d files from %d partition(s), status=%s",
            len(manifest_data["files"]),
            len(manifest_data["partitions_with_artifacts"]),
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
    ) -> Dict[str, Any]:
        """
        Parse extracted downloads databases and ingest into database.

        Workflow:
            1. Read manifest.json
            2. Delete previous run data (for re-ingestion)
            3. Create schema warning collector
            4. Register files in browser_cache_inventory
            5. For each downloads database:
               - Parse SQLite moz_annos or moz_downloads
               - Detect unknown tables and enum values
               - Insert into browser_downloads table
               - Cross-post URLs to unified urls table
            6. Flush schema warnings
            7. Return summary statistics
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"downloads": 0, "records": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])
        evidence_label = config.get("evidence_label", "")

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Continue statistics tracking for ingestion phase
        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Delete previous downloads from this run (for re-ingestion)
        callbacks.on_step("Cleaning previous run data")
        try:
            deleted = delete_browser_downloads_by_run(evidence_conn, evidence_id, run_id)
            if deleted > 0:
                LOGGER.debug("Deleted %d previous download records for run %s", deleted, run_id)
        except Exception as e:
            LOGGER.debug("No previous run data to delete: %s", e)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if collector:
                collector.report_ingested(evidence_id, self.metadata.name, records=0, downloads=0)
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"downloads": 0, "records": 0}

        total_records = 0
        total_complete = 0
        total_dangerous = 0
        total_urls = 0
        failed_files = 0

        callbacks.on_progress(0, len(files), "Parsing downloads databases")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(files),
                f"Parsing {file_entry.get('browser', 'unknown')} downloads"
            )

            try:
                # Register in inventory
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "unknown"),
                    artifact_type="browser_downloads",
                    run_id=run_id,
                    extracted_path=file_entry.get("extracted_path", ""),
                    extraction_status="ok",
                    extraction_timestamp_utc=manifest_data["extraction_timestamp_utc"],
                    logical_path=file_entry.get("logical_path", ""),
                    profile=file_entry.get("profile"),
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    forensic_path=file_entry.get("forensic_path"),
                    extraction_tool=manifest_data.get("extraction_tool"),
                    file_size_bytes=file_entry.get("file_size_bytes"),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                )

                # Parse and insert records
                db_path = Path(file_entry["extracted_path"])
                if not db_path.is_absolute():
                    db_path = output_dir / db_path

                counts = self._parse_and_insert(
                    db_path,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                    callbacks,
                    warning_collector=warning_collector,
                )

                # Update inventory
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    urls_parsed=counts["total"],
                    records_parsed=counts["total"],
                )

                total_records += counts["total"]
                total_complete += counts["complete"]
                total_dangerous += counts["dangerous"]
                total_urls += counts["urls_crossposted"]

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")
                failed_files += 1

                if "inventory_id" in locals():
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(e),
                    )

        # Flush schema warnings to database
        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)
            callbacks.on_log(f"Schema discovery: {warning_count} warnings recorded", level="info")

        evidence_conn.commit()

        # Report final statistics
        if collector:
            collector.report_ingested(evidence_id, self.metadata.name, records=total_records, downloads=total_records)
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        return {
            "downloads": total_records,
            "complete": total_complete,
            "dangerous": total_dangerous,
            "urls_crossposted": total_urls,
            "failed_files": failed_files,
            "schema_warnings": warning_count,
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

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context from evidence filesystem."""
        try:
            source_path = getattr(evidence_fs, "source_path", None)
            if source_path is not None and not isinstance(source_path, (str, Path)):
                source_path = None

            fs_type = getattr(evidence_fs, "fs_type", "unknown")
            if not isinstance(fs_type, str):
                fs_type = "unknown"

            return {
                "image_path": str(source_path) if source_path else None,
                "fs_type": fs_type,
            }
        except Exception:
            return {"image_path": None, "fs_type": "unknown"}

    def _get_tool_version(self) -> str:
        """Build extraction tool version string."""
        versions = []

        try:
            import pytsk3
            versions.append(f"pytsk3:{pytsk3.TSK_VERSION_STR}")
        except ImportError:
            versions.append("pytsk3:not_installed")

        try:
            import pyewf
            versions.append(f"pyewf:{pyewf.get_version()}")
        except ImportError:
            versions.append("pyewf:not_installed")

        return ";".join(versions)

    def _discover_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """
        Scan evidence for Firefox downloads files (single partition fallback).

        Args:
            evidence_fs: Mounted filesystem
            browsers: List of browser keys to search
            callbacks: Progress/log interface

        Returns:
            List of dicts with browser, profile, logical_path, partition_index, etc.
        """
        downloads_files = []
        seen_paths = set()  # Deduplicate
        partition_index = getattr(evidence_fs, 'partition_index', 0)

        for browser in browsers:
            if browser not in FIREFOX_BROWSERS:
                callbacks.on_log(f"Unknown browser: {browser}", "warning")
                continue

            patterns = get_patterns(browser, "downloads")

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        # Skip WAL/journal files (they're companion files)
                        if path_str.endswith(("-wal", "-shm", "-journal")):
                            continue

                        if path_str in seen_paths:
                            continue
                        seen_paths.add(path_str)

                        profile = extract_profile_from_path(path_str)
                        detected_browser = detect_browser_from_path(path_str)

                        downloads_files.append({
                            "logical_path": path_str,
                            "browser": detected_browser,
                            "profile": profile,
                            "artifact_type": "browser_downloads",
                            "display_name": get_browser_display_name(detected_browser),
                            "partition_index": partition_index,
                        })

                        callbacks.on_log(f"Found {detected_browser} downloads: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return downloads_files

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, List[Dict]]:
        """
        Discover Firefox downloads files across multiple partitions via file_list.

        Uses the file_list table populated by the file_list extractor to find
        places.sqlite files on ALL partitions without needing to mount each one.

        Args:
            evidence_fs: Mounted filesystem (for current partition fallback)
            evidence_conn: Database connection for file_list queries
            evidence_id: Evidence ID for queries
            browsers: List of browser keys to search
            callbacks: Progress/log interface

        Returns:
            Dict mapping partition_index -> list of file info dicts
        """
        files_by_partition: Dict[int, List[Dict]] = {}

        # Check if file_list is available
        if not check_file_list_available(evidence_conn, evidence_id):
            callbacks.on_log(
                "File list not available, falling back to single partition scan",
                "info"
            )
            # Fall back to single partition
            files = self._discover_files(evidence_fs, browsers, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if files:
                files_by_partition[partition_index] = files
            return files_by_partition

        # Build path patterns for Firefox browsers
        # Pattern examples from _patterns.py:
        #   "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles/*/places.sqlite"
        #   "home/*/.mozilla/firefox/*/places.sqlite"
        #   "*/Tor Browser/Browser/TorBrowser/Data/Browser/*/places.sqlite"
        path_patterns = set()
        for browser in browsers:
            if browser not in FIREFOX_BROWSERS:
                continue
            # Extract path patterns from browser roots
            profile_roots = FIREFOX_BROWSERS[browser].get("profile_roots", [])
            for root in profile_roots:
                # Convert glob-style pattern to SQL LIKE pattern
                # "Users/*/AppData/Roaming/Mozilla/Firefox/Profiles" -> "%Firefox%"
                if "Firefox" in root or "firefox" in root:
                    path_patterns.add("%Firefox%")
                if "Mozilla" in root or "mozilla" in root:
                    path_patterns.add("%Mozilla%")
                if "Tor Browser" in root or "tor-browser" in root or "TorBrowser" in root:
                    path_patterns.add("%Tor Browser%")
                    path_patterns.add("%TorBrowser%")
                    path_patterns.add("%tor-browser%")

        if not path_patterns:
            # Fallback: search for any Firefox-like paths
            path_patterns = {"%Firefox%", "%Mozilla%", "%Tor Browser%", "%TorBrowser%"}

        # Query file_list for places.sqlite files
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["places.sqlite"],
            path_patterns=list(path_patterns),
        )

        if result.is_empty:
            callbacks.on_log(
                "No places.sqlite files found in file_list, falling back to filesystem scan",
                "warning"
            )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        if result.is_multi_partition:
            callbacks.on_log(
                f"Found places.sqlite files on {len(result.partitions_with_matches)} partitions: "
                f"{result.partitions_with_matches}",
                "info"
            )

        # Convert FileListMatch objects to extractor's expected format
        for partition_index, matches in result.matches_by_partition.items():
            files_list = []
            for match in matches:
                # Skip WAL/journal files (shouldn't be returned with filename_patterns=["places.sqlite"])
                if match.file_path.endswith(("-wal", "-shm", "-journal")):
                    continue

                # Detect browser from path
                detected_browser = detect_browser_from_path(match.file_path)
                if detected_browser and detected_browser not in browsers:
                    continue  # Skip if browser not in selection

                profile = extract_profile_from_path(match.file_path)

                file_info = {
                    "logical_path": match.file_path,
                    "browser": detected_browser or "firefox",
                    "profile": profile,
                    "artifact_type": "browser_downloads",
                    "display_name": get_browser_display_name(detected_browser or "firefox"),
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                }
                files_list.append(file_info)

                callbacks.on_log(
                    f"Found {detected_browser or 'firefox'} downloads on partition {partition_index}: "
                    f"{match.file_path}",
                    "info"
                )

            if files_list:
                files_by_partition[partition_index] = files_list

        return files_by_partition

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        run_id: str,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """
        Copy file from evidence to workspace with metadata.

        Filename includes partition index to prevent collisions when same
        browser/profile exists on multiple partitions.
        """
        source_path = file_info["logical_path"]
        browser = file_info["browser"]
        profile = file_info["profile"]
        partition_index = file_info.get("partition_index", 0)

        # Create output filename with partition index to prevent collisions
        safe_profile = profile.replace(" ", "_").replace("/", "_").replace(".", "_")
        filename = f"{browser}_{safe_profile}_p{partition_index}_places_downloads.sqlite"
        dest_path = output_dir / filename

        callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

        # Read and write file
        file_content = evidence_fs.read_file(source_path)
        dest_path.write_bytes(file_content)

        # Calculate hashes
        md5 = hashlib.md5(file_content).hexdigest()
        sha256 = hashlib.sha256(file_content).hexdigest()
        size = len(file_content)

        # Copy companion files (WAL, journal, shm)
        companion_files = []
        for suffix in ["-wal", "-journal", "-shm"]:
            companion_path = source_path + suffix
            try:
                companion_content = evidence_fs.read_file(companion_path)
                companion_dest = Path(str(dest_path) + suffix)
                companion_dest.write_bytes(companion_content)
                companion_files.append({
                    "suffix": suffix,
                    "size_bytes": len(companion_content),
                })
                callbacks.on_log(f"Copied companion: {companion_path}", "info")
            except Exception:
                pass  # Companion doesn't exist

        return {
            "copy_status": "ok",
            "file_size_bytes": size,
            "md5": md5,
            "sha256": sha256,
            "extracted_path": str(dest_path),
            "local_filename": filename,
            "browser": browser,
            "profile": profile,
            "logical_path": source_path,
            "artifact_type": "browser_downloads",
            "partition_index": partition_index,
            "fs_type": file_info.get("fs_type"),
            "forensic_path": file_info.get("forensic_path"),
            "companion_files": companion_files,
        }

    def _parse_and_insert(
        self,
        db_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, int]:
        """
        Parse downloads database and insert records.

        Also cross-posts URLs to unified urls table for analysis.

        Args:
            db_path: Path to extracted places.sqlite
            file_entry: File metadata from manifest
            run_id: Current run ID
            evidence_id: Evidence ID
            evidence_conn: Database connection
            callbacks: Progress/log interface
            warning_collector: Optional collector for schema warnings

        Returns:
            Dict with counts: total, complete, dangerous, urls_crossposted
        """
        from urllib.parse import urlparse

        if not db_path.exists():
            LOGGER.warning("Downloads database not found: %s", db_path)
            return {"total": 0, "complete": 0, "dangerous": 0, "urls_crossposted": 0}

        browser = file_entry.get("browser", "firefox")
        profile = file_entry.get("profile", "unknown")  # Firefox profiles aren't "Default"
        source_path = file_entry.get("logical_path", "")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        count = 0
        complete_count = 0
        dangerous_count = 0
        url_records = []  # Collect URLs for unified urls table

        try:
            for download in parse_downloads(
                db_path,
                source_file=source_path,
                warning_collector=warning_collector,
            ):
                # Build notes with Firefox-specific metadata
                notes_parts = []
                if download.deleted:
                    notes_parts.append("deleted=true")
                notes = "; ".join(notes_parts) if notes_parts else None

                insert_browser_download_row(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=browser,
                    profile=profile,
                    url=download.url,
                    target_path=download.target_path,
                    filename=download.filename,
                    start_time_utc=download.start_time_utc,
                    end_time_utc=download.end_time_utc,
                    total_bytes=download.total_bytes,
                    received_bytes=download.received_bytes,
                    mime_type=download.mime_type,
                    referrer=download.referrer,
                    state=download.state,
                    danger_type=download.danger_type,  # From metaData.reputationCheckVerdict
                    opened=None,  # Firefox doesn't track opened status
                    run_id=run_id,
                    source_path=source_path,
                    discovered_by=discovered_by,
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    logical_path=file_entry.get("logical_path"),
                    forensic_path=file_entry.get("forensic_path"),
                    notes=notes,
                )
                count += 1

                # Track statistics
                if download.state == "complete":
                    complete_count += 1
                if download.danger_type:
                    dangerous_count += 1

                # Collect URL for unified urls table
                if download.url:
                    parsed = urlparse(download.url)
                    url_records.append({
                        "url": download.url,
                        "domain": parsed.netloc or None,
                        "scheme": parsed.scheme or None,
                        "discovered_by": discovered_by,
                        "run_id": run_id,
                        "source_path": source_path,
                        "context": f"download:{browser}:{profile}",
                        "first_seen_utc": download.start_time_utc,
                        "content_type": download.mime_type,
                    })

                # Also backfill referrer URL if present
                if download.referrer and not download.referrer.startswith(("javascript:", "data:")):
                    ref_parsed = urlparse(download.referrer)
                    url_records.append({
                        "url": download.referrer,
                        "domain": ref_parsed.netloc or None,
                        "scheme": ref_parsed.scheme or None,
                        "discovered_by": discovered_by,
                        "run_id": run_id,
                        "source_path": source_path,
                        "context": f"download_referrer:{browser}:{profile}",
                        "first_seen_utc": download.start_time_utc,
                    })

            # Cross-post URLs to unified urls table for analysis
            urls_crossposted = 0
            if url_records:
                try:
                    insert_urls(evidence_conn, evidence_id, url_records)
                    urls_crossposted = len(url_records)
                    LOGGER.debug("Cross-posted %d download URLs to urls table", urls_crossposted)
                except Exception as e:
                    LOGGER.debug("Failed to cross-post download URLs: %s", e)

        except Exception as e:
            LOGGER.error("Failed to read downloads database %s: %s", db_path, e)
            if warning_collector:
                warning_collector.add_warning(
                    warning_type="file_corrupt",
                    category="database",
                    severity="error",
                    artifact_type="downloads",
                    source_file=source_path,
                    item_name=str(db_path.name),
                    item_value=str(e),
                )
            return {"total": 0, "complete": 0, "dangerous": 0, "urls_crossposted": 0}

        if count:
            callbacks.on_log(f"Inserted {count} downloads from {browser} ({complete_count} complete)", "info")

        return {
            "total": count,
            "complete": complete_count,
            "dangerous": dangerous_count,
            "urls_crossposted": urls_crossposted,
        }
