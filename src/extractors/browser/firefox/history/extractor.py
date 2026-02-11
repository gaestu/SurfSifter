"""
Firefox History Extractor

Extracts browser history from all Firefox-based browsers (Firefox, Firefox ESR, Tor Browser).
Uses shared patterns from the firefox family module and local parsers.

Features:
- Per-visit record extraction (moz_historyvisits + moz_places)
- PRTime timestamp conversion to ISO 8601
- Multi-partition discovery via file_list
- Schema warning support for unknown tables/columns/visit types
- StatisticsCollector integration
- WAL/journal file copying for SQLite recovery
- Forensic provenance (run_id, source_path, partition context)
- Collision-safe filenames with partition index and path hash

 Changes:
- Added multi-partition support matching Chromium history extractor
- Added schema warning collection via ExtractionWarningCollector
- Fixed file overwrite risk with partition + hash in filenames
- Removed URL deduplication - all visit URLs now inserted
- Moved history parsing to local _parsers.py
- Added _schemas.py for known tables/columns/visit types
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
    parse_history_visits,
    parse_search_queries,
    get_history_stats,
)
from core.logging import get_logger
from core.database import (
    insert_browser_history_row,
    insert_browser_history_rows,
    insert_browser_inventory,
    insert_urls,
    update_inventory_ingestion_status,
)
from core.database.helpers.browser_search_terms import insert_search_terms


LOGGER = get_logger("extractors.browser.firefox.history")


class FirefoxHistoryExtractor(BaseExtractor):
    """
    Extract browser history from Firefox-based browsers.

    Supports: Firefox, Firefox ESR, Tor Browser

    Dual-phase workflow:
    - Extraction: Scans filesystem, copies places.sqlite files to workspace
    - Ingestion: Parses SQLite databases, inserts with forensic fields

    Features:
    - Per-visit record extraction (moz_historyvisits + moz_places)
    - PRTime timestamp conversion to ISO 8601
    - Multi-partition discovery via file_list table
    - Schema warning support for unknown tables/columns
    - StatisticsCollector integration for run tracking
    - WAL/journal file copying for SQLite recovery
    - Browser selection config widget
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="firefox_history",
            display_name="Firefox History",
            description="Extract browser history from Firefox, Firefox ESR, Tor Browser",
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

        Uses BrowserConfigWidget with Firefox browsers and multi-partition option.
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
            status_text = f"Firefox History\nFiles: {file_count}\nRun: {data.get('run_id', 'N/A')[:20]}"
        else:
            status_text = "Firefox History\nNo extraction yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "firefox_history"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Firefox history databases from evidence.

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
        callbacks.on_step("Initializing Firefox history extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting Firefox history extraction (run_id=%s)", run_id)

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
            "schema_version": "2.0.0",  # Bumped for multi-partition support
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

        # Scan for history files - use multi-partition if enabled and evidence_conn available
        callbacks.on_step("Scanning for Firefox history databases")

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
            history_files = self._discover_files(evidence_fs, browsers, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if history_files:
                files_by_partition[partition_index] = history_files

        # Flatten for counting
        all_history_files = []
        for files_list in files_by_partition.values():
            all_history_files.extend(files_list)

        # Update manifest with partition info
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = sorted(
            p for p, files in files_by_partition.items() if files
        )

        # Report discovered files
        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=len(all_history_files))

        callbacks.on_log(f"Found {len(all_history_files)} history file(s) across {len(files_by_partition)} partition(s)")

        if not all_history_files:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Firefox history files found")
            LOGGER.info("No history files found")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="success")
        else:
            callbacks.on_progress(0, len(all_history_files), "Extracting history databases")

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
                            break

                        try:
                            callbacks.on_progress(
                                file_index + 1, len(all_history_files),
                                f"Copying {file_info['browser']} history (partition {partition_index})"
                            )

                            extracted = self._extract_file(fs_to_use, file_info, output_dir, callbacks)
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
            "Firefox history extraction complete: %d files from %d partition(s), status=%s",
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
    ) -> Dict[str, int]:
        """
        Parse extracted history databases and ingest into database.

        Workflow:
            1. Read manifest.json
            2. Create warning collector for schema discovery
            3. Register files in browser_cache_inventory
            4. For each history database:
               - Parse SQLite visits table
               - Parse search queries (moz_places_metadata_search_queries, Firefox 75+)
               - Detect unknown tables/columns/visit types
               - Insert into browser_history table
               - Insert all URLs to urls table (no deduplication)
               - Insert search terms to browser_search_terms table
            5. Update inventory status
            6. Flush warnings to database
            7. Return counts
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"urls": 0, "records": 0, "search_terms": 0}

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

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if collector:
                collector.report_ingested(evidence_id, self.metadata.name, records=0, urls=0)
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"urls": 0, "records": 0, "search_terms": 0}

        total_records = 0
        total_urls = 0
        total_search_terms = 0
        failed_files = 0

        callbacks.on_progress(0, len(files), "Parsing history databases")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(files),
                f"Parsing {file_entry.get('browser', 'unknown')} history"
            )

            try:
                # Register in inventory
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "unknown"),
                    artifact_type="history",
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

                records, urls, search_count = self._parse_and_insert(
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
                    urls_parsed=urls,
                    records_parsed=records,
                )

                total_records += records
                total_urls += urls
                total_search_terms += search_count

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
            callbacks.on_log(f"Recorded {warning_count} schema warnings", "info")

        evidence_conn.commit()

        # Report final statistics
        if collector:
            collector.report_ingested(evidence_id, self.metadata.name, records=total_records, urls=total_urls)
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        if total_search_terms > 0:
            callbacks.on_log(f"Total: {total_search_terms} search terms extracted", "info")

        return {"urls": total_urls, "records": total_records, "search_terms": total_search_terms}

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
        Scan evidence for Firefox history files (single partition fallback).

        Args:
            evidence_fs: Mounted filesystem
            browsers: List of browser keys to search
            callbacks: Progress/log interface

        Returns:
            List of dicts with browser, profile, logical_path, etc.
        """
        history_files = []
        seen_paths: set[str] = set()  # Deduplicate across patterns

        for browser in browsers:
            if browser not in FIREFOX_BROWSERS:
                callbacks.on_log(f"Unknown browser: {browser}", "warning")
                continue

            patterns = get_patterns(browser, "history")

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        # Skip WAL/journal files (they're companion files)
                        if path_str.endswith(("-wal", "-shm", "-journal")):
                            continue

                        # Skip if we've already seen this path (multiple patterns may match)
                        if path_str in seen_paths:
                            continue
                        seen_paths.add(path_str)

                        profile = extract_profile_from_path(path_str)
                        detected_browser = detect_browser_from_path(path_str)
                        partition_index = getattr(evidence_fs, 'partition_index', 0)

                        history_files.append({
                            "logical_path": path_str,
                            "browser": detected_browser,
                            "profile": profile,
                            "artifact_type": "history",
                            "display_name": get_browser_display_name(detected_browser),
                            "partition_index": partition_index,
                        })

                        callbacks.on_log(f"Found {detected_browser} history: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return history_files

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, List[Dict]]:
        """
        Discover Firefox history files across ALL partitions using file_list.

        This method queries the pre-populated file_list table to find places.sqlite
        files across all partitions, not just the auto-selected main partition.

        Falls back to single-partition iter_paths() if file_list is empty.

        Args:
            evidence_fs: Evidence filesystem
            evidence_conn: Evidence database connection
            evidence_id: Evidence ID for file_list lookup
            browsers: List of browser keys to search
            callbacks: Progress/log callbacks

        Returns:
            Dict mapping partition_index -> list of file info dicts
        """
        # Check if file_list is available
        available, count = check_file_list_available(evidence_conn, evidence_id)

        if not available:
            callbacks.on_log(
                "file_list empty, falling back to single-partition discovery",
                "info"
            )
            # Fallback: use traditional iter_paths on main partition
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")

        # Build path patterns for file_list query
        # Firefox stores history in places.sqlite
        # We look for paths containing Firefox/Tor-specific strings
        path_patterns = []
        for browser in browsers:
            if browser not in FIREFOX_BROWSERS:
                continue
            # Firefox paths
            if browser in ("firefox", "firefox_esr"):
                path_patterns.append("%Mozilla%Firefox%Profiles%")
                path_patterns.append("%.mozilla%firefox%")
            # Tor Browser paths
            if browser == "tor":
                path_patterns.append("%Tor Browser%TorBrowser%Data%Browser%")
                path_patterns.append("%tor-browser%Browser%TorBrowser%Data%Browser%")

        # Remove duplicates
        path_patterns = list(set(path_patterns))

        # Query file_list
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["places.sqlite"],
            path_patterns=path_patterns if path_patterns else None,
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
                f"Found places.sqlite files on {len(result.partitions_with_matches)} partitions: {result.partitions_with_matches}",
                "info"
            )

        # Convert FileListMatch objects to extractor's expected format
        files_by_partition: Dict[int, List[Dict]] = {}

        for partition_index, matches in result.matches_by_partition.items():
            files_list = []
            for match in matches:
                # Detect browser from path
                browser = detect_browser_from_path(match.file_path)
                if browser and browser not in browsers:
                    continue  # Skip if browser not in selection

                profile = extract_profile_from_path(match.file_path)

                files_list.append({
                    "logical_path": match.file_path,
                    "browser": browser or "firefox",
                    "profile": profile,
                    "artifact_type": "history",
                    "display_name": get_browser_display_name(browser) if browser else "Mozilla Firefox",
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

                callbacks.on_log(
                    f"Found {browser or 'firefox'} history on partition {partition_index}: {match.file_path}",
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
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Copy file from evidence to workspace with metadata."""
        source_path = file_info["logical_path"]
        browser = file_info["browser"]
        profile = file_info.get("profile") or "Unknown"
        partition_index = file_info.get("partition_index", 0)

        # Create output filename with partition suffix and mini-hash to prevent collision
        # Mini-hash: first 8 chars of SHA256 of source path, ensures uniqueness for
        # same browser/profile in different locations (e.g., dual-boot, portable installs)
        # Format: {browser}_{profile}_p{partition}_{mini_hash}_places.sqlite
        safe_profile = profile.replace(" ", "_").replace("/", "_").replace(".", "_")
        path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]
        filename = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_places.sqlite"
        dest_path = output_dir / filename

        callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

        # Read and write file
        file_content = evidence_fs.read_file(source_path)
        dest_path.write_bytes(file_content)

        # Calculate hashes
        md5 = hashlib.md5(file_content).hexdigest()
        sha256 = hashlib.sha256(file_content).hexdigest()
        size = len(file_content)

        # Get filesystem type
        fs_type = getattr(evidence_fs, "fs_type", "unknown")
        if not isinstance(fs_type, str):
            fs_type = "unknown"

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
            "size_bytes": size,
            "file_size_bytes": size,
            "md5": md5,
            "sha256": sha256,
            "extracted_path": str(dest_path),
            "browser": browser,
            "profile": profile,
            "logical_path": source_path,
            "artifact_type": "history",
            "partition_index": partition_index,
            "fs_type": fs_type,
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
    ) -> tuple[int, int, int]:
        """
        Parse history database and insert records.

        Returns:
            Tuple of (records_count, urls_count, search_terms_count)
        """
        if not db_path.exists():
            LOGGER.warning("History database not found: %s", db_path)
            return 0, 0, 0

        browser = file_entry.get("browser", "firefox")
        profile = file_entry.get("profile", "Unknown")
        source_path = file_entry.get("logical_path", "")
        partition_index = file_entry.get("partition_index")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        records = []
        url_records = []

        try:
            for visit in parse_history_visits(db_path, warning_collector=warning_collector):
                # Build enhanced notes with all forensic metadata
                notes_data = {
                    "prtime_raw": visit.visit_date_raw,
                    "visit_type": visit.visit_type,
                    "visit_type_label": visit.visit_type_label,
                    "frecency": visit.frecency,
                    "hidden": visit.hidden,
                }
                if visit.typed_input:
                    notes_data["typed_input"] = visit.typed_input
                if visit.from_visit:
                    notes_data["from_visit"] = visit.from_visit

                records.append({
                    "browser": browser,
                    "url": visit.url,
                    "title": visit.title,
                    "visit_time_utc": visit.visit_time_utc,
                    "visit_count": visit.visit_count,
                    "typed_count": visit.typed,
                    "profile": profile,
                    "run_id": run_id,
                    "evidence_id": evidence_id,
                    "source_path": source_path,
                    "discovered_by": discovered_by,
                    "notes": json.dumps(notes_data),
                })

                # Build URL record for each visit (no deduplication)
                # Extract domain and scheme from URL
                domain = None
                scheme = None
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(visit.url)
                    scheme = parsed.scheme or None
                    domain = parsed.netloc or None
                except Exception:
                    pass

                url_records.append({
                    "url": visit.url,
                    "domain": domain,
                    "scheme": scheme,
                    "discovered_by": discovered_by,
                    "first_seen_utc": visit.visit_time_utc,
                    "last_seen_utc": visit.visit_time_utc,
                    "source_path": source_path,
                    "run_id": run_id,
                    "notes": json.dumps({
                        "browser": browser,
                        "profile": profile,
                        "title": visit.title,
                        "visit_type": visit.visit_type_label,
                    }),
                })

        except Exception as e:
            LOGGER.error("Failed to read history database %s: %s", db_path, e)
            return 0, 0, 0

        # Batch insert to browser_history table (per-visit records)
        if records:
            insert_browser_history_rows(evidence_conn, evidence_id, records)
            callbacks.on_log(f"Inserted {len(records)} history records from {browser}", "info")

        # Insert all URLs to urls table (no deduplication - one per visit)
        if url_records:
            insert_urls(evidence_conn, evidence_id, url_records)
            callbacks.on_log(f"Inserted {len(url_records)} URLs to urls table", "info")

        # Parse and insert search terms (Firefox 75+ metadata tables)
        search_term_records = []
        try:
            for query in parse_search_queries(db_path, warning_collector=warning_collector):
                search_term_records.append({
                    "evidence_id": evidence_id,
                    "term": query.term,
                    "normalized_term": query.normalized_term,
                    "url": query.url,
                    "browser": browser,
                    "profile": profile,
                    "search_engine": None,  # Firefox doesn't track which engine
                    "search_time_utc": query.search_time_utc,
                    "source_path": source_path,
                    "discovered_by": discovered_by,
                    "run_id": run_id,
                    "partition_index": partition_index,
                    "logical_path": source_path,
                    "notes": json.dumps({
                        "title": query.title,
                        "total_view_time_ms": query.total_view_time_ms,
                        "typing_time_ms": query.typing_time_ms,
                        "key_presses": query.key_presses,
                        "search_query_id": query.search_query_id,
                    }),
                })
        except Exception as e:
            LOGGER.warning("Failed to parse search queries from %s: %s", db_path, e)

        # Batch insert search terms
        if search_term_records:
            insert_search_terms(evidence_conn, evidence_id, search_term_records)
            callbacks.on_log(f"Inserted {len(search_term_records)} search terms from {browser}", "info")

        return len(records), len(url_records), len(search_term_records)
