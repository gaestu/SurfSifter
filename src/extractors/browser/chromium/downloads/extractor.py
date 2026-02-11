"""
Chromium Downloads Extractor

Extracts browser download history from all Chromium-based browsers (Chrome, Edge, Brave, Opera).
Uses shared patterns and parsers from the chromium family module.

Features:
- Chromium-only (Firefox/Safari excluded - use FirefoxDownloadsExtractor)
- Download state and danger type mapping
- URL chain extraction (redirect history)
- StatisticsCollector integration for run tracking
- Browser selection config widget (Chromium browsers only)

Download Schema:
- target_path, received_bytes, total_bytes
- start_time, end_time, last_access_time (WebKit timestamps)
- state (in_progress, complete, cancelled, interrupted)
- danger_type (not_dangerous, dangerous_file, dangerous_url, etc.)
- url_chain (list of URLs for redirect forensics)
- referrer, tab_url, mime_type

Note:
    Downloads are stored in the History database, not a separate file.
    This extractor reuses the History file but parses the downloads table.
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
from ...._shared.sqlite_helpers import safe_sqlite_connect, SQLiteReadError
from ...._shared.extraction_warnings import (
    ExtractionWarningCollector,
    discover_unknown_tables,
    track_unknown_values,
)
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    get_ewf_paths_from_evidence_fs,
)
from .._patterns import (
    CHROMIUM_BROWSERS,
    get_patterns,
    get_browser_display_name,
    get_all_browsers,
)
from .._parsers import (
    parse_downloads,
    get_download_stats,
    extract_profile_from_path,
    detect_browser_from_path,
)
from ._schemas import (
    KNOWN_DOWNLOADS_TABLES,
    DOWNLOADS_TABLE_PATTERNS,
    DOWNLOAD_STATE_MAP,
    DANGER_TYPE_MAP,
)
from core.logging import get_logger
from core.database.helpers.browser_downloads import delete_browser_downloads_by_run

LOGGER = get_logger("extractors.browser.chromium.downloads")


class ChromiumDownloadsExtractor(BaseExtractor):
    """
    Extract download history from Chromium-based browsers.

    Supports Chrome, Edge, Brave, Opera. All use identical SQLite schema.
    Firefox and Safari are handled by separate family extractors.

    Dual-phase workflow:
    - Extraction: Scans filesystem, copies History files to workspace
    - Ingestion: Parses SQLite databases (downloads table), inserts with forensic fields

    Features:
    - Download state and danger type classification
    - URL chain for redirect analysis (all URLs cross-posted to urls table)
    - WebKit timestamp conversion to ISO 8601
    - Multi-partition extraction support
    - Schema warning support for unknown tables/values
    - StatisticsCollector integration for run tracking
    - Browser selection config widget

    Added multi-partition extraction support
    Added schema warning support, removed URL deduplication
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="chromium_downloads",
            display_name="Chromium Downloads",
            description="Extract browser download history from Chrome, Edge, Brave, Opera",
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

        Uses BrowserConfigWidget with Chromium browsers and multi-partition option.
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
            status_text = f"Chromium Downloads\nFiles: {file_count}\nRun: {data.get('run_id', 'N/A')[:20]}"
        else:
            status_text = "Chromium Downloads\nNo extraction yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "chromium_downloads"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Chromium History files (for downloads) from evidence.

        Workflow:
            1. Generate run_id
            2. Scan evidence for Chromium History files (multi-partition if enabled)
            3. Copy matching files to output_dir/
            4. Calculate hashes, collect E01 context
            5. Write manifest.json

        Multi-partition support:
            When scan_all_partitions=True (default), uses file_list discovery to
            find History files across ALL partitions, not just the main partition.
            This captures browser artifacts from dual-boot systems, portable apps,
            and old OS installations.

        Note:
            Downloads are stored in the History database, so we extract
            the same file as the history extractor but parse different tables.
        """
        callbacks.on_step("Initializing Chromium downloads extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting Chromium downloads extraction (run_id=%s)", run_id)

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
            "schema_version": "2.1.0",
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

        # Scan for History files - use multi-partition if enabled and evidence_conn available
        callbacks.on_step("Scanning for Chromium History databases")

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

        callbacks.on_log(f"Found {len(all_history_files)} History database(s) across {len(files_by_partition)} partition(s)")

        if not all_history_files:
            LOGGER.info("No History files found")
        else:
            callbacks.on_progress(0, len(all_history_files), "Extracting History databases")

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
                                f"Copying {file_info['browser']} History (partition {partition_index})"
                            )

                            extracted = self._extract_file_from_info(
                                fs_to_use, file_info, output_dir, run_id
                            )
                            extracted["partition_index"] = partition_index
                            manifest_data["files"].append(extracted)
                            file_index += 1

                        except Exception as e:
                            error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            manifest_data["notes"].append(error_msg)
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
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

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
            "Chromium downloads extraction complete: %d files from %d partition(s), status=%s",
            len(manifest_data["files"]),
            len(manifest_data["partitions_with_artifacts"]),
            manifest_data["status"],
        )

        callbacks.on_step("Extraction complete")
        return manifest_data["status"] == "ok"

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, Any]:
        """
        Ingest extracted History files (downloads table) into evidence database.

        Workflow:
            1. Load manifest.json
            2. Delete previous run data (if re-running)
            3. For each extracted file:
               - Parse SQLite database (downloads table)
               - Detect unknown tables and enum values
               - Insert downloads with forensic context
               - Cross-post all URLs to unified urls table
            4. Flush schema warnings
            5. Return summary statistics
        """
        callbacks.on_step("Loading manifest")

        manifest_path = output_dir / "manifest.json"
        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", "unknown")
        evidence_label = config.get("evidence_label", "")

        # Continue statistics tracking
        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Delete previous downloads from this run (for re-ingestion)
        callbacks.on_step("Cleaning previous run data")
        try:
            delete_browser_downloads_by_run(evidence_conn, evidence_id, run_id)
        except Exception as e:
            LOGGER.debug("No previous run data to delete: %s", e)

        total_downloads = 0
        total_complete = 0
        total_dangerous = 0
        total_urls = 0
        failed_files = 0

        files = manifest_data.get("files", [])
        for i, file_info in enumerate(files):
            if callbacks.is_cancelled():
                break

            local_path = output_dir / file_info.get("local_filename", "")
            if not local_path.exists():
                callbacks.on_log(f"File not found: {local_path}", level="warning")
                failed_files += 1
                continue

            callbacks.on_progress(i + 1, len(files), f"Parsing {local_path.name}")

            try:
                counts = self._ingest_file(
                    local_path, evidence_conn, evidence_id, file_info, run_id, callbacks,
                    warning_collector=warning_collector,
                )
                total_downloads += counts["total"]
                total_complete += counts["complete"]
                total_dangerous += counts["dangerous"]
                total_urls += counts["urls_table"]
            except Exception as e:
                LOGGER.warning("Failed to ingest %s: %s", local_path, e)
                callbacks.on_log(f"Failed to parse {local_path.name}: {e}", level="warning")
                failed_files += 1
                if collector:
                    collector.report_failed(evidence_id, self.metadata.name, files=1)

        # Flush schema warnings to database
        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            LOGGER.info("Recorded %d extraction warnings", warning_count)
            callbacks.on_log(f"Schema discovery: {warning_count} warnings recorded", level="info")

        # Report ingestion stats
        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=total_downloads,
                downloads=total_downloads,
            )
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_step("Ingestion complete")

        return {
            "downloads": total_downloads,
            "complete": total_complete,
            "dangerous": total_dangerous,
            "urls_crossposted": total_urls,
            "failed_files": failed_files,
            "schema_warnings": warning_count,
        }

    # =========================================================================
    # Private helpers
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate unique run ID: timestamp + UUID4 prefix."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        return f"{ts}_{uid}"

    def _get_tool_version(self) -> str:
        """Get tool version string."""
        return f"{self.metadata.name} v{self.metadata.version}"

    def _get_e01_context(self, evidence_fs) -> Dict[str, Any]:
        """Extract E01 image context if available."""
        context = {"type": "unknown"}

        try:
            if hasattr(evidence_fs, "ewf_handle"):
                context["type"] = "ewf"
                try:
                    handle = evidence_fs.ewf_handle
                    if hasattr(handle, "get_media_size"):
                        media_size = handle.get_media_size()
                        if isinstance(media_size, int):
                            context["media_size"] = media_size
                except Exception:
                    pass
            elif hasattr(evidence_fs, "mount_point"):
                mount_point = getattr(evidence_fs, "mount_point", None)
                if isinstance(mount_point, (str, Path)):
                    context["type"] = "mounted"
                    context["mount_point"] = str(mount_point)
        except Exception:
            pass

        return context

    def _get_statistics_collector(self):
        """Get StatisticsCollector instance if available."""
        try:
            from core.statistics_collector import StatisticsCollector
            return StatisticsCollector.get_instance()
        except ImportError:
            return None

    def _discover_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """
        Scan evidence for Chromium History files (single partition fallback).

        Note: Downloads are in the History database, so we use "downloads"
        artifact patterns which point to History files.

        Args:
            evidence_fs: Mounted filesystem
            browsers: List of browser keys to search
            callbacks: Progress/log interface

        Returns:
            List of dicts with browser, profile, logical_path, etc.
        """
        found_files = []

        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                callbacks.on_log(f"Unknown browser: {browser}", "warning")
                continue

            # Use "downloads" patterns (which map to History files)
            patterns = get_patterns(browser, "downloads")

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        profile = extract_profile_from_path(path_str)
                        partition_index = getattr(evidence_fs, 'partition_index', 0)

                        found_files.append({
                            "logical_path": path_str,
                            "browser": browser,
                            "profile": profile,
                            "artifact_type": "downloads",
                            "display_name": get_browser_display_name(browser),
                            "partition_index": partition_index,
                        })

                        callbacks.on_log(f"Found {browser} History: {path_str}", "info")
                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return found_files

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, List[Dict]]:
        """
        Discover Chromium History files across ALL partitions using file_list.

        This method queries the pre-populated file_list table to find History
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
        # We look for "History" files in paths containing browser-specific strings
        path_patterns = []
        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                continue
            # Get patterns and convert to SQL LIKE patterns
            patterns = get_patterns(browser, "downloads")
            for pattern in patterns:
                # Convert glob to SQL-friendly pattern
                if "Chrome" in pattern:
                    path_patterns.append("%Google%Chrome%User Data%")
                elif "Edge" in pattern:
                    path_patterns.append("%Microsoft%Edge%User Data%")
                elif "Brave" in pattern:
                    path_patterns.append("%BraveSoftware%Brave-Browser%User Data%")
                elif "Opera" in pattern:
                    path_patterns.append("%Opera%")

        # Remove duplicates
        path_patterns = list(set(path_patterns))

        # Query file_list
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["History"],
            path_patterns=path_patterns if path_patterns else None,
        )

        if result.is_empty:
            callbacks.on_log(
                "No History files found in file_list, falling back to filesystem scan",
                "warning"
            )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        if result.is_multi_partition:
            callbacks.on_log(
                f"Found History files on {len(result.partitions_with_matches)} partitions: {result.partitions_with_matches}",
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
                    "browser": browser or "chromium",
                    "profile": profile,
                    "artifact_type": "downloads",
                    "display_name": get_browser_display_name(browser) if browser else "Chromium",
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

                callbacks.on_log(
                    f"Found {browser or 'chromium'} History on partition {partition_index}: {match.file_path}",
                    "info"
                )

            if files_list:
                files_by_partition[partition_index] = files_list

        return files_by_partition

    def _extract_file_from_info(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        run_id: str
    ) -> Dict[str, Any]:
        """
        Extract a single History file from evidence (dict-based API for multi-partition).

        Args:
            evidence_fs: Evidence filesystem handle
            file_info: Dict with logical_path, browser, profile, etc.
            output_dir: Output directory for extracted file
            run_id: Run ID for provenance

        Returns:
            File info dict for manifest
        """
        source_path = file_info["logical_path"]
        browser = file_info["browser"]
        profile = file_info.get("profile") or "Unknown"

        # Create output filename (guard against None profile)
        safe_profile = profile.replace(" ", "_").replace("/", "_")
        partition_index = file_info.get("partition_index", 0)
        filename = f"{browser}_{safe_profile}_p{partition_index}_History"
        dest_path = output_dir / filename

        # Read and write file
        file_content = evidence_fs.read_file(source_path)
        dest_path.write_bytes(file_content)

        # Calculate hashes
        md5_hash = hashlib.md5(file_content).hexdigest()
        sha256_hash = hashlib.sha256(file_content).hexdigest()
        size = len(file_content)

        # Copy companion files (WAL, journal, shm) for SQLite recovery
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
            except Exception:
                pass  # Companion doesn't exist

        return {
            "copy_status": "ok",
            "source_path": source_path,
            "local_filename": filename,
            "extracted_path": str(dest_path),
            "browser": browser,
            "profile": profile,
            "logical_path": source_path,
            "artifact_type": "downloads",
            "md5": md5_hash,
            "sha256": sha256_hash,
            "size_bytes": size,
            "companion_files": companion_files,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

    def _extract_file(
        self,
        evidence_fs,
        source_path: str,
        output_dir: Path,
        browser: str,
        run_id: str
    ) -> Dict[str, Any]:
        """
        Extract a single History file from evidence (legacy tuple-based API).

        Returns:
            File info dict for manifest
        """
        # Generate safe local filename
        safe_name = source_path.replace("/", "_").replace("\\", "_")
        local_filename = f"{browser}_{safe_name}"
        local_path = output_dir / local_filename

        # Copy file from evidence using read_file
        file_content = evidence_fs.read_file(source_path)
        local_path.write_bytes(file_content)

        # Copy companion files (WAL, journal, shm) for SQLite recovery
        for suffix in ["-wal", "-journal", "-shm"]:
            companion_path = source_path + suffix
            try:
                companion_content = evidence_fs.read_file(companion_path)
                companion_dest = Path(str(local_path) + suffix)
                companion_dest.write_bytes(companion_content)
            except Exception:
                pass  # Companion doesn't exist

        # Calculate hash
        md5_hash = hashlib.md5(file_content).hexdigest()
        sha256_hash = hashlib.sha256(file_content).hexdigest()

        # Get profile from path
        profile = extract_profile_from_path(source_path)

        return {
            "source_path": source_path,
            "local_filename": local_filename,
            "browser": browser,
            "profile": profile,
            "md5": md5_hash,
            "sha256": sha256_hash,
            "size_bytes": local_path.stat().st_size,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

    def _ingest_file(
        self,
        local_path: Path,
        evidence_conn,
        evidence_id: int,
        file_info: Dict[str, Any],
        run_id: str,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, int]:
        """
        Ingest downloads from a single History database.

        Args:
            local_path: Path to extracted History database
            evidence_conn: Evidence database connection
            evidence_id: Evidence ID
            file_info: File metadata from manifest
            run_id: Run ID for provenance
            callbacks: Extractor callbacks
            warning_collector: Optional collector for schema warnings

        Returns:
            Dict with counts: total, complete, dangerous, urls_table
        """
        from urllib.parse import urlparse
        from core.database import insert_browser_download_row, insert_urls

        browser = file_info.get("browser", "unknown")
        profile = file_info.get("profile", "Default")
        source_path = file_info.get("source_path", "")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        counts = {"total": 0, "complete": 0, "dangerous": 0, "urls_table": 0}
        url_records = []  # Collect ALL URLs for unified urls table (no deduplication)
        found_states = set()  # Track state values for schema warnings
        found_danger_types = set()  # Track danger_type values for schema warnings

        with safe_sqlite_connect(local_path) as conn:
            # Discover unknown tables (schema warnings)
            if warning_collector:
                try:
                    unknown_tables = discover_unknown_tables(
                        conn, KNOWN_DOWNLOADS_TABLES, DOWNLOADS_TABLE_PATTERNS
                    )
                    for table_info in unknown_tables:
                        warning_collector.add_unknown_table(
                            table_name=table_info["name"],
                            columns=table_info["columns"],
                            source_file=source_path,
                            artifact_type="downloads",
                        )
                except Exception as e:
                    LOGGER.debug("Failed to discover unknown tables: %s", e)

            for download in parse_downloads(conn):
                try:
                    # Track state and danger_type for schema warnings
                    # Note: parse_downloads returns string values, we need raw int from _parsers
                    # For now, track the mapped string values
                    found_states.add(download.state)
                    found_danger_types.add(download.danger_type)

                    # Extract filename from target_path
                    target_path = download.target_path
                    filename = target_path.split("/")[-1].split("\\")[-1] if target_path else ""

                    # Get primary URL from chain (last URL is the final destination)
                    # If no chain, leave as empty string
                    url = download.url_chain[-1] if download.url_chain else ""

                    insert_browser_download_row(
                        evidence_conn,
                        evidence_id,
                        browser,
                        url,
                        profile=profile,
                        target_path=target_path,
                        filename=filename,
                        start_time_utc=download.start_time_iso,
                        end_time_utc=download.end_time_iso,
                        received_bytes=download.received_bytes,
                        total_bytes=download.total_bytes,
                        state=download.state,
                        danger_type=download.danger_type,
                        opened=1 if download.opened else 0,
                        referrer=download.referrer,
                        mime_type=download.mime_type,
                        source_path=source_path,
                        discovered_by=discovered_by,
                        run_id=run_id,
                    )
                    counts["total"] += 1
                    if download.state == "complete":
                        counts["complete"] += 1
                    if download.danger_type != "not_dangerous":
                        counts["dangerous"] += 1

                    # Collect ALL URLs for unified urls table (no deduplication here)
                    # The urls table/UI handles deduplication - we want forensic completeness
                    for chain_url in (download.url_chain or []):
                        if chain_url and not chain_url.startswith(("javascript:", "data:")):
                            parsed = urlparse(chain_url)
                            url_records.append({
                                "url": chain_url,
                                "domain": parsed.netloc or None,
                                "scheme": parsed.scheme or None,
                                "discovered_by": discovered_by,
                                "run_id": run_id,
                                "source_path": source_path,
                                "context": f"download:{browser}:{profile}",
                                "first_seen_utc": download.start_time_iso,
                            })
                except Exception as e:
                    LOGGER.debug("Failed to insert download: %s", e)

        # Report unknown state/danger_type values (those with "unknown_" prefix)
        if warning_collector:
            for state in found_states:
                if state.startswith("unknown_"):
                    warning_collector.add_unknown_token_type(
                        token_type=state,
                        source_file=source_path,
                        artifact_type="downloads",
                    )
            for danger_type in found_danger_types:
                if danger_type.startswith("unknown_"):
                    warning_collector.add_unknown_token_type(
                        token_type=danger_type,
                        source_file=source_path,
                        artifact_type="downloads",
                    )

        # Cross-post URLs to unified urls table for analysis
        if url_records:
            try:
                insert_urls(evidence_conn, evidence_id, url_records)
                counts["urls_table"] = len(url_records)
                LOGGER.debug("Cross-posted %d download URLs to urls table", len(url_records))
            except Exception as e:
                LOGGER.debug("Failed to cross-post download URLs: %s", e)

        return counts
