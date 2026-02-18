"""
Chromium Sessions Extractor

Extracts and ingests browser session/tab data from Chromium browsers using SNSS format.

Features:
- Full SNSS binary format parsing using command-based decoding
- Complete URL extraction (all schemes: http, https, file, chrome, data, etc.)
- Page title extraction from navigation entries
- Timestamp extraction with Windows epoch conversion
- Referrer URL and transition type tracking
- Window and tab state reconstruction
- StatisticsCollector integration for run tracking
- Per-tab navigation history preservation (session_tab_history table)
- Support for both legacy (Chrome < 100) and new (Chrome 100+) session paths
- Multi-partition extraction support via file_list discovery
- Schema warnings for unknown SNSS versions, encrypted files, parse errors
- Hash-based filename uniqueness to prevent overwrites

Data Format:
- Chromium stores sessions in SNSS binary format
- Signature: 0x53534E53 ("SSNS")
- File versions 1-4 supported (encrypted versions detected)
- Command-based structure with pickle serialization
- Navigation entries contain: URL, title, timestamp, referrer, transition

Session File Locations:
- Legacy (Chrome < 100): Default/Current Session, Default/Last Session, etc.
- Chrome 100+: Default/Sessions/Session_<timestamp>, Default/Sessions/Tabs_<timestamp>

Forensic Value:
- Session files may persist after browser crash or ungraceful shutdown
- Last Session contains tabs from previous session (valuable for timeline)
- Page titles reveal content even without full page recovery
- Timestamps enable activity correlation
- Non-HTTP URLs (file://, chrome://) reveal local file access patterns
- Referrer URLs show navigation paths
- Back/forward history per-tab preserves full browsing sequence

Database Tables:
- session_tabs: Current tab state (one record per tab with current URL)
- session_tab_history: Full navigation history per tab (preserves back/forward)
- urls: Cross-posted URL events for unified analysis (each visit is a distinct event)
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
from .snss_parser import parse_snss_data, SNSSParseResult
from ....callbacks import ExtractorCallbacks
from ....widgets import BrowserConfigWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
    check_file_list_available,
    glob_to_sql_like,
)
from ...._shared.extraction_warnings import (
    ExtractionWarningCollector,
    CATEGORY_BINARY,
    SEVERITY_INFO,
    SEVERITY_WARNING,
    SEVERITY_ERROR,
    WARNING_TYPE_BINARY_FORMAT_ERROR,
    WARNING_TYPE_VERSION_UNSUPPORTED,
)
from .._patterns import CHROMIUM_BROWSERS, get_artifact_patterns, get_browser_display_name
from .._parsers import detect_browser_from_path, extract_profile_from_path
from .._embedded_discovery import (
    discover_artifacts_with_embedded_roots,
    get_embedded_root_paths,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_session_windows,
    insert_session_tabs,
    insert_session_tab_histories,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    delete_sessions_by_run,
    insert_urls,
)

LOGGER = get_logger("extractors.browser.chromium.sessions")


class ChromiumSessionsExtractor(BaseExtractor):
    """
    Extract browser session/tab data from Chromium SNSS files.

    Parses Session Restore, Current Session, Last Session files.
    Supports Chrome, Edge, Brave, Opera.

    Uses proper SNSS command parsing to extract URLs (all schemes),
    titles, timestamps, and navigation metadata.
    """

    SUPPORTED_BROWSERS = list(CHROMIUM_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="chromium_sessions",
            display_name="Chromium Session Restore",
            description="Extract session tabs with URLs, titles, and timestamps from Chrome/Edge/Opera/Brave SNSS files",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted. Please mount E01 image first."
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
        """Return configuration widget (browser selection + multi-partition)."""
        return BrowserConfigWidget(
            parent,
            supported_browsers=self.SUPPORTED_BROWSERS,
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
            status_text = f"Chromium Sessions\nFiles extracted: {file_count}\nRun ID: {data.get('run_id', 'N/A')}"
        else:
            status_text = "Chromium Sessions\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "chromium_sessions"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Chromium session files from evidence.

        Supports multi-partition extraction via file_list discovery when available.
        Falls back to single-partition iter_paths() when file_list is empty.
        """
        callbacks.on_step("Initializing Chromium sessions extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")
        scan_all_partitions = config.get("scan_all_partitions", True)

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Chromium sessions extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_extraction_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "multi_partition_extraction": scan_all_partitions,
            "partitions_scanned": [],
            "partitions_with_artifacts": [],
            "files": [],
            "status": "ok",
            "notes": [],
        }

        callbacks.on_step("Scanning for Chromium session files")

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        browsers_to_search = [b for b in browsers_to_search if b in self.SUPPORTED_BROWSERS]

        # Discover files - use multi-partition if enabled and evidence_conn available
        files_by_partition: Dict[int, List[Dict]] = {}

        if scan_all_partitions and evidence_conn is not None:
            # Multi-partition discovery via file_list
            files_by_partition = self._discover_files_multi_partition(
                evidence_fs, evidence_conn, evidence_id, browsers_to_search, callbacks
            )
        else:
            # Single partition fallback
            if scan_all_partitions and evidence_conn is None:
                callbacks.on_log(
                    "Multi-partition scan requested but no evidence_conn provided, using single partition",
                    "warning"
                )
            session_files = self._discover_session_files(evidence_fs, browsers_to_search, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if session_files:
                files_by_partition[partition_index] = session_files

        # Flatten for counting
        all_session_files = []
        for files_list in files_by_partition.values():
            all_session_files.extend(files_list)

        # Update manifest with partition info
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = sorted(
            p for p, files in files_by_partition.items() if files
        )

        # Report discovered files (always, even if 0)
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(all_session_files))

        callbacks.on_log(
            f"Found {len(all_session_files)} session file(s) across {len(files_by_partition)} partition(s)"
        )

        if not all_session_files:
            LOGGER.info("No Chromium session files found")
        else:
            callbacks.on_progress(0, len(all_session_files), "Copying session files")

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
                                file_index + 1, len(all_session_files),
                                f"Copying {file_info['browser']} session (partition {partition_index})"
                            )

                            extracted_file = self._extract_file(
                                fs_to_use,
                                file_info,
                                output_dir,
                                partition_index,
                                callbacks,
                            )
                            manifest_data["files"].append(extracted_file)
                            file_index += 1

                        except Exception as e:
                            error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            manifest_data["notes"].append(error_msg)
                            if stats:
                                stats.report_failed(evidence_id, self.metadata.name, files=1)
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

        # Finish statistics (once, at the end)
        if stats:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            stats.finish_run(evidence_id, self.metadata.name, status=status)

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
            "Chromium sessions extraction complete: %d files, status=%s",
            len(manifest_data["files"]),
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
        """Parse extracted SNSS files and ingest into database.

        Includes schema warning collection for:
        - Unknown SNSS file versions
        - Encrypted session files
        - Parse errors
        - Unknown command IDs
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"windows": 0, "tabs": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])

        # Continue statistics tracking (same run_id from manifest)
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0,
                    tabs=0,
                )
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"windows": 0, "tabs": 0}

        total_windows = 0
        total_tabs = 0

        # Accumulator for URL records across all session files (no deduplication)
        all_url_data: List[Dict] = []

        # Clear previous data for this run
        deleted = delete_sessions_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d session records from previous run %s", deleted, run_id)

        callbacks.on_progress(0, len(files), "Parsing session files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            if file_entry.get("copy_status") == "error":
                callbacks.on_log(f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}", "warning")
                continue

            callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry['browser']} session")

            try:
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry["browser"],
                    artifact_type="sessions",
                    run_id=run_id,
                    extracted_path=file_entry["extracted_path"],
                    extraction_status="ok",
                    extraction_timestamp_utc=manifest_data["extraction_timestamp_utc"],
                    logical_path=file_entry["logical_path"],
                    profile=file_entry.get("profile"),
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    forensic_path=file_entry.get("forensic_path"),
                    extraction_tool=manifest_data.get("extraction_tool"),
                    file_size_bytes=file_entry.get("file_size_bytes"),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                )

                db_path = Path(file_entry["extracted_path"])
                if not db_path.is_absolute():
                    # Try local_filename first (new format), then extracted_path
                    local_filename = file_entry.get("local_filename")
                    if local_filename:
                        db_path = output_dir / local_filename
                    else:
                        db_path = output_dir / db_path.name

                counts, file_url_data = self._parse_snss_file(
                    db_path,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                    callbacks,
                    warning_collector=warning_collector,
                )

                total_windows += counts.get("windows", 0)
                total_tabs += counts.get("tabs", 0)

                # Accumulate URL data (no deduplication - all events preserved)
                all_url_data.extend(file_url_data)

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=counts.get("tabs", 0),
                )

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry['extracted_path']}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")

                if 'inventory_id' in locals():
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

        # Cross-post all session URLs to unified urls table
        # Each URL+timestamp is a distinct forensic event for timeline reconstruction
        if all_url_data:
            try:
                insert_urls(evidence_conn, evidence_id, all_url_data)
                evidence_conn.commit()
                LOGGER.info("Cross-posted %d session URL events to urls table", len(all_url_data))
            except Exception as e:
                LOGGER.debug("Failed to cross-post session URLs: %s", e)

        # Report ingested counts and finish
        if stats:
            stats.report_ingested(
                evidence_id, self.metadata.name,
                records=total_tabs,
                tabs=total_tabs,
            )
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {"windows": total_windows, "tabs": total_tabs}

    # ─────────────────────────────────────────────────────────────────
    # Helper Methods
    # ─────────────────────────────────────────────────────────────────

    def _generate_run_id(self) -> str:
        """Generate run ID: sess_chromium_{timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"sess_chromium_{timestamp}_{unique_id}"

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

    def _get_extraction_tool_version(self) -> str:
        """Build extraction tool version string."""
        try:
            import pytsk3
            pytsk_version = pytsk3.TSK_VERSION_STR
        except ImportError:
            pytsk_version = "unknown"

        return f"pytsk3:{pytsk_version}"

    def _discover_session_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """Scan evidence for Chromium session files (single partition)."""
        session_files = []

        for browser_key in browsers:
            if browser_key not in CHROMIUM_BROWSERS:
                continue

            patterns = get_artifact_patterns(browser_key, "sessions")
            display_name = CHROMIUM_BROWSERS[browser_key]["display_name"]

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        profile = extract_profile_from_path(path_str) or "Default"
                        file_type = self._classify_session_file(path_str)

                        session_files.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "profile": profile,
                            "file_type": file_type,
                            "artifact_type": "sessions",
                            "display_name": display_name,
                        })

                        callbacks.on_log(f"Found {browser_key} session: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return session_files

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> Dict[int, List[Dict]]:
        """
        Discover session files across ALL partitions using file_list.

        This method queries the pre-populated file_list table to find session
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
            files = self._discover_session_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")

        path_patterns = set()
        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                continue
            for pattern in get_artifact_patterns(browser, "sessions"):
                path_patterns.add(glob_to_sql_like(pattern))

        result, embedded_roots = discover_artifacts_with_embedded_roots(
            evidence_conn,
            evidence_id,
            artifact="sessions",
            filename_patterns=[
                "Current Session",
                "Last Session",
                "Current Tabs",
                "Last Tabs",
                "Session_*",
                "Tabs_*",
            ],
            path_patterns=sorted(path_patterns) if path_patterns else None,
        )

        if result.is_empty:
            callbacks.on_log(
                "No session files found in file_list, falling back to filesystem scan",
                "warning"
            )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_session_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        if result.is_multi_partition:
            callbacks.on_log(
                f"Found session files on {len(result.partitions_with_matches)} partitions: {result.partitions_with_matches}",
                "info"
            )

        # Convert FileListMatch objects to extractor's expected format
        files_by_partition: Dict[int, List[Dict]] = {}

        for partition_index, matches in result.matches_by_partition.items():
            files_list = []
            for match in matches:
                # Detect browser from path
                embedded_paths = get_embedded_root_paths(embedded_roots, partition_index)
                browser = detect_browser_from_path(match.file_path, embedded_roots=embedded_paths)
                if browser and browser not in browsers and browser != "chromium_embedded":
                    continue  # Skip if browser not in selection

                profile = extract_profile_from_path(match.file_path) or "Default"
                display_name = (
                    get_browser_display_name(browser)
                    if browser in CHROMIUM_BROWSERS
                    else "Embedded Chromium"
                )

                file_type = self._classify_session_file(match.file_path)

                files_list.append({
                    "logical_path": match.file_path,
                    "browser": browser or "chromium",
                    "profile": profile,
                    "artifact_type": "sessions",
                    "file_type": file_type,
                    "display_name": display_name,
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

                callbacks.on_log(
                    f"Found {browser or 'chromium'} session on partition {partition_index}: {match.file_path}",
                    "info"
                )

            if files_list:
                files_by_partition[partition_index] = files_list

        return files_by_partition

        return session_files

    def _classify_session_file(self, path: str) -> str:
        """Classify session file type based on filename.

        Handles both legacy and Chrome 100+ naming:
        - Legacy: 'Current Session', 'Last Session', 'Current Tabs', 'Last Tabs'
        - Chrome 100+: 'Sessions/Session_<timestamp>', 'Sessions/Tabs_<timestamp>'
        """
        filename = path.split('/')[-1].lower()

        # Legacy format (Chrome < 100)
        if "current session" in filename:
            return "current_session"
        elif "last session" in filename:
            return "last_session"
        elif "current tabs" in filename:
            return "current_tabs"
        elif "last tabs" in filename:
            return "last_tabs"
        elif "session restore" in filename:
            return "session_restore"
        # Chrome 100+ format (timestamped files in Sessions/ directory)
        elif filename.startswith("session_"):
            return "session_timestamped"
        elif filename.startswith("tabs_"):
            return "tabs_timestamped"
        else:
            return "unknown"

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        partition_index: int,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Copy file from evidence to workspace and collect metadata.

        Uses a hash suffix to ensure uniqueness when multiple files have the same
        browser/profile/type combination (e.g., legacy "Current Session" files from
        different partitions or paths).
        """
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info.get("profile", "Default")
            file_type = file_info.get("file_type", "session")

            safe_profile = profile.replace(' ', '_').replace('/', '_')

            # Read file content first (needed for hash anyway)
            file_content = evidence_fs.read_file(source_path)

            # Compute hashes
            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()
            size = len(file_content)

            # Build unique filename with:
            # - browser, profile, file_type as base
            # - partition index for multi-partition differentiation
            # - timestamp suffix if present in original filename (Chrome 100+)
            # - short hash suffix (first 8 chars of MD5) for absolute uniqueness
            source_filename = source_path.split('/')[-1]
            timestamp_suffix = ""
            if '_' in source_filename and any(c.isdigit() for c in source_filename):
                # Extract timestamp part (e.g., "Session_13353533606528067" -> "13353533606528067")
                parts = source_filename.split('_', 1)
                if len(parts) > 1 and parts[1]:
                    timestamp_suffix = f"_{parts[1]}"

            # Include partition index and hash suffix for uniqueness
            hash_suffix = md5[:8]
            filename = f"{browser}_{safe_profile}_{file_type}_p{partition_index}{timestamp_suffix}_{hash_suffix}"
            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

            dest_path.write_bytes(file_content)

            return {
                "copy_status": "ok",
                "size_bytes": size,
                "file_size_bytes": size,
                "md5": md5,
                "sha256": sha256,
                "extracted_path": str(dest_path),
                "local_filename": filename,
                "browser": browser,
                "profile": profile,
                "file_type": file_type,
                "logical_path": source_path,
                "artifact_type": "sessions",
                "partition_index": partition_index,
            }

        except Exception as e:
            callbacks.on_log(f"Failed to extract {file_info['logical_path']}: {e}", "error")
            return {
                "copy_status": "error",
                "size_bytes": 0,
                "file_size_bytes": 0,
                "md5": None,
                "sha256": None,
                "extracted_path": None,
                "local_filename": None,
                "browser": file_info.get("browser"),
                "profile": file_info.get("profile"),
                "file_type": file_info.get("file_type"),
                "logical_path": file_info.get("logical_path"),
                "partition_index": partition_index,
                "error_message": str(e),
            }

    def _parse_snss_file(
        self,
        file_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> tuple[Dict[str, int], List[Dict]]:
        """
        Parse Chromium SNSS session file using proper command-based parsing.

        Extracts:
        - URLs (all schemes, not just http/https)
        - Page titles
        - Timestamps
        - Tab/window structure

        Also reports schema warnings for:
        - Unknown SNSS versions
        - Encrypted files
        - Parse errors
        - Unknown command IDs

        Returns:
            Tuple of (counts dict, list of URL data dicts for cross-posting)
        """
        counts = {"windows": 0, "tabs": 0}
        url_data_for_aggregation: List[Dict] = []
        source_file = file_entry.get("logical_path", str(file_path))

        try:
            data = file_path.read_bytes()
        except Exception as e:
            LOGGER.error("Failed to read SNSS file: %s", e)
            if warning_collector:
                warning_collector.add_warning(
                    warning_type=WARNING_TYPE_BINARY_FORMAT_ERROR,
                    category=CATEGORY_BINARY,
                    severity=SEVERITY_ERROR,
                    artifact_type="sessions",
                    source_file=source_file,
                    item_name="file_read_error",
                    item_value=str(e),
                )
            return counts, url_data_for_aggregation

        # Parse using the proper SNSS parser (now returns extended info)
        parse_result = parse_snss_data(data)

        # Report schema warnings from parse result
        if warning_collector:
            # Report encryption (if detected)
            if parse_result.is_encrypted:
                warning_collector.add_warning(
                    warning_type=WARNING_TYPE_VERSION_UNSUPPORTED,
                    category=CATEGORY_BINARY,
                    severity=SEVERITY_WARNING,
                    artifact_type="sessions",
                    source_file=source_file,
                    item_name="encrypted_session",
                    item_value=f"SNSS version {parse_result.version} (encrypted)",
                    context_json={"version": parse_result.version, "encrypted": True},
                )

            # Report unknown/unsupported versions
            if not parse_result.is_valid and "version" in str(parse_result.errors).lower():
                warning_collector.add_warning(
                    warning_type=WARNING_TYPE_VERSION_UNSUPPORTED,
                    category=CATEGORY_BINARY,
                    severity=SEVERITY_WARNING,
                    artifact_type="sessions",
                    source_file=source_file,
                    item_name="unknown_snss_version",
                    item_value=str(parse_result.version),
                )

            # Report parse errors
            for error in parse_result.errors:
                warning_collector.add_warning(
                    warning_type=WARNING_TYPE_BINARY_FORMAT_ERROR,
                    category=CATEGORY_BINARY,
                    severity=SEVERITY_ERROR if "invalid" in error.lower() else SEVERITY_WARNING,
                    artifact_type="sessions",
                    source_file=source_file,
                    item_name="snss_parse_error",
                    item_value=error,
                )

            # Report unknown command IDs (if parser collected them)
            if hasattr(parse_result, 'unknown_commands') and parse_result.unknown_commands:
                warning_collector.add_warning(
                    warning_type="unknown_command_id",
                    category=CATEGORY_BINARY,
                    severity=SEVERITY_INFO,
                    artifact_type="sessions",
                    source_file=source_file,
                    item_name="unknown_snss_commands",
                    item_value=str(sorted(parse_result.unknown_commands)),
                    context_json={"command_ids": sorted(parse_result.unknown_commands)},
                )

        if not parse_result.is_valid:
            for error in parse_result.errors:
                LOGGER.warning("SNSS parse error in %s: %s", file_path, error)
            return counts, url_data_for_aggregation

        browser = file_entry["browser"]
        profile = file_entry.get("profile", "Default")
        file_type = file_entry.get("file_type", "session")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        LOGGER.debug(
            "SNSS parse result for %s: %d commands, %d tabs, %d windows, %d navigation entries",
            file_path.name,
            parse_result.total_commands,
            len(parse_result.tabs),
            len(parse_result.windows),
            len(parse_result.navigation_entries),
        )

        # Create window records from parsed windows
        window_records = []
        for window in parse_result.windows:
            window_record = {
                "browser": browser,
                "profile": profile,
                "window_id": window.window_id,
                "selected_tab_index": window.selected_tab_index,
                "window_type": "normal",  # Could be enhanced with window.window_type
                "session_type": file_type,
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry["logical_path"],
                "forensic_path": file_entry.get("forensic_path"),
            }
            window_records.append(window_record)

        # If no windows were parsed but we have tabs, create a synthetic window
        if not window_records and parse_result.tabs:
            window_records.append({
                "browser": browser,
                "profile": profile,
                "window_id": 0,
                "selected_tab_index": 0,
                "window_type": "normal",
                "session_type": file_type,
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry["logical_path"],
                "forensic_path": file_entry.get("forensic_path"),
            })

        if window_records:
            counts["windows"] = insert_session_windows(evidence_conn, evidence_id, window_records)

        # Create tab records from parsed tabs (current tab state only)
        tab_records = []
        history_records = []  # Full navigation history per tab

        for tab in parse_result.tabs:
            # Get the current navigation entry (or last one if index is out of bounds)
            current_nav = None
            if tab.navigations:
                nav_idx = min(tab.current_navigation_index, len(tab.navigations) - 1)
                if nav_idx >= 0:
                    current_nav = tab.navigations[nav_idx]

            # Get URL and title from current navigation
            url = current_nav.url if current_nav else ""
            title = current_nav.title if current_nav else ""

            # Get timestamp - prefer navigation timestamp, fall back to last_active_time
            last_accessed = None
            if current_nav and current_nav.timestamp:
                last_accessed = current_nav.timestamp.isoformat()
            elif tab.last_active_time:
                last_accessed = tab.last_active_time.isoformat()

            # Skip tabs without URLs
            if not url:
                continue

            # Insert current tab state into session_tabs
            tab_record = {
                "browser": browser,
                "profile": profile,
                "window_id": tab.window_id,
                "tab_index": tab.index_in_window,
                "url": url,
                "title": title,
                "pinned": 1 if tab.pinned else 0,
                "group_id": tab.group_id,
                "last_accessed_utc": last_accessed,
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry["logical_path"],
                "forensic_path": file_entry.get("forensic_path"),
            }
            tab_records.append(tab_record)

            # Insert ALL navigation entries into session_tab_history
            # This preserves back/forward history per-tab without cross-tab deduplication
            for nav_idx, nav in enumerate(tab.navigations):
                if not nav.url:
                    continue

                # Map transition_type integer to descriptive string
                transition_str = self._map_transition_type(nav.transition_type)

                history_record = {
                    "browser": browser,
                    "profile": profile,
                    "tab_id": None,  # Will be resolved after tab insert
                    "_window_id": tab.window_id,  # Temp field for FK resolution
                    "_tab_index": tab.index_in_window,  # Temp field for FK resolution
                    "nav_index": nav_idx,
                    "url": nav.url,
                    "title": nav.title,
                    "transition_type": transition_str,
                    "timestamp_utc": nav.timestamp.isoformat() if nav.timestamp else None,
                    # Forensic metadata
                    "referrer_url": nav.referrer_url or None,
                    "original_request_url": nav.original_request_url or None,
                    "has_post_data": 1 if nav.has_post_data else 0,
                    "http_status_code": nav.http_status_code if nav.http_status_code else None,
                    # Provenance
                    "run_id": run_id,
                    "source_path": file_entry["logical_path"],
                    "discovered_by": discovered_by,
                    "partition_index": file_entry.get("partition_index"),
                    "fs_type": file_entry.get("fs_type"),
                    "logical_path": file_entry["logical_path"],
                    "forensic_path": file_entry.get("forensic_path"),
                }
                history_records.append(history_record)

        if tab_records:
            counts["tabs"] = insert_session_tabs(evidence_conn, evidence_id, tab_records)

            # Resolve tab_ids for history records (FK to session_tabs)
            if history_records:
                cursor = evidence_conn.execute(
                    "SELECT id, window_id, tab_index FROM session_tabs WHERE evidence_id = ? AND run_id = ?",
                    (evidence_id, run_id)
                )
                tab_id_map = {(row[1], row[2]): row[0] for row in cursor.fetchall()}

                for hr in history_records:
                    key = (hr.pop("_window_id", None), hr.pop("_tab_index", None))
                    hr["tab_id"] = tab_id_map.get(key)

        if history_records:
            counts["history"] = insert_session_tab_histories(evidence_conn, evidence_id, history_records)

        # Collect URL data for aggregation (deduplication happens at run_ingestion level)
        url_data_for_aggregation = self._collect_url_data(
            tab_records, history_records, browser, profile, run_id, discovered_by, file_entry
        )

        return counts, url_data_for_aggregation

    def _map_transition_type(self, transition: int) -> str:
        """Map Chromium transition type integer to descriptive string.

        See: ui/base/page_transition_types.h in Chromium source.
        """
        # Core transition types (lower 8 bits)
        core_type = transition & 0xFF
        type_map = {
            0: "link",           # LINK - user clicked a link
            1: "typed",          # TYPED - user typed URL
            2: "auto_bookmark",  # AUTO_BOOKMARK - from bookmark/home
            3: "auto_subframe",  # AUTO_SUBFRAME - subframe navigation
            4: "manual_subframe",# MANUAL_SUBFRAME - user clicked in subframe
            5: "generated",      # GENERATED - keyword-generated URL
            6: "auto_toplevel",  # AUTO_TOPLEVEL - automatic (e.g., meta refresh)
            7: "form_submit",    # FORM_SUBMIT - form submission
            8: "reload",         # RELOAD - page reload
            9: "keyword",        # KEYWORD - omnibox keyword
            10: "keyword_generated", # KEYWORD_GENERATED
        }
        return type_map.get(core_type, f"unknown:{transition}")

    def _collect_url_data(
        self,
        tab_records: List[Dict],
        history_records: List[Dict],
        browser: str,
        profile: str,
        run_id: str,
        discovered_by: str,
        file_entry: Dict
    ) -> List[Dict]:
        """Collect URL data from session file for insertion to urls table.

        Returns all URL records with timestamps - no deduplication.
        Each URL+timestamp represents a distinct visit event.
        Deduplication/aggregation can be done at query or report level.
        """
        from urllib.parse import urlparse

        url_data = []

        # Collect from current tabs
        for tab in tab_records:
            url = tab.get("url", "")
            if not url or url.startswith(("about:", "chrome:", "chrome-extension:", "javascript:", "data:")):
                continue

            parsed = urlparse(url)
            url_data.append({
                "url": url,
                "domain": parsed.netloc or None,
                "scheme": parsed.scheme or None,
                "discovered_by": discovered_by,
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "context": f"session:{browser}:{profile}",
                "first_seen_utc": tab.get("last_accessed_utc"),
            })

        # Collect from navigation history
        for hr in history_records:
            url = hr.get("url", "")
            if not url or url.startswith(("about:", "chrome:", "chrome-extension:", "javascript:", "data:")):
                continue

            parsed = urlparse(url)
            url_data.append({
                "url": url,
                "domain": parsed.netloc or None,
                "scheme": parsed.scheme or None,
                "discovered_by": discovered_by,
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "context": f"session:{browser}:{profile}",
                "first_seen_utc": hr.get("timestamp_utc"),
            })

        return url_data
