"""
Chromium History Extractor

Extracts browser history from all Chromium-based browsers (Chrome, Edge, Brave, Opera).
Uses shared patterns and parsers from the chromium family module.

Features:
- Per-visit record extraction (visits table, not just URLs)
- Keyword search terms extraction (keyword_search_terms table, )
- WebKit timestamp conversion to ISO 8601
- Transition type decoding (raw + human-readable, )
- StatisticsCollector integration
- WAL/journal file copying for SQLite recovery
- Forensic provenance (run_id, source_path, partition context)
- Multi-partition discovery - scans all partitions via file_list
- Schema warning support - detects unknown tables/columns/transitions
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Set

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import BrowserConfigWidget
from ...._shared.timestamps import webkit_to_iso
from ...._shared.sqlite_helpers import safe_sqlite_connect, SQLiteReadError
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    get_ewf_paths_from_evidence_fs,
    open_partition_for_extraction,
    FileListDiscoveryResult,
    glob_to_sql_like,
)
from ...._shared.extraction_warnings import (
    ExtractionWarningCollector,
    discover_unknown_tables,
    discover_unknown_columns,
    track_unknown_values,
)
from .._patterns import (
    CHROMIUM_BROWSERS,
    get_patterns,
    get_artifact_patterns,
    get_browser_display_name,
    get_all_browsers,
)
from .._embedded_discovery import (
    discover_artifacts_with_embedded_roots,
    get_embedded_root_paths,
)
from .._parsers import (
    parse_history_visits,
    parse_keyword_search_terms,
    extract_profile_from_path,
    detect_browser_from_path,
    get_history_stats,
)
from ._schemas import (
    KNOWN_HISTORY_TABLES,
    HISTORY_TABLE_PATTERNS,
    KNOWN_VISITS_COLUMNS,
    TRANSITION_CORE_TYPES,
    decode_transition_type,
    get_transition_core_name,
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


LOGGER = get_logger("extractors.browser.chromium.history")


class ChromiumHistoryExtractor(BaseExtractor):
    """
    Extract browser history from Chromium-based browsers.

    Supports: Chrome, Edge, Brave, Opera

    Dual-phase workflow:
    - Extraction: Scans filesystem, copies History files to workspace
    - Ingestion: Parses SQLite databases, inserts with forensic fields

    Features:
    - Per-visit record extraction (not just URL aggregates)
    - Keyword search terms extraction (omnibox queries)
    - WebKit timestamp conversion to ISO 8601
    - Transition type decoding (raw + human-readable)
    - StatisticsCollector integration for run tracking
    - WAL/journal file copying for SQLite recovery
    - Schema warning support for unknown tables/columns/transitions
    - Browser selection config widget
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="chromium_history",
            display_name="Chromium History",
            description="Extract browser history and search terms from Chrome, Edge, Brave, Opera",
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
            status_text = f"Chromium History\nFiles: {file_count}\nRun: {data.get('run_id', 'N/A')[:20]}"
        else:
            status_text = "Chromium History\nNo extraction yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "chromium_history"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Chromium history databases from evidence.

        Workflow:
            1. Generate run_id
            2. Scan evidence for Chromium history files (multi-partition if enabled)
            3. Copy matching files to output_dir/
            4. Calculate hashes, collect E01 context
            5. Write manifest.json

        Multi-partition support:
            When scan_all_partitions=True (default), uses file_list discovery to
            find History files across ALL partitions, not just the main partition.
            This captures browser artifacts from dual-boot systems, portable apps,
            and old OS installations.
        """
        callbacks.on_step("Initializing Chromium history extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting Chromium history extraction (run_id=%s)", run_id)

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
        callbacks.on_step("Scanning for Chromium history databases")

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
            LOGGER.info("No history files found")
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
            "Chromium history extraction complete: %d files from %d partition(s), status=%s",
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
               - Parse keyword_search_terms table
               - Insert into browser_history and browser_search_terms tables
               - Detect unknown tables/columns/transitions
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
                collector.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0,
                    urls=0,
                )
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"urls": 0, "records": 0, "search_terms": 0}

        total_records = 0
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

                records, search_count = self._parse_and_insert(
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
                    urls_parsed=records,
                    records_parsed=records,
                )

                total_records += records
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
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=total_records,
                urls=total_records,
            )
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        return {"urls": total_records, "records": total_records, "search_terms": total_search_terms}

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
        Scan evidence for Chromium history files (single partition fallback).

        Args:
            evidence_fs: Mounted filesystem
            browsers: List of browser keys to search
            callbacks: Progress/log interface

        Returns:
            List of dicts with browser, profile, logical_path, etc.
        """
        history_files = []

        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                callbacks.on_log(f"Unknown browser: {browser}", "warning")
                continue

            patterns = get_patterns(browser, "history")

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        profile = extract_profile_from_path(path_str)
                        partition_index = getattr(evidence_fs, 'partition_index', 0)

                        history_files.append({
                            "logical_path": path_str,
                            "browser": browser,
                            "profile": profile,
                            "artifact_type": "history",
                            "display_name": get_browser_display_name(browser),
                            "partition_index": partition_index,
                        })

                        callbacks.on_log(f"Found {browser} history: {path_str}", "info")

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
        Discover Chromium history files across ALL partitions using file_list.

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

        # Build path patterns for file_list query from canonical browser patterns.
        path_patterns = set()
        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                continue
            patterns = get_artifact_patterns(browser, "history")
            for pattern in patterns:
                path_patterns.add(glob_to_sql_like(pattern))

        result, embedded_roots = discover_artifacts_with_embedded_roots(
            evidence_conn,
            evidence_id,
            artifact="history",
            filename_patterns=["History"],
            path_patterns=sorted(path_patterns) if path_patterns else None,
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

                files_list.append({
                    "logical_path": match.file_path,
                    "browser": browser or "chromium",
                    "profile": profile,
                    "artifact_type": "history",
                    "display_name": display_name,
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

                callbacks.on_log(
                    f"Found {browser or 'chromium'} history on partition {partition_index}: {match.file_path}",
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
        # Format: {browser}_{profile}_p{partition}_{mini_hash}_History
        safe_profile = profile.replace(" ", "_").replace("/", "_")
        path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]
        filename = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_History"
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
            "size_bytes": size,
            "file_size_bytes": size,
            "md5": md5,
            "sha256": sha256,
            "extracted_path": str(dest_path),
            "browser": browser,
            "profile": profile,
            "logical_path": source_path,
            "artifact_type": "history",
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
    ) -> tuple[int, int]:
        """
        Parse history database and insert records.

        Returns:
            Tuple of (history_record_count, search_term_count)
        """
        if not db_path.exists():
            LOGGER.warning("History database not found: %s", db_path)
            return 0, 0

        browser = file_entry.get("browser", "unknown")
        profile = file_entry.get("profile", "Default")
        source_path = file_entry.get("logical_path", "")
        partition_index = file_entry.get("partition_index")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        records = []
        search_term_records = []
        found_transitions: Set[int] = set()

        try:
            with safe_sqlite_connect(db_path) as conn:
                # Discover unknown tables (schema warnings)
                if warning_collector:
                    unknown_tables = discover_unknown_tables(
                        conn, KNOWN_HISTORY_TABLES, HISTORY_TABLE_PATTERNS
                    )
                    for table_info in unknown_tables:
                        warning_collector.add_unknown_table(
                            table_name=table_info["name"],
                            columns=table_info["columns"],
                            source_file=source_path,
                            artifact_type="history",
                        )

                # Parse history visits
                for visit in parse_history_visits(conn):
                    # Decode transition type
                    transition_name = decode_transition_type(visit.transition)
                    found_transitions.add(visit.transition)

                    records.append({
                        "browser": browser,
                        "url": visit.url,
                        "title": visit.title,
                        "visit_time_utc": visit.visit_time_iso,
                        "visit_count": visit.visit_count,
                        "typed_count": visit.typed_count,
                        "profile": profile,
                        "run_id": run_id,
                        "source_path": source_path,
                        "discovered_by": discovered_by,
                        "partition_index": partition_index,
                        # Forensic visit metadata
                        "transition_type": visit.transition,
                        "transition_type_name": transition_name,  #
                        "from_visit": visit.from_visit,
                        "visit_duration_ms": visit.visit_duration_ms,
                        "hidden": 1 if visit.hidden else 0,
                        "chromium_visit_id": visit.visit_id,
                        "chromium_url_id": visit.url_id,
                    })

                # Parse keyword search terms
                for term in parse_keyword_search_terms(conn):
                    search_term_records.append({
                        "term": term.term,
                        "normalized_term": term.normalized_term,
                        "url": term.url,
                        "browser": browser,
                        "profile": profile,
                        "search_time_utc": term.search_time_iso,
                        "source_path": source_path,
                        "discovered_by": discovered_by,
                        "run_id": run_id,
                        "partition_index": partition_index,
                        "chromium_keyword_id": term.keyword_id,
                        "chromium_url_id": term.url_id,
                    })

                # Track unknown transition types
                if warning_collector and found_transitions:
                    unknown_transitions = track_unknown_values(
                        known_mapping=TRANSITION_CORE_TYPES,
                        found_values={t & 0xFF for t in found_transitions},  # Core type only
                    )
                    for unknown_type in unknown_transitions:
                        warning_collector.add_unknown_enum_value(
                            enum_name="transition_type",
                            value=unknown_type,
                            source_file=source_path,
                            artifact_type="history",
                        )

        except SQLiteReadError as e:
            LOGGER.error("Failed to read history database %s: %s", db_path, e)
            return 0, 0

        # Batch insert to browser_history table (per-visit records)
        if records:
            insert_browser_history_rows(evidence_conn, evidence_id, records)
            callbacks.on_log(f"Inserted {len(records)} history records from {browser}", "info")

            # Also insert each visit to urls table as distinct forensic event
            # No longer aggregating - each visit preserves its timestamp
            # for timeline reconstruction. Aggregation done at query/report time.
            url_records = []
            for record in records:
                url = record["url"]
                visit_time = record.get("visit_time_utc")

                # Extract domain and scheme from URL
                domain = None
                scheme = None
                try:
                    from urllib.parse import urlparse
                    parsed = urlparse(url)
                    scheme = parsed.scheme or None
                    domain = parsed.netloc or None
                except Exception:
                    pass

                url_records.append({
                    "url": url,
                    "domain": domain,
                    "scheme": scheme,
                    "discovered_by": discovered_by,
                    "first_seen_utc": visit_time,
                    "source_path": source_path,
                    "run_id": run_id,
                    "notes": json.dumps({
                        "browser": browser,
                        "profile": profile,
                        "title": record.get("title"),
                    }),
                })

            if url_records:
                insert_urls(evidence_conn, evidence_id, url_records)
                callbacks.on_log(f"Inserted {len(url_records)} URL events to urls table", "info")

        # Insert search terms
        if search_term_records:
            insert_search_terms(evidence_conn, evidence_id, search_term_records)
            callbacks.on_log(f"Inserted {len(search_term_records)} search terms from {browser}", "info")

        return len(records), len(search_term_records)
