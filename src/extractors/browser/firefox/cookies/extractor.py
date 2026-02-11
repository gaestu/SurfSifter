"""
Firefox Cookies Extractor

Extracts browser cookies from all Firefox-based browsers (Firefox, Firefox ESR, Tor Browser).
Uses shared patterns and parsers from the firefox family module.

Features:
- Plaintext cookie extraction (Firefox doesn't encrypt cookies like Chromium)
- PRTime timestamp conversion to ISO 8601
- Multi-partition discovery via file_list table
- Schema warning support for unknown columns/values
- StatisticsCollector integration
- WAL/journal file copying for SQLite recovery
- Forensic provenance (run_id, source_path, partition context)

 Changes:
- Added multi-partition support using discover_from_file_list
- Added schema warning collector for unknown tables/columns/values
- Fixed file overwrite risk with partition index and path hash in filenames
- Added _clear_previous_run to prevent duplicate records
- Improved browser detection for multi-partition discovery
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
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
from .._patterns import (
    FIREFOX_BROWSERS,
    get_patterns,
    get_browser_display_name,
    get_all_browsers,
    extract_profile_from_path,
    detect_browser_from_path,
)
from .._parsers import (
    parse_cookies,
    get_cookie_stats,
)
from ._schemas import (
    KNOWN_COOKIES_TABLES,
    COOKIES_TABLE_PATTERNS,
    KNOWN_MOZ_COOKIES_COLUMNS,
    KNOWN_ORIGIN_ATTRIBUTES_KEYS,
    SAMESITE_VALUES,
)
from extractors._shared.extraction_warnings import (
    ExtractionWarningCollector,
    discover_unknown_tables,
    discover_unknown_columns,
    track_unknown_values,
)
from core.logging import get_logger
from core.database import (
    insert_cookie_row,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)
from core.database.helpers.cookies import delete_cookies_by_run


LOGGER = get_logger("extractors.browser.firefox.cookies")


class FirefoxCookiesExtractor(BaseExtractor):
    """
    Extract browser cookies from Firefox-based browsers.

    Supports: Firefox, Firefox ESR, Tor Browser

    Dual-phase workflow:
    - Extraction: Scans filesystem, copies cookies.sqlite files to workspace
    - Ingestion: Parses SQLite databases, inserts with forensic fields

    Features:
    - Plaintext cookie extraction (Firefox cookies are NOT encrypted)
    - PRTime timestamp conversion to ISO 8601
    - Multi-partition discovery
    - Schema warning support
    - StatisticsCollector integration for run tracking
    - WAL/journal file copying for SQLite recovery
    - Browser selection config widget
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="firefox_cookies",
            display_name="Firefox Cookies",
            description="Extract browser cookies from Firefox, Firefox ESR, Tor Browser",
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
        Return configuration widget (browser selection).

        Uses BrowserConfigWidget filtered to Firefox browsers only.
        """
        return BrowserConfigWidget(
            parent,
            supported_browsers=get_all_browsers(),
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
            status_text = f"Firefox Cookies\nFiles: {file_count}\nRun: {data.get('run_id', 'N/A')[:20]}"
        else:
            status_text = "Firefox Cookies\nNo extraction yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "firefox_cookies"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Firefox cookies databases from evidence.

        Workflow:
            1. Generate run_id
            2. Scan evidence for Firefox cookies.sqlite files (multi-partition)
            3. Copy matching files to output_dir/
            4. Calculate hashes, collect E01 context
            5. Write manifest.json
        """
        callbacks.on_step("Initializing Firefox cookies extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting Firefox cookies extraction (run_id=%s)", run_id)

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
            "schema_version": "2.0.0",  # Multi-partition support
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "multi_partition": config.get("scan_all_partitions", True),
            "partitions_scanned": [],
            "partitions_with_artifacts": [],
            "files": [],
            "status": "ok",
            "notes": [],
        }

        # Determine which browsers to search
        browsers = config.get("browsers") or config.get("selected_browsers") or get_all_browsers()

        # Determine multi-partition mode
        scan_all_partitions = config.get("scan_all_partitions", True)

        # Scan for cookies files (multi-partition aware)
        callbacks.on_step("Scanning for Firefox cookies databases")

        files_by_partition: Dict[int, List[Dict]] = {}
        used_file_list = False

        if scan_all_partitions and evidence_conn:
            # Check if file_list is available for fast discovery
            available, count = check_file_list_available(evidence_conn, evidence_id)

            if available:
                callbacks.on_log(f"Using file_list index for discovery ({count:,} files indexed)", "info")
                files_by_partition = self._discover_files_multi_partition(
                    evidence_conn, evidence_id, browsers, callbacks
                )
                used_file_list = True
            else:
                callbacks.on_log("file_list not available - using filesystem scan", "warning")

        # Fall back to single-partition filesystem scan if:
        # - file_list was not available, OR
        # - file_list was available but returned no matches (scan current partition as fallback)
        if not files_by_partition:
            callbacks.on_log("Falling back to filesystem scan for current partition", "info")
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, browsers, callbacks)
            files_by_partition = {partition_index: files} if files else {}

        # Count total files
        total_files = sum(len(files) for files in files_by_partition.values())

        # Update manifest with partition info
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = sorted(
            p for p, f in files_by_partition.items() if f
        )

        # Report discovered files
        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=total_files)

        if total_files == 0:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Firefox cookies files found")
            LOGGER.info("No cookies files found")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="success")
        else:
            callbacks.on_log(f"Found {total_files} cookie database(s) across {len(files_by_partition)} partition(s)")
            callbacks.on_progress(0, total_files, "Extracting cookies databases")

            # Get EWF paths for opening other partitions
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            current_partition = getattr(evidence_fs, 'partition_index', 0)

            file_index = 0
            for partition_index in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_index]

                # Determine which filesystem to use
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
                            if collector:
                                collector.finish_run(evidence_id, self.metadata.name, status="cancelled")
                            break

                        file_index += 1
                        try:
                            callbacks.on_progress(
                                file_index, total_files,
                                f"Copying {file_info['browser']} cookies (partition {partition_index})"
                            )

                            extracted = self._extract_file(
                                fs_to_use, file_info, output_dir, callbacks,
                                partition_index=partition_index,
                            )
                            manifest_data["files"].append(extracted)

                        except Exception as e:
                            error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            manifest_data["notes"].append(error_msg)
                            manifest_data["status"] = "partial"
                            if collector:
                                collector.report_failed(evidence_id, self.metadata.name, files=1)

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
            evidence_conn=evidence_conn,
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
            "Firefox cookies extraction complete: %d files, status=%s",
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
        """
        Parse extracted cookies databases and ingest into database.

        Workflow:
            1. Read manifest.json
            2. Delete previous run data
            3. Create warning collector for schema discovery
            4. Register files in browser_cache_inventory
            5. For each cookies database:
               - Parse SQLite moz_cookies table with schema warnings
               - Insert into cookies table
            6. Flush warnings and update inventory status
            7. Return counts
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"cookies": 0, "records": 0}

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

        # Clear previous run data to prevent duplicates
        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if collector:
                collector.report_ingested(evidence_id, self.metadata.name, records=0, cookies=0)
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"cookies": 0, "records": 0}

        total_records = 0
        failed_files = 0

        callbacks.on_progress(0, len(files), "Parsing cookies databases")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(files),
                f"Parsing {file_entry.get('browser', 'unknown')} cookies"
            )

            try:
                # Register in inventory
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "unknown"),
                    artifact_type="cookies",
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

                # Parse and insert records with schema warnings
                db_path = Path(file_entry["extracted_path"])
                if not db_path.is_absolute():
                    db_path = output_dir / db_path

                records = self._parse_and_insert(
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

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")
                failed_files += 1

                # Record file corruption in warnings
                warning_collector.add_file_corrupt(
                    filename=str(file_entry.get('extracted_path', 'unknown')),
                    error=str(e),
                    artifact_type="cookies",
                )

                if "inventory_id" in locals():
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(e),
                    )

        # Flush collected warnings to database
        try:
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)
                callbacks.on_log(f"Schema warnings: {warning_count} items detected")
        except Exception as e:
            LOGGER.warning("Failed to flush extraction warnings: %s", e)

        evidence_conn.commit()

        # Report final statistics
        if collector:
            collector.report_ingested(evidence_id, self.metadata.name, records=total_records, cookies=total_records)
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        return {"cookies": total_records, "records": total_records}

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

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        """
        Clear cookies data from a previous run with same run_id.

        Ensures idempotent re-ingestion without duplicate records.
        """
        try:
            deleted = delete_cookies_by_run(evidence_conn, evidence_id, run_id)
            if deleted > 0:
                LOGGER.info("Cleared %d cookies from previous run %s", deleted, run_id)
        except Exception as e:
            LOGGER.debug("No previous run data to delete: %s", e)

    def _discover_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """
        Scan evidence for Firefox cookies files (single partition fallback).

        Args:
            evidence_fs: Mounted filesystem
            browsers: List of browser keys to search
            callbacks: Progress/log interface

        Returns:
            List of dicts with browser, profile, logical_path, etc.
        """
        cookies_files = []

        for browser in browsers:
            if browser not in FIREFOX_BROWSERS:
                callbacks.on_log(f"Unknown browser: {browser}", "warning")
                continue

            patterns = get_patterns(browser, "cookies")

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        # Skip WAL/journal files (they're companion files)
                        if path_str.endswith(("-wal", "-shm", "-journal")):
                            continue

                        profile = extract_profile_from_path(path_str)
                        detected_browser = detect_browser_from_path(path_str)

                        cookies_files.append({
                            "logical_path": path_str,
                            "browser": detected_browser,
                            "profile": profile,
                            "artifact_type": "cookies",
                            "display_name": get_browser_display_name(detected_browser),
                        })

                        callbacks.on_log(f"Found {detected_browser} cookies: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return cookies_files

    def _discover_files_multi_partition(
        self,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> Dict[int, List[Dict]]:
        """
        Discover Firefox cookies files across all partitions using file_list table.

        Returns:
            Dict mapping partition_index -> list of file info dicts
        """
        # Build path patterns for file_list query
        path_patterns = []
        for browser in browsers:
            if browser not in FIREFOX_BROWSERS:
                continue

            # Firefox-specific path patterns for SQL LIKE queries
            if browser == "firefox" or browser == "firefox_esr":
                path_patterns.extend([
                    "%Mozilla%Firefox%Profiles%",
                    "%mozilla%firefox%",
                ])
            elif browser == "tor":
                path_patterns.extend([
                    "%Tor Browser%TorBrowser%Data%Browser%",
                    "%tor-browser%TorBrowser%Data%Browser%",
                ])

        # Remove duplicates
        path_patterns = list(set(path_patterns))

        if not path_patterns:
            return {}

        # Query file_list
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["cookies.sqlite"],
            path_patterns=path_patterns,
        )

        if result.is_empty:
            callbacks.on_log(
                "No cookies files found in file_list, falling back to filesystem scan",
                "warning"
            )
            return {}

        if result.is_multi_partition:
            callbacks.on_log(
                f"Found cookies files on {len(result.partitions_with_matches)} partitions: {result.partitions_with_matches}",
                "info"
            )

        # Convert FileListMatch objects to extractor's expected format
        files_by_partition: Dict[int, List[Dict]] = {}

        for partition_index, matches in result.matches_by_partition.items():
            files_list = []
            for match in matches:
                # Skip WAL/journal files
                if match.file_name.endswith(("-wal", "-shm", "-journal")):
                    continue

                # Detect browser from path
                browser = detect_browser_from_path(match.file_path)
                if browser is None:
                    browser = self._detect_browser_from_path_fallback(match.file_path)

                if browser and browser not in browsers:
                    continue  # Skip if browser not in selection

                profile = extract_profile_from_path(match.file_path)

                files_list.append({
                    "logical_path": match.file_path,
                    "browser": browser or "firefox",
                    "profile": profile,
                    "artifact_type": "cookies",
                    "display_name": get_browser_display_name(browser) if browser else "Firefox",
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

                callbacks.on_log(
                    f"Found {browser or 'firefox'} cookies on partition {partition_index}: {match.file_path}",
                    "info"
                )

            if files_list:
                files_by_partition[partition_index] = files_list

        return files_by_partition

    def _detect_browser_from_path_fallback(self, path: str) -> str:
        """
        Fallback browser detection using path string matching.

        Args:
            path: File path from evidence

        Returns:
            Browser key or "firefox" as default
        """
        path_lower = path.lower()

        # Check for browser-specific path components
        if "tor browser" in path_lower or "torbrowser" in path_lower or "tor-browser" in path_lower:
            return "tor"
        elif ".default-esr" in path_lower or "firefox esr" in path_lower:
            return "firefox_esr"
        elif "mozilla" in path_lower or "firefox" in path_lower:
            return "firefox"

        return "firefox"

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks,
        *,
        partition_index: Optional[int] = None,
    ) -> Dict:
        """
        Copy file from evidence to workspace with metadata.

        Uses partition index and path hash in filename to prevent overwrites
        when same browser/profile exists on multiple partitions.
        """
        source_path = file_info["logical_path"]
        browser = file_info["browser"]
        profile = file_info.get("profile") or "Unknown"

        # Include partition index and path hash to prevent filename collisions
        # Mini-hash ensures uniqueness for same browser/profile in different locations
        safe_profile = profile.replace(" ", "_").replace("/", "_").replace(".", "_")
        path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]
        partition_suffix = f"_p{partition_index}" if partition_index is not None else ""
        filename = f"{browser}_{safe_profile}{partition_suffix}_{path_hash}_cookies.sqlite"
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
            "artifact_type": "cookies",
            "partition_index": partition_index,
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
    ) -> int:
        """
        Parse cookies database and insert records with schema warning support.

        Args:
            db_path: Path to extracted cookies.sqlite
            file_entry: Manifest file entry dict
            run_id: Extraction run identifier
            evidence_id: Database evidence ID
            evidence_conn: SQLite connection to evidence database
            callbacks: Progress/log callbacks
            warning_collector: Optional collector for schema warnings

        Returns:
            Number of cookies inserted
        """
        if not db_path.exists():
            LOGGER.warning("Cookies database not found: %s", db_path)
            return 0

        browser = file_entry.get("browser", "firefox")
        profile = file_entry.get("profile", "Default")
        source_path = file_entry.get("logical_path", "")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        # Check for unknown tables and columns
        if warning_collector:
            self._check_schema_warnings(
                db_path, source_path, warning_collector
            )

        count = 0
        found_samesite_values = set()
        found_origin_attr_keys = set()

        try:
            for cookie in parse_cookies(db_path):
                # Track sameSite values for warning detection
                if cookie.samesite_raw is not None:
                    found_samesite_values.add(cookie.samesite_raw)

                # Track originAttributes keys for warning detection
                if cookie.origin_attributes:
                    found_origin_attr_keys.update(
                        self._extract_origin_attr_keys(cookie.origin_attributes)
                    )

                insert_cookie_row(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=browser,
                    name=cookie.name,
                    domain=cookie.domain,
                    profile=profile,
                    value=cookie.value,
                    path=cookie.path,
                    expires_utc=cookie.expires_utc,
                    is_secure=cookie.is_secure,
                    is_httponly=cookie.is_httponly,
                    samesite=cookie.samesite,
                    samesite_raw=cookie.samesite_raw,
                    creation_utc=cookie.creation_utc,
                    last_access_utc=cookie.last_access_utc,
                    encrypted=0,  # Firefox cookies are never encrypted
                    # Firefox originAttributes
                    origin_attributes=cookie.origin_attributes,
                    user_context_id=cookie.user_context_id,
                    private_browsing_id=cookie.private_browsing_id,
                    first_party_domain=cookie.first_party_domain,
                    partition_key=cookie.partition_key,
                    run_id=run_id,
                    source_path=source_path,
                    discovered_by=discovered_by,
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    logical_path=file_entry.get("logical_path"),
                    forensic_path=file_entry.get("forensic_path"),
                )
                count += 1

        except Exception as e:
            LOGGER.error("Failed to read cookies database %s: %s", db_path, e)
            return 0

        # Report unknown sameSite values
        if warning_collector and found_samesite_values:
            track_unknown_values(
                warning_collector=warning_collector,
                known_mapping=SAMESITE_VALUES,
                found_values=found_samesite_values,
                value_name="sameSite",
                source_file=source_path,
                artifact_type="cookies",
            )

        # Report unknown originAttributes keys
        if warning_collector and found_origin_attr_keys:
            unknown_keys = found_origin_attr_keys - KNOWN_ORIGIN_ATTRIBUTES_KEYS
            for key in unknown_keys:
                warning_collector.add_warning(
                    warning_type="unknown_enum_value",
                    category="database",
                    severity="info",
                    artifact_type="cookies",
                    source_file=source_path,
                    item_name="originAttributes_key",
                    item_value=key,
                )

        if count:
            callbacks.on_log(f"Inserted {count} cookies from {browser}", "info")

        return count

    def _check_schema_warnings(
        self,
        db_path: Path,
        source_file: str,
        warning_collector: ExtractionWarningCollector,
    ) -> None:
        """
        Check database for unknown tables and columns.

        Args:
            db_path: Path to cookies.sqlite
            source_file: Original path in evidence for warnings
            warning_collector: Collector for schema warnings
        """
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            LOGGER.debug("Cannot open %s for schema check: %s", db_path, e)
            return

        try:
            # Check for unknown tables
            unknown_tables = discover_unknown_tables(
                conn, KNOWN_COOKIES_TABLES, COOKIES_TABLE_PATTERNS
            )
            for table_info in unknown_tables:
                warning_collector.add_unknown_table(
                    table_name=table_info["name"],
                    columns=table_info["columns"],
                    source_file=source_file,
                    artifact_type="cookies",
                )

            # Check for unknown columns in moz_cookies
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('moz_cookies', 'cookies')"
            )
            tables = [row[0] for row in cursor.fetchall()]

            for table_name in tables:
                unknown_columns = discover_unknown_columns(
                    conn, table_name, KNOWN_MOZ_COOKIES_COLUMNS
                )
                for col_info in unknown_columns:
                    warning_collector.add_unknown_column(
                        table_name=table_name,
                        column_name=col_info["name"],
                        column_type=col_info.get("type", "unknown"),
                        source_file=source_file,
                        artifact_type="cookies",
                    )

        except Exception as e:
            LOGGER.debug("Schema check failed for %s: %s", db_path, e)
        finally:
            conn.close()

    def _extract_origin_attr_keys(self, origin_attrs: str) -> set:
        """
        Extract keys from Firefox originAttributes string.

        Args:
            origin_attrs: Raw originAttributes string (e.g., "^userContextId=1&privateBrowsingId=0")

        Returns:
            Set of key names found
        """
        keys = set()
        if not origin_attrs:
            return keys

        # Strip leading caret
        attrs = origin_attrs.lstrip("^")
        if not attrs:
            return keys

        # Parse key=value pairs
        for pair in attrs.split("&"):
            if "=" in pair:
                key, _ = pair.split("=", 1)
                keys.add(key)

        return keys
