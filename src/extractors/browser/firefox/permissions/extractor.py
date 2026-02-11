"""
Firefox Permissions Extractor

Extracts and ingests site permissions from Firefox-based browsers:
- Firefox, Firefox ESR, Tor Browser

Features:
- permissions.sqlite parsing (moz_perms table)
- content-prefs.sqlite parsing (site zoom, autoplay, etc.)
- Schema version detection (v11+ with origin, legacy with host)
- Permission type normalization
- StatisticsCollector integration for run tracking
- Multi-partition support via file_list discovery
- Schema warning support for unknown tables/columns
- Modular architecture with _schemas.py and _parsers.py

Data Format:
- permissions.sqlite: SQLite database with moz_perms table
  - Permission values: 1=allow, 2=block
  - Timestamps: PRTime (microseconds since Unix epoch)
- content-prefs.sqlite: SQLite database with groups/settings/prefs tables
  - Stores site-specific settings (zoom, autoplay, etc.)
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
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
    check_file_list_available,
    FileListDiscoveryResult,
)
from .._patterns import (
    FIREFOX_BROWSERS,
    get_artifact_patterns,
    extract_profile_from_path,
    detect_browser_from_path,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_permissions,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    delete_permissions_by_run,
)
from extractors._shared.extraction_warnings import ExtractionWarningCollector

from ._schemas import (
    FIREFOX_PERMISSION_VALUES,
    FIREFOX_PERMISSION_TYPE_MAP,
)
from ._parsers import (
    parse_permissions_file,
    parse_content_prefs_file,
)

LOGGER = get_logger("extractors.browser.firefox.permissions")


class FirefoxPermissionsExtractor(BaseExtractor):
    """
    Extract site permissions from Firefox's permissions.sqlite database.

    Firefox stores site permission decisions in a SQLite database, typically
    located at: AppData/Roaming/Mozilla/Firefox/Profiles/<profile>/permissions.sqlite

    Features:
    - Multi-partition support: Discovers files across all partitions via file_list
    - Schema warnings: Tracks unknown tables/columns for schema evolution
    - Content-prefs: Also extracts site-specific preferences from content-prefs.sqlite
    """

    SUPPORTED_BROWSERS = list(FIREFOX_BROWSERS.keys())

    # Export mappings for tests (backward compatibility)
    FIREFOX_PERMISSION_VALUES = FIREFOX_PERMISSION_VALUES
    FIREFOX_PERMISSION_TYPE_MAP = FIREFOX_PERMISSION_TYPE_MAP

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="firefox_permissions",
            display_name="Firefox Site Permissions",
            description="Extract site permissions from Firefox/Tor Browser permissions.sqlite and content-prefs.sqlite",
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
            partitions = data.get("partitions_with_artifacts", [])
            status_text = (
                f"Firefox Permissions\n"
                f"Files: {file_count}\n"
                f"Partitions: {len(partitions)}\n"
                f"Run: {data.get('run_id', 'N/A')[:20]}"
            )
        else:
            status_text = "Firefox Permissions\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "firefox_permissions"

    # =========================================================================
    # Extraction
    # =========================================================================

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Firefox permissions.sqlite and content-prefs.sqlite files from evidence.

        Supports multi-partition extraction via file_list discovery when enabled.
        """
        callbacks.on_step("Initializing Firefox permissions extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")
        scan_all_partitions = config.get("scan_all_partitions", True)

        LOGGER.info("Starting Firefox permissions extraction (run_id=%s)", run_id)

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Get browser selection from config
        browsers = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        if isinstance(browsers, str):
            browsers = [browsers]

        callbacks.on_log(f"Selected browsers: {browsers}", "info")

        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize manifest
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
            "browsers": browsers,
            "files": [],
            "status": "ok",
            "notes": [],
        }

        # Discover permission files
        callbacks.on_step("Scanning for Firefox permissions files")

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
            permission_files = self._discover_permission_files(evidence_fs, browsers, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if permission_files:
                files_by_partition[partition_index] = permission_files

        # Flatten for counting
        all_permission_files = []
        for files_list in files_by_partition.values():
            all_permission_files.extend(files_list)

        # Update manifest with partition info
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = sorted(
            p for p, files in files_by_partition.items() if files
        )

        # Report discovered files
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(all_permission_files))

        callbacks.on_log(
            f"Found {len(all_permission_files)} permission file(s) across "
            f"{len(files_by_partition)} partition(s)"
        )

        if not all_permission_files:
            LOGGER.info("No Firefox permissions files found")
            manifest_data["notes"].append("No Firefox permissions files found")
        else:
            callbacks.on_progress(0, len(all_permission_files), "Extracting permission files")

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
                        fs_to_use = open_partition_for_extraction(ewf_paths, partition_index)
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
                                file_index + 1, len(all_permission_files),
                                f"Extracting {file_info['browser']} {file_info['file_type']} (partition {partition_index})"
                            )

                            extracted = self._extract_single_file(
                                fs_to_use, file_info, output_dir, partition_index
                            )
                            manifest_data["files"].append(extracted)
                            file_index += 1

                        except Exception as e:
                            error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            manifest_data["notes"].append(error_msg)
                            callbacks.on_error(error_msg, str(e))
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

        # Finish statistics
        if stats:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        # Write manifest
        callbacks.on_step("Writing manifest")
        (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2, default=str))

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

        LOGGER.info(
            "Firefox permissions extraction complete: %d files from %d partition(s), status=%s",
            len(manifest_data["files"]),
            len(manifest_data["partitions_with_artifacts"]),
            manifest_data["status"],
        )

        return manifest_data["status"] != "error"

    # =========================================================================
    # Ingestion
    # =========================================================================

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, Any]:
        """
        Parse extracted permissions and insert into database.

        Features:
        - Schema warning collection for unknown tables/columns
        - Clears previous run data before inserting
        - Tracks statistics for UI display
        """
        callbacks.on_step("Starting Firefox permissions ingestion")

        manifest_path = output_dir / "manifest.json"
        if not manifest_path.exists():
            callbacks.on_error("No manifest.json found", "Run extraction first")
            return {"records": 0, "permissions": 0}

        manifest = json.loads(manifest_path.read_text())
        run_id = manifest.get("run_id", self._generate_run_id())
        files = manifest.get("files", [])
        evidence_label = config.get("evidence_label", "")

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Continue statistics tracking with same run_id from extraction
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "info")
            # Flush any warnings (none expected but good practice)
            warning_collector.flush_to_database(evidence_conn)
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, permissions=0)
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"records": 0, "permissions": 0}

        # Clear previous run data to avoid duplicates on re-run
        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        total_permissions = 0
        failed_files = 0

        for i, file_entry in enumerate(files, 1):
            callbacks.on_progress(i, len(files), f"Parsing {file_entry.get('browser', 'unknown')}")

            # Skip files that failed extraction
            if file_entry.get("copy_status") == "error":
                callbacks.on_log(
                    f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}",
                    "warning"
                )
                continue

            extracted_path = file_entry.get("extracted_path")
            if not extracted_path or not Path(extracted_path).exists():
                callbacks.on_log(f"Skipping missing file: {extracted_path}", "warn")
                continue

            inventory_id = None
            try:
                file_type = file_entry.get("file_type", "permissions_sqlite")
                artifact_type = "content_prefs" if file_type == "content_prefs_sqlite" else "permissions"
                logical_path = file_entry.get("logical_path", "")

                # Register in browser_inventory
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id,
                    file_entry.get("browser", "firefox"),
                    artifact_type,
                    run_id,
                    extracted_path,
                    "ok",  # extraction_status
                    manifest.get("extraction_timestamp_utc", datetime.now(timezone.utc).isoformat()),
                    logical_path,
                    # Optional kwargs
                    profile=file_entry.get("profile"),
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    file_size_bytes=file_entry.get("file_size_bytes", 0),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                    forensic_path=file_entry.get("forensic_path"),
                    extraction_tool=manifest.get("extraction_tool"),
                )

                # Build discovered_by string
                discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

                # Parse permissions based on file type using modular parsers
                if file_type == "content_prefs_sqlite":
                    records = parse_content_prefs_file(
                        Path(extracted_path),
                        file_entry,
                        run_id,
                        discovered_by,
                        warning_collector=warning_collector,
                    )
                else:
                    records = parse_permissions_file(
                        Path(extracted_path),
                        file_entry,
                        run_id,
                        discovered_by,
                        warning_collector=warning_collector,
                    )

                # Insert records
                count = 0
                if records:
                    count = insert_permissions(evidence_conn, evidence_id, records)
                    callbacks.on_log(
                        f"Parsed {count} permissions from {file_entry.get('browser', 'firefox')} "
                        f"partition {file_entry.get('partition_index', 0)}",
                        "info"
                    )

                total_permissions += count

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=count,
                )

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")
                failed_files += 1

                if inventory_id is not None:
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

        # Report ingested counts and finish statistics tracking
        if stats:
            stats.report_ingested(evidence_id, self.metadata.name, records=total_permissions, permissions=total_permissions)
            status = "success" if failed_files == 0 else "partial"
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        return {"records": total_permissions, "permissions": total_permissions}

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate run ID: {timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

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
        """Clear permission data from a previous run."""
        deleted = delete_permissions_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d permission records from previous run %s", deleted, run_id)

    # =========================================================================
    # File Discovery
    # =========================================================================

    def _discover_permission_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """
        Scan evidence for Firefox permissions.sqlite and content-prefs.sqlite files.

        Single-partition fallback when file_list is not available.
        """
        permission_files = []

        for browser in browsers:
            if browser not in self.SUPPORTED_BROWSERS:
                callbacks.on_log(f"Unsupported browser: {browser}", "warn")
                continue

            if browser not in FIREFOX_BROWSERS:
                continue

            # Use get_artifact_patterns to get properly constructed glob patterns
            patterns = get_artifact_patterns(browser, "permissions")
            display_name = FIREFOX_BROWSERS[browser]["display_name"]

            for pattern in patterns:
                try:
                    # Use iter_paths (correct EvidenceFS API)
                    for path_str in evidence_fs.iter_paths(pattern):
                        path_lower = path_str.lower()

                        # Determine file type
                        if "permissions.sqlite" in path_lower:
                            file_type = "permissions_sqlite"
                        elif "content-prefs.sqlite" in path_lower:
                            file_type = "content_prefs_sqlite"
                        else:
                            continue

                        # Use shared profile extraction
                        profile = extract_profile_from_path(path_str)
                        partition_index = getattr(evidence_fs, 'partition_index', 0)

                        permission_files.append({
                            "browser": browser,
                            "profile": profile,
                            "logical_path": path_str,
                            "file_type": file_type,
                            "fs_type": getattr(evidence_fs, "fs_type", "unknown"),
                            "partition_index": partition_index,
                            "display_name": display_name,
                        })

                        callbacks.on_log(f"Found {browser} {file_type}: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Error with pattern %s: %s", pattern, e)

        return permission_files

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, List[Dict]]:
        """
        Discover Firefox permission files across ALL partitions using file_list.

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
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_permission_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")

        # Build filename patterns for permissions files
        filename_patterns = ["permissions.sqlite", "content-prefs.sqlite"]

        # Build path patterns for Firefox browsers
        path_patterns = []
        for browser in browsers:
            if browser not in FIREFOX_BROWSERS:
                continue

            # Add Firefox-specific path patterns
            if browser in ("firefox", "firefox_esr"):
                path_patterns.extend([
                    "%Mozilla%Firefox%Profiles%",
                    "%.mozilla%firefox%",
                ])
            elif browser == "tor":
                path_patterns.extend([
                    "%Tor Browser%",
                    "%TorBrowser%",
                    "%tor-browser%",
                ])

        # Remove duplicates while preserving order
        path_patterns = list(dict.fromkeys(path_patterns))

        # Query file_list table
        result: FileListDiscoveryResult = discover_from_file_list(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            filename_patterns=filename_patterns,
            path_patterns=path_patterns,
        )

        callbacks.on_log(f"Multi-partition discovery: {result.get_partition_summary()}")

        # Group by partition with full file info
        files_by_partition: Dict[int, List[Dict]] = {}

        for match in result.get_all_matches():
            path_str = match.file_path
            path_lower = path_str.lower()

            # Determine file type
            if "permissions.sqlite" in path_lower:
                file_type = "permissions_sqlite"
            elif "content-prefs.sqlite" in path_lower:
                file_type = "content_prefs_sqlite"
            else:
                continue

            partition_index = match.partition_index
            if partition_index not in files_by_partition:
                files_by_partition[partition_index] = []

            # Detect browser from path
            browser = detect_browser_from_path(path_str)
            if browser not in browsers:
                # Skip if browser not in requested list
                continue

            profile = extract_profile_from_path(path_str)
            display_name = FIREFOX_BROWSERS.get(browser, {}).get("display_name", browser)

            files_by_partition[partition_index].append({
                "logical_path": path_str,
                "browser": browser,
                "profile": profile,
                "file_type": file_type,
                "partition_index": partition_index,
                "fs_type": getattr(evidence_fs, "fs_type", "unknown"),
                "forensic_path": getattr(match, "forensic_path", None),
                "inode": match.inode,
                "display_name": display_name,
            })

            callbacks.on_log(
                f"Found {browser} {file_type} on partition {partition_index}: {path_str}",
                "info"
            )

        return files_by_partition

    # =========================================================================
    # File Extraction
    # =========================================================================

    def _extract_single_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        partition_index: int,
    ) -> Dict:
        """
        Extract a single permissions or content-prefs SQLite file.

        Filename includes partition index and content hash to prevent collisions.
        """
        logical_path = file_info["logical_path"]
        browser = file_info["browser"]
        profile = file_info.get("profile", "default")
        file_type = file_info.get("file_type", "permissions_sqlite")

        # Create output path with safe filename including partition and hash
        safe_profile = profile.replace(' ', '_').replace('/', '_').replace('.', '_')

        try:
            # Use read_file (correct EvidenceFS API)
            content = evidence_fs.read_file(logical_path)

            # Compute hashes
            md5 = hashlib.md5(content).hexdigest()
            sha256 = hashlib.sha256(content).hexdigest()

            # Include partition and short hash in filename to prevent collisions
            # e.g., firefox_abc123_p1_a1b2c3d4_permissions.sqlite
            short_hash = md5[:8]

            if file_type == "content_prefs_sqlite":
                safe_name = f"{browser}_{safe_profile}_p{partition_index}_{short_hash}_content-prefs.sqlite"
            else:
                safe_name = f"{browser}_{safe_profile}_p{partition_index}_{short_hash}_permissions.sqlite"

            output_path = output_dir / safe_name
            output_path.write_bytes(content)

            sidecar_files = []
            for sidecar_suffix in ("-wal", "-shm"):
                sidecar_logical = f"{logical_path}{sidecar_suffix}"
                try:
                    sidecar_content = evidence_fs.read_file(sidecar_logical)
                except Exception:
                    continue

                sidecar_md5 = hashlib.md5(sidecar_content).hexdigest()
                sidecar_sha256 = hashlib.sha256(sidecar_content).hexdigest()
                sidecar_name = f"{output_path.name}{sidecar_suffix}"
                sidecar_path = output_dir / sidecar_name

                try:
                    sidecar_path.write_bytes(sidecar_content)
                    sidecar_files.append({
                        "file_type": "sqlite_sidecar",
                        "suffix": sidecar_suffix,
                        "logical_path": sidecar_logical,
                        "extracted_path": str(sidecar_path),
                        "file_size_bytes": len(sidecar_content),
                        "md5": sidecar_md5,
                        "sha256": sidecar_sha256,
                    })
                except Exception as e:
                    LOGGER.debug("Failed to extract sidecar %s: %s", sidecar_logical, e)

            return {
                "copy_status": "ok",
                "file_size_bytes": len(content),
                "md5": md5,
                "sha256": sha256,
                "extracted_path": str(output_path),
                "browser": browser,
                "profile": profile,
                "file_type": file_type,
                "logical_path": logical_path,
                "partition_index": partition_index,
                "fs_type": file_info.get("fs_type"),
                "forensic_path": file_info.get("forensic_path"),
                "inode": file_info.get("inode"),
                "sidecar_files": sidecar_files,
            }

        except Exception as e:
            LOGGER.error("Failed to extract %s: %s", logical_path, e)
            return {
                "copy_status": "error",
                "file_size_bytes": 0,
                "md5": None,
                "sha256": None,
                "extracted_path": None,
                "browser": browser,
                "profile": profile,
                "file_type": file_type,
                "logical_path": logical_path,
                "partition_index": partition_index,
                "fs_type": file_info.get("fs_type"),
                "forensic_path": file_info.get("forensic_path"),
                "error_message": str(e),
            }


# Backward compatibility alias
PermissionsExtractor = FirefoxPermissionsExtractor
