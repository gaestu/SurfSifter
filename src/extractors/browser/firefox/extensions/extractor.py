"""Firefox Extensions Extractor

Extracts and analyzes browser extensions from Firefox-based browsers
with permission risk classification and known extension matching.

Supported Browsers:
- Firefox (standard release)
- Firefox ESR (Extended Support Release)
- Tor Browser

Note: Firefox Developer Edition and Waterfox use identical extension formats
and can be added to FIREFOX_BROWSERS in _patterns.py when needed.

Features:
- Extension metadata extraction from extensions.json (primary source)
- Additional metadata from addons.json (AMO metadata, icons, etc.)
- XPI extension archive extraction for code analysis
- Multi-partition support for forensic completeness
- Disabled/staged add-on detection
- Permission risk classification (low/medium/high/critical)
- Known extension reference list matching
- Addon signing state tracking
- Schema warning support for unknown JSON fields
- StatisticsCollector integration (with None guards for headless/test mode)
- ELT pattern: Files are copied to workspace before parsing
- Deduplication: Same extension per (browser, profile, extension_id)

Data Sources:
- extensions.json (primary - contains all installed extensions)
- addons.json (supplementary - AMO metadata, update URLs, icons)
- *.xpi files (extension archives for code analysis)

Architecture (ELT Pattern):
- Extract: Copy extensions.json, addons.json, and XPI files to output_dir
- Transform: Parse JSON files from disk during ingestion
- Load: Insert parsed extension metadata into database

Module Structure:
- _schemas.py: Known JSON keys, addon types, signed states
- _parsers.py: JSON parsing with schema warning support
- extractor.py: Main orchestrator (this file)
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Set, Tuple

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from .._patterns import FIREFOX_BROWSERS, get_artifact_patterns, extract_profile_from_path
from ....widgets import BrowserConfigWidget
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

# Import shared utilities
from ...._shared.risk_classifier import calculate_risk_level
from ...._shared.known_extensions import load_known_extensions, match_known_extension
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)

# Import local modules
from ._schemas import (
    EXTENSION_FILES,
    MAX_SAFE_PROFILE_LENGTH,
    is_unsigned_or_broken,
)
from ._parsers import (
    parse_extensions_json,
    parse_addons_json,
)

# Import warning support
from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.browser.firefox.extensions")


class FirefoxExtensionsExtractor(BaseExtractor):
    """
    Extract Firefox browser extension inventory from evidence images.

    Supports: Firefox, Firefox ESR, Tor Browser.
    (Browsers defined in FIREFOX_BROWSERS from _patterns.py)

    Features:
    - Extension metadata from extensions.json (primary source)
    - Additional metadata from addons.json (AMO info, icons, etc.)
    - XPI archive extraction for code analysis
    - Multi-partition support
    - Addon signing state tracking (signedState field)
    - Permission risk classification
    - Known extension reference list matching
    - Schema warning support for forensic completeness
    - Deduplication: One record per (browser, profile, extension_id)
    """

    SUPPORTED_BROWSERS = list(FIREFOX_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="firefox_extensions",
            display_name="Firefox Extensions",
            description="Extract browser extensions from Firefox-based browsers",
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
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget with multi-partition support."""
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
        """Return status widget."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            xpi_count = len(data.get("xpi_files", []))
            status_text = (
                f"Firefox Extensions\n"
                f"JSON files: {file_count}\n"
                f"XPI archives: {xpi_count}\n"
                f"Run ID: {data.get('run_id', 'N/A')}"
            )
        else:
            status_text = "Firefox Extensions\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory."""
        return case_root / "evidences" / evidence_label / "firefox_extensions"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract browser extension files from evidence.

        ELT Pattern:
        - Discovers extensions.json and addons.json files
        - Discovers XPI extension archives
        - Copies files to output_dir for forensic preservation
        - Records file metadata in manifest.json
        - Ingestion phase will parse these files
        """
        callbacks.on_step("Initializing Firefox extension extraction")

        run_id = self._generate_run_id()
        LOGGER.info("Starting Firefox extensions extraction (run_id=%s)", run_id)

        # Start statistics tracking (may be None in tests/headless mode)
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        stats = self._get_statistics_collector()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.1.0",  # Updated for XPI support
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_extraction_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "files": [],
            "xpi_files": [],
            "status": "ok",
            "notes": [],
        }

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        scan_all_partitions = config.get("scan_all_partitions", True)
        evidence_conn = config.get("evidence_conn")

        callbacks.on_step("Scanning for Firefox extension files")

        # Discover files using multi-partition file_list or fallback to single partition
        if scan_all_partitions and evidence_conn:
            files_by_partition = self._discover_files_multi_partition(
                evidence_fs, evidence_conn, evidence_id, browsers_to_search, callbacks
            )
        else:
            # Single partition fallback
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            discovered_files = self._discover_extension_files(
                evidence_fs, browsers_to_search, callbacks, scan_all_partitions=False
            )
            discovered_xpis = self._discover_xpi_files(
                evidence_fs, browsers_to_search, callbacks, scan_all_partitions=False
            )
            files_by_partition = {
                partition_index: {
                    "json_files": discovered_files,
                    "xpi_files": discovered_xpis,
                }
            } if discovered_files or discovered_xpis else {}

        # Count total files across all partitions
        total_json_files = sum(
            len(pdata.get("json_files", [])) for pdata in files_by_partition.values()
        )
        total_xpi_files = sum(
            len(pdata.get("xpi_files", [])) for pdata in files_by_partition.values()
        )
        total_files = total_json_files + total_xpi_files

        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = [
            p for p, f in files_by_partition.items() if f.get("json_files") or f.get("xpi_files")
        ]

        # Report discovered files
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=total_files)

        if total_files == 0:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Firefox extension files found")
            LOGGER.info("No Firefox extension files found")
        else:
            callbacks.on_log(
                f"Found {total_files} file(s) on {len(manifest_data['partitions_with_artifacts'])} partition(s)"
            )

            # Extract files from each partition
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            progress = 0
            callbacks.on_progress(0, total_files, "Extracting extension files")

            for partition_index in sorted(files_by_partition.keys()):
                partition_data = files_by_partition[partition_index]
                json_files = partition_data.get("json_files", [])
                xpi_files = partition_data.get("xpi_files", [])

                if not json_files and not xpi_files:
                    continue

                callbacks.on_log(
                    f"Processing partition {partition_index}: {len(json_files)} JSON, {len(xpi_files)} XPI"
                )

                # Determine how to access this partition
                current_partition = getattr(evidence_fs, 'partition_index', 0)
                if partition_index == current_partition:
                    fs_to_use = evidence_fs
                    need_close = False
                elif ewf_paths:
                    fs_to_use = None
                    need_close = True
                else:
                    callbacks.on_log(
                        f"Cannot access partition {partition_index} - skipping",
                        "warning"
                    )
                    continue

                try:
                    if need_close:
                        with open_partition_for_extraction(ewf_paths, partition_index) as fs:
                            progress = self._extract_partition_files(
                                fs, json_files, xpi_files, output_dir, callbacks,
                                manifest_data, progress, total_files
                            )
                    else:
                        progress = self._extract_partition_files(
                            fs_to_use, json_files, xpi_files, output_dir, callbacks,
                            manifest_data, progress, total_files
                        )
                except Exception as e:
                    error_msg = f"Failed to process partition {partition_index}: {e}"
                    LOGGER.error(error_msg, exc_info=True)
                    manifest_data["notes"].append(error_msg)
                    manifest_data["status"] = "partial"

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
            "Firefox extensions extraction complete: %d JSON files, %d XPI files, status=%s",
            len(manifest_data["files"]),
            len(manifest_data["xpi_files"]),
            manifest_data["status"],
        )

        # Complete statistics tracking
        final_status = "cancelled" if manifest_data["status"] == "cancelled" else "success" if manifest_data["status"] == "ok" else "partial"
        if stats:
            stats.finish_run(evidence_id, self.metadata.name, status=final_status)

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
        Parse extracted extension files and ingest into database.

        ELT Pattern:
        - Reads manifest.json from output_dir
        - Parses extensions.json/addons.json files from disk
        - Inserts extension metadata into database
        - Deduplicates by (browser, profile, extension_id)
        """
        from core.database import insert_extensions, delete_extensions_by_run

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"records": 0, "extensions": 0, "xpi_files": 0}

        manifest_data = json.loads(manifest_path.read_text())

        # Validate manifest is from this extractor
        if manifest_data.get("extractor") != self.metadata.name:
            callbacks.on_error(
                "Invalid manifest",
                f"Manifest is for {manifest_data.get('extractor')}, not {self.metadata.name}"
            )
            return {"records": 0, "extensions": 0, "xpi_files": 0}

        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])
        xpi_files = manifest_data.get("xpi_files", [])
        evidence_label = config.get("evidence_label", "")

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Continue statistics tracking with same run_id from extraction (may be None in tests)
        stats = self._get_statistics_collector()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No extension files to ingest", "warning")
            # Report ingested counts even when 0
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, extensions=0)
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"records": 0, "extensions": 0, "xpi_files": len(xpi_files)}

        # Clear previous data for this run
        delete_extensions_by_run(evidence_conn, evidence_id, run_id)

        # Load known extensions reference list
        known_extensions = load_known_extensions()

        # Group files by browser/profile for merging extensions.json and addons.json data
        file_groups = self._group_files_by_profile(files, output_dir)

        # Track seen extensions for deduplication: (browser, profile, extension_id)
        seen_extensions: Set[Tuple[str, str, str]] = set()
        records = []

        callbacks.on_progress(0, len(file_groups), "Processing extension files")

        for i, (group_key, group_files) in enumerate(file_groups.items()):
            if callbacks.is_cancelled():
                break

            browser, profile = group_key
            callbacks.on_progress(i + 1, len(file_groups), f"Processing {browser}/{profile}")

            # Parse addons.json first (supplementary data)
            addons_metadata = {}
            for file_entry in group_files:
                if file_entry.get("filename", "").endswith("addons.json"):
                    if file_entry.get("copy_status") == "error":
                        continue
                    addons_metadata = parse_addons_json(
                        Path(file_entry["extracted_path"]),
                        callbacks,
                        warning_collector=warning_collector,
                        source_file=file_entry.get("logical_path"),
                    )

            # Parse extensions.json (primary data) and merge
            for file_entry in group_files:
                if file_entry.get("filename", "").endswith("extensions.json"):
                    if file_entry.get("copy_status") == "error":
                        continue
                    extensions = parse_extensions_json(
                        Path(file_entry["extracted_path"]),
                        browser,
                        profile,
                        file_entry,
                        addons_metadata,
                        callbacks,
                        warning_collector=warning_collector,
                    )

                    for ext in extensions:
                        # Deduplication: Skip if already seen this (browser, profile, extension_id)
                        dedup_key = (ext.get("browser"), ext.get("profile"), ext.get("extension_id"))
                        if dedup_key in seen_extensions:
                            callbacks.on_log(
                                f"Skipping duplicate: {ext.get('name')} ({ext.get('extension_id')})",
                                "info"
                            )
                            continue
                        seen_extensions.add(dedup_key)

                        record = self._create_db_record(ext, run_id, known_extensions)
                        records.append(record)

        # Batch insert
        inserted = insert_extensions(evidence_conn, evidence_id, records)

        # Flush warnings to database
        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)

        evidence_conn.commit()

        # Report ingested counts and finish statistics tracking
        if stats:
            stats.report_ingested(evidence_id, self.metadata.name, records=inserted, extensions=inserted)
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {"records": inserted, "extensions": inserted, "xpi_files": len(xpi_files)}

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _normalize_text_field(self, value):
        """Normalize a value for insertion into a TEXT column."""
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value)

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context."""
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

    def _get_statistics_collector(self):
        """Get StatisticsCollector instance (may be None in tests/headless mode)."""
        try:
            return StatisticsCollector.instance()
        except Exception:
            return None

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> Dict[int, Dict[str, List[Dict]]]:
        """
        Discover Firefox extension files across all partitions via file_list.

        Args:
            evidence_fs: Evidence filesystem (for fallback)
            evidence_conn: Evidence database connection
            evidence_id: Evidence ID for file_list lookup
            browsers: List of browser keys to search
            callbacks: Progress/log callbacks

        Returns:
            Dict mapping partition_index -> {"json_files": [...], "xpi_files": [...]}
        """
        # Check if file_list is available
        available, count = check_file_list_available(evidence_conn, evidence_id)

        if not available:
            callbacks.on_log(
                "file_list empty, falling back to single-partition discovery",
                "info"
            )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            json_files = self._discover_extension_files(evidence_fs, browsers, callbacks, scan_all_partitions=False)
            xpi_files = self._discover_xpi_files(evidence_fs, browsers, callbacks, scan_all_partitions=False)
            if json_files or xpi_files:
                return {partition_index: {"json_files": json_files, "xpi_files": xpi_files}}
            return {}

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")

        # Build path patterns for Firefox profiles
        path_patterns = [
            # Windows
            "%Mozilla%Firefox%Profiles%",
            # macOS
            "%Application Support%Firefox%Profiles%",
            # Linux
            "%.mozilla/firefox%",
            # Tor Browser
            "%Tor Browser%TorBrowser%Data%Browser%",
            "%tor-browser%TorBrowser%Data%Browser%",
        ]

        # Query file_list for extension JSON files
        json_result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["extensions.json", "addons.json"],
            path_patterns=path_patterns,
        )

        # Query file_list for XPI files
        xpi_result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            path_patterns=path_patterns,
            extension_filter=[".xpi"],
        )

        if json_result.is_empty and xpi_result.is_empty:
            callbacks.on_log(
                "No Firefox extension files found in file_list, falling back to filesystem scan",
                "warning"
            )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            json_files = self._discover_extension_files(evidence_fs, browsers, callbacks, scan_all_partitions=False)
            xpi_files = self._discover_xpi_files(evidence_fs, browsers, callbacks, scan_all_partitions=False)
            if json_files or xpi_files:
                return {partition_index: {"json_files": json_files, "xpi_files": xpi_files}}
            return {}

        # Collect all partitions
        all_partitions = set(json_result.matches_by_partition.keys()) | set(xpi_result.matches_by_partition.keys())

        if len(all_partitions) > 1:
            callbacks.on_log(
                f"Found extension files on {len(all_partitions)} partitions: {sorted(all_partitions)}",
                "info"
            )

        # Convert FileListMatch objects to extractor's expected format
        files_by_partition: Dict[int, Dict[str, List[Dict]]] = {}

        for partition_index in all_partitions:
            json_files = []
            xpi_files = []

            # Process JSON files
            for match in json_result.matches_by_partition.get(partition_index, []):
                # Detect browser from path
                browser = self._detect_browser_from_path(match.file_path)
                if browser and browser not in browsers:
                    continue

                profile = extract_profile_from_path(match.file_path)

                json_files.append({
                    "logical_path": match.file_path,
                    "browser": browser or "firefox",
                    "profile": profile,
                    "filename": match.file_name,
                    "artifact_type": "extensions",
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                    "fs_type": None,
                })

                callbacks.on_log(
                    f"Found {browser or 'firefox'} {match.file_name} on partition {partition_index}: {match.file_path}",
                    "info"
                )

            # Process XPI files
            for match in xpi_result.matches_by_partition.get(partition_index, []):
                browser = self._detect_browser_from_path(match.file_path)
                if browser and browser not in browsers:
                    continue

                profile = extract_profile_from_path(match.file_path)
                ext_id = match.file_name.rsplit('.', 1)[0]

                xpi_files.append({
                    "logical_path": match.file_path,
                    "browser": browser or "firefox",
                    "profile": profile,
                    "filename": match.file_name,
                    "artifact_type": "xpi",
                    "extension_id": ext_id,
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                    "fs_type": None,
                    "staged": "/staged/" in match.file_path,
                })

                callbacks.on_log(
                    f"Found {browser or 'firefox'} XPI on partition {partition_index}: {match.file_name}",
                    "info"
                )

            if json_files or xpi_files:
                files_by_partition[partition_index] = {
                    "json_files": json_files,
                    "xpi_files": xpi_files,
                }

        return files_by_partition

    def _detect_browser_from_path(self, path: str) -> Optional[str]:
        """Detect Firefox browser variant from path."""
        path_lower = path.lower()

        if "tor browser" in path_lower or "tor-browser" in path_lower:
            return "tor"
        elif ".default-esr" in path_lower or "firefox esr" in path_lower:
            return "firefox_esr"
        elif "mozilla" in path_lower or "firefox" in path_lower:
            return "firefox"

        return None

    def _extract_partition_files(
        self,
        evidence_fs,
        json_files: List[Dict],
        xpi_files: List[Dict],
        output_dir: Path,
        callbacks: ExtractorCallbacks,
        manifest_data: Dict,
        progress: int,
        total_files: int
    ) -> int:
        """
        Extract files from a single partition.

        Args:
            evidence_fs: Filesystem for this partition
            json_files: List of JSON file info dicts
            xpi_files: List of XPI file info dicts
            output_dir: Output directory
            callbacks: Extractor callbacks
            manifest_data: Manifest dict to update
            progress: Current progress count
            total_files: Total file count

        Returns:
            Updated progress count
        """
        # Extract JSON files
        for file_info in json_files:
            if callbacks.is_cancelled():
                manifest_data["status"] = "cancelled"
                return progress

            progress += 1
            callbacks.on_progress(progress, total_files, f"Copying {file_info.get('filename', 'unknown')}")

            try:
                extracted = self._extract_file(evidence_fs, file_info, output_dir, callbacks)
                manifest_data["files"].append(extracted)
            except Exception as e:
                error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                manifest_data["notes"].append(error_msg)
                manifest_data["status"] = "partial"

        # Extract XPI files
        for file_info in xpi_files:
            if callbacks.is_cancelled():
                manifest_data["status"] = "cancelled"
                return progress

            progress += 1
            callbacks.on_progress(progress, total_files, f"Copying {file_info.get('filename', 'unknown')}")

            try:
                extracted = self._extract_file(evidence_fs, file_info, output_dir, callbacks)
                manifest_data["xpi_files"].append(extracted)
            except Exception as e:
                error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                manifest_data["notes"].append(error_msg)
                manifest_data["status"] = "partial"

        return progress

    def _discover_extension_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
        scan_all_partitions: bool = True
    ) -> List[Dict]:
        """
        Discover extensions.json and addons.json files from evidence.

        Returns list of file info dicts with metadata for extraction.
        """
        discovered = []
        seen_paths: Set[str] = set()

        for browser_key in browsers:
            if browser_key not in FIREFOX_BROWSERS:
                continue
            discovered.extend(
                self._discover_browser_extension_files(
                    evidence_fs, browser_key, callbacks, seen_paths, scan_all_partitions
                )
            )

        return discovered

    def _discover_browser_extension_files(
        self,
        evidence_fs,
        browser: str,
        callbacks: ExtractorCallbacks,
        seen_paths: Set[str],
        scan_all_partitions: bool
    ) -> List[Dict]:
        """Discover extension files for a specific Firefox browser."""
        files = []

        try:
            patterns = get_artifact_patterns(browser, "extensions")
        except ValueError:
            LOGGER.warning("Browser %s not in FIREFOX_BROWSERS, skipping", browser)
            return []

        for pattern in patterns:
            try:
                for path_str in evidence_fs.iter_paths(pattern):
                    # Only interested in extensions.json and addons.json
                    filename = path_str.split('/')[-1]
                    if filename not in EXTENSION_FILES:
                        continue

                    # Deduplication during discovery
                    if path_str in seen_paths:
                        continue
                    seen_paths.add(path_str)

                    profile = extract_profile_from_path(path_str)
                    partition_index = self._get_partition_index(evidence_fs, path_str)

                    file_info = {
                        "browser": browser,
                        "profile": profile,
                        "logical_path": path_str,
                        "filename": filename,
                        "artifact_type": "extensions",
                        "partition_index": partition_index,
                        "fs_type": getattr(evidence_fs, 'fs_type', None),
                    }

                    files.append(file_info)
                    callbacks.on_log(f"Found {browser} {filename} at {path_str}", "info")

            except Exception as e:
                LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return files

    def _discover_xpi_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
        scan_all_partitions: bool = True
    ) -> List[Dict]:
        """
        Discover XPI extension archive files from evidence.

        Returns list of XPI file info dicts for extraction.
        """
        discovered = []
        seen_paths: Set[str] = set()

        for browser_key in browsers:
            if browser_key not in FIREFOX_BROWSERS:
                continue

            browser_info = FIREFOX_BROWSERS[browser_key]
            profile_roots = browser_info.get("profile_roots", [])

            for root in profile_roots:
                # Search for XPI files in extensions folder
                xpi_pattern = f"{root}/*/extensions/*.xpi"
                staged_pattern = f"{root}/*/extensions/staged/*.xpi"

                for pattern in [xpi_pattern, staged_pattern]:
                    try:
                        for path_str in evidence_fs.iter_paths(pattern):
                            if path_str in seen_paths:
                                continue
                            seen_paths.add(path_str)

                            filename = path_str.split('/')[-1]
                            profile = extract_profile_from_path(path_str)
                            partition_index = self._get_partition_index(evidence_fs, path_str)

                            # Extract extension ID from XPI filename
                            # Format: {extension-id}.xpi or name.xpi
                            ext_id = filename.rsplit('.', 1)[0]

                            file_info = {
                                "browser": browser_key,
                                "profile": profile,
                                "logical_path": path_str,
                                "filename": filename,
                                "artifact_type": "xpi",
                                "extension_id": ext_id,
                                "partition_index": partition_index,
                                "fs_type": getattr(evidence_fs, 'fs_type', None),
                                "staged": "/staged/" in path_str,
                            }

                            discovered.append(file_info)
                            callbacks.on_log(f"Found {browser_key} XPI: {filename}", "info")

                    except Exception as e:
                        LOGGER.debug("XPI pattern %s failed: %s", pattern, e)

        return discovered

    def _get_partition_index(self, evidence_fs, path: str) -> Optional[int]:
        """Get partition index from evidence filesystem for a path."""
        try:
            if hasattr(evidence_fs, 'get_partition_index'):
                return evidence_fs.get_partition_index(path)
            elif hasattr(evidence_fs, 'partition_index'):
                return evidence_fs.partition_index
        except Exception:
            pass
        return None

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Copy extension file from evidence to workspace with metadata."""
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info["profile"]
            filename = file_info["filename"]
            partition_index = file_info.get("partition_index", 0) or 0

            # Create safe output filename with partition index to prevent overwrites
            safe_profile = re.sub(r'[^a-zA-Z0-9_-]', '_', profile)[:MAX_SAFE_PROFILE_LENGTH]
            output_filename = f"{browser}_{safe_profile}_p{partition_index}_{filename}"
            dest_path = output_dir / output_filename

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

            # Read and write file
            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

            # Calculate hashes
            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()
            size = len(file_content)

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
                "filename": filename,
                "artifact_type": file_info.get("artifact_type", "extensions"),
                "partition_index": partition_index,
                "fs_type": file_info.get("fs_type"),
            }

        except Exception as e:
            error_msg = f"Failed to extract {file_info.get('logical_path', 'unknown')}: {e}"
            LOGGER.error(error_msg, exc_info=True)
            callbacks.on_log(error_msg, "error")

            return {
                "copy_status": "error",
                "size_bytes": 0,
                "file_size_bytes": 0,
                "md5": None,
                "sha256": None,
                "extracted_path": None,
                "browser": file_info.get("browser"),
                "profile": file_info.get("profile"),
                "logical_path": file_info.get("logical_path"),
                "filename": file_info.get("filename"),
                "artifact_type": file_info.get("artifact_type", "extensions"),
                "partition_index": file_info.get("partition_index"),
                "fs_type": file_info.get("fs_type"),
                "error_message": str(e),
            }

    def _group_files_by_profile(
        self,
        files: List[Dict],
        output_dir: Path
    ) -> Dict[tuple, List[Dict]]:
        """Group extracted files by (browser, profile) for merging."""
        groups = {}
        for file_entry in files:
            browser = file_entry.get("browser", "unknown")
            profile = file_entry.get("profile", "unknown")
            key = (browser, profile)
            if key not in groups:
                groups[key] = []

            # Ensure extracted_path is absolute
            extracted_path = file_entry.get("extracted_path", "")
            if extracted_path and not Path(extracted_path).is_absolute():
                file_entry["extracted_path"] = str(output_dir / extracted_path)

            groups[key].append(file_entry)
        return groups

    def _create_db_record(
        self,
        ext: Dict,
        run_id: str,
        known_extensions: Dict
    ) -> Dict:
        """Create a database record from parsed extension info."""
        # Calculate permission risk
        permissions = ext.get("permissions", [])
        host_permissions = ext.get("host_permissions", [])
        risk_level = calculate_risk_level(permissions, host_permissions)

        # Convert risk level to numeric score
        risk_score_map = {"critical": 90, "high": 70, "medium": 40, "low": 10}
        risk_score = risk_score_map.get(risk_level, 0)

        # Build risk factors list
        risk_factors = []
        if risk_level == "critical":
            risk_factors.append(f"Critical risk: {risk_level}")
        elif risk_level == "high":
            risk_factors.append("High risk permissions detected")
        if "<all_urls>" in permissions:
            risk_factors.append("Has access to all URLs")

        # Firefox-specific: Check signing state
        signed_state = ext.get("signed_state")
        if signed_state is not None and is_unsigned_or_broken(signed_state):
            risk_factors.append("Extension is unsigned or signature invalid")

        # Check against known extensions
        known_match = match_known_extension(
            ext.get("extension_id", ""),
            ext.get("name", ""),
            known_extensions
        )

        return {
            "browser": ext.get("browser"),
            "profile": ext.get("profile"),
            "extension_id": ext.get("extension_id"),
            "name": ext.get("name", "Unknown"),
            "version": ext.get("version"),
            "description": self._normalize_text_field(ext.get("description")),
            "author": self._normalize_text_field(ext.get("author")),
            "homepage_url": self._normalize_text_field(ext.get("homepage_url")),
            "enabled": ext.get("enabled", 1),
            "manifest_version": ext.get("manifest_version"),
            "permissions": json.dumps(permissions) if permissions else None,
            "host_permissions": json.dumps(host_permissions) if host_permissions else None,
            "content_scripts": None,  # Not available in extensions.json
            "install_time": ext.get("install_time_utc") or ext.get("install_time"),
            "update_time": ext.get("update_time_utc") or ext.get("update_time"),
            "risk_score": risk_score,
            "risk_factors": json.dumps(risk_factors) if risk_factors else None,
            "known_category": known_match.get("category") if known_match else None,
            "run_id": run_id,
            "source_path": ext.get("source_path"),
            "partition_index": ext.get("partition_index"),
            "fs_type": ext.get("fs_type"),
            "logical_path": ext.get("logical_path"),
            "forensic_path": ext.get("forensic_path"),
            "notes": self._normalize_text_field(known_match.get("notes") if known_match else None),
        }
