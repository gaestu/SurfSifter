"""
Firefox Sessions Extractor

Extracts and ingests browser session/tab data from Firefox sessionstore.jsonlz4.

Features:
- sessionstore.jsonlz4 parsing (Mozilla LZ4 compressed JSON)
- Window and tab state reconstruction
- Navigation history within tabs
- Closed tab recovery
- Form data extraction (forensically valuable user input)
- Multi-partition support via file_list discovery
- Schema warning support for unknown JSON keys
- StatisticsCollector integration for run tracking

Data Format:
- Firefox stores sessions in Mozilla LZ4 format (mozLz4\\x00 header)
- JSON structure after decompression
- Contains windows, tabs, closed windows/tabs

Dependencies:
- lz4 module for decompression

Forensic Value:
- Complete tab history with navigation entries
- Closed tab recovery
- Form data (user-entered text in forms)
- Session files survive browser crashes

 Changes:
- Split into _schemas.py and _parsers.py for maintainability
- Added multi-partition support via file_list discovery
- Fixed filename collision with partition_index + mini-hash
- Added ExtractionWarningCollector for unknown JSON keys
- Added form data extraction
- Removed URL deduplication (all URLs preserved with timestamps)
"""

from __future__ import annotations

import hashlib
import json
import re
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
)
from .._patterns import FIREFOX_BROWSERS, get_artifact_patterns
from ._parsers import (
    decompress_session_file,
    parse_session_data,
    collect_all_urls,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_session_windows,
    insert_session_tabs,
    insert_session_tab_histories,
    insert_closed_tabs,
    insert_session_form_datas,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    delete_sessions_by_run,
    insert_urls,
)
from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.browser.firefox.sessions")

# Profile name extraction patterns (moved to module level)
PROFILE_PATTERNS = [
    re.compile(r"Profiles/([^/]+)/"),
    re.compile(r"\.mozilla/firefox/([^/]+)/"),
    re.compile(r"TorBrowser/Data/Browser/([^/]+)/"),
    re.compile(r"profile\.default"),
]


class FirefoxSessionsExtractor(BaseExtractor):
    """
    Extract browser session/tab data from Firefox sessionstore.jsonlz4.

    Parses Mozilla LZ4-compressed JSON session files with full tab history.
    Supports Firefox, Tor Browser.

    Features:
    - Multi-partition extraction via file_list
    - Form data extraction
    - Schema warning support
    """

    SUPPORTED_BROWSERS = list(FIREFOX_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="firefox_sessions",
            display_name="Firefox Session Restore",
            description="Extract session tabs, form data from Firefox/Tor sessionstore.jsonlz4",
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
        """Return configuration widget (browser selection)."""
        return BrowserConfigWidget(parent, supported_browsers=self.SUPPORTED_BROWSERS)

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
            status_text = f"Firefox Sessions\nFiles extracted: {file_count}\nRun ID: {data.get('run_id', 'N/A')}"
        else:
            status_text = "Firefox Sessions\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "firefox_sessions"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Firefox session files from evidence."""
        callbacks.on_step("Initializing Firefox sessions extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Firefox sessions extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.1.0",  # Bumped for multi-partition support
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_extraction_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "files": [],
            "status": "ok",
            "notes": [],
            "multi_partition": False,
        }

        callbacks.on_step("Scanning for Firefox session files")

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        browsers_to_search = [b for b in browsers_to_search if b in self.SUPPORTED_BROWSERS]

        # Try multi-partition discovery first, fall back to single partition
        files_by_partition = self._discover_files_multi_partition(
            evidence_fs, evidence_conn, evidence_id, browsers_to_search, callbacks
        )

        total_files = sum(len(files) for files in files_by_partition.values())
        manifest_data["multi_partition"] = len(files_by_partition) > 1

        # Report discovery count
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=total_files)

        if total_files == 0:
            manifest_data["notes"].append("No session files found")
            LOGGER.info("No Firefox session files found")
        else:
            callbacks.on_progress(0, total_files, "Copying session files")

            # Get EWF paths for multi-partition extraction
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)

            file_idx = 0
            for partition_idx, files in files_by_partition.items():
                # Use appropriate filesystem for this partition
                if len(files_by_partition) > 1 and ewf_paths:
                    # Multi-partition: open specific partition
                    try:
                        with open_partition_for_extraction(ewf_paths, partition_idx) as fs_to_use:
                            for file_info in files:
                                if callbacks.is_cancelled():
                                    manifest_data["status"] = "cancelled"
                                    break

                                file_idx += 1
                                callbacks.on_progress(file_idx, total_files, f"Copying {file_info['browser']} session (partition {partition_idx})")

                                extracted = self._extract_file(fs_to_use, file_info, output_dir, callbacks)
                                manifest_data["files"].append(extracted)
                    except Exception as e:
                        error_msg = f"Failed to open partition {partition_idx}: {e}"
                        LOGGER.error(error_msg, exc_info=True)
                        manifest_data["notes"].append(error_msg)
                        manifest_data["status"] = "partial"
                else:
                    # Single partition: use existing filesystem
                    for file_info in files:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            break

                        file_idx += 1
                        callbacks.on_progress(file_idx, total_files, f"Copying {file_info['browser']} session")

                        try:
                            extracted = self._extract_file(evidence_fs, file_info, output_dir, callbacks)
                            manifest_data["files"].append(extracted)
                        except Exception as e:
                            error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            manifest_data["notes"].append(error_msg)
                            manifest_data["status"] = "partial"
                            if stats:
                                stats.report_failed(evidence_id, self.metadata.name, files=1)

        # Finish statistics tracking
        if stats:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            stats.finish_run(evidence_id, self.metadata.name, status)

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
        )

        LOGGER.info(
            "Firefox sessions extraction complete: %d files, status=%s",
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
        """Parse extracted session files and ingest into database."""
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"windows": 0, "tabs": 0, "history": 0, "closed_tabs": 0, "urls": 0, "form_data": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Start statistics tracking
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0, windows=0, tabs=0, history=0, closed_tabs=0, urls=0, form_data=0
                )
                stats.finish_run(evidence_id, self.metadata.name, "success")
            return {"windows": 0, "tabs": 0, "history": 0, "closed_tabs": 0, "urls": 0, "form_data": 0}

        totals = {
            "windows": 0,
            "tabs": 0,
            "history": 0,
            "closed_tabs": 0,
            "urls": 0,
            "form_data": 0,
        }

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
                    db_path = output_dir / db_path

                counts = self._parse_session_file(
                    db_path,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                    callbacks,
                    warning_collector=warning_collector,
                )

                for key in totals:
                    totals[key] += counts.get(key, 0)

                total_records = sum(counts.values())
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=total_records,
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
            LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)

        evidence_conn.commit()

        total_all = sum(totals.values())

        if stats:
            stats.report_ingested(evidence_id, self.metadata.name, records=total_all, **totals)
            stats.finish_run(evidence_id, self.metadata.name, "success")

        return totals

    # ─────────────────────────────────────────────────────────────────
    # Helper Methods
    # ─────────────────────────────────────────────────────────────────

    def _generate_run_id(self) -> str:
        """Generate run ID: sess_firefox_{timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"sess_firefox_{timestamp}_{unique_id}"

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

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, List[Dict]]:
        """
        Discover Firefox session files across ALL partitions using file_list.

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
        if evidence_conn:
            available, count = check_file_list_available(evidence_conn, evidence_id)
        else:
            available, count = False, 0

        if not available:
            callbacks.on_log(
                "file_list empty, falling back to single-partition discovery",
                "info"
            )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")

        # Build path patterns for file_list query
        path_patterns = []
        filename_patterns = [
            "sessionstore.jsonlz4",
            "recovery.jsonlz4",
            "recovery.baklz4",
            "previous.jsonlz4",
            "upgrade.jsonlz4*",
            "sessionstore.js",
            "recovery.js",
            "previous.js",
        ]

        for browser in browsers:
            if browser not in FIREFOX_BROWSERS:
                continue
            if browser == "firefox":
                path_patterns.extend([
                    "%Mozilla%Firefox%Profiles%",
                    "%.mozilla%firefox%",
                ])
            elif browser == "tor":
                path_patterns.extend([
                    "%Tor Browser%TorBrowser%Data%Browser%",
                    "%tor-browser%Browser%TorBrowser%",
                ])

        # Query file_list
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=filename_patterns,
            path_patterns=path_patterns,
        )

        if result.is_empty:
            callbacks.on_log("No matches in file_list, falling back to iter_paths", "info")
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        callbacks.on_log(result.get_partition_summary(), "info")

        # Convert FileListMatch to file info dicts
        files_by_partition: Dict[int, List[Dict]] = {}

        for partition_idx, matches in result.matches_by_partition.items():
            files_by_partition[partition_idx] = []

            for match in matches:
                # Determine browser from path
                browser = self._detect_browser_from_path(match.file_path)
                if browser not in browsers:
                    continue

                profile = self._extract_profile_from_path(match.file_path)
                file_type = self._classify_session_file(match.file_path)

                files_by_partition[partition_idx].append({
                    "logical_path": match.file_path,
                    "browser": browser,
                    "profile": profile,
                    "file_type": file_type,
                    "artifact_type": "sessions",
                    "display_name": FIREFOX_BROWSERS[browser]["display_name"],
                    "partition_index": partition_idx,
                    "inode": match.inode,
                })

                callbacks.on_log(f"Found {browser} session: {match.file_path} (partition {partition_idx})", "info")

        return files_by_partition

    def _discover_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """Scan evidence for Firefox session files (single partition fallback)."""
        session_files = []

        for browser_key in browsers:
            if browser_key not in FIREFOX_BROWSERS:
                continue

            patterns = get_artifact_patterns(browser_key, "sessions")
            display_name = FIREFOX_BROWSERS[browser_key]["display_name"]
            partition_index = getattr(evidence_fs, 'partition_index', 0)

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        profile = self._extract_profile_from_path(path_str)
                        file_type = self._classify_session_file(path_str)

                        session_files.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "profile": profile,
                            "file_type": file_type,
                            "artifact_type": "sessions",
                            "display_name": display_name,
                            "partition_index": partition_index,
                        })

                        callbacks.on_log(f"Found {browser_key} session: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return session_files

    def _detect_browser_from_path(self, path: str) -> str:
        """Detect browser type from file path."""
        path_lower = path.lower()

        if "tor browser" in path_lower or "tor-browser" in path_lower or "torbrowser" in path_lower:
            return "tor"
        elif "firefox" in path_lower or ".mozilla" in path_lower:
            return "firefox"

        return "firefox"  # Default

    def _classify_session_file(self, path: str) -> str:
        """Classify session file type based on filename."""
        filename = path.split('/')[-1].lower()

        if filename == "sessionstore.jsonlz4":
            return "sessionstore_jsonlz4"
        elif filename == "recovery.jsonlz4":
            return "recovery_jsonlz4"
        elif filename == "recovery.baklz4":
            return "recovery_baklz4"
        elif filename == "previous.jsonlz4":
            return "previous_jsonlz4"
        elif filename.startswith("upgrade.jsonlz4"):
            return "upgrade_jsonlz4"
        elif filename == "sessionstore.js":
            return "sessionstore_js"
        elif filename == "recovery.js":
            return "recovery_js"
        elif filename == "previous.js":
            return "previous_js"
        elif filename.endswith(".jsonlz4"):
            return "jsonlz4"
        elif filename.endswith(".baklz4"):
            return "baklz4"
        else:
            return "unknown"

    def _extract_profile_from_path(self, path: str) -> str:
        """Extract Firefox profile name from file path."""
        for pattern in PROFILE_PATTERNS:
            match = pattern.search(path)
            if match:
                return match.group(1) if match.lastindex else "default"

        return "Default"

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Copy file from evidence to workspace with collision-safe naming."""
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info["profile"]
            file_type = file_info["file_type"]
            partition_index = file_info.get("partition_index", 0)

            safe_profile = profile.replace(' ', '_').replace('/', '_').replace('.', '_')

            # Create collision-safe filename with partition index and mini-hash
            # Mini-hash ensures uniqueness for same browser/profile in different locations
            path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]

            # For upgrade files, preserve the timestamp suffix
            source_filename = source_path.split('/')[-1]
            if file_type == "upgrade_jsonlz4" and "-" in source_filename:
                timestamp_part = source_filename.split("-", 1)[1] if "-" in source_filename else ""
                filename = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_{file_type}_{timestamp_part}"
            else:
                filename = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_{file_type}"

            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

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
                "browser": file_info.get("browser"),
                "profile": file_info.get("profile"),
                "file_type": file_info.get("file_type"),
                "logical_path": file_info.get("logical_path"),
                "partition_index": file_info.get("partition_index", 0),
                "error_message": str(e),
            }

    def _parse_session_file(
        self,
        file_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, int]:
        """Parse Firefox session file and insert records."""
        counts = {"windows": 0, "tabs": 0, "history": 0, "closed_tabs": 0, "urls": 0, "form_data": 0}

        file_type = file_entry.get("file_type", "")
        browser = file_entry["browser"]
        profile = file_entry.get("profile", "default")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        # Decompress and parse session file
        session_data = decompress_session_file(file_path, file_type, warning_collector)

        if not session_data:
            LOGGER.warning("No session data parsed from %s", file_path)
            return counts

        # Parse session data into records
        parsed = parse_session_data(
            session_data,
            file_entry,
            run_id,
            evidence_id,
            browser,
            profile,
            discovered_by,
            warning_collector,
        )

        # Insert window records
        if parsed["windows"]:
            counts["windows"] = insert_session_windows(evidence_conn, evidence_id, parsed["windows"])

        # Insert tab records
        if parsed["tabs"]:
            counts["tabs"] = insert_session_tabs(evidence_conn, evidence_id, parsed["tabs"])

            # Resolve tab_ids for history records
            if parsed["history"]:
                cursor = evidence_conn.execute(
                    "SELECT id, window_id, tab_index FROM session_tabs WHERE evidence_id = ? AND run_id = ?",
                    (evidence_id, run_id)
                )
                tab_id_map = {(row[1], row[2]): row[0] for row in cursor.fetchall()}

                for hr in parsed["history"]:
                    key = (hr.pop("_window_id", None), hr.pop("_tab_index", None))
                    hr["tab_id"] = tab_id_map.get(key)

        # Insert history records
        if parsed["history"]:
            counts["history"] = insert_session_tab_histories(evidence_conn, evidence_id, parsed["history"])

        # Insert closed tab records
        if parsed["closed_tabs"]:
            counts["closed_tabs"] = insert_closed_tabs(evidence_conn, evidence_id, parsed["closed_tabs"])

        # Insert form data records
        if parsed["form_data"]:
            counts["form_data"] = self._insert_form_data(evidence_conn, evidence_id, parsed["form_data"])

        # Collect ALL URLs (no deduplication) and insert
        url_records = collect_all_urls(
            parsed["tabs"],
            parsed["history"],
            parsed["closed_tabs"],
            browser,
            profile,
            run_id,
            discovered_by,
            file_entry,
        )
        if url_records:
            counts["urls"] = insert_urls(evidence_conn, evidence_id, url_records)

        return counts

    def _insert_form_data(
        self,
        evidence_conn,
        evidence_id: int,
        form_records: List[Dict],
    ) -> int:
        """Insert form data records into database using helper function.

        Uses the insert_session_form_datas helper for batch insertion.
        The helper handles the session_form_data table schema.
        """
        if not form_records:
            return 0

        try:
            return insert_session_form_datas(evidence_conn, evidence_id, form_records)
        except Exception as e:
            LOGGER.warning("Failed to insert form data: %s", e)
            return 0
