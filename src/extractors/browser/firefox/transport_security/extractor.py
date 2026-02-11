"""
Firefox Transport Security Extractor

Extracts HSTS (HTTP Strict Transport Security) entries from Firefox browsers.
Firefox stores domain names in cleartext - HIGH FORENSIC VALUE!

Features:
- SiteSecurityServiceState.txt parsing
- Cleartext domain extraction (unlike Chromium)
- URL table integration
- HSTS entries table with full metadata
- Multi-partition support via file_list
- Schema warning support for unknown entry types/state values
- File collision prevention with unique naming

Data Format:
- Firefox stores HSTS in SiteSecurityServiceState.txt (tab-separated)
- Format: host:HSTS<tab>score<tab>last_access<tab>data
- Data field: expiry_ms,state,include_subdomains

Forensic Value:
- Contains CLEARTEXT domain names (HIGH VALUE)
- Entries persist after "Clear History"
- Reveals secure connections even when browsing history deleted

Changes:
- Multi-partition discovery via file_list
- Schema warnings for unknown entry types and state values
- File collision prevention with partition/user/profile naming
- Refactored to use _schemas.py and _parsers.py modules
- Fixed regex for host:type parsing (IPv6 compatible)
- Fixed delete pattern to use exact run_id match
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Set, TYPE_CHECKING

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import BrowserConfigWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from ...._shared.extraction_warnings import ExtractionWarningCollector
from .._patterns import FIREFOX_BROWSERS, get_artifact_patterns
from ._schemas import (
    KNOWN_ENTRY_TYPES,
    KNOWN_STATE_VALUES,
    STATE_TO_MODE,
    WARNING_CATEGORY,
    WARNING_ARTIFACT_TYPE,
)
from ._parsers import (
    parse_transport_security_file,
    days_since_epoch_to_iso8601,
    ms_to_unix_seconds,
    unix_to_iso8601,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_urls,
    insert_hsts_entries,
    delete_hsts_by_run,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)

if TYPE_CHECKING:
    from core.evidence_fs import EvidenceFS

LOGGER = get_logger("extractors.browser.firefox.transport_security")


class FirefoxTransportSecurityExtractor(BaseExtractor):
    """
    Extract HSTS/Transport Security entries from Firefox browsers.

    Parses SiteSecurityServiceState.txt containing cleartext domain names.
    Supports Firefox, Tor Browser.

    HIGH FORENSIC VALUE: Unlike Chromium, Firefox stores domain names
    in cleartext, providing direct evidence of visited secure sites.

    Multi-partition support, schema warnings, collision prevention.
    """

    SUPPORTED_BROWSERS = list(FIREFOX_BROWSERS.keys())

    # Track extracted filenames to prevent collisions within a run
    _extracted_filenames: Set[str]

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="firefox_transport_security",
            display_name="Firefox HSTS",
            description="Extract HSTS entries from Firefox (cleartext domains - HIGH VALUE)",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True
        )

    def can_run_extraction(self, evidence_fs: "EvidenceFS") -> tuple[bool, str]:
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
            status_text = f"Firefox HSTS\nFiles extracted: {file_count}\nRun ID: {data.get('run_id', 'N/A')}"
        else:
            status_text = "Firefox HSTS\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "firefox_transport_security"

    # ─────────────────────────────────────────────────────────────────
    # Extraction
    # ─────────────────────────────────────────────────────────────────

    def run_extraction(
        self,
        evidence_fs: "EvidenceFS",
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Firefox SiteSecurityServiceState.txt files from evidence.

        Uses file_list for multi-partition discovery when available.
        Falls back to single-partition scan for mounted filesystems.
        """
        callbacks.on_step("Initializing Firefox transport security extraction")

        # Reset collision tracking for this run
        self._extracted_filenames = set()

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Firefox transport security extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.1.0",  # Updated for multi-partition
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

        callbacks.on_step("Scanning for Firefox SiteSecurityServiceState.txt files")

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        browsers_to_search = [b for b in browsers_to_search if b in self.SUPPORTED_BROWSERS]

        # Check if file_list is available for multi-partition discovery
        file_list_available = check_file_list_available(evidence_conn, evidence_id) if evidence_conn else False

        if file_list_available:
            # Multi-partition discovery via file_list
            callbacks.on_step("Discovering files via file_list (multi-partition)")
            manifest_data["multi_partition"] = True
            ts_files = self._discover_files_multi_partition(
                evidence_fs,
                evidence_conn,
                evidence_id,
                browsers_to_search,
                callbacks,
            )
        else:
            # Check if this is an E01 image - require file_list for E01
            is_e01 = self._is_e01_image(evidence_fs)
            if is_e01:
                error_msg = (
                    "E01 image detected but file_list not available. "
                    "Please run File List extractor first to enable multi-partition discovery."
                )
                callbacks.on_error("file_list required", error_msg)
                manifest_data["status"] = "error"
                manifest_data["notes"].append(error_msg)
                LOGGER.error(error_msg)

                # Write manifest with error
                manifest_path = output_dir / "manifest.json"
                manifest_path.write_text(json.dumps(manifest_data, indent=2))

                if stats:
                    stats.finish_run(evidence_id, self.metadata.name, status="error")

                return False

            # Single-partition fallback for mounted filesystems
            callbacks.on_step("Discovering files (single partition)")
            ts_files = self._discover_files_single_partition(
                evidence_fs,
                browsers_to_search,
                callbacks,
            )

        # Report discovered files (even if 0)
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(ts_files))

        if not ts_files:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No SiteSecurityServiceState.txt files found")
            LOGGER.info("No Firefox SiteSecurityServiceState.txt files found")
        else:
            callbacks.on_progress(0, len(ts_files), "Copying SiteSecurityServiceState files")

            for i, file_info in enumerate(ts_files):
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    break

                try:
                    callbacks.on_progress(
                        i + 1,
                        len(ts_files),
                        f"Copying {file_info['browser']} HSTS data (partition {file_info.get('partition_index', 0)})"
                    )

                    extracted_file = self._extract_file(
                        evidence_fs,
                        evidence_conn,
                        evidence_id,
                        file_info,
                        output_dir,
                        callbacks,
                    )
                    manifest_data["files"].append(extracted_file)

                except Exception as e:
                    error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                    LOGGER.error(error_msg, exc_info=True)
                    manifest_data["notes"].append(error_msg)
                    manifest_data["status"] = "partial"

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

        # Finish stats tracking
        if stats:
            status = "cancelled" if manifest_data["status"] == "cancelled" else "success" if manifest_data["status"] == "ok" else "partial"
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        LOGGER.info(
            "Firefox transport security extraction complete: %d files, status=%s",
            len(manifest_data["files"]),
            manifest_data["status"],
        )

        return manifest_data["status"] != "error"

    # ─────────────────────────────────────────────────────────────────
    # Ingestion
    # ─────────────────────────────────────────────────────────────────

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """Parse extracted SiteSecurityServiceState.txt and ingest into database.

        Inserts records into both urls table (backward compatibility) and
        hsts_entries table (full metadata). Uses ExtractionWarningCollector
        for schema warnings about unknown entry types and state values.
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"urls": 0, "hsts_entries": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])

        # Continue statistics tracking with manifest's run_id
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, hsts_entries=0)
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"urls": 0, "hsts_entries": 0}

        # Initialize warning collector for schema warnings
        warning_collector = ExtractionWarningCollector(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            extractor_name=self.metadata.name,
            run_id=run_id,
        )

        total_urls = 0
        total_hsts = 0

        # Clear previous data for this run (exact run_id match)
        self._clear_previous_run(evidence_conn, run_id)

        callbacks.on_progress(0, len(files), "Parsing SiteSecurityServiceState files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            if file_entry.get("copy_status") == "error":
                callbacks.on_log(
                    f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}",
                    "warning"
                )
                continue

            partition_idx = file_entry.get("partition_index", 0)
            callbacks.on_progress(
                i + 1,
                len(files),
                f"Parsing {file_entry['browser']} HSTS data (partition {partition_idx})"
            )

            try:
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry["browser"],
                    artifact_type="transport_security",
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

                # Parse file with warning collection
                entries = self._parse_file_with_warnings(
                    db_path,
                    file_entry,
                    warning_collector,
                )

                if not entries:
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="ok",
                        records_parsed=0,
                    )
                    continue

                # Insert into urls table (for backward compatibility)
                url_count = self._insert_urls(
                    entries,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                )

                # Insert into dedicated hsts_entries table with full metadata
                hsts_count = self._insert_hsts_entries(
                    entries,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                )

                total_urls += url_count
                total_hsts += hsts_count

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=url_count + hsts_count,
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

        # Flush any remaining warnings
        warning_collector.flush()

        evidence_conn.commit()

        # Report ingestion statistics
        if stats:
            stats.report_ingested(
                evidence_id,
                self.metadata.name,
                records=total_urls + total_hsts,
                hsts_entries=total_hsts
            )
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        callbacks.on_log(
            f"Inserted {total_urls} URLs and {total_hsts} HSTS entries (cleartext domains)",
            "info"
        )

        return {"urls": total_urls, "hsts_entries": total_hsts}

    # ─────────────────────────────────────────────────────────────────
    # Discovery Methods
    # ─────────────────────────────────────────────────────────────────

    def _discover_files_multi_partition(
        self,
        evidence_fs: "EvidenceFS",
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> List[Dict]:
        """Discover transport security files across all partitions via file_list.

        Uses discover_from_file_list to find SiteSecurityServiceState.txt
        across all partitions in the evidence.
        """
        ts_files = []

        for browser_key in browsers:
            if browser_key not in FIREFOX_BROWSERS:
                continue

            patterns = get_artifact_patterns(browser_key, "transport_security")
            display_name = FIREFOX_BROWSERS[browser_key]["display_name"]

            for pattern in patterns:
                try:
                    # Use file_list discovery for multi-partition support
                    discovered = discover_from_file_list(
                        evidence_conn=evidence_conn,
                        evidence_id=evidence_id,
                        pattern=pattern,
                    )

                    for item in discovered:
                        # Extract user and profile from path
                        user, profile = self._extract_user_profile_from_path(item["path"])

                        ts_files.append({
                            "logical_path": item["path"],
                            "browser": browser_key,
                            "user": user,
                            "profile": profile,
                            "file_type": "firefox",
                            "artifact_type": "transport_security",
                            "display_name": display_name,
                            "partition_index": item.get("partition_index", 0),
                            "fs_type": item.get("fs_type"),
                            "forensic_path": item.get("forensic_path"),
                            "file_list_id": item.get("id"),
                        })

                        callbacks.on_log(
                            f"Found {browser_key} SiteSecurityServiceState.txt: "
                            f"partition {item.get('partition_index', 0)}, {item['path']}",
                            "info"
                        )

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return ts_files

    def _discover_files_single_partition(
        self,
        evidence_fs: "EvidenceFS",
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> List[Dict]:
        """Discover transport security files in single partition (mounted FS fallback).

        Used when file_list is not available (mounted filesystem).
        """
        ts_files = []

        for browser_key in browsers:
            if browser_key not in FIREFOX_BROWSERS:
                continue

            patterns = get_artifact_patterns(browser_key, "transport_security")
            display_name = FIREFOX_BROWSERS[browser_key]["display_name"]

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        user, profile = self._extract_user_profile_from_path(path_str)

                        ts_files.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "user": user,
                            "profile": profile,
                            "file_type": "firefox",
                            "artifact_type": "transport_security",
                            "display_name": display_name,
                            "partition_index": 0,
                            "fs_type": getattr(evidence_fs, 'fs_type', 'unknown'),
                        })

                        callbacks.on_log(
                            f"Found {browser_key} SiteSecurityServiceState.txt: {path_str}",
                            "info"
                        )

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return ts_files

    def _extract_user_profile_from_path(self, path: str) -> tuple[str, str]:
        """Extract user and profile from Firefox path.

        Returns:
            Tuple of (user, profile) - defaults to ("unknown", "Default") if not found
        """
        import re

        user = "unknown"
        profile = "Default"

        # Windows: C:/Users/<user>/AppData/Roaming/Mozilla/Firefox/Profiles/<profile>/
        win_match = re.search(r'/Users/([^/]+)/AppData/', path, re.IGNORECASE)
        if win_match:
            user = win_match.group(1)

        # Linux: /home/<user>/.mozilla/firefox/<profile>/
        linux_match = re.search(r'/home/([^/]+)/\.mozilla/', path, re.IGNORECASE)
        if linux_match:
            user = linux_match.group(1)

        # macOS: /Users/<user>/Library/Application Support/Firefox/Profiles/<profile>/
        mac_match = re.search(r'/Users/([^/]+)/Library/', path, re.IGNORECASE)
        if mac_match:
            user = mac_match.group(1)

        # Profile extraction - Firefox uses random prefix like "abc123.default"
        profile_patterns = [
            r'Profiles/([^/]+)/',
            r'\.mozilla/firefox/([^/]+)/',
            r'Firefox/Profiles/([^/]+)/',
        ]

        for pattern in profile_patterns:
            match = re.search(pattern, path, re.IGNORECASE)
            if match:
                profile = match.group(1)
                break

        return user, profile

    # ─────────────────────────────────────────────────────────────────
    # File Extraction
    # ─────────────────────────────────────────────────────────────────

    def _extract_file(
        self,
        evidence_fs: "EvidenceFS",
        evidence_conn,
        evidence_id: int,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks,
    ) -> Dict:
        """Copy file from evidence to workspace with collision-safe naming.

        Filename format: p{partition}_{browser}_{user}_{profile}_SiteSecurityServiceState.txt
        If collision detected, appends counter: _2, _3, etc.
        """
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            user = file_info.get("user", "unknown")
            profile = file_info.get("profile", "Default")
            partition_idx = file_info.get("partition_index", 0)

            # Sanitize components for filename
            safe_user = self._sanitize_filename_component(user)
            safe_profile = self._sanitize_filename_component(profile)

            # Build base filename with partition prefix
            base_name = f"p{partition_idx}_{browser}_{safe_user}_{safe_profile}_SiteSecurityServiceState.txt"

            # Handle collisions with counter
            filename = base_name
            counter = 2
            while filename in self._extracted_filenames:
                name_part = base_name.rsplit('.', 1)[0]
                filename = f"{name_part}_{counter}.txt"
                counter += 1

            self._extracted_filenames.add(filename)
            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {filename}", "info")

            # Read file content - use partition-aware reading if available
            file_content = self._read_file_from_partition(
                evidence_fs,
                evidence_conn,
                evidence_id,
                file_info,
            )

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
                "user": user,
                "profile": profile,
                "file_type": "firefox",
                "logical_path": source_path,
                "artifact_type": "transport_security",
                "partition_index": partition_idx,
                "fs_type": file_info.get("fs_type"),
                "forensic_path": file_info.get("forensic_path"),
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
                "user": file_info.get("user"),
                "profile": file_info.get("profile"),
                "file_type": file_info.get("file_type"),
                "logical_path": file_info.get("logical_path"),
                "partition_index": file_info.get("partition_index"),
                "error_message": str(e),
            }

    def _read_file_from_partition(
        self,
        evidence_fs: "EvidenceFS",
        evidence_conn,
        evidence_id: int,
        file_info: Dict,
    ) -> bytes:
        """Read file content, handling multi-partition access.

        For multi-partition images, opens the specific partition.
        For single partition, reads directly from evidence_fs.
        """
        partition_idx = file_info.get("partition_index", 0)
        source_path = file_info["logical_path"]

        # If partition index > 0, need to open that specific partition
        if partition_idx > 0 and evidence_conn:
            try:
                ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
                if ewf_paths:
                    partition_fs = open_partition_for_extraction(
                        ewf_paths=ewf_paths,
                        partition_index=partition_idx,
                    )
                    if partition_fs:
                        try:
                            return partition_fs.read_file(source_path)
                        finally:
                            partition_fs.close()
            except Exception as e:
                LOGGER.warning(
                    "Failed to open partition %d, falling back to default: %s",
                    partition_idx, e
                )

        # Default: read from primary evidence_fs
        return evidence_fs.read_file(source_path)

    def _sanitize_filename_component(self, component: str) -> str:
        """Sanitize a string for use in filename."""
        import re
        # Replace unsafe characters with underscore
        safe = re.sub(r'[^\w\-.]', '_', component)
        # Collapse multiple underscores
        safe = re.sub(r'_+', '_', safe)
        # Remove leading/trailing underscores
        safe = safe.strip('_')
        return safe or "unknown"

    # ─────────────────────────────────────────────────────────────────
    # Parsing and Ingestion
    # ─────────────────────────────────────────────────────────────────

    def _parse_file_with_warnings(
        self,
        file_path: Path,
        file_entry: Dict,
        warning_collector: ExtractionWarningCollector,
    ) -> List[Dict]:
        """Parse SiteSecurityServiceState.txt with warning collection.

        Uses the parse_transport_security_file function from _parsers module.
        """
        if not file_path.exists():
            LOGGER.warning("SiteSecurityServiceState.txt not found: %s", file_path)
            return []

        try:
            content = file_path.read_text(encoding='utf-8', errors='replace')
        except Exception as e:
            LOGGER.warning("Failed to read SiteSecurityServiceState.txt: %s", e)
            return []

        source_file = file_entry.get("logical_path", str(file_path))

        # Use parser module for parsing with warning collection
        entries = parse_transport_security_file(
            content=content,
            source_file=source_file,
            warning_collector=warning_collector,
        )

        return entries

    def _insert_urls(
        self,
        entries: List[Dict],
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
    ) -> int:
        """Insert parsed entries into urls table.

        NO deduplication - inserts all entries.
        """
        browser = file_entry["browser"]
        profile = file_entry.get("profile", "Default")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        records = []

        for entry in entries:
            domain = entry.get("host", "")
            if not domain:
                continue

            # Convert to URL
            url = f"https://{domain}/"

            # Use last_access for last_seen (more forensically accurate)
            last_access_iso = entry.get("last_access_iso")
            expiry_iso = unix_to_iso8601(entry.get("expiry_seconds")) if entry.get("expiry_seconds") else None

            # Build detailed notes with HSTS metadata
            notes_parts = [f"HSTS entry from Firefox SiteSecurityServiceState.txt (profile: {profile})"]
            if entry.get("mode"):
                notes_parts.append(f"mode: {entry['mode']}")
            if entry.get("include_subdomains"):
                notes_parts.append("includeSubdomains: true")
            if expiry_iso:
                notes_parts.append(f"expiry: {expiry_iso}")

            record = {
                "url": url,
                "domain": domain,
                "scheme": "https",
                "discovered_by": discovered_by,
                "first_seen_utc": None,  # Not available from HSTS
                "last_seen_utc": last_access_iso,
                "source_path": file_entry["logical_path"],
                "notes": "; ".join(notes_parts),
                "run_id": run_id,
            }
            records.append(record)

        if records:
            insert_urls(evidence_conn, evidence_id, records)

        return len(records)

    def _insert_hsts_entries(
        self,
        entries: List[Dict],
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
    ) -> int:
        """Insert parsed entries into hsts_entries table.

        Firefox stores cleartext domains, so decoded_host is the actual domain.
        hashed_host is set to a placeholder since Firefox doesn't hash.
        """
        browser = file_entry["browser"]
        profile = file_entry.get("profile", "Default")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        records = []

        for entry in entries:
            domain = entry.get("host", "")
            if not domain:
                continue

            record = {
                "browser": browser,
                "profile": profile,
                # Firefox stores cleartext - use marker for hashed_host
                "hashed_host": f"firefox_cleartext:{domain}",
                "decoded_host": domain,
                "decode_method": "cleartext",  # Firefox stores cleartext natively
                "sts_observed": entry.get("expiry_seconds"),  # Unix seconds
                "expiry": entry.get("expiry_seconds"),  # Unix seconds
                "mode": entry.get("mode", "force-https"),
                "include_subdomains": entry.get("include_subdomains", 0),
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry.get("logical_path"),
                "forensic_path": file_entry.get("forensic_path"),
                "notes": f"last_access_days={entry.get('last_access_days')}, state={entry.get('state')}",
            }
            records.append(record)

        if records:
            insert_hsts_entries(evidence_conn, evidence_id, records)

        return len(records)

    def _clear_previous_run(self, evidence_conn, run_id: str) -> None:
        """Clear URL and HSTS data from a previous run.

        Uses exact run_id match to avoid clearing data from other runs.
        """
        try:
            # Delete URLs with exact run_id match
            cursor = evidence_conn.execute(
                "DELETE FROM urls WHERE run_id = ?",
                (run_id,)
            )
            url_deleted = cursor.rowcount

            # Delete from hsts_entries table with exact run_id
            hsts_deleted = delete_hsts_by_run(evidence_conn, None, run_id)

            if url_deleted > 0 or hsts_deleted > 0:
                LOGGER.info(
                    "Cleared %d URLs and %d HSTS entries from previous run %s",
                    url_deleted, hsts_deleted, run_id
                )
        except Exception as e:
            LOGGER.warning("Failed to clear previous run data: %s", e)

    # ─────────────────────────────────────────────────────────────────
    # Helper Methods
    # ─────────────────────────────────────────────────────────────────

    def _generate_run_id(self) -> str:
        """Generate run ID: ts_firefox_{timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ts_firefox_{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs: "EvidenceFS") -> dict:
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

    def _is_e01_image(self, evidence_fs: "EvidenceFS") -> bool:
        """Check if evidence_fs is backed by an E01 image."""
        try:
            # Check class name for PyEwfTskFS
            class_name = type(evidence_fs).__name__
            if "Ewf" in class_name or "E01" in class_name:
                return True

            # Check source path extension
            source_path = getattr(evidence_fs, 'source_path', None)
            if source_path:
                path_str = str(source_path).lower()
                if path_str.endswith('.e01') or '.e0' in path_str:
                    return True

            return False
        except Exception:
            return False
