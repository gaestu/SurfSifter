"""
Firefox Autofill Extractor

Extracts and ingests browser autofill data from Firefox-based browsers
(Firefox, Firefox ESR, Tor Browser) with full forensic provenance.

Features:
- Form autofill entries (formhistory.sqlite - moz_formhistory table)
- Deleted form history (moz_deleted_formhistory)
- Saved credentials (logins.json - encrypted passwords stored for forensic record)
- Legacy credentials (signons.sqlite - Firefox < 32)
- NSS key databases (key4.db, key3.db - copied for optional decryption)
- Multi-partition discovery - scans all partitions via file_list
- Schema warning support for unknown tables/columns/keys

Data Format:
- formhistory.sqlite: moz_formhistory table (PRTime microseconds)
- formhistory.sqlite: moz_deleted_formhistory table (PRTime microseconds)
- logins.json: encrypted credentials JSON (milliseconds since epoch)
- signons.sqlite: moz_logins table (PRTime microseconds) - legacy
- key4.db: Modern NSS key store (Firefox 58+) - PBKDF2 + AES-256-CBC
- key3.db: Legacy NSS key store (Firefox < 58) - BerkeleyDB + triple-DES

This is the canonical location for Firefox autofill extraction.
For backward compatibility, the unified extractor is available at:
- extractors.autofill (handles both Chromium and Firefox)
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
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
    check_file_list_available,
)
from .._patterns import (
    FIREFOX_BROWSERS,
    get_artifact_patterns,
    extract_profile_from_path,
    detect_browser_from_path,
    get_browser_display_name,
    get_all_browsers,
)
from ._schemas import classify_autofill_file
from ._parsers import (
    parse_moz_formhistory,
    parse_moz_deleted_formhistory,
    parse_logins_json,
    parse_moz_logins_signons,
)
from extractors._shared.extraction_warnings import ExtractionWarningCollector
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_autofill_entries,
    insert_credentials,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    delete_autofill_by_run,
    delete_credentials_by_run,
)
from core.database.helpers.deleted_form_history import (
    insert_deleted_form_history_entries,
    delete_deleted_form_history_by_run,
)

LOGGER = get_logger("extractors.browser.firefox.autofill")


class FirefoxAutofillExtractor(BaseExtractor):
    """
    Extract autofill data from Firefox-based browsers.

    Dual-helper strategy:
    - Extraction: Scans filesystem, copies formhistory.sqlite and logins.json files
    - Ingestion: Parses copied databases/JSON, inserts with forensic fields

    Supported browsers: Firefox, Firefox ESR, Tor Browser

    Note: Firefox does not have equivalent features for autofill profiles or credit cards.

     Changes:
    - Multi-partition discovery via file_list table
    - Schema warning support for unknown tables/columns/keys
    - Filename collision prevention with partition_index and path hash
    - Companion file copying (-wal, -shm)
    """

    SUPPORTED_BROWSERS = list(FIREFOX_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="firefox_autofill",
            display_name="Firefox Autofill & Credentials",
            description="Extract form history, deleted history, and saved logins from Firefox/Tor Browser",
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
        """
        Return configuration widget (browser selection + multi-partition).

        Uses BrowserConfigWidget with Firefox browsers and multi-partition option.
        """
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
            partition_count = len(data.get("partitions_with_artifacts", [1]))
            status_text = (
                f"Firefox Autofill\n"
                f"Files: {file_count}\n"
                f"Partitions: {partition_count}\n"
                f"Run: {data.get('run_id', 'N/A')[:20]}"
            )
        else:
            status_text = "Firefox Autofill\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "firefox_autofill"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Firefox autofill databases from evidence.

        Workflow:
            1. Generate run_id
            2. Scan evidence for Firefox autofill files (multi-partition if enabled)
            3. Copy matching files to output_dir/ with collision-safe naming
            4. Calculate hashes, copy companion files (-wal, -shm)
            5. Write manifest.json

        Multi-partition support:
            When scan_all_partitions=True (default), uses file_list discovery to
            find autofill files across ALL partitions, not just the main partition.
        """
        callbacks.on_step("Initializing Firefox autofill extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")
        scan_all_partitions = config.get("scan_all_partitions", True)

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Firefox autofill extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.0.0",  # Bumped for multi-partition support
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

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)

        # Scan for autofill files - use multi-partition if enabled and evidence_conn available
        callbacks.on_step("Scanning for Firefox autofill databases")

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
                    "Multi-partition requested but no evidence_conn - using single partition",
                    "warning"
                )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, browsers_to_search, callbacks)
            if files:
                files_by_partition[partition_index] = files

        # Count total files and partitions
        total_files = sum(len(files) for files in files_by_partition.values())
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = [
            p for p, f in files_by_partition.items() if f
        ]

        # Report discovered files (even if 0)
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=total_files)

        if total_files == 0:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Firefox autofill files found")
            LOGGER.info("No Firefox autofill files found")
        else:
            callbacks.on_log(
                f"Found {total_files} autofill file(s) on {len(manifest_data['partitions_with_artifacts'])} partition(s)"
            )

            # Extract files from each partition
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            file_index = 0

            for partition_index in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_index]
                if not partition_files:
                    continue

                callbacks.on_log(
                    f"Processing partition {partition_index}: {len(partition_files)} files"
                )

                # Determine how to access this partition
                current_partition = getattr(evidence_fs, 'partition_index', 0)
                if partition_index == current_partition:
                    # Use existing evidence_fs
                    fs_to_use = evidence_fs
                    need_close = False
                elif ewf_paths:
                    # Open specific partition
                    fs_to_use = None
                    need_close = True
                else:
                    # Can't access different partition
                    callbacks.on_log(
                        f"Cannot access partition {partition_index} - skipping",
                        "warning"
                    )
                    continue

                try:
                    if need_close:
                        # Open partition using context manager
                        with open_partition_for_extraction(ewf_paths, partition_index) as fs:
                            for file_info in partition_files:
                                if callbacks.is_cancelled():
                                    manifest_data["status"] = "cancelled"
                                    break

                                file_index += 1
                                callbacks.on_progress(
                                    file_index, total_files,
                                    f"Copying {file_info['browser']} {file_info['file_type']}"
                                )

                                try:
                                    extracted_file = self._extract_file(
                                        fs, file_info, output_dir, callbacks
                                    )
                                    manifest_data["files"].append(extracted_file)
                                except Exception as e:
                                    error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                                    LOGGER.error(error_msg, exc_info=True)
                                    manifest_data["notes"].append(error_msg)
                                    manifest_data["status"] = "partial"
                    else:
                        # Use existing filesystem
                        for file_info in partition_files:
                            if callbacks.is_cancelled():
                                manifest_data["status"] = "cancelled"
                                break

                            file_index += 1
                            callbacks.on_progress(
                                file_index, total_files,
                                f"Copying {file_info['browser']} {file_info['file_type']}"
                            )

                            try:
                                extracted_file = self._extract_file(
                                    fs_to_use, file_info, output_dir, callbacks
                                )
                                manifest_data["files"].append(extracted_file)
                            except Exception as e:
                                error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                                LOGGER.error(error_msg, exc_info=True)
                                manifest_data["notes"].append(error_msg)
                                manifest_data["status"] = "partial"

                except Exception as e:
                    LOGGER.error("Error accessing partition %d: %s", partition_index, e)
                    manifest_data["notes"].append(f"Partition {partition_index} error: {e}")

                if callbacks.is_cancelled():
                    break

        # Finish stats tracking
        if stats:
            status = (
                "cancelled" if manifest_data["status"] == "cancelled"
                else "success" if manifest_data["status"] == "ok"
                else "partial"
            )
            stats.finish_run(evidence_id, self.metadata.name, status=status)

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
            "Firefox autofill extraction complete: %d files from %d partition(s), status=%s",
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
        Parse extracted manifest and ingest into database.

        Workflow:
            1. Read manifest.json
            2. Create warning collector for schema discovery
            3. Register files in browser_inventory
            4. For each autofill file:
               - Parse using appropriate parser (formhistory, logins.json, signons)
               - Detect unknown tables/columns/keys
               - Insert records into database
            5. Flush warnings to database
            6. Return counts
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"autofill": 0, "credentials": 0, "deleted_form_history": 0}

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

        # Continue statistics tracking with manifest's run_id
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, entries=0)
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"autofill": 0, "credentials": 0, "deleted_form_history": 0}

        total_autofill = 0
        total_credentials = 0
        total_deleted = 0

        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        callbacks.on_progress(0, len(files), "Parsing Firefox autofill databases")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            if file_entry.get("copy_status") == "error":
                callbacks.on_log(f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}", "warning")
                continue

            callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry['browser']} {file_entry['file_type']}")

            try:
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry["browser"],
                    artifact_type=f"autofill_{file_entry['file_type']}",
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

                file_path = Path(file_entry["extracted_path"])
                if not file_path.is_absolute():
                    file_path = output_dir / file_path

                counts = self._parse_autofill_file(
                    file_path, file_entry, run_id, evidence_id, evidence_conn, callbacks,
                    warning_collector=warning_collector,
                )

                total_autofill += counts.get("autofill", 0)
                total_credentials += counts.get("credentials", 0)
                total_deleted += counts.get("deleted_form_history", 0)

                total_records = sum(counts.values())
                update_inventory_ingestion_status(
                    evidence_conn, inventory_id=inventory_id, status="ok", records_parsed=total_records
                )

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry['extracted_path']}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")

                if 'inventory_id' in locals():
                    update_inventory_ingestion_status(evidence_conn, inventory_id=inventory_id, status="error", notes=str(e))

        # Flush collected warnings to database before commit
        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)
            callbacks.on_log(f"Recorded {warning_count} schema warnings", "info")

        evidence_conn.commit()

        # Report ingestion statistics
        total_records = total_autofill + total_credentials + total_deleted
        if stats:
            stats.report_ingested(evidence_id, self.metadata.name, records=total_records, entries=total_autofill)
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {"autofill": total_autofill, "credentials": total_credentials, "deleted_form_history": total_deleted}

    # -------------------------------------------------------------------------
    # Private Helper Methods
    # -------------------------------------------------------------------------

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

            return {"image_path": str(source_path) if source_path else None, "fs_type": fs_type}
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

    def _discover_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """
        Scan evidence for Firefox autofill files (single partition fallback).

        Args:
            evidence_fs: Evidence filesystem
            browsers: List of browser keys to search
            callbacks: Progress/log callbacks

        Returns:
            List of file info dicts
        """
        autofill_files = []

        for browser_key in browsers:
            if browser_key not in FIREFOX_BROWSERS:
                callbacks.on_log(f"Unknown Firefox browser: {browser_key}", "warning")
                continue

            browser_config = FIREFOX_BROWSERS[browser_key]

            patterns = get_artifact_patterns(browser_key, "autofill")

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        file_type = classify_autofill_file(path_str)
                        profile = extract_profile_from_path(path_str)

                        autofill_files.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "profile": profile,
                            "file_type": file_type,
                            "artifact_type": "autofill",
                            "display_name": browser_config["display_name"],
                            "partition_index": getattr(evidence_fs, 'partition_index', 0),
                        })

                        callbacks.on_log(f"Found {browser_key} {file_type}: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return autofill_files

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> Dict[int, List[Dict]]:
        """
        Discover Firefox autofill files across all partitions via file_list.

        Args:
            evidence_fs: Evidence filesystem (for fallback)
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
            files = self._discover_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

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

        # Query file_list for autofill-related files
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=[
                "formhistory.sqlite",
                "logins.json",
                "key4.db",
                "key3.db",
                "signons.sqlite",
            ],
            path_patterns=path_patterns,
        )

        if result.is_empty:
            callbacks.on_log(
                "No Firefox autofill files found in file_list, falling back to filesystem scan",
                "warning"
            )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        if result.is_multi_partition:
            callbacks.on_log(
                f"Found autofill files on {len(result.partitions_with_matches)} partitions: {result.partitions_with_matches}",
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

                file_type = classify_autofill_file(match.file_path)
                profile = extract_profile_from_path(match.file_path)

                files_list.append({
                    "logical_path": match.file_path,
                    "browser": browser or "firefox",
                    "profile": profile,
                    "file_type": file_type,
                    "artifact_type": "autofill",
                    "display_name": get_browser_display_name(browser) if browser else "Firefox",
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

                callbacks.on_log(
                    f"Found {browser or 'firefox'} {file_type} on partition {partition_index}: {match.file_path}",
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
        """
        Copy file from evidence to workspace with collision-safe naming.

        Naming format: {browser}_{profile}_p{partition}_{path_hash}_{file_type}
        This prevents collisions when:
        - Same browser/profile exists on multiple partitions (dual-boot)
        - Profile name sanitization produces duplicates
        """
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info.get("profile") or "Unknown"
            file_type = file_info["file_type"]
            partition_index = file_info.get("partition_index", 0)

            # Create collision-safe filename with partition suffix and mini-hash
            # Mini-hash: first 8 chars of SHA256 of source path
            safe_profile = profile.replace(' ', '_').replace('/', '_').replace('.', '_')
            path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]
            filename = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_{file_type}"
            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()
            size = len(file_content)

            # Copy companion files (-wal, -shm) for SQLite databases
            companion_files = []
            if file_type in ("formhistory", "signons"):
                for suffix in ["-wal", "-shm"]:
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
                "file_type": file_type,
                "logical_path": source_path,
                "artifact_type": "autofill",
                "partition_index": partition_index,
                "companion_files": companion_files,
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

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        """Clear autofill data from a previous run."""
        deleted = 0
        deleted += delete_autofill_by_run(evidence_conn, evidence_id, run_id)
        deleted += delete_credentials_by_run(evidence_conn, evidence_id, run_id)
        deleted += delete_deleted_form_history_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d autofill records from previous run %s", deleted, run_id)

    def _parse_autofill_file(
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
        """Parse autofill file and insert records."""
        if not file_path.exists():
            LOGGER.warning("Autofill file not found: %s", file_path)
            return {"autofill": 0, "credentials": 0, "deleted_form_history": 0}

        file_type = file_entry["file_type"]
        browser = file_entry["browser"]

        counts = {"autofill": 0, "credentials": 0, "deleted_form_history": 0}
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        if file_type == "formhistory":
            counts.update(self._parse_formhistory_db(
                file_path, browser, file_entry, run_id, discovered_by,
                evidence_id, evidence_conn, callbacks,
                warning_collector=warning_collector,
            ))
        elif file_type == "logins_json":
            counts["credentials"] = self._parse_logins_json_file(
                file_path, browser, file_entry, run_id, discovered_by,
                evidence_id, evidence_conn, callbacks,
                warning_collector=warning_collector,
            )
        elif file_type == "signons":
            counts["credentials"] = self._parse_signons_db(
                file_path, browser, file_entry, run_id, discovered_by,
                evidence_id, evidence_conn, callbacks,
                warning_collector=warning_collector,
            )
        # key3.db and key4.db are NSS key stores - copy only, no parsing

        return counts

    def _parse_formhistory_db(
        self,
        db_path: Path,
        browser: str,
        file_entry: Dict,
        run_id: str,
        discovered_by: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, int]:
        """Parse Firefox formhistory.sqlite database."""
        counts = {"autofill": 0, "deleted_form_history": 0}

        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
        except Exception as e:
            LOGGER.error("Failed to open formhistory.sqlite: %s", e)
            if warning_collector:
                warning_collector.add_file_corrupt(
                    filename=str(db_path),
                    error=str(e),
                    artifact_type="autofill",
                )
            return counts

        try:
            # Parse main form history
            autofill_records = parse_moz_formhistory(
                conn, browser, file_entry, run_id, discovered_by,
                warning_collector=warning_collector,
            )
            if autofill_records:
                counts["autofill"] = insert_autofill_entries(evidence_conn, evidence_id, autofill_records)

            # Parse deleted form history
            deleted_records = parse_moz_deleted_formhistory(
                conn, browser, file_entry, run_id, discovered_by,
                warning_collector=warning_collector,
            )
            if deleted_records:
                counts["deleted_form_history"] = insert_deleted_form_history_entries(
                    evidence_conn, evidence_id, deleted_records
                )

        finally:
            conn.close()

        return counts

    def _parse_logins_json_file(
        self,
        json_path: Path,
        browser: str,
        file_entry: Dict,
        run_id: str,
        discovered_by: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> int:
        """Parse Firefox logins.json file."""
        records = parse_logins_json(
            json_path, browser, file_entry, run_id, discovered_by,
            warning_collector=warning_collector,
        )

        if records:
            return insert_credentials(evidence_conn, evidence_id, records)
        return 0

    def _parse_signons_db(
        self,
        db_path: Path,
        browser: str,
        file_entry: Dict,
        run_id: str,
        discovered_by: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> int:
        """Parse legacy Firefox signons.sqlite database."""
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
        except Exception as e:
            LOGGER.error("Failed to open signons.sqlite: %s", e)
            if warning_collector:
                warning_collector.add_file_corrupt(
                    filename=str(db_path),
                    error=str(e),
                    artifact_type="credentials",
                )
            return 0

        try:
            records = parse_moz_logins_signons(
                conn, browser, file_entry, run_id, discovered_by,
                warning_collector=warning_collector,
            )

            if records:
                callbacks.on_log(f"Parsed {len(records)} legacy credentials from signons.sqlite", "info")
                return insert_credentials(evidence_conn, evidence_id, records)
            return 0

        finally:
            conn.close()
