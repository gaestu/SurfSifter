"""
Firefox Sync Data Extractor

Extracts browser sync account information from Firefox signedInUser.json.

Features:
- Multi-partition support with discover_from_file_list utility
- signedInUser.json parsing (Firefox Accounts/Sync)
- Account and device extraction
- Schema warning support for unknown JSON keys
- StatisticsCollector integration for run tracking
- Browser inventory recording for file tracking
- Run-based deletion (preserves extraction history)

Data Format:
- Firefox stores sync data in signedInUser.json
- JSON file with accountData section
- Contains email, uid, displayName, device info

Forensic Value:
- Links Firefox profile to Mozilla/Firefox account
- Identifies sync-enabled devices
- Reveals account verification status

 Changes:
- Multi-partition support using discover_from_file_list (always enabled)
- Schema warning integration for unknown JSON keys
- Split into _schemas.py and _parsers.py for maintainability
- Use database helpers instead of raw SQL
- Partition-aware filenames to prevent overwrites
- Use shared extract_profile_from_path from _patterns.py
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
    glob_to_sql_like,
)
from ...._shared.extraction_warnings import ExtractionWarningCollector
from .._patterns import (
    FIREFOX_BROWSERS,
    get_artifact_patterns,
    extract_profile_from_path,
    detect_browser_from_path,
)
from ._parsers import parse_firefox_sync
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_browser_inventory,
    update_inventory_ingestion_status,
)
from core.database.helpers.sync_data import (
    insert_sync_datas,
    insert_synced_devices,
    delete_sync_data_by_run,
    delete_synced_devices_by_run,
)

LOGGER = get_logger("extractors.browser.firefox.sync_data")


class FirefoxSyncDataExtractor(BaseExtractor):
    """
    Extract browser sync account information from Firefox signedInUser.json.

    Firefox stores sync account data in JSON format, containing account info,
    device registration, and sync configuration.

    Features multi-partition support for disk images with multiple OS installs.
    """

    SUPPORTED_BROWSERS = list(FIREFOX_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="firefox_sync_data",
            display_name="Firefox Sync Data",
            description="Extract sync account and device info from Firefox/Tor signedInUser.json",
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
            accounts = data.get("accounts_found", 0)
            devices = data.get("devices_found", 0)
            status_text = (
                f"Firefox Sync Data\n"
                f"Accounts: {accounts}, Devices: {devices}\n"
                f"Run ID: {data.get('run_id', 'N/A')}"
            )
        else:
            status_text = "Firefox Sync Data\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "firefox_sync_data"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Firefox signedInUser.json files from evidence."""
        callbacks.on_step("Initializing Firefox sync data extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Firefox sync data extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.1.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_extraction_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "multi_partition": True,  # Always enabled
            "files": [],
            "accounts_found": 0,
            "devices_found": 0,
            "status": "ok",
            "notes": [],
        }

        callbacks.on_step("Scanning for Firefox signedInUser.json files")

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        browsers_to_search = [b for b in browsers_to_search if b in self.SUPPORTED_BROWSERS]

        # Discover files using multi-partition discovery (always enabled)
        if evidence_conn:
            files_by_partition = self._discover_files_multi_partition(
                evidence_conn, evidence_id, browsers_to_search, callbacks
            )
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
        else:
            # Fallback to single-partition filesystem walk
            files_by_partition = {
                None: self._discover_sync_files_filesystem(evidence_fs, browsers_to_search, callbacks)
            }
            ewf_paths = None

        # Count total files
        total_files = sum(len(files) for files in files_by_partition.values())

        # Report discovered files (even if 0)
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=total_files)

        if total_files == 0:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No signedInUser.json files found")
            LOGGER.info("No Firefox signedInUser.json files found")
        else:
            callbacks.on_progress(0, total_files, "Copying sync data files")

            total_accounts = 0
            total_devices = 0
            file_index = 0

            for partition_index, files in files_by_partition.items():
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    manifest_data["notes"].append("Extraction cancelled by user")
                    break

                # Get partition-specific filesystem
                with open_partition_for_extraction(
                    ewf_paths if ewf_paths else evidence_fs,
                    partition_index
                ) as partition_fs:
                    if partition_fs is None:
                        LOGGER.warning("Cannot open partition %s", partition_index)
                        manifest_data["notes"].append(f"Failed to open partition {partition_index}")
                        continue

                    for file_info in files:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            manifest_data["notes"].append("Extraction cancelled by user")
                            break

                        file_index += 1
                        callbacks.on_progress(
                            file_index, total_files,
                            f"Copying {file_info['browser']} sync data"
                        )

                        try:
                            extracted_file = self._extract_file(
                                partition_fs,
                                file_info,
                                output_dir,
                                callbacks,
                                partition_index=partition_index,
                            )
                            manifest_data["files"].append(extracted_file)

                            total_accounts += extracted_file.get("accounts_preview", 0)
                            total_devices += extracted_file.get("devices_preview", 0)

                        except Exception as e:
                            error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            manifest_data["notes"].append(error_msg)
                            manifest_data["status"] = "partial"
                            if stats:
                                stats.report_failed(evidence_id, self.metadata.name, files=1)

            manifest_data["accounts_found"] = total_accounts
            manifest_data["devices_found"] = total_devices

        # Finish statistics tracking
        if stats:
            status = "cancelled" if manifest_data["status"] == "cancelled" else "success" if manifest_data["status"] == "ok" else "partial"
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        # Write manifest
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
            "Firefox sync data extraction complete: %d files, %d accounts, %d devices, status=%s",
            len(manifest_data["files"]),
            manifest_data["accounts_found"],
            manifest_data["devices_found"],
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
        """Parse extracted signedInUser.json and ingest sync data into database."""
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"accounts": 0, "devices": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])

        # Continue statistics tracking with manifest's run_id
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Clear previous run data using helpers (preserves extraction history)
        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, accounts=0)
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"accounts": 0, "devices": 0}

        results = {"accounts": 0, "devices": 0}
        include_raw = config.get("include_raw", False)

        # Create warning collector for schema warnings
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        callbacks.on_progress(0, len(files), "Parsing Firefox sync data")

        # Batch records for efficient insertion
        accounts_batch: List[Dict[str, Any]] = []
        devices_batch: List[Dict[str, Any]] = []
        ingestion_cancelled = False

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                ingestion_cancelled = True
                break

            if file_entry.get("copy_status") == "error":
                callbacks.on_log(f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}", "warning")
                continue

            callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry['browser']} sync data")

            # Reset inventory_id for each iteration to avoid stale reference
            inventory_id = None

            try:
                extracted_path = Path(file_entry["extracted_path"])
                if not extracted_path.is_absolute():
                    extracted_path = output_dir / extracted_path

                if not extracted_path.exists():
                    callbacks.on_log(f"Missing file: {extracted_path}", "warning")
                    continue

                # Register inventory record for provenance tracking
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry["browser"],
                    artifact_type="sync_data",
                    run_id=run_id,
                    extracted_path=str(extracted_path),
                    extraction_status="ok" if file_entry.get("copy_status") == "ok" else "error",
                    extraction_timestamp_utc=manifest_data["extraction_timestamp_utc"],
                    logical_path=file_entry.get("logical_path"),
                    profile=file_entry.get("profile"),
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    forensic_path=file_entry.get("forensic_path"),
                    extraction_tool=manifest_data.get("extraction_tool"),
                    file_size_bytes=file_entry.get("file_size_bytes"),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                )

                content = extracted_path.read_text(encoding="utf-8", errors="replace")
                data = json.loads(content)

                browser = file_entry.get("browser", "firefox")
                profile = file_entry.get("profile", "Default")
                source_path = file_entry.get("logical_path", "")
                partition_index = file_entry.get("partition_index")

                # Parse Firefox sync data with schema warning support
                parsed = parse_firefox_sync(
                    data,
                    warning_collector=warning_collector,
                    source_file=source_path,
                )

                # Prepare account records for batch insert
                for account in parsed.get("accounts", []):
                    accounts_batch.append({
                        "browser": browser,
                        "profile": profile,
                        "account_id": account.get("account_id", ""),
                        "account_email": account.get("email", ""),  # Helper expects account_email
                        "email": account.get("email", ""),
                        "display_name": account.get("display_name", ""),
                        "gaia_id": account.get("gaia_id", ""),
                        "profile_path": account.get("profile_path", ""),
                        "last_sync_utc": account.get("last_sync_time"),
                        "last_sync_time": account.get("last_sync_time"),
                        "sync_enabled": 1 if account.get("sync_enabled") else 0,
                        "sync_types": json.dumps(account.get("synced_types", [])),
                        "synced_types": json.dumps(account.get("synced_types", [])),
                        "raw_data": json.dumps(account.get("raw_data", {})) if include_raw else None,
                        "source_path": source_path,
                        "partition_index": partition_index,
                        "run_id": run_id,
                    })
                    results["accounts"] += 1

                # Prepare device records for batch insert
                for device in parsed.get("devices", []):
                    devices_batch.append({
                        "browser": browser,
                        "profile": profile,
                        "device_id": device.get("device_id", ""),
                        "device_name": device.get("device_name", ""),
                        "device_type": device.get("device_type", ""),
                        "os_type": device.get("os_type", ""),
                        "chrome_version": device.get("browser_version", ""),  # Schema uses chrome_version
                        "last_updated_utc": device.get("last_updated"),
                        "sync_account_id": device.get("sync_account_id", ""),
                        "raw_data": json.dumps(device.get("raw_data", {})) if include_raw else None,
                        "source_path": source_path,
                        "partition_index": partition_index,
                        "run_id": run_id,
                    })
                    results["devices"] += 1

                # Update inventory with ingestion result
                file_records = len(parsed.get("accounts", [])) + len(parsed.get("devices", []))
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=file_records,
                )

            except json.JSONDecodeError as e:
                warning_collector.add_json_parse_error(
                    filename=str(file_entry.get("extracted_path", "unknown")),
                    error=str(e),
                )
                callbacks.on_log(f"JSON parse error: {e}", "error")
                if inventory_id is not None:
                    update_inventory_ingestion_status(
                        evidence_conn, inventory_id=inventory_id, status="error", notes=str(e)
                    )
            except Exception as e:
                error_msg = f"Failed to parse {file_entry['extracted_path']}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_log(error_msg, "error")

                if inventory_id is not None:
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(e),
                    )

        # Batch insert accounts and devices using helpers
        try:
            if accounts_batch:
                insert_sync_datas(evidence_conn, evidence_id, accounts_batch)
            if devices_batch:
                insert_synced_devices(evidence_conn, evidence_id, devices_batch)
            evidence_conn.commit()
        except Exception as e:
            LOGGER.error("Failed to insert sync data: %s", e, exc_info=True)
            callbacks.on_log(f"Database insert error: {e}", "error")

        # Flush warnings to database
        try:
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info("Recorded %d extraction warnings", warning_count)
        except Exception as e:
            LOGGER.warning("Failed to flush warnings: %s", e)

        # Report ingestion statistics
        total_records = results["accounts"] + results["devices"]
        if stats:
            stats.report_ingested(evidence_id, self.metadata.name, records=total_records, accounts=results["accounts"])
            final_status = "cancelled" if ingestion_cancelled else "success"
            stats.finish_run(evidence_id, self.metadata.name, status=final_status)

        callbacks.on_step(f"Ingested {results['accounts']} accounts, {results['devices']} devices")

        return results

    # ─────────────────────────────────────────────────────────────────
    # Helper Methods
    # ─────────────────────────────────────────────────────────────────

    def _generate_run_id(self) -> str:
        """Generate run ID: sync_firefox_{timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"sync_firefox_{timestamp}_{unique_id}"

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

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        """Clear data from previous run with same run_id using database helpers.

        Uses run-based deletion to preserve extraction history across multiple runs.
        """
        try:
            # Delete sync data from this specific run
            sync_deleted = delete_sync_data_by_run(evidence_conn, evidence_id, run_id)

            # Delete synced devices from this specific run
            devices_deleted = delete_synced_devices_by_run(evidence_conn, evidence_id, run_id)

            # Delete warnings from this specific run
            warnings_deleted = 0
            try:
                cursor = evidence_conn.execute(
                    "DELETE FROM extraction_warnings WHERE evidence_id = ? AND run_id = ?",
                    (evidence_id, run_id)
                )
                warnings_deleted = cursor.rowcount
            except Exception:
                # extraction_warnings table may not exist in older schemas
                pass

            evidence_conn.commit()

            if sync_deleted > 0 or devices_deleted > 0 or warnings_deleted > 0:
                LOGGER.info(
                    "Cleared previous run data for evidence %d, run %s: %d sync, %d devices, %d warnings",
                    evidence_id, run_id, sync_deleted, devices_deleted, warnings_deleted
                )
        except Exception as e:
            LOGGER.warning("Failed to clear previous run data: %s", e)

    def _discover_files_multi_partition(
        self,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> Dict[Optional[int], List[Dict]]:
        """Discover signedInUser.json files across all partitions using discover_from_file_list.

        Uses the shared file_list_discovery utility for consistent multi-partition
        artifact discovery across all extractors.

        Returns:
            Dictionary mapping partition_index -> list of file info dicts
        """
        files_by_partition: Dict[Optional[int], List[Dict]] = {}

        # Build path patterns from browser definitions using get_artifact_patterns
        path_patterns = []
        browser_pattern_map: Dict[str, List[str]] = {}  # pattern -> browser_key

        for browser_key in browsers:
            if browser_key not in FIREFOX_BROWSERS:
                continue

            # Skip firefox_esr (label-only entry with no patterns)
            if not FIREFOX_BROWSERS[browser_key].get("profile_roots"):
                continue

            # Get patterns using the shared utility
            patterns = get_artifact_patterns(browser_key, "sync_data")
            # Filter to only signedInUser.json
            patterns = [p for p in patterns if "signedInUser.json" in p]

            for pattern in patterns:
                # Convert glob pattern to SQL LIKE pattern
                sql_pattern = glob_to_sql_like(pattern)
                path_patterns.append(sql_pattern)

                # Track which browser this pattern belongs to
                if sql_pattern not in browser_pattern_map:
                    browser_pattern_map[sql_pattern] = []
                browser_pattern_map[sql_pattern].append(browser_key)

        if not path_patterns:
            LOGGER.warning("No path patterns built for browsers: %s", browsers)
            return files_by_partition

        try:
            # Use discover_from_file_list for consistent discovery
            result = discover_from_file_list(
                evidence_conn,
                evidence_id,
                filename_patterns=["signedInUser.json"],
                path_patterns=path_patterns,
            )

            if result.is_empty:
                LOGGER.info("No signedInUser.json files found via file_list discovery")
                return files_by_partition

            LOGGER.info("discover_from_file_list: %s", result.get_partition_summary())

            # Convert FileListMatch objects to our file_info dicts
            for partition_index, matches in result.matches_by_partition.items():
                for match in matches:
                    # Determine browser from path
                    browser_key = detect_browser_from_path(match.file_path)
                    if browser_key not in browsers:
                        LOGGER.debug("Browser %s not in search list for: %s", browser_key, match.file_path)
                        continue

                    display_name = FIREFOX_BROWSERS[browser_key]["display_name"]
                    profile = extract_profile_from_path(match.file_path)

                    file_info = {
                        "logical_path": match.file_path,
                        "browser": browser_key,
                        "profile": profile,
                        "artifact_type": "sync_data",
                        "display_name": display_name,
                        "partition_index": partition_index,
                        "inode": match.inode,
                        "size_bytes": match.size_bytes,
                    }

                    if partition_index not in files_by_partition:
                        files_by_partition[partition_index] = []
                    files_by_partition[partition_index].append(file_info)

                    callbacks.on_log(
                        f"Found {browser_key} signedInUser.json (partition {partition_index}): {match.file_path}",
                        "info"
                    )

        except Exception as e:
            LOGGER.error("Multi-partition discovery failed: %s", e, exc_info=True)
            callbacks.on_log(f"Multi-partition discovery failed: {e}", "warning")

        return files_by_partition

    def _discover_sync_files_filesystem(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """Fallback: Scan evidence for Firefox signedInUser.json files using filesystem.

        Used when evidence_conn is not available (no file_list table).
        """
        sync_files = []

        for browser_key in browsers:
            if browser_key not in FIREFOX_BROWSERS:
                continue

            # Skip firefox_esr (label-only entry with no patterns)
            if not FIREFOX_BROWSERS[browser_key].get("profile_roots"):
                continue

            # Use sync_data patterns which include signedInUser.json
            patterns = get_artifact_patterns(browser_key, "sync_data")
            # Filter to only signedInUser.json
            patterns = [p for p in patterns if "signedInUser.json" in p]

            display_name = FIREFOX_BROWSERS[browser_key]["display_name"]

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        profile = extract_profile_from_path(path_str)

                        sync_files.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "profile": profile,
                            "artifact_type": "sync_data",
                            "display_name": display_name,
                        })

                        callbacks.on_log(f"Found {browser_key} signedInUser.json: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return sync_files

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks,
        *,
        partition_index: Optional[int] = None,
    ) -> Dict:
        """Copy file from evidence to workspace and collect metadata.

        Includes partition_index in filename to prevent overwrites from
        multi-partition extractions.
        """
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info["profile"]

            safe_profile = profile.replace(' ', '_').replace('/', '_').replace('.', '_')

            # Include partition index and hash suffix in filename to prevent overwrites
            # Hash suffix handles case where same profile name exists in different paths
            path_hash = hashlib.md5(source_path.encode()).hexdigest()[:6]
            if partition_index is not None:
                filename = f"p{partition_index}_{browser}_{safe_profile}_{path_hash}_signedInUser.json"
            else:
                filename = f"{browser}_{safe_profile}_{path_hash}_signedInUser.json"

            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()
            size = len(file_content)

            # Preview parsing for counts
            accounts_preview = 0
            devices_preview = 0
            try:
                data = json.loads(file_content.decode("utf-8", errors="replace"))
                parsed = parse_firefox_sync(data)
                accounts_preview = len(parsed.get("accounts", []))
                devices_preview = len(parsed.get("devices", []))
            except Exception:
                pass

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
                "artifact_type": "sync_data",
                "partition_index": partition_index,
                "accounts_preview": accounts_preview,
                "devices_preview": devices_preview,
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
                "logical_path": file_info.get("logical_path"),
                "partition_index": partition_index,
                "error_message": str(e),
            }
