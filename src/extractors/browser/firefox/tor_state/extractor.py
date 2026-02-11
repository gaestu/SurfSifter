"""
Firefox Tor State Extractor

Extracts Tor Browser config/state files from the Tor data directory.

Features:
- Multi-partition discovery via file_list table
- Collision-safe filenames with partition index + path hash
- Schema warning support for unknown torrc directives
- torrc parsing with database storage (browser_config table)
- state file parsing with timestamps and guard info (tor_state table)
- cached-* file analysis

Artifacts extracted:
- torrc, torrc-defaults: Tor configuration
- state: Runtime state with guards, timestamps
- cached-*: Relay/circuit cache
- control_auth_cookie, geoip, pt_state/, keys/

 Changes:
- Added multi-partition support via discover_from_file_list
- Fixed file overwrite risk with partition + hash in filenames
- Added schema warning collection via ExtractionWarningCollector
- Moved patterns to _patterns.py
- Added torrc/state parsing to _parsers.py
- Added database storage for parsed config (browser_config, tor_state tables)
- Added _clear_previous_run method
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
    check_file_list_available,
)
from ...._shared.extraction_warnings import ExtractionWarningCollector
from ._patterns import (
    TOR_DATA_ROOTS,
    TOR_ARTIFACT_PATTERNS,
    get_all_tor_patterns,
    classify_tor_file,
)
from ._parsers import parse_torrc, parse_state_file, parse_cached_file
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import insert_browser_inventory, update_inventory_ingestion_status
from core.database.helpers.browser_config import (
    insert_browser_configs,
    insert_tor_states,
    delete_browser_config_by_run,
    delete_tor_state_by_run,
)

LOGGER = get_logger("extractors.browser.firefox.tor_state")


class FirefoxTorStateExtractor(BaseExtractor):
    """
    Extract Tor Browser config/state files from evidence images.

    Captures Tor-specific artifacts under TorBrowser/Data/Tor/:
    - Configuration files (torrc, torrc-defaults)
    - State files with guards and timestamps
    - Cache files with relay/circuit info
    - Authentication cookies and crypto keys

    Supports multi-partition discovery for finding Tor Browser
    installed across different partitions (dual-boot, portable installs).
    """

    SUPPORTED_BROWSERS = ["tor"]

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="tor_state",
            display_name="Tor State",
            description="Extract Tor Browser config/state files (torrc, state, cached-*, pt_state)",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        if evidence_fs is None:
            return False, "No evidence filesystem mounted. Please mount E01 image first."
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return config widget with multi-partition option."""
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
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            partitions = data.get("partitions_with_artifacts", [])
            partition_info = f", {len(partitions)} partitions" if len(partitions) > 1 else ""
            status_text = f"Tor State\nFiles: {file_count}{partition_info}\nRun: {data.get('run_id', 'N/A')[:20]}"
        else:
            status_text = "Tor State\nNo extraction yet"
        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        return case_root / "evidences" / evidence_label / "tor_state"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Tor Browser state files from evidence.

        Workflow:
            1. Generate run_id
            2. Scan evidence for Tor files (multi-partition if enabled)
            3. Copy matching files to output_dir with collision-safe names
            4. Calculate hashes, collect E01 context
            5. Write manifest.json
        """
        callbacks.on_step("Initializing Tor state extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")
        scan_all_partitions = config.get("scan_all_partitions", True)

        LOGGER.info("Starting Tor state extraction (run_id=%s)", run_id)

        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

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

        callbacks.on_step("Scanning for Tor Browser state files")

        # Discover files - multi-partition or single
        files_by_partition: Dict[int, List[Dict]] = {}

        if scan_all_partitions and evidence_conn is not None:
            files_by_partition = self._discover_files_multi_partition(
                evidence_fs, evidence_conn, evidence_id, callbacks
            )
        else:
            if scan_all_partitions and evidence_conn is None:
                callbacks.on_log(
                    "Multi-partition scan requested but no evidence_conn, using single partition",
                    "warning"
                )
            tor_files = self._discover_files_single_partition(evidence_fs, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if tor_files:
                files_by_partition[partition_index] = tor_files

        # Flatten for processing
        all_tor_files = []
        for partition_idx, files in files_by_partition.items():
            manifest_data["partitions_scanned"].append(partition_idx)
            if files:
                manifest_data["partitions_with_artifacts"].append(partition_idx)
                all_tor_files.extend(files)

        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(all_tor_files))

        if not all_tor_files:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Tor Browser state files found")
            LOGGER.info("No Tor Browser state files found")
        else:
            callbacks.on_progress(0, len(all_tor_files), "Copying Tor state files")

            for i, file_info in enumerate(all_tor_files):
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    manifest_data["notes"].append("Extraction cancelled by user")
                    break

                try:
                    callbacks.on_progress(i + 1, len(all_tor_files), f"Copying {file_info['file_type']}")
                    extracted_file = self._extract_file(evidence_fs, file_info, output_dir, callbacks)
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

        if stats:
            status = "cancelled" if manifest_data["status"] == "cancelled" else "success" if manifest_data["status"] == "ok" else "partial"
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        LOGGER.info(
            "Tor state extraction complete: %d files from %d partitions, status=%s",
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
        Parse extracted Tor files and ingest into database.

        Parses:
        - torrc: Configuration directives -> browser_config table
        - state: Runtime state -> tor_state table
        - cached-*: Basic analysis (counts) -> inventory notes
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"files": 0, "config_records": 0, "state_records": 0}

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

        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, files=0)
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"files": 0, "config_records": 0, "state_records": 0}

        # Clear previous run data
        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        total_files = 0
        total_config_records = 0
        total_state_records = 0

        callbacks.on_progress(0, len(files), "Parsing Tor state files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            if file_entry.get("copy_status") == "error":
                callbacks.on_log(
                    f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}",
                    "warning",
                )
                continue

            callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry.get('file_type')}")

            try:
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser="tor",
                    artifact_type="tor_state",
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
                    extraction_notes=f"type={file_entry.get('file_type')}",
                    file_size_bytes=file_entry.get("file_size_bytes"),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                )

                file_path = Path(file_entry["extracted_path"])
                if not file_path.is_absolute():
                    file_path = output_dir / file_path

                records_parsed = 0
                notes = None
                file_type = file_entry.get("file_type")
                source_path = file_entry.get("logical_path", "")

                # Parse based on file type
                if file_type == "torrc":
                    config_records, notes_dict = self._ingest_torrc(
                        file_path, source_path, file_entry, run_id, evidence_id, evidence_conn,
                        warning_collector=warning_collector,
                    )
                    total_config_records += config_records
                    records_parsed = config_records
                    notes = json.dumps(notes_dict, sort_keys=True)

                elif file_type == "state":
                    state_records, notes_dict = self._ingest_state_file(
                        file_path, source_path, file_entry, run_id, evidence_id, evidence_conn,
                        warning_collector=warning_collector,
                    )
                    total_state_records += state_records
                    records_parsed = state_records
                    notes = json.dumps(notes_dict, sort_keys=True)

                elif file_type == "cached":
                    # Basic cached file analysis
                    parsed = parse_cached_file(
                        file_path, source_path, file_type,
                        warning_collector=warning_collector,
                    )
                    notes = json.dumps(parsed.get("summary", {}), sort_keys=True)

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=records_parsed,
                    notes=notes,
                )

                total_files += 1

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")

                if "inventory_id" in locals():
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(e),
                    )

        # Flush warnings to database
        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)

        evidence_conn.commit()

        total_records = total_config_records + total_state_records
        if stats:
            stats.report_ingested(evidence_id, self.metadata.name, records=total_records, files=total_files)
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {
            "files": total_files,
            "config_records": total_config_records,
            "state_records": total_state_records,
        }

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def _generate_run_id(self) -> str:
        """Generate run ID: tor_state_{timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"tor_state_{timestamp}_{unique_id}"

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

    def _get_extraction_tool_version(self) -> str:
        """Build extraction tool version string."""
        try:
            import pytsk3
            pytsk_version = pytsk3.TSK_VERSION_STR
        except ImportError:
            pytsk_version = "unknown"
        return f"pytsk3:{pytsk_version}"

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        """Clear data from a previous run with the same run_id."""
        deleted = 0
        deleted += delete_browser_config_by_run(evidence_conn, evidence_id, run_id)
        deleted += delete_tor_state_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d records from previous run %s", deleted, run_id)

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, List[Dict]]:
        """
        Discover Tor files across all partitions using file_list table.

        Returns:
            Dict mapping partition_index -> list of file info dicts
        """
        files_by_partition: Dict[int, List[Dict]] = {}

        # Check if file_list is available
        if not check_file_list_available(evidence_conn, evidence_id):
            callbacks.on_log(
                "file_list table empty, falling back to single partition scan",
                "warning"
            )
            tor_files = self._discover_files_single_partition(evidence_fs, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if tor_files:
                files_by_partition[partition_index] = tor_files
            return files_by_partition

        # Use file_list discovery
        # Build filename patterns from artifact patterns
        filename_patterns = []
        for artifact in TOR_ARTIFACT_PATTERNS:
            # Handle wildcards in artifact names
            if "*" in artifact or "/" in artifact:
                # For patterns like "cached-*" or "pt_state/*", extract the base
                base = artifact.split("/")[-1]
                if "*" in base:
                    filename_patterns.append(base)
                else:
                    filename_patterns.append(base)
            else:
                filename_patterns.append(artifact)

        # Build path patterns from TOR_DATA_ROOTS
        path_patterns = [f"%{root.replace('*', '%')}%" for root in TOR_DATA_ROOTS]

        try:
            result = discover_from_file_list(
                evidence_conn,
                evidence_id,
                filename_patterns=filename_patterns,
                path_patterns=path_patterns,
            )

            LOGGER.info("discover_from_file_list: %s", result.get_partition_summary())

            if result.is_empty:
                callbacks.on_log("No Tor files found via file_list discovery", "info")
                return files_by_partition

            # Process matches by partition
            for partition_idx, matches in result.matches_by_partition.items():
                partition_files = []
                for match in matches:
                    file_type = classify_tor_file(match.file_path)
                    partition_files.append({
                        "logical_path": match.file_path,
                        "browser": "tor",
                        "profile": "tor_data",
                        "file_type": file_type,
                        "artifact_type": "tor_state",
                        "partition_index": partition_idx,
                        "inode": match.inode,
                        "size_bytes": match.size_bytes,
                    })
                    callbacks.on_log(
                        f"Found Tor file (partition {partition_idx}): {match.file_path}",
                        "info"
                    )

                if partition_files:
                    files_by_partition[partition_idx] = partition_files

        except Exception as e:
            LOGGER.warning("file_list discovery failed: %s, falling back", e)
            callbacks.on_log(f"file_list discovery failed: {e}", "warning")
            tor_files = self._discover_files_single_partition(evidence_fs, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if tor_files:
                files_by_partition[partition_index] = tor_files

        return files_by_partition

    def _discover_files_single_partition(
        self,
        evidence_fs,
        callbacks: ExtractorCallbacks
    ) -> List[Dict[str, Any]]:
        """
        Discover Tor files using filesystem iteration (single partition).

        Fallback when file_list is not available.
        """
        files = []
        seen_paths = set()
        partition_index = getattr(evidence_fs, 'partition_index', 0)

        for root in TOR_DATA_ROOTS:
            for artifact in TOR_ARTIFACT_PATTERNS:
                pattern = f"{root}/{artifact}"
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        if path_str in seen_paths:
                            continue
                        seen_paths.add(path_str)

                        file_type = classify_tor_file(path_str)
                        files.append({
                            "logical_path": path_str,
                            "browser": "tor",
                            "profile": "tor_data",
                            "file_type": file_type,
                            "artifact_type": "tor_state",
                            "partition_index": partition_index,
                        })
                        callbacks.on_log(f"Found Tor state file: {path_str}", "info")
                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return files

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict[str, Any],
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Dict[str, Any]:
        """
        Copy file from evidence to workspace with collision-safe naming.

        Filename format: tor_{type}_p{partition}_{hash}_{name}
        The mini-hash ensures uniqueness for same-named files in different locations.
        """
        try:
            source_path = file_info["logical_path"]
            file_type = file_info["file_type"]
            partition_index = file_info.get("partition_index", 0)

            # Create collision-safe filename
            safe_type = file_type.replace("/", "_")
            original_name = Path(source_path).name
            path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]
            filename = f"tor_{safe_type}_p{partition_index}_{path_hash}_{original_name}"
            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()
            size = len(file_content)

            # Get filesystem type
            fs_type = getattr(evidence_fs, "fs_type", "unknown")
            if not isinstance(fs_type, str):
                fs_type = "unknown"

            return {
                "copy_status": "ok",
                "size_bytes": size,
                "file_size_bytes": size,
                "md5": md5,
                "sha256": sha256,
                "extracted_path": str(dest_path),
                "logical_path": source_path,
                "browser": "tor",
                "profile": "tor_data",
                "file_type": file_type,
                "artifact_type": "tor_state",
                "partition_index": partition_index,
                "fs_type": fs_type,
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
                "logical_path": file_info.get("logical_path"),
                "browser": "tor",
                "profile": "tor_data",
                "file_type": file_info.get("file_type"),
                "artifact_type": "tor_state",
                "partition_index": file_info.get("partition_index", 0),
                "error_message": str(e),
            }

    def _ingest_torrc(
        self,
        file_path: Path,
        source_path: str,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> tuple[int, Dict]:
        """
        Parse torrc and insert into browser_config table.

        Returns:
            Tuple of (record_count, summary_dict)
        """
        parsed = parse_torrc(file_path, source_path, warning_collector=warning_collector)

        if not parsed["records"]:
            return 0, parsed["summary"]

        # Build records for database
        records = []
        for rec in parsed["records"]:
            records.append({
                "run_id": run_id,
                "browser": "tor",
                "profile": file_entry.get("profile", "tor_data"),
                "config_type": "torrc",
                "config_key": rec["config_key"],
                "config_value": rec["config_value"],
                "value_count": rec["value_count"],
                "source_path": source_path,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": source_path,
            })

        count = insert_browser_configs(evidence_conn, evidence_id, records)
        return count, parsed["summary"]

    def _ingest_state_file(
        self,
        file_path: Path,
        source_path: str,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> tuple[int, Dict]:
        """
        Parse Tor state file and insert into tor_state table.

        Returns:
            Tuple of (record_count, summary_dict)
        """
        parsed = parse_state_file(file_path, source_path, warning_collector=warning_collector)

        if not parsed["records"]:
            return 0, parsed["summary"]

        # Build records for database
        records = []
        for rec in parsed["records"]:
            records.append({
                "run_id": run_id,
                "profile": file_entry.get("profile", "tor_data"),
                "state_key": rec["state_key"],
                "state_value": rec["state_value"],
                "timestamp_utc": rec.get("timestamp_utc"),
                "source_path": source_path,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": source_path,
            })

        count = insert_tor_states(evidence_conn, evidence_id, records)
        return count, parsed["summary"]
