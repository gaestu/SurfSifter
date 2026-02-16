"""
Internet Explorer Typed URLs Extractor

Extracts typed URLs from the Windows Registry (NTUSER.DAT).

IE stores manually typed URLs in the registry at:
    HKCU\\Software\\Microsoft\\Internet Explorer\\TypedURLs

This extractor:
1. Discovers NTUSER.DAT files from evidence
2. Copies them to workspace
3. Parses registry to extract TypedURLs values
4. Inserts into urls table with provenance

Note: This extractor handles extraction AND ingestion since
TypedURLs are stored in user profile registry hives.

Dependencies:
- regipy (for offline registry parsing)
- system_registry extractor (shared registry parsing code)
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
from ....widgets import MultiPartitionWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    get_ewf_paths_from_evidence_fs,
    open_partition_for_extraction,
)
from .._patterns import (
    extract_user_from_path,
)
from .._timestamps import filetime_to_iso
from core.logging import get_logger
from core.database import (
    insert_urls,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.typed_urls")


# TypedURLs registry key paths
TYPED_URLS_KEY = "Software\\Microsoft\\Internet Explorer\\TypedURLs"
TYPED_URLS_TIME_KEY = "Software\\Microsoft\\Internet Explorer\\TypedURLsTime"


def _check_regipy_available() -> tuple[bool, Optional[str]]:
    """Check if regipy is available for registry parsing."""
    try:
        import regipy
        from regipy.registry import RegistryHive
        return True, regipy.__version__ if hasattr(regipy, '__version__') else "unknown"
    except ImportError:
        return False, None


class IETypedURLsExtractor(BaseExtractor):
    """
    Extract typed URLs from Windows Registry (NTUSER.DAT).

    IE stores manually typed URLs (from address bar) in:
        HKCU\\Software\\Microsoft\\Internet Explorer\\TypedURLs

    This provides forensic evidence of URLs directly typed by
    the user (not accessed via links/bookmarks).

    Workflow:
    1. Discover NTUSER.DAT files from evidence
    2. Copy to workspace for analysis
    3. Parse registry offline using regipy
    4. Extract TypedURLs values (url1, url2, ...)
    5. Insert into urls table
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="ie_typed_urls",
            display_name="IE Typed URLs",
            description="Extract manually typed URLs from Windows Registry",
            category="browser",
            requires_tools=["regipy"],
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted"
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run (manifest + regipy)."""
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"

        available, _ = _check_regipy_available()
        if not available:
            return False, "regipy library not installed"

        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget (multi-partition option)."""
        return MultiPartitionWidget(parent, default_scan_all=True)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction/ingestion state."""
        available, version = _check_regipy_available()

        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"IE Typed URLs\nFiles: {file_count}"
            if not available:
                status_text += "\n⚠️ regipy not installed"
        else:
            status_text = "IE Typed URLs\nNo extraction yet"
            if not available:
                status_text += "\n⚠️ regipy not installed"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "ie_typed_urls"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract NTUSER.DAT files from evidence.

        We copy the registry hives to workspace for offline parsing.
        """
        callbacks.on_step("Initializing IE Typed URLs extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting IE Typed URLs extraction (run_id=%s)", run_id)

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
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "multi_partition_extraction": True,
            "partitions_scanned": [],
            "partitions_with_artifacts": [],
            "files": [],
            "status": "ok",
            "notes": [],
        }

        # Scan for NTUSER.DAT files
        callbacks.on_step("Scanning for NTUSER.DAT files")

        if evidence_conn is None:
            error_msg = (
                "file_list discovery requires evidence_conn; cannot run Typed URLs "
                "extraction without file_list data"
            )
            LOGGER.error(error_msg)
            callbacks.on_error(error_msg, "")
            manifest_data["status"] = "error"
            manifest_data["notes"].append(error_msg)
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="error")
            callbacks.on_step("Writing manifest")
            (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))
            return False

        available, count = check_file_list_available(evidence_conn, evidence_id)
        if not available:
            error_msg = (
                "file_list is empty/unavailable for this evidence; cannot run Typed URLs "
                "extraction without file_list data. Run file_list extraction first."
            )
            LOGGER.error(error_msg)
            callbacks.on_error(error_msg, "")
            manifest_data["status"] = "error"
            manifest_data["notes"].append(error_msg)
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="error")
            callbacks.on_step("Writing manifest")
            (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))
            return False

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")
        files_by_partition = self._discover_files_multi_partition(
            evidence_conn, evidence_id, callbacks
        )

        # Flatten for counting
        all_files = []
        for files_list in files_by_partition.values():
            all_files.extend(files_list)

        # Update manifest with partition info
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = sorted(
            p for p, files in files_by_partition.items() if files
        )

        # Report discovered files
        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=len(all_files))

        callbacks.on_log(f"Found {len(all_files)} NTUSER.DAT file(s)")

        if not all_files:
            LOGGER.info("No NTUSER.DAT files found")
            manifest_data["notes"].append("No NTUSER.DAT files found")
        else:
            callbacks.on_progress(0, len(all_files), "Extracting registry hives")

            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            file_index = 0

            for partition_index in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_index]
                current_partition = getattr(evidence_fs, 'partition_index', 0)

                fs_ctx = (
                    open_partition_for_extraction(evidence_fs, None)
                    if (partition_index == current_partition or ewf_paths is None)
                    else open_partition_for_extraction(ewf_paths, partition_index)
                )

                try:
                    with fs_ctx as fs_to_use:
                        if fs_to_use is None:
                            callbacks.on_log(f"Failed to open partition {partition_index}", "warning")
                            continue

                        for file_info in partition_files:
                            if callbacks.is_cancelled():
                                manifest_data["status"] = "cancelled"
                                break

                            file_index += 1
                            callbacks.on_progress(
                                file_index, len(all_files),
                                f"Copying {file_info.get('user', 'unknown')}/NTUSER.DAT"
                            )

                            try:
                                result = self._extract_file(
                                    fs_to_use, file_info, output_dir, callbacks,
                                    partition_index=partition_index,
                                )
                                result["partition_index"] = partition_index
                                manifest_data["files"].append(result)
                            except Exception as e:
                                error_msg = f"Failed to extract {file_info.get('logical_path')}: {e}"
                                LOGGER.error(error_msg, exc_info=True)
                                manifest_data["notes"].append(error_msg)
                except Exception as e:
                    error_msg = f"Failed to open/read partition {partition_index}: {e}"
                    callbacks.on_log(error_msg, "warning")
                    manifest_data["notes"].append(error_msg)

                if manifest_data["status"] == "cancelled":
                    break

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
            "IE Typed URLs extraction complete: %d files, status=%s",
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
        Parse extracted NTUSER.DAT files and ingest TypedURLs.

        Workflow:
            1. Read manifest.json
            2. For each NTUSER.DAT:
               - Open registry hive with regipy
               - Navigate to TypedURLs key
               - Extract url1, url2, etc.
               - Insert into urls table
            3. Return counts
        """
        callbacks.on_step("Reading typed URLs manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"urls": 0}

        # Check regipy availability
        available, _ = _check_regipy_available()
        if not available:
            callbacks.on_error("regipy not installed", "Install with: pip install regipy")
            return {"urls": 0}

        from regipy.registry import RegistryHive

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())
        evidence_label = config.get("evidence_label", "")
        files = manifest_data.get("files", [])

        # Continue statistics tracking
        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"urls": 0}

        total_urls = 0
        failed_files = 0
        all_url_records = []

        callbacks.on_progress(0, len(files), "Parsing registry hives")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(files),
                f"Parsing {file_entry.get('user', 'unknown')}/NTUSER.DAT"
            )

            inventory_id = None
            try:
                hive_path = Path(file_entry["extracted_path"])
                if not hive_path.is_absolute():
                    hive_path = output_dir / hive_path

                if not hive_path.exists():
                    callbacks.on_log(f"File not found: {hive_path}", "warning")
                    failed_files += 1
                    continue

                # Register in browser inventory (provenance tracking)
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser="ie",
                    artifact_type="typed_urls",
                    run_id=run_id,
                    extracted_path=file_entry.get("extracted_path", ""),
                    extraction_status="ok",
                    extraction_timestamp_utc=manifest_data.get("extraction_timestamp_utc"),
                    logical_path=file_entry.get("logical_path", ""),
                    profile=file_entry.get("user"),
                    partition_index=file_entry.get("partition_index"),
                    file_size_bytes=file_entry.get("file_size_bytes"),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                )

                # Parse TypedURLs from registry
                urls = self._parse_typed_urls(
                    hive_path, file_entry, run_id, RegistryHive
                )

                # Update inventory with ingestion status
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=len(urls),
                )

                # Collect URL records for batch insert
                all_url_records.extend(urls)
                total_urls += len(urls)

            except Exception as e:
                error_msg = f"Failed to parse {file_entry.get('extracted_path')}: {e}"
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

        # Batch insert all URL records
        if all_url_records:
            insert_urls(evidence_conn, evidence_id, all_url_records)

        evidence_conn.commit()

        # Report final statistics
        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=total_urls,
            )
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(f"Ingested {total_urls} typed URLs", "info")

        return {"urls": total_urls}

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

    def _discover_files_multi_partition(
        self,
        evidence_conn,
        evidence_id: int,
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, List[Dict]]:
        """Discover NTUSER.DAT files across ALL partitions using file_list."""
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["NTUSER.DAT"],
            path_patterns=["%Users%", "%Documents and Settings%"],
        )

        if result.is_empty:
            callbacks.on_log("No NTUSER.DAT files found in file_list", "info")
            return {}

        files_by_partition: Dict[int, List[Dict]] = {}

        for partition_index, matches in result.matches_by_partition.items():
            files_list = []
            for match in matches:
                if match.file_name.upper() != "NTUSER.DAT":
                    continue

                user = extract_user_from_path(match.file_path)

                files_list.append({
                    "logical_path": match.file_path,
                    "user": user,
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

            if files_list:
                files_by_partition[partition_index] = files_list

        return files_by_partition

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks,
        partition_index: int = 0,
    ) -> Dict:
        """Copy NTUSER.DAT from evidence to workspace."""
        source_path = file_info["logical_path"]
        user = file_info.get("user", "unknown")

        # Create output filename (include partition to avoid collisions)
        safe_user = user.replace(" ", "_").replace("/", "_").replace("\\", "_")
        filename = f"p{partition_index}_{safe_user}_NTUSER.DAT"
        dest_path = output_dir / filename

        # Read and write file
        file_content = evidence_fs.read_file(source_path)
        dest_path.write_bytes(file_content)

        # Calculate hashes
        md5 = hashlib.md5(file_content).hexdigest()
        sha256 = hashlib.sha256(file_content).hexdigest()

        return {
            "copy_status": "ok",
            "file_size_bytes": len(file_content),
            "md5": md5,
            "sha256": sha256,
            "extracted_path": str(dest_path.relative_to(output_dir)),
            "logical_path": source_path,
            "user": user,
        }

    def _parse_typed_urls(
        self,
        hive_path: Path,
        file_entry: Dict,
        run_id: str,
        RegistryHive
    ) -> List[Dict]:
        """
        Parse TypedURLs from NTUSER.DAT registry hive.

        Parses both TypedURLs (url1, url2, ...) and TypedURLsTime (url1Time, url2Time, ...)
        to correlate URLs with their individual timestamps.

        Returns list of URL records for database insertion.
        """
        urls = []

        try:
            hive = RegistryHive(str(hive_path))
        except Exception as e:
            LOGGER.warning("Failed to open registry hive %s: %s", hive_path, e)
            return urls

        try:
            # Navigate to TypedURLs key
            key = hive.get_key(TYPED_URLS_KEY)
            if key is None:
                LOGGER.info("TypedURLs key not found in %s", hive_path)
                return urls

            # Get key's last write timestamp (FILETIME format) as fallback
            key_timestamp_iso = None
            try:
                if hasattr(key, 'timestamp') and key.timestamp:
                    # regipy returns timestamp as datetime object
                    if isinstance(key.timestamp, datetime):
                        key_timestamp_iso = key.timestamp.isoformat()
                    else:
                        key_timestamp_iso = key.timestamp
            except Exception as e:
                LOGGER.debug("Could not get key timestamp: %s", e)

            # Parse TypedURLsTime for per-URL timestamps
            # IE stores individual timestamps in a separate key
            url_timestamps = self._parse_typed_urls_time(hive)

            # Extract url1, url2, etc.
            for value in key.iter_values():
                value_name = value.name.lower()

                # TypedURLs are stored as url1, url2, url3, ...
                if value_name.startswith("url") and value_name[3:].isdigit():
                    url = value.value
                    if url and isinstance(url, str):
                        url_index = int(value_name[3:])

                        # Use per-URL timestamp if available, else fall back to key timestamp
                        url_timestamp = url_timestamps.get(url_index, key_timestamp_iso)

                        urls.append(self._build_url_record(
                            url, file_entry, run_id,
                            index=url_index,
                            url_timestamp=url_timestamp,
                            key_timestamp=key_timestamp_iso
                        ))

        except Exception as e:
            LOGGER.warning("Error parsing TypedURLs from %s: %s", hive_path, e)

        return urls

    def _parse_typed_urls_time(
        self,
        hive,
    ) -> Dict[int, str]:
        r"""
        Parse TypedURLsTime registry key for per-URL timestamps.

        IE stores URL typing timestamps in:
            HKCU\Software\Microsoft\Internet Explorer\TypedURLsTime

        Values are named url1Time, url2Time, etc. and contain FILETIME values
        (64-bit little-endian integers).

        Returns:
            Dict mapping URL index (1, 2, 3, ...) to ISO timestamp string
        """
        url_timestamps: Dict[int, str] = {}

        try:
            time_key = hive.get_key(TYPED_URLS_TIME_KEY)
            if time_key is None:
                LOGGER.debug("TypedURLsTime key not found (may not exist on older IE)")
                return url_timestamps

            for value in time_key.iter_values():
                value_name = value.name.lower()

                # TypedURLsTime values are named url1Time, url2Time, etc.
                if value_name.startswith("url") and value_name.endswith("time"):
                    # Extract index from "url1time" -> 1
                    index_str = value_name[3:-4]  # Remove "url" prefix and "time" suffix
                    if index_str.isdigit():
                        url_index = int(index_str)

                        # Value is binary FILETIME (8 bytes, little-endian)
                        raw_value = value.value
                        if raw_value and isinstance(raw_value, bytes) and len(raw_value) == 8:
                            filetime = int.from_bytes(raw_value, byteorder='little')
                            iso_timestamp = filetime_to_iso(filetime)
                            if iso_timestamp:
                                url_timestamps[url_index] = iso_timestamp
                                LOGGER.debug(
                                    "TypedURLsTime url%d: %s",
                                    url_index, iso_timestamp
                                )
                        elif raw_value and isinstance(raw_value, int):
                            # Some versions may store as integer directly
                            iso_timestamp = filetime_to_iso(raw_value)
                            if iso_timestamp:
                                url_timestamps[url_index] = iso_timestamp

            if url_timestamps:
                LOGGER.info(
                    "Found %d TypedURLsTime entries",
                    len(url_timestamps)
                )

        except Exception as e:
            LOGGER.debug("Error parsing TypedURLsTime: %s", e)

        return url_timestamps

    def _build_url_record(
        self,
        url: str,
        file_entry: Dict,
        run_id: str,
        index: int = 0,
        url_timestamp: Optional[str] = None,
        key_timestamp: Optional[str] = None,
    ) -> Dict:
        """Build URL record for database insertion.

        Args:
            url: The typed URL
            file_entry: File metadata dict
            run_id: Extraction run identifier
            index: URL index from registry (url1, url2, etc.)
            url_timestamp: Per-URL timestamp from TypedURLsTime (ISO format)
                          This is the actual typing timestamp if available
            key_timestamp: Registry key's last write time (ISO format)
                          Used as fallback if per-URL timestamp unavailable
        """
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

        # Use per-URL timestamp if available, otherwise fall back to key timestamp
        timestamp = url_timestamp or key_timestamp
        timestamp_source = "TypedURLsTime" if url_timestamp else "key_last_write"

        return {
            "url": url,
            "domain": domain,
            "scheme": scheme,
            "source_path": file_entry.get("logical_path", ""),
            "discovered_by": f"{self.metadata.name}:{self.metadata.version}:{run_id}",
            "run_id": run_id,
            "first_seen_utc": timestamp,
            "last_seen_utc": timestamp,
            "notes": f"TypedURL index: {index}, User: {file_entry.get('user', 'unknown')}, Timestamp source: {timestamp_source}",
        }
