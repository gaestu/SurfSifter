"""
Internet Explorer WebCache Extractor

Extracts the WebCacheV01.dat ESE database from evidence images.
This is the core extractor that copies the raw WebCache database and its
log files for subsequent parsing by artifact-specific extractors.

The WebCache database contains:
- History: Browsing history entries
- Cookies: Cookie data
- iedownload: Download history
- Content: Cached content metadata
- DOMStore: DOM storage

Features:
- Multi-partition discovery via file_list
- ESE journal/log file copying for database recovery
- Forensic provenance tracking
- StatisticsCollector integration
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
    IE_BROWSERS,
    IE_ARTIFACTS,
    get_patterns,
    get_all_patterns,
    detect_browser_from_path,
    extract_user_from_path,
)
from .._ese_reader import check_ese_available
from core.logging import get_logger


LOGGER = get_logger("extractors.browser.ie_legacy.webcache")


class IEWebCacheExtractor(BaseExtractor):
    """
    Extract WebCacheV01.dat ESE database from IE/Legacy Edge.

    This extractor handles the raw extraction phase - copying the WebCache
    database and its associated log files from the evidence image to the
    case workspace.

    The ingestion phase is handled by artifact-specific extractors:
    - IEHistoryExtractor: Parse history entries
    - IECookiesExtractor: Parse cookie entries
    - IEDownloadsExtractor: Parse download entries

    WebCache Location:
        %LOCALAPPDATA%\\Microsoft\\Windows\\WebCache\\WebCacheV01.dat

    Associated Files:
        - V01.log, V01.chk: Transaction logs
        - V01*.log: Additional logs
        - V01res*.jrs: Reserved logs
        - WebCacheV01.jfm: Flush map
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="ie_webcache",
            display_name="IE/Edge WebCache",
            description="Extract WebCacheV01.dat database (History, Cookies, Downloads)",
            category="browser",
            requires_tools=[],  # ESE library checked at runtime
            can_extract=True,
            can_ingest=False,  # Ingestion handled by artifact-specific extractors
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted"
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """
        WebCache extractor doesn't do ingestion.

        Use IEHistoryExtractor, IECookiesExtractor, etc. for ingestion.
        """
        return False, "Use artifact-specific extractors for ingestion (IEHistoryExtractor, etc.)"

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """
        Return configuration widget (multi-partition option).
        """
        return MultiPartitionWidget(parent, default_scan_all=True)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction state."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"IE/Edge WebCache\nFiles: {file_count}\nRun: {data.get('run_id', 'N/A')[:20]}"
        else:
            status_text = "IE/Edge WebCache\nNo extraction yet"

        # Add ESE library status
        ese_ok, ese_info = check_ese_available()
        if not ese_ok:
            status_text += "\n⚠️ ESE library not installed"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "ie_webcache"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract WebCache database and log files from evidence.

        Workflow:
            1. Generate run_id
            2. Scan evidence for WebCacheV01.dat files (multi-partition if enabled)
            3. Copy WebCache files and associated logs to output_dir/
            4. Calculate hashes, collect E01 context
            5. Write manifest.json
        """
        callbacks.on_step("Initializing IE/Edge WebCache extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting IE/Edge WebCache extraction (run_id=%s)", run_id)

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
            "schema_version": "1.0.0",
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

        # Scan for WebCache files
        callbacks.on_step("Scanning for WebCache databases")

        files_by_partition: Dict[int, List[Dict]] = {}

        if scan_all_partitions and evidence_conn is not None:
            files_by_partition = self._discover_files_multi_partition(
                evidence_fs, evidence_conn, evidence_id, callbacks
            )
        else:
            if scan_all_partitions and evidence_conn is None:
                callbacks.on_log(
                    "Multi-partition scan requested but no evidence_conn provided, using single partition",
                    "warning"
                )
            webcache_files = self._discover_files(evidence_fs, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if webcache_files:
                files_by_partition[partition_index] = webcache_files

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

        callbacks.on_log(f"Found {len(all_files)} WebCache file(s) across {len(files_by_partition)} partition(s)")

        if not all_files:
            LOGGER.info("No WebCache files found")
            manifest_data["notes"].append("No WebCacheV01.dat files found in evidence")
        else:
            callbacks.on_progress(0, len(all_files), "Extracting WebCache databases")

            # Get EWF paths for opening other partitions
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            file_index = 0

            # Process each partition
            for partition_index in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_index]

                # Determine which filesystem to use
                current_partition = getattr(evidence_fs, 'partition_index', 0)

                fs_ctx = (
                    open_partition_for_extraction(evidence_fs, None)
                    if (partition_index == current_partition or ewf_paths is None)
                    else open_partition_for_extraction(ewf_paths, partition_index)
                )

                try:
                    with fs_ctx as fs_to_use:
                        if fs_to_use is None:
                            callbacks.on_log(
                                f"Failed to open partition {partition_index}, skipping",
                                "warning"
                            )
                            continue

                        for file_info in partition_files:
                            if callbacks.is_cancelled():
                                break

                            file_index += 1
                            callbacks.on_progress(
                                file_index, len(all_files),
                                f"Extracting {file_info.get('user', 'unknown')} WebCache"
                            )

                            try:
                                # Copy main WebCache file
                                result = self._extract_file(
                                    fs_to_use, file_info, output_dir, callbacks
                                )
                                result["partition_index"] = partition_index
                                result["user"] = file_info.get("user", "unknown")
                                result["browser"] = file_info.get("browser", "ie")
                                manifest_data["files"].append(result)

                                # Copy associated log files
                                log_files = self._extract_log_files(
                                    fs_to_use, file_info, output_dir, callbacks
                                )
                                for log_file in log_files:
                                    log_file["partition_index"] = partition_index
                                    log_file["file_type"] = "log"
                                    manifest_data["files"].append(log_file)

                            except Exception as e:
                                error_msg = f"Failed to extract {file_info.get('logical_path')}: {e}"
                                LOGGER.error(error_msg, exc_info=True)
                                callbacks.on_error(error_msg, "")
                                manifest_data["notes"].append(error_msg)
                except Exception as e:
                    LOGGER.error("Failed to open partition %d: %s", partition_index, e)

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
            "IE/Edge WebCache extraction complete: %d files from %d partition(s), status=%s",
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
        Ingestion is handled by artifact-specific extractors.
        """
        callbacks.on_log(
            "WebCache extractor only handles extraction. "
            "Use IEHistoryExtractor for history ingestion.",
            "info"
        )
        return {}

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

        # ESE library
        ese_ok, ese_info = check_ese_available()
        if ese_ok:
            versions.append(f"ese:{ese_info}")
        else:
            versions.append("ese:not_installed")

        # pytsk3/pyewf
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
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """
        Scan evidence for WebCache files (single partition).
        """
        webcache_files = []

        # Get patterns for all browsers
        patterns = get_all_patterns("webcache")

        for pattern in patterns:
            try:
                for match in evidence_fs.iter_paths(pattern):
                    # Skip ($FILE_NAME) entries
                    if "($FILE_NAME)" in match:
                        continue

                    # Only match WebCacheV01.dat
                    if not match.lower().endswith("webcachev01.dat"):
                        continue

                    browser = detect_browser_from_path(match)
                    user = extract_user_from_path(match)

                    webcache_files.append({
                        "logical_path": match,
                        "browser": browser,
                        "user": user,
                        "artifact_type": "webcache",
                    })

                    callbacks.on_log(f"Found WebCache: {match}", "info")

            except Exception as e:
                LOGGER.warning("Error scanning pattern %s: %s", pattern, e)

        return webcache_files

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, List[Dict]]:
        """
        Discover WebCache files across ALL partitions using file_list.
        """
        # Check if file_list is available
        available, count = check_file_list_available(evidence_conn, evidence_id) if evidence_conn else (False, 0)

        if not available:
            callbacks.on_log(
                "file_list empty, falling back to single-partition discovery",
                "info"
            )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, callbacks)
            return {partition_index: files} if files else {}

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")

        # Query file_list for WebCache files
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["WebCacheV01.dat"],
            path_patterns=["%WebCache%"],
        )

        if result.is_empty:
            callbacks.on_log(
                "No WebCache files found in file_list, falling back to filesystem scan",
                "warning"
            )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, callbacks)
            return {partition_index: files} if files else {}

        if result.is_multi_partition:
            callbacks.on_log(
                f"Found WebCache files on {len(result.partitions_with_matches)} partitions: "
                f"{result.partitions_with_matches}",
                "info"
            )

        # Convert to extractor format
        files_by_partition: Dict[int, List[Dict]] = {}

        for partition_index, matches in result.matches_by_partition.items():
            files_list = []
            for match in matches:
                # Skip non-WebCache files
                if not match.file_name.lower() == "webcachev01.dat":
                    continue

                browser = detect_browser_from_path(match.file_path)
                user = extract_user_from_path(match.file_path)

                files_list.append({
                    "logical_path": match.file_path,
                    "browser": browser,
                    "user": user,
                    "artifact_type": "webcache",
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

                callbacks.on_log(
                    f"Found WebCache on partition {partition_index}: {match.file_path}",
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
        """Copy WebCache file from evidence to workspace with metadata."""
        source_path = file_info["logical_path"]
        user = file_info.get("user", "unknown")

        # Create output filename
        safe_user = user.replace(" ", "_").replace("/", "_").replace("\\", "_")
        filename = f"{safe_user}_WebCacheV01.dat"
        dest_path = output_dir / filename

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
            "logical_path": source_path,
            "artifact_type": "webcache",
            "file_type": "database",
        }

    def _extract_log_files(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """
        Copy WebCache log/journal files for ESE database recovery.

        These files are needed to properly open the ESE database if it
        wasn't cleanly closed. ESE uses transaction logs and reserved
        journal files for crash recovery.

        Files copied:
            - V01.log: Current transaction log
            - V01.chk: Checkpoint file
            - V01tmp.log: Temporary transaction log
            - WebCacheV01.jfm: Flush map file
            - V01*.log: Numbered transaction log files (V0100001.log, etc.)
            - V01res*.jrs: Reserved journal files for emergency recovery
        """
        source_path = file_info["logical_path"]
        source_dir = str(Path(source_path).parent)
        user = file_info.get("user", "unknown")
        safe_user = user.replace(" ", "_").replace("/", "_").replace("\\", "_")

        log_files = []

        # Static log file patterns to look for
        log_patterns = [
            "V01.log",
            "V01.chk",
            "V01tmp.log",
            "WebCacheV01.jfm",
        ]

        # Extract static log files
        for pattern in log_patterns:
            try:
                log_path = f"{source_dir}/{pattern}"
                log_content = evidence_fs.read_file(log_path)

                dest_filename = f"{safe_user}_{pattern}"
                dest_path = output_dir / dest_filename
                dest_path.write_bytes(log_content)

                log_files.append({
                    "copy_status": "ok",
                    "size_bytes": len(log_content),
                    "md5": hashlib.md5(log_content).hexdigest(),
                    "sha256": hashlib.sha256(log_content).hexdigest(),
                    "extracted_path": str(dest_path),
                    "logical_path": log_path,
                    "artifact_type": "webcache_log",
                    "user": user,
                })

                callbacks.on_log(f"Copied log file: {pattern}", "info")

            except Exception:
                # Log file doesn't exist or can't be read
                pass

        # Extract numbered transaction log files (V0100001.log, V0100002.log, etc.)
        try:
            for entry in evidence_fs.iter_paths(f"{source_dir}/V01*.log"):
                if entry == source_path:
                    continue
                filename = Path(entry).name
                # Skip already-extracted static files
                if filename in log_patterns:
                    continue
                if filename.startswith("V01") and filename.endswith(".log"):
                    try:
                        log_content = evidence_fs.read_file(entry)
                        dest_filename = f"{safe_user}_{filename}"
                        dest_path = output_dir / dest_filename
                        dest_path.write_bytes(log_content)

                        log_files.append({
                            "copy_status": "ok",
                            "size_bytes": len(log_content),
                            "md5": hashlib.md5(log_content).hexdigest(),
                            "sha256": hashlib.sha256(log_content).hexdigest(),
                            "extracted_path": str(dest_path),
                            "logical_path": entry,
                            "artifact_type": "webcache_log",
                            "user": user,
                        })

                        callbacks.on_log(f"Copied numbered log: {filename}", "info")
                    except Exception:
                        pass
        except Exception:
            pass

        # Extract reserved journal files (V01res00001.jrs, V01res00002.jrs, etc.)
        # These are critical for ESE database recovery after crashes
        try:
            for entry in evidence_fs.iter_paths(f"{source_dir}/V01res*.jrs"):
                filename = Path(entry).name
                if filename.startswith("V01res") and filename.endswith(".jrs"):
                    try:
                        log_content = evidence_fs.read_file(entry)
                        dest_filename = f"{safe_user}_{filename}"
                        dest_path = output_dir / dest_filename
                        dest_path.write_bytes(log_content)

                        log_files.append({
                            "copy_status": "ok",
                            "size_bytes": len(log_content),
                            "md5": hashlib.md5(log_content).hexdigest(),
                            "sha256": hashlib.sha256(log_content).hexdigest(),
                            "extracted_path": str(dest_path),
                            "logical_path": entry,
                            "artifact_type": "webcache_reserved_log",
                            "user": user,
                        })

                        callbacks.on_log(f"Copied reserved journal: {filename}", "info")
                    except Exception:
                        pass
        except Exception:
            pass

        return log_files
