"""
Internet Explorer / Legacy Edge Tab Recovery Extractor.

Extracts and parses session recovery files (.dat) from IE and Legacy Edge.
These files contain information about open tabs and windows that were
saved for session restoration.

Recovery File Locations:
- IE: Users/*/AppData/Local/Microsoft/Internet Explorer/Recovery/Active/*.dat
- IE: Users/*/AppData/Local/Microsoft/Internet Explorer/Recovery/Last Active/*.dat
- Edge Legacy: Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/User/Default/Recovery/Active/*.dat

Recovery File Format:
IE recovery .dat files are proprietary binary format containing:
- Window and tab state
- URLs of open tabs
- Tab history (back/forward navigation)
- Scroll positions and form data

Forensic Value:
- Shows tabs/windows that were open at crash or shutdown
- "Last Active" contains previous session (before current)
- May contain URLs not in regular history
- Tab order reveals user workflow and attention
- InPrivate remnants may appear in recovery files

Dependencies:
- None (pure Python parsing with binary format analysis)
"""

from __future__ import annotations

import hashlib
import json
import re
import struct
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse

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
    detect_browser_from_path,
)
from .._timestamps import filetime_to_datetime
from core.logging import get_logger
from core.database import (
    insert_urls,
    insert_browser_history_rows,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.tab_recovery")


# Recovery file signatures and patterns
IE_RECOVERY_SIGNATURE = b'\x00\x00\x00\x00'  # Placeholder - actual sig varies
URL_PATTERN = re.compile(rb'https?://[^\x00\x01-\x1f\x7f-\x9f]+', re.IGNORECASE)


class IETabRecoveryExtractor(BaseExtractor):
    """
    Extract and parse IE/Legacy Edge tab recovery files.

    Session recovery files contain the state of browser windows and tabs
    at the time of last shutdown or crash. These files can reveal:
    - URLs that were open (even if not in history)
    - Tab ordering and window arrangement
    - Previous session state ("Last Active" folder)
    - Potential InPrivate browsing remnants

    This extractor handles both extraction AND ingestion since recovery
    files can be parsed directly without ESE library.
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="ie_tab_recovery",
            display_name="IE/Edge Tab Recovery",
            description="Extract session recovery files (open tabs at shutdown/crash)",
            category="browser",
            requires_tools=[],  # Pure Python
            can_extract=True,
            can_ingest=True,
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
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"IE/Edge Tab Recovery\nFiles: {file_count}"
        else:
            status_text = "IE/Edge Tab Recovery\nNo extraction yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "ie_tab_recovery"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract tab recovery .dat files from evidence.

        Scans for recovery files in:
        - IE Recovery/Active and Recovery/Last Active folders
        - Edge Legacy UWP Recovery paths
        """
        callbacks.on_step("Initializing IE/Edge Tab Recovery extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting IE/Edge Tab Recovery extraction (run_id=%s)", run_id)

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

        # Scan for recovery files
        callbacks.on_step("Scanning for tab recovery files")

        if evidence_conn is None:
            error_msg = (
                "file_list discovery requires evidence_conn; cannot run Tab Recovery "
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
                "file_list is empty/unavailable for this evidence; cannot run Tab Recovery "
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

        callbacks.on_log(f"Found {len(all_files)} recovery file(s)")

        if not all_files:
            LOGGER.info("No recovery files found")
            manifest_data["notes"].append("No tab recovery files found")
        else:
            callbacks.on_progress(0, len(all_files), "Extracting recovery files")

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
                                f"Copying {Path(file_info.get('logical_path', '')).name}"
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
                    error_msg = f"Failed to access partition {partition_index}: {e}"
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
            "IE/Edge Tab Recovery extraction complete: %d files, status=%s",
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
        Parse extracted recovery files and ingest URLs.

        Parses binary recovery .dat files to extract:
        - URLs from open tabs
        - Session metadata
        """
        callbacks.on_step("Reading tab recovery manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"urls": 0, "tabs": 0}

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
            return {"urls": 0, "tabs": 0}

        total_urls = 0
        total_tabs = 0
        failed_files = 0
        all_url_records = []
        all_history_records = []

        callbacks.on_progress(0, len(files), "Parsing recovery files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(files),
                f"Parsing {Path(file_entry.get('extracted_path', '')).name}"
            )

            inventory_id = None
            try:
                dat_path = Path(file_entry["extracted_path"])
                if not dat_path.is_absolute():
                    dat_path = output_dir / dat_path

                if not dat_path.exists():
                    callbacks.on_log(f"File not found: {dat_path}", "warning")
                    failed_files += 1
                    continue

                # Register in browser inventory
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "ie"),
                    artifact_type="tab_recovery",
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

                # Parse recovery file
                tabs, urls = self._parse_recovery_file(
                    dat_path, file_entry, run_id, callbacks
                )

                # Update inventory with ingestion status
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=len(tabs),
                    urls_parsed=len(urls),
                )

                # Add to batch records
                all_history_records.extend(tabs)
                all_url_records.extend(urls)
                total_tabs += len(tabs)
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

        # Batch insert records
        if all_history_records:
            insert_browser_history_rows(evidence_conn, evidence_id, all_history_records)

        if all_url_records:
            insert_urls(evidence_conn, evidence_id, all_url_records)

        evidence_conn.commit()

        # Report final statistics
        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=total_tabs,
                urls=total_urls,
            )
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(
            f"Ingested {total_tabs} tab records, {total_urls} unique URLs from recovery files",
            "info"
        )

        return {"urls": total_urls, "tabs": total_tabs}

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
        """Discover recovery files across ALL partitions using file_list."""
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["*.dat"],
            path_patterns=["%Recovery%Active%", "%Recovery%Last Active%", "%Recovery%Immersive%"],
        )

        if result.is_empty:
            callbacks.on_log("No tab recovery files found in file_list", "info")
            return {}

        files_by_partition: Dict[int, List[Dict]] = {}

        for partition_index, matches in result.matches_by_partition.items():
            files_list = []
            for match in matches:
                # Only include .dat files in Recovery paths
                if not match.file_name.lower().endswith(".dat"):
                    continue
                if "recovery" not in match.file_path.lower():
                    continue

                user = extract_user_from_path(match.file_path)
                browser = detect_browser_from_path(match.file_path)
                recovery_type = self._detect_recovery_type(match.file_path)

                files_list.append({
                    "logical_path": match.file_path,
                    "user": user,
                    "browser": browser,
                    "recovery_type": recovery_type,
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

            if files_list:
                files_by_partition[partition_index] = files_list

        return files_by_partition

    def _detect_recovery_type(self, path: str) -> str:
        """Detect recovery type from path (active, last_active, immersive, inprivate)."""
        path_lower = path.lower()

        if "last active" in path_lower or "last_active" in path_lower:
            return "last_active"
        elif "immersive" in path_lower:
            return "immersive"
        elif "inprivate" in path_lower:
            return "inprivate"
        elif "active" in path_lower:
            return "active"
        else:
            return "unknown"

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks,
        partition_index: int = 0,
    ) -> Dict:
        """Copy recovery .dat file from evidence to workspace."""
        source_path = file_info["logical_path"]
        user = file_info.get("user", "unknown")
        recovery_type = file_info.get("recovery_type", "unknown")

        # Get original filename
        original_name = Path(source_path).name

        # Create unique output filename
        safe_user = user.replace(" ", "_").replace("/", "_").replace("\\", "_")
        filename = f"p{partition_index}_{safe_user}_{recovery_type}_{original_name}"
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
            "browser": file_info.get("browser", "ie"),
            "recovery_type": recovery_type,
        }

    def _parse_recovery_file(
        self,
        dat_path: Path,
        file_entry: Dict,
        run_id: str,
        callbacks: ExtractorCallbacks,
    ) -> tuple[List[Dict], List[Dict]]:
        """
        Parse IE/Edge recovery .dat file to extract URLs.

        IE recovery files are binary format. We use multiple strategies:
        1. Regex URL extraction (reliable for any format)
        2. Structure parsing for known formats (if signature matches)

        Returns:
            Tuple of (tab_records, url_records)
        """
        user = file_entry.get("user", "unknown")
        browser = file_entry.get("browser", "ie")
        source_path = file_entry.get("logical_path", "")
        recovery_type = file_entry.get("recovery_type", "unknown")
        partition_index = file_entry.get("partition_index", 0)
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        tab_records = []
        url_records = []
        seen_urls = set()

        try:
            data = dat_path.read_bytes()

            # Strategy 1: Extract URLs via regex (works on any binary format)
            urls_found = self._extract_urls_from_binary(data)

            callbacks.on_log(
                f"Found {len(urls_found)} URLs in {dat_path.name} ({recovery_type})",
                "info"
            )

            tab_index = 0
            for url in urls_found:
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                # Parse URL for domain
                domain = None
                scheme = None
                try:
                    parsed = urlparse(url)
                    scheme = parsed.scheme or None
                    domain = parsed.netloc or None
                except Exception:
                    pass

                tab_index += 1

                # Build history record (represents a recovered tab)
                tab_records.append({
                    "url": url,
                    "title": "",  # Recovery files don't reliably store titles
                    "visit_time_utc": None,  # No reliable timestamp in recovery files
                    "visit_count": 1,
                    "browser": browser,
                    "profile": user,
                    "source_path": source_path,
                    "discovered_by": discovered_by,
                    "run_id": run_id,
                    "partition_index": partition_index,
                    "notes": f"Recovered from {recovery_type} session, tab #{tab_index}",
                })

                # Build URL record
                url_records.append({
                    "url": url,
                    "domain": domain,
                    "scheme": scheme,
                    "source_path": source_path,
                    "discovered_by": discovered_by,
                    "run_id": run_id,
                    "first_seen_utc": None,
                    "last_seen_utc": None,
                    "notes": f"IE tab recovery ({recovery_type}), User: {user}",
                })

            # Strategy 2: Try to parse structured data for additional metadata
            # IE recovery files may have FILETIME timestamps embedded
            self._parse_structured_timestamps(data, tab_records, url_records)

        except Exception as e:
            LOGGER.warning("Error parsing recovery file %s: %s", dat_path, e)

        return tab_records, url_records

    def _extract_urls_from_binary(self, data: bytes) -> List[str]:
        """
        Extract URLs from binary data.

        IE/Edge recovery files use UTF-16 LE encoding (Windows standard).
        We scan for http:// and https:// prefixes in UTF-16 LE format
        and extract complete URLs character by character.

        Also falls back to ASCII regex for any non-UTF16 content.
        """
        urls = []
        seen = set()

        # UTF-16 LE prefixes for http:// and https://
        HTTP_UTF16 = b'h\x00t\x00t\x00p\x00:\x00/\x00/\x00'
        HTTPS_UTF16 = b'h\x00t\x00t\x00p\x00s\x00:\x00/\x00/\x00'

        # Characters that terminate a URL (in low byte of UTF-16 LE)
        URL_TERMINATORS = {
            ord('<'), ord('>'), ord('"'), ord("'"), ord('\\'),
            ord(' '), ord('\t'), ord('\r'), ord('\n'),
        }

        # Strategy 1: Extract UTF-16 LE encoded URLs (primary for IE/Edge)
        for prefix in [HTTPS_UTF16, HTTP_UTF16]:
            idx = 0
            while True:
                pos = data.find(prefix, idx)
                if pos == -1:
                    break

                # Extract UTF-16 LE string character by character
                end_pos = pos
                while end_pos < len(data) - 1:
                    lo = data[end_pos]      # Low byte
                    hi = data[end_pos + 1]  # High byte

                    # ASCII chars in UTF-16 LE have high byte = 0x00
                    if hi != 0:
                        break

                    # Stop at non-printable or URL-unsafe characters
                    if lo < 0x21 or lo > 0x7e:  # Outside printable ASCII
                        break
                    if lo in URL_TERMINATORS:
                        break

                    end_pos += 2

                # Must have at least the prefix + some domain
                if end_pos > pos + len(prefix) + 4:
                    try:
                        url_bytes = data[pos:end_pos]
                        url_str = url_bytes.decode('utf-16-le', errors='ignore')

                        # Validate URL structure
                        if 10 < len(url_str) < 4096:
                            parsed = urlparse(url_str)
                            if parsed.scheme in ('http', 'https') and parsed.netloc:
                                # Additional validation: domain must have a dot
                                # and be at least X.YY format (e.g. a.co)
                                netloc = parsed.netloc.split(':')[0]  # Remove port
                                if '.' in netloc:
                                    # TLD must be at least 2 chars, all letters, lowercase
                                    tld = netloc.rsplit('.', 1)[-1]
                                    if len(tld) >= 2 and tld.isalpha() and tld.islower():
                                        if url_str not in seen:
                                            seen.add(url_str)
                                            urls.append(url_str)
                    except Exception:
                        pass

                idx = pos + 2

        # Strategy 2: Also try ASCII regex (fallback for any ASCII content)
        for match in URL_PATTERN.finditer(data):
            try:
                url_bytes = match.group(0)
                url_str = url_bytes.decode('utf-8', errors='ignore')

                # Clean up trailing garbage
                url_str = url_str.split('\x00')[0]
                url_str = url_str.split('\r')[0]
                url_str = url_str.split('\n')[0]

                if 10 < len(url_str) < 4096:
                    parsed = urlparse(url_str)
                    if parsed.scheme in ('http', 'https') and parsed.netloc:
                        # Additional validation: domain must have valid TLD
                        netloc = parsed.netloc.split(':')[0]
                        if '.' in netloc:
                            tld = netloc.rsplit('.', 1)[-1]
                            if len(tld) >= 2 and tld.isalpha() and tld.islower():
                                if url_str not in seen:
                                    seen.add(url_str)
                                    urls.append(url_str)
            except Exception:
                continue

        return urls

    def _parse_structured_timestamps(
        self,
        data: bytes,
        tab_records: List[Dict],
        url_records: List[Dict]
    ) -> None:
        """
        Try to extract FILETIME timestamps from recovery file structure.

        IE recovery files often contain FILETIME values near URLs.
        This is a best-effort attempt to correlate timestamps with URLs.
        """
        # Look for FILETIME patterns (8 bytes that decode to reasonable dates)
        # FILETIME for ~2000-2040 range: approximately 0x01BF... to 0x01F1...

        # This is heuristic - we look for 8-byte sequences that decode
        # to reasonable timestamps and are near URL data
        filetime_candidates = []

        i = 0
        while i < len(data) - 8:
            # Check for potential FILETIME (little-endian 64-bit)
            try:
                potential_ft = struct.unpack('<Q', data[i:i+8])[0]

                # Check if it's in reasonable range (2000-2040)
                # FILETIME for 2000-01-01: 125911584000000000
                # FILETIME for 2040-01-01: 138534624000000000
                if 125000000000000000 < potential_ft < 139000000000000000:
                    dt = filetime_to_datetime(potential_ft)
                    if dt:
                        filetime_candidates.append((i, dt))
            except Exception:
                pass
            i += 1

        # If we found timestamps, try to associate with tab records
        # (This is approximate - recovery file format varies by IE version)
        if filetime_candidates and tab_records:
            # Log all candidates for debugging timestamp issues
            if len(filetime_candidates) > 1:
                LOGGER.debug(
                    "Found %d FILETIME candidates in recovery file: %s",
                    len(filetime_candidates),
                    [(offset, dt.isoformat()) for offset, dt in filetime_candidates[:10]]  # Log first 10
                )

            # Select timestamp using "most common year" heuristic
            # Random binary data may decode to valid-looking FILETIMEs in future years.
            # Real session timestamps typically cluster in the same year.
            # We find the most common year, then pick the most recent timestamp from that year.
            from collections import Counter
            year_counts = Counter(dt.year for _, dt in filetime_candidates)
            most_common_year = year_counts.most_common(1)[0][0]

            # Filter to only timestamps from the most common year
            same_year_candidates = [
                (offset, dt) for offset, dt in filetime_candidates
                if dt.year == most_common_year
            ]

            # Pick the most recent from that year
            latest_dt = max(dt for _, dt in same_year_candidates)
            latest_iso = latest_dt.isoformat()

            LOGGER.debug(
                "Selected session timestamp: %s (year %d had %d/%d candidates)",
                latest_iso, most_common_year, len(same_year_candidates), len(filetime_candidates)
            )

            # Warn if we filtered out future-year false positives
            if len(same_year_candidates) < len(filetime_candidates):
                other_years = sorted(set(dt.year for _, dt in filetime_candidates) - {most_common_year})
                LOGGER.debug(
                    "Filtered out %d timestamps from other years: %s",
                    len(filetime_candidates) - len(same_year_candidates),
                    other_years
                )

            # Apply to records that don't have timestamps
            for record in tab_records:
                if record.get("visit_time_utc") is None:
                    record["visit_time_utc"] = latest_iso
                    record["notes"] = record.get("notes", "") + f", Session time: {latest_iso[:19]}"

            for record in url_records:
                if record.get("first_seen_utc") is None:
                    record["first_seen_utc"] = latest_iso
                    record["last_seen_utc"] = latest_iso
