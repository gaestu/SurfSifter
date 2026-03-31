"""
Internet Explorer Classic History.IE5 Extractor.

Extracts and performs basic parsing of classic IE History.IE5 database files
(container.dat and index.dat). These are pre-IE10 history storage files that
may still be present (live or deleted) on modern Windows systems.

History.IE5 File Locations:
- Users/*/AppData/Local/Microsoft/Windows/History/History.IE5/container.dat
- Users/*/AppData/Local/Microsoft/Windows/History/History.IE5/index.dat
- Users/*/AppData/Local/Microsoft/Windows/History/History.IE5/*/container.dat
- Users/*/AppData/Local/Microsoft/Windows/History/History.IE5/*/index.dat
- Users/*/AppData/Local/Microsoft/Windows/History/Low/History.IE5/...
- Users/*/AppData/Local/Microsoft/Windows/Temporary Internet Files/Content.IE5/...

Forensic Value:
- Pre-IE10 browsing history not stored in WebCacheV01.dat
- Deleted History.IE5 files may contain URLs not found elsewhere
- Low-integrity zone artifacts (Protected Mode browsing)
- Subdirectories represent time-bucketed history containers

Dependencies:
- None (pure Python — binary regex URL extraction)
"""

from __future__ import annotations

import hashlib
import json
import re
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
    get_all_patterns,
    extract_user_from_path,
    detect_browser_from_path,
)
from core.logging import get_logger
from core.database import (
    insert_urls,
    insert_browser_history_rows,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.classic_history")

# Regex for extracting URLs from binary data (ASCII)
URL_PATTERN = re.compile(rb'https?://[^\x00\x01-\x1f\x7f-\x9f]+', re.IGNORECASE)


class IEClassicHistoryExtractor(BaseExtractor):
    """
    Extract and parse classic IE History.IE5 database files.

    History.IE5 directories contain container.dat and index.dat files that
    store browsing history in a proprietary binary format used by IE versions
    prior to IE10. These files may persist on modern Windows systems and are
    especially valuable when found as deleted artifacts.

    This extractor handles both extraction AND ingestion:
    - Extraction: discovers and copies History.IE5 files to workspace
    - Ingestion: performs regex-based URL extraction from binary content
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="ie_classic_history",
            display_name="IE Classic History (History.IE5)",
            description="Extract classic IE History.IE5 container.dat/index.dat files",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        if evidence_fs is None:
            return False, "No evidence filesystem mounted"
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        return MultiPartitionWidget(parent, default_scan_all=True)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
    ) -> QWidget:
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"IE Classic History (History.IE5)\nFiles: {file_count}"
        else:
            status_text = "IE Classic History (History.IE5)\nNo extraction yet"
        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        return case_root / "evidences" / evidence_label / "ie_classic_history"

    # =========================================================================
    # Extraction
    # =========================================================================

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        """Extract History.IE5 container.dat/index.dat files from evidence."""
        callbacks.on_step("Initializing IE Classic History extraction")

        run_id = self._generate_run_id()
        LOGGER.info("Starting IE Classic History extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        collector = self._get_statistics_collector()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

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

        callbacks.on_step("Scanning for History.IE5 files")

        if evidence_conn is None:
            error_msg = (
                "file_list discovery requires evidence_conn; cannot run Classic History "
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
                "file_list is empty/unavailable for this evidence; cannot run Classic History "
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

        # include_deleted defaults to True for classic history (high-value deleted artifacts)
        include_deleted = config.get("include_deleted", True)
        files_by_partition = self._discover_files_multi_partition(
            evidence_conn, evidence_id, callbacks,
            include_deleted=include_deleted,
        )

        all_files = []
        for files_list in files_by_partition.values():
            all_files.extend(files_list)

        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = sorted(
            p for p, files in files_by_partition.items() if files
        )

        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=len(all_files))

        callbacks.on_log(f"Found {len(all_files)} History.IE5 file(s)")

        if not all_files:
            LOGGER.info("No History.IE5 files found")
            manifest_data["notes"].append("No History.IE5 files found")
        else:
            callbacks.on_progress(0, len(all_files), "Extracting History.IE5 files")

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
                            callbacks.on_log(
                                f"Failed to open partition {partition_index}", "warning"
                            )
                            continue

                        for file_info in partition_files:
                            if callbacks.is_cancelled():
                                manifest_data["status"] = "cancelled"
                                break

                            file_index += 1
                            callbacks.on_progress(
                                file_index, len(all_files),
                                f"Copying {Path(file_info.get('logical_path', '')).name}",
                            )

                            try:
                                result = self._extract_file(
                                    fs_to_use, file_info, output_dir, callbacks,
                                    partition_index=partition_index,
                                )
                                result["partition_index"] = partition_index
                                manifest_data["files"].append(result)
                            except Exception as e:
                                error_msg = (
                                    f"Failed to extract {file_info.get('logical_path')}: {e}"
                                )
                                LOGGER.error(error_msg, exc_info=True)
                                manifest_data["notes"].append(error_msg)

                except Exception as e:
                    error_msg = f"Failed to access partition {partition_index}: {e}"
                    callbacks.on_log(error_msg, "warning")
                    manifest_data["notes"].append(error_msg)

                if manifest_data["status"] == "cancelled":
                    break

        if collector:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_step("Writing manifest")
        (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))

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
            "IE Classic History extraction complete: %d files, status=%s",
            len(manifest_data["files"]),
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
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, int]:
        """
        Parse extracted History.IE5 files and ingest URLs.

        Performs binary regex URL extraction from container.dat/index.dat files.
        Full IE5 header/record parsing is not yet implemented.
        """
        callbacks.on_step("Reading classic history manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"urls": 0, "history_records": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())
        evidence_label = config.get("evidence_label", "")
        files = manifest_data.get("files", [])

        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"urls": 0, "history_records": 0}

        total_urls = 0
        total_history = 0
        failed_files = 0
        all_url_records: List[Dict] = []
        all_history_records: List[Dict] = []

        callbacks.on_progress(0, len(files), "Parsing History.IE5 files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(files),
                f"Parsing {Path(file_entry.get('extracted_path', '')).name}",
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

                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "ie"),
                    artifact_type="classic_history",
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

                history_records, url_records = self._parse_history_file(
                    dat_path, file_entry, run_id, callbacks
                )

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=len(history_records),
                    urls_parsed=len(url_records),
                )

                all_history_records.extend(history_records)
                all_url_records.extend(url_records)
                total_history += len(history_records)
                total_urls += len(url_records)

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

        if all_history_records:
            insert_browser_history_rows(evidence_conn, evidence_id, all_history_records)

        if all_url_records:
            insert_urls(evidence_conn, evidence_id, all_url_records)

        evidence_conn.commit()

        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=total_history,
                urls=total_urls,
            )
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(
            f"Ingested {total_history} history records, {total_urls} unique URLs "
            f"from History.IE5 files (regex extraction)",
            "info",
        )

        LOGGER.warning(
            "Full History.IE5 header/record parsing is not yet implemented. "
            "URLs were extracted via binary regex scan only."
        )

        return {"urls": total_urls, "history_records": total_history}

    # =========================================================================
    # Private Helpers
    # =========================================================================

    def _generate_run_id(self) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_statistics_collector(self):
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
        include_deleted: bool = True,
    ) -> Dict[int, List[Dict]]:
        """Discover History.IE5 files across all partitions using file_list."""
        # Use patterns from _patterns.py for discovery
        all_patterns = get_all_patterns("history")

        # Convert glob patterns to SQL LIKE patterns for file_list discovery
        path_patterns = []
        for pattern in all_patterns:
            # Convert glob * to SQL %, remove leading path separator
            sql_pattern = pattern.replace("*", "%")
            path_patterns.append(f"%{sql_pattern}")

        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["container.dat", "index.dat"],
            path_patterns=["%History.IE5%", "%Content.IE5%"],
            exclude_deleted=not include_deleted,
        )

        if result.is_empty:
            callbacks.on_log("No History.IE5 files found in file_list", "info")
            return {}

        files_by_partition: Dict[int, List[Dict]] = {}

        for partition_index, matches in result.matches_by_partition.items():
            files_list = []
            for match in matches:
                name_lower = match.file_name.lower()
                if name_lower not in ("container.dat", "index.dat"):
                    continue
                path_lower = match.file_path.lower()
                if "history.ie5" not in path_lower and "content.ie5" not in path_lower:
                    continue

                user = extract_user_from_path(match.file_path)
                browser = detect_browser_from_path(match.file_path)

                files_list.append({
                    "logical_path": match.file_path,
                    "user": user,
                    "browser": browser,
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
        """Copy a History.IE5 file from evidence to workspace."""
        source_path = file_info["logical_path"]
        user = file_info.get("user", "unknown")
        original_name = Path(source_path).name

        safe_user = user.replace(" ", "_").replace("/", "_").replace("\\", "_")
        path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]
        filename = f"p{partition_index}_{safe_user}_{path_hash}_{original_name}"
        dest_path = output_dir / filename

        file_content = evidence_fs.read_file(source_path)
        dest_path.write_bytes(file_content)

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
        }

    def _parse_history_file(
        self,
        dat_path: Path,
        file_entry: Dict,
        run_id: str,
        callbacks: ExtractorCallbacks,
    ) -> tuple[List[Dict], List[Dict]]:
        """
        Parse a History.IE5 file to extract URLs via binary regex scan.

        Full IE5 header/record parsing is deferred. Currently extracts URLs
        found as ASCII or UTF-16LE strings in the binary content.

        Returns:
            Tuple of (history_records, url_records)
        """
        user = file_entry.get("user", "unknown")
        browser = file_entry.get("browser", "ie")
        source_path = file_entry.get("logical_path", "")
        partition_index = file_entry.get("partition_index", 0)
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        history_records: List[Dict] = []
        url_records: List[Dict] = []

        try:
            data = dat_path.read_bytes()
            urls_found = self._extract_urls_from_binary(data)

            callbacks.on_log(
                f"Found {len(urls_found)} URLs in {dat_path.name}",
                "info",
            )

            for url in urls_found:
                domain = None
                scheme = None
                try:
                    parsed = urlparse(url)
                    scheme = parsed.scheme or None
                    domain = parsed.netloc or None
                except Exception:
                    pass

                history_records.append({
                    "url": url,
                    "title": "",
                    "visit_time_utc": None,
                    "visit_count": 1,
                    "browser": browser,
                    "profile": user,
                    "source_path": source_path,
                    "discovered_by": discovered_by,
                    "run_id": run_id,
                    "partition_index": partition_index,
                    "notes": "Recovered from History.IE5 (regex extraction)",
                })

                url_records.append({
                    "url": url,
                    "domain": domain,
                    "scheme": scheme,
                    "source_path": source_path,
                    "discovered_by": discovered_by,
                    "run_id": run_id,
                    "first_seen_utc": None,
                    "last_seen_utc": None,
                    "notes": f"IE classic history (History.IE5), User: {user}",
                })

        except Exception as e:
            LOGGER.warning("Error parsing History.IE5 file %s: %s", dat_path, e)

        return history_records, url_records

    def _extract_urls_from_binary(self, data: bytes) -> List[str]:
        """
        Extract URLs from binary data via ASCII regex.

        History.IE5 files store URLs primarily as ASCII strings.
        """
        urls: List[str] = []
        seen: set[str] = set()

        for match in URL_PATTERN.finditer(data):
            try:
                url_bytes = match.group(0)
                url_str = url_bytes.decode('utf-8', errors='ignore')

                # Clean trailing garbage
                url_str = url_str.split('\x00')[0]
                url_str = url_str.split('\r')[0]
                url_str = url_str.split('\n')[0]

                if 10 < len(url_str) < 4096:
                    parsed = urlparse(url_str)
                    if parsed.scheme in ('http', 'https') and parsed.netloc:
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
