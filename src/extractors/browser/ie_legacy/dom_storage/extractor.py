"""
IE/Legacy Edge DOM Storage Extractor.

Extracts DOM Storage (localStorage/sessionStorage) data from:
1. WebCache database DOMStore containers (IE)
2. File-based DOMStore in Legacy Edge UWP paths

DOM Storage Format:
- IE stores DOM Storage in WebCache ESE database containers named "DOMStore*"
- Legacy Edge stores in XML files at User/Default/DOMStore/

Forensic Value:
- Contains persistent website data that survives cache clears
- May contain authentication tokens, user preferences, PII
- Often overlooked in investigations

Dependencies:
- libesedb-python or dissect.esedb (optional, for WebCache parsing)
"""

from __future__ import annotations

import hashlib
import json
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse

from PySide6.QtWidgets import QWidget, QLabel, QVBoxLayout

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from .._patterns import (
    get_all_patterns,
    get_patterns,
    extract_user_from_path,
    detect_browser_from_path,
)
from .._ese_reader import (
    ESE_AVAILABLE,
    WebCacheReader,
    check_ese_available,
)
from core.logging import get_logger
from core.database import (
    insert_local_storage,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.dom_storage")


class IEDOMStorageExtractor(BaseExtractor):
    """
    Extract DOM Storage data from IE WebCache and Legacy Edge files.

    This extractor handles two sources:
    1. WebCache DOMStore containers (IE) - via IEWebCacheExtractor output
    2. Edge Legacy DOMStore files (XML format)

    Output goes to local_storage table (shared with Chromium/Firefox).
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="ie_dom_storage",
            display_name="IE/Edge DOM Storage",
            description="Extract localStorage/sessionStorage from WebCache and Edge files",
            category="browser",
            requires_tools=[],  # ESE is optional
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
        """Return configuration widget."""
        return MultiPartitionWidget(parent, default_scan_all=True)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction/ingestion state."""
        widget = QWidget(parent)
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)

        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"IE/Edge DOM Storage\nFiles: {file_count}"
        else:
            status_text = "IE/Edge DOM Storage\nNo extraction yet"

        ese_ok, _ = check_ese_available()
        if not ese_ok:
            status_text += "\n⚠️ ESE library not installed (IE WebCache unavailable)"

        layout.addWidget(QLabel(status_text, widget))
        return widget

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "ie_dom_storage"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract DOM Storage files from Edge Legacy paths.

        Note: IE DOM Storage is in WebCache, processed via IEWebCacheExtractor.
        This extractor discovers Edge Legacy DOMStore files.
        """
        callbacks.on_step("Initializing DOM Storage extraction")

        run_id = self._generate_run_id()
        LOGGER.info("Starting DOM Storage extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")
        scan_all_partitions = config.get("scan_all_partitions", True)

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
            "extraction_tool": self._get_tool_version(),
            "files": [],
            "partitions_scanned": [],
            "partitions_with_artifacts": [],
            "status": "ok",
            "notes": [],
        }

        # Get Edge Legacy patterns
        patterns = get_patterns("edge_legacy", "dom_storage")
        if not patterns:
            manifest_data["notes"].append("No Edge Legacy DOMStore patterns")
            callbacks.on_log("No Edge Legacy DOMStore patterns defined", "warning")

        callbacks.on_step(f"Searching {len(patterns)} DOM Storage patterns")

        # Discover files
        discovered_files = []

        available, count = check_file_list_available(evidence_conn, evidence_id) if evidence_conn else (False, 0)
        if available:
            callbacks.on_step(f"Using file_list index for discovery ({count:,} files indexed)")
            partition_filter = None if scan_all_partitions else {0}
            result = discover_from_file_list(
                evidence_conn, evidence_id,
                path_patterns=patterns,
                partition_filter=partition_filter,
            )
            # Convert FileListMatch objects to expected dict format
            discovered_files = [
                {
                    "logical_path": m.file_path,
                    "filename": m.file_name,
                    "partition_index": m.partition_index,
                }
                for m in result.get_all_matches()
            ]
        else:
            callbacks.on_step("Walking filesystem for DOMStore files")
            discovered_files = self._walk_for_files(evidence_fs, patterns, callbacks)

        if not discovered_files:
            manifest_data["notes"].append("No Edge Legacy DOMStore files found")
            callbacks.on_log("No Edge Legacy DOMStore files found", "info")
            # This is OK - IE uses WebCache instead
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="no_artifacts")
            (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))
            return True

        callbacks.on_log(f"Found {len(discovered_files)} DOMStore files", "info")
        callbacks.on_progress(0, len(discovered_files), "Extracting DOMStore files")

        partitions_with_artifacts = set()

        # Get EWF paths for multi-partition extraction
        ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
        current_partition = getattr(evidence_fs, 'partition_index', 0)

        # Group files by partition to avoid reopening partitions repeatedly
        files_by_partition: Dict[int, List[Dict]] = {}
        for file_info in discovered_files:
            partition_index = file_info.get("partition_index", 0)
            files_by_partition.setdefault(partition_index, []).append(file_info)

        total_files = len(discovered_files)
        file_index = 0

        for partition_index in sorted(files_by_partition.keys()):
            if ewf_paths is None and partition_index != current_partition:
                msg = (
                    f"EWF paths unavailable; skipping partition {partition_index} "
                    "for DOM Storage extraction"
                )
                callbacks.on_log(msg, "warning")
                manifest_data["notes"].append(msg)
                continue

            fs_ctx = (
                open_partition_for_extraction(evidence_fs, None)
                if (partition_index == current_partition or ewf_paths is None)
                else open_partition_for_extraction(ewf_paths, partition_index)
            )

            try:
                with fs_ctx as fs_to_use:
                    if fs_to_use is None:
                        msg = f"Failed to open partition {partition_index}; skipping"
                        callbacks.on_log(msg, "warning")
                        manifest_data["notes"].append(msg)
                        continue

                    for file_info in files_by_partition[partition_index]:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            break

                        file_index += 1
                        callbacks.on_progress(
                            file_index, total_files,
                            f"Extracting {file_info.get('filename', 'file')}"
                        )

                        try:
                            logical_path = file_info.get("logical_path", "")
                            user = extract_user_from_path(logical_path)

                            safe_name = Path(logical_path).name
                            out_path = output_dir / f"p{partition_index}" / user / safe_name
                            out_path.parent.mkdir(parents=True, exist_ok=True)

                            content = fs_to_use.read_file(logical_path)
                            out_path.write_bytes(content)

                            md5 = hashlib.md5(content).hexdigest()
                            sha256 = hashlib.sha256(content).hexdigest()

                            manifest_data["files"].append({
                                "logical_path": logical_path,
                                "extracted_path": str(out_path.relative_to(output_dir)),
                                "user": user,
                                "browser": "edge_legacy",
                                "partition_index": partition_index,
                                "size_bytes": len(content),
                                "md5": md5,
                                "sha256": sha256,
                                "artifact_type": "dom_storage",
                            })
                            partitions_with_artifacts.add(partition_index)

                        except Exception as e:
                            error_msg = f"Failed to extract {file_info.get('logical_path', 'unknown')}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            callbacks.on_error(error_msg, "")
                            manifest_data["notes"].append(error_msg)
            except Exception as e:
                error_msg = f"Failed to open partition {partition_index}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_log(error_msg, "warning")
                manifest_data["notes"].append(error_msg)

        manifest_data["partitions_with_artifacts"] = sorted(partitions_with_artifacts)

        if collector:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            collector.finish_run(evidence_id, self.metadata.name, status=status)

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
            "DOM Storage extraction complete: %d files, status=%s",
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
        Parse DOM Storage from Edge Legacy files AND WebCache database.
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"local_storage": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())
        evidence_label = config.get("evidence_label", "")

        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        total_entries = 0
        failed_files = 0

        # Process Edge Legacy files
        files = manifest_data.get("files", [])
        if files:
            callbacks.on_progress(0, len(files), "Parsing Edge Legacy DOMStore files")

            for i, file_entry in enumerate(files):
                if callbacks.is_cancelled():
                    break

                callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry.get('user', 'unknown')} storage")

                try:
                    extracted_path = Path(file_entry["extracted_path"])
                    if not extracted_path.is_absolute():
                        extracted_path = output_dir / extracted_path

                    if not extracted_path.exists():
                        callbacks.on_log(f"File not found: {extracted_path}", "warning")
                        failed_files += 1
                        continue

                    inventory_id = insert_browser_inventory(
                        evidence_conn,
                        evidence_id=evidence_id,
                        browser=file_entry.get("browser", "edge_legacy"),
                        artifact_type="dom_storage",
                        run_id=run_id,
                        extracted_path=str(extracted_path),
                        extraction_status="ok",
                        extraction_timestamp_utc=manifest_data.get("extraction_timestamp_utc"),
                        logical_path=file_entry.get("logical_path", ""),
                        profile=file_entry.get("user"),
                        partition_index=file_entry.get("partition_index"),
                        extraction_tool=manifest_data.get("extraction_tool"),
                        file_size_bytes=file_entry.get("size_bytes"),
                        file_md5=file_entry.get("md5"),
                        file_sha256=file_entry.get("sha256"),
                    )

                    entries = self._parse_edge_domstore_file(
                        extracted_path,
                        file_entry,
                        run_id,
                        evidence_id,
                        evidence_conn,
                        callbacks,
                    )

                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="ok",
                        records_parsed=entries,
                    )

                    total_entries += entries

                except Exception as e:
                    error_msg = f"Failed to parse {file_entry.get('extracted_path')}: {e}"
                    LOGGER.error(error_msg, exc_info=True)
                    callbacks.on_error(error_msg, "")
                    failed_files += 1

        # Also try to parse WebCache DOMStore containers
        webcache_entries = self._parse_webcache_domstore(
            output_dir.parent / "ie_webcache",
            run_id,
            evidence_id,
            evidence_conn,
            callbacks,
        )
        total_entries += webcache_entries

        evidence_conn.commit()

        if collector:
            collector.report_ingested(evidence_id, self.metadata.name, records=total_entries)
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(f"Ingested {total_entries} DOM Storage entries", "info")

        return {"local_storage": total_entries}

    # =========================================================================
    # Private Helper Methods
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_statistics_collector(self):
        """Get StatisticsCollector instance."""
        try:
            from core.statistics_collector import StatisticsCollector
            return StatisticsCollector.get_instance()
        except Exception:
            return None

    def _get_tool_version(self) -> str:
        """Build extraction tool version string."""
        ese_ok, ese_lib = check_ese_available()
        if ese_ok:
            return f"{self.metadata.name}:{self.metadata.version}+{ese_lib}"
        return f"{self.metadata.name}:{self.metadata.version}"

    def _walk_for_files(
        self,
        evidence_fs,
        patterns: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict[str, Any]]:
        """Walk filesystem to find matching files."""
        import fnmatch

        results = []

        try:
            for path in evidence_fs.iter_all_files():
                if callbacks.is_cancelled():
                    break

                normalized = path.replace("\\", "/")
                for pattern in patterns:
                    if fnmatch.fnmatch(normalized.lower(), pattern.lower()):
                        results.append({
                            "logical_path": path,
                            "filename": Path(path).name,
                            "partition_index": 0,
                        })
                        break
        except Exception as e:
            LOGGER.error("Error walking filesystem: %s", e)

        return results

    def _parse_edge_domstore_file(
        self,
        file_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
    ) -> int:
        """
        Parse Edge Legacy DOMStore XML file.

        Edge Legacy stores DOM data in XML format.
        """
        user = file_entry.get("user", "unknown")
        source_path = file_entry.get("logical_path", "")
        partition_index = file_entry.get("partition_index", 0)
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        try:
            content = file_path.read_bytes()

            # Try to decode
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError:
                text = content.decode("utf-16-le", errors="replace")

            # Attempt XML parse
            entries = 0

            try:
                root = ET.fromstring(text)

                # Look for storage items
                for item in root.iter():
                    key = item.get("key") or item.get("name")
                    value = item.text or item.get("value", "")
                    origin = item.get("origin") or item.get("url", "")

                    if key:
                        try:
                            insert_local_storage(
                                evidence_conn,
                                evidence_id=evidence_id,
                                browser="edge_legacy",
                                profile=user,
                                origin=origin,
                                key=key,
                                value=value,
                                source_path=source_path,
                                discovered_by=discovered_by,
                                run_id=run_id,
                                partition_index=partition_index,
                            )
                            entries += 1
                        except Exception as e:
                            LOGGER.debug("Failed to insert storage entry: %s", e)

            except ET.ParseError:
                # Not XML - try key-value parsing
                entries = self._parse_keyvalue_format(
                    text, user, source_path, discovered_by,
                    run_id, evidence_id, evidence_conn, partition_index
                )

            return entries

        except Exception as e:
            LOGGER.error("Failed to parse DOMStore file %s: %s", file_path, e)
            return 0

    def _parse_keyvalue_format(
        self,
        text: str,
        user: str,
        source_path: str,
        discovered_by: str,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        partition_index: int,
    ) -> int:
        """Parse simple key=value format if not XML."""
        entries = 0

        for line in text.split("\n"):
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip()

                if key:
                    try:
                        insert_local_storage(
                            evidence_conn,
                            evidence_id=evidence_id,
                            browser="edge_legacy",
                            profile=user,
                            origin="",
                            key=key,
                            value=value,
                            source_path=source_path,
                            discovered_by=discovered_by,
                            run_id=run_id,
                            partition_index=partition_index,
                        )
                        entries += 1
                    except Exception:
                        pass

        return entries

    def _parse_webcache_domstore(
        self,
        webcache_dir: Path,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
    ) -> int:
        """
        Parse DOMStore containers from WebCache database.

        Looks for manifest.json in webcache_dir and parses WebCache files
        for DOMStore containers.
        """
        ese_ok, _ = check_ese_available()
        if not ese_ok:
            callbacks.on_log("ESE library not available - skipping WebCache DOMStore", "info")
            return 0

        manifest_path = webcache_dir / "manifest.json"
        if not manifest_path.exists():
            callbacks.on_log("No WebCache manifest - skipping WebCache DOMStore", "info")
            return 0

        try:
            manifest_data = json.loads(manifest_path.read_text())
        except Exception as e:
            callbacks.on_log(f"Failed to read WebCache manifest: {e}", "warning")
            return 0

        webcache_files = [
            f for f in manifest_data.get("files", [])
            if f.get("file_type") == "database" or f.get("artifact_type") == "webcache"
        ]

        if not webcache_files:
            return 0

        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"
        total_entries = 0

        for file_entry in webcache_files:
            db_path = Path(file_entry["extracted_path"])
            if not db_path.is_absolute():
                db_path = webcache_dir / db_path

            if not db_path.exists():
                continue

            try:
                with WebCacheReader(db_path) as reader:
                    containers = reader.get_containers()

                    # Find DOMStore containers
                    domstore_containers = [
                        c for c in containers
                        if c.get("name") and "domstore" in c.get("name", "").lower()
                    ]

                    if not domstore_containers:
                        continue

                    user = file_entry.get("user", "unknown")
                    source_path = file_entry.get("logical_path", "")
                    partition_index = file_entry.get("partition_index", 0)

                    for container in domstore_containers:
                        container_id = container.get("container_id")
                        table_name = f"Container_{container_id}"

                        if table_name not in reader.tables():
                            continue

                        for record in reader.read_table(table_name):
                            url = record.get("Url") or record.get("Key", "")
                            if not url:
                                continue

                            # Extract origin from URL
                            try:
                                parsed = urlparse(url)
                                origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme else url
                            except Exception:
                                origin = url

                            # Extract key and value
                            key = record.get("Key") or url
                            value = ""

                            # Value might be in ResponseHeaders or Data
                            if "Data" in record:
                                data = record.get("Data")
                                if isinstance(data, bytes):
                                    try:
                                        value = data.decode("utf-8", errors="replace")
                                    except Exception:
                                        value = data.hex()[:200]
                                else:
                                    value = str(data)[:2000]

                            try:
                                insert_local_storage(
                                    evidence_conn,
                                    evidence_id=evidence_id,
                                    browser="ie",
                                    profile=user,
                                    origin=origin,
                                    key=key,
                                    value=value,
                                    source_path=source_path,
                                    discovered_by=discovered_by,
                                    run_id=run_id,
                                    partition_index=partition_index,
                                )
                                total_entries += 1
                            except Exception as e:
                                LOGGER.debug("Failed to insert DOMStore entry: %s", e)

            except Exception as e:
                LOGGER.error("Failed to parse WebCache DOMStore %s: %s", db_path, e)

        if total_entries > 0:
            callbacks.on_log(f"Parsed {total_entries} entries from WebCache DOMStore", "info")

        return total_entries
