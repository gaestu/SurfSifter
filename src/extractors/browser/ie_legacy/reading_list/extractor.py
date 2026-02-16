"""
Edge Legacy Reading List Extractor.

Extracts reading list entries from Legacy Edge (EdgeHTML/UWP) browser.

Legacy Edge stored a "Reading List" feature for saving articles to read later.
This data is stored in the UWP package at:
    Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/User/Default/ReadingList/

File Format:
- Individual files per saved article
- May be JSON, XML, or proprietary format

Forensic Value:
- Shows articles user intended to read
- Contains original URLs and metadata
- May indicate interests and research topics
- Often forgotten during anti-forensic measures

Dependencies:
- None (pure Python parsing)
"""

from __future__ import annotations

import hashlib
import json
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

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
    get_patterns,
    extract_user_from_path,
)
from core.logging import get_logger
from core.database import (
    insert_bookmark_row,
    insert_browser_inventory,
    insert_urls,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.reading_list")


class EdgeReadingListExtractor(BaseExtractor):
    """
    Extract and parse Edge Legacy Reading List.

    Reading List entries are treated similarly to bookmarks since they
    represent URLs the user wanted to save for later reading.
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="edge_reading_list",
            display_name="Edge Legacy Reading List",
            description="Extract Reading List entries from Legacy Edge",
            category="browser",
            requires_tools=[],
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
        """Return status widget."""
        widget = QWidget(parent)
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)

        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"Edge Reading List\nFiles: {file_count}"
        else:
            status_text = "Edge Reading List\nNo extraction yet"

        layout.addWidget(QLabel(status_text, widget))
        return widget

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "edge_reading_list"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Reading List files from Edge Legacy UWP paths.
        """
        callbacks.on_step("Initializing Reading List extraction")

        run_id = self._generate_run_id()
        LOGGER.info("Starting Reading List extraction (run_id=%s)", run_id)

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
            "extraction_tool": self._get_tool_version(),
            "files": [],
            "partitions_scanned": [],
            "partitions_with_artifacts": [],
            "status": "ok",
            "notes": [],
        }

        # Get patterns (only Edge Legacy has reading list)
        patterns = get_patterns("edge_legacy", "reading_list")
        # Also check feeds patterns as they're related
        patterns.extend(get_patterns("edge_legacy", "feeds"))

        if not patterns:
            manifest_data["notes"].append("No Reading List patterns defined")
            callbacks.on_log("No Reading List patterns defined", "warning")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="no_artifacts")
            (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))
            return True

        LOGGER.debug("Patterns to search: %s", patterns)
        callbacks.on_step(f"Searching {len(patterns)} Reading List patterns")

        # Discover files via file_list only (fail-fast when unavailable).
        if evidence_conn is None:
            error_msg = (
                "file_list discovery requires evidence_conn; cannot run Reading List "
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
        LOGGER.debug("file_list available=%s, count=%d", available, count)
        if not available:
            error_msg = (
                "file_list is empty/unavailable for this evidence; cannot run Reading List "
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

        callbacks.on_step(f"Using file_list index for discovery ({count:,} files indexed)")
        LOGGER.info("Using file_list discovery with %d indexed files", count)
        result = discover_from_file_list(
            evidence_conn, evidence_id,
            path_patterns=patterns,
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
        LOGGER.info("file_list discovery found %d matches", len(discovered_files))

        if not discovered_files:
            manifest_data["notes"].append("No Reading List files found")
            callbacks.on_log("No Reading List files found", "info")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="no_artifacts")
            (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))
            return True

        callbacks.on_log(f"Found {len(discovered_files)} Reading List files", "info")
        callbacks.on_progress(0, len(discovered_files), "Extracting Reading List files")

        partitions_with_artifacts = set()

        # Get EWF paths for multi-partition extraction
        ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
        current_partition = getattr(evidence_fs, 'partition_index', 0)

        # Group files by partition to avoid reopening the same partition repeatedly
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
                    "for Reading List extraction"
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
                                "artifact_type": "reading_list",
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
            "Reading List extraction complete: %d files, status=%s",
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
        Parse Reading List files and ingest as bookmarks.
        Also cross-posts URLs to urls table.
        """
        from urllib.parse import urlparse

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"bookmarks": 0, "urls": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())
        evidence_label = config.get("evidence_label", "")

        files = manifest_data.get("files", [])
        if not files:
            callbacks.on_log("No Reading List files to process", "warning")
            return {"bookmarks": 0, "urls": 0}

        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        total_entries = 0
        failed_files = 0
        all_url_records = []  # Collect URLs for unified urls table
        seen_urls = set()  # Deduplicate URLs

        callbacks.on_progress(0, len(files), "Parsing Reading List files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry.get('user', 'unknown')} entries")

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
                    artifact_type="reading_list",
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

                entries, url_records = self._parse_reading_list_file(
                    extracted_path,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                    callbacks,
                )

                # Collect unique URLs for batch insert
                for url_rec in url_records:
                    url = url_rec.get("url", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_url_records.append(url_rec)

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

        # Cross-post URLs to unified urls table for analysis
        if all_url_records:
            try:
                insert_urls(evidence_conn, evidence_id, all_url_records)
                LOGGER.debug("Cross-posted %d Reading List URLs to urls table", len(all_url_records))
            except Exception as e:
                LOGGER.debug("Failed to cross-post Reading List URLs: %s", e)

        evidence_conn.commit()

        if collector:
            collector.report_ingested(evidence_id, self.metadata.name, records=total_entries)
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(f"Ingested {total_entries} Reading List entries, {len(all_url_records)} URLs", "info")

        return {"bookmarks": total_entries, "urls": len(all_url_records)}

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
        return f"{self.metadata.name}:{self.metadata.version}"

    def _parse_reading_list_file(
        self,
        file_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
    ) -> tuple[int, List[Dict]]:
        """
        Parse a Reading List file.

        Attempts JSON, XML, and plain text parsing.
        Reading List entries are stored as bookmarks.

        Returns:
            Tuple of (bookmarks_inserted, url_records_for_urls_table)
        """
        from urllib.parse import urlparse

        user = file_entry.get("user", "unknown")
        source_path = file_entry.get("logical_path", "")
        partition_index = file_entry.get("partition_index", 0)
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        url_records = []  # Collect URLs for dual-write

        try:
            content = file_path.read_bytes()

            # Try UTF-8, UTF-16, fallback to latin-1
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError:
                try:
                    text = content.decode("utf-16-le")
                except UnicodeDecodeError:
                    text = content.decode("latin-1", errors="replace")

            entries = []

            # Try JSON first
            try:
                data = json.loads(text)
                entries = self._extract_from_json(data)
            except json.JSONDecodeError:
                pass

            # Try XML if JSON failed
            if not entries:
                try:
                    root = ET.fromstring(text)
                    entries = self._extract_from_xml(root)
                except ET.ParseError:
                    pass

            # Try plain text URL extraction if structured parsing failed
            if not entries:
                entries = self._extract_urls_from_text(text)

            # Insert entries as bookmarks
            inserted = 0
            for entry in entries:
                url = entry.get("url", "")
                if not url:
                    continue

                try:
                    insert_bookmark_row(
                        evidence_conn,
                        evidence_id=evidence_id,
                        browser="edge_legacy",
                        profile=user,
                        title=entry.get("title", "Reading List Item"),
                        url=url,
                        date_added=entry.get("date_added"),
                        folder_path="Reading List",
                        source_path=source_path,
                        discovered_by=discovered_by,
                        run_id=run_id,
                        partition_index=partition_index,
                    )
                    inserted += 1

                    # Collect URL for unified urls table (dual-write)
                    if not url.startswith(("javascript:", "data:")):
                        try:
                            parsed = urlparse(url)
                            url_records.append({
                                "url": url,
                                "domain": parsed.netloc or None,
                                "scheme": parsed.scheme or None,
                                "discovered_by": discovered_by,
                                "run_id": run_id,
                                "source_path": source_path,
                                "context": f"reading_list:edge_legacy:{user}",
                                "first_seen_utc": entry.get("date_added"),
                            })
                        except Exception:
                            pass

                except Exception as e:
                    LOGGER.debug("Failed to insert reading list entry: %s", e)

            return inserted, url_records

        except Exception as e:
            LOGGER.error("Failed to parse Reading List file %s: %s", file_path, e)
            return 0, []

    def _extract_from_json(self, data: Any) -> List[Dict]:
        """Extract reading list entries from JSON data."""
        entries = []

        if isinstance(data, dict):
            # Look for common keys
            url = data.get("url") or data.get("Url") or data.get("URI") or data.get("href")
            title = data.get("title") or data.get("Title") or data.get("name") or data.get("Name")
            date = data.get("dateAdded") or data.get("DateAdded") or data.get("timestamp")

            if url:
                entries.append({
                    "url": url,
                    "title": title or "",
                    "date_added": self._parse_timestamp(date),
                })

            # Recursively check nested structures
            for value in data.values():
                if isinstance(value, (dict, list)):
                    entries.extend(self._extract_from_json(value))

        elif isinstance(data, list):
            for item in data:
                entries.extend(self._extract_from_json(item))

        return entries

    def _extract_from_xml(self, root: ET.Element) -> List[Dict]:
        """Extract reading list entries from XML."""
        entries = []

        for elem in root.iter():
            url = elem.get("url") or elem.get("href") or elem.findtext("url") or elem.findtext("Url")
            title = elem.get("title") or elem.get("name") or elem.findtext("title") or elem.findtext("Title")
            date = elem.get("dateAdded") or elem.findtext("dateAdded")

            if url:
                entries.append({
                    "url": url,
                    "title": title or "",
                    "date_added": self._parse_timestamp(date),
                })

        return entries

    def _extract_urls_from_text(self, text: str) -> List[Dict]:
        """Extract URLs from plain text."""
        import re

        entries = []
        url_pattern = re.compile(r'https?://[^\s<>"\']+')

        for match in url_pattern.finditer(text):
            url = match.group(0)
            # Clean up trailing punctuation
            url = url.rstrip('.,;:!?)\'\"')
            entries.append({
                "url": url,
                "title": "",
                "date_added": None,
            })

        return entries

    def _parse_timestamp(self, value: Any) -> Optional[str]:
        """Parse various timestamp formats to ISO string."""
        if not value:
            return None

        if isinstance(value, str):
            # Already ISO format
            if "T" in value or "-" in value:
                return value
            # Try Unix timestamp string
            try:
                ts = int(value)
                return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            except (ValueError, OSError):
                pass

        elif isinstance(value, (int, float)):
            try:
                # Unix timestamp
                if value < 1e12:
                    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
                # Milliseconds
                elif value < 1e15:
                    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).isoformat()
                # WebKit timestamp (microseconds since 1601)
                else:
                    from .._timestamps import webkit_to_datetime
                    dt = webkit_to_datetime(int(value))
                    return dt.isoformat() if dt else None
            except (ValueError, OSError, OverflowError):
                pass

        return None
