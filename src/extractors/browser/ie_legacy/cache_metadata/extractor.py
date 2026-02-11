"""
IE/Legacy Edge Cache Metadata Extractor.

Extracts cache metadata from WebCache Content containers. This provides
a listing of cached resources (URLs, timestamps, sizes) without extracting
the actual cached files.

WebCache Content Containers:
- Content: Main cache container
- ContentLow: Low integrity (Protected Mode) cache
- MSHist*: History containers (also listed in Content)

Forensic Value:
- Shows all cached resources even if actual cache files are deleted
- Timestamps reveal browsing activity
- File sizes indicate download volumes
- Cache directories show where content was stored

Dependencies:
- libesedb-python or dissect.esedb (required for WebCache parsing)
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse

from PySide6.QtWidgets import QWidget, QLabel, QVBoxLayout

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from .._ese_reader import (
    WebCacheReader,
    check_ese_available,
)
from .._timestamps import filetime_to_iso
from core.logging import get_logger
from core.database import (
    insert_urls,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.cache_metadata")


class IECacheMetadataExtractor(BaseExtractor):
    """
    Extract cache metadata from IE WebCache Content containers.

    This extractor processes WebCacheV01.dat files that were extracted
    by IEWebCacheExtractor. It parses the Content containers to extract
    cache metadata (URLs, timestamps, sizes).

    Note: This extractor does NOT extract actual cached files.
    For cache file extraction, use file carving or INetCache patterns.
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="ie_cache_metadata",
            display_name="IE/Edge Cache Metadata",
            description="Extract cache URL listing from WebCache Content containers",
            category="browser",
            requires_tools=["libesedb-python"],
            can_extract=False,  # Uses IEWebCacheExtractor output
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """This extractor doesn't do extraction."""
        return False, "Use IEWebCacheExtractor for extraction phase"

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run."""
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run IEWebCacheExtractor first"

        ese_ok, ese_msg = check_ese_available()
        if not ese_ok:
            return False, f"ESE library required: {ese_msg}"

        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return ESE status widget."""
        from ....widgets import ESEStatusWidget
        return ESEStatusWidget(parent, show_install_hint=True)

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
            file_count = len([f for f in data.get("files", []) if f.get("file_type") != "log"])
            status_text = f"IE/Edge Cache Metadata\nWebCache files: {file_count}"
        else:
            status_text = "IE/Edge Cache Metadata\nNo WebCache extracted"

        ese_ok, _ = check_ese_available()
        if not ese_ok:
            status_text += "\n⚠️ ESE library not installed"

        layout.addWidget(QLabel(status_text, widget))
        return widget

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory - same as WebCache extractor."""
        return case_root / "evidences" / evidence_label / "ie_webcache"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extraction is handled by IEWebCacheExtractor."""
        callbacks.on_log(
            "Use IEWebCacheExtractor for extraction. This extractor only handles ingestion.",
            "warning"
        )
        return False

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """
        Parse WebCache Content containers and extract cache metadata.
        """
        callbacks.on_step("Reading WebCache manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"urls": 0, "cache_entries": 0}

        ese_ok, ese_msg = check_ese_available()
        if not ese_ok:
            callbacks.on_error("ESE library not available", ese_msg)
            return {"urls": 0, "cache_entries": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())
        evidence_label = config.get("evidence_label", "")

        webcache_files = [
            f for f in manifest_data.get("files", [])
            if f.get("file_type") == "database" or f.get("artifact_type") == "webcache"
        ]

        if not webcache_files:
            callbacks.on_log("No WebCache database files to process", "warning")
            return {"urls": 0, "cache_entries": 0}

        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        total_entries = 0
        total_urls = 0
        failed_files = 0

        callbacks.on_progress(0, len(webcache_files), "Parsing cache metadata")

        for i, file_entry in enumerate(webcache_files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(webcache_files),
                f"Parsing {file_entry.get('user', 'unknown')} cache"
            )

            try:
                db_path = Path(file_entry["extracted_path"])
                if not db_path.is_absolute():
                    db_path = output_dir / db_path

                if not db_path.exists():
                    callbacks.on_log(f"WebCache file not found: {db_path}", "warning")
                    failed_files += 1
                    continue

                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "ie"),
                    artifact_type="cache_metadata",
                    run_id=run_id,
                    extracted_path=str(db_path),
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

                entries, urls = self._parse_cache_metadata(
                    db_path,
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
                total_urls += urls

            except Exception as e:
                error_msg = f"Failed to parse {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")
                failed_files += 1

        evidence_conn.commit()

        if collector:
            collector.report_ingested(evidence_id, self.metadata.name, records=total_urls)
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(f"Ingested {total_entries} cache entries, {total_urls} URLs", "info")

        return {"urls": total_urls, "cache_entries": total_entries}

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

    def _parse_cache_metadata(
        self,
        db_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
    ) -> tuple[int, int]:
        """
        Parse WebCache Content containers for cache metadata.

        Returns:
            Tuple of (cache_entries, urls_inserted)
        """
        user = file_entry.get("user", "unknown")
        browser = file_entry.get("browser", "ie")
        source_path = file_entry.get("logical_path", "")
        partition_index = file_entry.get("partition_index", 0)
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        total_entries = 0
        url_records = []

        try:
            with WebCacheReader(db_path) as reader:
                containers = reader.get_containers()

                # Find Content containers
                content_containers = [
                    c for c in containers
                    if c.get("name") and (
                        c.get("name").lower().startswith("content") or
                        "cache" in c.get("name", "").lower()
                    )
                ]

                if not content_containers:
                    callbacks.on_log(f"No Content containers found in {db_path.name}", "info")
                    return 0, 0

                callbacks.on_log(
                    f"Found {len(content_containers)} Content container(s)",
                    "info"
                )

                for container in content_containers:
                    container_id = container.get("container_id")
                    container_name = container.get("name", "Content")
                    table_name = f"Container_{container_id}"

                    if table_name not in reader.tables():
                        continue

                    # Determine if low integrity cache
                    is_low = "low" in container_name.lower()

                    for record in reader.read_table(table_name):
                        url = record.get("Url")
                        if not url:
                            continue

                        total_entries += 1

                        # Extract metadata
                        accessed_time = record.get("AccessedTime")
                        modified_time = record.get("ModifiedTime")
                        expiry_time = record.get("ExpiryTime")
                        sync_time = record.get("SyncTime")

                        accessed_iso = filetime_to_iso(accessed_time) if accessed_time else None
                        modified_iso = filetime_to_iso(modified_time) if modified_time else None
                        expiry_iso = filetime_to_iso(expiry_time) if expiry_time else None

                        # Size info
                        file_size = record.get("FileSize") or 0

                        # Cache directory
                        directory = container.get("directory", "")

                        # Response headers may contain content-type
                        content_type = ""
                        response_headers = record.get("ResponseHeaders")
                        if response_headers:
                            if isinstance(response_headers, bytes):
                                try:
                                    headers = response_headers.decode("utf-8", errors="replace")
                                    for line in headers.split("\r\n"):
                                        if line.lower().startswith("content-type:"):
                                            content_type = line.split(":", 1)[1].strip()
                                            break
                                except Exception:
                                    pass

                        # Build notes
                        notes_parts = []
                        if is_low:
                            notes_parts.append("protected_mode")
                        if content_type:
                            notes_parts.append(f"type={content_type}")
                        if file_size:
                            notes_parts.append(f"size={file_size}")
                        if directory:
                            notes_parts.append(f"dir={directory}")
                        notes_parts.append("cached_resource")

                        # Extract domain and scheme from URL
                        domain = None
                        scheme = None
                        try:
                            parsed = urlparse(url)
                            scheme = parsed.scheme or None
                            domain = parsed.netloc or None
                        except Exception:
                            pass

                        url_records.append({
                            "url": url,
                            "domain": domain,
                            "scheme": scheme,
                            "discovered_by": discovered_by,
                            "source_path": source_path,
                            "run_id": run_id,
                            "first_seen_utc": accessed_iso or modified_iso,
                            "last_seen_utc": accessed_iso or modified_iso,
                            "content_type": content_type or None,
                            "notes": "; ".join(notes_parts) if notes_parts else None,
                        })

        except Exception as e:
            LOGGER.error("Failed to read WebCache %s: %s", db_path, e, exc_info=True)
            callbacks.on_error(f"ESE parse error: {e}", str(db_path))
            return 0, 0

        # Insert URLs in batch
        if url_records:
            try:
                insert_urls(evidence_conn, evidence_id, url_records)
            except Exception as e:
                LOGGER.error("Failed to insert URLs: %s", e)

        if total_entries > 0:
            callbacks.on_log(
                f"Parsed {total_entries} cache entries for {user} ({browser})",
                "info"
            )

        return total_entries, len(url_records)
