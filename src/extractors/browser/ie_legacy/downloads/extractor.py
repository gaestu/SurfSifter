"""
Internet Explorer Downloads Extractor

Parses download history from WebCacheV01.dat ESE database and ingests
into the evidence database.

This extractor uses the output from IEWebCacheExtractor as input.
It reads the extracted WebCacheV01.dat files and parses the iedownload
container to extract download entries.

WebCache Download Container:
- Container name: "iedownload"
- Table: Container_{N} where N is the iedownload ContainerId
- Key columns: Url, AccessedTime, ModifiedTime, Filename, ResponseHeaders

Features:
- FILETIME timestamp conversion
- URL and filename extraction
- File size parsing from ResponseHeaders
- Integration with browser_downloads table
- Multi-user support (multiple WebCache files)
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse, unquote

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from .._patterns import (
    IE_BROWSERS,
    get_browser_display_name,
    extract_user_from_path,
)
from .._ese_reader import (
    ESE_AVAILABLE,
    WebCacheReader,
    check_ese_available,
)
from .._timestamps import filetime_to_iso, filetime_to_datetime
from core.logging import get_logger
from core.database import (
    insert_browser_download_row,
    insert_browser_inventory,
    insert_urls,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.downloads")


class IEDownloadsExtractor(BaseExtractor):
    """
    Parse and ingest IE/Legacy Edge download history.

    This extractor processes WebCacheV01.dat files that were extracted
    by IEWebCacheExtractor. It parses the ESE database to extract
    download entries from the iedownload container.

    Workflow:
    1. Read manifest from WebCache extraction
    2. For each WebCache file:
       - Open ESE database
       - Find iedownload container
       - Parse download entries
       - Insert into browser_downloads table
    3. Return counts

    Note: This extractor only handles ingestion. The extraction phase
    is performed by IEWebCacheExtractor.
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="ie_downloads",
            display_name="IE/Edge Downloads",
            description="Parse download history from WebCache database",
            category="browser",
            requires_tools=[],  # ESE library checked at runtime
            can_extract=False,  # Extraction handled by IEWebCacheExtractor
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """
        This extractor doesn't do extraction.

        Use IEWebCacheExtractor for extraction.
        """
        return False, "Use IEWebCacheExtractor for extraction phase"

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run (manifest exists and ESE library available)."""
        # Check for manifest
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run IEWebCacheExtractor first"

        # Check for ESE library
        ese_ok, ese_msg = check_ese_available()
        if not ese_ok:
            return False, f"ESE library required: {ese_msg}"

        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return ESE status widget for configuration display."""
        from ....widgets import ESEStatusWidget
        return ESEStatusWidget(parent, show_install_hint=True)

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
            file_count = len([f for f in data.get("files", []) if f.get("file_type") != "log"])
            status_text = f"IE/Edge Downloads\nWebCache files: {file_count}"
        else:
            status_text = "IE/Edge Downloads\nNo WebCache extracted"

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
        """Return output directory - same as WebCache extractor."""
        return case_root / "evidences" / evidence_label / "ie_webcache"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extraction is handled by IEWebCacheExtractor.
        """
        callbacks.on_log(
            "Use IEWebCacheExtractor for extraction. "
            "This extractor only handles ingestion.",
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
        Parse WebCache databases and ingest downloads into database.

        Workflow:
            1. Read manifest.json from WebCache extraction
            2. For each WebCache database file:
               - Open ESE database with WebCacheReader
               - Find iedownload container
               - Parse download entries
               - Insert into browser_downloads table
            3. Return counts
        """
        callbacks.on_step("Reading WebCache manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"downloads": 0}

        # Check ESE library
        ese_ok, ese_msg = check_ese_available()
        if not ese_ok:
            callbacks.on_error("ESE library not available", ese_msg)
            return {"downloads": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())
        evidence_label = config.get("evidence_label", "")

        # Filter to WebCache database files only
        webcache_files = [
            f for f in manifest_data.get("files", [])
            if f.get("file_type") == "database" or f.get("artifact_type") == "webcache"
        ]

        if not webcache_files:
            callbacks.on_log("No WebCache database files to process", "warning")
            return {"downloads": 0}

        # Continue statistics tracking for ingestion phase
        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        total_downloads = 0
        failed_files = 0

        callbacks.on_progress(0, len(webcache_files), "Parsing WebCache downloads")

        for i, file_entry in enumerate(webcache_files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(webcache_files),
                f"Parsing {file_entry.get('user', 'unknown')} downloads"
            )

            try:
                # Parse WebCache and insert downloads
                db_path = Path(file_entry["extracted_path"])
                if not db_path.is_absolute():
                    db_path = output_dir / db_path

                if not db_path.exists():
                    callbacks.on_log(f"WebCache file not found: {db_path}", "warning")
                    failed_files += 1
                    continue

                # Register in browser inventory
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "ie"),
                    artifact_type="downloads",
                    run_id=run_id,
                    extracted_path=str(db_path),
                    extraction_status="ok",
                    extraction_timestamp_utc=manifest_data.get("extraction_timestamp_utc"),
                    logical_path=file_entry.get("logical_path", ""),
                    profile=file_entry.get("user"),
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    forensic_path=file_entry.get("forensic_path"),
                    extraction_tool=manifest_data.get("extraction_tool"),
                    file_size_bytes=file_entry.get("size_bytes"),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                )

                downloads = self._parse_and_insert_downloads(
                    db_path,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                    callbacks,
                )

                # Update inventory with ingestion status
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=downloads,
                )

                total_downloads += downloads

            except Exception as e:
                error_msg = f"Failed to parse {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")
                failed_files += 1

                # Update inventory status on failure
                if "inventory_id" in locals() and inventory_id:
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(e),
                    )

        evidence_conn.commit()

        # Report final statistics
        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=total_downloads,
            )
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(
            f"Ingested {total_downloads} downloads",
            "info"
        )

        return {"downloads": total_downloads}

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

    def _parse_and_insert_downloads(
        self,
        db_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
    ) -> int:
        """
        Parse WebCache downloads and insert into database.

        Returns:
            Number of downloads inserted
        """
        user = file_entry.get("user", "unknown")
        browser = file_entry.get("browser", "ie")
        source_path = file_entry.get("logical_path", "")
        partition_index = file_entry.get("partition_index", 0)
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        total_downloads = 0
        url_records = []  # Collect URLs for unified urls table
        seen_urls = set()  # Deduplicate URLs

        try:
            with WebCacheReader(db_path) as reader:
                # Get containers to find iedownload
                containers = reader.get_containers()
                download_container = None

                for container in containers:
                    if container.get("name") == "iedownload":
                        download_container = container
                        break

                if download_container is None:
                    callbacks.on_log(f"No iedownload container found in {db_path.name}", "info")
                    return 0

                # Get the download table name
                container_id = download_container.get("container_id")
                download_table = f"Container_{container_id}"

                if download_table not in reader.tables():
                    callbacks.on_log(f"Download table {download_table} not found", "warning")
                    return 0

                callbacks.on_log(f"Reading downloads from {download_table}", "info")

                # Parse download entries
                for record in reader.read_table(download_table):
                    url = record.get("Url")
                    if not url:
                        continue

                    # Skip non-http URLs
                    if not url.startswith(("http://", "https://")):
                        continue

                    # Convert timestamps
                    accessed_time = record.get("AccessedTime")
                    modified_time = record.get("ModifiedTime")

                    start_time_iso = filetime_to_iso(modified_time) if modified_time else None
                    end_time_iso = filetime_to_iso(accessed_time) if accessed_time else None

                    # Extract filename from URL or Filename field
                    filename = record.get("Filename") or ""
                    if not filename:
                        try:
                            parsed = urlparse(url)
                            filename = unquote(parsed.path.split("/")[-1]) or "unknown"
                        except Exception:
                            filename = "unknown"

                    # Try to extract file size and MIME type from ResponseHeaders
                    response_headers = record.get("ResponseHeaders")
                    file_size = self._extract_content_length(response_headers)
                    mime_type = self._extract_mime_type(response_headers)

                    # Determine target path (if available)
                    target_path = record.get("TargetPath") or ""

                    # WebCache doesn't track download state well
                    # Assume complete if we have an AccessedTime
                    state = "complete" if accessed_time else "unknown"

                    # Insert download
                    insert_browser_download_row(
                        evidence_conn,
                        evidence_id=evidence_id,
                        browser=browser,
                        profile=user,
                        url=url,
                        filename=filename,
                        target_path=target_path,
                        start_time_utc=start_time_iso,
                        end_time_utc=end_time_iso,
                        received_bytes=file_size,
                        total_bytes=file_size,
                        mime_type=mime_type,
                        state=state,
                        danger_type="not_dangerous",
                        opened=False,
                        source_path=source_path,
                        discovered_by=discovered_by,
                        run_id=run_id,
                        partition_index=partition_index,
                    )

                    total_downloads += 1

                    # Collect URL for unified urls table (dual-write)
                    if url not in seen_urls:
                        seen_urls.add(url)
                        try:
                            parsed = urlparse(url)
                            url_records.append({
                                "url": url,
                                "domain": parsed.netloc or None,
                                "scheme": parsed.scheme or None,
                                "discovered_by": discovered_by,
                                "run_id": run_id,
                                "source_path": source_path,
                                "context": f"download:{browser}:{user}",
                                "first_seen_utc": start_time_iso,
                            })
                        except Exception:
                            pass

        except Exception as e:
            LOGGER.error("Failed to read WebCache %s: %s", db_path, e, exc_info=True)
            callbacks.on_error(f"ESE parse error: {e}", str(db_path))
            return 0

        # Cross-post URLs to unified urls table for analysis
        if url_records:
            try:
                insert_urls(evidence_conn, evidence_id, url_records)
                LOGGER.debug("Cross-posted %d download URLs to urls table", len(url_records))
            except Exception as e:
                LOGGER.debug("Failed to cross-post download URLs: %s", e)

        if total_downloads > 0:
            callbacks.on_log(
                f"Inserted {total_downloads} downloads for {user} ({browser})",
                "info"
            )

        return total_downloads

    def _extract_content_length(self, response_headers: Optional[bytes]) -> Optional[int]:
        """
        Extract Content-Length from HTTP response headers.

        Args:
            response_headers: Raw bytes of HTTP response headers

        Returns:
            Content-Length value or None
        """
        if not response_headers:
            return None

        try:
            # Try to decode headers
            if isinstance(response_headers, bytes):
                headers_str = response_headers.decode("utf-8", errors="ignore")
            else:
                headers_str = str(response_headers)

            # Look for Content-Length header
            match = re.search(r"Content-Length:\s*(\d+)", headers_str, re.IGNORECASE)
            if match:
                return int(match.group(1))
        except Exception:
            pass

        return None

    def _extract_mime_type(self, response_headers: Optional[bytes]) -> Optional[str]:
        """
        Extract Content-Type (MIME type) from HTTP response headers.

        Args:
            response_headers: Raw bytes of HTTP response headers

        Returns:
            MIME type string (without charset/parameters) or None
        """
        if not response_headers:
            return None

        try:
            # Try to decode headers
            if isinstance(response_headers, bytes):
                headers_str = response_headers.decode("utf-8", errors="ignore")
            else:
                headers_str = str(response_headers)

            # Look for Content-Type header (capture up to ; or newline)
            match = re.search(r"Content-Type:\s*([^\r\n;]+)", headers_str, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        except Exception:
            pass

        return None
