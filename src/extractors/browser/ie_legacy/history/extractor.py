"""
Internet Explorer History Extractor

Parses browsing history from WebCacheV01.dat ESE database and ingests
into the evidence database.

This extractor uses the output from IEWebCacheExtractor as input.
It reads the extracted WebCacheV01.dat files and parses the History
container to extract browsing history entries.

WebCache History Container:
- Table: Container_{N} where N is the History ContainerId
- Key columns: Url, AccessedTime, ModifiedTime, ExpiryTime, AccessCount

Features:
- FILETIME timestamp conversion
- URL parsing and domain extraction
- Visit count tracking
- Integration with browser_history table
- Multi-user support (multiple WebCache files)
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
from urllib.parse import urlparse

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
    insert_browser_history_row,
    insert_browser_history_rows,
    insert_browser_inventory,
    insert_urls,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.history")


class IEHistoryExtractor(BaseExtractor):
    """
    Parse and ingest IE/Legacy Edge browsing history.

    This extractor processes WebCacheV01.dat files that were extracted
    by IEWebCacheExtractor. It parses the ESE database to extract
    browsing history entries.

    Workflow:
    1. Read manifest from WebCache extraction
    2. For each WebCache file:
       - Open ESE database
       - Find History container
       - Parse history entries
       - Insert into browser_history table
    3. Return counts

    Note: This extractor only handles ingestion. The extraction phase
    is performed by IEWebCacheExtractor.
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="ie_history",
            display_name="IE/Edge History",
            description="Parse browsing history from WebCache database",
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
            status_text = f"IE/Edge History\nWebCache files: {file_count}"
        else:
            status_text = "IE/Edge History\nNo WebCache extracted"

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
        Parse WebCache databases and ingest history into database.

        Workflow:
            1. Read manifest.json from WebCache extraction
            2. For each WebCache database file:
               - Open ESE database with WebCacheReader
               - Find History container
               - Parse history entries
               - Insert into browser_history table
            3. Return counts
        """
        callbacks.on_step("Reading WebCache manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"urls": 0, "records": 0}

        # Check ESE library
        ese_ok, ese_msg = check_ese_available()
        if not ese_ok:
            callbacks.on_error("ESE library not available", ese_msg)
            return {"urls": 0, "records": 0}

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
            return {"urls": 0, "records": 0}

        # Continue statistics tracking for ingestion phase
        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        total_records = 0
        total_urls = 0
        failed_files = 0

        callbacks.on_progress(0, len(webcache_files), "Parsing WebCache history")

        for i, file_entry in enumerate(webcache_files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(webcache_files),
                f"Parsing {file_entry.get('user', 'unknown')} history"
            )

            try:
                # Parse WebCache and insert history
                db_path = Path(file_entry["extracted_path"])
                if not db_path.is_absolute():
                    db_path = output_dir / db_path

                if not db_path.exists():
                    callbacks.on_log(f"WebCache file not found: {db_path}", "warning")
                    failed_files += 1
                    continue

                # Register in browser inventory (like Chromium extractor)
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "ie"),
                    artifact_type="history",
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

                records, urls = self._parse_and_insert_history(
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
                    urls_parsed=urls,
                    records_parsed=records,
                )

                total_records += records
                total_urls += urls

            except Exception as e:
                error_msg = f"Failed to parse {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")
                failed_files += 1

                # Update inventory status on failure (if inventory was created)
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
                records=total_records,
                urls=total_urls,
            )
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(
            f"Ingested {total_records} history records, {total_urls} unique URLs",
            "info"
        )

        return {"urls": total_urls, "records": total_records}

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

    def _parse_and_insert_history(
        self,
        db_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
    ) -> tuple[int, int]:
        """
        Parse WebCache history and insert into database.

        Returns:
            Tuple of (records_inserted, unique_urls)
        """
        user = file_entry.get("user", "unknown")
        browser = file_entry.get("browser", "ie")
        source_path = file_entry.get("logical_path", "")
        partition_index = file_entry.get("partition_index", 0)
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        records = []
        url_set = set()

        try:
            with WebCacheReader(db_path) as reader:
                # Log available tables
                tables = reader.tables()
                LOGGER.debug("WebCache tables: %s", tables)

                # Get containers to find ALL History containers
                # WebCache can have multiple History containers for different contexts
                containers = reader.get_containers()
                history_containers = [
                    c for c in containers
                    if c.get("name") == "History"
                ]

                if not history_containers:
                    callbacks.on_log(f"No History container found in {db_path.name}", "warning")
                    return 0, 0

                callbacks.on_log(
                    f"Found {len(history_containers)} History container(s) in {db_path.name}",
                    "info"
                )

                # Process ALL History containers
                for history_container in history_containers:
                    container_id = history_container.get("container_id")
                    history_table = f"Container_{container_id}"

                    if history_table not in tables:
                        callbacks.on_log(f"History table {history_table} not found", "warning")
                        continue

                    callbacks.on_log(f"Reading history from {history_table}", "info")

                    # Parse history entries
                    for record in reader.read_table(history_table):
                        raw_url = record.get("Url")
                        if not raw_url:
                            continue

                        # WebCache URLs have format: "Visited: username@http://..." or just "http://..."
                        # Also handle: "username@http://..." without "Visited: " prefix
                        url = self._extract_url_from_webcache_entry(raw_url)

                        # Skip empty URLs only (ingest all schemes: http, https, file, res, mailto, etc.)
                        if not url:
                            continue

                        # Convert timestamps
                        accessed_time = record.get("AccessedTime")
                        modified_time = record.get("ModifiedTime")
                        expiry_time = record.get("ExpiryTime")
                        access_count = record.get("AccessCount") or 1

                        accessed_iso = filetime_to_iso(accessed_time) if accessed_time else None
                        modified_iso = filetime_to_iso(modified_time) if modified_time else None

                        # Try to get page title (some WebCache versions store it)
                        # Note: WebCache typically does NOT store page titles.
                        # ResponseHeaders contains binary property store data, not text.
                        # Only use explicit title columns if they exist.
                        title = record.get("PageTitle") or record.get("Title") or ""

                        # Convert bytes to string if needed (ESE may return bytes)
                        if isinstance(title, bytes):
                            try:
                                title = title.decode("utf-8", errors="replace")
                            except Exception:
                                title = ""

                        # Validate title is actually readable text (not binary garbage)
                        if title:
                            # Check for binary data indicators (null bytes, excessive non-printable chars)
                            non_printable = sum(1 for c in title[:100] if ord(c) < 32 and c not in '\t\n\r')
                            if non_printable > 5 or '\x00' in title:
                                title = ""  # Binary data, not a real title

                        # Parse URL for domain
                        try:
                            parsed = urlparse(url)
                            domain = parsed.netloc
                        except Exception:
                            domain = ""

                        # Build record for browser_history table
                        history_record = {
                            "url": url,
                            "title": title,
                            "visit_time_utc": accessed_iso,
                            "visit_count": access_count,
                            "browser": browser,
                            "profile": user,
                            "source_path": source_path,
                            "discovered_by": discovered_by,
                            "run_id": run_id,
                            "partition_index": partition_index,
                        }

                        records.append(history_record)
                        url_set.add(url)

        except Exception as e:
            LOGGER.error("Failed to read WebCache %s: %s", db_path, e, exc_info=True)
            callbacks.on_error(f"ESE parse error: {e}", str(db_path))
            return 0, 0

        # Batch insert to browser_history table
        if records:
            insert_browser_history_rows(evidence_conn, evidence_id, records)
            callbacks.on_log(
                f"Inserted {len(records)} history records for {user} ({browser})",
                "info"
            )

            # Build URL records with proper first_seen/last_seen aggregation
            url_timestamps: Dict[str, Dict[str, Any]] = {}
            for r in records:
                url = r["url"]
                visit_time = r.get("visit_time_utc")

                if url not in url_timestamps:
                    url_timestamps[url] = {
                        "first_seen": visit_time,
                        "last_seen": visit_time,
                        "visit_count": r.get("visit_count", 1),
                    }
                else:
                    existing = url_timestamps[url]
                    # Update first_seen if earlier
                    if visit_time and existing["first_seen"]:
                        if visit_time < existing["first_seen"]:
                            existing["first_seen"] = visit_time
                    elif visit_time and not existing["first_seen"]:
                        existing["first_seen"] = visit_time

                    # Update last_seen if later
                    if visit_time and existing["last_seen"]:
                        if visit_time > existing["last_seen"]:
                            existing["last_seen"] = visit_time
                    elif visit_time and not existing["last_seen"]:
                        existing["last_seen"] = visit_time

                    # Accumulate visit count
                    existing["visit_count"] += r.get("visit_count", 1)

            # Insert unique URLs to urls table
            url_records = []
            for url, timestamps in url_timestamps.items():
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
                    "first_seen_utc": timestamps["first_seen"],
                    "last_seen_utc": timestamps["last_seen"],
                })

            if url_records:
                insert_urls(evidence_conn, evidence_id, url_records)

        return len(records), len(url_set)

    def _extract_url_from_webcache_entry(self, raw_url: str) -> Optional[str]:
        """
        Extract the actual URL from a WebCache history entry.

        WebCache stores URLs in various formats:
        - "Visited: username@https://example.com/path"
        - "username@https://example.com/path"
        - "https://example.com/path" (plain URL)
        - "Visited: username@file://C:/path" (file URLs)
        - "username@ftp://server/path" (other schemes)

        Args:
            raw_url: Raw URL string from WebCache container

        Returns:
            Extracted URL or None if no valid URL found
        """
        if not raw_url:
            return None

        url = raw_url

        # Strip "Visited: " prefix (common in History containers)
        if url.lower().startswith("visited:"):
            url = url[8:].strip()

        # Handle "username@url" format generically for any scheme
        # Look for @ followed by a URL scheme (scheme://)
        # Pattern: anything before @ where after @ we have scheme://
        # Handles nested schemes like blob:https:// or view-source:https://
        at_pos = url.find("@")
        if at_pos != -1:
            after_at = url[at_pos + 1:]
            # Check if what follows @ looks like a URL (contains ://)
            scheme_sep = after_at.find("://")
            if scheme_sep != -1:
                # Extract the base scheme (part before first colon)
                # This handles nested schemes like blob:https:// or view-source:https://
                first_colon = after_at.find(":")
                if first_colon != -1:
                    base_scheme = after_at[:first_colon]
                    # Verify it's a valid scheme (alphanumeric, +, -, .)
                    if base_scheme and all(c.isalnum() or c in "+-." for c in base_scheme):
                        url = after_at  # Strip the username@ prefix

        return url.strip() if url else None
