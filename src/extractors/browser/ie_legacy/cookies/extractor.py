"""
Internet Explorer Cookie Metadata Extractor

Parses cookie *metadata* from WebCacheV01.dat ESE database and ingests
into the evidence database.

IMPORTANT: This extractor retrieves cookie URL references and timestamps
from the WebCache database, NOT actual cookie names/values. The WebCache
Container_N tables for cookies store metadata (URL, access times, flags)
as an index to external .cookie files.

For actual cookie content (name, value), use:
- IEINetCookiesExtractor: Parses .cookie files in INetCookies folders
- The INetCookies files contain the actual name=value pairs

WebCache Cookie Containers:
- Multiple containers may exist: Cookies, CookiesLow, etc.
- Key columns: Url, AccessedTime, ModifiedTime, ExpiryTime, Flags
- Url format: Cookie:user@domain.com/path

This extractor uses the output from IEWebCacheExtractor as input.
It reads the extracted WebCacheV01.dat files and parses the Cookies
container(s) to extract cookie metadata entries.

Features:
- FILETIME timestamp conversion
- Domain extraction from URL
- Secure/HttpOnly flag parsing
- Multi-user support (multiple WebCache files)
- Integration with cookies table (marked as metadata-only)

Forensic Value:
- Provides cookie access/modification timeline
- Shows cookie URL patterns even if .cookie files are deleted
- Correlates with INetCookies files for complete cookie forensics
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
    insert_cookie_row,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.cookies")


class IECookiesExtractor(BaseExtractor):
    """
    Parse and ingest IE/Legacy Edge cookies.

    This extractor processes WebCacheV01.dat files that were extracted
    by IEWebCacheExtractor. It parses the ESE database to extract
    cookie entries from all cookie-related containers.

    Workflow:
    1. Read manifest from WebCache extraction
    2. For each WebCache file:
       - Open ESE database
       - Find all cookie containers (Cookies, CookiesLow, etc.)
       - Parse cookie entries
       - Insert into cookies table
    3. Return counts

    Note: This extractor only handles ingestion. The extraction phase
    is performed by IEWebCacheExtractor.
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="ie_cookies",
            display_name="IE/Edge Cookies",
            description="Parse cookies from WebCache database",
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
            status_text = f"IE/Edge Cookies\nWebCache files: {file_count}"
        else:
            status_text = "IE/Edge Cookies\nNo WebCache extracted"

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
        Parse WebCache databases and ingest cookies into database.

        Workflow:
            1. Read manifest.json from WebCache extraction
            2. For each WebCache database file:
               - Open ESE database with WebCacheReader
               - Find all cookie containers
               - Parse cookie entries
               - Insert into cookies table
            3. Return counts
        """
        callbacks.on_step("Reading WebCache manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"cookies": 0}

        # Check ESE library
        ese_ok, ese_msg = check_ese_available()
        if not ese_ok:
            callbacks.on_error("ESE library not available", ese_msg)
            return {"cookies": 0}

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
            return {"cookies": 0}

        # Continue statistics tracking for ingestion phase
        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        total_cookies = 0
        failed_files = 0

        callbacks.on_progress(0, len(webcache_files), "Parsing WebCache cookies")

        for i, file_entry in enumerate(webcache_files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(webcache_files),
                f"Parsing {file_entry.get('user', 'unknown')} cookies"
            )

            try:
                # Parse WebCache and insert cookies
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
                    artifact_type="cookies",
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

                cookies = self._parse_and_insert_cookies(
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
                    records_parsed=cookies,
                )

                total_cookies += cookies

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
                records=total_cookies,
            )
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(
            f"Ingested {total_cookies} cookies",
            "info"
        )

        return {"cookies": total_cookies}

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

    def _parse_and_insert_cookies(
        self,
        db_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
    ) -> int:
        """
        Parse WebCache cookies and insert into database.

        Returns:
            Number of cookies inserted
        """
        user = file_entry.get("user", "unknown")
        browser = file_entry.get("browser", "ie")
        source_path = file_entry.get("logical_path", "")
        partition_index = file_entry.get("partition_index", 0)
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        total_cookies = 0

        try:
            with WebCacheReader(db_path) as reader:
                # Get all containers
                containers = reader.get_containers()

                # Find all cookie-related containers
                cookie_containers = [
                    c for c in containers
                    if c.get("name") and "cookie" in c.get("name", "").lower()
                ]

                if not cookie_containers:
                    callbacks.on_log(f"No cookie containers found in {db_path.name}", "warning")
                    return 0

                callbacks.on_log(
                    f"Found {len(cookie_containers)} cookie container(s)",
                    "info"
                )

                # Process each cookie container
                for container in cookie_containers:
                    container_id = container.get("container_id")
                    container_name = container.get("name", "Cookies")
                    table_name = f"Container_{container_id}"

                    if table_name not in reader.tables():
                        continue

                    callbacks.on_log(f"Reading cookies from {container_name}", "info")

                    # Parse cookie entries
                    for record in reader.read_table(table_name):
                        url = record.get("Url")
                        if not url:
                            continue

                        # Extract domain from URL
                        try:
                            parsed = urlparse(url)
                            domain = parsed.netloc or url
                            path = parsed.path or "/"
                        except Exception:
                            domain = url
                            path = "/"

                        # Convert timestamps
                        accessed_time = record.get("AccessedTime")
                        modified_time = record.get("ModifiedTime")
                        expiry_time = record.get("ExpiryTime")

                        accessed_iso = filetime_to_iso(accessed_time) if accessed_time else None
                        modified_iso = filetime_to_iso(modified_time) if modified_time else None
                        expiry_iso = filetime_to_iso(expiry_time) if expiry_time else None

                        # Parse flags for secure/httponly
                        # IE WebCache flags: 0x01=Secure, 0x02=HttpOnly (unconfirmed)
                        flags = record.get("Flags") or 0
                        is_secure = bool(flags & 0x01) if isinstance(flags, int) else False
                        is_httponly = bool(flags & 0x02) if isinstance(flags, int) else False

                        # WebCache cookie containers store URL-based metadata,
                        # not individual cookie name/value pairs.
                        # The actual cookie content (name, value) is stored in:
                        # - External .cookie files in INetCookies (use ie_inetcookies extractor)
                        # - CookieEntryEx_XX tables for Edge (binary blobs)
                        #
                        # We store the URL as an identifier and note this is metadata-only.
                        # The cookie "name" is derived from the URL to provide uniqueness.
                        cookie_name = self._extract_cookie_name_from_url(url)
                        cookie_value = ""  # Content in external .cookie files

                        # Insert cookie metadata record
                        # Note: is_persistent not stored - can be inferred from expires_utc presence
                        insert_cookie_row(
                            evidence_conn,
                            evidence_id=evidence_id,
                            browser=browser,
                            profile=user,
                            domain=domain,
                            name=cookie_name,
                            path=path,
                            value=cookie_value,
                            creation_utc=modified_iso,
                            last_access_utc=accessed_iso,
                            expires_utc=expiry_iso,
                            is_secure=is_secure,
                            is_httponly=is_httponly,
                            samesite="None",
                            source_path=source_path,
                            discovered_by=discovered_by,
                            run_id=run_id,
                            partition_index=partition_index,
                            notes="webcache_metadata:url_reference_only",
                        )

                        total_cookies += 1

        except Exception as e:
            LOGGER.error("Failed to read WebCache %s: %s", db_path, e, exc_info=True)
            callbacks.on_error(f"ESE parse error: {e}", str(db_path))
            return 0

        if total_cookies > 0:
            callbacks.on_log(
                f"Inserted {total_cookies} cookie metadata entries for {user} ({browser})",
                "info"
            )

        return total_cookies

    def _extract_cookie_name_from_url(self, url: str) -> str:
        """
        Extract a meaningful cookie identifier from a WebCache cookie URL.

        WebCache stores cookie metadata indexed by URL, not individual cookies.
        We derive a unique identifier from the URL for database uniqueness.

        Cookie URLs in WebCache typically look like:
        - Cookie:user@domain.com/path
        - http://domain.com/path (for some entries)

        Args:
            url: The URL from the WebCache cookie container

        Returns:
            A descriptive identifier for the cookie metadata entry
        """
        if not url:
            return "unknown"

        # Handle Cookie: prefix format (common in WebCache)
        if url.lower().startswith("cookie:"):
            # Format: Cookie:user@domain.com/path
            cookie_part = url[7:]  # Strip "Cookie:"
            if "@" in cookie_part:
                # Extract domain part after @
                domain_path = cookie_part.split("@", 1)[1]
                return f"webcache:{domain_path[:50]}" if len(domain_path) > 50 else f"webcache:{domain_path}"
            return f"webcache:{cookie_part[:50]}" if len(cookie_part) > 50 else f"webcache:{cookie_part}"

        # For regular URLs, use domain + path hash for uniqueness
        try:
            parsed = urlparse(url)
            identifier = f"{parsed.netloc}{parsed.path}"
            return f"webcache:{identifier[:50]}" if len(identifier) > 50 else f"webcache:{identifier}"
        except Exception:
            # Fallback: use truncated URL
            return f"webcache:{url[:50]}" if len(url) > 50 else f"webcache:{url}"
