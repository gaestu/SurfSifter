"""
Chromium Bookmarks Extractor

Extracts browser bookmarks from all Chromium-based browsers (Chrome, Edge, Brave, Opera).
Uses local parser with schema discovery support.

Features:
- Chromium-only (Firefox/Safari excluded - use FirefoxBookmarksExtractor)
- JSON format parsing with recursive folder traversal
- Folder hierarchy path reconstruction
- Schema warning support for unknown JSON keys/types
- StatisticsCollector integration for run tracking
- Browser selection config widget (Chromium browsers only)

Bookmark Schema:
- id, name, url (None for folders)
- date_added, date_modified, date_last_used (WebKit timestamps)
- bookmark_type (url or folder)
- folder_path (e.g., "Bookmarks Bar/Tech/Dev")
- guid (unique identifier)

Added schema warning support for unknown keys detection
Initial release
"""

from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import BrowserConfigWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from ...._shared.extraction_warnings import ExtractionWarningCollector
from .._patterns import (
    CHROMIUM_BROWSERS,
    get_patterns,
    get_browser_display_name,
    get_all_browsers,
)
from .._parsers import (
    extract_profile_from_path,
    detect_browser_from_path,
)
# Local parser with schema warning support
from ._parser import parse_bookmarks_json, get_bookmark_stats

from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.browser.chromium.bookmarks")


class ChromiumBookmarksExtractor(BaseExtractor):
    """
    Extract bookmark files from Chromium-based browsers.

    Supports Chrome, Edge, Brave, Opera. All use identical JSON format.
    Firefox and Safari are handled by separate family extractors.

    Dual-phase workflow:
    - Extraction: Scans filesystem, copies Bookmarks JSON files to workspace
    - Ingestion: Parses JSON files, inserts with forensic fields

    Features:
    - Recursive folder structure parsing
    - Folder path reconstruction
    - WebKit timestamp conversion to ISO 8601
    - StatisticsCollector integration for run tracking
    - Browser selection config widget
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="chromium_bookmarks",
            display_name="Chromium Bookmarks",
            description="Extract browser bookmarks from Chrome, Edge, Brave, Opera",
            category="browser",
            requires_tools=[],  # Pure Python, no external tools
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted"
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run (manifest exists)."""
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """
        Return configuration widget (browser selection + multi-partition).

        Uses BrowserConfigWidget with Chromium browsers and multi-partition option.
        """
        return BrowserConfigWidget(
            parent,
            supported_browsers=get_all_browsers(),
            default_scan_all_partitions=True,
        )

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
            status_text = f"Chromium Bookmarks\nFiles: {file_count}\nRun: {data.get('run_id', 'N/A')[:20]}"
        else:
            status_text = "Chromium Bookmarks\nNo extraction yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "chromium_bookmarks"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Chromium bookmark files from evidence.

        Workflow:
            1. Generate run_id
            2. Scan evidence for Chromium bookmark files
            3. Copy matching files to output_dir/
            4. Calculate hashes, collect E01 context
            5. Write manifest.json
        """
        callbacks.on_step("Initializing Chromium bookmarks extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting Chromium bookmarks extraction (run_id=%s)", run_id)

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        # Start statistics tracking
        stats_collector = StatisticsCollector.instance()
        if stats_collector:
            stats_collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

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
            "files": [],
            "status": "ok",
            "notes": [],
        }

        # Determine which browsers to search
        browsers = config.get("browsers") or config.get("selected_browsers") or get_all_browsers()

        # Scan for bookmark files
        callbacks.on_step("Scanning for Chromium bookmark files")
        bookmark_files = self._discover_files(evidence_fs, browsers, callbacks)

        # Report discovered files
        if stats_collector:
            stats_collector.report_discovered(evidence_id, self.metadata.name, files=len(bookmark_files))

        callbacks.on_log(f"Found {len(bookmark_files)} bookmark file(s)")

        # Extract each file
        for i, (source_path, browser) in enumerate(bookmark_files):
            if callbacks.is_cancelled():
                manifest_data["status"] = "cancelled"
                manifest_data["notes"].append("Extraction cancelled by user")
                break

            callbacks.on_progress(i + 1, len(bookmark_files), f"Extracting {source_path}")

            try:
                file_info = self._extract_file(
                    evidence_fs, source_path, output_dir, browser, run_id
                )
                manifest_data["files"].append(file_info)
            except Exception as e:
                LOGGER.warning("Failed to extract %s: %s", source_path, e)
                manifest_data["notes"].append(f"Failed: {source_path}: {e}")
                if stats_collector:
                    stats_collector.report_failed(evidence_id, self.metadata.name, files=1)

        # Finish statistics
        if stats_collector:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            stats_collector.finish_run(evidence_id, self.metadata.name, status=status)

        # Write manifest
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

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

        callbacks.on_step("Extraction complete")
        return manifest_data["status"] == "ok"

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, Any]:
        """
        Ingest extracted bookmark files into evidence database.

        Workflow:
            1. Load manifest.json
            2. Create warning collector for schema discovery
            3. For each extracted file:
               - Parse JSON file with warning collector
               - Insert bookmarks with forensic context
            4. Flush warnings to database
            5. Return summary statistics
        """
        callbacks.on_step("Loading manifest")

        manifest_path = output_dir / "manifest.json"
        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", "unknown")
        evidence_label = config.get("evidence_label", "")

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Continue statistics tracking
        stats_collector = StatisticsCollector.instance()
        if stats_collector:
            stats_collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        total_bookmarks = 0
        total_urls = 0
        total_folders = 0
        failed_files = 0

        files = manifest_data.get("files", [])

        try:
            for i, file_info in enumerate(files):
                if callbacks.is_cancelled():
                    break

                local_path = output_dir / file_info.get("local_filename", "")
                if not local_path.exists():
                    callbacks.on_log(f"File not found: {local_path}", level="warning")
                    failed_files += 1
                    continue

                callbacks.on_progress(i + 1, len(files), f"Parsing {local_path.name}")

                try:
                    counts = self._ingest_file(
                        local_path, evidence_conn, evidence_id, file_info, run_id, callbacks,
                        warning_collector=warning_collector,
                    )
                    total_bookmarks += counts["total"]
                    total_urls += counts["urls"]
                    total_folders += counts["folders"]
                except Exception as e:
                    LOGGER.warning("Failed to ingest %s: %s", local_path, e)
                    callbacks.on_log(f"Failed to parse {local_path.name}: {e}", level="warning")
                    failed_files += 1
                    if stats_collector:
                        stats_collector.report_failed(evidence_id, self.metadata.name, files=1)
        finally:
            # Always flush warnings to database, even on error
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)

        # Report ingestion stats
        if stats_collector:
            stats_collector.report_ingested(
                evidence_id, self.metadata.name,
                records=total_bookmarks,
                bookmarks=total_urls,
            )
            status = "success" if failed_files == 0 else "partial"
            stats_collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_step("Ingestion complete")

        return {
            "bookmarks": total_bookmarks,
            "urls": total_urls,
            "folders": total_folders,
            "failed_files": failed_files,
            "warnings": warning_count,
        }

    # =========================================================================
    # Private helpers
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate unique run ID: timestamp + UUID4 prefix."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        return f"{ts}_{uid}"

    def _get_tool_version(self) -> str:
        """Get tool version string."""
        return f"{self.metadata.name} v{self.metadata.version}"

    def _get_e01_context(self, evidence_fs) -> Dict[str, Any]:
        """Extract E01 image context if available."""
        context = {"type": "unknown"}

        try:
            if hasattr(evidence_fs, "ewf_handle"):
                context["type"] = "ewf"
                try:
                    handle = evidence_fs.ewf_handle
                    if hasattr(handle, "get_media_size"):
                        media_size = handle.get_media_size()
                        if isinstance(media_size, int):
                            context["media_size"] = media_size
                except Exception:
                    pass
            elif hasattr(evidence_fs, "mount_point"):
                mount_point = getattr(evidence_fs, "mount_point", None)
                if isinstance(mount_point, (str, Path)):
                    context["type"] = "mounted"
                    context["mount_point"] = str(mount_point)
        except Exception:
            pass

        return context

    def _discover_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[tuple]:
        """
        Discover bookmark files in evidence filesystem.

        Returns:
            List of (source_path, browser) tuples
        """
        found_files = []

        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                continue

            patterns = get_patterns(browser, "bookmarks")

            for pattern in patterns:
                try:
                    matches = list(evidence_fs.iter_paths(pattern))
                    for match in matches:
                        found_files.append((match, browser))
                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return found_files

    def _extract_file(
        self,
        evidence_fs,
        source_path: str,
        output_dir: Path,
        browser: str,
        run_id: str
    ) -> Dict[str, Any]:
        """
        Extract a single bookmark file from evidence.

        Returns:
            File info dict for manifest
        """
        # Generate safe local filename
        safe_name = source_path.replace("/", "_").replace("\\", "_")
        local_filename = f"{browser}_{safe_name}"
        local_path = output_dir / local_filename

        # Copy file from evidence using EvidenceFS API
        content = evidence_fs.read_file(source_path)
        local_path.write_bytes(content)

        # Calculate hash
        md5_hash = hashlib.md5(content).hexdigest()
        sha256_hash = hashlib.sha256(content).hexdigest()

        # Get profile from path
        profile = extract_profile_from_path(source_path)

        return {
            "source_path": source_path,
            "local_filename": local_filename,
            "browser": browser,
            "profile": profile,
            "md5": md5_hash,
            "sha256": sha256_hash,
            "size_bytes": len(content),
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

    def _ingest_file(
        self,
        local_path: Path,
        evidence_conn,
        evidence_id: int,
        file_info: Dict[str, Any],
        run_id: str,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, int]:
        """
        Ingest a single bookmark JSON file.

        Args:
            local_path: Path to extracted Bookmarks JSON file
            evidence_conn: Database connection
            evidence_id: Evidence ID
            file_info: File metadata from manifest
            run_id: Extraction run ID
            callbacks: Extractor callbacks
            warning_collector: Optional warning collector for schema discovery

        Returns:
            Dict with counts: total, urls, folders, urls_table
        """
        from urllib.parse import urlparse
        from core.database import insert_bookmark_row, insert_urls

        browser = file_info.get("browser", "unknown")
        profile = file_info.get("profile", "Default")
        source_path = file_info.get("source_path", "")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        # Parse JSON with error handling for warnings
        try:
            with open(local_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            # Report JSON parse error to warning collector
            if warning_collector:
                warning_collector.add_json_parse_error(
                    filename=source_path or str(local_path),
                    error=str(e),
                )
            raise  # Re-raise to be handled by caller

        counts = {"total": 0, "urls": 0, "folders": 0, "urls_table": 0}
        url_records = []  # Collect URLs for unified urls table

        # Parse bookmarks with schema discovery
        for bookmark in parse_bookmarks_json(
            data,
            warning_collector=warning_collector,
            source_file=source_path,
        ):
            try:
                insert_bookmark_row(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=browser,
                    url=bookmark.url or "",  # Folders have no URL, use empty string
                    profile=profile,
                    title=bookmark.name,
                    date_added_utc=bookmark.date_added_iso,
                    date_modified_utc=bookmark.date_modified_iso,
                    bookmark_type=bookmark.bookmark_type,
                    folder_path=bookmark.folder_path,
                    guid=bookmark.guid,
                    source_path=source_path,
                    discovered_by=discovered_by,
                    run_id=run_id,
                )
                counts["total"] += 1
                if bookmark.bookmark_type == "url":
                    counts["urls"] += 1
                    # Collect URL for unified urls table
                    if bookmark.url and not bookmark.url.startswith(("javascript:", "data:")):
                        parsed = urlparse(bookmark.url)
                        url_records.append({
                            "url": bookmark.url,
                            "domain": parsed.netloc or None,
                            "scheme": parsed.scheme or None,
                            "discovered_by": discovered_by,
                            "run_id": run_id,
                            "source_path": source_path,
                            "context": f"bookmark:{browser}:{profile}",
                            "first_seen_utc": bookmark.date_added_iso,
                        })
                elif bookmark.bookmark_type == "folder":
                    counts["folders"] += 1
            except Exception as e:
                LOGGER.debug("Failed to insert bookmark: %s", e)

        # Cross-post URLs to unified urls table for analysis
        if url_records:
            try:
                insert_urls(evidence_conn, evidence_id, url_records)
                counts["urls_table"] = len(url_records)
                LOGGER.debug("Cross-posted %d bookmark URLs to urls table", len(url_records))
            except Exception as e:
                LOGGER.debug("Failed to cross-post bookmark URLs: %s", e)

        return counts
