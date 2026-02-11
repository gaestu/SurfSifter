"""
Firefox Favicons Extractor

Extracts favicon icons from Firefox and Tor Browser.

Features:
- Firefox favicons.sqlite parsing (moz_icons, moz_pages_w_icons, moz_icons_to_pages)
- Legacy Firefox support (moz_favicons table for Firefox < 55)
- Multi-partition support via file_list discovery
- Schema warning support for unknown tables/columns
- Icon deduplication via SHA256 hashing
- Size guardrails (skip icons > 1MB)
- Page URL to icon mapping
- StatisticsCollector integration
- Clean URL fields (no hash fallback pollution)
- URL integration: icon URLs and page URLs added to urls table
- Image integration: icons >= 64px added to images table with pHash

Data Sources:
- favicons.sqlite: moz_icons (icon data), moz_pages_w_icons (URLs), moz_icons_to_pages (mapping)
- Legacy: moz_favicons table (Firefox < 55, ~2017)

Notes:
- fixed_icon_url_hash in moz_icons is a numeric hash, not a URL - we don't use it as fallback
- Legacy moz_favicons support enables forensic analysis of older Firefox profiles
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from .._patterns import (
    FIREFOX_BROWSERS,
    FIREFOX_ARTIFACTS,
    get_artifact_patterns,
    detect_browser_from_path,
    extract_profile_from_path,
)
from ....widgets import BrowserConfigWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from ._schemas import (
    KNOWN_FAVICONS_TABLES,
    FAVICONS_TABLE_PATTERNS,
)
from ._parsers import (
    parse_favicons_database,
    parse_page_mappings,
)
from extractors._shared.extraction_warnings import ExtractionWarningCollector
from core.logging import get_logger
from core.phash import compute_phash, compute_phash_prefix

LOGGER = get_logger("extractors.browser.firefox.favicons")

# Maximum icon size to store (1MB)
MAX_ICON_SIZE_BYTES = 1 * 1024 * 1024

# Minimum icon size for image table integration (64px - standard favicon size is 16px)
MIN_ICON_SIZE_FOR_IMAGES = 64


class FirefoxFaviconsExtractor(BaseExtractor):
    """
    Extract Firefox browser favicon icons from evidence images.

    Supports: Firefox, Firefox ESR, Tor Browser.

    Features:
    - Favicon icons with page mappings
    - Multi-partition support via file_list discovery
    - Schema warning support for unknown tables/columns
    - Icon deduplication via hashing
    - Size guardrails
    """

    SUPPORTED_BROWSERS = list(FIREFOX_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="firefox_favicons",
            display_name="Firefox Favicons",
            description="Extract favicon icons from Firefox browsers (Firefox, Firefox ESR, Tor)",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True
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
        return BrowserConfigWidget(parent, supported_browsers=self.SUPPORTED_BROWSERS)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            favicon_count = len(data.get("files", []))
            status_text = (
                f"Firefox Favicons\n"
                f"Databases: {favicon_count}\n"
                f"Run ID: {data.get('run_id', 'N/A')}"
            )
        else:
            status_text = "Firefox Favicons\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory."""
        return case_root / "evidences" / evidence_label / "firefox_favicons"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract favicon databases from evidence.

        Copies favicons.sqlite databases to workspace.
        Uses file_list discovery for multi-partition support.
        """
        callbacks.on_step("Initializing Firefox favicon extraction")

        run_id = self._generate_run_id()
        LOGGER.info("Starting Firefox favicons extraction (run_id=%s)", run_id)

        # Start statistics tracking
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")
        collector = self._get_statistics_collector()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "e01_context": self._get_e01_context(evidence_fs),
            "files": [],
            "status": "ok",
            "notes": [],
        }

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)

        callbacks.on_step("Scanning for favicon databases")

        # Discover favicon databases using file_list for multi-partition support
        files_by_partition = self._discover_via_file_list(
            evidence_fs, evidence_conn, evidence_id, browsers_to_search, callbacks
        )

        if not files_by_partition:
            # Fallback to filesystem scan if file_list is empty
            callbacks.on_log("No files in file_list, falling back to filesystem scan", "warning")
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            legacy_files = self._discover_and_copy_databases_legacy(
                evidence_fs, output_dir, browsers_to_search, callbacks
            )
            if legacy_files:
                files_by_partition = {partition_index: legacy_files}

        # Extract files from each partition
        total_files = sum(len(files) for files in files_by_partition.values())

        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=total_files)

        if not files_by_partition:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Firefox favicon databases found")
            LOGGER.info("No Firefox favicon databases found")
        else:
            # Get EWF paths for multi-partition extraction
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)

            file_index = 0
            for partition_index, file_list in files_by_partition.items():
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    break

                # Open partition for extraction
                try:
                    with open_partition_for_extraction(ewf_paths, partition_index) as part_fs:
                        for file_info in file_list:
                            if callbacks.is_cancelled():
                                break

                            file_index += 1
                            callbacks.on_progress(file_index, total_files, f"Copying {file_info['browser']} favicons")

                            try:
                                extracted = self._copy_database(
                                    part_fs, file_info, output_dir, callbacks
                                )
                                if extracted:
                                    manifest_data["files"].append(extracted)
                                    callbacks.on_log(
                                        f"Copied {file_info['browser']} favicons: {file_info['logical_path']}", "info"
                                    )
                            except Exception as e:
                                LOGGER.debug("Failed to copy %s: %s", file_info['logical_path'], e)

                except Exception as e:
                    LOGGER.error("Failed to open partition %d: %s", partition_index, e)
                    manifest_data["notes"].append(f"Failed to open partition {partition_index}: {e}")

        # Write manifest
        callbacks.on_step("Writing manifest")
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        # Record extracted files to audit table
        from extractors._shared.extracted_files_audit import record_browser_files
        record_browser_files(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            run_id=run_id,
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
            manifest_data=manifest_data,
            callbacks=callbacks,
        )

        LOGGER.info(
            "Firefox favicons extraction complete: %d favicon DBs",
            len(manifest_data["files"]),
        )

        # Finish statistics tracking (exactly once)
        if collector:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            collector.finish_run(evidence_id, self.metadata.name, status=status)

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
        Parse extracted databases and ingest into evidence database.
        """
        from core.database import (
            insert_favicon, insert_favicon_mappings,
            delete_favicons_by_run, insert_urls
        )

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"favicons": 0, "favicon_mappings": 0, "urls": 0, "images": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])
        evidence_label = config.get("evidence_label", "")

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Continue statistics tracking with same run_id
        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Handle empty case
        if not files:
            callbacks.on_log("No databases to ingest", "warning")
            if collector:
                collector.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0, favicons=0, favicon_mappings=0, urls=0, images=0
                )
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"favicons": 0, "favicon_mappings": 0, "urls": 0, "images": 0}

        # Clear previous data for this run
        delete_favicons_by_run(evidence_conn, evidence_id, run_id)

        favicon_count = 0
        mapping_count = 0
        url_count = 0
        image_count = 0
        failed_files = 0

        callbacks.on_progress(0, len(files), "Processing databases")

        for i, file_info in enumerate(files):
            if callbacks.is_cancelled():
                break

            local_path = file_info.get("local_path")
            if not local_path:
                continue

            db_path = Path(local_path)
            if not db_path.exists():
                callbacks.on_log(f"Database not found: {db_path}", "warning")
                failed_files += 1
                if collector:
                    collector.report_failed(evidence_id, self.metadata.name, files=1)
                continue

            browser = file_info.get("browser")
            profile = file_info.get("profile")

            callbacks.on_progress(i + 1, len(files), f"Processing {browser} favicons")

            try:
                fc, mc, uc, ic = self._ingest_favicons(
                    db_path, evidence_conn, evidence_id, run_id,
                    browser, profile, file_info, callbacks, output_dir,
                    warning_collector=warning_collector,
                )
                favicon_count += fc
                mapping_count += mc
                url_count += uc
                image_count += ic

            except Exception as e:
                error_msg = f"Failed to ingest favicons from {db_path}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_log(error_msg, "error")
                failed_files += 1
                if collector:
                    collector.report_failed(evidence_id, self.metadata.name, files=1)

        # Flush collected warnings to database before commit
        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)

        evidence_conn.commit()

        # Report ingested counts and finish
        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=favicon_count + mapping_count + url_count + image_count,
                favicons=favicon_count,
                favicon_mappings=mapping_count,
                urls=url_count,
                images=image_count
            )
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        return {
            "favicons": favicon_count,
            "favicon_mappings": mapping_count,
            "urls": url_count,
            "images": image_count
        }

    # =========================================================================
    # Discovery Methods
    # =========================================================================

    def _discover_via_file_list(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, List[Dict]]:
        """
        Discover favicon databases using file_list for multi-partition support.

        Returns dict mapping partition_index -> list of file info dicts.
        """
        if evidence_conn is None:
            return {}

        # Build path patterns for Firefox browsers
        path_patterns = []
        for browser in browsers:
            if browser not in FIREFOX_BROWSERS:
                continue
            browser_info = FIREFOX_BROWSERS[browser]
            for root in browser_info.get("profile_roots", []):
                if root:  # Skip empty roots (like firefox_esr)
                    # Convert to SQL LIKE pattern
                    path_patterns.append(f"%{root.replace('*', '%')}%")

        if not path_patterns:
            return {}

        # Remove duplicates
        path_patterns = list(set(path_patterns))

        # Query file_list for favicons.sqlite files
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["favicons.sqlite"],
            path_patterns=path_patterns if path_patterns else None,
        )

        if result.is_empty:
            callbacks.on_log(
                "No favicons.sqlite files found in file_list",
                "info"
            )
            return {}

        if result.is_multi_partition:
            callbacks.on_log(
                f"Found favicons.sqlite on {len(result.partitions_with_matches)} partitions: {result.partitions_with_matches}",
                "info"
            )

        # Convert FileListMatch objects to extractor's expected format
        files_by_partition: Dict[int, List[Dict]] = {}

        for partition_index, matches in result.matches_by_partition.items():
            files_list = []
            for match in matches:
                # Detect browser from path
                browser = detect_browser_from_path(match.file_path)
                if browser and browser not in browsers:
                    continue  # Skip if browser not in selection

                profile = extract_profile_from_path(match.file_path)

                files_list.append({
                    "logical_path": match.file_path,
                    "browser": browser or "firefox",
                    "profile": profile,
                    "artifact_type": "favicons",
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

            if files_list:
                files_by_partition[partition_index] = files_list

        return files_by_partition

    def _discover_and_copy_databases_legacy(
        self,
        evidence_fs,
        output_dir: Path,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """
        Legacy discovery method using filesystem iteration.

        Used as fallback when file_list is empty.
        """
        copied_files = []

        for browser in browsers:
            if browser not in FIREFOX_BROWSERS:
                continue

            try:
                patterns = get_artifact_patterns(browser, "favicons")
            except ValueError:
                continue

            if not patterns:
                continue

            for pattern in patterns:
                # Skip journal/wal files for separate handling
                if "-wal" in pattern or "-shm" in pattern:
                    continue

                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        # Skip journal/wal files for separate tracking
                        if "-journal" in path_str or "-wal" in path_str or "-shm" in path_str:
                            continue

                        try:
                            content = evidence_fs.read_file(path_str)
                            profile = extract_profile_from_path(path_str)
                            partition_index = getattr(evidence_fs, 'partition_index', 0)

                            file_info = {
                                "logical_path": path_str,
                                "browser": browser,
                                "profile": profile,
                                "artifact_type": "favicons",
                                "partition_index": partition_index,
                                "content": content,  # Pre-read for legacy path
                            }

                            # Copy directly since we have content
                            extracted = self._copy_database_from_content(
                                file_info, content, output_dir, callbacks
                            )
                            if extracted:
                                copied_files.append(extracted)
                                callbacks.on_log(
                                    f"Copied {browser} favicons: {path_str}", "info"
                                )
                        except Exception as e:
                            LOGGER.debug("Failed to copy %s: %s", path_str, e)

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return copied_files

    # =========================================================================
    # File Copy Methods
    # =========================================================================

    def _copy_database(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Optional[Dict]:
        """Copy a single database file and its journal files."""
        try:
            source_path = file_info["logical_path"]
            content = evidence_fs.read_file(source_path)
            return self._copy_database_from_content(file_info, content, output_dir, callbacks)
        except Exception as e:
            LOGGER.error("Failed to copy %s: %s", file_info.get("logical_path"), e)
            return None

    def _copy_database_from_content(
        self,
        file_info: Dict,
        content: bytes,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Optional[Dict]:
        """Copy database content to output directory with collision-safe naming."""
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info["profile"]
            partition_index = file_info.get("partition_index", 0)

            # Calculate hashes
            md5 = hashlib.md5(content).hexdigest()
            sha256 = hashlib.sha256(content).hexdigest()

            # Generate safe filename with hash prefix to prevent collisions
            safe_browser = re.sub(r'[^a-zA-Z0-9_-]', '_', browser)
            safe_profile = re.sub(r'[^a-zA-Z0-9_-]', '_', profile)
            original_name = Path(source_path).name
            content_hash = sha256[:8]

            # Format: {browser}_{profile}_{hash}_{original_name}
            filename = f"{safe_browser}_{safe_profile}_{content_hash}_{original_name}"

            dest_path = output_dir / filename
            dest_path.write_bytes(content)

            return {
                "local_path": str(dest_path),
                "source_path": source_path,
                "logical_path": source_path,
                "browser": browser,
                "profile": profile,
                "artifact_type": "favicons",
                "md5": md5,
                "sha256": sha256,
                "size_bytes": len(content),
                "partition_index": partition_index,
            }

        except Exception as e:
            LOGGER.error("Failed to copy database: %s", e)
            return None

    # =========================================================================
    # Ingestion Methods
    # =========================================================================

    def _ingest_favicons(
        self,
        db_path: Path,
        evidence_conn,
        evidence_id: int,
        run_id: str,
        browser: str,
        profile: str,
        file_info: Dict,
        callbacks: ExtractorCallbacks,
        output_dir: Path,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Tuple[int, int, int, int]:
        """Ingest favicon data from a favicons.sqlite database.

        Returns:
            Tuple of (favicon_count, mapping_count, url_count, image_count)
        """
        from core.database import insert_favicon, insert_favicon_mappings, insert_urls

        favicon_count = 0
        mapping_count = 0
        url_count = 0
        image_count = 0

        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row

            source_file = file_info.get("source_path", str(db_path))

            # Parse database using new parser module
            icons, page_mappings, is_legacy = parse_favicons_database(
                conn, source_file, warning_collector=warning_collector
            )

            conn.close()

            if not icons:
                return 0, 0, 0, 0

            # Process icons
            discovered_by = f"firefox_favicons:{self.metadata.version}"
            collected_urls: List[Dict[str, Any]] = []
            icon_id_map: Dict[int, int] = {}  # original_id -> favicon_id

            for icon in icons:
                favicon_id = insert_favicon(
                    evidence_conn, evidence_id,
                    browser=browser,
                    icon_url=icon["icon_url"],
                    profile=profile,
                    icon_type=icon["icon_type"],
                    width=icon["width"],
                    height=None,
                    icon_md5=icon["icon_md5"],
                    icon_sha256=icon["icon_sha256"],
                    run_id=run_id,
                    source_path=file_info.get("source_path"),
                    partition_index=file_info.get("partition_index"),
                    fs_type=file_info.get("fs_type"),
                    logical_path=file_info.get("source_path"),
                    forensic_path=file_info.get("source_path"),
                    notes=f"Legacy schema" if is_legacy else None,
                )

                if favicon_id:
                    favicon_count += 1
                    icon_id_map[icon["id"]] = favicon_id

                    # Collect icon URL for batch insert
                    if icon["icon_url"]:
                        collected_urls.append(self._make_url_record(
                            url=icon["icon_url"],
                            discovered_by=discovered_by,
                            run_id=run_id,
                            source_path=file_info.get("source_path"),
                            context=f"favicon_icon:{browser}:{profile}",
                        ))

                    # Add large icons to images table
                    if icon["width"] and icon["width"] >= MIN_ICON_SIZE_FOR_IMAGES:
                        image_count += self._insert_icon_as_image(
                            evidence_conn=evidence_conn,
                            evidence_id=evidence_id,
                            icon_data=icon["data"],
                            icon_sha256=icon["icon_sha256"],
                            icon_md5=icon["icon_md5"],
                            icon_url=icon["icon_url"],
                            run_id=run_id,
                            file_info=file_info,
                            browser=browser,
                            profile=profile,
                            output_dir=output_dir,
                        )

            # Process page mappings
            if page_mappings and icon_id_map:
                mappings_to_insert = []
                for mapping in page_mappings:
                    original_icon_id = mapping["icon_id"]
                    if original_icon_id in icon_id_map:
                        mappings_to_insert.append({
                            "favicon_id": icon_id_map[original_icon_id],
                            "page_url": mapping["page_url"],
                            "browser": browser,
                            "profile": profile,
                            "run_id": run_id,
                        })

                        # Collect page URL for batch insert
                        if mapping["page_url"]:
                            collected_urls.append(self._make_url_record(
                                url=mapping["page_url"],
                                discovered_by=discovered_by,
                                run_id=run_id,
                                source_path=file_info.get("source_path"),
                                context=f"favicon_page:{browser}:{profile}",
                            ))

                if mappings_to_insert:
                    mapping_count = insert_favicon_mappings(evidence_conn, evidence_id, mappings_to_insert)

            # Batch insert all URLs
            if collected_urls:
                try:
                    insert_urls(evidence_conn, evidence_id, collected_urls)
                    url_count = len(collected_urls)
                    LOGGER.info("Inserted %d URLs from favicons", url_count)
                except Exception as e:
                    LOGGER.debug("Error inserting favicon URLs: %s", e)

        except Exception as e:
            LOGGER.error("Failed to parse favicons from %s: %s", db_path, e)

        return favicon_count, mapping_count, url_count, image_count

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_statistics_collector(self):
        """Get StatisticsCollector instance if available."""
        try:
            from core.statistics_collector import StatisticsCollector
            return StatisticsCollector.get_instance()
        except ImportError:
            return None

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context."""
        try:
            source_path = evidence_fs.source_path if hasattr(evidence_fs, 'source_path') else None
            if source_path is not None and not isinstance(source_path, (str, Path)):
                source_path = None

            fs_type = getattr(evidence_fs, 'fs_type', "unknown")
            if not isinstance(fs_type, str):
                fs_type = "unknown"

            return {
                "image_path": str(source_path) if source_path else None,
                "fs_type": fs_type,
            }
        except Exception:
            return {"image_path": None, "fs_type": "unknown"}

    def _make_url_record(
        self,
        url: str,
        discovered_by: str,
        run_id: str,
        source_path: Optional[str],
        context: str,
    ) -> Dict[str, Any]:
        """Create a URL record for insertion into urls table.

        Note: first_seen_utc is set to None because Firefox favicons.sqlite
        does not store creation/visit timestamps for favicon URLs.
        The expire_ms field is an expiration time, not a first-seen time.
        """
        parsed = urlparse(url)
        return {
            "url": url,
            "domain": parsed.netloc or None,
            "scheme": parsed.scheme or None,
            "discovered_by": discovered_by,
            "run_id": run_id,
            "source_path": source_path,
            "context": context,
            "first_seen_utc": None,  # No creation timestamp in favicons.sqlite
        }

    def _insert_icon_as_image(
        self,
        evidence_conn,
        evidence_id: int,
        icon_data: bytes,
        icon_sha256: str,
        icon_md5: str,
        icon_url: Optional[str],
        run_id: str,
        file_info: Dict,
        browser: str,
        profile: str,
        output_dir: Path,
    ) -> int:
        """Insert icon as image with discovery record, write to disk, and generate thumbnail.

        Returns:
            1 if inserted/enriched, 0 on error
        """
        from core.database import insert_image_with_discovery
        from extractors._shared.carving.exif import generate_thumbnail

        try:
            # Generate paths for the image file
            # rel_path format: favicons/<browser>/<hash_prefix>/<sha256>.<ext>
            # Determine extension from magic bytes
            ext = self._detect_image_extension(icon_data)
            rel_path = f"favicons/{browser}/{icon_sha256[:2]}/{icon_sha256}.{ext}"
            filename = f"{icon_sha256}.{ext}"

            # Check if file already exists before writing
            image_dir = output_dir / "favicons" / browser / icon_sha256[:2]
            image_path = image_dir / filename

            # Compute pHash from icon bytes
            phash = compute_phash(io.BytesIO(icon_data))
            phash_prefix = compute_phash_prefix(phash)

            image_data = {
                "rel_path": rel_path,
                "filename": filename,
                "md5": icon_md5,
                "sha256": icon_sha256,
                "phash": phash,
                "phash_prefix": phash_prefix,
                "size_bytes": len(icon_data),
                "notes": f"Favicon from {browser}/{profile}",
            }

            discovery_data = {
                "discovered_by": "firefox_favicons",
                "run_id": run_id,
                "extractor_version": self.metadata.version,
                "source_path": file_info.get("source_path"),
                "cache_url": icon_url,  # Store icon URL as cache_url for provenance
            }

            image_id, was_inserted = insert_image_with_discovery(
                evidence_conn, evidence_id, image_data, discovery_data
            )

            # Only write file if this is a new image (not already on disk)
            if was_inserted and not image_path.exists():
                image_dir.mkdir(parents=True, exist_ok=True)
                image_path.write_bytes(icon_data)

                # Generate thumbnail for formats PIL can handle (not SVG or unknown)
                if ext not in ("svg", "bin"):
                    thumb_dir = output_dir / "thumbnails" / "favicons" / browser / icon_sha256[:2]
                    thumb_dir.mkdir(parents=True, exist_ok=True)
                    thumb_path = thumb_dir / f"{icon_sha256}_thumb.jpg"
                    if not thumb_path.exists():
                        generate_thumbnail(image_path, thumb_path)

                LOGGER.debug("Inserted favicon as image id=%d (sha256=%s)", image_id, icon_sha256[:8])
            else:
                LOGGER.debug("Enriched existing image id=%d with favicon discovery", image_id)

            return 1

        except Exception as e:
            LOGGER.debug("Error inserting icon as image: %s", e)
            return 0

    def _detect_image_extension(self, data: bytes) -> str:
        """Detect image format from magic bytes and return appropriate extension."""
        if len(data) < 8:
            return "ico"

        # PNG: 89 50 4E 47 0D 0A 1A 0A
        if data[:8] == b'\x89PNG\r\n\x1a\n':
            return "png"
        # JPEG: FF D8 FF
        if data[:3] == b'\xff\xd8\xff':
            return "jpg"
        # GIF: GIF87a or GIF89a
        if data[:6] in (b'GIF87a', b'GIF89a'):
            return "gif"
        # WebP: RIFF....WEBP
        if data[:4] == b'RIFF' and data[8:12] == b'WEBP':
            return "webp"
        # BMP: BM
        if data[:2] == b'BM':
            return "bmp"
        # ICO: 00 00 01 00
        if data[:4] == b'\x00\x00\x01\x00':
            return "ico"
        # SVG: starts with <svg or <?xml (check first 100 bytes for svg tag)
        text_start = data[:100].lower()
        if b'<svg' in text_start or (b'<?xml' in text_start and b'svg' in text_start):
            return "svg"

        # Default to bin for unknown binary data
        return "bin"
