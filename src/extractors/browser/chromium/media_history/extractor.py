"""
Media History Extractor

Extracts and ingests browser media playback history from Chromium browsers
with full forensic provenance.

Features:
- Chromium Media History SQLite database parsing
- Playback history with watch time tracking
- Media session metadata (title, artist, album)
- Video/audio content detection
- Multi-partition extraction support
- Schema warning support for unknown tables/columns
- Album art extraction to shared images table
- Origin table parsing

Data Formats:
- Chromium (Chrome, Edge, Opera, Brave):
  - Media History: playback, playbackSession, origin, mediaImage tables
  - WebKit timestamps (microseconds since 1601-01-01)
- Firefox:
  - No centralized media history database (uses session storage)

Multi-partition, schema warnings, album art, origins, no URL dedup
Initial release
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from .._patterns import (
    CHROMIUM_BROWSERS,
    get_patterns,
    get_all_browsers,
    get_browser_display_name,
)
from .._parsers import (
    extract_profile_from_path,
    detect_browser_from_path,
)
from ....widgets import BrowserConfigWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    get_ewf_paths_from_evidence_fs,
)
from ...._shared.extraction_warnings import (
    ExtractionWarningCollector,
    discover_unknown_tables,
    discover_unknown_columns,
)
from ._schemas import (
    KNOWN_MEDIA_HISTORY_TABLES,
    MEDIA_HISTORY_TABLE_PATTERNS,
    KNOWN_COLUMNS_BY_TABLE,
)
from core.logging import get_logger
from core.database import (
    insert_media_playbacks,
    insert_media_sessions,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    delete_media_by_run,
    insert_urls,
    insert_image_with_discovery,
    delete_discoveries_by_run,
)
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.media_history")


class MediaHistoryExtractor(BaseExtractor):
    """
    Extract browser media playback history from evidence images.

    Dual-helper strategy:
    - Extraction: Scans filesystem, copies Media History SQLite files
    - Ingestion: Parses media databases, inserts with forensic fields

    Supports all Chromium-based browsers including beta/dev/canary channels:
    - Chrome, Chrome Beta, Chrome Dev, Chrome Canary
    - Chromium (open-source)
    - Edge, Edge Beta, Edge Dev, Edge Canary
    - Brave, Brave Beta, Brave Nightly
    - Opera, Opera GX

    Note: Firefox stores media state in session storage, not a dedicated DB.
    """

    # All Chromium browsers support Media History database
    SUPPORTED_BROWSERS = get_all_browsers()

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="media_history",
            display_name="Chromium Media History",
            description="Extract browser media playback history (videos, audio, watch time, album art)",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted. Please mount E01 image first."
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
        """Return configuration widget (browser selection + multi-partition)."""
        return BrowserConfigWidget(
            parent,
            supported_browsers=self.SUPPORTED_BROWSERS,
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
            status_text = f"Media History\nFiles extracted: {file_count}\nRun ID: {data.get('run_id', 'N/A')}"
        else:
            status_text = "Media History\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "media_history"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract browser media history files from evidence.
        """
        callbacks.on_step("Initializing media history extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        LOGGER.info("Starting media history extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Get configuration
        evidence_conn = config.get("evidence_conn")
        scan_all_partitions = config.get("scan_all_partitions", True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_extraction_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "multi_partition_extraction": scan_all_partitions,
            "partitions_scanned": [],
            "partitions_with_artifacts": [],
            "files": [],
            "status": "ok",
            "notes": [],
        }

        callbacks.on_step("Scanning for media history files")

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)

        # Discover files - use multi-partition if enabled and evidence_conn available
        files_by_partition: Dict[int, List[Dict]] = {}

        if scan_all_partitions and evidence_conn is not None:
            # Multi-partition discovery via file_list
            files_by_partition = self._discover_files_multi_partition(
                evidence_fs, evidence_conn, evidence_id, browsers_to_search, callbacks
            )
        else:
            # Single partition fallback
            if scan_all_partitions and evidence_conn is None:
                callbacks.on_log(
                    "Multi-partition scan requested but no evidence_conn provided, using single partition",
                    "warning"
                )
            media_files = self._discover_media_files(evidence_fs, browsers_to_search, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if media_files:
                files_by_partition[partition_index] = media_files

        # Flatten for counting
        all_media_files = []
        for files_list in files_by_partition.values():
            all_media_files.extend(files_list)

        # Update manifest with partition info
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = sorted(
            p for p, files in files_by_partition.items() if files
        )

        # Report discovered files (always, even if 0)
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(all_media_files))

        callbacks.on_log(
            f"Found {len(all_media_files)} media history file(s) across {len(files_by_partition)} partition(s)"
        )

        if not all_media_files:
            LOGGER.info("No media history files found")
        else:
            callbacks.on_progress(0, len(all_media_files), "Copying media history files")

            # Get EWF paths for opening other partitions
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            file_index = 0

            # Process each partition
            for partition_index in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_index]

                # Determine which filesystem to use
                current_partition = getattr(evidence_fs, 'partition_index', 0)

                if partition_index == current_partition or ewf_paths is None:
                    # Use existing filesystem handle
                    fs_to_use = evidence_fs
                    need_close = False
                else:
                    # Open partition-specific filesystem
                    try:
                        from core.evidence_fs import open_ewf_partition
                        fs_to_use = open_ewf_partition(ewf_paths, partition_index=partition_index)
                        need_close = True
                        callbacks.on_log(f"Opened partition {partition_index} for extraction", "info")
                    except Exception as e:
                        callbacks.on_log(
                            f"Failed to open partition {partition_index}: {e}",
                            "error"
                        )
                        manifest_data["notes"].append(f"Failed to open partition {partition_index}: {e}")
                        continue

                try:
                    for file_info in partition_files:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            manifest_data["notes"].append("Extraction cancelled by user")
                            break

                        try:
                            callbacks.on_progress(
                                file_index + 1, len(all_media_files),
                                f"Copying {file_info['browser']} media history (partition {partition_index})"
                            )

                            extracted_file = self._extract_file(
                                fs_to_use,
                                file_info,
                                output_dir,
                                partition_index,
                                callbacks,
                            )
                            manifest_data["files"].append(extracted_file)
                            file_index += 1

                        except Exception as e:
                            error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            manifest_data["notes"].append(error_msg)
                            if stats:
                                stats.report_failed(evidence_id, self.metadata.name, files=1)
                            file_index += 1

                    if callbacks.is_cancelled():
                        break

                finally:
                    # Close partition handle if we opened it
                    if need_close and fs_to_use is not None:
                        try:
                            close_method = getattr(fs_to_use, 'close', None)
                            if close_method and callable(close_method):
                                close_method()
                        except Exception as e:
                            LOGGER.debug("Error closing partition %d handle: %s", partition_index, e)

        # Finish statistics (once, at the end)
        if stats:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_step("Writing manifest")
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

        LOGGER.info(
            "Media history extraction complete: %d files from %d partition(s), status=%s",
            len(manifest_data["files"]),
            len(manifest_data.get("partitions_with_artifacts", [1])),
            manifest_data["status"],
        )

        callbacks.on_step("Extraction complete")
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
        Parse extracted manifest and ingest into database.

        Workflow:
            1. Load manifest.json
            2. Clean up previous album art directory (unconditional)
            3. Delete previous run data (if re-running)
            4. Create schema warning collector
            5. For each extracted file:
               - Parse SQLite database (playback, session tables)
               - Detect unknown tables and columns
               - Parse mediaImage table (extract album art)
               - Parse origin table
               - Cross-post URLs to unified urls table
            6. Flush schema warnings
            7. Return summary statistics
        """
        callbacks.on_step("Reading manifest")

        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"playback": 0, "sessions": 0, "origins": 0, "images": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])
        evidence_label = config.get("evidence_label", "")

        # Continue statistics tracking (same run_id from manifest)
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Always clean up previous album art directory for fresh extraction
        album_art_dir = output_dir / "album_art"
        if album_art_dir.exists():
            shutil.rmtree(album_art_dir)
            callbacks.on_log("Cleaned previous album art directory", "info")

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0,
                    playbacks=0,
                )
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"playback": 0, "sessions": 0, "origins": 0, "images": 0}

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        total_playback = 0
        total_sessions = 0
        total_origins = 0
        total_images = 0

        # Clear previous data for this run
        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        # Also clear previous image discoveries from this run
        try:
            delete_discoveries_by_run(evidence_conn, evidence_id, run_id)
        except Exception as e:
            LOGGER.debug("No previous image discoveries to delete: %s", e)

        callbacks.on_progress(0, len(files), "Parsing media history files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            if file_entry.get("copy_status") == "error":
                callbacks.on_log(f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}", "warning")
                continue

            callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry['browser']} media history")

            try:
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry["browser"],
                    artifact_type="media_history",
                    run_id=run_id,
                    extracted_path=file_entry["extracted_path"],
                    extraction_status="ok",
                    extraction_timestamp_utc=manifest_data["extraction_timestamp_utc"],
                    logical_path=file_entry["logical_path"],
                    profile=file_entry.get("profile"),
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    forensic_path=file_entry.get("forensic_path"),
                    extraction_tool=manifest_data.get("extraction_tool"),
                    file_size_bytes=file_entry.get("file_size_bytes"),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                )

                db_path = Path(file_entry["extracted_path"])
                if not db_path.is_absolute():
                    # Try local_filename first (new format), then extracted_path
                    local_filename = file_entry.get("local_filename")
                    if local_filename:
                        db_path = output_dir / local_filename
                    else:
                        db_path = output_dir / db_path.name

                # Parse main tables (playback, sessions)
                counts = self._parse_media_history(
                    db_path,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                    callbacks,
                )

                total_playback += counts.get("playback", 0)
                total_sessions += counts.get("sessions", 0)

                # Parse additional tables (mediaImage, origin) and detect unknown schemas
                if db_path.exists():
                    try:
                        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
                        conn.row_factory = sqlite3.Row

                        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"
                        browser = file_entry["browser"]
                        profile = file_entry.get("profile", "Default")

                        # Detect unknown tables
                        unknown_tables = discover_unknown_tables(
                            conn, KNOWN_MEDIA_HISTORY_TABLES, MEDIA_HISTORY_TABLE_PATTERNS
                        )
                        for table_info in unknown_tables:
                            warning_collector.add_unknown_table(
                                table_name=table_info["name"],
                                columns=table_info["columns"],
                                source_file=file_entry["logical_path"],
                                artifact_type="media_history",
                            )

                        # Detect unknown columns in known tables
                        for table_name, known_columns in KNOWN_COLUMNS_BY_TABLE.items():
                            unknown_cols = discover_unknown_columns(
                                conn, table_name, known_columns
                            )
                            for col_info in unknown_cols:
                                warning_collector.add_unknown_column(
                                    table_name=table_name,
                                    column_name=col_info["name"],
                                    column_type=col_info["type"],
                                    source_file=file_entry["logical_path"],
                                    artifact_type="media_history",
                                )

                        # Parse origin table
                        origin_count = self._parse_origin_table(
                            conn, browser, profile, file_entry, run_id,
                            evidence_id, evidence_conn, discovered_by
                        )
                        total_origins += origin_count

                        # Parse mediaImage table and extract album art
                        image_count, image_urls = self._parse_media_image_table(
                            conn, browser, profile, file_entry, run_id,
                            evidence_id, evidence_conn, output_dir, discovered_by, callbacks
                        )
                        total_images += image_count

                        # Cross-post image URLs
                        if image_urls:
                            from urllib.parse import urlparse
                            url_records = []
                            for url, timestamp in image_urls:
                                if url and not url.startswith(("javascript:", "data:")):
                                    parsed = urlparse(url)
                                    url_records.append({
                                        "url": url,
                                        "domain": parsed.netloc or None,
                                        "scheme": parsed.scheme or None,
                                        "discovered_by": discovered_by,
                                        "run_id": run_id,
                                        "source_path": file_entry["logical_path"],
                                        "context": f"media_image:{browser}:{profile}",
                                        "first_seen_utc": timestamp,
                                    })
                            if url_records:
                                try:
                                    insert_urls(evidence_conn, evidence_id, url_records)
                                except Exception as e:
                                    LOGGER.debug("Failed to cross-post image URLs: %s", e)

                        conn.close()

                    except Exception as e:
                        LOGGER.warning("Failed to parse additional tables for %s: %s", db_path, e)

                total_records = counts.get("playback", 0) + counts.get("sessions", 0) + total_origins + total_images
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=total_records,
                )

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry.get('extracted_path', 'unknown')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")

                if 'inventory_id' in locals():
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(e),
                    )

        # Flush schema warnings to database
        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            callbacks.on_log(f"Recorded {warning_count} schema warnings", "info")

        evidence_conn.commit()

        # Report ingested counts and finish
        total_records = total_playback + total_sessions + total_origins + total_images
        if stats:
            stats.report_ingested(
                evidence_id, self.metadata.name,
                records=total_records,
                playbacks=total_playback,
            )
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {
            "playback": total_playback,
            "sessions": total_sessions,
            "origins": total_origins,
            "images": total_images,
        }

    # Helper Methods

    def _generate_run_id(self) -> str:
        """Generate run ID: {timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context from evidence filesystem."""
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

    def _get_extraction_tool_version(self) -> str:
        """Build extraction tool version string."""
        try:
            import pytsk3
            pytsk_version = pytsk3.TSK_VERSION_STR
        except ImportError:
            pytsk_version = "unknown"

        return f"pytsk3:{pytsk_version}"

    def _discover_media_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """Scan evidence for browser media history files.

        Uses chromium/_patterns for comprehensive browser coverage including
        beta/dev/canary channels and open-source Chromium.
        """
        media_files = []

        for browser_key in browsers:
            if browser_key not in CHROMIUM_BROWSERS:
                callbacks.on_log(f"Unknown browser: {browser_key}", "warning")
                continue

            # Get patterns from chromium/_patterns module
            media_patterns = get_patterns(browser_key, "media_history")
            display_name = get_browser_display_name(browser_key)

            if not media_patterns:
                continue

            for pattern in media_patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        profile = self._extract_profile_from_path(path_str, browser_key)

                        media_files.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "profile": profile,
                            "file_type": "media_history",
                            "artifact_type": "media_history",
                            "display_name": display_name,
                        })

                        callbacks.on_log(f"Found {browser_key} media history: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return media_files

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> Dict[int, List[Dict]]:
        """
        Discover Media History files across ALL partitions using file_list.

        This method queries the pre-populated file_list table to find Media History
        files across all partitions, not just the auto-selected main partition.

        Falls back to single-partition iter_paths() if file_list is empty.

        Args:
            evidence_fs: Evidence filesystem
            evidence_conn: Evidence database connection
            evidence_id: Evidence ID for file_list lookup
            browsers: List of browser keys to search
            callbacks: Progress/log callbacks

        Returns:
            Dict mapping partition_index -> list of file info dicts
        """
        # Check if file_list is available
        available, count = check_file_list_available(evidence_conn, evidence_id)

        if not available:
            callbacks.on_log(
                "file_list empty, falling back to single-partition discovery",
                "info"
            )
            # Fallback: use traditional iter_paths on main partition
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_media_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")

        # Build path patterns for file_list query
        # We look for "Media History" files in paths containing browser-specific strings
        path_patterns = []
        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                continue
            # Convert browser paths to SQL-friendly patterns
            if "chrome" in browser.lower():
                path_patterns.append("%Google%Chrome%User Data%")
                path_patterns.append("%chrome%")  # Linux
            elif "edge" in browser.lower():
                path_patterns.append("%Microsoft%Edge%User Data%")
            elif "brave" in browser.lower():
                path_patterns.append("%BraveSoftware%Brave-Browser%User Data%")
            elif "opera" in browser.lower():
                path_patterns.append("%Opera%")
            elif "vivaldi" in browser.lower():
                path_patterns.append("%Vivaldi%User Data%")
            elif "chromium" in browser.lower():
                path_patterns.append("%Chromium%User Data%")

        # Remove duplicates
        path_patterns = list(set(path_patterns))

        # Query file_list
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["Media History"],
            path_patterns=path_patterns if path_patterns else None,
        )

        if result.is_empty:
            callbacks.on_log(
                "No Media History files found in file_list, falling back to filesystem scan",
                "warning"
            )
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_media_files(evidence_fs, browsers, callbacks)
            return {partition_index: files} if files else {}

        if result.is_multi_partition:
            callbacks.on_log(
                f"Found Media History files on {len(result.partitions_with_matches)} partitions: {result.partitions_with_matches}",
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
                    "browser": browser or "chromium",
                    "profile": profile,
                    "artifact_type": "media_history",
                    "file_type": "media_history",
                    "display_name": get_browser_display_name(browser) if browser else "Chromium",
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

                callbacks.on_log(
                    f"Found {browser or 'chromium'} Media History on partition {partition_index}: {match.file_path}",
                    "info"
                )

            if files_list:
                files_by_partition[partition_index] = files_list

        return files_by_partition

    def _extract_profile_from_path(self, path: str, browser: str) -> str:
        """Extract browser profile name from file path.

        Handles both standard Chromium browsers (Default/Profile * structure)
        and Opera's flat profile structure.
        """
        from .._patterns import is_flat_profile_browser

        parts = path.split('/')

        # Opera-style browsers have flat profile structure
        if is_flat_profile_browser(browser):
            # For Opera, the profile root IS the profile name
            if "Opera Stable" in path:
                return "Opera Stable"
            elif "Opera GX Stable" in path:
                return "Opera GX Stable"
            return "Default"

        # Standard Chromium browsers: look for "User Data" marker
        try:
            idx = parts.index("User Data")
            return parts[idx + 1] if idx + 1 < len(parts) else "Default"
        except (ValueError, IndexError):
            pass

        # Linux/macOS without "User Data" - look for Default/Profile pattern
        for i, part in enumerate(parts):
            if part == "Default" or part.startswith("Profile "):
                return part

        return "Default"

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        partition_index: int,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Copy file from evidence to workspace and collect metadata.

        Filename pattern includes partition index and path hash to ensure
        uniqueness across multiple partitions (dual-boot, portable installs).

        Also copies SQLite companion files (WAL, journal, shm) if present
        for potential recovery of uncommitted transactions.
        """
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info["profile"]

            # Generate unique filename with partition + path hash
            safe_profile = profile.replace(' ', '_').replace('/', '_')
            path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]
            filename = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_Media_History"
            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()
            size = len(file_content)

            # Copy companion files (WAL, journal, shm) for SQLite recovery
            companion_files = []
            for suffix in ["-wal", "-journal", "-shm"]:
                companion_path = source_path + suffix
                try:
                    companion_content = evidence_fs.read_file(companion_path)
                    companion_dest = Path(str(dest_path) + suffix)
                    companion_dest.write_bytes(companion_content)
                    companion_files.append({
                        "suffix": suffix,
                        "size_bytes": len(companion_content),
                    })
                    callbacks.on_log(f"Copied companion: {companion_path}", "info")
                except Exception:
                    pass  # Companion doesn't exist

            # Get filesystem type
            fs_type = getattr(evidence_fs, 'fs_type', None)
            if not isinstance(fs_type, str):
                fs_type = None

            return {
                "copy_status": "ok",
                "size_bytes": size,
                "file_size_bytes": size,
                "md5": md5,
                "sha256": sha256,
                "extracted_path": str(dest_path),
                "local_filename": dest_path.name,
                "browser": browser,
                "profile": profile,
                "file_type": "media_history",
                "logical_path": source_path,
                "artifact_type": "media_history",
                "partition_index": partition_index,
                "fs_type": fs_type,
                "forensic_path": f"p{partition_index}:{source_path}",
                "inode": file_info.get("inode"),
                "companion_files": companion_files,
            }

        except Exception as e:
            callbacks.on_log(f"Failed to extract {file_info['logical_path']}: {e}", "error")
            return {
                "copy_status": "error",
                "size_bytes": 0,
                "file_size_bytes": 0,
                "md5": None,
                "sha256": None,
                "extracted_path": None,
                "local_filename": None,
                "browser": file_info.get("browser"),
                "profile": file_info.get("profile"),
                "file_type": file_info.get("file_type"),
                "logical_path": file_info.get("logical_path"),
                "partition_index": partition_index,
                "error_message": str(e),
            }

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        """Clear media history data from a previous run."""
        deleted = delete_media_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d media records from previous run %s", deleted, run_id)

    def _parse_media_history(
        self,
        db_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """Parse Chromium Media History SQLite database."""
        from urllib.parse import urlparse
        from core.database import insert_urls

        counts = {"playback": 0, "sessions": 0, "urls_table": 0}

        if not db_path.exists():
            LOGGER.warning("Media history file not found: %s", db_path)
            return counts

        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
        except Exception as e:
            LOGGER.error("Failed to open Media History: %s", e)
            return counts

        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"
        browser = file_entry["browser"]
        profile = file_entry.get("profile", "Default")

        # Collect URLs for cross-posting (no deduplication - timestamps matter forensically)
        url_records = []

        try:
            # Parse playback table
            playback_count, playback_urls = self._parse_playback_table(
                conn, browser, profile, file_entry, run_id, evidence_id, evidence_conn, discovered_by
            )
            counts["playback"] = playback_count
            for url, timestamp in playback_urls:
                if url and not url.startswith(("javascript:", "data:")):
                    parsed = urlparse(url)
                    url_records.append({
                        "url": url,
                        "domain": parsed.netloc or None,
                        "scheme": parsed.scheme or None,
                        "discovered_by": discovered_by,
                        "run_id": run_id,
                        "source_path": file_entry["logical_path"],
                        "context": f"media_playback:{browser}:{profile}",
                        "first_seen_utc": timestamp,
                    })

            # Parse playbackSession table
            session_count, session_urls = self._parse_playback_session_table(
                conn, browser, profile, file_entry, run_id, evidence_id, evidence_conn, discovered_by
            )
            counts["sessions"] = session_count
            for url, timestamp in session_urls:
                if url and not url.startswith(("javascript:", "data:")):
                    parsed = urlparse(url)
                    url_records.append({
                        "url": url,
                        "domain": parsed.netloc or None,
                        "scheme": parsed.scheme or None,
                        "discovered_by": discovered_by,
                        "run_id": run_id,
                        "source_path": file_entry["logical_path"],
                        "context": f"media_session:{browser}:{profile}",
                        "first_seen_utc": timestamp,
                    })

        finally:
            conn.close()

        # Cross-post URLs to unified urls table for analysis
        if url_records:
            try:
                insert_urls(evidence_conn, evidence_id, url_records)
                counts["urls_table"] = len(url_records)
                LOGGER.debug("Cross-posted %d media history URLs to urls table", len(url_records))
            except Exception as e:
                LOGGER.debug("Failed to cross-post media history URLs: %s", e)

        return counts

    def _parse_playback_table(
        self, conn, browser, profile, file_entry, run_id, evidence_id, evidence_conn, discovered_by
    ) -> tuple:
        """Parse Chromium playback table.

        Returns:
            Tuple of (count, urls) where urls is a list of (url, timestamp) tuples
        """
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='playback'")
        if not cursor.fetchone():
            return 0, []

        cursor.execute("PRAGMA table_info(playback)")
        columns = {row[1] for row in cursor.fetchall()}

        # Need to join with origin table to get URL
        # Check for origin table
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='origin'")
        has_origin_table = cursor.fetchone() is not None

        if has_origin_table and "origin_id" in columns:
            query = """
                SELECT p.*, o.origin as origin_url
                FROM playback p
                LEFT JOIN origin o ON p.origin_id = o.id
            """
        else:
            query = "SELECT * FROM playback"

        cursor.execute(query)

        records = []
        for row in cursor:
            record = {
                "browser": browser,
                "profile": profile,
                "url": row["url"] if "url" in columns else "",
                "origin": row.get("origin_url") or "",
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry["logical_path"],
                "forensic_path": file_entry.get("forensic_path"),
            }

            if "watch_time_s" in columns:
                record["watch_time_seconds"] = row["watch_time_s"]
            elif "watchtime" in columns:
                record["watch_time_seconds"] = row["watchtime"]
            else:
                record["watch_time_seconds"] = 0

            if "has_video" in columns:
                record["has_video"] = 1 if row["has_video"] else 0
            else:
                record["has_video"] = 0

            if "has_audio" in columns:
                record["has_audio"] = 1 if row["has_audio"] else 0
            else:
                record["has_audio"] = 1  # Assume audio if not specified

            if "last_updated_time_s" in columns and row["last_updated_time_s"]:
                record["last_played_utc"] = self._webkit_to_iso8601(int(row["last_updated_time_s"]) * 1_000_000)
            elif "last_updated_time" in columns and row["last_updated_time"]:
                record["last_played_utc"] = self._webkit_to_iso8601(row["last_updated_time"])
            else:
                record["last_played_utc"] = None

            records.append(record)

        # Collect URLs for cross-posting
        urls = [(r.get("url"), r.get("last_played_utc")) for r in records if r.get("url")]

        if records:
            return insert_media_playbacks(evidence_conn, evidence_id, records), urls
        return 0, urls

    def _parse_playback_session_table(
        self, conn, browser, profile, file_entry, run_id, evidence_id, evidence_conn, discovered_by
    ) -> tuple:
        """Parse Chromium playbackSession table.

        Returns:
            Tuple of (count, urls) where urls is a list of (url, timestamp) tuples
        """
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='playbackSession'")
        if not cursor.fetchone():
            return 0, []

        cursor.execute("PRAGMA table_info(playbackSession)")
        columns = {row[1] for row in cursor.fetchall()}

        cursor.execute("SELECT * FROM playbackSession")

        records = []
        for row in cursor:
            record = {
                "browser": browser,
                "profile": profile,
                "url": row["url"] if "url" in columns else "",
                "origin": "",  # Will be empty for sessions
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry["logical_path"],
                "forensic_path": file_entry.get("forensic_path"),
            }

            if "title" in columns:
                record["title"] = row["title"]
            if "artist" in columns:
                record["artist"] = row["artist"]
            if "album" in columns:
                record["album"] = row["album"]
            if "source_title" in columns:
                record["source_title"] = row["source_title"]

            if "duration_ms" in columns:
                record["duration_ms"] = row["duration_ms"] or 0
            elif "duration" in columns:
                record["duration_ms"] = row["duration"] or 0
            else:
                record["duration_ms"] = 0

            if "position_ms" in columns:
                record["position_ms"] = row["position_ms"] or 0
            elif "position" in columns:
                record["position_ms"] = row["position"] or 0
            else:
                record["position_ms"] = 0

            # Calculate completion percentage
            duration = record.get("duration_ms", 0)
            position = record.get("position_ms", 0)
            if duration > 0:
                record["completion_percent"] = round((position / duration) * 100, 1)
            else:
                record["completion_percent"] = None

            if "last_updated_time_s" in columns and row["last_updated_time_s"]:
                record["last_played_utc"] = self._webkit_to_iso8601(int(row["last_updated_time_s"]) * 1_000_000)
            elif "last_updated_time" in columns and row["last_updated_time"]:
                record["last_played_utc"] = self._webkit_to_iso8601(row["last_updated_time"])
            else:
                record["last_played_utc"] = None

            records.append(record)

        # Collect URLs for cross-posting
        urls = [(r.get("url"), r.get("last_played_utc")) for r in records if r.get("url")]

        if records:
            return insert_media_sessions(evidence_conn, evidence_id, records), urls
        return 0, urls

    def _webkit_to_iso8601(self, webkit_timestamp: int) -> Optional[str]:
        """Convert WebKit timestamp to ISO 8601."""
        if webkit_timestamp == 0:
            return None

        WEBKIT_EPOCH_OFFSET = 11644473600000000

        try:
            unix_microseconds = webkit_timestamp - WEBKIT_EPOCH_OFFSET
            unix_seconds = unix_microseconds / 1_000_000
            dt = datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
            return dt.isoformat()
        except (ValueError, OSError, OverflowError):
            return None

    def _parse_origin_table(
        self,
        conn,
        browser: str,
        profile: str,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        discovered_by: str,
    ) -> int:
        """Parse Chromium origin table - raw records (no aggregation).

        Origins represent unique sites that played media, providing a domain-level
        view of "which sites did user watch media from?" for forensic analysis.

        Returns:
            Number of records inserted
        """
        from core.database import insert_media_origins

        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='origin'")
        if not cursor.fetchone():
            return 0

        cursor.execute("PRAGMA table_info(origin)")
        columns = {row[1] for row in cursor.fetchall()}

        cursor.execute("SELECT * FROM origin")

        records = []
        for row in cursor:
            last_updated = None
            if "last_updated_time_s" in columns and row["last_updated_time_s"]:
                last_updated = self._webkit_to_iso8601(int(row["last_updated_time_s"]) * 1_000_000)
            elif "last_updated_time" in columns and row["last_updated_time"]:
                last_updated = self._webkit_to_iso8601(row["last_updated_time"])

            records.append({
                "browser": browser,
                "profile": profile,
                "origin": row["origin"],
                "origin_id_source": row["id"],  # Preserve source ID for cross-reference
                "last_updated_utc": last_updated,
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry["logical_path"],
                "forensic_path": file_entry.get("forensic_path"),
            })

        if records:
            return insert_media_origins(evidence_conn, evidence_id, records)
        return 0

    def _parse_media_image_table(
        self,
        conn,
        browser: str,
        profile: str,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        output_dir: Path,
        discovered_by: str,
        callbacks: ExtractorCallbacks,
    ) -> tuple:
        """Parse Chromium mediaImage table and extract album art to workspace.

        Following ELT pattern:
        - Extract blobs to workspace files during ingestion
        - Store metadata in shared images + image_discoveries tables
        - Dedup by SHA256 (same image content = one file, multiple discoveries)

        Album art location: {output_dir}/album_art/{sha256[:16]}.{ext}

        Returns:
            Tuple of (count, urls) where urls is a list of (url, timestamp) tuples
        """
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='mediaImage'")
        if not cursor.fetchone():
            return 0, []

        cursor.execute("PRAGMA table_info(mediaImage)")
        columns = {row[1] for row in cursor.fetchall()}

        cursor.execute("SELECT * FROM mediaImage WHERE data IS NOT NULL")

        # Create album_art subdirectory
        images_dir = output_dir / "album_art"
        images_dir.mkdir(exist_ok=True)

        count = 0
        urls = []

        for row in cursor:
            try:
                blob = row["data"]
                if not blob or len(blob) < 8:
                    continue

                # Calculate hashes
                sha256 = hashlib.sha256(blob).hexdigest()
                md5 = hashlib.md5(blob).hexdigest()

                # Determine extension from mime_type
                mime_type = row["mime_type"] if "mime_type" in columns and row["mime_type"] else "image/jpeg"
                ext = {
                    "image/jpeg": ".jpg",
                    "image/png": ".png",
                    "image/webp": ".webp",
                    "image/gif": ".gif",
                }.get(mime_type, ".jpg")

                # Filename based on content hash (dedup)
                filename = f"{sha256[:16]}{ext}"
                rel_path = f"album_art/{filename}"
                dest_path = images_dir / filename

                # Only write if not already exists (dedup by content)
                if not dest_path.exists():
                    dest_path.write_bytes(blob)

                # Prepare image data for shared images table
                image_data = {
                    "rel_path": rel_path,
                    "filename": filename,
                    "md5": md5,
                    "sha256": sha256,
                    "size_bytes": len(blob),
                    "file_type": mime_type.split("/")[1] if "/" in mime_type else "jpeg",
                    "extracted_path": str(dest_path),
                }

                # Prepare discovery data
                page_url = row["url"] if "url" in columns else None
                src_url = row["src_url"] if "src_url" in columns else None

                discovery_data = {
                    "discovered_by": self.metadata.name,
                    "run_id": run_id,
                    "extractor_version": self.metadata.version,
                    "fs_path": file_entry["logical_path"],
                    # Use cache fields for browser context
                    "cache_url": page_url,  # Page where media was playing
                    "source_metadata_json": {
                        "browser": browser,
                        "profile": profile,
                        "src_url": src_url,
                        "image_type": row["image_type"] if "image_type" in columns else None,
                        "playback_origin_id": row["playback_origin_id"] if "playback_origin_id" in columns else None,
                    },
                }

                # Insert using shared helper (handles dedup)
                try:
                    image_id, was_inserted = insert_image_with_discovery(
                        evidence_conn, evidence_id, image_data, discovery_data
                    )
                    count += 1
                except Exception as e:
                    LOGGER.debug("Failed to insert album art image: %s", e)

                # Collect URLs for cross-posting
                if page_url:
                    urls.append((page_url, None))
                if src_url:
                    urls.append((src_url, None))

            except Exception as e:
                callbacks.on_log(f"Failed to extract album art: {e}", "warning")

        if count > 0:
            callbacks.on_log(f"Extracted {count} album art images", "info")

        return count, urls
