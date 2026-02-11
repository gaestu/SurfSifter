"""Firefox Bookmarks Extractor

Extracts browser bookmarks from all Firefox-based browsers (Firefox, Firefox ESR, Tor Browser).
Uses shared patterns and parsers from the firefox family module.

Features:
- Bookmark extraction with folder hierarchy
- Bookmark backup extraction (bookmarkbackups/*.jsonlz4) for historical/deleted bookmarks
- PRTime timestamp conversion to ISO 8601
- StatisticsCollector integration
- WAL/journal file copying for SQLite recovery
- Forensic provenance (run_id, source_path, partition context)
- Source tracking (live vs backup) for forensic analysis
- Multi-partition discovery - scans all partitions via file_list
- Schema warning support for unknown tables/keys
- Collision-safe filename with partition index and path hash

 Changes:
- Multi-partition discovery via file_list table (replaces slow iter_paths)
- Schema warning support for unknown tables/columns/keys
- Filename collision prevention with partition_index and path hash
- Delete-by-run support for clean re-ingestion
- lz4 dependency now raises error instead of warning
"""

from __future__ import annotations

import hashlib
import json
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
    check_file_list_available,
)
from extractors._shared.extraction_warnings import (
    ExtractionWarningCollector,
    discover_unknown_tables,
    discover_unknown_json_keys,
    CATEGORY_DATABASE,
    CATEGORY_JSON,
    SEVERITY_ERROR,
    SEVERITY_WARNING,
    SEVERITY_INFO,
    WARNING_TYPE_COMPRESSION_ERROR,
    WARNING_TYPE_JSON_PARSE_ERROR,
    WARNING_TYPE_JSON_UNKNOWN_KEY,
)
from .._patterns import (
    FIREFOX_BROWSERS,
    get_patterns,
    get_browser_display_name,
    get_all_browsers,
    extract_profile_from_path,
    detect_browser_from_path,
)
from .._parsers import (
    parse_bookmarks,
    get_bookmark_stats,
    parse_bookmark_backup,
    get_bookmark_backup_stats,
    extract_backup_timestamp,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_bookmark_row,
    insert_browser_inventory,
    insert_urls,
    update_inventory_ingestion_status,
    delete_bookmarks_by_run,
)


LOGGER = get_logger("extractors.browser.firefox.bookmarks")


class FirefoxBookmarksExtractor(BaseExtractor):
    """
    Extract browser bookmarks from Firefox-based browsers.

    Supports: Firefox, Firefox ESR, Tor Browser

    Dual-phase workflow:
    - Extraction: Scans filesystem, copies places.sqlite AND bookmarkbackups/*.jsonlz4
    - Ingestion: Parses SQLite databases and JSON backups, inserts with forensic fields

    Features:
    - Bookmark extraction with folder hierarchy (moz_bookmarks + moz_places)
    - Bookmark backup extraction (bookmarkbackups/*.jsonlz4) for historical/deleted bookmarks
    - PRTime timestamp conversion to ISO 8601
    - StatisticsCollector integration for run tracking
    - WAL/journal file copying for SQLite recovery
    - Browser selection config widget
    - Source tracking: "live" (places.sqlite) vs "backup" (jsonlz4)
    - Multi-partition discovery via file_list table
    - Schema warning support for unknown tables/keys

    Note: Firefox stores bookmarks in places.sqlite (same file as history).
    Bookmark backups contain historical snapshots that may include deleted bookmarks.

     Changes:
    - Multi-partition discovery via file_list table
    - Schema warning support for unknown tables/columns/keys
    - Filename collision prevention with partition_index and path hash
    - Delete-by-run support for clean re-ingestion
    - lz4 dependency now raises error instead of warning
    """

    # Known tables in places.sqlite for schema warning discovery
    KNOWN_PLACES_TABLES = {
        "moz_bookmarks",
        "moz_places",
        "moz_historyvisits",
        "moz_annos",
        "moz_anno_attributes",
        "moz_bookmarks_deleted",
        "moz_favicons",
        "moz_hosts",
        "moz_inputhistory",
        "moz_items_annos",
        "moz_keywords",
        "moz_origins",
        "moz_meta",
        "moz_places_metadata",
        "moz_places_metadata_search_queries",
        "moz_places_metadata_snapshots",
        "moz_places_metadata_snapshots_extra",
        "moz_places_metadata_snapshots_groups",
        "moz_previews_tombstones",
        "moz_session_metadata",
        "moz_session_to_places",
        "sqlite_stat1",
        "sqlite_sequence",
    }

    # Patterns for bookmark-related tables (for schema warning filtering)
    BOOKMARK_TABLE_PATTERNS = ["bookmark", "place", "anno", "keyword"]

    # Known JSON keys in Firefox bookmark backup files (jsonlz4)
    # Used for schema warning discovery to alert on new/unknown keys
    KNOWN_BACKUP_JSON_KEYS = {
        # Node identification
        "type",           # text/x-moz-place, text/x-moz-place-container, text/x-moz-place-separator
        "id",             # Internal bookmark ID
        "guid",           # Sync GUID
        "index",          # Position in parent folder
        "title",          # Bookmark/folder title

        # URL bookmarks
        "uri",            # The bookmarked URL
        "charset",        # Character encoding hint
        "iconUri",        # Favicon URI (data: or moz-anno:)
        "keyword",        # Keyword shortcut
        "postData",       # POST data for keyword bookmarks
        "tags",           # Comma-separated tags string

        # Timestamps (PRTime = microseconds since epoch)
        "dateAdded",      # When bookmark was created
        "lastModified",   # When last modified

        # Structure
        "children",       # Child nodes array (folders only)
        "root",           # Root folder marker
        "typeCode",       # Numeric type code

        # Annotations (Firefox 57+ moved some data here)
        "annos",          # Annotations array
        "annos.name",     # Annotation name in annos array
        "annos.value",    # Annotation value
        "annos.expires",  # Expiration
        "annos.flags",    # Flags
    }

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="firefox_bookmarks",
            display_name="Firefox Bookmarks",
            description="Extract browser bookmarks and backup history from Firefox, Firefox ESR, Tor Browser",
            category="browser",
            requires_tools=[],  # Pure Python, lz4 required for backups
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
        Return configuration widget (browser selection).

        Uses BrowserConfigWidget filtered to Firefox browsers only.
        """
        return BrowserConfigWidget(
            parent,
            supported_browsers=get_all_browsers(),
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
            status_text = f"Firefox Bookmarks\nFiles: {file_count}\nRun: {data.get('run_id', 'N/A')[:20]}"
        else:
            status_text = "Firefox Bookmarks\nNo extraction yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "firefox_bookmarks"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Firefox bookmarks databases from evidence.

        Workflow:
            1. Generate run_id
            2. Discover bookmarks files across ALL partitions via file_list
            3. Copy matching files to output_dir/ with collision-safe naming
            4. Calculate hashes, collect E01 context
            5. Write manifest.json
        """
        callbacks.on_step("Initializing Firefox bookmarks extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting Firefox bookmarks extraction (run_id=%s)", run_id)

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Initialize manifest
        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "files": [],
            "partitions_with_artifacts": [],
            "status": "ok",
            "notes": [],
        }

        # Determine which browsers to search
        browsers = config.get("browsers") or config.get("selected_browsers") or get_all_browsers()

        # Discover bookmarks files across all partitions
        callbacks.on_step("Discovering Firefox bookmarks files")
        files_by_partition = self._discover_files_multi_partition(
            evidence_fs, evidence_conn, evidence_id, browsers, callbacks
        )

        # Count total files
        total_files = sum(len(files) for files in files_by_partition.values())
        manifest_data["partitions_with_artifacts"] = sorted(files_by_partition.keys())

        # Report discovered files
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=total_files)

        if not files_by_partition:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Firefox bookmarks files found")
            LOGGER.info("No bookmarks files found")
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, status="success")
        else:
            callbacks.on_log(
                f"Found {total_files} bookmarks file(s) on {len(files_by_partition)} partition(s)"
            )

            # Extract files from each partition
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            file_index = 0

            for partition_index in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_index]
                if not partition_files:
                    continue

                callbacks.on_log(
                    f"Processing partition {partition_index}: {len(partition_files)} files"
                )

                # Determine how to access this partition
                current_partition = getattr(evidence_fs, 'partition_index', 0)
                if partition_index == current_partition:
                    # Use existing evidence_fs
                    fs_to_use = evidence_fs
                    need_close = False
                elif ewf_paths:
                    # Open specific partition
                    fs_to_use = None
                    need_close = True
                else:
                    # Can't access different partition
                    callbacks.on_log(
                        f"Cannot access partition {partition_index} - skipping",
                        "warning"
                    )
                    continue

                try:
                    if need_close:
                        # Open partition using context manager
                        with open_partition_for_extraction(ewf_paths, partition_index) as fs:
                            for file_info in partition_files:
                                if callbacks.is_cancelled():
                                    manifest_data["status"] = "cancelled"
                                    break

                                file_index += 1
                                callbacks.on_progress(
                                    file_index, total_files,
                                    f"Copying {file_info['browser']} {file_info.get('source_type', 'bookmarks')}"
                                )

                                try:
                                    extracted_file = self._extract_file(
                                        fs, file_info, output_dir, callbacks
                                    )
                                    manifest_data["files"].append(extracted_file)
                                except Exception as e:
                                    error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                                    LOGGER.error(error_msg, exc_info=True)
                                    manifest_data["notes"].append(error_msg)
                                    manifest_data["status"] = "partial"
                                    if stats:
                                        stats.report_failed(evidence_id, self.metadata.name, files=1)
                    else:
                        # Use existing filesystem
                        for file_info in partition_files:
                            if callbacks.is_cancelled():
                                manifest_data["status"] = "cancelled"
                                break

                            file_index += 1
                            callbacks.on_progress(
                                file_index, total_files,
                                f"Copying {file_info['browser']} {file_info.get('source_type', 'bookmarks')}"
                            )

                            try:
                                extracted_file = self._extract_file(
                                    fs_to_use, file_info, output_dir, callbacks
                                )
                                manifest_data["files"].append(extracted_file)
                            except Exception as e:
                                error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                                LOGGER.error(error_msg, exc_info=True)
                                manifest_data["notes"].append(error_msg)
                                manifest_data["status"] = "partial"
                                if stats:
                                    stats.report_failed(evidence_id, self.metadata.name, files=1)

                except Exception as e:
                    error_msg = f"Failed to process partition {partition_index}: {e}"
                    LOGGER.error(error_msg, exc_info=True)
                    manifest_data["notes"].append(error_msg)
                    manifest_data["status"] = "partial"

        # Write manifest
        callbacks.on_step("Writing manifest")
        (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))

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

        # Finish statistics tracking
        if stats and manifest_data["status"] not in ("cancelled",):
            status = "success" if manifest_data["status"] == "ok" else "partial"
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        LOGGER.info(
            "Firefox bookmarks extraction complete: %d files, status=%s",
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
        Parse extracted bookmarks databases and ingest into database.

        Workflow:
            1. Read manifest.json
            2. Create warning collector for schema discovery
            3. Clear previous run data
            4. Register files in browser_cache_inventory
            5. For each bookmarks database:
               - Parse SQLite moz_bookmarks + moz_places
               - Insert into bookmarks table
            6. Flush warnings and update inventory status
            7. Return counts
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"bookmarks": 0, "records": 0}

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

        # Continue statistics tracking for ingestion phase
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, bookmarks=0)
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"bookmarks": 0, "records": 0}

        # Clear previous run data before ingestion
        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        total_records = 0
        failed_files = 0

        callbacks.on_progress(0, len(files), "Parsing bookmarks files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            # Skip files that failed extraction
            if file_entry.get("copy_status") == "error":
                callbacks.on_log(f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}", "warning")
                continue

            source_type = file_entry.get("source_type", "live")
            artifact_type = file_entry.get("artifact_type", "bookmarks")

            callbacks.on_progress(
                i + 1, len(files),
                f"Parsing {file_entry.get('browser', 'unknown')} bookmarks ({source_type})"
            )

            try:
                # Register in inventory
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "unknown"),
                    artifact_type=artifact_type,
                    run_id=run_id,
                    extracted_path=file_entry.get("extracted_path", ""),
                    extraction_status="ok",
                    extraction_timestamp_utc=manifest_data["extraction_timestamp_utc"],
                    logical_path=file_entry.get("logical_path", ""),
                    profile=file_entry.get("profile"),
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    forensic_path=file_entry.get("forensic_path"),
                    extraction_tool=manifest_data.get("extraction_tool"),
                    file_size_bytes=file_entry.get("file_size_bytes"),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                )

                # Parse and insert records
                file_path = Path(file_entry["extracted_path"])
                if not file_path.is_absolute():
                    file_path = output_dir / file_path

                records = self._parse_and_insert(
                    file_path,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                    callbacks,
                    warning_collector=warning_collector,
                )

                # Update inventory
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    urls_parsed=records,
                    records_parsed=records,
                )

                total_records += records

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")
                failed_files += 1

                if "inventory_id" in locals():
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(e),
                    )

        # Flush collected warnings to database before commit
        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)

        evidence_conn.commit()

        # Report final statistics
        if stats:
            stats.report_ingested(evidence_id, self.metadata.name, records=total_records, bookmarks=total_records)
            if failed_files:
                stats.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        return {"bookmarks": total_records, "records": total_records}

    # =========================================================================
    # Private Helper Methods
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate run ID: {timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        """Clear bookmark data from a previous run."""
        deleted = delete_bookmarks_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d bookmark records from previous run %s", deleted, run_id)

    def _discover_backup_json_keys(
        self,
        file_path: Path,
        source_path: str,
        warning_collector: ExtractionWarningCollector,
    ) -> None:
        """
        Discover unknown JSON keys in a bookmark backup file.

        Parses the backup JSON and reports any keys not in KNOWN_BACKUP_JSON_KEYS
        as schema warnings for forensic review.

        Args:
            file_path: Path to the jsonlz4 backup file
            source_path: Original evidence path for warning context
            warning_collector: Warning collector to add findings to
        """
        try:
            from .._parsers import decompress_mozlz4
            import json

            data = file_path.read_bytes()
            json_data = decompress_mozlz4(data)
            backup = json.loads(json_data)

            # Discover unknown keys recursively
            unknown_keys = self._collect_unknown_json_keys(backup, "", set())

            for key_info in unknown_keys:
                warning_collector.add_warning(
                    warning_type=WARNING_TYPE_JSON_UNKNOWN_KEY,
                    category=CATEGORY_JSON,
                    severity=SEVERITY_INFO,
                    artifact_type="bookmark_backup",
                    source_file=source_path,
                    item_name=key_info["path"],
                    item_value=key_info["sample"],
                    context_json={"type": key_info["type"]},
                )
                LOGGER.debug(
                    "Unknown JSON key in backup %s: %s (%s)",
                    file_path.name, key_info["path"], key_info["type"]
                )

        except Exception as e:
            LOGGER.debug("JSON key discovery failed for %s: %s", file_path, e)

    def _collect_unknown_json_keys(
        self,
        node: dict,
        path: str,
        seen_paths: set,
    ) -> list:
        """
        Recursively collect unknown JSON keys from bookmark tree.

        Args:
            node: Current JSON node
            path: Current dot-separated path
            seen_paths: Set of paths already reported (to avoid duplicates)

        Returns:
            List of unknown key info dicts
        """
        if not isinstance(node, dict):
            return []

        results = []

        for key, value in node.items():
            full_path = f"{path}.{key}" if path else key

            # Check if this key is known
            if key not in self.KNOWN_BACKUP_JSON_KEYS and full_path not in seen_paths:
                seen_paths.add(full_path)

                # Get sample value
                if isinstance(value, (dict, list)):
                    sample = f"<{type(value).__name__}>"
                else:
                    sample = str(value)[:50]

                results.append({
                    "path": full_path,
                    "type": type(value).__name__,
                    "sample": sample,
                })

            # Recurse into children
            if isinstance(value, dict):
                results.extend(self._collect_unknown_json_keys(value, full_path, seen_paths))
            elif isinstance(value, list):
                # Check items in children arrays
                for i, item in enumerate(value[:5]):  # Sample first 5 items
                    if isinstance(item, dict):
                        results.extend(self._collect_unknown_json_keys(item, path, seen_paths))

        return results

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context from evidence filesystem."""
        try:
            source_path = getattr(evidence_fs, "source_path", None)
            if source_path is not None and not isinstance(source_path, (str, Path)):
                source_path = None

            fs_type = getattr(evidence_fs, "fs_type", "unknown")
            if not isinstance(fs_type, str):
                fs_type = "unknown"

            return {
                "image_path": str(source_path) if source_path else None,
                "fs_type": fs_type,
            }
        except Exception:
            return {"image_path": None, "fs_type": "unknown"}

    def _get_tool_version(self) -> str:
        """Build extraction tool version string."""
        versions = []

        try:
            import pytsk3
            versions.append(f"pytsk3:{pytsk3.TSK_VERSION_STR}")
        except ImportError:
            versions.append("pytsk3:not_installed")

        try:
            import pyewf
            versions.append(f"pyewf:{pyewf.get_version()}")
        except ImportError:
            versions.append("pyewf:not_installed")

        return ";".join(versions)

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> Dict[int, List[Dict]]:
        """
        Discover Firefox bookmarks files across all partitions via file_list.

        Discovers:
        - places.sqlite: Live bookmark database
        - bookmarkbackups/*.jsonlz4: Historical bookmark backups

        Args:
            evidence_fs: Evidence filesystem (for partition info)
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
                "file_list empty - cannot discover bookmarks files",
                "warning"
            )
            return {}

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")

        # Build path patterns for Firefox profiles
        path_patterns = [
            # Windows
            "%Mozilla%Firefox%Profiles%",
            # macOS
            "%Application Support%Firefox%Profiles%",
            # Linux
            "%.mozilla/firefox%",
            # Tor Browser
            "%Tor Browser%TorBrowser%Data%Browser%",
            "%tor-browser%TorBrowser%Data%Browser%",
        ]

        # Query file_list for bookmarks-related files
        # Live bookmarks: places.sqlite
        live_result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["places.sqlite"],
            path_patterns=path_patterns,
        )

        # Backup bookmarks: *.jsonlz4 in bookmarkbackups
        backup_result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["*.jsonlz4"],
            path_patterns=["%bookmarkbackups%"],
        )

        if live_result.is_empty and backup_result.is_empty:
            callbacks.on_log("No Firefox bookmarks files found in file_list", "info")
            return {}

        total_partitions = set(live_result.partitions_with_matches) | set(backup_result.partitions_with_matches)
        if len(total_partitions) > 1:
            callbacks.on_log(
                f"Found bookmarks files on {len(total_partitions)} partitions: {sorted(total_partitions)}",
                "info"
            )

        # Convert FileListMatch objects to extractor's expected format
        files_by_partition: Dict[int, List[Dict]] = {}

        # Process live bookmarks
        for partition_index, matches in live_result.matches_by_partition.items():
            for match in matches:
                # Skip WAL/journal files (companion files)
                if match.file_name.endswith(("-wal", "-shm", "-journal")):
                    continue

                # Detect browser from path
                browser = detect_browser_from_path(match.file_path)
                if browser and browser not in browsers:
                    continue  # Skip if browser not in selection

                profile = extract_profile_from_path(match.file_path)

                file_info = {
                    "logical_path": match.file_path,
                    "browser": browser or "firefox",
                    "profile": profile,
                    "artifact_type": "bookmarks",
                    "source_type": "live",
                    "display_name": get_browser_display_name(browser) if browser else "Firefox",
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                }

                if partition_index not in files_by_partition:
                    files_by_partition[partition_index] = []
                files_by_partition[partition_index].append(file_info)

                callbacks.on_log(
                    f"Found {browser or 'firefox'} bookmarks on partition {partition_index}: {match.file_path}",
                    "info"
                )

        # Process backup bookmarks
        for partition_index, matches in backup_result.matches_by_partition.items():
            for match in matches:
                # Detect browser from path
                browser = detect_browser_from_path(match.file_path)
                if browser and browser not in browsers:
                    continue  # Skip if browser not in selection

                profile = extract_profile_from_path(match.file_path)
                backup_date = extract_backup_timestamp(match.file_name)

                file_info = {
                    "logical_path": match.file_path,
                    "browser": browser or "firefox",
                    "profile": profile,
                    "artifact_type": "bookmark_backup",
                    "source_type": "backup",
                    "backup_date": backup_date,
                    "display_name": get_browser_display_name(browser) if browser else "Firefox",
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                }

                if partition_index not in files_by_partition:
                    files_by_partition[partition_index] = []
                files_by_partition[partition_index].append(file_info)

                callbacks.on_log(
                    f"Found {browser or 'firefox'} bookmark backup on partition {partition_index}: {match.file_path} "
                    f"(date: {backup_date or 'unknown'})",
                    "info"
                )

        return files_by_partition

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """
        Copy file from evidence to workspace with collision-safe naming.

        Naming format: {browser}_{profile}_p{partition}_{path_hash}_{artifact_type}
        This prevents collisions when:
        - Same browser/profile exists on multiple partitions (dual-boot)
        - Profile name sanitization produces duplicates
        """
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info.get("profile") or "Unknown"
            artifact_type = file_info.get("artifact_type", "bookmarks")
            source_type = file_info.get("source_type", "live")
            partition_index = file_info.get("partition_index", 0)

            # Create collision-safe filename with partition suffix and mini-hash
            # Mini-hash: first 8 chars of SHA256 of source path
            safe_profile = profile.replace(" ", "_").replace("/", "_").replace(".", "_")
            path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]

            if artifact_type == "bookmark_backup":
                # Preserve original backup filename for forensic reference
                original_filename = Path(source_path).name
                filename = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_backup_{original_filename}"
            else:
                filename = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_places.sqlite"

            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

            # Read and write file
            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

            # Calculate hashes
            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()
            size = len(file_content)

            result = {
                "copy_status": "ok",
                "size_bytes": size,
                "file_size_bytes": size,
                "md5": md5,
                "sha256": sha256,
                "extracted_path": str(dest_path),
                "browser": browser,
                "profile": profile,
                "logical_path": source_path,
                "artifact_type": artifact_type,
                "source_type": source_type,
                "partition_index": partition_index,
            }

            # Add backup-specific metadata
            if artifact_type == "bookmark_backup":
                result["backup_date"] = file_info.get("backup_date")
                result["original_filename"] = Path(source_path).name
            else:
                # Copy companion files (WAL, journal, shm) for SQLite databases
                companion_files = []
                for suffix in ["-wal", "-shm"]:
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

                result["companion_files"] = companion_files

            return result

        except Exception as e:
            callbacks.on_log(f"Failed to extract {file_info['logical_path']}: {e}", "error")
            return {
                "copy_status": "error",
                "size_bytes": 0,
                "file_size_bytes": 0,
                "md5": None,
                "sha256": None,
                "extracted_path": None,
                "browser": file_info.get("browser"),
                "profile": file_info.get("profile"),
                "artifact_type": file_info.get("artifact_type"),
                "source_type": file_info.get("source_type"),
                "logical_path": file_info.get("logical_path"),
                "partition_index": file_info.get("partition_index", 0),
                "error_message": str(e),
            }

    def _parse_and_insert(
        self,
        file_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> int:
        """Parse bookmarks file (SQLite or jsonlz4 backup) and insert records.

        Handles:
        - places.sqlite: Live bookmarks from moz_bookmarks + moz_places
        - *.jsonlz4: Historical bookmark backup files

        Also:
        - Cross-posts URLs to unified urls table for analysis
        - Discovers unknown tables/keys for schema warnings
        """
        import sqlite3
        from urllib.parse import urlparse

        if not file_path.exists():
            LOGGER.warning("Bookmarks file not found: %s", file_path)
            return 0

        browser = file_entry.get("browser", "firefox")
        profile = file_entry.get("profile", "Default")
        source_path = file_entry.get("logical_path", "")
        artifact_type = file_entry.get("artifact_type", "bookmarks")
        source_type = file_entry.get("source_type", "live")
        backup_date = file_entry.get("backup_date")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        # Add source_type and backup_date to discovered_by for provenance
        if source_type == "backup":
            discovered_by = f"{discovered_by}:backup"
            if backup_date:
                discovered_by = f"{discovered_by}:{backup_date}"

        count = 0
        url_records = []  # Collect URLs for unified urls table

        try:
            # Choose parser based on artifact type
            if artifact_type == "bookmark_backup":
                # Parse jsonlz4 backup file - lz4 is REQUIRED, raise error if not available
                try:
                    import lz4.block  # noqa: F401
                except ImportError as e:
                    error_msg = f"lz4 module required for bookmark backup parsing. Install with: pip install lz4"
                    LOGGER.error(error_msg)
                    callbacks.on_error(error_msg, str(file_path))

                    # Record as extraction warning
                    if warning_collector:
                        warning_collector.add_warning(
                            warning_type=WARNING_TYPE_COMPRESSION_ERROR,
                            category=CATEGORY_JSON,
                            severity=SEVERITY_ERROR,
                            artifact_type="bookmark_backup",
                            source_file=source_path,
                            item_name="lz4_dependency",
                            item_value=str(e),
                            context_json={"file_path": str(file_path)},
                        )
                    raise ImportError(error_msg) from e

                # Discover unknown JSON keys for schema warnings
                if warning_collector:
                    self._discover_backup_json_keys(
                        file_path, source_path, warning_collector
                    )

                bookmarks_iter = parse_bookmark_backup(file_path)
            else:
                # Parse SQLite database
                # Discover unknown tables for schema warnings
                if warning_collector:
                    try:
                        db_conn = sqlite3.connect(f"file:{file_path}?mode=ro", uri=True)
                        unknown_tables = discover_unknown_tables(
                            db_conn,
                            self.KNOWN_PLACES_TABLES,
                            self.BOOKMARK_TABLE_PATTERNS,
                        )
                        for unknown in unknown_tables:
                            warning_collector.add_unknown_table(
                                table_name=unknown["name"],
                                columns=unknown["columns"],
                                source_file=source_path,
                                artifact_type="bookmarks",
                            )
                        db_conn.close()
                    except Exception as e:
                        LOGGER.debug("Schema discovery failed for %s: %s", file_path, e)

                bookmarks_iter = parse_bookmarks(file_path)

            for bookmark in bookmarks_iter:
                insert_bookmark_row(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=browser,
                    url=bookmark.url,
                    profile=profile,
                    title=bookmark.title,
                    folder_path=bookmark.folder_path,
                    bookmark_type=bookmark.bookmark_type,
                    guid=bookmark.guid,
                    date_added_utc=bookmark.date_added_utc,
                    date_modified_utc=bookmark.date_modified_utc,
                    run_id=run_id,
                    source_path=source_path,
                    discovered_by=discovered_by,
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    logical_path=file_entry.get("logical_path"),
                    forensic_path=file_entry.get("forensic_path"),
                )
                count += 1

                # Collect URL for unified urls table (skip javascript/data URIs)
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
                        "first_seen_utc": bookmark.date_added_utc,
                    })

            # Cross-post URLs to unified urls table for analysis
            if url_records:
                try:
                    insert_urls(evidence_conn, evidence_id, url_records)
                    LOGGER.debug("Cross-posted %d bookmark URLs to urls table", len(url_records))
                except Exception as e:
                    LOGGER.debug("Failed to cross-post bookmark URLs: %s", e)

        except ImportError:
            # lz4 module not available - already handled above with error
            return 0

        except ValueError as e:
            # JSON parse error or decompression error from parse_bookmark_backup
            error_msg = f"Failed to parse bookmark backup: {e}"
            LOGGER.error(error_msg)
            callbacks.on_error(error_msg, str(file_path))

            if warning_collector:
                warning_collector.add_warning(
                    warning_type=WARNING_TYPE_JSON_PARSE_ERROR,
                    category=CATEGORY_JSON,
                    severity=SEVERITY_ERROR,
                    artifact_type="bookmark_backup",
                    source_file=source_path,
                    item_name="parse_error",
                    item_value=str(e),
                )
            return 0

        except Exception as e:
            LOGGER.error("Failed to parse bookmarks file %s: %s", file_path, e)
            callbacks.on_error(f"Failed to parse bookmarks: {e}", str(file_path))
            return 0

        if count:
            source_label = f"backup ({backup_date})" if source_type == "backup" else "live"
            callbacks.on_log(f"Inserted {count} bookmarks from {browser} ({source_label})", "info")

        return count
