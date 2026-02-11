"""
Internet Explorer Favorites Extractor

Extracts bookmarks/favorites from .url shortcut files in the Favorites folder.

IE/Edge favorites are stored as Windows URL shortcut files (.url) which are
INI-style text files containing:
- URL - The bookmark URL
- IconFile - Optional icon path
- IconIndex - Icon index in the file
- Modified - Optional modification timestamp

Favorites folder locations:
- Users/{username}/Favorites/*.url
- Users/{username}/Favorites/**/*.url (subfolders = bookmark folders)

Features:
- INI-style .url file parsing
- Folder hierarchy extraction
- Multi-user support
- Multi-partition discovery via file_list
- Integration with bookmarks table
"""

from __future__ import annotations

import configparser
import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Iterator

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    get_ewf_paths_from_evidence_fs,
    open_partition_for_extraction,
)
from .._patterns import (
    IE_ARTIFACTS,
    get_patterns,
    get_all_patterns,
    detect_browser_from_path,
    extract_user_from_path,
)
from core.logging import get_logger
from core.database import (
    insert_bookmark_row,
    insert_browser_inventory,
    insert_urls,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.favorites")


class IEFavoritesExtractor(BaseExtractor):
    """
    Extract IE/Legacy Edge favorites (bookmarks) from .url files.

    This extractor scans for .url shortcut files in the Favorites folder
    and parses them to extract bookmark URLs.

    Unlike the WebCache-based extractors, this handles both extraction
    AND ingestion since .url files are simple text files that can be
    parsed directly.

    Workflow:
    1. Scan evidence for .url files in Favorites folders
    2. Copy files to workspace (optional, for forensic preservation)
    3. Parse INI-style content
    4. Insert into bookmarks table
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="ie_favorites",
            display_name="IE/Edge Favorites",
            description="Extract bookmarks from .url shortcut files",
            category="browser",
            requires_tools=[],  # Pure Python
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
        """Return configuration widget (multi-partition option)."""
        return MultiPartitionWidget(parent, default_scan_all=True)

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
            status_text = f"IE/Edge Favorites\nFiles: {file_count}"
        else:
            status_text = "IE/Edge Favorites\nNo extraction yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "ie_favorites"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract .url files from evidence.

        Workflow:
            1. Generate run_id
            2. Scan evidence for .url files (multi-partition if enabled)
            3. Copy files to output_dir/ with metadata
            4. Write manifest.json
        """
        callbacks.on_step("Initializing IE/Edge Favorites extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting IE/Edge Favorites extraction (run_id=%s)", run_id)

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")
        scan_all_partitions = config.get("scan_all_partitions", True)

        # Start statistics tracking
        collector = self._get_statistics_collector()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Initialize manifest
        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "multi_partition_extraction": scan_all_partitions,
            "partitions_scanned": [],
            "partitions_with_artifacts": [],
            "files": [],
            "status": "ok",
            "notes": [],
        }

        # Scan for .url files
        callbacks.on_step("Scanning for .url favorites files")

        files_by_partition: Dict[int, List[Dict]] = {}

        if scan_all_partitions and evidence_conn is not None:
            files_by_partition = self._discover_files_multi_partition(
                evidence_fs, evidence_conn, evidence_id, callbacks
            )
        else:
            url_files = self._discover_files(evidence_fs, callbacks)
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            if url_files:
                files_by_partition[partition_index] = url_files

        # Flatten for counting
        all_files = []
        for files_list in files_by_partition.values():
            all_files.extend(files_list)

        # Update manifest with partition info
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        manifest_data["partitions_with_artifacts"] = sorted(
            p for p, files in files_by_partition.items() if files
        )

        # Report discovered files
        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=len(all_files))

        callbacks.on_log(f"Found {len(all_files)} .url file(s) across {len(manifest_data['partitions_with_artifacts'])} partition(s)", "info")

        if not all_files:
            LOGGER.info("No .url files found in any Favorites folders")
            callbacks.on_log("No .url files found in any Favorites folders", "warning")
            manifest_data["notes"].append("No .url files found in Favorites folders")
        else:
            callbacks.on_log(f"Starting extraction of {len(all_files)} favorites file(s)", "info")
            callbacks.on_progress(0, len(all_files), "Extracting favorites")

            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            file_index = 0

            for partition_index in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_index]
                current_partition = getattr(evidence_fs, 'partition_index', 0)

                fs_ctx = (
                    open_partition_for_extraction(evidence_fs, None)
                    if (partition_index == current_partition or ewf_paths is None)
                    else open_partition_for_extraction(ewf_paths, partition_index)
                )

                try:
                    with fs_ctx as fs_to_use:
                        if fs_to_use is None:
                            callbacks.on_log(f"Failed to open partition {partition_index}", "warning")
                            continue

                        for file_info in partition_files:
                            if callbacks.is_cancelled():
                                manifest_data["status"] = "cancelled"
                                break

                            file_index += 1
                            callbacks.on_progress(
                                file_index, len(all_files),
                                f"Copying {file_info.get('name', 'unknown')}"
                            )

                            try:
                                result = self._extract_file(
                                    fs_to_use, file_info, output_dir, callbacks
                                )
                                result["partition_index"] = partition_index
                                manifest_data["files"].append(result)
                            except Exception as e:
                                error_msg = f"Failed to extract {file_info.get('logical_path')}: {e}"
                                LOGGER.error(error_msg, exc_info=True)
                                manifest_data["notes"].append(error_msg)
                except Exception as e:
                    LOGGER.error("Failed to open partition %d: %s", partition_index, e)

        # Finish statistics
        if collector:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        # Write manifest
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

        # Log completion summary
        successful_files = len([f for f in manifest_data["files"] if f.get("copy_status") == "ok"])
        LOGGER.info(
            "IE/Edge Favorites extraction complete: %d/%d files extracted, status=%s",
            successful_files,
            len(all_files) if all_files else 0,
            manifest_data["status"],
        )
        callbacks.on_log(
            f"Extraction complete: {successful_files} file(s) extracted successfully",
            "info" if manifest_data["status"] == "ok" else "warning"
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
        Parse extracted .url files and ingest into database.

        Workflow:
            1. Read manifest.json
            2. For each .url file:
               - Parse INI-style content
               - Extract URL and metadata
               - Insert into bookmarks table
               - Cross-post URL to urls table
            3. Return counts
        """
        from urllib.parse import urlparse

        callbacks.on_step("Reading favorites manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"bookmarks": 0, "urls": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())
        evidence_label = config.get("evidence_label", "")
        files = manifest_data.get("files", [])

        # Continue statistics tracking
        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        callbacks.on_log(f"Found {len(files)} extracted file(s) in manifest", "info")

        if not files:
            callbacks.on_log("No files to ingest - manifest is empty", "warning")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"bookmarks": 0, "urls": 0}

        total_bookmarks = 0
        failed_files = 0
        url_records = []  # Collect URLs for unified urls table
        seen_urls = set()  # Deduplicate URLs

        callbacks.on_progress(0, len(files), "Parsing favorites")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(
                i + 1, len(files),
                f"Parsing {file_entry.get('name', 'unknown')}"
            )

            try:
                db_path = Path(file_entry["extracted_path"])
                if not db_path.is_absolute():
                    db_path = output_dir / db_path

                if not db_path.exists():
                    callbacks.on_log(f"File not found: {db_path}", "warning")
                    failed_files += 1
                    continue

                # Register in browser inventory
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "ie"),
                    artifact_type="bookmarks",
                    run_id=run_id,
                    extracted_path=str(db_path),
                    extraction_status="ok",
                    extraction_timestamp_utc=manifest_data.get("extraction_timestamp_utc"),
                    logical_path=file_entry.get("logical_path", ""),
                    profile=file_entry.get("user"),
                    partition_index=file_entry.get("partition_index"),
                )

                # Parse and insert
                bookmark = self._parse_url_file(db_path, file_entry, run_id)

                if bookmark and bookmark.get("url"):
                    insert_bookmark_row(
                        evidence_conn,
                        evidence_id=evidence_id,
                        **bookmark
                    )
                    total_bookmarks += 1

                    # Collect URL for unified urls table (dual-write)
                    url = bookmark["url"]
                    if url not in seen_urls and not url.startswith(("javascript:", "data:")):
                        seen_urls.add(url)
                        try:
                            parsed = urlparse(url)
                            browser = file_entry.get("browser", "ie")
                            user = file_entry.get("user", "unknown")
                            url_records.append({
                                "url": url,
                                "domain": parsed.netloc or None,
                                "scheme": parsed.scheme or None,
                                "discovered_by": bookmark.get("discovered_by", f"{self.metadata.name}:{self.metadata.version}:{run_id}"),
                                "run_id": run_id,
                                "source_path": file_entry.get("logical_path", ""),
                                "context": f"bookmark:{browser}:{user}",
                                "first_seen_utc": bookmark.get("date_added"),
                            })
                        except Exception as e:
                            LOGGER.debug("Failed to parse URL for cross-post: %s - %s", url[:100], e)

                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="ok",
                        records_parsed=1,
                    )
                else:
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="ok",
                        records_parsed=0,
                        notes="No URL found in file",
                    )

            except Exception as e:
                error_msg = f"Failed to parse {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")
                failed_files += 1

        # Cross-post URLs to unified urls table for analysis
        if url_records:
            try:
                insert_urls(evidence_conn, evidence_id, url_records)
                LOGGER.debug("Cross-posted %d favorite URLs to urls table", len(url_records))
            except Exception as e:
                LOGGER.debug("Failed to cross-post favorite URLs: %s", e)

        evidence_conn.commit()

        # Report final statistics
        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=total_bookmarks,
            )
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        # Log detailed completion summary
        callbacks.on_log(
            f"Ingestion complete: {total_bookmarks} bookmark(s), {len(url_records)} URL(s), {failed_files} failed",
            "info" if failed_files == 0 else "warning"
        )
        LOGGER.info(
            "IE/Edge Favorites ingestion complete: %d bookmarks, %d URLs, %d failed files",
            total_bookmarks, len(url_records), failed_files
        )

        return {"bookmarks": total_bookmarks, "urls": len(url_records)}

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

    def _discover_files(
        self,
        evidence_fs,
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """
        Scan evidence for .url files (single partition).

        OPTIMIZATION: Avoids recursive ** glob patterns which trigger full
        filesystem walks on large E01 images. Instead:
        1. Use non-recursive patterns to find Favorites directories
        2. Recursively walk each Favorites folder directly
        """
        url_files = []
        seen_paths = set()  # Deduplicate across patterns

        # Get all patterns but filter out ** recursive patterns
        patterns = get_all_patterns("favorites")
        non_recursive_patterns = [p for p in patterns if "**" not in p]

        # Track Favorites directories we've found for recursive scanning
        favorites_dirs = set()

        # Phase 1: Use non-recursive patterns (fast targeted search)
        callbacks.on_log(f"Phase 1: Scanning {len(non_recursive_patterns)} non-recursive pattern(s)", "info")

        for pattern in non_recursive_patterns:
            try:
                LOGGER.info("Scanning pattern: %s", pattern)
                callbacks.on_step(f"Scanning: {pattern}")
                pattern_matches = 0
                for match in evidence_fs.iter_paths(pattern):
                    if "($FILE_NAME)" in match:
                        continue

                    if not match.lower().endswith(".url"):
                        continue

                    if match in seen_paths:
                        continue
                    seen_paths.add(match)

                    browser = detect_browser_from_path(match)
                    user = extract_user_from_path(match)
                    folder_path = self._extract_folder_path(match)
                    name = Path(match).stem

                    url_files.append({
                        "logical_path": match,
                        "browser": browser,
                        "user": user,
                        "name": name,
                        "folder_path": folder_path,
                    })

                    pattern_matches += 1

                    # Track parent Favorites directory for recursive scan
                    fav_dir = self._get_favorites_parent_dir(match)
                    if fav_dir:
                        favorites_dirs.add(fav_dir)

                if pattern_matches > 0:
                    LOGGER.info("Pattern %s matched %d file(s)", pattern, pattern_matches)
                    callbacks.on_log(f"Found {pattern_matches} file(s) matching {pattern}", "info")

            except Exception as e:
                LOGGER.warning("Error scanning pattern %s: %s", pattern, e)
                callbacks.on_log(f"Error scanning {pattern}: {e}", "warning")

        # Phase 2: Recursively scan each Favorites directory for subfolders
        # This is faster than ** glob because we target specific directories
        if favorites_dirs:
            callbacks.on_log(f"Phase 2: Scanning {len(favorites_dirs)} Favorites folder(s) for subfolders", "info")

        for fav_dir in favorites_dirs:
            try:
                LOGGER.info("Recursively scanning Favorites dir: %s", fav_dir)
                callbacks.on_step(f"Scanning subfolders: {fav_dir}")
                subfolder_count = 0

                for file_path in self._walk_favorites_dir(evidence_fs, fav_dir):
                    if file_path in seen_paths:
                        continue

                    if not file_path.lower().endswith(".url"):
                        continue

                    seen_paths.add(file_path)

                    browser = detect_browser_from_path(file_path)
                    user = extract_user_from_path(file_path)
                    folder_path = self._extract_folder_path(file_path)
                    name = Path(file_path).stem

                    url_files.append({
                        "logical_path": file_path,
                        "browser": browser,
                        "user": user,
                        "name": name,
                        "folder_path": folder_path,
                    })
                    subfolder_count += 1

                if subfolder_count > 0:
                    LOGGER.info("Found %d additional .url file(s) in subfolders of %s", subfolder_count, fav_dir)
                    callbacks.on_log(f"Found {subfolder_count} file(s) in subfolders of {fav_dir}", "info")

            except Exception as e:
                LOGGER.warning("Error walking Favorites dir %s: %s", fav_dir, e)
                callbacks.on_log(f"Error walking {fav_dir}: {e}", "warning")

        callbacks.on_log(f"Discovery complete: {len(url_files)} total .url file(s) found", "info")
        return url_files

    def _get_favorites_parent_dir(self, file_path: str) -> Optional[str]:
        """Extract the Favorites directory path from a .url file path."""
        path = file_path.replace("\\", "/")
        parts = path.split("/")

        try:
            fav_idx = next(
                i for i, p in enumerate(parts)
                if p.lower() == "favorites"
            )
            # Return path up to and including Favorites
            return "/".join(parts[:fav_idx + 1])
        except StopIteration:
            return None

    def _walk_favorites_dir(self, evidence_fs, favorites_dir: str) -> Iterator[str]:
        """
        Recursively walk a Favorites directory yielding .url file paths.

        Uses targeted directory traversal instead of ** glob patterns.
        """
        try:
            # Try to use walk_directory if available (faster than iter_paths)
            if hasattr(evidence_fs, 'walk_directory'):
                LOGGER.debug("Using optimized walk_directory for %s", favorites_dir)
                file_count = 0
                for path in evidence_fs.walk_directory(favorites_dir):
                    if path.lower().endswith(".url"):
                        file_count += 1
                        yield path
                LOGGER.debug("walk_directory yielded %d .url files from %s", file_count, favorites_dir)
            else:
                # Fallback: Use list_dir recursively
                LOGGER.debug("Using fallback recursive_list_dir for %s", favorites_dir)
                yield from self._recursive_list_dir(evidence_fs, favorites_dir)
        except Exception as e:
            LOGGER.warning("Error walking %s: %s", favorites_dir, e)

    def _recursive_list_dir(
        self,
        evidence_fs,
        dir_path: str,
        depth: int = 0,
        max_depth: int = 10
    ) -> Iterator[str]:
        """Recursively list directory contents (fallback method)."""
        if depth > max_depth:
            return

        try:
            if not hasattr(evidence_fs, 'list_dir'):
                return

            for entry in evidence_fs.list_dir(dir_path):
                entry_path = f"{dir_path}/{entry}".replace("//", "/")

                if entry.endswith("/"):
                    # Directory - recurse into it
                    yield from self._recursive_list_dir(
                        evidence_fs,
                        entry_path.rstrip("/"),
                        depth + 1,
                        max_depth
                    )
                elif entry.lower().endswith(".url"):
                    yield entry_path

        except Exception as e:
            LOGGER.debug("Cannot list dir %s: %s", dir_path, e)

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, List[Dict]]:
        """Discover .url files across ALL partitions using file_list."""
        available, count = check_file_list_available(evidence_conn, evidence_id) if evidence_conn else (False, 0)

        if not available:
            callbacks.on_log("file_list empty, falling back to single-partition discovery", "info")
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, callbacks)
            return {partition_index: files} if files else {}

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")

        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["%.url"],
            path_patterns=["%Favorites%"],
        )

        if result.is_empty:
            partition_index = getattr(evidence_fs, 'partition_index', 0)
            files = self._discover_files(evidence_fs, callbacks)
            return {partition_index: files} if files else {}

        files_by_partition: Dict[int, List[Dict]] = {}

        for partition_index, matches in result.matches_by_partition.items():
            files_list = []
            for match in matches:
                if not match.file_name.lower().endswith(".url"):
                    continue

                browser = detect_browser_from_path(match.file_path)
                user = extract_user_from_path(match.file_path)
                folder_path = self._extract_folder_path(match.file_path)
                name = Path(match.file_name).stem

                files_list.append({
                    "logical_path": match.file_path,
                    "browser": browser,
                    "user": user,
                    "name": name,
                    "folder_path": folder_path,
                    "partition_index": partition_index,
                    "inode": match.inode,
                    "size_bytes": match.size_bytes,
                })

            if files_list:
                files_by_partition[partition_index] = files_list

        return files_by_partition

    def _extract_folder_path(self, file_path: str) -> str:
        """Extract folder hierarchy from Favorites path."""
        # Normalize path
        path = file_path.replace("\\", "/")

        # Find Favorites folder
        parts = path.split("/")
        try:
            fav_idx = next(
                i for i, p in enumerate(parts)
                if p.lower() == "favorites"
            )
            # Get subfolders between Favorites and the file
            subfolders = parts[fav_idx + 1:-1]  # Exclude Favorites and filename
            return "/".join(subfolders) if subfolders else ""
        except StopIteration:
            return ""

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Copy .url file from evidence to workspace."""
        source_path = file_info["logical_path"]
        user = file_info.get("user", "unknown")
        name = file_info.get("name", "unknown")
        folder = file_info.get("folder_path", "")

        # Create output filename with collision prevention
        safe_user = user.replace(" ", "_").replace("/", "_").replace("\\", "_")
        safe_folder = folder.replace("/", "_").replace("\\", "_") if folder else "root"
        base_filename = f"{safe_user}_{safe_folder}_{name}"
        dest_path = output_dir / f"{base_filename}.url"

        # Handle filename collisions
        counter = 1
        while dest_path.exists():
            dest_path = output_dir / f"{base_filename}_{counter}.url"
            counter += 1

        # Get original timestamps from evidence BEFORE copying
        crtime_epoch = None
        mtime_epoch = None
        try:
            if hasattr(evidence_fs, 'stat'):
                stat_info = evidence_fs.stat(source_path)
                crtime_epoch = stat_info.crtime_epoch
                mtime_epoch = stat_info.mtime_epoch
        except Exception as e:
            LOGGER.debug("Failed to stat %s: %s", source_path, e)

        # Read and write file
        file_content = evidence_fs.read_file(source_path)
        dest_path.write_bytes(file_content)

        # Calculate hashes
        md5 = hashlib.md5(file_content).hexdigest()
        sha256 = hashlib.sha256(file_content).hexdigest()

        return {
            "copy_status": "ok",
            "size_bytes": len(file_content),
            "md5": md5,
            "sha256": sha256,
            "extracted_path": str(dest_path),
            "logical_path": source_path,
            "browser": file_info.get("browser", "ie"),
            "user": user,
            "name": name,
            "folder_path": folder,
            "crtime_epoch": crtime_epoch,
            "mtime_epoch": mtime_epoch,
        }

    def _parse_url_file(
        self,
        file_path: Path,
        file_entry: Dict,
        run_id: str
    ) -> Optional[Dict]:
        """
        Parse a .url shortcut file.

        Format is INI-style:
        [InternetShortcut]
        URL=http://example.com
        IconFile=...
        IconIndex=...
        """
        # Try multiple encodings: UTF-8, UTF-16, Windows-1252, Latin-1
        content = None
        raw_bytes = file_path.read_bytes()

        for encoding in ("utf-8", "utf-16", "cp1252", "latin-1"):
            try:
                content = raw_bytes.decode(encoding)
                # Validate we got something reasonable
                if "[" in content or "url=" in content.lower():
                    break
            except (UnicodeDecodeError, LookupError):
                continue

        if content is None:
            # Last resort: force decode with replacement
            content = raw_bytes.decode("utf-8", errors="replace")

        # Parse as INI
        config = configparser.ConfigParser(interpolation=None)
        try:
            config.read_string(content)
        except Exception:
            # Try to extract URL manually
            for line in content.splitlines():
                if line.lower().startswith("url="):
                    url = line[4:].strip()
                    return self._build_bookmark_record(url, file_entry, run_id, file_path)
            return None

        # Get URL from InternetShortcut section
        url = None
        if config.has_section("InternetShortcut"):
            url = config.get("InternetShortcut", "URL", fallback=None)
        elif config.has_section("DEFAULT"):
            url = config.get("DEFAULT", "URL", fallback=None)

        if not url:
            return None

        return self._build_bookmark_record(url, file_entry, run_id, file_path)

    def _build_bookmark_record(
        self,
        url: str,
        file_entry: Dict,
        run_id: str,
        extracted_path: Optional[Path] = None,
    ) -> Dict:
        """Build bookmark record for database insertion."""
        # Use original evidence timestamps (crtime preferred, then mtime)
        # These are captured during extraction and stored in the manifest
        date_added = None

        # Priority: crtime (creation time) > mtime (modification time)
        crtime_epoch = file_entry.get("crtime_epoch")
        mtime_epoch = file_entry.get("mtime_epoch")

        if crtime_epoch is not None:
            try:
                date_added = datetime.fromtimestamp(crtime_epoch, tz=timezone.utc).isoformat()
            except (ValueError, OSError, OverflowError):
                pass

        if date_added is None and mtime_epoch is not None:
            try:
                date_added = datetime.fromtimestamp(mtime_epoch, tz=timezone.utc).isoformat()
            except (ValueError, OSError, OverflowError):
                pass

        return {
            "browser": file_entry.get("browser", "ie"),
            "profile": file_entry.get("user", "unknown"),
            "url": url,
            "title": file_entry.get("name", ""),
            "folder_path": file_entry.get("folder_path", ""),
            "source_path": file_entry.get("logical_path", ""),
            "discovered_by": f"{self.metadata.name}:{self.metadata.version}:{run_id}",
            "run_id": run_id,
            "partition_index": file_entry.get("partition_index", 0),
            "date_added_utc": date_added,
        }
