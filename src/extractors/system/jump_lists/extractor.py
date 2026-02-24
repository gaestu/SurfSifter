"""
System Jump Lists & Recent Items Extractor

Windows Jump Lists and Related Shortcuts extraction with multi-partition support.

Jump Lists contain "Recent" and "Frequent" items from taskbar applications:
- For browsers: visited URLs that may survive history clearing
- For other apps: recently opened files (Word docs, media files, etc.)

Extracted artifact types:
- AutomaticDestinations-ms: OLE compound files (recent/frequent items)
- CustomDestinations-ms: Concatenated LNK files (pinned items)
- Recent Items: Standalone .lnk files created when files are opened
- Desktop Shortcuts: User-created or installed shortcuts on desktop
- Taskbar Pinned: User-pinned applications to taskbar (shows intent)
- Start Menu Pinned: User-pinned applications to Start Menu
- Quick Launch: Legacy quick launch items

Multi-partition support: Uses file_list table for discovery across ALL partitions
in E01 images, with fallback to filesystem iteration for mounted paths.

Location patterns:
  %APPDATA%/Microsoft/Windows/Recent/AutomaticDestinations/
  %APPDATA%/Microsoft/Windows/Recent/CustomDestinations/
  %APPDATA%/Microsoft/Windows/Recent/*.lnk
  %USERPROFILE%/Desktop/*.lnk
  %APPDATA%/Microsoft/Internet Explorer/Quick Launch/User Pinned/TaskBar/*.lnk
  %APPDATA%/Microsoft/Internet Explorer/Quick Launch/User Pinned/StartMenu/*.lnk
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import QWidget, QLabel

from ...base import BaseExtractor, ExtractorMetadata
from ...callbacks import ExtractorCallbacks
from core.statistics_collector import StatisticsCollector
from core.database import insert_urls

from .appid_registry import load_browser_appids, is_browser_jumplist, get_app_name
from .ole_parser import parse_jumplist_file
from .lnk_parser import extract_url_from_lnk, parse_lnk_data

# Multi-partition discovery support
from extractors._shared.file_list_discovery import (
    discover_from_file_list,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
    check_file_list_available,
)

LOGGER = logging.getLogger(__name__)


# Source types for forensic categorization (used in pin_status)
SOURCE_TYPE_JUMPLIST_AUTO = "jump_list_automatic"
SOURCE_TYPE_JUMPLIST_CUSTOM = "jump_list_custom"
SOURCE_TYPE_RECENT_ITEM = "recent_item"
SOURCE_TYPE_DESKTOP = "desktop"
SOURCE_TYPE_TASKBAR_PINNED = "taskbar_pinned"
SOURCE_TYPE_START_MENU_PINNED = "start_menu_pinned"
SOURCE_TYPE_QUICK_LAUNCH = "quick_launch"


# Path patterns for Jump List, Recent Item, and other forensically valuable LNK files.
# Each entry: (glob_pattern, source_type, file_list_path_pattern, file_list_filename_pattern)
#
# AutomaticDestinations = recent/frequent items (MRU/MFU)
# CustomDestinations = user-pinned items (Tasks section)
# Standalone .lnk = shortcuts created when files are opened or user-pinned items
JUMPLIST_PATTERNS = [
    # === Jump Lists (OLE containers) ===
    # AutomaticDestinations (recent/frequent)
    (
        "Users/*/AppData/Roaming/Microsoft/Windows/Recent/AutomaticDestinations/*.automaticDestinations-ms",
        SOURCE_TYPE_JUMPLIST_AUTO,
        "%/Recent/AutomaticDestinations/%",
        "%.automaticDestinations-ms",
    ),
    (
        "Documents and Settings/*/Application Data/Microsoft/Windows/Recent/AutomaticDestinations/*.automaticDestinations-ms",
        SOURCE_TYPE_JUMPLIST_AUTO,
        "%/Recent/AutomaticDestinations/%",
        "%.automaticDestinations-ms",
    ),
    # CustomDestinations (pinned tasks)
    (
        "Users/*/AppData/Roaming/Microsoft/Windows/Recent/CustomDestinations/*.customDestinations-ms",
        SOURCE_TYPE_JUMPLIST_CUSTOM,
        "%/Recent/CustomDestinations/%",
        "%.customDestinations-ms",
    ),
    (
        "Documents and Settings/*/Application Data/Microsoft/Windows/Recent/CustomDestinations/*.customDestinations-ms",
        SOURCE_TYPE_JUMPLIST_CUSTOM,
        "%/Recent/CustomDestinations/%",
        "%.customDestinations-ms",
    ),
    
    # === Recent Items (standalone LNK files) ===
    # These are created when user opens any file
    (
        "Users/*/AppData/Roaming/Microsoft/Windows/Recent/*.lnk",
        SOURCE_TYPE_RECENT_ITEM,
        "%/Roaming/Microsoft/Windows/Recent/%.lnk",
        "%.lnk",
    ),
    (
        "Documents and Settings/*/Recent/*.lnk",
        SOURCE_TYPE_RECENT_ITEM,
        "%/Recent/%.lnk",
        "%.lnk",
    ),
    
    # === Desktop Shortcuts ===
    # User-created or installed shortcuts on desktop (shows intent)
    (
        "Users/*/Desktop/*.lnk",
        SOURCE_TYPE_DESKTOP,
        "%/Desktop/%.lnk",
        "%.lnk",
    ),
    (
        "Users/Public/Desktop/*.lnk",
        SOURCE_TYPE_DESKTOP,
        "%/Public/Desktop/%.lnk",
        "%.lnk",
    ),
    
    # === Taskbar Pinned Items ===
    # User-pinned applications to taskbar (shows intent)
    (
        "Users/*/AppData/Roaming/Microsoft/Internet Explorer/Quick Launch/User Pinned/TaskBar/*.lnk",
        SOURCE_TYPE_TASKBAR_PINNED,
        "%/User Pinned/TaskBar/%.lnk",
        "%.lnk",
    ),
    
    # === Start Menu Pinned Items ===
    # User-pinned applications to Start Menu (shows intent)
    (
        "Users/*/AppData/Roaming/Microsoft/Internet Explorer/Quick Launch/User Pinned/StartMenu/*.lnk",
        SOURCE_TYPE_START_MENU_PINNED,
        "%/User Pinned/StartMenu/%.lnk",
        "%.lnk",
    ),
    
    # === Quick Launch Items ===
    # Direct Quick Launch items (not in User Pinned subfolders)
    (
        "Users/*/AppData/Roaming/Microsoft/Internet Explorer/Quick Launch/*.lnk",
        SOURCE_TYPE_QUICK_LAUNCH,
        "%/Quick Launch/%.lnk",
        "%.lnk",
    ),
]


class SystemJumpListsExtractor(BaseExtractor):
    """
    Windows Jump Lists extractor with statistics tracking.

    Extracts browser URLs from Windows Jump Lists (AutomaticDestinations-ms files).
    These are OS-level artifacts maintained by Windows that persist independently
    of browser data and can reveal browsing activity after history is cleared.

    Features:
        - AutomaticDestinations-ms file discovery
        - CustomDestinations-ms file discovery
        - Standalone .lnk Recent Items discovery
        - OLE compound file parsing
        - LNK shortcut parsing within Jump Lists
        - Browser AppID recognition (Chrome, Edge, Firefox, Opera, Brave)
        - URL extraction from browser Jump Lists
        - Statistics tracking via StatisticsCollector
    """

    def __init__(self) -> None:
        """Initialize extractor."""
        super().__init__()

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="system_jump_lists",
            display_name="Windows Jump Lists & Recent Items",
            description="Extract browser URLs and file paths from Windows Jump Lists",
            category="system",
            requires_tools=["olefile"],
            can_extract=True,
            can_ingest=True,
        )

    def _check_olefile_available(self) -> tuple[bool, str]:
        """Check if olefile library is available for OLE parsing."""
        try:
            import olefile  # noqa: F401
            return True, ""
        except ImportError:
            return False, (
                "olefile library not installed. "
                "Install with: pip install olefile"
            )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        # Check olefile dependency first
        available, msg = self._check_olefile_available()
        if not available:
            return False, msg
        if evidence_fs is None:
            return False, "No evidence filesystem mounted."
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run."""
        # Check olefile dependency first (needed for parsing during ingestion)
        available, msg = self._check_olefile_available()
        if not available:
            return False, msg
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output has existing extraction."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: Optional[QWidget] = None) -> QWidget:
        """No configuration needed."""
        return None

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
            try:
                data = json.loads(manifest.read_text())
                jl_count = len(data.get("files", []))
                browser_count = sum(1 for f in data.get("files", []) if f.get("is_browser"))
                status_text = (
                    f"Windows Jump Lists\n"
                    f"Jump Lists found: {jl_count}\n"
                    f"Browser Jump Lists: {browser_count}\n"
                    f"Run ID: {data.get('run_id', 'N/A')}"
                )
            except (json.JSONDecodeError, IOError):
                status_text = "Windows Jump Lists\nError reading manifest"
        else:
            status_text = "Windows Jump Lists\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory."""
        return case_root / "evidences" / evidence_label / "jump_lists"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Jump List files from evidence with statistics tracking.

        Args:
            evidence_fs: Evidence filesystem interface.
            output_dir: Directory for extracted files.
            config: Extraction configuration.
            callbacks: Progress and logging callbacks.

        Returns:
            True if extraction succeeded, False otherwise.
        """
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        run_id = self._generate_run_id()
        stats = StatisticsCollector.instance()

        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)
        start_time = datetime.now(timezone.utc)

        callbacks.on_step("Initializing Jump Lists extraction")
        LOGGER.info("Starting Jump Lists extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "files": [],
            "status": "ok",
            "notes": [],
        }

        success = False
        status = "failed"
        file_count = 0

        try:
            callbacks.on_step("Scanning for Jump List files")
            
            # Get evidence_conn for file_list discovery (multi-partition support)
            evidence_conn = config.get("evidence_conn")
            
            jumplist_files = self._discover_jumplist_files(
                evidence_fs, callbacks,
                evidence_conn=evidence_conn,
                evidence_id=evidence_id,
            )

            if not jumplist_files:
                manifest_data["status"] = "skipped"
                manifest_data["notes"].append("No Jump List files found (non-Windows image?)")
                LOGGER.info("No Jump List files found")
                status = "success"  # Not finding files is not an error
            else:
                callbacks.on_progress(0, len(jumplist_files), "Copying Jump List files")
                
                # Group files by partition for efficient multi-partition extraction
                files_by_partition: Dict[int, List[Dict]] = {}
                for file_info in jumplist_files:
                    part_idx = file_info.get("partition_index")
                    files_by_partition.setdefault(part_idx, []).append(file_info)
                
                # Get EWF paths for multi-partition support
                ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
                
                file_idx = 0
                for partition_idx, partition_files in files_by_partition.items():
                    # Open partition (or use existing evidence_fs if partition_idx is None)
                    with open_partition_for_extraction(
                        ewf_paths if ewf_paths and partition_idx is not None else evidence_fs,
                        partition_idx if ewf_paths else None
                    ) as fs:
                        for file_info in partition_files:
                            if callbacks.is_cancelled():
                                manifest_data["status"] = "cancelled"
                                status = "cancelled"
                                break

                            try:
                                file_idx += 1
                                callbacks.on_progress(
                                    file_idx, len(jumplist_files), f"Copying {file_info['filename']}"
                                )

                                extracted_file = self._extract_file(
                                    fs,
                                    file_info,
                                    output_dir,
                                    callbacks,
                                )
                                manifest_data["files"].append(extracted_file)

                            except Exception as e:
                                error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                                LOGGER.error(error_msg, exc_info=True)
                                manifest_data["notes"].append(error_msg)
                                manifest_data["status"] = "partial"
                    
                    if callbacks.is_cancelled():
                        break

                if manifest_data["status"] != "cancelled":
                    status = "success" if manifest_data["status"] == "ok" else "partial"

            file_count = len(manifest_data["files"])
            success = manifest_data["status"] != "error"

            LOGGER.info(
                "Jump Lists extraction complete: %d files, status=%s, duration=%.2fs",
                file_count,
                manifest_data["status"],
                (datetime.now(timezone.utc) - start_time).total_seconds(),
            )

            return success

        except Exception as e:
            status = "failed"
            manifest_data["status"] = "error"
            manifest_data["notes"].append(f"Extraction failed: {e}")
            LOGGER.error("Jump Lists extraction failed: %s", e, exc_info=True)
            raise

        finally:
            # Always write manifest
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

            if stats:
                stats.report_discovered(evidence_id, self.metadata.name, files=file_count)
                stats.finish_run(evidence_id, self.metadata.name, status)

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """
        Parse Jump List files and ingest browser URLs with statistics tracking.

        Args:
            output_dir: Directory with extraction output.
            evidence_conn: Evidence database connection.
            evidence_id: Evidence identifier.
            config: Ingestion configuration.
            callbacks: Progress and logging callbacks.

        Returns:
            Dictionary with record counts by type.
        """
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()

        # Read run_id from manifest to correlate with extraction stats
        manifest_path = output_dir / "manifest.json"
        run_id = config.get("run_id", "")
        if not run_id and manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text())
                run_id = manifest.get("run_id", "")
            except (json.JSONDecodeError, IOError):
                pass
        if not run_id:
            run_id = self._generate_run_id()

        # Continue statistics tracking from extraction phase (same extractor name)
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)
        start_time = datetime.now(timezone.utc)

        result: Dict[str, int] = {"jump_list_entries": 0, "urls": 0}
        status = "failed"

        try:
            callbacks.on_step("Reading manifest")

            if not manifest_path.exists():
                callbacks.on_error("Manifest not found", str(manifest_path))
                return result

            manifest_data = json.loads(manifest_path.read_text())
            files = manifest_data.get("files", [])

            if not files:
                callbacks.on_log("No Jump List files to ingest", "warning")
                status = "success"
                return result

            total_entries = 0
            total_urls = 0

            # Clear previous run data
            self._clear_previous_run(evidence_conn, evidence_id, run_id)

            # Load browser AppIDs
            browser_appids = load_browser_appids()

            callbacks.on_progress(0, len(files), "Parsing Jump List files")

            for i, file_entry in enumerate(files):
                if callbacks.is_cancelled():
                    status = "cancelled"
                    break

                if file_entry.get("copy_status") == "error":
                    continue

                callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry['filename']}")

                try:
                    # Always resolve path relative to output_dir (fixes portability when cases are moved)
                    extracted_path = file_entry.get("extracted_path", file_entry.get("filename", ""))
                    file_path = output_dir / Path(extracted_path).name

                    # Check if this is a browser Jump List
                    appid = file_entry.get("appid", "")
                    is_browser, browser_name = is_browser_jumplist(appid, browser_appids)
                    app_name = get_app_name(appid)

                    # Log what we're processing
                    filename = file_entry['filename']
                    is_standalone_lnk = filename.lower().endswith('.lnk')
                    source_type = file_entry.get("source_type", "")
                    
                    # Determine source description for logging
                    source_desc_map = {
                        SOURCE_TYPE_JUMPLIST_AUTO: f"{app_name} Jump List (Recent)",
                        SOURCE_TYPE_JUMPLIST_CUSTOM: f"{app_name} Jump List (Pinned)",
                        SOURCE_TYPE_RECENT_ITEM: "Recent Item",
                        SOURCE_TYPE_DESKTOP: "Desktop Shortcut",
                        SOURCE_TYPE_TASKBAR_PINNED: "Taskbar Pinned",
                        SOURCE_TYPE_START_MENU_PINNED: "Start Menu Pinned",
                        SOURCE_TYPE_QUICK_LAUNCH: "Quick Launch",
                    }
                    source_desc = source_desc_map.get(source_type, "LNK File")

                    if is_standalone_lnk:
                        callbacks.on_log(f"Processing {source_desc}: {filename}", "info")
                    else:
                        callbacks.on_log(f"Processing {app_name} Jump List", "info")

                    # Parse based on file type
                    if is_standalone_lnk:
                        # Standalone LNK file - parse directly
                        lnk_data = parse_lnk_data(file_path.read_bytes())
                        if lnk_data:
                            # Wrap in list with pin_status for consistency
                            lnk_data["pin_status"] = "recent_item"
                            entries = [lnk_data]
                        else:
                            entries = []
                    else:
                        # Jump List container (auto-detects OLE vs Custom format)
                        entries = parse_jumplist_file(file_path)

                    if not entries:
                        continue

                    # Process entries
                    discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

                    jl_records = []
                    url_records = []

                    for entry in entries:
                        # Try to extract URL from LNK (primarily useful for browsers)
                        url = entry.get("url")
                        if not url:
                            url = extract_url_from_lnk(entry)

                        # Extract title
                        # For standalone LNK: use the .lnk filename (e.g., "Faronics_DFS.lnk" -> "Faronics_DFS")
                        # For Jump List entries: use target filename
                        title = entry.get("title")
                        if not title:
                            if is_standalone_lnk:
                                # Use LNK filename without extension as title
                                title = filename.rsplit('.', 1)[0] if '.' in filename else filename
                            else:
                                target_path = entry.get("target_path", "")
                                if target_path:
                                    # Use filename as title (e.g., "report.docx")
                                    title = target_path.replace("\\", "/").split("/")[-1]

                        jl_record = {
                            "appid": appid if not is_standalone_lnk else "",
                            "browser": browser_name if is_browser else "",
                            "jumplist_path": file_entry["logical_path"],
                            "entry_id": entry.get("entry_id"),
                            "target_path": entry.get("target_path"),
                            "arguments": entry.get("arguments"),
                            "working_directory": entry.get("working_directory"),
                            "url": url,
                            "title": title,
                            "lnk_creation_time": entry.get("creation_time"),
                            "lnk_modification_time": entry.get("modification_time"),
                            # Prefer DestList access_time (Windows-level "last used")
                            # when available; fall back to LNK header access_time
                            "lnk_access_time": (
                                entry.get("destlist_access_time")
                                or entry.get("access_time")
                            ),
                            "access_count": entry.get("access_count"),
                            # pin_status: use source_type for LNK files, or entry pin_status for Jump Lists
                            "pin_status": source_type if is_standalone_lnk else entry.get("pin_status", "recent"),
                            "run_id": run_id,
                            "source_path": file_entry["logical_path"],
                            "discovered_by": discovered_by,
                            "partition_index": file_entry.get("partition_index"),
                        }
                        jl_records.append(jl_record)

                        # Add any valid URLs to the urls table (forensically valuable from any app)
                        if url:
                            domain = self._extract_domain(url)
                            url_records.append({
                                "url": url,
                                "domain": domain,
                                "scheme": url.split("://")[0] if "://" in url else None,
                                "discovered_by": discovered_by,
                                "source_path": file_entry["logical_path"],
                                "notes": f"From {source_desc}",
                                "run_id": run_id,
                            })

                    # Insert records
                    if jl_records:
                        count = self._insert_jumplist_entries(
                            evidence_conn, evidence_id, jl_records
                        )
                        total_entries += count

                    if url_records:
                        insert_urls(evidence_conn, evidence_id, url_records)
                        total_urls += len(url_records)

                    callbacks.on_log(
                        f"Extracted {len(jl_records)} entries" +
                        (f", {len(url_records)} URLs" if url_records else ""),
                        "info"
                    )

                except Exception as e:
                    LOGGER.error("Failed to ingest %s: %s", file_entry['filename'], e, exc_info=True)
                    callbacks.on_error(f"Ingestion failed: {file_entry['filename']}", str(e))

            evidence_conn.commit()

            result = {"jump_list_entries": total_entries, "urls": total_urls}
            status = "success"

            LOGGER.info(
                "Jump Lists ingestion completed: entries=%d, urls=%d, duration=%.2fs",
                total_entries,
                total_urls,
                (datetime.now(timezone.utc) - start_time).total_seconds(),
            )

            return result

        except Exception as e:
            status = "failed"
            LOGGER.error("Jump Lists ingestion failed: %s", e, exc_info=True)
            raise

        finally:
            if stats:
                entries = result.get("jump_list_entries", 0)
                urls = result.get("urls", 0)
                stats.report_ingested(evidence_id, self.metadata.name, entries=entries, urls=urls)
                stats.finish_run(evidence_id, self.metadata.name, status)

    # Helper Methods

    def _generate_run_id(self) -> str:
        """Generate unique run ID: timestamp + UUID4 prefix."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        return f"{ts}_{uid}"

    def _discover_jumplist_files(
        self,
        evidence_fs,
        callbacks: ExtractorCallbacks,
        evidence_conn=None,
        evidence_id: int = 1,
    ) -> List[Dict]:
        """
        Scan evidence for Jump List files with multi-partition support.
        
        Uses file_list table for fast discovery across ALL partitions.
        Falls back to iter_paths if file_list is empty (e.g., mounted paths).
        
        Args:
            evidence_fs: Evidence filesystem interface
            callbacks: Progress callbacks
            evidence_conn: Evidence database connection (for file_list query)
            evidence_id: Evidence ID (for file_list query)
        
        Returns:
            List of file info dicts with logical_path, filename, appid, user, 
            source_type, and partition_index.
        """
        jumplist_files = []
        seen_paths = set()  # Deduplicate across patterns
        
        # Try file_list discovery first (enables multi-partition support)
        use_file_list = False
        if evidence_conn is not None:
            try:
                available, count = check_file_list_available(evidence_conn, evidence_id)
                if available:
                    callbacks.on_log(f"Using file_list for discovery ({count} files indexed)", "info")
                    use_file_list = True
                else:
                    callbacks.on_log("file_list empty, using filesystem scan", "info")
            except Exception as e:
                LOGGER.debug("file_list check failed: %s", e)
        
        if use_file_list:
            # Query file_list for each pattern type
            for pattern, source_type, path_pattern, filename_pattern in JUMPLIST_PATTERNS:
                try:
                    result = discover_from_file_list(
                        evidence_conn,
                        evidence_id,
                        filename_patterns=[filename_pattern],
                        path_patterns=[path_pattern],
                    )
                    
                    for match in result.get_all_matches():
                        path_str = match.file_path
                        
                        # Filter paths more precisely using the glob pattern
                        # path_pattern is a broad SQL LIKE pattern, need to verify
                        if not self._matches_source_type(path_str, source_type):
                            continue
                        
                        if path_str in seen_paths:
                            continue
                        seen_paths.add(path_str)
                        
                        filename = match.file_name
                        appid = filename.split('.')[0] if not filename.lower().endswith('.lnk') else ""
                        user = self._extract_user_from_path(path_str)
                        
                        jumplist_files.append({
                            "logical_path": path_str,
                            "filename": filename,
                            "appid": appid,
                            "user": user,
                            "source_type": source_type,
                            "partition_index": match.partition_index,
                            "inode": match.inode,
                        })
                        
                        callbacks.on_log(f"Found {source_type}: {filename}", "debug")
                        
                except Exception as e:
                    LOGGER.debug("file_list query for %s failed: %s", source_type, e)
            
            if jumplist_files:
                # Log partition distribution
                partitions = set(f["partition_index"] for f in jumplist_files)
                callbacks.on_log(
                    f"Discovered {len(jumplist_files)} files across {len(partitions)} partition(s)",
                    "info"
                )
        
        # Fallback to iter_paths if file_list didn't find anything  
        if not jumplist_files:
            callbacks.on_log("Using filesystem scan for discovery", "info")
            
            for pattern, source_type, _, _ in JUMPLIST_PATTERNS:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        if path_str in seen_paths:
                            continue
                        seen_paths.add(path_str)
                        
                        filename = path_str.split('/')[-1]
                        appid = filename.split('.')[0] if not filename.lower().endswith('.lnk') else ""
                        user = self._extract_user_from_path(path_str)
                        
                        jumplist_files.append({
                            "logical_path": path_str,
                            "filename": filename,
                            "appid": appid,
                            "user": user,
                            "source_type": source_type,
                            "partition_index": None,  # Unknown from iter_paths
                        })
                        
                        callbacks.on_log(f"Found {source_type}: {filename}", "debug")
                        
                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)
        
        callbacks.on_log(f"Total: {len(jumplist_files)} Jump List files found", "info")
        return jumplist_files
    
    def _matches_source_type(self, path: str, source_type: str) -> bool:
        """
        Verify a path matches the expected source type.
        
        Used to filter file_list results more precisely since SQL LIKE
        patterns can be overly broad.
        """
        path_lower = path.lower()
        
        if source_type == SOURCE_TYPE_JUMPLIST_AUTO:
            return "/automaticdestinations/" in path_lower and path_lower.endswith(".automaticdestinations-ms")
        elif source_type == SOURCE_TYPE_JUMPLIST_CUSTOM:
            return "/customdestinations/" in path_lower and path_lower.endswith(".customdestinations-ms")
        elif source_type == SOURCE_TYPE_RECENT_ITEM:
            # Recent Items: in Recent folder but NOT in AutomaticDestinations or CustomDestinations
            return (
                "/recent/" in path_lower and 
                path_lower.endswith(".lnk") and
                "/automaticdestinations/" not in path_lower and
                "/customdestinations/" not in path_lower
            )
        elif source_type == SOURCE_TYPE_DESKTOP:
            return "/desktop/" in path_lower and path_lower.endswith(".lnk")
        elif source_type == SOURCE_TYPE_TASKBAR_PINNED:
            return "/user pinned/taskbar/" in path_lower and path_lower.endswith(".lnk")
        elif source_type == SOURCE_TYPE_START_MENU_PINNED:
            return "/user pinned/startmenu/" in path_lower and path_lower.endswith(".lnk")
        elif source_type == SOURCE_TYPE_QUICK_LAUNCH:
            # Quick Launch but NOT in User Pinned subfolders
            return (
                "/quick launch/" in path_lower and
                path_lower.endswith(".lnk") and
                "/user pinned/" not in path_lower
            )
        return True

    def _extract_user_from_path(self, path: str) -> str:
        """Extract Windows username from path."""
        parts = path.split('/')
        try:
            idx = parts.index("Users")
            return parts[idx + 1] if idx + 1 < len(parts) else "unknown"
        except ValueError:
            try:
                idx = parts.index("Documents and Settings")
                return parts[idx + 1] if idx + 1 < len(parts) else "unknown"
            except ValueError:
                return "unknown"

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Copy Jump List file to workspace."""
        try:
            source_path = file_info["logical_path"]
            filename = file_info["filename"]
            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path}", "info")

            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()

            # Check if browser Jump List
            browser_appids = load_browser_appids()
            is_browser, browser = is_browser_jumplist(file_info["appid"], browser_appids)

            return {
                "copy_status": "ok",
                "size_bytes": len(file_content),
                "md5": md5,
                "sha256": sha256,
                # Store relative path (filename only) for portability - absolute paths break when cases are moved
                "extracted_path": filename,
                "filename": filename,
                "appid": file_info["appid"],
                "user": file_info["user"],
                "logical_path": source_path,
                "is_browser": is_browser,
                "browser": browser,
                "source_type": file_info.get("source_type", ""),
                "partition_index": file_info.get("partition_index"),
            }

        except Exception as e:
            callbacks.on_log(f"Failed to extract {file_info['logical_path']}: {e}", "error")
            return {
                "copy_status": "error",
                "error_message": str(e),
                "filename": file_info.get("filename"),
                "appid": file_info.get("appid"),
                "logical_path": file_info.get("logical_path"),
            }

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        """Clear data from previous runs before ingestion.

        Clears ALL Jump List entries for this evidence, not just the current run_id,
        to avoid UNIQUE constraint violations when re-ingesting.
        """
        try:
            # Clear all Jump List entries for this evidence (re-ingestion scenario)
            cursor = evidence_conn.execute(
                "DELETE FROM jump_list_entries WHERE evidence_id = ?",
                (evidence_id,)
            )
            deleted = cursor.rowcount

            # Clear associated URLs (discovered_by starts with system_jump_lists)
            cursor = evidence_conn.execute(
                "DELETE FROM urls WHERE evidence_id = ? AND discovered_by LIKE 'system_jump_lists:%'",
                (evidence_id,)
            )

            if deleted > 0:
                LOGGER.info("Cleared %d Jump List entries from previous ingestion", deleted)
        except Exception as e:
            LOGGER.warning("Failed to clear previous run: %s", e)

    def _insert_jumplist_entries(
        self,
        evidence_conn,
        evidence_id: int,
        records: List[Dict]
    ) -> int:
        """Insert Jump List entries into database."""
        from core.database import insert_jump_list_entries
        return insert_jump_list_entries(evidence_conn, evidence_id, records)

    def _extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL."""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc.lower() if parsed.netloc else None
        except Exception:
            return None
