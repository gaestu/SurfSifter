"""
System Jump Lists & Recent Items Extractor

Windows Jump Lists and Recent Items extraction with StatisticsCollector integration.

Jump Lists contain "Recent" and "Frequent" items from taskbar applications:
- For browsers: visited URLs that may survive history clearing
- For other apps: recently opened files (Word docs, media files, etc.)

Recent Items are standalone LNK shortcuts created when files are opened.

Supported formats:
- AutomaticDestinations-ms: OLE compound files (recent/frequent items)
- CustomDestinations-ms: Concatenated LNK files (pinned items)
- Standalone .lnk files: Individual recent item shortcuts

Location: %APPDATA%/Microsoft/Windows/Recent/AutomaticDestinations/
          %APPDATA%/Microsoft/Windows/Recent/CustomDestinations/
          %APPDATA%/Microsoft/Windows/Recent/*.lnk
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

LOGGER = logging.getLogger(__name__)


# Path patterns for Jump List and Recent Item files
# AutomaticDestinations = recent/frequent items (MRU/MFU)
# CustomDestinations = user-pinned items (Tasks section)
# Standalone .lnk = individual recent item shortcuts
JUMPLIST_PATTERNS = [
    # AutomaticDestinations (recent/frequent)
    "Users/*/AppData/Roaming/Microsoft/Windows/Recent/AutomaticDestinations/*.automaticDestinations-ms",
    "Documents and Settings/*/Application Data/Microsoft/Windows/Recent/AutomaticDestinations/*.automaticDestinations-ms",
    # CustomDestinations (pinned tasks) - different OLE structure but same forensic value
    "Users/*/AppData/Roaming/Microsoft/Windows/Recent/CustomDestinations/*.customDestinations-ms",
    "Documents and Settings/*/Application Data/Microsoft/Windows/Recent/CustomDestinations/*.customDestinations-ms",
    # Standalone LNK files (Recent Items - created when any file is opened)
    "Users/*/AppData/Roaming/Microsoft/Windows/Recent/*.lnk",
    "Documents and Settings/*/Recent/*.lnk",
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
            jumplist_files = self._discover_jumplist_files(evidence_fs, callbacks)

            if not jumplist_files:
                manifest_data["status"] = "skipped"
                manifest_data["notes"].append("No Jump List files found (non-Windows image?)")
                LOGGER.info("No Jump List files found")
                status = "success"  # Not finding files is not an error
            else:
                callbacks.on_progress(0, len(jumplist_files), "Copying Jump List files")

                for i, file_info in enumerate(jumplist_files):
                    if callbacks.is_cancelled():
                        manifest_data["status"] = "cancelled"
                        status = "cancelled"
                        break

                    try:
                        callbacks.on_progress(
                            i + 1, len(jumplist_files), f"Copying {file_info['filename']}"
                        )

                        extracted_file = self._extract_file(
                            evidence_fs,
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
                    file_path = Path(file_entry["extracted_path"])
                    if not file_path.is_absolute():
                        file_path = output_dir / file_path

                    # Check if this is a browser Jump List
                    appid = file_entry.get("appid", "")
                    is_browser, browser_name = is_browser_jumplist(appid, browser_appids)
                    app_name = get_app_name(appid)

                    # Log what we're processing
                    filename = file_entry['filename']
                    is_standalone_lnk = filename.lower().endswith('.lnk')

                    if is_standalone_lnk:
                        callbacks.on_log(f"Processing Recent Item: {filename}", "info")
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
                            "lnk_access_time": entry.get("access_time"),
                            "access_count": entry.get("access_count"),
                            "pin_status": entry.get("pin_status", "recent"),
                            "run_id": run_id,
                            "source_path": file_entry["logical_path"],
                            "discovered_by": discovered_by,
                        }
                        jl_records.append(jl_record)

                        # Add any valid URLs to the urls table (forensically valuable from any app)
                        if url:
                            domain = self._extract_domain(url)
                            source_desc = "Recent Item" if is_standalone_lnk else f"{app_name} Jump List"
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
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """Scan evidence for Jump List files."""
        jumplist_files = []

        for pattern in JUMPLIST_PATTERNS:
            try:
                for path_str in evidence_fs.iter_paths(pattern):
                    filename = path_str.split('/')[-1]

                    # Extract AppID from filename (format: {appid}.automaticDestinations-ms)
                    appid = filename.split('.')[0]

                    # Extract user from path
                    user = self._extract_user_from_path(path_str)

                    jumplist_files.append({
                        "logical_path": path_str,
                        "filename": filename,
                        "appid": appid,
                        "user": user,
                    })

                    callbacks.on_log(f"Found Jump List: {filename}", "info")

            except Exception as e:
                LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return jumplist_files

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
                "extracted_path": str(dest_path),
                "filename": filename,
                "appid": file_info["appid"],
                "user": file_info["user"],
                "logical_path": source_path,
                "is_browser": is_browser,
                "browser": browser,
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
