"""
Chromium Transport Security Extractor

Extracts HSTS (HTTP Strict Transport Security) entries from Chromium browsers.
These artifacts persist after history clearing, providing forensic evidence
of visited domains.

Features:
- TransportSecurity JSON parsing
- SHA256+Base64 hashed domain storage
- Multi-partition support via file_list discovery
- StatisticsCollector integration for run tracking
- Extraction warning support for unknown JSON keys

Data Format:
- Chromium stores HSTS in TransportSecurity JSON file
- Domains are SHA256 hashed (cannot be directly decoded)
- Contains sts_observed and expiry timestamps

Forensic Value:
- Entries persist after "Clear History"
- Hashed domains can be decoded via external rainbow tables or cross-referencing
- Reveals secure connections to specific domains

Multi-partition support, file overwrite fix, cleanup
Initial release with statistics tracking
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from PySide6.QtWidgets import QLabel, QWidget

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import BrowserConfigWidget
from ...._shared.file_list_discovery import (
    FileListDiscoveryResult,
    check_file_list_available,
    discover_from_file_list,
    get_ewf_paths_from_evidence_fs,
    glob_to_sql_like,
    open_partition_for_extraction,
)
from .._parsers import detect_browser_from_path, extract_profile_from_path
from .._patterns import CHROMIUM_BROWSERS, get_artifact_patterns
from .._embedded_discovery import (
    discover_artifacts_with_embedded_roots,
    get_embedded_root_paths,
)
from core.database import (
    delete_hsts_by_run,
    insert_browser_inventory,
    insert_hsts_entries,
    update_inventory_ingestion_status,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.browser.chromium.transport_security")

# ============================================================================
# Known JSON keys for schema warning support
# ============================================================================

# Known top-level keys in TransportSecurity JSON
KNOWN_TOP_LEVEL_KEYS: Set[str] = {"sts", "pkp", "expect_ct", "version"}

# Known keys in STS entry dicts
KNOWN_STS_ENTRY_KEYS: Set[str] = {
    "host",
    "sts_observed",
    "expiry",
    "mode",
    "sts_include_subdomains",
}


def _parse_timestamp(value: Any) -> Optional[float]:
    """Parse timestamp value (may be float or string)."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


class ChromiumTransportSecurityExtractor(BaseExtractor):
    """
    Extract HSTS/Transport Security entries from Chromium browsers.

    Parses TransportSecurity JSON files containing SHA256-hashed domain entries.
    Supports Chrome, Edge, Brave, Opera with multi-partition discovery.
    """

    SUPPORTED_BROWSERS = list(CHROMIUM_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="chromium_transport_security",
            display_name="Chromium HSTS",
            description="Extract HSTS entries from Chrome/Edge/Opera/Brave (persist after history clearing)",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
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
        self, parent: QWidget, output_dir: Path, evidence_conn, evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction/ingestion state."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"Chromium HSTS\nFiles extracted: {file_count}\nRun ID: {data.get('run_id', 'N/A')}"
        else:
            status_text = "Chromium HSTS\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "chromium_transport_security"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        """Extract Chromium TransportSecurity files from evidence."""
        callbacks.on_step("Initializing Chromium transport security extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Chromium transport security extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.1.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_extraction_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "files": [],
            "status": "ok",
            "notes": [],
        }

        callbacks.on_step("Scanning for Chromium TransportSecurity files")

        browsers_to_search = config.get("browsers") or config.get(
            "selected_browsers", self.SUPPORTED_BROWSERS
        )
        browsers_to_search = [b for b in browsers_to_search if b in self.SUPPORTED_BROWSERS]

        # Discover files using file_list (fast) or iter_paths (mounted paths only)
        ts_files: List[Dict] = []

        if evidence_conn:
            # Forensic workflow: require file_list for efficient discovery
            file_list_available, file_count = check_file_list_available(evidence_conn, evidence_id)
            if file_list_available:
                callbacks.on_log(f"Using file_list discovery ({file_count} files indexed)", "info")
                ts_files = self._discover_files_multi_partition(
                    evidence_fs, evidence_conn, evidence_id, browsers_to_search, callbacks
                )
            else:
                # Don't fall back to slow iter_paths on E01 images
                callbacks.on_error(
                    "File list not populated",
                    "Run the 'File List' extractor first for efficient discovery. "
                    "Direct filesystem scanning on E01 images is too slow.",
                )
                if stats:
                    stats.finish_run(evidence_id, self.metadata.name, status="error")
                return False
        else:
            # No database connection (mounted path or testing) - iter_paths is acceptable
            callbacks.on_log("No evidence database, using direct filesystem scan", "info")
            ts_files = self._discover_files_single_partition(evidence_fs, browsers_to_search, callbacks)

        # Report discovered files (always, even if 0)
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(ts_files))

        callbacks.on_log(f"Found {len(ts_files)} TransportSecurity file(s)")

        if not ts_files:
            LOGGER.info("No Chromium TransportSecurity files found")
        else:
            callbacks.on_progress(0, len(ts_files), "Copying TransportSecurity files")

            # Get EWF paths for multi-partition extraction
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)

            for i, file_info in enumerate(ts_files):
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    manifest_data["notes"].append("Extraction cancelled by user")
                    break

                try:
                    callbacks.on_progress(
                        i + 1, len(ts_files), f"Copying {file_info['browser']} HSTS data"
                    )

                    partition_idx = file_info.get("partition_index")

                    # Open appropriate partition for extraction
                    with open_partition_for_extraction(
                        ewf_paths if partition_idx is not None else evidence_fs,
                        partition_idx,
                    ) as fs:
                        extracted_file = self._extract_file(fs, file_info, output_dir, callbacks)

                    manifest_data["files"].append(extracted_file)

                except Exception as e:
                    error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                    LOGGER.error(error_msg, exc_info=True)
                    manifest_data["notes"].append(error_msg)
                    if stats:
                        stats.report_failed(evidence_id, self.metadata.name, files=1)

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
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            run_id=run_id,
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
            manifest_data=manifest_data,
            callbacks=callbacks,
        )

        LOGGER.info(
            "Chromium transport security extraction complete: %d files, status=%s",
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
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, int]:
        """Parse extracted TransportSecurity files and ingest into database."""
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"hsts_entries": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])

        # Continue statistics tracking (same run_id from manifest)
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(
                    evidence_id,
                    self.metadata.name,
                    records=0,
                    hsts_entries=0,
                )
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"hsts_entries": 0}

        # Create warning collector for schema discovery
        from extractors._shared.extraction_warnings import ExtractionWarningCollector

        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        total_hsts = 0

        # Clear previous data for this run (using helper)
        deleted = delete_hsts_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d HSTS entries from previous run %s", deleted, run_id)

        callbacks.on_progress(0, len(files), "Parsing TransportSecurity files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            if file_entry.get("copy_status") == "error":
                callbacks.on_log(
                    f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}",
                    "warning",
                )
                continue

            callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry['browser']} HSTS data")

            try:
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry["browser"],
                    artifact_type="transport_security",
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
                    db_path = output_dir / db_path

                hsts_count = self._parse_and_insert_hsts(
                    db_path,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                    callbacks,
                    warning_collector=warning_collector,
                )

                total_hsts += hsts_count

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=hsts_count,
                )

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry['extracted_path']}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")

                if "inventory_id" in locals():
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(e),
                    )

        evidence_conn.commit()

        # Flush schema warnings to database
        try:
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                callbacks.on_log(f"Recorded {warning_count} extraction warnings", "info")
        except Exception as e:
            LOGGER.warning("Failed to flush extraction warnings: %s", e)

        # Report ingested counts and finish
        if stats:
            stats.report_ingested(
                evidence_id,
                self.metadata.name,
                records=total_hsts,
                hsts_entries=total_hsts,
            )
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {"hsts_entries": total_hsts}

    # ─────────────────────────────────────────────────────────────────
    # Helper Methods
    # ─────────────────────────────────────────────────────────────────

    def _generate_run_id(self) -> str:
        """Generate run ID: ts_chromium_{timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ts_chromium_{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context from evidence filesystem."""
        try:
            source_path = evidence_fs.source_path if hasattr(evidence_fs, "source_path") else None
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

    def _get_extraction_tool_version(self) -> str:
        """Build extraction tool version string."""
        try:
            import pytsk3

            pytsk_version = pytsk3.TSK_VERSION_STR
        except ImportError:
            pytsk_version = "unknown"

        return f"pytsk3:{pytsk_version}"

    # ─────────────────────────────────────────────────────────────────
    # File Discovery Methods
    # ─────────────────────────────────────────────────────────────────

    def _discover_files_single_partition(
        self, evidence_fs, browsers: List[str], callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """
        Discover TransportSecurity files using iter_paths (single partition).

        Fallback method when file_list is not available.
        """
        ts_files = []

        for browser_key in browsers:
            if browser_key not in CHROMIUM_BROWSERS:
                continue

            patterns = get_artifact_patterns(browser_key, "transport_security")
            display_name = CHROMIUM_BROWSERS[browser_key]["display_name"]

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        profile = extract_profile_from_path(path_str) or "Default"
                        user = self._extract_user_from_path(path_str)

                        ts_files.append(
                            {
                                "logical_path": path_str,
                                "browser": browser_key,
                                "profile": profile,
                                "user": user,
                                "file_type": "chromium",
                                "artifact_type": "transport_security",
                                "display_name": display_name,
                                "partition_index": None,  # Single partition mode
                                "fs_type": getattr(evidence_fs, "fs_type", None),
                            }
                        )

                        callbacks.on_log(f"Found {browser_key} TransportSecurity: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return ts_files

    def _discover_files_multi_partition(
        self,
        evidence_fs,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> List[Dict]:
        """
        Discover TransportSecurity files across ALL partitions using file_list.

        This provides comprehensive coverage for multi-partition disk images.
        """
        # Build filename pattern for TransportSecurity
        filename_patterns = ["TransportSecurity"]

        path_patterns = set()
        for browser_key in browsers:
            if browser_key not in CHROMIUM_BROWSERS:
                continue
            for pattern in get_artifact_patterns(browser_key, "transport_security"):
                path_patterns.add(glob_to_sql_like(pattern))

        if not path_patterns:
            return []

        result, embedded_roots = discover_artifacts_with_embedded_roots(
            evidence_conn,
            evidence_id,
            artifact="transport_security",
            filename_patterns=filename_patterns,
            path_patterns=sorted(path_patterns),
        )

        callbacks.on_log(
            f"Multi-partition discovery: {result.total_matches} files across "
            f"{len(result.partitions_with_matches)} partition(s)"
        )

        # Convert matches to file info dicts
        ts_files = []
        for partition_idx, matches in result.matches_by_partition.items():
            for match in matches:
                # Detect browser from path
                embedded_paths = get_embedded_root_paths(embedded_roots, partition_idx)
                browser = detect_browser_from_path(match.file_path, embedded_roots=embedded_paths)
                if browser is None:
                    browser = "chromium"
                if browser not in browsers and browser != "chromium_embedded":
                    continue

                profile = extract_profile_from_path(match.file_path) or "Default"
                user = self._extract_user_from_path(match.file_path)
                display_name = CHROMIUM_BROWSERS.get(browser, {}).get("display_name", "Embedded Chromium")

                ts_files.append(
                    {
                        "logical_path": match.file_path,
                        "browser": browser,
                        "profile": profile,
                        "user": user,
                        "file_type": "chromium",
                        "artifact_type": "transport_security",
                        "display_name": display_name,
                        "partition_index": partition_idx,
                        "inode": match.inode,
                        "size_bytes": match.size_bytes,
                    }
                )

                callbacks.on_log(
                    f"Found {browser} TransportSecurity (partition {partition_idx}): {match.file_path}",
                    "info",
                )

        return ts_files

    def _extract_user_from_path(self, path: str) -> Optional[str]:
        """
        Extract OS username from a file path.

        Handles Windows (Users/USERNAME) and Linux/macOS (home/USERNAME) patterns.
        """
        parts = path.replace("\\", "/").split("/")

        # Windows: Users/USERNAME/...
        if "Users" in parts:
            try:
                idx = parts.index("Users")
                if idx + 1 < len(parts):
                    user = parts[idx + 1]
                    # Skip common non-user directories
                    if user.lower() not in ("public", "default", "all users"):
                        return user
            except (ValueError, IndexError):
                pass

        # Linux/macOS: home/USERNAME/...
        if "home" in parts:
            try:
                idx = parts.index("home")
                if idx + 1 < len(parts):
                    return parts[idx + 1]
            except (ValueError, IndexError):
                pass

        return None

    # ─────────────────────────────────────────────────────────────────
    # File Extraction Methods
    # ─────────────────────────────────────────────────────────────────

    def _extract_file(
        self, evidence_fs, file_info: Dict, output_dir: Path, callbacks: ExtractorCallbacks
    ) -> Dict:
        """
        Copy file from evidence to workspace with unique filename.

        Filename includes partition, browser, user, and profile to prevent overwrites.
        """
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info["profile"]
            partition_idx = file_info.get("partition_index")
            user = file_info.get("user")

            # Build unique filename to prevent overwrites
            safe_profile = profile.replace(" ", "_").replace("/", "_")
            safe_user = (user or "unknown").replace(" ", "_").replace("/", "_")

            # Include partition and user for uniqueness
            partition_prefix = f"p{partition_idx}_" if partition_idx is not None else ""
            filename = f"{partition_prefix}{browser}_{safe_user}_{safe_profile}_TransportSecurity"
            dest_path = output_dir / filename

            # Handle potential collisions (same user+profile but different paths)
            counter = 0
            original_dest = dest_path
            while dest_path.exists():
                counter += 1
                dest_path = output_dir / f"{original_dest.stem}_{counter}{original_dest.suffix}"

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()
            size = len(file_content)

            return {
                "copy_status": "ok",
                "size_bytes": size,
                "file_size_bytes": size,
                "md5": md5,
                "sha256": sha256,
                "extracted_path": str(dest_path),
                "browser": browser,
                "profile": profile,
                "user": user,
                "file_type": "chromium",
                "logical_path": source_path,
                "artifact_type": "transport_security",
                "partition_index": partition_idx,
                "fs_type": file_info.get("fs_type"),
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
                "browser": file_info.get("browser"),
                "profile": file_info.get("profile"),
                "user": file_info.get("user"),
                "file_type": file_info.get("file_type"),
                "logical_path": file_info.get("logical_path"),
                "partition_index": file_info.get("partition_index"),
                "error_message": str(e),
            }

    # ─────────────────────────────────────────────────────────────────
    # Parsing Methods
    # ─────────────────────────────────────────────────────────────────

    def _parse_and_insert_hsts(
        self,
        file_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector=None,
    ) -> int:
        """Parse TransportSecurity file and insert records."""
        if not file_path.exists():
            LOGGER.warning("TransportSecurity file not found: %s", file_path)
            return 0

        # Parse JSON
        try:
            content = file_path.read_text(encoding="utf-8", errors="replace")
            data = json.loads(content)
        except json.JSONDecodeError as e:
            LOGGER.warning("Failed to parse TransportSecurity JSON: %s", e)
            if warning_collector:
                warning_collector.add_json_parse_error(
                    filename=str(file_path),
                    error=str(e),
                )
            return 0

        # Check for unknown top-level keys
        if warning_collector and isinstance(data, dict):
            unknown_keys = set(data.keys()) - KNOWN_TOP_LEVEL_KEYS
            for key in unknown_keys:
                warning_collector.add_warning(
                    warning_type="json_unknown_key",
                    category="json",
                    severity="info",
                    artifact_type="transport_security",
                    source_file=file_entry["logical_path"],
                    item_name=key,
                    item_value=str(type(data[key]).__name__),
                )

        browser = file_entry["browser"]
        profile = file_entry.get("profile", "Default")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        # Parse STS entries
        sts_list = data.get("sts", [])
        records = []

        for item in sts_list:
            if not isinstance(item, dict):
                continue

            hashed_host = item.get("host")
            if not hashed_host:
                continue

            # Check for unknown entry keys
            if warning_collector:
                unknown_entry_keys = set(item.keys()) - KNOWN_STS_ENTRY_KEYS
                for key in unknown_entry_keys:
                    warning_collector.add_warning(
                        warning_type="json_unknown_key",
                        category="json",
                        severity="info",
                        artifact_type="transport_security",
                        source_file=file_entry["logical_path"],
                        item_name=f"sts[].{key}",
                        item_value=str(type(item[key]).__name__),
                    )

            record = {
                "browser": browser,
                "profile": profile,
                "hashed_host": hashed_host,
                "sts_observed": _parse_timestamp(item.get("sts_observed")),
                "expiry": _parse_timestamp(item.get("expiry")),
                "mode": item.get("mode", "force-https"),
                "include_subdomains": 1 if item.get("sts_include_subdomains", False) else 0,
                "decoded_host": None,
                "decode_method": None,
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry["logical_path"],
                "forensic_path": file_entry.get("forensic_path"),
            }
            records.append(record)

        if records:
            count = insert_hsts_entries(evidence_conn, evidence_id, records)
            callbacks.on_log(f"Inserted {count} Chromium HSTS entries (hashed domains)", "info")
            return count

        return 0
