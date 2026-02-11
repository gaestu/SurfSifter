"""
Chromium Cookies Extractor

Extracts browser cookies from all Chromium-based browsers (Chrome, Edge, Brave, Opera).
Uses local parsers and schemas for cookie-specific handling.

Features:
- Chromium-only (Firefox/Safari excluded - use FirefoxCookiesExtractor)
- Encrypted cookie detection (DPAPI on Windows, Keychain on macOS)
- SameSite attribute parsing with raw value preservation
- Schema warning support for unknown columns and enum values
- StatisticsCollector integration for run tracking
- Browser selection config widget (Chromium browsers only)

Cookie Schema:
- host_key, name, value, path
- creation_utc, expires_utc, last_access_utc (WebKit timestamps)
- is_secure, is_httponly, samesite, is_persistent
- encrypted_value (raw bytes for forensic preservation)

 Changes:
- Added schema warning support for unknown columns and samesite values
- Split into _schemas.py and _parsers.py for better modularity
- Fixed: Pass samesite_raw to database helper
- Fixed: Delete previous run data before ingestion
- Fixed: Browser detection in multi-partition discovery
- Fixed: Removed unused shutil import
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
from ...._shared.sqlite_helpers import safe_sqlite_connect, SQLiteReadError
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    open_partition_for_extraction,
)
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
# Use local parsers for cookie-specific handling with schema warnings
from ._parsers import parse_cookies, get_cookie_stats
from ._schemas import KNOWN_COOKIES_TABLES, COOKIES_TABLE_PATTERNS
from extractors._shared.extraction_warnings import (
    ExtractionWarningCollector,
    discover_unknown_tables,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database.helpers.cookies import delete_cookies_by_run

LOGGER = get_logger("extractors.browser.chromium.cookies")


class ChromiumCookiesExtractor(BaseExtractor):
    """
    Extract cookie databases from Chromium-based browsers.

    Supports Chrome, Edge, Brave, Opera. All use identical SQLite schema.
    Firefox and Safari are handled by separate family extractors.

    Dual-phase workflow:
    - Extraction: Scans filesystem, copies Cookies files to workspace
    - Ingestion: Parses SQLite databases, inserts with forensic fields

    Features:
    - Per-cookie extraction with all metadata
    - Encrypted cookie detection and preservation
    - WebKit timestamp conversion to ISO 8601
    - Schema warning support for unknown columns
    - StatisticsCollector integration for run tracking
    - Browser selection config widget
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="chromium_cookies",
            display_name="Chromium Cookies",
            description="Extract browser cookies from Chrome, Edge, Brave, Opera",
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
        Return configuration widget (browser selection + partition config).

        Uses BrowserConfigWidget with Chromium browsers only.
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
            status_text = f"Chromium Cookies\nFiles: {file_count}\nRun: {data.get('run_id', 'N/A')[:20]}"
        else:
            status_text = "Chromium Cookies\nNo extraction yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "chromium_cookies"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Chromium cookie databases from evidence.

        Workflow:
            1. Generate run_id
            2. Scan evidence for Chromium cookie files
            3. Copy matching files to output_dir/
            4. Calculate hashes, collect E01 context
            5. Write manifest.json
        """
        callbacks.on_step("Initializing Chromium cookies extraction")

        # Generate run_id
        run_id = self._generate_run_id()
        LOGGER.info("Starting Chromium cookies extraction (run_id=%s)", run_id)

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Initialize manifest
        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.0.0",  # Multi-partition support
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "multi_partition": config.get("scan_all_partitions", True),
            "files": [],
            "status": "ok",
            "notes": [],
        }

        # Determine which browsers to search
        browsers = config.get("browsers") or config.get("selected_browsers") or get_all_browsers()

        # Determine multi-partition mode
        scan_all_partitions = config.get("scan_all_partitions", True)
        evidence_db_path = config.get("evidence_db_path")

        # Scan for cookie files (multi-partition aware)
        callbacks.on_step("Scanning for Chromium cookie databases")

        if scan_all_partitions and evidence_db_path:
            # Use file_list discovery for multi-partition support
            files_by_partition = self._discover_files_multi_partition(
                evidence_db_path, evidence_id, browsers, callbacks
            )
        else:
            # Single partition fallback
            files_by_partition = {
                None: self._discover_files(evidence_fs, browsers, callbacks)
            }

        # Count total files
        total_files = sum(len(files) for files in files_by_partition.values())

        # Report discovered files
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=total_files)

        callbacks.on_log(f"Found {total_files} cookie database(s)")

        # Extract each file
        file_index = 0
        for partition_index, files in files_by_partition.items():
            # Get partition-specific filesystem
            with open_partition_for_extraction(evidence_fs, partition_index) as partition_fs:
                if partition_fs is None:
                    LOGGER.warning("Cannot open partition %s", partition_index)
                    manifest_data["notes"].append(f"Failed to open partition {partition_index}")
                    continue

                for source_path, browser in files:
                    if callbacks.is_cancelled():
                        manifest_data["status"] = "cancelled"
                        manifest_data["notes"].append("Extraction cancelled by user")
                        break

                    file_index += 1
                    callbacks.on_progress(file_index, total_files, f"Extracting {source_path}")

                    try:
                        file_info = self._extract_file(
                            partition_fs, source_path, output_dir, browser, run_id,
                            partition_index=partition_index
                        )
                        manifest_data["files"].append(file_info)
                    except Exception as e:
                        LOGGER.warning("Failed to extract %s: %s", source_path, e)
                        manifest_data["notes"].append(f"Failed: {source_path}: {e}")
                        if stats:
                            stats.report_failed(evidence_id, self.metadata.name, files=1)

            if callbacks.is_cancelled():
                break

        # Finish statistics
        if stats:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            stats.finish_run(evidence_id, self.metadata.name, status=status)

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
        Ingest extracted cookie databases into evidence database.

        Workflow:
            1. Load manifest.json
            2. Delete previous run data (if re-running)
            3. Create warning collector for schema discovery
            4. For each extracted file:
               - Parse SQLite database with schema warnings
               - Insert cookies with forensic context
            5. Flush warnings and return summary statistics
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
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Delete previous run data before re-ingesting
        try:
            delete_cookies_by_run(evidence_conn, evidence_id, run_id)
        except Exception as e:
            LOGGER.debug("No previous run data to delete: %s", e)

        total_cookies = 0
        total_encrypted = 0
        failed_files = 0

        files = manifest_data.get("files", [])
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
                count, encrypted = self._ingest_file(
                    local_path, evidence_conn, evidence_id, file_info, run_id, callbacks,
                    warning_collector=warning_collector,
                )
                total_cookies += count
                total_encrypted += encrypted
            except Exception as e:
                LOGGER.warning("Failed to ingest %s: %s", local_path, e)
                callbacks.on_log(f"Failed to parse {local_path.name}: {e}", level="warning")
                failed_files += 1
                if stats:
                    stats.report_failed(evidence_id, self.metadata.name, files=1)
                # Record file corruption in warnings
                warning_collector.add_file_corrupt(
                    filename=str(local_path),
                    error=str(e),
                    artifact_type="cookies",
                )

        # Flush warnings to database
        try:
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info("Recorded %d extraction warnings for cookies", warning_count)
                callbacks.on_log(f"Schema warnings: {warning_count} items detected")
        except Exception as e:
            LOGGER.warning("Failed to flush extraction warnings: %s", e)

        # Report ingestion stats
        if stats:
            stats.report_ingested(
                evidence_id, self.metadata.name,
                records=total_cookies,
                cookies=total_cookies,
            )
            status = "success" if failed_files == 0 else "partial"
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_step("Ingestion complete")

        return {
            "cookies": total_cookies,
            "encrypted": total_encrypted,
            "failed_files": failed_files,
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
        Discover cookie files in evidence filesystem (single partition).

        Returns:
            List of (source_path, browser) tuples
        """
        found_files = []

        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                continue

            patterns = get_patterns(browser, "cookies")

            for pattern in patterns:
                try:
                    matches = list(evidence_fs.iter_paths(pattern))
                    for match in matches:
                        found_files.append((match, browser))
                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return found_files

    def _discover_files_multi_partition(
        self,
        evidence_db_path: str,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> Dict[Optional[int], List[tuple]]:
        """
        Discover cookie files across all partitions using file_list table.

        Returns:
            Dict mapping partition_index -> list of (source_path, browser) tuples.
            partition_index=None means use default evidence_fs.
        """
        # Collect all patterns and map them to browsers
        all_patterns = []
        browser_for_pattern: Dict[str, str] = {}

        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                continue
            patterns = get_patterns(browser, "cookies")
            for pattern in patterns:
                all_patterns.append(pattern)
                browser_for_pattern[pattern] = browser

        if not all_patterns:
            return {}

        # Query file_list
        result = discover_from_file_list(
            evidence_db_path=evidence_db_path,
            evidence_id=evidence_id,
            patterns=all_patterns,
            partition_index=None,  # All partitions
        )

        callbacks.on_log(f"Multi-partition discovery: {result.total_matches} files across {len(result.by_partition)} partition(s)")

        # Group by partition with browser info
        files_by_partition: Dict[Optional[int], List[tuple]] = {}

        for match in result.matches:
            partition = match.partition_index
            if partition not in files_by_partition:
                files_by_partition[partition] = []

            # Improved browser detection using detect_browser_from_path
            browser = detect_browser_from_path(match.path)
            if browser is None:
                # Fallback to pattern-based detection if path detection fails
                browser = self._detect_browser_from_path_patterns(match.path)

            files_by_partition[partition].append((match.path, browser))

        return files_by_partition

    def _detect_browser_from_path_patterns(self, path: str) -> str:
        """
        Fallback browser detection using path string matching.

        Args:
            path: File path from evidence

        Returns:
            Browser key or "unknown"
        """
        path_lower = path.lower()

        # Check for browser-specific path components (case-insensitive)
        browser_markers = [
            ("google/chrome", "chrome"),
            ("google-chrome", "chrome"),
            ("microsoft/edge", "edge"),
            ("microsoft-edge", "edge"),
            ("bravesoftware/brave-browser", "brave"),
            ("brave-browser", "brave"),
            ("opera software/opera", "opera"),
            ("opera stable", "opera"),
            ("opera gx", "opera_gx"),
            ("com.operasoftware.opera", "opera"),
            (".config/opera", "opera"),
            ("chromium", "chromium"),
        ]

        for marker, browser in browser_markers:
            if marker in path_lower:
                return browser

        return "unknown"

    def _extract_file(
        self,
        evidence_fs,
        source_path: str,
        output_dir: Path,
        browser: str,
        run_id: str,
        partition_index: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Extract a single cookie file from evidence.

        Returns:
            File info dict for manifest
        """
        # Generate safe local filename (include partition for uniqueness)
        safe_name = source_path.replace("/", "_").replace("\\", "_")
        partition_prefix = f"p{partition_index}_" if partition_index is not None else ""
        local_filename = f"{partition_prefix}{browser}_{safe_name}"
        local_path = output_dir / local_filename

        # Copy file from evidence using read_file
        file_content = evidence_fs.read_file(source_path)
        local_path.write_bytes(file_content)

        # Copy companion files (WAL, journal, shm) for SQLite recovery
        for suffix in ["-wal", "-journal", "-shm"]:
            companion_path = source_path + suffix
            try:
                companion_content = evidence_fs.read_file(companion_path)
                companion_dest = Path(str(local_path) + suffix)
                companion_dest.write_bytes(companion_content)
            except Exception:
                pass  # Companion doesn't exist

        # Calculate hash
        md5_hash = hashlib.md5(file_content).hexdigest()
        sha256_hash = hashlib.sha256(file_content).hexdigest()

        # Get profile from path
        profile = extract_profile_from_path(source_path)

        result = {
            "source_path": source_path,
            "local_filename": local_filename,
            "browser": browser,
            "profile": profile,
            "md5": md5_hash,
            "sha256": sha256_hash,
            "size_bytes": local_path.stat().st_size,
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

        if partition_index is not None:
            result["partition_index"] = partition_index

        return result

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
    ) -> tuple:
        """
        Ingest a single cookie database.

        Args:
            local_path: Path to extracted cookie database
            evidence_conn: Evidence database connection
            evidence_id: Evidence ID
            file_info: File metadata from manifest
            run_id: Extraction run ID
            callbacks: Extractor callbacks
            warning_collector: Optional collector for schema warnings

        Returns:
            (cookie_count, encrypted_count)
        """
        from core.database import insert_cookie_row

        browser = file_info.get("browser", "unknown")
        profile = file_info.get("profile", "Default")
        source_path = file_info.get("source_path", "")

        count = 0
        encrypted = 0

        with safe_sqlite_connect(local_path) as conn:
            # Discover unknown tables for schema warnings
            if warning_collector:
                try:
                    unknown_tables = discover_unknown_tables(
                        conn, KNOWN_COOKIES_TABLES, COOKIES_TABLE_PATTERNS
                    )
                    for table_info in unknown_tables:
                        warning_collector.add_unknown_table(
                            table_name=table_info["name"],
                            columns=table_info["columns"],
                            source_file=source_path,
                            artifact_type="cookies",
                        )
                except Exception as e:
                    LOGGER.debug("Failed to discover unknown tables: %s", e)

            # Parse cookies with schema warning support
            for cookie in parse_cookies(
                conn,
                warning_collector=warning_collector,
                source_file=source_path,
            ):
                try:
                    insert_cookie_row(
                        evidence_conn,
                        evidence_id=evidence_id,
                        browser=browser,
                        name=cookie.name,
                        domain=cookie.host_key,  # Chromium uses host_key for domain
                        profile=profile,
                        value=cookie.value,
                        path=cookie.path,
                        creation_utc=cookie.creation_utc_iso,
                        expires_utc=cookie.expires_utc_iso,
                        last_access_utc=cookie.last_access_utc_iso,
                        is_secure=1 if cookie.is_secure else 0,
                        is_httponly=1 if cookie.is_httponly else 0,
                        samesite=cookie.samesite,
                        samesite_raw=cookie.samesite_raw,  # Pass raw value
                        encrypted=1 if cookie.is_encrypted else 0,
                        encrypted_value=cookie.encrypted_value,
                        source_path=source_path,
                        discovered_by=f"{self.metadata.name}:{self.metadata.version}:{run_id}",
                        run_id=run_id,
                    )
                    count += 1
                    if cookie.is_encrypted:
                        encrypted += 1
                except Exception as e:
                    LOGGER.debug("Failed to insert cookie: %s", e)

        return count, encrypted
