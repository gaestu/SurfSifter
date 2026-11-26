"""
Firefox Transport Security Extractor

Extracts HSTS (HTTP Strict Transport Security) entries from Firefox browsers.
Firefox stores domain names in cleartext - HIGH FORENSIC VALUE!

Features:
- SiteSecurityServiceState.txt parsing
- Cleartext domain extraction (unlike Chromium)
- URL table integration
- HSTS entries table with full metadata
- Discovery through the file list only, on all partitions (no filesystem scan)
- Schema warning support for unknown entry types/state values
- File collision prevention with unique naming

Data Format:
- Firefox stores HSTS in SiteSecurityServiceState.txt (tab-separated)
- Format: host:HSTS<tab>score<tab>last_access<tab>data
- Data field: expiry_ms,state,include_subdomains

Forensic Value:
- Contains CLEARTEXT domain names (HIGH VALUE)
- Entries persist after "Clear History"
- Reveals secure connections even when browsing history deleted

Changes:
- Multi-partition discovery via file_list
- Schema warnings for unknown entry types and state values
- File collision prevention with partition/user/profile naming
- Refactored to use _schemas.py and _parsers.py modules
- Fixed regex for host:type parsing (IPv6 compatible)
- Fixed delete pattern to use exact run_id match
"""

from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, TYPE_CHECKING

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import BrowserConfigWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from ...._shared.extraction_warnings import ExtractionWarningCollector
from .._patterns import (
    FIREFOX_BROWSERS,
    detect_browser_from_path,
    get_all_patterns,
    get_browser_display_name,
)
from ._schemas import (
    KNOWN_ENTRY_TYPES,
    KNOWN_STATE_VALUES,
    STATE_TO_MODE,
    WARNING_CATEGORY,
    WARNING_ARTIFACT_TYPE,
)
from ._parsers import (
    parse_transport_security_file,
    days_since_epoch_to_iso8601,
    ms_to_unix_seconds,
    unix_to_iso8601,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_urls,
    delete_urls_by_run,
    insert_hsts_entries,
    delete_hsts_by_run,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)

if TYPE_CHECKING:
    from core.evidence_fs import EvidenceFS

LOGGER = get_logger("extractors.browser.firefox.transport_security")

HSTS_FILENAME = "SiteSecurityServiceState.txt"

# Real files hold a few thousand short lines. The limit only keeps a planted or
# corrupt file of that name from being read into memory and copied.
MAX_HSTS_FILE_BYTES = 16 * 1024 * 1024

# Longest user or profile name used in the name of a copied file
MAX_NAME_COMPONENT_CHARS = 50


class FirefoxTransportSecurityExtractor(BaseExtractor):
    """
    Extract HSTS/Transport Security entries from Firefox browsers.

    Parses SiteSecurityServiceState.txt containing cleartext domain names.
    Supports Firefox, Tor Browser.

    HIGH FORENSIC VALUE: Unlike Chromium, Firefox stores domain names
    in cleartext, providing direct evidence of visited secure sites.

    Multi-partition support, schema warnings, collision prevention.
    """

    SUPPORTED_BROWSERS = list(FIREFOX_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="firefox_transport_security",
            display_name="Firefox HSTS",
            description="Extract HSTS entries from Firefox (cleartext domains - HIGH VALUE)",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True
        )

    def can_run_extraction(self, evidence_fs: "EvidenceFS") -> tuple[bool, str]:
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
        """Check if output directory has existing extraction output.

        The manifest of a run that ended with an error (for example: no file
        list) or was cancelled is not output, so that the next run extracts
        again instead of reusing an empty or incomplete result.
        """
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False
        try:
            return json.loads(manifest.read_text()).get("status") not in ("error", "cancelled")
        except (OSError, ValueError, AttributeError):
            return True

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget (browser selection)."""
        return BrowserConfigWidget(parent, supported_browsers=self.SUPPORTED_BROWSERS)

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
            status_text = f"Firefox HSTS\nFiles extracted: {file_count}\nRun ID: {data.get('run_id', 'N/A')}"
        else:
            status_text = "Firefox HSTS\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "firefox_transport_security"

    # ─────────────────────────────────────────────────────────────────
    # Extraction
    # ─────────────────────────────────────────────────────────────────

    def run_extraction(
        self,
        evidence_fs: "EvidenceFS",
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Firefox SiteSecurityServiceState.txt files from evidence.

        Files are found through the file list only, on all partitions. The
        filesystem is never scanned: a scan would cover the open partition
        alone. Without a file list the run ends with an error.
        """
        callbacks.on_step("Initializing Firefox transport security extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Firefox transport security extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.2.0",  # skipped_files; per file: listed_partition_index, local_filename, inode
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_extraction_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "files": [],
            "skipped_files": [],
            "status": "ok",
            "notes": [],
            "multi_partition": True,
        }

        callbacks.on_step("Looking up SiteSecurityServiceState.txt in the file list")

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        browsers_to_search = [b for b in browsers_to_search if b in self.SUPPORTED_BROWSERS]

        # The file list is the only source for discovery.
        file_list_available, file_count = (
            check_file_list_available(evidence_conn, evidence_id)
            if evidence_conn is not None
            else (False, 0)
        )

        if not file_list_available:
            return self._abort_extraction(
                "File list required",
                "No file list for this evidence. Firefox HSTS finds files through the "
                "file list and does not scan the filesystem. Run the File List extractor first.",
                manifest_data,
                output_dir,
                callbacks,
            )

        callbacks.on_log(f"Using file list for discovery ({file_count:,} files indexed)", "info")

        open_partition = getattr(evidence_fs, "partition_index", 0)
        if not isinstance(open_partition, int):
            open_partition = 0

        try:
            files_by_partition, rows_without_partition = self._discover_files(
                evidence_conn,
                evidence_id,
                browsers_to_search,
                open_partition,
                callbacks,
            )
        except Exception as e:
            LOGGER.error("File list discovery failed: %s", e, exc_info=True)
            return self._abort_extraction(
                "File list discovery failed",
                f"File list discovery for {HSTS_FILENAME} failed: {e}",
                manifest_data,
                output_dir,
                callbacks,
            )

        total_files = sum(len(files) for files in files_by_partition.values())

        if rows_without_partition:
            note = (
                f"{rows_without_partition} file list row(s) for {HSTS_FILENAME} have no partition "
                f"number. Their paths were looked up on the open filesystem only "
                f"(partition {open_partition})."
            )
            callbacks.on_log(note, "warning")
            manifest_data["notes"].append(note)

        # Report discovered files (even if 0)
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=total_files)

        if total_files == 0:
            note = f"No {HSTS_FILENAME} files found in the file list"
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append(note)
            callbacks.on_log(note, "info")
            LOGGER.info("No Firefox SiteSecurityServiceState.txt files found")
        else:
            self._extract_partitions(
                evidence_fs,
                open_partition,
                files_by_partition,
                total_files,
                output_dir,
                manifest_data,
                callbacks,
            )

            if manifest_data["status"] == "cancelled":
                note = (
                    f"Extraction cancelled after {len(manifest_data['files'])} of "
                    f"{total_files} file(s)"
                )
                callbacks.on_log(note, "warning")
                manifest_data["notes"].append(note)

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

        # Finish stats tracking
        if stats:
            status = {"cancelled": "cancelled", "ok": "success", "skipped": "success"}.get(
                manifest_data["status"], "partial"
            )
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        LOGGER.info(
            "Firefox transport security extraction complete: %d files, status=%s",
            len(manifest_data["files"]),
            manifest_data["status"],
        )

        return manifest_data["status"] != "error"

    # ─────────────────────────────────────────────────────────────────
    # Ingestion
    # ─────────────────────────────────────────────────────────────────

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """Parse extracted SiteSecurityServiceState.txt and ingest into database.

        Inserts records into both urls table (backward compatibility) and
        hsts_entries table (full metadata). Uses ExtractionWarningCollector
        for schema warnings about unknown entry types and state values.
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"urls": 0, "hsts_entries": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])

        # Continue statistics tracking with manifest's run_id
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, hsts_entries=0)
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"urls": 0, "hsts_entries": 0}

        # Initialize warning collector for schema warnings
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        total_urls = 0
        total_hsts = 0

        # Clear previous data for this run (exact run_id match)
        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        callbacks.on_progress(0, len(files), "Parsing SiteSecurityServiceState files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            if file_entry.get("copy_status") == "error":
                callbacks.on_log(
                    f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}",
                    "warning"
                )
                continue

            partition_idx = file_entry.get("partition_index", 0)
            callbacks.on_progress(
                i + 1,
                len(files),
                f"Parsing {file_entry['browser']} HSTS data (partition {partition_idx})"
            )

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

                # Parse file with warning collection
                entries = self._parse_file_with_warnings(
                    db_path,
                    file_entry,
                    warning_collector,
                )

                if not entries:
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="ok",
                        records_parsed=0,
                    )
                    continue

                # Insert into urls table (for backward compatibility)
                url_count = self._insert_urls(
                    entries,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                )

                # Insert into dedicated hsts_entries table with full metadata
                hsts_count = self._insert_hsts_entries(
                    entries,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                )

                total_urls += url_count
                total_hsts += hsts_count

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=url_count + hsts_count,
                )

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry['extracted_path']}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")

                if 'inventory_id' in locals():
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(e),
                    )

        # Flush collected warnings to database
        try:
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)
                callbacks.on_log(f"Schema warnings: {warning_count} items detected")
        except Exception as e:
            LOGGER.warning("Failed to flush extraction warnings: %s", e)

        evidence_conn.commit()

        # Report ingestion statistics
        if stats:
            stats.report_ingested(
                evidence_id,
                self.metadata.name,
                records=total_urls + total_hsts,
                hsts_entries=total_hsts
            )
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        callbacks.on_log(
            f"Inserted {total_urls} URLs and {total_hsts} HSTS entries (cleartext domains)",
            "info"
        )

        return {"urls": total_urls, "hsts_entries": total_hsts}

    # ─────────────────────────────────────────────────────────────────
    # Discovery Methods
    # ─────────────────────────────────────────────────────────────────

    def _discover_files(
        self,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        open_partition: int,
        callbacks: ExtractorCallbacks,
    ) -> tuple[Dict[int, List[Dict]], int]:
        """Find SiteSecurityServiceState.txt on all partitions through the file list.

        The partition of a file info dict is the partition the file is read
        from. A file list row without a partition number (-1, or 0 for a row
        without a value) cannot name a partition to open: its path is looked
        up on the open filesystem and gets that filesystem's partition index.
        ``listed_partition_index`` keeps the value from the file list.

        Returns:
            Tuple of (dict mapping partition_index -> list of file info dicts,
            both in ascending order; number of files from rows without a
            partition number).

        Raises:
            RuntimeError: If the file list query failed. An empty dict means
                the query ran and matched nothing.
        """
        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=[HSTS_FILENAME],
            path_patterns=get_all_patterns("transport_security"),
        )

        if result.error:
            raise RuntimeError(f"file list query failed: {result.error}")

        files_by_partition: Dict[int, List[Dict]] = {}
        seen: set[tuple[int, str]] = set()
        rows_without_partition = 0

        def has_partition_number(listed_partition: int) -> bool:
            return listed_partition > 0 or listed_partition == open_partition

        # Numbered rows first: if a file is also listed without a number, the
        # numbered row is the one that is kept.
        listed_partitions = sorted(
            result.matches_by_partition,
            key=lambda listed: (not has_partition_number(listed), listed),
        )

        for listed_partition in listed_partitions:
            has_number = has_partition_number(listed_partition)
            partition_index = listed_partition if has_number else open_partition
            matches = sorted(result.matches_by_partition[listed_partition], key=lambda m: m.file_path)

            for match in matches:
                # ESR and regular Firefox share their profile roots, so the
                # browser comes from the path, not from the pattern.
                browser = detect_browser_from_path(match.file_path)
                if browser not in browsers:
                    continue

                # The same file can be listed with and without a partition number
                key = (partition_index, match.file_path.lstrip("/"))
                if key in seen:
                    continue
                seen.add(key)

                if not has_number:
                    rows_without_partition += 1

                user, profile = self._extract_user_profile_from_path(match.file_path)

                files_by_partition.setdefault(partition_index, []).append({
                    "logical_path": match.file_path,
                    "browser": browser,
                    "user": user,
                    "profile": profile,
                    "file_type": "firefox",
                    "artifact_type": "transport_security",
                    "display_name": get_browser_display_name(browser),
                    "partition_index": partition_index,
                    # Differs from partition_index for a row without a number
                    "listed_partition_index": listed_partition,
                    # The inode of a row without a number may belong to a
                    # file on another partition
                    "inode": match.inode if has_number else None,
                })

                callbacks.on_log(
                    f"Found {browser} {HSTS_FILENAME}: "
                    f"partition {partition_index}, {match.file_path}",
                    "info"
                )

        for partition_files in files_by_partition.values():
            partition_files.sort(key=lambda f: f["logical_path"])

        return files_by_partition, rows_without_partition

    def _extract_user_profile_from_path(self, path: str) -> tuple[str, str]:
        """Extract user and profile from Firefox path.

        Returns:
            Tuple of (user, profile) - defaults to ("unknown", "Default") if not found
        """
        import re

        user = "unknown"
        profile = "Default"

        # Windows and macOS: Users/<user>/..., Linux: home/<user>/...
        # File list paths come with or without a leading slash.
        user_match = re.search(r'(?:^|/)(?:Users|home)/([^/]+)/', path, re.IGNORECASE)
        if user_match:
            user = user_match.group(1)

        # Profile extraction - Firefox uses random prefix like "abc123.default"
        profile_patterns = [
            r'Profiles/([^/]+)/',
            r'\.mozilla/firefox/([^/]+)/',
            r'TorBrowser/Data/Browser/([^/]+)/',
        ]

        for pattern in profile_patterns:
            match = re.search(pattern, path, re.IGNORECASE)
            if match:
                profile = match.group(1)
                break

        return user, profile

    # ─────────────────────────────────────────────────────────────────
    # File Extraction
    # ─────────────────────────────────────────────────────────────────

    def _extract_partitions(
        self,
        evidence_fs: "EvidenceFS",
        open_partition: int,
        files_by_partition: Dict[int, List[Dict]],
        total_files: int,
        output_dir: Path,
        manifest_data: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> None:
        """Copy the discovered files, each from the partition given for it.

        A partition that cannot be opened is skipped with a note. Its paths are
        never read from the open filesystem: the copy would be recorded for a
        partition it was not read from.
        """
        ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
        used_names: set[str] = set()

        callbacks.on_progress(0, total_files, "Copying SiteSecurityServiceState files")
        file_index = 0

        for partition_index in sorted(files_by_partition):
            if callbacks.is_cancelled():
                manifest_data["status"] = "cancelled"
                break

            partition_files = files_by_partition[partition_index]

            if partition_index == open_partition:
                fs_ctx = open_partition_for_extraction(evidence_fs, None)
            elif ewf_paths is None:
                self._skip_partition(
                    partition_index,
                    partition_files,
                    "the evidence has no EWF paths, so the partition cannot be opened",
                    manifest_data,
                    callbacks,
                )
                file_index += len(partition_files)
                continue
            else:
                fs_ctx = open_partition_for_extraction(ewf_paths, partition_index)

            with ExitStack() as stack:
                try:
                    partition_fs = stack.enter_context(fs_ctx)
                except Exception as e:
                    LOGGER.error("Failed to open partition %d: %s", partition_index, e, exc_info=True)
                    self._skip_partition(
                        partition_index,
                        partition_files,
                        f"the partition could not be opened: {e}",
                        manifest_data,
                        callbacks,
                    )
                    file_index += len(partition_files)
                    continue

                # The filesystem that was opened must be the partition that was asked for
                opened_index = getattr(partition_fs, "partition_index", partition_index)
                if (
                    partition_index != open_partition
                    and isinstance(opened_index, int)
                    and opened_index != partition_index
                ):
                    self._skip_partition(
                        partition_index,
                        partition_files,
                        f"the filesystem that was opened reports partition {opened_index}",
                        manifest_data,
                        callbacks,
                    )
                    file_index += len(partition_files)
                    continue

                for file_info in partition_files:
                    if callbacks.is_cancelled():
                        manifest_data["status"] = "cancelled"
                        break

                    file_index += 1
                    callbacks.on_progress(
                        file_index,
                        total_files,
                        f"Copying {file_info['browser']} HSTS data (partition {partition_index})"
                    )

                    extracted_file = self._extract_file(
                        partition_fs,
                        file_info,
                        output_dir,
                        used_names,
                        callbacks,
                    )
                    manifest_data["files"].append(extracted_file)

                    if extracted_file["copy_status"] == "error":
                        manifest_data["notes"].append(
                            f"Failed to extract {file_info['logical_path']} "
                            f"(partition {partition_index}): {extracted_file['error_message']}"
                        )
                        manifest_data["status"] = "partial"

    def _skip_partition(
        self,
        partition_index: int,
        partition_files: List[Dict],
        reason: str,
        manifest_data: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> None:
        """Record in the evidence log and the manifest that a partition's files were not copied."""
        note = (
            f"Skipped {len(partition_files)} {HSTS_FILENAME} file(s) on partition "
            f"{partition_index}: {reason}"
        )
        callbacks.on_log(note, "warning")
        LOGGER.warning(note)
        manifest_data["notes"].append(note)
        manifest_data["status"] = "partial"

        for file_info in partition_files:
            manifest_data["skipped_files"].append({
                "logical_path": file_info["logical_path"],
                "partition_index": partition_index,
                "listed_partition_index": file_info.get("listed_partition_index", partition_index),
                "inode": file_info.get("inode"),
                "browser": file_info["browser"],
                "reason": reason,
            })

    def _extract_file(
        self,
        partition_fs: "EvidenceFS",
        file_info: Dict,
        output_dir: Path,
        used_names: set[str],
        callbacks: ExtractorCallbacks,
    ) -> Dict:
        """Copy one file from the given partition's filesystem to the workspace.

        Filename format:
        p{partition}_{browser}_{user}_{profile}_{path hash}_SiteSecurityServiceState.txt
        A name that was already used in this run gets a counter before the
        file name part (_2, _3, ...), so that no copy overwrites another.
        """
        source_path = file_info["logical_path"]
        partition_idx = file_info.get("partition_index", 0)
        forensic_path = f"p{partition_idx}:{source_path}"
        filename = None

        try:
            browser = file_info["browser"]
            user = file_info.get("user", "unknown")
            profile = file_info.get("profile", "Default")

            # Sanitize components for filename
            safe_user = self._sanitize_filename_component(user)
            safe_profile = self._sanitize_filename_component(profile)

            # Partition prefix and path hash keep files with the same user and profile apart
            path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]
            filename = self._unique_filename(
                f"p{partition_idx}_{browser}_{safe_user}_{safe_profile}_{path_hash}",
                used_names,
            )
            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {filename}", "info")

            declared_size = partition_fs.stat(source_path).size_bytes
            if declared_size > MAX_HSTS_FILE_BYTES:
                raise ValueError(
                    f"file has {declared_size} bytes, above the {MAX_HSTS_FILE_BYTES} byte safety limit"
                )

            # Bounded read, in case the size above was not the real one
            with partition_fs.open_for_read(source_path) as source:
                file_content = source.read(MAX_HSTS_FILE_BYTES + 1)
            if len(file_content) > MAX_HSTS_FILE_BYTES:
                raise ValueError(f"file is above the {MAX_HSTS_FILE_BYTES} byte safety limit")

            dest_path.write_bytes(file_content)

            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()
            size = len(file_content)

            # Provenance of the filesystem the file was read from
            fs_type = getattr(partition_fs, "fs_type", None)
            if not isinstance(fs_type, str):
                fs_type = None

            return {
                "copy_status": "ok",
                "size_bytes": size,
                "file_size_bytes": size,
                "md5": md5,
                "sha256": sha256,
                "extracted_path": str(dest_path),
                "local_filename": filename,
                "browser": browser,
                "user": user,
                "profile": profile,
                "file_type": "firefox",
                "logical_path": source_path,
                "artifact_type": "transport_security",
                "partition_index": partition_idx,
                "listed_partition_index": file_info.get("listed_partition_index", partition_idx),
                "inode": file_info.get("inode"),
                "fs_type": fs_type,
                "forensic_path": forensic_path,
            }

        except Exception as e:
            callbacks.on_log(f"Failed to extract {source_path}: {e}", "error")
            return {
                "copy_status": "error",
                "size_bytes": 0,
                "file_size_bytes": 0,
                "md5": None,
                "sha256": None,
                "extracted_path": None,
                "local_filename": filename,
                "browser": file_info.get("browser"),
                "user": file_info.get("user"),
                "profile": file_info.get("profile"),
                "file_type": file_info.get("file_type"),
                "logical_path": source_path,
                "artifact_type": "transport_security",
                "partition_index": partition_idx,
                "listed_partition_index": file_info.get("listed_partition_index", partition_idx),
                "inode": file_info.get("inode"),
                "forensic_path": forensic_path,
                "error_message": str(e),
                "error": str(e),
            }

    @staticmethod
    def _unique_filename(prefix: str, used_names: set[str]) -> str:
        """Return a file name for the prefix that was not used in this run.

        Names are compared without letter case, because the case workspace can
        be on a case-insensitive filesystem.
        """
        candidate = f"{prefix}_{HSTS_FILENAME}"
        counter = 1
        while candidate.lower() in used_names:
            counter += 1
            candidate = f"{prefix}_{counter}_{HSTS_FILENAME}"
        used_names.add(candidate.lower())
        return candidate

    def _sanitize_filename_component(self, component: str) -> str:
        """Sanitize a string for use in filename."""
        import re
        # Replace unsafe characters with underscore. ASCII only: a character is
        # then one byte for the file name limit, and names that differ only in
        # Unicode form or case cannot be the same file in the workspace.
        safe = re.sub(r'[^\w\-.]', '_', component, flags=re.ASCII)
        # Collapse multiple underscores
        safe = re.sub(r'_+', '_', safe)
        # Remove leading/trailing underscores
        safe = safe.strip('_')
        # A very long name would exceed the file name limit of the workspace
        safe = safe[:MAX_NAME_COMPONENT_CHARS]
        return safe or "unknown"

    # ─────────────────────────────────────────────────────────────────
    # Parsing and Ingestion
    # ─────────────────────────────────────────────────────────────────

    def _parse_file_with_warnings(
        self,
        file_path: Path,
        file_entry: Dict,
        warning_collector: ExtractionWarningCollector,
    ) -> List[Dict]:
        """Parse SiteSecurityServiceState.txt with warning collection.

        Uses the parse_transport_security_file function from _parsers module.
        """
        if not file_path.exists():
            LOGGER.warning("SiteSecurityServiceState.txt not found: %s", file_path)
            return []

        if file_path.stat().st_size > MAX_HSTS_FILE_BYTES:
            raise ValueError(f"Copied file exceeds the {MAX_HSTS_FILE_BYTES} byte safety limit")

        try:
            content = file_path.read_text(encoding='utf-8', errors='replace')
        except Exception as e:
            LOGGER.warning("Failed to read SiteSecurityServiceState.txt: %s", e)
            return []

        source_file = file_entry.get("logical_path", str(file_path))

        # Use parser module for parsing with warning collection
        entries = parse_transport_security_file(
            content=content,
            source_file=source_file,
            warning_collector=warning_collector,
        )

        return entries

    def _insert_urls(
        self,
        entries: List[Dict],
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
    ) -> int:
        """Insert parsed entries into urls table.

        NO deduplication - inserts all entries.
        """
        browser = file_entry["browser"]
        profile = file_entry.get("profile", "Default")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        records = []

        for entry in entries:
            domain = entry.get("host", "")
            if not domain:
                continue

            # Convert to URL
            url = f"https://{domain}/"

            # Use last_access for last_seen (more forensically accurate)
            last_access_iso = entry.get("last_access_iso")
            expiry_iso = unix_to_iso8601(entry.get("expiry_seconds")) if entry.get("expiry_seconds") else None

            # Build detailed notes with HSTS metadata
            notes_parts = [f"HSTS entry from Firefox SiteSecurityServiceState.txt (profile: {profile})"]
            if entry.get("mode"):
                notes_parts.append(f"mode: {entry['mode']}")
            if entry.get("include_subdomains"):
                notes_parts.append("includeSubdomains: true")
            if expiry_iso:
                notes_parts.append(f"expiry: {expiry_iso}")
            # The urls table has no partition column
            if file_entry.get("partition_index") is not None:
                notes_parts.append(f"partition: {file_entry['partition_index']}")

            record = {
                "url": url,
                "domain": domain,
                "scheme": "https",
                "discovered_by": discovered_by,
                "first_seen_utc": None,  # Not available from HSTS
                "last_seen_utc": last_access_iso,
                "source_path": file_entry["logical_path"],
                "notes": "; ".join(notes_parts),
                "run_id": run_id,
            }
            records.append(record)

        if records:
            insert_urls(evidence_conn, evidence_id, records)

        return len(records)

    def _insert_hsts_entries(
        self,
        entries: List[Dict],
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
    ) -> int:
        """Insert parsed entries into hsts_entries table.

        Firefox stores cleartext domains, so decoded_host is the actual domain.
        hashed_host is set to a placeholder since Firefox doesn't hash.
        """
        browser = file_entry["browser"]
        profile = file_entry.get("profile", "Default")
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        records = []

        for entry in entries:
            domain = entry.get("host", "")
            if not domain:
                continue

            record = {
                "browser": browser,
                "profile": profile,
                # Firefox stores cleartext - use marker for hashed_host
                "hashed_host": f"firefox_cleartext:{domain}",
                "decoded_host": domain,
                "decode_method": "cleartext",  # Firefox stores cleartext natively
                "sts_observed": entry.get("expiry_seconds"),  # Unix seconds
                "expiry": entry.get("expiry_seconds"),  # Unix seconds
                "mode": entry.get("mode", "force-https"),
                "include_subdomains": entry.get("include_subdomains", 0),
                "run_id": run_id,
                "source_path": file_entry["logical_path"],
                "discovered_by": discovered_by,
                "partition_index": file_entry.get("partition_index"),
                "fs_type": file_entry.get("fs_type"),
                "logical_path": file_entry.get("logical_path"),
                "forensic_path": file_entry.get("forensic_path"),
                "notes": f"last_access_days={entry.get('last_access_days')}, state={entry.get('state')}",
            }
            records.append(record)

        if records:
            insert_hsts_entries(evidence_conn, evidence_id, records)

        return len(records)

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        """Clear URL and HSTS data from a previous run.

        Uses exact run_id match to avoid clearing data from other runs.
        """
        try:
            # Delete URLs with exact run_id match
            url_deleted = delete_urls_by_run(evidence_conn, evidence_id, run_id)

            # Delete from hsts_entries table with exact run_id
            hsts_deleted = delete_hsts_by_run(evidence_conn, evidence_id, run_id)

            if url_deleted > 0 or hsts_deleted > 0:
                LOGGER.info(
                    "Cleared %d URLs and %d HSTS entries from previous run %s",
                    url_deleted, hsts_deleted, run_id
                )
        except Exception as e:
            LOGGER.warning("Failed to clear previous run data: %s", e)

    # ─────────────────────────────────────────────────────────────────
    # Helper Methods
    # ─────────────────────────────────────────────────────────────────

    def _abort_extraction(
        self,
        error: str,
        error_msg: str,
        manifest_data: Dict[str, Any],
        output_dir: Path,
        callbacks: ExtractorCallbacks,
    ) -> bool:
        """End the run with status ``error``: evidence log, manifest note, statistics.

        Returns False, the result of a failed ``run_extraction``.
        """
        callbacks.on_error(error, error_msg)
        LOGGER.error(error_msg)
        manifest_data["status"] = "error"
        manifest_data["notes"].append(error_msg)

        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        stats = StatisticsCollector.instance()
        if stats:
            stats.finish_run(manifest_data["evidence_id"], self.metadata.name, status="error")

        return False

    def _generate_run_id(self) -> str:
        """Generate run ID: ts_firefox_{timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"ts_firefox_{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs: "EvidenceFS") -> dict:
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
