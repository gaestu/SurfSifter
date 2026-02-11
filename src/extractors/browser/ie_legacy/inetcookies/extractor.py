"""
IE/Legacy Edge File-based Cookies Extractor (INetCookies).

Extracts and parses .cookie files from INetCookies folders and legacy
.txt cookie files from older Windows versions.

These are individual cookie files stored outside the WebCache database.
While WebCache contains cookie metadata, these files contain the actual
cookie content including values.

Cookie File Formats:
- .cookie (Windows 10+): Binary/text hybrid format
- .txt (Windows 7/8): Plain text cookie format

Locations:
- Windows 10+: Users/*/AppData/Local/Microsoft/Windows/INetCookies/
- Legacy: Users/*/AppData/Roaming/Microsoft/Windows/Cookies/
- Edge Legacy: Microsoft.MicrosoftEdge_*/AC/MicrosoftEdge/Cookies/

Forensic Value:
- Contains actual cookie values (WebCache only has metadata)
- May persist after WebCache is cleared
- Low integrity cookies show sandboxed browsing

Dependencies:
- None (pure Python parsing)
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from PySide6.QtWidgets import QWidget, QLabel, QVBoxLayout

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    check_file_list_available,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from .._patterns import (
    get_all_patterns,
    extract_user_from_path,
    detect_browser_from_path,
    get_browser_display_name,
)
from core.logging import get_logger
from core.database import (
    insert_cookie_row,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)


LOGGER = get_logger("extractors.browser.ie_legacy.inetcookies")


class IEINetCookiesExtractor(BaseExtractor):
    """
    Extract and parse file-based IE/Legacy Edge cookies.

    This extractor handles .cookie and .txt cookie files that exist
    outside the WebCache ESE database. These files contain actual
    cookie values, not just metadata.

    Workflow:
    1. Discover .cookie/.txt files in INetCookies and Cookies folders
    2. Copy files to workspace
    3. Parse cookie content
    4. Insert into cookies table
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="ie_inetcookies",
            display_name="IE/Edge File Cookies",
            description="Extract file-based cookies (.cookie, .txt) from INetCookies",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
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
        return MultiPartitionWidget(parent, default_scan_all=True)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction/ingestion state."""
        widget = QWidget(parent)
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)

        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"IE/Edge File Cookies\nFiles: {file_count}"
        else:
            status_text = "IE/Edge File Cookies\nNo extraction yet"

        layout.addWidget(QLabel(status_text, widget))
        return widget

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "ie_inetcookies"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract .cookie and .txt files from INetCookies paths.
        """
        callbacks.on_step("Initializing INetCookies extraction")

        run_id = self._generate_run_id()
        LOGGER.info("Starting INetCookies extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")
        scan_all_partitions = config.get("scan_all_partitions", True)

        collector = self._get_statistics_collector()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_tool_version(),
            "files": [],
            "partitions_scanned": [],
            "partitions_with_artifacts": [],
            "status": "ok",
            "notes": [],
        }

        # Get patterns
        patterns = get_all_patterns("inetcookies")
        callbacks.on_step(f"Searching {len(patterns)} cookie file patterns")

        # Discover files
        discovered_files = []

        available, count = check_file_list_available(evidence_conn, evidence_id) if evidence_conn else (False, 0)
        if available:
            callbacks.on_step(f"Using file_list index for discovery ({count:,} files indexed)")
            partition_filter = None if scan_all_partitions else {0}
            result = discover_from_file_list(
                evidence_conn, evidence_id,
                path_patterns=patterns,
                partition_filter=partition_filter,
            )
            # Convert FileListMatch objects to expected dict format
            discovered_files = [
                {
                    "logical_path": m.file_path,
                    "filename": m.file_name,
                    "partition_index": m.partition_index,
                }
                for m in result.get_all_matches()
            ]
        else:
            callbacks.on_step("Walking filesystem for cookie files")
            discovered_files = self._walk_for_files(evidence_fs, patterns, callbacks)

        if not discovered_files:
            manifest_data["notes"].append("No cookie files found")
            callbacks.on_log("No cookie files found", "warning")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="no_artifacts")
            (output_dir / "manifest.json").write_text(json.dumps(manifest_data, indent=2))
            return True

        callbacks.on_log(f"Found {len(discovered_files)} cookie files", "info")
        callbacks.on_progress(0, len(discovered_files), "Extracting cookie files")

        # Process each file
        partitions_with_artifacts = set()

        # Get EWF paths for multi-partition extraction
        ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
        current_partition = getattr(evidence_fs, 'partition_index', 0)
        # Group files by partition to avoid reopening the same partition repeatedly
        files_by_partition: Dict[int, List[Dict]] = {}
        for file_info in discovered_files:
            partition_index = file_info.get("partition_index", 0)
            files_by_partition.setdefault(partition_index, []).append(file_info)

        total_files = len(discovered_files)
        file_index = 0

        for partition_index in sorted(files_by_partition.keys()):
            # If we cannot open other partitions, skip them and record a note
            if ewf_paths is None and partition_index != current_partition:
                msg = (
                    f"EWF paths unavailable; skipping partition {partition_index} "
                    "for INetCookies extraction"
                )
                callbacks.on_log(msg, "warning")
                manifest_data["notes"].append(msg)
                continue

            fs_ctx = (
                open_partition_for_extraction(evidence_fs, None)
                if (partition_index == current_partition or ewf_paths is None)
                else open_partition_for_extraction(ewf_paths, partition_index)
            )

            try:
                with fs_ctx as fs_to_use:
                    if fs_to_use is None:
                        msg = f"Failed to open partition {partition_index}; skipping"
                        callbacks.on_log(msg, "warning")
                        manifest_data["notes"].append(msg)
                        continue

                    for file_info in files_by_partition[partition_index]:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            break

                        file_index += 1
                        callbacks.on_progress(
                            file_index, total_files,
                            f"Extracting {file_info.get('filename', 'file')}"
                        )

                        try:
                            logical_path = file_info.get("logical_path", "")
                            user = extract_user_from_path(logical_path)
                            browser = detect_browser_from_path(logical_path)

                            is_low_integrity = "/low/" in logical_path.lower() or "\\low\\" in logical_path.lower()

                            safe_name = Path(logical_path).name
                            out_path = output_dir / f"p{partition_index}" / user / safe_name
                            out_path.parent.mkdir(parents=True, exist_ok=True)

                            content = fs_to_use.read_file(logical_path)
                            out_path.write_bytes(content)

                            md5 = hashlib.md5(content).hexdigest()
                            sha256 = hashlib.sha256(content).hexdigest()

                            manifest_data["files"].append({
                                "logical_path": logical_path,
                                "extracted_path": str(out_path.relative_to(output_dir)),
                                "user": user,
                                "browser": browser,
                                "partition_index": partition_index,
                                "size_bytes": len(content),
                                "md5": md5,
                                "sha256": sha256,
                                "is_low_integrity": is_low_integrity,
                                "artifact_type": "inetcookies",
                            })
                            partitions_with_artifacts.add(partition_index)

                        except Exception as e:
                            error_msg = f"Failed to extract {file_info.get('logical_path', 'unknown')}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            callbacks.on_error(error_msg, "")
                            manifest_data["notes"].append(error_msg)
            except Exception as e:
                error_msg = f"Failed to open partition {partition_index}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_log(error_msg, "warning")
                manifest_data["notes"].append(error_msg)

        manifest_data["partitions_with_artifacts"] = sorted(partitions_with_artifacts)

        if collector:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            collector.finish_run(evidence_id, self.metadata.name, status=status)

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

        LOGGER.info(
            "INetCookies extraction complete: %d files, status=%s",
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
        Parse extracted cookie files and ingest into database.
        """
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"cookies": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())
        evidence_label = config.get("evidence_label", "")

        files = manifest_data.get("files", [])
        if not files:
            callbacks.on_log("No cookie files to process", "warning")
            return {"cookies": 0}

        collector = self._get_statistics_collector()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        total_cookies = 0
        failed_files = 0

        callbacks.on_progress(0, len(files), "Parsing cookie files")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry.get('user', 'unknown')} cookies")

            try:
                extracted_path = Path(file_entry["extracted_path"])
                if not extracted_path.is_absolute():
                    extracted_path = output_dir / extracted_path

                if not extracted_path.exists():
                    callbacks.on_log(f"File not found: {extracted_path}", "warning")
                    failed_files += 1
                    continue

                # Register inventory
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry.get("browser", "ie"),
                    artifact_type="inetcookies",
                    run_id=run_id,
                    extracted_path=str(extracted_path),
                    extraction_status="ok",
                    extraction_timestamp_utc=manifest_data.get("extraction_timestamp_utc"),
                    logical_path=file_entry.get("logical_path", ""),
                    profile=file_entry.get("user"),
                    partition_index=file_entry.get("partition_index"),
                    extraction_tool=manifest_data.get("extraction_tool"),
                    file_size_bytes=file_entry.get("size_bytes"),
                    file_md5=file_entry.get("md5"),
                    file_sha256=file_entry.get("sha256"),
                )

                # Parse and insert
                cookies = self._parse_cookie_file(
                    extracted_path,
                    file_entry,
                    run_id,
                    evidence_id,
                    evidence_conn,
                    callbacks,
                )

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=cookies,
                )

                total_cookies += cookies

            except Exception as e:
                error_msg = f"Failed to parse {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")
                failed_files += 1

        evidence_conn.commit()

        if collector:
            collector.report_ingested(evidence_id, self.metadata.name, records=total_cookies)
            if failed_files:
                collector.report_failed(evidence_id, self.metadata.name, files=failed_files)
            status = "success" if failed_files == 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_log(f"Ingested {total_cookies} cookies from file-based storage", "info")

        return {"cookies": total_cookies}

    # =========================================================================
    # Private Helper Methods
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_statistics_collector(self):
        """Get StatisticsCollector instance."""
        try:
            from core.statistics_collector import StatisticsCollector
            return StatisticsCollector.get_instance()
        except Exception:
            return None

    def _get_tool_version(self) -> str:
        """Build extraction tool version string."""
        return f"{self.metadata.name}:{self.metadata.version}"

    def _walk_for_files(
        self,
        evidence_fs,
        patterns: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict[str, Any]]:
        """Walk filesystem to find matching files."""
        import fnmatch

        results = []

        try:
            for path in evidence_fs.iter_all_files():
                if callbacks.is_cancelled():
                    break

                normalized = path.replace("\\", "/")
                for pattern in patterns:
                    if fnmatch.fnmatch(normalized.lower(), pattern.lower()):
                        results.append({
                            "logical_path": path,
                            "filename": Path(path).name,
                            "partition_index": 0,
                        })
                        break
        except Exception as e:
            LOGGER.error("Error walking filesystem: %s", e)

        return results

    def _parse_cookie_file(
        self,
        file_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
    ) -> int:
        """
        Parse a .cookie or .txt cookie file.

        Returns number of cookies parsed.
        """
        user = file_entry.get("user", "unknown")
        browser = file_entry.get("browser", "ie")
        source_path = file_entry.get("logical_path", "")
        partition_index = file_entry.get("partition_index", 0)
        is_low_integrity = file_entry.get("is_low_integrity", False)
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        try:
            content = file_path.read_bytes()

            # Try to decode as text
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError:
                try:
                    text = content.decode("utf-16-le")
                except UnicodeDecodeError:
                    text = content.decode("latin-1", errors="replace")

            # Parse based on format
            if file_path.suffix.lower() == ".cookie":
                cookies = self._parse_cookie_format(text, file_entry)
            else:
                cookies = self._parse_txt_format(text, file_entry)

            # Insert cookies
            inserted = 0
            for cookie in cookies:
                try:
                    insert_cookie_row(
                        evidence_conn,
                        evidence_id=evidence_id,
                        browser=browser,
                        profile=user,
                        domain=cookie.get("domain", ""),
                        name=cookie.get("name", ""),
                        path=cookie.get("path", "/"),
                        value=cookie.get("value", ""),
                        creation_utc=cookie.get("creation_utc"),
                        last_access_utc=cookie.get("last_access_utc"),
                        expires_utc=cookie.get("expires_utc"),
                        is_secure=cookie.get("is_secure", False),
                        is_httponly=cookie.get("is_httponly", False),
                        is_persistent=cookie.get("expires_utc") is not None,
                        samesite="None",
                        source_path=source_path,
                        discovered_by=discovered_by,
                        run_id=run_id,
                        partition_index=partition_index,
                        notes=f"low_integrity={is_low_integrity}" if is_low_integrity else None,
                    )
                    inserted += 1
                except Exception as e:
                    LOGGER.debug("Failed to insert cookie: %s", e)

            return inserted

        except Exception as e:
            LOGGER.error("Failed to parse cookie file %s: %s", file_path, e)
            return 0

    def _parse_cookie_format(self, text: str, file_entry: Dict) -> List[Dict]:
        """
        Parse Windows .cookie format.

        Format (newline-separated):
        - Line 1: Cookie name
        - Line 2: Cookie value
        - Line 3: Domain/path (e.g., example.com/)
        - Line 4-7: Expiry info (FILETIME low, high, etc.)
        - Line 8: Flags (* delimiter)

        Multiple cookies separated by * on its own line.
        """
        cookies = []

        # Split by cookie delimiter
        parts = text.split("\n*\n")

        for part in parts:
            lines = part.strip().split("\n")
            if len(lines) < 3:
                continue

            name = lines[0].strip() if len(lines) > 0 else ""
            value = lines[1].strip() if len(lines) > 1 else ""
            domain_path = lines[2].strip() if len(lines) > 2 else ""

            # Parse domain and path
            if "/" in domain_path:
                domain, path = domain_path.split("/", 1)
                path = "/" + path
            else:
                domain = domain_path
                path = "/"

            # Parse expiry (FILETIME format in lines 4-5)
            expires_utc = None
            if len(lines) >= 5:
                try:
                    low = int(lines[3].strip())
                    high = int(lines[4].strip())
                    if high > 0:  # Non-zero means has expiry
                        filetime = (high << 32) | low
                        # Convert FILETIME to datetime
                        from .._timestamps import filetime_to_iso
                        expires_utc = filetime_to_iso(filetime)
                except (ValueError, IndexError):
                    pass

            # Parse flags
            flags = 0
            if len(lines) >= 8:
                try:
                    flags = int(lines[7].strip())
                except (ValueError, IndexError):
                    pass

            is_secure = bool(flags & 0x01)
            is_httponly = bool(flags & 0x02)

            cookies.append({
                "name": name,
                "value": value,
                "domain": domain,
                "path": path,
                "expires_utc": expires_utc,
                "is_secure": is_secure,
                "is_httponly": is_httponly,
            })

        return cookies

    def _parse_txt_format(self, text: str, file_entry: Dict) -> List[Dict]:
        """
        Parse legacy .txt cookie format (Netscape format).

        Format (tab-separated):
        domain\tflag\tpath\tsecure\texpiration\tname\tvalue
        """
        cookies = []

        for line in text.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = line.split("\t")
            if len(parts) < 7:
                continue

            domain = parts[0]
            # flag = parts[1]  # HTTP-only flag
            path = parts[2]
            is_secure = parts[3].lower() == "true"
            expiration = parts[4]
            name = parts[5]
            value = parts[6] if len(parts) > 6 else ""

            # Parse expiration (Unix timestamp)
            expires_utc = None
            try:
                exp_ts = int(expiration)
                if exp_ts > 0:
                    expires_utc = datetime.fromtimestamp(exp_ts, tz=timezone.utc).isoformat()
            except (ValueError, OSError):
                pass

            cookies.append({
                "name": name,
                "value": value,
                "domain": domain,
                "path": path,
                "expires_utc": expires_utc,
                "is_secure": is_secure,
                "is_httponly": False,  # Not in Netscape format
            })

        return cookies
