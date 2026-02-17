"""
Safari Cookies Extractor.

Extracts cookies from Safari on macOS.
Uses binary cookies format (Cookies.binarycookies).

Note: Safari support is EXPERIMENTAL.
Requires 'binarycookies' library for parsing.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from .._patterns import get_patterns, extract_user_from_path
from .._parsers import parse_cookies, get_cookie_stats

from core.logging import get_logger
from core.database import (
    insert_browser_inventory,
    update_inventory_ingestion_status,
)

LOGGER = get_logger("extractors.browser.safari.cookies")


class SafariCookiesExtractor(BaseExtractor):
    """
    Extract Safari cookies from macOS evidence.

    Parses Cookies.binarycookies binary format file.
    Requires the 'binarycookies' library.

    Note: Safari cookies are NOT encrypted locally (unlike Chromium).
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="safari_cookies",
            display_name="Safari Cookies (macOS)",
            description="Extract Safari cookies from macOS - EXPERIMENTAL",
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
        """Return configuration widget for multi-partition support."""
        return MultiPartitionWidget(parent)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction results."""
        manifest_path = output_dir / "manifest.json"

        if manifest_path.exists():
            try:
                data = json.loads(manifest_path.read_text())
                file_count = len(data.get("files", []))
                parsed = data.get("parsed_counts", {})
                cookies = parsed.get("cookies", 0)
                domains = parsed.get("domains", 0)
                status_text = (
                    f"Safari Cookies (EXPERIMENTAL)\n"
                    f"Files: {file_count}\n"
                    f"Cookies: {cookies}\n"
                    f"Domains: {domains}\n"
                    f"Run ID: {data.get('run_id', 'N/A')}"
                )
            except Exception:
                status_text = "Safari Cookies - Error reading manifest"
        else:
            status_text = "Safari Cookies (EXPERIMENTAL)\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for Safari cookies extraction."""
        return case_root / "evidences" / evidence_label / "safari_cookies"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Safari Cookies.binarycookies files from evidence."""
        from core.statistics_collector import StatisticsCollector

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        collector = StatisticsCollector.get_instance()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        callbacks.on_step("Initializing Safari cookies extraction")
        LOGGER.info("Starting Safari cookies extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)
        files_dir = output_dir / run_id
        files_dir.mkdir(parents=True, exist_ok=True)

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
            "notes": ["Safari support is EXPERIMENTAL"],
        }

        patterns = get_patterns("cookies")
        discovered_files = []

        callbacks.on_step("Discovering Safari cookies files")

        for pattern in patterns:
            try:
                for path_str in evidence_fs.iter_paths(pattern):
                    discovered_files.append(path_str)
            except Exception as e:
                LOGGER.debug("Pattern %s failed: %s", pattern, e)

        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=len(discovered_files))

        if not discovered_files:
            LOGGER.info("No Safari cookies files found")
            manifest_data["notes"].append("No Safari cookies files found")
        else:
            callbacks.on_step(f"Extracting {len(discovered_files)} Safari cookies files")

            for path_str in discovered_files:
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    manifest_data["notes"].append("Extraction cancelled by user")
                    break

                try:
                    file_info = self._extract_file(evidence_fs, path_str, files_dir, run_id)
                    if file_info:
                        manifest_data["files"].append(file_info)
                        callbacks.on_log(f"Copied: {path_str}", "info")
                except Exception as e:
                    LOGGER.warning("Failed to extract %s: %s", path_str, e)
                    manifest_data["notes"].append(f"Failed: {path_str}: {e}")
                    if collector:
                        collector.report_failed(evidence_id, self.metadata.name, files=1)

        # Finish statistics tracking (exactly once)
        if collector:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            collector.finish_run(evidence_id, self.metadata.name, status=status)

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

        LOGGER.info("Safari cookies extraction complete: %d files", len(manifest_data["files"]))

        return manifest_data["status"] != "error"

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """Parse extracted Safari cookies and ingest into database."""
        from core.statistics_collector import StatisticsCollector
        from core.database import insert_cookies

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"cookies": 0, "domains": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", "unknown")
        evidence_label = config.get("evidence_label", "")

        collector = StatisticsCollector.get_instance()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        files = manifest_data.get("files", [])

        if not files:
            callbacks.on_log("No Safari cookies files to ingest", "warning")
            if collector:
                # CRITICAL: Report ingested counts even when 0
                collector.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0,
                    cookies=0,
                )
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"cookies": 0, "domains": 0}

        total_cookies = 0
        total_domains = 0
        all_records = []

        discovered_by = self.metadata.name

        for file_info in files:
            local_path = file_info.get("local_path")
            source_path = file_info.get("source_path", "")

            if not local_path:
                continue

            local_path = Path(local_path)
            if not local_path.exists():
                continue

            callbacks.on_step(f"Parsing {local_path.name}")

            user = extract_user_from_path(source_path)
            profile = user or "Default"

            inventory_id = insert_browser_inventory(
                evidence_conn,
                evidence_id=evidence_id,
                browser="safari",
                artifact_type="cookies",
                run_id=run_id,
                extracted_path=str(local_path),
                extraction_status="ok",
                extraction_timestamp_utc=manifest_data.get("extraction_timestamp_utc", ""),
                logical_path=source_path,
                profile=profile,
                partition_index=file_info.get("partition_index"),
                fs_type=file_info.get("fs_type"),
                forensic_path=source_path,
                file_size_bytes=file_info.get("size_bytes"),
                file_md5=file_info.get("md5"),
                file_sha256=file_info.get("sha256"),
            )

            try:
                cookies = parse_cookies(local_path)

                if not cookies:
                    # Library might not be installed
                    callbacks.on_log(
                        "binarycookies library may not be installed",
                        "warning"
                    )
                    continue

                for cookie in cookies:
                    all_records.append({
                        "browser": "safari",
                        "profile": profile,
                        "domain": cookie.domain,
                        "name": cookie.name,
                        "value": cookie.value,
                        "path": cookie.path,
                        "expires_utc": cookie.expires_utc,
                        "creation_utc": cookie.creation_time_utc,
                        "is_secure": cookie.is_secure,
                        "is_httponly": cookie.is_httponly,
                        "is_encrypted": False,  # Safari cookies are not encrypted
                        "run_id": run_id,
                        "source_path": source_path,
                        "partition_index": file_info.get("partition_index"),
                        "fs_type": file_info.get("fs_type"),
                        "logical_path": source_path,
                        "forensic_path": source_path,
                        "discovered_by": discovered_by,
                    })

                stats = get_cookie_stats(cookies)
                total_cookies += stats.get("total_cookies", 0)
                total_domains += stats.get("unique_domains", 0)

                callbacks.on_log(f"Parsed {len(cookies)} cookies from {profile}", "info")

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=len(cookies),
                )

            except Exception as e:
                LOGGER.error("Failed to parse %s: %s", local_path, e)
                callbacks.on_log(f"Parse error: {e}", "error")
                if collector:
                    collector.report_failed(evidence_id, self.metadata.name, files=1)
                if "inventory_id" in locals():
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(e),
                    )

        inserted = 0
        if all_records:
            callbacks.on_step("Inserting cookie records")
            inserted = insert_cookies(evidence_conn, evidence_id, all_records)
            evidence_conn.commit()
            callbacks.on_log(f"Ingested {inserted} cookies", "info")

        manifest_data["parsed_counts"] = {
            "cookies": total_cookies,
            "domains": total_domains,
        }
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        # Report ingested counts (always, even if 0) and finish
        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=inserted,
                cookies=inserted,
            )
            status = "success" if inserted > 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        return {"cookies": total_cookies, "domains": total_domains}

    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context safely."""
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

    def _extract_file(
        self,
        evidence_fs,
        path_str: str,
        output_dir: Path,
        run_id: str
    ) -> Optional[Dict[str, Any]]:
        """Extract a single file from evidence."""
        try:
            content = evidence_fs.read_file(path_str)

            original_name = Path(path_str).name
            user = extract_user_from_path(path_str) or "unknown"
            safe_name = f"safari_{user}_{original_name}"

            dest_path = output_dir / safe_name
            dest_path.write_bytes(content)

            md5 = hashlib.md5(content).hexdigest()
            sha256 = hashlib.sha256(content).hexdigest()

            return {
                "local_path": str(dest_path),
                "source_path": path_str,
                "artifact_type": "cookies",
                "browser": "safari",
                "user": user,
                "md5": md5,
                "sha256": sha256,
                "size_bytes": len(content),
            }

        except Exception as e:
            LOGGER.debug("Failed to extract %s: %s", path_str, e)
            return None
