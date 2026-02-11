"""
Safari History Extractor.

Extracts browser history from Safari on macOS.
Uses Cocoa timestamps (seconds since January 1, 2001).

Note: Safari support is EXPERIMENTAL.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
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
from .._patterns import get_patterns, extract_user_from_path, get_browser_display_name
from .._parsers import parse_history_visits, get_history_stats, cocoa_to_iso

from core.logging import get_logger
from core.database import insert_urls

LOGGER = get_logger("extractors.browser.safari.history")


class SafariHistoryExtractor(BaseExtractor):
    """
    Extract Safari history from macOS evidence.

    Parses History.db SQLite database with Cocoa timestamps.

    Safari History Schema:
    - history_items: id, url, domain_expansion, ...
    - history_visits: history_item, title, visit_time, redirect_source, redirect_destination

    Timestamps are Cocoa format (seconds since 2001-01-01 UTC).
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="safari_history",
            display_name="Safari History (macOS)",
            description="Extract Safari browser history from macOS - EXPERIMENTAL",
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
                visits = parsed.get("visits", 0)
                unique_urls = parsed.get("unique_urls", 0)
                status_text = (
                    f"Safari History (EXPERIMENTAL)\n"
                    f"Files: {file_count}\n"
                    f"Visits: {visits}\n"
                    f"Unique URLs: {unique_urls}\n"
                    f"Run ID: {data.get('run_id', 'N/A')}"
                )
            except Exception:
                status_text = "Safari History - Error reading manifest"
        else:
            status_text = "Safari History (EXPERIMENTAL)\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for Safari history extraction."""
        return case_root / "evidences" / evidence_label / "safari_history"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Safari History.db files from evidence.

        Discovers and copies all History.db files along with WAL/journal files.
        """
        from core.statistics_collector import StatisticsCollector

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        # Start statistics tracking
        collector = StatisticsCollector.get_instance()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        callbacks.on_step("Initializing Safari history extraction")
        LOGGER.info("Starting Safari history extraction (run_id=%s)", run_id)

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

        # Get patterns for Safari history
        patterns = get_patterns("history")
        discovered_files = []

        callbacks.on_step("Discovering Safari history databases")

        for pattern in patterns:
            try:
                for path_str in evidence_fs.iter_paths(pattern):
                    discovered_files.append(path_str)
            except Exception as e:
                LOGGER.debug("Pattern %s failed: %s", pattern, e)

        # Report discovered count
        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=len(discovered_files))

        if not discovered_files:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Safari history databases found")
            LOGGER.info("No Safari history found")

            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="skipped")
        else:
            callbacks.on_step(f"Extracting {len(discovered_files)} Safari history files")

            for path_str in discovered_files:
                try:
                    file_info = self._extract_file(
                        evidence_fs, path_str, files_dir, run_id
                    )
                    if file_info:
                        manifest_data["files"].append(file_info)
                        callbacks.on_log(f"Copied: {path_str}", "info")
                except Exception as e:
                    LOGGER.warning("Failed to extract %s: %s", path_str, e)
                    if collector:
                        collector.report_failed(evidence_id, self.metadata.name, files=1)

            if collector:
                status = "success" if manifest_data["files"] else "partial"
                collector.finish_run(evidence_id, self.metadata.name, status=status)

        # Write manifest
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

        LOGGER.info("Safari history extraction complete: %d files", len(manifest_data["files"]))

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
        Parse extracted Safari history and ingest into database.
        """
        from core.statistics_collector import StatisticsCollector
        from core.database import insert_browser_history_rows

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"visits": 0, "urls": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", "unknown")
        evidence_label = config.get("evidence_label", "")

        # Continue statistics tracking
        collector = StatisticsCollector.get_instance()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        files = manifest_data.get("files", [])

        if not files:
            callbacks.on_log("No Safari history files to ingest", "warning")
            if collector:
                # CRITICAL: Report ingested counts even when 0
                collector.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0, visits=0, urls=0
                )
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"visits": 0, "urls": 0}

        total_visits = 0
        total_urls = 0
        all_records = []
        url_records = []  # Collect URLs for unified urls table

        discovered_by = self.metadata.name

        for file_info in files:
            artifact_type = file_info.get("artifact_type", "")
            local_path = file_info.get("local_path")
            source_path = file_info.get("source_path", "")

            # Skip WAL/journal files - only parse main History.db
            if not local_path or not artifact_type == "history_db":
                continue

            local_path = Path(local_path)
            if not local_path.exists():
                continue

            callbacks.on_step(f"Parsing {local_path.name}")

            # Extract user from path
            user = extract_user_from_path(source_path)
            profile = user or "Default"

            try:
                from urllib.parse import urlparse

                visits = parse_history_visits(local_path)

                for visit in visits:
                    all_records.append({
                        "browser": "safari",
                        "profile": profile,
                        "url": visit.url,
                        "title": visit.title or "",
                        "visit_time_utc": visit.visit_time_utc,
                        "visit_count": 1,
                        "typed_count": 0,
                        "transition_type": "link",
                        "run_id": run_id,
                        "source_path": source_path,
                        "partition_index": file_info.get("partition_index"),
                        "fs_type": file_info.get("fs_type"),
                        "logical_path": source_path,
                        "forensic_path": source_path,
                        "discovered_by": discovered_by,
                    })

                    # Collect URL for unified urls table
                    if visit.url:
                        parsed = urlparse(visit.url)
                        url_records.append({
                            "url": visit.url,
                            "domain": parsed.netloc or None,
                            "scheme": parsed.scheme or None,
                            "discovered_by": discovered_by,
                            "run_id": run_id,
                            "source_path": source_path,
                            "context": f"history:safari:{profile}",
                            "first_seen_utc": visit.visit_time_utc,
                        })

                total_visits += len(visits)
                stats = get_history_stats(visits)
                total_urls += stats.get("unique_urls", 0)

                callbacks.on_log(
                    f"Parsed {len(visits)} visits from {profile}",
                    "info"
                )

            except Exception as e:
                LOGGER.error("Failed to parse %s: %s", local_path, e)
                callbacks.on_log(f"Parse error: {e}", "error")
                if collector:
                    collector.report_failed(evidence_id, self.metadata.name, files=1)

        # Batch insert all records
        inserted = 0
        if all_records:
            callbacks.on_step("Inserting history records")
            inserted = insert_browser_history_rows(evidence_conn, evidence_id, all_records)
            evidence_conn.commit()
            callbacks.on_log(f"Ingested {inserted} history records", "info")

        # Cross-post URLs to unified urls table for analysis
        if url_records:
            try:
                insert_urls(evidence_conn, evidence_id, url_records)
                evidence_conn.commit()
                LOGGER.debug("Cross-posted %d history URLs to urls table", len(url_records))
            except Exception as e:
                LOGGER.debug("Failed to cross-post history URLs: %s", e)

        # Update manifest with parsed counts
        manifest_data["parsed_counts"] = {
            "visits": total_visits,
            "unique_urls": total_urls,
        }
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        # Report ingested counts (always, even if 0)
        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=inserted, visits=total_visits, urls=total_urls
            )
            status = "success" if total_visits > 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        return {"visits": total_visits, "urls": total_urls}

    # =========================================================================
    # Helper Methods
    # =========================================================================

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
        """
        Extract a single file from evidence.

        Returns file info dict or None on failure.
        """
        try:
            content = evidence_fs.read_file(path_str)

            # Generate safe filename
            original_name = Path(path_str).name

            # Determine artifact type
            if original_name == "History.db":
                artifact_type = "history_db"
            elif original_name.endswith("-wal"):
                artifact_type = "history_wal"
            elif original_name.endswith("-journal"):
                artifact_type = "history_journal"
            elif original_name.endswith("-shm"):
                artifact_type = "history_shm"
            else:
                artifact_type = "history_other"

            # Extract user for unique naming
            user = extract_user_from_path(path_str) or "unknown"
            safe_name = f"safari_{user}_{original_name}"

            dest_path = output_dir / safe_name
            dest_path.write_bytes(content)

            md5 = hashlib.md5(content).hexdigest()
            sha256 = hashlib.sha256(content).hexdigest()

            return {
                "local_path": str(dest_path),
                "source_path": path_str,
                "artifact_type": artifact_type,
                "browser": "safari",
                "user": user,
                "md5": md5,
                "sha256": sha256,
                "size_bytes": len(content),
            }

        except Exception as e:
            LOGGER.debug("Failed to extract %s: %s", path_str, e)
            return None
