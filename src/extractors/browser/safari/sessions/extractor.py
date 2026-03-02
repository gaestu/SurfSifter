"""
Safari Sessions Extractor.

Extracts session recovery artifacts from Safari on macOS:
- LastSession.plist
- RecentlyClosedTabs.plist

Note: Safari support is EXPERIMENTAL.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from ...._shared.file_list_discovery import (
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from .._discovery import discover_safari_files, discover_safari_files_fallback
from .._patterns import get_patterns, extract_user_from_path
from .._parsers import (
    parse_session_plist,
    parse_recently_closed_tabs,
    get_session_stats,
)

from core.logging import get_logger
from core.database import (
    insert_session_windows,
    insert_session_tabs,
    insert_session_tab_histories,
    insert_closed_tabs,
    delete_sessions_by_run,
    insert_urls,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)

LOGGER = get_logger("extractors.browser.safari.sessions")


class SafariSessionsExtractor(BaseExtractor):
    """Extract Safari session recovery artifacts."""

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="safari_sessions",
            display_name="Safari Sessions (macOS)",
            description="Extract Safari open sessions and recently closed tabs from macOS - EXPERIMENTAL",
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
        evidence_id: int,
    ) -> QWidget:
        """Return status widget showing extraction results."""
        manifest_path = output_dir / "manifest.json"

        if manifest_path.exists():
            try:
                data = json.loads(manifest_path.read_text())
                file_count = len(data.get("files", []))
                parsed = data.get("parsed_counts", {})
                status_text = (
                    f"Safari Sessions (EXPERIMENTAL)\n"
                    f"Files: {file_count}\n"
                    f"Windows: {parsed.get('windows', 0)}\n"
                    f"Tabs: {parsed.get('tabs', 0)}\n"
                    f"History: {parsed.get('history', 0)}\n"
                    f"Closed Tabs: {parsed.get('closed_tabs', 0)}\n"
                    f"Run ID: {data.get('run_id', 'N/A')}"
                )
            except Exception:
                status_text = "Safari Sessions - Error reading manifest"
        else:
            status_text = "Safari Sessions (EXPERIMENTAL)\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Return output directory for Safari sessions extraction."""
        return case_root / "evidences" / evidence_label / "safari_sessions"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        """Extract Safari session plist files from evidence."""
        from core.statistics_collector import StatisticsCollector

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        collector = StatisticsCollector.get_instance()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        callbacks.on_step("Initializing Safari sessions extraction")
        LOGGER.info("Starting Safari sessions extraction (run_id=%s)", run_id)

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

        # Multi-partition discovery with fallback to filesystem iteration
        callbacks.on_step("Discovering Safari session files")
        evidence_conn = config.get("evidence_conn")

        files_by_partition = discover_safari_files(
            evidence_conn, evidence_id,
            artifact_names=["sessions", "recently_closed_tabs"],
            callbacks=callbacks,
        )
        if not files_by_partition:
            files_by_partition = discover_safari_files_fallback(
                evidence_fs,
                artifact_names=["sessions", "recently_closed_tabs"],
                callbacks=callbacks,
            )

        manifest_data["multi_partition"] = len(files_by_partition) > 1
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        total_discovered = sum(len(v) for v in files_by_partition.values())

        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=total_discovered)

        if not files_by_partition:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Safari session files found")
            LOGGER.info("No Safari session files found")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="skipped")
        else:
            callbacks.on_step(f"Extracting {total_discovered} Safari session files")

            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            current_partition = getattr(evidence_fs, "partition_index", 0)
            multi = len(files_by_partition) > 1

            for partition_idx in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_idx]

                if ewf_paths is not None and partition_idx != current_partition:
                    ctx = open_partition_for_extraction(ewf_paths, partition_idx)
                else:
                    ctx = open_partition_for_extraction(evidence_fs, None)

                with ctx as fs_to_use:
                    for file_data in partition_files:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            manifest_data["notes"].append("Extraction cancelled by user")
                            break

                        try:
                            path_str = file_data["logical_path"]
                            file_info = self._extract_file(
                                fs_to_use, path_str, files_dir,
                                partition_index=partition_idx if multi else None,
                            )
                            if file_info:
                                file_info["partition_index"] = partition_idx
                                if file_data.get("inode"):
                                    file_info["inode"] = file_data["inode"]
                                manifest_data["files"].append(file_info)
                                callbacks.on_log(f"Copied: {path_str}", "info")
                        except Exception as e:
                            LOGGER.warning("Failed to extract %s: %s", file_data.get("logical_path", "?"), e)
                            if collector:
                                collector.report_failed(evidence_id, self.metadata.name, files=1)

            if collector:
                status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
                collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_step("Writing manifest")
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        # Record extracted files to audit table.
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

        LOGGER.info("Safari sessions extraction complete: %d files", len(manifest_data["files"]))
        return manifest_data["status"] != "error"

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, int]:
        """Parse extracted Safari session files and ingest into session tables."""
        from core.statistics_collector import StatisticsCollector

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        empty_counts = {"windows": 0, "tabs": 0, "history": 0, "closed_tabs": 0, "urls": 0}

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return empty_counts

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", "unknown")
        evidence_label = config.get("evidence_label", "")
        files = manifest_data.get("files", [])

        collector = StatisticsCollector.get_instance()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No Safari session files to ingest", "warning")
            if collector:
                collector.report_ingested(evidence_id, self.metadata.name, records=0, **empty_counts)
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return empty_counts

        totals = dict(empty_counts)
        discovered_by = self.metadata.name
        url_records: List[Dict[str, Any]] = []

        deleted = delete_sessions_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d existing Safari session records for run_id=%s", deleted, run_id)

        callbacks.on_progress(0, len(files), "Parsing Safari session files")

        for idx, file_info in enumerate(files):
            if callbacks.is_cancelled():
                break

            local_path_str = file_info.get("local_path")
            source_path = file_info.get("source_path", "")
            artifact_type = file_info.get("artifact_type", "")

            if not local_path_str:
                continue

            local_path = Path(local_path_str)
            if not local_path.exists():
                continue

            callbacks.on_progress(idx + 1, len(files), f"Parsing {local_path.name}")

            user = extract_user_from_path(source_path)
            profile = user or "Default"

            inventory_id = insert_browser_inventory(
                evidence_conn,
                evidence_id=evidence_id,
                browser="safari",
                artifact_type="sessions",
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
                if artifact_type == "last_session":
                    parsed = parse_session_plist(local_path)
                    stats = get_session_stats(parsed["windows"], parsed["tabs"])

                    window_records = []
                    for window in parsed["windows"]:
                        window_records.append(
                            {
                                "browser": "safari",
                                "profile": profile,
                                "window_id": window.window_index,
                                "selected_tab_index": window.selected_tab_index,
                                "window_type": "private" if window.is_private else "normal",
                                "session_type": "last_session",
                                "run_id": run_id,
                                "source_path": source_path,
                                "discovered_by": discovered_by,
                                "partition_index": file_info.get("partition_index"),
                                "fs_type": file_info.get("fs_type"),
                                "logical_path": source_path,
                                "forensic_path": source_path,
                            }
                        )

                    tab_records = []
                    for tab in parsed["tabs"]:
                        tab_records.append(
                            {
                                "browser": "safari",
                                "profile": profile,
                                "window_id": tab.window_index,
                                "tab_index": tab.tab_index,
                                "url": tab.tab_url,
                                "title": tab.tab_title,
                                "pinned": 1 if tab.is_pinned else 0,
                                "last_accessed_utc": tab.last_visit_time.isoformat() if tab.last_visit_time else None,
                                "run_id": run_id,
                                "source_path": source_path,
                                "discovered_by": discovered_by,
                                "partition_index": file_info.get("partition_index"),
                                "fs_type": file_info.get("fs_type"),
                                "logical_path": source_path,
                                "forensic_path": source_path,
                            }
                        )

                        url_record = self._make_url_record(
                            url=tab.tab_url,
                            context=f"session:safari:{profile}",
                            timestamp=tab.last_visit_time.isoformat() if tab.last_visit_time else None,
                            discovered_by=discovered_by,
                            run_id=run_id,
                            source_path=source_path,
                        )
                        if url_record:
                            url_records.append(url_record)

                    if window_records:
                        totals["windows"] += insert_session_windows(evidence_conn, evidence_id, window_records)
                    if tab_records:
                        totals["tabs"] += insert_session_tabs(evidence_conn, evidence_id, tab_records)

                    history_records = []
                    if parsed["history"]:
                        tab_id_map = self._build_tab_id_map(
                            evidence_conn,
                            evidence_id=evidence_id,
                            run_id=run_id,
                            source_path=source_path,
                        )

                        for history in parsed["history"]:
                            tab_id = tab_id_map.get((history.get("window_index"), history.get("tab_index")))
                            history_records.append(
                                {
                                    "browser": "safari",
                                    "profile": profile,
                                    "tab_id": tab_id,
                                    "nav_index": history.get("nav_index"),
                                    "url": history.get("url", ""),
                                    "title": history.get("title", ""),
                                    "transition_type": "",
                                    "timestamp_utc": history.get("timestamp_utc"),
                                    "run_id": run_id,
                                    "source_path": source_path,
                                    "discovered_by": discovered_by,
                                    "partition_index": file_info.get("partition_index"),
                                    "fs_type": file_info.get("fs_type"),
                                    "logical_path": source_path,
                                    "forensic_path": source_path,
                                }
                            )

                            url_record = self._make_url_record(
                                url=history.get("url", ""),
                                context=f"session:safari:{profile}",
                                timestamp=history.get("timestamp_utc"),
                                discovered_by=discovered_by,
                                run_id=run_id,
                                source_path=source_path,
                            )
                            if url_record:
                                url_records.append(url_record)

                    if history_records:
                        totals["history"] += insert_session_tab_histories(evidence_conn, evidence_id, history_records)

                    callbacks.on_log(
                        f"Parsed session file for {profile}: windows={stats['total_windows']}, tabs={stats['total_tabs']}",
                        "info",
                    )

                elif artifact_type == "recently_closed_tabs":
                    closed_tabs = parse_recently_closed_tabs(local_path)

                    closed_records = []
                    for closed in closed_tabs:
                        closed_at_utc = closed.date_closed.isoformat() if closed.date_closed else None
                        closed_records.append(
                            {
                                "browser": "safari",
                                "profile": profile,
                                "url": closed.tab_url,
                                "title": closed.tab_title,
                                "closed_at_utc": closed_at_utc,
                                "run_id": run_id,
                                "source_path": source_path,
                                "discovered_by": discovered_by,
                                "partition_index": file_info.get("partition_index"),
                                "fs_type": file_info.get("fs_type"),
                                "logical_path": source_path,
                                "forensic_path": source_path,
                            }
                        )

                        url_record = self._make_url_record(
                            url=closed.tab_url,
                            context=f"closed_tab:safari:{profile}",
                            timestamp=closed_at_utc,
                            discovered_by=discovered_by,
                            run_id=run_id,
                            source_path=source_path,
                        )
                        if url_record:
                            url_records.append(url_record)

                    if closed_records:
                        totals["closed_tabs"] += insert_closed_tabs(evidence_conn, evidence_id, closed_records)

                    callbacks.on_log(f"Parsed {len(closed_tabs)} recently closed tabs from {profile}", "info")

                file_records = (
                    len(window_records) + len(tab_records) + len(history_records)
                    if artifact_type == "last_session"
                    else len(closed_records)
                )
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=file_records,
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

        if url_records:
            totals["urls"] = insert_urls(evidence_conn, evidence_id, url_records)
            LOGGER.debug("Cross-posted %d Safari session URLs to urls table", totals["urls"])

        evidence_conn.commit()

        manifest_data["parsed_counts"] = totals
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        if collector:
            total_records = sum(totals.values())
            collector.report_ingested(evidence_id, self.metadata.name, records=total_records, **totals)
            status = "success" if total_records > 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        return totals

    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context safely."""
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

    def _extract_file(
        self,
        evidence_fs,
        path_str: str,
        output_dir: Path,
        partition_index: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Extract a single Safari session file from evidence."""
        try:
            content = evidence_fs.read_file(path_str)

            original_name = Path(path_str).name
            original_lower = original_name.lower()

            if original_lower == "lastsession.plist":
                artifact_type = "last_session"
            elif original_lower == "recentlyclosedtabs.plist":
                artifact_type = "recently_closed_tabs"
            else:
                artifact_type = "session_other"

            user = extract_user_from_path(path_str) or "unknown"
            path_hash = hashlib.sha256(path_str.encode("utf-8", errors="ignore")).hexdigest()[:8]
            if partition_index is not None:
                safe_name = f"safari_{user}_p{partition_index}_{path_hash}_{original_name}"
            else:
                safe_name = f"safari_{user}_{path_hash}_{original_name}"

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
                "partition_index": getattr(evidence_fs, "partition_index", None),
                "fs_type": getattr(evidence_fs, "fs_type", None),
            }

        except Exception as e:
            LOGGER.debug("Failed to extract %s: %s", path_str, e)
            return None

    def _build_tab_id_map(
        self,
        evidence_conn,
        *,
        evidence_id: int,
        run_id: str,
        source_path: str,
    ) -> Dict[tuple[int, int], int]:
        """Build (window_id, tab_index) -> session_tabs.id map for a single source file."""
        cursor = evidence_conn.execute(
            """
            SELECT id, window_id, tab_index
            FROM session_tabs
            WHERE evidence_id = ?
              AND run_id = ?
              AND browser = 'safari'
              AND source_path = ?
            """,
            (evidence_id, run_id, source_path),
        )
        rows = cursor.fetchall()
        return {(row[1], row[2]): row[0] for row in rows}

    def _make_url_record(
        self,
        *,
        url: str,
        context: str,
        timestamp: Optional[str],
        discovered_by: str,
        run_id: str,
        source_path: str,
    ) -> Optional[Dict[str, Any]]:
        """Build URL row for cross-posting, skipping Safari internal/about URLs."""
        url = (url or "").strip()
        if not url:
            return None

        lower = url.lower()
        if lower.startswith(("about:", "safari-", "javascript:", "data:")):
            return None

        parsed = urlparse(url)
        return {
            "url": url,
            "domain": parsed.netloc or None,
            "scheme": parsed.scheme or None,
            "discovered_by": discovered_by,
            "run_id": run_id,
            "source_path": source_path,
            "context": context,
            "first_seen_utc": timestamp,
            "last_seen_utc": timestamp,
        }
