"""
Safari Bookmarks Extractor.

Extracts bookmarks from Safari on macOS.
Uses plist format (binary or XML).

Note: Safari support is EXPERIMENTAL.
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
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from .._discovery import discover_safari_files, discover_safari_files_fallback
from .._patterns import get_patterns, extract_user_from_path
from .._parsers import parse_bookmarks, get_bookmark_stats

from core.logging import get_logger
from core.database import (
    insert_urls,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)

LOGGER = get_logger("extractors.browser.safari.bookmarks")


class SafariBookmarksExtractor(BaseExtractor):
    """
    Extract Safari bookmarks from macOS evidence.

    Parses Bookmarks.plist file (binary or XML plist format).

    Safari bookmarks use WebBookmarkType to distinguish:
    - WebBookmarkTypeLeaf: Actual bookmarks
    - WebBookmarkTypeList: Folders
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="safari_bookmarks",
            display_name="Safari Bookmarks (macOS)",
            description="Extract Safari bookmarks from macOS - EXPERIMENTAL",
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
                bookmarks = parsed.get("bookmarks", 0)
                folders = parsed.get("folders", 0)
                status_text = (
                    f"Safari Bookmarks (EXPERIMENTAL)\n"
                    f"Files: {file_count}\n"
                    f"Bookmarks: {bookmarks}\n"
                    f"Folders: {folders}\n"
                    f"Run ID: {data.get('run_id', 'N/A')}"
                )
            except Exception:
                status_text = "Safari Bookmarks - Error reading manifest"
        else:
            status_text = "Safari Bookmarks (EXPERIMENTAL)\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Path:
        """Return output directory for Safari bookmarks extraction."""
        return case_root / "evidences" / evidence_label / "safari_bookmarks"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Safari Bookmarks.plist files from evidence."""
        from core.statistics_collector import StatisticsCollector

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        collector = StatisticsCollector.get_instance()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        callbacks.on_step("Initializing Safari bookmarks extraction")
        LOGGER.info("Starting Safari bookmarks extraction (run_id=%s)", run_id)

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
        callbacks.on_step("Discovering Safari bookmarks files")
        evidence_conn = config.get("evidence_conn")

        files_by_partition = discover_safari_files(
            evidence_conn, evidence_id,
            artifact_names=["bookmarks"],
            callbacks=callbacks,
        )
        if not files_by_partition:
            files_by_partition = discover_safari_files_fallback(
                evidence_fs, artifact_names=["bookmarks"], callbacks=callbacks,
            )

        manifest_data["multi_partition"] = len(files_by_partition) > 1
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        total_discovered = sum(len(v) for v in files_by_partition.values())

        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=total_discovered)

        if not files_by_partition:
            LOGGER.info("No Safari bookmarks files found")
            manifest_data["notes"].append("No Safari bookmarks files found")
        else:
            callbacks.on_step(f"Extracting {total_discovered} Safari bookmarks files")

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
                                fs_to_use, path_str, files_dir, run_id,
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
                            manifest_data["notes"].append(f"Failed: {file_data.get('logical_path', '?')}: {e}")
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

        LOGGER.info("Safari bookmarks extraction complete: %d files", len(manifest_data["files"]))

        return manifest_data["status"] != "error"

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """Parse extracted Safari bookmarks and ingest into database."""
        from core.statistics_collector import StatisticsCollector
        from core.database import insert_bookmarks

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"bookmarks": 0, "folders": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", "unknown")
        evidence_label = config.get("evidence_label", "")

        collector = StatisticsCollector.get_instance()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        files = manifest_data.get("files", [])

        if not files:
            callbacks.on_log("No Safari bookmarks files to ingest", "warning")
            if collector:
                # CRITICAL: Report ingested counts even when 0
                collector.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0,
                    bookmarks=0,
                )
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return {"bookmarks": 0, "folders": 0}

        total_bookmarks = 0
        total_folders = 0
        all_records = []
        url_records = []  # Collect URLs for unified urls table

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
                artifact_type="bookmarks",
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
                from urllib.parse import urlparse

                bookmarks = parse_bookmarks(local_path)

                for bookmark in bookmarks:
                    # Only insert actual bookmarks (not folder entries)
                    if bookmark.bookmark_type != "leaf":
                        continue

                    all_records.append({
                        "browser": "safari",
                        "profile": profile,
                        "url": bookmark.url,
                        "title": bookmark.title,
                        "folder_path": bookmark.folder_path,
                        "date_added": bookmark.date_added_utc,
                        "run_id": run_id,
                        "source_path": source_path,
                        "partition_index": file_info.get("partition_index"),
                        "fs_type": file_info.get("fs_type"),
                        "logical_path": source_path,
                        "forensic_path": source_path,
                        "discovered_by": discovered_by,
                    })

                    # Collect URL for unified urls table (skip javascript/data URIs)
                    if bookmark.url and not bookmark.url.startswith(("javascript:", "data:")):
                        parsed = urlparse(bookmark.url)
                        url_records.append({
                            "url": bookmark.url,
                            "domain": parsed.netloc or None,
                            "scheme": parsed.scheme or None,
                            "discovered_by": discovered_by,
                            "run_id": run_id,
                            "source_path": source_path,
                            "context": f"bookmark:safari:{profile}",
                            "first_seen_utc": bookmark.date_added_utc,
                        })

                stats = get_bookmark_stats(bookmarks)
                total_bookmarks += stats.get("total_bookmarks", 0)
                total_folders += stats.get("unique_folders", 0)

                callbacks.on_log(f"Parsed {len(bookmarks)} bookmarks from {profile}", "info")

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=len(bookmarks),
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
            callbacks.on_step("Inserting bookmark records")
            inserted = insert_bookmarks(evidence_conn, evidence_id, all_records)
            evidence_conn.commit()
            callbacks.on_log(f"Ingested {inserted} bookmarks", "info")

        # Cross-post URLs to unified urls table for analysis
        if url_records:
            try:
                insert_urls(evidence_conn, evidence_id, url_records)
                evidence_conn.commit()
                LOGGER.debug("Cross-posted %d bookmark URLs to urls table", len(url_records))
            except Exception as e:
                LOGGER.debug("Failed to cross-post bookmark URLs: %s", e)

        manifest_data["parsed_counts"] = {
            "bookmarks": total_bookmarks,
            "folders": total_folders,
        }
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        # Report ingested counts (always, even if 0) and finish
        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=inserted,
                bookmarks=inserted,
            )
            status = "success" if inserted > 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        return {"bookmarks": total_bookmarks, "folders": total_folders}

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
        run_id: str,
        partition_index: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """Extract a single file from evidence."""
        try:
            content = evidence_fs.read_file(path_str)

            original_name = Path(path_str).name
            user = extract_user_from_path(path_str) or "unknown"
            if partition_index is not None:
                safe_name = f"safari_{user}_p{partition_index}_{original_name}"
            else:
                safe_name = f"safari_{user}_{original_name}"

            dest_path = output_dir / safe_name
            dest_path.write_bytes(content)

            md5 = hashlib.md5(content).hexdigest()
            sha256 = hashlib.sha256(content).hexdigest()

            return {
                "local_path": str(dest_path),
                "source_path": path_str,
                "artifact_type": "bookmarks",
                "browser": "safari",
                "user": user,
                "md5": md5,
                "sha256": sha256,
                "size_bytes": len(content),
            }

        except Exception as e:
            LOGGER.debug("Failed to extract %s: %s", path_str, e)
            return None
