"""
Chromium Favicons & Top Sites Extractor

Extracts favicon icons and top sites from Chrome, Edge, Opera, Brave.

Features:
- Chromium Favicons SQLite parsing (favicon_bitmaps, favicons, icon_mapping)
- Top Sites SQLite parsing (top_sites table)
- Icon deduplication via SHA256 hashing
- Size guardrails (skip icons > 1MB)
- Page URL to icon mapping
- StatisticsCollector integration
- Schema warning support for unknown tables/columns
- Cross-posting favicons and thumbnails to images table

Data Sources:
- Favicons database: favicon_bitmaps, favicons, icon_mapping tables
- Top Sites database: top_sites, thumbnails tables

Split into modules, added schema warnings, removed URL dedup,
        fixed insert_favicon signature, cross-post thumbnails to images
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from .._patterns import (
    CHROMIUM_BROWSERS,
    get_artifact_patterns,
)
from ....widgets import BrowserConfigWidget
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.browser.chromium.favicons")


class ChromiumFaviconsExtractor(BaseExtractor):
    """
    Extract Chromium browser favicon icons and top sites from evidence images.

    Supports: Chrome, Edge, Opera, Brave.

    Features:
    - Favicon icons with page mappings
    - Top sites with URL rank
    - Icon deduplication via hashing
    - Size guardrails
    - Schema warning support
    """

    SUPPORTED_BROWSERS = list(CHROMIUM_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="chromium_favicons",
            display_name="Chromium Favicons & Top Sites",
            description="Extract favicon icons and top sites from Chromium browsers (Chrome, Edge, Opera, Brave)",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True
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
        """Return configuration widget (browser selection + multi-partition)."""
        return BrowserConfigWidget(
            parent,
            supported_browsers=self.SUPPORTED_BROWSERS,
            default_scan_all_partitions=True,
        )

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction summary."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            files = data.get("files", [])
            # Count by artifact type
            favicon_db_count = sum(1 for f in files if f.get("artifact_type") == "favicons")
            top_sites_db_count = sum(1 for f in files if f.get("artifact_type") == "top_sites")
            status_text = (
                f"Chromium Favicons & Top Sites\n"
                f"Favicon DBs: {favicon_db_count}\n"
                f"Top Sites DBs: {top_sites_db_count}\n"
                f"Run ID: {data.get('run_id', 'N/A')}"
            )
        else:
            status_text = "Chromium Favicons & Top Sites\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory."""
        return case_root / "evidences" / evidence_label / "chromium_favicons"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract favicon and top sites databases from evidence.

        Copies Favicons and Top Sites SQLite databases to workspace.
        """
        callbacks.on_step("Initializing Chromium favicon extraction")

        run_id = self._generate_run_id()
        LOGGER.info("Starting Chromium favicons extraction (run_id=%s)", run_id)

        # Start statistics tracking
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.get_instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "e01_context": self._get_e01_context(evidence_fs),
            "files": [],
            "status": "ok",
            "notes": [],
        }

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)

        favicon_db_count = 0
        top_sites_db_count = 0

        callbacks.on_step("Scanning for favicon databases")

        # Discover and copy favicon databases
        favicon_dbs = self._discover_and_copy_databases(
            evidence_fs, output_dir, browsers_to_search, "favicons", callbacks
        )
        manifest_data["files"].extend(favicon_dbs)
        favicon_db_count = len(favicon_dbs)

        callbacks.on_step("Scanning for top sites databases")

        # Discover and copy top sites databases
        top_sites_dbs = self._discover_and_copy_databases(
            evidence_fs, output_dir, browsers_to_search, "top_sites", callbacks
        )
        manifest_data["files"].extend(top_sites_dbs)
        top_sites_db_count = len(top_sites_dbs)

        if not favicon_dbs and not top_sites_dbs:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Chromium favicon or top sites databases found")
            LOGGER.info("No Chromium favicon or top sites databases found")

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

        LOGGER.info(
            "Chromium favicons extraction complete: %d favicon DBs, %d top sites DBs",
            favicon_db_count,
            top_sites_db_count,
        )

        # Complete statistics tracking
        final_status = "skipped" if manifest_data["status"] == "skipped" else "success" if manifest_data["status"] == "ok" else "partial"
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, favicon_dbs=favicon_db_count, top_sites_dbs=top_sites_db_count)
            stats.finish_run(evidence_id, self.metadata.name, status=final_status)

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
        Parse extracted databases and ingest into evidence database.
        """
        from core.database import delete_favicons_by_run, delete_top_sites_by_run, insert_urls
        from extractors._shared.extraction_warnings import ExtractionWarningCollector
        from ._parsers import parse_favicons_database, parse_top_sites_database

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"records": 0, "favicons": 0, "favicon_mappings": 0, "top_sites": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])
        evidence_label = config.get("evidence_label", "")

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Continue statistics tracking with same run_id from extraction
        stats = StatisticsCollector.get_instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No databases to ingest", "warning")
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, favicons=0, top_sites=0)
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"records": 0, "favicons": 0, "favicon_mappings": 0, "top_sites": 0}

        # Clear previous data for this run
        delete_favicons_by_run(evidence_conn, evidence_id, run_id)
        delete_top_sites_by_run(evidence_conn, evidence_id, run_id)

        favicon_count = 0
        mapping_count = 0
        top_sites_count = 0
        url_records: List[Dict[str, Any]] = []
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"

        callbacks.on_progress(0, len(files), "Processing databases")

        try:
            for i, file_info in enumerate(files):
                if callbacks.is_cancelled():
                    break

                local_path = file_info.get("local_path")
                if not local_path:
                    continue

                db_path = Path(local_path)
                if not db_path.exists():
                    callbacks.on_log(f"Database not found: {db_path}", "warning")
                    continue

                browser = file_info.get("browser")
                profile = file_info.get("profile")
                artifact_type = file_info.get("artifact_type")

                callbacks.on_progress(i + 1, len(files), f"Processing {browser} {artifact_type}")

                try:
                    if artifact_type == "favicons":
                        fc, mc, urls = parse_favicons_database(
                            db_path=db_path,
                            evidence_conn=evidence_conn,
                            evidence_id=evidence_id,
                            run_id=run_id,
                            browser=browser,
                            profile=profile,
                            file_info=file_info,
                            output_dir=output_dir,
                            extractor_name=self.metadata.name,
                            extractor_version=self.metadata.version,
                            callbacks=callbacks,
                            warning_collector=warning_collector,
                        )
                        favicon_count += fc
                        mapping_count += mc

                        # Collect URLs without deduplication
                        for url_info in urls:
                            url = url_info.get("url")
                            if url and not url.startswith(("javascript:", "data:")):
                                url_records.append({
                                    "url": url,
                                    "domain": url_info.get("domain"),
                                    "scheme": url_info.get("scheme"),
                                    "discovered_by": discovered_by,
                                    "run_id": run_id,
                                    "source_path": file_info.get("source_path"),
                                    "context": url_info.get("context", f"favicon:{browser}:{profile}"),
                                    "first_seen_utc": url_info.get("timestamp"),
                                })

                    elif artifact_type == "top_sites":
                        ts, urls = parse_top_sites_database(
                            db_path=db_path,
                            evidence_conn=evidence_conn,
                            evidence_id=evidence_id,
                            run_id=run_id,
                            browser=browser,
                            profile=profile,
                            file_info=file_info,
                            output_dir=output_dir,
                            extractor_name=self.metadata.name,
                            extractor_version=self.metadata.version,
                            callbacks=callbacks,
                            warning_collector=warning_collector,
                        )
                        top_sites_count += ts

                        # Collect URLs without deduplication
                        for url_info in urls:
                            url = url_info.get("url")
                            if url and not url.startswith(("javascript:", "data:")):
                                url_records.append({
                                    "url": url,
                                    "domain": url_info.get("domain"),
                                    "scheme": url_info.get("scheme"),
                                    "discovered_by": discovered_by,
                                    "run_id": run_id,
                                    "source_path": file_info.get("source_path"),
                                    "context": f"top_sites:{browser}:{profile}",
                                    "first_seen_utc": url_info.get("timestamp"),
                                })

                except Exception as e:
                    error_msg = f"Failed to ingest {artifact_type} from {db_path}: {e}"
                    LOGGER.error(error_msg, exc_info=True)
                    callbacks.on_log(error_msg, "error")

            # Cross-post URLs to unified urls table for analysis
            urls_table_count = 0
            if url_records:
                try:
                    insert_urls(evidence_conn, evidence_id, url_records)
                    urls_table_count = len(url_records)
                    LOGGER.debug("Cross-posted %d favicon/top_sites URLs to urls table", urls_table_count)
                except Exception as e:
                    LOGGER.debug("Failed to cross-post favicon URLs: %s", e)

            evidence_conn.commit()

        finally:
            # Always flush warnings at the end
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info("Recorded %d extraction warnings", warning_count)
                callbacks.on_log(f"Recorded {warning_count} schema warnings", "info")

        # Report ingested counts and finish statistics tracking
        total_records = favicon_count + mapping_count + top_sites_count
        if stats:
            stats.report_ingested(evidence_id, self.metadata.name, records=total_records, favicons=favicon_count, top_sites=top_sites_count)
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {
            "records": total_records,
            "favicons": favicon_count,
            "favicon_mappings": mapping_count,
            "top_sites": top_sites_count,
            "urls_table": urls_table_count,
        }

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context."""
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

    def _discover_and_copy_databases(
        self,
        evidence_fs,
        output_dir: Path,
        browsers: List[str],
        artifact_type: str,
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """Discover and copy databases for a given artifact type."""
        copied_files = []

        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                continue

            try:
                patterns = get_artifact_patterns(browser, artifact_type)
            except ValueError:
                continue

            if not patterns:
                continue

            for pattern in patterns:
                # Skip journal files for separate handling
                if "-journal" in pattern:
                    continue

                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        # Skip journal/wal files for separate tracking
                        if "-journal" in path_str or "-wal" in path_str or "-shm" in path_str:
                            continue

                        try:
                            file_info = self._copy_database(
                                evidence_fs, path_str, output_dir,
                                browser, artifact_type, callbacks
                            )
                            if file_info:
                                copied_files.append(file_info)
                                callbacks.on_log(
                                    f"Copied {browser} {artifact_type}: {path_str}", "info"
                                )
                        except Exception as e:
                            LOGGER.debug("Failed to copy %s: %s", path_str, e)

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return copied_files

    def _copy_database(
        self,
        evidence_fs,
        source_path: str,
        output_dir: Path,
        browser: str,
        artifact_type: str,
        callbacks: ExtractorCallbacks
    ) -> Optional[Dict]:
        """Copy a single database file and its journal files."""
        try:
            content = evidence_fs.read_file(source_path)

            # Extract profile from path
            profile = self._extract_profile(source_path, browser)

            # Generate safe filename
            safe_browser = re.sub(r'[^a-zA-Z0-9_-]', '_', browser)
            safe_profile = re.sub(r'[^a-zA-Z0-9_-]', '_', profile)

            # Determine original filename
            original_name = Path(source_path).name
            filename = f"{safe_browser}_{safe_profile}_{original_name}"

            dest_path = output_dir / filename
            dest_path.write_bytes(content)

            # Calculate hashes
            md5 = hashlib.md5(content).hexdigest()
            sha256 = hashlib.sha256(content).hexdigest()

            # Copy journal/wal files if they exist
            journal_files = []
            for suffix in ["-journal", "-wal", "-shm"]:
                journal_path = source_path + suffix
                try:
                    journal_content = evidence_fs.read_file(journal_path)
                    journal_dest = output_dir / (filename + suffix)
                    journal_dest.write_bytes(journal_content)
                    journal_files.append(str(journal_dest))
                except Exception:
                    pass

            return {
                "local_path": str(dest_path),
                "source_path": source_path,
                "browser": browser,
                "profile": profile,
                "artifact_type": artifact_type,
                "md5": md5,
                "sha256": sha256,
                "size_bytes": len(content),
                "journal_files": journal_files,
            }

        except Exception as e:
            LOGGER.error("Failed to copy %s: %s", source_path, e)
            return None

    def _extract_profile(self, path: str, browser: str) -> str:
        """Extract profile name from path."""
        parts = path.split('/')

        # Chromium-style: .../User Data/Default/... or .../User Data/Profile 1/...
        for i, part in enumerate(parts):
            if part == "User Data" and i + 1 < len(parts):
                return parts[i + 1]
            # Opera: directly under Opera Stable
            if part in ("Opera Stable", "Opera GX Stable") and i + 1 < len(parts):
                return parts[i + 1] if parts[i + 1] not in ("Favicons", "Top Sites") else "Default"

        return "Default"
