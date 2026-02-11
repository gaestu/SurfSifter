"""
Chromium Site Engagement Extractor

Extracts and ingests site engagement data from Chromium-based browsers:
- Chrome, Edge, Opera, Brave

Features:
- Preferences JSON parsing (profile.content_settings.exceptions)
- Site engagement metrics (rawScore, lastEngagementTime, etc.)
- Media engagement metrics (visits, mediaPlaybacks, etc.)
- StatisticsCollector integration for run tracking
- URL cross-posting to unified urls table

Data Format:
- Chromium stores engagement in Preferences JSON file
- Sections:
  - profile.content_settings.exceptions.site_engagement
  - profile.content_settings.exceptions.media_engagement
- Sites with higher engagement are prioritized by Chrome for various features
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import BrowserConfigWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    open_partition_for_extraction,
)
from ...._shared.timestamps import webkit_to_datetime
from ...._shared.extraction_warnings import ExtractionWarningCollector
from .._patterns import CHROMIUM_BROWSERS, get_artifact_patterns
from .._parsers import detect_browser_from_path
from ._schemas import (
    ENGAGEMENT_SETTING_KEYS,
    SITE_ENGAGEMENT_SETTING_FIELDS,
    MEDIA_ENGAGEMENT_SETTING_FIELDS,
    ALL_ENGAGEMENT_SETTING_FIELDS,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_site_engagements,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    delete_site_engagement_by_run,
)

LOGGER = get_logger("extractors.browser.chromium.site_engagement")


class ChromiumSiteEngagementExtractor(BaseExtractor):
    """
    Extract site engagement data from Chromium-based browsers.

    Parses Preferences JSON file to extract engagement metrics.
    Supports Chrome, Edge, Brave, Opera.
    """

    SUPPORTED_BROWSERS = list(CHROMIUM_BROWSERS.keys())

    # Engagement types to extract (keys in profile.content_settings.exceptions)
    ENGAGEMENT_TYPES = ("site_engagement", "media_engagement")

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="chromium_site_engagement",
            display_name="Chromium Site Engagement",
            description="Extract site/media engagement from Chrome/Edge/Opera/Brave Preferences",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True
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
            status_text = f"Chromium Site Engagement\nFiles extracted: {file_count}\nRun ID: {data.get('run_id', 'N/A')}"
        else:
            status_text = "Chromium Site Engagement\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "chromium_site_engagement"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Chromium Preferences files from evidence."""
        callbacks.on_step("Initializing Chromium site engagement extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Chromium site engagement extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        # Determine multi-partition mode
        scan_all_partitions = config.get("scan_all_partitions", True)
        evidence_db_path = config.get("evidence_db_path")

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_extraction_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "multi_partition": scan_all_partitions,
            "files": [],
            "status": "ok",
            "notes": [],
        }

        callbacks.on_step("Scanning for Chromium Preferences files")

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        # Filter to only Chromium browsers
        browsers_to_search = [b for b in browsers_to_search if b in self.SUPPORTED_BROWSERS]

        # Scan for Preferences files (multi-partition aware)
        # Note: We re-use permissions artifact pattern since both read Preferences
        if scan_all_partitions and evidence_db_path:
            # Use file_list discovery for multi-partition support
            files_by_partition = self._discover_files_multi_partition(
                evidence_db_path, evidence_id, browsers_to_search, callbacks
            )
        else:
            # Single partition fallback
            files_by_partition = {
                None: self._discover_preference_files(evidence_fs, browsers_to_search, callbacks)
            }

        # Count total files
        total_files = sum(len(files) for files in files_by_partition.values())

        # Report discovered files (always, even if 0)
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=total_files)

        callbacks.on_log(f"Found {total_files} Preferences file(s)")

        if total_files == 0:
            LOGGER.info("No Chromium Preferences files found")
        else:
            callbacks.on_progress(0, total_files, "Copying Preferences files")

            file_index = 0
            for partition_index, files in files_by_partition.items():
                # Get partition-specific filesystem
                with open_partition_for_extraction(evidence_fs, partition_index) as partition_fs:
                    if partition_fs is None:
                        LOGGER.warning("Cannot open partition %s", partition_index)
                        manifest_data["notes"].append(f"Failed to open partition {partition_index}")
                        continue

                    for file_info in files:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            manifest_data["notes"].append("Extraction cancelled by user")
                            break

                        file_index += 1
                        callbacks.on_progress(
                            file_index, total_files,
                            f"Copying {file_info['browser']} Preferences"
                        )

                        try:
                            extracted_file = self._extract_file(
                                partition_fs,
                                file_info,
                                output_dir,
                                callbacks,
                            )
                            manifest_data["files"].append(extracted_file)

                        except Exception as e:
                            error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                            LOGGER.error(error_msg, exc_info=True)
                            manifest_data["notes"].append(error_msg)
                            if stats:
                                stats.report_failed(evidence_id, self.metadata.name, files=1)

                if callbacks.is_cancelled():
                    break

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
            evidence_conn=config.get("evidence_conn"),
            evidence_id=evidence_id,
            run_id=run_id,
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
            manifest_data=manifest_data,
            callbacks=callbacks,
        )

        LOGGER.info(
            "Chromium site engagement extraction complete: %d files, status=%s",
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
        """Parse extracted Preferences and ingest engagement data into database."""
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"records": 0, "site_engagement": 0, "media_engagement": 0}

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
                    evidence_id, self.metadata.name,
                    records=0,
                    site_engagement=0,
                    media_engagement=0,
                )
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"records": 0, "site_engagement": 0, "media_engagement": 0}

        total_site_engagement = 0
        total_media_engagement = 0
        url_records = []  # Collect URLs for unified urls table

        # Create warning collector for schema warnings
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Clear previous data for this run
        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        callbacks.on_progress(0, len(files), "Parsing Preferences files")

        try:
            for i, file_entry in enumerate(files):
                if callbacks.is_cancelled():
                    break

                if file_entry.get("copy_status") == "error":
                    callbacks.on_log(f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}", "warning")
                    continue

                callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry['browser']} engagement data")

                try:
                    inventory_id = insert_browser_inventory(
                        evidence_conn,
                        evidence_id=evidence_id,
                        browser=file_entry["browser"],
                        artifact_type="site_engagement",
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

                    site_count, media_count, urls = self._parse_preferences_file(
                        db_path,
                        file_entry,
                        run_id,
                        evidence_id,
                        evidence_conn,
                        callbacks,
                        warning_collector=warning_collector,
                    )

                    total_site_engagement += site_count
                    total_media_engagement += media_count

                    # Collect URLs for cross-posting
                    url_records.extend(urls)

                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="ok",
                        records_parsed=site_count + media_count,
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
        finally:
            # Always flush warnings, even on error
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info("Recorded %d extraction warnings for schema unknowns", warning_count)

        # Cross-post URLs to unified urls table for analysis
        urls_table_count = 0
        if url_records:
            try:
                from core.database import insert_urls
                insert_urls(evidence_conn, evidence_id, url_records)
                urls_table_count = len(url_records)
                LOGGER.debug("Cross-posted %d engagement URLs to urls table", urls_table_count)
            except Exception as e:
                LOGGER.debug("Failed to cross-post engagement URLs: %s", e)

        evidence_conn.commit()

        total_records = total_site_engagement + total_media_engagement

        # Report ingested counts and finish
        if stats:
            stats.report_ingested(
                evidence_id, self.metadata.name,
                records=total_records,
                site_engagement=total_site_engagement,
                media_engagement=total_media_engagement,
            )
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        callbacks.on_log(
            f"Ingested {total_site_engagement} site engagement + {total_media_engagement} media engagement = {total_records} total",
            "info",
        )

        return {
            "records": total_records,
            "site_engagement": total_site_engagement,
            "media_engagement": total_media_engagement,
            "urls_table": urls_table_count,
        }

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _generate_run_id(self) -> str:
        """Generate run ID: {timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
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

    def _discover_preference_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """Scan evidence for Chromium Preferences files."""
        preference_files = []

        for browser_key in browsers:
            if browser_key not in CHROMIUM_BROWSERS:
                continue

            # Use permissions pattern since we're reading the same file
            patterns = get_artifact_patterns(browser_key, "permissions")

            if not patterns:
                continue

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        # Only process Preferences files
                        filename = path_str.split('/')[-1].lower()
                        if filename != "preferences":
                            continue

                        profile = self._extract_profile_from_path(path_str, browser_key)

                        preference_files.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "profile": profile,
                            "file_type": "preferences",
                        })
                except Exception as e:
                    LOGGER.debug("Error scanning pattern %s: %s", pattern, e)

        return preference_files

    def _discover_files_multi_partition(
        self,
        evidence_db_path: str,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> Dict[Optional[int], List[Dict]]:
        """Discover Preferences files across partitions using file_list."""
        files_by_partition: Dict[Optional[int], List[Dict]] = {}

        # Build filename patterns for Preferences
        filename_patterns = ["Preferences"]

        # Build path patterns for each browser
        path_patterns = []
        for browser_key in browsers:
            if browser_key not in CHROMIUM_BROWSERS:
                continue
            browser_info = CHROMIUM_BROWSERS[browser_key]
            for path_template in browser_info.get("profile_paths", []):
                # Convert template to a match pattern
                # e.g., "Users/*/AppData/Local/Google/Chrome/User Data/*"
                path_patterns.append(path_template.replace("*", "%"))

        discovered = discover_from_file_list(
            evidence_db_path=evidence_db_path,
            evidence_id=evidence_id,
            filename_patterns=filename_patterns,
            path_like_patterns=path_patterns,
        )

        for entry in discovered:
            partition_index = entry.get("partition_index")
            if partition_index not in files_by_partition:
                files_by_partition[partition_index] = []

            path_str = entry.get("logical_path", "")
            browser = detect_browser_from_path(path_str)
            if browser not in browsers:
                continue

            profile = self._extract_profile_from_path(path_str, browser)

            files_by_partition[partition_index].append({
                "logical_path": path_str,
                "browser": browser,
                "profile": profile,
                "file_type": "preferences",
                "partition_index": partition_index,
                "fs_type": entry.get("fs_type"),
                "forensic_path": entry.get("forensic_path"),
            })

        return files_by_partition

    def _extract_profile_from_path(self, path: str, browser: str) -> str:
        """Extract profile name from Preferences path."""
        parts = path.replace("\\", "/").split("/")

        # Look for "User Data" or browser-specific profile markers
        for i, part in enumerate(parts):
            if part.lower() == "user data" and i + 1 < len(parts):
                return parts[i + 1]
            if part.lower() == "profiles" and i + 1 < len(parts):
                return parts[i + 1]

        return "Default"

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Extract single Preferences file from evidence."""
        logical_path = file_info["logical_path"]
        browser = file_info["browser"]
        profile = file_info.get("profile", "Default")

        # Create unique output filename
        safe_browser = browser.replace("/", "_").replace("\\", "_")
        safe_profile = profile.replace("/", "_").replace("\\", "_")
        partition_suffix = f"_p{file_info.get('partition_index', 0)}" if file_info.get('partition_index') else ""
        output_filename = f"{safe_browser}_{safe_profile}{partition_suffix}_Preferences"
        output_path = output_dir / output_filename

        result = {
            "logical_path": logical_path,
            "extracted_path": str(output_path),
            "browser": browser,
            "profile": profile,
            "partition_index": file_info.get("partition_index"),
            "fs_type": file_info.get("fs_type"),
            "forensic_path": file_info.get("forensic_path"),
            "copy_status": "ok",
        }

        try:
            # Read file from evidence
            content = evidence_fs.read_file(logical_path)

            # Write to output
            output_path.write_bytes(content)

            # Calculate hashes
            result["md5"] = hashlib.md5(content).hexdigest()
            result["sha256"] = hashlib.sha256(content).hexdigest()
            result["file_size_bytes"] = len(content)

        except Exception as e:
            result["copy_status"] = "error"
            result["error_message"] = str(e)
            LOGGER.error("Failed to extract %s: %s", logical_path, e)

        return result

    def _clear_previous_run(
        self,
        evidence_conn,
        evidence_id: int,
        run_id: str
    ) -> None:
        """Clear data from previous run with same run_id."""
        try:
            delete_site_engagement_by_run(evidence_conn, evidence_id, run_id)
        except Exception as e:
            LOGGER.debug("Error clearing previous run: %s", e)

    def _parse_preferences_file(
        self,
        file_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Tuple[int, int, List[Dict]]:
        """Parse Chromium Preferences JSON for engagement data.

        Args:
            file_path: Path to extracted Preferences file
            file_entry: File metadata dict from manifest
            run_id: Current run ID
            evidence_id: Evidence ID
            evidence_conn: Database connection
            callbacks: Extraction callbacks
            warning_collector: Optional warning collector for schema warnings

        Returns:
            Tuple of (site_engagement_count, media_engagement_count, url_list)
        """
        if not file_path.exists():
            LOGGER.warning("Preferences file not found: %s", file_path)
            return 0, 0, []

        try:
            prefs = json.loads(file_path.read_text(encoding='utf-8', errors='replace'))
        except Exception as e:
            LOGGER.error("Failed to parse Preferences: %s", e)
            return 0, 0, []

        browser = file_entry["browser"]
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"
        profile = file_entry.get("profile", "Default")
        source_file = file_entry.get("logical_path", str(file_path))

        site_records = []
        media_records = []
        url_list = []

        # Navigate to content_settings.exceptions
        content_settings = prefs.get("profile", {}).get("content_settings", {})
        exceptions = content_settings.get("exceptions", {})

        for engagement_type in self.ENGAGEMENT_TYPES:
            origins = exceptions.get(engagement_type, {})
            if not isinstance(origins, dict):
                continue

            for origin, entry in origins.items():
                if not isinstance(entry, dict):
                    continue

                # Track unknown entry keys
                if warning_collector:
                    for key in entry.keys():
                        if key not in ENGAGEMENT_SETTING_KEYS:
                            warning_collector.add_warning(
                                warning_type="json_unknown_key",
                                item_name=f"exceptions.{engagement_type}.{key}",
                                severity="info",
                                category="json",
                                artifact_type="site_engagement",
                                source_file=source_file,
                                item_value=str(type(entry[key]).__name__),
                            )

                # Get the setting dict
                setting = entry.get("setting", {})
                if not isinstance(setting, dict):
                    # Some entries have scalar setting values - skip
                    continue

                # Track unknown setting fields
                if warning_collector:
                    for key in setting.keys():
                        if key not in ALL_ENGAGEMENT_SETTING_FIELDS:
                            warning_collector.add_warning(
                                warning_type="json_unknown_key",
                                item_name=f"exceptions.{engagement_type}.setting.{key}",
                                severity="info",
                                category="json",
                                artifact_type="site_engagement",
                                source_file=source_file,
                                item_value=str(type(setting[key]).__name__),
                            )

                # Parse timestamps
                last_modified = entry.get("last_modified")
                last_modified_utc = None
                if last_modified:
                    try:
                        dt = webkit_to_datetime(int(last_modified))
                        last_modified_utc = dt.isoformat() if dt else None
                    except (ValueError, TypeError, OSError, OverflowError):
                        pass

                expiration = entry.get("expiration")
                expiration_utc = None
                if expiration:
                    try:
                        dt = webkit_to_datetime(int(expiration))
                        expiration_utc = dt.isoformat() if dt else None
                    except (ValueError, TypeError, OSError, OverflowError):
                        pass

                # Build base record
                base_record = {
                    "browser": browser,
                    "profile": profile,
                    "origin": origin,
                    "engagement_type": engagement_type,
                    "last_modified_webkit": last_modified,
                    "expiration": expiration_utc,
                    "model": entry.get("model"),
                    "run_id": run_id,
                    "source_path": file_entry["logical_path"],
                    "discovered_by": discovered_by,
                    "partition_index": file_entry.get("partition_index"),
                    "fs_type": file_entry.get("fs_type"),
                    "logical_path": file_entry["logical_path"],
                    "forensic_path": file_entry.get("forensic_path"),
                }

                if engagement_type == "site_engagement":
                    # Site engagement specific fields
                    raw_score = setting.get("rawScore")

                    last_engagement_time = setting.get("lastEngagementTime")
                    last_engagement_utc = None
                    if last_engagement_time:
                        try:
                            dt = webkit_to_datetime(int(last_engagement_time))
                            last_engagement_utc = dt.isoformat() if dt else None
                        except (ValueError, TypeError, OSError, OverflowError):
                            pass

                    last_shortcut_time = setting.get("lastShortcutLaunchTime")
                    last_shortcut_utc = None
                    if last_shortcut_time:
                        try:
                            dt = webkit_to_datetime(int(last_shortcut_time))
                            last_shortcut_utc = dt.isoformat() if dt else None
                        except (ValueError, TypeError, OSError, OverflowError):
                            pass

                    record = {
                        **base_record,
                        "raw_score": raw_score,
                        "points_added_today": setting.get("pointsAddedToday"),
                        "last_engagement_time_utc": last_engagement_utc,
                        "last_shortcut_launch_time_utc": last_shortcut_utc,
                        "has_high_score": setting.get("hasHighScore"),
                        # Media-specific fields are null for site engagement
                        "media_playbacks": None,
                        "visits": None,
                        "last_media_playback_time_utc": None,
                    }
                    site_records.append(record)

                else:  # media_engagement
                    # Media engagement specific fields
                    last_playback_time = setting.get("lastMediaPlaybackTime")
                    last_playback_utc = None
                    if last_playback_time:
                        try:
                            dt = webkit_to_datetime(int(last_playback_time))
                            last_playback_utc = dt.isoformat() if dt else None
                        except (ValueError, TypeError, OSError, OverflowError):
                            pass

                    record = {
                        **base_record,
                        "media_playbacks": setting.get("mediaPlaybacks"),
                        "visits": setting.get("visits"),
                        "last_media_playback_time_utc": last_playback_utc,
                        "has_high_score": setting.get("hasHighScore"),
                        # Site-specific fields are null for media engagement
                        "raw_score": None,
                        "points_added_today": None,
                        "last_engagement_time_utc": None,
                        "last_shortcut_launch_time_utc": None,
                    }
                    media_records.append(record)

                # Collect origin URL for cross-posting
                origin_url = origin.split(",")[0] if "," in origin else origin
                if origin_url and origin_url.startswith(("http://", "https://")):
                    parsed = urlparse(origin_url)
                    url_list.append({
                        "url": origin_url,
                        "domain": parsed.netloc or None,
                        "scheme": parsed.scheme or None,
                        "discovered_by": discovered_by,
                        "run_id": run_id,
                        "source_path": file_entry["logical_path"],
                        "context": f"{engagement_type}:{browser}:{profile}",
                        "first_seen_utc": last_modified_utc,
                    })

        # Insert records
        site_count = 0
        media_count = 0

        all_records = site_records + media_records
        if all_records:
            count = insert_site_engagements(evidence_conn, evidence_id, all_records)
            site_count = len(site_records)
            media_count = len(media_records)
            LOGGER.debug(
                "Inserted %d site + %d media engagement records from %s",
                site_count, media_count, source_file
            )

        return site_count, media_count, url_list
