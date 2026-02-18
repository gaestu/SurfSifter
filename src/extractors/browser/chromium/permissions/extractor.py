"""
Chromium Permissions Extractor

Extracts and ingests site permissions from Chromium-based browsers:
- Chrome, Edge, Opera, Brave

Features:
- Preferences JSON parsing (profile.content_settings.exceptions)
- Permission type normalization (notifications, geolocation, camera, etc.)
- Permission value mapping (allow, block, ask)
- StatisticsCollector integration for run tracking

Data Format:
- Chromium stores permissions in Preferences JSON file
- Section: profile.content_settings.exceptions
- Permission values: 0=default, 1=allow, 2=block, 3=ask
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

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
    CHROMIUM_PERMISSION_TYPES,
    CHROMIUM_PERMISSION_VALUES,
    KNOWN_EXCEPTION_KEYS,
    KNOWN_SETTING_KEYS,
    KNOWN_SETTING_VALUES,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_permissions,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    delete_permissions_by_run,
)

LOGGER = get_logger("extractors.browser.chromium.permissions")


class ChromiumPermissionsExtractor(BaseExtractor):
    """
    Extract site permissions from Chromium-based browsers.

    Parses Preferences JSON file to extract content settings exceptions.
    Supports Chrome, Edge, Brave, Opera.
    """

    SUPPORTED_BROWSERS = list(CHROMIUM_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="chromium_permissions",
            display_name="Chromium Site Permissions",
            description="Extract site permissions from Chrome/Edge/Opera/Brave Preferences",
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
            status_text = f"Chromium Permissions\nFiles extracted: {file_count}\nRun ID: {data.get('run_id', 'N/A')}"
        else:
            status_text = "Chromium Permissions\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "chromium_permissions"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Chromium Preferences files from evidence."""
        callbacks.on_step("Initializing Chromium permissions extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Chromium permissions extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        # Determine multi-partition mode
        scan_all_partitions = config.get("scan_all_partitions", True)
        evidence_db_path = config.get("evidence_db_path")

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.0.0",  # Multi-partition support
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
            "Chromium permissions extraction complete: %d files, status=%s",
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
        """Parse extracted Preferences and ingest permissions into database."""
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"records": 0, "permissions": 0}

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
                    permissions=0,
                )
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"records": 0, "permissions": 0}

        total_permissions = 0
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

                callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry['browser']} permissions")

                try:
                    inventory_id = insert_browser_inventory(
                        evidence_conn,
                        evidence_id=evidence_id,
                        browser=file_entry["browser"],
                        artifact_type="permissions",
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

                    count, urls = self._parse_preferences_file(
                        db_path,
                        file_entry,
                        run_id,
                        evidence_id,
                        evidence_conn,
                        callbacks,
                        warning_collector=warning_collector,
                    )

                    total_permissions += count

                    # Collect URLs for cross-posting (insert all, no deduplication)
                    for url_info in urls:
                        url = url_info["url"]
                        # Only filter out non-URL schemes, keep all valid URLs
                        if url and not url.startswith(("javascript:", "data:")):
                            url_records.append(url_info)

                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="ok",
                        records_parsed=count,
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
                LOGGER.debug("Cross-posted %d permission origin URLs to urls table", urls_table_count)
            except Exception as e:
                LOGGER.debug("Failed to cross-post permission URLs: %s", e)

        evidence_conn.commit()

        # Report ingested counts and finish
        if stats:
            stats.report_ingested(
                evidence_id, self.metadata.name,
                records=total_permissions,
                permissions=total_permissions,
            )
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {"records": total_permissions, "permissions": total_permissions, "urls_table": urls_table_count}

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

            patterns = get_artifact_patterns(browser_key, "permissions")

            if not patterns:
                continue

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        # Only process Preferences files (not permissions.sqlite)
                        filename = path_str.split('/')[-1].lower()
                        if filename != "preferences":
                            continue

                        profile = self._extract_profile_from_path(path_str, browser_key)

                        preference_files.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "profile": profile,
                            "file_type": "preferences",
                            "artifact_type": "permissions",
                            "display_name": CHROMIUM_BROWSERS[browser_key]["display_name"],
                        })

                        callbacks.on_log(f"Found {browser_key} Preferences: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return preference_files

    def _discover_files_multi_partition(
        self,
        evidence_db_path: str,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> Dict[Optional[int], List[Dict]]:
        """
        Discover Preferences files across all partitions using file_list table.

        Returns:
            Dict mapping partition_index -> list of file_info dicts.
            partition_index=None means use default evidence_fs.
        """
        # Collect all patterns
        all_patterns = []
        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                continue
            patterns = get_artifact_patterns(browser, "permissions")
            all_patterns.extend(patterns)

        if not all_patterns:
            return {}

        # Query file_list table
        result = discover_from_file_list(
            evidence_db_path=evidence_db_path,
            evidence_id=evidence_id,
            patterns=all_patterns,
            partition_index=None,  # All partitions
        )

        callbacks.on_log(
            f"Multi-partition discovery: {result.total_matches} files across "
            f"{len(result.by_partition)} partition(s)"
        )

        # Group by partition with full file info
        files_by_partition: Dict[Optional[int], List[Dict]] = {}

        for match in result.matches:
            # Only process Preferences files (not permissions.sqlite or other files)
            filename = match.path.split('/')[-1].lower()
            if filename != "preferences":
                continue

            partition = match.partition_index
            if partition not in files_by_partition:
                files_by_partition[partition] = []

            # Detect browser from path (fallback to chrome if not recognized)
            browser = detect_browser_from_path(match.path) or "chrome"

            profile = self._extract_profile_from_path(match.path, browser)

            files_by_partition[partition].append({
                "logical_path": match.path,
                "browser": browser,
                "profile": profile,
                "file_type": "preferences",
                "artifact_type": "permissions",
                "partition_index": partition,
                "fs_type": match.fs_type,
                "forensic_path": match.forensic_path,
                "display_name": CHROMIUM_BROWSERS.get(browser, {}).get("display_name", browser),
            })

        return files_by_partition

    def _extract_profile_from_path(self, path: str, browser: str) -> str:
        """Extract browser profile name from file path."""
        parts = path.split('/')

        try:
            idx = parts.index("User Data")
            return parts[idx + 1] if idx + 1 < len(parts) else "Default"
        except (ValueError, IndexError):
            pass

        # Opera special handling
        if browser == "opera":
            if "Opera Stable" in path:
                return "Opera Stable"
            elif "Opera GX Stable" in path:
                return "Opera GX Stable"

        return "Default"

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Copy file from evidence to workspace and collect metadata."""
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info["profile"]
            partition_index = file_info.get("partition_index", 0) or 0

            # Generate unique filename to avoid collisions across partitions
            safe_profile = profile.replace(' ', '_').replace('/', '_')
            path_hash = hashlib.sha256(source_path.encode()).hexdigest()[:8]
            filename = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_preferences"
            dest_path = output_dir / filename

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
                "file_type": "preferences",
                "logical_path": source_path,
                "artifact_type": "permissions",
                "partition_index": partition_index,
                "fs_type": file_info.get("fs_type"),
                "forensic_path": file_info.get("forensic_path"),
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
                "file_type": "preferences",
                "logical_path": file_info.get("logical_path"),
                "artifact_type": "permissions",
                "partition_index": file_info.get("partition_index"),
                "fs_type": file_info.get("fs_type"),
                "forensic_path": file_info.get("forensic_path"),
                "error_message": str(e),
            }

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        """Clear permission data from a previous run."""
        deleted = delete_permissions_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d permission records from previous run %s", deleted, run_id)

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
    ) -> Tuple[int, List[Dict]]:
        """Parse Chromium Preferences JSON for site permissions.

        Args:
            file_path: Path to extracted Preferences file
            file_entry: File metadata dict from manifest
            run_id: Current run ID
            evidence_id: Evidence ID
            evidence_conn: Database connection
            callbacks: Extraction callbacks
            warning_collector: Optional warning collector for schema warnings

        Returns:
            Tuple of (count, url_list)
        """
        from urllib.parse import urlparse

        if not file_path.exists():
            LOGGER.warning("Preferences file not found: %s", file_path)
            return 0, []

        try:
            prefs = json.loads(file_path.read_text(encoding='utf-8', errors='replace'))
        except Exception as e:
            LOGGER.error("Failed to parse Preferences: %s", e)
            return 0, []

        browser = file_entry["browser"]
        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"
        profile = file_entry.get("profile", "Default")
        source_file = file_entry.get("logical_path", str(file_path))

        records = []
        url_list = []

        # Track unknown types/values for warnings
        found_permission_types: set[str] = set()
        found_setting_values: set[int] = set()

        # Navigate to content_settings.exceptions
        content_settings = prefs.get("profile", {}).get("content_settings", {})
        exceptions = content_settings.get("exceptions", {})
        skipped_non_permission_entries = 0
        skipped_types: set[str] = set()

        for perm_type, origins in exceptions.items():
            if not isinstance(origins, dict):
                continue

            found_permission_types.add(perm_type)
            normalized_type = CHROMIUM_PERMISSION_TYPES.get(perm_type, perm_type)

            for origin, settings in origins.items():
                if not isinstance(settings, dict):
                    continue

                # Track unknown setting keys
                if warning_collector:
                    for key in settings.keys():
                        if key not in KNOWN_SETTING_KEYS:
                            warning_collector.add_warning(
                                warning_type="json_unknown_key",
                                item_name=f"exceptions.{perm_type}.{key}",
                                severity="info",
                                category="json",
                                artifact_type="permissions",
                                source_file=source_file,
                                item_value=str(type(settings[key]).__name__),
                            )

                setting_value_raw = settings.get("setting", 0)
                setting_code = self._coerce_permission_setting_code(setting_value_raw)

                # Even if this isn't a standard permission (dict-type settings like engagement),
                # we should still extract the URL for cross-posting to the urls table
                # The engagement data itself is handled by the site_engagement extractor
                origin_url = origin.split(",")[0] if "," in origin else origin
                last_modified = settings.get("last_modified")
                modified_utc = None
                if last_modified:
                    try:
                        dt = webkit_to_datetime(int(last_modified))
                        modified_utc = dt.isoformat() if dt else None
                    except (ValueError, TypeError, OSError, OverflowError):
                        pass

                if setting_code is None:
                    skipped_non_permission_entries += 1
                    skipped_types.add(perm_type)
                    # Still collect URL from skipped entries (engagement, etc.)
                    if origin_url and origin_url.startswith(("http://", "https://")):
                        try:
                            parsed = urlparse(origin_url)
                        except ValueError:
                            continue
                        url_list.append({
                            "url": origin_url,
                            "domain": parsed.netloc or None,
                            "scheme": parsed.scheme or None,
                            "discovered_by": discovered_by,
                            "run_id": run_id,
                            "source_path": file_entry["logical_path"],
                            "context": f"preferences:{browser}:{profile}:{normalized_type}",
                            "first_seen_utc": modified_utc,
                        })
                    continue

                found_setting_values.add(setting_code)
                permission_value = CHROMIUM_PERMISSION_VALUES.get(setting_code, "unknown")

                # Parse expiration if present
                expiration = settings.get("expiration")
                expires_at_utc = None
                if expiration:
                    try:
                        dt = webkit_to_datetime(int(expiration))
                        expires_at_utc = dt.isoformat() if dt else None
                    except (ValueError, TypeError, OSError, OverflowError):
                        pass

                # Use already-parsed modified_utc as granted_at_utc
                granted_at_utc = modified_utc

                record = {
                    "browser": browser,
                    "profile": profile,
                    "origin": origin,
                    "permission_type": normalized_type,
                    "permission_value": permission_value,
                    "raw_type": perm_type,
                    "raw_value": setting_code,
                    "granted_at_utc": granted_at_utc,
                    "expires_at_utc": expires_at_utc,
                    "expires_type": "expiring" if expiration else "permanent",
                    "run_id": run_id,
                    "source_path": file_entry["logical_path"],
                    "discovered_by": discovered_by,
                    "partition_index": file_entry.get("partition_index"),
                    "fs_type": file_entry.get("fs_type"),
                    "logical_path": file_entry["logical_path"],
                    "forensic_path": file_entry.get("forensic_path"),
                }
                records.append(record)

                # Collect origin URL for cross-posting (origin_url already parsed above)
                if origin_url and origin_url.startswith(("http://", "https://")):
                    try:
                        parsed = urlparse(origin_url)
                    except ValueError:
                        pass
                    else:
                        url_list.append({
                            "url": origin_url,
                            "domain": parsed.netloc or None,
                            "scheme": parsed.scheme or None,
                            "discovered_by": discovered_by,
                            "run_id": run_id,
                            "source_path": file_entry["logical_path"],
                            "context": f"permission:{browser}:{profile}:{normalized_type}",
                            "first_seen_utc": granted_at_utc,
                        })

        if skipped_non_permission_entries:
            callbacks.on_log(
                f"Skipped {skipped_non_permission_entries} non-permission entries for types: {', '.join(sorted(skipped_types))}",
                "debug",
            )

        # Report unknown permission types and values
        if warning_collector:
            unknown_types = found_permission_types - KNOWN_EXCEPTION_KEYS
            for unknown_type in unknown_types:
                warning_collector.add_warning(
                    warning_type="unknown_enum_value",
                    item_name="permission_type",
                    severity="warning",
                    category="json",
                    artifact_type="permissions",
                    source_file=source_file,
                    item_value=unknown_type,
                )

            unknown_values = found_setting_values - KNOWN_SETTING_VALUES
            for unknown_value in unknown_values:
                warning_collector.add_warning(
                    warning_type="unknown_enum_value",
                    item_name="setting_value",
                    severity="info",
                    category="json",
                    artifact_type="permissions",
                    source_file=source_file,
                    item_value=str(unknown_value),
                )

        if records:
            return insert_permissions(evidence_conn, evidence_id, records), url_list
        return 0, url_list

    def _coerce_permission_setting_code(self, setting_value: Any) -> Optional[int]:
        """
        Extract Chromium content setting code for site permissions.

        Chromium `Preferences` stores most permission exceptions with a scalar
        integer `setting` (e.g., 1=allow, 2=block). However, many entries under
        `profile.content_settings.exceptions` are *not* site permissions and
        store nested objects. Those should be ignored.
        """
        if setting_value is None:
            return None

        if isinstance(setting_value, bool):
            return int(setting_value)

        if isinstance(setting_value, int):
            return setting_value

        if isinstance(setting_value, float) and setting_value.is_integer():
            return int(setting_value)

        if isinstance(setting_value, str):
            value_str = setting_value.strip().lower()
            if value_str in ("default", "allow", "block", "ask"):
                reverse = {v: k for k, v in CHROMIUM_PERMISSION_VALUES.items()}
                return reverse.get(value_str)
            try:
                return int(value_str)
            except ValueError:
                return None

        if isinstance(setting_value, dict):
            for key in ("setting", "value", "permission", "state"):
                if key in setting_value:
                    return self._coerce_permission_setting_code(setting_value.get(key))
            return None

        return None
