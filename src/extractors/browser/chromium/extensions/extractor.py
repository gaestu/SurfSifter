"""
Chromium Extensions Extractor

Extracts and analyzes browser extensions from Chrome, Edge, Opera, Brave
with permission risk classification and known extension matching.

Features:
- Extension metadata extraction from manifest.json
- JavaScript file extraction (background scripts, content scripts, service workers)
- Preferences JSON parsing for runtime state (enabled/disabled, install_time)
- Permission risk classification (low/medium/high/critical)
- Known extension reference list matching
- All installed versions captured (installation history)
- Content Security Policy (CSP) extraction for security analysis
- update_url tracking for alternative update server detection
- Schema warning support for unknown manifest keys and preference fields
- StatisticsCollector integration

Data Sources:
- Extensions/{id}/{version}/manifest.json — Static extension metadata
- Extensions/{id}/{version}/*.js — Background/content scripts for code analysis
- Preferences JSON (extensions.settings) — Runtime state, timestamps, installation info:
  - install_time: When extension was installed (WebKit timestamp)
  - state: 0=disabled, 1=enabled
  - disable_reasons: Bitmask for why disabled (user, policy, corrupted, etc.)
  - location: Installation source code (webstore, sideload, policy, etc.)
  - from_webstore: Boolean indicating official store installation
  - granted_permissions: Permissions actually granted at runtime

Architecture:
- _schemas.py: Constants, known tables/fields, enum mappings
- _discovery.py: Extension manifest discovery and parsing
- _preferences.py: Preferences JSON parsing for runtime state
- _scripts.py: JavaScript file extraction

Refactored into multiple modules, added schema warning support,
         partition tracking, CSP/update_url extraction
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ...._shared.file_list_discovery import (
    check_file_list_available,
    glob_to_sql_like,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from .._patterns import CHROMIUM_BROWSERS
from .._patterns import get_artifact_patterns
from .._embedded_discovery import (
    discover_artifacts_with_embedded_roots,
    discover_embedded_roots,
    get_embedded_root_paths,
)
from ....widgets import BrowserConfigWidget
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

# Import shared utilities
from ...._shared.risk_classifier import calculate_risk_level
from ...._shared.known_extensions import load_known_extensions, match_known_extension

# Import local modules
from ._schemas import (
    INSTALL_LOCATION_MAP,
    decode_disable_reasons,
    WEB_REQUEST_PERMISSIONS,
)
from ._discovery import discover_extensions
from ._preferences import parse_all_preferences, merge_preferences_data
from ._scripts import extract_extension_scripts, extract_extension_manifest

# Import warning support
from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.browser.chromium.extensions")


class ChromiumExtensionsExtractor(BaseExtractor):
    """
    Extract Chromium browser extension inventory from evidence images.

    Supports: Chrome, Edge, Opera, Brave (all channels).

    Features:
    - Extension metadata from manifest.json
    - Permission risk classification
    - Known extension reference list matching
    - Schema warning support for forensic completeness
    - All versions retained (no deduplication) for installation history
    """

    SUPPORTED_BROWSERS = list(CHROMIUM_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="chromium_extensions",
            display_name="Chromium Extensions",
            description="Extract browser extensions from Chromium browsers (Chrome, Edge, Opera, Brave)",
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
        """Return status widget."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            ext_count = len(data.get("extensions", []))
            status_text = (
                f"Chromium Extensions\n"
                f"Extensions found: {ext_count}\n"
                f"Run ID: {data.get('run_id', 'N/A')}"
            )
        else:
            status_text = "Chromium Extensions\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory."""
        return case_root / "evidences" / evidence_label / "chromium_extensions"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract browser extension metadata from evidence.

        Three-phase discovery:
        1. Parse Preferences JSON files for extensions.settings (runtime state)
        2. Parse manifest.json files from Extensions directories (static metadata)
        3. Merge data to get complete picture with enabled/disabled state
        """
        callbacks.on_step("Initializing Chromium extension extraction")

        run_id = self._generate_run_id()
        LOGGER.info("Starting Chromium extensions extraction (run_id=%s)", run_id)

        # Start statistics tracking
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "2.1.0",  # Updated schema with CSP/update_url
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "e01_context": self._get_e01_context(evidence_fs),
            "extensions": [],
            "files": [],
            "preferences_files": [],
            "status": "ok",
            "notes": [],
        }

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        scan_all_partitions = config.get("scan_all_partitions", True)
        evidence_conn = config.get("evidence_conn")
        ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs) if scan_all_partitions else None

        partitions: List[Optional[int]] = [None]
        embedded_roots_all = []
        if scan_all_partitions and evidence_conn is not None:
            partitions = self._discover_relevant_partitions(
                evidence_conn, evidence_id, browsers_to_search
            )
            if not partitions:
                partitions = [None]
            embedded_roots_all = discover_embedded_roots(evidence_conn, evidence_id)

        preferences_data = {"extensions": {}, "files_parsed": []}
        extensions: List[Dict[str, Any]] = []

        for partition_index in partitions:
            with open_partition_for_extraction(
                ewf_paths if ewf_paths else evidence_fs, partition_index
            ) as partition_fs:
                if partition_fs is None:
                    continue

                partition_embedded_roots = (
                    get_embedded_root_paths(embedded_roots_all, partition_index)
                    if embedded_roots_all
                    else None
                )

                callbacks.on_step("Parsing Preferences files for extension state")
                pref_result = parse_all_preferences(
                    partition_fs,
                    browsers_to_search,
                    output_dir,
                    callbacks,
                    embedded_roots=partition_embedded_roots,
                    warning_collector=warning_collector,
                )
                preferences_data["extensions"].update(pref_result.get("extensions", {}))
                preferences_data["files_parsed"].extend(pref_result.get("files_parsed", []))

                callbacks.on_step("Scanning for browser extension manifests")
                partition_extensions = discover_extensions(
                    partition_fs,
                    browsers_to_search,
                    callbacks,
                    embedded_roots=partition_embedded_roots,
                    warning_collector=warning_collector,
                )
                for ext in partition_extensions:
                    ext["partition_index"] = partition_index
                extensions.extend(partition_extensions)

        manifest_data["preferences_files"] = list(preferences_data.get("files_parsed", []))

        # Phase 3: Merge Preferences data with manifest data
        callbacks.on_step("Merging extension data")
        extensions = merge_preferences_data(extensions, preferences_data)

        if not extensions:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Chromium browser extensions found")
            LOGGER.info("No Chromium browser extensions found")
        else:
            callbacks.on_progress(0, len(extensions), "Processing extensions")

            for i, ext_info in enumerate(extensions):
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    break

                try:
                    callbacks.on_progress(i + 1, len(extensions), f"Processing {ext_info['browser']} extension")

                    # Copy manifest and calculate hashes
                    ext_partition_index = ext_info.get("partition_index") if scan_all_partitions else None
                    with open_partition_for_extraction(
                        ewf_paths if ewf_paths else evidence_fs, ext_partition_index
                    ) as ext_fs:
                        if ext_fs is None:
                            raise RuntimeError(f"Cannot open partition {ext_partition_index}")

                        ext_info = extract_extension_manifest(ext_fs, ext_info, output_dir)

                        # Extract script files
                        ext_output_dir = ext_info.pop("_output_dir", None)
                        if ext_output_dir:
                            extracted_scripts = extract_extension_scripts(
                                ext_fs, ext_info, ext_output_dir, callbacks
                            )
                            ext_info["extracted_scripts"] = extracted_scripts

                    manifest_data["extensions"].append(ext_info)
                    if ext_info.get("file_path"):
                        manifest_data["files"].append({
                            "path": ext_info["file_path"],
                            "md5": ext_info.get("md5"),
                            "sha256": ext_info.get("sha256"),
                            "type": "manifest",
                        })

                    # Add extracted scripts to files list
                    for script in ext_info.get("extracted_scripts", []):
                        manifest_data["files"].append({
                            "path": script["local_path"],
                            "md5": script.get("md5"),
                            "sha256": script.get("sha256"),
                            "type": script.get("type", "script"),
                            "source_path": script.get("source_path"),
                        })

                except Exception as e:
                    error_msg = f"Failed to extract {ext_info.get('extension_id', 'unknown')}: {e}"
                    LOGGER.error(error_msg, exc_info=True)
                    manifest_data["notes"].append(error_msg)
                    manifest_data["status"] = "partial"

        # Flush warnings to database
        if evidence_conn:
            try:
                warning_count = warning_collector.flush_to_database(evidence_conn)
                if warning_count > 0:
                    LOGGER.info("Recorded %d extraction warnings", warning_count)
                    manifest_data["notes"].append(f"Recorded {warning_count} schema warnings")
            except Exception as e:
                LOGGER.warning("Failed to flush extraction warnings: %s", e)

        # Write manifest
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

        LOGGER.info(
            "Chromium extensions extraction complete: %d extensions, status=%s",
            len(manifest_data["extensions"]),
            manifest_data["status"],
        )

        # Complete statistics tracking
        final_status = "cancelled" if manifest_data["status"] == "cancelled" else "success" if manifest_data["status"] == "ok" else "partial"
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(manifest_data["files"]), extensions=len(manifest_data["extensions"]))
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
        Parse extracted extensions and ingest into database.

        All extension versions are inserted without deduplication to preserve
        complete installation history for forensic analysis.
        """
        from core.database import insert_extensions, delete_extensions_by_run

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"records": 0, "extensions": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        extensions = manifest_data.get("extensions", [])
        evidence_label = config.get("evidence_label", "")

        # Continue statistics tracking with same run_id from extraction
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Create warning collector for ingestion-phase warnings
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        if not extensions:
            callbacks.on_log("No extensions to ingest", "warning")
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, extensions=0)
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"records": 0, "extensions": 0}

        # Clear previous data for this run
        delete_extensions_by_run(evidence_conn, evidence_id, run_id)

        # Load known extensions reference list
        known_extensions = load_known_extensions()

        records = []

        callbacks.on_progress(0, len(extensions), "Processing extensions")

        for i, ext in enumerate(extensions):
            if callbacks.is_cancelled():
                break

            callbacks.on_progress(i + 1, len(extensions), f"Processing {ext.get('name', 'unknown')}")

            record = self._build_extension_record(
                ext,
                run_id,
                known_extensions,
                warning_collector,
            )
            records.append(record)

        # Batch insert (no deduplication - all versions retained)
        inserted = insert_extensions(evidence_conn, evidence_id, records)
        evidence_conn.commit()

        # Flush any ingestion warnings
        try:
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info("Recorded %d ingestion warnings", warning_count)
        except Exception as e:
            LOGGER.warning("Failed to flush ingestion warnings: %s", e)

        # Report ingested counts and finish statistics tracking
        if stats:
            stats.report_ingested(evidence_id, self.metadata.name, records=inserted, extensions=inserted)
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {"records": inserted, "extensions": inserted}

    def _build_extension_record(
        self,
        ext: Dict[str, Any],
        run_id: str,
        known_extensions: Dict[str, Any],
        warning_collector: ExtractionWarningCollector,
    ) -> Dict[str, Any]:
        """
        Build a database record for an extension.

        Args:
            ext: Extension info dict from extraction
            run_id: Run ID for this ingestion
            known_extensions: Known extensions reference data
            warning_collector: Warning collector for unknown values

        Returns:
            Record dict ready for database insertion
        """
        # Calculate permission risk
        permissions = ext.get("permissions", [])
        host_permissions = ext.get("host_permissions", [])
        risk_level = calculate_risk_level(permissions, host_permissions)

        # Convert risk level to numeric score
        risk_score_map = {"critical": 90, "high": 70, "medium": 40, "low": 10}
        risk_score = risk_score_map.get(risk_level, 0)

        # Build risk factors list
        risk_factors = self._build_risk_factors(ext, risk_level, permissions, host_permissions)

        # Check against known extensions
        known_match = match_known_extension(
            ext.get("extension_id", ""),
            ext.get("name", ""),
            known_extensions,
        )

        # Get install location info
        install_location = ext.get("install_location")
        disable_reasons = ext.get("disable_reasons", 0)

        record = {
            "browser": ext.get("browser"),
            "profile": ext.get("profile"),
            "extension_id": ext.get("extension_id"),
            "name": ext.get("name", "Unknown"),
            "version": ext.get("version"),
            "description": self._normalize_text_field(ext.get("description")),
            "author": self._normalize_text_field(ext.get("author")),
            "homepage_url": self._normalize_text_field(ext.get("homepage_url")),
            "enabled": ext.get("enabled", 1),
            "manifest_version": ext.get("manifest_version"),
            "permissions": json.dumps(permissions) if permissions else None,
            "host_permissions": json.dumps(host_permissions) if host_permissions else None,
            "content_scripts": self._normalize_json_text_field(ext.get("content_scripts")),
            "install_time": ext.get("install_time_utc") or ext.get("install_time"),
            "update_time": ext.get("update_time_utc") or ext.get("update_time"),
            "risk_score": risk_score,
            "risk_factors": json.dumps(risk_factors) if risk_factors else None,
            "known_category": known_match.get("category") if known_match else None,
            # Preferences data
            "disable_reasons": disable_reasons if disable_reasons else None,
            "install_location": install_location,
            "install_location_text": INSTALL_LOCATION_MAP.get(install_location) if install_location else None,
            "from_webstore": ext.get("from_webstore"),
            "granted_permissions": self._normalize_json_text_field(ext.get("granted_permissions")),
            # Forensic provenance
            "run_id": run_id,
            "source_path": ext.get("source_path"),
            "partition_index": ext.get("partition_index"),
            "fs_type": ext.get("fs_type"),
            "logical_path": ext.get("logical_path"),
            "forensic_path": ext.get("forensic_path"),
            "notes": self._normalize_text_field(known_match.get("notes") if known_match else None),
        }

        return record

    def _build_risk_factors(
        self,
        ext: Dict[str, Any],
        risk_level: str,
        permissions: List[str],
        host_permissions: List[str],
    ) -> List[str]:
        """
        Build list of risk factors for an extension.

        Args:
            ext: Extension info dict
            risk_level: Calculated risk level
            permissions: Extension permissions
            host_permissions: Extension host permissions

        Returns:
            List of risk factor strings
        """
        risk_factors = []

        if risk_level == "critical":
            risk_factors.append(f"Critical risk: {risk_level}")
        elif risk_level == "high":
            risk_factors.append("High risk permissions detected")

        if "<all_urls>" in permissions or "<all_urls>" in host_permissions:
            risk_factors.append("Has access to all URLs")

        # Check for web request interception permissions
        web_request_perms = set(permissions) & WEB_REQUEST_PERMISSIONS
        if web_request_perms:
            risk_factors.append(f"Can intercept web traffic: {', '.join(web_request_perms)}")

        # Check for alternative update URL (potential malicious distribution)
        update_url = ext.get("update_url")
        if update_url and "clients2.google.com" not in update_url:
            risk_factors.append(f"Uses alternative update server: {update_url}")

        # Check for relaxed or missing CSP
        csp = ext.get("content_security_policy")
        if csp:
            if isinstance(csp, str) and "unsafe-eval" in csp:
                risk_factors.append("CSP allows unsafe-eval")
            elif isinstance(csp, dict):
                for policy in csp.values():
                    if isinstance(policy, str) and "unsafe-eval" in policy:
                        risk_factors.append("CSP allows unsafe-eval")
                        break

        # Add risk factors from Preferences data
        disable_reasons = ext.get("disable_reasons", 0)
        if disable_reasons:
            risk_factors.append(f"Disabled (reasons: {decode_disable_reasons(disable_reasons)})")

        if ext.get("from_webstore") == 0:
            risk_factors.append("Not from official webstore (sideloaded)")

        install_location = ext.get("install_location")
        if install_location == 4:  # Unpacked/developer mode
            risk_factors.append("Loaded in developer mode (unpacked)")

        return risk_factors

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _discover_relevant_partitions(
        self,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
    ) -> List[Optional[int]]:
        """Find partitions containing extension manifests or Preferences files."""
        available, _ = check_file_list_available(evidence_conn, evidence_id)
        if not available:
            return [None]

        partitions: set[Optional[int]] = set()
        for artifact, filenames in (
            ("preferences", ["Preferences"]),
            ("extensions", ["manifest.json"]),
        ):
            path_patterns = set()
            for browser in browsers:
                if browser not in CHROMIUM_BROWSERS:
                    continue
                try:
                    for pattern in get_artifact_patterns(browser, artifact):
                        path_patterns.add(glob_to_sql_like(pattern))
                except ValueError:
                    continue

            result, _ = discover_artifacts_with_embedded_roots(
                evidence_conn,
                evidence_id,
                artifact=artifact,
                filename_patterns=filenames,
                path_patterns=sorted(path_patterns) if path_patterns else None,
            )
            partitions.update(result.matches_by_partition.keys())

        return sorted(partitions) if partitions else [None]

    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _normalize_text_field(self, value) -> Optional[str]:
        """Normalize a value for insertion into a TEXT column."""
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value)

    def _normalize_json_text_field(self, value) -> Optional[str]:
        """Normalize a value for insertion into a JSON-as-TEXT column."""
        if value is None or value == {} or value == []:
            return None
        if isinstance(value, str):
            return value
        return json.dumps(value, ensure_ascii=False, sort_keys=True)

    def _get_e01_context(self, evidence_fs) -> Dict[str, Any]:
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
