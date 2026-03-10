"""
macOS Plist System Extractor

Extracts macOS system artifacts from property list (plist) files found on
evidence volumes.  Supports both XML and binary plist formats via the
stdlib ``plistlib`` module.

Extraction phase:
    Scan evidence for known plist paths, copy them to the case workspace,
    and write ``manifest.json``.

Ingestion phase:
    Parse the copied plists (and the QuarantineEventsV2 SQLite DB),
    extract OS indicators, and insert them into the ``os_indicators`` table.
"""
from __future__ import annotations

import json
import plistlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import QWidget, QLabel

from ...base import BaseExtractor, ExtractorMetadata
from ...callbacks import ExtractorCallbacks
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.system.macos_plist")


# ---------------------------------------------------------------------------
# Search patterns for macOS plist files
# ---------------------------------------------------------------------------

# Each entry is (glob_pattern, parser_function_name)
# The parser name is used during ingestion to dispatch to the correct parser.
PLIST_SEARCH_PATTERNS: List[tuple[str, str]] = [
    # System config
    ("System/Library/CoreServices/SystemVersion.plist", "system_version"),
    ("Library/Preferences/com.apple.SystemProfiler.plist", "system_profiler"),
    ("Library/Preferences/SystemConfiguration/preferences.plist", "network_config"),
    ("Library/Preferences/.GlobalPreferences.plist", "global_preferences"),
    # Installed applications
    ("Applications/*/Contents/Info.plist", "app_info_plist"),
    ("var/db/receipts/*.plist", "install_receipt"),
    # Application execution
    ("Users/*/Library/Preferences/com.apple.LaunchServices.plist", "launch_services"),
    # User activity
    ("Users/*/Library/Preferences/com.apple.recentitems.plist", "recent_items"),
    ("Users/*/Library/Preferences/com.apple.finder.plist", "finder_preferences"),
    ("Users/*/Library/Preferences/com.apple.Spotlight.plist", "spotlight_preferences"),
    # Quarantine (SQLite, not plist — handled separately during ingestion)
    (
        "Users/*/Library/Preferences/com.apple.LaunchServices.QuarantineEventsV2",
        "quarantine_events",
    ),
    # Default browser detection
    (
        "Users/*/Library/Preferences/com.apple.LaunchServices/com.apple.launchservices.secure.plist",
        "default_browser",
    ),
]


class SystemMacosPlistExtractor(BaseExtractor):
    """
    Extract macOS system artifacts from property list (plist) files.

    Features:
    - Scans evidence for system, application, and user plist files
    - Copies files to case workspace for reproducible analysis
    - Parses both XML and binary plists via stdlib ``plistlib``
    - Handles QuarantineEventsV2 SQLite database
    - Forensic provenance tracking via ``os_indicators.provenance = 'macos_plist'``
    - StatisticsCollector integration
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="system_macos_plist",
            display_name="macOS Plist Reader",
            description="Extract macOS system artifacts from property list (plist) files",
            category="system",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
        )

    # ------------------------------------------------------------------
    # Capability checks
    # ------------------------------------------------------------------

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """plistlib is stdlib — always available."""
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        return (output_dir / "manifest.json").exists()

    # ------------------------------------------------------------------
    # UI widgets
    # ------------------------------------------------------------------

    def get_config_widget(self, parent: QWidget) -> QWidget:
        return QLabel("No configuration required — all known plist paths will be scanned.", parent)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
    ) -> QWidget:
        manifest = output_dir / "manifest.json"
        status_text = "macOS Plist Reader\n"

        if manifest.exists():
            try:
                data = json.loads(manifest.read_text())
                file_count = len(data.get("files", []))
                run_id = data.get("run_id", "N/A")
                timestamp = data.get("timestamp", "")
                try:
                    ts = datetime.fromisoformat(timestamp).strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    ts = timestamp
                status_text += (
                    f"Files Copied: {file_count}\n"
                    f"Last Run: {ts}\n"
                    f"Run ID: {run_id}"
                )
            except Exception:
                status_text += "Error reading manifest"
        else:
            status_text += "No extraction run yet"

        # Ingestion summary
        summary_path = output_dir / "ingestion_macos_plist.json"
        if summary_path.exists():
            try:
                summary = json.loads(summary_path.read_text())
                indicators = summary.get("indicators_inserted", 0)
                errors_count = summary.get("errors", 0)
                status_text += f"\n\nIngestion:\nIndicators: {indicators}\nErrors: {errors_count}"
            except Exception:
                pass

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        return case_root / "evidences" / evidence_label / "macos_plist"

    # ------------------------------------------------------------------
    # Extraction
    # ------------------------------------------------------------------

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        run_id = self._generate_run_id()

        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        callbacks.on_step("Starting macOS plist extraction")

        output_dir.mkdir(parents=True, exist_ok=True)
        plists_dir = output_dir / "plists"
        plists_dir.mkdir(parents=True, exist_ok=True)

        files_info: List[Dict[str, Any]] = []
        total_patterns = len(PLIST_SEARCH_PATTERNS)

        try:
            for idx, (pattern, parser_name) in enumerate(PLIST_SEARCH_PATTERNS, 1):
                if callbacks.is_cancelled():
                    callbacks.on_log("Cancelled by user", "warning")
                    if stats:
                        stats.finish_run(evidence_id, self.metadata.name, "cancelled")
                    return False

                callbacks.on_progress(idx, total_patterns, f"Scanning: {pattern}")
                callbacks.on_log(f"Scanning pattern {idx}/{total_patterns}: {pattern}", "info")

                try:
                    paths = list(evidence_fs.iter_paths(pattern))
                except Exception as e:
                    callbacks.on_log(f"Error scanning {pattern}: {e}", "error")
                    LOGGER.warning("Error scanning pattern %s: %s", pattern, e)
                    continue

                if paths:
                    callbacks.on_log(f"  Found {len(paths)} matches", "info")

                for evidence_path in paths:
                    try:
                        # Build a safe local filename preserving relative structure
                        relative = evidence_path.replace("\\", "/").lstrip("/")
                        local_rel = Path(relative)
                        local_path = plists_dir / local_rel
                        local_path.parent.mkdir(parents=True, exist_ok=True)

                        # Copy file from evidence
                        file_size = 0
                        with evidence_fs.open_for_read(evidence_path) as src, open(local_path, "wb") as dst:
                            while True:
                                chunk = src.read(8192)
                                if not chunk:
                                    break
                                dst.write(chunk)
                                file_size += len(chunk)

                        files_info.append({
                            "original_path": evidence_path,
                            "local_path": str(local_rel),
                            "parser": parser_name,
                            "size": file_size,
                        })

                        LOGGER.info("Copied %s (%d bytes)", evidence_path, file_size)

                    except Exception as e:
                        LOGGER.error("Failed to copy %s: %s", evidence_path, e)
                        callbacks.on_error(f"Failed to copy {Path(evidence_path).name}: {e}")

            # Write manifest
            manifest = {
                "run_id": run_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "extractor": "macos_plist",
                "version": self.metadata.version,
                "patterns_scanned": [p[0] for p in PLIST_SEARCH_PATTERNS],
                "files": files_info,
            }
            (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

            # Record extracted files for audit
            try:
                from extractors._shared.extracted_files_audit import record_browser_files
                record_browser_files(
                    evidence_conn=config.get("evidence_conn"),
                    evidence_id=evidence_id,
                    run_id=run_id,
                    extractor_name=self.metadata.name,
                    extractor_version=self.metadata.version,
                    manifest_data=manifest,
                    callbacks=callbacks,
                )
            except Exception as e:
                LOGGER.warning("Failed to record extracted files audit: %s", e)

            if stats:
                stats.report_discovered(evidence_id, self.metadata.name, plists=len(files_info))
                stats.finish_run(evidence_id, self.metadata.name, "ok")

        except Exception as e:
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "error")
            callbacks.on_error(f"macOS plist extraction failed: {e}")
            LOGGER.exception("macOS plist extraction failed")
            return False

        callbacks.on_step(f"macOS plist extraction complete: {len(files_info)} files copied")
        LOGGER.info("macOS plist extraction complete (run_id=%s, files=%d)", run_id, len(files_info))
        return True

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        from .parsers import (
            parse_system_version,
            parse_system_profiler,
            parse_global_preferences,
            parse_network_config,
            parse_app_info_plist,
            parse_install_receipt,
            parse_launch_services,
            parse_recent_items,
            parse_finder_preferences,
            parse_spotlight_preferences,
            parse_quarantine_events,
            parse_default_browser,
        )

        evidence_label = config.get("evidence_label", "")

        stats = StatisticsCollector.instance()
        callbacks.on_step("Starting macOS plist ingestion")

        # Read manifest
        manifest_path = output_dir / "manifest.json"
        if not manifest_path.exists():
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "error")
            callbacks.on_error("No manifest.json found")
            return False

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())

        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Parser dispatch table
        parser_map = {
            "system_version": parse_system_version,
            "system_profiler": parse_system_profiler,
            "global_preferences": parse_global_preferences,
            "network_config": parse_network_config,
            "app_info_plist": parse_app_info_plist,
            "install_receipt": parse_install_receipt,
            "launch_services": parse_launch_services,
            "recent_items": parse_recent_items,
            "finder_preferences": parse_finder_preferences,
            "spotlight_preferences": parse_spotlight_preferences,
            # quarantine_events handled separately (SQLite, not plist)
            "default_browser": parse_default_browser,
        }

        files_list = manifest_data.get("files", [])
        plists_dir = output_dir / "plists"
        all_records: List[Dict[str, Any]] = []
        errors = 0

        try:
            # Delete prior run's indicators for idempotent re-ingestion
            try:
                from core.database import delete_os_indicators_by_run
                deleted = delete_os_indicators_by_run(evidence_conn, evidence_id, run_id)
                if deleted:
                    callbacks.on_log(f"Cleaned up {deleted} previous macOS plist indicators", "info")
                    LOGGER.info("Deleted %d os_indicators for run_id=%s", deleted, run_id)
            except Exception as e:
                LOGGER.warning("Failed to delete prior os_indicators for run_id=%s: %s", run_id, e)

            for idx, file_info in enumerate(files_list, 1):
                if callbacks.is_cancelled():
                    callbacks.on_log("Cancelled by user", "warning")
                    if stats:
                        stats.finish_run(evidence_id, self.metadata.name, "cancelled")
                    return False

                local_rel = file_info.get("local_path", "")
                parser_name = file_info.get("parser", "")
                original_path = file_info.get("original_path", local_rel)
                local_path = plists_dir / local_rel

                callbacks.on_progress(idx, len(files_list), f"Parsing: {Path(local_rel).name}")

                if not local_path.exists():
                    LOGGER.warning("File not found: %s", local_path)
                    errors += 1
                    continue

                # Quarantine events are SQLite — special handling
                if parser_name == "quarantine_events":
                    try:
                        records = parse_quarantine_events(str(local_path), original_path, run_id)
                        all_records.extend(records)
                        callbacks.on_log(
                            f"Parsed quarantine DB: {len(records)} events", "info"
                        )
                    except Exception as e:
                        LOGGER.error("Error parsing quarantine DB %s: %s", local_path, e)
                        callbacks.on_log(f"Error parsing quarantine DB: {e}", "error")
                        errors += 1
                    continue

                # Normal plist parsing
                parser_func = parser_map.get(parser_name)
                if not parser_func:
                    LOGGER.warning("Unknown parser %s for %s", parser_name, local_rel)
                    errors += 1
                    continue

                try:
                    raw_data = local_path.read_bytes()
                    plist_data = plistlib.loads(raw_data)
                except Exception as e:
                    LOGGER.error("Failed to parse plist %s: %s", local_path, e)
                    callbacks.on_log(f"Failed to parse {Path(local_rel).name}: {e}", "error")
                    errors += 1
                    continue

                try:
                    records = parser_func(plist_data, original_path, run_id)
                    all_records.extend(records)
                    if records:
                        callbacks.on_log(
                            f"Parsed {Path(local_rel).name}: {len(records)} indicators", "info"
                        )
                except Exception as e:
                    LOGGER.error("Error in parser %s for %s: %s", parser_name, local_rel, e)
                    callbacks.on_log(f"Parser error for {Path(local_rel).name}: {e}", "error")
                    errors += 1

            # Batch insert
            inserted = 0
            if all_records:
                try:
                    from core.database import insert_os_indicators
                    insert_os_indicators(evidence_conn, evidence_id, all_records)
                    inserted = len(all_records)
                    callbacks.on_step(f"Inserted {inserted} macOS indicators")
                except Exception as e:
                    LOGGER.exception("Failed to insert macOS plist indicators: %s", e)
                    callbacks.on_error(f"Database insert failed: {e}")
                    errors += len(all_records)
            else:
                callbacks.on_log("No indicators extracted from plist files", "warning")

            # Write ingestion summary
            summary = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "run_id": run_id,
                "files_processed": len(files_list),
                "indicators_inserted": inserted,
                "errors": errors,
            }
            try:
                (output_dir / "ingestion_macos_plist.json").write_text(
                    json.dumps(summary, indent=2)
                )
            except Exception as e:
                LOGGER.error("Failed to save ingestion summary: %s", e)

            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, indicators=inserted)
                if errors > 0:
                    stats.report_failed(evidence_id, self.metadata.name, indicators=errors)
                status = "ok" if errors == 0 else "partial"
                stats.finish_run(evidence_id, self.metadata.name, status)

        except Exception as e:
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "error")
            callbacks.on_error(f"macOS plist ingestion failed: {e}")
            LOGGER.exception("macOS plist ingestion failed")
            return False

        callbacks.on_step(f"Ingested {inserted} indicators ({errors} errors)")
        LOGGER.info("macOS plist ingestion complete (inserted=%d, errors=%d)", inserted, errors)
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _generate_run_id(self) -> str:
        """Generate unique run ID: timestamp + UUID4 prefix."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        return f"{ts}_{uid}"
