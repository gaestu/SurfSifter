"""First-class Chromium browser configuration extractor."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import QLabel, QWidget

from ...._shared.extraction_warnings import ExtractionWarningCollector
from ...._shared.file_list_discovery import (
    check_file_list_available,
    get_ewf_paths_from_evidence_fs,
    glob_to_sql_like,
    open_partition_for_extraction,
)
from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import BrowserConfigWidget
from .._embedded_discovery import (
    discover_artifacts_with_embedded_roots,
    get_embedded_root_paths,
)
from .._parsers import detect_browser_from_path, extract_profile_from_path
from .._patterns import CHROMIUM_BROWSERS, get_artifact_patterns
from ._parser import parse_local_state_config, parse_preferences_config
from core.database import insert_browser_inventory, update_inventory_ingestion_status
from core.database.helpers.browser_config import (
    delete_browser_config_by_run,
    insert_browser_configs,
)
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.browser.chromium.config")

MAX_CONFIG_FILE_BYTES = 64 * 1024 * 1024


class ChromiumConfigExtractor(BaseExtractor):
    """Extract curated Chromium Preferences and Local State settings."""

    SUPPORTED_BROWSERS = list(CHROMIUM_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="chromium_config",
            display_name="Chromium Browser Config",
            description="Extract curated browser configuration from Chromium Preferences and Local State",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted. Please mount E01 image first."
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
        evidence_id: int,
    ) -> QWidget:
        """Return status widget showing extraction state."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = (
                f"Chromium Browser Config\n"
                f"Files extracted: {file_count}\n"
                f"Run ID: {data.get('run_id', 'N/A')}"
            )
        else:
            status_text = "Chromium Browser Config\nNo extraction run yet"
        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "chromium_config"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        """Copy Chromium Preferences and Local State files from evidence."""
        callbacks.on_step("Initializing Chromium browser config extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")
        scan_all_partitions = config.get("scan_all_partitions", True)

        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data: Dict[str, Any] = {
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

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        browsers_to_search = [b for b in browsers_to_search if b in self.SUPPORTED_BROWSERS]

        callbacks.on_step("Scanning for Chromium config files")
        if scan_all_partitions and evidence_conn is not None:
            files_by_partition = self._discover_files_multi_partition(
                evidence_conn,
                evidence_id,
                browsers_to_search,
                callbacks,
            )
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            if not files_by_partition:
                callbacks.on_log(
                    "No file_list config matches found; falling back to active partition discovery",
                    "warning",
                )
                files_by_partition = {
                    None: self._discover_config_files(evidence_fs, browsers_to_search, callbacks)
                }
                ewf_paths = None
        else:
            files_by_partition = {
                None: self._discover_config_files(evidence_fs, browsers_to_search, callbacks)
            }
            ewf_paths = None

        total_files = sum(len(files) for files in files_by_partition.values())
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=total_files)

        callbacks.on_log(f"Found {total_files} Chromium config file(s)")

        file_index = 0
        if total_files == 0:
            LOGGER.info("No Chromium config files found")
        else:
            callbacks.on_progress(0, total_files, "Copying Chromium config files")
            for partition_index in self._sorted_partitions(files_by_partition):
                files = self._sorted_file_entries(files_by_partition[partition_index])
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    manifest_data["notes"].append("Extraction cancelled by user")
                    break

                with open_partition_for_extraction(
                    ewf_paths if ewf_paths else evidence_fs,
                    partition_index,
                ) as partition_fs:
                    if partition_fs is None:
                        LOGGER.warning("Cannot open partition %s", partition_index)
                        manifest_data["notes"].append(f"Failed to open partition {partition_index}")
                        file_index += len(files)
                        callbacks.on_progress(
                            file_index,
                            total_files,
                            f"Skipped unavailable partition {partition_index}",
                        )
                        continue

                    for file_info in files:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            manifest_data["notes"].append("Extraction cancelled by user")
                            break

                        file_index += 1
                        callbacks.on_progress(
                            file_index,
                            total_files,
                            f"Copying {file_info['browser']} {file_info['file_type']}",
                        )
                        extracted_file = self._extract_file(partition_fs, file_info, output_dir, callbacks)
                        manifest_data["files"].append(extracted_file)

        if stats:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_step("Writing manifest")
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

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
            "Chromium browser config extraction complete: %d files, status=%s",
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
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, int]:
        """Parse extracted config files into browser_config rows."""
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"
        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"records": 0, "config_records": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = self._sorted_file_entries(manifest_data.get("files", []))
        evidence_label = config.get("evidence_label", "")

        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        delete_browser_config_by_run(evidence_conn, evidence_id, run_id)

        total_records = 0
        callbacks.on_progress(0, len(files), "Parsing Chromium config files")
        try:
            for index, file_entry in enumerate(files):
                if callbacks.is_cancelled():
                    break
                callbacks.on_progress(
                    index + 1,
                    len(files),
                    f"Parsing {file_entry.get('browser', 'chromium')} {file_entry.get('file_type', 'config')}",
                )
                if file_entry.get("copy_status") == "error":
                    callbacks.on_log(
                        f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}",
                        "warning",
                    )
                    continue

                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry["browser"],
                    artifact_type="config",
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

                extracted_path = Path(file_entry["extracted_path"])
                if not extracted_path.is_absolute():
                    extracted_path = output_dir / extracted_path

                try:
                    self._validate_config_file_size(extracted_path, file_entry)
                    config_data = json.loads(extracted_path.read_text(encoding="utf-8", errors="replace"))
                except json.JSONDecodeError as exc:
                    warning_collector.add_json_parse_error(
                        file_entry["logical_path"],
                        str(exc),
                        artifact_type=file_entry.get("file_type"),
                    )
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(exc),
                    )
                    continue
                except ValueError as exc:
                    warning_collector.add_warning(
                        warning_type="file_corrupt",
                        item_name=file_entry["logical_path"],
                        severity="warning",
                        category="json",
                        artifact_type=file_entry.get("file_type"),
                        source_file=file_entry["logical_path"],
                        item_value=str(exc),
                    )
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(exc),
                    )
                    continue
                except OSError as exc:
                    warning_collector.add_json_parse_error(
                        file_entry["logical_path"],
                        str(exc),
                        artifact_type=file_entry.get("file_type"),
                    )
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="error",
                        notes=str(exc),
                    )
                    continue

                records = self._parse_file_records(config_data, file_entry, run_id, warning_collector)
                inserted = insert_browser_configs(evidence_conn, evidence_id, records) if records else 0
                total_records += inserted
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=inserted,
                )
        finally:
            warning_collector.flush_to_database(evidence_conn)

        evidence_conn.commit()

        if stats:
            stats.report_ingested(
                evidence_id,
                self.metadata.name,
                records=total_records,
                config_records=total_records,
            )
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {"records": total_records, "config_records": total_records}

    def _discover_config_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> List[Dict[str, Any]]:
        """Discover config files using the active evidence filesystem."""
        discovered: List[Dict[str, Any]] = []
        seen_paths: set[str] = set()
        for browser in browsers:
            for file_type, artifact in (("preferences", "preferences"), ("local_state", "local_state")):
                for pattern in get_artifact_patterns(browser, artifact):
                    try:
                        for path_str in evidence_fs.iter_paths(pattern):
                            normalized = str(path_str).replace("\\", "/")
                            if normalized in seen_paths:
                                continue
                            filename = normalized.rsplit("/", 1)[-1].lower()
                            if file_type == "preferences" and filename != "preferences":
                                continue
                            if file_type == "local_state" and filename != "local state":
                                continue
                            seen_paths.add(normalized)
                            profile = extract_profile_from_path(normalized) if file_type == "preferences" else None
                            partition_index = getattr(evidence_fs, "partition_index", 0)
                            discovered.append({
                                "logical_path": normalized,
                                "browser": browser,
                                "profile": profile or ("Default" if file_type == "preferences" else None),
                                "file_type": file_type,
                                "artifact_type": "config",
                                "partition_index": partition_index,
                                "fs_type": getattr(evidence_fs, "fs_type", None),
                                "forensic_path": self._build_forensic_path(partition_index, normalized),
                                "display_name": CHROMIUM_BROWSERS[browser]["display_name"],
                            })
                            callbacks.on_log(f"Found {browser} {file_type}: {normalized}", "info")
                    except Exception as exc:
                        LOGGER.debug("Pattern %s failed: %s", pattern, exc)
        return discovered

    def _discover_files_multi_partition(
        self,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        callbacks: ExtractorCallbacks,
    ) -> Dict[Optional[int], List[Dict[str, Any]]]:
        """Discover config files across partitions using file_list."""
        available, count = check_file_list_available(evidence_conn, evidence_id)
        if not available:
            callbacks.on_log("file_list not available, using single-partition discovery", "warning")
            return {}

        callbacks.on_log(f"Using file_list discovery ({count:,} files indexed)", "info")
        files_by_partition: Dict[Optional[int], List[Dict[str, Any]]] = {}
        seen: set[tuple[Optional[int], str]] = set()

        for file_type, artifact, filenames in (
            ("preferences", "preferences", ["Preferences"]),
            ("local_state", "local_state", ["Local State"]),
        ):
            path_patterns = []
            for browser in browsers:
                for pattern in get_artifact_patterns(browser, artifact):
                    path_patterns.append(glob_to_sql_like(pattern))

            result, embedded_roots = discover_artifacts_with_embedded_roots(
                evidence_conn,
                evidence_id,
                artifact=artifact,
                filename_patterns=filenames,
                path_patterns=sorted(set(path_patterns)),
            )
            callbacks.on_log(
                f"{file_type} discovery: {result.total_matches} files across "
                f"{len(result.partitions_with_matches)} partition(s)",
                "info",
            )

            for partition in self._sorted_partitions(result.matches_by_partition):
                matches = sorted(
                    result.matches_by_partition[partition],
                    key=lambda match: (match.file_path, match.inode or 0),
                )
                embedded_paths = get_embedded_root_paths(embedded_roots, partition)
                for match in matches:
                    key = (partition, match.file_path)
                    if key in seen:
                        continue
                    seen.add(key)

                    browser = detect_browser_from_path(match.file_path, embedded_roots=embedded_paths)
                    if browser and browser not in browsers and browser != "chromium_embedded":
                        continue
                    browser = browser or "chromium"
                    profile = extract_profile_from_path(match.file_path) if file_type == "preferences" else None
                    display_name = CHROMIUM_BROWSERS.get(browser, {}).get("display_name", "Embedded Chromium")

                    files_by_partition.setdefault(partition, []).append({
                        "logical_path": match.file_path,
                        "browser": browser,
                        "profile": profile or ("Default" if file_type == "preferences" else None),
                        "file_type": file_type,
                        "artifact_type": "config",
                        "partition_index": partition,
                        "inode": match.inode,
                        "size_bytes": match.size_bytes,
                        "forensic_path": self._build_forensic_path(partition, match.file_path),
                        "display_name": display_name,
                    })

        return files_by_partition

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict[str, Any],
        output_dir: Path,
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, Any]:
        """Copy a config file to the case workspace and hash it."""
        source_path = file_info["logical_path"]
        browser = file_info.get("browser") or "chromium"
        profile = file_info.get("profile") or "user_data"
        file_type = file_info.get("file_type") or "config"
        partition_index = file_info.get("partition_index")
        if partition_index is None:
            partition_index = getattr(evidence_fs, "partition_index", 0) or 0
        fs_type = file_info.get("fs_type") or getattr(evidence_fs, "fs_type", None)
        forensic_path = file_info.get("forensic_path") or self._build_forensic_path(partition_index, source_path)

        declared_size = self._coerce_size(file_info.get("size_bytes") or file_info.get("file_size_bytes"))
        if declared_size is not None and declared_size > MAX_CONFIG_FILE_BYTES:
            message = f"Config file exceeds {MAX_CONFIG_FILE_BYTES} byte safety limit"
            callbacks.on_log(f"Skipping {source_path}: {message}", "warning")
            return self._build_failed_file_entry(
                file_info,
                browser=browser,
                profile=file_info.get("profile"),
                file_type=file_type,
                source_path=source_path,
                partition_index=partition_index,
                fs_type=fs_type,
                forensic_path=forensic_path,
                error_message=message,
                size_bytes=declared_size,
            )

        try:
            safe_profile = re.sub(r"[^A-Za-z0-9_-]", "_", str(profile))[:40] or "user_data"
            path_hash = hashlib.sha256(source_path.encode("utf-8")).hexdigest()[:10]
            filename = f"{browser}_{safe_profile}_p{partition_index}_{path_hash}_{file_type}.json"
            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")
            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

            return {
                "copy_status": "ok",
                "size_bytes": len(file_content),
                "file_size_bytes": len(file_content),
                "md5": hashlib.md5(file_content).hexdigest(),
                "sha256": hashlib.sha256(file_content).hexdigest(),
                "extracted_path": str(dest_path),
                "browser": browser,
                "profile": file_info.get("profile"),
                "file_type": file_type,
                "logical_path": source_path,
                "artifact_type": "config",
                "partition_index": partition_index,
                "fs_type": fs_type,
                "forensic_path": forensic_path,
            }
        except Exception as exc:
            callbacks.on_log(f"Failed to extract {source_path}: {exc}", "error")
            return self._build_failed_file_entry(
                file_info,
                browser=browser,
                profile=file_info.get("profile"),
                file_type=file_type,
                source_path=source_path,
                partition_index=partition_index,
                fs_type=fs_type,
                forensic_path=forensic_path,
                error_message=str(exc),
            )

    def _parse_file_records(
        self,
        config_data: dict,
        file_entry: Dict[str, Any],
        run_id: str,
        warning_collector: ExtractionWarningCollector,
    ) -> List[Dict[str, Any]]:
        """Parse one copied config file according to its source type."""
        common = {
            "browser": file_entry.get("browser", "chromium"),
            "source_path": file_entry["logical_path"],
            "run_id": run_id,
            "partition_index": file_entry.get("partition_index"),
            "fs_type": file_entry.get("fs_type"),
            "logical_path": file_entry.get("logical_path"),
            "forensic_path": file_entry.get("forensic_path"),
            "warning_collector": warning_collector,
        }
        if file_entry.get("file_type") == "local_state":
            return parse_local_state_config(config_data, **common)
        return parse_preferences_config(
            config_data,
            profile=file_entry.get("profile") or "Default",
            **common,
        )

    def _generate_run_id(self) -> str:
        """Generate a run identifier for this extractor."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        return f"{timestamp}_{str(uuid.uuid4())[:8]}"

    @staticmethod
    def _sorted_partitions(mapping) -> List[Optional[int]]:
        """Return partition keys in stable order, with active/default first."""
        return sorted(mapping.keys(), key=lambda value: (-1 if value is None else value))

    @staticmethod
    def _sorted_file_entries(files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Return files in stable source order for deterministic manifests and rows."""
        return sorted(
            files,
            key=lambda item: (
                -1 if item.get("partition_index") is None else item.get("partition_index"),
                item.get("logical_path") or "",
                item.get("file_type") or "",
                item.get("inode") or 0,
            ),
        )

    @staticmethod
    def _build_forensic_path(partition_index: Optional[int], logical_path: Optional[str]) -> Optional[str]:
        """Build a stable partition-qualified evidence path when no richer path is available."""
        if not logical_path:
            return None
        partition = 0 if partition_index is None else partition_index
        return f"p{partition}:{logical_path}"

    @staticmethod
    def _coerce_size(value: Any) -> Optional[int]:
        """Normalize optional size metadata from file_list or manifest entries."""
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _validate_config_file_size(self, extracted_path: Path, file_entry: Dict[str, Any]) -> None:
        """Fail closed before reading unexpectedly large copied config files."""
        declared_size = self._coerce_size(file_entry.get("file_size_bytes") or file_entry.get("size_bytes"))
        if declared_size is not None and declared_size > MAX_CONFIG_FILE_BYTES:
            raise ValueError(f"Config file exceeds {MAX_CONFIG_FILE_BYTES} byte safety limit")

        actual_size = extracted_path.stat().st_size
        if actual_size > MAX_CONFIG_FILE_BYTES:
            raise ValueError(f"Copied config file exceeds {MAX_CONFIG_FILE_BYTES} byte safety limit")

    def _build_failed_file_entry(
        self,
        file_info: Dict[str, Any],
        *,
        browser: str,
        profile: Optional[str],
        file_type: str,
        source_path: str,
        partition_index: Optional[int],
        fs_type: Optional[str],
        forensic_path: Optional[str],
        error_message: str,
        size_bytes: int = 0,
    ) -> Dict[str, Any]:
        """Build a failed extraction manifest entry with audit-compatible error keys."""
        return {
            "copy_status": "error",
            "size_bytes": size_bytes,
            "file_size_bytes": size_bytes,
            "md5": None,
            "sha256": None,
            "extracted_path": None,
            "browser": browser,
            "profile": profile,
            "file_type": file_type,
            "logical_path": source_path,
            "artifact_type": "config",
            "partition_index": partition_index,
            "fs_type": fs_type,
            "forensic_path": forensic_path,
            "error": error_message,
            "error_message": error_message,
            "inode": file_info.get("inode"),
        }

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context from evidence filesystem."""
        try:
            source_path = getattr(evidence_fs, "source_path", None)
            if source_path is not None and not isinstance(source_path, (str, Path)):
                source_path = None
            fs_type = getattr(evidence_fs, "fs_type", "unknown")
            if not isinstance(fs_type, str):
                fs_type = "unknown"
            return {"image_path": str(source_path) if source_path else None, "fs_type": fs_type}
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