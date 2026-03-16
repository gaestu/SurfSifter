"""Safari Extensions extractor implementation."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import QLabel, QWidget

from core.database import (
    delete_extensions_by_run,
    insert_browser_inventory,
    insert_extensions,
    update_inventory_ingestion_status,
)
from core.logging import get_logger
from extractors._shared.extracted_files_audit import record_browser_files
from extractors._shared.extraction_warnings import (
    ExtractionWarningCollector,
    WARNING_TYPE_PLIST_UNKNOWN_KEY,
    WARNING_TYPE_PLIST_PARSE_ERROR,
    WARNING_TYPE_JSON_UNKNOWN_KEY,
    CATEGORY_PLIST,
    CATEGORY_JSON,
)
from extractors._shared.file_list_discovery import (
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from extractors._shared.known_extensions import load_known_extensions, match_known_extension
from extractors._shared.risk_classifier import calculate_risk_level
from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from .._discovery import discover_safari_files, discover_safari_files_fallback
from .._parsers import (
    SafariExtension,
    get_extension_stats,
    parse_appex_info_plist,
    parse_extensions_plist,
    parse_safariextz_info,
    parse_webextension_manifest,
)
from .._patterns import extract_user_from_path
from ._discovery import discover_app_extensions

LOGGER = get_logger("extractors.browser.safari.extensions")


class SafariExtensionsExtractor(BaseExtractor):
    """Extract Safari extension artifacts across all 3 eras."""

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="safari_extensions",
            display_name="Safari Extensions",
            description="Extract Safari extension metadata (Legacy, App Extension, Web Extension)",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        if evidence_fs is None:
            return False, "No evidence filesystem mounted"
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        manifest_path = self._find_latest_manifest(output_dir)
        if not manifest_path:
            return False, "No extraction manifest found - run extraction first"
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception as exc:
            return False, f"Failed to read manifest: {exc}"
        status = manifest.get("status", "ok")
        if status != "ok":
            return False, f"Extraction status is {status!r} - re-run extraction"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        return self._find_latest_manifest(output_dir) is not None

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        return MultiPartitionWidget(parent)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
    ) -> QWidget:
        manifest_path = self._find_latest_manifest(output_dir)
        if not manifest_path:
            return QLabel("Safari Extensions\nNo extraction run yet", parent)
        try:
            manifest = json.loads(manifest_path.read_text())
            file_count = len(manifest.get("files", []))
            parsed = manifest.get("parsed_counts", {})
            text = (
                "Safari Extensions\n"
                f"Run ID: {manifest.get('run_id', 'N/A')}\n"
                f"Extension Files: {file_count}\n"
                f"Extensions Parsed: {parsed.get('total_count', 0)}"
            )
        except Exception:
            text = "Safari Extensions\nFailed to read manifest"
        return QLabel(text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        return case_root / "evidences" / evidence_label / "safari_extensions"

    # ------------------------------------------------------------------ #
    #  Extraction Phase
    # ------------------------------------------------------------------ #

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        from core.statistics_collector import StatisticsCollector

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        callbacks.on_step("Initializing Safari extensions extraction")
        LOGGER.info("Starting Safari extensions extraction (run_id=%s)", run_id)

        collector = StatisticsCollector.get_instance()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        output_dir.mkdir(parents=True, exist_ok=True)
        run_dir = output_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        manifest_data: Dict[str, Any] = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "e01_context": self._get_e01_context(evidence_fs),
            "files": [],
            "status": "ok",
            "notes": [],
        }

        evidence_conn = config.get("evidence_conn")

        # --- Legacy discovery (Extensions.plist + .safariextz) ---
        callbacks.on_step("Discovering Safari extension artifacts")
        files_by_partition = discover_safari_files(
            evidence_conn, evidence_id,
            artifact_names=["extensions"],
            callbacks=callbacks,
        )
        if not files_by_partition:
            files_by_partition = discover_safari_files_fallback(
                evidence_fs, artifact_names=["extensions"], callbacks=callbacks,
            )

        # --- App Extension discovery via file_list ---
        appex_by_partition: Dict[int, List[Dict[str, Any]]] = {}
        if evidence_conn is not None:
            appex_by_partition = discover_app_extensions(
                evidence_conn, evidence_id, callbacks=callbacks,
            )

        manifest_data["multi_partition"] = (
            len(files_by_partition) > 1 or len(appex_by_partition) > 1
        )
        all_partitions = sorted(
            set(files_by_partition.keys()) | set(appex_by_partition.keys())
        )
        manifest_data["partitions_scanned"] = all_partitions

        total_discovered = (
            sum(len(v) for v in files_by_partition.values())
            + sum(len(v) for v in appex_by_partition.values())
        )

        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=total_discovered)

        if not files_by_partition and not appex_by_partition:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Safari extension files found")
        else:
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            current_partition = getattr(evidence_fs, "partition_index", 0)

            # --- Copy legacy extension files ---
            for partition_idx in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_idx]
                multi = len(all_partitions) > 1

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

                        source_path = file_data["logical_path"]
                        artifact_type = self._classify_legacy_artifact(source_path)

                        file_info = self._extract_file(
                            fs_to_use, source_path, run_dir, output_dir,
                            partition_index=partition_idx if multi else None,
                            artifact_type=artifact_type,
                        )
                        if file_info:
                            file_info["partition_index"] = partition_idx
                            if file_data.get("inode"):
                                file_info["inode"] = file_data["inode"]
                            manifest_data["files"].append(file_info)
                            callbacks.on_log(f"Copied: {source_path}", "info")

            # --- Copy app extension files ---
            for partition_idx in sorted(appex_by_partition.keys()):
                bundles = appex_by_partition[partition_idx]
                multi = len(all_partitions) > 1

                if ewf_paths is not None and partition_idx != current_partition:
                    ctx = open_partition_for_extraction(ewf_paths, partition_idx)
                else:
                    ctx = open_partition_for_extraction(evidence_fs, None)

                with ctx as fs_to_use:
                    for bundle in bundles:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            manifest_data["notes"].append("Extraction cancelled by user")
                            break

                        # Copy Info.plist
                        info_path = bundle["info_plist_path"]
                        if info_path:
                            file_info = self._extract_file(
                                fs_to_use, info_path, run_dir, output_dir,
                                partition_index=partition_idx if multi else None,
                                artifact_type="appex_info_plist",
                            )
                            if file_info:
                                file_info["partition_index"] = partition_idx
                                file_info["bundle_path"] = bundle["bundle_path"]
                                manifest_data["files"].append(file_info)
                                callbacks.on_log(f"Copied: {info_path}", "info")

                        # Copy manifest.json if present
                        manifest_json_path = bundle.get("manifest_json_path")
                        if manifest_json_path:
                            file_info = self._extract_file(
                                fs_to_use, manifest_json_path, run_dir, output_dir,
                                partition_index=partition_idx if multi else None,
                                artifact_type="appex_manifest_json",
                            )
                            if file_info:
                                file_info["partition_index"] = partition_idx
                                file_info["bundle_path"] = bundle["bundle_path"]
                                manifest_data["files"].append(file_info)
                                callbacks.on_log(f"Copied: {manifest_json_path}", "info")

        manifest_path = run_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        record_browser_files(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            run_id=run_id,
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
            manifest_data=manifest_data,
            callbacks=callbacks,
        )

        if collector:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_step("Safari extensions extraction complete")
        return manifest_data["status"] not in {"error", "cancelled"}

    # ------------------------------------------------------------------ #
    #  Ingestion Phase
    # ------------------------------------------------------------------ #

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, int]:
        from core.statistics_collector import StatisticsCollector

        counts: Dict[str, int] = {"extensions": 0}
        manifest_path = self._find_latest_manifest(output_dir)
        if not manifest_path:
            callbacks.on_error("Manifest not found", str(output_dir))
            return counts

        manifest = json.loads(manifest_path.read_text())
        run_id = manifest.get("run_id", "")
        if not run_id:
            callbacks.on_error("Manifest missing run_id", str(manifest_path))
            return counts

        evidence_label = config.get("evidence_label", "")
        collector = StatisticsCollector.get_instance()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        files = manifest.get("files", [])
        if not files:
            if collector:
                collector.report_ingested(evidence_id, self.metadata.name, records=0, extensions=0)
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return counts

        callbacks.on_step("Initializing Safari extensions ingestion")
        delete_extensions_by_run(evidence_conn, evidence_id, run_id)

        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        known_data = load_known_extensions()
        all_extensions: List[SafariExtension] = []
        extension_records: List[Dict[str, Any]] = []

        # --- Index appex files by bundle_path for merging ---
        appex_info_by_bundle: Dict[str, Dict[str, Any]] = {}
        appex_manifest_by_bundle: Dict[str, Dict[str, Any]] = {}
        for file_info in files:
            atype = file_info.get("artifact_type")
            bp = file_info.get("bundle_path")
            if atype == "appex_info_plist" and bp:
                appex_info_by_bundle[bp] = file_info
            elif atype == "appex_manifest_json" and bp:
                appex_manifest_by_bundle[bp] = file_info

        # --- Process legacy Extensions.plist files ---
        for file_info in files:
            if file_info.get("artifact_type") != "extensions_plist":
                continue
            local_path = file_info.get("local_path")
            if not local_path:
                continue
            local_path_obj = Path(local_path)
            if not local_path_obj.exists():
                continue

            profile = file_info.get("profile") or file_info.get("user") or "Default"
            source_path = file_info.get("source_path") or str(local_path_obj)

            inventory_id = insert_browser_inventory(
                evidence_conn,
                evidence_id=evidence_id,
                browser="safari",
                artifact_type="extensions",
                run_id=run_id,
                extracted_path=str(local_path_obj),
                extraction_status="ok",
                extraction_timestamp_utc=manifest.get("extraction_timestamp_utc", ""),
                logical_path=source_path,
                profile=profile,
                partition_index=file_info.get("partition_index"),
                fs_type=file_info.get("fs_type"),
                forensic_path=source_path,
                extraction_tool=f"{self.metadata.name}:{self.metadata.version}",
                file_size_bytes=file_info.get("size_bytes"),
                file_md5=file_info.get("md5"),
                file_sha256=file_info.get("sha256"),
            )

            try:
                callbacks.on_step(f"Parsing {local_path_obj.name}")
                extensions = parse_extensions_plist(local_path_obj)
                all_extensions.extend(extensions)

                for ext in extensions:
                    # Report unknown plist keys as warnings
                    for key in ext.unknown_keys:
                        warning_collector.add_warning(
                            WARNING_TYPE_PLIST_UNKNOWN_KEY,
                            key,
                            category=CATEGORY_PLIST,
                            artifact_type="extensions_plist",
                            source_file=source_path,
                        )

                    record = self._build_extension_record(
                        ext, run_id, source_path, profile,
                        file_info.get("partition_index"),
                        file_info.get("fs_type"),
                        known_data,
                    )
                    extension_records.append(record)

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=len(extensions),
                )
            except Exception as exc:
                LOGGER.error("Failed to parse %s: %s", local_path_obj, exc, exc_info=True)
                warning_collector.add_warning(
                    WARNING_TYPE_PLIST_PARSE_ERROR,
                    local_path_obj.name,
                    category=CATEGORY_PLIST,
                    artifact_type="extensions_plist",
                    source_file=source_path,
                    item_value=str(exc),
                )
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="error",
                    notes=str(exc),
                )

        # --- Process legacy .safariextz bundles ---
        for file_info in files:
            if file_info.get("artifact_type") != "safariextz":
                continue
            local_path = file_info.get("local_path")
            if not local_path:
                continue
            local_path_obj = Path(local_path)
            if not local_path_obj.exists():
                continue

            profile = file_info.get("profile") or file_info.get("user") or "Default"
            source_path = file_info.get("source_path") or str(local_path_obj)

            inventory_id = insert_browser_inventory(
                evidence_conn,
                evidence_id=evidence_id,
                browser="safari",
                artifact_type="extensions",
                run_id=run_id,
                extracted_path=str(local_path_obj),
                extraction_status="ok",
                extraction_timestamp_utc=manifest.get("extraction_timestamp_utc", ""),
                logical_path=source_path,
                profile=profile,
                partition_index=file_info.get("partition_index"),
                fs_type=file_info.get("fs_type"),
                forensic_path=source_path,
                extraction_tool=f"{self.metadata.name}:{self.metadata.version}",
                file_size_bytes=file_info.get("size_bytes"),
                file_md5=file_info.get("md5"),
                file_sha256=file_info.get("sha256"),
            )

            try:
                callbacks.on_step(f"Parsing {local_path_obj.name}")
                ext = parse_safariextz_info(local_path_obj)
                if ext is None:
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="ok",
                        records_parsed=0,
                        notes="Failed to parse .safariextz Info.plist",
                    )
                    continue

                all_extensions.append(ext)

                for key in ext.unknown_keys:
                    warning_collector.add_warning(
                        WARNING_TYPE_PLIST_UNKNOWN_KEY,
                        key,
                        category=CATEGORY_PLIST,
                        artifact_type="safariextz",
                        source_file=source_path,
                    )

                record = self._build_extension_record(
                    ext, run_id, source_path, profile,
                    file_info.get("partition_index"),
                    file_info.get("fs_type"),
                    known_data,
                )
                extension_records.append(record)

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=1,
                )
            except Exception as exc:
                LOGGER.error("Failed to parse %s: %s", local_path_obj, exc, exc_info=True)
                warning_collector.add_warning(
                    WARNING_TYPE_PLIST_PARSE_ERROR,
                    local_path_obj.name,
                    category=CATEGORY_PLIST,
                    artifact_type="safariextz",
                    source_file=source_path,
                    item_value=str(exc),
                )
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="error",
                    notes=str(exc),
                )

        # --- Process App Extension .appex bundles ---
        processed_bundles: set = set()
        for bundle_path, info_file in appex_info_by_bundle.items():
            if bundle_path in processed_bundles:
                continue
            processed_bundles.add(bundle_path)

            local_path = info_file.get("local_path")
            if not local_path:
                continue
            local_path_obj = Path(local_path)
            if not local_path_obj.exists():
                continue

            profile = info_file.get("profile") or info_file.get("user") or "Default"
            source_path = info_file.get("source_path") or str(local_path_obj)

            inventory_id = insert_browser_inventory(
                evidence_conn,
                evidence_id=evidence_id,
                browser="safari",
                artifact_type="extensions",
                run_id=run_id,
                extracted_path=str(local_path_obj),
                extraction_status="ok",
                extraction_timestamp_utc=manifest.get("extraction_timestamp_utc", ""),
                logical_path=source_path,
                profile=profile,
                partition_index=info_file.get("partition_index"),
                fs_type=info_file.get("fs_type"),
                forensic_path=source_path,
                extraction_tool=f"{self.metadata.name}:{self.metadata.version}",
                file_size_bytes=info_file.get("size_bytes"),
                file_md5=info_file.get("md5"),
                file_sha256=info_file.get("sha256"),
            )

            try:
                callbacks.on_step(f"Parsing {bundle_path}")
                ext = parse_appex_info_plist(local_path_obj)

                if ext is None:
                    # Not a Safari extension — skip
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id=inventory_id,
                        status="ok",
                        records_parsed=0,
                        notes="Not a Safari extension (NSExtensionPointIdentifier mismatch)",
                    )
                    continue

                # Report unknown Info.plist keys BEFORE merging with manifest.json
                for key in ext.unknown_keys:
                    warning_collector.add_warning(
                        WARNING_TYPE_PLIST_UNKNOWN_KEY,
                        key,
                        category=CATEGORY_PLIST,
                        artifact_type="appex_info_plist",
                        source_file=source_path,
                    )

                # Check for accompanying manifest.json (Web Extension)
                manifest_file = appex_manifest_by_bundle.get(bundle_path)
                if manifest_file:
                    mj_local = manifest_file.get("local_path")
                    if mj_local and Path(mj_local).exists():
                        web_ext = parse_webextension_manifest(
                            Path(mj_local), bundle_info=ext,
                        )
                        if web_ext:
                            # Report unknown manifest.json keys
                            for key in web_ext.unknown_keys:
                                warning_collector.add_warning(
                                    WARNING_TYPE_JSON_UNKNOWN_KEY,
                                    key,
                                    category=CATEGORY_JSON,
                                    artifact_type="appex_manifest_json",
                                    source_file=manifest_file.get("source_path", ""),
                                )
                            ext = web_ext

                all_extensions.append(ext)
                record = self._build_extension_record(
                    ext, run_id, source_path, profile,
                    info_file.get("partition_index"),
                    info_file.get("fs_type"),
                    known_data,
                )
                extension_records.append(record)

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    records_parsed=1,
                )
            except Exception as exc:
                LOGGER.error("Failed to parse %s: %s", bundle_path, exc, exc_info=True)
                warning_collector.add_warning(
                    WARNING_TYPE_PLIST_PARSE_ERROR,
                    Path(bundle_path).name,
                    category=CATEGORY_PLIST,
                    artifact_type="appex_info_plist",
                    source_file=source_path,
                    item_value=str(exc),
                )
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="error",
                    notes=str(exc),
                )

        # --- Batch insert ---
        if extension_records:
            counts["extensions"] = insert_extensions(evidence_conn, evidence_id, extension_records)

        # --- Warnings and stats ---
        warning_collector.flush_to_database(evidence_conn)

        ext_stats = get_extension_stats(all_extensions)
        manifest["parsed_counts"] = ext_stats
        manifest_path.write_text(json.dumps(manifest, indent=2))

        evidence_conn.commit()
        callbacks.on_step("Safari extensions ingestion complete")

        if collector:
            collector.report_ingested(
                evidence_id,
                self.metadata.name,
                records=counts["extensions"],
                extensions=counts["extensions"],
            )
            status = "success" if counts["extensions"] > 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        return counts

    # ------------------------------------------------------------------ #
    #  Helper: build extension record dict for DB insert
    # ------------------------------------------------------------------ #

    @staticmethod
    def _build_extension_record(
        ext: SafariExtension,
        run_id: str,
        source_path: str,
        profile: str,
        partition_index: Optional[int],
        fs_type: Optional[str],
        known_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Parse permissions for risk calculation
        perms_list: List[str] = []
        if ext.permissions:
            try:
                parsed = json.loads(ext.permissions)
                if isinstance(parsed, list):
                    perms_list = parsed
                elif isinstance(parsed, dict):
                    # SFSafariWebsiteAccess format: {"Allowed Domains": [...]}
                    for v in parsed.values():
                        if isinstance(v, list):
                            perms_list.extend(str(i) for i in v)
                        elif isinstance(v, str):
                            perms_list.append(v)
            except (json.JSONDecodeError, TypeError):
                pass

        host_perms_list: List[str] = []
        if ext.host_permissions:
            try:
                parsed = json.loads(ext.host_permissions)
                if isinstance(parsed, list):
                    host_perms_list = parsed
            except (json.JSONDecodeError, TypeError):
                pass

        known_match = match_known_extension(
            ext.bundle_identifier, ext.name, known_data,
        )

        notes = None
        if ext.extension_era != "legacy":
            notes = json.dumps({
                "extension_era": ext.extension_era,
                "extension_point": ext.extension_point,
            })

        return {
            "browser": "safari",
            "profile": profile,
            "extension_id": ext.bundle_identifier,
            "name": ext.name,
            "version": ext.version,
            "description": ext.description,
            "enabled": 1 if ext.enabled else (0 if ext.enabled is False else None),
            "permissions": json.dumps(perms_list) if perms_list else None,
            "host_permissions": ext.host_permissions,
            "manifest_version": ext.manifest_version,
            "content_scripts": ext.content_scripts,
            "install_time_utc": ext.added_date_utc,
            "risk_level": calculate_risk_level(perms_list, host_perms_list),
            "known_extension": json.dumps(known_match) if known_match else None,
            "run_id": run_id,
            "source_path": source_path,
            "partition_index": partition_index,
            "fs_type": fs_type,
            "logical_path": source_path,
            "forensic_path": source_path,
            "discovered_by": "safari_extensions",
            "notes": notes,
        }

    # ------------------------------------------------------------------ #
    #  Helper: classify legacy artifact type from path
    # ------------------------------------------------------------------ #

    @staticmethod
    def _classify_legacy_artifact(source_path: str) -> str:
        lower = source_path.lower()
        if lower.endswith("extensions.plist"):
            return "extensions_plist"
        elif lower.endswith(".safariextz"):
            return "safariextz"
        return "extensions_unknown"

    # ------------------------------------------------------------------ #
    #  File extraction helper
    # ------------------------------------------------------------------ #

    def _extract_file(
        self,
        evidence_fs,
        source_path: str,
        run_dir: Path,
        output_dir: Path,
        partition_index: Optional[int] = None,
        artifact_type: str = "extensions_unknown",
    ) -> Optional[Dict[str, Any]]:
        try:
            content = evidence_fs.read_file(source_path)
        except Exception:
            return None

        user = extract_user_from_path(source_path) or "unknown"
        profile = self._extract_profile(source_path, user=user)

        # Build a descriptive filename
        filename = Path(source_path).name
        slug = _safe_slug(profile)
        if partition_index is not None:
            base_name = f"safari_{slug}_p{partition_index}_{filename}"
        else:
            base_name = f"safari_{slug}_{filename}"

        dest_path = run_dir / base_name
        if dest_path.exists():
            suffix = hashlib.sha1(source_path.encode("utf-8", errors="ignore")).hexdigest()[:8]
            stem = dest_path.stem
            dest_path = run_dir / f"{stem}_{suffix}{dest_path.suffix}"

        dest_path.write_bytes(content)

        extracted_path = str(dest_path.relative_to(output_dir.parent))
        return {
            "local_path": str(dest_path),
            "extracted_path": extracted_path,
            "source_path": source_path,
            "artifact_type": artifact_type,
            "browser": "safari",
            "user": user,
            "profile": profile,
            "md5": hashlib.md5(content).hexdigest(),
            "sha256": hashlib.sha256(content).hexdigest(),
            "size_bytes": len(content),
            "partition_index": getattr(evidence_fs, "partition_index", None),
            "fs_type": getattr(evidence_fs, "fs_type", None),
        }

    # ------------------------------------------------------------------ #
    #  Shared static helpers (same as top_sites pattern)
    # ------------------------------------------------------------------ #

    @staticmethod
    def _extract_profile(source_path: str, *, user: str) -> str:
        normalized = source_path.replace("\\", "/")
        parts = [part for part in normalized.split("/") if part]
        try:
            idx = parts.index("Profiles")
        except ValueError:
            idx = -1
        if idx >= 0 and idx + 1 < len(parts):
            profile_id = parts[idx + 1]
            return f"{user}:{profile_id}" if user else profile_id
        return user or "Default"

    @staticmethod
    def _find_latest_manifest(output_dir: Path) -> Optional[Path]:
        manifests = sorted(output_dir.glob("*/manifest.json"))
        if manifests:
            return manifests[-1]
        fallback = output_dir / "manifest.json"
        if fallback.exists():
            return fallback
        return None

    @staticmethod
    def _generate_run_id() -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        return f"{timestamp}_{str(uuid.uuid4())[:8]}"

    @staticmethod
    def _get_e01_context(evidence_fs) -> Dict[str, Any]:
        try:
            source_path = getattr(evidence_fs, "source_path", None)
            fs_type = getattr(evidence_fs, "fs_type", "unknown")
            return {
                "image_path": str(source_path) if source_path else None,
                "fs_type": fs_type if isinstance(fs_type, str) else "unknown",
            }
        except Exception:
            return {"image_path": None, "fs_type": "unknown"}


def _safe_slug(value: str) -> str:
    out = []
    for char in value:
        if char.isalnum() or char in {"-", "_", "."}:
            out.append(char)
        else:
            out.append("_")
    return "".join(out) or "default"
