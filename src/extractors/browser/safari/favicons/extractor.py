"""Safari Favicons extractor implementation."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from PySide6.QtWidgets import QLabel, QWidget

from core.logging import get_logger
from extractors._shared.extracted_files_audit import record_browser_files
from extractors._shared.file_list_discovery import (
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from .._discovery import discover_safari_files, discover_safari_files_fallback
from .._patterns import extract_user_from_path, get_patterns
from .ingestion import FaviconsIngestionHandler

LOGGER = get_logger("extractors.browser.safari.favicons")


class SafariFaviconsExtractor(BaseExtractor):
    """Extract Safari favicon cache artifacts from macOS evidence."""

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="safari_favicons",
            display_name="Safari Favicons",
            description="Extract Safari Favicons.db mappings and icon cache files",
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
            return QLabel("Safari Favicons\nNo extraction run yet", parent)
        try:
            manifest = json.loads(manifest_path.read_text())
            files = manifest.get("files", [])
            db_count = sum(1 for f in files if f.get("artifact_type") == "favicons_db")
            touch_count = sum(1 for f in files if f.get("artifact_type") == "touch_icon_file")
            template_count = sum(1 for f in files if f.get("artifact_type") == "template_icon_file")
            text = (
                "Safari Favicons\n"
                f"Run ID: {manifest.get('run_id', 'N/A')}\n"
                f"Favicons.db Files: {db_count}\n"
                f"Touch Icon Files: {touch_count}\n"
                f"Template Icon Files: {template_count}"
            )
        except Exception:
            text = "Safari Favicons\nFailed to read manifest"
        return QLabel(text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        return case_root / "evidences" / evidence_label / "safari_favicons"

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
        callbacks.on_step("Initializing Safari favicons extraction")
        LOGGER.info("Starting Safari favicons extraction (run_id=%s)", run_id)

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

        callbacks.on_step("Discovering Safari favicon artifacts")
        # Multi-partition discovery with fallback to filesystem iteration
        evidence_conn = config.get("evidence_conn")
        files_by_partition = discover_safari_files(
            evidence_conn, evidence_id,
            artifact_names=["favicons"],
            callbacks=callbacks,
        )
        if not files_by_partition:
            files_by_partition = discover_safari_files_fallback(
                evidence_fs, artifact_names=["favicons"], callbacks=callbacks,
            )

        # Filter to supported paths only (same logic as old _discover_favicon_paths)
        for part_idx in list(files_by_partition.keys()):
            files_by_partition[part_idx] = [
                f for f in files_by_partition[part_idx]
                if self._classify_source_path(f["logical_path"])[0] is not None
            ]
            if not files_by_partition[part_idx]:
                del files_by_partition[part_idx]

        manifest_data["multi_partition"] = len(files_by_partition) > 1
        manifest_data["partitions_scanned"] = sorted(files_by_partition.keys())
        total_discovered = sum(len(v) for v in files_by_partition.values())

        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=total_discovered)

        if not files_by_partition:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Safari favicon cache artifacts found")
        else:
            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            current_partition = getattr(evidence_fs, "partition_index", 0)

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
                        file_info = self._extract_file(
                            fs_to_use, file_data["logical_path"], run_dir, output_dir,
                        )
                        if file_info:
                            file_info["partition_index"] = partition_idx
                            if file_data.get("inode"):
                                file_info["inode"] = file_data["inode"]
                            manifest_data["files"].append(file_info)
                            callbacks.on_log(f"Copied: {file_data['logical_path']}", "info")

        manifest_path = run_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        record_browser_files(
            evidence_conn=config.get("evidence_conn"),
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

        callbacks.on_step("Safari favicons extraction complete")
        return manifest_data["status"] not in {"error", "cancelled"}

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, Any]:
        handler = FaviconsIngestionHandler(
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
        )
        return handler.run(output_dir, evidence_conn, evidence_id, config, callbacks)

    def _extract_file(
        self,
        evidence_fs,
        source_path: str,
        run_dir: Path,
        output_dir: Path,
    ) -> Optional[Dict[str, Any]]:
        artifact_type, group, relative_tail = self._classify_source_path(source_path)
        if artifact_type is None or group is None:
            return None

        try:
            content = evidence_fs.read_file(source_path)
        except Exception:
            return None

        user = extract_user_from_path(source_path) or "unknown"
        profile = self._extract_profile(source_path, user=user)
        dest_path = run_dir / group / _safe_slug(profile) / relative_tail
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        if dest_path.exists():
            # Preserve both files deterministically if a collision occurs.
            suffix = hashlib.sha1(source_path.encode("utf-8", errors="ignore")).hexdigest()[:8]
            dest_path = dest_path.with_name(f"{dest_path.stem}_{suffix}{dest_path.suffix}")
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

    @staticmethod
    def _classify_source_path(source_path: str) -> tuple[Optional[str], Optional[str], Optional[Path]]:
        normalized = source_path.replace("\\", "/")
        path = Path(normalized)
        name = path.name

        if "/Favicon Cache/" in normalized:
            rel_tail = _tail_after_marker(normalized, "Favicon Cache")
            if name == "Favicons.db":
                return "favicons_db", "favicons", rel_tail
            if name in {"Favicons.db-wal", "Favicons.db-shm", "Favicons.db-journal"}:
                return "favicons_db_aux", "favicons", rel_tail
            return "favicon_cache_file", "favicons", rel_tail

        if "/Touch Icons Cache/" in normalized:
            rel_tail = _tail_after_marker(normalized, "Touch Icons Cache")
            return "touch_icon_file", "touch_icons", rel_tail

        if "/Template Icons/" in normalized:
            rel_tail = _tail_after_marker(normalized, "Template Icons")
            return "template_icon_file", "template_icons", rel_tail

        return None, None, None

    @staticmethod
    def _extract_profile(source_path: str, *, user: str) -> str:
        normalized = source_path.replace("\\", "/")
        parts = [p for p in normalized.split("/") if p]
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


def _tail_after_marker(normalized_path: str, marker: str) -> Path:
    marker_token = f"/{marker}/"
    idx = normalized_path.find(marker_token)
    if idx < 0:
        return Path(Path(normalized_path).name)
    tail = normalized_path[idx + len(marker_token):].strip("/")
    return Path(tail) if tail else Path(Path(normalized_path).name)


def _safe_slug(value: str) -> str:
    out = []
    for char in value:
        if char.isalnum() or char in {"-", "_", "."}:
            out.append(char)
        else:
            out.append("_")
    return "".join(out) or "default"
