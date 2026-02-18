"""Safari Cache extractor implementation."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import QLabel, QWidget

from core.logging import get_logger
from extractors._shared.extracted_files_audit import record_browser_files
from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from .._patterns import extract_user_from_path, get_patterns
from .ingestion import CacheIngestionHandler

LOGGER = get_logger("extractors.browser.safari.cache")

_CACHE_DB_NAMES = {"Cache.db", "Cache.db-wal", "Cache.db-journal", "Cache.db-shm"}


class SafariCacheExtractor(BaseExtractor):
    """Extract Safari cache database and fsCachedData files."""

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="safari_cache",
            display_name="Safari Cache",
            description="Extract Safari Cache.db and cached response bodies",
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
            return QLabel("Safari Cache\nNo extraction run yet", parent)
        try:
            manifest = json.loads(manifest_path.read_text())
            files = manifest.get("files", [])
            cache_db_count = sum(1 for f in files if f.get("artifact_type") == "cache_db")
            fs_count = sum(1 for f in files if f.get("artifact_type") == "fscached_data")
            text = (
                "Safari Cache\n"
                f"Run ID: {manifest.get('run_id', 'N/A')}\n"
                f"Cache DB Files: {cache_db_count}\n"
                f"fsCachedData Files: {fs_count}"
            )
        except Exception:
            text = "Safari Cache\nFailed to read manifest"
        return QLabel(text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        return case_root / "evidences" / evidence_label / "safari_cache"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        output_dir.mkdir(parents=True, exist_ok=True)
        run_dir = output_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        callbacks.on_step("Discovering Safari cache files")
        discovered = self._discover_cache_paths(evidence_fs)
        cache_db_paths = sorted(p for p in discovered if Path(p).name == "Cache.db")

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
            "notes": [],
        }

        if not cache_db_paths:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Safari Cache.db files found")
        else:
            for cache_db_path in cache_db_paths:
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    manifest_data["notes"].append("Extraction cancelled by user")
                    break

                cache_root = str(Path(cache_db_path).parent)
                group_id = hashlib.sha1(cache_root.encode("utf-8", errors="ignore")).hexdigest()[:12]
                group_dir = run_dir / f"cache_{group_id}"
                group_dir.mkdir(parents=True, exist_ok=True)

                group_paths = self._collect_group_paths(cache_db_path, discovered)
                for source_path in sorted(group_paths):
                    file_info = self._extract_file(evidence_fs, source_path, group_dir, run_dir)
                    if file_info:
                        file_info["group_id"] = group_id
                        manifest_data["files"].append(file_info)

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
        callbacks.on_step("Safari cache extraction complete")
        return manifest_data["status"] not in {"error", "cancelled"}

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, Any]:
        handler = CacheIngestionHandler(
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
        )
        return handler.run(output_dir, evidence_conn, evidence_id, config, callbacks)

    def _discover_cache_paths(self, evidence_fs) -> List[str]:
        discovered: List[str] = []
        seen = set()
        for pattern in get_patterns("cache"):
            try:
                for path_str in evidence_fs.iter_paths(pattern):
                    if path_str in seen:
                        continue
                    seen.add(path_str)
                    if self._is_supported_cache_path(path_str):
                        discovered.append(path_str)
            except Exception as exc:
                LOGGER.debug("Safari cache pattern failed (%s): %s", pattern, exc)
        return discovered

    def _is_supported_cache_path(self, path_str: str) -> bool:
        path = path_str.replace("\\", "/")
        if "/WebKitCache/" in path or "/WebKit/NetworkCache/" in path or "/WebKit/CacheStorage/" in path:
            return False
        name = Path(path).name
        if name in _CACHE_DB_NAMES:
            return True
        if "/fsCachedData/" in path:
            return True
        return False

    def _collect_group_paths(self, cache_db_path: str, discovered: List[str]) -> List[str]:
        root = str(Path(cache_db_path).parent).rstrip("/")
        selected = set()

        for item in discovered:
            normalized = item.rstrip("/")
            if normalized.startswith(f"{root}/fsCachedData/"):
                selected.add(item)
            elif (
                normalized == cache_db_path
                or (
                    Path(normalized).name in _CACHE_DB_NAMES
                    and str(Path(normalized).parent) == root
                )
            ):
                selected.add(item)

        # Ensure companion files are attempted even if glob discovery missed them.
        for name in _CACHE_DB_NAMES:
            candidate = f"{root}/{name}"
            selected.add(candidate)

        return sorted(selected)

    def _extract_file(
        self,
        evidence_fs,
        source_path: str,
        group_dir: Path,
        run_dir: Path,
    ) -> Optional[Dict[str, Any]]:
        try:
            content = evidence_fs.read_file(source_path)
        except Exception:
            return None

        try:
            source_name = Path(source_path).name
            if "/fsCachedData/" in source_path.replace("\\", "/"):
                artifact_type = "fscached_data"
                rel_dest = Path("fsCachedData") / source_name
            elif source_name in _CACHE_DB_NAMES:
                artifact_type = "cache_db"
                rel_dest = Path(source_name)
            else:
                return None

            dest_path = group_dir / rel_dest
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            dest_path.write_bytes(content)

            md5 = hashlib.md5(content).hexdigest()
            sha256 = hashlib.sha256(content).hexdigest()
            user = extract_user_from_path(source_path) or "unknown"
            extracted_path = str(dest_path.relative_to(run_dir.parent))
            return {
                "local_path": str(dest_path),
                "extracted_path": extracted_path,
                "source_path": source_path,
                "artifact_type": artifact_type,
                "browser": "safari",
                "user": user,
                "md5": md5,
                "sha256": sha256,
                "size_bytes": len(content),
                "partition_index": None,
                "fs_type": None,
            }
        except Exception as exc:
            LOGGER.debug("Failed to extract Safari cache file %s: %s", source_path, exc)
            return None

    def _find_latest_manifest(self, output_dir: Path) -> Optional[Path]:
        manifests = sorted(output_dir.glob("*/manifest.json"))
        if manifests:
            return manifests[-1]
        fallback = output_dir / "manifest.json"
        if fallback.exists():
            return fallback
        return None

    def _generate_run_id(self) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        return f"{timestamp}_{str(uuid.uuid4())[:8]}"

    def _get_e01_context(self, evidence_fs) -> Dict[str, Any]:
        try:
            source_path = getattr(evidence_fs, "source_path", None)
            fs_type = getattr(evidence_fs, "fs_type", "unknown")
            return {
                "image_path": str(source_path) if source_path else None,
                "fs_type": fs_type if isinstance(fs_type, str) else "unknown",
            }
        except Exception:
            return {"image_path": None, "fs_type": "unknown"}
