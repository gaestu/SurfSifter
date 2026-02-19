"""Safari Top Sites extractor implementation."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from PySide6.QtWidgets import QLabel, QWidget

from core.database import (
    delete_top_sites_by_run,
    delete_urls_by_run,
    insert_browser_inventory,
    insert_top_sites,
    insert_urls,
    update_inventory_ingestion_status,
)
from core.logging import get_logger
from extractors._shared.extracted_files_audit import record_browser_files
from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from .._parsers import SafariTopSite, get_top_site_stats, parse_top_sites
from .._patterns import extract_user_from_path, get_patterns

LOGGER = get_logger("extractors.browser.safari.top_sites")


class SafariTopSitesExtractor(BaseExtractor):
    """Extract Safari TopSites.plist artifacts from macOS evidence."""

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="safari_top_sites",
            display_name="Safari Top Sites",
            description="Extract Safari TopSites.plist frequently visited sites",
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
            return QLabel("Safari Top Sites\nNo extraction run yet", parent)
        try:
            manifest = json.loads(manifest_path.read_text())
            file_count = len(manifest.get("files", []))
            parsed = manifest.get("parsed_counts", {})
            text = (
                "Safari Top Sites\n"
                f"Run ID: {manifest.get('run_id', 'N/A')}\n"
                f"TopSites.plist Files: {file_count}\n"
                f"Top Sites Parsed: {parsed.get('total_sites', 0)}\n"
                f"Unique URLs: {parsed.get('unique_urls', 0)}"
            )
        except Exception:
            text = "Safari Top Sites\nFailed to read manifest"
        return QLabel(text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        return case_root / "evidences" / evidence_label / "safari_top_sites"

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
        callbacks.on_step("Initializing Safari top sites extraction")
        LOGGER.info("Starting Safari top sites extraction (run_id=%s)", run_id)

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

        callbacks.on_step("Discovering Safari top sites artifacts")
        discovered = self._discover_top_sites_paths(evidence_fs)
        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=len(discovered))

        if not discovered:
            manifest_data["status"] = "skipped"
            manifest_data["notes"].append("No Safari TopSites.plist files found")
        else:
            for source_path in discovered:
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    manifest_data["notes"].append("Extraction cancelled by user")
                    break
                file_info = self._extract_file(evidence_fs, source_path, run_dir, output_dir)
                if file_info:
                    manifest_data["files"].append(file_info)
                    callbacks.on_log(f"Copied: {source_path}", "info")

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

        callbacks.on_step("Safari top sites extraction complete")
        return manifest_data["status"] not in {"error", "cancelled"}

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, int]:
        from core.statistics_collector import StatisticsCollector

        counts = {"top_sites": 0, "urls": 0, "built_in": 0}
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
                collector.report_ingested(evidence_id, self.metadata.name, records=0, top_sites=0, urls=0)
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            return counts

        callbacks.on_step("Initializing Safari top sites ingestion")
        delete_top_sites_by_run(evidence_conn, evidence_id, run_id)
        delete_urls_by_run(evidence_conn, evidence_id, run_id)

        parsed_sites: List[SafariTopSite] = []
        top_site_records: List[Dict[str, Any]] = []
        url_records: List[Dict[str, Any]] = []

        for file_info in files:
            if file_info.get("artifact_type") != "top_sites_plist":
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
                artifact_type="top_sites",
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
                sites = parse_top_sites(local_path_obj)
                parsed_sites.extend(sites)

                urls_parsed = 0
                for site in sites:
                    top_site_records.append(
                        {
                            "browser": "safari",
                            "profile": profile,
                            "url": site.url,
                            "title": site.title,
                            "url_rank": site.rank,
                            "run_id": run_id,
                            "source_path": source_path,
                            "partition_index": file_info.get("partition_index"),
                            "fs_type": file_info.get("fs_type"),
                            "logical_path": source_path,
                            "forensic_path": source_path,
                            "notes": "built-in" if site.is_built_in else None,
                        }
                    )

                    url_record = self._make_url_record(
                        url=site.url,
                        run_id=run_id,
                        source_path=source_path,
                        profile=profile,
                    )
                    if url_record:
                        url_records.append(url_record)
                        urls_parsed += 1

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    urls_parsed=urls_parsed,
                    records_parsed=len(sites),
                )
            except Exception as exc:
                LOGGER.error("Failed to parse %s: %s", local_path_obj, exc, exc_info=True)
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="error",
                    notes=str(exc),
                )

        if top_site_records:
            counts["top_sites"] = insert_top_sites(evidence_conn, evidence_id, top_site_records)
        if url_records:
            counts["urls"] = insert_urls(evidence_conn, evidence_id, url_records)

        site_stats = get_top_site_stats(parsed_sites)
        counts["built_in"] = site_stats.get("built_in_count", 0)

        manifest["parsed_counts"] = {
            **site_stats,
            "urls_cross_posted": counts["urls"],
        }
        manifest_path.write_text(json.dumps(manifest, indent=2))

        evidence_conn.commit()
        callbacks.on_step("Safari top sites ingestion complete")

        if collector:
            collector.report_ingested(
                evidence_id,
                self.metadata.name,
                records=counts["top_sites"],
                top_sites=counts["top_sites"],
                urls=counts["urls"],
            )
            status = "success" if counts["top_sites"] > 0 else "partial"
            collector.finish_run(evidence_id, self.metadata.name, status=status)

        return counts

    def _discover_top_sites_paths(self, evidence_fs) -> List[str]:
        discovered: List[str] = []
        seen: set[str] = set()
        for pattern in get_patterns("top_sites"):
            try:
                for path_str in evidence_fs.iter_paths(pattern):
                    if path_str in seen:
                        continue
                    seen.add(path_str)
                    discovered.append(path_str)
            except Exception as exc:
                LOGGER.debug("Safari top sites pattern failed (%s): %s", pattern, exc)
        return discovered

    def _extract_file(
        self,
        evidence_fs,
        source_path: str,
        run_dir: Path,
        output_dir: Path,
    ) -> Optional[Dict[str, Any]]:
        try:
            content = evidence_fs.read_file(source_path)
        except Exception:
            return None

        user = extract_user_from_path(source_path) or "unknown"
        profile = self._extract_profile(source_path, user=user)
        base_name = f"safari_{_safe_slug(profile)}_TopSites.plist"
        dest_path = run_dir / base_name
        if dest_path.exists():
            suffix = hashlib.sha1(source_path.encode("utf-8", errors="ignore")).hexdigest()[:8]
            dest_path = run_dir / f"safari_{_safe_slug(profile)}_{suffix}_TopSites.plist"
        dest_path.write_bytes(content)

        extracted_path = str(dest_path.relative_to(output_dir.parent))
        return {
            "local_path": str(dest_path),
            "extracted_path": extracted_path,
            "source_path": source_path,
            "artifact_type": "top_sites_plist",
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
    def _make_url_record(
        *,
        url: str,
        run_id: str,
        source_path: str,
        profile: str,
    ) -> Optional[Dict[str, Any]]:
        candidate = (url or "").strip()
        if not candidate:
            return None

        lowered = candidate.lower()
        if lowered.startswith(("about:", "safari-", "javascript:", "data:", "topsites://")):
            return None

        parsed = urlparse(candidate)
        return {
            "url": candidate,
            "domain": parsed.netloc or None,
            "scheme": parsed.scheme or None,
            "discovered_by": "safari_top_sites",
            "run_id": run_id,
            "source_path": source_path,
            "context": f"top_sites:safari:{profile}",
        }

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
