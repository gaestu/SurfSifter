"""Ingestion handler for Safari favicons extraction runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from core.database import (
    delete_discoveries_by_run,
    delete_extraction_warnings_by_run,
    delete_favicon_mappings_by_run,
    delete_favicons_by_run,
    delete_urls_by_run,
    insert_browser_inventory,
    insert_favicon,
    insert_favicon_mappings,
    insert_image_with_discovery,
    insert_urls,
    update_inventory_ingestion_status,
)
from core.logging import get_logger
from extractors._shared.extraction_warnings import ExtractionWarningCollector

from .._parsers import cocoa_to_iso
from ._parser import SafariIconRecord, parse_favicons_db
from ._schemas import ICON_TYPE_FAVICON, ICON_TYPE_MASK_ICON, ICON_TYPE_TOUCH_ICON
from ._touch_icons import ParsedIconFile, parse_icon_file

LOGGER = get_logger("extractors.browser.safari.favicons.ingestion")
MIN_ICON_SIZE_FOR_IMAGES = 64


class FaviconsIngestionHandler:
    """Load + Transform phases for Safari favicons ingestion."""

    def __init__(self, extractor_name: str, extractor_version: str):
        self.extractor_name = extractor_name
        self.extractor_version = extractor_version

    def run(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks,
    ) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "db_icons_parsed": 0,
            "favicons_inserted": 0,
            "mappings_inserted": 0,
            "urls_inserted": 0,
            "images_inserted": 0,
            "inventory_entries": 0,
            "warnings": 0,
            "errors": [],
        }

        manifest_path = self._find_latest_manifest(output_dir)
        if not manifest_path:
            stats["errors"].append("No extraction manifests found")
            return stats

        manifest = json.loads(manifest_path.read_text())
        run_dir = manifest_path.parent
        run_id = manifest.get("run_id", "")
        if not run_id:
            stats["errors"].append("Manifest missing run_id")
            return stats
        if manifest.get("status", "ok") != "ok":
            stats["errors"].append(f"Extraction status is {manifest.get('status')!r}, cannot ingest")
            return stats

        callbacks.on_step("Initializing Safari favicons ingestion")
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.extractor_name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        files = manifest.get("files", [])
        if not files:
            return stats

        cache_index = self._build_cache_file_index(files)
        url_records: List[Dict[str, Any]] = []
        mapping_records: List[Dict[str, Any]] = []

        db_entries = [f for f in files if f.get("artifact_type") == "favicons_db"]
        for file_entry in db_entries:
            local_path = _resolve_local_path(file_entry, run_dir)
            if not local_path or not local_path.exists():
                continue

            profile = _profile_from_entry(file_entry)
            source_path = file_entry.get("source_path") or str(local_path)
            inventory_id = insert_browser_inventory(
                evidence_conn,
                evidence_id=evidence_id,
                browser="safari",
                artifact_type="favicons_safari",
                run_id=run_id,
                extracted_path=str(local_path),
                extraction_status="ok",
                extraction_timestamp_utc=manifest.get("extraction_timestamp_utc", ""),
                logical_path=source_path,
                profile=profile,
                partition_index=file_entry.get("partition_index"),
                fs_type=file_entry.get("fs_type"),
                forensic_path=source_path,
                extraction_tool=f"{self.extractor_name}:{self.extractor_version}",
                file_size_bytes=file_entry.get("size_bytes"),
                file_md5=file_entry.get("md5"),
                file_sha256=file_entry.get("sha256"),
            )
            stats["inventory_entries"] += 1

            try:
                callbacks.on_step(f"Parsing {local_path.name}")
                icons, page_mappings = parse_favicons_db(
                    local_path,
                    warning_collector=warning_collector,
                    source_file=source_path,
                )
                stats["db_icons_parsed"] += len(icons)

                records_parsed = 0
                urls_parsed = 0
                for icon in icons:
                    parsed_file, cache_file_entry = self._find_matching_cache_file(
                        icon=icon,
                        profile=profile,
                        cache_index=cache_index,
                        run_dir=run_dir,
                    )
                    favicon_id = self._insert_db_icon(
                        evidence_conn=evidence_conn,
                        evidence_id=evidence_id,
                        run_id=run_id,
                        icon=icon,
                        profile=profile,
                        source_path=source_path,
                        db_file_entry=file_entry,
                        parsed_file=parsed_file,
                        cache_file_entry=cache_file_entry,
                    )
                    if not favicon_id:
                        continue

                    stats["favicons_inserted"] += 1
                    records_parsed += 1

                    icon_url_record = _make_url_record(
                        url=icon.icon_url,
                        discovered_by=self.extractor_name,
                        run_id=run_id,
                        source_path=source_path,
                        context=f"favicon:safari:icon:{profile}",
                        first_seen_utc=cocoa_to_iso(icon.timestamp),
                    )
                    if icon_url_record:
                        url_records.append(icon_url_record)
                        urls_parsed += 1

                    for page_url in page_mappings.get(icon.uuid, []):
                        mapping_records.append(
                            {
                                "favicon_id": favicon_id,
                                "page_url": page_url,
                                "browser": "safari",
                                "profile": profile,
                                "run_id": run_id,
                            }
                        )
                        page_url_record = _make_url_record(
                            url=page_url,
                            discovered_by=self.extractor_name,
                            run_id=run_id,
                            source_path=source_path,
                            context=f"favicon:safari:page:{profile}",
                            first_seen_utc=cocoa_to_iso(icon.timestamp),
                        )
                        if page_url_record:
                            url_records.append(page_url_record)
                            urls_parsed += 1

                    if parsed_file and _should_cross_post(parsed_file.width, parsed_file.height):
                        if self._insert_icon_image(
                            evidence_conn=evidence_conn,
                            evidence_id=evidence_id,
                            run_id=run_id,
                            parsed_file=parsed_file,
                            icon_url=icon.icon_url,
                            profile=profile,
                            source_path=cache_file_entry.get("source_path") if cache_file_entry else source_path,
                            extracted_path=cache_file_entry.get("extracted_path") if cache_file_entry else None,
                            icon_label="favicon",
                        ):
                            stats["images_inserted"] += 1

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    urls_parsed=urls_parsed,
                    records_parsed=records_parsed,
                )
            except Exception as exc:
                LOGGER.error("Failed to ingest %s: %s", local_path, exc, exc_info=True)
                stats["errors"].append(f"{local_path}: {exc}")
                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="failed",
                    notes=str(exc),
                )

        # Touch and template icon files are independent cache artifacts.
        stats["favicons_inserted"] += self._ingest_icon_cache_files(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            files=files,
            run_dir=run_dir,
            run_id=run_id,
            icon_type=ICON_TYPE_TOUCH_ICON,
            artifact_type="touch_icon_file",
            icon_label="touch_icon",
            warning_collector=warning_collector,
            image_counter=stats,
            url_records=url_records,
        )
        stats["favicons_inserted"] += self._ingest_icon_cache_files(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            files=files,
            run_dir=run_dir,
            run_id=run_id,
            icon_type=ICON_TYPE_MASK_ICON,
            artifact_type="template_icon_file",
            icon_label="mask_icon",
            warning_collector=warning_collector,
            image_counter=stats,
            url_records=url_records,
        )

        if mapping_records:
            stats["mappings_inserted"] = insert_favicon_mappings(
                evidence_conn, evidence_id, mapping_records
            )

        if url_records:
            stats["urls_inserted"] = insert_urls(evidence_conn, evidence_id, url_records)

        warning_count = warning_collector.flush_to_database(evidence_conn)
        stats["warnings"] = warning_count
        evidence_conn.commit()
        callbacks.on_step("Safari favicons ingestion complete")
        return stats

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        delete_favicon_mappings_by_run(evidence_conn, evidence_id, run_id)
        delete_favicons_by_run(evidence_conn, evidence_id, run_id)
        delete_urls_by_run(evidence_conn, evidence_id, run_id)
        delete_discoveries_by_run(evidence_conn, evidence_id, run_id)
        delete_extraction_warnings_by_run(
            evidence_conn,
            evidence_id,
            self.extractor_name,
            run_id,
        )

    def _find_matching_cache_file(
        self,
        *,
        icon: SafariIconRecord,
        profile: str,
        cache_index: Dict[tuple[str, str], List[Dict[str, Any]]],
        run_dir: Path,
    ) -> tuple[Optional[ParsedIconFile], Optional[Dict[str, Any]]]:
        candidates = cache_index.get((profile, icon.uuid), [])
        for entry in candidates:
            local = _resolve_local_path(entry, run_dir)
            if not local or not local.exists():
                continue
            parsed = parse_icon_file(local, ICON_TYPE_FAVICON, icon_url=icon.icon_url)
            if parsed:
                return parsed, entry
        return None, None

    def _insert_db_icon(
        self,
        *,
        evidence_conn,
        evidence_id: int,
        run_id: str,
        icon: SafariIconRecord,
        profile: str,
        source_path: str,
        db_file_entry: Dict[str, Any],
        parsed_file: Optional[ParsedIconFile],
        cache_file_entry: Optional[Dict[str, Any]],
    ) -> int:
        width = parsed_file.width if parsed_file and parsed_file.width else icon.width
        height = parsed_file.height if parsed_file and parsed_file.height else icon.height
        notes = None
        if cache_file_entry and cache_file_entry.get("source_path"):
            notes = f"Icon cache file: {cache_file_entry.get('source_path')}"
        return insert_favicon(
            evidence_conn,
            evidence_id=evidence_id,
            browser="safari",
            icon_url=icon.icon_url,
            profile=profile,
            icon_md5=parsed_file.md5 if parsed_file else None,
            icon_sha256=parsed_file.sha256 if parsed_file else None,
            icon_type=ICON_TYPE_FAVICON,
            width=width,
            height=height,
            last_updated_utc=cocoa_to_iso(icon.timestamp),
            run_id=run_id,
            source_path=source_path,
            partition_index=db_file_entry.get("partition_index"),
            fs_type=db_file_entry.get("fs_type"),
            logical_path=source_path,
            forensic_path=source_path,
            notes=notes,
        )

    def _ingest_icon_cache_files(
        self,
        *,
        evidence_conn,
        evidence_id: int,
        files: List[Dict[str, Any]],
        run_dir: Path,
        run_id: str,
        icon_type: int,
        artifact_type: str,
        icon_label: str,
        warning_collector: ExtractionWarningCollector,
        image_counter: Dict[str, Any],
        url_records: List[Dict[str, Any]],
    ) -> int:
        inserted = 0
        for file_entry in files:
            if file_entry.get("artifact_type") != artifact_type:
                continue
            local_path = _resolve_local_path(file_entry, run_dir)
            if not local_path or not local_path.exists():
                continue

            source_path = file_entry.get("source_path") or str(local_path)
            parsed = parse_icon_file(local_path, icon_type)
            if not parsed:
                continue

            profile = _profile_from_entry(file_entry)
            icon_url = _source_path_to_file_url(source_path)
            try:
                insert_favicon(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser="safari",
                    icon_url=icon_url,
                    profile=profile,
                    icon_md5=parsed.md5,
                    icon_sha256=parsed.sha256,
                    icon_type=icon_type,
                    width=parsed.width,
                    height=parsed.height,
                    run_id=run_id,
                    source_path=source_path,
                    partition_index=file_entry.get("partition_index"),
                    fs_type=file_entry.get("fs_type"),
                    logical_path=source_path,
                    forensic_path=source_path,
                    notes=f"{icon_label} cache file",
                )
                inserted += 1
            except Exception as exc:
                warning_collector.add_warning(
                    warning_type="file_corrupt",
                    category="binary",
                    severity="warning",
                    artifact_type="favicons",
                    source_file=source_path,
                    item_name=local_path.name,
                    item_value=str(exc),
                )
                continue

            # Only add valid web URLs to urls table.
            if parsed.icon_url:
                record = _make_url_record(
                    url=parsed.icon_url,
                    discovered_by=self.extractor_name,
                    run_id=run_id,
                    source_path=source_path,
                    context=f"favicon:safari:{icon_label}:{profile}",
                )
                if record:
                    url_records.append(record)

            if _should_cross_post(parsed.width, parsed.height):
                if self._insert_icon_image(
                    evidence_conn=evidence_conn,
                    evidence_id=evidence_id,
                    run_id=run_id,
                    parsed_file=parsed,
                    icon_url=None,
                    profile=profile,
                    source_path=source_path,
                    extracted_path=file_entry.get("extracted_path"),
                    icon_label=icon_label,
                ):
                    image_counter["images_inserted"] += 1

        return inserted

    def _insert_icon_image(
        self,
        *,
        evidence_conn,
        evidence_id: int,
        run_id: str,
        parsed_file: ParsedIconFile,
        icon_url: Optional[str],
        profile: str,
        source_path: str,
        extracted_path: Optional[str],
        icon_label: str,
    ) -> bool:
        try:
            rel_path = extracted_path or f"safari_favicons/{parsed_file.sha256[:2]}/{parsed_file.path.name}"
            image_data = {
                "rel_path": rel_path,
                "filename": Path(rel_path).name,
                "md5": parsed_file.md5,
                "sha256": parsed_file.sha256,
                "phash": parsed_file.phash,
                "size_bytes": parsed_file.size_bytes,
                "notes": f"Safari {icon_label} ({profile})",
            }
            discovery_data = {
                "discovered_by": self.extractor_name,
                "run_id": run_id,
                "extractor_version": self.extractor_version,
                "cache_url": icon_url,
                "cache_filename": parsed_file.path.name,
                "source_metadata_json": {
                    "browser": "safari",
                    "profile": profile,
                    "icon_label": icon_label,
                    "source_path": source_path,
                    "file_type": parsed_file.file_type,
                },
            }
            insert_image_with_discovery(evidence_conn, evidence_id, image_data, discovery_data)
            return True
        except Exception:
            return False

    def _build_cache_file_index(self, files: List[Dict[str, Any]]) -> Dict[tuple[str, str], List[Dict[str, Any]]]:
        """
        Build lookup map for favicon cache binaries by (profile, uuid-candidate).

        Safari commonly stores icon files named by UUID; we use both filename and stem.
        """
        index: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        for file_entry in files:
            if file_entry.get("artifact_type") != "favicon_cache_file":
                continue
            profile = _profile_from_entry(file_entry)
            source_path = str(file_entry.get("source_path") or "")
            name = Path(source_path).name
            if not name:
                continue
            for token in {name, Path(name).stem}:
                token = token.strip()
                if not token:
                    continue
                index.setdefault((profile, token), []).append(file_entry)
        return index

    @staticmethod
    def _find_latest_manifest(output_dir: Path) -> Optional[Path]:
        manifests = sorted(output_dir.glob("*/manifest.json"))
        if manifests:
            return manifests[-1]
        fallback = output_dir / "manifest.json"
        if fallback.exists():
            return fallback
        return None


def _profile_from_entry(file_entry: Dict[str, Any]) -> str:
    profile = str(file_entry.get("profile") or "").strip()
    if profile:
        return profile
    user = str(file_entry.get("user") or "").strip()
    return user or "Default"


def _resolve_local_path(file_entry: Dict[str, Any], run_dir: Path) -> Optional[Path]:
    local = file_entry.get("local_path")
    if local:
        candidate = Path(local)
        if candidate.exists():
            return candidate
    extracted = file_entry.get("extracted_path")
    if extracted:
        candidate = run_dir.parent / extracted
        if candidate.exists():
            return candidate
        candidate = run_dir / extracted
        if candidate.exists():
            return candidate
    return None


def _source_path_to_file_url(source_path: str) -> str:
    normalized = source_path if source_path.startswith("/") else f"/{source_path}"
    return f"file://{normalized}"


def _make_url_record(
    *,
    url: str,
    discovered_by: str,
    run_id: str,
    source_path: str,
    context: str,
    first_seen_utc: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme in {"javascript", "data"}:
        return None
    return {
        "url": url,
        "domain": parsed.netloc or None,
        "scheme": scheme or None,
        "discovered_by": discovered_by,
        "run_id": run_id,
        "source_path": source_path,
        "context": context,
        "first_seen_utc": first_seen_utc,
    }


def _should_cross_post(width: Optional[int], height: Optional[int]) -> bool:
    if width is None and height is None:
        return False
    return max(width or 0, height or 0) >= MIN_ICON_SIZE_FOR_IMAGES
