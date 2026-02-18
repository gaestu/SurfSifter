"""Ingestion handler for Safari cache extraction runs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from core.database import (
    delete_discoveries_by_run,
    delete_extraction_warnings_by_run,
    delete_urls_by_run,
    insert_browser_inventory,
    insert_image_with_discovery,
    insert_urls,
    update_inventory_ingestion_status,
)
from core.logging import get_logger
from extractors._shared.extraction_warnings import (
    CATEGORY_BINARY,
    SEVERITY_INFO,
    ExtractionWarningCollector,
)

from ._blob_parser import parse_request_object, parse_response_object
from ._image_carver import CarvedImage, carve_image_from_cache_entry, carve_orphan_images
from ._parser import get_cache_db_columns, get_cache_db_tables, parse_cache_db
from ._schemas import (
    KNOWN_COLUMNS_BLOB_DATA,
    KNOWN_COLUMNS_RECEIVER_DATA,
    KNOWN_COLUMNS_RESPONSE,
    KNOWN_CONTENT_TYPES,
    KNOWN_HTTP_REQUEST_METHODS,
    KNOWN_HTTP_RESPONSE_HEADERS,
    KNOWN_STORAGE_POLICIES,
    KNOWN_TABLES,
)

LOGGER = get_logger("extractors.browser.safari.cache.ingestion")


class CacheIngestionHandler:
    """Handles Load + Transform phases for Safari cache ingestion."""

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
        stats = {
            "entries_parsed": 0,
            "urls_inserted": 0,
            "images_inserted": 0,
            "inventory_entries": 0,
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

        callbacks.on_step("Initializing Safari cache ingestion")
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.extractor_name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        self._clear_previous_run(evidence_conn, evidence_id, run_id)
        files = manifest.get("files", [])
        cache_db_entries = [f for f in files if f.get("artifact_type") == "cache_db" and _is_cache_db_file(f)]

        for file_entry in cache_db_entries:
            local_path = _resolve_local_path(file_entry, run_dir)
            if not local_path or not local_path.exists():
                continue

            source_path = file_entry.get("source_path", "")
            profile = file_entry.get("user") or "Default"
            inventory_id = insert_browser_inventory(
                evidence_conn,
                evidence_id=evidence_id,
                browser="safari",
                artifact_type="cache_safari",
                run_id=run_id,
                extracted_path=str(local_path),
                extraction_status="ok",
                extraction_timestamp_utc=manifest.get("extraction_timestamp_utc", ""),
                logical_path=source_path or str(local_path),
                profile=profile,
                partition_index=file_entry.get("partition_index"),
                fs_type=file_entry.get("fs_type"),
                forensic_path=source_path or str(local_path),
                extraction_tool=f"{self.extractor_name}:{self.extractor_version}",
                file_size_bytes=file_entry.get("size_bytes"),
                file_md5=file_entry.get("md5"),
                file_sha256=file_entry.get("sha256"),
            )
            stats["inventory_entries"] += 1

            try:
                callbacks.on_step(f"Parsing {local_path.name}")
                self._collect_schema_warnings(local_path, source_path or str(local_path), warning_collector)

                entries = parse_cache_db(local_path)
                stats["entries_parsed"] += len(entries)

                cache_group_dir = local_path.parent
                fscached_dir = cache_group_dir / "fsCachedData"
                known_entry_ids = set()
                url_records: List[Dict[str, Any]] = []

                for entry in entries:
                    known_entry_ids.add(entry.entry_id)

                    response_meta = parse_response_object(entry.response_blob or b"")
                    request_meta = parse_request_object(entry.request_blob or b"")

                    if entry.storage_policy not in KNOWN_STORAGE_POLICIES:
                        warning_collector.add_warning(
                            warning_type="unknown_storage_policy",
                            category="database",
                            severity=SEVERITY_INFO,
                            artifact_type="cache_safari",
                            source_file=source_path or str(local_path),
                            item_name="storage_policy",
                            item_value=str(entry.storage_policy),
                        )

                    if request_meta and request_meta.http_method:
                        if request_meta.http_method.upper() not in KNOWN_HTTP_REQUEST_METHODS:
                            warning_collector.add_warning(
                                warning_type="unknown_request_method",
                                category=CATEGORY_BINARY,
                                severity=SEVERITY_INFO,
                                artifact_type="cache_safari",
                                source_file=source_path or str(local_path),
                                item_name="method",
                                item_value=request_meta.http_method,
                            )

                    if response_meta:
                        self._collect_header_warnings(
                            response_meta.all_headers,
                            source_path or str(local_path),
                            warning_collector,
                        )
                        if response_meta.content_type:
                            base_ct = response_meta.content_type.split(";", 1)[0].strip().lower()
                            if base_ct and base_ct not in KNOWN_CONTENT_TYPES:
                                warning_collector.add_warning(
                                    warning_type="unknown_content_type",
                                    category=CATEGORY_BINARY,
                                    severity=SEVERITY_INFO,
                                    artifact_type="cache_safari",
                                    source_file=source_path or str(local_path),
                                    item_name="Content-Type",
                                    item_value=response_meta.content_type,
                                )

                    url_record = _make_url_record(
                        entry=entry,
                        source_path=source_path or str(local_path),
                        discovered_by=self.extractor_name,
                        run_id=run_id,
                        profile=profile,
                        response_meta=response_meta,
                    )
                    if url_record:
                        url_records.append(url_record)

                    carved = carve_image_from_cache_entry(entry, response_meta, run_dir, fscached_dir)
                    if carved:
                        self._insert_carved_image(
                            evidence_conn=evidence_conn,
                            evidence_id=evidence_id,
                            run_id=run_id,
                            carved=carved,
                            entry_context={
                                "entry_id": entry.entry_id,
                                "partition": entry.partition,
                                "timestamp_utc": entry.timestamp_utc,
                                "http_status": response_meta.http_status if response_meta else None,
                            },
                        )
                        stats["images_inserted"] += 1

                orphan_images = carve_orphan_images(fscached_dir, known_entry_ids, run_dir)
                for orphan in orphan_images:
                    self._insert_carved_image(
                        evidence_conn=evidence_conn,
                        evidence_id=evidence_id,
                        run_id=run_id,
                        carved=orphan,
                        entry_context={"source": "orphan"},
                    )
                    stats["images_inserted"] += 1

                if url_records:
                    stats["urls_inserted"] += insert_urls(evidence_conn, evidence_id, url_records)

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id=inventory_id,
                    status="ok",
                    urls_parsed=len(url_records),
                    records_parsed=len(entries),
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

        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count:
            stats["warnings"] = warning_count
        evidence_conn.commit()
        callbacks.on_step("Safari cache ingestion complete")
        return stats

    def _find_latest_manifest(self, output_dir: Path) -> Optional[Path]:
        manifests = sorted(output_dir.glob("*/manifest.json"))
        if manifests:
            return manifests[-1]
        fallback = output_dir / "manifest.json"
        if fallback.exists():
            return fallback
        return None

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        delete_urls_by_run(evidence_conn, evidence_id, run_id)
        delete_discoveries_by_run(evidence_conn, evidence_id, run_id)
        delete_extraction_warnings_by_run(
            evidence_conn,
            evidence_id,
            self.extractor_name,
            run_id,
        )
        evidence_conn.execute(
            "DELETE FROM browser_cache_inventory WHERE evidence_id = ? AND run_id = ?",
            (evidence_id, run_id),
        )

    def _collect_schema_warnings(
        self,
        db_path: Path,
        source_file: str,
        warning_collector: ExtractionWarningCollector,
    ) -> None:
        tables = get_cache_db_tables(db_path)
        unknown_tables = sorted(tables - KNOWN_TABLES)
        for table_name in unknown_tables:
            columns = sorted(get_cache_db_columns(db_path, table_name))
            warning_collector.add_unknown_table(
                table_name=table_name,
                columns=columns,
                source_file=source_file,
                artifact_type="cache_safari",
            )

        column_map = {
            "cfurl_cache_response": KNOWN_COLUMNS_RESPONSE,
            "cfurl_cache_blob_data": KNOWN_COLUMNS_BLOB_DATA,
            "cfurl_cache_receiver_data": KNOWN_COLUMNS_RECEIVER_DATA,
        }
        for table_name, known_columns in column_map.items():
            if table_name not in tables:
                continue
            for column_name in sorted(get_cache_db_columns(db_path, table_name) - known_columns):
                warning_collector.add_unknown_column(
                    table_name=table_name,
                    column_name=column_name,
                    column_type="UNKNOWN",
                    source_file=source_file,
                    artifact_type="cache_safari",
                )

    def _collect_header_warnings(
        self,
        headers: Dict[str, str],
        source_file: str,
        warning_collector: ExtractionWarningCollector,
    ) -> None:
        known_lower = {h.lower() for h in KNOWN_HTTP_RESPONSE_HEADERS}
        for header_name, value in headers.items():
            if header_name.lower() in known_lower:
                continue
            warning_collector.add_warning(
                warning_type="unknown_http_header",
                category=CATEGORY_BINARY,
                severity=SEVERITY_INFO,
                artifact_type="cache_safari",
                source_file=source_file,
                item_name=header_name,
                item_value=(value or "")[:200],
            )

    def _insert_carved_image(
        self,
        *,
        evidence_conn,
        evidence_id: int,
        run_id: str,
        carved: CarvedImage,
        entry_context: Dict[str, Any],
    ) -> None:
        image_data = {
            "rel_path": carved.rel_path,
            "filename": carved.filename,
            "md5": carved.md5,
            "sha256": carved.sha256,
            "phash": carved.phash,
            "size_bytes": carved.size_bytes,
            "notes": f"Carved from Safari cache ({carved.source_type})",
        }
        source_context = {
            "source_type": carved.source_type,
            "source_url": carved.source_url,
            "entry_id": carved.source_entry_id,
            "content_type": carved.content_type,
            **entry_context,
        }
        discovery_data = {
            "discovered_by": self.extractor_name,
            "run_id": run_id,
            "extractor_version": self.extractor_version,
            "cache_url": carved.source_url,
            "cache_key": str(carved.source_entry_id) if carved.source_entry_id is not None else None,
            "cache_filename": carved.filename,
            "cache_response_time": entry_context.get("timestamp_utc"),
            "source_metadata_json": json.dumps(source_context),
        }
        insert_image_with_discovery(evidence_conn, evidence_id, image_data, discovery_data)


def _is_cache_db_file(file_entry: Dict[str, Any]) -> bool:
    source_path = str(file_entry.get("source_path", ""))
    local_path = str(file_entry.get("local_path", ""))
    return source_path.endswith("Cache.db") or local_path.endswith("Cache.db")


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


def _make_url_record(
    *,
    entry,
    source_path: str,
    discovered_by: str,
    run_id: str,
    profile: str,
    response_meta,
) -> Optional[Dict[str, Any]]:
    url = (entry.url or "").strip()
    if not url:
        return None

    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https"}:
        return None

    context = {
        "entry_id": entry.entry_id,
        "storage_policy": entry.storage_policy,
        "partition": entry.partition,
        "http_status": response_meta.http_status if response_meta else None,
        "content_type": response_meta.content_type if response_meta else None,
        "profile": profile,
    }
    return {
        "url": url,
        "domain": parsed.hostname,
        "scheme": scheme,
        "discovered_by": discovered_by,
        "run_id": run_id,
        "source_path": source_path,
        "context": json.dumps(context),
        "first_seen_utc": entry.timestamp_utc,
        "last_seen_utc": entry.timestamp_utc,
        "cache_key": str(entry.entry_id),
        "cache_filename": "Cache.db",
        "response_code": response_meta.http_status if response_meta else None,
        "content_type": response_meta.content_type if response_meta else None,
    }
