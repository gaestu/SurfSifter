"""
Safari Browser Storage Extractor.

Extracts LocalStorage and IndexedDB from Safari on macOS.

Features:
- LocalStorage: Per-origin .localstorage SQLite files (ItemTable with UTF-16LE)
- IndexedDB: WebKit-specific SQLite files (Records, ObjectStoreInfo, etc.)
- Multi-partition discovery via file_list SQL queries
- Modern WebsiteData path support (Safari 10+)
- Schema warnings for unknown tables/columns
- Deep value analysis (URLs, tokens, emails, identifiers)
- StatisticsCollector integration for run tracking

Data Sources:
- LocalStorage (legacy): ~/Library/Safari/LocalStorage/*.localstorage
- LocalStorage (modern): ~/Library/WebKit/.../WebsiteData/LocalStorage/
- IndexedDB (legacy): ~/Library/Safari/Databases/___IndexedDB/v*/
- IndexedDB (modern): ~/Library/WebKit/.../WebsiteData/IndexedDB/v*/

Note: Safari support is EXPERIMENTAL.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import MultiPartitionWidget
from ...._shared.file_list_discovery import (
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from .._patterns import extract_user_from_path
from ._discovery import discover_storage_files, extract_storage_file
from ._parsers import parse_safari_localstorage, parse_safari_indexeddb

from core.logging import get_logger
from core.database import (
    insert_urls,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    insert_local_storages,
    delete_local_storage_by_run,
    insert_indexeddb_database,
    insert_indexeddb_entries,
    delete_indexeddb_databases_by_run,
    delete_indexeddb_entries_by_run,
    insert_storage_tokens,
    delete_storage_tokens_by_run,
    insert_storage_identifiers,
    delete_storage_identifiers_by_run,
)

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.browser.safari.storage")


class SafariStorageExtractor(BaseExtractor):
    """
    Extract Safari browser web storage from macOS evidence.

    Parses LocalStorage (.localstorage SQLite) and IndexedDB
    (WebKit SQLite) artifacts with multi-partition support and
    optional deep value analysis.

    Note: Safari support is EXPERIMENTAL.
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="safari_storage",
            display_name="Safari Browser Storage (macOS)",
            description=(
                "Extract LocalStorage and IndexedDB from Safari "
                "with deep value analysis — EXPERIMENTAL"
            ),
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
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
        """Return configuration widget for multi-partition support."""
        return MultiPartitionWidget(parent)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
    ) -> QWidget:
        """Return status widget showing extraction results."""
        manifest_path = output_dir / "manifest.json"

        if manifest_path.exists():
            try:
                data = json.loads(manifest_path.read_text())
                locations = len(data.get("storage_locations", []))
                parsed = data.get("parsed_counts", {})
                ls_count = parsed.get("local_storage", 0)
                idb_count = parsed.get("indexeddb", 0)
                status_text = (
                    f"Safari Storage (EXPERIMENTAL)\n"
                    f"Locations: {locations}\n"
                    f"LocalStorage records: {ls_count}\n"
                    f"IndexedDB entries: {idb_count}\n"
                    f"Run ID: {data.get('run_id', 'N/A')}"
                )
            except Exception:
                status_text = "Safari Storage - Error reading manifest"
        else:
            status_text = "Safari Storage (EXPERIMENTAL)\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Return output directory for Safari storage extraction."""
        return case_root / "evidences" / evidence_label / "safari_storage"

    # ─────────────────────────────────────────────────────────────────
    # Extraction
    # ─────────────────────────────────────────────────────────────────

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        """
        Extract Safari storage files from evidence.

        Discovers and copies all LocalStorage and IndexedDB files,
        along with their WAL/journal companions.
        """
        from core.statistics_collector import StatisticsCollector

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        # Start statistics tracking
        collector = StatisticsCollector.get_instance()
        if collector:
            collector.start_run(
                evidence_id, evidence_label, self.metadata.name, run_id
            )

        callbacks.on_step("Initializing Safari storage extraction")
        LOGGER.info("Starting Safari storage extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data: Dict[str, Any] = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "e01_context": self._get_e01_context(evidence_fs),
            "storage_locations": [],
            "partitions_scanned": [],
            "config": {
                "local_storage": config.get("local_storage", True),
                "indexeddb": config.get("indexeddb", True),
            },
            "status": "ok",
            "notes": ["Safari support is EXPERIMENTAL"],
        }

        # Discover storage files across partitions
        callbacks.on_step("Discovering Safari storage files (multi-partition)")

        files_by_partition = discover_storage_files(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            evidence_fs=evidence_fs,
            config=config,
            callbacks=callbacks,
        )

        # Flatten for extraction
        all_locations: List[Dict[str, Any]] = []
        for partition_idx, locations in files_by_partition.items():
            manifest_data["partitions_scanned"].append(partition_idx)
            all_locations.extend(locations)

        # Report discovered count
        if collector:
            collector.report_discovered(
                evidence_id, self.metadata.name, files=len(all_locations)
            )

        if not all_locations:
            manifest_data["notes"].append("No Safari storage files found")
            LOGGER.info("No Safari storage found")

            if collector:
                collector.finish_run(
                    evidence_id, self.metadata.name, status="skipped"
                )
        else:
            callbacks.on_step(
                f"Extracting {len(all_locations)} Safari storage files"
            )

            ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
            current_partition = getattr(evidence_fs, "partition_index", 0)
            multi = len(files_by_partition) > 1

            for partition_idx in sorted(files_by_partition.keys()):
                partition_files = files_by_partition[partition_idx]

                if ewf_paths is not None and partition_idx != current_partition:
                    ctx = open_partition_for_extraction(ewf_paths, partition_idx)
                else:
                    ctx = open_partition_for_extraction(evidence_fs, None)

                with ctx as fs_to_use:
                    for loc in partition_files:
                        if callbacks.is_cancelled():
                            manifest_data["status"] = "cancelled"
                            manifest_data["notes"].append(
                                "Extraction cancelled by user"
                            )
                            break

                        try:
                            extracted = extract_storage_file(
                                fs_to_use, loc, output_dir, callbacks
                            )
                            manifest_data["storage_locations"].append(extracted)

                        except Exception as e:
                            error_msg = (
                                f"Failed to extract {loc.get('storage_type', '?')} "
                                f"from {loc.get('logical_path', '?')}: {e}"
                            )
                            LOGGER.warning(error_msg)
                            manifest_data["notes"].append(error_msg)
                            manifest_data["status"] = "partial"
                            if collector:
                                collector.report_failed(
                                    evidence_id, self.metadata.name, files=1
                                )

            if collector:
                status = (
                    "success"
                    if manifest_data["status"] == "ok"
                    else manifest_data["status"]
                )
                collector.finish_run(
                    evidence_id, self.metadata.name, status=status
                )

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
            "Safari storage extraction complete: %d locations, status=%s",
            len(manifest_data["storage_locations"]),
            manifest_data["status"],
        )

        return manifest_data["status"] != "error"

    # ─────────────────────────────────────────────────────────────────
    # Ingestion
    # ─────────────────────────────────────────────────────────────────

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, int]:
        """
        Parse extracted Safari storage and ingest into database.

        Processes LocalStorage and IndexedDB files, inserts records,
        and optionally runs deep value analysis (URLs, tokens, emails,
        identifiers).
        """
        from core.statistics_collector import StatisticsCollector
        from extractors._shared.extraction_warnings import ExtractionWarningCollector

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"local_storage": 0, "indexeddb": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        storage_locations = manifest_data.get("storage_locations", [])

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Start statistics tracking
        evidence_label = config.get("evidence_label", "")
        collector = StatisticsCollector.get_instance()
        if collector:
            collector.continue_run(
                evidence_id, evidence_label, self.metadata.name, run_id
            )

        try:
            if not storage_locations:
                callbacks.on_log("No Safari storage to ingest", "warning")
                if collector:
                    collector.report_ingested(
                        evidence_id,
                        self.metadata.name,
                        records=0,
                        local_storage=0,
                        indexeddb=0,
                    )
                    collector.finish_run(
                        evidence_id, self.metadata.name, "success"
                    )
                return {"local_storage": 0, "indexeddb": 0}

            # Clear previous data for this run (idempotent re-ingestion)
            delete_local_storage_by_run(evidence_conn, evidence_id, run_id)
            delete_indexeddb_entries_by_run(evidence_conn, evidence_id, run_id)
            delete_indexeddb_databases_by_run(evidence_conn, evidence_id, run_id)
            delete_storage_tokens_by_run(evidence_conn, evidence_id, run_id)
            delete_storage_identifiers_by_run(
                evidence_conn, evidence_id, run_id
            )

            excerpt_size = config.get("excerpt_size", 4096)

            # Value analysis setup
            analyze_values = config.get("analyze_values", True)
            analyzer = None
            if analyze_values:
                try:
                    from extractors.browser.firefox.storage.analyzer import (
                        StorageValueAnalyzer,
                    )

                    analyzer = StorageValueAnalyzer(
                        extract_urls=config.get("extract_urls", True),
                        extract_emails=config.get("extract_emails", True),
                        detect_tokens=config.get("detect_tokens", True),
                        extract_identifiers=config.get(
                            "extract_identifiers", True
                        ),
                    )
                except ImportError:
                    LOGGER.warning(
                        "StorageValueAnalyzer not available — "
                        "value analysis disabled"
                    )

            local_count = 0
            indexeddb_count = 0

            # Accumulators for analyzed artifacts
            all_urls: List[Dict[str, Any]] = []
            all_emails: List[Dict[str, Any]] = []
            all_tokens: List[Dict[str, Any]] = []
            all_identifiers: List[Dict[str, Any]] = []

            callbacks.on_progress(
                0, len(storage_locations), "Parsing Safari storage data"
            )

            for i, loc in enumerate(storage_locations):
                if callbacks.is_cancelled():
                    break

                if loc.get("copy_status") == "error":
                    callbacks.on_log(
                        f"Skipping failed extraction: "
                        f"{loc.get('error_message', 'unknown')}",
                        "warning",
                    )
                    continue

                storage_type = loc.get("storage_type", "")
                browser = loc.get("browser", "safari")
                profile = loc.get("user", "Default")
                origin = loc.get("origin", "")

                callbacks.on_progress(
                    i + 1,
                    len(storage_locations),
                    f"Parsing Safari {storage_type}",
                )

                try:
                    extracted_path = Path(loc.get("extracted_path", ""))
                    if not extracted_path.is_absolute():
                        extracted_path = output_dir / extracted_path

                    if not extracted_path.exists():
                        continue

                    # Register browser inventory
                    inventory_id = insert_browser_inventory(
                        evidence_conn,
                        evidence_id=evidence_id,
                        browser="safari",
                        artifact_type=storage_type,
                        run_id=run_id,
                        extracted_path=str(extracted_path),
                        extraction_status="ok",
                        extraction_timestamp_utc=manifest_data.get(
                            "extraction_timestamp_utc", ""
                        ),
                        logical_path=loc.get("logical_path", ""),
                        profile=profile,
                        partition_index=loc.get("partition_index"),
                        fs_type=loc.get("fs_type"),
                        forensic_path=loc.get("logical_path", ""),
                        file_size_bytes=loc.get("size_bytes"),
                        file_md5=loc.get("md5"),
                        file_sha256=loc.get("sha256"),
                    )

                    if storage_type == "local_storage":
                        records = parse_safari_localstorage(
                            extracted_path,
                            loc,
                            run_id,
                            evidence_id,
                            excerpt_size,
                            warning_collector=warning_collector,
                        )

                        if records:
                            local_count += insert_local_storages(
                                evidence_conn, evidence_id, records
                            )

                            # Value analysis
                            if analyzer:
                                self._analyze_storage_records(
                                    records,
                                    analyzer,
                                    loc,
                                    run_id,
                                    browser,
                                    profile,
                                    "local_storage",
                                    all_urls,
                                    all_emails,
                                    all_tokens,
                                    all_identifiers,
                                )

                        update_inventory_ingestion_status(
                            evidence_conn,
                            inventory_id=inventory_id,
                            status="ok",
                            records_parsed=len(records),
                        )

                    elif storage_type == "indexeddb":
                        db_results = parse_safari_indexeddb(
                            extracted_path,
                            loc,
                            run_id,
                            evidence_id,
                            excerpt_size,
                            include_index_records=config.get(
                                "include_index_records", False
                            ),
                            warning_collector=warning_collector,
                        )

                        for db_record, entries in db_results:
                            try:
                                db_id = insert_indexeddb_database(
                                    evidence_conn,
                                    evidence_id,
                                    browser="safari",
                                    origin=db_record.get("origin", origin),
                                    database_name=db_record.get(
                                        "database_name", ""
                                    ),
                                    profile=profile,
                                    version=db_record.get("database_version"),
                                    total_entries=len(entries),
                                    object_stores=db_record.get(
                                        "object_stores"
                                    ),
                                    run_id=run_id,
                                    source_path=db_record.get("source_path"),
                                    partition_index=db_record.get(
                                        "partition_index"
                                    ),
                                    fs_type=db_record.get("fs_type"),
                                    logical_path=db_record.get("logical_path"),
                                    forensic_path=db_record.get(
                                        "forensic_path"
                                    ),
                                )

                                if entries:
                                    for entry in entries:
                                        entry["database_id"] = db_id
                                    insert_indexeddb_entries(
                                        evidence_conn, evidence_id, entries
                                    )
                                    indexeddb_count += len(entries)

                                    # Value analysis for IndexedDB
                                    if analyzer:
                                        self._analyze_indexeddb_entries(
                                            entries,
                                            analyzer,
                                            loc,
                                            run_id,
                                            browser,
                                            profile,
                                            db_record.get("origin", origin),
                                            all_urls,
                                            all_emails,
                                            all_tokens,
                                            all_identifiers,
                                        )
                            except Exception as e:
                                LOGGER.error(
                                    "Failed to insert IndexedDB: %s", e
                                )

                        total_entries = sum(
                            len(e) for _, e in db_results
                        )
                        update_inventory_ingestion_status(
                            evidence_conn,
                            inventory_id=inventory_id,
                            status="ok",
                            records_parsed=total_entries,
                        )

                except Exception as e:
                    LOGGER.error(
                        "Failed to parse storage %s: %s",
                        loc.get("extracted_path"),
                        e,
                    )
                    callbacks.on_log(
                        f"Failed to parse {storage_type}: {e}", "error"
                    )

            # Insert analyzed artifacts
            url_count = 0
            email_count = 0
            token_count = 0
            identifier_count = 0

            if all_urls:
                callbacks.on_step(
                    f"Ingesting {len(all_urls)} extracted URLs"
                )
                insert_urls(evidence_conn, evidence_id, all_urls)
                url_count = len(all_urls)

            if all_emails:
                callbacks.on_step(
                    f"Ingesting {len(all_emails)} extracted emails"
                )
                from core.database import insert_emails

                insert_emails(evidence_conn, evidence_id, all_emails)
                email_count = len(all_emails)

            if all_tokens:
                callbacks.on_step(
                    f"Ingesting {len(all_tokens)} detected tokens"
                )
                token_count = insert_storage_tokens(
                    evidence_conn, evidence_id, all_tokens
                )

            if all_identifiers:
                callbacks.on_step(
                    f"Ingesting {len(all_identifiers)} identifiers"
                )
                identifier_count = insert_storage_identifiers(
                    evidence_conn, evidence_id, all_identifiers
                )

            # Cross-post origins to urls table
            origin_urls = self._collect_origin_urls(
                storage_locations, run_id
            )
            if origin_urls:
                try:
                    insert_urls(evidence_conn, evidence_id, origin_urls)
                except Exception as e:
                    LOGGER.debug(
                        "Failed to cross-post storage origins: %s", e
                    )

            # Flush schema warnings to database
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info(
                    "Recorded %d extraction warnings for schema discovery",
                    warning_count,
                )

            evidence_conn.commit()

            total = local_count + indexeddb_count

            # Update manifest with parsed counts
            manifest_data["parsed_counts"] = {
                "local_storage": local_count,
                "indexeddb": indexeddb_count,
                "urls": url_count,
                "emails": email_count,
                "tokens": token_count,
                "identifiers": identifier_count,
            }
            manifest_path.write_text(json.dumps(manifest_data, indent=2))

            if collector:
                collector.report_ingested(
                    evidence_id,
                    self.metadata.name,
                    records=total,
                    local_storage=local_count,
                    indexeddb=indexeddb_count,
                    urls=url_count,
                    emails=email_count,
                    tokens=token_count,
                    identifiers=identifier_count,
                )
                collector.finish_run(
                    evidence_id, self.metadata.name, "success"
                )

            callbacks.on_log(
                f"Safari storage ingested: {local_count} LS records, "
                f"{indexeddb_count} IDB entries. "
                f"Analyzed: {url_count} URLs, {email_count} emails, "
                f"{token_count} tokens, {identifier_count} identifiers",
                "info",
            )

            return {
                "local_storage": local_count,
                "indexeddb": indexeddb_count,
                "urls": url_count,
                "emails": email_count,
                "tokens": token_count,
                "identifiers": identifier_count,
            }

        finally:
            # Ensure warnings are always flushed, even on error
            try:
                warning_collector.flush_to_database(evidence_conn)
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────────────
    # Helper Methods
    # ─────────────────────────────────────────────────────────────────

    def _generate_run_id(self) -> str:
        """Generate unique run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"storage_safari_{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context safely."""
        try:
            source_path = (
                evidence_fs.source_path
                if hasattr(evidence_fs, "source_path")
                else None
            )
            if source_path is not None and not isinstance(
                source_path, (str, Path)
            ):
                source_path = None

            fs_type = getattr(evidence_fs, "fs_type", "unknown")
            if not isinstance(fs_type, str):
                fs_type = "unknown"

            return {
                "image_path": str(source_path) if source_path else None,
                "fs_type": fs_type,
            }
        except Exception:
            return {"image_path": None, "fs_type": "unknown"}

    def _collect_origin_urls(
        self,
        storage_locations: List[Dict[str, Any]],
        run_id: str,
    ) -> List[Dict[str, Any]]:
        """Collect unique origin URLs from storage locations for cross-posting."""
        seen_origins: set = set()
        url_records: List[Dict[str, Any]] = []

        for loc in storage_locations:
            origin = loc.get("origin", "")
            if not origin or origin in seen_origins:
                continue
            seen_origins.add(origin)

            try:
                from urllib.parse import urlparse

                parsed = urlparse(origin)
                url_records.append(
                    {
                        "url": origin,
                        "domain": parsed.netloc or None,
                        "scheme": parsed.scheme or None,
                        "discovered_by": "safari_storage",
                        "run_id": run_id,
                        "source_path": loc.get("logical_path"),
                        "context": f"storage:safari:{loc.get('storage_type', 'storage')}",
                    }
                )
            except Exception:
                pass

        return url_records

    def _analyze_storage_records(
        self,
        records: List[Dict],
        analyzer,
        loc: Dict,
        run_id: str,
        browser: str,
        profile: str,
        storage_type: str,
        all_urls: List,
        all_emails: List,
        all_tokens: List,
        all_identifiers: List,
    ) -> None:
        """Analyze storage records and collect forensic artifacts."""
        for record in records:
            key = record.get("key", "")
            value = record.get("value", "")
            rec_origin = record.get("origin", "")

            result = analyzer.analyze_value(key, value, rec_origin)

            for url in result.urls:
                all_urls.append(
                    {
                        "url": url.url,
                        "discovered_by": "safari_storage",
                        "source_path": loc.get("logical_path"),
                        "first_seen_utc": url.first_seen_utc,
                        "last_seen_utc": url.last_seen_utc,
                        "context": url.context,
                        "run_id": run_id,
                    }
                )

            for email in result.emails:
                all_emails.append(
                    {
                        "email": email.email,
                        "discovered_by": "safari_storage",
                        "source_path": loc.get("logical_path"),
                        "context": email.context,
                        "run_id": run_id,
                    }
                )

            for token in result.tokens:
                all_tokens.append(
                    {
                        "run_id": run_id,
                        "browser": browser,
                        "profile": profile,
                        "origin": rec_origin,
                        "storage_type": storage_type,
                        "storage_key": key,
                        "token_type": token.token_type,
                        "token_value": token.token_value,
                        "token_hash": token.token_hash,
                        "issuer": token.issuer,
                        "subject": token.subject,
                        "audience": token.audience,
                        "associated_email": token.associated_email,
                        "associated_user_id": token.associated_user_id,
                        "issued_at_utc": token.issued_at_utc,
                        "expires_at_utc": token.expires_at_utc,
                        "risk_level": token.risk_level,
                        "is_expired": 1 if token.is_expired else 0,
                        "source_path": loc.get("logical_path"),
                        "notes": token.notes,
                    }
                )

            for ident in result.identifiers:
                all_identifiers.append(
                    {
                        "run_id": run_id,
                        "browser": browser,
                        "profile": profile,
                        "origin": rec_origin,
                        "storage_type": storage_type,
                        "storage_key": key,
                        "identifier_type": ident.identifier_type,
                        "identifier_name": ident.identifier_name,
                        "identifier_value": ident.identifier_value,
                        "first_seen_utc": ident.first_seen_utc,
                        "last_seen_utc": ident.last_seen_utc,
                        "source_path": loc.get("logical_path"),
                    }
                )

    def _analyze_indexeddb_entries(
        self,
        entries: List[Dict],
        analyzer,
        loc: Dict,
        run_id: str,
        browser: str,
        profile: str,
        origin: str,
        all_urls: List,
        all_emails: List,
        all_tokens: List,
        all_identifiers: List,
    ) -> None:
        """Analyze IndexedDB entries and collect forensic artifacts."""
        for entry in entries:
            key = entry.get("key", "")
            value = entry.get("value", "")

            result = analyzer.analyze_value(str(key), value, origin)

            for url in result.urls:
                all_urls.append(
                    {
                        "url": url.url,
                        "discovered_by": "safari_storage_indexeddb",
                        "source_path": loc.get("logical_path"),
                        "first_seen_utc": url.first_seen_utc,
                        "last_seen_utc": url.last_seen_utc,
                        "context": url.context,
                        "run_id": run_id,
                    }
                )

            for email in result.emails:
                all_emails.append(
                    {
                        "email": email.email,
                        "discovered_by": "safari_storage_indexeddb",
                        "source_path": loc.get("logical_path"),
                        "context": email.context,
                        "run_id": run_id,
                    }
                )

            for token in result.tokens:
                all_tokens.append(
                    {
                        "run_id": run_id,
                        "browser": browser,
                        "profile": profile,
                        "origin": origin,
                        "storage_type": "indexeddb",
                        "storage_key": str(key),
                        "token_type": token.token_type,
                        "token_value": token.token_value,
                        "token_hash": token.token_hash,
                        "issuer": token.issuer,
                        "subject": token.subject,
                        "audience": token.audience,
                        "associated_email": token.associated_email,
                        "associated_user_id": token.associated_user_id,
                        "issued_at_utc": token.issued_at_utc,
                        "expires_at_utc": token.expires_at_utc,
                        "risk_level": token.risk_level,
                        "is_expired": 1 if token.is_expired else 0,
                        "source_path": loc.get("logical_path"),
                        "notes": token.notes,
                    }
                )

            for ident in result.identifiers:
                all_identifiers.append(
                    {
                        "run_id": run_id,
                        "browser": browser,
                        "profile": profile,
                        "origin": origin,
                        "storage_type": "indexeddb",
                        "storage_key": str(key),
                        "identifier_type": ident.identifier_type,
                        "identifier_name": ident.identifier_name,
                        "identifier_value": ident.identifier_value,
                        "first_seen_utc": ident.first_seen_utc,
                        "last_seen_utc": ident.last_seen_utc,
                        "source_path": loc.get("logical_path"),
                    }
                )
