"""
Firefox Browser Storage Extractor

Extracts Local Storage and IndexedDB from Firefox browsers.

Features:
- Local Storage (legacy webappsstore.sqlite AND modern data.sqlite)
- IndexedDB (SQLite files per origin with proper metadata parsing)
- Multi-partition discovery via file_list SQL queries (fast!)
- Schema warnings for unknown tables/columns/enums
- Configurable value excerpt size
- StatisticsCollector integration for run tracking
- Deep value analysis (URLs, tokens, emails, identifiers)

Data Sources:
- Local Storage (legacy): {Profile}/webappsstore.sqlite
- Local Storage (modern): {Profile}/storage/default/*/ls/data.sqlite
- IndexedDB: {Profile}/storage/default/*/idb/*.sqlite

Architecture:
- extractor.py: Main extractor class (this file)
- _discovery.py: Multi-partition discovery using file_list
- _parsers.py: SQLite parsing with schema warnings
- _schemas.py: Known tables, columns, and enums
- widget.py: Configuration UI widget
- analyzer.py: Deep value analysis for forensic artifacts

Complete refactor - split into modules, multi-partition discovery,
        schema warnings, removed URL/email deduplication, removed IndexedDB limit
Value analysis for URLs, tokens, emails, identifiers
Modern LocalStorage support, Snappy decompression
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, TYPE_CHECKING

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from .._patterns import FIREFOX_BROWSERS
from .widget import FirefoxStorageWidget
from ._discovery import (
    discover_storage_multi_partition,
    extract_storage_file,
)
from ._parsers import (
    parse_webappsstore,
    parse_modern_localstorage,
    parse_indexeddb_sqlite,
)

from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

if TYPE_CHECKING:
    from extractors._shared.extraction_warnings import ExtractionWarningCollector

LOGGER = get_logger("extractors.browser.firefox.storage")


class FirefoxStorageExtractor(BaseExtractor):
    """
    Extract browser web storage from Firefox browsers.

    Uses SQLite parsing for webappsstore.sqlite, modern data.sqlite,
    and IndexedDB files. Supports Firefox, Firefox ESR, Tor Browser.

    Features multi-partition discovery via file_list SQL queries for
    fast, comprehensive artifact discovery.
    """

    SUPPORTED_BROWSERS = list(FIREFOX_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata."""
        return ExtractorMetadata(
            name="firefox_browser_storage",
            display_name="Firefox Browser Storage",
            description="Extract Local Storage and IndexedDB from Firefox/Tor with deep value analysis (URLs, tokens, emails)",
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
        """Check if output has existing extraction."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget."""
        return FirefoxStorageWidget(parent, supported_browsers=self.SUPPORTED_BROWSERS)

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
            storage_count = len(data.get("storage_locations", []))
            partitions = set(
                loc.get("partition_index", 0)
                for loc in data.get("storage_locations", [])
            )
            partition_info = f" across {len(partitions)} partition(s)" if len(partitions) > 1 else ""
            status_text = (
                f"Firefox Storage\n"
                f"Locations found: {storage_count}{partition_info}\n"
                f"Run ID: {data.get('run_id', 'N/A')}"
            )
        else:
            status_text = "Firefox Storage\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory."""
        return case_root / "evidences" / evidence_label / "firefox_browser_storage"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """Extract Firefox storage files from evidence."""
        callbacks.on_step("Initializing Firefox storage extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        evidence_conn = config.get("evidence_conn")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Firefox storage extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "3.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_extraction_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "storage_locations": [],
            "partitions_scanned": [],
            "config": {
                "local_storage": config.get("local_storage", True),
                "indexeddb": config.get("indexeddb", True),
                "excerpt_size": config.get("excerpt_size", 4096),
            },
            "status": "ok",
            "notes": [],
        }

        callbacks.on_step("Discovering Firefox storage (multi-partition)")

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)
        browsers_to_search = [b for b in browsers_to_search if b in self.SUPPORTED_BROWSERS]

        # Multi-partition discovery using file_list SQL queries
        files_by_partition = discover_storage_multi_partition(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            evidence_fs=evidence_fs,
            browsers=browsers_to_search,
            config=config,
            callbacks=callbacks,
        )

        # Flatten locations for extraction
        all_locations = []
        for partition_idx, locations in files_by_partition.items():
            manifest_data["partitions_scanned"].append(partition_idx)
            all_locations.extend(locations)

        # Report discovery count
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(all_locations))

        if not all_locations:
            manifest_data["notes"].append("No Firefox storage found")
            LOGGER.info("No Firefox storage found")
        else:
            callbacks.on_progress(0, len(all_locations), "Copying storage data")

            for i, loc in enumerate(all_locations):
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    manifest_data["notes"].append("Extraction cancelled by user")
                    break

                try:
                    callbacks.on_progress(
                        i + 1, len(all_locations),
                        f"Copying {loc['browser']} {loc['storage_type']}"
                    )

                    extracted = extract_storage_file(evidence_fs, loc, output_dir, callbacks)
                    manifest_data["storage_locations"].append(extracted)

                except Exception as e:
                    error_msg = f"Failed to extract {loc['storage_type']} from {loc['browser']}: {e}"
                    LOGGER.error(error_msg, exc_info=True)
                    manifest_data["notes"].append(error_msg)
                    manifest_data["status"] = "partial"
                    if stats:
                        stats.report_failed(evidence_id, self.metadata.name, files=1)

        # Finish statistics tracking
        if stats:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            stats.finish_run(evidence_id, self.metadata.name, status)

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
            "Firefox storage extraction complete: %d locations across %d partitions, status=%s",
            len(manifest_data["storage_locations"]),
            len(manifest_data["partitions_scanned"]),
            manifest_data["status"],
        )

        return manifest_data["status"] != "error"

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """Parse extracted storage and ingest into database."""
        from core.database import (
            insert_local_storages, delete_local_storage_by_run,
            insert_indexeddb_database, insert_indexeddb_entries,
            delete_indexeddb_entries_by_run,
            insert_urls, insert_emails,
            insert_storage_tokens, delete_storage_tokens_by_run,
            insert_storage_identifiers, delete_storage_identifiers_by_run,
        )
        from .analyzer import StorageValueAnalyzer
        from extractors._shared.extraction_warnings import ExtractionWarningCollector

        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"local_storage": 0, "indexeddb": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        storage_locations = manifest_data.get("storage_locations", [])
        manifest_config = manifest_data.get("config", {})

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Start statistics tracking
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        try:
            if not storage_locations:
                callbacks.on_log("No storage to ingest", "warning")
                if stats:
                    stats.report_ingested(
                        evidence_id, self.metadata.name,
                        records=0, local_storage=0, indexeddb=0
                    )
                    stats.finish_run(evidence_id, self.metadata.name, "success")
                return {"local_storage": 0, "indexeddb": 0}

            # Clear previous data for this run
            delete_local_storage_by_run(evidence_conn, evidence_id, run_id)
            delete_indexeddb_entries_by_run(evidence_conn, evidence_id, run_id)
            delete_storage_tokens_by_run(evidence_conn, evidence_id, run_id)
            delete_storage_identifiers_by_run(evidence_conn, evidence_id, run_id)

            excerpt_size = manifest_config.get("excerpt_size", 4096)

            # Get analysis config options
            analyze_values = config.get("analyze_values", True)
            extract_urls = config.get("extract_urls", True)
            extract_emails = config.get("extract_emails", True)
            detect_tokens = config.get("detect_tokens", True)
            extract_identifiers = config.get("extract_identifiers", True)

            # Create analyzer if analysis enabled
            analyzer = None
            if analyze_values:
                analyzer = StorageValueAnalyzer(
                    extract_urls=extract_urls,
                    extract_emails=extract_emails,
                    detect_tokens=detect_tokens,
                    extract_identifiers=extract_identifiers,
                )

            local_count = 0
            indexeddb_count = 0

            # Accumulators for analyzed artifacts
            all_urls = []
            all_emails = []
            all_tokens = []
            all_identifiers = []

            callbacks.on_progress(0, len(storage_locations), "Parsing storage data")

            for i, loc in enumerate(storage_locations):
                if callbacks.is_cancelled():
                    break

                if loc.get("copy_status") == "error":
                    callbacks.on_log(
                        f"Skipping failed extraction: {loc.get('error_message', 'unknown')}",
                        "warning"
                    )
                    continue

                storage_type = loc.get("storage_type", "")
                storage_format = loc.get("storage_format", "")
                browser = loc.get("browser", "firefox")
                profile = loc.get("profile", "")
                origin = loc.get("origin", "")

                callbacks.on_progress(i + 1, len(storage_locations), f"Parsing {browser} {storage_type}")

                try:
                    extracted_path = Path(loc.get("extracted_path", ""))
                    if not extracted_path.is_absolute():
                        extracted_path = output_dir / extracted_path

                    if not extracted_path.exists():
                        continue

                    if storage_type == "local_storage":
                        if storage_format == "modern_ls":
                            records = parse_modern_localstorage(
                                extracted_path, loc, run_id, evidence_id, excerpt_size,
                                warning_collector=warning_collector,
                            )
                        else:
                            records = parse_webappsstore(
                                extracted_path, loc, run_id, evidence_id, excerpt_size,
                                warning_collector=warning_collector,
                            )

                        if records:
                            local_count += insert_local_storages(evidence_conn, evidence_id, records)

                            # Analyze values for forensic artifacts
                            if analyzer:
                                self._analyze_storage_records(
                                    records, analyzer, loc, run_id, browser, profile,
                                    "local_storage", all_urls, all_emails, all_tokens, all_identifiers
                                )

                    elif storage_type == "indexeddb":
                        db_results = parse_indexeddb_sqlite(
                            extracted_path, loc, run_id, evidence_id, excerpt_size,
                            warning_collector=warning_collector,
                        )

                        for db_record, entries in db_results:
                            try:
                                db_id = insert_indexeddb_database(evidence_conn, evidence_id, db_record)
                                if entries:
                                    for entry in entries:
                                        entry["database_id"] = db_id
                                    insert_indexeddb_entries(evidence_conn, evidence_id, entries)
                                    indexeddb_count += len(entries)

                                    # Analyze IndexedDB values (no limit!)
                                    if analyzer:
                                        self._analyze_indexeddb_entries(
                                            entries, analyzer, loc, run_id, browser, profile,
                                            db_record.get("origin", origin),
                                            all_urls, all_emails, all_tokens, all_identifiers
                                        )
                            except Exception as e:
                                LOGGER.error("Failed to insert IndexedDB: %s", e)

                except Exception as e:
                    LOGGER.error("Failed to parse storage %s: %s", loc.get("extracted_path"), e)
                    callbacks.on_log(f"Failed to parse {storage_type}: {e}", "error")

            # Insert analyzed artifacts
            url_count = 0
            email_count = 0
            token_count = 0
            identifier_count = 0

            if all_urls:
                callbacks.on_step(f"Ingesting {len(all_urls)} extracted URLs")
                insert_urls(evidence_conn, evidence_id, all_urls)
                url_count = len(all_urls)

            if all_emails:
                callbacks.on_step(f"Ingesting {len(all_emails)} extracted emails")
                insert_emails(evidence_conn, evidence_id, all_emails)
                email_count = len(all_emails)

            if all_tokens:
                callbacks.on_step(f"Ingesting {len(all_tokens)} detected tokens")
                token_count = insert_storage_tokens(evidence_conn, evidence_id, all_tokens)

            if all_identifiers:
                callbacks.on_step(f"Ingesting {len(all_identifiers)} identifiers")
                identifier_count = insert_storage_identifiers(evidence_conn, evidence_id, all_identifiers)

            # Flush schema warnings to database
            warning_count = warning_collector.flush_to_database(evidence_conn)
            if warning_count > 0:
                LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)

            evidence_conn.commit()

            total = local_count + indexeddb_count

            if stats:
                stats.report_ingested(
                    evidence_id, self.metadata.name,
                    records=total,
                    local_storage=local_count,
                    indexeddb=indexeddb_count,
                    urls=url_count,
                    emails=email_count,
                    tokens=token_count,
                    identifiers=identifier_count,
                )
                stats.finish_run(evidence_id, self.metadata.name, "success")

            callbacks.on_log(
                f"Analyzed: {url_count} URLs, {email_count} emails, {token_count} tokens, {identifier_count} identifiers",
                "info"
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
        """Generate run ID."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"storage_firefox_{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
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

    def _get_extraction_tool_version(self) -> str:
        """Build extraction tool version string."""
        try:
            import pytsk3
            pytsk_version = pytsk3.TSK_VERSION_STR
        except ImportError:
            pytsk_version = "unknown"
        return f"pytsk3:{pytsk_version}"

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

            # Collect URLs
            for url in result.urls:
                all_urls.append({
                    "url": url.url,
                    "discovered_by": "firefox_storage",
                    "source_path": loc.get("logical_path"),
                    "first_seen_utc": url.first_seen_utc,
                    "last_seen_utc": url.last_seen_utc,
                    "context": url.context,
                    "run_id": run_id,
                })

            # Collect emails
            for email in result.emails:
                all_emails.append({
                    "email": email.email,
                    "discovered_by": "firefox_storage",
                    "source_path": loc.get("logical_path"),
                    "context": email.context,
                    "run_id": run_id,
                })

            # Collect tokens
            for token in result.tokens:
                all_tokens.append({
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
                })

            # Collect identifiers
            for ident in result.identifiers:
                all_identifiers.append({
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
                })

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
                all_urls.append({
                    "url": url.url,
                    "discovered_by": "firefox_storage_indexeddb",
                    "source_path": loc.get("logical_path"),
                    "first_seen_utc": url.first_seen_utc,
                    "last_seen_utc": url.last_seen_utc,
                    "context": url.context,
                    "run_id": run_id,
                })

            for email in result.emails:
                all_emails.append({
                    "email": email.email,
                    "discovered_by": "firefox_storage_indexeddb",
                    "source_path": loc.get("logical_path"),
                    "context": email.context,
                    "run_id": run_id,
                })

            for token in result.tokens:
                all_tokens.append({
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
                })

            for ident in result.identifiers:
                all_identifiers.append({
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
                })
