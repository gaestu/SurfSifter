"""
Chromium Autofill Extractor

Extracts and ingests browser autofill data from Chromium-based browsers
(Chrome, Edge, Opera, Brave) with full forensic provenance.

Features:
- Form autofill entries (name-value pairs from Web Data)
- Edge-specific autofill_edge_field_values table support
- Address profiles (structured contact info from Web Data)
- Modern Chromium address tokens (addresses/address_type_tokens, )
- Legacy address tokens (contact_info/local_addresses_type_tokens, )
- Edge server addresses (edge_server_addresses_type_tokens, )
- Search engines/keywords
- Saved credentials (Login Data - encrypted values stored for forensic record)
- Credential security metadata (insecure_credentials, breached, password_notes, )
- Credit cards (encrypted card numbers stored from Web Data)
- IBANs (local_ibans and masked_ibans from Web Data, )
- Schema warning support for unknown tables/columns/enums

Data Format:
- Web Data: autofill, autofill_profiles, credit_cards, keywords tables
- Web Data: local_ibans, masked_ibans tables
- Web Data (Chromium 131+): addresses, address_type_tokens tables
- Web Data (Chromium 100-130): contact_info_type_tokens, local_addresses_type_tokens
- Login Data: logins, insecure_credentials, breached, password_notes tables
- WebKit timestamps (microseconds since 1601-01-01)

This is the canonical location for Chromium autofill extraction.
For backward compatibility, the unified extractor is available at:
- extractors.autofill (handles both Chromium and Firefox)
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ....widgets import BrowserConfigWidget
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
)
from .._patterns import CHROMIUM_BROWSERS, get_artifact_patterns
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector
from core.database import (
    insert_autofill_entries,
    insert_autofill_profiles,
    insert_credentials,
    insert_credit_cards,
    insert_browser_inventory,
    update_inventory_ingestion_status,
    delete_autofill_by_run,
    delete_autofill_profiles_by_run,
    delete_credentials_by_run,
    delete_credit_cards_by_run,
)
from core.database.helpers.autofill_ibans import (
    insert_autofill_ibans,
    delete_autofill_ibans_by_run,
)
from core.database.helpers.search_engines import (
    insert_search_engines,
    delete_search_engines_by_run,
)
from core.database.helpers.autofill_profile_tokens import (
    insert_autofill_profile_tokens,
    delete_autofill_profile_tokens_by_run,
)
from core.database.helpers.autofill_block_list import (
    insert_autofill_block_list_entries,
    delete_autofill_block_list_by_run,
)
from ._schemas import (
    KNOWN_WEB_DATA_TABLES,
    KNOWN_LOGIN_DATA_TABLES,
    AUTOFILL_TABLE_PATTERNS,
)
from ._parsers import (
    parse_autofill_table,
    parse_autofill_profiles_table,
    parse_credit_cards_table,
    parse_keywords_table,
    parse_iban_tables,
    parse_all_token_tables,
    parse_logins_table,
    parse_edge_autofill_field_values,
    parse_edge_autofill_block_list,
)
from extractors._shared.extraction_warnings import (
    ExtractionWarningCollector,
    discover_unknown_tables,
)

LOGGER = get_logger("extractors.browser.chromium.autofill")


class ChromiumAutofillExtractor(BaseExtractor):
    """
    Extract autofill data from Chromium-based browsers.

    Dual-helper strategy:
    - Extraction: Scans filesystem, copies Web Data and Login Data files
    - Ingestion: Parses copied SQLite databases, inserts with forensic fields

    Supported browsers: Chrome, Edge, Opera, Brave
    """

    SUPPORTED_BROWSERS = list(CHROMIUM_BROWSERS.keys())

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="chromium_autofill",
            display_name="Chromium Autofill & Credentials",
            description="Extract autofill, saved logins, addresses, credit cards, and search engines from Chrome/Edge/Opera/Brave",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted. Please mount E01 image first."
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run (manifest exists)."""
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget (browser selection + partition config)."""
        return BrowserConfigWidget(parent, supported_browsers=self.SUPPORTED_BROWSERS)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction/ingestion state."""
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status_text = f"Chromium Autofill\nFiles extracted: {file_count}\nRun ID: {data.get('run_id', 'N/A')}"
        else:
            status_text = "Chromium Autofill\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "chromium_autofill"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Chromium autofill databases from evidence.
        """
        callbacks.on_step("Initializing Chromium autofill extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting Chromium autofill extraction (run_id=%s)", run_id)

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest_data = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "schema_version": "1.0.0",
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "extraction_tool": self._get_extraction_tool_version(),
            "e01_context": self._get_e01_context(evidence_fs),
            "files": [],
            "status": "ok",
            "notes": [],
        }

        callbacks.on_step("Scanning for Chromium autofill databases")

        browsers_to_search = config.get("browsers") or config.get("selected_browsers", self.SUPPORTED_BROWSERS)

        autofill_files = self._discover_autofill_files(evidence_fs, browsers_to_search, callbacks)

        # Report discovered files (always, even if 0)
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(autofill_files))

        callbacks.on_log(f"Found {len(autofill_files)} autofill file(s)")

        if not autofill_files:
            LOGGER.info("No Chromium autofill files found")
        else:
            callbacks.on_progress(0, len(autofill_files), "Copying autofill databases")

            for i, file_info in enumerate(autofill_files):
                if callbacks.is_cancelled():
                    manifest_data["status"] = "cancelled"
                    manifest_data["notes"].append("Extraction cancelled by user")
                    break

                try:
                    callbacks.on_progress(i + 1, len(autofill_files), f"Copying {file_info['browser']} {file_info['file_type']}")

                    extracted_file = self._extract_file(evidence_fs, file_info, output_dir, callbacks)
                    manifest_data["files"].append(extracted_file)

                except Exception as e:
                    error_msg = f"Failed to extract {file_info['logical_path']}: {e}"
                    LOGGER.error(error_msg, exc_info=True)
                    manifest_data["notes"].append(error_msg)
                    if stats:
                        stats.report_failed(evidence_id, self.metadata.name, files=1)

        # Finish statistics (once, at the end)
        if stats:
            status = "success" if manifest_data["status"] == "ok" else manifest_data["status"]
            stats.finish_run(evidence_id, self.metadata.name, status=status)

        callbacks.on_step("Writing manifest")
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2))

        # Record extracted files to audit table
        from extractors._shared.extracted_files_audit import record_browser_files
        record_browser_files(
            evidence_conn=config.get("evidence_conn"),
            evidence_id=evidence_id,
            run_id=run_id,
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
            manifest_data=manifest_data,
            callbacks=callbacks,
        )

        LOGGER.info(
            "Chromium autofill extraction complete: %d files, status=%s",
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
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """Parse extracted manifest and ingest into database."""
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", f"No manifest at {manifest_path}")
            return {"autofill": 0, "profiles": 0, "credentials": 0, "credit_cards": 0, "ibans": 0, "search_engines": 0, "profile_tokens": 0}

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]
        files = manifest_data.get("files", [])
        evidence_label = config.get("evidence_label", "")

        # Create warning collector for schema discovery
        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        # Continue statistics tracking (same run_id from manifest)
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        if not files:
            callbacks.on_log("No files to ingest", "warning")
            if stats:
                stats.report_ingested(
                    evidence_id, self.metadata.name,
                    records=0,
                    entries=0,
                )
                stats.finish_run(evidence_id, self.metadata.name, status="success")
            return {"autofill": 0, "profiles": 0, "credentials": 0, "credit_cards": 0, "ibans": 0, "search_engines": 0, "profile_tokens": 0}

        total_autofill = 0
        total_profiles = 0
        total_credentials = 0
        total_credit_cards = 0
        total_ibans = 0
        total_search_engines = 0
        total_profile_tokens = 0
        total_block_list = 0

        self._clear_previous_run(evidence_conn, evidence_id, run_id)

        callbacks.on_progress(0, len(files), "Parsing Chromium autofill databases")

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                break

            if file_entry.get("copy_status") == "error":
                callbacks.on_log(f"Skipping failed extraction: {file_entry.get('error_message', 'unknown')}", "warning")
                continue

            callbacks.on_progress(i + 1, len(files), f"Parsing {file_entry['browser']} {file_entry['file_type']}")

            try:
                inventory_id = insert_browser_inventory(
                    evidence_conn,
                    evidence_id=evidence_id,
                    browser=file_entry["browser"],
                    artifact_type=f"autofill_{file_entry['file_type']}",
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

                db_path = Path(file_entry["extracted_path"])
                if not db_path.is_absolute():
                    db_path = output_dir / db_path

                counts = self._parse_autofill_file(
                    db_path, file_entry, run_id, evidence_id, evidence_conn, callbacks,
                    warning_collector=warning_collector,
                )

                total_autofill += counts.get("autofill", 0)
                total_profiles += counts.get("profiles", 0)
                total_credentials += counts.get("credentials", 0)
                total_credit_cards += counts.get("credit_cards", 0)
                total_ibans += counts.get("ibans", 0)
                total_search_engines += counts.get("search_engines", 0)
                total_profile_tokens += counts.get("profile_tokens", 0)
                total_block_list += counts.get("block_list", 0)

                total_records = sum(counts.values())
                update_inventory_ingestion_status(
                    evidence_conn, inventory_id=inventory_id, status="ok", records_parsed=total_records
                )

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry['extracted_path']}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                callbacks.on_error(error_msg, "")

                if 'inventory_id' in locals():
                    update_inventory_ingestion_status(evidence_conn, inventory_id=inventory_id, status="error", notes=str(e))

        # Flush collected warnings to database before commit
        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            LOGGER.info("Recorded %d extraction warnings for schema discovery", warning_count)

        evidence_conn.commit()

        # Report ingested counts and finish
        total_records = total_autofill + total_profiles + total_credentials + total_credit_cards + total_ibans + total_search_engines + total_profile_tokens + total_block_list
        if stats:
            stats.report_ingested(
                evidence_id, self.metadata.name,
                records=total_records,
                entries=total_records,
            )
            stats.finish_run(evidence_id, self.metadata.name, status="success")

        return {
            "autofill": total_autofill,
            "profiles": total_profiles,
            "credentials": total_credentials,
            "credit_cards": total_credit_cards,
            "ibans": total_ibans,
            "search_engines": total_search_engines,
            "profile_tokens": total_profile_tokens,
            "block_list": total_block_list,
        }

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def _generate_run_id(self) -> str:
        """Generate run ID: {timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context from evidence filesystem."""
        try:
            source_path = evidence_fs.source_path if hasattr(evidence_fs, 'source_path') else None
            if source_path is not None and not isinstance(source_path, (str, Path)):
                source_path = None

            fs_type = getattr(evidence_fs, 'fs_type', "unknown")
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

    def _discover_autofill_files(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: ExtractorCallbacks
    ) -> List[Dict]:
        """Scan evidence for Chromium autofill files (Web Data, Login Data)."""
        autofill_files = []

        for browser_key in browsers:
            if browser_key not in CHROMIUM_BROWSERS:
                callbacks.on_log(f"Unknown Chromium browser: {browser_key}", "warning")
                continue

            browser_config = CHROMIUM_BROWSERS[browser_key]

            # Get autofill patterns (Web Data and Login Data)
            patterns = get_artifact_patterns(browser_key, "autofill")

            for pattern in patterns:
                try:
                    for path_str in evidence_fs.iter_paths(pattern):
                        file_type = self._classify_autofill_file(path_str)
                        profile = self._extract_profile_from_path(path_str)

                        autofill_files.append({
                            "logical_path": path_str,
                            "browser": browser_key,
                            "profile": profile,
                            "file_type": file_type,
                            "artifact_type": "autofill",
                            "display_name": browser_config["display_name"],
                        })

                        callbacks.on_log(f"Found {browser_key} {file_type}: {path_str}", "info")

                except Exception as e:
                    LOGGER.debug("Pattern %s failed: %s", pattern, e)

        return autofill_files

    def _classify_autofill_file(self, path: str) -> str:
        """Classify autofill file type based on filename."""
        filename = path.split('/')[-1].lower()

        if filename == "web data":
            return "web_data"
        elif filename == "login data":
            return "login_data"
        else:
            return "unknown"

    def _extract_profile_from_path(self, path: str) -> str:
        """Extract browser profile name from file path."""
        parts = path.split('/')

        try:
            idx = parts.index("User Data")
            return parts[idx + 1] if idx + 1 < len(parts) else "Default"
        except (ValueError, IndexError):
            # Opera uses different structure
            if "Opera Stable" in path:
                return "Opera Stable"
            elif "Opera GX Stable" in path:
                return "Opera GX Stable"
            return "Default"

    def _extract_file(
        self,
        evidence_fs,
        file_info: Dict,
        output_dir: Path,
        callbacks: ExtractorCallbacks
    ) -> Dict:
        """Copy file from evidence to workspace and collect metadata."""
        try:
            source_path = file_info["logical_path"]
            browser = file_info["browser"]
            profile = file_info["profile"]
            file_type = file_info["file_type"]

            safe_profile = profile.replace(' ', '_').replace('/', '_')
            filename = f"{browser}_{safe_profile}_{file_type}"
            dest_path = output_dir / filename

            callbacks.on_log(f"Copying {source_path} to {dest_path.name}", "info")

            file_content = evidence_fs.read_file(source_path)
            dest_path.write_bytes(file_content)

            md5 = hashlib.md5(file_content).hexdigest()
            sha256 = hashlib.sha256(file_content).hexdigest()
            size = len(file_content)

            return {
                "copy_status": "ok",
                "size_bytes": size,
                "file_size_bytes": size,
                "md5": md5,
                "sha256": sha256,
                "extracted_path": str(dest_path),
                "browser": browser,
                "profile": profile,
                "file_type": file_type,
                "logical_path": source_path,
                "artifact_type": "autofill",
            }

        except Exception as e:
            callbacks.on_log(f"Failed to extract {file_info['logical_path']}: {e}", "error")
            return {
                "copy_status": "error",
                "size_bytes": 0,
                "file_size_bytes": 0,
                "md5": None,
                "sha256": None,
                "extracted_path": None,
                "browser": file_info.get("browser"),
                "profile": file_info.get("profile"),
                "file_type": file_info.get("file_type"),
                "logical_path": file_info.get("logical_path"),
                "error_message": str(e),
            }

    def _clear_previous_run(self, evidence_conn, evidence_id: int, run_id: str) -> None:
        """Clear autofill data from a previous run."""
        deleted = 0
        deleted += delete_autofill_by_run(evidence_conn, evidence_id, run_id)
        deleted += delete_autofill_profiles_by_run(evidence_conn, evidence_id, run_id)
        deleted += delete_credentials_by_run(evidence_conn, evidence_id, run_id)
        deleted += delete_credit_cards_by_run(evidence_conn, evidence_id, run_id)
        deleted += delete_autofill_ibans_by_run(evidence_conn, evidence_id, run_id)
        deleted += delete_search_engines_by_run(evidence_conn, evidence_id, run_id)
        deleted += delete_autofill_profile_tokens_by_run(evidence_conn, evidence_id, run_id)
        deleted += delete_autofill_block_list_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d autofill records from previous run %s", deleted, run_id)

    def _parse_autofill_file(
        self,
        db_path: Path,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, int]:
        """Parse autofill file and insert records."""
        if not db_path.exists():
            LOGGER.warning("Autofill file not found: %s", db_path)
            return {"autofill": 0, "profiles": 0, "credentials": 0, "credit_cards": 0, "ibans": 0, "search_engines": 0, "profile_tokens": 0, "block_list": 0}

        file_type = file_entry["file_type"]
        browser = file_entry["browser"]

        counts = {"autofill": 0, "profiles": 0, "credentials": 0, "credit_cards": 0, "ibans": 0, "search_engines": 0, "profile_tokens": 0, "block_list": 0}

        if file_type == "web_data":
            counts = self._parse_web_data(
                db_path, browser, file_entry, run_id, evidence_id, evidence_conn, callbacks,
                warning_collector=warning_collector,
            )
        elif file_type == "login_data":
            counts["credentials"] = self._parse_login_data(
                db_path, browser, file_entry, run_id, evidence_id, evidence_conn, callbacks,
                warning_collector=warning_collector,
            )

        return counts

    def _parse_web_data(
        self,
        db_path: Path,
        browser: str,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, int]:
        """Parse Chromium Web Data SQLite database using modular parsers."""
        counts = {"autofill": 0, "profiles": 0, "credentials": 0, "credit_cards": 0, "ibans": 0, "search_engines": 0, "profile_tokens": 0, "block_list": 0}

        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
        except Exception as e:
            LOGGER.error("Failed to open Web Data: %s", e)
            if warning_collector:
                warning_collector.add_file_corrupt(
                    filename=str(db_path),
                    error=str(e),
                    artifact_type="autofill",
                )
            return counts

        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"
        source_file = file_entry.get("logical_path", str(db_path))

        try:
            # Discover unknown tables for schema tracking
            if warning_collector:
                unknown_tables = discover_unknown_tables(
                    conn, KNOWN_WEB_DATA_TABLES, AUTOFILL_TABLE_PATTERNS
                )
                for table_info in unknown_tables:
                    warning_collector.add_unknown_table(
                        table_name=table_info["name"],
                        columns=table_info["columns"],
                        source_file=source_file,
                        artifact_type="autofill",
                    )

            # Parse autofill table (name-value pairs)
            autofill_records = parse_autofill_table(
                conn, browser, file_entry, run_id, discovered_by,
                warning_collector=warning_collector,
            )
            if autofill_records:
                counts["autofill"] = insert_autofill_entries(evidence_conn, evidence_id, autofill_records)

            # Edge-specific autofill tables
            if browser == "edge":
                edge_records = parse_edge_autofill_field_values(
                    conn, browser, file_entry, run_id, discovered_by,
                    warning_collector=warning_collector,
                )
                if edge_records:
                    counts["autofill"] += insert_autofill_entries(evidence_conn, evidence_id, edge_records)

                block_list_records = parse_edge_autofill_block_list(
                    conn, browser, file_entry, run_id, discovered_by,
                    warning_collector=warning_collector,
                )
                if block_list_records:
                    counts["block_list"] = insert_autofill_block_list_entries(evidence_conn, evidence_id, block_list_records)

            # Parse autofill profiles (legacy address storage)
            profile_records = parse_autofill_profiles_table(
                conn, browser, file_entry, run_id, discovered_by,
                warning_collector=warning_collector,
            )
            if profile_records:
                counts["profiles"] = insert_autofill_profiles(evidence_conn, evidence_id, profile_records)

            # Parse modern token-based address tables
            token_records = parse_all_token_tables(
                conn, browser, file_entry, run_id, discovered_by,
                warning_collector=warning_collector,
            )
            if token_records:
                counts["profile_tokens"] = insert_autofill_profile_tokens(evidence_conn, evidence_id, token_records)

            # Parse credit cards
            credit_card_records = parse_credit_cards_table(
                conn, browser, file_entry, run_id, discovered_by,
                warning_collector=warning_collector,
            )
            if credit_card_records:
                counts["credit_cards"] = insert_credit_cards(evidence_conn, evidence_id, credit_card_records)

            # Parse IBAN tables (local_ibans, masked_ibans)
            iban_records = parse_iban_tables(
                conn, browser, file_entry, run_id, discovered_by,
                warning_collector=warning_collector,
            )
            if iban_records:
                counts["ibans"] = insert_autofill_ibans(evidence_conn, evidence_id, iban_records)

            # Parse search engines (keywords table)
            search_engine_records = parse_keywords_table(
                conn, browser, file_entry, run_id, discovered_by,
                warning_collector=warning_collector,
            )
            if search_engine_records:
                counts["search_engines"] = insert_search_engines(evidence_conn, evidence_id, search_engine_records)

        finally:
            conn.close()

        return counts

    def _parse_login_data(
        self,
        db_path: Path,
        browser: str,
        file_entry: Dict,
        run_id: str,
        evidence_id: int,
        evidence_conn,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> int:
        """Parse Chromium Login Data SQLite database using modular parser."""
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
        except Exception as e:
            LOGGER.error("Failed to open Login Data: %s", e)
            if warning_collector:
                warning_collector.add_file_corrupt(
                    filename=str(db_path),
                    error=str(e),
                    artifact_type="credentials",
                )
            return 0

        discovered_by = f"{self.metadata.name}:{self.metadata.version}:{run_id}"
        source_file = file_entry.get("logical_path", str(db_path))

        try:
            # Discover unknown tables for schema tracking
            if warning_collector:
                unknown_tables = discover_unknown_tables(
                    conn, KNOWN_LOGIN_DATA_TABLES, ["login", "password", "credential"]
                )
                for table_info in unknown_tables:
                    warning_collector.add_unknown_table(
                        table_name=table_info["name"],
                        columns=table_info["columns"],
                        source_file=source_file,
                        artifact_type="credentials",
                    )

            # Parse logins table using modular parser
            records = parse_logins_table(
                conn, browser, file_entry, run_id, discovered_by,
                warning_collector=warning_collector,
            )

            if records:
                return insert_credentials(evidence_conn, evidence_id, records)
            return 0

        finally:
            conn.close()
