"""
Chromium Embedded Artifacts Extractor.

Discovers and ingests artifacts from embedded Chromium/CEF/CefSharp
applications that are not covered by standard browser extractors.

Currently supported artifacts:

  * **debug.log** — Chromium internal log that contains JavaScript console
    source URLs, navigation events, and error context.

Architecture
------------
  Extraction:
    1. Discover embedded Chromium roots via ``_embedded_discovery``
    2. Find ``debug.log`` files near those roots via ``file_list``
    3. Copy matching files to the case workspace

  Ingestion:
    1. Parse each ``debug.log`` using ``_debuglog`` parser
    2. Extract unique URLs
    3. Insert into ``urls`` table with ``discovered_by = "embedded_debuglog"``
    4. Log the operation via ``process_log``

Forensic Value
--------------
  CefSharp/CEF debug.log may contain thousands of CONSOLE source URLs
  (JavaScript execution origins) that are unavailable from any other artifact.
  The timestamps embedded in the log format enable activity correlation.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from PySide6.QtWidgets import QWidget, QLabel

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ...._shared.file_list_discovery import (
    discover_from_file_list,
    open_partition_for_extraction,
    get_ewf_paths_from_evidence_fs,
    check_file_list_available,
)
from .._embedded_discovery import (
    discover_embedded_roots,
    EmbeddedRoot,
)
from ._debuglog import parse_debuglog, extract_urls
from core.logging import get_logger
from core.database import (
    insert_urls,
    delete_urls_by_run,
    insert_process_log,
)

LOGGER = get_logger("extractors.browser.chromium.embedded_artifacts")


class ChromiumEmbeddedArtifactsExtractor(BaseExtractor):
    """
    Extract artifacts from embedded Chromium/CEF/CefSharp applications.

    Currently handles ``debug.log`` — future artifacts (``settings.ini``,
    CefSharp preferences, etc.) can be added as new sub-parsers.
    """

    # ─────────────────────────────────────────────────────────────────
    # Metadata
    # ─────────────────────────────────────────────────────────────────

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="chromium_embedded_artifacts",
            display_name="Embedded Chromium Artifacts",
            description=(
                "Extract debug.log and related artifacts from embedded "
                "Chromium/CEF/CefSharp applications"
            ),
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
        )

    # ─────────────────────────────────────────────────────────────────
    # Capability checks
    # ─────────────────────────────────────────────────────────────────

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        if evidence_fs is None:
            return False, "No evidence filesystem mounted."
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found — run extraction first."
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        return (output_dir / "manifest.json").exists()

    # ─────────────────────────────────────────────────────────────────
    # UI widgets
    # ─────────────────────────────────────────────────────────────────

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        # No configuration needed — auto-discovers embedded roots.
        return None

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
    ) -> QWidget:
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            try:
                data = json.loads(manifest.read_text())
                file_count = len(data.get("files", []))
                run_id = data.get("run_id", "N/A")
                status_text = (
                    f"Embedded Chromium Artifacts\n"
                    f"Files extracted: {file_count}\n"
                    f"Run ID: {run_id}"
                )
            except Exception:
                status_text = "Embedded Chromium Artifacts\nManifest unreadable"
        else:
            status_text = "Embedded Chromium Artifacts\nNo extraction run yet"
        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        return case_root / "evidences" / evidence_label / "chromium_embedded_artifacts"

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
        """Discover and copy embedded Chromium artifact files from evidence."""
        callbacks.on_step("Initialising embedded artifacts extraction")

        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_conn = config.get("evidence_conn")

        output_dir.mkdir(parents=True, exist_ok=True)

        manifest: Dict[str, Any] = {
            "extractor": self.metadata.name,
            "version": self.metadata.version,
            "run_id": run_id,
            "evidence_id": evidence_id,
            "extraction_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "files": [],
            "embedded_roots": [],
            "status": "ok",
            "notes": [],
        }

        # --- Discover embedded roots ---
        callbacks.on_step("Discovering embedded Chromium roots")

        if evidence_conn is None:
            callbacks.on_log(
                "No evidence_conn — cannot discover embedded roots", "error"
            )
            manifest["status"] = "error"
            manifest["notes"].append("No evidence_conn provided")
            self._write_manifest(output_dir, manifest)
            return False

        available, count = check_file_list_available(evidence_conn, evidence_id)
        if not available:
            callbacks.on_log(
                "file_list is empty — run File List extractor first", "warning"
            )
            manifest["status"] = "no_data"
            manifest["notes"].append("file_list empty")
            self._write_manifest(output_dir, manifest)
            return True  # not a hard failure

        callbacks.on_log(f"file_list contains {count:,} records", "info")

        embedded_roots = discover_embedded_roots(evidence_conn, evidence_id)
        if not embedded_roots:
            callbacks.on_log("No embedded Chromium roots detected", "info")
            manifest["notes"].append("No embedded Chromium roots found")
            self._write_manifest(output_dir, manifest)
            return True  # not a failure — just nothing to do

        manifest["embedded_roots"] = [
            {
                "root_path": r.root_path,
                "partition_index": r.partition_index,
                "signals": r.signals,
                "signal_count": r.signal_count,
            }
            for r in embedded_roots
        ]
        callbacks.on_log(
            f"Found {len(embedded_roots)} embedded root(s): "
            + ", ".join(r.root_path for r in embedded_roots),
            "info",
        )

        # --- Find debug.log files near embedded roots ---
        callbacks.on_step("Searching for debug.log files")

        root_path_patterns = []
        seen_patterns: set[str] = set()
        for root in embedded_roots:
            norm = root.root_path.replace("\\", "/").rstrip("/")
            # Search inside and one level below the root
            for pat in (
                f"{norm}/debug.log",
                f"{norm}/%/debug.log",
            ):
                if pat not in seen_patterns:
                    seen_patterns.add(pat)
                    root_path_patterns.append(pat)
            # CefSharp/CEF writes debug.log to the application working
            # directory which is often the *parent* of the browser data
            # root (e.g. /Application/debug.log when root is /Application/cache).
            parent = norm.rsplit("/", 1)[0] if "/" in norm else ""
            if parent:
                pat = f"{parent}/debug.log"
                if pat not in seen_patterns:
                    seen_patterns.add(pat)
                    root_path_patterns.append(pat)

        result = discover_from_file_list(
            evidence_conn,
            evidence_id,
            filename_patterns=["debug.log"],
            path_patterns=root_path_patterns,
            exclude_deleted=True,
        )

        if result.is_empty:
            callbacks.on_log("No debug.log found near embedded roots", "info")
            manifest["notes"].append("No debug.log files found")
            self._write_manifest(output_dir, manifest)
            return True

        # --- Copy files from evidence ---
        all_matches = result.get_all_matches()
        callbacks.on_log(f"Found {len(all_matches)} debug.log file(s)", "info")
        callbacks.on_progress(0, len(all_matches), "Copying files")

        ewf_paths = get_ewf_paths_from_evidence_fs(evidence_fs)
        copied = 0

        for idx, match in enumerate(all_matches):
            if callbacks.is_cancelled():
                manifest["status"] = "cancelled"
                manifest["notes"].append("Extraction cancelled by user")
                break

            callbacks.on_progress(idx + 1, len(all_matches), f"Copying {match.file_path}")

            partition_index = match.partition_index
            try:
                with open_partition_for_extraction(
                    ewf_paths if ewf_paths else evidence_fs,
                    partition_index if ewf_paths else None,
                ) as fs:
                    file_content = fs.read_file(match.file_path)

                if file_content is None:
                    callbacks.on_log(
                        f"Cannot read {match.file_path} (partition {partition_index})",
                        "warning",
                    )
                    manifest["files"].append({
                        "logical_path": match.file_path,
                        "partition_index": partition_index,
                        "copy_status": "error",
                        "error_message": "read_file returned None",
                    })
                    continue

                md5 = hashlib.md5(file_content).hexdigest()
                sha256 = hashlib.sha256(file_content).hexdigest()

                safe_name = f"debug_p{partition_index}_{md5[:8]}.log"
                dest = output_dir / safe_name
                dest.write_bytes(file_content)

                manifest["files"].append({
                    "logical_path": match.file_path,
                    "extracted_path": str(dest),
                    "filename": safe_name,
                    "artifact_type": "debug_log",
                    "partition_index": partition_index,
                    "file_size_bytes": len(file_content),
                    "md5": md5,
                    "sha256": sha256,
                    "copy_status": "ok",
                })
                copied += 1
                callbacks.on_log(
                    f"Copied {match.file_path} ({len(file_content):,} bytes)", "info"
                )

            except Exception as exc:
                error_msg = f"Failed to extract {match.file_path}: {exc}"
                LOGGER.warning(error_msg, exc_info=True)
                callbacks.on_log(error_msg, "error")
                manifest["files"].append({
                    "logical_path": match.file_path,
                    "partition_index": partition_index,
                    "copy_status": "error",
                    "error_message": str(exc),
                })

        callbacks.on_log(f"Extraction complete: {copied} file(s) copied", "info")
        self._write_manifest(output_dir, manifest)
        return True

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
        """Parse extracted debug.log files and ingest URLs into the database."""
        callbacks.on_step("Reading manifest")
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("Manifest not found", str(manifest_path))
            return {"urls": 0}

        manifest = json.loads(manifest_path.read_text())
        run_id = manifest["run_id"]
        files = [f for f in manifest.get("files", []) if f.get("copy_status") == "ok"]

        if not files:
            callbacks.on_log("No successfully extracted files to ingest", "warning")
            return {"urls": 0}

        # Clear previous ingestion for this run (idempotent re-ingestion)
        deleted = delete_urls_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            LOGGER.info("Cleared %d URLs from previous run %s", deleted, run_id)
            callbacks.on_log(f"Cleared {deleted} previous URLs for re-ingestion", "info")

        callbacks.on_progress(0, len(files), "Parsing debug.log files")

        total_urls = 0
        total_entries = 0

        for i, file_entry in enumerate(files):
            if callbacks.is_cancelled():
                evidence_conn.rollback()
                return {"urls": 0}

            extracted_path = Path(file_entry["extracted_path"])
            logical_path = file_entry["logical_path"]
            partition_index = file_entry.get("partition_index")

            callbacks.on_progress(i + 1, len(files), f"Parsing {extracted_path.name}")

            if not extracted_path.exists():
                callbacks.on_log(
                    f"Extracted file missing: {extracted_path}", "warning"
                )
                continue

            try:
                entries = parse_debuglog(extracted_path)
                url_records = extract_urls(entries)
                total_entries += len(entries)

                callbacks.on_log(
                    f"{extracted_path.name}: {len(entries)} log entries, "
                    f"{len(url_records)} unique URLs",
                    "info",
                )

                if not url_records:
                    continue

                # Build URL dicts for the urls table
                discovered_by = (
                    f"embedded_debuglog:{self.metadata.version}:{run_id}"
                )
                db_records = self._build_url_records(
                    url_records=url_records,
                    discovered_by=discovered_by,
                    run_id=run_id,
                    logical_path=logical_path,
                    partition_index=partition_index,
                )

                insert_urls(evidence_conn, evidence_id, db_records, run_id=run_id)
                total_urls += len(db_records)

            except Exception as exc:
                error_msg = f"Failed to parse {extracted_path.name}: {exc}"
                LOGGER.warning(error_msg, exc_info=True)
                callbacks.on_log(error_msg, "error")

        # Write process_log entry (forensic audit trail)
        try:
            insert_process_log(
                evidence_conn,
                evidence_id,
                tool_name=self.metadata.name,
                command_line=f"ingestion run_id={run_id}",
                finished_at=datetime.now(timezone.utc).isoformat(),
                exit_code=0,
                output_path=str(output_dir),
                run_id=run_id,
                extractor_version=self.metadata.version,
                record_count=total_urls,
                metadata=json.dumps({
                    "log_entries_parsed": total_entries,
                    "unique_urls_ingested": total_urls,
                    "files_processed": len(files),
                }),
            )
        except Exception as exc:
            LOGGER.warning("Failed to write process_log: %s", exc)

        callbacks.on_log(
            f"Ingestion complete: {total_urls} URLs from {total_entries} log entries",
            "info",
        )
        return {"urls": total_urls}

    # ─────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────

    @staticmethod
    def _build_url_records(
        url_records: List[Dict[str, object]],
        discovered_by: str,
        run_id: str,
        logical_path: str,
        partition_index: Optional[int],
    ) -> List[Dict[str, Any]]:
        """Convert parsed URL dicts to urls-table records."""
        records: List[Dict[str, Any]] = []
        for rec in url_records:
            url = str(rec["url"])
            try:
                parsed = urlparse(url)
                domain = parsed.netloc or None
            except Exception:
                domain = None

            occurrence_count = int(rec.get("occurrence_count", 1))  # type: ignore[arg-type]
            notes_parts = [
                f"context={rec.get('source_context', '')}",
                f"severity={rec.get('severity', '')}",
                f"occurrences={occurrence_count}",
            ]
            first_seen_code = str(rec.get("first_seen", ""))
            last_seen_code = str(rec.get("last_seen", ""))
            if first_seen_code:
                notes_parts.append(f"first_seen_logtime={first_seen_code}")
            if last_seen_code:
                notes_parts.append(f"last_seen_logtime={last_seen_code}")

            records.append({
                "url": url,
                "domain": domain,
                "discovered_by": discovered_by,
                "run_id": run_id,
                "source_path": logical_path,
                "partition_index": partition_index,
                "notes": "; ".join(notes_parts),
            })
        return records

    def _generate_run_id(self) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"emb_artifacts_{timestamp}_{unique_id}"

    @staticmethod
    def _write_manifest(output_dir: Path, manifest: Dict[str, Any]) -> None:
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
