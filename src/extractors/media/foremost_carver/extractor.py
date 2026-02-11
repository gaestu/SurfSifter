"""
Foremost (Image Carving) extractor.

Independent extractor with strict ELT separation and manifest validation.
Supports order-independent enrichment via image_discoveries table.
Records extracted files to extracted_files audit table.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

from PySide6.QtWidgets import QWidget, QLabel
from PIL import Image

from ...base import BaseExtractor, ExtractorMetadata
from ...callbacks import ExtractorCallbacks
from core.logging import get_logger
from core.tool_discovery import discover_tools
from core.manifest import validate_image_carving_manifest, ManifestValidationError
from core.database.helpers.extracted_files import (
    insert_extracted_files_batch,
    delete_extracted_files_by_run,
)
from extractors._shared.carving.worker import (
    run_carving_extraction,
    parse_foremost_audit,
)
from extractors._shared.carving.enrichment import parse_foremost_audit_with_bytes
from extractors._shared.carving.ingestion import run_image_ingestion
from extractors._shared.carving.common import (
    generate_run_id,
    get_evidence_context,
    create_manifest,
)
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.foremost_carver")


class ForemostCarverExtractor(BaseExtractor):
    """Foremost-only image carving extractor."""

    # Default carving configuration (dynamically generates config file)
    DEFAULT_FILE_TYPES = {
        "jpg": {
            "header": "\\xff\\xd8\\xff",
            "footer": "\\xff\\xd9",
            "extension": "jpg",
            "max_size": 100000000,  # 100MB
            "enabled": True,
        },
        "jpeg": {
            "header": "\\xff\\xd8\\xff\\xe0",
            "footer": "\\xff\\xd9",
            "extension": "jpeg",
            "max_size": 100000000,
            "enabled": True,
        },
        "jpe": {
            "header": "\\xff\\xd8\\xff\\xe1",
            "footer": "\\xff\\xd9",
            "extension": "jpe",
            "max_size": 100000000,
            "enabled": True,
        },
        "png": {
            "header": "\\x89\\x50\\x4e\\x47\\x0d\\x0a\\x1a\\x0a",
            "footer": "IEND\\xae\\x42\\x60\\x82",
            "extension": "png",
            "max_size": 100000000,
            "enabled": True,
        },
        "gif": {
            "header": "GIF8",
            "footer": "\\x00\\x3b",
            "extension": "gif",
            "max_size": 50000000,  # 50MB
            "enabled": True,
        },
        "bmp": {
            "header": "BM",
            "footer": "",
            "extension": "bmp",
            "max_size": 50000000,
            "enabled": True,
        },
        "tif": {
            "header": "II*\\x00",
            "footer": "",
            "extension": "tif",
            "max_size": 200000000,  # 200MB
            "enabled": True,
        },
        "tiff": {
            "header": "MM\\x00*",
            "footer": "",
            "extension": "tiff",
            "max_size": 200000000,
            "enabled": True,
        },
        "webp": {
            "header": "RIFF",
            "footer": "WEBP",
            "extension": "webp",
            "max_size": 100000000,
            "enabled": True,
        },
    }

    VERSION = "1.6.0"

    @property
    def metadata(self) -> ExtractorMetadata:
        tools = discover_tools()
        foremost = tools.get("foremost")
        requires_tools = ["foremost"]
        if foremost and foremost.available:
            requires_tools = ["foremost"]

        return ExtractorMetadata(
            name="foremost_carver",
            display_name="Foremost (Image Carving)",
            description="Carve deleted images from unallocated space using foremost.",
            category="media",
            version=self.VERSION,
            requires_tools=requires_tools,
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        tools = discover_tools()
        foremost = tools.get("foremost")
        if not (foremost and foremost.available):
            return False, "Foremost not available. Install foremost to carve images."
        if evidence_fs is None:
            return False, "No evidence filesystem mounted. Please mount evidence first."
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
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
            data = json.loads(manifest.read_text())
            file_count = len(data.get("carved_files", data.get("files", [])))
            tool_obj = data.get("tool") or {}
            tool = tool_obj.get("name") or "foremost"
            ingested = data.get("ingestion", {})
            skipped = ingested.get("skipped_duplicates", 0) if isinstance(ingested, dict) else 0

            # Get command for verbosity
            command_list = data.get("command", [])
            command_str = " ".join(command_list) if isinstance(command_list, list) else str(command_list)

            status_text = (
                "Foremost (Image Carving)\n"
                f"Files carved: {file_count}\n"
                f"Tool: {tool}\n"
                f"Run ID: {data.get('run_id', 'N/A')}\n"
                f"Duplicates skipped: {skipped}\n"
                f"Command: {command_str}"
            )
        else:
            status_text = "Foremost (Image Carving)\nNo extraction run yet"
        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        extractor_name = self.metadata.name
        if config and "extractor_name" in config:
            extractor_name = config["extractor_name"]
        return case_root / "evidences" / evidence_label / extractor_name

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        callbacks.on_step("Initializing Foremost carving")
        start_time = datetime.now(timezone.utc)
        run_id = generate_run_id()
        output_dir.mkdir(parents=True, exist_ok=True)

        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        # Start statistics tracking (may be None in tests)
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Get file types configuration (dynamically generates config file)
        file_types = config.get("file_types", self.DEFAULT_FILE_TYPES)

        tools = discover_tools()
        foremost = tools.get("foremost")
        if not foremost or not foremost.available or not foremost.path:
            callbacks.on_error("Foremost not available")
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        try:
            run_result = run_carving_extraction(
                evidence_fs=evidence_fs,
                output_dir=output_dir,
                carving_tool="foremost",
                tool_path=foremost.path,
                file_types=file_types,
                callbacks=callbacks,
            )
        except Exception as exc:
            callbacks.on_error(f"Carving extraction failed: {exc}")
            LOGGER.exception("Carving extraction failed")
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        audit_entries = parse_foremost_audit(run_result.audit_path) if run_result.audit_path else []
        end_time = datetime.now(timezone.utc)

        try:
            manifest_data = create_manifest(
                extractor_name=self.metadata.name,
                tool_name="foremost",
                tool_version=foremost.version,
                tool_path=foremost.path,
                command=run_result.command,
                run_id=run_id,
                start_time=start_time,
                end_time=end_time,
                input_info={
                    "source": run_result.input_source,
                    "source_type": run_result.input_type,
                    "evidence_id": evidence_id,
                    "context": get_evidence_context(evidence_fs),
                },
                output_dir=output_dir,
                file_types=file_types,
                carved_files=run_result.carved_files,
                returncode=run_result.returncode,
                stdout=run_result.stdout,
                stderr=run_result.stderr,
                audit_entries=audit_entries,
            )
        except ManifestValidationError as exc:
            callbacks.on_error(f"Manifest validation failed: {exc}")
            LOGGER.exception("Manifest validation failed")
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")
        callbacks.on_step(f"Extraction complete, manifest written ({len(run_result.carved_files)} files)")

        # Record extracted files to audit table
        self._record_extracted_files(
            output_dir=output_dir,
            evidence_id=evidence_id,
            run_id=run_id,
            manifest_data=manifest_data,
            audit_entries=audit_entries,
            config=config,
            callbacks=callbacks,
        )

        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(run_result.carved_files))
            stats.finish_run(evidence_id, self.metadata.name, "success")
        return True

    def _record_extracted_files(
        self,
        output_dir: Path,
        evidence_id: int,
        run_id: str,
        manifest_data: Dict[str, Any],
        audit_entries: List[Dict[str, Any]],
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> None:
        """
        Record carved files to extracted_files audit table.

        Foremost provides byte offsets via audit.txt which we record
        for forensic provenance.
        """
        carved_files = manifest_data.get("carved_files", [])
        if not carved_files:
            return

        # Get database connection
        evidence_conn = config.get("evidence_conn")
        if evidence_conn is None:
            callbacks.on_log(
                "Skipping extracted_files audit (no database connection)",
                level="debug"
            )
            return

        try:
            # Build offset lookup from audit entries
            # audit_entries format: [{"filename": "xxx.jpg", "offset": 12345, ...}, ...]
            offset_map = {
                entry.get("filename"): entry.get("offset")
                for entry in audit_entries
                if entry.get("filename")
            }

            # Clean up previous run records if re-extracting
            deleted = delete_extracted_files_by_run(evidence_conn, evidence_id, run_id)
            if deleted > 0:
                callbacks.on_log(
                    f"Cleaned up {deleted} previous extracted_files records",
                    level="debug"
                )

            # Convert carved_files to extracted_files format
            extracted_records = []
            for file_info in carved_files:
                rel_path = file_info.get("rel_path", "")
                filename = Path(rel_path).name if rel_path else ""

                # Get byte offset from audit if available
                offset = offset_map.get(filename) or file_info.get("offset")

                record = {
                    "dest_rel_path": rel_path,
                    "dest_filename": filename,
                    "source_path": None,  # Carved files don't have source path
                    "source_inode": None,
                    "partition_index": None,
                    "source_offset_bytes": offset,
                    "source_block_size": 512,  # foremost default block size
                    "size_bytes": file_info.get("size"),
                    "file_type": file_info.get("file_type"),
                    "md5": file_info.get("md5"),
                    "sha256": file_info.get("sha256"),
                    "status": "ok" if not file_info.get("errors") else "error",
                    "error_message": "; ".join(file_info.get("errors", [])) or None,
                    "metadata_json": json.dumps({
                        "warnings": file_info.get("warnings"),
                        "validated": file_info.get("validated"),
                    }) if file_info.get("warnings") or file_info.get("validated") else None,
                }
                extracted_records.append(record)

            # Batch insert
            count = insert_extracted_files_batch(
                evidence_conn,
                evidence_id,
                self.metadata.name,
                run_id,
                extracted_records,
                extractor_version=self.VERSION,
            )
            evidence_conn.commit()

            callbacks.on_log(
                f"Recorded {count:,} carved files to extracted_files audit table",
                level="debug"
            )
            LOGGER.debug(
                "Recorded %d carved files to audit table (run_id=%s)",
                count, run_id
            )

        except Exception as exc:
            callbacks.on_log(
                f"Failed to record extracted files: {exc}",
                level="warning"
            )
            LOGGER.warning("Failed to record extracted files: %s", exc, exc_info=True)

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        callbacks.on_step("Starting Foremost ingestion")

        manifest_path = output_dir / "manifest.json"
        if not manifest_path.exists():
            callbacks.on_error("No manifest.json found")
            return False

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", "unknown")

        # Start statistics tracking for ingestion - use run_id from manifest
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)
        try:
            validate_image_carving_manifest(manifest_data)
        except ManifestValidationError as exc:
            callbacks.on_error(f"Manifest validation failed: {exc}")
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        # Parse audit.txt to build offset map for enrichment
        audit_path = output_dir / "carved" / "audit.txt"
        offset_map = {}
        if audit_path.exists():
            offset_map = parse_foremost_audit_with_bytes(audit_path)
            LOGGER.info("Parsed %d offset entries from audit.txt", len(offset_map))

        try:
            result = run_image_ingestion(
                output_dir=output_dir,
                evidence_conn=evidence_conn,
                evidence_id=evidence_id,
                manifest_data=manifest_data,
                callbacks=callbacks,
                discovered_by="foremost_carver",
                extractor_version=self.metadata.version,
                offset_map=offset_map,
            )
        except Exception as exc:
            callbacks.on_error(f"Ingestion failed: {exc}")
            LOGGER.exception("Foremost ingestion failed")
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        # Persist ingestion stats into manifest
        try:
            manifest_data["ingestion"] = result
            manifest_path.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")
        except Exception as exc:
            LOGGER.warning("Failed to update manifest ingestion stats: %s", exc)

        callbacks.on_step(
            f"Ingested {result['inserted']} images "
            f"(enriched={result.get('enriched', 0)}, errors={result['errors']})"
        )
        if stats:
            stats.report_ingested(
                evidence_id, self.metadata.name,
                records=result.get("inserted", 0),
                images=result.get("inserted", 0),
                enriched=result.get("enriched", 0)
            )
            stats.finish_run(evidence_id, self.metadata.name, "success")
        return True

