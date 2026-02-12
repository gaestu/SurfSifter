"""
Scalpel (Image Carving) extractor.

Independent extractor with strict ELT separation and manifest validation.
Supports order-independent enrichment via image_discoveries table.
Records extracted files to extracted_files audit table.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
from extractors._shared.carving.worker import run_carving_extraction
from extractors._shared.carving.ingestion import run_image_ingestion
from extractors._shared.carving.common import (
    generate_run_id,
    get_evidence_context,
    create_manifest,
)
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.scalpel")


class ScalpelExtractor(BaseExtractor):
    """Scalpel image carving extractor."""

    # Default config file path (in extractor directory).
    # In frozen builds __file__ resolves inside _MEIPASS which is correct
    # as long as the .conf is bundled as a data file.
    DEFAULT_CONFIG_PATH = Path(__file__).parent / "default.conf"
    VERSION = "1.6.0"

    @property
    def metadata(self) -> ExtractorMetadata:
        tools = discover_tools()
        scalpel = tools.get("scalpel")
        requires_tools = ["scalpel"]
        if scalpel and scalpel.available:
            requires_tools = ["scalpel"]

        return ExtractorMetadata(
            name="scalpel",
            display_name="Scalpel",
            description="Carve deleted images from unallocated space using scalpel.",
            category="media",
            version=self.VERSION,
            requires_tools=requires_tools,
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        tools = discover_tools()
        scalpel = tools.get("scalpel")
        if not (scalpel and scalpel.available):
            return False, "Scalpel not available. Install scalpel to carve images."
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
            tool = tool_obj.get("name") or "scalpel"
            ingested = data.get("ingestion", {})
            skipped = ingested.get("skipped_duplicates", 0) if isinstance(ingested, dict) else 0
            status_text = (
                "Scalpel\n"
                f"Files carved: {file_count}\n"
                f"Tool: {tool}\n"
                f"Run ID: {data.get('run_id', 'N/A')}\n"
                f"Duplicates skipped: {skipped}"
            )
        else:
            status_text = "Scalpel\nNo extraction run yet"
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
        callbacks.on_step("Initializing Scalpel carving")
        start_time = datetime.now(timezone.utc)
        run_id = generate_run_id()
        output_dir.mkdir(parents=True, exist_ok=True)

        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")

        # Start statistics tracking (may be None in tests)
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        config_file = config.get("config_file", self.DEFAULT_CONFIG_PATH)

        # Resolve config file path (support both absolute and relative to project root)
        if not isinstance(config_file, Path):
            config_file = Path(config_file)

        if not config_file.is_absolute():
            # Resolve relative to project root
            import sys as _sys
            if getattr(_sys, "frozen", False):
                project_root = Path(getattr(_sys, "_MEIPASS", "."))
            else:
                project_root = Path(__file__).parent.parent.parent.parent.parent
            config_file = project_root / config_file

        if not config_file.exists():
            callbacks.on_error(f"Carving config file not found: {config_file}")
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        tools = discover_tools()
        scalpel = tools.get("scalpel")
        if not scalpel or not scalpel.available or not scalpel.path:
            callbacks.on_error("Scalpel not available")
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        try:
            run_result = run_carving_extraction(
                evidence_fs=evidence_fs,
                output_dir=output_dir,
                carving_tool="scalpel",
                tool_path=scalpel.path,
                config_file=config_file,
                callbacks=callbacks,
            )
        except Exception as exc:
            callbacks.on_error(f"Carving extraction failed: {exc}")
            LOGGER.exception("Carving extraction failed")
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        end_time = datetime.now(timezone.utc)

        try:
            manifest_data = create_manifest(
                extractor_name=self.metadata.name,
                tool_name="scalpel",
                tool_version=scalpel.version,
                tool_path=scalpel.path,
                command=run_result.command,
                run_id=run_id,
                start_time=start_time,
                end_time=end_time,
                input_info={
                    "source": run_result.input_source,
                    "source_type": run_result.input_type,
                    "evidence_id": evidence_id,
                    "context": get_evidence_context(evidence_fs),
                    "config_file": str(config_file),
                },
                output_dir=output_dir,
                file_types={},
                carved_files=run_result.carved_files,
                returncode=run_result.returncode,
                stdout=run_result.stdout,
                stderr=run_result.stderr,
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
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> None:
        """
        Record carved files to extracted_files audit table.

        Scalpel provides byte offsets via scalpel.log (stored in manifest)
        which we record for forensic provenance.
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

                # Scalpel stores offset directly in carved_files
                offset = file_info.get("offset")

                record = {
                    "dest_rel_path": rel_path,
                    "dest_filename": filename,
                    "source_path": None,  # Carved files don't have source path
                    "source_inode": None,
                    "partition_index": None,
                    "source_offset_bytes": offset,
                    "source_block_size": 512,  # scalpel default block size
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
        callbacks.on_step("Starting Scalpel ingestion")

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

        # Scalpel doesn't produce a foremost-style audit.txt, but we still
        # support enrichment with empty offset_map for future enhancement
        try:
            result = run_image_ingestion(
                output_dir=output_dir,
                evidence_conn=evidence_conn,
                evidence_id=evidence_id,
                manifest_data=manifest_data,
                callbacks=callbacks,
                discovered_by="scalpel",
                extractor_version=self.metadata.version,
                offset_map=None,  # Scalpel doesn't provide audit offsets
            )
        except Exception as exc:
            callbacks.on_error(f"Ingestion failed: {exc}")
            LOGGER.exception("Scalpel ingestion failed")
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

