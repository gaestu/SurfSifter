"""
System Registry Extractor

Extracts Windows registry indicators for forensic analysis (Deep Freeze, kiosk mode, etc.).
Includes StatisticsCollector integration for run tracking.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

from PySide6.QtWidgets import QWidget, QLabel

from ...base import BaseExtractor, ExtractorMetadata
from ...callbacks import ExtractorCallbacks
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.system.registry")


class SystemRegistryExtractor(BaseExtractor):
    """
    Extract Windows registry indicators from offline registry hives.

    Features:
    - Offline registry parsing (no Windows required)
    - SYSTEM and SOFTWARE hive support
    - Rule-based detection (Deep Freeze, kiosk mode, etc.)
    - Forensic provenance tracking
    - StatisticsCollector integration
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        # Check if regipy is available (only needed for ingestion)
        try:
            import regipy  # type: ignore
            requires_tools = []
        except ImportError:
            requires_tools = ["regipy"]

        return ExtractorMetadata(
            name="system_registry",
            display_name="Registry Reader",
            description="Extract Windows registry indicators (Deep Freeze, kiosk mode, etc.)",
            category="system",
            requires_tools=requires_tools,
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        # Extraction (export) does not require regipy
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run."""
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found"

        # Ingestion requires regipy
        try:
            import regipy
        except ImportError:
            return False, "regipy not installed (pip install regipy)"

        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> QWidget:
        """Return configuration widget for hive selection and rules."""
        from .ui import RegistryConfigWidget
        return RegistryConfigWidget(parent)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction state."""
        manifest = output_dir / "manifest.json"
        status_text = "Registry Reader\n"

        if manifest.exists():
            try:
                data = json.loads(manifest.read_text())
                extracted_count = len(data.get("extracted_hives", []))
                run_id = data.get("run_id", "N/A")
                timestamp = data.get("timestamp", "")

                try:
                    ts = datetime.fromisoformat(timestamp).strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    ts = timestamp

                status_text += (
                    f"Hives Exported: {extracted_count}\n"
                    f"Last Run: {ts}\n"
                    f"Run ID: {run_id}"
                )
            except Exception:
                status_text += "Error reading manifest"
        else:
            status_text += "No extraction run yet"

        # Check for ingestion summary
        summary_path = output_dir / "ingestion_registry.json"
        if summary_path.exists():
            try:
                summary = json.loads(summary_path.read_text())
                findings = summary.get("findings_count", 0)
                rules = summary.get("rules_matched", 0)
                status_text += f"\n\nIngestion:\nFindings: {findings}\nRules Matched: {rules}"
            except Exception:
                pass

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "registry"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract registry hives from evidence.

        Workflow:
            1. Generate run_id
            2. Scan for standard machine and user hives
            3. Copy hive files to output directory
            4. Write manifest.json with file metadata
        """
        from .worker import run_registry_extraction

        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        run_id = self._generate_run_id()

        # Start statistics tracking
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        callbacks.on_step("Starting registry extraction")

        try:
            result = run_registry_extraction(
                evidence_fs=evidence_fs,
                output_dir=output_dir,
                config=config,
                callbacks=callbacks,
            )

            hive_count = result.get('extracted_hives', 0)

            # Record extracted files to audit table
            manifest_path = output_dir / "manifest.json"
            if manifest_path.exists():
                import json
                manifest_data = json.loads(manifest_path.read_text())
                # Registry uses 'extracted_hives' instead of 'files'
                if "extracted_hives" in manifest_data and "files" not in manifest_data:
                    manifest_data["files"] = manifest_data["extracted_hives"]
                from extractors._shared.extracted_files_audit import record_browser_files
                record_browser_files(
                    evidence_conn=config.get("evidence_conn"),
                    evidence_id=evidence_id,
                    run_id=result.get("run_id", run_id),
                    extractor_name=self.metadata.name,
                    extractor_version=self.metadata.version,
                    manifest_data=manifest_data,
                    callbacks=callbacks,
                )

            if stats:
                stats.report_discovered(evidence_id, self.metadata.name, hives=hive_count)
                stats.finish_run(evidence_id, self.metadata.name, "ok")

        except Exception as e:
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "error")
            callbacks.on_error(f"Registry extraction failed: {e}")
            LOGGER.exception("Registry extraction failed")
            return False

        callbacks.on_step(f"Registry extraction complete: {hive_count} hives extracted")
        LOGGER.info("Registry extraction complete (run_id=%s, hives=%d)", result['run_id'], hive_count)

        return True

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Ingest registry findings into database (Analysis + Load).

        Workflow:
            1. Read manifest.json to identify exported hives
            2. Apply registry_offline.yml rules to local hive files
            3. Insert findings to os_indicators table
        """
        from .ingestion import run_registry_ingestion

        evidence_label = config.get("evidence_label", "")

        # Continue statistics tracking (run_id populated after manifest read)
        stats = StatisticsCollector.instance()

        callbacks.on_step("Starting registry ingestion")

        # Read manifest
        manifest_path = output_dir / "manifest.json"
        if not manifest_path.exists():
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "error")
            callbacks.on_error("No manifest.json found")
            return False

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())

        # Continue statistics tracking from extraction phase (same extractor name)
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        try:
            ingestion_config = dict(config)
            ingestion_config["run_id"] = run_id
            result = run_registry_ingestion(
                manifest_data=manifest_data,
                evidence_conn=evidence_conn,
                evidence_id=evidence_id,
                callbacks=callbacks,
                output_dir=output_dir,
                config=ingestion_config,
            )

            inserted = result.get('inserted', 0)
            errors = result.get('errors', 0)

            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, indicators=inserted)
                if errors > 0:
                    stats.report_failed(evidence_id, self.metadata.name, indicators=errors)
                status = "ok" if errors == 0 else "partial"
                stats.finish_run(evidence_id, self.metadata.name, status)

        except Exception as e:
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "error")
            callbacks.on_error(f"Registry ingestion failed: {e}")
            LOGGER.exception("Registry ingestion failed")
            return False

        callbacks.on_step(f"Ingested {inserted} findings ({result['errors']} errors)")
        LOGGER.info("Registry ingestion complete (inserted=%d, errors=%d)", inserted, result['errors'])

        return True

    def _generate_run_id(self) -> str:
        """Generate unique run ID: timestamp + UUID4 prefix."""
        import uuid
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        return f"{ts}_{uid}"
