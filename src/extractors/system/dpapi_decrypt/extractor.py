"""
DPAPI Decrypt Extractor

Extracts DPAPI-protected files from evidence and decrypts Chromium browser
secrets (passwords, cookies, credit cards) using offline DPAPI techniques.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from PySide6.QtWidgets import QWidget, QLabel

from ...base import BaseExtractor, ExtractorMetadata
from ...callbacks import ExtractorCallbacks
from core.logging import get_logger
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.system.dpapi_decrypt")


class SystemDpapiDecryptExtractor(BaseExtractor):
    """
    Extract and decrypt DPAPI-protected browser secrets from offline evidence.

    Extraction phase:
        - Collects SYSTEM/SAM/SECURITY hives, DPAPI master key files,
          and Chromium Local State files from evidence.

    Ingestion phase:
        - Derives boot key, extracts NTLM hashes, decrypts master keys.
        - Unwraps Chromium AES keys and decrypts v10-encrypted blobs.
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="system_dpapi_decrypt",
            display_name="DPAPI Decrypt",
            description="Decrypt DPAPI-protected browser secrets (passwords, cookies, credit cards)",
            category="system",
            requires_tools=[],
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found"
        try:
            import regipy  # noqa: F401
        except ImportError:
            return False, "regipy not installed (pip install regipy)"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> QWidget:
        from .ui import DPAPIDecryptConfigWidget
        return DPAPIDecryptConfigWidget(parent)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
    ) -> QWidget:
        manifest = output_dir / "manifest.json"
        status_text = "DPAPI Decrypt\n"

        if manifest.exists():
            try:
                data = json.loads(manifest.read_text())
                user_count = len(data.get("users", []))
                mk_count = sum(
                    len(u.get("master_keys", []))
                    for u in data.get("users", [])
                )
                run_id = data.get("run_id", "N/A")
                timestamp = data.get("timestamp", "")
                try:
                    ts = datetime.fromisoformat(timestamp).strftime("%Y-%m-%d %H:%M")
                except ValueError:
                    ts = timestamp

                status_text += (
                    f"Users Found: {user_count}\n"
                    f"Master Keys: {mk_count}\n"
                    f"Last Run: {ts}\n"
                    f"Run ID: {run_id}"
                )
            except Exception:
                status_text += "Error reading manifest"
        else:
            status_text += "No extraction run yet"

        # Check for ingestion summary
        summary_path = output_dir / "ingestion_dpapi.json"
        if summary_path.exists():
            try:
                summary = json.loads(summary_path.read_text())
                decrypted = summary.get("decrypted", 0)
                failed = summary.get("failed", 0)
                status_text += (
                    f"\n\nDecryption:\n"
                    f"Decrypted: {decrypted}\n"
                    f"Failed: {failed}"
                )
            except Exception:
                pass

        return QLabel(status_text, parent)

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        return case_root / "evidences" / evidence_label / "dpapi_decrypt"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        from ._collector import collect_dpapi_evidence

        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        run_id = self._generate_run_id()

        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        callbacks.on_step("Starting DPAPI evidence collection")
        output_dir.mkdir(parents=True, exist_ok=True)

        try:
            collected = collect_dpapi_evidence(
                evidence_fs=evidence_fs,
                output_dir=output_dir,
                run_id=run_id,
                callbacks=callbacks,
            )

            # Build manifest
            manifest_data = collected.to_manifest(self.metadata.version)
            manifest_path = output_dir / "manifest.json"
            manifest_path.write_text(json.dumps(manifest_data, indent=2))

            mk_count = sum(
                len(u.get("master_keys", []))
                for u in manifest_data.get("users", [])
            )

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

            if stats:
                stats.report_discovered(
                    evidence_id, self.metadata.name, master_keys=mk_count
                )
                stats.finish_run(evidence_id, self.metadata.name, "ok")

        except Exception as e:
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "error")
            callbacks.on_error(f"DPAPI collection failed: {e}")
            LOGGER.exception("DPAPI collection failed")
            return False

        callbacks.on_step(
            f"DPAPI collection complete: {len(collected.users)} users, "
            f"{mk_count} master keys"
        )
        LOGGER.info(
            "DPAPI collection complete (run_id=%s, users=%d, master_keys=%d)",
            run_id, len(collected.users), mk_count,
        )
        return True

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        from ._pipeline import run_dpapi_pipeline

        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()

        callbacks.on_step("Starting DPAPI decryption pipeline")

        manifest_path = output_dir / "manifest.json"
        if not manifest_path.exists():
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "error")
            callbacks.on_error("No manifest.json found")
            return False

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id", self._generate_run_id())

        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        try:
            ingestion_config = dict(config)
            ingestion_config["run_id"] = run_id
            result = run_dpapi_pipeline(
                output_dir=output_dir,
                evidence_conn=evidence_conn,
                evidence_id=evidence_id,
                config=ingestion_config,
                callbacks=callbacks,
            )

            decrypted = result.get("decrypted", 0)
            failed = result.get("failed", 0)

            # Write ingestion summary
            summary_path = output_dir / "ingestion_dpapi.json"
            summary_path.write_text(json.dumps(result, indent=2))

            if stats:
                stats.report_ingested(
                    evidence_id, self.metadata.name, credentials=decrypted
                )
                if failed > 0:
                    stats.report_failed(
                        evidence_id, self.metadata.name, credentials=failed
                    )
                status = "ok" if failed == 0 else "partial"
                stats.finish_run(evidence_id, self.metadata.name, status)

        except Exception as e:
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "error")
            callbacks.on_error(f"DPAPI decryption pipeline failed: {e}")
            LOGGER.exception("DPAPI decryption pipeline failed")
            return False

        callbacks.on_step(
            f"DPAPI decryption complete: {decrypted} decrypted, {failed} failed"
        )
        LOGGER.info(
            "DPAPI decryption complete (decrypted=%d, failed=%d)", decrypted, failed
        )
        return True

    def _generate_run_id(self) -> str:
        import uuid
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        return f"{ts}_{uid}"
