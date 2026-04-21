"""SwiftBeaver carver extractor implementation."""

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Optional, List
from urllib.parse import urlparse
import subprocess
import os
import time
import uuid

from PySide6.QtWidgets import QWidget

from ...base import BaseExtractor, ExtractorMetadata, ExtractorCallbacks
from core.tool_discovery import discover_tools
from core.logging import get_logger
from core.subprocess_env import clean_subprocess_env
from core.database import insert_urls, delete_discoveries_by_run
from core.database.helpers.process_log import insert_process_log
from core.manifest import validate_image_carving_manifest, ManifestValidationError
from extractors._shared.carving.processor import ParallelImageProcessor
from extractors._shared.carving.enrichment import ingest_with_enrichment
from extractors._shared.extracted_files_audit import record_carved_files
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.swiftbeaver")

# Default configuration
DEFAULT_IMAGE_TYPES = ["jpeg", "png", "gif", "webp", "bmp", "tiff", "heic", "ico"]
DEFAULT_MIN_SIZE_BYTES = 4096  # 4 KB
DEFAULT_SCAN_URLS = True

# SwiftBeaver output file locations (inside run directory)
METADATA_DIR = "metadata"
CARVED_DIR = "carved"
CARVED_FILES_JSONL = "carved_files.jsonl"
STRING_ARTEFACTS_JSONL = "string_artefacts.jsonl"
RUN_SUMMARY_JSONL = "run_summary.jsonl"


def find_latest_run_dir(output_dir: Path) -> Optional[Path]:
    """Return the most recent SwiftBeaver run directory under ``output_dir``.

    A run directory is any direct subdirectory that contains a ``metadata/``
    folder. Sorting is by directory name (SwiftBeaver uses timestamped names).
    """
    if not output_dir.exists():
        return None
    run_dirs = sorted(
        [d for d in output_dir.iterdir() if d.is_dir() and (d / METADATA_DIR).exists()],
        key=lambda d: d.name,
        reverse=True,
    )
    return run_dirs[0] if run_dirs else None


class SwiftbeaverExtractor(BaseExtractor):
    """
    SwiftBeaver forensic carver extractor.

    Dual-phase workflow:
    1. Extraction: Run SwiftBeaver subprocess (reads E01 natively via libewf)
       - Produces carved images + JSONL metadata
       - No database writes, just file generation

    2. Ingestion: Parse JSONL metadata and load into database
       - Carved images → images table (with phash/EXIF enrichment)
       - String artefacts (URLs) → urls table
       - Can be re-run with different filters without re-extracting

    Configuration:
        - image_types: List[str] (default: all supported types)
        - scan_urls: bool (default: True)
        - num_workers: int (default: auto-detect)
        - min_size_bytes: int (default: 4096)
        - output_reuse_policy: "reuse" | "overwrite"
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="swiftbeaver",
            display_name="SwiftBeaver (Images, URLs)",
            description="Carve images and extract URLs using SwiftBeaver",
            category="forensic",
            requires_tools=["swiftbeaver"],
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_source_path: Path) -> tuple[bool, str]:
        """
        Check if SwiftBeaver tool is available and evidence source exists.

        Args:
            evidence_source_path: Path to E01/EWF file or evidence source

        Returns:
            (can_run, reason) tuple
        """
        tools = discover_tools()
        tool = tools.get("swiftbeaver")

        if not tool or not tool.available:
            return False, "swiftbeaver not installed or not in PATH"

        if evidence_source_path is None:
            return False, "No evidence source specified"

        if not evidence_source_path.exists():
            return False, f"Evidence source not found: {evidence_source_path}"

        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if SwiftBeaver output files exist."""
        if not output_dir.exists():
            return False, "Output directory does not exist"

        # Check for at least one run directory with metadata
        run_dir = self._find_latest_run_dir(output_dir)
        if not run_dir:
            return False, "No SwiftBeaver run directories found with metadata"

        metadata_dir = run_dir / METADATA_DIR
        has_carved = (metadata_dir / CARVED_FILES_JSONL).exists()
        has_strings = (metadata_dir / STRING_ARTEFACTS_JSONL).exists()

        if not has_carved and not has_strings:
            return False, "No carved_files.jsonl or string_artefacts.jsonl found"

        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """
        Check if output directory has existing SwiftBeaver run directories.

        Args:
            output_dir: Output directory to check

        Returns:
            True if run directories exist, False otherwise
        """
        if not output_dir.exists():
            return False

        return self._find_latest_run_dir(output_dir) is not None

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return config widget for SwiftBeaver configuration."""
        from .config_widget import SwiftbeaverConfigWidget
        return SwiftbeaverConfigWidget(parent)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction status and ingestion options."""
        from .status_widget import SwiftbeaverStatusWidget
        return SwiftbeaverStatusWidget(
            parent,
            output_dir,
            evidence_conn,
            evidence_id
        )

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """
        Return output directory for SwiftBeaver files.

        Convention: {case_root}/evidences/{evidence_label}/swiftbeaver/
        """
        return case_root / "evidences" / evidence_label / "swiftbeaver"

    def run_extraction(
        self,
        evidence_source_path: Path,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Run SwiftBeaver subprocess to carve images and extract URLs.

        This is the "extraction" phase - runs the tool, no database writes.

        Args:
            evidence_source_path: Path to E01/EWF file or evidence source
            output_dir: Output directory for SwiftBeaver files
            config: Configuration dict with:
                - image_types: List[str] (default: all types)
                - scan_urls: bool (default: True)
                - num_workers: int (default: auto-detect)
                - min_size_bytes: int (default: 4096)
                - output_reuse_policy: str (default: "reuse")
            callbacks: Progress/log callbacks

        Returns:
            True if successful, False otherwise
        """
        callbacks.on_step("Preparing SwiftBeaver extraction")

        # Start statistics tracking
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        run_id = self._generate_run_id()
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Get config
        image_types = config.get("image_types", DEFAULT_IMAGE_TYPES)
        scan_urls = config.get("scan_urls", DEFAULT_SCAN_URLS)
        num_workers = config.get("num_workers")
        min_size_bytes = config.get("min_size_bytes", DEFAULT_MIN_SIZE_BYTES)
        reuse_policy = config.get("output_reuse_policy", "reuse")

        # Auto-detect workers
        if num_workers is None:
            cpu_count = os.cpu_count() or 4
            num_workers = max(1, min(cpu_count - 2, 16))

        callbacks.on_log(
            f"Configuration: {num_workers} workers, types: {', '.join(image_types)}, "
            f"scan_urls: {scan_urls}, min_size: {min_size_bytes} bytes",
            "info"
        )

        # Check output reuse policy
        if output_dir.exists() and reuse_policy == "reuse":
            run_dir = self._find_latest_run_dir(output_dir)
            if run_dir:
                callbacks.on_log(
                    f"✓ Reusing existing SwiftBeaver output (run: {run_dir.name})",
                    "info"
                )
                callbacks.on_step("Using existing output (reuse policy)")
                if stats:
                    stats.complete_run(self.metadata.name, evidence_id, "skipped")
                return True

        # Prepare output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get tool
        tools = discover_tools()
        tool = tools.get("swiftbeaver")
        if not tool or not tool.available:
            callbacks.on_error("swiftbeaver not available", "Tool not found in PATH")
            if stats:
                stats.complete_run(self.metadata.name, evidence_id, "failed", error="Tool not available")
            return False

        input_path = str(evidence_source_path)
        callbacks.on_log(f"Input: {input_path}", "info")

        # Build command
        cmd = [
            str(tool.path),
            "--input", str(evidence_source_path),
            "--output", str(output_dir),
            "--metadata-backend", "jsonl",
            "--types", ",".join(image_types),
            "--hash-algorithms", "md5,sha256",
            "--dedupe", "--skip-duplicates",
            "--validate-carved", "--remove-invalid",
            "--workers", str(num_workers),
            "--progress-interval-secs", "2",
            "--log-format", "json",
        ]
        if scan_urls:
            cmd.extend(["--scan-strings", "--scan-urls", "--no-scan-emails", "--no-scan-phones"])
        else:
            cmd.append("--no-scan-strings")

        callbacks.on_log(f"Running: {' '.join(cmd)}", "info")
        callbacks.on_step(f"Running SwiftBeaver ({num_workers} workers)")

        try:
            start_time = time.time()
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                env=clean_subprocess_env(),
            )

            for line in process.stdout:
                if callbacks.is_cancelled():
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                    callbacks.on_log("SwiftBeaver cancelled by user", "warning")
                    if stats:
                        stats.complete_run(self.metadata.name, evidence_id, "cancelled")
                    return False

                line = line.strip()
                if not line:
                    continue

                try:
                    log_entry = json.loads(line)
                    if "progress_percent" in log_entry:
                        try:
                            percent = float(log_entry["progress_percent"])
                            callbacks.on_progress(int(percent), 100, f"Processing... {percent:.1f}%")
                        except (TypeError, ValueError):
                            pass
                    if "message" in log_entry:
                        callbacks.on_log(
                            log_entry["message"],
                            log_entry.get("level", "info").lower()
                        )
                except json.JSONDecodeError:
                    callbacks.on_log(line, "debug")

            stderr_str = process.stderr.read()
            exit_code = process.wait()
            duration = time.time() - start_time

            if exit_code != 0:
                log_path = output_dir / "swiftbeaver_stderr.log"
                try:
                    log_path.write_text(stderr_str or "", encoding="utf-8")
                except Exception:
                    pass
                callbacks.on_error(
                    f"SwiftBeaver failed with exit code {exit_code}",
                    stderr_str
                )
                if stats:
                    stats.complete_run(
                        self.metadata.name, evidence_id, "failed",
                        error=f"Exit code {exit_code}"
                    )
                return False

            callbacks.on_log(f"✓ SwiftBeaver completed in {duration:.1f}s", "info")

            # Write extraction info for audit (read during ingestion)
            extraction_info = {
                "command": cmd,
                "exit_code": exit_code,
                "duration_secs": round(duration, 2),
                "started_at": datetime.fromtimestamp(start_time, tz=timezone.utc).replace(microsecond=0).isoformat(),
                "finished_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                "evidence_source": str(evidence_source_path),
                "run_id": run_id,
            }
            extraction_info_path = output_dir / "extraction_info.json"
            try:
                extraction_info_path.write_text(
                    json.dumps(extraction_info, indent=2), encoding="utf-8"
                )
            except Exception as e:
                LOGGER.warning("Failed to write extraction_info.json: %s", e)

            # Report discovered files
            run_dir = self._find_latest_run_dir(output_dir)
            discovered_count = 0
            if run_dir and (run_dir / CARVED_DIR).exists():
                discovered_count = sum(1 for _ in (run_dir / CARVED_DIR).rglob("*") if _.is_file())

            if stats:
                stats.complete_run(
                    self.metadata.name,
                    evidence_id,
                    "success",
                    discovered={"files": discovered_count}
                )
            return True

        except Exception as e:
            callbacks.on_error(f"Extraction failed: {e}", str(e))
            if stats:
                stats.complete_run(self.metadata.name, evidence_id, "failed", error=str(e))
            return False

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """
        Parse SwiftBeaver JSONL output and load into database.

        This is the "ingestion" phase - reads JSONL metadata, writes to DB.

        Args:
            output_dir: Output directory with SwiftBeaver run directories
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID
            config: Configuration dict with:
                - import_images: bool (default: True)
                - import_urls: bool (default: True)
                - min_size_bytes: int (default: 4096)
                - overwrite_mode: str (optional) "overwrite", "append", or "cancel"
            callbacks: Progress/log callbacks

        Returns:
            Dict with counts: {"images": {...}, "urls": N}
        """
        callbacks.on_step("Checking existing data")

        # Capture start time for accurate process_log entry
        started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        # Continue statistics tracking from extraction phase. Reuse the
        # extraction-phase run_id when available so process_log entries and
        # discovery records correlate across phases.
        evidence_label = config.get("evidence_label", "")
        run_id = self._read_extraction_run_id(output_dir) or self._generate_run_id()
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        import_images = config.get("import_images", True)
        import_urls = config.get("import_urls", True)
        min_size_bytes = config.get("min_size_bytes", DEFAULT_MIN_SIZE_BYTES)

        # Find latest run directory
        run_dir = self._find_latest_run_dir(output_dir)
        if not run_dir:
            callbacks.on_error("No SwiftBeaver run directory found", "")
            if stats:
                stats.complete_run(self.metadata.name, evidence_id, "failed", error="No run directory")
            return {}

        callbacks.on_log(f"Using run directory: {run_dir.name}", "info")
        metadata_dir = run_dir / METADATA_DIR

        # Check existing data
        existing_counts = self._check_existing_data(evidence_conn, evidence_id)
        total_existing = sum(existing_counts.values())

        if total_existing > 0:
            overwrite_mode = config.get("overwrite_mode")

            if not overwrite_mode:
                callbacks.on_log(
                    f"⚠️ Found {total_existing:,} existing SwiftBeaver artifacts. "
                    "Ingestion cancelled (no overwrite mode specified).",
                    "warning"
                )
                if stats:
                    stats.complete_run(self.metadata.name, evidence_id, "skipped")
                return {}

            if overwrite_mode == "cancel":
                callbacks.on_log("Ingestion cancelled by user", "warning")
                if stats:
                    stats.complete_run(self.metadata.name, evidence_id, "cancelled")
                return {}

            elif overwrite_mode == "overwrite":
                callbacks.on_step("Removing existing SwiftBeaver data")
                callbacks.on_log(
                    f"Removing {total_existing:,} existing SwiftBeaver artifacts...",
                    "info"
                )
                self._delete_swiftbeaver_data(evidence_conn, evidence_id)
                callbacks.on_log("✓ Existing data removed", "info")

            elif overwrite_mode == "append":
                callbacks.on_log(
                    f"Appending to {total_existing:,} existing artifacts",
                    "info"
                )

        results: Dict[str, Any] = {}

        # Determine phases
        has_carved = (metadata_dir / CARVED_FILES_JSONL).exists()
        has_strings = (metadata_dir / STRING_ARTEFACTS_JSONL).exists()
        total_phases = sum([
            import_images and has_carved,
            import_urls and has_strings,
            1,  # finalize
        ])
        current_phase = 0

        # Phase: Ingest carved images
        if import_images and has_carved:
            current_phase += 1
            callbacks.on_step(f"Phase {current_phase}/{total_phases}: Processing carved images")
            image_stats = self._ingest_carved_images(
                run_dir, metadata_dir, evidence_conn, evidence_id,
                run_id, min_size_bytes, callbacks
            )
            results["images"] = image_stats
        elif import_images and not has_carved:
            callbacks.on_log("No carved_files.jsonl found — skipping image import", "info")

        # Phase: Ingest URLs
        if import_urls and has_strings:
            current_phase += 1
            callbacks.on_step(f"Phase {current_phase}/{total_phases}: Importing URLs")
            url_count = self._ingest_urls(
                metadata_dir, evidence_conn, evidence_id, run_id, callbacks
            )
            results["urls"] = url_count
        elif import_urls and not has_strings:
            callbacks.on_log("No string_artefacts.jsonl found — skipping URL import", "info")

        # Finalize
        current_phase += 1
        callbacks.on_step(f"Phase {current_phase}/{total_phases}: Finalizing")

        # Write process log entry
        total_records = 0
        for v in results.values():
            if isinstance(v, int):
                total_records += v
            elif isinstance(v, dict):
                total_records += v.get("inserted", 0)

        # Write extraction-phase process_log if extraction_info.json exists
        extraction_info_path = output_dir / "extraction_info.json"
        if extraction_info_path.exists():
            try:
                ext_info = json.loads(extraction_info_path.read_text(encoding="utf-8"))
                insert_process_log(
                    evidence_conn, evidence_id,
                    tool_name="swiftbeaver",
                    command_line=" ".join(ext_info.get("command", [])),
                    started_at=ext_info.get("started_at", started_at),
                    finished_at=ext_info.get("finished_at", started_at),
                    exit_code=ext_info.get("exit_code", 0),
                    output_path=str(output_dir),
                    run_id=run_id,
                    extractor_name=self.metadata.name,
                    extractor_version=self.metadata.version,
                    record_count=0,
                )
            except Exception as e:
                LOGGER.warning("Failed to read extraction_info.json: %s", e)

        # Write ingestion-phase process_log
        insert_process_log(
            evidence_conn, evidence_id,
            tool_name="swiftbeaver",
            command_line=f"swiftbeaver ingestion from {run_dir.name}",
            started_at=started_at,
            finished_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            exit_code=0,
            output_path=str(output_dir),
            run_id=run_id,
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
            record_count=total_records,
        )

        callbacks.on_log(f"🎉 Ingestion complete! Total artifacts: {total_records:,}", "info")

        # Report stats
        if stats:
            ingested_counts = {}
            for artifact_type, count in results.items():
                if isinstance(count, int):
                    ingested_counts[artifact_type] = count
                elif isinstance(count, dict):
                    ingested_counts["images"] = count.get("inserted", 0)

            stats.complete_run(
                self.metadata.name,
                evidence_id,
                "success",
                ingested=ingested_counts
            )

        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _generate_run_id(self) -> str:
        """Generate unique run ID: timestamp + UUID4 prefix."""
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        return f"{ts}_{uid}"

    def _find_latest_run_dir(self, output_dir: Path) -> Optional[Path]:
        """Find the latest SwiftBeaver run directory."""
        return find_latest_run_dir(output_dir)

    def _check_existing_data(
        self,
        evidence_conn,
        evidence_id: int,
        artifact_types: Optional[List[str]] = None,  # noqa: ARG002 — accepted for API compat with extraction tab
    ) -> Dict[str, int]:
        """Check how many SwiftBeaver artifacts already exist in database."""
        counts = {}
        cursor = evidence_conn.cursor()

        # Check URLs
        cursor.execute(
            "SELECT COUNT(*) FROM urls WHERE evidence_id = ? AND discovered_by LIKE 'swiftbeaver%'",
            (evidence_id,)
        )
        url_count = cursor.fetchone()[0]
        if url_count > 0:
            counts["url"] = url_count

        # Check image discoveries
        cursor.execute(
            "SELECT COUNT(*) FROM image_discoveries WHERE evidence_id = ? AND discovered_by LIKE 'swiftbeaver%'",
            (evidence_id,)
        )
        img_count = cursor.fetchone()[0]
        if img_count > 0:
            counts["images"] = img_count

        return counts

    def _delete_swiftbeaver_data(self, evidence_conn, evidence_id: int):
        """Delete all SwiftBeaver artifacts from database."""
        cursor = evidence_conn.cursor()
        cursor.execute(
            "DELETE FROM urls WHERE evidence_id = ? AND discovered_by LIKE 'swiftbeaver%'",
            (evidence_id,)
        )
        cursor.execute(
            "DELETE FROM image_discoveries WHERE evidence_id = ? AND discovered_by LIKE 'swiftbeaver%'",
            (evidence_id,)
        )
        evidence_conn.commit()

    def _read_extraction_run_id(self, output_dir: Path) -> Optional[str]:
        """Read the run_id persisted by run_extraction, if available."""
        info_path = output_dir / "extraction_info.json"
        if not info_path.exists():
            return None
        try:
            data = json.loads(info_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            LOGGER.warning("Failed to read extraction_info.json: %s", exc)
            return None
        run_id = data.get("run_id")
        return run_id if isinstance(run_id, str) and run_id else None

    def _ingest_carved_images(
        self,
        run_dir: Path,
        metadata_dir: Path,
        evidence_conn,
        evidence_id: int,
        run_id: str,
        min_size_bytes: int,
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, int]:
        """
        Ingest carved images from SwiftBeaver JSONL metadata.

        Parses carved_files.jsonl, processes images with ParallelImageProcessor
        for phash/EXIF/thumbnail, then ingests via ingest_with_enrichment using
        pre-computed hashes from SwiftBeaver.

        Returns:
            Dict with ingestion stats: {"inserted": N, "enriched": M, "errors": E}
        """
        carved_jsonl = metadata_dir / CARVED_FILES_JSONL
        if not carved_jsonl.exists():
            callbacks.on_log("No carved_files.jsonl found", "info")
            return {"inserted": 0, "enriched": 0, "errors": 0}

        # Parse JSONL and collect image paths + pre-computed hashes
        image_paths: List[Path] = []
        precomputed_hashes: Dict[str, Dict[str, Any]] = {}
        skipped_small = 0
        parse_errors = 0

        callbacks.on_log(f"Parsing {carved_jsonl.name}...", "info")

        with open(carved_jsonl, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    parse_errors += 1
                    continue

                # Filter by min size
                if entry.get("size", 0) < min_size_bytes:
                    skipped_small += 1
                    continue

                # Resolve file path (with path traversal protection)
                file_path_value = entry.get("file_path")
                if not file_path_value or not isinstance(file_path_value, str):
                    parse_errors += 1
                    continue
                carved_path = (run_dir / file_path_value).resolve()
                if not carved_path.is_relative_to(run_dir.resolve()):
                    LOGGER.warning("Path traversal rejected: %s", file_path_value)
                    continue
                if not carved_path.exists():
                    LOGGER.warning("Carved file not found: %s", carved_path)
                    continue

                precomputed_hashes[str(carved_path)] = {
                    "md5": entry.get("md5"),
                    "sha256": entry.get("sha256"),
                    "file_type": entry.get("file_type"),
                    "size_bytes": entry.get("size"),
                    "carved_offset_bytes": entry.get("global_start"),
                }
                image_paths.append(carved_path)

        if skipped_small:
            callbacks.on_log(f"Skipped {skipped_small} images below {min_size_bytes} bytes", "info")
        if parse_errors:
            callbacks.on_log(f"Skipped {parse_errors} unparseable JSONL lines", "warning")

        if not image_paths:
            callbacks.on_log("No images to process after filtering", "info")
            return {"inserted": 0, "enriched": 0, "errors": 0}

        callbacks.on_log(f"Processing {len(image_paths)} carved images", "info")

        # Capture start time for manifest
        processing_started_at = datetime.now(timezone.utc).isoformat()

        # Process images for phash, EXIF, thumbnails
        processor = ParallelImageProcessor(enable_parallel=True)
        results = processor.process_images(image_paths, run_dir)

        # Clean up previous run discoveries if re-ingesting
        deleted = delete_discoveries_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            callbacks.on_log(f"Cleaned up {deleted} previous discovery records")

        inserted = 0
        enriched = 0
        error_count = 0

        for i, result in enumerate(results):
            if callbacks.is_cancelled():
                callbacks.on_log("Image ingestion cancelled by user", "warning")
                break

            if (i + 1) % 100 == 0 or i == len(results) - 1:
                callbacks.on_progress(i + 1, len(results), f"Image {i + 1}/{len(results)}")

            if result.error is not None:
                error_count += 1
                LOGGER.warning("Skipping failed image %s: %s", result.path, result.error)
                continue

            try:
                # Use pre-computed hashes from SwiftBeaver (skip re-hashing)
                precomputed = precomputed_hashes.get(str(result.path), {})
                if precomputed.get("sha256"):
                    result.sha256 = precomputed["sha256"]
                if precomputed.get("md5"):
                    result.md5 = precomputed["md5"]

                record = result.to_db_record("swiftbeaver")
                record["file_type"] = precomputed.get("file_type", result.path.suffix.lstrip(".").lower())
                record["size_bytes"] = precomputed.get("size_bytes", result.size_bytes)

                image_id, was_inserted = ingest_with_enrichment(
                    conn=evidence_conn,
                    evidence_id=evidence_id,
                    image_data=record,
                    discovered_by="swiftbeaver",
                    run_id=run_id,
                    extractor_version=self.metadata.version,
                    carved_offset_bytes=precomputed.get("carved_offset_bytes"),
                    carved_tool_output=str(result.path.relative_to(run_dir)),
                )

                if was_inserted:
                    inserted += 1
                else:
                    enriched += 1

            except Exception as e:
                error_count += 1
                LOGGER.warning("Error ingesting %s: %s", result.path, e)

        evidence_conn.commit()

        # Write manifest
        manifest = self._build_image_manifest(
            run_id, run_dir, image_paths, inserted, error_count, enriched,
            processing_started_at,
            precomputed_hashes=precomputed_hashes,
            evidence_id=evidence_id,
        )
        manifest_path = run_dir / "swiftbeaver_images_manifest.json"
        try:
            validate_image_carving_manifest(manifest)
        except ManifestValidationError as exc:
            callbacks.on_log(f"Manifest validation warnings: {exc}", "warning")

        manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        # Record carved files to extracted_files audit table
        record_carved_files(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            run_id=run_id,
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
            manifest_data=manifest,
            callbacks=callbacks,
            files_key="carved_files",
        )

        callbacks.on_log(
            f"✅ Image ingestion complete (inserted={inserted}, enriched={enriched}, errors={error_count})",
            "info"
        )
        return {"inserted": inserted, "enriched": enriched, "errors": error_count}

    def _ingest_urls(
        self,
        metadata_dir: Path,
        evidence_conn,
        evidence_id: int,
        run_id: str,
        callbacks: ExtractorCallbacks,
    ) -> int:
        """
        Ingest URLs from SwiftBeaver string_artefacts.jsonl.

        Parses JSONL entries where artefact_kind == "url" and batch-inserts
        into the urls table.

        Returns:
            Count of inserted URL records.
        """
        artefacts_jsonl = metadata_dir / STRING_ARTEFACTS_JSONL
        if not artefacts_jsonl.exists():
            callbacks.on_log("No string_artefacts.jsonl found", "info")
            return 0

        callbacks.on_log(f"Parsing {artefacts_jsonl.name}...", "info")

        url_batch: List[Dict[str, Any]] = []
        total_inserted = 0
        line_count = 0
        skipped = 0

        with open(artefacts_jsonl, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                line_count += 1
                if line_count % 10000 == 0:
                    callbacks.on_log(
                        f"  Processed {line_count:,} lines, imported {total_inserted:,} URLs...",
                        "info"
                    )

                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    skipped += 1
                    continue

                if entry.get("artefact_kind") != "url":
                    continue

                url_value = entry.get("content", "").strip()
                if not url_value:
                    skipped += 1
                    continue

                # Extract domain and scheme
                domain = None
                scheme = None
                try:
                    parsed = urlparse(url_value)
                    scheme = parsed.scheme or None
                    domain = parsed.netloc or None
                except Exception:
                    pass

                url_batch.append({
                    "url": url_value,
                    "domain": domain,
                    "scheme": scheme,
                    "context": None,
                    "discovered_by": "swiftbeaver:url",
                    "first_seen_utc": None,
                    "last_seen_utc": None,
                    "source_path": f"string_artefacts.jsonl:{entry.get('global_start', '')}",
                    "tags": None,
                    "notes": None,
                })

                if len(url_batch) >= 1000:
                    if callbacks.is_cancelled():
                        callbacks.on_log("URL ingestion cancelled by user", "warning")
                        break
                    total_inserted += insert_urls(
                        evidence_conn, evidence_id, url_batch, run_id=run_id
                    )
                    url_batch = []

        # Final batch (skip if ingestion was cancelled)
        if url_batch and not callbacks.is_cancelled():
            total_inserted += insert_urls(
                evidence_conn, evidence_id, url_batch, run_id=run_id
            )

        if skipped:
            callbacks.on_log(f"Skipped {skipped} unparseable or empty entries", "info")

        callbacks.on_log(f"✅ Imported {total_inserted:,} URLs", "info")
        return total_inserted

    def _build_image_manifest(
        self,
        run_id: str,
        run_dir: Path,
        files: List[Path],
        inserted: int,
        errors: int,
        enriched: int,
        started_at: Optional[str] = None,
        precomputed_hashes: Optional[Dict[str, Dict[str, Any]]] = None,
        evidence_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Build manifest for carved image ingestion."""
        precomputed_hashes = precomputed_hashes or {}
        run_dir_resolved = run_dir.resolve()

        def _rel_path(f: Path) -> str:
            try:
                return f.resolve().relative_to(run_dir_resolved).as_posix()
            except ValueError:
                # File is outside run_dir; fall back to filename only to avoid
                # leaking absolute workstation paths into the forensic manifest.
                return f.name

        carved_files_entries = []
        for f in files:
            meta = precomputed_hashes.get(str(f), {})
            carved_files_entries.append({
                "rel_path": _rel_path(f),
                "size": meta.get("size_bytes") or (f.stat().st_size if f.exists() else 0),
                "md5": meta.get("md5"),
                "sha256": meta.get("sha256"),
                "file_type": meta.get("file_type") or f.suffix.lstrip(".").lower(),
                "offset": meta.get("carved_offset_bytes"),
                "warnings": [],
                "errors": [],
            })

        return {
            "schema_version": "1.0.0",
            "run_id": run_id,
            "extractor": "swiftbeaver",
            "tool": {
                "name": "swiftbeaver",
                "version": None,
                "path": None,
                "arguments": [],
            },
            "started_at": started_at or datetime.now(timezone.utc).isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "input": {
                "source": "swiftbeaver_output",
                "source_type": "path",
                "evidence_id": evidence_id,
                "context": {},
            },
            "output": {
                "root": str(run_dir),
                "carved_dir": str(run_dir / CARVED_DIR),
                "manifest_path": str(run_dir / "swiftbeaver_images_manifest.json"),
            },
            "file_types": None,
            "stats": {
                "carved_total": len(files),
                "zero_byte": 0,
                "failed_validation": errors,
                "by_type": {},
            },
            "warnings": [],
            "notes": [],
            "process": {
                "command": [],
                "returncode": 0,
                "stdout": "",
                "stderr": "",
            },
            "carved_files": carved_files_entries,
            "ingestion": {
                "inserted": inserted,
                "errors": errors,
                "enriched": enriched,
                "skipped_duplicates": enriched,
            },
        }
