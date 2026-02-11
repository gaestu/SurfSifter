"""bulk_extractor modular extractor implementation."""

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Optional, List
from urllib.parse import urlparse
import subprocess
import os
import time

from PySide6.QtWidgets import QWidget

from ...base import BaseExtractor, ExtractorMetadata, ExtractorCallbacks
from core.tool_discovery import discover_tools
from core.logging import get_logger
from core.database import (
    insert_urls,
    insert_emails,
    insert_domains,
    insert_ip_addresses,
    insert_bitcoin_addresses,
    insert_ethereum_addresses,
    insert_telephone_numbers,
    delete_discoveries_by_run,
)
from core.manifest import validate_image_carving_manifest, ManifestValidationError
from extractors._shared.carving.processor import ParallelImageProcessor
from extractors._shared.carving.enrichment import ingest_with_enrichment
from extractors._shared.extracted_files_audit import record_carved_files
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.bulk_extractor")

# Default scanners - all enabled by default to match config widget UI
DEFAULT_SCANNERS = ["email", "accts"]  # email produces url.txt, accts produces telephone.txt etc.
DEFAULT_CARVE_IMAGES = True  # Enable jpeg_carve by default

# Mapping of bulk_extractor output files to artifact types
BULK_EXTRACTOR_OUTPUT_FILES = {
    "url.txt": "url",
    "email.txt": "email",
    "domain.txt": "domain",
    "ip.txt": "ip",
    "telephone.txt": "telephone",
    "ccn.txt": "ccn",
    "bitcoin.txt": "bitcoin",
    "ether.txt": "ether",
}


class BulkExtractorExtractor(BaseExtractor):
    """
    bulk_extractor forensic tool extractor.

    Dual-phase workflow:
    1. Extraction: Run bulk_extractor subprocess (slow, can run overnight)
       - Produces url.txt, email.txt, ip.txt, etc.
       - No database writes, just file generation

    2. Ingestion: Parse output files and load into database (fast, selective)
       - User can choose which artifacts to import
       - Can be re-run with different filters without re-extracting

    Configuration:
        - scanners: List of scanner names (default: ["email"] for URLs-only)
        - num_threads: Thread count (default: auto-detect)
        - output_reuse_policy: "overwrite" | "reuse" | "skip"
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="bulk_extractor",
            display_name="bulk_extractor (URLs, Emails, IPs)",
            description="Forensic bulk data extraction using bulk_extractor tool",
            category="forensic",
            requires_tools=["bulk_extractor"],
            can_extract=True,   # Run bulk_extractor subprocess
            can_ingest=True     # Parse output files into database
        )

    def can_run_extraction(self, evidence_source_path: Path) -> tuple[bool, str]:
        """
        Check if bulk_extractor tool is available and evidence source exists.

        Args:
            evidence_source_path: Path to E01/EWF file or evidence source

        Returns:
            (can_run, reason) tuple
        """
        # Check if tool is available first (allows tests to run without evidence)
        tools = discover_tools()
        tool = tools.get("bulk_extractor")

        if not tool or not tool.available:
            return False, "bulk_extractor not installed or not in PATH"

        # Check if evidence source exists (None is acceptable for tests)
        if evidence_source_path is None:
            return False, "No evidence source specified"

        if not evidence_source_path.exists():
            return False, f"Evidence source not found: {evidence_source_path}"

        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if bulk_extractor output files exist."""
        if not output_dir.exists():
            return False, "Output directory does not exist"

        # Check for at least one recognizable output file
        output_files = list(output_dir.glob("*.txt"))
        recognized = [f for f in output_files if f.name in BULK_EXTRACTOR_OUTPUT_FILES]

        if not recognized:
            return False, "No bulk_extractor output files found (e.g., url.txt, email.txt)"

        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """
        Check if output directory has existing bulk_extractor output files.

        Args:
            output_dir: Output directory to check

        Returns:
            True if output files exist, False otherwise
        """
        if not output_dir.exists():
            return False

        output_files = list(output_dir.glob("*.txt"))
        recognized = [f for f in output_files if f.name in BULK_EXTRACTOR_OUTPUT_FILES]

        return len(recognized) > 0

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return config widget for scanner selection and thread count."""
        from .config_widget import BulkExtractorConfigWidget
        return BulkExtractorConfigWidget(parent)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction status and ingestion options."""
        from .status_widget import BulkExtractorStatusWidget
        return BulkExtractorStatusWidget(
            parent,
            output_dir,
            evidence_conn,
            evidence_id
        )

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """
        Return output directory for bulk_extractor files.

        Convention: {case_root}/evidences/{evidence_label}/bulk_extractor/
        """
        return case_root / "evidences" / evidence_label / "bulk_extractor"

    def run_extraction(
        self,
        evidence_source_path: Path,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Run bulk_extractor subprocess to generate output files.

        This is the "extraction" phase - runs the tool, no database writes.
        Can take hours for large images.

        Args:
            evidence_source_path: Path to E01/EWF file or evidence source
            output_dir: Output directory for bulk_extractor files
            config: Configuration dict with:
                - scanners: List[str] (default: ["email"])
                - num_threads: int (default: auto-detect)
                - output_reuse_policy: str (default: "reuse")
            callbacks: Progress/log callbacks

        Returns:
            True if successful, False otherwise
        """
        callbacks.on_step("Preparing bulk_extractor extraction")

        # Start statistics tracking (may be None in tests)
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        run_id = self._generate_run_id()
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Get config
        scanners = config.get("scanners", DEFAULT_SCANNERS)
        carve_images = config.get("carve_images", DEFAULT_CARVE_IMAGES)
        num_threads = config.get("num_threads")
        reuse_policy = config.get("output_reuse_policy", "reuse")

        # Auto-detect threading
        if num_threads is None:
            cpu_count = os.cpu_count() or 4
            num_threads = max(1, min(cpu_count - 2, 16))

        callbacks.on_log(f"Configuration: {num_threads} threads, scanners: {', '.join(scanners)}", "info")

        # Check output reuse policy
        if output_dir.exists() and reuse_policy == "reuse":
            # Check for existing output
            output_files = list(output_dir.glob("*.txt"))
            recognized = [f for f in output_files if f.name in BULK_EXTRACTOR_OUTPUT_FILES]

            if recognized:
                callbacks.on_log(
                    f"✓ Reusing existing bulk_extractor output ({len(recognized)} files found)",
                    "info"
                )
                callbacks.on_step("Using existing output (reuse policy)")
                if stats:
                    stats.complete_run(self.metadata.name, evidence_id, "skipped")
                return True

        # Prepare output directory
        if output_dir.exists() and reuse_policy == "overwrite":
            callbacks.on_log(f"Removing existing output directory", "warning")
            import shutil
            shutil.rmtree(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)

        # Get tool
        tools = discover_tools()
        tool = tools.get("bulk_extractor")
        if not tool or not tool.available:
            callbacks.on_error("bulk_extractor not available", "Tool not found in PATH")
            if stats:
                stats.complete_run(self.metadata.name, evidence_id, "failed", error="Tool not available")
            return False

        # Use evidence source path directly (bulk_extractor can read E01 files)
        input_path = str(evidence_source_path)
        callbacks.on_log(f"Input: {input_path}", "info")

        # Build scanner args
        scanner_args = []
        if scanners:
            scanner_args.append("-x")
            scanner_args.append("all")
            for scanner in scanners:
                scanner_args.extend(["-e", scanner])
        if carve_images:
            # bulk_extractor 2.1.1 doesn’t have a dedicated jpeg scanner; use exif + jpeg_carve_mode.
            if "exif" not in scanners:
                scanners.append("exif")
                scanner_args.extend(["-e", "exif"])
            scanner_args.extend(["-S", "jpeg_carve_mode=2"])

        cmd = [
            str(tool.path),
            "-j", str(num_threads),
            "-o", str(output_dir),
            *scanner_args,
            input_path,
        ]
        callbacks.on_log(f"Running: {' '.join(cmd)}", "info")
        callbacks.on_step(f"Running bulk_extractor ({num_threads} threads)")

        try:
            start_time = time.time()
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            stdout_lines: list[str] = []
            for line in process.stdout:
                if callbacks.is_cancelled():
                    process.terminate()
                    callbacks.on_log("bulk_extractor cancelled by user", "warning")
                    if stats:
                        stats.complete_run(self.metadata.name, evidence_id, "cancelled")
                    return False
                stdout_lines.append(line)
                if "Offset" in line and "%" in line:
                    import re
                    match = re.search(r'Offset\\s+(\\d+)([KMG]?B)\\s+\\((\\d+(?:\\.\\d+)?)%\\)', line)
                    if match:
                        percent = float(match.group(3))
                        callbacks.on_progress(int(percent), 100, f"Processing... {percent:.1f}%")
                callbacks.on_log(line.strip(), "debug")
            stdout_str, stderr_str = "".join(stdout_lines), process.stderr.read()
            exit_code = process.wait()
            duration = time.time() - start_time
            if exit_code != 0:
                log_path = output_dir / "bulk_extractor_stderr.log"
                try:
                    log_path.write_text(stderr_str or "", encoding="utf-8")
                except Exception:
                    pass
                callbacks.on_error(
                    f"bulk_extractor failed with exit code {exit_code}",
                    stderr_str
                )
                if stats:
                    stats.complete_run(self.metadata.name, evidence_id, "failed", error=f"Exit code {exit_code}")
                return False
            callbacks.on_log(
                f"✓ bulk_extractor completed in {duration:.1f}s",
                "info"
            )
            output_files = list(output_dir.glob("*.txt"))
            recognized = [f for f in output_files if f.name in BULK_EXTRACTOR_OUTPUT_FILES]
            callbacks.on_log(f"Generated {len(recognized)} artifact files", "info")

            # Report discovered files and complete extraction phase
            # Ingestion will continue_run() to add ingested counts
            if stats:
                stats.complete_run(
                    self.metadata.name,
                    evidence_id,
                    "success",
                    discovered={"files": len(recognized)}
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
        Parse bulk_extractor output files and load into database.

        This is the "ingestion" phase - reads files, writes to DB.
        Fast and can be re-run with different filters.

        Args:
            output_dir: Output directory with bulk_extractor files
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID
            config: Configuration dict with:
                - artifact_types: List[str] (which artifacts to import)
                - overwrite_mode: str (optional) "overwrite", "append", or "cancel"
            callbacks: Progress/log callbacks

        Returns:
            Dict with counts per artifact type: {"urls": 123, "emails": 45, ...}
        """
        callbacks.on_step("Checking existing data")

        # Continue statistics tracking from extraction phase (unified card)
        # This continues the same run started during extraction, preserving discovered counts
        evidence_label = config.get("evidence_label", "")
        run_id = self._generate_run_id()
        stats = StatisticsCollector.instance()
        if stats:
            # Use continue_run to preserve extraction stats in the same card
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Get artifact types to import from config
        artifact_types = config.get("artifact_types", ["url"])  # Default: URLs only

        # Check if data already exists
        existing_counts = self._check_existing_data(evidence_conn, evidence_id, artifact_types)
        total_existing = sum(existing_counts.values())

        if total_existing > 0:
            # Data already exists - check overwrite mode
            overwrite_mode = config.get("overwrite_mode")

            if not overwrite_mode:
                # No mode specified - this shouldn't happen if GUI is working correctly
                # Default to cancel for safety
                callbacks.on_log(
                    f"⚠️ Found {total_existing:,} existing bulk_extractor artifacts. "
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
                callbacks.on_step("Removing existing bulk_extractor data")
                callbacks.on_log(
                    f"Removing {total_existing:,} existing bulk_extractor artifacts...",
                    "info"
                )
                self._delete_bulk_extractor_data(evidence_conn, evidence_id, artifact_types)
                callbacks.on_log("✓ Existing data removed", "info")

            elif overwrite_mode == "append":
                callbacks.on_log(
                    f"Appending to {total_existing:,} existing artifacts",
                    "info"
                )
            # else: append mode, just continue

        # Determine total phases for progress
        carve_images = config.get("carve_images", DEFAULT_CARVE_IMAGES)
        has_carved_images = bool(self._detect_carved_images(output_dir))
        total_phases = 2 + (1 if carve_images and has_carved_images else 0)

        callbacks.on_step(f"Phase 1/{total_phases}: Parsing bulk_extractor output files")
        callbacks.on_log(f"Importing artifact types: {', '.join(artifact_types)}", "info")

        results = {}

        # Import each artifact type
        for artifact_type in artifact_types:
            if callbacks.is_cancelled():
                callbacks.on_log("Ingestion cancelled by user", "warning")
                break

            # Find corresponding output file
            output_file = None
            for filename, file_type in BULK_EXTRACTOR_OUTPUT_FILES.items():
                if file_type == artifact_type:
                    output_file = output_dir / filename
                    break

            if not output_file or not output_file.exists():
                callbacks.on_log(f"⚠️ No output file for {artifact_type}", "warning")
                continue

            callbacks.on_step(f"Phase 1/{total_phases}: Importing {artifact_type} from {output_file.name}")
            callbacks.on_log(f"📄 Reading {output_file.name} ({output_file.stat().st_size:,} bytes)", "info")

            # Parse file and import
            count = self._import_artifact_file(
                output_file,
                artifact_type,
                evidence_conn,
                evidence_id,
                callbacks
            )

            results[artifact_type] = count
            callbacks.on_log(f"✅ Successfully imported {count:,} {artifact_type} entries to database", "info")

        callbacks.on_step(f"Phase 2/{total_phases}: Finalizing artifacts")
        callbacks.on_log(f"Artifact ingestion complete: {sum(results.values()):,} total", "info")

        # Phase 3: Ingest carved images (if enabled and available)
        if carve_images and has_carved_images:
            callbacks.on_step(f"Phase 3/{total_phases}: Processing carved images")
            image_stats = self._ingest_carved_images(
                output_dir, evidence_conn, evidence_id, config, callbacks
            )
            results["images"] = image_stats
        elif carve_images and not has_carved_images:
            callbacks.on_log("No carved images found (jpeg_carved/ directory missing or empty)", "info")

        total_records = sum(v if isinstance(v, int) else v.get('inserted', 0) for v in results.values())
        callbacks.on_log(f"🎉 Ingestion complete! Total artifacts: {total_records:,}", "info")

        # Report detailed ingestion stats to unified card
        if stats:
            # Build ingested counts dict with breakdown by artifact type
            ingested_counts = {}
            for artifact_type, count in results.items():
                if isinstance(count, int):
                    ingested_counts[artifact_type] = count
                elif isinstance(count, dict):
                    # Image stats come as dict with 'inserted', 'enriched', etc.
                    ingested_counts["images"] = count.get("inserted", 0)

            stats.complete_run(
                self.metadata.name,
                evidence_id,
                "success",
                ingested=ingested_counts
            )
        return results

    def _generate_run_id(self) -> str:
        """Generate unique run ID: timestamp + UUID4 prefix."""
        import uuid
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        return f"{ts}_{uid}"

    def _check_existing_data(
        self,
        evidence_conn,
        evidence_id: int,
        artifact_types: List[str]
    ) -> Dict[str, int]:
        """
        Check how many bulk_extractor artifacts already exist in database.

        Args:
            evidence_conn: Database connection
            evidence_id: Evidence ID
            artifact_types: List of artifact types to check

        Returns:
            Dict mapping artifact_type -> count
        """
        # Map artifact types to table names
        table_map = {
            "url": "urls",
            "email": "emails",
            "domain": "domains",
            "ip": "ip_addresses",
            "bitcoin": "bitcoin_addresses",
            "ether": "ethereum_addresses",
            "telephone": "telephone_numbers"
        }

        counts = {}
        cursor = evidence_conn.cursor()

        for artifact_type in artifact_types:
            table = table_map.get(artifact_type)
            if not table:
                continue

            # Count records discovered by bulk_extractor
            cursor.execute(
                f"SELECT COUNT(*) FROM {table} WHERE evidence_id = ? AND discovered_by LIKE 'bulk_extractor:%'",
                (evidence_id,)
            )
            count = cursor.fetchone()[0]
            if count > 0:
                counts[artifact_type] = count

        return counts

    def _delete_bulk_extractor_data(
        self,
        evidence_conn,
        evidence_id: int,
        artifact_types: List[str]
    ):
        """
        Delete all bulk_extractor artifacts from database.

        Args:
            evidence_conn: Database connection
            evidence_id: Evidence ID
            artifact_types: List of artifact types to delete
        """
        # Map artifact types to table names
        table_map = {
            "url": "urls",
            "email": "emails",
            "domain": "domains",
            "ip": "ip_addresses",
            "bitcoin": "bitcoin_addresses",
            "ether": "ethereum_addresses",
            "telephone": "telephone_numbers"
        }

        cursor = evidence_conn.cursor()

        for artifact_type in artifact_types:
            table = table_map.get(artifact_type)
            if not table:
                continue

            # Delete records discovered by bulk_extractor
            cursor.execute(
                f"DELETE FROM {table} WHERE evidence_id = ? AND discovered_by LIKE 'bulk_extractor:%'",
                (evidence_id,)
            )

        evidence_conn.commit()

    def _detect_carved_images(self, output_dir: Path) -> List[Path]:
        """
        Detect carved images from bulk_extractor output.

        Checks for jpeg_carved/, jpeg/, or images/ subdirectories and returns
        all image files found.

        Args:
            output_dir: bulk_extractor output directory

        Returns:
            Sorted list of image file paths
        """
        candidates = []
        for name in ("jpeg_carved", "jpeg", "images"):
            root = output_dir / name
            if root.exists():
                candidates.extend(
                    p for p in root.rglob("*")
                    if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".gif", ".webp"}
                )
        return sorted(candidates)

    def _ingest_carved_images(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, int]:
        """
        Ingest carved images from bulk_extractor jpeg_carve output.

        Processes images with ParallelImageProcessor (phash, EXIF, size) and
        ingests via ingest_with_enrichment for SHA256 deduplication.

        Args:
            output_dir: bulk_extractor output directory
            evidence_conn: Evidence database connection
            evidence_id: Evidence ID
            config: Extractor configuration
            callbacks: Progress callbacks

        Returns:
            Dict with ingestion stats: {"inserted": N, "enriched": M, "errors": E}
        """
        image_files = self._detect_carved_images(output_dir)
        if not image_files:
            callbacks.on_log("No carved images found", "info")
            return {"inserted": 0, "enriched": 0, "errors": 0}

        callbacks.on_log(f"Processing {len(image_files)} carved images", "info")

        enable_parallel = config.get("enable_parallel", True)
        processor = ParallelImageProcessor(enable_parallel=enable_parallel)
        results = processor.process_images(image_files, output_dir)

        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        discovered_by = "bulk_extractor"
        extractor_version = self.metadata.version

        # Clean up previous run if re-ingesting
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

            # Progress update
            if (i + 1) % 100 == 0 or i == len(results) - 1:
                callbacks.on_progress(i + 1, len(results), f"Image {i+1}/{len(results)}")

            if result.error is not None:
                error_count += 1
                LOGGER.warning("Skipping failed image %s: %s", result.path, result.error)
                continue

            try:
                record = result.to_db_record(discovered_by)
                record["run_id"] = run_id

                # Get relative path for carved_tool_output
                carved_tool_output = str(result.path.relative_to(output_dir))

                image_id, was_inserted = ingest_with_enrichment(
                    conn=evidence_conn,
                    evidence_id=evidence_id,
                    image_data=record,
                    discovered_by=discovered_by,
                    run_id=run_id,
                    extractor_version=extractor_version,
                    carved_tool_output=carved_tool_output,
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
        manifest = self._build_image_manifest(run_id, output_dir, image_files, inserted, error_count, enriched)
        manifest_path = output_dir / "bulk_extractor_images_manifest.json"
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

        callbacks.on_log(f"✅ Image ingestion complete (inserted={inserted}, enriched={enriched}, errors={error_count})", "info")
        return {"inserted": inserted, "enriched": enriched, "errors": error_count}

    def _build_image_manifest(
        self,
        run_id: str,
        output_dir: Path,
        files: List[Path],
        inserted: int,
        errors: int,
        enriched: int,
    ) -> Dict[str, Any]:
        """Build manifest for carved image ingestion."""
        return {
            "schema_version": "1.0.0",
            "run_id": run_id,
            "extractor": "bulk_extractor",
            "tool": {
                "name": "bulk_extractor",
                "version": None,
                "path": None,
                "arguments": [],
            },
            "started_at": datetime.now(timezone.utc).isoformat(),
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "input": {
                "source": "bulk_extractor_output",
                "source_type": "path",
                "evidence_id": None,
                "context": {},
            },
            "output": {
                "root": str(output_dir),
                "carved_dir": str(output_dir),
                "manifest_path": str(output_dir / "bulk_extractor_images_manifest.json"),
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
            "carved_files": [
                {
                    "rel_path": f.relative_to(output_dir).as_posix() if output_dir in f.parents else f.as_posix(),
                    "size": f.stat().st_size,
                    "md5": None,
                    "sha256": None,
                    "file_type": f.suffix.lstrip(".").lower(),
                    "offset": None,
                    "warnings": [],
                    "errors": [],
                }
                for f in files
            ],
            "ingestion": {
                "inserted": inserted,
                "errors": errors,
                "enriched": enriched,
                "skipped_duplicates": enriched,
            },
        }

    def _import_artifact_file(
        self,
        file_path: Path,
        artifact_type: str,
        evidence_conn,
        evidence_id: int,
        callbacks: ExtractorCallbacks
    ) -> int:
        """
        Import a single bulk_extractor output file into database.

        bulk_extractor format (tab-separated):
            offset<tab>feature<tab>context

        Example:
            17940236<tab>http://example.com/path<tab><binary context>

        Routes artifacts to appropriate tables:
            - url → urls table
            - email → emails table
            - domain → domains table
            - ip → ip_addresses table
            - bitcoin → bitcoin_addresses table
            - ether → ethereum_addresses table

        Returns count of imported records.
        """
        callbacks.on_log(f"Parsing {file_path.name}...", "info")

        artifacts_batch = []
        batch_size = 1000  # Insert every 1000 items
        total_imported = 0
        line_count = 0
        skipped_count = 0

        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if callbacks.is_cancelled():
                        callbacks.on_log("Import cancelled by user", "warning")
                        break

                    line_count += 1

                    # Progress update every 10000 lines
                    if line_count % 10000 == 0:
                        callbacks.on_log(
                            f"  Processed {line_count:,} lines, imported {total_imported:,} {artifact_type} records...",
                            "info"
                        )

                    # Skip comments and empty lines
                    line = line.strip()
                    if not line or line.startswith('#'):
                        skipped_count += 1
                        continue

                    # Parse bulk_extractor line format
                    parsed = self._parse_bulk_extractor_line(line, artifact_type, file_path.name)
                    if not parsed:
                        skipped_count += 1
                        continue

                    # Build record for appropriate table
                    record = self._build_artifact_record(parsed, artifact_type, file_path.name)
                    if record:
                        artifacts_batch.append(record)
                    else:
                        skipped_count += 1

                    # Batch insert
                    if len(artifacts_batch) >= batch_size:
                        callbacks.on_log(
                            f"  Inserting batch of {len(artifacts_batch)} {artifact_type} records...",
                            "info"
                        )
                        inserted = self._insert_artifact_batch(
                            evidence_conn, evidence_id, artifact_type, artifacts_batch
                        )
                        total_imported += inserted
                        if inserted < len(artifacts_batch):
                            callbacks.on_log(
                                f"    Skipped {len(artifacts_batch) - inserted} duplicate {artifact_type} records (already in database)",
                                "info"
                            )
                        artifacts_batch.clear()

                        # Report progress (use 0 for unknown total)
                        callbacks.on_progress(line_count, 0, "")

            # Insert remaining batch
            if artifacts_batch:
                callbacks.on_log(
                    f"  Inserting final batch of {len(artifacts_batch)} {artifact_type} records...",
                    "info"
                )
                inserted = self._insert_artifact_batch(
                    evidence_conn, evidence_id, artifact_type, artifacts_batch
                )
                total_imported += inserted
                if inserted < len(artifacts_batch):
                    callbacks.on_log(
                        f"    Skipped {len(artifacts_batch) - inserted} duplicate {artifact_type} records (already in database)",
                        "info"
                    )

            callbacks.on_log(
                f"✓ Processed {line_count:,} lines from {file_path.name}",
                "info"
            )
            callbacks.on_log(
                f"  Imported: {total_imported:,} {artifact_type} records",
                "info"
            )
            if skipped_count > 0:
                callbacks.on_log(
                    f"  Skipped: {skipped_count:,} lines (comments, empty, invalid)",
                    "info"
                )
            callbacks.on_log(f"✓ Processed {line_count:,} lines, imported {total_imported:,} {artifact_type} items", "info")
            return total_imported

        except Exception as e:
            callbacks.on_log(f"Error importing {file_path.name}: {e}", "error")
            LOGGER.exception(f"Import error for {file_path}")
            return total_imported  # Return what we managed to import


    def _build_artifact_record(
        self,
        parsed: Dict[str, Any],
        artifact_type: str,
        source_filename: str
    ) -> Optional[Dict[str, Any]]:
        """
        Build a database record for the parsed artifact.

        Different artifact types require different record structures.

        Note: context column exists in schema but insert functions don't use it yet.
        TODO(post-beta): Update insert functions to include context parameter.
        See docs/developer/DEFERRED_FEATURES.md
        """
        base_record = {
            "discovered_by": f"bulk_extractor:{artifact_type}",
            "first_seen_utc": None,
            "last_seen_utc": None,
            "source_path": f"{source_filename}:{parsed['offset']}",
            "tags": None,
            "notes": None,
            # "context": parsed["context"],  # TODO(post-beta): Add when insert functions support it
        }

        if artifact_type == "url":
            return {
                **base_record,
                "url": parsed["value"],
                "domain": parsed["domain"],
                "scheme": parsed["scheme"],
                "context": parsed["context"],  # URLs table insert function DOES support context
            }

        elif artifact_type == "email":
            return {
                **base_record,
                "email": parsed["value"],
                "domain": parsed["domain"],
            }

        elif artifact_type == "domain":
            return {
                **base_record,
                "domain": parsed["value"],
            }

        elif artifact_type == "ip":
            return {
                **base_record,
                "ip_address": parsed["value"],
                "ip_version": "IPv4",  # bulk_extractor doesn't distinguish, assume IPv4
            }

        elif artifact_type == "bitcoin":
            return {
                **base_record,
                "address": parsed["value"],
            }

        elif artifact_type == "ether":
            return {
                **base_record,
                "address": parsed["value"],
            }

        elif artifact_type == "telephone":
            return {
                **base_record,
                "phone_number": parsed["value"],
                "country_code": None,  # bulk_extractor doesn't parse country code separately
            }

        elif artifact_type == "ccn":
            # CCNs are detected but not stored for PII reasons
            return None

        else:
            # Unknown artifact type
            LOGGER.warning(f"Unknown artifact type: {artifact_type}")
            return None


    def _insert_artifact_batch(
        self,
        evidence_conn,
        evidence_id: int,
        artifact_type: str,
        artifacts_batch: list
    ) -> int:
        """
        Insert a batch of artifacts into the appropriate table.

        Maps artifact types to database tables:
        - url → urls
        - email → emails
        - domain → domains
        - ip → ip_addresses
        - bitcoin → bitcoin_addresses
        - ether → ethereum_addresses
        """
        if not artifacts_batch:
            return 0

        before_changes = evidence_conn.total_changes  # Track actual inserts (INSERT OR IGNORE safe)

        # Table mapping for logging
        table_map = {
            "url": "urls",
            "email": "emails",
            "domain": "domains",
            "ip": "ip_addresses",
            "bitcoin": "bitcoin_addresses",
            "ether": "ethereum_addresses"
        }

        table_name = table_map.get(artifact_type, "unknown")
        LOGGER.debug(f"Inserting {len(artifacts_batch)} {artifact_type} records into {table_name} table")

        try:
            if artifact_type == "url":
                insert_urls(evidence_conn, evidence_id, artifacts_batch)

            elif artifact_type == "email":
                insert_emails(evidence_conn, evidence_id, artifacts_batch)

            elif artifact_type == "domain":
                insert_domains(evidence_conn, evidence_id, artifacts_batch)

            elif artifact_type == "ip":
                insert_ip_addresses(evidence_conn, evidence_id, artifacts_batch)

            elif artifact_type == "bitcoin":
                insert_bitcoin_addresses(evidence_conn, evidence_id, artifacts_batch)

            elif artifact_type == "ether":
                insert_ethereum_addresses(evidence_conn, evidence_id, artifacts_batch)

            elif artifact_type == "telephone":
                insert_telephone_numbers(evidence_conn, evidence_id, artifacts_batch)

            else:
                LOGGER.warning(f"No insert function for artifact type: {artifact_type}")

        except Exception as e:
            LOGGER.error(f"Error inserting {artifact_type} batch: {e}")
            raise

        return evidence_conn.total_changes - before_changes


    def _parse_bulk_extractor_line(
        self,
        line: str,
        artifact_type: str,
        source_filename: str
    ) -> Optional[Dict[str, Any]]:
        """
        Parse a single bulk_extractor output line.

        Format: offset<tab>feature<tab>context

        Returns dict with:
            - offset: File offset (int)
            - value: The extracted value (URL, email, domain, IP, etc.)
            - domain: Extracted domain (for URLs and emails)
            - scheme: URL scheme (for URLs)
            - context: Binary context (optional)

        Returns None if line cannot be parsed.
        """
        try:
            # Split on tab
            parts = line.split('\t', 2)  # Max 3 parts: offset, feature, context

            if len(parts) < 2:
                return None  # Invalid format

            offset_str = parts[0].strip()
            feature = parts[1].strip()
            context = parts[2] if len(parts) > 2 else ""

            # Parse offset
            try:
                offset = int(offset_str)
            except ValueError:
                return None  # Invalid offset

            # Normalize the feature based on artifact type
            value = self._normalize_feature(feature, artifact_type)
            if not value:
                return None

            # Extract domain and scheme (for URLs and emails)
            domain = None
            scheme = None

            if artifact_type == "url":
                try:
                    parsed_url = urlparse(value)
                    scheme = parsed_url.scheme or None
                    domain = parsed_url.netloc or None
                except Exception:
                    pass

            elif artifact_type == "email":
                # Extract domain from email address
                if "@" in value:
                    domain = value.split("@", 1)[1]

            return {
                "offset": offset,
                "value": value,
                "domain": domain,
                "scheme": scheme,
                "context": context[:500] if context else None,  # Limit context size
            }

        except Exception as e:
            LOGGER.debug(f"Failed to parse line from {source_filename}: {e}")
            return None


    def _normalize_feature(self, feature: str, artifact_type: str) -> Optional[str]:
        """
        Normalize a bulk_extractor feature based on artifact type.

        Args:
            feature: Raw feature string from bulk_extractor
            artifact_type: Type of artifact (url, email, domain, etc.)

        Returns:
            Normalized feature string, or None if invalid
        """
        if not feature:
            return None

        feature = feature.strip()

        # Handle different artifact types
        if artifact_type == "url":
            # Already a URL, just validate it looks reasonable
            if not any(feature.startswith(prefix) for prefix in ["http://", "https://", "ftp://", "file://"]):
                # Try to add http:// if it looks like a URL without scheme
                if "." in feature and " " not in feature:
                    feature = "http://" + feature
                else:
                    return None  # Not a valid URL
            return feature

        elif artifact_type == "email":
            # Return plain email address (not mailto: URL)
            if "@" in feature and " " not in feature:
                # Strip mailto: prefix if present
                if feature.startswith("mailto:"):
                    return feature[7:]  # Remove "mailto:"
                return feature
            return None

        elif artifact_type == "domain":
            # Return plain domain (not as http:// URL)
            if "." in feature and " " not in feature:
                # Strip any protocol prefix
                for prefix in ["http://", "https://", "ftp://"]:
                    if feature.startswith(prefix):
                        feature = feature[len(prefix):]
                return feature
            return None

        elif artifact_type == "ip":
            # Return plain IP address
            return feature

        elif artifact_type in ["bitcoin", "ether", "telephone", "ccn"]:
            # Return as-is for other types
            return feature

        else:
            # Unknown type, return as-is
            return feature
