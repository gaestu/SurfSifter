"""
File List extractor module.

v2.0: Dual-path - Generate from E01 (fls) or Import external CSV (FTK/EnCase).
Generates or imports file lists into the evidence database.
v2.2: Unified as primary extractor (wrapper removed).
"""

import json
import logging
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional
from PySide6.QtWidgets import QWidget

from ...base import BaseExtractor, ExtractorMetadata
from ...callbacks import ExtractorCallbacks
from .worker import FileListExtractor
from .ui import FileListConfigWidget, FileListStatusWidget

LOGGER = logging.getLogger(__name__)


class SystemFileListExtractor(BaseExtractor):
    """
    File List Importer - Generate from E01 or Import FTK/EnCase CSV.

    Dual-path workflow:

    PATH A: Generate from E01 (Primary)
        - Requires EWF evidence + SleuthKit fls
        - Uses SleuthKitFileListGenerator (writes directly to DB)
        - Preserves partition_index/inode for filesystem_images compatibility

    PATH B: Import External CSV (Fallback)
        - For investigations with only FTK/EnCase exports
        - Copies CSV to output_dir, then ingests to database
        - Supports partition_index/inode columns when present
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="file_list",
            display_name="File List",
            description="Generate file list from E01 or import FTK/EnCase CSV",
            category="system",
            requires_tools=[],  # fls is optional (checked at runtime)
            can_extract=True,   # fls generation OR CSV copy
            can_ingest=True     # CSV import to DB (after copy)
        )

    def _get_statistics_collector(self):
        """Get StatisticsCollector instance (may be None in tests)."""
        try:
            from core.statistics_collector import StatisticsCollector
            return StatisticsCollector.instance()
        except Exception:
            return None

    def _generate_run_id(self) -> str:
        """Generate run ID: {timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """
        Extraction phase can run in two modes:
        1. fls generation - requires PyEwfTskFS evidence + fls available
        2. CSV import - always available (user provides CSV file)
        """
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """
        Ingestion is available when file_list.csv exists in output_dir.
        (Only for CSV import path - fls writes directly to DB)

        For fls generation mode, data was written directly to DB during extraction,
        so ingestion should be skipped gracefully (not reported as failure).
        """
        # Check if fls generation was used (wrote directly to DB)
        manifest_path = output_dir / "manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text())
                if manifest.get("source") == "fls_generation":
                    # Data already in database - skip ingestion gracefully
                    return False, "Data already in database (fls generation)"
            except (json.JSONDecodeError, OSError):
                pass

        # CSV import path: check for file_list.csv
        csv_file = output_dir / "file_list.csv"
        if csv_file.exists():
            return True, ""
        return False, "No file_list.csv found - run extraction first or use fls generation"

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return config widget for CSV file selection."""
        return FileListConfigWidget(parent)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing import results."""
        return FileListStatusWidget(
            parent,
            output_dir,
            evidence_conn,
            evidence_id
        )

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """
        Return output directory for file list CSV.

        Convention: {case_root}/evidences/{evidence_label}/file_list/
        """
        return case_root / "evidences" / evidence_label / "file_list"

    def has_existing_output(self, output_dir: Path) -> bool:
        """
        Check if output already exists.

        For file list importer, we always return False because:
        1. We don't produce output files (we import into DB)
        2. We don't want the UI to prompt for overwrite (which deletes the directory)
        3. The import process handles duplicates gracefully
        """
        return False

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extraction phase - two modes:

        1. fls generation (generate_from_e01=True):
           - Uses SleuthKitFileListGenerator to enumerate files
           - Writes directly to file_list table (no intermediate CSV)
           - Requires PyEwfTskFS evidence and fls available

        2. CSV import (imported_csv_path set):
           - Copies external CSV to output_dir/file_list.csv
           - User runs ingestion separately to import to DB

        Args:
            evidence_fs: Evidence filesystem (PyEwfTskFS for fls mode)
            output_dir: Output directory for CSV files
            config: Configuration dict with mode flags
            callbacks: Progress/log callbacks

        Returns:
            True if successful, False otherwise
        """
        from .sleuthkit_generator import SleuthKitFileListGenerator

        # Statistics tracking
        collector = self._get_statistics_collector()
        run_id = self._generate_run_id()
        evidence_id = config.get("evidence_id")
        evidence_label = config.get("evidence_label", "")

        # Auto-detect mode if not explicitly set
        # Priority: 1) explicit generate_from_e01, 2) imported_csv_path, 3) auto-detect E01+fls
        generate_from_e01 = config.get("generate_from_e01", False)
        imported_csv_path = config.get("imported_csv_path")

        callbacks.on_log(
            f"Config: generate_from_e01={generate_from_e01}, "
            f"csv_path={imported_csv_path}, evidence_fs={type(evidence_fs).__name__}",
            "debug"
        )

        if not generate_from_e01 and not imported_csv_path:
            # Auto-detect: if we have E01 evidence and fls is available, use fls generation
            from core.evidence_fs import PyEwfTskFS
            if isinstance(evidence_fs, PyEwfTskFS):
                # Check if fls is available (bundled or PATH)
                from .sleuthkit_utils import get_sleuthkit_bin
                fls_path = get_sleuthkit_bin("fls")
                if fls_path:
                    generate_from_e01 = True
                    callbacks.on_log("Auto-detected E01 evidence with fls available", "info")
                else:
                    callbacks.on_log(
                        "E01 evidence detected but fls not found (bundled or PATH). "
                        "Install SleuthKit or bundle binaries for automatic file list generation.",
                        "warning"
                    )
            else:
                callbacks.on_log(
                    f"Evidence type {type(evidence_fs).__name__} - fls generation requires E01 image. "
                    "Use 'Import CSV' for directory evidence.",
                    "info"
                )

        # Mode A: Generate from E01 using fls
        if generate_from_e01:
            callbacks.on_step("Checking fls availability")

            # Start statistics tracking
            if collector and evidence_id:
                collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

            # Check for PyEwfTskFS
            from core.evidence_fs import PyEwfTskFS
            if not isinstance(evidence_fs, PyEwfTskFS):
                callbacks.on_error(
                    "E01 evidence required",
                    "fls generation requires an EWF image.\n"
                    "Use 'Import CSV' for mounted filesystems or folder evidence."
                )
                if collector and evidence_id:
                    collector.complete_run(self.metadata.name, evidence_id, "failed")
                return False

            # Get evidence connection from config or create one
            evidence_conn = config.get("evidence_conn")
            own_conn = False  # Track if we created the connection

            if not evidence_conn:
                # Try to get db_manager from config to create connection
                db_manager = config.get("db_manager")
                if db_manager and evidence_id and evidence_label:
                    try:
                        evidence_conn = db_manager.get_evidence_conn(evidence_id, evidence_label)
                        own_conn = True
                        callbacks.on_log("Created database connection for fls generation", "debug")
                    except Exception as e:
                        callbacks.on_error(
                            "Database connection failed",
                            f"Could not connect to evidence database: {e}"
                        )
                        if collector and evidence_id:
                            collector.complete_run(self.metadata.name, evidence_id, "failed")
                        return False

            if not evidence_conn or not evidence_id:
                callbacks.on_error(
                    "Missing context",
                    "Evidence connection not provided by application.\n"
                    "Ensure db_manager is available in config."
                )
                if collector and evidence_id:
                    collector.complete_run(self.metadata.name, evidence_id, "failed")
                return False

            # Create generator
            callbacks.on_log(f"Creating SleuthKit generator with {len(evidence_fs.ewf_paths)} EWF paths", "info")
            generator = SleuthKitFileListGenerator(
                evidence_conn=evidence_conn,
                evidence_id=evidence_id,
                ewf_paths=evidence_fs.ewf_paths,
            )
            callbacks.on_log(f"Generator created, fls_available={generator.fls_available}", "info")

            if not generator.fls_available:
                callbacks.on_error(
                    "SleuthKit not installed",
                    "Install SleuthKit or bundle binaries to use fls generation:\n"
                    "  Linux: apt install sleuthkit\n"
                    "  macOS: brew install sleuthkit\n"
                    "  Windows: download from sleuthkit.org"
                )
                if collector and evidence_id:
                    collector.complete_run(self.metadata.name, evidence_id, "failed")
                return False

            callbacks.on_log("Starting file list generation...", "info")
            callbacks.on_step("Generating file list from E01")

            def progress_cb(files: int, part_idx: int, msg: str):
                if callbacks.is_cancelled():
                    raise InterruptedError("Generation cancelled by user")
                callbacks.on_step(f"Partition {part_idx}: {files:,} files")

            try:
                result = generator.generate(progress_callback=progress_cb)
            except InterruptedError:
                callbacks.on_log("Generation cancelled by user", "warning")
                if collector and evidence_id:
                    collector.complete_run(self.metadata.name, evidence_id, "cancelled")
                if own_conn and evidence_conn:
                    evidence_conn.close()
                return False

            if not result.success:
                callbacks.on_error("Generation failed", result.error_message or "Unknown error")
                if collector and evidence_id:
                    collector.complete_run(self.metadata.name, evidence_id, "failed")
                if own_conn and evidence_conn:
                    evidence_conn.close()
                return False

            # Log any fls errors (even on success, there may be warnings)
            if result.fls_errors:
                for err in result.fls_errors:
                    callbacks.on_log(
                        f"fls warning: partition {err['partition']} ({err['pass_type']}) "
                        f"exit code {err['exit_code']}",
                        "warning"
                    )

            callbacks.on_log(
                f"Generated {result.total_files:,} entries from "
                f"{result.partitions_processed} partition(s) in {result.duration_seconds:.1f}s",
                "info"
            )

            # Write manifest for status widget
            output_dir.mkdir(parents=True, exist_ok=True)
            manifest = {
                "source": "fls_generation",
                "run_id": run_id,
                "total_files": result.total_files,
                "partitions_processed": result.partitions_processed,
                "duration_seconds": result.duration_seconds,
                "partition_stats": result.partition_stats,
                "fls_errors": result.fls_errors,
            }
            (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

            # Report completion with file count (fls writes directly to DB, so this is both discovered and ingested)
            if collector and evidence_id:
                collector.complete_run(
                    self.metadata.name, evidence_id, "success",
                    discovered={"files": result.total_files},
                    ingested={"records": result.total_files}
                )

            # Close connection if we created it
            if own_conn and evidence_conn:
                evidence_conn.close()

            return True

        # Mode B: Import external CSV
        elif imported_csv_path:
            src_path = Path(imported_csv_path)

            # Start statistics tracking for CSV mode
            if collector and evidence_id:
                collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

            if not src_path.exists():
                callbacks.on_error(
                    "CSV file not found",
                    f"Selected file does not exist:\n{src_path}"
                )
                if collector and evidence_id:
                    collector.complete_run(self.metadata.name, evidence_id, "failed")
                return False

            callbacks.on_step("Copying CSV to output directory")

            output_dir.mkdir(parents=True, exist_ok=True)
            dst_path = output_dir / "file_list.csv"

            shutil.copy2(src_path, dst_path)
            callbacks.on_log(f"Copied {src_path.name} to output directory", "info")
            callbacks.on_log("Run 'Ingest Results' to import CSV into database", "info")

            # Report file copy as discovered (ingestion will report actual records)
            if collector and evidence_id:
                # Don't finish - extraction just copies file, ingestion does the work
                # This is a two-phase extractor
                collector.report_discovered(evidence_id, self.metadata.name, files=1)
                collector.finish_run(evidence_id, self.metadata.name, "success")

            return True

        else:
            # Provide context-aware error message
            from core.evidence_fs import PyEwfTskFS, MountedFS
            if isinstance(evidence_fs, MountedFS):
                callbacks.on_error(
                    "File List requires configuration",
                    "For directory evidence, use 'Import CSV' with an FTK/EnCase export.\n"
                    "Configure the extractor first, or skip File List for batch runs."
                )
            elif evidence_fs is None:
                callbacks.on_error(
                    "No evidence mounted",
                    "Evidence filesystem not available. Load evidence before running."
                )
            else:
                callbacks.on_error(
                    "No action selected",
                    "For E01 images: Install SleuthKit or bundle binaries for automatic generation.\n"
                    "Or configure manually: Select 'Use fls Generation' or choose a CSV file."
                )
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
        Ingestion phase - import CSV file into file_list table.

        This is only used for the CSV import path. fls generation
        writes directly to DB during extraction.

        Args:
            output_dir: Output directory containing file_list.csv
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID
            config: Configuration dict. Supports:
                - force_reimport: bool - If True, clear existing data before import
            callbacks: Progress/log callbacks

        Returns:
            Statistics dictionary with inserted/skipped counts
        """
        csv_file = output_dir / "file_list.csv"

        # Statistics tracking
        collector = self._get_statistics_collector()
        evidence_label = config.get("evidence_label", "")
        run_id = self._generate_run_id()
        force_reimport = config.get("force_reimport", False)

        # Continue from extraction stats if they exist
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Check if file_list already has data for this evidence (from fls or previous import)
        try:
            cursor = evidence_conn.execute(
                "SELECT COUNT(*) FROM file_list WHERE evidence_id = ?",
                (evidence_id,)
            )
            existing_count = cursor.fetchone()[0]
            if existing_count > 0:
                if force_reimport:
                    callbacks.on_log(
                        f"Clearing {existing_count:,} existing entries (force_reimport=True)",
                        "info"
                    )
                    evidence_conn.execute(
                        "DELETE FROM file_list WHERE evidence_id = ?",
                        (evidence_id,)
                    )
                    evidence_conn.commit()
                else:
                    callbacks.on_log(
                        f"File list already contains {existing_count:,} entries - skipping import. "
                        "Set force_reimport=True to clear and reimport.",
                        "info"
                    )
                    if collector:
                        collector.complete_run(self.metadata.name, evidence_id, "skipped")
                    return {"file_list_entries": 0, "skipped": existing_count, "already_exists": True}
        except Exception as e:
            LOGGER.debug(f"Could not check existing file_list entries: {e}")

        if not csv_file.exists():
            callbacks.on_error(
                "No file list CSV found",
                "Run extraction first to copy CSV file, or use fls generation."
            )
            if collector:
                collector.complete_run(self.metadata.name, evidence_id, "failed")
            return {}

        callbacks.on_step("Reading CSV file")
        callbacks.on_log(f"Importing from file_list.csv", "info")

        # Get import source type from config (or auto-detect)
        import_source = config.get("import_source", "auto")

        # Create extractor worker
        extractor = FileListExtractor(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            csv_path=csv_file,
            import_source=import_source
        )

        # Define progress callback
        def progress_callback(current: int, total: int):
            if callbacks.is_cancelled():
                raise InterruptedError("Import cancelled by user")
            callbacks.on_progress(current, total, f"Importing {current:,}/{total:,} entries")

        callbacks.on_step("Importing file list entries")

        try:
            # Run import
            stats = extractor.run(progress_callback=progress_callback)

            # Report results
            callbacks.on_log(
                f"Imported {stats['inserted_rows']:,}/{stats['total_rows']:,} entries "
                f"in {stats['duration_seconds']:.2f}s "
                f"({stats['skipped_rows']} skipped)",
                "info"
            )

            # Report completion
            if collector:
                collector.complete_run(
                    self.metadata.name, evidence_id, "success",
                    ingested={"records": stats["inserted_rows"]},
                )

            return {
                "file_list_entries": stats["inserted_rows"],
                "skipped": stats["skipped_rows"],
            }

        except InterruptedError:
            callbacks.on_log("Import cancelled by user", "warning")
            evidence_conn.rollback()
            if collector:
                collector.complete_run(self.metadata.name, evidence_id, "cancelled")
            return {}
        except Exception as e:
            callbacks.on_error(f"Import failed: {e}", str(e))
            evidence_conn.rollback()
            if collector:
                collector.complete_run(self.metadata.name, evidence_id, "failed")
            return {}
