"""
Image Carving Extractor

Main extractor class for forensic image carving using foremost/scalpel.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

from PySide6.QtWidgets import QWidget, QLabel

from PIL import Image

from extractors.base import BaseExtractor, ExtractorMetadata
from extractors.callbacks import ExtractorCallbacks
from core.logging import get_logger
from core.tool_discovery import discover_tools
from core.manifest import validate_image_carving_manifest, ManifestValidationError
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .worker import CarvingRunResult

from .ingestion import run_image_ingestion
from .common import (
    generate_run_id,
    get_evidence_context,
    create_manifest,
    summarize_carved_files,
    safe_rel_path,
    verify_image,
)

LOGGER = get_logger("extractors._shared.carving")


class ImageCarvingExtractor(BaseExtractor):
    """
    Extract carved images from unallocated space using forensic carving tools.

    Dual-helper strategy:
    - Extraction: Runs foremost/scalpel on E01 image, carves deleted images
    - Ingestion: Processes carved images (phash, EXIF), inserts with forensic fields

    Features:
    - Forensic file carving (foremost/scalpel)
    - Perceptual hash clustering
    - EXIF metadata extraction
    - Parallel image processing
    """

    # Default carving configuration
    DEFAULT_FILE_TYPES = {
        "jpg": {
            "header": "\\xff\\xd8\\xff",
            "footer": "\\xff\\xd9",
            "extension": "jpg",
            "enabled": True,
        },
        "png": {
            "header": "\\x89\\x50\\x4e\\x47",
            "footer": "",  # PNG has internal structure markers
            "extension": "png",
            "enabled": True,
        },
        "gif": {
            "header": "GIF8",
            "footer": "\\x00\\x3b",
            "extension": "gif",
            "enabled": True,
        },
    }

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        # Check for available carving tools
        tools = discover_tools()
        foremost = tools.get("foremost")
        scalpel = tools.get("scalpel")

        requires_tools = []
        if foremost and foremost.available:
            requires_tools.append("foremost")
        elif scalpel and scalpel.available:
            requires_tools.append("scalpel")
        else:
            requires_tools.append("foremost")  # Prefer foremost (even if not available)

        return ExtractorMetadata(
            name="image_carving",
            display_name="Image Carving",
            description="Carve deleted images from unallocated space (JPEG, PNG, GIF)",
            category="media",
            requires_tools=requires_tools,
            can_extract=True,
            can_ingest=True
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """
        Check if extraction can run.

        Args:
            evidence_fs: Mounted evidence filesystem (PyEwfTskFS or MountedFS)

        Returns:
            Tuple of (can_run, reason_if_not)
        """
        if evidence_fs is None:
            return False, "No evidence filesystem mounted. Please mount E01 image first."

        # Check for carving tool availability
        tools = discover_tools()
        foremost = tools.get("foremost")
        scalpel = tools.get("scalpel")

        if not (foremost and foremost.available) and not (scalpel and scalpel.available):
            return False, "Neither foremost nor scalpel found. Install one to carve images."

        # Note: Carving tools can work with E01 files directly (no mount needed)
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """
        Check if ingestion can run (manifest exists).

        Args:
            output_dir: Directory where extraction wrote files

        Returns:
            Tuple of (can_run, reason_if_not)
        """
        manifest = output_dir / "manifest.json"
        if not manifest.exists():
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """
        Check if output directory has existing extraction output.

        Args:
            output_dir: Directory to check

        Returns:
            True if manifest.json exists
        """
        return (output_dir / "manifest.json").exists()

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget for file type selection."""
        # MVP: No config UI yet (use defaults)
        # TODO(post-beta): Create ImageCarvingConfigWidget in future version
        # See docs/developer/DEFERRED_FEATURES.md
        return None

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction/ingestion state."""
        # MVP: Simple label
        manifest = output_dir / "manifest.json"
        if manifest.exists():
            data = json.loads(manifest.read_text())
            file_count = len(data.get("carved_files", data.get("files", [])))
            tool_obj = data.get("tool") or {}
            tool = tool_obj.get("name") or data.get("carving_tool", "unknown")
            status_text = f"Image Carving\\nFiles carved: {file_count}\\nTool: {tool}\\nRun ID: {data.get('run_id', 'N/A')}"
        else:
            status_text = "Image Carving\\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor.

        Supports configurable extractor name to differentiate between multiple carving runs.
        Example: image_carving_foremost, image_carving_scalpel, image_carving_photorec

        Args:
            case_root: Root directory of case workspace
            evidence_label: Evidence label/slug
            config: Optional config dict with 'extractor_name' key

        Returns:
            Path like {case_root}/evidences/{evidence_label}/{extractor_name}/
        """
        # Default to 'image_carving', but allow customization via config
        extractor_name = "image_carving"
        if config and "extractor_name" in config:
            extractor_name = config["extractor_name"]

        return case_root / "evidences" / evidence_label / extractor_name

    def run_extraction(
        self,
        evidence_fs,       # PyEwfTskFS or MountedFS instance
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract carved images from evidence using foremost/scalpel.

        Workflow:
            1. Generate run_id (timestamp + UUID4)
            2. Discover carving tool (foremost/scalpel)
            3. Configure file types to carve
            4. Run carver on E01 image
            5. Collect carved files
            6. Calculate hashes + verify readability
            7. Validate and write manifest.json with forensic metadata

        Args:
            evidence_fs: Mounted evidence filesystem
            output_dir: Directory to write carved images
            config: Extractor configuration dict
            callbacks: Progress callbacks for UI updates

        Returns:
            True if extraction succeeded, False otherwise
        """
        callbacks.on_step("Initializing image carving")
        start_time = datetime.now(timezone.utc)

        # Generate run_id
        run_id = generate_run_id()
        LOGGER.info("Starting image carving (run_id=%s)", run_id)

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Get configuration
        evidence_id = config.get("evidence_id", 1)
        file_types = config.get("file_types", self.DEFAULT_FILE_TYPES)

        # Discover carving tool
        tools = discover_tools()
        carving_tool = None
        tool_info = None

        for candidate in ("foremost", "scalpel"):
            info = tools.get(candidate)
            if info and info.available:
                carving_tool = candidate
                tool_info = info
                break

        if not carving_tool or not tool_info or not tool_info.path:
            callbacks.on_error("No carving tool found (foremost/scalpel)")
            return False

        callbacks.on_step(f"Using {carving_tool} for image carving")
        LOGGER.info("Using carving tool: %s at %s", carving_tool, tool_info.path)

        # Run carving extraction
        try:
            from .worker import run_carving_extraction, parse_foremost_audit
            run_result = run_carving_extraction(
                evidence_fs=evidence_fs,
                output_dir=output_dir,
                carving_tool=carving_tool,
                tool_path=tool_info.path,
                file_types=file_types,
                callbacks=callbacks,
            )
        except Exception as e:
            callbacks.on_error(f"Carving extraction failed: {e}")
            LOGGER.exception("Carving extraction failed")
            return False

        end_time = datetime.now(timezone.utc)
        callbacks.on_step(f"Carved {len(run_result.carved_files)} image files")

        audit_entries = parse_foremost_audit(run_result.audit_path) if run_result.audit_path else []

        try:
            manifest_data = create_manifest(
                extractor_name=self.metadata.name,
                tool_name=carving_tool,
                tool_version=tool_info.version,
                tool_path=tool_info.path,
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
            return False

        # Write manifest
        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")

        callbacks.on_step("Extraction complete, manifest written")
        LOGGER.info("Image carving extraction complete (run_id=%s, files=%d)", run_id, len(run_result.carved_files))

        return True

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,     # sqlite3.Connection to evidence database
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Ingest carved images into database with perceptual hashing.

        Workflow:
            1. Read manifest.json
            2. Process images in parallel (phash, EXIF, thumbnails)
            3. Insert to images table with forensic fields
            4. Log ingestion completion

        Args:
            output_dir: Directory containing carved images
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID
            config: Ingestion configuration
            callbacks: Progress callbacks

        Returns:
            True if ingestion succeeded, False otherwise
        """
        callbacks.on_step("Starting image ingestion")

        # Read manifest
        manifest_path = output_dir / "manifest.json"
        if not manifest_path.exists():
            callbacks.on_error("No manifest.json found")
            return False

        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id")

        LOGGER.info("Starting image ingestion (run_id=%s)", run_id)

        # Run ingestion
        try:
            result = run_image_ingestion(
                output_dir=output_dir,
                evidence_conn=evidence_conn,
                evidence_id=evidence_id,
                manifest_data=manifest_data,
                callbacks=callbacks,
                discovered_by=self.metadata.name,
            )
        except Exception as e:
            callbacks.on_error(f"Image ingestion failed: {e}")
            LOGGER.exception("Image ingestion failed")
            return False

        # Persist ingestion stats back into manifest for UI/status visibility
        try:
            manifest_data["ingestion"] = result
            manifest_path.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")
        except Exception as exc:
            LOGGER.warning("Failed to update manifest ingestion stats: %s", exc)

        callbacks.on_step(
            f"Ingested {result['inserted']} images "
            f"(duplicates skipped={result.get('skipped_duplicates', 0)}, errors={result['errors']})"
        )
        LOGGER.info(
            "Image ingestion complete (inserted=%d, duplicates=%d, errors=%d)",
            result["inserted"],
            result.get("skipped_duplicates", 0),
            result["errors"],
        )

        return True

    def import_carved_files(
        self,
        source_dir: Path,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Import previously carved images from another tool run.

        This allows you to import foremost/scalpel output directories from
        external carving runs without re-running the carving process.

        Workflow:
            1. Detect source directory structure (foremost/scalpel/custom)
            2. Copy carved files to output_dir/carved/
            3. Generate manifest.json with import metadata
            4. Run ingestion (phash, EXIF, database insert)

        Args:
            source_dir: Directory containing carved files (foremost output, etc.)
            output_dir: Destination directory for extractor output
            evidence_conn: SQLite connection to evidence database
            evidence_id: Evidence ID
            config: Import configuration:
                - tool (str): Name of carving tool (e.g., "foremost", "scalpel")
                - notes (str): Optional notes about the import
                - extractor_name (str): Optional custom extractor name for output_dir
            callbacks: Progress callbacks

        Returns:
            True if import succeeded, False otherwise

        Example:
            # Import foremost output from external run
            extractor.import_carved_files(
                source_dir=Path("/mnt/external/case123/foremost_output"),
                output_dir=Path("workspace/evidences/laptop-1/image_carving"),
                evidence_conn=conn,
                evidence_id=1,
                config={"tool": "foremost", "notes": "External carving run 2024-11-15"},
                callbacks=callbacks
            )
        """
        callbacks.on_step("Importing carved images from external source")
        LOGGER.info("Starting carved image import from %s", source_dir)

        if not source_dir.exists():
            callbacks.on_error(f"Source directory not found: {source_dir}")
            return False

        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Detect source structure and collect files
        callbacks.on_step("Detecting source directory structure")
        carved_files = self._collect_import_files(source_dir, callbacks)

        if not carved_files:
            callbacks.on_error("No image files found in source directory")
            return False

        callbacks.on_step(f"Found {len(carved_files)} files to import")
        LOGGER.info("Found %d files to import", len(carved_files))

        # Copy files to output directory
        import shutil
        callbacks.on_step("Copying files to workspace")

        carved_dir = output_dir / "carved"

        copied_files = []
        for src_file in carved_files:
            try:
                # Preserve directory structure if it exists
                rel_path = src_file.relative_to(source_dir)
                dest_file = carved_dir / rel_path
                dest_file.parent.mkdir(parents=True, exist_ok=True)

                shutil.copy2(src_file, dest_file)
                copied_files.append(dest_file)

            except Exception as e:
                LOGGER.warning("Failed to copy %s: %s", src_file, e)
                callbacks.on_log(f"Skipped {src_file.name}: {e}", "warning")

        if not copied_files:
            callbacks.on_error("Failed to copy any files")
            return False

        callbacks.on_step(f"Copied {len(copied_files)} files")
        LOGGER.info("Copied %d files to %s", len(copied_files), carved_dir)

        # Generate manifest
        callbacks.on_step("Generating manifest")
        run_id = generate_run_id()
        start_time = datetime.now(timezone.utc)

        try:
            manifest_data = create_manifest(
                extractor_name=self.metadata.name,
                tool_name=config.get("tool", "unknown"),
                tool_version=None,
                tool_path=None,
                command=[],
                run_id=run_id,
                start_time=start_time,
                end_time=datetime.now(timezone.utc),
                input_info={
                    "source": str(source_dir),
                    "source_type": "path",
                    "evidence_id": evidence_id,
                    "context": {"import": True},
                },
                output_dir=output_dir,
                file_types={},
                carved_files=copied_files,
                returncode=0,
                stdout="",
                stderr="",
            )

            # Add notes manually since create_manifest doesn't support them yet
            manifest_data["notes"] = [config.get("notes", "Imported from external carving run")]

        except ManifestValidationError as exc:
            callbacks.on_error(f"Manifest validation failed: {exc}")
            LOGGER.exception("Import manifest validation failed")
            return False

        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")

        callbacks.on_step("Manifest created")
        LOGGER.info("Manifest written: %s", manifest_path)

        # Run ingestion (phash, EXIF, database insert)
        callbacks.on_step("Running ingestion on imported files")
        success = self.run_ingestion(
            output_dir=output_dir,
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            config=config,
            callbacks=callbacks
        )

        if success:
            callbacks.on_step(f"Import complete: {len(copied_files)} files imported")
            LOGGER.info("Import complete: %d files imported (run_id=%s)", len(copied_files), run_id)
        else:
            callbacks.on_error("Ingestion failed after import")

        return success

    def _collect_import_files(self, source_dir: Path, callbacks: ExtractorCallbacks) -> List[Path]:
        """
        Collect image files from import source directory.

        Detects common carving tool output structures:
        - Foremost: jpg/, png/, gif/ subdirectories
        - Scalpel: jpg-*, png-*, gif-* subdirectories
        - Custom: any *.jpg, *.png, *.gif files

        Args:
            source_dir: Source directory to scan
            callbacks: Progress callbacks

        Returns:
            List of image file paths to import
        """
        image_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif"}
        files = []

        # Detect structure
        subdirs = [d for d in source_dir.iterdir() if d.is_dir()]

        # Check for foremost structure (jpg/, png/, gif/)
        has_foremost = any(d.name in ["jpg", "png", "gif"] for d in subdirs)

        # Check for scalpel structure (jpg-0-0/, jpg-1-0/, etc.)
        has_scalpel = any(d.name.startswith(("jpg-", "png-", "gif-")) for d in subdirs)

        if has_foremost:
            callbacks.on_log("Detected foremost output structure", "info")
            LOGGER.info("Detected foremost output structure")
            # Scan foremost subdirectories
            for subdir in ["jpg", "png", "gif", "bmp", "tiff"]:
                subdir_path = source_dir / subdir
                if subdir_path.exists():
                    files.extend(subdir_path.glob("*"))

        elif has_scalpel:
            callbacks.on_log("Detected scalpel output structure", "info")
            LOGGER.info("Detected scalpel output structure")
            # Scan scalpel subdirectories
            for subdir in subdirs:
                if subdir.name.startswith(("jpg-", "png-", "gif-", "bmp-", "tiff-")):
                    files.extend(subdir.glob("*"))

        else:
            callbacks.on_log("Scanning for image files recursively", "info")
            LOGGER.info("No known structure detected, scanning recursively")
            # Custom structure - scan recursively
            for ext in image_extensions:
                files.extend(source_dir.rglob(f"*{ext}"))
                files.extend(source_dir.rglob(f"*{ext.upper()}"))

        # Filter to only valid files
        valid_files = [f for f in files if f.is_file() and f.suffix.lower() in image_extensions]

        LOGGER.info("Collected %d image files for import", len(valid_files))
        return sorted(valid_files)

