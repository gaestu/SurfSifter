"""
Filesystem Images Extractor

Main extractor class for extracting images from evidence filesystem
with full path context and timestamp preservation.

Removed slow filesystem walker fallback. Now requires file_list table.
        Fails with clear error if file_list empty and SleuthKit unavailable.
Zero-content detection for OneDrive/sparse files that extract as all zeros.
Discovery manifest (files_to_extract.csv + discovery_summary.json) for debugging.
Sparse file detection for OneDrive "Files On-Demand" placeholders.
Extension-based discovery (20x faster), signature verification during extraction.
Responsive cancellation during file reads, log accuracy fixes.
Cancellation propagation, enable_parallel honored, audit-accurate manifests.
Parallel extraction using multiple PyEwfTskFS instances.
Optimized to compute hashes during copy (single pass instead of two).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path, PurePosixPath
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING
from uuid import uuid4

from PySide6.QtWidgets import QWidget, QLabel, QVBoxLayout

import csv

from ...base import BaseExtractor, ExtractorMetadata

if TYPE_CHECKING:
    from .parallel_extractor import ExtractionTask
from ...callbacks import ExtractorCallbacks
from core.logging import get_logger
from core.database import DatabaseManager, find_case_database, slugify_label
from core.database import (
    get_image_by_sha256,
    insert_image_with_discovery,
    delete_discoveries_by_run,
    insert_extracted_files_batch,
    delete_extracted_files_by_run,
)
from extractors._shared.carving.processor import ParallelImageProcessor
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.filesystem_images")


def _format_size(size_bytes: int) -> str:
    """Format bytes as human-readable string."""
    if size_bytes >= 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
    elif size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes} B"


def generate_run_id() -> str:
    """Generate unique run ID for this extraction."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = uuid4().hex[:8]
    return f"fs_{ts}_{uid}"


class FilesystemImagesExtractor(BaseExtractor):
    """
    Extract images from evidence filesystem with full path context.

    Key features:
    - Requires file_list table for discovery (- walker removed)
    - Auto-generates file_list via SleuthKit fls for EWF images
    - Optional signature verification during extraction
    - Flags extension/signature mismatches as forensically interesting
    - Zero-content detection for OneDrive/sparse placeholders
    - Preserves original filesystem paths
    - Records MACB timestamps (mtime, atime, ctime, crtime)
    - Supports order-independent enrichment via image_discoveries table
    - Stream-based hashing for memory efficiency
    - Parallel extraction with multiple PyEwfTskFS instances
    - Sparse/OneDrive file detection and skipping
    - Discovery manifest for debugging
    """

    VERSION = "1.9.0"

    @staticmethod
    def compute_flat_rel_path(
        fs_path: str,
        filename: Optional[str],
        inode: Optional[int],
        prefix: Optional[str] = None,
    ) -> str:
        """
        Compute relative path for flat extraction mode.

        Delegates to utils.compute_flat_rel_path for implementation.
        Kept here for backward compatibility and convenience.
        """
        from .utils import compute_flat_rel_path as _compute
        return _compute(fs_path, filename, inode, prefix)

    @staticmethod
    def _parse_iso_epoch(ts_value: Optional[str]) -> Optional[float]:
        if not ts_value:
            return None
        ts_value = ts_value.strip()
        if not ts_value:
            return None
        try:
            if ts_value.endswith("Z"):
                ts_value = ts_value[:-1] + "+00:00"
            return datetime.fromisoformat(ts_value).timestamp()
        except ValueError:
            return None

    @staticmethod
    def _coerce_inode(inode_value: Any) -> Optional[int]:
        if inode_value is None:
            return None
        if isinstance(inode_value, int):
            return inode_value
        if isinstance(inode_value, str):
            first = inode_value.split("-", 1)[0]
            if first.isdigit():
                return int(first)
        return None

    @staticmethod
    def _matches_patterns(
        path_value: str,
        include_patterns: List[str],
        exclude_patterns: List[str],
    ) -> bool:
        # Normalize Windows backslashes to forward slashes for consistent glob matching
        # This ensures patterns like "Users/*/Pictures/**" match paths from FTK/EnCase CSV
        # imports which may contain Windows-style paths like "C:\Users\John\Pictures\photo.jpg"
        path_lower = path_value.replace("\\", "/").lower()
        if include_patterns:
            if not any(fnmatch(path_lower, pat.lower()) for pat in include_patterns):
                return False
        if exclude_patterns:
            if any(fnmatch(path_lower, pat.lower()) for pat in exclude_patterns):
                return False
        return True

    @staticmethod
    def _write_discovery_manifest(
        output_dir: Path,
        tasks: List["ExtractionTask"],
        tasks_by_partition: Dict[int, List["ExtractionTask"]],
        callbacks: "ExtractorCallbacks",
    ) -> None:
        """
        Write discovery manifest after Phase 1 for debugging.

        Creates:
        - files_to_extract.csv: List of all files to be extracted
        - discovery_summary.json: Aggregated statistics
        """
        from pathlib import PurePosixPath

        csv_path = output_dir / "files_to_extract.csv"
        summary_path = output_dir / "discovery_summary.json"

        # Write CSV with all discovered files
        callbacks.on_step(f"Writing discovery manifest ({len(tasks):,} files)...")

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["fs_path", "filename", "extension", "size_bytes", "partition_index", "inode"])

            for task in tasks:
                ext = PurePosixPath(task.filename).suffix.lower() if task.filename else ""
                # Determine partition_index from tasks_by_partition
                partition_index = -1
                for part_idx, part_tasks in tasks_by_partition.items():
                    if task in part_tasks:
                        partition_index = part_idx
                        break

                writer.writerow([
                    task.fs_path,
                    task.filename,
                    ext,
                    task.size_bytes,
                    partition_index,
                    task.inode,
                ])

        # Build summary statistics
        total_files = len(tasks)
        total_size = sum(t.size_bytes for t in tasks)

        # Extension breakdown
        ext_counts: Dict[str, int] = {}
        ext_sizes: Dict[str, int] = {}
        for task in tasks:
            raw_ext = PurePosixPath(task.filename).suffix.lower() if task.filename else ""
            ext = raw_ext if raw_ext else "(no extension)"
            ext_counts[ext] = ext_counts.get(ext, 0) + 1
            ext_sizes[ext] = ext_sizes.get(ext, 0) + task.size_bytes

        # Partition breakdown
        partition_counts: Dict[str, int] = {}
        partition_sizes: Dict[str, int] = {}
        for part_idx, part_tasks in tasks_by_partition.items():
            key = f"partition_{part_idx}" if part_idx >= 0 else "partition_auto"
            partition_counts[key] = len(part_tasks)
            partition_sizes[key] = sum(t.size_bytes for t in part_tasks)

        # Size distribution buckets
        size_buckets = {
            "0_bytes": 0,
            "1B_to_1KB": 0,
            "1KB_to_10KB": 0,
            "10KB_to_100KB": 0,
            "100KB_to_1MB": 0,
            "1MB_to_10MB": 0,
            "10MB_to_100MB": 0,
            "100MB_plus": 0,
        }
        for task in tasks:
            size = task.size_bytes
            if size == 0:
                size_buckets["0_bytes"] += 1
            elif size < 1024:
                size_buckets["1B_to_1KB"] += 1
            elif size < 10 * 1024:
                size_buckets["1KB_to_10KB"] += 1
            elif size < 100 * 1024:
                size_buckets["10KB_to_100KB"] += 1
            elif size < 1024 * 1024:
                size_buckets["100KB_to_1MB"] += 1
            elif size < 10 * 1024 * 1024:
                size_buckets["1MB_to_10MB"] += 1
            elif size < 100 * 1024 * 1024:
                size_buckets["10MB_to_100MB"] += 1
            else:
                size_buckets["100MB_plus"] += 1

        summary = {
            "discovery_time": datetime.now(timezone.utc).isoformat(),
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_human": _format_size(total_size),
            "by_extension": {
                "counts": dict(sorted(ext_counts.items(), key=lambda x: -x[1])),
                "sizes": {k: _format_size(v) for k, v in sorted(ext_sizes.items(), key=lambda x: -x[1])},
            },
            "by_partition": {
                "counts": partition_counts,
                "sizes": {k: _format_size(v) for k, v in partition_sizes.items()},
            },
            "size_distribution": size_buckets,
        }

        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        callbacks.on_log(f"Discovery manifest written: {csv_path.name}, {summary_path.name}")

    def _record_extracted_files(
        self,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: "ExtractorCallbacks",
        evidence_id: int,
        run_id: str,
        files_manifest: List[Dict[str, Any]],
        tasks_by_partition: Dict[int, List["ExtractionTask"]],
        was_cancelled: bool,
    ) -> None:
        """
        Record extracted files to extracted_files audit table.

        This provides a universal audit trail of all extracted files across
        all extractors, queryable via SQL for forensic reporting.

        Args:
            output_dir: Extractor output directory
            config: Extraction configuration
            callbacks: Extractor callbacks for logging
            evidence_id: Evidence ID
            run_id: Unique run ID for this extraction
            files_manifest: List of file records from extraction
            tasks_by_partition: Tasks grouped by partition (for partition_index lookup)
            was_cancelled: Whether extraction was cancelled
        """
        if not files_manifest:
            return

        # Try to get database connection
        try:
            db_manager, evidence_conn, db_evidence_id, _, owns_conn = (
                self._open_evidence_conn(output_dir, config, callbacks)
            )
        except Exception as exc:
            callbacks.on_log(
                f"Could not record extracted files to audit table: {exc}",
                level="warning"
            )
            return

        if evidence_conn is None:
            callbacks.on_log(
                "Skipping extracted_files audit (no database connection)",
                level="debug"
            )
            return

        try:
            # Build reverse lookup for partition_index from tasks_by_partition
            # fs_path -> partition_index
            path_to_partition: Dict[str, int] = {}
            for part_idx, tasks in tasks_by_partition.items():
                for task in tasks:
                    path_to_partition[task.fs_path] = part_idx

            # Clean up previous run if re-extracting
            deleted = delete_extracted_files_by_run(evidence_conn, evidence_id, run_id)
            if deleted > 0:
                callbacks.on_log(
                    f"Cleaned up {deleted} previous extracted_files records",
                    level="debug"
                )

            # Convert manifest entries to extracted_files format
            extracted_records = []
            for file_info in files_manifest:
                fs_path = file_info.get("fs_path", "")
                rel_path = file_info.get("rel_path", "")
                filename = file_info.get("filename", "")

                # Determine partition from task lookup
                partition_index = path_to_partition.get(fs_path)

                record = {
                    "dest_rel_path": f"extracted/{rel_path}" if rel_path else "",
                    "dest_filename": filename,
                    "source_path": fs_path,
                    "source_inode": str(file_info.get("inode")) if file_info.get("inode") else None,
                    "partition_index": partition_index,
                    "size_bytes": file_info.get("size_bytes"),
                    "file_type": file_info.get("detected_type"),  # From signature detection
                    "md5": file_info.get("md5"),
                    "sha256": file_info.get("sha256"),
                    "status": "ok",
                    # Store MACB timestamps as JSON metadata
                    "metadata_json": json.dumps({
                        "mtime_epoch": file_info.get("mtime_epoch"),
                        "atime_epoch": file_info.get("atime_epoch"),
                        "crtime_epoch": file_info.get("crtime_epoch"),
                        "ctime_epoch": file_info.get("ctime_epoch"),
                        "signature_valid": file_info.get("signature_valid"),
                    }) if any(file_info.get(k) for k in ["mtime_epoch", "atime_epoch", "crtime_epoch", "ctime_epoch", "signature_valid"]) else None,
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
                f"Recorded {count:,} files to extracted_files audit table",
                level="debug"
            )
            LOGGER.debug(
                "Recorded %d extracted files to audit table (run_id=%s)",
                count, run_id
            )

        except Exception as exc:
            callbacks.on_log(
                f"Error recording extracted files: {exc}",
                level="warning"
            )
            LOGGER.warning("Error recording extracted files to audit table: %s", exc)
        finally:
            # Close connection if we own it
            if db_manager is not None:
                db_manager.close_all()
            elif owns_conn and evidence_conn is not None:
                try:
                    evidence_conn.close()
                except sqlite3.Error:
                    pass

    def _open_evidence_conn(
        self,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> tuple[Optional[DatabaseManager], Optional[sqlite3.Connection], Optional[int], Optional[str], bool]:
        """
        Open evidence database connection.

        Returns:
            Tuple of (db_manager, connection, evidence_id, evidence_label, owns_connection)

            owns_connection: If True, caller must close the connection.
                            If False, connection is managed externally (from config's db_manager).
        """
        evidence_id = config.get("evidence_id")
        evidence_label = config.get("evidence_label")
        case_root = config.get("case_root")
        case_db_path = config.get("case_db_path")

        # Fast path: use db_manager from config if available
        # Connection lifecycle is managed by the caller's db_manager
        ext_db_manager = config.get("db_manager")
        if ext_db_manager is not None and evidence_id is not None and evidence_label is not None:
            try:
                evidence_conn = ext_db_manager.get_evidence_conn(evidence_id, evidence_label)
                # Return owns_connection=False - caller's db_manager owns this connection
                return None, evidence_conn, evidence_id, evidence_label, False
            except Exception as exc:
                callbacks.on_log(f"db_manager connection failed, falling back: {exc}", level="debug")

        if case_root and not isinstance(case_root, Path):
            case_root = Path(case_root)
        if case_db_path and not isinstance(case_db_path, Path):
            case_db_path = Path(case_db_path)

        if case_root is None:
            try:
                case_root = output_dir.parents[2]
            except IndexError:
                case_root = None

        if case_db_path is None and case_root is not None:
            case_db_path = find_case_database(case_root)

        if (evidence_id is None or evidence_label is None) and case_db_path:
            evidence_slug = output_dir.parent.name
            try:
                with sqlite3.connect(case_db_path) as case_conn:
                    case_conn.row_factory = sqlite3.Row
                    rows = case_conn.execute("SELECT id, label FROM evidences").fetchall()
                for row in rows:
                    label = row["label"]
                    ev_id = row["id"]
                    try:
                        slug = slugify_label(label, ev_id)
                    except ValueError:
                        continue
                    if slug == evidence_slug:
                        evidence_id = ev_id
                        evidence_label = label
                        break
            except sqlite3.Error as exc:
                callbacks.on_log(f"File list lookup failed to read case database: {exc}", level="warning")

        if case_root is None or case_db_path is None:
            return None, None, None, None, False
        if evidence_id is None or evidence_label is None:
            callbacks.on_log(
                "File list lookup skipped: unable to resolve evidence ID/label",
                level="warning",
            )
            return None, None, None, None, False

        try:
            db_manager = DatabaseManager(case_root, case_db_path=case_db_path)
            evidence_conn = db_manager.get_evidence_conn(evidence_id, evidence_label)
            # owns_connection=True - we created the db_manager, so we own it
            return db_manager, evidence_conn, evidence_id, evidence_label, True
        except Exception as exc:
            callbacks.on_log(f"File list lookup skipped: {exc}", level="warning")
            return None, None, None, None, False

    def _collect_tasks_from_file_list(
        self,
        evidence_conn: sqlite3.Connection,
        evidence_id: int,
        include_patterns: List[str],
        exclude_patterns: List[str],
        min_size_bytes: int,
        max_size_bytes: Optional[int],
        extensions: List[str],
        allowed_partitions: Optional[Set[int]],
        callbacks: ExtractorCallbacks,
    ) -> tuple[List["ExtractionTask"], Dict[int, List["ExtractionTask"]], int, int, bool]:
        from .parallel_extractor import ExtractionTask

        tasks: List[ExtractionTask] = []
        tasks_by_partition: Dict[int, List[ExtractionTask]] = {}
        processed = 0
        found = 0

        ext_list = [ext.lower() for ext in extensions]
        ext_clause = ""
        params: List[Any] = [evidence_id]
        if ext_list:
            # Only match files with one of the specified extensions
            # Do NOT include NULL/empty extensions - those are not images
            ext_placeholders = ", ".join(["?"] * len(ext_list))
            ext_clause = f"AND extension IN ({ext_placeholders})"
            params.extend(ext_list)
        else:
            # No extensions specified - this shouldn't happen for image extraction
            # but if it does, match nothing rather than everything
            ext_clause = "AND 1 = 0"

        # Defense in depth: exclude NTFS metadata that may have slipped through
        # during file_list generation (entries with ($FILE_NAME), ($DATA), etc.)
        ntfs_filter = "AND file_path NOT LIKE '%($%' AND file_name NOT LIKE '%($%'"

        query = f"""
            SELECT file_path, file_name, extension, size_bytes, created_ts,
                   modified_ts, accessed_ts, inode, deleted, partition_index
            FROM file_list
            WHERE evidence_id = ?
              AND COALESCE(deleted, 0) = 0
              {ext_clause}
              {ntfs_filter}
        """

        cursor = evidence_conn.execute(query, params)
        for row in cursor:
            if callbacks.is_cancelled():
                return tasks, tasks_by_partition, processed, found, True

            processed += 1
            if processed % 1000 == 0:
                callbacks.on_step(
                    f"Phase 1: Scanning file_list... {found:,} images matched (of {processed:,} candidates)"
                )

            file_path = row[0] or ""
            if not file_path:
                continue
            fs_path = file_path.lstrip("/")

            if not self._matches_patterns(fs_path, include_patterns, exclude_patterns):
                continue

            size_value = row[3]
            try:
                size_bytes = int(size_value) if size_value is not None else 0
            except (TypeError, ValueError):
                size_bytes = 0

            if size_bytes < min_size_bytes:
                continue
            if max_size_bytes is not None and size_bytes > max_size_bytes:
                continue

            entry_partition = row[9]
            try:
                entry_partition = int(entry_partition) if entry_partition is not None else -1
            except (TypeError, ValueError):
                entry_partition = -1
            if allowed_partitions is not None and entry_partition not in allowed_partitions:
                continue

            filename = row[1] or PurePosixPath(fs_path).name
            mtime_epoch = self._parse_iso_epoch(row[5])
            crtime_epoch = self._parse_iso_epoch(row[4])
            atime_epoch = self._parse_iso_epoch(row[6])
            inode = self._coerce_inode(row[7])

            task = ExtractionTask(
                fs_path=fs_path,
                filename=filename,
                size_bytes=size_bytes,
                mtime_epoch=mtime_epoch,
                crtime_epoch=crtime_epoch,
                atime_epoch=atime_epoch,
                ctime_epoch=None,
                inode=inode,
            )
            tasks.append(task)
            tasks_by_partition.setdefault(entry_partition, []).append(task)
            found += 1

        return tasks, tasks_by_partition, processed, found, False

    @property
    def metadata(self) -> ExtractorMetadata:
        return ExtractorMetadata(
            name="filesystem_images",
            display_name="Filesystem Images",
            description="Extract images from filesystem with path and timestamp context.",
            category="media",
            version=self.VERSION,
            requires_tools=[],  # Pure Python, no external tools
            can_extract=True,
            can_ingest=True,
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
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
        # Return configuration widget
        from .config_widget import FilesystemImagesConfigWidget
        return FilesystemImagesConfigWidget(parent)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
    ) -> QWidget:
        """Return widget showing current extraction status."""
        manifest = output_dir / "manifest.json"

        widget = QWidget(parent)
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)

        if manifest.exists():
            try:
                data = json.loads(manifest.read_text())
                file_count = len(data.get("files", []))
                total_bytes = data.get("total_bytes", 0)
                run_id = data.get("run_id", "unknown")
                extraction_time = data.get("extraction_time", "")

                # Format size
                if total_bytes >= 1024 * 1024 * 1024:
                    size_str = f"{total_bytes / (1024 * 1024 * 1024):.2f} GB"
                elif total_bytes >= 1024 * 1024:
                    size_str = f"{total_bytes / (1024 * 1024):.2f} MB"
                else:
                    size_str = f"{total_bytes / 1024:.2f} KB"

                # Ingestion stats if available
                ingestion = data.get("ingestion", {})
                if ingestion:
                    inserted = ingestion.get("inserted", 0)
                    enriched = ingestion.get("enriched", 0)
                    errors = ingestion.get("errors", 0)
                    ingestion_text = f"Ingested: {inserted} new, {enriched} enriched, {errors} errors"
                else:
                    ingestion_text = "Not ingested yet"

                label = QLabel(
                    f"✓ Extracted {file_count} images ({size_str})\n"
                    f"Run ID: {run_id}\n"
                    f"Time: {extraction_time}\n"
                    f"{ingestion_text}"
                )
            except Exception as e:
                label = QLabel(f"⚠ Error reading manifest: {e}")
        else:
            label = QLabel("No extraction output found")

        layout.addWidget(label)
        return widget

    def get_output_dir(
        self,
        case_root: Path,
        evidence_label: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Return output directory for this extractor's files."""
        return case_root / "evidences" / evidence_label / "filesystem_images"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        """
        Run extraction phase - walk filesystem and copy images.

        Workflow:
        1. Query file_list table for image candidates (requires pre-populated file_list)
           - If file_list empty and EWF image: auto-generate via SleuthKit fls
           - If file_list unavailable: fail with clear error message
        2. For each image (parallel extraction phase):
           - Compute MD5/SHA256 via streaming
           - Optionally verify signature matches extension
           - Copy to output_dir/extracted/{relative_path}
           - Record metadata in manifest
        3. Write manifest.json

        Removed slow filesystem walker fallback. Now requires file_list.
        Discovery uses extension-only filtering (20x faster).
        Signature verification happens during extraction (free - first 32 bytes).

        Uses parallel extraction when evidence_fs is PyEwfTskFS and
        parallel extraction is enabled. Respects ParallelConfig.enable_parallel.
        Multiple workers each open their own pyewf handle for true parallelism.
        """
        from .parallel_extractor import ParallelExtractor, ExtractionTask, MAX_WORKERS_CAP
        from core.evidence_fs import PyEwfTskFS
        from core.config import ParallelConfig
        from extractors.image_signatures import SUPPORTED_IMAGE_EXTENSIONS
        from extractors.system.file_list import SleuthKitFileListGenerator

        run_id = generate_run_id()
        evidence_id = config.get("evidence_id", 1)
        evidence_label = config.get("evidence_label", "")
        callbacks.on_step(f"Starting extraction (run_id: {run_id})")
        LOGGER.info("Starting filesystem image extraction with run_id=%s", run_id)

        # Start statistics tracking (may be None in tests)
        stats = StatisticsCollector.instance()
        if stats:
            stats.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Create output directories
        output_dir.mkdir(parents=True, exist_ok=True)
        extracted_dir = output_dir / "extracted"
        extracted_dir.mkdir(exist_ok=True)

        # Parse configuration
        include_patterns = config.get("include_patterns", [])
        exclude_patterns = config.get("exclude_patterns", [])
        # Default min_size=1024 (1KB) filters out 0-byte placeholders and tiny files
        # Can be set to 0 in UI to disable this filter
        min_size = config.get("min_size_bytes", 1024)
        max_size = config.get("max_size_bytes")
        use_signatures = config.get("use_signature_detection", True)
        preserve_structure = config.get("preserve_folder_structure", False)  # Default: flat with inode (avoids collisions)
        parallel_workers = config.get("parallel_workers", None)  # None = auto

        # Get parallel config (respects environment variables)
        parallel_cfg = ParallelConfig.from_environment()

        # Build extension set from config
        extensions = None
        ext_config = config.get("extensions", {})
        if ext_config:
            extensions = set()
            for ext, enabled in ext_config.items():
                if enabled:
                    if not ext.startswith("."):
                        ext = "." + ext
                    extensions.add(ext.lower())

        # If extensions config was provided but nothing enabled, short-circuit
        # (user explicitly deselected all formats)
        if ext_config and extensions is not None and len(extensions) == 0:
            callbacks.on_log("No image formats selected - nothing to extract", level="warning")
            # Write empty manifest for auditability
            manifest_data = {
                "run_id": run_id,
                "extractor": self.metadata.name,
                "extractor_version": self.VERSION,
                "extraction_time": datetime.now(timezone.utc).isoformat(),
                "total_files": 0,
                "total_bytes": 0,
                "error_count": 0,
                "was_cancelled": False,
                "extraction_mode": {
                    "used_parallel": False,
                    "effective_workers": 0,
                    "configured_workers": None,
                    "parallel_enabled": parallel_cfg.enable_parallel,
                    "max_workers_cap": MAX_WORKERS_CAP,
                },
                "signature_verification": {
                    "enabled": use_signatures,
                    "mismatches": 0,
                },
                "config": {
                    "include_patterns": include_patterns,
                    "exclude_patterns": exclude_patterns,
                    "min_size_bytes": min_size,
                    "max_size_bytes": max_size,
                    "use_signature_detection": use_signatures,
                    "preserve_folder_structure": preserve_structure,
                },
                "files": [],
            }
            manifest_path = output_dir / "manifest.json"
            manifest_path.write_text(json.dumps(manifest_data, indent=2))
            callbacks.on_step("No formats selected - wrote empty manifest")
            if stats:
                stats.report_discovered(evidence_id, self.metadata.name, files=0)
                stats.finish_run(evidence_id, self.metadata.name, "skipped")
            return True

        # Phase 1: Discovery - collect all image metadata from file_list
        callbacks.on_step("Phase 1: Discovering images...")
        tasks: List[ExtractionTask] = []
        tasks_by_partition: Dict[int, List[ExtractionTask]] = {}
        used_file_list = False
        discovery_cancelled = False

        file_list_extensions = sorted(extensions or SUPPORTED_IMAGE_EXTENSIONS)
        file_list_manager = None
        file_list_conn = None
        file_list_evidence_id = None
        file_list_evidence_label = None
        file_list_owns_conn = False  # Track if we need to close the connection

        try:
            file_list_manager, file_list_conn, file_list_evidence_id, file_list_evidence_label, file_list_owns_conn = (
                self._open_evidence_conn(output_dir, config, callbacks)
            )

            allowed_partitions = None
            if not isinstance(evidence_fs, PyEwfTskFS):
                allowed_partitions = {0, -1}

            if file_list_conn is not None and file_list_evidence_id is not None:
                cursor = file_list_conn.execute(
                    "SELECT COUNT(*) FROM file_list WHERE evidence_id = ?",
                    (file_list_evidence_id,),
                )
                file_list_count = cursor.fetchone()[0]

                if file_list_count > 0:
                    callbacks.on_step(f"Phase 1: Querying file_list ({file_list_count:,} entries)...")
                    callbacks.on_log(
                        f"Using file_list table for discovery ({file_list_count:,} entries)",
                        level="info",
                    )
                    LOGGER.info(
                        "Starting file_list discovery: %d total entries, filtering for %d extensions",
                        file_list_count, len(file_list_extensions)
                    )
                    tasks, tasks_by_partition, _, _, discovery_cancelled = (
                        self._collect_tasks_from_file_list(
                            file_list_conn,
                            file_list_evidence_id,
                            include_patterns,
                            exclude_patterns,
                            min_size,
                            max_size,
                            file_list_extensions,
                            allowed_partitions,
                            callbacks,
                        )
                    )
                    used_file_list = True
                elif isinstance(evidence_fs, PyEwfTskFS):
                    generator = SleuthKitFileListGenerator(
                        file_list_conn,
                        file_list_evidence_id,
                        evidence_fs.ewf_paths,
                    )
                    if generator.fls_available:
                        callbacks.on_step("Phase 1: Generating file list (fls)...")

                        def fls_progress(processed: int, part_idx: int, message: str) -> None:
                            callbacks.on_step(f"Phase 1: Generating file list... {message}")

                        result = generator.generate(progress_callback=fls_progress)
                        if result.success:
                            callbacks.on_log(
                                f"File list generated with fls: {result.total_files:,} files",
                                level="info",
                            )
                            tasks, tasks_by_partition, _, _, discovery_cancelled = (
                                self._collect_tasks_from_file_list(
                                    file_list_conn,
                                    file_list_evidence_id,
                                    include_patterns,
                                    exclude_patterns,
                                    min_size,
                                    max_size,
                                    file_list_extensions,
                                    allowed_partitions,
                                    callbacks,
                                )
                            )
                            used_file_list = True
                        else:
                            callbacks.on_log(
                                f"fls file list generation failed: {result.error_message}",
                                level="warning",
                            )
                    else:
                        # fls not available and no file_list - cannot proceed
                        error_msg = (
                            "File list required for image extraction. "
                            "Run 'Generate File List' extractor first, "
                            "or install SleuthKit (fls) for automatic generation."
                        )
                        callbacks.on_error(error_msg)
                        LOGGER.error(error_msg)
                        if stats:
                            stats.finish_run(evidence_id, self.metadata.name, "failed")
                        return False
        finally:
            # Only close resources we own
            if file_list_manager is not None:
                file_list_manager.close_all()
            elif file_list_owns_conn and file_list_conn is not None:
                # Only close connection if we created our own db_manager
                # Don't close connections from config's db_manager (cached/shared)
                try:
                    file_list_conn.close()
                except sqlite3.Error:
                    pass

        if discovery_cancelled:
            callbacks.on_log("Discovery cancelled by user", level="warning")
            return False

        if not used_file_list:
            # No file_list and not an EWF image (or fls failed) - cannot proceed
            error_msg = (
                "File list required for image extraction. "
                "Run 'Generate File List' extractor first to populate the file_list table."
            )
            callbacks.on_error(error_msg)
            LOGGER.error(error_msg)
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "failed")
            return False

        if not tasks:
            callbacks.on_log("No images found")
            # Write empty manifest
            manifest_data = {
                "run_id": run_id,
                "extractor": self.metadata.name,
                "extractor_version": self.VERSION,
                "extraction_time": datetime.now(timezone.utc).isoformat(),
                "total_files": 0,
                "total_bytes": 0,
                "error_count": 0,
                "config": {
                    "include_patterns": include_patterns,
                    "exclude_patterns": exclude_patterns,
                    "min_size_bytes": min_size,
                    "max_size_bytes": max_size,
                    "use_signature_detection": use_signatures,
                    "preserve_folder_structure": preserve_structure,
                },
                "files": [],
            }
            manifest_path = output_dir / "manifest.json"
            manifest_path.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")
            return True

        # Calculate total estimated size for logging
        total_estimated_bytes = sum(t.size_bytes for t in tasks)
        callbacks.on_log(f"Phase 1 complete: found {len(tasks):,} images (~{_format_size(total_estimated_bytes)})")
        LOGGER.info(
            "Discovery complete: %d images, ~%s estimated across %d partition(s)",
            len(tasks), _format_size(total_estimated_bytes), len(tasks_by_partition)
        )

        # Write discovery manifest for debugging
        # This helps identify problematic files (OneDrive placeholders, etc.) before extraction
        self._write_discovery_manifest(output_dir, tasks, tasks_by_partition, callbacks)

        # Report discovery count
        if stats:
            stats.report_discovered(evidence_id, self.metadata.name, files=len(tasks))

        # Phase 2: Extraction
        # Check if we can AND should use parallel extraction
        # Conditions: PyEwfTskFS only + parallel not disabled via config/env
        from core.evidence_fs import open_ewf_partition

        # Calculate effective max_workers
        max_workers = parallel_workers or parallel_cfg.max_workers
        # Cap at system limit
        max_workers = min(max_workers, MAX_WORKERS_CAP) if max_workers else MAX_WORKERS_CAP

        def extract_task_batch(
            batch_fs,
            batch_tasks: List[ExtractionTask],
            label: Optional[str],
            path_prefix: Optional[str],
        ) -> tuple[
            List[Dict[str, Any]],
            int,
            int,
            bool,
            int,
            bool,
            int,
        ]:
            label_suffix = f" ({label})" if label else ""
            can_parallel = isinstance(batch_fs, PyEwfTskFS) and parallel_cfg.enable_parallel

            if can_parallel:
                # Calculate estimated total size for rate estimation
                estimated_total_bytes = sum(t.size_bytes for t in batch_tasks)
                start_time = time.monotonic()
                last_log_time = start_time

                callbacks.on_step(f"Phase 2: Starting extraction{label_suffix} ({len(batch_tasks):,} images, ~{_format_size(estimated_total_bytes)})")
                LOGGER.info(
                    "Starting parallel extraction%s: %d images, ~%s, %d workers",
                    label_suffix, len(batch_tasks), _format_size(estimated_total_bytes), max_workers
                )

                parallel = ParallelExtractor(
                    ewf_paths=batch_fs.ewf_paths,
                    partition_index=batch_fs.partition_index,
                    output_dir=output_dir,
                    max_workers=max_workers,
                    preserve_structure=preserve_structure,
                    verify_signatures=use_signatures,
                    path_prefix=path_prefix,
                )

                def extract_progress(extracted: int, errors: int, total_bytes: int) -> None:
                    nonlocal last_log_time
                    elapsed = time.monotonic() - start_time
                    mb_done = total_bytes / (1024 * 1024)
                    rate = mb_done / elapsed if elapsed > 0 else 0
                    pct = (extracted / len(batch_tasks) * 100) if batch_tasks else 0

                    # User-facing progress: concise but informative
                    callbacks.on_step(
                        f"Phase 2: Extracting{label_suffix}... {extracted:,}/{len(batch_tasks):,} "
                        f"({pct:.0f}%) • {mb_done:.1f} MB • {rate:.1f} MB/s"
                    )

                    # Periodic log (every 30 seconds) for long extractions
                    now = time.monotonic()
                    if now - last_log_time >= 30:
                        last_log_time = now
                        LOGGER.info(
                            "Extraction progress%s: %d/%d (%.0f%%), %.1f MB, %.1f MB/s, %d errors",
                            label_suffix, extracted, len(batch_tasks), pct, mb_done, rate, errors
                        )

                summary = parallel.extract_all(
                    tasks=batch_tasks,
                    progress_callback=extract_progress,
                    cancel_check=callbacks.is_cancelled,
                )

                files_manifest: List[Dict[str, Any]] = []
                for result in summary.results:
                    if result.success:
                        entry = {
                            "fs_path": result.task.fs_path,
                            "filename": result.task.filename,
                            "rel_path": result.rel_path,
                            "size_bytes": result.task.size_bytes,
                            "md5": result.md5,
                            "sha256": result.sha256,
                            "mtime_epoch": result.task.mtime_epoch,
                            "crtime_epoch": result.task.crtime_epoch,
                            "atime_epoch": result.task.atime_epoch,
                            "ctime_epoch": result.task.ctime_epoch,
                            "inode": result.task.inode,
                        }
                        if use_signatures:
                            entry["detected_type"] = result.detected_type
                            entry["signature_valid"] = result.signature_valid
                        files_manifest.append(entry)
                    elif result.is_sparse:
                        # Sparse/OneDrive files are not errors - just skipped
                        # Don't spam logs with tens of thousands of these
                        pass
                    else:
                        # Actual extraction error
                        callbacks.on_log(
                            f"Error: {result.task.fs_path} - {result.error}",
                            level="warning",
                        )

                if summary.used_parallel:
                    elapsed = time.monotonic() - start_time
                    mb_done = summary.total_bytes / (1024 * 1024)
                    rate = mb_done / elapsed if elapsed > 0 else 0
                    LOGGER.info(
                        "Parallel extraction complete%s: %d images, %.1f MB in %.1f sec (%.1f MB/s), %d errors, %d sparse",
                        label_suffix, summary.extracted_count, mb_done, elapsed, rate,
                        summary.error_count, summary.sparse_count
                    )
                    callbacks.on_log(
                        f"Extracted {summary.extracted_count:,} images ({mb_done:.1f} MB) in {elapsed:.1f}s "
                        f"using {summary.effective_workers} workers ({rate:.1f} MB/s){label_suffix}"
                    )
                else:
                    callbacks.on_log(
                        f"Used sequential extraction (batch size {len(batch_tasks)} < parallel threshold){label_suffix}"
                    )

                return (
                    files_manifest,
                    summary.total_bytes,
                    summary.error_count,
                    summary.was_cancelled,
                    summary.effective_workers,
                    summary.used_parallel,
                    summary.signature_mismatches,
                    summary.sparse_count,
                )

            if not parallel_cfg.enable_parallel and isinstance(batch_fs, PyEwfTskFS):
                callbacks.on_log("Parallel extraction disabled via config/environment")
            else:
                callbacks.on_log("Using sequential extraction (mounted filesystem)")
            callbacks.on_step(f"Phase 2: Sequential extraction{label_suffix}...")

            files_manifest: List[Dict[str, Any]] = []
            total_bytes = 0
            error_count = 0
            signature_mismatches = 0
            sparse_count = 0  # OneDrive/sparse files with 0 actual content
            extraction_cancelled = False

            for i, task in enumerate(batch_tasks):
                if callbacks.is_cancelled():
                    extraction_cancelled = True
                    callbacks.on_log("Extraction cancelled by user", level="warning")
                    break

                try:
                    if preserve_structure:
                        prefix = f"{path_prefix}/" if path_prefix else ""
                        rel_path = f"{prefix}{task.fs_path}"
                    else:
                        # Use shared helper for deterministic flat naming
                        rel_path = self.compute_flat_rel_path(
                            task.fs_path, task.filename, task.inode, path_prefix
                        )

                    dest_path = extracted_dir / rel_path
                    dest_path.parent.mkdir(parents=True, exist_ok=True)

                    callbacks.on_step(
                        f"Phase 2: Extracting{label_suffix} {i+1:,}/{len(batch_tasks):,} - {task.filename}"
                    )
                    md5 = hashlib.md5()
                    sha256 = hashlib.sha256()
                    bytes_since_check = 0
                    actual_bytes_written = 0  # Track actual content size
                    cancel_interval = 1024 * 1024  # 1 MB
                    cancelled_during_file = False
                    header_bytes = b""
                    first_chunk = True
                    all_zeros = True  # Track if content is all zeros
                    zero_check_limit = 64 * 1024  # Only check first 64KB for zeros

                    with open(dest_path, "wb") as out_file:
                        for chunk in batch_fs.open_for_stream(task.fs_path):
                            if use_signatures and first_chunk:
                                header_bytes = chunk[:32] if len(chunk) >= 32 else chunk
                                first_chunk = False

                            # Track zero-content in first 64KB
                            # Use count() for efficiency - avoids allocating zero-filled comparison buffer
                            if all_zeros and actual_bytes_written < zero_check_limit:
                                check_len = min(len(chunk), zero_check_limit - actual_bytes_written)
                                if chunk[:check_len].count(b'\x00') != check_len:
                                    all_zeros = False

                            bytes_since_check += len(chunk)
                            actual_bytes_written += len(chunk)
                            if bytes_since_check >= cancel_interval:
                                bytes_since_check = 0
                                if callbacks.is_cancelled():
                                    cancelled_during_file = True
                                    break
                            out_file.write(chunk)
                            md5.update(chunk)
                            sha256.update(chunk)

                    if cancelled_during_file:
                        try:
                            dest_path.unlink()
                        except OSError:
                            pass
                        extraction_cancelled = True
                        callbacks.on_log("Extraction cancelled during file read", level="warning")
                        break

                    # Sparse/placeholder file detection (enhanced)
                    # OneDrive "Files On-Demand" have NTFS-reported size but:
                    #   - 0 actual content (cloud-only, no data runs)
                    #   - OR all-zero content (sparse allocation / placeholder)
                    min_size_for_zero_check = 1024  # Only check files claiming >1KB
                    is_sparse = False

                    if task.size_bytes > 0 and actual_bytes_written == 0:
                        # Case 1: No data at all (original detection)
                        is_sparse = True
                        sparse_count += 1
                        LOGGER.debug(
                            "Sparse/OneDrive file: %s - NTFS reports %d bytes but extracted 0",
                            task.fs_path, task.size_bytes
                        )
                        try:
                            dest_path.unlink()
                        except OSError:
                            pass
                    elif (
                        task.size_bytes >= min_size_for_zero_check
                        and actual_bytes_written > 0
                        and all_zeros
                    ):
                        # Case 2: File extracted but content is all zeros
                        is_sparse = True
                        sparse_count += 1
                        LOGGER.debug(
                            "Zero-content file: %s - claimed %d bytes, extracted %d bytes of zeros",
                            task.fs_path, task.size_bytes, actual_bytes_written
                        )
                        try:
                            dest_path.unlink()
                        except OSError:
                            pass

                    if is_sparse:
                        continue  # Skip to next file, don't add to manifest

                    md5_hex = md5.hexdigest()
                    sha256_hex = sha256.hexdigest()

                    detected_type = None
                    signature_valid = None
                    if use_signatures and header_bytes:
                        from extractors.image_signatures import detect_image_type

                        detection_result = detect_image_type(header_bytes)
                        ext = PurePosixPath(task.filename).suffix.lower()
                        type_to_ext = {
                            "jpeg": {".jpg", ".jpeg", ".jpe", ".jfif"},
                            "png": {".png"},
                            "gif": {".gif"},
                            "webp": {".webp"},
                            "bmp": {".bmp", ".dib"},
                            "ico": {".ico", ".cur"},
                            "tiff": {".tif", ".tiff"},
                            "svg": {".svg"},
                            "avif": {".avif"},
                            "heic": {".heic", ".heif"},
                        }

                        if detection_result is None:
                            detected_type = None
                            signature_valid = False
                            signature_mismatches += 1
                        else:
                            # detect_image_type returns (format_name, extension) tuple
                            detected_type = detection_result[0]
                            expected_exts = type_to_ext.get(detected_type, set())
                            signature_valid = ext in expected_exts or not ext
                            if not signature_valid:
                                signature_mismatches += 1

                    entry = {
                        "fs_path": task.fs_path,
                        "filename": task.filename,
                        "rel_path": rel_path,
                        "size_bytes": task.size_bytes,
                        "md5": md5_hex,
                        "sha256": sha256_hex,
                        "mtime_epoch": task.mtime_epoch,
                        "crtime_epoch": task.crtime_epoch,
                        "atime_epoch": task.atime_epoch,
                        "ctime_epoch": task.ctime_epoch,
                        "inode": task.inode,
                    }
                    if use_signatures:
                        entry["detected_type"] = detected_type
                        entry["signature_valid"] = signature_valid
                    files_manifest.append(entry)

                    total_bytes += actual_bytes_written  # Use actual bytes, not NTFS-reported size

                except Exception as e:
                    error_count += 1
                    LOGGER.warning("Error extracting %s: %s", task.fs_path, e)
                    callbacks.on_log(f"Error: {task.fs_path} - {e}", level="warning")

            return (
                files_manifest,
                total_bytes,
                error_count,
                extraction_cancelled,
                1,
                False,
                signature_mismatches,
                sparse_count,
            )

        # Track actual extraction mode for manifest
        extraction_cancelled = False
        effective_workers = 0
        used_parallel = False
        signature_mismatches = 0
        sparse_count = 0  # OneDrive/sparse files with 0 actual content
        total_bytes = 0
        error_count = 0
        files_manifest: List[Dict[str, Any]] = []

        if used_file_list and isinstance(evidence_fs, PyEwfTskFS) and tasks_by_partition:
            partition_keys = sorted(tasks_by_partition.keys())
            use_partition_prefix = len(partition_keys) > 1

            for partition_index in partition_keys:
                batch_tasks = tasks_by_partition[partition_index]
                if not batch_tasks:
                    continue

                label = "partition auto" if partition_index == -1 else f"partition {partition_index}"
                path_prefix = None
                if use_partition_prefix:
                    path_prefix = "partition_auto" if partition_index == -1 else f"partition_{partition_index}"

                # Track whether we need to close batch_fs (only if we opened it)
                should_close_batch_fs = False
                batch_fs = None

                if partition_index == evidence_fs.partition_index:
                    batch_fs = evidence_fs
                else:
                    try:
                        batch_fs = open_ewf_partition(
                            evidence_fs.ewf_paths,
                            partition_index=partition_index,
                        )
                        should_close_batch_fs = True
                    except Exception as exc:
                        callbacks.on_log(f"Failed to open {label}: {exc}", level="warning")
                        error_count += len(batch_tasks)
                        continue

                try:
                    (
                        batch_manifest,
                        batch_bytes,
                        batch_errors,
                        batch_cancelled,
                        batch_workers,
                        batch_used_parallel,
                        batch_mismatches,
                        batch_sparse,
                    ) = extract_task_batch(batch_fs, batch_tasks, label, path_prefix)

                    files_manifest.extend(batch_manifest)
                    total_bytes += batch_bytes
                    error_count += batch_errors
                    signature_mismatches += batch_mismatches
                    sparse_count += batch_sparse
                    used_parallel = used_parallel or batch_used_parallel
                    effective_workers = max(effective_workers, batch_workers)

                    if batch_cancelled:
                        extraction_cancelled = True
                        break
                finally:
                    # Close handles opened for other partitions to avoid leaks
                    if should_close_batch_fs and batch_fs is not None:
                        try:
                            batch_fs.close()
                        except Exception as close_exc:
                            LOGGER.debug("Error closing partition handle: %s", close_exc)
        else:
            (
                files_manifest,
                total_bytes,
                error_count,
                extraction_cancelled,
                effective_workers,
                used_parallel,
                signature_mismatches,
                sparse_count,
            ) = extract_task_batch(evidence_fs, tasks, None, None)

        # Write manifest - ALWAYS write even on cancellation for auditability
        # The manifest accurately records what was actually extracted
        manifest_data = {
            "run_id": run_id,
            "extractor": self.metadata.name,
            "extractor_version": self.VERSION,
            "extraction_time": datetime.now(timezone.utc).isoformat(),
            "total_files": len(files_manifest),
            "total_bytes": total_bytes,
            "error_count": error_count,
            "was_cancelled": extraction_cancelled,
            # Audit fields - record what actually happened
            "extraction_mode": {
                "used_parallel": used_parallel,
                "effective_workers": effective_workers,
                "configured_workers": parallel_workers,
                "parallel_enabled": parallel_cfg.enable_parallel,
                "max_workers_cap": MAX_WORKERS_CAP,
            },
            # Signature verification stats
            "signature_verification": {
                "enabled": use_signatures,
                "mismatches": signature_mismatches,
            },
            # Sparse file detection stats
            "sparse_files": {
                "count": sparse_count,
                "description": "OneDrive/cloud-only files with NTFS-reported size but 0 actual content",
            },
            "config": {
                "include_patterns": include_patterns,
                "exclude_patterns": exclude_patterns,
                "min_size_bytes": min_size,
                "max_size_bytes": max_size,
                "use_signature_detection": use_signatures,
                "preserve_folder_structure": preserve_structure,
            },
            "files": files_manifest,
        }

        manifest_path = output_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")

        # Populate extracted_files audit table
        # This records what was extracted for forensic audit purposes
        self._record_extracted_files(
            output_dir, config, callbacks, evidence_id, run_id,
            files_manifest, tasks_by_partition, extraction_cancelled
        )

        if extraction_cancelled:
            callbacks.on_log(
                f"Extraction cancelled: {len(files_manifest):,} images extracted before cancellation"
            )
            LOGGER.info(
                "Extraction cancelled: %d images, %d bytes, %d errors, %d sparse",
                len(files_manifest), total_bytes, error_count, sparse_count
            )
            if stats:
                stats.finish_run(evidence_id, self.metadata.name, "cancelled")
            return False

        sparse_msg = f", {sparse_count:,} sparse/OneDrive skipped" if sparse_count > 0 else ""
        callbacks.on_log(
            f"Extraction complete: {len(files_manifest):,} images ({total_bytes / 1024 / 1024:.2f} MB){sparse_msg}"
        )
        LOGGER.info(
            "Extraction complete: %d images, %d bytes, %d errors, %d sparse, parallel=%s, workers=%d",
            len(files_manifest), total_bytes, error_count, sparse_count, used_parallel, effective_workers
        )

        if stats:
            stats.finish_run(evidence_id, self.metadata.name, "success")
        return True

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, int]:
        """
        Run ingestion phase - process images and add to database.

        Workflow:
        1. Load manifest.json
        2. For each image:
           - Check if SHA256 exists in DB
           - If not: process (pHash, EXIF, thumbnail), insert image + discovery
           - If exists: add discovery record only (enrichment)
        3. Update manifest with ingestion stats
        4. Return statistics
        """
        manifest_path = output_dir / "manifest.json"

        if not manifest_path.exists():
            callbacks.on_error("No manifest.json found")
            return {"inserted": 0, "enriched": 0, "errors": 0, "total": 0}

        callbacks.on_step("Loading manifest")
        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data.get("run_id")

        if not run_id:
            callbacks.on_error("Manifest missing run_id")
            return {"inserted": 0, "enriched": 0, "errors": 0, "total": 0}

        # Start statistics tracking for ingestion (may be None in tests)
        evidence_label = config.get("evidence_label", "")
        stats = StatisticsCollector.instance()
        if stats:
            stats.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        files = manifest_data.get("files", [])
        total = len(files)

        if total == 0:
            callbacks.on_log("No files to ingest")
            if stats:
                stats.report_ingested(evidence_id, self.metadata.name, records=0, images=0)
                stats.finish_run(evidence_id, self.metadata.name, "skipped")
            return {"inserted": 0, "enriched": 0, "errors": 0, "total": 0}

        callbacks.on_step(f"Processing {total} images")

        # Clean up previous run if re-ingesting
        deleted = delete_discoveries_by_run(evidence_conn, evidence_id, run_id)
        if deleted > 0:
            callbacks.on_log(f"Cleaned up {deleted} previous discovery records")

        # Prepare image processor for pHash/EXIF/thumbnail
        from core.config import ParallelConfig
        parallel_cfg = ParallelConfig.from_environment()
        processor = ParallelImageProcessor(
            max_workers=parallel_cfg.max_workers,
            enable_parallel=parallel_cfg.enable_parallel and total < 20000,
        )

        extracted_dir = output_dir / "extracted"

        # Collect paths for processing, track missing files
        image_paths = []
        missing_files = 0
        for file_info in files:
            rel_path = file_info.get("rel_path")
            if rel_path:
                full_path = extracted_dir / rel_path
                if full_path.exists():
                    image_paths.append(full_path)
                else:
                    missing_files += 1

        if missing_files > 0:
            callbacks.on_log(
                f"Warning: {missing_files} extracted files not found on disk",
                level="warning",
            )

        # Process images (pHash, EXIF, thumbnail)
        callbacks.on_step("Computing perceptual hashes and EXIF")
        try:
            results = processor.process_images(image_paths, output_dir)
        except Exception as exc:
            LOGGER.warning("Parallel processing failed (%s), retrying sequentially", exc)
            callbacks.on_log("Parallel processing failed; retrying sequentially", level="warning")
            processor = ParallelImageProcessor(enable_parallel=False)
            results = processor.process_images(image_paths, output_dir)

        # Build lookup by path
        result_by_path = {str(r.path): r for r in results}

        # Ingest to database
        callbacks.on_step("Inserting into database")
        inserted = 0
        enriched = 0
        errors = 0

        for file_info in files:
            if callbacks.is_cancelled():
                evidence_conn.rollback()
                if stats:
                    stats.finish_run(evidence_id, self.metadata.name, "cancelled")
                return {"inserted": inserted, "enriched": enriched, "errors": errors, "total": total}

            rel_path = file_info.get("rel_path")
            full_path = str(extracted_dir / rel_path) if rel_path else None

            # Get processing result
            result = result_by_path.get(full_path) if full_path else None

            try:
                # Build image data
                image_data = {
                    "rel_path": rel_path or file_info.get("fs_path"),
                    "filename": file_info.get("filename"),
                    "md5": file_info.get("md5"),
                    "sha256": file_info.get("sha256"),
                    "size_bytes": file_info.get("size_bytes"),
                    "discovered_by": self.metadata.name,
                    "run_id": run_id,
                }

                # Add processing results if available
                if result and result.error is None:
                    image_data["phash"] = result.phash
                    image_data["exif_json"] = result.exif_json if result.exif_json else None
                elif result and result.error:
                    # Hash-only fallback for corrupt images
                    image_data["notes"] = f"Processing error: {result.error}"

                # Build discovery data
                discovery_data = {
                    "discovered_by": self.metadata.name,
                    "run_id": run_id,
                    "extractor_version": self.VERSION,
                    "fs_path": file_info.get("fs_path"),
                    "fs_mtime_epoch": file_info.get("mtime_epoch"),
                    "fs_crtime_epoch": file_info.get("crtime_epoch"),
                    "fs_atime_epoch": file_info.get("atime_epoch"),
                    "fs_ctime_epoch": file_info.get("ctime_epoch"),
                    "fs_inode": file_info.get("inode"),
                }

                # Insert or enrich
                image_id, was_inserted = insert_image_with_discovery(
                    evidence_conn, evidence_id, image_data, discovery_data
                )

                if was_inserted:
                    inserted += 1
                else:
                    enriched += 1

            except Exception as e:
                errors += 1
                LOGGER.warning("Error ingesting %s: %s", file_info.get("fs_path"), e)

        evidence_conn.commit()

        # Update manifest with ingestion stats
        manifest_data["ingestion"] = {
            "inserted": inserted,
            "enriched": enriched,
            "errors": errors,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        manifest_path.write_text(json.dumps(manifest_data, indent=2), encoding="utf-8")

        callbacks.on_log(
            f"Ingestion complete: {inserted} new, {enriched} enriched, {errors} errors"
        )
        LOGGER.info(
            "Ingestion complete: inserted=%d, enriched=%d, errors=%d",
            inserted, enriched, errors
        )

        if stats:
            stats.report_ingested(
                evidence_id, self.metadata.name,
                records=inserted + enriched,
                images=inserted,
                enriched=enriched
            )
            stats.finish_run(evidence_id, self.metadata.name, "success")
        return {
            "inserted": inserted,
            "enriched": enriched,
            "errors": errors,
            "total": total,
        }
