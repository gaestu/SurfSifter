"""
Firefox Cache2 Extractor

Extracts and ingests Firefox HTTP cache with full forensic provenance.
Parses cache2 format (body-first layout), carves images, generates perceptual hashes.

Architecture:
- parser.py: Cache2 format parsing (URL, metadata, elements)
- manifest.py: Incremental JSONL part-file manifest writer
- strategies/: Pluggable extraction strategies (icat, concurrent, sequential)
- ingestion.py: Database ingestion with image carving
- image_carver.py: Image extraction and perceptual hashing
- _schemas.py: Known constants for schema warning discovery

Features:
- Multi-partition discovery via file_list for complete coverage
- Three extraction strategies: icat (fastest), concurrent, sequential
- O(1) incremental manifest writes using JSONL part-files
- Full HTTP header parsing from response-head element
- Image carving with decompression (gzip/brotli/zstd)
- MD5 + SHA-256 dual hashing for forensic chain of custody
- Schema warning support for unknown elements/headers
- Idempotent re-ingestion support

Reference:
- https://www.forensicswiki.org/wiki/Mozilla_Cache2
- https://firefox-source-docs.mozilla.org/netwerk/cache2/cache2.html
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

from PySide6.QtWidgets import (
    QWidget, QLabel, QVBoxLayout, QHBoxLayout, QGroupBox,
    QSpinBox, QComboBox,
)

from ....base import BaseExtractor, ExtractorMetadata
from ....widgets import MultiPartitionWidget
from ....callbacks import ExtractorCallbacks

# New modular imports
from .parser import parse_cache2_entry, Cache2ParseResult
from .manifest import ManifestWriter, create_extraction_summary, load_manifest
from .strategies import (
    CHUNK_SIZE,
    ExtractionResult,
    DiscoveredFile,
    ExtractionContext,
    ExtractionStrategy,
    IcatExtractionStrategy,
    ConcurrentExtractionStrategy,
    SequentialExtractionStrategy,
    icat_available,
    extract_profile_from_path,
)
from .ingestion import CacheIngestionHandler
from .image_carver import carve_image_from_cache_entry

from core.logging import get_logger
from core.database import DatabaseManager, find_case_database, slugify_label
from core.statistics_collector import StatisticsCollector

LOGGER = get_logger("extractors.cache_firefox")

# Configuration Constants
DEFAULT_WORKERS = min(12, os.cpu_count() or 4)
MAX_WORKERS = 16


class CacheFirefoxExtractor(BaseExtractor):
    """
    Extract Firefox cache2 files with modular architecture.

    Firefox switched to "cache2" format in Firefox 32 (2014). Files are stored as:
    - cache2/entries/ directory
    - Each entry is a hash-based filename (e.g., "7F8A3B2E...")

    Extraction is delegated to strategy modules:
    - IcatExtractionStrategy: Uses SleuthKit icat (fastest for E01 with inodes)
    - ConcurrentExtractionStrategy: ThreadPool with per-worker E01 handles
    - SequentialExtractionStrategy: Simple sequential fallback
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="cache_firefox",
            display_name="Firefox Cache",
            description="Extract Firefox HTTP cache files (cache2 format) - modular, multi-partition",
            category="browser",
            requires_tools=[],
            can_extract=True,
            can_ingest=True
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted. Please mount E01 image first."
        return True, ""

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run (manifest exists with ok status)."""
        manifests = list(output_dir.glob("*/manifest.json"))
        if not manifests:
            return False, "No extraction manifests found. Run extraction first."

        latest_manifest = sorted(manifests)[-1]
        try:
            manifest_data = json.loads(latest_manifest.read_text())
            status = manifest_data.get("status", "ok")
            if status != "ok":
                return False, f"Extraction status is '{status}', not 'ok'. Re-run extraction."
        except (json.JSONDecodeError, IOError) as e:
            return False, f"Cannot read manifest: {e}"

        return True, ""

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget for extraction options."""
        widget = QWidget(parent)
        layout = QVBoxLayout(widget)

        # Multi-partition discovery
        self._partition_widget = MultiPartitionWidget(
            parent=widget,
            default_enabled=True,
        )
        layout.addWidget(self._partition_widget)

        # Performance options
        perf_group = QGroupBox("Performance Options")
        perf_layout = QVBoxLayout(perf_group)

        # Worker count
        worker_row = QHBoxLayout()
        worker_label = QLabel("Worker Threads:")
        worker_label.setToolTip(
            "Number of parallel extraction threads.\n"
            "More workers = faster extraction but higher memory usage."
        )
        self._worker_spinbox = QSpinBox()
        self._worker_spinbox.setRange(1, MAX_WORKERS)
        self._worker_spinbox.setValue(DEFAULT_WORKERS)
        self._worker_spinbox.setToolTip("Recommended: 4-8 for HDD, 8-14 for SSD/NVMe")
        worker_row.addWidget(worker_label)
        worker_row.addWidget(self._worker_spinbox)
        worker_row.addStretch()
        perf_layout.addLayout(worker_row)

        # Hash mode
        hash_row = QHBoxLayout()
        hash_label = QLabel("Hash Mode:")
        hash_label.setToolTip(
            "When to compute SHA-256 hashes:\n"
            "• During Extraction: Hash computed inline (slower)\n"
            "• During Ingestion: Faster extraction (recommended)\n"
            "• Disabled: For testing only"
        )
        self._hash_combo = QComboBox()
        self._hash_combo.addItems([
            "During Extraction",
            "During Ingestion (Recommended)",
            "Disabled (Testing Only)",
        ])
        self._hash_combo.setCurrentIndex(1)
        hash_row.addWidget(hash_label)
        hash_row.addWidget(self._hash_combo)
        hash_row.addStretch()
        perf_layout.addLayout(hash_row)

        layout.addWidget(perf_group)
        layout.addStretch()

        return widget

    def _get_config_from_widget(self) -> Dict[str, Any]:
        """Extract configuration values from widget controls."""
        config = {
            "worker_count": DEFAULT_WORKERS,
            "hash_mode": "extraction",
            "scan_all_partitions": True,
        }

        if hasattr(self, "_partition_widget"):
            config.update(self._partition_widget.get_config())

        if hasattr(self, "_worker_spinbox"):
            config["worker_count"] = self._worker_spinbox.value()

        if hasattr(self, "_hash_combo"):
            idx = self._hash_combo.currentIndex()
            config["hash_mode"] = ["extraction", "ingestion", "disabled"][idx]

        return config

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing last extraction run."""
        manifests = sorted(output_dir.glob("*/manifest.json"))
        if not manifests:
            return QLabel("No extraction runs found.", parent)

        latest = manifests[-1]
        manifest_data = json.loads(latest.read_text())
        run_id = manifest_data.get("run_id", "unknown")
        file_count = len(manifest_data.get("files", []))

        status_text = f"Last run: {run_id[:16]}... ({file_count} files)"
        return QLabel(status_text, parent)

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return any(output_dir.glob("*/manifest.json"))

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "cache_firefox"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> bool:
        """
        Extract Firefox cache2 files from evidence filesystem.

        Delegates to appropriate extraction strategy based on:
        1. icat availability and file_list with inodes → IcatStrategy
        2. E01 image with PyEwfTskFS → ConcurrentStrategy
        3. Fallback → SequentialStrategy
        """
        # Merge widget config
        widget_config = self._get_config_from_widget()
        merged_config = {**widget_config, **config}

        worker_count = merged_config.get("worker_count", DEFAULT_WORKERS)
        hash_mode = merged_config.get("hash_mode", "extraction")
        evidence_id = merged_config.get("evidence_id", 0)
        evidence_label = merged_config.get("evidence_label", "")
        run_id = merged_config.get("run_id") or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        scan_all_partitions = merged_config.get("scan_all_partitions", True)

        compute_hash = hash_mode == "extraction"

        # Track with StatisticsCollector
        stats_collector = StatisticsCollector.instance()
        if stats_collector:
            stats_collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        callbacks.on_step("Initializing Firefox cache2 extraction")
        callbacks.on_log(f"Configuration: workers={worker_count}, hash_mode={hash_mode}")

        # Create run directory
        run_dir = output_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

        # Initialize manifest writer with extractor version
        manifest_writer = ManifestWriter(
            run_dir,
            run_id,
            extractor_version=self.metadata.version,
        )
        manifest_writer.begin()

        # Discover files
        discovered_files, discovery_cancelled = self._discover_cache_files(
            evidence_fs, output_dir, merged_config, callbacks
        )

        if discovery_cancelled:
            callbacks.on_log("Discovery cancelled", level="warning")
            manifest_writer.write_partial("cancelled")
            if stats_collector:
                stats_collector.finish_run(evidence_id, self.metadata.name, status="cancelled")
            return False

        if not discovered_files:
            callbacks.on_log("No Firefox cache files found")
            manifest_writer.finalize(create_extraction_summary(
                extraction_mode="none",
                total_discovered=0,
                total_extracted=0,
                total_errors=0,
                duration_seconds=0,
            ))
            if stats_collector:
                stats_collector.finish_run(evidence_id, self.metadata.name, status="success")
            return True

        callbacks.on_log(f"Discovered {len(discovered_files)} cache files")

        # Report discovered to stats
        if stats_collector:
            stats_collector.report_discovered(evidence_id, self.metadata.name, files=len(discovered_files))

        # Create extraction context with all necessary parameters
        import threading
        cancel_event = threading.Event()

        context = ExtractionContext(
            evidence_fs=evidence_fs,
            output_dir=run_dir,
            run_id=run_id,
            manifest_writer=manifest_writer,
            cancel_event=cancel_event,
            progress_callback=callbacks.on_progress,
            log_callback=callbacks.on_log,
            is_cancelled_callback=callbacks.is_cancelled,  # Link to callbacks
            compute_hash=compute_hash,
            extractor_version=self.metadata.version,
        )

        # Select and run strategy
        start_time = time.time()
        strategy = self._select_strategy(evidence_fs, discovered_files, worker_count)
        callbacks.on_log(f"Using extraction strategy: {strategy.name}")

        # For non-icat strategies, filter to only files from the current partition
        # Sequential/concurrent use a single EvidenceFS handle bound to one partition
        files_to_extract = discovered_files
        if strategy.name != "icat":
            current_partition = getattr(evidence_fs, '_partition_index', None)
            if current_partition is not None:
                files_to_extract = [
                    f for f in discovered_files
                    if f.partition_index is None or f.partition_index == current_partition
                ]
                if len(files_to_extract) < len(discovered_files):
                    skipped = len(discovered_files) - len(files_to_extract)
                    callbacks.on_log(
                        f"Filtered {skipped} files from other partitions "
                        f"(current partition: {current_partition})",
                        level="info"
                    )

        try:
            extracted, errors = strategy.run(files_to_extract, context)
        except Exception as e:
            LOGGER.error("Extraction failed: %s", e, exc_info=True)
            callbacks.on_log(f"Extraction error: {e}", level="error")
            manifest_writer.write_partial("error")
            if stats_collector:
                stats_collector.finish_run(evidence_id, self.metadata.name, status="error")
            return False

        duration = time.time() - start_time

        # Finalize manifest
        summary = create_extraction_summary(
            extraction_mode=strategy.name,
            total_discovered=len(discovered_files),
            total_extracted=extracted,
            total_errors=errors,
            duration_seconds=duration,
            hash_mode=hash_mode,
            status="ok",
        )
        manifest_path = manifest_writer.finalize(summary)

        # Record extracted files to audit table
        from extractors._shared.extracted_files_audit import record_browser_files
        if manifest_path.exists():
            manifest_data = json.loads(manifest_path.read_text())
            record_browser_files(
                evidence_conn=config.get("evidence_conn"),
                evidence_id=evidence_id,
                run_id=run_id,
                extractor_name=self.metadata.name,
                extractor_version=self.metadata.version,
                manifest_data=manifest_data,
                callbacks=callbacks,
            )

        callbacks.on_log(
            f"Extraction complete: {extracted} files in {duration:.1f}s "
            f"({extracted/duration:.1f} files/sec)"
        )

        # Report stats
        if stats_collector:
            stats_collector.finish_run(evidence_id, self.metadata.name, status="success")

        return extracted > 0 or errors == 0

    def _discover_cache_files(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> tuple[List[DiscoveredFile], bool]:
        """
        Discover Firefox cache files from evidence via file_list database.

        Requires file_list to be populated (via SleuthKit file list generation).
        This is much faster than filesystem walk and provides inode information
        needed for parallel icat extraction.
        """
        evidence_id = config.get("evidence_id")

        callbacks.on_step("Discovering Firefox cache files from file_list")

        # Open evidence database connection
        db_manager, evidence_conn, ev_id, ev_label = self._open_evidence_conn(
            output_dir, config, callbacks
        )

        if not evidence_conn or not evidence_id:
            callbacks.on_log(
                "file_list not available - run 'Generate File List' first for fast discovery",
                level="error"
            )
            return [], False

        files, cancelled = self._discover_from_file_list(
            evidence_conn, evidence_id, callbacks
        )

        if cancelled:
            return [], True

        if not files:
            callbacks.on_log(
                "No Firefox cache files found in file_list. "
                "Ensure file_list was generated and evidence contains Firefox cache2 data.",
                level="warning"
            )

        return files, False

    def _discover_from_file_list(
        self,
        evidence_conn: sqlite3.Connection,
        evidence_id: int,
        callbacks: ExtractorCallbacks,
    ) -> tuple[List[DiscoveredFile], bool]:
        """Discover cache files via shared file_list discovery.

        Uses :func:`extractors._shared.file_list_discovery.discover_from_file_list`
        with cache-specific SQL LIKE patterns derived from
        :func:`extractors.browser.firefox._patterns.get_cache_discovery_patterns`,
        then classifies each match into its artifact type via
        :func:`extractors.browser.firefox._patterns.classify_cache_path`.
        """
        from extractors._shared.file_list_discovery import (
            discover_from_file_list as _discover,
        )
        from extractors.browser.firefox._patterns import (
            classify_cache_path,
            get_cache_discovery_patterns,
        )

        path_patterns = get_cache_discovery_patterns()
        result = _discover(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            path_patterns=path_patterns,
            exclude_deleted=True,
        )

        if result.is_empty:
            return [], False

        callbacks.on_log(f"Found {result.total_matches} Firefox cache files in file_list")

        files: List[DiscoveredFile] = []
        for match in result.get_all_matches():
            if callbacks.is_cancelled():
                return files, True

            # Extra safety filters not covered by the shared module:
            # TSK-annotated deleted files and NTFS alternate data streams
            if match.file_path.endswith(" (deleted)") or "($FILE_NAME)" in match.file_path:
                continue

            files.append(DiscoveredFile(
                path=match.file_path.lstrip("/"),
                partition_index=match.partition_index,
                inode=match.inode,
                artifact_type=classify_cache_path(match.file_path),
            ))

        # Log breakdown by type
        type_counts: Dict[str, int] = {}
        for f in files:
            type_counts[f.artifact_type] = type_counts.get(f.artifact_type, 0) + 1
        breakdown = ", ".join(f"{k}: {v}" for k, v in sorted(type_counts.items()))
        callbacks.on_log(f"Discovered {len(files)} cache files")
        if breakdown:
            callbacks.on_log(f"Cache file breakdown: {breakdown}")

        return files, False

    def _select_strategy(
        self,
        evidence_fs,
        files: List[DiscoveredFile],
        worker_count: int,
    ) -> ExtractionStrategy:
        """Select best extraction strategy based on context."""
        # Check if files have inodes and icat is available
        files_with_inodes = [f for f in files if f.inode is not None]

        if files_with_inodes and len(files_with_inodes) > len(files) * 0.8 and icat_available():
            # Most files have inodes - try icat strategy
            icat_strategy = IcatExtractionStrategy(max_workers=worker_count)
            # Create a temporary context to validate can_run
            from .manifest import ManifestWriter
            temp_manifest = ManifestWriter(Path("/tmp"), "temp")
            temp_context = ExtractionContext(
                evidence_fs=evidence_fs,
                output_dir=Path("/tmp"),
                run_id="temp",
                manifest_writer=temp_manifest,
            )
            if icat_strategy.can_run(temp_context):
                return icat_strategy

        # Check for E01 with concurrent capability (ewf_paths or source_path)
        ewf_paths = getattr(evidence_fs, 'ewf_paths', None)
        source_path = getattr(evidence_fs, 'source_path', None)
        if ewf_paths or (source_path and str(source_path).lower().endswith('.e01')):
            return ConcurrentExtractionStrategy(max_workers=worker_count)

        # Fallback to sequential
        return SequentialExtractionStrategy()

    def _open_evidence_conn(
        self,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> tuple[Optional[DatabaseManager], Optional[sqlite3.Connection], Optional[int], Optional[str]]:
        """Open evidence database connection for file_list queries."""
        evidence_id = config.get("evidence_id")
        evidence_label = config.get("evidence_label")
        case_root = config.get("case_root")
        case_db_path = config.get("case_db_path")

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
                callbacks.on_log(f"File list lookup failed: {exc}", level="warning")

        if case_root is None or case_db_path is None:
            return None, None, None, None
        if evidence_id is None or evidence_label is None:
            return None, None, None, None

        try:
            db_manager = DatabaseManager(case_root, case_db_path=case_db_path)
            evidence_conn = db_manager.get_evidence_conn(evidence_id, evidence_label)
            return db_manager, evidence_conn, evidence_id, evidence_label
        except Exception as exc:
            callbacks.on_log(f"File list lookup skipped: {exc}", level="warning")
            return None, None, None, None

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> Dict[str, Any]:
        """
        Ingest extracted Firefox cache2 files into evidence database.

        Delegates to CacheIngestionHandler for modular ingestion logic.
        """
        handler = CacheIngestionHandler(
            extractor_name=self.metadata.name,
            extractor_version=self.metadata.version,
        )
        return handler.run(
            output_dir=output_dir,
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            config=config,
            callbacks=callbacks,
        )

    # -------------------------------------------------------------------------
    # Backward Compatibility Methods (for tests that call these directly)
    # -------------------------------------------------------------------------

    def _parse_cache2_entry(self, file_path, file_entry=None):
        """Parse cache2 entry file (backward compatibility wrapper)."""
        result = parse_cache2_entry(
            file_path if isinstance(file_path, Path) else Path(file_path),
            file_entry
        )
        return result.to_dict()

    def _extract_url_from_key(self, key_str):
        """Extract URL from cache key (backward compatibility wrapper)."""
        from .parser import extract_url_from_key as _extract_url
        return _extract_url(key_str)

    def _parse_elements(self, data):
        """Parse elements section (backward compatibility wrapper)."""
        from .parser import parse_elements as _parse_elem
        return _parse_elem(data)

    def _extract_http_metadata(self, elements):
        """Extract HTTP metadata (backward compatibility wrapper)."""
        from .parser import extract_http_metadata as _extract_meta
        result = _extract_meta(elements)
        return result.to_dict()


# Backward compatibility alias
FirefoxCacheExtractor = CacheFirefoxExtractor


# Backward compatibility: expose parser functions as module-level
# These were previously methods on the extractor class
def _parse_cache2_entry_compat(file_path, file_entry=None):
    """Backward compatibility wrapper for parse_cache2_entry."""
    result = parse_cache2_entry(file_path if isinstance(file_path, Path) else Path(file_path), file_entry)
    return result.to_dict()


def _extract_url_from_key_compat(key_str):
    """Backward compatibility wrapper for extract_url_from_key."""
    from .parser import extract_url_from_key
    return extract_url_from_key(key_str)


def _parse_elements_compat(data):
    """Backward compatibility wrapper for parse_elements."""
    from .parser import parse_elements
    return parse_elements(data)


def _extract_http_metadata_compat(elements):
    """Backward compatibility wrapper for extract_http_metadata."""
    from .parser import extract_http_metadata
    result = extract_http_metadata(elements)
    return result.to_dict()
