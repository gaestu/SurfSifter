"""
Chrome Cache Extractor

Extracts and ingests Chrome/Edge/Opera/Brave HTTP cache with full forensic provenance.
Supports both modern simple cache format and legacy blockfile format.

Verified Formats:

1. Simple Cache (modern, post-2013):
   - SimpleFileHeader: 24 bytes (magic, version, key_length, key_hash, padding)
   - Key (URL): variable length following header
   - Stream 1 (body): follows key, may be gzip/brotli/zstd compressed
   - EOF1: 24 bytes with stream 1 size
   - Stream 0 (HTTP headers): Pickle-serialized HttpResponseInfo
   - Optional SHA256: 32 bytes if FLAG_HAS_KEY_SHA256
   - EOF0: 24 bytes with stream 0 size

2. Blockfile Cache (legacy, pre-2015):
   - index: 256-byte header + hash table of CacheAddr entries
   - data_0: 36-byte RankingsNode blocks (LRU tracking)
   - data_1: 256-byte EntryStore blocks (entry metadata)
   - data_2: 1024-byte blocks (small data streams)
   - data_3: 4096-byte blocks (medium data streams)
   - f_XXXXXX: External files for data > 16KB

Features:
- Auto-detect cache format (simple vs blockfile)
- Correct 24-byte header parsing for simple cache
- Full blockfile index/entry parsing per Chromium source
- Key (URL) extraction from both formats
- HTTP header parsing from stream 0
- Body decompression (gzip, brotli, zstd, deflate)
- Image carving from body stream with pHash computation
- Concurrent extraction with per-worker E01 handles (4-8 workers)
- Streaming copy with inline MD5+SHA-256 hashing (1MB chunks)
- Resume capability from partial extractions
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Dict, Any, Optional, List, Tuple
from urllib.parse import urlparse

from PySide6.QtWidgets import (
    QWidget, QLabel, QVBoxLayout, QHBoxLayout, QGroupBox,
    QSpinBox, QComboBox, QCheckBox, QMessageBox,
)

from ....base import BaseExtractor, ExtractorMetadata
from ....callbacks import ExtractorCallbacks
from ...._shared.file_list_discovery import (
    check_file_list_available,
    glob_to_sql_like,
    open_partition_for_extraction,
)
from .._patterns import (
    CHROMIUM_BROWSERS,
    get_patterns,
    get_artifact_patterns,
    get_all_browsers,
    get_browser_display_name,
    get_stable_browsers,
)
from .._embedded_discovery import (
    discover_artifacts_with_embedded_roots,
    discover_embedded_roots,
    get_embedded_root_paths,
)
from ....widgets import BrowserConfigWidget
from ...._shared.extraction_warnings import (
    ExtractionWarningCollector,
)
from core.logging import get_logger
from core.database import (
    insert_image_with_discovery,
    insert_urls,
    insert_browser_inventory,
    update_inventory_ingestion_status,
)
from core.statistics_collector import StatisticsCollector

# Import from refactored modules
from ._schemas import (
    SIMPLE_INITIAL_MAGIC,
    SIMPLE_INDEX_MAGIC,
    SIMPLE_INDEX_VERSION,
    SIMPLE_INDEX_MIN_VERSION,
)
from ._decompression import decompress_body as _decompress_body
from ._index import (
    IndexEntry,
    IndexMetadata,
    parse_index_file as _parse_index_file,
)
from ._parser import (
    CacheEntry,
    parse_cache_entry as _parse_cache_entry,
    read_stream as _read_stream,
    parse_http_headers as _parse_http_headers,
)
from ._carving import (
    detect_image_format as _detect_image_format,
    carve_and_hash_image as _carve_and_hash_image,
    carve_blockfile_image,
)
from ._workers import (
    ResumeAction,
    ExtractionResult,
    get_cache_file_type as _get_cache_file_type,
    get_entry_hash_from_filename as _get_entry_hash_from_filename,
    stream_copy_hash as _stream_copy_hash,
    extraction_worker as _extraction_worker,
    cache_dir_id as _cache_dir_id,
    CHUNK_SIZE,
)
from ._discovery import (
    discover_cache_directories,
    scan_cache_pattern,
    discover_cache_storage_directories,
    extract_profile_from_path,
)
from ._manifest import (
    write_partial_manifest,
    write_final_manifest,
    find_incomplete_extraction,
    load_manifest,
    get_latest_manifest,
)
from ._ingestion import (
    compute_deferred_hashes,
    register_inventory_entry,
    parse_and_ingest_cache_file,
    build_index_lookup,
)
from ._blockfile_ingestion import (
    find_blockfile_directories,
    ingest_blockfile_directory,
)

# Blockfile cache support
from .blockfile import (
    detect_blockfile_cache,
    parse_blockfile_cache,
    read_stream_data,
    BlockfileCacheEntry,
)

if TYPE_CHECKING:
    pass  # Future type imports if needed

LOGGER = get_logger("extractors.cache_simple")

# -----------------------------------------------------------------------------
# Configuration Constants
# -----------------------------------------------------------------------------
WORK_QUEUE_SIZE = 512  # Bounded queue to control memory
FLUSH_INTERVAL_FILES = 500  # Write partial manifest every N files
FLUSH_INTERVAL_BYTES = 50 * 1024 * 1024  # Or every 50 MB
PROGRESS_LOG_INTERVAL = 500  # Log progress every N files
DEFAULT_WORKERS = min(4, os.cpu_count() or 4)
MAX_WORKERS = 8

# Legacy constant for backward compatibility
SIMPLE_CACHE_MAGIC = SIMPLE_INITIAL_MAGIC


class CacheSimpleExtractor(BaseExtractor):
    """
    Extract Chrome/Edge/Opera/Brave simple cache files.

    Chromium switched to "simple cache" format in 2013. Files are stored as:
    - Cache/Cache_Data/{16-hex-hash}_0  (entry file with streams 0+1)
    - Cache/Cache_Data/{16-hex-hash}_1  (optional stream 2, code caching)
    - Cache/Cache_Data/index, index-dir/the-real-index (cache index)
    - Cache/Cache_Data/f_NNNNNN (legacy block files, backward compat)

    Entry file layout (per simple_entry_format.h):
    - 24-byte SimpleFileHeader (magic, version, key_length, key_hash, padding)
    - Key (the URL, key_length bytes)
    - Stream 1 (response body, may be gzip/brotli/zstd compressed)
    - 24-byte EOF1 (final_magic, flags, crc32, stream_size, padding)
    - Stream 0 (HTTP headers, Pickle-serialized HttpResponseInfo)
    - Optional 32-byte SHA256 of key (if FLAG_HAS_KEY_SHA256)
    - 24-byte EOF0 (final_magic, flags, crc32, stream_size, padding)

    Forensic Features:
    - Correct 24-byte header parsing (fixed from incorrect 256-byte)
    - Key extraction directly after header (the URL!)
    - HTTP header parsing from stream 0 (response_code, content_type)
    - Body decompression (gzip, brotli, zstd, deflate)
    - Image carving from stream 1 with pHash computation
    - E01 partition/filesystem context
    - Run ID for idempotent re-ingestion
    - MD5 + SHA-256 hashing with streaming (single pass)
    - Concurrent extraction with per-worker E01 handles
    - Incremental manifest for crash recovery
    - Resume capability from partial extractions
    - Support for Chrome, Edge, Opera, Brave browsers
    """

    @property
    def metadata(self) -> ExtractorMetadata:
        """Return extractor metadata for registry and UI."""
        return ExtractorMetadata(
            name="cache_simple",
            display_name="Chromium Cache",
            description="Extract Chrome/Edge/Opera/Brave HTTP cache (simple + blockfile formats) with URL extraction, HTTP metadata, and image carving",
            category="browser",
            requires_tools=[],  # Pure Python (optional: brotli, zstandard)
            can_extract=True,
            can_ingest=True
        )

    def can_run_extraction(self, evidence_fs) -> tuple[bool, str]:
        """Check if extraction can run."""
        if evidence_fs is None:
            return False, "No evidence filesystem mounted. Please mount E01 image first."
        return True, ""

    def has_existing_output(self, output_dir: Path) -> bool:
        """Check if output directory has existing extraction output."""
        return any(output_dir.glob("*/manifest.json"))

    def can_run_ingestion(self, output_dir: Path) -> tuple[bool, str]:
        """Check if ingestion can run (manifest exists)."""
        manifests = list(output_dir.glob("*/manifest.json"))
        if not manifests:
            return False, "No manifest.json found - run extraction first"
        return True, ""

    def get_config_widget(self, parent: QWidget) -> Optional[QWidget]:
        """Return configuration widget with browser selection and extraction options."""
        widget = QWidget(parent)
        layout = QVBoxLayout(widget)

        # Browser selection with multi-partition support
        browser_group = QGroupBox("Browser Selection")
        browser_layout = QVBoxLayout(browser_group)
        default_browsers = get_stable_browsers()
        self._browser_widget = BrowserConfigWidget(
            None,
            default_browsers=default_browsers,
            supported_browsers=get_all_browsers(),
        )
        browser_layout.addWidget(self._browser_widget)
        layout.addWidget(browser_group)

        # Cache sources group
        sources_group = QGroupBox("Cache Sources")
        sources_layout = QVBoxLayout(sources_group)

        self._disk_cache_checkbox = QCheckBox("Disk Cache (Cache_Data)")
        self._disk_cache_checkbox.setChecked(True)
        self._disk_cache_checkbox.setToolTip("Standard browser HTTP cache (images, scripts, etc.)")
        sources_layout.addWidget(self._disk_cache_checkbox)

        self._cache_storage_checkbox = QCheckBox("Service Worker CacheStorage")
        self._cache_storage_checkbox.setChecked(False)
        self._cache_storage_checkbox.setToolTip(
            "Service Worker CacheStorage used by Progressive Web Apps.\n"
            "May contain offline copies of web assets and API responses."
        )
        sources_layout.addWidget(self._cache_storage_checkbox)

        layout.addWidget(sources_group)

        # Performance options group
        perf_group = QGroupBox("Performance Options")
        perf_layout = QVBoxLayout(perf_group)

        # Worker count
        worker_row = QHBoxLayout()
        worker_label = QLabel("Worker Threads:")
        worker_label.setToolTip(
            "Number of parallel extraction threads. Each worker opens its own E01 handle.\n"
            "More workers = faster extraction but higher memory usage (~50MB per worker)."
        )
        self._worker_spinbox = QSpinBox()
        self._worker_spinbox.setRange(1, MAX_WORKERS)
        self._worker_spinbox.setValue(DEFAULT_WORKERS)
        self._worker_spinbox.setToolTip("Recommended: 4 for HDD, 4-8 for SSD")
        worker_row.addWidget(worker_label)
        worker_row.addWidget(self._worker_spinbox)
        worker_row.addStretch()
        perf_layout.addLayout(worker_row)

        # Hash mode
        hash_row = QHBoxLayout()
        hash_label = QLabel("Hash Mode:")
        hash_label.setToolTip(
            "When to compute MD5+SHA-256 hashes:\n"
            "• During Extraction: Forensically preferred, hash computed inline\n"
            "• During Ingestion: Faster extraction, hash from local files\n"
            "• Disabled: For testing only, no chain of custody"
        )
        self._hash_combo = QComboBox()
        self._hash_combo.addItems([
            "During Extraction (Recommended)",
            "During Ingestion",
            "Disabled (Testing Only)",
        ])
        self._hash_combo.setCurrentIndex(0)
        hash_row.addWidget(hash_label)
        hash_row.addWidget(self._hash_combo)
        hash_row.addStretch()
        perf_layout.addLayout(hash_row)

        layout.addWidget(perf_group)

        # Resume options group
        resume_group = QGroupBox("Resume Options")
        resume_layout = QVBoxLayout(resume_group)

        self._resume_checkbox = QCheckBox("Resume from partial extraction if available")
        self._resume_checkbox.setChecked(True)
        self._resume_checkbox.setToolTip(
            "If a previous extraction was interrupted, skip already-copied files.\n"
            "Saves time when restarting after crashes or cancellations."
        )
        resume_layout.addWidget(self._resume_checkbox)

        layout.addWidget(resume_group)
        layout.addStretch()

        return widget

    def _get_config_from_widget(self) -> Dict[str, Any]:
        """Extract configuration values from widget controls."""
        config = {
            "browsers": ["chrome", "edge", "opera", "brave"],
            "worker_count": DEFAULT_WORKERS,
            "hash_mode": "extraction",
            "resume_enabled": True,
            "include_disk_cache": True,
            "include_cache_storage": False,
        }

        if hasattr(self, "_browser_widget"):
            config["browsers"] = self._browser_widget.get_selected_browsers()

        if hasattr(self, "_worker_spinbox"):
            config["worker_count"] = self._worker_spinbox.value()

        if hasattr(self, "_hash_combo"):
            idx = self._hash_combo.currentIndex()
            config["hash_mode"] = ["extraction", "ingestion", "disabled"][idx]

        if hasattr(self, "_resume_checkbox"):
            config["resume_enabled"] = self._resume_checkbox.isChecked()

        if hasattr(self, "_disk_cache_checkbox"):
            config["include_disk_cache"] = self._disk_cache_checkbox.isChecked()

        if hasattr(self, "_cache_storage_checkbox"):
            config["include_cache_storage"] = self._cache_storage_checkbox.isChecked()

        return config

    # -------------------------------------------------------------------------
    # Discovery method wrappers (delegate to module functions)
    # These enable patching in tests while keeping logic in separate modules.
    # -------------------------------------------------------------------------

    def _discover_cache_directories(
        self,
        evidence_fs,
        browsers: List[str],
        callbacks: "ExtractorCallbacks",
        include_cache_storage: bool = False,
        include_disk_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Scan evidence for Chromium browser cache directories.

        This implementation delegates to self._scan_cache_pattern and
        self._discover_cache_storage_directories to enable test patching.
        """
        cache_directories = []

        for browser in browsers:
            if browser not in CHROMIUM_BROWSERS:
                callbacks.on_log(f"Unknown browser: {browser}", "warning")
                continue

            display_name = get_browser_display_name(browser)
            callbacks.on_log(f"Scanning for {display_name} cache directories")

            # Scan disk cache patterns (only if enabled)
            if include_disk_cache:
                cache_patterns = get_patterns(browser, "cache")
                for pattern in cache_patterns:
                    dirs = self._scan_cache_pattern(
                        evidence_fs, pattern, browser, "disk_cache", callbacks
                    )
                    cache_directories.extend(dirs)

            # Scan CacheStorage if enabled
            if include_cache_storage:
                try:
                    from extractors.browser_patterns import get_browser_paths
                    cache_storage_patterns = get_browser_paths(browser, 'cache_storage')
                    if cache_storage_patterns:
                        callbacks.on_log(f"Scanning {display_name} Service Worker CacheStorage")
                        dirs = self._discover_cache_storage_directories(
                            evidence_fs, browser, cache_storage_patterns, callbacks
                        )
                        cache_directories.extend(dirs)
                except Exception as e:
                    LOGGER.debug("CacheStorage discovery failed for %s: %s", browser, e)

        return cache_directories

    def _scan_cache_pattern(
        self,
        evidence_fs,
        pattern: str,
        browser: str,
        cache_type: str,
        callbacks: "ExtractorCallbacks",
    ) -> List[Dict[str, Any]]:
        """Wrapper for scan_cache_pattern module function."""
        return scan_cache_pattern(
            evidence_fs=evidence_fs,
            pattern=pattern,
            browser=browser,
            cache_type=cache_type,
            callbacks=callbacks,
        )

    def _discover_cache_storage_directories(
        self,
        evidence_fs,
        browser: str,
        patterns: List[str],
        callbacks: "ExtractorCallbacks",
    ) -> List[Dict[str, Any]]:
        """Wrapper for discover_cache_storage_directories module function."""
        return discover_cache_storage_directories(
            evidence_fs=evidence_fs,
            browser=browser,
            patterns=patterns,
            callbacks=callbacks,
        )

    def _extract_profile_from_path(self, file_path: str, browser: str) -> str:
        """Wrapper for extract_profile_from_path module function."""
        return extract_profile_from_path(file_path, browser)

    def get_status_widget(
        self,
        parent: QWidget,
        output_dir: Path,
        evidence_conn,
        evidence_id: int
    ) -> QWidget:
        """Return status widget showing extraction/ingestion state."""
        manifests = sorted(output_dir.glob("*/manifest.json"))
        if manifests:
            manifest = manifests[-1]
            data = json.loads(manifest.read_text())
            file_count = len(data.get("files", []))
            status = data.get("status", "ok")
            hash_mode = data.get("hash_mode", "extraction")
            status_text = (
                f"Chrome Cache\n"
                f"Files extracted: {file_count}\n"
                f"Run ID: {data.get('run_id', 'N/A')}\n"
                f"Status: {status}\n"
                f"Hash Mode: {hash_mode}"
            )
        else:
            status_text = "Chrome Cache\nNo extraction run yet"

        return QLabel(status_text, parent)

    def get_output_dir(self, case_root: Path, evidence_label: str, config: Optional[Dict[str, Any]] = None) -> Path:
        """Return output directory for this extractor."""
        return case_root / "evidences" / evidence_label / "cache_simple"

    def run_extraction(
        self,
        evidence_fs,
        output_dir: Path,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> bool:
        """
        Extract Chrome/Edge simple cache files from evidence.

        Uses concurrent extraction for E01 images, sequential for mounted filesystems.
        """
        callbacks.on_step("Initializing Chromium cache extraction")

        # Get configuration from widget or use provided config
        widget_config = self._get_config_from_widget()
        merged_config = {**widget_config, **config}

        # Default to stable browsers if not specified
        browsers = merged_config.get("browsers") or merged_config.get("selected_browsers", get_stable_browsers())
        evidence_id = merged_config.get("evidence_id", 0)
        evidence_label = merged_config.get("evidence_label", "")
        worker_count = merged_config.get("worker_count", DEFAULT_WORKERS)
        hash_mode = merged_config.get("hash_mode", "extraction")
        resume_enabled = merged_config.get("resume_enabled", True)
        include_cache_storage = merged_config.get("include_cache_storage", False)
        include_disk_cache = merged_config.get("include_disk_cache", True)

        # Check for incomplete extraction
        incomplete = None
        if resume_enabled and output_dir.exists():
            incomplete = self._find_incomplete_extraction(output_dir)

        if incomplete:
            action = self._prompt_resume_action(incomplete)
            if action == ResumeAction.CANCEL:
                callbacks.on_log("Extraction cancelled by user")
                return False
            elif action == ResumeAction.OVERWRITE:
                partial_path = incomplete["run_dir"] / "manifest.partial.json"
                if partial_path.exists():
                    partial_path.unlink()
                incomplete = None

        # Determine run_id and output directory
        if incomplete:
            run_id = incomplete["run_id"]
            run_output = incomplete["run_dir"]
            already_copied = {
                (f.get("partition_index"), f.get("source_path"))
                for f in incomplete["files"]
                if f.get("source_path")
            }
            manifest_files = incomplete["files"]
            extraction_stats = incomplete["stats"]
            hash_mode = incomplete.get("hash_mode", hash_mode)
            LOGGER.info("Resuming extraction run_id=%s, %d files already copied", run_id, len(already_copied))
            callbacks.on_log(f"Resuming extraction: {len(already_copied)} files already copied")
        else:
            run_id = self._generate_run_id()
            run_output = output_dir / run_id
            run_output.mkdir(parents=True, exist_ok=True)
            already_copied = set()
            manifest_files = []
            extraction_stats = {
                "cache_files_discovered": 0,
                "cache_files_copied": 0,
                "cache_files_skipped": 0,
                "cache_files_failed": 0,
                "bytes_copied": 0,
            }

        LOGGER.info("Starting cache extraction (run_id=%s)", run_id)

        # START: Begin statistics tracking
        collector = StatisticsCollector.instance()
        if collector:
            collector.start_run(evidence_id, evidence_label, self.metadata.name, run_id)

        # Scan for cache directories using partition-aware discovery.
        callbacks.on_step("Scanning for cache directories")
        evidence_conn = merged_config.get("evidence_conn")
        scan_all_partitions = merged_config.get("scan_all_partitions", True)

        discovered_embedded_roots = []
        if evidence_conn is not None:
            try:
                discovered_embedded_roots = discover_embedded_roots(evidence_conn, evidence_id)
            except Exception as e:
                LOGGER.debug("Embedded root discovery failed for cache extractor: %s", e)

        partition_indices: List[Optional[int]] = [None]
        if scan_all_partitions and evidence_conn is not None:
            try:
                partition_indices = self._discover_cache_partitions(
                    evidence_conn=evidence_conn,
                    evidence_id=evidence_id,
                    browsers=browsers,
                    include_disk_cache=include_disk_cache,
                )
            except Exception as e:
                LOGGER.debug("Cache partition discovery failed, falling back to active partition: %s", e)
                partition_indices = [None]

        if any(idx is not None for idx in partition_indices) and not (hasattr(evidence_fs, "ewf_paths") and evidence_fs.ewf_paths):
            partition_indices = [None]

        discovered_by_partition: Dict[Optional[int], List[Dict[str, Any]]] = {}
        for partition_index in partition_indices:
            with open_partition_for_extraction(
                evidence_fs.ewf_paths if partition_index is not None and hasattr(evidence_fs, "ewf_paths") else evidence_fs,
                partition_index,
            ) as partition_fs:
                if partition_fs is None:
                    continue

                effective_partition = partition_index
                if effective_partition is None:
                    effective_partition = getattr(partition_fs, "partition_index", 0)

                embedded_roots = get_embedded_root_paths(
                    discovered_embedded_roots,
                    partition_index=effective_partition,
                )

                dirs = discover_cache_directories(
                    partition_fs,
                    browsers,
                    callbacks,
                    include_cache_storage=include_cache_storage,
                    include_disk_cache=include_disk_cache,
                    embedded_roots=embedded_roots,
                )

                dedup_seen: set[tuple] = set()
                normalized_dirs: List[Dict[str, Any]] = []
                for item in dirs:
                    item["partition_index"] = effective_partition
                    dedup_key = (
                        item.get("partition_index"),
                        item.get("path"),
                        item.get("browser"),
                        item.get("cache_type"),
                    )
                    if dedup_key in dedup_seen:
                        continue
                    dedup_seen.add(dedup_key)
                    normalized_dirs.append(item)

                if normalized_dirs:
                    discovered_by_partition[effective_partition] = normalized_dirs

        if not discovered_by_partition:
            callbacks.on_log("No cache directories found")
            if collector:
                collector.report_discovered(evidence_id, self.metadata.name, files=0)
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            self._write_final_manifest(
                run_output, run_id, evidence_id, manifest_files, extraction_stats, hash_mode,
                status="skipped", notes=["No cache directories found"],
                extraction_tool=self._get_extraction_tool_version(),
                e01_context=self._get_e01_context(evidence_fs),
                config={"browsers": browsers},
            )
            return True

        # Build extraction work grouped by partition.
        all_files_by_partition: Dict[Optional[int], List[Tuple[Dict, Dict]]] = {}
        total_pending = 0
        for partition_index, cache_dirs in discovered_by_partition.items():
            partition_files: List[Tuple[Dict, Dict]] = []
            for cache_dir_info in cache_dirs:
                for file_info in cache_dir_info["files"]:
                    key = (partition_index, file_info["path"])
                    if key in already_copied or (None, file_info["path"]) in already_copied:
                        continue
                    partition_files.append((file_info, cache_dir_info))
            if partition_files:
                all_files_by_partition[partition_index] = partition_files
                total_pending += len(partition_files)

        extraction_stats["cache_files_discovered"] = total_pending + len(already_copied)
        extraction_stats["cache_files_skipped"] = len(already_copied)

        if collector:
            collector.report_discovered(evidence_id, self.metadata.name, files=extraction_stats["cache_files_discovered"])

        callbacks.on_log(f"Total files to extract: {total_pending} (skipping {len(already_copied)} already copied)")

        if total_pending == 0:
            callbacks.on_log("All files already extracted")
            if collector:
                collector.finish_run(evidence_id, self.metadata.name, status="success")
            self._write_final_manifest(
                run_output, run_id, evidence_id, manifest_files, extraction_stats, hash_mode,
                status="ok", notes=[],
                extraction_tool=self._get_extraction_tool_version(),
                e01_context=self._get_e01_context(evidence_fs),
                config={"browsers": browsers},
            )
            return True

        # Extract per partition.
        for partition_index, partition_files in all_files_by_partition.items():
            if not partition_files:
                continue

            if partition_index is not None and hasattr(evidence_fs, "ewf_paths") and evidence_fs.ewf_paths:
                callbacks.on_log(
                    f"Extracting partition {partition_index} with {worker_count} workers",
                    "info",
                )
                partition_fs_context = SimpleNamespace(
                    ewf_paths=evidence_fs.ewf_paths,
                    partition_index=partition_index,
                    source_path=getattr(evidence_fs, "source_path", None),
                    fs_type=getattr(evidence_fs, "fs_type", "unknown"),
                )
                self._run_concurrent_extraction(
                    partition_fs_context,
                    partition_files,
                    output_dir,
                    run_id,
                    evidence_id,
                    worker_count,
                    hash_mode,
                    manifest_files,
                    extraction_stats,
                    callbacks,
                )
            else:
                callbacks.on_log(
                    f"Extracting partition {partition_index if partition_index is not None else 'current'} sequentially",
                    "info",
                )
                with open_partition_for_extraction(evidence_fs, partition_index) as partition_fs:
                    if partition_fs is None:
                        continue
                    self._run_sequential_extraction(
                        partition_fs,
                        partition_files,
                        run_output,
                        run_id,
                        evidence_id,
                        hash_mode,
                        manifest_files,
                        extraction_stats,
                        callbacks,
                    )

        # Determine final status
        status = "ok"
        notes = []
        if extraction_stats["cache_files_failed"] > 0:
            status = "partial"
            notes.append(f"{extraction_stats['cache_files_failed']} files failed to extract")
            if collector:
                collector.report_failed(evidence_id, self.metadata.name, files=extraction_stats["cache_files_failed"])

        # Write final manifest
        self._write_final_manifest(
            run_output, run_id, evidence_id, manifest_files, extraction_stats, hash_mode,
            status=status, notes=notes,
            extraction_tool=self._get_extraction_tool_version(),
            e01_context=self._get_e01_context(evidence_fs),
            config={"browsers": browsers},
        )

        # Record extracted files to audit table
        from extractors._shared.extracted_files_audit import record_browser_files
        manifest_path = run_output / "manifest.json"
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

        # Remove partial manifest if exists
        partial_path = run_output / "manifest.partial.json"
        if partial_path.exists():
            partial_path.unlink()

        LOGGER.info(
            "Cache extraction complete: %d files copied, %d failed, status=%s",
            extraction_stats["cache_files_copied"],
            extraction_stats["cache_files_failed"],
            status,
        )

        # FINISH: End statistics tracking
        if collector:
            final_status = "success" if status == "ok" else ("partial" if status == "partial" else "error")
            collector.finish_run(evidence_id, self.metadata.name, status=final_status)

        return status != "error"

    def _discover_cache_partitions(
        self,
        evidence_conn,
        evidence_id: int,
        browsers: List[str],
        include_disk_cache: bool,
    ) -> List[Optional[int]]:
        """Discover partitions that likely contain Chromium cache artifacts."""
        available, _ = check_file_list_available(evidence_conn, evidence_id)
        if not available:
            return [None]

        path_patterns = set()
        if include_disk_cache:
            for browser in browsers:
                if browser not in CHROMIUM_BROWSERS:
                    continue
                for pattern in get_artifact_patterns(browser, "cache"):
                    path_patterns.add(glob_to_sql_like(pattern))

        if not path_patterns:
            return [None]

        result, embedded_roots = discover_artifacts_with_embedded_roots(
            evidence_conn,
            evidence_id,
            artifact="cache",
            filename_patterns=["index", "the-real-index", "f_*", "data_*"],
            path_patterns=sorted(path_patterns) if path_patterns else None,
        )

        partitions = set(result.matches_by_partition.keys())
        partitions.update(root.partition_index for root in embedded_roots if root.partition_index is not None)
        if not partitions:
            return [None]
        return sorted(partitions)

    # -------------------------------------------------------------------------
    # Resume Detection & Handling
    # -------------------------------------------------------------------------

    def _find_incomplete_extraction(self, output_dir: Path) -> Optional[Dict[str, Any]]:
        """Find an incomplete extraction (has partial manifest but no final manifest)."""
        return find_incomplete_extraction(output_dir)

    def _prompt_resume_action(self, incomplete_info: Dict[str, Any]) -> ResumeAction:
        """Prompt user for action when incomplete extraction is detected."""
        run_id = incomplete_info["run_id"]
        file_count = len(incomplete_info["files"])

        msg = QMessageBox()
        msg.setIcon(QMessageBox.Question)
        msg.setWindowTitle("Incomplete Extraction Found")
        msg.setText(
            f"An incomplete extraction was found:\n\n"
            f"Run ID: {run_id}\n"
            f"Files copied: {file_count}\n\n"
            "What would you like to do?"
        )

        cancel_btn = msg.addButton("Cancel", QMessageBox.RejectRole)
        overwrite_btn = msg.addButton("Start Fresh", QMessageBox.DestructiveRole)
        continue_btn = msg.addButton("Continue", QMessageBox.AcceptRole)
        msg.setDefaultButton(continue_btn)

        msg.exec()

        clicked = msg.clickedButton()
        if clicked == continue_btn:
            return ResumeAction.CONTINUE
        elif clicked == overwrite_btn:
            return ResumeAction.OVERWRITE
        else:
            return ResumeAction.CANCEL

    def _write_partial_manifest(
        self,
        run_dir: Path,
        run_id: str,
        evidence_id: int,
        manifest_files: List[Dict],
        stats: Dict[str, Any],
        hash_mode: str,
    ) -> None:
        """Write incremental partial manifest for crash recovery."""
        write_partial_manifest(run_dir, run_id, evidence_id, manifest_files, stats, hash_mode)

    def _can_use_concurrent_extraction(self, evidence_fs) -> bool:
        """Check if concurrent extraction is possible (E01 with ewf_paths)."""
        return hasattr(evidence_fs, 'ewf_paths') and evidence_fs.ewf_paths

    def _run_concurrent_extraction(
        self,
        evidence_fs,
        files_to_extract: List[Tuple[Dict, Dict]],
        output_dir: Path,
        run_id: str,
        evidence_id: int,
        worker_count: int,
        hash_mode: str,
        manifest_files: List[Dict],
        stats: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> None:
        """Run concurrent extraction with worker threads."""
        compute_hash = hash_mode == "extraction"
        ewf_paths = evidence_fs.ewf_paths
        partition_index = evidence_fs.partition_index
        run_output = output_dir / run_id

        work_queue = queue.Queue(maxsize=WORK_QUEUE_SIZE)
        result_queue = queue.Queue()
        stop_event = threading.Event()

        workers = []
        for i in range(worker_count):
            t = threading.Thread(
                target=_extraction_worker,
                args=(
                    ewf_paths, partition_index, work_queue, result_queue,
                    output_dir, run_id, compute_hash, stop_event, i
                ),
                daemon=True,
            )
            t.start()
            workers.append(t)

        callbacks.on_step("Extracting cache files")
        total_files = len(files_to_extract)

        producer_thread = threading.Thread(
            target=self._producer_thread,
            args=(files_to_extract, work_queue, len(workers), stop_event, callbacks),
            daemon=True,
        )
        producer_thread.start()

        start_time = time.time()
        processed = 0
        bytes_copied = stats.get("bytes_copied", 0)
        files_since_flush = 0
        bytes_since_flush = 0

        try:
            while processed < total_files:
                if callbacks.is_cancelled():
                    stop_event.set()
                    callbacks.on_log("Extraction cancelled by user")
                    break

                try:
                    result = result_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                processed += 1

                if result.source_path == "__WORKER_INIT_ERROR__":
                    stats["cache_files_failed"] += 1
                    LOGGER.error("Worker init error: %s", result.error)
                    continue

                if result.success:
                    e01_context = self._get_e01_context(evidence_fs)
                    forensic_path = ""
                    if e01_context.get("image_path"):
                        forensic_path = f"E01://{Path(e01_context['image_path']).name}/{e01_context.get('fs_type', 'unknown')}/{result.source_path}"

                    manifest_entry = {
                        "source_path": result.source_path,
                        "logical_path": result.source_path,
                        "forensic_path": forensic_path,
                        "partition_index": e01_context.get("partition_index", 0),
                        "fs_type": e01_context.get("fs_type", "unknown"),
                        "extracted_path": result.extracted_path,
                        "size_bytes": result.size_bytes,
                        "md5": result.md5,
                        "sha256": result.sha256,
                        "browser": result.browser,
                        "profile": result.profile,
                        "artifact_type": "cache_simple",
                        "file_type": result.file_type,
                        "entry_hash": result.entry_hash,
                    }
                    manifest_files.append(manifest_entry)
                    stats["cache_files_copied"] += 1
                    bytes_copied += result.size_bytes
                    stats["bytes_copied"] = bytes_copied
                    files_since_flush += 1
                    bytes_since_flush += result.size_bytes
                else:
                    stats["cache_files_failed"] += 1
                    LOGGER.warning("Failed to extract %s: %s", result.source_path, result.error)

                if processed % PROGRESS_LOG_INTERVAL == 0 or processed == total_files:
                    elapsed = time.time() - start_time
                    rate_mb = (bytes_copied / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                    eta_secs = (total_files - processed) / (processed / elapsed) if processed > 0 and elapsed > 0 else 0
                    eta_str = f"{int(eta_secs // 60)}m {int(eta_secs % 60)}s" if eta_secs > 0 else "calculating..."

                    callbacks.on_log(
                        f"[{processed}/{total_files}] {bytes_copied / (1024*1024):.1f} MB, "
                        f"{rate_mb:.1f} MB/s, ETA: {eta_str}"
                    )

                callbacks.on_progress(processed, total_files)

                if files_since_flush >= FLUSH_INTERVAL_FILES or bytes_since_flush >= FLUSH_INTERVAL_BYTES:
                    self._write_partial_manifest(
                        run_output, run_id, evidence_id, manifest_files, stats, hash_mode
                    )
                    files_since_flush = 0
                    bytes_since_flush = 0

        finally:
            stop_event.set()

            for _ in workers:
                try:
                    work_queue.put_nowait(None)
                except queue.Full:
                    pass

            for t in workers:
                t.join(timeout=5.0)

            producer_thread.join(timeout=2.0)

    def _producer_thread(
        self,
        files_to_extract: List[Tuple[Dict, Dict]],
        work_queue: queue.Queue,
        num_workers: int,
        stop_event: threading.Event,
        callbacks: ExtractorCallbacks,
    ) -> None:
        """Producer thread: enqueue files into work queue."""
        try:
            for file_info, cache_dir_info in files_to_extract:
                if stop_event.is_set():
                    break
                work_queue.put((file_info, cache_dir_info))

            for _ in range(num_workers):
                if stop_event.is_set():
                    break
                work_queue.put(None)
        except Exception as e:
            LOGGER.error("Producer thread error: %s", e)

    def _run_sequential_extraction(
        self,
        evidence_fs,
        files_to_extract: List[Tuple[Dict, Dict]],
        run_output: Path,
        run_id: str,
        evidence_id: int,
        hash_mode: str,
        manifest_files: List[Dict],
        stats: Dict[str, Any],
        callbacks: ExtractorCallbacks,
    ) -> None:
        """Run sequential extraction for non-E01 evidence (MountedFS)."""
        compute_hash = hash_mode == "extraction"
        total_files = len(files_to_extract)

        start_time = time.time()
        bytes_copied = stats.get("bytes_copied", 0)
        files_since_flush = 0
        bytes_since_flush = 0

        for idx, (file_info, cache_dir_info) in enumerate(files_to_extract):
            if callbacks.is_cancelled():
                callbacks.on_log("Extraction cancelled by user")
                break

            browser = cache_dir_info["browser"]
            profile = cache_dir_info.get("profile", "Default")
            partition_index = cache_dir_info.get("partition_index", getattr(evidence_fs, "partition_index", 0))
            source_path = file_info["path"]
            filename = file_info["filename"]

            file_type = _get_cache_file_type(filename)
            entry_hash = _get_entry_hash_from_filename(filename)

            try:
                source_cache_path = cache_dir_info.get("path", "")
                subdir_id = _cache_dir_id(source_cache_path)
                profile_dir = run_output / f"p{partition_index}_{browser}_{profile}" / subdir_id
                profile_dir.mkdir(parents=True, exist_ok=True)

                dest_path = profile_dir / filename

                with evidence_fs.open_for_read(source_path) as src:
                    size_bytes, md5, sha256 = _stream_copy_hash(src, dest_path, compute_hash)

                e01_context = self._get_e01_context(evidence_fs)
                forensic_path = ""
                if e01_context.get("image_path"):
                    forensic_path = f"E01://{Path(e01_context['image_path']).name}/{e01_context.get('fs_type', 'unknown')}/{source_path}"

                manifest_entry = {
                    "source_path": source_path,
                    "logical_path": source_path,
                    "forensic_path": forensic_path,
                    "partition_index": partition_index,
                    "fs_type": e01_context.get("fs_type", "unknown"),
                    "extracted_path": str(dest_path.relative_to(run_output.parent)),
                    "size_bytes": size_bytes,
                    "md5": md5,
                    "sha256": sha256,
                    "browser": browser,
                    "profile": profile,
                    "artifact_type": "cache_simple",
                    "file_type": file_type,
                    "entry_hash": entry_hash,
                }
                manifest_files.append(manifest_entry)
                stats["cache_files_copied"] += 1
                bytes_copied += size_bytes
                stats["bytes_copied"] = bytes_copied
                files_since_flush += 1
                bytes_since_flush += size_bytes

            except Exception as e:
                error_msg = f"Failed to extract {source_path}: {e}"
                LOGGER.error(error_msg, exc_info=True)
                stats["cache_files_failed"] += 1
                callbacks.on_error(error_msg, "")

            processed = idx + 1
            if processed % PROGRESS_LOG_INTERVAL == 0 or processed == total_files:
                elapsed = time.time() - start_time
                rate_mb = (bytes_copied / (1024 * 1024)) / elapsed if elapsed > 0 else 0
                eta_secs = (total_files - processed) / (processed / elapsed) if processed > 0 and elapsed > 0 else 0
                eta_str = f"{int(eta_secs // 60)}m {int(eta_secs % 60)}s" if eta_secs > 0 else "calculating..."

                callbacks.on_log(
                    f"[{processed}/{total_files}] {bytes_copied / (1024*1024):.1f} MB, "
                    f"{rate_mb:.1f} MB/s, ETA: {eta_str}"
                )

            callbacks.on_progress(processed, total_files)

            if files_since_flush >= FLUSH_INTERVAL_FILES or bytes_since_flush >= FLUSH_INTERVAL_BYTES:
                self._write_partial_manifest(
                    run_output, run_id, evidence_id, manifest_files, stats, hash_mode
                )
                files_since_flush = 0
                bytes_since_flush = 0

    def _write_final_manifest(
        self,
        run_output: Path,
        run_id: str,
        evidence_id: int,
        manifest_files: List[Dict],
        stats: Dict[str, Any],
        hash_mode: str,
        status: str,
        notes: List[str],
        extraction_tool: str,
        e01_context: Dict[str, Any],
        config: Dict[str, Any],
    ) -> None:
        """Write final manifest.json with all extraction metadata."""
        write_final_manifest(
            run_output, run_id, evidence_id, manifest_files, stats, hash_mode,
            status, notes, extraction_tool, e01_context, config
        )

    def run_ingestion(
        self,
        output_dir: Path,
        evidence_conn,
        evidence_id: int,
        config: Dict[str, Any],
        callbacks: ExtractorCallbacks
    ) -> Dict[str, int]:
        """Parse extracted cache files and ingest into database."""
        callbacks.on_step("Reading manifest")

        evidence_label = config.get("evidence_label", "")

        manifests = sorted(output_dir.glob("*/manifest.json"))
        if not manifests:
            LOGGER.warning("No manifests found in %s", output_dir)
            callbacks.on_error("No manifest found - run extraction first", str(output_dir))
            return {"urls": 0, "images": 0, "records": 0}

        manifest_path = manifests[-1]
        manifest_data = json.loads(manifest_path.read_text())
        run_id = manifest_data["run_id"]

        warning_collector = ExtractionWarningCollector(
            extractor_name=self.metadata.name,
            run_id=run_id,
            evidence_id=evidence_id,
        )

        collector = StatisticsCollector.instance()
        if collector:
            collector.continue_run(evidence_id, evidence_label, self.metadata.name, run_id)

        LOGGER.info("Starting ingestion for run_id=%s", run_id)

        # Deferred hash computation if needed
        hash_mode = manifest_data.get("hash_mode", "extraction")
        if hash_mode == "ingestion":
            callbacks.on_step("Computing deferred hashes")
            compute_deferred_hashes(manifest_data, manifest_path, output_dir, callbacks)

        stats = {
            "urls": 0,
            "images": 0,
            "records": 0,
            "inventory_entries": 0,
            "index_entries": 0,
            "blockfile_entries": 0,
        }

        files = manifest_data.get("files", [])

        # Phase 0: Detect and process blockfile cache directories
        blockfile_dirs = self._find_blockfile_directories(files, output_dir)
        blockfile_processed_files = set()

        if blockfile_dirs:
            callbacks.on_step(f"Processing {len(blockfile_dirs)} blockfile cache directories")
            for cache_dir_info in blockfile_dirs:
                cache_dir = cache_dir_info["path"]
                browser = cache_dir_info.get("browser", "chrome")
                profile = cache_dir_info.get("profile", "Default")

                blockfile_file_entries = [
                    fe for fe in files
                    if fe.get("extracted_path", "") in cache_dir_info["files"]
                ]

                try:
                    blockfile_result = self._ingest_blockfile_directory(
                        evidence_conn=evidence_conn,
                        evidence_id=evidence_id,
                        run_id=run_id,
                        cache_dir=cache_dir,
                        browser=browser,
                        profile=profile,
                        extraction_dir=output_dir,
                        callbacks=callbacks,
                        warning_collector=warning_collector,
                        manifest_data=manifest_data,
                        file_entries=blockfile_file_entries,
                    )
                    stats["urls"] += blockfile_result["urls"]
                    stats["images"] += blockfile_result["images"]
                    stats["records"] += blockfile_result["records"]
                    stats["blockfile_entries"] += blockfile_result.get("entries", 0)
                    stats["inventory_entries"] += blockfile_result.get("inventory_entries", 0)

                    callbacks.on_log(
                        f"Blockfile cache {browser}/{profile}: "
                        f"{blockfile_result['entries']} entries, {blockfile_result['urls']} URLs"
                    )

                    for f in cache_dir_info["files"]:
                        blockfile_processed_files.add(f)

                except Exception as e:
                    LOGGER.error("Failed to process blockfile cache %s: %s", cache_dir, e, exc_info=True)
                    callbacks.on_error(f"Blockfile cache error: {e}", str(cache_dir))

        # Phase 1: Parse index files
        callbacks.on_progress(0, len(files), "Parsing index files")
        index_lookup = self._build_index_lookup(files, output_dir, callbacks)
        stats["index_entries"] = len(index_lookup)

        if index_lookup:
            callbacks.on_log(f"Loaded {len(index_lookup)} entries from cache index files")

        # Phase 2: Parse cache entry files
        callbacks.on_progress(0, len(files), "Parsing cache files")

        for i, file_entry in enumerate(files):
            extracted_path = file_entry.get("extracted_path", "")

            if extracted_path in blockfile_processed_files:
                callbacks.on_progress(i + 1, len(files), "Parsing cache files")
                continue

            try:
                inventory_id = self._register_inventory_entry(
                    evidence_conn,
                    evidence_id,
                    run_id,
                    manifest_data,
                    file_entry,
                )
                stats["inventory_entries"] += 1

                parse_result = self._parse_and_ingest_cache_file(
                    evidence_conn,
                    evidence_id,
                    run_id,
                    file_entry,
                    output_dir,
                    callbacks,
                    index_lookup=index_lookup,
                    warning_collector=warning_collector,
                )

                stats["urls"] += parse_result["urls"]
                stats["images"] += parse_result["images"]
                stats["records"] += parse_result["records"]

                update_inventory_ingestion_status(
                    evidence_conn,
                    inventory_id,
                    status="ok",
                    urls_parsed=parse_result["urls"],
                    records_parsed=parse_result["records"],
                    notes=parse_result.get("notes"),
                )

            except Exception as e:
                error_msg = f"Failed to ingest {file_entry.get('extracted_path')}: {e}"
                LOGGER.error(error_msg, exc_info=True)

                if 'inventory_id' in locals():
                    update_inventory_ingestion_status(
                        evidence_conn,
                        inventory_id,
                        status="failed",
                        notes=error_msg,
                    )

            callbacks.on_progress(i + 1, len(files), "Parsing cache files")

        LOGGER.info(
            "Ingestion complete: %d inventory entries, %d URLs, %d images, %d blockfile entries",
            stats["inventory_entries"],
            stats["urls"],
            stats["images"],
            stats["blockfile_entries"],
        )

        warning_count = warning_collector.flush_to_database(evidence_conn)
        if warning_count > 0:
            LOGGER.info("Recorded %d extraction warnings for cache_simple", warning_count)
            callbacks.on_log(f"Recorded {warning_count} extraction warnings (unknown formats/values)")

        if collector:
            collector.report_ingested(
                evidence_id, self.metadata.name,
                records=stats["urls"] + stats["images"],
                urls=stats["urls"],
                images=stats["images"],
                entries=stats["inventory_entries"],
            )
            collector.finish_run(evidence_id, self.metadata.name, status="success")

        return stats

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def _generate_run_id(self) -> str:
        """Generate run ID: {timestamp}_{uuid4}."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _get_e01_context(self, evidence_fs) -> dict:
        """Extract E01 context from evidence filesystem."""
        return {
            "image_path": str(evidence_fs.source_path) if hasattr(evidence_fs, 'source_path') else None,
            "partition_index": getattr(evidence_fs, 'partition_index', 0),
            "fs_type": getattr(evidence_fs, 'fs_type', "unknown"),
        }

    def _get_extraction_tool_version(self) -> str:
        """Build extraction tool version string."""
        try:
            import pytsk3
            pytsk3_version = pytsk3.TSK_VERSION_STR if hasattr(pytsk3, 'TSK_VERSION_STR') else 'unknown'
        except ImportError:
            pytsk3_version = 'not_installed'

        try:
            import pyewf
            pyewf_version = pyewf.get_version() if hasattr(pyewf, 'get_version') else 'unknown'
        except ImportError:
            pyewf_version = 'not_installed'

        return f"pytsk3:{pytsk3_version};pyewf:{pyewf_version}"

    def _register_inventory_entry(
        self,
        evidence_conn,
        evidence_id: int,
        run_id: str,
        manifest: Dict[str, Any],
        file_entry: Dict[str, Any],
    ) -> int:
        """Insert row into browser_cache_inventory table."""
        return register_inventory_entry(
            evidence_conn, evidence_id, run_id, manifest, file_entry
        )

    def _parse_and_ingest_cache_file(
        self,
        evidence_conn,
        evidence_id: int,
        run_id: str,
        file_entry: Dict[str, Any],
        extraction_dir: Path,
        callbacks: ExtractorCallbacks,
        index_lookup: Optional[Dict[int, IndexEntry]] = None,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
    ) -> Dict[str, int]:
        """Parse cache file and insert into database."""
        return parse_and_ingest_cache_file(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            run_id=run_id,
            file_entry=file_entry,
            extraction_dir=extraction_dir,
            callbacks=callbacks,
            extractor_version=self.metadata.version,
            index_lookup=index_lookup,
            warning_collector=warning_collector,
        )

    def _build_index_lookup(
        self,
        files: List[Dict[str, Any]],
        output_dir: Path,
        callbacks: ExtractorCallbacks,
    ) -> Dict[int, IndexEntry]:
        """Build a lookup table from cache index files."""
        return build_index_lookup(files, output_dir, callbacks)

    # -------------------------------------------------------------------------
    # Blockfile Cache Support
    # -------------------------------------------------------------------------

    def _find_blockfile_directories(
        self,
        files: List[Dict[str, Any]],
        output_dir: Path,
    ) -> List[Dict[str, Any]]:
        """Find directories containing blockfile cache (data_0/1/2/3 + index)."""
        return find_blockfile_directories(files, output_dir)

    def _ingest_blockfile_directory(
        self,
        evidence_conn,
        evidence_id: int,
        run_id: str,
        cache_dir: Path,
        browser: str,
        profile: str,
        extraction_dir: Path,
        callbacks: ExtractorCallbacks,
        *,
        warning_collector: Optional[ExtractionWarningCollector] = None,
        manifest_data: Optional[Dict[str, Any]] = None,
        file_entries: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, int]:
        """Parse and ingest a blockfile cache directory."""
        return ingest_blockfile_directory(
            evidence_conn=evidence_conn,
            evidence_id=evidence_id,
            run_id=run_id,
            cache_dir=cache_dir,
            browser=browser,
            profile=profile,
            extraction_dir=extraction_dir,
            callbacks=callbacks,
            extractor_version=self.metadata.version,
            warning_collector=warning_collector,
            manifest_data=manifest_data,
            file_entries=file_entries,
        )
