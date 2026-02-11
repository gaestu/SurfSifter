"""
Firefox Cache Extraction - Concurrent Strategy

Uses ThreadPoolExecutor with per-worker EvidenceFS handles for parallel extraction.
Optimized for E01 images where icat is not available.
"""

from __future__ import annotations

import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, List, Optional, Tuple

from core.logging import get_logger
from .base import (
    ExtractionStrategy,
    ExtractionContext,
    ExtractionResult,
    DiscoveredFile,
    stream_copy_with_hash,
    stream_copy_with_hash_from_iterator,
    extract_profile_from_path,
    CHUNK_SIZE,
    WORK_QUEUE_SIZE,
)

LOGGER = get_logger("extractors.cache_firefox.strategies.concurrent")


# Thread-local storage for per-worker EvidenceFS handles
_thread_local = threading.local()


class ConcurrentExtractionStrategy(ExtractionStrategy):
    """
    Concurrent extraction using ThreadPoolExecutor.

    Creates per-worker EvidenceFS handles to avoid contention on
    PyEWF/PyTSK3 C-extension state. Uses producer-consumer pattern
    with bounded work queue.
    """

    def __init__(self, max_workers: int = 4):
        """
        Initialize concurrent strategy.

        Args:
            max_workers: Maximum parallel workers
        """
        self._max_workers = max_workers

    @property
    def name(self) -> str:
        return "concurrent"

    def can_run(self, context: ExtractionContext) -> bool:
        """
        Check if concurrent extraction is viable.

        Requires EvidenceFS that supports creating additional handles.
        """
        evidence_fs = context.evidence_fs

        # Check if we can create per-worker handles
        ewf_paths = getattr(evidence_fs, 'ewf_paths', None)
        source_path = getattr(evidence_fs, 'source_path', None)

        # Concurrent works for both E01 and mounted filesystems
        # but is optimized for E01 where we create per-worker handles
        return True

    def run(
        self,
        files: List[DiscoveredFile],
        context: ExtractionContext,
    ) -> Tuple[int, int]:
        """
        Extract files using thread pool with per-worker handles.

        Args:
            files: List of discovered files
            context: Extraction context

        Returns:
            Tuple of (extracted_count, error_count)
        """
        if not files:
            return 0, 0

        total = len(files)

        evidence_fs = context.evidence_fs
        # Get E01 path (prefer ewf_paths[0], fallback to source_path)
        ewf_paths = getattr(evidence_fs, 'ewf_paths', None)
        e01_path = ewf_paths[0] if ewf_paths else getattr(evidence_fs, 'source_path', None)

        # Create work queue
        work_queue: queue.Queue = queue.Queue(maxsize=WORK_QUEUE_SIZE)

        # Track extracted files for dedup
        extracted_hashes: set = set()
        hash_lock = threading.Lock()

        # Thread-safe filename collision tracking
        used_filenames: set = set()
        filename_lock = threading.Lock()

        # Results collection with atomic counters for real-time progress
        results: List[ExtractionResult] = []
        results_lock = threading.Lock()
        extracted_count = [0]  # Use list for mutable int in closure
        error_count = [0]
        progress_lock = threading.Lock()
        last_progress_report = [0]  # Track last reported count

        def report_progress():
            """Report progress if enough files have been processed."""
            with progress_lock:
                current = extracted_count[0] + error_count[0]
                # Report every 50 files or at completion
                if current - last_progress_report[0] >= 50 or current == total:
                    if context.progress_callback:
                        context.progress_callback(
                            current, total,
                            f"Extracting: {extracted_count[0]} ok, {error_count[0]} errors"
                        )
                    last_progress_report[0] = current

        def worker():
            """Worker function that processes files from queue."""
            # Get or create per-worker EvidenceFS handle
            worker_fs = self._get_worker_fs(evidence_fs, e01_path)

            while True:
                # Check cancellation (unified method)
                if context.is_cancelled():
                    break

                try:
                    item = work_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                if item is None:  # Sentinel
                    work_queue.task_done()
                    break

                file = item
                result = self._extract_single(
                    file=file,
                    worker_fs=worker_fs,
                    context=context,
                    extracted_hashes=extracted_hashes,
                    hash_lock=hash_lock,
                    used_filenames=used_filenames,
                    filename_lock=filename_lock,
                )

                with results_lock:
                    results.append(result)
                    if result.success:
                        extracted_count[0] += 1
                    else:
                        error_count[0] += 1

                # Report progress from worker thread
                report_progress()

                work_queue.task_done()

        # Start workers
        workers = []
        LOGGER.info("Starting %d extraction workers", self._max_workers)
        if context.log_callback:
            context.log_callback(f"Starting {self._max_workers} extraction workers")

        for _ in range(self._max_workers):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            workers.append(t)

        # Feed work queue (fast, no progress needed here)
        for file in files:
            if context.is_cancelled():
                break
            work_queue.put(file)

        # Send sentinels to stop workers
        for _ in workers:
            work_queue.put(None)

        # Wait for completion
        work_queue.join()

        # Wait for workers to finish
        for t in workers:
            t.join(timeout=5.0)

        # Process results for manifest
        for result in results:
            context.manifest_writer.append(result.to_dict())

        # Final progress
        if context.progress_callback:
            context.progress_callback(
                total, total,
                f"Complete: {extracted_count[0]} extracted, {error_count[0]} errors"
            )

        return extracted_count[0], error_count[0]

    def _get_worker_fs(self, evidence_fs: Any, e01_path: Optional[str]) -> Any:
        """
        Get or create per-worker EvidenceFS handle.

        Args:
            evidence_fs: Main EvidenceFS instance
            e01_path: Path to E01 image (if applicable)

        Returns:
            Worker-local EvidenceFS handle
        """
        # Check thread-local storage
        if hasattr(_thread_local, 'evidence_fs'):
            return _thread_local.evidence_fs

        # For E01 images, create new handle per worker
        if e01_path:
            try:
                from core.evidence_fs import PyEwfTskFS, find_ewf_segments
                # Get all segment paths for the E01 image
                segments = find_ewf_segments(Path(e01_path))
                # Get partition_index from main evidence_fs if available
                partition_index = getattr(evidence_fs, '_partition_index', None)
                worker_fs = PyEwfTskFS(segments, partition_index=partition_index)
                _thread_local.evidence_fs = worker_fs
                return worker_fs
            except Exception as e:
                LOGGER.warning("Failed to create per-worker EvidenceFS: %s", e)
                # Fall back to shared handle
                _thread_local.evidence_fs = evidence_fs
                return evidence_fs

        # For mounted filesystems, use shared handle
        _thread_local.evidence_fs = evidence_fs
        return evidence_fs

    def _extract_single(
        self,
        file: DiscoveredFile,
        worker_fs: Any,
        context: ExtractionContext,
        extracted_hashes: set,
        hash_lock: threading.Lock,
        used_filenames: set,
        filename_lock: threading.Lock,
    ) -> ExtractionResult:
        """
        Extract a single file.

        Args:
            file: File to extract
            worker_fs: Worker-local EvidenceFS
            context: Extraction context
            extracted_hashes: Set of already extracted SHA256 hashes
            hash_lock: Lock for hash set access
            used_filenames: Set of already used filenames (thread-safe)
            filename_lock: Lock for filename set access

        Returns:
            ExtractionResult
        """
        try:
            # Generate unique output path with thread-safe collision handling
            base_filename = Path(file.path).name

            with filename_lock:
                output_filename = base_filename
                counter = 0
                while output_filename in used_filenames:
                    counter += 1
                    output_filename = f"{base_filename}_{counter}"
                used_filenames.add(output_filename)

            output_path = context.output_dir / output_filename

            # Read file from evidence (partition_index handled internally by EvidenceFS)
            try:
                # Try streaming read via iterator
                if hasattr(worker_fs, 'open_for_stream'):
                    # open_for_stream returns an iterator, not a context manager
                    chunks = worker_fs.open_for_stream(file.path)
                    file_size, md5, sha256 = stream_copy_with_hash_from_iterator(
                        chunks, output_path,
                        compute_hash=context.compute_hash,
                    )
                elif hasattr(worker_fs, 'iter_file_chunks'):
                    chunks = worker_fs.iter_file_chunks(
                        file.path,
                        chunk_size=CHUNK_SIZE,
                    )
                    file_size, md5, sha256 = stream_copy_with_hash_from_iterator(
                        chunks, output_path,
                        compute_hash=context.compute_hash,
                    )
                elif hasattr(worker_fs, 'read_file'):
                    # Fallback to full read
                    data = worker_fs.read_file(file.path)
                    if data is None:
                        return ExtractionResult(
                            success=False,
                            source_path=file.path,
                            error="File not found or empty",
                            partition_index=file.partition_index,
                        )

                    import hashlib
                    if context.compute_hash:
                        md5 = hashlib.md5(data).hexdigest()
                        sha256 = hashlib.sha256(data).hexdigest()
                    else:
                        md5 = None
                        sha256 = None
                    file_size = len(data)

                    output_path.write_bytes(data)
                else:
                    return ExtractionResult(
                        success=False,
                        source_path=file.path,
                        error="EvidenceFS does not support file reading",
                        partition_index=file.partition_index,
                    )

            except Exception as e:
                return ExtractionResult(
                    success=False,
                    source_path=file.path,
                    error=f"Read error: {e}",
                    partition_index=file.partition_index,
                )

            # Check for duplicate content (only when hashing is enabled)
            with hash_lock:
                if sha256 is not None and sha256 in extracted_hashes:
                    # Remove duplicate
                    output_path.unlink()
                    return ExtractionResult(
                        success=False,
                        source_path=file.path,
                        error="Duplicate content (SHA256)",
                        partition_index=file.partition_index,
                    )
                if sha256 is not None:
                    extracted_hashes.add(sha256)

            return ExtractionResult(
                success=True,
                source_path=file.path,
                extracted_path=output_path.name,  # Just filename, ingestion joins with run_dir
                size_bytes=file_size,
                md5=md5,
                sha256=sha256,
                partition_index=file.partition_index,
                inode=file.inode,
                logical_path=file.path,
                profile=extract_profile_from_path(file.path),
                artifact_type=file.artifact_type,
            )

        except Exception as e:
            LOGGER.error("Error extracting %s: %s", file.path, e)
            return ExtractionResult(
                success=False,
                source_path=file.path,
                error=str(e),
                partition_index=file.partition_index,
            )
