"""
Parallel Extraction Worker for Filesystem Images

Provides parallel extraction from E01 images using multiple PyEwfTskFS
instances, each with its own pyewf handle for thread safety.

Key optimization: Each worker opens its own copy of the E01 segments,
avoiding pyewf's internal locking and enabling true parallelism.

Performance characteristics:
- Workers scale with CPU cores (default: cpu_count - 2)
- Best on SSD storage for output directory
- Responsive cancellation via periodic checks during file streaming
- Optional signature verification during extraction
- Zero-content detection for OneDrive/sparse files
"""

from __future__ import annotations

import hashlib
import os
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from queue import Queue
from threading import Event, Lock
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from core.logging import get_logger

LOGGER = get_logger("extractors.filesystem_images.parallel")

# Auto workers: use all cores minus 2 (keep at least 1)
CPU_COUNT = os.cpu_count() or 4
AUTO_WORKERS = max(1, CPU_COUNT - 2)
# Maximum workers cap matches auto for consistency
MAX_WORKERS_CAP = AUTO_WORKERS


@dataclass
class ExtractionTask:
    """Task for extracting a single image file."""
    fs_path: str
    filename: str
    size_bytes: int
    mtime_epoch: Optional[float]
    crtime_epoch: Optional[float]
    atime_epoch: Optional[float]
    ctime_epoch: Optional[float]
    inode: Optional[int]


@dataclass
class ExtractionResult:
    """Result of extracting a single image file."""
    task: ExtractionTask
    success: bool
    md5: Optional[str] = None
    sha256: Optional[str] = None
    rel_path: Optional[str] = None
    error: Optional[str] = None
    # Signature verification
    detected_type: Optional[str] = None   # Detected image format from magic bytes
    signature_valid: Optional[bool] = None  # True if signature matches extension
    # Sparse file detection
    actual_bytes: Optional[int] = None  # Actual extracted size (may differ from task.size_bytes for sparse/OneDrive files)
    is_sparse: bool = False  # True if file had NTFS size but extracted as empty/smaller


@dataclass
class ExtractionSummary:
    """Summary of extraction run for manifest."""
    results: List[ExtractionResult]
    extracted_count: int
    error_count: int
    total_bytes: int
    was_cancelled: bool
    effective_workers: int  # Actual workers used (for audit)
    used_parallel: bool     # True if parallel extraction was used
    signature_mismatches: int = 0  # Count of extension/signature mismatches
    sparse_count: int = 0  # Count of sparse/OneDrive files with 0 actual content


class ParallelExtractor:
    """
    Parallel extraction manager for filesystem images.

    Creates multiple PyEwfTskFS instances to enable true parallel
    reads from E01 images. Each worker gets its own pyewf handle
    to avoid contention.

    Thread safety:
    - Each worker has its own PyEwfTskFS instance (no sharing)
    - Output directory writes use unique paths (no locks needed)
    - Stats counters protected by lock
    - Cancellation via threading.Event for clean shutdown
    """

    def __init__(
        self,
        ewf_paths: List[Path],
        partition_index: int,
        output_dir: Path,
        max_workers: int = 0,
        preserve_structure: bool = True,
        verify_signatures: bool = True,
        path_prefix: Optional[str] = None,
    ):
        """
        Initialize parallel extractor.

        Args:
            ewf_paths: E01 segment paths
            partition_index: Partition index to mount (from original FS)
            output_dir: Directory for extracted files
            max_workers: Number of parallel workers (0 = auto based on CPU count)
            preserve_structure: Keep original directory structure
            verify_signatures: Verify image signatures during extraction
            path_prefix: Optional subdirectory prefix for extracted files
        """
        self.ewf_paths = ewf_paths
        self.partition_index = partition_index
        self.output_dir = output_dir
        self.extracted_dir = output_dir / "extracted"
        self.preserve_structure = preserve_structure
        self.verify_signatures = verify_signatures
        self.path_prefix = path_prefix.strip("/") if path_prefix else None

        # Calculate max workers: 0 = auto (CPU count - 2)
        if max_workers <= 0:
            self.max_workers = AUTO_WORKERS
        else:
            self.max_workers = max(1, min(max_workers, MAX_WORKERS_CAP))

        # Stats
        self._lock = Lock()
        self._extracted_count = 0
        self._error_count = 0
        self._total_bytes = 0
        self._signature_mismatches = 0
        self._sparse_count = 0  # OneDrive/sparse files with 0 actual content

        # Cancellation event - shared across all workers
        self._cancel_event = Event()

        LOGGER.info(
            "ParallelExtractor initialized: workers=%d (cap=%d), partition=%d, verify_signatures=%s",
            self.max_workers, MAX_WORKERS_CAP, self.partition_index, self.verify_signatures
        )

    def request_cancellation(self) -> None:
        """Request cancellation of extraction - thread-safe."""
        self._cancel_event.set()
        LOGGER.info("Cancellation requested")

    def is_cancelled(self) -> bool:
        """Check if cancellation was requested - thread-safe."""
        return self._cancel_event.is_set()

    @property
    def signature_mismatches(self) -> int:
        """Number of files with extension/signature mismatches."""
        return self._signature_mismatches

    @property
    def sparse_count(self) -> int:
        """Number of sparse/OneDrive files with 0 actual content."""
        return self._sparse_count

    def _create_evidence_fs(self):
        """Create a new PyEwfTskFS instance for a worker thread."""
        from core.evidence_fs import PyEwfTskFS
        return PyEwfTskFS(self.ewf_paths, partition_index=self.partition_index)

    def _extract_single(
        self,
        evidence_fs,
        task: ExtractionTask,
    ) -> ExtractionResult:
        """
        Extract a single image file.

        Reads from evidence filesystem, computes hashes, and writes
        to output directory in a single pass.

        If verify_signatures is enabled, validates the file header matches
        a known image format and records the result.

        Checks cancellation periodically during streaming to allow
        responsive cancellation even for large files.
        """
        from pathlib import PurePosixPath
        from .utils import compute_flat_rel_path
        from extractors.image_signatures import detect_image_type

        try:
            # Determine output path using shared helper for deterministic naming
            if self.preserve_structure:
                prefix = f"{self.path_prefix}/" if self.path_prefix else ""
                rel_path = f"{prefix}{task.fs_path}"
            else:
                rel_path = compute_flat_rel_path(
                    task.fs_path, task.filename, task.inode, self.path_prefix
                )

            dest_path = self.extracted_dir / rel_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)

            # Stream file: read → hash → write (single pass)
            # Check cancellation every ~1MB to stay responsive
            # Capture first 32 bytes for signature verification
            # Track zero-content for sparse/OneDrive detection
            md5 = hashlib.md5()
            sha256 = hashlib.sha256()
            bytes_since_check = 0
            actual_bytes_written = 0  # Track actual content size
            CANCEL_CHECK_INTERVAL = 1024 * 1024  # 1 MB
            header_bytes = b""
            first_chunk = True
            all_zeros = True  # Track if content is all zeros
            ZERO_CHECK_LIMIT = 64 * 1024  # Only check first 64KB for zeros

            with open(dest_path, "wb") as out_file:
                for chunk in evidence_fs.open_for_stream(task.fs_path):
                    # Capture header for signature verification
                    if first_chunk:
                        header_bytes = chunk[:32] if len(chunk) >= 32 else chunk
                        first_chunk = False

                    # Track zero-content in first 64KB
                    # OneDrive placeholders and sparse files often read as all zeros
                    # Use count() for efficiency - avoids allocating zero-filled comparison buffer
                    if all_zeros and actual_bytes_written < ZERO_CHECK_LIMIT:
                        check_len = min(len(chunk), ZERO_CHECK_LIMIT - actual_bytes_written)
                        if chunk[:check_len].count(b'\x00') != check_len:
                            all_zeros = False

                    # Check for cancellation periodically
                    bytes_since_check += len(chunk)
                    if bytes_since_check >= CANCEL_CHECK_INTERVAL:
                        bytes_since_check = 0
                        if self._cancel_event.is_set():
                            # Clean up partial file
                            out_file.close()
                            try:
                                dest_path.unlink()
                            except OSError:
                                pass
                            return ExtractionResult(
                                task=task,
                                success=False,
                                error="Cancelled during extraction",
                            )
                    out_file.write(chunk)
                    md5.update(chunk)
                    sha256.update(chunk)
                    actual_bytes_written += len(chunk)

            md5_hex = md5.hexdigest()
            sha256_hex = sha256.hexdigest()

            # Sparse/placeholder file detection (enhanced)
            # OneDrive "Files On-Demand" have NTFS-reported size but:
            #   - 0 actual content (cloud-only, no data runs)
            #   - OR all-zero content (sparse allocation / placeholder)
            # Detect both cases to avoid filling extraction with garbage
            is_sparse = False
            MIN_SIZE_FOR_ZERO_CHECK = 1024  # Only check files claiming >1KB

            if task.size_bytes > 0 and actual_bytes_written == 0:
                # Case 1: No data at all (original detection)
                is_sparse = True
                with self._lock:
                    self._sparse_count += 1
                LOGGER.debug(
                    "Sparse/OneDrive file: %s - NTFS reports %d bytes but extracted 0",
                    task.fs_path, task.size_bytes
                )
                try:
                    dest_path.unlink()
                except OSError:
                    pass
            elif (
                task.size_bytes >= MIN_SIZE_FOR_ZERO_CHECK
                and actual_bytes_written > 0
                and all_zeros
            ):
                # Case 2: File extracted but content is all zeros
                # This catches OneDrive placeholders that read as zero-filled
                is_sparse = True
                with self._lock:
                    self._sparse_count += 1
                LOGGER.debug(
                    "Zero-content file: %s - claimed %d bytes, extracted %d bytes of zeros",
                    task.fs_path, task.size_bytes, actual_bytes_written
                )
                try:
                    dest_path.unlink()
                except OSError:
                    pass

            # Signature verification
            detected_type = None
            signature_valid = None

            if self.verify_signatures and header_bytes:
                detection_result = detect_image_type(header_bytes)
                # Check if detected type matches extension
                ext = PurePosixPath(task.filename).suffix.lower()

                if detection_result is None:
                    detected_type = None
                    signature_valid = False
                    with self._lock:
                        self._signature_mismatches += 1
                    LOGGER.debug(
                        "Signature mismatch: %s - no valid image signature detected",
                        task.fs_path
                    )
                else:
                    # detect_image_type returns (format_name, extension) tuple
                    detected_type = detection_result[0]
                    # Map detected type to expected extensions
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
                    expected_exts = type_to_ext.get(detected_type, set())
                    signature_valid = ext in expected_exts or not ext  # Empty ext is OK

                    if not signature_valid:
                        with self._lock:
                            self._signature_mismatches += 1
                        LOGGER.debug(
                            "Signature mismatch: %s - detected %s but extension is %s",
                            task.fs_path, detected_type, ext
                        )

            # Update stats (only count non-sparse files)
            with self._lock:
                if not is_sparse:
                    self._extracted_count += 1
                    self._total_bytes += actual_bytes_written

            return ExtractionResult(
                task=task,
                success=not is_sparse,  # Sparse files are not "successful" extractions
                md5=md5_hex if not is_sparse else None,
                sha256=sha256_hex if not is_sparse else None,
                rel_path=rel_path if not is_sparse else None,
                detected_type=detected_type,
                signature_valid=signature_valid,
                actual_bytes=actual_bytes_written,
                is_sparse=is_sparse,
                error="Sparse/OneDrive file - no actual content" if is_sparse else None,
            )

        except Exception as e:
            LOGGER.warning("Error extracting %s: %s", task.fs_path, e)
            with self._lock:
                self._error_count += 1
            return ExtractionResult(
                task=task,
                success=False,
                error=str(e),
            )

    def _worker_loop(
        self,
        task_queue: Queue,
        progress_callback: Optional[Callable[[int, int, int], None]] = None,
    ) -> List[ExtractionResult]:
        """
        Worker thread main loop.

        Opens its own PyEwfTskFS instance and processes tasks from queue.
        Each worker is fully independent - no shared evidence_fs.
        Checks cancel event between tasks for clean shutdown.
        """
        results = []

        # Create worker-local evidence filesystem
        try:
            evidence_fs = self._create_evidence_fs()
        except Exception as e:
            LOGGER.error("Worker failed to open evidence: %s", e)
            return results

        try:
            while not self._cancel_event.is_set():
                try:
                    task = task_queue.get_nowait()
                except Exception:
                    # Queue empty
                    break

                if task is None:  # Sentinel
                    break

                # Check cancellation before processing
                if self._cancel_event.is_set():
                    LOGGER.debug("Worker stopping due to cancellation")
                    break

                result = self._extract_single(evidence_fs, task)
                results.append(result)

                # Report progress
                if progress_callback:
                    with self._lock:
                        count = self._extracted_count
                        errors = self._error_count
                        total_bytes = self._total_bytes
                    progress_callback(count, errors, total_bytes)

        finally:
            # Close the evidence filesystem
            try:
                if hasattr(evidence_fs, 'close'):
                    evidence_fs.close()
                elif hasattr(evidence_fs, '_handle'):
                    evidence_fs._handle.close()
            except Exception as e:
                LOGGER.debug("Error closing worker evidence_fs: %s", e)

        return results

    def extract_all(
        self,
        tasks: List[ExtractionTask],
        progress_callback: Optional[Callable[[int, int, int], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> ExtractionSummary:
        """
        Extract all images, using parallel or sequential extraction as appropriate.

        Args:
            tasks: List of extraction tasks
            progress_callback: Called with (extracted_count, error_count, total_bytes)
            cancel_check: Callable returning True if extraction should stop

        Returns:
            ExtractionSummary with results, stats, and audit info
        """
        if not tasks:
            return ExtractionSummary(
                results=[],
                extracted_count=0,
                error_count=0,
                total_bytes=0,
                was_cancelled=False,
                effective_workers=0,
                used_parallel=False,
                signature_mismatches=0,
                sparse_count=0,
            )

        self.extracted_dir.mkdir(parents=True, exist_ok=True)

        # Reset stats and cancel event
        self._extracted_count = 0
        self._error_count = 0
        self._total_bytes = 0
        self._signature_mismatches = 0
        self._sparse_count = 0
        self._cancel_event.clear()

        # Determine effective worker count
        # For small batches, use fewer workers to avoid overhead
        effective_workers = min(self.max_workers, max(1, len(tasks) // 10))
        use_parallel = effective_workers > 1 and len(tasks) >= 10

        if not use_parallel:
            # Sequential extraction for small batches
            LOGGER.info("Using sequential extraction for %d tasks", len(tasks))
            results = self._extract_sequential(tasks, progress_callback, cancel_check)
            was_cancelled = self._cancel_event.is_set()
        else:
            # Parallel extraction
            LOGGER.info(
                "Starting parallel extraction: %d tasks, %d workers",
                len(tasks), effective_workers
            )
            results = self._extract_parallel(tasks, effective_workers, progress_callback, cancel_check)
            was_cancelled = self._cancel_event.is_set()

        # Sort results for deterministic ordering
        results.sort(key=lambda r: r.task.fs_path)

        LOGGER.info(
            "Extraction %s: %d extracted, %d errors, %d sparse, %d bytes, %d sig mismatches, parallel=%s, workers=%d",
            "cancelled" if was_cancelled else "complete",
            self._extracted_count, self._error_count, self._sparse_count, self._total_bytes,
            self._signature_mismatches, use_parallel, effective_workers
        )

        return ExtractionSummary(
            results=results,
            extracted_count=self._extracted_count,
            error_count=self._error_count,
            total_bytes=self._total_bytes,
            was_cancelled=was_cancelled,
            effective_workers=effective_workers,
            used_parallel=use_parallel,
            signature_mismatches=self._signature_mismatches,
            sparse_count=self._sparse_count,
        )

    def _extract_sequential(
        self,
        tasks: List[ExtractionTask],
        progress_callback: Optional[Callable[[int, int, int], None]],
        cancel_check: Optional[Callable[[], bool]],
    ) -> List[ExtractionResult]:
        """Sequential extraction for small batches or when parallel is disabled."""
        results = []
        evidence_fs = self._create_evidence_fs()

        try:
            for task in tasks:
                # Check external cancel callback
                if cancel_check and cancel_check():
                    LOGGER.info("Extraction cancelled via callback")
                    self._cancel_event.set()
                    break

                # Check internal cancel event
                if self._cancel_event.is_set():
                    LOGGER.info("Extraction cancelled via event")
                    break

                result = self._extract_single(evidence_fs, task)
                results.append(result)

                if progress_callback:
                    with self._lock:
                        progress_callback(
                            self._extracted_count,
                            self._error_count,
                            self._total_bytes
                        )
        finally:
            try:
                if hasattr(evidence_fs, '_handle'):
                    evidence_fs._handle.close()
            except Exception:
                pass

        return results

    def _extract_parallel(
        self,
        tasks: List[ExtractionTask],
        effective_workers: int,
        progress_callback: Optional[Callable[[int, int, int], None]],
        cancel_check: Optional[Callable[[], bool]],
    ) -> List[ExtractionResult]:
        """Parallel extraction using thread pool."""
        # Create task queue
        task_queue: Queue = Queue()
        for task in tasks:
            task_queue.put(task)

        # Add sentinel values for clean shutdown
        for _ in range(effective_workers):
            task_queue.put(None)

        all_results = []

        with ThreadPoolExecutor(max_workers=effective_workers) as executor:
            futures = {
                executor.submit(self._worker_loop, task_queue, progress_callback)
                for _ in range(effective_workers)
            }

            # Poll for cancellation even while workers are busy
            while futures:
                done, futures = wait(
                    futures, timeout=0.2, return_when=FIRST_COMPLETED
                )

                if cancel_check and cancel_check() and not self._cancel_event.is_set():
                    LOGGER.info("Cancellation requested, signaling workers")
                    self._cancel_event.set()

                for future in done:
                    try:
                        worker_results = future.result()
                        all_results.extend(worker_results)
                    except Exception as e:
                        LOGGER.error("Worker failed: %s", e)

        return all_results

    @property
    def extracted_count(self) -> int:
        """Number of successfully extracted files."""
        with self._lock:
            return self._extracted_count

    @property
    def error_count(self) -> int:
        """Number of extraction errors."""
        with self._lock:
            return self._error_count

    @property
    def total_bytes(self) -> int:
        """Total bytes extracted."""
        with self._lock:
            return self._total_bytes
