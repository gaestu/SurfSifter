"""
Concurrent extraction workers for cache extraction.

Handles parallel file extraction from E01 images with per-worker
EWF handles for thread safety.
"""

from __future__ import annotations

import hashlib
import queue
import re
import threading
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple

from core.logging import get_logger

LOGGER = get_logger("extractors.cache_simple.workers")

# Configuration constants
CHUNK_SIZE = 1024 * 1024  # 1MB - optimal for streaming copy


class ResumeAction(Enum):
    """User's choice when incomplete extraction is detected."""
    CANCEL = "cancel"
    OVERWRITE = "overwrite"
    CONTINUE = "continue"


@dataclass
class ExtractionResult:
    """Result from a single file extraction by a worker."""
    source_path: str
    extracted_path: str
    size_bytes: int
    md5: Optional[str]
    sha256: Optional[str]
    browser: str
    profile: str
    success: bool
    file_type: str = "unknown"  # entry, sparse, index, block, unknown
    entry_hash: Optional[str] = None  # 16-char hex hash from filename
    error: Optional[str] = None


def get_cache_file_type(filename: str) -> str:
    """
    Determine cache file type from filename.

    Returns:
        'entry': Primary cache entry files (hash_0) - simple cache
        'sparse': Sparse/stream files (hash_1, hash_s) - simple cache
        'index': Index files (index, the-real-index) - both formats
        'block': Block files (f_NNNNNN) - both formats (external data)
        'data_block': Blockfile data files (data_0/1/2/3) - blockfile format
        'unknown': Unrecognized file type
    """
    if filename in ('index', 'the-real-index'):
        return 'index'
    if filename.startswith('f_'):
        return 'block'
    # Blockfile data files: data_0, data_1, data_2, data_3
    if re.match(r'^data_[0-3]$', filename):
        return 'data_block'
    # Entry files: 16 hex chars + _0, _1, or _s
    if re.match(r'^[0-9a-f]{16}_0$', filename):
        return 'entry'
    if re.match(r'^[0-9a-f]{16}_[1s]$', filename):
        return 'sparse'
    return 'unknown'


def get_entry_hash_from_filename(filename: str) -> Optional[str]:
    """
    Extract the 16-character entry hash from a cache filename.

    Entry files are named: {16-hex-hash}_0/1/s
    The hash is derived from the cache key (URL) using SuperFastHash.

    Returns:
        The 16-character hex hash, or None if not an entry file.
    """
    match = re.match(r'^([0-9a-f]{16})_[01s]$', filename)
    if match:
        return match.group(1)
    return None


def stream_copy_hash(
    src_handle,
    dest_path: Path,
    compute_hash: bool = True
) -> Tuple[int, Optional[str], Optional[str]]:
    """
    Copy file with streaming hash, return (size, md5, sha256).

    Reads in chunks to avoid loading entire file into memory.
    Computes MD5 + SHA-256 inline with write operations in single pass.

    Args:
        src_handle: File-like object from evidence_fs.open_for_read()
        dest_path: Destination path in workspace
        compute_hash: If False, skip hashing (for deferred hash mode)

    Returns:
        (size_bytes, md5_hex, sha256_hex) or (size_bytes, None, None) if compute_hash=False
    """
    md5 = hashlib.md5() if compute_hash else None
    sha256 = hashlib.sha256() if compute_hash else None
    size = 0
    with open(dest_path, "wb") as dst:
        while True:
            chunk = src_handle.read(CHUNK_SIZE)
            if not chunk:
                break
            dst.write(chunk)
            if compute_hash:
                md5.update(chunk)
                sha256.update(chunk)
            size += len(chunk)
    return size, (md5.hexdigest() if md5 else None), (sha256.hexdigest() if sha256 else None)


def extraction_worker(
    ewf_paths: List[Path],
    partition_index: int,
    work_queue: queue.Queue,
    result_queue: queue.Queue,
    output_dir: Path,
    run_id: str,
    compute_hash: bool,
    stop_event: threading.Event,
    worker_id: int,
) -> None:
    """
    Worker thread for concurrent extraction.

    Each worker creates its own PyEwfTskFS instance with the pre-computed
    partition_index, avoiding redundant partition scans.

    Args:
        ewf_paths: List of E01 segment paths
        partition_index: Pre-computed partition index from main thread
        work_queue: Queue of (file_info, cache_dir_info) tuples to extract
        result_queue: Queue for extraction results
        output_dir: Base output directory
        run_id: Extraction run ID
        compute_hash: Whether to compute hashes inline
        stop_event: Signal to stop the worker
        worker_id: Worker identifier for logging
    """
    from core.evidence_fs import PyEwfTskFS

    evidence_fs = None
    try:
        # Each worker opens its own E01 handle
        evidence_fs = PyEwfTskFS(ewf_paths, partition_index)
        LOGGER.debug("Worker %d initialized with E01 handle", worker_id)

        while not stop_event.is_set():
            try:
                # Get next file to process (with timeout for shutdown check)
                work_item = work_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            if work_item is None:  # Poison pill - shutdown
                work_queue.task_done()
                break

            file_info, cache_dir_info = work_item

            try:
                # Extract browser and profile info
                browser = cache_dir_info["browser"]
                profile = cache_dir_info.get("profile", "Default")
                source_path = file_info["path"]
                filename = file_info["filename"]

                # Determine file type and entry hash from filename
                file_type = get_cache_file_type(filename)
                entry_hash = get_entry_hash_from_filename(filename)

                # Create profile-specific subdirectory
                profile_dir = output_dir / run_id / f"p{partition_index}_{browser}_{profile}"
                profile_dir.mkdir(parents=True, exist_ok=True)

                # Destination path
                dest_path = profile_dir / filename

                # Stream copy with inline hashes
                with evidence_fs.open_for_read(source_path) as src:
                    size_bytes, md5, sha256 = stream_copy_hash(src, dest_path, compute_hash)

                # Create result
                result = ExtractionResult(
                    source_path=source_path,
                    extracted_path=str(dest_path.relative_to(output_dir)),
                    size_bytes=size_bytes,
                    md5=md5,
                    sha256=sha256,
                    browser=browser,
                    profile=profile,
                    success=True,
                    file_type=file_type,
                    entry_hash=entry_hash,
                )

            except Exception as e:
                LOGGER.warning("Worker %d failed to extract %s: %s", worker_id, file_info.get("path", "?"), e)
                result = ExtractionResult(
                    source_path=file_info.get("path", ""),
                    extracted_path="",
                    size_bytes=0,
                    md5=None,
                    sha256=None,
                    browser=cache_dir_info.get("browser", ""),
                    profile=cache_dir_info.get("profile", ""),
                    success=False,
                    error=str(e),
                )

            result_queue.put(result)
            work_queue.task_done()

    except Exception as e:
        LOGGER.error("Worker %d failed to initialize: %s", worker_id, e)
        # Put error result to signal main thread
        result_queue.put(ExtractionResult(
            source_path="__WORKER_INIT_ERROR__",
            extracted_path="",
            size_bytes=0,
            md5=None,
            sha256=None,
            browser="",
            profile="",
            success=False,
            error=f"Worker {worker_id} init failed: {e}",
        ))
    finally:
        # Cleanup
        if evidence_fs:
            try:
                del evidence_fs
            except Exception:
                pass
        LOGGER.debug("Worker %d shutdown", worker_id)


# Backward compatibility aliases
_get_cache_file_type = get_cache_file_type
_get_entry_hash_from_filename = get_entry_hash_from_filename
_stream_copy_hash = stream_copy_hash
_extraction_worker = extraction_worker
