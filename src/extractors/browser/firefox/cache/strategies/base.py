"""
Firefox Cache Extraction Strategies - Base Module

Defines the protocol/interface for extraction strategies and shared utilities.
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Protocol, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from core.database import Database
    from extractors.filesystem_images.evidence_fs import EvidenceFS
    from extractors.browser.firefox.cache.manifest import ManifestWriter

from core.logging import get_logger

LOGGER = get_logger("extractors.cache_firefox.strategies")

# Extraction constants
CHUNK_SIZE = 65536  # 64 KB read chunks
WORK_QUEUE_SIZE = 200  # Max items in work queue


@dataclass
class ExtractionResult:
    """Result from extracting a single cache file."""
    success: bool
    source_path: str
    extracted_path: Optional[str] = None  # Relative path from output_dir
    size_bytes: int = 0
    md5: Optional[str] = None
    sha256: Optional[str] = None
    detected_type: Optional[str] = None
    signature_valid: Optional[bool] = None
    error: Optional[str] = None
    partition_index: Optional[int] = None
    inode: Optional[int] = None
    profile: Optional[str] = None
    logical_path: Optional[str] = None  # Original path in evidence
    artifact_type: str = "cache_firefox"  # Artifact type for manifest

    def to_dict(self) -> Dict[str, Any]:
        """Convert to manifest entry dict (matches original extractor format)."""
        result = {
            "source_path": self.source_path,
            "success": self.success,
        }
        if self.extracted_path:
            result["extracted_path"] = self.extracted_path
        if self.size_bytes:
            result["size_bytes"] = self.size_bytes
        if self.md5:
            result["md5"] = self.md5
        if self.sha256:
            result["sha256"] = self.sha256
        if self.detected_type:
            result["detected_type"] = self.detected_type
        if self.signature_valid is not None:
            result["signature_valid"] = self.signature_valid
        if self.error:
            result["error"] = self.error
        if self.partition_index is not None:
            result["partition_index"] = self.partition_index
        if self.inode is not None:
            result["inode"] = self.inode
        if self.profile:
            result["profile"] = self.profile
        if self.logical_path:
            result["logical_path"] = self.logical_path
        # Artifact type (cache_firefox, cache_index, cache_journal, cache_doomed, cache_trash)
        result["artifact_type"] = self.artifact_type
        return result


@dataclass
class IcatResult:
    """Result from icat command execution."""
    success: bool
    output_path: Optional[Path] = None
    file_size: int = 0
    error: Optional[str] = None


@dataclass
class DiscoveredFile:
    """A discovered cache file to be extracted."""
    path: str
    partition_index: Optional[int] = None
    inode: Optional[int] = None
    file_id: Optional[int] = None  # ID in file_list table
    artifact_type: str = "cache_firefox"  # cache_firefox, cache_index, cache_journal, cache_doomed, cache_trash

    def __hash__(self):
        return hash((self.path, self.partition_index, self.inode))

    def __eq__(self, other):
        if not isinstance(other, DiscoveredFile):
            return False
        return (self.path == other.path and
                self.partition_index == other.partition_index and
                self.inode == other.inode)


@dataclass
class ExtractionContext:
    """Context for extraction operations."""
    evidence_fs: Any  # EvidenceFS
    output_dir: Path
    run_id: str
    manifest_writer: Any  # ManifestWriter
    cancel_event: Optional[Any] = None  # threading.Event
    progress_callback: Optional[Callable[[int, int, str], None]] = None
    log_callback: Optional[Callable[[str], None]] = None
    is_cancelled_callback: Optional[Callable[[], bool]] = None  # Link to callbacks.is_cancelled()
    compute_hash: bool = True  # Whether to compute MD5/SHA256 during extraction
    extractor_version: str = "1.12.0"  # For manifest metadata

    # Statistics
    extracted_count: int = 0
    error_count: int = 0
    skipped_count: int = 0

    def is_cancelled(self) -> bool:
        """Check if extraction should be cancelled."""
        if self.cancel_event and self.cancel_event.is_set():
            return True
        if self.is_cancelled_callback and self.is_cancelled_callback():
            return True
        return False


class ExtractionStrategy(ABC):
    """
    Abstract base class for extraction strategies.

    Strategies handle the actual file extraction from evidence.
    Each strategy optimizes for different scenarios:
    - IcatStrategy: Uses SleuthKit icat for E01 images (fastest, inode-based)
    - ConcurrentStrategy: ThreadPool with per-worker E01 handles
    - SequentialStrategy: Simple sequential extraction (MountedFS fallback)
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Strategy name for logging/manifest."""
        ...

    @abstractmethod
    def can_run(self, context: ExtractionContext) -> bool:
        """
        Check if this strategy can run with current context.

        Args:
            context: Extraction context

        Returns:
            True if strategy is usable
        """
        ...

    @abstractmethod
    def run(
        self,
        files: List[DiscoveredFile],
        context: ExtractionContext,
    ) -> Tuple[int, int]:
        """
        Extract files using this strategy.

        Args:
            files: List of files to extract
            context: Extraction context with output dir, callbacks, etc.

        Returns:
            Tuple of (extracted_count, error_count)
        """
        ...


# Utility functions shared by strategies

def stream_copy_with_hash(
    source,
    dest_path: Path,
    chunk_size: int = CHUNK_SIZE,
    compute_hash: bool = True,
) -> Tuple[int, Optional[str], Optional[str]]:
    """
    Stream copy from file-like source to destination with optional hashing.

    Args:
        source: File-like object to read from
        dest_path: Destination path to write to
        chunk_size: Read chunk size
        compute_hash: Whether to compute MD5/SHA256 (default True)

    Returns:
        Tuple of (file_size, md5_hex or None, sha256_hex or None)
    """
    if compute_hash:
        md5 = hashlib.md5()
        sha256 = hashlib.sha256()
    file_size = 0

    with open(dest_path, 'wb') as f:
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            f.write(chunk)
            if compute_hash:
                md5.update(chunk)
                sha256.update(chunk)
            file_size += len(chunk)

    if compute_hash:
        return file_size, md5.hexdigest(), sha256.hexdigest()
    return file_size, None, None


def stream_copy_with_hash_from_iterator(
    chunks: Iterator[bytes],
    dest_path: Path,
    compute_hash: bool = True,
) -> Tuple[int, Optional[str], Optional[str]]:
    """
    Stream copy from chunk iterator to destination with optional hashing.

    Args:
        chunks: Iterator yielding byte chunks
        dest_path: Destination path
        compute_hash: Whether to compute MD5/SHA256 (default True)

    Returns:
        Tuple of (file_size, md5_hex or None, sha256_hex or None)
    """
    if compute_hash:
        md5 = hashlib.md5()
        sha256 = hashlib.sha256()
    file_size = 0

    with open(dest_path, 'wb') as f:
        for chunk in chunks:
            f.write(chunk)
            if compute_hash:
                md5.update(chunk)
                sha256.update(chunk)
            file_size += len(chunk)

    if compute_hash:
        return file_size, md5.hexdigest(), sha256.hexdigest()
    return file_size, None, None


def compute_file_hashes(file_path: Path) -> Tuple[str, str]:
    """
    Compute MD5 and SHA256 hashes of a file.

    Args:
        file_path: Path to file

    Returns:
        Tuple of (md5_hex, sha256_hex)
    """
    md5 = hashlib.md5()
    sha256 = hashlib.sha256()

    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            md5.update(chunk)
            sha256.update(chunk)

    return md5.hexdigest(), sha256.hexdigest()


def extract_profile_from_path(path: str) -> Optional[str]:
    """
    Extract Firefox profile name from cache path.

    Path patterns (cross-platform):
    - Windows: .../Mozilla/Firefox/Profiles/xxxxxxxx.profile_name/cache2/entries/...
    - Linux:   .../.mozilla/firefox/xxxxxxxx.profile_name/cache2/entries/...
    - macOS:   .../Library/Caches/Firefox/Profiles/xxxxxxxx.profile_name/cache2/...

    Args:
        path: Full path to cache file

    Returns:
        Profile identifier or None
    """
    import re

    # Pattern 1: Windows-style Profiles/<profile_folder>/
    match = re.search(r'Profiles[/\\]([^/\\]+)[/\\]', path, re.IGNORECASE)
    if match:
        return match.group(1)

    # Pattern 2: Linux-style .mozilla/firefox/<profile_folder>/cache2
    # or mozilla/firefox/<profile_folder>/cache2
    match = re.search(r'\.?mozilla[/\\]firefox[/\\]([^/\\]+)[/\\]cache2', path, re.IGNORECASE)
    if match:
        return match.group(1)

    return None


def sanitize_filename(name: str, max_length: int = 200) -> str:
    """
    Sanitize filename for filesystem.

    Args:
        name: Original filename
        max_length: Maximum length

    Returns:
        Sanitized filename
    """
    # Remove/replace problematic characters
    for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '\x00']:
        name = name.replace(char, '_')

    # Truncate if needed
    if len(name) > max_length:
        name = name[:max_length]

    return name


def generate_output_filename(source_path: str, index: int = 0) -> str:
    """
    Generate output filename for extracted cache entry.

    Args:
        source_path: Source cache file path
        index: Optional index for uniqueness

    Returns:
        Output filename
    """
    # Use the cache entry filename (32-char hex hash)
    base_name = Path(source_path).name

    # Ensure uniqueness
    if index > 0:
        return f"{base_name}_{index}"

    return base_name
